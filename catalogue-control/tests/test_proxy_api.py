"""Proxy control-plane authorization and mutation tests against PostgreSQL."""

import base64
import json
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from mb_ceramics_catalogue.providers.base import (
    ProviderError,
    Subscription,
    SubUser,
    UsageBucket,
    UsageReport,
)

from catalogue_control.app import create_app
from catalogue_control.proxy_api import _probe_identity
from catalogue_control.proxy_control import finalize_draining_profiles
from catalogue_control.settings import Settings

from .conftest import TOKEN, postgres_dsn, requires_postgres


def test_probe_identity_accepts_decodos_nested_proxy_shape():
    assert _probe_identity({"proxy": {"ip": "192.0.2.4", "country_code": "fr"}}) == {
        "exit_ip": "192.0.2.4",
        "exit_country": "FR",
    }


class FakeProvider:
    def __init__(self) -> None:
        self.created = 0
        self.create_error: ProviderError | None = None
        self.usage_groupings: list[str] = []
        self.updated_resources: list[str] = []
        self.subscription_days = 30
        self.subscription_limit: int | None = 3_000_000_000
        self.usage_error: ProviderError | None = None

    async def create_subuser(self, **values):
        if self.create_error is not None:
            raise self.create_error
        self.created += 1
        return SubUser(
            id=f"sub-{self.created}", username=values["username"], status="active",
            traffic_limit_bytes=values["traffic_limit_bytes"], auto_disable=True,
        )

    async def subscription(self):
        now = datetime.now(UTC)
        return Subscription(
            service_type="residential_proxies", traffic_limit_bytes=self.subscription_limit,
            raw_traffic_limit=3, valid_from=now - timedelta(days=1),
            valid_until=now + timedelta(days=self.subscription_days - 1), users_limit=5,
        )

    async def update_subuser(self, resource_id, **values):
        self.updated_resources.append(resource_id)
        return SubUser(id=resource_id, username=resource_id, status=values.get("status", "active"))

    async def usage(self, start, end, *, group_by="day"):
        self.usage_groupings.append(group_by)
        if self.usage_error is not None:
            raise self.usage_error
        key = start.isoformat() if group_by == "day" else "example.test"
        return UsageReport(
            total_transmitted_bytes=100, total_received_bytes=900, total_bytes=1000,
            requests=2,
            buckets=[UsageBucket(
                key=key, transmitted_bytes=100, received_bytes=900,
                total_bytes=1000, requests=2,
            )],
        )


def assertion(
    private_key, method: str, path: str, *, nonce=None, role="admin", auth_time=None,
):
    now = int(time.time())
    claims = {
        "kid": "test", "sub": "operator@example.test", "role": role,
        "aud": "catalogue-control", "iat": now, "exp": now + 45,
        "nonce": str(nonce or uuid4()), "method": method, "path": path,
        "auth_time": now if auth_time is None else auth_time,
    }
    raw = json.dumps(claims, separators=(",", ":")).encode()
    return {
        "x-catalogue-actor": base64.urlsafe_b64encode(raw).rstrip(b"=").decode(),
        "x-catalogue-actor-signature": base64.urlsafe_b64encode(private_key.sign(raw)).rstrip(b"=").decode(),
    }


@pytest.fixture
async def proxy_client(db, tmp_path):
    private = Ed25519PrivateKey.generate()
    public_pem = private.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode()
    public_file = tmp_path / "operator-public.json"
    public_file.write_text(json.dumps({"test": public_pem}))
    secret_file = tmp_path / "profiles.json"
    fake = FakeProvider()
    settings = Settings(
        dsn=postgres_dsn() or "", control_token=TOKEN,
        proxy_actor_public_keys_file=public_file, proxy_secret_file=secret_file,
        proxy_webshare_gateway_secret_file=tmp_path / "webshare-gateway.json",
        proxy_mutations_enabled=True, proxy_enabled=False,
    )
    app = create_app(settings, proxy_provider=fake)
    async with app.router.lifespan_context(app), httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://control",
        headers={"authorization": f"Bearer {TOKEN}"},
    ) as client:
        client.app = app  # type: ignore[attr-defined]
        app.state.providers["iproyal"] = fake
        app.state.providers["webshare"] = fake
        yield client, private, fake, secret_file


@pytest.mark.postgres
@requires_postgres
async def test_proxy_reads_require_a_verified_operator(proxy_client):
    client, private, _, _ = proxy_client
    assert (await client.get("/v1/proxy/overview")).status_code == 403
    viewer = assertion(private, "GET", "/v1/proxy/overview", role="viewer")
    assert (await client.get("/v1/proxy/overview", headers=viewer)).status_code == 200

    reservations_path = "/v1/proxy/reservations"
    reservations_viewer = assertion(private, "GET", reservations_path, role="viewer")
    reservations = await client.get(reservations_path, headers=reservations_viewer)
    assert reservations.status_code == 200
    assert reservations.json() == {"reservations": []}

    wrong_path = assertion(private, "GET", "/v1/proxy/cycles", role="viewer")
    assert (await client.get("/v1/proxy/overview", headers=wrong_path)).status_code == 403
    forged = dict(viewer)
    forged["x-catalogue-actor-signature"] = "AAAA"
    assert (await client.get("/v1/proxy/overview", headers=forged)).status_code == 403


@pytest.mark.postgres
@requires_postgres
async def test_provider_write_requires_recent_authentication(proxy_client):
    client, private, _, _ = proxy_client
    path = "/v1/proxy/profiles"
    headers = {
        **assertion(private, "POST", path, auth_time=int(time.time()) - 601),
        "idempotency-key": "expired-auth",
    }
    assert (await client.post(path, json={}, headers=headers)).status_code == 403


@pytest.mark.postgres
@requires_postgres
async def test_promoted_source_can_transition_to_always(proxy_client, db):
    client, private, _, _ = proxy_client
    profile_id = uuid4()
    route_id = uuid4()
    await db.execute(
        """insert into catalogue.proxy_profiles
                  (id, provider, logical_name, provider_resource_id, display_name,
                   username_mask, username_fingerprint, enabled, lifecycle,
                   created_by, updated_by)
           values (%(id)s, 'decodo', 'promoted-test', 'provider-user', 'Promoted test',
                   'user-***', 'fingerprint', true, 'enabled',
                   'operator@example.test', 'operator@example.test')""",
        {"id": profile_id},
    )
    await db.execute(
        """insert into catalogue.proxy_routes
                  (id, provider, label, profile_id, protocol, max_bytes, pilot, enabled,
                   created_by, updated_by)
           values (%(id)s, 'decodo', 'Promoted route', %(profile)s, 'http', 25000000,
                   true, true, 'operator@example.test', 'operator@example.test')""",
        {"id": route_id, "profile": profile_id},
    )
    await db.execute(
        """insert into catalogue.source_proxy_policies
                  (source_id, policy, route_id, max_bytes, pilot, evidence_count,
                   evidence_state, updated_by)
           values ('the-ceramic-shop', 'fallback', %(route)s, 25000000, true,
                   3, 'promoted', 'operator@example.test')""",
        {"route": route_id},
    )
    path = "/v1/sources/the-ceramic-shop"
    response = await client.put(
        path,
        json={
            "proxy": {
                "policy": "always",
                "route_id": str(route_id),
                "max_megabytes": 25,
                "pilot": True,
            }
        },
        headers=assertion(private, "PUT", path),
    )
    assert response.status_code == 200, response.text
    assert response.json()["proxy"]["policy"] == "always"


@pytest.mark.postgres
@requires_postgres
async def test_mutation_nonce_is_single_use_and_idempotency_replays(proxy_client, db):
    client, private, _, _ = proxy_client
    now = datetime.now(UTC)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
             (provider, cycle_start, cycle_end, purchased_bytes, operational_bytes,
              daily_bytes, pilot_bytes, lifecycle, reconciliation_ok, reconciled_at)
             values ('decodo', %(start)s, %(end)s, 3000000000, 2400000000,
                     80000000, 300000000, 'active', true, now())""",
        {"start": now - timedelta(days=1), "end": now + timedelta(days=29)},
    )
    path = "/v1/proxy/kill-switch/activate"
    nonce = uuid4()
    headers = {**assertion(private, "POST", path, nonce=nonce), "idempotency-key": "stop-once"}
    first = await client.post(path, json={}, headers=headers)
    assert first.status_code == 202
    assert (await client.post(path, json={}, headers=headers)).status_code == 403

    replay_headers = {**assertion(private, "POST", path), "idempotency-key": "stop-once"}
    replay = await client.post(path, json={}, headers=replay_headers)
    assert replay.status_code == 202
    assert replay.json() == first.json()


@pytest.mark.postgres
@requires_postgres
async def test_profile_creation_is_bounded_and_installs_dynamic_secret(proxy_client, db):
    client, private, fake, secret_file = proxy_client
    now = datetime.now(UTC)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
             (provider, cycle_start, cycle_end, purchased_bytes, operational_bytes,
              daily_bytes, pilot_bytes, lifecycle)
             values ('decodo', %(start)s, %(end)s, 3000000000, 2400000000,
                     80000000, 300000000, 'active')""",
        {"start": now - timedelta(days=1), "end": now + timedelta(days=29)},
    )
    path = "/v1/proxy/profiles"
    response = await client.post(path, json={
        "logical_name": "primary", "display_name": "Primary",
        "allocated_bytes": 100_000_000, "provider_traffic_limit_bytes": 90_000_000,
        "confirmation": "CREATE primary",
    }, headers={**assertion(private, "POST", path), "idempotency-key": "create-primary"})
    assert response.status_code == 201
    assert fake.created == 1
    installed = json.loads(secret_file.read_text())["primary"]
    assert installed["generation"] == 1
    assert "password" not in response.text and installed["password"] not in response.text


@pytest.mark.postgres
@requires_postgres
async def test_profile_creation_persists_the_selected_provider(proxy_client, db):
    client, private, fake, _ = proxy_client
    now = datetime.now(UTC)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
             (provider, cycle_start, cycle_end, purchased_bytes, operational_bytes,
              daily_bytes, pilot_bytes, lifecycle)
             values ('iproyal', %(start)s, %(end)s, 3000000000, 2400000000,
                     80000000, 300000000, 'active')""",
        {"start": now - timedelta(days=1), "end": now + timedelta(days=29)},
    )
    path = "/v1/proxy/profiles?provider=iproyal"
    response = await client.post(
        path,
        json={
            "logical_name": "iproyal-primary",
            "display_name": "IPRoyal primary",
            "allocated_bytes": 100_000_000,
            "provider_traffic_limit_bytes": 90_000_000,
            "confirmation": "CREATE iproyal-primary",
        },
        headers={
            **assertion(private, "POST", "/v1/proxy/profiles"),
            "idempotency-key": "create-iproyal-primary",
        },
    )

    assert response.status_code == 201, response.text
    assert fake.created == 1
    profile_cursor = await db.execute(
        "select provider from catalogue.proxy_profiles where logical_name = 'iproyal-primary'"
    )
    allocation_cursor = await db.execute(
        """select provider from catalogue.proxy_profile_allocations
            where profile_id = (
                select id from catalogue.proxy_profiles where logical_name = 'iproyal-primary'
            )"""
    )
    assert (await profile_cursor.fetchone())["provider"] == "iproyal"
    assert (await allocation_cursor.fetchone())["provider"] == "iproyal"


@pytest.mark.postgres
@requires_postgres
async def test_webshare_profile_provisioning_fails_before_local_intent(proxy_client, db):
    client, private, fake, _ = proxy_client
    path = "/v1/proxy/profiles?provider=webshare"

    response = await client.post(
        path,
        json={
            "logical_name": "webshare-primary",
            "allocated_bytes": 100_000_000,
            "confirmation": "CREATE webshare-primary",
        },
        headers={
            **assertion(private, "POST", "/v1/proxy/profiles"),
            "idempotency-key": "unsupported-webshare-profile",
        },
    )

    assert response.status_code == 409
    assert response.json()["title"] == "Profile provisioning unsupported"
    assert fake.created == 0
    cursor = await db.execute(
        "select count(*) as count from catalogue.proxy_profiles where provider = 'webshare'"
    )
    assert (await cursor.fetchone())["count"] == 0


@pytest.mark.postgres
@requires_postgres
@pytest.mark.parametrize(
    ("subscription_days", "subscription_limit", "title"),
    [
        (91, 3_000_000_000, "Provider usage window unsupported"),
        (30, None, "Finite cycle ceiling required"),
    ],
)
async def test_webshare_cycle_proposal_refuses_unreconcilable_terms(
    proxy_client,
    db,
    subscription_days,
    subscription_limit,
    title,
):
    client, private, fake, _ = proxy_client
    fake.subscription_days = subscription_days
    fake.subscription_limit = subscription_limit
    path = "/v1/proxy/cycles/propose?provider=webshare"

    response = await client.post(
        path,
        json={},
        headers=assertion(private, "POST", "/v1/proxy/cycles/propose"),
    )

    assert response.status_code == 409
    assert response.json()["title"] == title
    cursor = await db.execute(
        "select count(*) as count from catalogue.proxy_budget_cycles where provider = 'webshare'"
    )
    assert (await cursor.fetchone())["count"] == 0


@pytest.mark.postgres
@requires_postgres
async def test_conclusive_profile_rejection_releases_local_allocation(proxy_client, db):
    client, private, fake, _ = proxy_client
    now = datetime.now(UTC)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
             (provider, cycle_start, cycle_end, purchased_bytes, operational_bytes,
              daily_bytes, pilot_bytes, lifecycle)
             values ('decodo', %(start)s, %(end)s, 3000000000, 2400000000,
                     80000000, 300000000, 'active')""",
        {"start": now - timedelta(days=1), "end": now + timedelta(days=29)},
    )
    fake.create_error = ProviderError("provider_bad_request_password", "rejected")
    path = "/v1/proxy/profiles"
    response = await client.post(path, json={
        "logical_name": "retryable", "display_name": "Retryable",
        "allocated_bytes": 100_000_000, "provider_traffic_limit_bytes": 90_000_000,
        "confirmation": "CREATE retryable",
    }, headers={**assertion(private, "POST", path), "idempotency-key": "rejected-profile"})
    assert response.status_code == 502
    assert response.json()["error"] == "provider_bad_request_password"
    profile = await db.execute(
        "select count(*) as n from catalogue.proxy_profiles where logical_name = 'retryable'"
    )
    allocation = await db.execute(
        "select count(*) as n from catalogue.proxy_profile_allocations"
    )
    assert (await profile.fetchone())["n"] == 0
    assert (await allocation.fetchone())["n"] == 0


@pytest.mark.postgres
@requires_postgres
async def test_proxy_audit_is_immutable_for_runtime_connection(proxy_client, db):
    client, private, _, _ = proxy_client
    path = "/v1/proxy/kill-switch/activate"
    await client.post(
        path, json={},
        headers={**assertion(private, "POST", path), "idempotency-key": "audit-row"},
    )
    with pytest.raises(Exception, match="proxy audit rows are immutable"):
        await db.execute("delete from catalogue.proxy_admin_audit")


@pytest.mark.postgres
@requires_postgres
async def test_proxy_audit_maintenance_checks_invoking_session(proxy_client, db):
    client, private, _, _ = proxy_client
    path = "/v1/proxy/kill-switch/activate"
    await client.post(
        path, json={},
        headers={**assertion(private, "POST", path), "idempotency-key": "audit-session"},
    )
    await db.execute("create role catalogue_proxy_maintenance")
    await db.execute("create role catalogue_proxy_untrusted")
    await db.execute("grant usage on schema catalogue to catalogue_proxy_untrusted")
    await db.execute(
        "grant select, delete on catalogue.proxy_admin_audit to catalogue_proxy_untrusted"
    )
    try:
        await db.execute("set session authorization catalogue_proxy_untrusted")
        await db.execute("set catalogue.proxy_audit_maintenance = 'on'")
        with pytest.raises(Exception, match="proxy audit rows are immutable"):
            await db.execute("delete from catalogue.proxy_admin_audit")
    finally:
        await db.execute("reset session authorization")
        await db.execute("drop owned by catalogue_proxy_untrusted")
        await db.execute("drop role catalogue_proxy_untrusted")
        await db.execute("drop role catalogue_proxy_maintenance")


@pytest.mark.postgres
@requires_postgres
async def test_reconciliation_persists_supported_provider_groupings(proxy_client, db):
    client, private, _, _ = proxy_client
    now = datetime.now(UTC)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
             (provider, cycle_start, cycle_end, purchased_bytes, operational_bytes,
              daily_bytes, pilot_bytes, lifecycle)
             values ('decodo', %(start)s, %(end)s, 3000000000, 2400000000,
                     80000000, 300000000, 'active')""",
        {"start": now - timedelta(days=1), "end": now + timedelta(days=29)},
    )
    path = "/v1/proxy/reconcile"
    response = await client.post(
        path, json={},
        headers={**assertion(private, "POST", path), "idempotency-key": "reconcile-groups"},
    )
    assert response.status_code == 202
    cursor = await db.execute(
        """select grouping_dimension, total_bytes
             from catalogue.proxy_provider_snapshots order by grouping_dimension"""
    )
    assert [(row["grouping_dimension"], row["total_bytes"]) for row in await cursor.fetchall()] == [
        ("day", 1000), ("target", 1000),
    ]


@pytest.mark.postgres
@requires_postgres
async def test_webshare_reconciliation_is_cycle_total_and_provider_isolated(proxy_client, db):
    client, private, fake, _ = proxy_client
    now = datetime.now(UTC)
    start = now - timedelta(days=1)
    end = now + timedelta(days=29)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
             (provider, cycle_start, cycle_end, purchased_bytes, operational_bytes,
              daily_bytes, pilot_bytes, lifecycle, provider_reported_bytes,
              reconciliation_ok, reconciled_at)
             values
               ('decodo', %(start)s, %(end)s, 3000000000, 2400000000,
                80000000, 300000000, 'active', 77, true, now()),
               ('webshare', %(start)s, %(end)s, 3000000000, 2400000000,
                80000000, 300000000, 'active', 0, false, null)""",
        {"start": start, "end": end},
    )
    await db.execute(
        """insert into catalogue.proxy_reconcile_requests (provider, reason, dedup_key)
             values ('decodo', 'test', 'decodo:test'),
                    ('webshare', 'test', 'webshare:test')"""
    )
    path = "/v1/proxy/reconcile?provider=webshare"

    response = await client.post(
        path,
        json={},
        headers={
            **assertion(private, "POST", "/v1/proxy/reconcile"),
            "idempotency-key": "reconcile-webshare-total",
        },
    )

    assert response.status_code == 202, response.text
    assert fake.usage_groupings == ["total"]
    snapshots = await db.execute(
        """select provider, grouping_dimension, grouping_key, bucket_start, bucket_end,
                  total_bytes
             from catalogue.proxy_provider_snapshots"""
    )
    assert await snapshots.fetchall() == [
        {
            "provider": "webshare",
            "grouping_dimension": "total",
            "grouping_key": "total",
            "bucket_start": start,
            "bucket_end": end,
            "total_bytes": 1000,
        }
    ]
    cycles = await db.execute(
        """select provider, provider_reported_bytes, reconciliation_ok
             from catalogue.proxy_budget_cycles order by provider"""
    )
    assert await cycles.fetchall() == [
        {"provider": "decodo", "provider_reported_bytes": 77, "reconciliation_ok": True},
        {"provider": "webshare", "provider_reported_bytes": 1000, "reconciliation_ok": True},
    ]
    pending = await db.execute(
        """select provider, completed_at is not null as completed
             from catalogue.proxy_reconcile_requests order by provider"""
    )
    assert await pending.fetchall() == [
        {"provider": "decodo", "completed": False},
        {"provider": "webshare", "completed": True},
    ]


@pytest.mark.postgres
@requires_postgres
async def test_webshare_reconciliation_failure_cannot_mark_decodo_unsafe(proxy_client, db):
    client, private, fake, _ = proxy_client
    fake.usage_error = ProviderError("provider_unavailable", "provider unavailable")
    now = datetime.now(UTC)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
             (provider, cycle_start, cycle_end, purchased_bytes, operational_bytes,
              daily_bytes, pilot_bytes, lifecycle, reconciliation_ok, reconciled_at,
              kill_switch)
             values
               ('decodo', %(start)s, %(end)s, 3000000000, 2400000000,
                80000000, 300000000, 'active', true, now(), false),
               ('webshare', %(start)s, %(end)s, 3000000000, 2400000000,
                80000000, 300000000, 'active', true, now(), false)""",
        {"start": now - timedelta(days=1), "end": now + timedelta(days=29)},
    )
    path = "/v1/proxy/reconcile?provider=webshare"

    response = await client.post(
        path,
        json={},
        headers={
            **assertion(private, "POST", "/v1/proxy/reconcile"),
            "idempotency-key": "reconcile-webshare-failure",
        },
    )

    assert response.status_code == 502
    cycles = await db.execute(
        """select provider, reconciliation_ok, kill_switch
             from catalogue.proxy_budget_cycles order by provider"""
    )
    assert await cycles.fetchall() == [
        {"provider": "decodo", "reconciliation_ok": True, "kill_switch": False},
        {"provider": "webshare", "reconciliation_ok": False, "kill_switch": False},
    ]


@pytest.mark.postgres
@requires_postgres
async def test_profile_finalization_cannot_cross_provider_boundary(proxy_client, db):
    _, _, fake, secret_file = proxy_client
    await db.execute(
        """insert into catalogue.proxy_profiles
             (provider, logical_name, provider_resource_id, display_name, lifecycle,
              pending_action, created_by, updated_by)
             values ('decodo', 'decodo-draining', 'decodo-resource', 'Decodo', 'draining',
                     'disable', 'test', 'test'),
                    ('webshare', 'webshare-draining', 'webshare-resource', 'Webshare', 'draining',
                     'disable', 'test', 'test')"""
    )

    await finalize_draining_profiles(
        db,
        fake,
        secret_file,
        provider_name="webshare",
    )

    assert fake.updated_resources == ["webshare-resource"]
    profiles = await db.execute(
        """select provider, lifecycle, pending_action
             from catalogue.proxy_profiles order by provider"""
    )
    assert await profiles.fetchall() == [
        {"provider": "decodo", "lifecycle": "draining", "pending_action": "disable"},
        {"provider": "webshare", "lifecycle": "disabled", "pending_action": None},
    ]


def webshare_import_body(
    generation: int,
    *,
    expected_generation: int | None,
    password: str,
) -> dict:
    creating = expected_generation is None
    return {
        "profile": {
            "provider": "webshare",
            "logical_name": "operator-gateway",
            "generation": generation,
            "gateway": {
                "endpoint_id": "webshare-residential-backbone",
                "protocol": "http",
                "host": "p.webshare.io",
                "port": 10_000,
            },
            "credentials": {"username": "issued-user", "password": password},
            "capabilities": {
                "countries": ["FR", "US"],
                "sticky_session_ttl_seconds": 600,
            },
        },
        "expected_generation": expected_generation,
        "display_name": "Operator gateway" if creating else None,
        "allocated_bytes": 250_000 if creating else None,
        "confirmation": f"IMPORT webshare/operator-gateway GENERATION {generation}",
    }


async def insert_safe_webshare_cycle(db, *, safe: bool = True):
    now = datetime.now(UTC)
    cursor = await db.execute(
        """insert into catalogue.proxy_budget_cycles
                  (provider, cycle_start, cycle_end, purchased_bytes, operational_bytes,
                   daily_bytes, pilot_bytes, lifecycle, reconciliation_ok, reconciled_at,
                   kill_switch)
           values ('webshare', %(start)s, %(end)s, 1000000, 800000,
                   100000, 100000, 'active', %(safe)s,
                   case when %(safe)s then now() else null end, false)
           returning cycle_start""",
        {"start": now - timedelta(days=1), "end": now + timedelta(days=29), "safe": safe},
    )
    return (await cursor.fetchone())["cycle_start"]


@pytest.mark.postgres
@requires_postgres
async def test_webshare_import_requires_admin_idempotency_and_replays_without_credentials(
    proxy_client, db, tmp_path
):
    client, private, fake, _ = proxy_client
    await insert_safe_webshare_cycle(db)
    url = "/v1/proxy/profiles/import?provider=webshare"
    signed_path = "/v1/proxy/profiles/import"
    body = webshare_import_body(1, expected_generation=None, password="issued-password")

    assert (await client.post(url, json=body)).status_code == 403
    stale = await client.post(
        url,
        json=body,
        headers=assertion(
            private, "POST", signed_path, auth_time=int(time.time()) - 601
        ),
    )
    assert stale.status_code == 403
    viewer = await client.post(
        url,
        json=body,
        headers=assertion(private, "POST", signed_path, role="viewer"),
    )
    assert viewer.status_code == 403
    client.app.state.settings.proxy_mutations_enabled = False
    gated = await client.post(
        url,
        json=body,
        headers={
            **assertion(private, "POST", signed_path),
            "idempotency-key": "mutations-disabled",
        },
    )
    assert gated.status_code == 409
    client.app.state.settings.proxy_mutations_enabled = True
    no_key = await client.post(
        url, json=body, headers=assertion(private, "POST", signed_path)
    )
    assert no_key.status_code == 409
    encoded = json.dumps(body)
    duplicate = encoded[:-1] + ',"confirmation":"duplicate"}'
    strict = await client.post(
        url,
        content=duplicate,
        headers={
            **assertion(private, "POST", signed_path),
            "content-type": "application/json",
            "idempotency-key": "duplicate-json",
        },
    )
    assert strict.status_code == 422
    assert "issued-password" not in strict.text

    headers = {
        **assertion(private, "POST", signed_path),
        "idempotency-key": "webshare-create",
    }
    created = await client.post(url, json=body, headers=headers)
    assert created.status_code == 201, created.text
    data = created.json()
    assert data["state"] == "completed"
    assert data["provider"] == "webshare"
    assert data["generation"] == 1
    assert "issued-user" not in created.text
    assert "issued-password" not in created.text
    assert fake.created == 0

    replay = await client.post(
        url,
        json=body,
        headers={
            **assertion(private, "POST", signed_path),
            "idempotency-key": "webshare-create",
        },
    )
    assert replay.status_code == 201
    assert replay.json() == data

    database = await db.execute(
        """select concat_ws(' ', m::text, a::text, p::text, i::text) as contents
             from catalogue.proxy_mutation_requests m
             join catalogue.proxy_admin_audit a on a.operation_id = m.operation_id
             join catalogue.proxy_profile_secret_intents i on i.operation_id = m.operation_id
             join catalogue.proxy_profiles p on p.id = i.profile_id
            where m.operation_id = %(operation)s limit 1""",
        {"operation": data["operation_id"]},
    )
    contents = (await database.fetchone())["contents"]
    assert "issued-user" not in contents
    assert "issued-password" not in contents
    stored = json.loads((tmp_path / "webshare-gateway.json").read_text())
    assert stored["profiles"]["webshare/operator-gateway"]["credentials"] == {
        "username": "issued-user",
        "password": "issued-password",
    }


@pytest.mark.postgres
@requires_postgres
async def test_webshare_rotation_draining_is_terminal_and_new_key_finishes(
    proxy_client, db, tmp_path
):
    client, private, _, _ = proxy_client
    cycle_start = await insert_safe_webshare_cycle(db)
    url = "/v1/proxy/profiles/import?provider=webshare"
    signed_path = "/v1/proxy/profiles/import"
    create = await client.post(
        url,
        json=webshare_import_body(1, expected_generation=None, password="first-password"),
        headers={
            **assertion(private, "POST", signed_path),
            "idempotency-key": "create-before-drain",
        },
    )
    assert create.status_code == 201, create.text
    profile_id = create.json()["profile_id"]
    route = await db.execute(
        """insert into catalogue.proxy_routes
                  (provider, label, profile_id, enabled, created_by, updated_by)
           values ('webshare', 'route', %(profile)s, true, 'test', 'test') returning id""",
        {"profile": profile_id},
    )
    route_id = (await route.fetchone())["id"]
    probe = await db.execute(
        """insert into catalogue.proxy_probes
                  (route_id, profile_id, protocol, actor, request_id)
           values (%(route)s, %(profile)s, 'http', 'test', %(request)s) returning id""",
        {"route": route_id, "profile": profile_id, "request": uuid4()},
    )
    probe_id = (await probe.fetchone())["id"]
    reservation = await db.execute(
        """insert into catalogue.proxy_reservations
                  (provider, profile, cycle_start, reserved_bytes, probe_id,
                   profile_id, route_id, purpose, secret_generation)
           values ('webshare', 'operator-gateway', %(cycle)s, 1000, %(probe)s,
                   %(profile)s, %(route)s, 'probe', 1) returning id""",
        {
            "cycle": cycle_start,
            "probe": probe_id,
            "profile": profile_id,
            "route": route_id,
        },
    )
    reservation_id = (await reservation.fetchone())["id"]
    rotation = webshare_import_body(
        2, expected_generation=1, password="rotated-password"
    )
    drain_headers = {
        **assertion(private, "POST", signed_path),
        "idempotency-key": "rotate-draining",
    }
    draining = await client.post(url, json=rotation, headers=drain_headers)
    assert draining.status_code == 202, draining.text
    assert draining.json()["state"] == "draining"
    assert "rotated-password" not in draining.text
    replay = await client.post(
        url,
        json=rotation,
        headers={
            **assertion(private, "POST", signed_path),
            "idempotency-key": "rotate-draining",
        },
    )
    assert replay.json() == draining.json()
    mutation = await db.execute(
        """select state from catalogue.proxy_mutation_requests
            where operation_id = %(operation)s""",
        {"operation": draining.json()["operation_id"]},
    )
    assert (await mutation.fetchone())["state"] == "succeeded"

    await db.execute(
        """update catalogue.proxy_reservations
              set state = 'closed', closed_at = now()
            where id = %(reservation)s""",
        {"reservation": reservation_id},
    )
    completed = await client.post(
        url,
        json=rotation,
        headers={
            **assertion(private, "POST", signed_path),
            "idempotency-key": "rotate-after-drain",
        },
    )
    assert completed.status_code == 200, completed.text
    assert completed.json()["state"] == "completed"
    stored = json.loads((tmp_path / "webshare-gateway.json").read_text())
    assert stored["profiles"]["webshare/operator-gateway"]["generation"] == 2


@pytest.mark.postgres
@requires_postgres
async def test_webshare_import_service_error_is_safe_stable_and_replayed(proxy_client, db):
    client, private, _, _ = proxy_client
    await insert_safe_webshare_cycle(db, safe=False)
    url = "/v1/proxy/profiles/import?provider=webshare"
    signed_path = "/v1/proxy/profiles/import"
    body = webshare_import_body(1, expected_generation=None, password="never-return-this")
    headers = {
        **assertion(private, "POST", signed_path),
        "idempotency-key": "unsafe-cycle",
    }
    failed = await client.post(url, json=body, headers=headers)
    assert failed.status_code == 409, failed.text
    assert failed.json()["error_code"] == "webshare_cycle_unsafe"
    assert "never-return-this" not in failed.text
    replay = await client.post(
        url,
        json=body,
        headers={
            **assertion(private, "POST", signed_path),
            "idempotency-key": "unsafe-cycle",
        },
    )
    assert replay.status_code == 409
    assert replay.json() == failed.json()


@pytest.mark.postgres
@requires_postgres
async def test_webshare_installed_remediation_resumes_with_same_key(
    proxy_client, db, monkeypatch: pytest.MonkeyPatch
):
    from catalogue_control import webshare_profile_import as service

    client, private, _, _ = proxy_client
    await insert_safe_webshare_cycle(db)
    url = "/v1/proxy/profiles/import?provider=webshare"
    signed_path = "/v1/proxy/profiles/import"
    created = await client.post(
        url,
        json=webshare_import_body(1, expected_generation=None, password="first-password"),
        headers={
            **assertion(private, "POST", signed_path),
            "idempotency-key": "create-before-remediation",
        },
    )
    assert created.status_code == 201, created.text

    original_finalize = service._finalize

    async def make_cycle_unsafe_after_cas(connection, intent):
        await connection.execute(
            """update catalogue.proxy_budget_cycles set kill_switch = true
                where provider = 'webshare' and lifecycle = 'active'"""
        )
        return await original_finalize(connection, intent)

    monkeypatch.setattr(service, "_finalize", make_cycle_unsafe_after_cas)
    body = webshare_import_body(2, expected_generation=1, password="second-password")
    first = await client.post(
        url,
        json=body,
        headers={
            **assertion(private, "POST", signed_path),
            "idempotency-key": "rotation-remediation",
        },
    )
    assert first.status_code == 202, first.text
    assert first.json()["state"] == "installed"
    assert first.json()["remediation"] == "cycle_rebind_required"
    mutation = await db.execute(
        """select state from catalogue.proxy_mutation_requests
            where operation_id = %(operation)s""",
        {"operation": first.json()["operation_id"]},
    )
    assert (await mutation.fetchone())["state"] == "started"

    monkeypatch.undo()
    await db.execute(
        """update catalogue.proxy_budget_cycles
              set kill_switch = false, reconciliation_ok = true, reconciled_at = now()
            where provider = 'webshare' and lifecycle = 'active'"""
    )
    resumed = await client.post(
        url,
        json=body,
        headers={
            **assertion(private, "POST", signed_path),
            "idempotency-key": "rotation-remediation",
        },
    )
    assert resumed.status_code == 200, resumed.text
    assert resumed.json()["state"] == "completed"
    assert resumed.json()["operation_id"] == first.json()["operation_id"]
