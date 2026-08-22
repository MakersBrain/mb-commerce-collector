from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from mb_ceramics_catalogue.webshare_gateway_secrets import (
    WebshareGatewaySecret,
    WebshareGatewaySecretStore,
    load_webshare_gateway_secrets,
)
from pydantic import SecretStr

from catalogue_control.webshare_profile_import import (
    MUTATION_ACTION,
    WebshareProfileImportError,
    install_webshare_profile,
    recover_webshare_profile_import,
)

from .conftest import requires_postgres


def _secret(generation: int, *, password: str = "not-in-postgres") -> WebshareGatewaySecret:
    return WebshareGatewaySecret(
        provider="webshare",
        logical_name="operator-gateway",
        generation=generation,
        endpoint_id="webshare-residential-backbone",
        protocol="http",
        host="p.webshare.io",
        port=10_000,
        username=SecretStr("operator-user"),
        password=SecretStr(password),
        countries=frozenset({"FR", "US"}),
        sticky_session_ttl_seconds=600,
    )


async def _cycle(db, *, safe: bool = True):
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
    row = await cursor.fetchone()
    assert row is not None
    return row["cycle_start"]


async def _operation(db, actor: str = "operator@example.test") -> UUID:
    operation_id = uuid4()
    await db.execute(
        """insert into catalogue.proxy_mutation_requests
                  (operation_id, actor, action, idempotency_key)
           values (%(operation)s, %(actor)s, %(action)s, %(key)s)""",
        {
            "operation": operation_id,
            "actor": actor,
            "action": MUTATION_ACTION,
            "key": str(operation_id),
        },
    )
    return operation_id


def _store(tmp_path: Path) -> WebshareGatewaySecretStore:
    directory = tmp_path / "private"
    directory.mkdir(mode=0o700)
    return WebshareGatewaySecretStore(directory / "webshare.json")


async def _create(db, store: WebshareGatewaySecretStore):
    operation_id = await _operation(db)
    result = await install_webshare_profile(
        db,
        store,
        operation_id=operation_id,
        actor_id="operator@example.test",
        secret=_secret(1),
        expected_generation=None,
        display_name="Operator gateway",
        allocated_bytes=250_000,
    )
    return operation_id, result


@pytest.mark.postgres
@requires_postgres
async def test_create_and_rotate_keep_credentials_out_of_postgres(db, tmp_path: Path):
    await _cycle(db)
    store = _store(tmp_path)
    create_operation, created = await _create(db, store)
    assert created.state == "completed"
    assert created.generation == 1

    rotate_operation = await _operation(db)
    rotated = await install_webshare_profile(
        db,
        store,
        operation_id=rotate_operation,
        actor_id="operator@example.test",
        secret=_secret(2, password="rotated-secret"),
        expected_generation=1,
    )
    assert rotated.state == "completed"
    assert rotated.profile_id == created.profile_id
    assert rotated.generation == 2

    profile_cursor = await db.execute(
        """select enabled, lifecycle, secret_generation, provider_resource_id,
                  username_mask, username_fingerprint, pending_action
             from catalogue.proxy_profiles where id = %(profile)s""",
        {"profile": created.profile_id},
    )
    assert await profile_cursor.fetchone() == {
        "enabled": True,
        "lifecycle": "enabled",
        "secret_generation": 2,
        "provider_resource_id": None,
        "username_mask": None,
        "username_fingerprint": None,
        "pending_action": None,
    }
    intents = await db.execute(
        """select operation_id, state, expected_generation, target_generation
             from catalogue.proxy_profile_secret_intents order by created_at"""
    )
    assert await intents.fetchall() == [
        {
            "operation_id": create_operation,
            "state": "completed",
            "expected_generation": None,
            "target_generation": 1,
        },
        {
            "operation_id": rotate_operation,
            "state": "completed",
            "expected_generation": 1,
            "target_generation": 2,
        },
    ]
    installed = load_webshare_gateway_secrets(store.path)[("webshare", "operator-gateway")]
    assert installed.generation == 2
    assert installed.password.get_secret_value() == "rotated-secret"
    database_text = await db.execute(
        """select concat_ws(' ', p::text, i::text, a::text) as text
             from catalogue.proxy_profiles p
             join catalogue.proxy_profile_secret_intents i on i.profile_id = p.id
             join catalogue.proxy_profile_allocations a on a.profile_id = p.id
            limit 1"""
    )
    row = await database_text.fetchone()
    assert row is not None
    assert "operator-user" not in row["text"]
    assert "rotated-secret" not in row["text"]


@pytest.mark.postgres
@requires_postgres
async def test_rotation_drains_only_its_active_reservations_before_writing_intent(
    db, tmp_path: Path
):
    cycle_start = await _cycle(db)
    store = _store(tmp_path)
    _, created = await _create(db, store)
    route = await db.execute(
        """insert into catalogue.proxy_routes
                  (provider, label, profile_id, enabled, created_by, updated_by)
           values ('webshare', 'webshare route', %(profile)s, true, 'test', 'test')
           returning id""",
        {"profile": created.profile_id},
    )
    route_row = await route.fetchone()
    assert route_row is not None
    probe = await db.execute(
        """insert into catalogue.proxy_probes
                  (route_id, profile_id, protocol, actor, request_id)
           values (%(route)s, %(profile)s, 'http', 'test', %(request)s) returning id""",
        {"route": route_row["id"], "profile": created.profile_id, "request": uuid4()},
    )
    probe_row = await probe.fetchone()
    assert probe_row is not None
    reservation = await db.execute(
        """insert into catalogue.proxy_reservations
                  (provider, profile, cycle_start, reserved_bytes, probe_id,
                   profile_id, route_id, purpose, secret_generation)
           values ('webshare', 'operator-gateway', %(cycle)s, 1000, %(probe)s,
                   %(profile)s, %(route)s, 'probe', 1) returning id""",
        {
            "cycle": cycle_start,
            "probe": probe_row["id"],
            "profile": created.profile_id,
            "route": route_row["id"],
        },
    )
    reservation_row = await reservation.fetchone()
    assert reservation_row is not None

    operation_id = await _operation(db)
    result = await install_webshare_profile(
        db,
        store,
        operation_id=operation_id,
        actor_id="operator@example.test",
        secret=_secret(2, password="must-not-be-written"),
        expected_generation=1,
    )
    assert result.state == "draining"
    assert load_webshare_gateway_secrets(store.path)[result.provider, result.logical_name].generation == 1
    intent = await db.execute(
        "select count(*) as count from catalogue.proxy_profile_secret_intents where operation_id = %s",
        (operation_id,),
    )
    assert (await intent.fetchone())["count"] == 0
    profile = await db.execute(
        "select enabled, lifecycle, pending_action from catalogue.proxy_profiles where id = %s",
        (created.profile_id,),
    )
    assert await profile.fetchone() == {
        "enabled": False,
        "lifecycle": "pending",
        "pending_action": None,
    }
    revoked = await db.execute(
        "select state, revocation_requested from catalogue.proxy_reservations where id = %s",
        (reservation_row["id"],),
    )
    assert await revoked.fetchone() == {
        "state": "revocation_requested",
        "revocation_requested": True,
    }


@pytest.mark.postgres
@requires_postgres
async def test_recovery_finalizes_target_generation_after_process_crash(
    db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    await _cycle(db)
    store = _store(tmp_path)
    operation_id = await _operation(db)

    async def crash_after_install(*args, **kwargs):
        raise RuntimeError("simulated process exit")

    monkeypatch.setattr("catalogue_control.webshare_profile_import._finalize", crash_after_install)
    with pytest.raises(RuntimeError, match="simulated process exit"):
        await install_webshare_profile(
            db,
            store,
            operation_id=operation_id,
            actor_id="operator@example.test",
            secret=_secret(1),
            expected_generation=None,
            display_name="Operator gateway",
            allocated_bytes=250_000,
        )
    intent = await db.execute(
        "select state from catalogue.proxy_profile_secret_intents where operation_id = %s",
        (operation_id,),
    )
    assert (await intent.fetchone())["state"] == "prepared"

    monkeypatch.undo()
    recovered = await recover_webshare_profile_import(db, store, operation_id=operation_id)
    assert recovered.state == "completed"
    profile = await db.execute(
        "select enabled, secret_generation from catalogue.proxy_profiles where id = %s",
        (recovered.profile_id,),
    )
    assert await profile.fetchone() == {"enabled": True, "secret_generation": 1}


@pytest.mark.postgres
@requires_postgres
async def test_prepared_recovery_without_installed_generation_fails_closed(
    db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    await _cycle(db)
    store = _store(tmp_path)
    operation_id = await _operation(db)

    def crash_before_install(*args, **kwargs):
        raise RuntimeError("simulated process exit")

    monkeypatch.setattr(store, "install", crash_before_install)
    with pytest.raises(RuntimeError, match="simulated process exit"):
        await install_webshare_profile(
            db,
            store,
            operation_id=operation_id,
            actor_id="operator@example.test",
            secret=_secret(1),
            expected_generation=None,
            display_name="Operator gateway",
            allocated_bytes=250_000,
        )
    recovered = await recover_webshare_profile_import(db, store, operation_id=operation_id)
    assert recovered.state == "failed"
    cursor = await db.execute(
        """select state, error_code, installed_at, completed_at
             from catalogue.proxy_profile_secret_intents where operation_id = %s""",
        (operation_id,),
    )
    row = await cursor.fetchone()
    assert row["state"] == "failed"
    assert row["error_code"] == "credential_resubmission_required"
    assert row["installed_at"] is None
    assert row["completed_at"] is not None

    monkeypatch.undo()
    retry_operation = await _operation(db)
    retried = await install_webshare_profile(
        db,
        store,
        operation_id=retry_operation,
        actor_id="operator@example.test",
        secret=_secret(1, password="resubmitted-secret"),
        expected_generation=None,
        display_name="Operator gateway",
        allocated_bytes=250_000,
    )
    assert retried.state == "completed"
    assert retried.profile_id == recovered.profile_id
    allocations = await db.execute(
        "select count(*) as count from catalogue.proxy_profile_allocations"
    )
    assert (await allocations.fetchone())["count"] == 1


@pytest.mark.postgres
@requires_postgres
async def test_invalid_constructed_secret_is_rejected_before_database_mutation(db, tmp_path: Path):
    await _cycle(db)
    store = _store(tmp_path)
    operation_id = await _operation(db)
    invalid = replace(_secret(1), host="unverified.example")
    with pytest.raises(WebshareProfileImportError) as raised:
        await install_webshare_profile(
            db,
            store,
            operation_id=operation_id,
            actor_id="operator@example.test",
            secret=invalid,
            expected_generation=None,
            display_name="Operator gateway",
            allocated_bytes=250_000,
        )
    assert raised.value.code == "invalid_secret"
    profiles = await db.execute("select count(*) as count from catalogue.proxy_profiles")
    assert (await profiles.fetchone())["count"] == 0


@pytest.mark.postgres
@requires_postgres
async def test_resumed_operation_must_match_its_original_identity(
    db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    await _cycle(db)
    store = _store(tmp_path)
    operation_id = await _operation(db)

    monkeypatch.setattr(
        store,
        "install",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("crash")),
    )
    with pytest.raises(RuntimeError, match="crash"):
        await install_webshare_profile(
            db,
            store,
            operation_id=operation_id,
            actor_id="operator@example.test",
            secret=_secret(1),
            expected_generation=None,
            display_name="Operator gateway",
            allocated_bytes=250_000,
        )
    monkeypatch.undo()

    with pytest.raises(WebshareProfileImportError) as raised:
        await install_webshare_profile(
            db,
            store,
            operation_id=operation_id,
            actor_id="operator@example.test",
            secret=replace(_secret(1), logical_name="different-gateway"),
            expected_generation=None,
            display_name="Operator gateway",
            allocated_bytes=250_000,
        )
    assert raised.value.code == "operation_intent_mismatch"
    assert not store.path.exists()


@pytest.mark.postgres
@requires_postgres
async def test_unsafe_cycle_rejects_before_profile_intent_or_file(db, tmp_path: Path):
    await _cycle(db, safe=False)
    store = _store(tmp_path)
    operation_id = await _operation(db)
    with pytest.raises(WebshareProfileImportError) as raised:
        await install_webshare_profile(
            db,
            store,
            operation_id=operation_id,
            actor_id="operator@example.test",
            secret=_secret(1),
            expected_generation=None,
            display_name="Operator gateway",
            allocated_bytes=250_000,
        )
    assert raised.value.code == "webshare_cycle_unsafe"
    profiles = await db.execute("select count(*) as count from catalogue.proxy_profiles")
    intents = await db.execute("select count(*) as count from catalogue.proxy_profile_secret_intents")
    assert (await profiles.fetchone())["count"] == 0
    assert (await intents.fetchone())["count"] == 0
    assert not store.path.exists()
