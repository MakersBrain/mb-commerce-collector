"""Paid proxy safety tests that never contact a real provider."""

import asyncio
import io
import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg.rows import dict_row
from pydantic import ValidationError

from mb_ceramics_catalogue.config.settings import CrawlParams
from mb_ceramics_catalogue.config.sources import SourceConfig
from mb_ceramics_catalogue.crawl.session import open_session
from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.proxy import (
    ProxyDenied,
    ProxyLease,
    ProxyProfile,
    authorize_reservation_attempt,
    load_api_key,
    load_profiles,
    provider_usage,
    reconcile,
    reconcile_reservation_attempt,
    redact_url,
    release_reservation_attempt,
    reserve,
    scrub_secrets,
)
from mb_ceramics_catalogue.scrapers.base import BrowserRenderer

from .conftest import postgres_dsn, requires_postgres


class ProxyConfigurationTests(unittest.TestCase):
    def test_authenticated_urls_are_redacted(self):
        value = "connect failed for http://named-user:secret@proxy.example:10000/path"
        cleaned = redact_url(value)
        self.assertEqual(
            "connect failed for http://[REDACTED]@proxy.example:10000/path", cleaned
        )
        self.assertNotIn("secret", cleaned)

    def test_configured_secrets_are_scrubbed_even_outside_urls(self):
        self.assertEqual(
            "credentials [REDACTED]/[REDACTED]",
            scrub_secrets("credentials named-user/secret", {"named-user", "secret"}),
        )

    def test_structured_logs_scrub_urls_registered_values_and_exceptions(self):
        stream = io.StringIO()
        obs.configure(json=True, stream=stream)
        obs.register_secrets({"gateway-password"})
        logger = obs.get_logger("redaction-test")
        try:
            raise RuntimeError(
                "http://user:gateway-password@gate.test failed with gateway-password"
            )
        except RuntimeError:
            logger.exception("proxy.failed", endpoint="http://user:gateway-password@gate.test")
        rendered = stream.getvalue()
        self.assertNotIn("gateway-password", rendered)
        self.assertNotIn("user:", rendered)
        self.assertIn("[REDACTED]", rendered)

    def test_secret_file_must_be_private_and_hosts_cannot_be_urls(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "proxy.json"
            path.write_text(json.dumps({"decodo": {
                "host": "gate.example", "port": 10000,
                "username": "user", "password": "password",
            }}))
            os.chmod(path, 0o644)
            with self.assertRaises(ProxyDenied):
                load_profiles(path)
            os.chmod(path, 0o600)
            self.assertEqual("gate.example", load_profiles(path)["decodo"].host)

            path.write_text(json.dumps({"decodo": {
                "host": "http://user:password@gate.example", "port": 10000,
                "username": "user", "password": "password",
            }}))
            with self.assertRaises(ProxyDenied):
                load_profiles(path)

    def test_decodo_env_is_parsed_without_sourcing_shell_code(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "decodo.env"
            path.write_text("IGNORED=$(must-not-run)\nDECODO_API_KEY='api-value'\n")
            os.chmod(path, 0o600)
            self.assertEqual("api-value", load_api_key(path))

    def test_ordinary_run_params_cannot_enable_or_select_a_proxy(self):
        for params in (
            {"proxy_policy": "always"},
            {"proxy_profile": "decodo"},
            {"proxy_max_megabytes": 100},
            {"proxy_url": "http://user:password@example"},
        ):
            with self.subTest(params=params), self.assertRaises(ValidationError):
                CrawlParams.model_validate(params)
        narrowed = CrawlParams.model_validate(
            {"proxy_policy": "never", "proxy_max_megabytes": 10}
        )
        self.assertEqual("never", narrowed.proxy_policy)
        self.assertEqual(10, narrowed.proxy_max_megabytes)

    def test_removed_static_source_proxy_fields_are_rejected(self):
        common = {"label": "Shop", "url": "https://shop.test", "scraper": "pagecrawl"}
        for field, value in (
            ("proxy_policy", "always"),
            ("proxy_profile", "decodo"),
            ("proxy_country", "FR"),
            ("proxy_session_minutes", 30),
            ("proxy_max_megabytes", 25),
            ("proxy_pilot", True),
        ):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                SourceConfig.model_validate({**common, field: value})


class ProxyLeaseTests(unittest.TestCase):
    def lease(self, maximum=1000):
        profile = ProxyProfile("decodo", "gate.example", 10000, "base-user", "secret")
        return ProxyLease.build(uuid4(), uuid4(), profile, "FR", 30, maximum)

    def test_one_opaque_sticky_identity_is_reused(self):
        lease = self.lease()
        first = lease.url
        self.assertEqual(first, lease.url)
        self.assertTrue(lease.username.startswith("user-base-user-country-fr-session-"))
        self.assertIn(lease.session, lease.username)
        self.assertEqual(lease.username, lease.browser_proxy["username"])
        self.assertEqual("http://gate.example:10000", lease.browser_proxy["server"])
        self.assertNotIn("base-user", lease.display_name)
        self.assertNotIn("secret", lease.display_name)

    def test_session_rotation_requests_a_new_provider_identity(self):
        lease = self.lease()
        previous = lease.session
        lease.rotate_session()
        self.assertNotEqual(previous, lease.session)
        self.assertIn(lease.session, lease.username)

    def test_no_new_request_starts_after_the_reservation_is_spent(self):
        lease = self.lease(100)
        lease.account(40, 60)
        with self.assertRaises(ProxyDenied):
            lease.ensure_request_allowed()

    def test_accounting_never_accepts_negative_bytes(self):
        lease = self.lease()
        lease.account(-10, 20)
        self.assertEqual(20, lease.used_bytes)
        self.assertEqual(1, lease.requests)


async def test_fallback_proxy_has_an_independent_rate_limiter():
    profile = ProxyProfile("decodo", "gate.example", 10000, "base-user", "secret")
    lease = ProxyLease.build(uuid4(), uuid4(), profile, "FR", 30, 100_000)
    params = CrawlParams(browser="never", impersonate="never", cache_mode="off")

    async with open_session(params, proxy_lease=lease, proxy_policy="fallback") as session:
        fallback = session.fetcher.proxy_fallback
        assert fallback is not None
        assert fallback.limiter is not session.limiter
        previous_session = lease.session
        previous_client = fallback.client
        await session.fetcher.rotate_client()
        assert lease.session != previous_session
        assert fallback.client is not previous_client


async def test_provider_usage_reads_upload_plus_download_total():
    seen = {}

    def handler(request):
        seen["authorization"] = request.headers["authorization"]
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={
            "metadata": {"totals": {"total_rx_tx": 123456}}, "data": []
        })

    profile = ProxyProfile(
        "decodo", "gate.example", 7000, "user", "password", api_key="statistics-key"
    )
    now = datetime.now(UTC)
    total = await provider_usage(
        profile, now - timedelta(days=1), now + timedelta(days=1),
        transport=httpx.MockTransport(handler),
    )
    assert total == 123456
    assert seen["authorization"] == "statistics-key"
    assert seen["body"]["proxyType"] == "residential_proxies"


async def test_local_fake_http_proxy_receives_the_sticky_job_identity():
    """Exercise httpx's real proxy path without paid credentials or egress."""
    seen: dict[str, str] = {}

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        request_line = (await reader.readline()).decode("ascii").strip()
        headers: dict[str, str] = {}
        while line := await reader.readline():
            decoded = line.decode("ascii").strip()
            if not decoded:
                break
            name, value = decoded.split(":", 1)
            headers[name.lower()] = value.strip()
        seen["request_line"] = request_line
        seen["authorization"] = headers.get("proxy-authorization", "")
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    profile = ProxyProfile("fake", "127.0.0.1", port, "base-user", "secret")
    lease = ProxyLease.build(uuid4(), uuid4(), profile, "FR", 30, 100_000)
    try:
        async with server, httpx.AsyncClient(proxy=lease.url) as client:
            response = await client.get("http://origin.invalid/catalogue")
    finally:
        server.close()
        await server.wait_closed()
    assert response.text == "ok"
    assert seen["request_line"] == "GET http://origin.invalid/catalogue HTTP/1.1"
    assert seen["authorization"].startswith("Basic ")


async def test_proxied_browser_blocks_noise_and_meters_allowed_subresources():
    lease = ProxyLease.build(
        uuid4(), uuid4(), ProxyProfile("fake", "gate.test", 1000, "user", "secret"),
        "FR", 30, 100_000,
    )
    callbacks = {}

    class Page:
        async def route(self, _pattern, callback):
            callbacks["request"] = callback

        def on(self, _event, callback):
            callbacks["response"] = callback

    class Route:
        def __init__(self):
            self.action = ""

        async def abort(self):
            self.action = "abort"

        async def continue_(self):
            self.action = "continue"

    class Request:
        method = "GET"

        def __init__(self, resource_type, url):
            self.resource_type = resource_type
            self.url = url

    class Response:
        @property
        def headers(self):
            return {"content-length": "321"}

    renderer = BrowserRenderer(True, proxy_lease=lease)
    await renderer._meter_page(Page())
    image = Route()
    await callbacks["request"](image, Request("image", "https://shop.test/large.jpg"))
    assert image.action == "abort"
    assert lease.requests == 0

    script = Route()
    await callbacks["request"](script, Request("script", "https://shop.test/app.js"))
    callbacks["response"](Response())
    assert script.action == "continue"
    assert lease.requests == 1
    assert lease.used_bytes >= 321


@pytest.mark.postgres
@requires_postgres
async def test_atomic_reservations_cannot_oversubscribe_a_cycle(db):
    start = datetime.now(UTC) - timedelta(days=1)
    end = datetime.now(UTC) + timedelta(hours=12)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
           (provider, cycle_start, cycle_end, purchased_bytes, operational_bytes,
            daily_bytes, pilot_bytes, reconciled_at, reconciliation_ok, kill_switch)
           values ('decodo', %s, %s, 100000000, 25000000, 80000000, 300000000,
                   now(), true, false)""",
        (start, end),
    )
    run_id = uuid4()
    await db.execute(
        "insert into catalogue.runs(id, kind, status) values (%s, 'manual', 'running')",
        (run_id,),
    )
    jobs = [uuid4(), uuid4()]
    for job_id in jobs:
        await db.execute(
            """insert into catalogue.jobs(id, run_id, source_id, host, state)
               values (%s, %s, %s, 'shop.test', 'running')""",
            (job_id, run_id, str(job_id),),
        )

    dsn = postgres_dsn()
    assert dsn
    first = await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row, autocommit=True)
    second = await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row, autocommit=True)
    try:
        results = await asyncio.gather(
            reserve(
                first, job_id=jobs[0], profile="default", cycle_start=start,
                cycle_end=end, requested_bytes=20_000_000,
            ),
            reserve(
                second, job_id=jobs[1], profile="default", cycle_start=start,
                cycle_end=end, requested_bytes=20_000_000,
            ),
            return_exceptions=True,
        )
    finally:
        await first.close()
        await second.close()
    assert sum(not isinstance(value, Exception) for value in results) == 1
    assert sum(isinstance(value, ProxyDenied) for value in results) == 1


@pytest.mark.postgres
@requires_postgres
async def test_attempt_authorizations_are_atomic_and_reconcile_exactly_once(db):
    start = datetime.now(UTC) - timedelta(days=1)
    end = datetime.now(UTC) + timedelta(hours=12)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
           (provider, cycle_start, cycle_end, purchased_bytes, operational_bytes,
            daily_bytes, pilot_bytes, reconciled_at, reconciliation_ok, kill_switch)
           values ('decodo', %s, %s, 100000000, 25000000, 80000000, 300000000,
                   now(), true, false)""",
        (start, end),
    )
    run_id, job_id = uuid4(), uuid4()
    await db.execute(
        "insert into catalogue.runs(id, kind, status) values (%s, 'manual', 'running')",
        (run_id,),
    )
    await db.execute(
        """insert into catalogue.jobs(id, run_id, source_id, host, state)
           values (%s, %s, 'shop', 'shop.test', 'running')""",
        (job_id, run_id),
    )
    reservation_id = await reserve(
        db,
        job_id=job_id,
        profile="default",
        cycle_start=start,
        cycle_end=end,
        requested_bytes=100,
    )

    dsn = postgres_dsn()
    assert dsn
    first = await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row, autocommit=True)
    second = await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row, autocommit=True)
    try:
        authorizations = await asyncio.gather(
            authorize_reservation_attempt(
                first,
                reservation_id=reservation_id,
                estimated_bytes=60,
                maximum_requests=2,
            ),
            authorize_reservation_attempt(
                second,
                reservation_id=reservation_id,
                estimated_bytes=60,
                maximum_requests=2,
            ),
        )
        accepted = [value for value in authorizations if value is not None]
        assert len(accepted) == 1
        usage = await reconcile_reservation_attempt(
            first,
            authorization_id=accepted[0],
            actual_bytes=40,
            physical_requests=1,
        )
        repeated = await reconcile_reservation_attempt(
            second,
            authorization_id=accepted[0],
            actual_bytes=40,
            physical_requests=1,
        )
        assert usage == repeated
        assert (usage.estimated_bytes, usage.request_count) == (40, 1)

        returned = await authorize_reservation_attempt(
            first,
            reservation_id=reservation_id,
            estimated_bytes=60,
            maximum_requests=2,
        )
        assert returned is not None
        await release_reservation_attempt(second, authorization_id=returned)
    finally:
        await first.close()
        await second.close()


@pytest.mark.postgres
@requires_postgres
async def test_a_stopped_pilot_says_so_rather_than_blaming_the_budget(db):
    """One message for two denials sent an operator to a budget that was 18% used.

    the-ceramic-shop failed nightly for five nights reading "allocation would be
    exceeded" while the pilot had spent nothing: it had been stopped.
    """
    start = datetime.now(UTC) - timedelta(days=1)
    end = datetime.now(UTC) + timedelta(days=29)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
           (provider, cycle_start, cycle_end, purchased_bytes, operational_bytes,
            daily_bytes, pilot_bytes, pilot_active, reconciled_at, reconciliation_ok,
            kill_switch)
           values ('decodo', %s, %s, 3000000000, 2400000000, 80000000, 300000000,
                   false, now(), true, false)""",
        (start, end),
    )
    run_id, job_id = uuid4(), uuid4()
    await db.execute(
        "insert into catalogue.runs(id, kind, status) values (%s, 'manual', 'running')",
        (run_id,),
    )
    await db.execute(
        """insert into catalogue.jobs(id, run_id, source_id, host, state)
           values (%s, %s, 'the-ceramic-shop', 'shop.test', 'running')""",
        (job_id, run_id),
    )
    with pytest.raises(ProxyDenied) as denied:
        await reserve(
            db, job_id=job_id, profile="default", cycle_start=start, cycle_end=end,
            requested_bytes=25_000_000, pilot=True,
        )
    assert "not active" in str(denied.value)

    await db.execute(
        "update catalogue.proxy_budget_cycles set pilot_active = true, pilot_bytes = 1000"
    )
    with pytest.raises(ProxyDenied) as exceeded:
        await reserve(
            db, job_id=job_id, profile="default", cycle_start=start, cycle_end=end,
            requested_bytes=25_000_000, pilot=True,
        )
    assert "allocation would be exceeded" in str(exceeded.value)


@pytest.mark.postgres
@requires_postgres
async def test_reconciliation_never_lowers_provider_accounting(db):
    start = datetime.now(UTC) - timedelta(days=1)
    end = datetime.now(UTC) + timedelta(days=29)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
           (provider, cycle_start, cycle_end, provider_reported_bytes)
           values ('decodo', %s, %s, 9000)""",
        (start, end),
    )
    await reconcile(db, cycle_start=start, provider_reported_bytes=1000, successful=True)
    cursor = await db.execute(
        "select provider_reported_bytes from catalogue.proxy_budget_cycles where cycle_start = %s",
        (start,),
    )
    assert (await cursor.fetchone())["provider_reported_bytes"] == 9000


@pytest.mark.postgres
@requires_postgres
async def test_provider_outage_and_operational_ceiling_fail_closed(db):
    start = datetime.now(UTC) - timedelta(days=1)
    end = datetime.now(UTC) + timedelta(days=10)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
           (provider, cycle_start, cycle_end, reconciled_at, reconciliation_ok, kill_switch)
           values ('decodo', %s, %s, now(), true, false)""",
        (start, end),
    )
    run_id, job_id = uuid4(), uuid4()
    await db.execute(
        "insert into catalogue.runs(id, kind, status) values (%s, 'manual', 'running')",
        (run_id,),
    )
    await db.execute(
        """insert into catalogue.jobs(id, run_id, source_id, host, state)
           values (%s, %s, 'shop', 'shop.test', 'running')""",
        (job_id, run_id),
    )
    await reconcile(db, cycle_start=start, provider_reported_bytes=0, successful=False)
    with pytest.raises(ProxyDenied, match="unsafe"):
        await reserve(
            db, job_id=job_id, profile="default", cycle_start=start, cycle_end=end,
            requested_bytes=1_000_000,
        )

    await reconcile(db, cycle_start=start, provider_reported_bytes=2_400_000_000, successful=True)
    cursor = await db.execute(
        "select kill_switch from catalogue.proxy_budget_cycles where cycle_start = %s", (start,)
    )
    assert (await cursor.fetchone())["kill_switch"] is True
