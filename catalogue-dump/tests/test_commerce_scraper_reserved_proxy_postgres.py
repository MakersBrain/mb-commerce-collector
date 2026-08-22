"""PostgreSQL intercall gate for durable Webshare data-plane accounting."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from mb_commerce_scraper.proxy import ProxyOutcome, ProxyRequest
from pydantic import SecretStr

from mb_ceramics_catalogue.ops.commerce_scraper_proxy import (
    DurableProxyIdentity,
    PostgresReservedProxyPool,
)
from mb_ceramics_catalogue.ops.commerce_scraper_webshare import (
    WebshareGatewayConfig,
    WebshareGatewayPool,
)
from mb_ceramics_catalogue.proxy import ProxyDenied

from .conftest import requires_postgres

pytestmark = [pytest.mark.postgres, requires_postgres]


class BorrowedConnectionPool:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        yield self._connection


async def _cycle(db: Any, provider: str) -> tuple[datetime, datetime]:
    start = datetime.now(UTC) - timedelta(days=1)
    end = datetime.now(UTC) + timedelta(days=1)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
             (provider, cycle_start, cycle_end, purchased_bytes,
              operational_bytes, daily_bytes, reconciled_at,
              reconciliation_ok, kill_switch)
             values (%s, %s, %s, 10000000, 5000000, 1000000,
                     now(), true, false)""",
        (provider, start, end),
    )
    return start, end


async def _identity(
    db: Any,
    *,
    provider: str,
    cycle_start: datetime,
    profile: str,
    generation: int,
) -> DurableProxyIdentity:
    profile_cursor = await db.execute(
        """insert into catalogue.proxy_profiles
             (provider, logical_name, display_name, enabled, lifecycle,
              secret_generation, created_by, updated_by)
             values (%s, %s, %s, true, 'enabled', %s, 'test', 'test')
             returning id""",
        (provider, profile, profile, generation),
    )
    profile_id = (await profile_cursor.fetchone())["id"]
    route_cursor = await db.execute(
        """insert into catalogue.proxy_routes
             (provider, label, profile_id, protocol, country, session_mode,
              session_minutes, max_bytes, enabled, created_by, updated_by)
             values (%s, %s, %s, 'http', 'FR', 'sticky', 30, 1000,
                     true, 'test', 'test')
             returning id""",
        (provider, profile, profile_id),
    )
    route_id = (await route_cursor.fetchone())["id"]
    await db.execute(
        """insert into catalogue.proxy_profile_allocations
             (provider, cycle_start, profile_id, allocated_bytes, updated_by)
             values (%s, %s, %s, 1000, 'test')""",
        (provider, cycle_start, profile_id),
    )
    return DurableProxyIdentity(
        provider=provider,
        profile=profile,
        profile_id=profile_id,
        route_id=route_id,
        secret_generation=generation,
    )


async def _job(db: Any) -> UUID:
    run_id = uuid4()
    job_id = uuid4()
    await db.execute(
        "insert into catalogue.runs(id, kind, status) "
        "values (%s, 'manual', 'running')",
        (run_id,),
    )
    await db.execute(
        """insert into catalogue.jobs(id, run_id, source_id, host, state)
             values (%s, %s, 'shop', 'shop.test', 'running')""",
        (job_id, run_id),
    )
    return job_id


def _pool(
    db: Any,
    *,
    job_id: UUID,
    identity: DurableProxyIdentity,
) -> PostgresReservedProxyPool:
    gateway = WebshareGatewayPool(
        WebshareGatewayConfig(
            username=SecretStr("provider-user"),
            password=SecretStr("provider-password"),
            endpoint_id=str(identity.route_id),
            countries=frozenset({"FR"}),
            sticky_session_ttl_seconds=1_800,
        ),
        session_id_factory=lambda: "123456",
    )
    return PostgresReservedProxyPool(
        BorrowedConnectionPool(db),
        gateway,
        job_id=job_id,
        identity=identity,
        maximum_bytes=800,
    )


def _request() -> ProxyRequest:
    return ProxyRequest(
        source_id="shop",
        target_host="shop.test",
        country="FR",
        sticky=True,
        session_ttl_seconds=900,
        maximum_requests=2,
        maximum_bytes=800,
        preferred_providers=("webshare",),
    )


async def _one(db: Any, query: str, params: tuple[Any, ...] = ()) -> Any:
    cursor = await db.execute(query, params)
    row = await cursor.fetchone()
    assert row is not None
    return row


async def test_webshare_pool_reconciles_and_closes_only_its_durable_graph(db: Any) -> None:
    webshare_start, _webshare_end = await _cycle(db, "webshare")
    decodo_start, _decodo_end = await _cycle(db, "decodo")
    webshare = await _identity(
        db,
        provider="webshare",
        cycle_start=webshare_start,
        profile="webshare-primary",
        generation=7,
    )
    decodo = await _identity(
        db,
        provider="decodo",
        cycle_start=decodo_start,
        profile="decodo-control",
        generation=3,
    )
    pool = _pool(db, job_id=await _job(db), identity=webshare)

    lease = await pool.acquire(_request())
    authorization = await pool.authorize(lease, 250)
    assert authorization is not None
    await authorization.reconcile(
        ProxyOutcome(
            target_host="shop.test",
            status=200,
            transmitted_bytes=40,
            received_bytes=110,
            classification="success",
        )
    )
    await pool.report(
        lease,
        ProxyOutcome(target_host="shop.test", status=200, classification="success"),
    )
    await pool.release(lease)

    reservation = await _one(
        db,
        """select provider, profile, profile_id, route_id, secret_generation,
                  reserved_bytes, estimated_bytes, request_count, state
             from catalogue.proxy_reservations""",
    )
    assert reservation == {
        "provider": "webshare",
        "profile": "webshare-primary",
        "profile_id": webshare.profile_id,
        "route_id": webshare.route_id,
        "secret_generation": 7,
        "reserved_bytes": 800,
        "estimated_bytes": 150,
        "request_count": 1,
        "state": "closed",
    }
    attempt = await _one(
        db,
        """select a.state, a.estimated_bytes, a.actual_bytes,
                  a.physical_requests, r.provider
             from catalogue.proxy_attempt_authorizations a
             join catalogue.proxy_reservations r on r.id = a.reservation_id""",
    )
    assert attempt == {
        "state": "reconciled",
        "estimated_bytes": 250,
        "actual_bytes": 150,
        "physical_requests": 1,
        "provider": "webshare",
    }
    cycles = await db.execute(
        """select provider, application_bytes
             from catalogue.proxy_budget_cycles order by provider"""
    )
    assert await cycles.fetchall() == [
        {"provider": "decodo", "application_bytes": 0},
        {"provider": "webshare", "application_bytes": 150},
    ]
    allocations = await db.execute(
        """select a.provider, a.profile_id, a.allocated_bytes
             from catalogue.proxy_profile_allocations a order by a.provider"""
    )
    assert await allocations.fetchall() == [
        {
            "provider": "decodo",
            "profile_id": decodo.profile_id,
            "allocated_bytes": 1000,
        },
        {
            "provider": "webshare",
            "profile_id": webshare.profile_id,
            "allocated_bytes": 1000,
        },
    ]
    reconcile_request = await _one(
        db,
        "select provider, reason from catalogue.proxy_reconcile_requests",
    )
    assert reconcile_request == {
        "provider": "webshare",
        "reason": "reservation_closed",
    }


async def test_unsafe_webshare_cycle_denies_without_an_attempt_row(db: Any) -> None:
    start, _end = await _cycle(db, "webshare")
    identity = await _identity(
        db,
        provider="webshare",
        cycle_start=start,
        profile="webshare-primary",
        generation=2,
    )
    pool = _pool(db, job_id=await _job(db), identity=identity)
    lease = await pool.acquire(_request())
    await db.execute(
        """update catalogue.proxy_budget_cycles
              set kill_switch = true
            where provider = 'webshare' and cycle_start = %s""",
        (start,),
    )

    with pytest.raises(ProxyDenied, match="does not authorize new paid traffic"):
        await pool.authorize(lease, 100)

    attempts = await _one(
        db,
        "select count(*) as count from catalogue.proxy_attempt_authorizations",
    )
    assert attempts["count"] == 0
    assert not lease.can_start(1)
    await pool.release(lease)
