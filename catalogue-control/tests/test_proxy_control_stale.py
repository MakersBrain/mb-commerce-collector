"""Bounded cleanup for abandoned paid-proxy envelopes."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Any
from uuid import UUID, uuid4

import pytest

from catalogue_control import proxy_control

from .conftest import requires_postgres


class _Cursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    async def fetchall(self) -> list[dict[str, Any]]:
        return self.rows


class _Transaction(AbstractAsyncContextManager[None]):
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


class _Connection:
    def __init__(self) -> None:
        self.reservation_id = uuid4()
        self.probe_id = uuid4()
        self.cycle_start = datetime(2026, 8, 1, tzinfo=UTC)
        self.statements: list[tuple[str, dict[str, Any] | None]] = []

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def execute(
        self,
        statement: str,
        parameters: dict[str, Any] | None = None,
    ) -> _Cursor:
        self.statements.append((statement, parameters))
        if "select r.id, r.provider, r.cycle_start" in statement:
            return _Cursor(
                [
                    {
                        "id": self.reservation_id,
                        "provider": "webshare",
                        "cycle_start": self.cycle_start,
                    }
                ]
            )
        if "update catalogue.proxy_reservations" in statement:
            return _Cursor(
                [
                    {
                        "id": self.reservation_id,
                        "provider": "webshare",
                        "probe_id": self.probe_id,
                    }
                ]
            )
        return _Cursor([])


async def test_stale_cleanup_locks_cycle_before_reservation_and_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection()

    async def emit(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(proxy_control.events, "emit", emit)

    assert await proxy_control.close_stale_reservations(connection) == 1

    statements = [statement for statement, _parameters in connection.statements]
    cycle_lock = next(
        index
        for index, statement in enumerate(statements)
        if "from catalogue.proxy_budget_cycles" in statement and "for update" in statement
    )
    reservation_update = next(
        index
        for index, statement in enumerate(statements)
        if "update catalogue.proxy_reservations" in statement
    )
    probe_update = next(
        index
        for index, statement in enumerate(statements)
        if "update catalogue.proxy_probes" in statement
    )
    assert cycle_lock < reservation_update < probe_update
    candidate_parameters = connection.statements[0][1]
    assert candidate_parameters == {
        "probe_timeout": proxy_control.STALE_PROBE_TIMEOUT_SECONDS
    }


async def _insert_probe_reservation(
    db: Any,
    *,
    profile_id: UUID,
    route_id: UUID,
    cycle_start: datetime,
    state: str,
    requested_at: datetime,
    created_at: datetime,
) -> tuple[UUID, UUID]:
    probe_cursor = await db.execute(
        """insert into catalogue.proxy_probes
                  (route_id, profile_id, state, requested_at, protocol, actor, request_id)
           values (%(route)s, %(profile)s, %(state)s, %(requested)s,
                   'http', 'stale-test', %(request)s)
           returning id""",
        {
            "route": route_id,
            "profile": profile_id,
            "state": state,
            "requested": requested_at,
            "request": uuid4(),
        },
    )
    probe = await probe_cursor.fetchone()
    reservation_cursor = await db.execute(
        """insert into catalogue.proxy_reservations
                  (provider, profile, cycle_start, reserved_bytes, created_at,
                   probe_id, profile_id, route_id, purpose)
           values ('webshare', 'stale-profile', %(cycle)s, 1100000, %(created)s,
                   %(probe)s, %(profile)s, %(route)s, 'probe')
           returning id""",
        {
            "cycle": cycle_start,
            "created": created_at,
            "probe": probe["id"],
            "profile": profile_id,
            "route": route_id,
        },
    )
    reservation = await reservation_cursor.fetchone()
    return probe["id"], reservation["id"]


@pytest.mark.postgres
@requires_postgres
async def test_stale_pending_and_running_probes_release_only_expired_envelopes(db: Any) -> None:
    now = datetime.now(UTC)
    cycle_start = now - timedelta(days=1)
    await db.execute(
        """insert into catalogue.proxy_budget_cycles
                  (provider, cycle_start, cycle_end, reconciliation_ok, kill_switch)
           values ('webshare', %(start)s, %(end)s, true, false)""",
        {"start": cycle_start, "end": now + timedelta(days=29)},
    )
    profile_cursor = await db.execute(
        """insert into catalogue.proxy_profiles
                  (provider, logical_name, display_name, enabled, lifecycle,
                   created_by, updated_by)
           values ('webshare', 'stale-profile', 'Stale profile', true, 'enabled',
                   'test', 'test') returning id"""
    )
    profile = await profile_cursor.fetchone()
    route_cursor = await db.execute(
        """insert into catalogue.proxy_routes
                  (provider, label, profile_id, protocol, enabled, created_by, updated_by)
           values ('webshare', 'stale route', %(profile)s, 'http', true, 'test', 'test')
           returning id""",
        {"profile": profile["id"]},
    )
    route = await route_cursor.fetchone()

    expired = now - timedelta(seconds=proxy_control.STALE_PROBE_TIMEOUT_SECONDS + 60)
    stale_pending = await _insert_probe_reservation(
        db,
        profile_id=profile["id"],
        route_id=route["id"],
        cycle_start=cycle_start,
        state="pending",
        requested_at=expired,
        created_at=expired,
    )
    stale_running = await _insert_probe_reservation(
        db,
        profile_id=profile["id"],
        route_id=route["id"],
        cycle_start=cycle_start,
        state="running",
        requested_at=expired,
        created_at=expired,
    )
    fresh = await _insert_probe_reservation(
        db,
        profile_id=profile["id"],
        route_id=route["id"],
        cycle_start=cycle_start,
        state="pending",
        requested_at=now,
        created_at=now,
    )
    delayed_reservation = await _insert_probe_reservation(
        db,
        profile_id=profile["id"],
        route_id=route["id"],
        cycle_start=cycle_start,
        state="pending",
        requested_at=expired,
        created_at=now,
    )

    assert await proxy_control.close_stale_reservations(db) == 2

    cursor = await db.execute(
        """select p.id as probe_id, p.state as probe_state, p.error_category,
                  p.completed_at is not null as completed, r.id as reservation_id,
                  r.state as reservation_state
             from catalogue.proxy_probes p
             join catalogue.proxy_reservations r on r.probe_id = p.id
            order by p.requested_at, r.created_at, p.id"""
    )
    rows = {row["probe_id"]: row for row in await cursor.fetchall()}
    for probe_id, reservation_id in (stale_pending, stale_running):
        assert rows[probe_id] == {
            "probe_id": probe_id,
            "probe_state": "cancelled",
            "error_category": "stale_timeout",
            "completed": True,
            "reservation_id": reservation_id,
            "reservation_state": "cancelled",
        }
    for probe_id, reservation_id in (fresh, delayed_reservation):
        assert rows[probe_id] == {
            "probe_id": probe_id,
            "probe_state": "pending",
            "error_category": None,
            "completed": False,
            "reservation_id": reservation_id,
            "reservation_state": "active",
        }

    reconciles = await db.execute(
        """select reservation_id from catalogue.proxy_reconcile_requests
            where reason = 'stale_reservation' order by reservation_id"""
    )
    assert {row["reservation_id"] for row in await reconciles.fetchall()} == {
        stale_pending[1],
        stale_running[1],
    }
