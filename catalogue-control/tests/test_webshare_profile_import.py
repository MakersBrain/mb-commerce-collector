from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest
from mb_ceramics_catalogue.proxy import close_reservation
from mb_ceramics_catalogue.webshare_gateway_secrets import (
    WebshareGatewaySecret,
    WebshareGatewaySecretStore,
    load_webshare_gateway_secrets,
)
from psycopg.rows import dict_row
from pydantic import SecretStr

from catalogue_control.webshare_profile_import import (
    MUTATION_ACTION,
    WebshareProfileImportError,
    install_webshare_profile,
    recover_webshare_profile_import,
)

from .conftest import postgres_dsn, requires_postgres


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


class _PausedCloseConnection:
    def __init__(
        self,
        connection: Any,
        reservation_locked: asyncio.Event,
        release_close: asyncio.Event,
    ) -> None:
        self._connection = connection
        self._reservation_locked = reservation_locked
        self._release_close = release_close
        self._paused = False

    def transaction(self) -> Any:
        return self._connection.transaction()

    async def execute(self, query: str, params: Any = None) -> Any:
        cursor = await self._connection.execute(query, params)
        statement = " ".join(query.lower().split())
        if (
            not self._paused
            and statement.startswith("update catalogue.proxy_reservations")
            and "set estimated_bytes = greatest" in statement
        ):
            self._paused = True
            self._reservation_locked.set()
            await self._release_close.wait()
        return cursor


class _ObservedRotationConnection:
    def __init__(self, connection: Any, cycle_query_started: asyncio.Event) -> None:
        self._connection = connection
        self._cycle_query_started = cycle_query_started

    def transaction(self) -> Any:
        return self._connection.transaction()

    async def execute(self, query: str, params: Any = None) -> Any:
        statement = " ".join(query.lower().split())
        if "from catalogue.proxy_budget_cycles" in statement and "for update" in statement:
            self._cycle_query_started.set()
        return await self._connection.execute(query, params)


class _UnlockFailedCursor:
    async def fetchone(self) -> dict[str, bool]:
        return {"unlocked": False}


class _UnlockFailedConnection:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    async def execute(self, query: str, params: Any = None) -> Any:
        if "pg_advisory_unlock" in query:
            return _UnlockFailedCursor()
        return await self._connection.execute(query, params)

    async def close(self) -> None:
        await self._connection.close()


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


async def _installed_rotation_after_crash(
    db: Any,
    store: WebshareGatewaySecretStore,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Any, UUID, Any]:
    cycle_start = await _cycle(db)
    _, created = await _create(db, store)
    operation_id = await _operation(db)

    async def crash_after_install(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise RuntimeError("simulated process exit")

    monkeypatch.setattr(
        "catalogue_control.webshare_profile_import._finalize", crash_after_install
    )
    with pytest.raises(RuntimeError, match="simulated process exit"):
        await install_webshare_profile(
            db,
            store,
            operation_id=operation_id,
            actor_id="operator@example.test",
            secret=_secret(2, password="rotated-secret"),
            expected_generation=1,
        )
    monkeypatch.undo()
    return cycle_start, operation_id, created


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


@pytest.mark.postgres
@requires_postgres
async def test_close_and_rotation_share_cycle_first_lock_order(db, tmp_path: Path):
    cycle_start = await _cycle(db)
    store = _store(tmp_path)
    _, created = await _create(db, store)
    route_cursor = await db.execute(
        """insert into catalogue.proxy_routes
                  (provider, label, profile_id, enabled, created_by, updated_by)
           values ('webshare', 'webshare route', %(profile)s, true, 'test', 'test')
           returning id""",
        {"profile": created.profile_id},
    )
    route = await route_cursor.fetchone()
    assert route is not None
    probe_cursor = await db.execute(
        """insert into catalogue.proxy_probes
                  (route_id, profile_id, protocol, actor, request_id)
           values (%(route)s, %(profile)s, 'http', 'test', %(request)s)
           returning id""",
        {"route": route["id"], "profile": created.profile_id, "request": uuid4()},
    )
    probe = await probe_cursor.fetchone()
    assert probe is not None
    reservation_cursor = await db.execute(
        """insert into catalogue.proxy_reservations
                  (provider, profile, cycle_start, reserved_bytes, probe_id,
                   profile_id, route_id, purpose, secret_generation)
           values ('webshare', 'operator-gateway', %(cycle)s, 1000, %(probe)s,
                   %(profile)s, %(route)s, 'probe', 1)
           returning id""",
        {
            "cycle": cycle_start,
            "probe": probe["id"],
            "profile": created.profile_id,
            "route": route["id"],
        },
    )
    reservation = await reservation_cursor.fetchone()
    assert reservation is not None
    operation_id = await _operation(db)

    dsn = postgres_dsn()
    assert dsn
    close_raw = await psycopg.AsyncConnection.connect(
        dsn, row_factory=dict_row, autocommit=True
    )
    rotation_raw = await psycopg.AsyncConnection.connect(
        dsn, row_factory=dict_row, autocommit=True
    )
    reservation_locked = asyncio.Event()
    rotation_cycle_query = asyncio.Event()
    release_close = asyncio.Event()
    close_connection = _PausedCloseConnection(
        close_raw, reservation_locked, release_close
    )
    rotation_connection = _ObservedRotationConnection(
        rotation_raw, rotation_cycle_query
    )
    lease = SimpleNamespace(
        reservation_id=reservation["id"], used_bytes=12, requests=1
    )
    try:
        close_task = asyncio.create_task(
            close_reservation(close_connection, lease)  # type: ignore[arg-type]
        )
        await asyncio.wait_for(reservation_locked.wait(), timeout=2)
        rotation_task = asyncio.create_task(
            install_webshare_profile(
                rotation_connection,
                store,
                operation_id=operation_id,
                actor_id="operator@example.test",
                secret=_secret(2, password="rotated-secret"),
                expected_generation=1,
            )
        )
        await asyncio.wait_for(rotation_cycle_query.wait(), timeout=2)
        release_close.set()
        _, rotated = await asyncio.wait_for(
            asyncio.gather(close_task, rotation_task), timeout=5
        )
    finally:
        release_close.set()
        await close_raw.close()
        await rotation_raw.close()

    assert rotated.state == "completed"
    state_cursor = await db.execute(
        """select r.state, r.estimated_bytes, r.request_count,
                  p.enabled, p.lifecycle, p.secret_generation,
                  c.application_bytes
             from catalogue.proxy_reservations r
             join catalogue.proxy_profiles p on p.id = r.profile_id
             join catalogue.proxy_budget_cycles c
               on c.provider = r.provider and c.cycle_start = r.cycle_start
            where r.id = %(reservation)s""",
        {"reservation": reservation["id"]},
    )
    assert await state_cursor.fetchone() == {
        "state": "closed",
        "estimated_bytes": 12,
        "request_count": 1,
        "enabled": True,
        "lifecycle": "enabled",
        "secret_generation": 2,
        "application_bytes": 12,
    }


@pytest.mark.postgres
@requires_postgres
async def test_operation_lock_fences_recovery_while_secret_cas_is_in_flight(
    db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    await _cycle(db)
    store = _store(tmp_path)
    operation_id = await _operation(db)
    dsn = postgres_dsn()
    assert dsn
    first_connection = await psycopg.AsyncConnection.connect(
        dsn, row_factory=dict_row, autocommit=True
    )
    recovery_connection = await psycopg.AsyncConnection.connect(
        dsn, row_factory=dict_row, autocommit=True
    )
    entered_cas = threading.Event()
    release_cas = threading.Event()
    original_install = store.install

    def blocked_install(*args: Any, **kwargs: Any) -> int:
        entered_cas.set()
        if not release_cas.wait(timeout=5):
            raise RuntimeError("test CAS release timed out")
        return original_install(*args, **kwargs)

    monkeypatch.setattr(store, "install", blocked_install)
    first_task = asyncio.create_task(
        install_webshare_profile(
            first_connection,
            store,
            operation_id=operation_id,
            actor_id="operator@example.test",
            secret=_secret(1),
            expected_generation=None,
            display_name="Operator gateway",
            allocated_bytes=250_000,
        )
    )
    try:
        assert await asyncio.to_thread(entered_cas.wait, 2)
        with pytest.raises(WebshareProfileImportError) as raised:
            await asyncio.wait_for(
                recover_webshare_profile_import(
                    recovery_connection, store, operation_id=operation_id
                ),
                timeout=1,
            )
        assert raised.value.code == "operation_busy"
        intent_cursor = await db.execute(
            """select state from catalogue.proxy_profile_secret_intents
                where operation_id = %(operation)s""",
            {"operation": operation_id},
        )
        assert (await intent_cursor.fetchone())["state"] == "prepared"
        assert not store.path.exists()
        release_cas.set()
        result = await asyncio.wait_for(first_task, timeout=5)
    finally:
        release_cas.set()
        if not first_task.done():
            first_task.cancel()
        await first_connection.close()
        await recovery_connection.close()

    assert result.state == "completed"
    assert load_webshare_gateway_secrets(store.path)[
        ("webshare", "operator-gateway")
    ].generation == 1


@pytest.mark.postgres
@requires_postgres
async def test_repeated_cancellation_keeps_operation_locked_until_cas_finishes(
    db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    await _cycle(db)
    store = _store(tmp_path)
    operation_id = await _operation(db)
    dsn = postgres_dsn()
    assert dsn
    install_connection = await psycopg.AsyncConnection.connect(
        dsn, row_factory=dict_row, autocommit=True
    )
    recovery_connection = await psycopg.AsyncConnection.connect(
        dsn, row_factory=dict_row, autocommit=True
    )
    entered_cas = threading.Event()
    release_cas = threading.Event()
    original_install = store.install

    def blocked_install(*args: Any, **kwargs: Any) -> int:
        entered_cas.set()
        if not release_cas.wait(timeout=5):
            raise RuntimeError("test CAS release timed out")
        return original_install(*args, **kwargs)

    monkeypatch.setattr(store, "install", blocked_install)
    install_task = asyncio.create_task(
        install_webshare_profile(
            install_connection,
            store,
            operation_id=operation_id,
            actor_id="operator@example.test",
            secret=_secret(1),
            expected_generation=None,
            display_name="Operator gateway",
            allocated_bytes=250_000,
        )
    )
    try:
        assert await asyncio.to_thread(entered_cas.wait, 2)
        install_task.cancel()
        await asyncio.sleep(0)
        install_task.cancel()
        await asyncio.sleep(0)

        with pytest.raises(WebshareProfileImportError) as busy:
            await recover_webshare_profile_import(
                recovery_connection, store, operation_id=operation_id
            )
        assert busy.value.code == "operation_busy"

        release_cas.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(install_task, timeout=5)
        recovered = await recover_webshare_profile_import(
            recovery_connection, store, operation_id=operation_id
        )
    finally:
        release_cas.set()
        if not install_task.done():
            install_task.cancel()
        await install_connection.close()
        await recovery_connection.close()

    assert recovered.state == "completed"
    assert load_webshare_gateway_secrets(store.path)[
        ("webshare", "operator-gateway")
    ].generation == 1


@pytest.mark.postgres
@requires_postgres
async def test_uncertain_operation_unlock_discards_connection(db, tmp_path: Path):
    await _cycle(db)
    store = _store(tmp_path)
    operation_id, _ = await _create(db, store)
    dsn = postgres_dsn()
    assert dsn
    raw = await psycopg.AsyncConnection.connect(
        dsn, row_factory=dict_row, autocommit=True
    )
    connection = _UnlockFailedConnection(raw)

    with pytest.raises(WebshareProfileImportError) as raised:
        await recover_webshare_profile_import(
            connection, store, operation_id=operation_id
        )

    assert raised.value.code == "operation_unlock_failed"
    assert raw.closed


@pytest.mark.postgres
@requires_postgres
async def test_recovery_completes_when_database_already_has_target_generation(
    db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    store = _store(tmp_path)
    _, operation_id, created = await _installed_rotation_after_crash(
        db, store, monkeypatch
    )
    await db.execute(
        """update catalogue.proxy_profiles
              set secret_generation = 2, enabled = false, lifecycle = 'pending'
            where id = %(profile)s""",
        {"profile": created.profile_id},
    )

    recovered = await recover_webshare_profile_import(
        db, store, operation_id=operation_id
    )

    assert recovered.state == "completed"
    assert recovered.error_code is None
    profile = await db.execute(
        """select enabled, lifecycle, secret_generation
             from catalogue.proxy_profiles where id = %(profile)s""",
        {"profile": created.profile_id},
    )
    assert await profile.fetchone() == {
        "enabled": True,
        "lifecycle": "enabled",
        "secret_generation": 2,
    }


@pytest.mark.postgres
@requires_postgres
async def test_installed_recovery_rebinds_to_allocated_current_cycle(
    db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    store = _store(tmp_path)
    old_cycle, operation_id, created = await _installed_rotation_after_crash(
        db, store, monkeypatch
    )
    await db.execute(
        """update catalogue.proxy_budget_cycles set lifecycle = 'closed'
            where provider = 'webshare' and cycle_start = %(cycle)s""",
        {"cycle": old_cycle},
    )
    current_cycle = await _cycle(db)
    await db.execute(
        """insert into catalogue.proxy_profile_allocations
                  (provider, cycle_start, profile_id, allocated_bytes, updated_by)
           values ('webshare', %(cycle)s, %(profile)s, 250000, 'test')""",
        {"cycle": current_cycle, "profile": created.profile_id},
    )

    recovered = await recover_webshare_profile_import(
        db, store, operation_id=operation_id
    )

    assert recovered.state == "completed"
    intent = await db.execute(
        """select state, cycle_start, error_code
             from catalogue.proxy_profile_secret_intents
            where operation_id = %(operation)s""",
        {"operation": operation_id},
    )
    assert await intent.fetchone() == {
        "state": "completed",
        "cycle_start": current_cycle,
        "error_code": None,
    }


@pytest.mark.postgres
@requires_postgres
async def test_installed_recovery_waits_for_current_cycle_allocation(
    db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    store = _store(tmp_path)
    old_cycle, operation_id, created = await _installed_rotation_after_crash(
        db, store, monkeypatch
    )
    await db.execute(
        """update catalogue.proxy_budget_cycles set lifecycle = 'closed'
            where provider = 'webshare' and cycle_start = %(cycle)s""",
        {"cycle": old_cycle},
    )
    current_cycle = await _cycle(db)

    first = await recover_webshare_profile_import(db, store, operation_id=operation_id)
    repeated = await recover_webshare_profile_import(
        db, store, operation_id=operation_id
    )

    assert (first.state, first.error_code) == ("installed", "cycle_rebind_required")
    assert repeated == first
    state = await db.execute(
        """select i.state, i.error_code, i.cycle_start,
                  p.enabled, p.lifecycle, p.secret_generation
             from catalogue.proxy_profile_secret_intents i
             join catalogue.proxy_profiles p on p.id = i.profile_id
            where i.operation_id = %(operation)s""",
        {"operation": operation_id},
    )
    assert await state.fetchone() == {
        "state": "installed",
        "error_code": "cycle_rebind_required",
        "cycle_start": old_cycle,
        "enabled": False,
        "lifecycle": "pending",
        "secret_generation": 1,
    }
    assert created.profile_id == first.profile_id
    await db.execute(
        """insert into catalogue.proxy_profile_allocations
                  (provider, cycle_start, profile_id, allocated_bytes, updated_by)
           values ('webshare', %(cycle)s, %(profile)s, 250000, 'test')""",
        {"cycle": current_cycle, "profile": created.profile_id},
    )
    completed = await recover_webshare_profile_import(
        db, store, operation_id=operation_id
    )
    assert (completed.state, completed.error_code) == ("completed", None)


@pytest.mark.postgres
@requires_postgres
async def test_installed_recovery_fails_closed_on_database_generation_conflict(
    db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    store = _store(tmp_path)
    _, operation_id, created = await _installed_rotation_after_crash(
        db, store, monkeypatch
    )
    await db.execute(
        """update catalogue.proxy_profiles
              set secret_generation = 9, enabled = true, lifecycle = 'enabled'
            where id = %(profile)s""",
        {"profile": created.profile_id},
    )

    recovered = await recover_webshare_profile_import(
        db, store, operation_id=operation_id
    )
    repeated = await recover_webshare_profile_import(
        db, store, operation_id=operation_id
    )

    assert (recovered.state, recovered.error_code) == (
        "failed",
        "database_generation_conflict",
    )
    assert repeated == recovered
    state = await db.execute(
        """select i.state, i.error_code, i.completed_at is not null as terminal,
                  p.enabled, p.lifecycle, p.secret_generation
             from catalogue.proxy_profile_secret_intents i
             join catalogue.proxy_profiles p on p.id = i.profile_id
            where i.operation_id = %(operation)s""",
        {"operation": operation_id},
    )
    assert await state.fetchone() == {
        "state": "failed",
        "error_code": "database_generation_conflict",
        "terminal": True,
        "enabled": False,
        "lifecycle": "pending",
        "secret_generation": 9,
    }
