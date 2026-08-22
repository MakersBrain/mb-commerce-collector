"""Crash-safe coordination of Webshare profiles and operator-owned secrets.

The database records only identities and generations.  Credentials remain in
the private gateway-secret file, whose compare-and-swap happens deliberately
outside PostgreSQL transactions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from mb_ceramics_catalogue.proxy import ProxyDenied
from mb_ceramics_catalogue.webshare_gateway_secrets import (
    MAX_GENERATION,
    WebshareGatewaySecret,
    WebshareGatewaySecretStore,
    load_webshare_gateway_secrets,
    validate_webshare_gateway_secret,
)

PROVIDER = "webshare"
MUTATION_ACTION = "profile.secret.install"
_LOCK_KEY = "proxy:webshare"

ImportState = Literal["draining", "installed", "completed", "failed"]


class WebshareProfileImportError(RuntimeError):
    """A safe, credential-free application error."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class WebshareProfileImportResult:
    operation_id: UUID
    profile_id: UUID
    provider: str
    logical_name: str
    generation: int
    state: ImportState
    created_profile: bool


@dataclass(frozen=True, slots=True)
class _Intent:
    operation_id: UUID
    profile_id: UUID
    provider: str
    logical_name: str
    cycle_start: Any
    expected_generation: int | None
    target_generation: int
    created_profile: bool
    state: str


def _strict_expected(value: int | None) -> int | None:
    if value is None:
        return None
    if type(value) is not int or not 1 <= value < MAX_GENERATION:
        raise WebshareProfileImportError("invalid_expected_generation")
    return value


def _result(intent: _Intent, state: ImportState) -> WebshareProfileImportResult:
    return WebshareProfileImportResult(
        operation_id=intent.operation_id,
        profile_id=intent.profile_id,
        provider=intent.provider,
        logical_name=intent.logical_name,
        generation=intent.target_generation,
        state=state,
        created_profile=intent.created_profile,
    )


def _intent(row: dict[str, Any]) -> _Intent:
    return _Intent(
        operation_id=row["operation_id"],
        profile_id=row["profile_id"],
        provider=row["provider"],
        logical_name=row["logical_name"],
        cycle_start=row["cycle_start"],
        expected_generation=row["expected_generation"],
        target_generation=row["target_generation"],
        created_profile=row["created_profile"],
        state=row["state"],
    )


async def _intent_for_operation(connection: Any, operation_id: UUID) -> _Intent | None:
    cursor = await connection.execute(
        """select operation_id, provider, profile_id, logical_name, cycle_start,
                  expected_generation, target_generation, created_profile, state
             from catalogue.proxy_profile_secret_intents
            where operation_id = %(operation)s""",
        {"operation": operation_id},
    )
    row = await cursor.fetchone()
    return None if row is None else _intent(row)


async def _lock_cycle(connection: Any, cycle_start: Any | None = None) -> dict[str, Any]:
    await connection.execute(
        "select pg_advisory_xact_lock(hashtext(%(key)s))", {"key": _LOCK_KEY}
    )
    if cycle_start is None:
        cursor = await connection.execute(
            """select * from catalogue.proxy_budget_cycles
                where provider = %(provider)s and lifecycle = 'active'
                  and cycle_start <= now() and cycle_end > now()
                order by cycle_start
                for update""",
            {"provider": PROVIDER},
        )
        rows = await cursor.fetchall()
        if len(rows) != 1:
            raise WebshareProfileImportError("webshare_cycle_unavailable")
        cycle = rows[0]
    else:
        cursor = await connection.execute(
            """select * from catalogue.proxy_budget_cycles
                where provider = %(provider)s and cycle_start = %(cycle)s
                for update""",
            {"provider": PROVIDER, "cycle": cycle_start},
        )
        cycle = await cursor.fetchone()
        if cycle is None:
            raise WebshareProfileImportError("webshare_cycle_unavailable")
    return cycle


def _cycle_is_safe(cycle: dict[str, Any]) -> bool:
    return bool(
        cycle["lifecycle"] == "active"
        and cycle["cycle_start"] <= cycle["database_now"] < cycle["cycle_end"]
        and not cycle["kill_switch"]
        and cycle["reconciliation_ok"]
        and cycle["reconciled_at"] is not None
    )


async def _database_now(connection: Any, cycle: dict[str, Any]) -> dict[str, Any]:
    cursor = await connection.execute("select now() as database_now")
    row = await cursor.fetchone()
    assert row is not None
    return {**cycle, "database_now": row["database_now"]}


async def _require_operation(connection: Any, operation_id: UUID, actor_id: str) -> None:
    cursor = await connection.execute(
        """select actor, action, state
             from catalogue.proxy_mutation_requests
            where operation_id = %(operation)s
            for update""",
        {"operation": operation_id},
    )
    row = await cursor.fetchone()
    if (
        row is None
        or row["actor"] != actor_id
        or row["action"] != MUTATION_ACTION
        or row["state"] != "started"
    ):
        raise WebshareProfileImportError("invalid_operation")


async def _prepare(
    connection: Any,
    *,
    operation_id: UUID,
    actor_id: str,
    secret: WebshareGatewaySecret,
    expected_generation: int | None,
    display_name: str | None,
    allocated_bytes: int | None,
) -> WebshareProfileImportResult | _Intent:
    async with connection.transaction():
        cycle = await _database_now(connection, await _lock_cycle(connection))
        if not _cycle_is_safe(cycle):
            raise WebshareProfileImportError("webshare_cycle_unsafe")
        await _require_operation(connection, operation_id, actor_id)

        profile_cursor = await connection.execute(
            """select * from catalogue.proxy_profiles
                where provider = %(provider)s and logical_name = %(logical_name)s
                for update""",
            {"provider": PROVIDER, "logical_name": secret.logical_name},
        )
        profile = await profile_cursor.fetchone()
        created_profile = expected_generation is None
        if created_profile:
            if allocated_bytes is None or type(allocated_bytes) is not int or allocated_bytes <= 0:
                raise WebshareProfileImportError("invalid_allocation")
            if display_name is None or not display_name or len(display_name) > 200:
                raise WebshareProfileImportError("invalid_display_name")
            if profile is None:
                capacity_cursor = await connection.execute(
                    """select coalesce(sum(allocated_bytes), 0) as allocated
                         from catalogue.proxy_profile_allocations
                        where provider = %(provider)s and cycle_start = %(cycle)s""",
                    {"provider": PROVIDER, "cycle": cycle["cycle_start"]},
                )
                capacity = await capacity_cursor.fetchone()
                assert capacity is not None
                if (
                    capacity["allocated"]
                    + cycle["unmanaged_allocation_bytes"]
                    + allocated_bytes
                    > cycle["operational_bytes"]
                ):
                    raise WebshareProfileImportError("allocation_exceeds_operational_budget")
                inserted = await connection.execute(
                    """insert into catalogue.proxy_profiles
                              (provider, logical_name, display_name, auto_disable, enabled,
                               lifecycle, secret_generation, created_by, updated_by)
                       values (%(provider)s, %(logical_name)s, %(display_name)s, false, false,
                               'pending', 0, %(actor)s, %(actor)s)
                       returning *""",
                    {
                        "provider": PROVIDER,
                        "logical_name": secret.logical_name,
                        "display_name": display_name,
                        "actor": actor_id,
                    },
                )
                profile = await inserted.fetchone()
                assert profile is not None
                await connection.execute(
                    """insert into catalogue.proxy_profile_allocations
                              (provider, cycle_start, profile_id, allocated_bytes, updated_by)
                       values (%(provider)s, %(cycle)s, %(profile)s, %(allocated)s, %(actor)s)""",
                    {
                        "provider": PROVIDER,
                        "cycle": cycle["cycle_start"],
                        "profile": profile["id"],
                        "allocated": allocated_bytes,
                        "actor": actor_id,
                    },
                )
            else:
                # A failed create retains its non-secret audit intent.  A later
                # operation may adopt only the exact pristine generation-zero
                # row and its unchanged current-cycle allocation.
                allocation_cursor = await connection.execute(
                    """select allocated_bytes from catalogue.proxy_profile_allocations
                        where provider = %(provider)s and cycle_start = %(cycle)s
                          and profile_id = %(profile)s
                        for update""",
                    {
                        "provider": PROVIDER,
                        "cycle": cycle["cycle_start"],
                        "profile": profile["id"],
                    },
                )
                allocation = await allocation_cursor.fetchone()
                failed_cursor = await connection.execute(
                    """select 1 from catalogue.proxy_profile_secret_intents
                        where provider = %(provider)s and profile_id = %(profile)s
                          and created_profile and target_generation = 1 and state = 'failed'
                        limit 1""",
                    {"provider": PROVIDER, "profile": profile["id"]},
                )
                has_failed_create = await failed_cursor.fetchone() is not None
                if (
                    profile["secret_generation"] != 0
                    or profile["enabled"]
                    or profile["lifecycle"] != "pending"
                    or profile["pending_action"] is not None
                    or profile["provider_resource_id"] is not None
                    or profile["display_name"] != display_name
                    or allocation is None
                    or allocation["allocated_bytes"] != allocated_bytes
                    or not has_failed_create
                ):
                    raise WebshareProfileImportError("profile_already_exists")
        else:
            if display_name is not None or allocated_bytes is not None:
                raise WebshareProfileImportError("rotation_metadata_not_allowed")
            if profile is None:
                raise WebshareProfileImportError("profile_not_found")
            if profile["lifecycle"] == "retired":
                raise WebshareProfileImportError("profile_retired")
            if profile["secret_generation"] != expected_generation:
                raise WebshareProfileImportError("database_generation_conflict")
            allocation_cursor = await connection.execute(
                """select 1 from catalogue.proxy_profile_allocations
                    where provider = %(provider)s and cycle_start = %(cycle)s
                      and profile_id = %(profile)s
                    for update""",
                {
                    "provider": PROVIDER,
                    "cycle": cycle["cycle_start"],
                    "profile": profile["id"],
                },
            )
            if await allocation_cursor.fetchone() is None:
                raise WebshareProfileImportError("profile_allocation_missing")
            active_cursor = await connection.execute(
                """select id from catalogue.proxy_reservations
                    where provider = %(provider)s and profile_id = %(profile)s
                      and state in ('active', 'revocation_requested')
                    for update""",
                {"provider": PROVIDER, "profile": profile["id"]},
            )
            active = await active_cursor.fetchall()
            await connection.execute(
                """update catalogue.proxy_profiles
                      set enabled = false, lifecycle = 'pending', pending_action = null,
                          updated_at = now(), updated_by = %(actor)s
                    where id = %(profile)s""",
                {"profile": profile["id"], "actor": actor_id},
            )
            if active:
                await connection.execute(
                    """update catalogue.proxy_reservations
                          set state = 'revocation_requested', revocation_requested = true
                        where provider = %(provider)s and profile_id = %(profile)s
                          and state = 'active'""",
                    {"provider": PROVIDER, "profile": profile["id"]},
                )
                return WebshareProfileImportResult(
                    operation_id=operation_id,
                    profile_id=profile["id"],
                    provider=PROVIDER,
                    logical_name=secret.logical_name,
                    generation=secret.generation,
                    state="draining",
                    created_profile=False,
                )

        active_intent = await connection.execute(
            """select 1 from catalogue.proxy_profile_secret_intents
                where provider = %(provider)s and profile_id = %(profile)s
                  and state in ('prepared', 'installed')
                for update""",
            {"provider": PROVIDER, "profile": profile["id"]},
        )
        if await active_intent.fetchone() is not None:
            raise WebshareProfileImportError("profile_intent_in_progress")
        intent_cursor = await connection.execute(
            """insert into catalogue.proxy_profile_secret_intents
                      (operation_id, provider, profile_id, logical_name, cycle_start,
                       expected_generation, target_generation, created_profile)
               values (%(operation)s, %(provider)s, %(profile)s, %(logical_name)s,
                       %(cycle)s, %(expected)s, %(target)s, %(created)s)
               returning operation_id, provider, profile_id, logical_name, cycle_start,
                         expected_generation, target_generation, created_profile, state""",
            {
                "operation": operation_id,
                "provider": PROVIDER,
                "profile": profile["id"],
                "logical_name": secret.logical_name,
                "cycle": cycle["cycle_start"],
                "expected": expected_generation,
                "target": secret.generation,
                "created": created_profile,
            },
        )
        row = await intent_cursor.fetchone()
        assert row is not None
        return _intent(row)


def _secret_generation(store: WebshareGatewaySecretStore, intent: _Intent) -> int | None:
    if not store.path.exists():
        return None
    try:
        profile = load_webshare_gateway_secrets(store.path).get(
            (intent.provider, intent.logical_name)
        )
    except ProxyDenied:
        raise WebshareProfileImportError("secret_store_unavailable") from None
    return None if profile is None else profile.generation


async def _mark_failed(connection: Any, intent: _Intent, code: str) -> WebshareProfileImportResult:
    async with connection.transaction():
        await _lock_cycle(connection, intent.cycle_start)
        await connection.execute(
            """update catalogue.proxy_profiles
                  set enabled = false, lifecycle = 'pending', pending_action = null,
                      updated_at = now()
                where provider = %(provider)s and id = %(profile)s""",
            {"provider": intent.provider, "profile": intent.profile_id},
        )
        cursor = await connection.execute(
            """update catalogue.proxy_profile_secret_intents
                  set state = 'failed', error_code = %(code)s, updated_at = now(),
                      completed_at = now()
                where operation_id = %(operation)s
                  and state in ('prepared', 'installed')
                returning state""",
            {"operation": intent.operation_id, "code": code},
        )
        if await cursor.fetchone() is None:
            current = await _intent_for_operation(connection, intent.operation_id)
            if current is None or current.state != "failed":
                raise WebshareProfileImportError("intent_state_conflict")
    return _result(intent, "failed")


async def _finalize(connection: Any, intent: _Intent) -> WebshareProfileImportResult:
    outcome: ImportState = "installed"
    async with connection.transaction():
        cycle = await _database_now(connection, await _lock_cycle(connection, intent.cycle_start))
        profile_cursor = await connection.execute(
            """select secret_generation from catalogue.proxy_profiles
                where provider = %(provider)s and id = %(profile)s
                for update""",
            {"provider": intent.provider, "profile": intent.profile_id},
        )
        profile = await profile_cursor.fetchone()
        if profile is None:
            raise WebshareProfileImportError("profile_not_found")
        intent_cursor = await connection.execute(
            """select operation_id, provider, profile_id, logical_name, cycle_start,
                      expected_generation, target_generation, created_profile, state
                 from catalogue.proxy_profile_secret_intents
                where operation_id = %(operation)s
                for update""",
            {"operation": intent.operation_id},
        )
        row = await intent_cursor.fetchone()
        if row is None:
            raise WebshareProfileImportError("intent_not_found")
        current = _intent(row)
        if current.state == "completed":
            return _result(current, "completed")
        if current.state == "failed":
            return _result(current, "failed")
        await connection.execute(
            """update catalogue.proxy_profile_secret_intents
                  set state = 'installed', installed_at = coalesce(installed_at, now()),
                      updated_at = now()
                where operation_id = %(operation)s""",
            {"operation": intent.operation_id},
        )
        allocation_cursor = await connection.execute(
            """select 1 from catalogue.proxy_profile_allocations
                where provider = %(provider)s and cycle_start = %(cycle)s
                  and profile_id = %(profile)s
                for update""",
            {
                "provider": intent.provider,
                "cycle": intent.cycle_start,
                "profile": intent.profile_id,
            },
        )
        allocation_exists = await allocation_cursor.fetchone() is not None
        expected_database_generation = intent.expected_generation or 0
        if (
            _cycle_is_safe(cycle)
            and allocation_exists
            and profile["secret_generation"] == expected_database_generation
        ):
            await connection.execute(
                """update catalogue.proxy_profiles
                      set secret_generation = %(target)s, secret_installed_at = now(),
                          enabled = true, lifecycle = 'enabled', pending_action = null,
                          updated_at = now()
                    where provider = %(provider)s and id = %(profile)s""",
                {
                    "target": intent.target_generation,
                    "provider": intent.provider,
                    "profile": intent.profile_id,
                },
            )
            await connection.execute(
                """update catalogue.proxy_profile_secret_intents
                      set state = 'completed', error_code = null, updated_at = now(),
                          completed_at = now()
                    where operation_id = %(operation)s""",
                {"operation": intent.operation_id},
            )
            outcome = "completed"
    return _result(intent, outcome)


async def recover_webshare_profile_import(
    connection: Any,
    store: WebshareGatewaySecretStore,
    *,
    operation_id: UUID,
) -> WebshareProfileImportResult:
    """Recover one durable intent using only the installed file generation."""
    intent = await _intent_for_operation(connection, operation_id)
    if intent is None:
        raise WebshareProfileImportError("intent_not_found")
    if intent.state == "completed":
        return _result(intent, "completed")
    if intent.state == "failed":
        return _result(intent, "failed")
    generation = _secret_generation(store, intent)
    if generation == intent.target_generation:
        return await _finalize(connection, intent)
    if intent.state == "prepared" and (
        generation is None or generation == intent.expected_generation
    ):
        return await _mark_failed(connection, intent, "credential_resubmission_required")
    return await _mark_failed(connection, intent, "secret_generation_conflict")


async def install_webshare_profile(
    connection: Any,
    store: WebshareGatewaySecretStore,
    *,
    operation_id: UUID,
    actor_id: str,
    secret: WebshareGatewaySecret,
    expected_generation: int | None,
    display_name: str | None = None,
    allocated_bytes: int | None = None,
) -> WebshareProfileImportResult:
    """Prepare, install, and finalize one validated Webshare secret generation."""
    expected = _strict_expected(expected_generation)
    if not actor_id or len(actor_id) > 200:
        raise WebshareProfileImportError("invalid_actor")
    try:
        secret = validate_webshare_gateway_secret(secret)
    except ProxyDenied:
        raise WebshareProfileImportError("invalid_secret") from None
    if secret.provider != PROVIDER:
        raise WebshareProfileImportError("invalid_secret_identity")
    target = 1 if expected is None else expected + 1
    if secret.generation != target:
        raise WebshareProfileImportError("invalid_target_generation")
    existing = await _intent_for_operation(connection, operation_id)
    if existing is not None:
        operation_cursor = await connection.execute(
            """select actor, action, state from catalogue.proxy_mutation_requests
                where operation_id = %(operation)s""",
            {"operation": operation_id},
        )
        operation = await operation_cursor.fetchone()
        if (
            operation is None
            or operation["actor"] != actor_id
            or operation["action"] != MUTATION_ACTION
            or operation["state"] != "started"
            or existing.provider != secret.provider
            or existing.logical_name != secret.logical_name
            or existing.expected_generation != expected
            or existing.target_generation != secret.generation
        ):
            raise WebshareProfileImportError("operation_intent_mismatch")
        return await recover_webshare_profile_import(connection, store, operation_id=operation_id)
    prepared = await _prepare(
        connection,
        operation_id=operation_id,
        actor_id=actor_id,
        secret=secret,
        expected_generation=expected,
        display_name=display_name,
        allocated_bytes=allocated_bytes,
    )
    if isinstance(prepared, WebshareProfileImportResult):
        return prepared
    try:
        installed = store.install(secret, expected_generation=expected)
    except ProxyDenied:
        return await recover_webshare_profile_import(connection, store, operation_id=operation_id)
    if installed != prepared.target_generation:
        return await _mark_failed(connection, prepared, "secret_generation_conflict")
    return await _finalize(connection, prepared)
