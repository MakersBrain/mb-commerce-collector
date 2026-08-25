"""Proxy control-plane reconciliation and durable mutation bookkeeping."""

from __future__ import annotations

import asyncio
import contextlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from mb_ceramics_catalogue.observability import metrics
from mb_ceramics_catalogue.ops import events
from mb_ceramics_catalogue.providers.base import ProviderError, ProxyProvider, UsageReport
from mb_ceramics_catalogue.providers.registry import ProviderSpec
from mb_ceramics_catalogue.providers.registry import spec as provider_spec
from mb_ceramics_catalogue.proxy import reconcile
from mb_ceramics_catalogue.proxy_secrets import ProfileSecretStore, generate_password
from psycopg.types.json import Jsonb

from catalogue_control.auth import Actor
from catalogue_control.telemetry import get_logger

LOGGER = get_logger("catalogue.control.proxy")

# Paid probes have a 30-second HTTP timeout. Five minutes leaves ample room for
# scheduling and database latency while placing a hard upper bound on an
# abandoned envelope blocking profile draining.
STALE_PROBE_TIMEOUT_SECONDS = 5 * 60


def _jsonb(value: Any) -> Jsonb:
    return Jsonb(value, dumps=lambda item: json.dumps(item, default=str))


@dataclass(frozen=True)
class Mutation:
    operation_id: UUID
    replay_status: int | None = None
    replay_data: dict[str, Any] | None = None


async def begin_mutation(
    connection: Any, actor: Actor, action: str, idempotency_key: str | None,
) -> Mutation:
    if not idempotency_key or len(idempotency_key) > 200:
        raise ValueError("Idempotency-Key is required and must be at most 200 characters")
    operation_id = uuid4()
    cursor = await connection.execute(
        """
        insert into catalogue.proxy_mutation_requests
               (operation_id, actor, action, idempotency_key)
        values (%(operation)s, %(actor)s, %(action)s, %(key)s)
        on conflict (actor, action, idempotency_key) do nothing
        returning operation_id
        """,
        {"operation": operation_id, "actor": actor.id, "action": action, "key": idempotency_key},
    )
    if await cursor.fetchone() is None:
        previous = await connection.execute(
            """select operation_id, state, response_status, response_data
                 from catalogue.proxy_mutation_requests
                where actor = %(actor)s and action = %(action)s and idempotency_key = %(key)s""",
            {"actor": actor.id, "action": action, "key": idempotency_key},
        )
        row = await previous.fetchone()
        if row is None or row["state"] == "started":
            raise RuntimeError("the idempotent operation is still in progress or ambiguous")
        return Mutation(row["operation_id"], row["response_status"], row["response_data"] or {})
    await append_audit(
        connection, actor, operation_id, action, "request", None, "started",
        idempotency_key=idempotency_key,
    )
    return Mutation(operation_id)


async def finish_mutation(
    connection: Any,
    mutation: Mutation,
    actor: Actor,
    action: str,
    *,
    status: int,
    data: dict[str, Any],
    state: str = "succeeded",
    resource_type: str = "request",
    resource_id: str | None = None,
    error_code: str | None = None,
) -> None:
    await connection.execute(
        """
        update catalogue.proxy_mutation_requests
           set state = %(state)s, response_status = %(status)s,
               response_data = %(data)s, completed_at = now()
         where operation_id = %(operation)s
        """,
        {
            "state": state, "status": status, "data": _jsonb(data),
            "operation": mutation.operation_id,
        },
    )
    await append_audit(
        connection, actor, mutation.operation_id, action, resource_type, resource_id,
        state, success=state == "succeeded", error_code=error_code,
        response_status=status, response_data=data,
    )


async def append_audit(
    connection: Any,
    actor: Actor,
    operation_id: UUID,
    action: str,
    resource_type: str,
    resource_id: str | None,
    state: str,
    *,
    idempotency_key: str | None = None,
    success: bool | None = None,
    error_code: str | None = None,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    response_status: int | None = None,
    response_data: dict[str, Any] | None = None,
) -> None:
    await connection.execute(
        """
        insert into catalogue.proxy_admin_audit
               (operation_id, actor, actor_role, request_id, idempotency_key,
                action, resource_type, resource_id, state, success, error_code,
                before_data, after_data, response_status, response_data)
        values (%(operation)s, %(actor)s, %(role)s, %(request)s, %(key)s,
                %(action)s, %(resource_type)s, %(resource_id)s, %(state)s,
                %(success)s, %(error)s, %(before)s, %(after)s, %(status)s, %(response)s)
        """,
        {
            "operation": operation_id, "actor": actor.id, "role": actor.role,
            "request": actor.nonce, "key": idempotency_key, "action": action,
            "resource_type": resource_type, "resource_id": resource_id, "state": state,
            "success": success, "error": error_code,
            "before": _jsonb(before) if before is not None else None,
            "after": _jsonb(after) if after is not None else None,
            "status": response_status,
            "response": _jsonb(response_data) if response_data is not None else None,
        },
    )


def _bucket_bounds(key: str, group_by: str) -> tuple[datetime, datetime]:
    try:
        start = datetime.fromisoformat(key.replace("Z", "+00:00"))
    except ValueError as error:
        raise ProviderError("provider_invalid_bucket", "provider returned an invalid traffic bucket") from error
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    delta = timedelta(hours=1) if group_by == "hour" else timedelta(days=1)
    return start.astimezone(UTC), (start + delta).astimezone(UTC)


async def reconcile_now(
    connection: Any,
    provider: ProxyProvider,
    *,
    reason: str,
    spec: ProviderSpec | None = None,
) -> UsageReport:
    selected = spec or provider_spec("decodo")
    if not selected.reconciliation_groupings:
        raise ProviderError(
            "provider_reconciliation_unsupported",
            f"{selected.label} cannot report usage for a billing window",
        )
    locked = await connection.execute(
        "select pg_try_advisory_lock(hashtext(%(lock)s)) as locked",
        {"lock": selected.lock_key},
    )
    lock_row = await locked.fetchone()
    if not lock_row or not lock_row["locked"]:
        raise RuntimeError(f"a {selected.label} reconciliation is already running")
    try:
        active = await connection.execute(
            """select * from catalogue.proxy_budget_cycles
                where provider = %(provider)s and lifecycle = 'active'""",
            {"provider": selected.name},
        )
        cycle = await active.fetchone()
        if cycle is None:
            raise RuntimeError(f"there is no active {selected.label} billing cycle")
        try:
            reports = await asyncio.gather(
                *(
                    provider.usage(
                        cycle["cycle_start"],
                        cycle["cycle_end"],
                        group_by=grouping,
                    )
                    for grouping in selected.reconciliation_groupings
                )
            )
            grouped_reports = tuple(zip(selected.reconciliation_groupings, reports, strict=True))
            for grouping, report in grouped_reports:
                if grouping == "total" and len(report.buckets) != 1:
                    raise ProviderError(
                        "provider_usage_unreadable",
                        f"{selected.label} did not return exactly one aggregate usage bucket",
                    )
        except Exception:
            metrics.proxy_reconciliation(False)
            await reconcile(
                connection, cycle_start=cycle["cycle_start"], provider_reported_bytes=0,
                successful=False, provider=selected.name,
            )
            await connection.execute(
                """update catalogue.proxy_reconcile_requests
                      set attempts = attempts + 1, error_code = 'provider_failure', claimed_at = now()
                    where provider = %(provider)s and completed_at is null""",
                {"provider": selected.name},
            )
            await events.notify(
                connection, "proxy.reconciliation_failed",
                f"{selected.label} reconciliation failed",
                severity=events.Severity.CRITICAL,
                dedup_key=f"proxy:{selected.name}:reconciliation",
                body="Paid traffic remains fail-closed until provider usage can be reconciled.",
            )
            raise

        async with connection.transaction():
            for dimension, grouped_report in grouped_reports:
                for bucket in grouped_report.buckets:
                    if dimension == "day":
                        start, end = _bucket_bounds(bucket.key, "day")
                        grouping_key = bucket.key
                    elif dimension == "total":
                        start, end = cycle["cycle_start"], cycle["cycle_end"]
                        grouping_key = "total"
                    else:
                        start, end = cycle["cycle_start"], cycle["cycle_end"]
                        grouping_key = bucket.key[:500]
                    await connection.execute(
                        """
                        insert into catalogue.proxy_provider_snapshots
                           (provider, cycle_start, source_endpoint, grouping_dimension,
                            grouping_key, bucket_start, bucket_end, transmitted_bytes,
                            received_bytes, total_bytes, request_count)
                    values (%(provider)s, %(cycle)s, 'traffic', %(dimension)s, %(key)s, %(start)s,
                            %(end)s, %(tx)s, %(rx)s, %(total)s, %(requests)s)
                    on conflict (provider, cycle_start, source_endpoint, grouping_dimension,
                                 grouping_key, bucket_start, bucket_end) do update
                      set transmitted_bytes = greatest(
                            catalogue.proxy_provider_snapshots.transmitted_bytes,
                            excluded.transmitted_bytes),
                          received_bytes = greatest(
                            catalogue.proxy_provider_snapshots.received_bytes,
                            excluded.received_bytes),
                          total_bytes = greatest(catalogue.proxy_provider_snapshots.total_bytes,
                                                 excluded.total_bytes),
                          request_count = greatest(catalogue.proxy_provider_snapshots.request_count,
                                                   excluded.request_count),
                          last_observed_at = now()
                        """,
                        {
                            "provider": selected.name,
                            "cycle": cycle["cycle_start"], "dimension": dimension,
                            "key": grouping_key,
                            "start": start, "end": end,
                            "tx": bucket.transmitted_bytes, "rx": bucket.received_bytes,
                            "total": bucket.total_bytes, "requests": bucket.requests,
                        },
                    )
            await reconcile(
                connection, cycle_start=cycle["cycle_start"],
                provider_reported_bytes=reports[0].total_bytes, successful=True,
                provider=selected.name,
            )
            await connection.execute(
                """update catalogue.proxy_reconcile_requests
                      set completed_at = now(), claimed_at = coalesce(claimed_at, now()),
                          attempts = attempts + 1, error_code = null
                    where provider = %(provider)s and completed_at is null""",
                {"provider": selected.name},
            )
            await events.emit(
                connection, events.Topic.PROXY, "proxy.usage_updated",
                payload={
                    "provider": selected.name,
                    "bytes": reports[0].total_bytes,
                    "reason": reason,
                },
            )
            metrics.proxy_reconciliation(True)
            metrics.proxy_bytes("provider", reports[0].total_bytes)
            metrics.proxy_bytes(
                "operational_headroom",
                max(0, cycle["operational_bytes"] - reports[0].total_bytes),
            )
            await events.resolve(connection, f"proxy:{selected.name}:reconciliation")
            if reports[0].total_bytes >= cycle["operational_bytes"]:
                await events.notify(
                    connection, "proxy.budget_exhausted",
                    f"{selected.label} operational budget exhausted",
                    severity=events.Severity.CRITICAL,
                    dedup_key=f"proxy:{selected.name}:budget",
                    body="The database kill switch is active; no new paid lease can start.",
                )
            else:
                await events.resolve(connection, f"proxy:{selected.name}:budget")
        return reports[0]
    finally:
        await connection.execute(
            "select pg_advisory_unlock(hashtext(%(lock)s))",
            {"lock": selected.lock_key},
        )


async def finalize_retirements(
    connection: Any,
    provider: ProxyProvider,
    *,
    provider_name: str = "decodo",
) -> None:
    cursor = await connection.execute(
        """
        select x.*, p.provider_resource_id
          from catalogue.proxy_profile_retirements x
         join catalogue.proxy_profiles p on p.id = x.profile_id
         where p.provider = %(provider)s and x.state = 'draining'
           and not exists (
             select 1 from catalogue.proxy_reservations r
              where r.profile_id = x.profile_id
                and r.secret_generation = x.old_secret_generation
                and r.state in ('active', 'revocation_requested')
           )
         order by x.created_at for update skip locked
         limit 10
        """,
        {"provider": provider_name},
    )
    for row in await cursor.fetchall():
        await connection.execute(
            "update catalogue.proxy_profile_retirements set state = 'finalizing' where id = %(id)s",
            {"id": row["id"]},
        )
        try:
            await provider.update_subuser(row["old_provider_resource_id"], status="disabled")
            await provider.delete_subuser(row["old_provider_resource_id"])
            await provider.update_subuser(
                row["replacement_resource_id"], traffic_limit_bytes=row["target_limit_bytes"]
            )
        except ProviderError as error:
            await connection.execute(
                """update catalogue.proxy_profile_retirements
                      set state = 'failed', error_code = %(error)s where id = %(id)s""",
                {"id": row["id"], "error": error.code},
            )
            await connection.execute(
                "update catalogue.proxy_budget_cycles set kill_switch = true "
                "where provider = %(provider)s and lifecycle = 'active'",
                {"provider": provider_name},
            )
            continue
        async with connection.transaction():
            await connection.execute(
                """update catalogue.proxy_profile_retirements
                      set state = 'completed', completed_at = now(), error_code = null
                    where id = %(id)s""",
                {"id": row["id"]},
            )
            await connection.execute(
                """update catalogue.proxy_profiles
                      set provider_traffic_limit_bytes = %(limit)s,
                          provider_observed_at = now(), updated_at = now()
                    where id = %(id)s""",
                {"id": row["profile_id"], "limit": row["target_limit_bytes"]},
            )
            await events.emit(
                connection, events.Topic.PROXY, "proxy.profile_rotation_completed",
                payload={"profile_id": str(row["profile_id"])},
            )


async def finalize_draining_profiles(
    connection: Any,
    provider: ProxyProvider,
    secret_file: Path,
    *,
    provider_name: str = "decodo",
) -> None:
    """Finish drain-first disable, rotation, and retirement after lease quiescence."""
    cursor = await connection.execute(
        """select p.* from catalogue.proxy_profiles p
             where p.provider = %(provider)s
               and p.lifecycle = 'draining' and p.pending_action is not null
               and not exists (
                 select 1 from catalogue.proxy_reservations r
                  where r.profile_id = p.id
                    and r.state in ('active', 'revocation_requested')
               )
               and not exists (
                 select 1 from catalogue.proxy_profile_retirements x
                  where x.profile_id = p.id and x.state in ('draining', 'finalizing')
               )
             order by p.updated_at for update skip locked limit 10""",
        {"provider": provider_name},
    )
    for row in await cursor.fetchall():
        resource = row["provider_resource_id"]
        action = row["pending_action"]
        try:
            if action == "rotate":
                store = ProfileSecretStore(secret_file)
                installed = store.read_raw().get(row["logical_name"], {})
                username = str(installed.get("username", ""))
                if not username:
                    raise RuntimeError("installed profile username is missing")
                password = generate_password()
                await provider.update_subuser(resource, password=password)
                generation = store.install(
                    row["logical_name"], username=username, password=password
                )
                await connection.execute(
                    """update catalogue.proxy_profiles set lifecycle = 'enabled', enabled = true,
                              pending_action = null, secret_generation = %(generation)s,
                              secret_installed_at = now(), updated_at = now()
                        where id = %(id)s""",
                    {"id": row["id"], "generation": generation},
                )
            elif action == "disable":
                await provider.update_subuser(resource, status="disabled")
                await connection.execute(
                    """update catalogue.proxy_profiles set lifecycle = 'disabled', enabled = false,
                              pending_action = null, updated_at = now() where id = %(id)s""",
                    {"id": row["id"]},
                )
            elif action == "retire":
                await provider.update_subuser(resource, status="disabled")
                await provider.delete_subuser(resource)
                ProfileSecretStore(secret_file).remove(row["logical_name"])
                await connection.execute(
                    """update catalogue.proxy_profiles set lifecycle = 'retired', enabled = false,
                              pending_action = null, retired_at = now(), updated_at = now()
                        where id = %(id)s""",
                    {"id": row["id"]},
                )
            await events.emit(
                connection, events.Topic.PROXY, "proxy.profile_changed",
                payload={"profile_id": str(row["id"]), "state": action},
            )
        except (ProviderError, OSError, RuntimeError, TypeError, ValueError):
            await connection.execute(
                """update catalogue.proxy_profiles
                      set lifecycle = 'provider_changed_local_failed', enabled = false,
                          updated_at = now() where id = %(id)s""",
                {"id": row["id"]},
            )
            await connection.execute(
                "update catalogue.proxy_budget_cycles set kill_switch = true "
                "where provider = %(provider)s and lifecycle = 'active'",
                {"provider": provider_name},
            )
            LOGGER.warning("proxy.profile_drain_finalize_failed", extra={"profile_id": str(row["id"])})


async def close_stale_reservations(connection: Any) -> int:
    """Release envelopes abandoned by jobs or timed-out paid probes."""
    async with connection.transaction():
        candidates_cursor = await connection.execute(
            """select r.id, r.provider, r.cycle_start
                 from catalogue.proxy_reservations r
                 left join catalogue.jobs j on j.id = r.job_id
                 left join catalogue.proxy_probes p on p.id = r.probe_id
                where r.state in ('active', 'revocation_requested')
                  and (
                    (r.job_id is not null
                     and (j.state in ('succeeded', 'degraded', 'failed', 'cancelled', 'skipped')
                          or (j.state in ('leased', 'running')
                              and j.lease_expires_at < now())))
                    or
                    (r.probe_id is not null
                     and p.state in ('pending', 'running')
                     and greatest(p.requested_at, r.created_at)
                         < now() - make_interval(secs => %(probe_timeout)s))
                  )
                order by r.provider, r.cycle_start, r.id""",
            {"probe_timeout": STALE_PROBE_TIMEOUT_SECONDS},
        )
        candidates = await candidates_cursor.fetchall()
        if not candidates:
            return 0

        # Match the paid-traffic lock hierarchy used by reserve/close/revoke:
        # budget cycle first, then reservation. Deterministic cycle ordering
        # also prevents two cleanup runs from deadlocking each other.
        cycles = sorted({(row["provider"], row["cycle_start"]) for row in candidates})
        for provider, cycle_start in cycles:
            await connection.execute(
                """select 1
                     from catalogue.proxy_budget_cycles
                    where provider = %(provider)s and cycle_start = %(cycle_start)s
                    for update""",
                {"provider": provider, "cycle_start": cycle_start},
            )

        cursor = await connection.execute(
            """update catalogue.proxy_reservations r
                  set state = 'cancelled', closed_at = now()
                where r.id = any(%(candidate_ids)s::uuid[])
                  and r.state in ('active', 'revocation_requested')
                  and (
                    exists (
                      select 1 from catalogue.jobs j
                       where j.id = r.job_id
                         and (j.state in
                                ('succeeded', 'degraded', 'failed', 'cancelled', 'skipped')
                              or (j.state in ('leased', 'running')
                                  and j.lease_expires_at < now()))
                    )
                    or exists (
                      select 1 from catalogue.proxy_probes p
                       where p.id = r.probe_id
                         and p.state in ('pending', 'running')
                         and greatest(p.requested_at, r.created_at)
                             < now() - make_interval(secs => %(probe_timeout)s)
                    )
                  )
                returning r.id, r.provider, r.cycle_start, r.probe_id""",
            {
                "candidate_ids": [row["id"] for row in candidates],
                "probe_timeout": STALE_PROBE_TIMEOUT_SECONDS,
            },
        )
        rows = await cursor.fetchall()
        settled_cycles = sorted(
            {(row["provider"], row["cycle_start"]) for row in rows}
        )
        for provider, cycle_start in settled_cycles:
            await connection.execute(
                """update catalogue.proxy_budget_cycles
                      set application_bytes = greatest(
                            application_bytes,
                            (select coalesce(sum(estimated_bytes), 0)
                               from catalogue.proxy_reservations
                              where provider = %(provider)s
                                and cycle_start = %(cycle_start)s
                                and state in ('closed', 'cancelled'))
                          ),
                          kill_switch = kill_switch or (
                            select coalesce(sum(estimated_bytes), 0) >= operational_bytes
                              from catalogue.proxy_reservations
                             where provider = %(provider)s
                               and cycle_start = %(cycle_start)s
                               and state in ('closed', 'cancelled')
                          )
                    where provider = %(provider)s and cycle_start = %(cycle_start)s""",
                {"provider": provider, "cycle_start": cycle_start},
            )
        probe_ids = [row["probe_id"] for row in rows if row["probe_id"] is not None]
        if probe_ids:
            await connection.execute(
                """update catalogue.proxy_probes
                      set state = 'cancelled', completed_at = coalesce(completed_at, now()),
                          error_category = coalesce(error_category, 'stale_timeout')
                    where id = any(%(probe_ids)s::uuid[])
                      and state in ('pending', 'running')""",
                {"probe_ids": probe_ids},
            )
        for row in rows:
            await connection.execute(
                """insert into catalogue.proxy_reconcile_requests
                           (provider, reason, reservation_id, dedup_key)
                    values (%(provider)s, 'stale_reservation', %(id)s,
                            'reservation:' || %(id)s::text)
                    on conflict (dedup_key) do nothing""",
                row,
            )
        if rows:
            await events.emit(
                connection, events.Topic.PROXY, "proxy.reservations_stale_closed",
                payload={"count": len(rows)},
            )
        return len(rows)


class ReconciliationScheduler:
    def __init__(
        self, pool: Any, provider: ProxyProvider, interval: float,
        secret_file: Path | None = None, *, provider_name: str = "decodo",
    ) -> None:
        self.pool = pool
        self.provider = provider
        self.interval = max(60.0, interval)
        self.secret_file = secret_file
        self.spec = provider_spec(provider_name)
        self.task: asyncio.Task[None] | None = None
        self.stopping = False

    async def start(self) -> None:
        self.task = asyncio.create_task(self._run(), name="proxy-reconciliation")

    async def stop(self) -> None:
        self.stopping = True
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task

    async def _run(self) -> None:
        last_scheduled = 0.0
        while not self.stopping:
            try:
                async with self.pool.connection() as connection:
                    await close_stale_reservations(connection)
                    if self.spec.can_provision_subusers and self.spec.has_subuser_status:
                        await finalize_retirements(
                            connection,
                            self.provider,
                            provider_name=self.spec.name,
                        )
                    if self.spec.can_provision_subusers and self.secret_file is not None:
                        await finalize_draining_profiles(
                            connection,
                            self.provider,
                            self.secret_file,
                            provider_name=self.spec.name,
                        )
                    pending = await connection.execute(
                        "select exists(select 1 from catalogue.proxy_reconcile_requests "
                        "where provider = %(provider)s and completed_at is null) as pending",
                        {"provider": self.spec.name},
                    )
                    row = await pending.fetchone()
                    now = asyncio.get_running_loop().time()
                    if (row and row["pending"]) or now - last_scheduled >= self.interval:
                        await reconcile_now(
                            connection,
                            self.provider,
                            reason="outbox" if row and row["pending"] else "scheduled",
                            spec=self.spec,
                        )
                        last_scheduled = now
            except Exception:
                LOGGER.warning("proxy.reconciliation_failed", exc_info=True)
            await asyncio.sleep(5)
