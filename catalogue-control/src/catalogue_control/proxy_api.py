"""HTTP handlers for the provider-aware `/v1/proxy` control plane."""

from __future__ import annotations

import ipaddress
import json
import secrets
import time
from datetime import datetime
from typing import Any
from uuid import UUID

import httpx
from mb_ceramics_catalogue.observability import metrics
from mb_ceramics_catalogue.ops import events
from mb_ceramics_catalogue.providers.base import ProviderError
from mb_ceramics_catalogue.providers.registry import ProviderSpec, known, spec
from mb_ceramics_catalogue.proxy import (
    ProxyDenied,
    ProxyLease,
    close_reservation,
    load_profiles,
    reserve,
)
from mb_ceramics_catalogue.proxy_secrets import (
    ProfileSecretStore,
    generate_password,
    mask_username,
    username_fingerprint,
)
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from catalogue_control.auth import Actor, ActorRejected, require_actor
from catalogue_control.proxy_control import (
    Mutation,
    append_audit,
    begin_mutation,
    finish_mutation,
    reconcile_now,
)


def problem(status: int, title: str, detail: str) -> Response:
    return JSONResponse(
        {"type": "about:blank", "title": title, "status": status, "detail": detail},
        status_code=status,
        media_type="application/problem+json",
    )


def payload(data: dict[str, Any], *, status: int = 200) -> Response:
    return Response(json.dumps(data, default=str), status_code=status, media_type="application/json")


async def mutation_payload(
    connection: Any, mutation: Mutation, actor: Actor, action: str,
    profile_id: UUID, data: dict[str, Any], *, status: int = 202,
) -> Response:
    await finish_mutation(
        connection, mutation, actor, action, status=status, data=data,
        resource_type="profile", resource_id=str(profile_id),
    )
    return payload(data, status=status)


async def mutation_problem(
    connection: Any, mutation: Mutation, actor: Actor, action: str,
    profile_id: UUID, *, status: int, title: str, detail: str, code: str,
    state: str = "failed",
) -> Response:
    await finish_mutation(
        connection, mutation, actor, action, status=status, data={"error": code},
        state=state, resource_type="profile", resource_id=str(profile_id), error_code=code,
    )
    return problem(status, title, detail)


async def actor_for(
    request: Request, *, admin: bool = False, recent: bool = False,
) -> Actor | Response:
    try:
        return await require_actor(
            request, role="admin" if admin else "viewer", recent=recent
        )
    except ActorRejected as error:
        return problem(403, "Forbidden", str(error))


def provider_name(request: Request) -> str:
    """Which provider this request is about.

    A `?provider=` query names it; otherwise the configured default. Every
    handler resolves this once and threads it into its SQL as a bound parameter,
    which is what replaced twenty-five occurrences of the literal 'decodo'.
    """
    requested = request.query_params.get("provider")
    if requested:
        return requested
    return request.app.state.settings.proxy_default_provider


def provider_spec(request: Request) -> ProviderSpec | Response:
    try:
        return spec(provider_name(request))
    except ProviderError:
        return problem(
            404, "Unknown provider",
            f"no such proxy provider; known providers are {', '.join(known())}",
        )


def provider_for(request: Request) -> Any | Response:
    """The constructed adapter for this request's provider.

    Distinct from `provider_spec`, which is static description and always
    available. This is the live client and may be absent because no credential
    was configured for that provider, which is a different failure and a
    different status code.
    """
    name = provider_name(request)
    providers = getattr(request.app.state, "providers", None) or {}
    provider = providers.get(name)
    if provider is None and name == request.app.state.settings.proxy_default_provider:
        provider = request.app.state.provider
    if provider is None:
        return problem(
            503, "Provider unavailable", f"{name} API access is not configured"
        )
    return provider


def _uuid(request: Request, name: str = "id") -> UUID | None:
    try:
        return UUID(request.path_params[name])
    except (KeyError, ValueError):
        return None


def _cycle_confirmation_matches(body: dict[str, Any], row: dict[str, Any]) -> bool:
    for field in ("purchased_bytes", "operational_bytes", "daily_bytes", "pilot_bytes",
                  "unmanaged_allocation_bytes"):
        try:
            value = body.get(field)
            if value is None or int(value) != int(row[field]):
                return False
        except (TypeError, ValueError):
            return False
    for field in ("cycle_start", "cycle_end"):
        value = body.get(field)
        if not isinstance(value, str):
            return False
        try:
            confirmed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
        if confirmed != row[field]:
            return False
    return True


async def overview(request: Request) -> Response:
    actor = await actor_for(request)
    if isinstance(actor, Response):
        return actor
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    async with request.app.state.pool.connection() as connection:
        cursor = await connection.execute(
            """
            select c.*,
                   coalesce(r.active_reserved_bytes, 0) as active_reserved_bytes,
                   coalesce(r.active_reservations, 0) as active_reservations,
                   coalesce(r.daily_used_bytes, 0) as daily_used_bytes,
                   greatest(c.provider_reported_bytes, c.application_bytes)
                     + coalesce(r.active_reserved_bytes, 0) as accounted_bytes,
                   greatest(0, c.operational_bytes
                     - greatest(c.provider_reported_bytes, c.application_bytes)
                     - coalesce(r.active_reserved_bytes, 0)) as remaining_operational_bytes,
                   greatest(c.provider_reported_bytes - c.application_bytes, 0)
                     as provider_application_discrepancy_bytes,
                   extract(epoch from now() - c.reconciled_at) as reconciliation_age_seconds,
                   least(c.daily_bytes, greatest(0,
                     (c.operational_bytes
                       - greatest(c.provider_reported_bytes, c.application_bytes)
                       - coalesce(r.active_reserved_bytes, 0))
                     / greatest(1, ceil(extract(epoch from (c.cycle_end - now())) / 86400))))
                     as dynamic_daily_bytes
              from catalogue.proxy_budget_cycles c
              left join lateral (
                select coalesce(sum(reserved_bytes) filter (
                         where state in ('active', 'revocation_requested')), 0)
                         as active_reserved_bytes,
                       count(*) filter (
                         where state in ('active', 'revocation_requested'))
                         as active_reservations,
                       coalesce(sum(estimated_bytes) filter (
                         where state = 'closed' and created_at >= date_trunc('day', now())), 0)
                         as daily_used_bytes
                  from catalogue.proxy_reservations
                 where provider = c.provider and cycle_start = c.cycle_start
              ) r on true
             where c.provider = %(provider)s
             order by (c.lifecycle = 'active') desc, c.cycle_start desc
             limit 1
            """,
            {"provider": pspec.name},
        )
        cycle = await cursor.fetchone()
        profiles_cursor = await connection.execute(
            """select count(*) filter (where enabled) as enabled,
                      count(*) as total from catalogue.proxy_profiles"""
        )
        profiles = await profiles_cursor.fetchone()
    resolved = provider_for(request)
    provider = None if isinstance(resolved, Response) else resolved
    subscription: dict[str, Any] | None = None
    provider_error: str | None = None
    # A provider that cannot propose cycles has no subscription to read, and
    # asking would report a provider error for what is a design fact.
    if provider is not None and pspec.proposes_cycles:
        try:
            subscription = (await provider.subscription()).model_dump(mode="json")
        except ProviderError as error:
            provider_error = error.code
    return payload(
        {
            "deployment_enabled": request.app.state.settings.proxy_enabled,
            "mutations_enabled": request.app.state.settings.proxy_mutations_enabled,
            "paid_probe_enabled": request.app.state.settings.proxy_paid_probe_enabled,
            "provider_configured": provider is not None,
            "provider_error": provider_error,
            "subscription": subscription,
            "cycle": cycle,
            "profiles": profiles or {"enabled": 0, "total": 0},
            # What the UI needs to stop hardcoding "Decodo Residential".
            "provider": {
                "name": pspec.name,
                "label": pspec.label,
                "proposes_cycles": pspec.proposes_cycles,
                "has_subuser_status": pspec.has_subuser_status,
                "known": known(),
            },
        }
    )


async def cycles(request: Request) -> Response:
    actor = await actor_for(request)
    if isinstance(actor, Response):
        return actor
    async with request.app.state.pool.connection() as connection:
        cursor = await connection.execute(
            "select * from catalogue.proxy_budget_cycles order by cycle_start desc limit 100"
        )
        rows = await cursor.fetchall()
    return payload({"cycles": rows})


async def usage(request: Request) -> Response:
    actor = await actor_for(request)
    if isinstance(actor, Response):
        return actor
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    group_by = request.query_params.get("group_by", "day")
    allowed = {"day", "source", "target", "profile"}
    if group_by not in allowed:
        return problem(400, "Bad Request", "group_by must be day, target, source, or profile")
    async with request.app.state.pool.connection() as connection:
        if group_by in {"day", "target"}:
            cursor = await connection.execute(
                """select case when %(group)s = 'day' then bucket_start::text
                                else grouping_key end as key,
                          transmitted_bytes, received_bytes,
                          total_bytes, request_count, last_observed_at
                     from catalogue.proxy_provider_snapshots
                    where provider = %(provider)s and grouping_dimension = %(group)s
                    order by bucket_start desc limit 180""",
                {"group": group_by, "provider": pspec.name},
            )
        elif group_by == "profile":
            cursor = await connection.execute(
                """select coalesce(p.logical_name, 'retired') as key,
                          null::bigint as transmitted_bytes,
                          null::bigint as received_bytes,
                          sum(r.estimated_bytes) as total_bytes,
                          sum(r.request_count) as request_count,
                          max(r.closed_at) as last_observed_at
                     from catalogue.proxy_reservations r
                     left join catalogue.proxy_profiles p on p.id = r.profile_id
                    where r.state = 'closed'
                    group by coalesce(p.logical_name, 'retired')
                    order by total_bytes desc limit 200"""
            )
        else:
            column = "j.source_id"
            cursor = await connection.execute(
                f"""select {column} as key, sum(r.estimated_bytes) as total_bytes,
                            sum(r.request_count) as request_count,
                            max(r.closed_at) as last_observed_at
                       from catalogue.proxy_reservations r
                       left join catalogue.jobs j on j.id = r.job_id
                      where r.state = 'closed'
                      group by {column} order by total_bytes desc nulls last limit 200"""
            )
        rows = await cursor.fetchall()
    return payload({"group_by": group_by, "usage": rows})


async def reservations(request: Request) -> Response:
    actor = await actor_for(request)
    if isinstance(actor, Response):
        return actor
    state = request.query_params.get("state")
    async with request.app.state.pool.connection() as connection:
        cursor = await connection.execute(
            """
            select r.*, j.source_id, j.run_id, j.state as job_state,
                   p.state as probe_state
              from catalogue.proxy_reservations r
              left join catalogue.jobs j on j.id = r.job_id
              left join catalogue.proxy_probes p on p.id = r.probe_id
             where (%(state)s::text is null or r.state = %(state)s::text)
             order by (r.state in ('active', 'revocation_requested')) desc,
                      r.created_at desc limit 500
            """,
            {"state": state},
        )
        rows = await cursor.fetchall()
    return payload({"reservations": rows})


async def profiles(request: Request) -> Response:
    actor = await actor_for(request)
    if isinstance(actor, Response):
        return actor
    async with request.app.state.pool.connection() as connection:
        cursor = await connection.execute(
            """
            select p.*, a.allocated_bytes,
                   count(distinct r.id) as route_count,
                   count(distinct s.source_id) as source_count,
                   count(distinct v.id) filter (where v.state in
                     ('active', 'revocation_requested')) as active_reservations
              from catalogue.proxy_profiles p
              left join catalogue.proxy_budget_cycles c
                on c.provider = p.provider and c.lifecycle = 'active'
              left join catalogue.proxy_profile_allocations a
                on a.provider = c.provider and a.cycle_start = c.cycle_start
               and a.profile_id = p.id
              left join catalogue.proxy_routes r on r.profile_id = p.id and r.retired_at is null
              left join catalogue.source_proxy_policies s on s.route_id = r.id and s.policy <> 'never'
              left join catalogue.proxy_reservations v on v.profile_id = p.id
             group by p.id, a.allocated_bytes order by p.created_at desc
            """
        )
        rows = await cursor.fetchall()
    return payload({"profiles": rows})


async def routes(request: Request) -> Response:
    actor = await actor_for(request)
    if isinstance(actor, Response):
        return actor
    async with request.app.state.pool.connection() as connection:
        cursor = await connection.execute(
            """select r.*, p.logical_name as profile, p.username_mask,
                      count(s.source_id) filter (where s.policy <> 'never') as source_count
                 from catalogue.proxy_routes r
                 join catalogue.proxy_profiles p on p.id = r.profile_id
                 left join catalogue.source_proxy_policies s on s.route_id = r.id
                where r.retired_at is null
                group by r.id, p.logical_name, p.username_mask
                order by r.created_at desc"""
        )
        rows = await cursor.fetchall()
    return payload({"routes": rows})


async def probes(request: Request) -> Response:
    actor = await actor_for(request)
    if isinstance(actor, Response):
        return actor
    async with request.app.state.pool.connection() as connection:
        cursor = await connection.execute(
            """select p.id, p.route_id, p.profile_id, p.state, p.requested_at,
                      p.completed_at, p.error_category, p.estimated_bytes,
                      p.provider_requests, p.exit_country,
                      case when p.exit_ip_expires_at > now() then host(p.exit_ip) end as exit_ip,
                      p.latency_ms, p.protocol, p.actor, p.request_id,
                      r.id as reservation_id
                 from catalogue.proxy_probes p
                 left join catalogue.proxy_reservations r on r.probe_id = p.id
                order by p.requested_at desc limit 200"""
        )
        rows = await cursor.fetchall()
    return payload({"probes": rows})


async def audit(request: Request) -> Response:
    actor = await actor_for(request)
    if isinstance(actor, Response):
        return actor
    async with request.app.state.pool.connection() as connection:
        cursor = await connection.execute(
            """select id, operation_id, actor, actor_role, request_id, action,
                      resource_type, resource_id, at, state, success, error_code,
                      before_data, after_data, response_status
                 from catalogue.proxy_admin_audit order by id desc limit 500"""
        )
        rows = await cursor.fetchall()
    return payload({"audit": rows})


async def candidates(request: Request) -> Response:
    actor = await actor_for(request)
    if isinstance(actor, Response):
        return actor
    sources = request.app.state.sources
    eligible = [name for name, config in sources.items() if config.proxy_eligible]
    async with request.app.state.pool.connection() as connection:
        cursor = await connection.execute(
            """
            select j.source_id, count(*) filter (where j.state = 'failed') as failures,
                   count(*) as runs, max(j.finished_at) as last_finished_at,
                   (array_agg(j.error order by j.finished_at desc)
                     filter (where j.error is not null))[1] as last_error,
                   p.policy, p.evidence_state, p.evidence_count, p.route_id
              from catalogue.jobs j
              left join catalogue.source_proxy_policies p on p.source_id = j.source_id
             where j.source_id = any(%(eligible)s) and j.finished_at > now() - interval '30 days'
             group by j.source_id, p.policy, p.evidence_state, p.evidence_count, p.route_id
             order by failures desc, j.source_id
            """,
            {"eligible": eligible},
        )
        rows = await cursor.fetchall()
    return payload({"candidates": rows, "eligible_sources": eligible})


async def reconcile_action(request: Request) -> Response:
    actor = await actor_for(request, admin=True)
    if isinstance(actor, Response):
        return actor
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    provider = provider_for(request)
    if isinstance(provider, Response):
        return provider
    async with request.app.state.pool.connection() as connection:
        try:
            mutation = await begin_mutation(
                connection, actor, "reconcile", request.headers.get("idempotency-key")
            )
        except (ValueError, RuntimeError) as error:
            return problem(409, "Idempotency conflict", str(error))
        if mutation.replay_status:
            return payload(mutation.replay_data or {}, status=mutation.replay_status)
        data: dict[str, Any]
        try:
            report = await reconcile_now(
                connection,
                provider,
                reason=f"manual:{actor.id}",
                spec=pspec,
            )
        except (ProviderError, RuntimeError) as error:
            data = {"error": "reconciliation_failed"}
            status = 502 if isinstance(error, ProviderError) else 409
            await finish_mutation(
                connection, mutation, actor, "reconcile", status=status, data=data,
                state="failed", error_code="reconciliation_failed",
            )
            return problem(status, "Reconciliation failed", str(error))
        data = {"status": "reconciled", "provider_reported_bytes": report.total_bytes}
        await finish_mutation(connection, mutation, actor, "reconcile", status=202, data=data)
    return payload(data, status=202)


async def kill_switch(request: Request) -> Response:
    action = request.path_params["action"]
    if action not in {"activate", "clear", "revoke"}:
        return problem(404, "Not Found", "unknown kill-switch action")
    actor = await actor_for(request, admin=True, recent=action in {"clear", "revoke"})
    if isinstance(actor, Response):
        return actor
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    if action in {"clear", "revoke"}:
        body = await request.json()
        expected = (
            "ENABLE PAID PROXY TRAFFIC" if action == "clear"
            else "REVOKE ACTIVE PROXY LEASES"
        )
        if not isinstance(body, dict) or body.get("confirmation") != expected:
            return problem(422, "Confirmation required", f"confirmation must equal {expected!r}")
    async with request.app.state.pool.connection() as connection:
        try:
            mutation = await begin_mutation(connection, actor, f"kill_switch.{action}", request.headers.get("idempotency-key"))
        except (ValueError, RuntimeError) as error:
            return problem(409, "Idempotency conflict", str(error))
        if mutation.replay_status:
            return payload(mutation.replay_data or {}, status=mutation.replay_status)
        if action == "activate":
            cursor = await connection.execute(
                "update catalogue.proxy_budget_cycles set kill_switch = true "
                "where provider = %(provider)s and lifecycle = 'active' returning id",
                {"provider": pspec.name},
            )
        elif action == "revoke":
            async with connection.transaction():
                cursor = await connection.execute(
                    "update catalogue.proxy_budget_cycles set kill_switch = true "
                    "where provider = %(provider)s and lifecycle = 'active' returning id",
                    {"provider": pspec.name},
                )
                await connection.execute(
                    """update catalogue.proxy_reservations
                          set revocation_requested = true, state = 'revocation_requested'
                        where provider = %(provider)s and state = 'active'""",
                    {"provider": pspec.name},
                )
        else:
            if not request.app.state.settings.proxy_enabled:
                data = {"error": "deployment_proxy_disabled"}
                await finish_mutation(
                    connection, mutation, actor, f"kill_switch.{action}", status=409,
                    data=data, state="failed", error_code="deployment_proxy_disabled",
                )
                return problem(409, "Unsafe proxy state", "deployment proxy support is disabled")
            cursor = await connection.execute(
                """
                update catalogue.proxy_budget_cycles c set kill_switch = false
                 where provider = %(provider)s and lifecycle = 'active'
                   and reconciliation_ok and reconciled_at > now() - interval '20 minutes'
                   and greatest(provider_reported_bytes, application_bytes) < operational_bytes
                   and exists(select 1 from catalogue.proxy_profiles
                               where provider = %(provider)s and enabled)
                returning id
                """,
                {"provider": pspec.name},
            )
        row = await cursor.fetchone()
        if row is None:
            data = {"error": "safety_gates_failed"}
            await finish_mutation(
                connection, mutation, actor, f"kill_switch.{action}", status=409,
                data=data, state="failed", error_code="safety_gates_failed",
            )
            return problem(409, "Unsafe proxy state", "no active cycle satisfies the safety gates")
        data = {"kill_switch": action != "clear", "revocation_requested": action == "revoke"}
        await finish_mutation(connection, mutation, actor, f"kill_switch.{action}", status=202, data=data)
        await events.emit(
            connection,
            events.Topic.PROXY,
            "proxy.kill_switch_changed",
            payload={**data, "provider": pspec.name},
        )
    return payload(data, status=202)


async def pilot_action(request: Request) -> Response:
    actor = await actor_for(
        request, admin=True, recent=request.path_params["action"] == "start"
    )
    if isinstance(actor, Response):
        return actor
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    enabled = request.path_params["action"] == "start"
    if request.path_params["action"] not in {"start", "stop"}:
        return problem(404, "Not Found", "unknown pilot action")
    if enabled:
        body = await request.json()
        if not isinstance(body, dict) or body.get("confirmation") != "START PAID PROXY PILOT":
            return problem(422, "Confirmation required", "confirmation must equal 'START PAID PROXY PILOT'")
    async with request.app.state.pool.connection() as connection:
        if enabled:
            cursor = await connection.execute(
                """update catalogue.proxy_budget_cycles set pilot_active = true
                     where provider = %(provider)s and lifecycle = 'active'
                       and reconciliation_ok and reconciled_at > now() - interval '20 minutes'
                     returning id""",
                {"provider": pspec.name},
            )
        else:
            cursor = await connection.execute(
                """update catalogue.proxy_budget_cycles set pilot_active = false
                     where provider = %(provider)s and lifecycle = 'active' returning id""",
                {"provider": pspec.name},
            )
        if await cursor.fetchone() is None:
            return problem(409, "Unsafe proxy state", "pilot state could not be changed")
    return payload({"pilot_active": enabled}, status=202)


async def propose_cycle(request: Request) -> Response:
    actor = await actor_for(request, admin=True)
    if isinstance(actor, Response):
        return actor
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    provider = provider_for(request)
    if isinstance(provider, Response):
        return provider
    if not pspec.proposes_cycles:
        # The subscription's dates become cycle_start/cycle_end, and
        # (provider, cycle_start) is the conflict key. A provider that sells a
        # prepaid balance has no such window to offer, so the cycle has to be
        # opened by hand with dates an operator states.
        return problem(
            409, "Cycle cannot be proposed",
            f"{pspec.label} has no dated subscription; open the cycle with explicit dates",
        )
    try:
        subscription = await provider.subscription()
    except ProviderError as error:
        return problem(502, "Provider unavailable", str(error))
    if (
        pspec.max_reconciliation_window is not None
        and subscription.valid_until - subscription.valid_from
        > pspec.max_reconciliation_window
    ):
        return problem(
            409,
            "Provider usage window unsupported",
            f"{pspec.label} cannot reconcile the complete subscription window",
        )
    if subscription.traffic_limit_bytes is None:
        return problem(
            409,
            "Finite cycle ceiling required",
            "set an explicit finite operator ceiling before proposing this cycle",
        )
    async with request.app.state.pool.connection() as connection:
        cursor = await connection.execute(
            """
            insert into catalogue.proxy_budget_cycles
                   (provider, cycle_start, cycle_end, purchased_bytes, operational_bytes,
                    daily_bytes, pilot_bytes, unmanaged_allocation_bytes, lifecycle,
                    provider_resource_id, proposed_at, proposed_by, kill_switch)
            values (%(provider)s, %(start)s, %(end)s, %(purchased)s,
                    least(%(purchased)s, 2400000000), 80000000, 300000000, 0,
                    'proposed', %(resource)s, now(), %(actor)s, true)
            on conflict (provider, cycle_start) do update
              set provider_resource_id = excluded.provider_resource_id,
                  proposed_at = now(), proposed_by = excluded.proposed_by
              where catalogue.proxy_budget_cycles.lifecycle = 'proposed'
            returning *
            """,
            {
                "provider": pspec.name,
                "start": subscription.valid_from, "end": subscription.valid_until,
                "purchased": subscription.traffic_limit_bytes,
                "resource": subscription.provider_resource_id, "actor": actor.id,
            },
        )
        row = await cursor.fetchone()
        if row is None:
            return problem(409, "Cycle conflict", "that provider cycle is already active or closed")
    return payload({"cycle": row}, status=201)


async def open_or_close_cycle(request: Request) -> Response:
    actor = await actor_for(request, admin=True, recent=True)
    if isinstance(actor, Response):
        return actor
    cycle_id = _uuid(request)
    if cycle_id is None:
        return problem(400, "Bad Request", "cycle id must be a UUID")
    action = request.path_params["action"]
    body = await request.json()
    if not isinstance(body, dict):
        return problem(400, "Bad Request", "a JSON object is required")
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    confirmation = body.get("confirmation")
    # Derived from the provider, so typing "OPEN DECODO CYCLE" cannot open an
    # IPRoyal cycle.
    expected = pspec.confirmation(action)
    if confirmation != expected:
        return problem(422, "Confirmation required", f"confirmation must equal {expected!r}")
    provider = provider_for(request)
    if isinstance(provider, Response):
        return provider
    if action == "close":
        async with request.app.state.pool.connection() as reconcile_connection:
            try:
                await reconcile_now(
                    reconcile_connection,
                    provider,
                    reason=f"cycle_close:{actor.id}",
                    spec=pspec,
                )
            except (ProviderError, RuntimeError) as error:
                return problem(502, "Final reconciliation failed", str(error))
    async with request.app.state.pool.connection() as connection, connection.transaction():
        await connection.execute(
            "select pg_advisory_xact_lock(hashtext(%(lock)s))", {"lock": pspec.lock_key}
        )
        if action == "open":
            proposed = await connection.execute(
                """select * from catalogue.proxy_budget_cycles
                    where id = %(id)s and provider = %(provider)s for update""",
                {"id": cycle_id, "provider": pspec.name},
            )
            row = await proposed.fetchone()
            if row is None or row["lifecycle"] != "proposed":
                return problem(409, "Cycle conflict", "cycle is not proposed")
            if not _cycle_confirmation_matches(body, row):
                return problem(422, "Confirmation mismatch", "confirmed cycle values do not match proposal")
            await connection.execute(
                """update catalogue.proxy_budget_cycles
                      set lifecycle = 'closed', closed_at = now(), closed_by = %(actor)s
                    where provider = %(provider)s and lifecycle = 'active' and cycle_end <= now()""",
                {"actor": actor.id, "provider": pspec.name},
            )
            changed = await connection.execute(
                """update catalogue.proxy_budget_cycles
                      set lifecycle = 'active', confirmed_at = now(), confirmed_by = %(actor)s,
                          opened_at = now(), opened_by = %(actor)s, kill_switch = true,
                          reconciliation_ok = false
                    where id = %(id)s and provider = %(provider)s
                      and lifecycle = 'proposed' returning *""",
                {"id": cycle_id, "actor": actor.id, "provider": pspec.name},
            )
        elif action == "close":
            changed = await connection.execute(
                """update catalogue.proxy_budget_cycles c
                      set lifecycle = 'closed', closed_at = now(), closed_by = %(actor)s,
                          kill_switch = true
                    where id = %(id)s and provider = %(provider)s
                      and lifecycle = 'active' and cycle_end <= now()
                      and not exists(select 1 from catalogue.proxy_reservations r
                        where r.provider = c.provider and r.cycle_start = c.cycle_start
                          and r.state in ('active', 'revocation_requested'))
                    returning *""",
                {"id": cycle_id, "actor": actor.id, "provider": pspec.name},
            )
        else:
            return problem(404, "Not Found", "unknown cycle action")
        row = await changed.fetchone()
        if row is None:
            return problem(409, "Cycle conflict", "cycle safety conditions are not satisfied")
        await events.emit(connection, events.Topic.PROXY, f"proxy.cycle_{action}ed", payload={"id": str(cycle_id)})
    if action == "open":
        async with request.app.state.pool.connection() as reconcile_connection:
            try:
                await reconcile_now(
                    reconcile_connection,
                    provider,
                    reason=f"cycle_open:{actor.id}",
                    spec=pspec,
                )
            except (ProviderError, RuntimeError) as error:
                return problem(
                    502, "Initial reconciliation failed",
                    f"cycle remains kill-switched: {error}",
                )
    return payload({"cycle": row}, status=202)


async def create_route(request: Request) -> Response:
    actor = await actor_for(request, admin=True)
    if isinstance(actor, Response):
        return actor
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    try:
        body = await request.json()
        profile_id = UUID(str(body["profile_id"]))
        protocol = str(body.get("protocol", "http"))
        country = str(body["country"]).upper() if body.get("country") else None
        session_mode = str(body.get("session_mode", "random"))
        session_minutes = int(body.get("session_minutes", 30))
        max_bytes = int(body.get("max_bytes", 25_000_000))
    except (KeyError, TypeError, ValueError):
        return problem(422, "Invalid route", "route fields are malformed")
    if protocol not in {"http", "https", "socks5"} or session_mode not in {"random", "sticky"}:
        return problem(422, "Invalid route", "unsupported protocol or session mode")
    if country and (len(country) != 2 or not country.isalpha()):
        return problem(422, "Invalid route", "country must be a two-letter code")
    if not 1 <= session_minutes <= 1440 or not 1 <= max_bytes <= 25_000_000:
        return problem(422, "Invalid route", "route duration or byte maximum is unsafe")
    async with request.app.state.pool.connection() as connection:
        cursor = await connection.execute(
            """
            insert into catalogue.proxy_routes
                   (provider, label, profile_id, protocol, country, state, city, session_mode,
                    session_minutes, max_bytes, pilot, enabled, created_by, updated_by)
            select p.provider, %(label)s, p.id, %(protocol)s, %(country)s, %(state)s, %(city)s,
                   %(mode)s, %(minutes)s, %(bytes)s, %(pilot)s, %(enabled)s,
                   %(actor)s, %(actor)s
              from catalogue.proxy_profiles p
             where p.id = %(profile)s and p.provider = %(provider)s
               and p.enabled and p.lifecycle = 'enabled'
            returning *
            """,
            {
                "provider": pspec.name,
                "label": str(body.get("label", "New route"))[:200], "profile": profile_id,
                "protocol": protocol, "country": country, "state": body.get("state"),
                "city": body.get("city"), "mode": session_mode, "minutes": session_minutes,
                "bytes": max_bytes, "pilot": bool(body.get("pilot", True)),
                "enabled": bool(body.get("enabled", False)), "actor": actor.id,
            },
        )
        row = await cursor.fetchone()
        if row is None:
            return problem(409, "Profile unavailable", "profile is not enabled")
        await events.emit(connection, events.Topic.PROXY, "proxy.route_changed", payload={"id": str(row["id"])})
    return payload({"route": row}, status=201)


async def update_or_delete_route(request: Request) -> Response:
    actor = await actor_for(request, admin=True)
    if isinstance(actor, Response):
        return actor
    route_id = _uuid(request)
    if route_id is None:
        return problem(400, "Bad Request", "route id must be a UUID")
    async with request.app.state.pool.connection() as connection, connection.transaction():
        if request.method == "DELETE":
            cursor = await connection.execute(
                """update catalogue.proxy_routes r
                      set enabled = false, retired_at = now(), updated_at = now(), updated_by = %(actor)s
                    where id = %(id)s and retired_at is null
                      and not exists(select 1 from catalogue.source_proxy_policies s
                        where s.route_id = r.id and s.policy <> 'never')
                    returning *""",
                {"id": route_id, "actor": actor.id},
            )
        else:
            body = await request.json()
            if not isinstance(body, dict):
                return problem(400, "Bad Request", "a JSON object is required")
            max_bytes = int(body.get("max_bytes", 25_000_000))
            minutes = int(body.get("session_minutes", 30))
            if not 1 <= max_bytes <= 25_000_000 or not 1 <= minutes <= 1440:
                return problem(422, "Invalid route", "route limits are unsafe")
            cursor = await connection.execute(
                """update catalogue.proxy_routes
                      set label = %(label)s, country = %(country)s, state = %(state)s,
                          city = %(city)s, session_mode = %(mode)s,
                          session_minutes = %(minutes)s, max_bytes = %(bytes)s,
                          pilot = %(pilot)s, enabled = %(enabled)s,
                          updated_at = now(), updated_by = %(actor)s
                    where id = %(id)s and retired_at is null returning *""",
                {
                    "id": route_id, "label": str(body.get("label", "Route"))[:200],
                    "country": str(body["country"]).upper() if body.get("country") else None,
                    "state": body.get("state"), "city": body.get("city"),
                    "mode": body.get("session_mode", "random"), "minutes": minutes,
                    "bytes": max_bytes, "pilot": bool(body.get("pilot", True)),
                    "enabled": bool(body.get("enabled", False)), "actor": actor.id,
                },
            )
        row = await cursor.fetchone()
        if row is None:
            return problem(409, "Route conflict", "route is missing, retired, or referenced")
        if request.method == "DELETE":
            await connection.execute(
                """update catalogue.source_proxy_policies set policy = 'never', route_id = null,
                          disabled_at = now(), revision = revision + 1,
                          updated_at = now(), updated_by = %(actor)s
                    where route_id = %(id)s""",
                {"id": route_id, "actor": actor.id},
            )
        await events.emit(connection, events.Topic.PROXY, "proxy.route_changed", payload={"id": str(route_id)})
    return payload({"route": row}, status=202)


def _writes_enabled(request: Request) -> Response | None:
    if not request.app.state.settings.proxy_mutations_enabled:
        return problem(409, "Provider mutations disabled", "enable the deployment mutation gate first")
    if request.app.state.settings.proxy_secret_file is None:
        return problem(503, "Secret store unavailable", "proxy secret volume is not configured")
    return None


async def create_profile(request: Request) -> Response:
    actor = await actor_for(request, admin=True, recent=True)
    if isinstance(actor, Response):
        return actor
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    if not pspec.can_provision_subusers:
        return problem(
            409,
            "Profile provisioning unsupported",
            f"{pspec.label} does not support caller-chosen proxy credentials",
        )
    if denied := _writes_enabled(request):
        return denied
    provider = provider_for(request)
    if isinstance(provider, Response):
        return provider
    try:
        body = await request.json()
        logical_name = str(body["logical_name"])
        display_name = str(body.get("display_name") or logical_name)
        allocation = int(body["allocated_bytes"])
        provider_limit = int(body.get("provider_traffic_limit_bytes", allocation))
    except (KeyError, TypeError, ValueError):
        return problem(422, "Invalid profile", "profile name and allocation are required")
    if body.get("confirmation") != f"CREATE {logical_name}":
        return problem(422, "Confirmation required", f"confirmation must equal 'CREATE {logical_name}'")
    if not logical_name or len(logical_name) > 64 or any(
        char not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for char in logical_name
    ):
        return problem(422, "Invalid profile", "logical name must use lower-case letters, numbers, _ or -")
    if allocation <= 0 or provider_limit <= 0 or provider_limit > allocation:
        return problem(422, "Invalid profile", "provider limit must fit a positive allocation")
    username = f"catalogue_{secrets.token_hex(8)}"
    password = generate_password()
    async with request.app.state.pool.connection() as connection:
        try:
            mutation = await begin_mutation(
                connection, actor, "profile.create", request.headers.get("idempotency-key")
            )
        except (ValueError, RuntimeError) as error:
            return problem(409, "Idempotency conflict", str(error))
        if mutation.replay_status:
            return payload(mutation.replay_data or {}, status=mutation.replay_status)
        async with connection.transaction():
            await connection.execute(
                "select pg_advisory_xact_lock(hashtext(%(lock)s))", {"lock": pspec.lock_key}
            )
            cycle_cursor = await connection.execute(
                "select * from catalogue.proxy_budget_cycles where provider = %(provider)s "
                "and lifecycle = 'active' for update",
                {"provider": pspec.name},
            )
            cycle = await cycle_cursor.fetchone()
            if cycle is None:
                data = {"error": "no_active_cycle", "operation_id": str(mutation.operation_id)}
                await finish_mutation(
                    connection, mutation, actor, "profile.create", status=409, data=data,
                    state="failed", error_code="no_active_cycle",
                )
                return problem(409, "No active cycle", "open and reconcile a billing cycle first")
            allocated_cursor = await connection.execute(
                """select coalesce(sum(allocated_bytes), 0) as allocated
                     from catalogue.proxy_profile_allocations
                    where provider = %(provider)s and cycle_start = %(start)s""",
                {"start": cycle["cycle_start"], "provider": pspec.name},
            )
            allocated_row = await allocated_cursor.fetchone()
            allocated = int(allocated_row["allocated"] if allocated_row else 0)
            if allocated + allocation + cycle["unmanaged_allocation_bytes"] > cycle["operational_bytes"]:
                data = {"error": "allocation_exhausted", "operation_id": str(mutation.operation_id)}
                await finish_mutation(
                    connection, mutation, actor, "profile.create", status=409, data=data,
                    state="failed", error_code="allocation_exhausted",
                )
                return problem(409, "Allocation exhausted", "profile allocation exceeds operational capacity")
            profile_cursor = await connection.execute(
                """
                insert into catalogue.proxy_profiles
                       (provider, logical_name, display_name, lifecycle, created_by, updated_by)
                values (%(provider)s, %(name)s, %(display)s, 'pending', %(actor)s, %(actor)s)
                returning *
                """,
                {
                    "provider": pspec.name,
                    "name": logical_name,
                    "display": display_name[:200],
                    "actor": actor.id,
                },
            )
            profile = await profile_cursor.fetchone()
            assert profile is not None
            await connection.execute(
                """insert into catalogue.proxy_profile_allocations
                           (provider, cycle_start, profile_id, allocated_bytes, updated_by)
                    values (%(provider)s, %(start)s, %(profile)s, %(bytes)s, %(actor)s)""",
                {
                    "provider": pspec.name,
                    "start": cycle["cycle_start"], "profile": profile["id"],
                    "bytes": allocation, "actor": actor.id,
                },
            )
        try:
            subuser = await provider.create_subuser(
                username=username,
                password=password,
                traffic_limit_bytes=provider_limit,
                traffic_count_from=cycle["cycle_start"],
            )
        except ProviderError as error:
            state = "ambiguous" if error.ambiguous else "failed"
            data = {"error": error.code, "operation_id": str(mutation.operation_id)}
            if not error.ambiguous:
                # A conclusive provider rejection created no external resource.
                # Remove the pending local intent and its allocation so an
                # operator can correct and retry the same logical profile
                # without leaking the bounded pilot budget.  Ambiguous network
                # outcomes deliberately remain pending for reconciliation.
                async with connection.transaction():
                    await connection.execute(
                        "delete from catalogue.proxy_profile_allocations "
                        "where profile_id = %(id)s", {"id": profile["id"]},
                    )
                    await connection.execute(
                        "delete from catalogue.proxy_profiles where id = %(id)s "
                        "and lifecycle = 'pending' and provider_resource_id is null",
                        {"id": profile["id"]},
                    )
            await finish_mutation(
                connection, mutation, actor, "profile.create", status=502, data=data,
                state=state, resource_type="profile", resource_id=str(profile["id"]),
                error_code=error.code,
            )
            return payload(data, status=502)
        try:
            generation = ProfileSecretStore(
                request.app.state.settings.proxy_secret_file
            ).install(logical_name, username=username, password=password)
        except (OSError, RuntimeError, TypeError, ValueError):
            await connection.execute(
                """update catalogue.proxy_profiles
                      set lifecycle = 'provider_changed_local_failed', enabled = false,
                          provider_resource_id = %(resource)s, updated_at = now(), updated_by = %(actor)s
                    where id = %(id)s""",
                {"resource": subuser.id, "actor": actor.id, "id": profile["id"]},
            )
            await connection.execute(
                "update catalogue.proxy_budget_cycles set kill_switch = true "
                "where provider = %(provider)s and lifecycle = 'active'",
                {"provider": pspec.name},
            )
            data = {"error": "provider_changed_local_failed", "operation_id": str(mutation.operation_id)}
            await finish_mutation(
                connection, mutation, actor, "profile.create", status=500, data=data,
                state="provider_changed_local_failed", resource_type="profile",
                resource_id=str(profile["id"]), error_code="secret_install_failed",
            )
            return payload(data, status=500)
        updated = await connection.execute(
            """update catalogue.proxy_profiles
                  set provider_resource_id = %(resource)s, username_mask = %(mask)s,
                      username_fingerprint = %(fingerprint)s,
                      provider_traffic_limit_bytes = %(limit)s, auto_disable = %(auto)s,
                      enabled = true, lifecycle = 'enabled', secret_generation = %(generation)s,
                      secret_installed_at = now(), provider_observed_at = now(),
                      updated_at = now(), updated_by = %(actor)s
                where id = %(id)s returning *""",
            {
                "resource": subuser.id, "mask": mask_username(username),
                "fingerprint": username_fingerprint(username), "limit": provider_limit,
                "generation": generation, "auto": subuser.auto_disable,
                "actor": actor.id, "id": profile["id"],
            },
        )
        row = await updated.fetchone()
        data = {"profile": row, "operation_id": str(mutation.operation_id)}
        await finish_mutation(
            connection, mutation, actor, "profile.create", status=201, data=data,
            resource_type="profile", resource_id=str(profile["id"]),
        )
        await events.emit(connection, events.Topic.PROXY, "proxy.profile_changed", payload={"id": str(profile["id"])})
    return payload(data, status=201)


async def refresh_profiles(request: Request) -> Response:
    actor = await actor_for(request, admin=True)
    if isinstance(actor, Response):
        return actor
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    provider = provider_for(request)
    if isinstance(provider, Response):
        return provider
    try:
        users = await provider.list_subusers()
    except ProviderError as error:
        return problem(502, "Provider unavailable", str(error))
    by_id = {user.id: user for user in users}
    async with request.app.state.pool.connection() as connection, connection.transaction():
        cursor = await connection.execute(
            "select * from catalogue.proxy_profiles where provider = %(provider)s for update",
            {"provider": pspec.name},
        )
        rows = await cursor.fetchall()
        drift: list[dict[str, Any]] = []
        for row in rows:
            resource = row["provider_resource_id"]
            user = by_id.get(resource) if resource else None
            if user is None:
                if resource:
                    drift.append({"profile_id": str(row["id"]), "kind": "missing_at_provider"})
                continue
            await connection.execute(
                """update catalogue.proxy_profiles
                      set username_mask = %(mask)s, username_fingerprint = %(fingerprint)s,
                          provider_traffic_limit_bytes = coalesce(%(limit)s, provider_traffic_limit_bytes),
                          auto_disable = %(auto)s, provider_observed_at = now(), updated_at = now(),
                          enabled = enabled and %(active)s
                    where id = %(id)s""",
                {
                    "mask": mask_username(user.username), "fingerprint": username_fingerprint(user.username),
                    "limit": user.traffic_limit_bytes, "auto": user.auto_disable,
                    "active": user.status == "active", "id": row["id"],
                },
            )
            if user.traffic_limit_bytes is not None and row["provider_traffic_limit_bytes"] != user.traffic_limit_bytes:
                drift.append({"profile_id": str(row["id"]), "kind": "limit", "provider": user.traffic_limit_bytes})
        await events.emit(connection, events.Topic.PROXY, "proxy.profile_changed", payload={"refreshed": len(rows)})
    return payload({"refreshed": len(rows), "drift": drift}, status=202)


async def profile_action(request: Request) -> Response:
    actor = await actor_for(request, admin=True, recent=True)
    if isinstance(actor, Response):
        return actor
    if denied := _writes_enabled(request):
        return denied
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    provider = provider_for(request)
    if isinstance(provider, Response):
        return provider
    profile_id = _uuid(request)
    if profile_id is None:
        return problem(400, "Bad Request", "profile id must be a UUID")
    action = request.path_params["action"]
    body = await request.json()
    if not isinstance(body, dict):
        return problem(400, "Bad Request", "a JSON object is required")
    if action == "rotate" and not pspec.can_provision_subusers:
        return problem(
            409,
            "Profile rotation unsupported",
            f"{pspec.label} does not support caller-chosen proxy credentials",
        )
    if action == "disable" and not pspec.has_subuser_status:
        return problem(
            409,
            "Profile disable unsupported",
            f"{pspec.label} does not expose a sub-user status control",
        )
    async with request.app.state.pool.connection() as connection:
        cursor = await connection.execute(
            """select p.*, a.allocated_bytes, a.cycle_start
                 from catalogue.proxy_profiles p
                 left join catalogue.proxy_budget_cycles c on c.provider = p.provider and c.lifecycle = 'active'
                 left join catalogue.proxy_profile_allocations a on a.profile_id = p.id
                  and a.provider = c.provider and a.cycle_start = c.cycle_start
                where p.id = %(id)s and p.provider = %(provider)s""",
            {"id": profile_id, "provider": pspec.name},
        )
        profile = await cursor.fetchone()
        if profile is None or not profile["provider_resource_id"]:
            return problem(404, "Not Found", "profile was not found")
        mutation_action = f"profile.{action}"
        try:
            mutation = await begin_mutation(
                connection, actor, mutation_action, request.headers.get("idempotency-key")
            )
        except (ValueError, RuntimeError) as error:
            return problem(409, "Idempotency conflict", str(error))
        if mutation.replay_status:
            return payload(mutation.replay_data or {}, status=mutation.replay_status)
        active_cursor = await connection.execute(
            """select count(*) as count from catalogue.proxy_reservations
                where provider = %(provider)s and profile_id = %(id)s
                  and state in ('active', 'revocation_requested')""",
            {"id": profile_id, "provider": pspec.name},
        )
        active_row = await active_cursor.fetchone()
        active = int(active_row["count"] if active_row else 0)

        expected_confirmation = {
            "rotate": f"ROTATE {profile['logical_name']}",
            "disable": f"DISABLE {profile['logical_name']}",
            "limit": f"SET LIMIT {body.get('provider_traffic_limit_bytes')} FOR {profile['logical_name']}",
            "allocation": f"SET ALLOCATION {body.get('allocated_bytes')} FOR {profile['logical_name']}",
        }.get(action)
        if expected_confirmation and body.get("confirmation") != expected_confirmation:
            return await mutation_problem(
                connection, mutation, actor, mutation_action, profile_id, status=422,
                title="Confirmation required",
                detail=f"confirmation must equal {expected_confirmation!r}",
                code="confirmation_required",
            )

        if action == "rotate":
            mode = body.get("mode", "drain")
            if mode not in {"drain", "blue-green"}:
                return await mutation_problem(
                    connection, mutation, actor, mutation_action, profile_id, status=422,
                    title="Invalid rotation", detail="mode must be drain or blue-green",
                    code="invalid_rotation",
                )
            if mode == "blue-green" and not pspec.has_subuser_status:
                return await mutation_problem(
                    connection,
                    mutation,
                    actor,
                    mutation_action,
                    profile_id,
                    status=409,
                    title="Blue-green rotation unsupported",
                    detail=f"{pspec.label} cannot disable the retired provider profile",
                    code="provider_status_unsupported",
                )
            if mode == "drain" and active:
                await connection.execute(
                    "update catalogue.proxy_profiles set lifecycle = 'draining', enabled = false, pending_action = 'rotate', "
                    "updated_at = now(), updated_by = %(actor)s where id = %(id)s",
                    {"id": profile_id, "actor": actor.id},
                )
                return await mutation_payload(
                    connection, mutation, actor, mutation_action, profile_id,
                    {"profile_id": profile_id, "rotation": "draining"},
                )
            password = generate_password()
            if mode == "drain":
                try:
                    await provider.update_subuser(profile["provider_resource_id"], password=password)
                    generation = ProfileSecretStore(request.app.state.settings.proxy_secret_file).install(
                        profile["logical_name"], username=_username_from_store(
                            request.app.state.settings.proxy_secret_file, profile["logical_name"]
                        ), password=password,
                    )
                except ProviderError as error:
                    return await mutation_problem(
                        connection, mutation, actor, mutation_action, profile_id, status=502,
                        title="Rotation failed", detail=str(error), code=error.code,
                        state="ambiguous" if error.ambiguous else "failed",
                    )
                except (OSError, RuntimeError, TypeError, ValueError):
                    await _credential_install_failure(connection, profile_id, actor.id, pspec.name)
                    return await mutation_problem(
                        connection, mutation, actor, mutation_action, profile_id, status=500,
                        title="Rotation failed", detail="provider changed but local install failed",
                        code="provider_changed_local_failed", state="provider_changed_local_failed",
                    )
                await connection.execute(
                    """update catalogue.proxy_profiles set enabled = true, lifecycle = 'enabled',
                              secret_generation = %(generation)s, secret_installed_at = now(),
                              updated_at = now(), updated_by = %(actor)s where id = %(id)s""",
                    {"generation": generation, "actor": actor.id, "id": profile_id},
                )
                return await mutation_payload(
                    connection, mutation, actor, mutation_action, profile_id,
                    {"profile_id": profile_id, "rotation": "completed"},
                )
            return await _blue_green_rotation(
                request, connection, provider, actor, profile, active, mutation
            )

        if action == "disable":
            await connection.execute(
                "update catalogue.proxy_profiles set enabled = false, lifecycle = 'draining', pending_action = 'disable', "
                "updated_at = now(), updated_by = %(actor)s where id = %(id)s",
                {"id": profile_id, "actor": actor.id},
            )
            await connection.execute(
                """update catalogue.source_proxy_policies s set policy = 'never', route_id = null,
                          disabled_at = now(), revision = revision + 1, updated_at = now(), updated_by = %(actor)s
                    from catalogue.proxy_routes r where r.profile_id = %(id)s and s.route_id = r.id""",
                {"id": profile_id, "actor": actor.id},
            )
            if not active:
                try:
                    await provider.update_subuser(profile["provider_resource_id"], status="disabled")
                except ProviderError as error:
                    return await mutation_problem(
                        connection, mutation, actor, mutation_action, profile_id, status=502,
                        title="Provider disable failed", detail=str(error), code=error.code,
                        state="ambiguous" if error.ambiguous else "failed",
                    )
                await connection.execute(
                    "update catalogue.proxy_profiles set lifecycle = 'disabled', pending_action = null where id = %(id)s",
                    {"id": profile_id},
                )
            return await mutation_payload(
                connection, mutation, actor, mutation_action, profile_id,
                {"profile_id": profile_id, "state": "draining" if active else "disabled"},
            )

        if action == "limit":
            limit = int(body.get("provider_traffic_limit_bytes", 0))
            if limit <= 0 or profile["allocated_bytes"] is None or limit > profile["allocated_bytes"]:
                return await mutation_problem(
                    connection, mutation, actor, mutation_action, profile_id, status=422,
                    title="Invalid limit", detail="provider limit must fit the active allocation",
                    code="invalid_limit",
                )
            try:
                user = await provider.update_subuser(profile["provider_resource_id"], traffic_limit_bytes=limit)
            except ProviderError as error:
                return await mutation_problem(
                    connection, mutation, actor, mutation_action, profile_id, status=502,
                    title="Provider update failed", detail=str(error), code=error.code,
                    state="ambiguous" if error.ambiguous else "failed",
                )
            await connection.execute(
                "update catalogue.proxy_profiles set provider_traffic_limit_bytes = %(limit)s, "
                "auto_disable = %(auto)s, provider_observed_at = now(), updated_at = now(), "
                "updated_by = %(actor)s where id = %(id)s",
                {
                    "limit": user.traffic_limit_bytes or limit, "auto": user.auto_disable,
                    "actor": actor.id, "id": profile_id,
                },
            )
            return await mutation_payload(
                connection, mutation, actor, mutation_action, profile_id,
                {"profile_id": profile_id, "provider_traffic_limit_bytes": limit},
            )

        if action == "allocation":
            allocation = int(body.get("allocated_bytes", -1))
            if allocation < 0 or allocation < int(profile["provider_traffic_limit_bytes"] or 0):
                return await mutation_problem(
                    connection, mutation, actor, mutation_action, profile_id, status=422,
                    title="Invalid allocation", detail="allocation cannot be below provider limit",
                    code="invalid_allocation",
                )
            if active and allocation < int(profile["allocated_bytes"] or 0):
                return await mutation_problem(
                    connection, mutation, actor, mutation_action, profile_id, status=409,
                    title="Profile active", detail="drain reservations before lowering allocation",
                    code="profile_active",
                )
            async with connection.transaction():
                await connection.execute(
                    "select pg_advisory_xact_lock(hashtext(%(lock)s))",
                    {"lock": pspec.lock_key},
                )
                capacity_cursor = await connection.execute(
                    """select c.operational_bytes, c.unmanaged_allocation_bytes,
                              coalesce(sum(a.allocated_bytes) filter (where a.profile_id <> %(id)s), 0) as other
                         from catalogue.proxy_budget_cycles c
                         left join catalogue.proxy_profile_allocations a
                           on a.provider = c.provider and a.cycle_start = c.cycle_start
                        where c.provider = %(provider)s and c.lifecycle = 'active'
                        group by c.operational_bytes, c.unmanaged_allocation_bytes""",
                    {"id": profile_id, "provider": pspec.name},
                )
                capacity = await capacity_cursor.fetchone()
                if capacity is None or capacity["other"] + capacity["unmanaged_allocation_bytes"] + allocation > capacity["operational_bytes"]:
                    return await mutation_problem(
                        connection, mutation, actor, mutation_action, profile_id, status=409,
                        title="Allocation exhausted", detail="allocation exceeds operational capacity",
                        code="allocation_exhausted",
                    )
                await connection.execute(
                    """update catalogue.proxy_profile_allocations set allocated_bytes = %(bytes)s,
                              updated_at = now(), updated_by = %(actor)s
                        where provider = %(provider)s and profile_id = %(id)s
                          and cycle_start = %(start)s""",
                    {
                        "provider": pspec.name,
                        "bytes": allocation,
                        "actor": actor.id,
                        "id": profile_id,
                        "start": profile["cycle_start"],
                    },
                )
            return await mutation_payload(
                connection, mutation, actor, mutation_action, profile_id,
                {"profile_id": profile_id, "allocated_bytes": allocation},
            )
        return await mutation_problem(
            connection, mutation, actor, mutation_action, profile_id, status=404,
            title="Not Found", detail="unknown profile action", code="unknown_action",
        )


async def retire_profile(request: Request) -> Response:
    actor = await actor_for(request, admin=True, recent=True)
    if isinstance(actor, Response):
        return actor
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    if not pspec.has_subuser_status:
        return problem(
            409,
            "Profile retirement unsupported",
            f"{pspec.label} does not expose the status transition required for retirement",
        )
    if denied := _writes_enabled(request):
        return denied
    profile_id = _uuid(request)
    if profile_id is None:
        return problem(400, "Bad Request", "profile id must be a UUID")
    async with request.app.state.pool.connection() as connection, connection.transaction():
        cursor = await connection.execute(
            """select id, logical_name from catalogue.proxy_profiles
                where id = %(id)s and provider = %(provider)s
                  and retired_at is null for update""",
            {"id": profile_id, "provider": pspec.name},
        )
        profile = await cursor.fetchone()
        if profile is None:
            return problem(404, "Not Found", "profile was not found")
        body = await request.json()
        expected = f"RETIRE {profile['logical_name']}"
        if not isinstance(body, dict) or body.get("confirmation") != expected:
            return problem(422, "Confirmation required", f"confirmation must equal {expected!r}")
        await connection.execute(
            """update catalogue.proxy_profiles
                  set enabled = false, lifecycle = 'draining', pending_action = 'retire',
                      updated_at = now(), updated_by = %(actor)s
                where id = %(id)s""",
            {"id": profile_id, "actor": actor.id},
        )
        await connection.execute(
            """update catalogue.proxy_routes set enabled = false, retired_at = coalesce(retired_at, now()),
                      updated_at = now(), updated_by = %(actor)s where profile_id = %(id)s""",
            {"id": profile_id, "actor": actor.id},
        )
        await connection.execute(
            """update catalogue.source_proxy_policies s set policy = 'never', route_id = null,
                      disabled_at = now(), revision = revision + 1, updated_at = now(), updated_by = %(actor)s
                  from catalogue.proxy_routes r where r.profile_id = %(id)s and s.route_id = r.id""",
            {"id": profile_id, "actor": actor.id},
        )
        await events.emit(
            connection, events.Topic.PROXY, "proxy.profile_changed",
            payload={"id": str(profile_id), "state": "draining", "pending_action": "retire"},
        )
    return payload({"profile_id": profile_id, "state": "draining"}, status=202)


def _username_from_store(path: Any, logical_name: str) -> str:
    raw = ProfileSecretStore(path).read_raw()
    profile = raw.get(logical_name)
    if not profile or not profile.get("username"):
        raise RuntimeError("installed profile username is missing")
    return str(profile["username"])


async def _credential_install_failure(
    connection: Any, profile_id: UUID, actor: str, provider: str
) -> None:
    await connection.execute(
        """update catalogue.proxy_profiles set lifecycle = 'provider_changed_local_failed',
                  enabled = false, updated_at = now(), updated_by = %(actor)s where id = %(id)s""",
        {"id": profile_id, "actor": actor},
    )
    await connection.execute(
        "update catalogue.proxy_budget_cycles set kill_switch = true "
        "where provider = %(provider)s and lifecycle = 'active'",
        {"provider": provider},
    )


async def _blue_green_rotation(
    request: Request, connection: Any, provider: Any, actor: Actor,
    profile: dict[str, Any], active: int, mutation: Mutation,
) -> Response:
    action = "profile.rotate"
    allocation = int(profile["allocated_bytes"] or 0)
    minimum = min(allocation, 25_000_000)
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    async with connection.transaction():
        await connection.execute(
            "select pg_advisory_xact_lock(hashtext(%(lock)s))", {"lock": pspec.lock_key}
        )
        capacity_cursor = await connection.execute(
            """select c.operational_bytes - c.unmanaged_allocation_bytes
                    - coalesce(sum(a.allocated_bytes), 0)
                    - coalesce((select sum(x.temporary_allocation_bytes)
                                  from catalogue.proxy_profile_retirements x
                                 where x.state in ('creating', 'draining', 'finalizing')), 0) as spare
             from catalogue.proxy_budget_cycles c
             left join catalogue.proxy_profile_allocations a
               on a.provider = c.provider and a.cycle_start = c.cycle_start
            where c.provider = %(provider)s and c.lifecycle = 'active'
            group by c.operational_bytes, c.unmanaged_allocation_bytes""",
            {"provider": pspec.name},
        )
        capacity = await capacity_cursor.fetchone()
        if capacity is None or int(capacity["spare"]) < minimum:
            return await mutation_problem(
                connection, mutation, actor, action, profile["id"], status=409,
                title="Rotation capacity unavailable",
                detail="blue-green rotation needs spare provider allocation",
                code="rotation_capacity_unavailable",
            )
        pending_cursor = await connection.execute(
            """insert into catalogue.proxy_profile_retirements
                       (profile_id, old_provider_resource_id, replacement_resource_id,
                        target_limit_bytes, temporary_allocation_bytes,
                        old_secret_generation, state)
                values (%(profile)s, %(old)s, %(pending)s, %(target)s, %(temporary)s,
                        %(generation)s, 'creating') returning id""",
            {
                "profile": profile["id"], "old": profile["provider_resource_id"],
                "pending": f"pending:{mutation.operation_id}", "target": allocation,
                "temporary": minimum, "generation": profile["secret_generation"],
            },
        )
        pending = await pending_cursor.fetchone()
        assert pending is not None
    username = f"catalogue_{secrets.token_hex(8)}"
    password = generate_password()
    try:
        replacement = await provider.create_subuser(
            username=username, password=password, traffic_limit_bytes=minimum,
            traffic_count_from=profile["cycle_start"],
        )
        generation = ProfileSecretStore(request.app.state.settings.proxy_secret_file).install(
            profile["logical_name"], username=username, password=password,
        )
    except ProviderError as error:
        await connection.execute(
            "update catalogue.proxy_profile_retirements set state = 'failed', error_code = %(error)s "
            "where id = %(id)s",
            {"id": pending["id"], "error": error.code},
        )
        return await mutation_problem(
            connection, mutation, actor, action, profile["id"], status=502,
            title="Rotation failed", detail=str(error), code=error.code,
            state="ambiguous" if error.ambiguous else "failed",
        )
    except (OSError, RuntimeError, TypeError, ValueError):
        await connection.execute(
            "update catalogue.proxy_profile_retirements set state = 'failed', "
            "error_code = 'secret_install_failed' where id = %(id)s",
            {"id": pending["id"]},
        )
        await _credential_install_failure(connection, profile["id"], actor.id, pspec.name)
        return await mutation_problem(
            connection, mutation, actor, action, profile["id"], status=500,
            title="Rotation failed", detail="replacement exists but local install failed",
            code="provider_changed_local_failed", state="provider_changed_local_failed",
        )
    async with connection.transaction():
        await connection.execute(
            """update catalogue.proxy_profile_retirements
                  set replacement_resource_id = %(new)s, state = 'draining', error_code = null
                where id = %(id)s""",
            {"new": replacement.id, "id": pending["id"]},
        )
        await connection.execute(
            """update catalogue.proxy_profiles
                  set provider_resource_id = %(new)s, username_mask = %(mask)s,
                      username_fingerprint = %(fingerprint)s,
                      provider_traffic_limit_bytes = %(limit)s,
                      secret_generation = %(generation)s, secret_installed_at = now(),
                      lifecycle = 'enabled', enabled = true, updated_at = now(), updated_by = %(actor)s
                where id = %(id)s""",
            {
                "new": replacement.id, "mask": mask_username(username),
                "fingerprint": username_fingerprint(username), "limit": minimum,
                "generation": generation, "actor": actor.id, "id": profile["id"],
            },
        )
    return await mutation_payload(
        connection, mutation, actor, action, profile["id"],
        {"profile_id": profile["id"], "rotation": "draining_old_generation", "active": active},
    )


async def apply_source_policy(
    request: Request, connection: Any, source_id: str, body: Any,
) -> dict[str, Any] | Response:
    actor = await actor_for(request, admin=True, recent=True)
    if isinstance(actor, Response):
        return actor
    if not isinstance(body, dict):
        return problem(422, "Invalid proxy policy", "proxy must be an object")
    try:
        policy = str(body.get("policy", "never"))
        route_id = UUID(str(body["route_id"])) if body.get("route_id") else None
        max_bytes = int(body.get("max_megabytes", 25)) * 1_000_000
        pilot = bool(body.get("pilot", True))
    except (TypeError, ValueError):
        return problem(422, "Invalid proxy policy", "proxy policy fields are malformed")
    if policy not in {"never", "fallback", "always"}:
        return problem(422, "Invalid proxy policy", "policy must be never, fallback, or always")
    config = request.app.state.sources.get(source_id)
    if config is None:
        return problem(404, "Not Found", "source does not exist")
    if policy != "never" and not config.proxy_eligible:
        return problem(409, "Source ineligible", "source is not checked-in as proxy eligible")
    if policy == "fallback":
        failure_cursor = await connection.execute(
            """select exists(
                 select 1 from catalogue.jobs
                  where source_id = %(source)s and state = 'failed'
                    and finished_at > now() - interval '30 days'
                    and coalesce(error, '') ~* '(403|407|429|blocked|captcha|challenge|access denied|geo)'
               ) as eligible""",
            {"source": source_id},
        )
        failure = await failure_cursor.fetchone()
        if not failure or not failure["eligible"]:
            return problem(
                409, "Classified failure required",
                "fallback requires a recent blocking, throttling, challenge, or geo failure",
            )
    route: dict[str, Any] | None = None
    if policy != "never":
        route_cursor = await connection.execute(
            """select r.*, p.enabled as profile_enabled, p.lifecycle as profile_lifecycle
                 from catalogue.proxy_routes r join catalogue.proxy_profiles p on p.id = r.profile_id
                where r.id = %(id)s and r.enabled and r.retired_at is null for update of r, p""",
            {"id": route_id},
        )
        route = await route_cursor.fetchone()
        if route is None or not route["profile_enabled"] or route["profile_lifecycle"] != "enabled":
            return problem(409, "Route unavailable", "route and profile must be enabled")
        if max_bytes > route["max_bytes"]:
            return problem(422, "Invalid proxy policy", "source maximum exceeds the route maximum")
    operation_id = UUID(int=actor.nonce.int)
    before_cursor = await connection.execute(
        "select * from catalogue.source_proxy_policies where source_id = %(source)s for update",
        {"source": source_id},
    )
    before = await before_cursor.fetchone()
    if policy == "always" and (
        before is None
        or before["evidence_state"] != "promoted"
        or int(before["evidence_count"]) < 3
    ):
        return problem(
            409, "Pilot evidence required", "always requires three promoted pilot runs"
        )
    try:
        changed = await connection.execute(
            """
            insert into catalogue.source_proxy_policies
                   (source_id, policy, route_id, max_bytes, pilot, evidence_count,
                    evidence_state, enabled_at, disabled_at, updated_by)
            values (%(source)s, %(policy)s, %(route)s, %(bytes)s, %(pilot)s,
                    case when %(policy)s = 'always' then 3 else 0 end,
                    case when %(policy)s = 'always' then 'promoted'
                         when %(policy)s = 'fallback' then 'eligible' else 'unproven' end,
                    case when %(policy)s <> 'never' then now() end,
                    case when %(policy)s = 'never' then now() end, %(actor)s)
            on conflict (source_id) do update
              set policy = excluded.policy, route_id = excluded.route_id,
                  max_bytes = excluded.max_bytes, pilot = excluded.pilot,
                  evidence_state = case
                    when catalogue.source_proxy_policies.evidence_state = 'promoted'
                      then 'promoted'
                    when excluded.policy = 'fallback' then 'eligible'
                    else catalogue.source_proxy_policies.evidence_state end,
                  enabled_at = case when excluded.policy <> 'never'
                    then coalesce(catalogue.source_proxy_policies.enabled_at, now())
                    else catalogue.source_proxy_policies.enabled_at end,
                  disabled_at = case when excluded.policy = 'never' then now() else null end,
                  revision = catalogue.source_proxy_policies.revision + 1,
                  updated_at = now(), updated_by = excluded.updated_by
            returning *
            """,
            {
                "source": source_id, "policy": policy, "route": route_id,
                "bytes": max_bytes, "pilot": pilot, "actor": actor.id,
            },
        )
    except Exception as error:
        if policy == "always":
            return problem(409, "Pilot evidence required", "always requires three promoted pilot runs")
        raise error
    row = await changed.fetchone()
    assert row is not None
    if policy == "never":
        await connection.execute(
            """update catalogue.jobs set proxy_snapshot = '{}'::jsonb
                where source_id = %(source)s and state = 'queued'""",
            {"source": source_id},
        )
    await append_audit(
        connection, actor, operation_id, "source_policy.update", "source", source_id,
        "succeeded", success=True, before=before, after=row,
    )
    await events.emit(
        connection, events.Topic.PROXY, "proxy.source_policy_changed",
        source_id=source_id, payload={"policy": policy, "route_id": str(route_id) if route_id else None},
    )
    return row


def _probe_identity(parsed: dict[str, Any]) -> dict[str, str]:
    """Normalize both flat and nested Decodo IP-check response shapes."""
    nested = parsed.get("proxy")
    proxy = nested if isinstance(nested, dict) else {}
    ip_value = parsed.get("ip") or proxy.get("ip")
    country = (
        parsed.get("country")
        or parsed.get("country_code")
        or proxy.get("country")
        or proxy.get("country_code")
    )
    result: dict[str, str] = {}
    if ip_value:
        result["exit_ip"] = str(ipaddress.ip_address(str(ip_value)))
    if isinstance(country, str) and len(country) == 2:
        result["exit_country"] = country.upper()
    return result


async def probe_route(request: Request) -> Response:
    actor = await actor_for(request, admin=True, recent=True)
    if isinstance(actor, Response):
        return actor
    if not request.app.state.settings.proxy_paid_probe_enabled:
        return problem(409, "Paid probes disabled", "enable the paid-probe deployment gate first")
    if not request.app.state.settings.proxy_enabled:
        return problem(409, "Proxy disabled", "deployment proxy support is disabled")
    secret_path = request.app.state.settings.proxy_secret_file
    if secret_path is None:
        return problem(503, "Secret store unavailable", "proxy secret volume is not configured")
    route_id = _uuid(request)
    if route_id is None:
        return problem(400, "Bad Request", "route id must be a UUID")
    body = await request.json()
    if not isinstance(body, dict) or body.get("confirmation") != "SPEND UP TO 1.1 MB":
        return problem(422, "Confirmation required", "confirmation must equal 'SPEND UP TO 1.1 MB'")
    pspec = provider_spec(request)
    if isinstance(pspec, Response):
        return pspec
    # A probe spends real traffic, so an unknown IP-check endpoint is refused
    # rather than guessed. Configure one per provider to enable probing.
    probe_url = request.app.state.settings.proxy_provider_probe_urls.get(
        pspec.name, pspec.probe_url
    )
    if not probe_url:
        return problem(
            409, "Probe unavailable",
            f"no IP-check endpoint is configured for {pspec.label}",
        )
    app_cap = 1_000_000
    reservation_cap = 1_100_000
    async with request.app.state.pool.connection() as connection:
        route_cursor = await connection.execute(
            """select r.*, p.logical_name as profile, p.enabled as profile_enabled,
                      p.lifecycle as profile_lifecycle
                 from catalogue.proxy_routes r join catalogue.proxy_profiles p on p.id = r.profile_id
                where r.id = %(id)s and r.enabled and r.retired_at is null""",
            {"id": route_id},
        )
        route = await route_cursor.fetchone()
        if route is None or not route["profile_enabled"] or route["profile_lifecycle"] != "enabled":
            return problem(409, "Route unavailable", "route and profile must be enabled")
        if route["max_bytes"] < reservation_cap:
            return problem(409, "Route budget too small", "route cannot hold the probe safety envelope")
        rate_cursor = await connection.execute(
            """select count(*) as recent,
                      max(requested_at) as latest
                 from catalogue.proxy_probes
                where actor = %(actor)s and profile_id = %(profile)s
                  and requested_at > now() - interval '1 hour'""",
            {"actor": actor.id, "profile": route["profile_id"]},
        )
        rate = await rate_cursor.fetchone()
        if rate and (
            int(rate["recent"]) >= 3
            or (rate["latest"] is not None
                and (datetime.now(rate["latest"].tzinfo) - rate["latest"]).total_seconds() < 30)
        ):
            return problem(429, "Probe rate limited", "wait before spending another probe envelope")
        request_id = actor.nonce
        probe_cursor = await connection.execute(
            """insert into catalogue.proxy_probes
                       (route_id, profile_id, protocol, actor, request_id)
                values (%(route)s, %(profile)s, %(protocol)s, %(actor)s, %(request)s)
                returning id""",
            {
                "route": route_id, "profile": route["profile_id"],
                "protocol": route["protocol"], "actor": actor.id, "request": request_id,
            },
        )
        probe = await probe_cursor.fetchone()
        assert probe is not None
        profiles_by_name = load_profiles(secret_path)
        profile = profiles_by_name.get(route["profile"])
        if profile is None:
            return problem(409, "Secret unavailable", "route profile is not installed")
        try:
            reservation_id = await reserve(
                connection, probe_id=probe["id"], profile=route["profile"],
                profile_id=route["profile_id"], route_id=route_id,
                requested_bytes=reservation_cap, pilot=True,
                secret_generation=profile.generation,
            )
        except ProxyDenied as error:
            metrics.proxy_probe("budget_denied")
            await connection.execute(
                "update catalogue.proxy_probes set state = 'failed', completed_at = now(), "
                "error_category = 'budget_denied' where id = %(id)s",
                {"id": probe["id"]},
            )
            return problem(409, "Probe denied", str(error))
        lease = ProxyLease.build(
            reservation_id, probe["id"], profile, route["country"],
            route["session_minutes"], reservation_cap, route["protocol"],
        )
        started = time.monotonic()
        result: dict[str, Any] = {}
        error_category: str | None = None
        try:
            async with (
                httpx.AsyncClient(proxy=lease.url, timeout=30, follow_redirects=False) as client,
                client.stream("GET", probe_url) as response,
            ):
                    response.raise_for_status()
                    content_length = int(response.headers.get("content-length", "0") or 0)
                    if content_length > app_cap:
                        raise ValueError("probe response Content-Length exceeds application cap")
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(body) + len(chunk) > app_cap:
                            raise ValueError("probe response exceeded application cap")
                        body.extend(chunk)
                    lease.account(2_048, len(body) + 8_192)
                    parsed = json.loads(body)
                    if not isinstance(parsed, dict):
                        raise ValueError("IP-check response was not an object")
                    result.update(_probe_identity(parsed))
        except (httpx.HTTPError, ValueError, json.JSONDecodeError) as error:
            error_category = "probe_failed"
            result["error"] = str(error)[:300]
        finally:
            await close_reservation(connection, lease)
        latency = int((time.monotonic() - started) * 1000)
        state = "failed" if error_category else "succeeded"
        metrics.proxy_probe(state)
        await connection.execute(
            """update catalogue.proxy_probes
                  set state = %(state)s, completed_at = now(), error_category = %(error)s,
                      estimated_bytes = %(bytes)s, provider_requests = %(requests)s,
                      exit_country = %(country)s, exit_ip = %(ip)s::inet,
                      exit_ip_expires_at = case when %(ip)s::inet is null
                           then null else now() + interval '7 days' end,
                      latency_ms = %(latency)s
                where id = %(id)s""",
            {
                "state": state, "error": error_category, "bytes": lease.used_bytes,
                "requests": lease.requests, "country": result.get("exit_country"),
                "ip": result.get("exit_ip"), "latency": latency, "id": probe["id"],
            },
        )
        await events.emit(
            connection, events.Topic.PROXY, "proxy.probe_finished",
            payload={"probe_id": str(probe["id"]), "state": state},
        )
    return payload(
        {
            "probe_id": probe["id"], "reservation_id": reservation_id,
            "state": state, "application_bytes": lease.used_bytes,
            "reserved_bytes": reservation_cap, "latency_ms": latency, **result,
        },
        status=200 if state == "succeeded" else 502,
    )
