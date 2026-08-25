"""Creating and finishing runs and jobs.

A run is the unit an operator asks for; a job is one source's share of it. They
are separate rows because everything interesting — retry, cancel, pause, "which
source is stuck" — happens per source, and because a run that is 79 sources
succeeded and one failed is a real and common outcome that a single status
cannot express.

`finish_job` carries the piece of that which is easy to get wrong under
concurrency: when the last sibling of a run reaches a terminal state, *some*
worker has to notice and close the run. Doing it without a lock means two
finishing workers race and publish different summaries.
"""

from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.ops import events, outbox
from mb_ceramics_catalogue.scrapers import adapter_capabilities

LOGGER = obs.get_logger("catalogue.runs")

Connection = psycopg.AsyncConnection[dict[str, Any]]

#: States a job can no longer leave.
TERMINAL = ("succeeded", "degraded", "failed", "cancelled", "skipped")

def host_of(url: str) -> str:
    return urlparse(url).netloc


async def create_run(
    connection: Connection,
    *,
    kind: str = "manual",
    requested_by: str | None = None,
    params: dict[str, Any] | None = None,
    schedule_id: str | None = None,
    scheduled_fire_at: Any = None,
) -> UUID | None:
    """Insert a run. Returns None when a scheduled occurrence already exists.

    The `on conflict do nothing` is what makes firing a schedule idempotent
    rather than merely mutually exclusive: a leader that dies after committing
    leaves the next tick a no-op for that fire time instead of a second run.
    """
    row = await _one(
        connection,
        """
        insert into catalogue.runs (kind, schedule_id, scheduled_fire_at, requested_by, params, status)
        values (%(kind)s, %(schedule_id)s, %(fire_at)s, %(requested_by)s, %(params)s, 'queued')
        on conflict (schedule_id, scheduled_fire_at) where schedule_id is not null
        do nothing
        returning id
        """,
        {
            "kind": kind,
            "schedule_id": schedule_id,
            "fire_at": scheduled_fire_at,
            "requested_by": requested_by,
            "params": Jsonb(params or {}),
        },
    )
    if row is None:
        return None
    run_id = row["id"]
    await events.emit(
        connection,
        events.Topic.RUN,
        "run.created",
        run_id=run_id,
        payload={"kind": kind, "requested_by": requested_by, "params": params or {}},
    )
    return run_id  # type: ignore[no-any-return]


async def create_jobs(
    connection: Connection,
    run_id: UUID,
    sources: SourcesFile,
    selected: list[str],
    *,
    priority: int = 100,
    max_attempts: int = 3,
) -> dict[str, UUID]:
    """Fan a run out into one job per source. Returns source -> job id.

    Sources that `catalogue.source_settings` disables are left out entirely; a
    paused one gets a job that simply will not be claimed, which is the
    difference between "not part of future runs" and "not right now".

    Jobs are staggered by host so the fan-out is not a thundering herd: eighty
    jobs created at once on eighty hosts is fine, but two sources on one host
    both starting at 03:00:00 is exactly what `catalogue.hosts` exists to stop,
    and giving the second a later `scheduled_for` avoids the contention rather
    than resolving it.
    """
    settings = {
        row["source_id"]: row
        for row in await _all(
            connection,
            "select source_id, enabled, paused from catalogue.source_settings "
            "where source_id = any(%(names)s)",
            {"names": selected},
        )
    }
    policy_rows = await _all(
        connection,
        """
        select p.source_id, p.policy, p.max_bytes, p.pilot, p.revision,
               r.id as route_id, r.protocol, r.country, r.state as route_state,
               r.city, r.session_mode, r.session_minutes, r.max_bytes as route_max_bytes,
               r.enabled as route_enabled,
               f.id as profile_id, f.provider, f.logical_name as profile,
               f.secret_generation,
               f.enabled as profile_enabled, f.lifecycle as profile_lifecycle
          from catalogue.source_proxy_policies p
          left join catalogue.proxy_routes r on r.id = p.route_id
          left join catalogue.proxy_profiles f on f.id = r.profile_id
         where p.source_id = any(%(names)s)
        """,
        {"names": selected},
    )
    policies = {row["source_id"]: row for row in policy_rows}

    seen_per_host: dict[str, int] = {}
    created: dict[str, UUID] = {}

    for name in selected:
        setting = settings.get(name, {})
        if setting.get("enabled") is False:
            LOGGER.info("job.skipped", source=name, reason="disabled in source_settings")
            continue

        config = sources[name]
        proxy_snapshot: dict[str, Any] = {}
        policy = policies.get(name)
        if policy and policy["policy"] != "never":
            if not config.proxy_eligible:
                raise ValueError(f"source {name!r} is not checked-in as proxy eligible")
            if not policy["route_enabled"] or not policy["profile_enabled"]:
                raise ValueError(f"source {name!r} references a disabled proxy route/profile")
            if policy["profile_lifecycle"] != "enabled":
                raise ValueError(f"source {name!r} proxy profile is not operational")
            proxy_snapshot = {
                "policy": policy["policy"],
                "policy_revision": policy["revision"],
                "route_id": str(policy["route_id"]),
                "profile_id": str(policy["profile_id"]),
                "provider": policy["provider"],
                "profile": policy["profile"],
                "secret_generation": policy["secret_generation"],
                "protocol": policy["protocol"],
                "country": policy["country"] or (config.country.upper() if config.country else None),
                "state": policy["route_state"],
                "city": policy["city"],
                "session_mode": policy["session_mode"],
                "session_minutes": policy["session_minutes"],
                "max_bytes": min(int(policy["max_bytes"]), int(policy["route_max_bytes"])),
                "pilot": bool(policy["pilot"]),
            }
        host = host_of(config.url)
        position = seen_per_host.get(host, 0)
        seen_per_host[host] = position + 1

        row = await _one(
            connection,
            """
            insert into catalogue.jobs
                   (run_id, source_id, host, state, priority, max_attempts, requires,
                    scheduled_for, proxy_snapshot)
            values (%(run_id)s, %(source_id)s, %(host)s, 'queued', %(priority)s, %(max_attempts)s,
                    %(requires)s, now() + make_interval(secs => %(stagger)s), %(proxy_snapshot)s)
            on conflict (run_id, source_id) do nothing
            returning id
            """,
            {
                "run_id": run_id,
                "source_id": name,
                "host": host,
                "priority": priority,
                "max_attempts": max_attempts,
                "requires": list(
                    adapter_capabilities(config.scraper).required_worker_capabilities
                ),
                "proxy_snapshot": Jsonb(proxy_snapshot),
                # Only sources sharing a host are spread out; the common case of
                # one source per host gets no delay at all.
                "stagger": position * 30,
            },
        )
        if row is not None:
            created[name] = row["id"]
            await outbox.enqueue_job(connection, row["id"])

    await events.emit(
        connection, events.Topic.RUN, "run.planned", run_id=run_id,
        payload={"jobs": len(created), "sources": sorted(created)},
    )
    if not created:
        # Every selected source was disabled, so there is no job whose finish
        # would ever close this run. Without this it sits in `queued` for ever
        # — the same hang that makes `paused` the wrong lever for taking a
        # source out of runs, arriving through an empty selection instead.
        await close_run_if_done(connection, run_id)
    return created


async def start_run(connection: Connection, run_id: UUID) -> bool:
    """Move a run to `running` the first time one of its jobs starts.

    Idempotent, and the edge follows the update rather than the call: every
    worker that picks up a job calls this, so an unconditional emit would put
    eighty `run.started` events on the stream for one run that started once.
    """
    changed = await _execute(
        connection,
        "update catalogue.runs set status = 'running', started_at = coalesce(started_at, now()) "
        "where id = %(id)s and status = 'queued'",
        {"id": run_id},
    )
    if not changed:
        return False
    await events.emit(connection, events.Topic.RUN, "run.started", run_id=run_id)
    return True


async def record_job_start(
    connection: Connection, job_id: UUID, *, trace_id: str | None = None
) -> None:
    await _execute(
        connection,
        "update catalogue.jobs set started_at = coalesce(started_at, now()), "
        "trace_id = coalesce(%(trace)s, trace_id) where id = %(id)s",
        {"id": job_id, "trace": trace_id},
    )
    await _execute(
        connection,
        "insert into catalogue.job_progress (job_id, phase) values (%(id)s, 'discovering') "
        "on conflict (job_id) do update set phase = 'discovering', updated_at = now()",
        {"id": job_id},
    )


async def finish_job(
    connection: Connection,
    job_id: UUID,
    *,
    state: str,
    summary: dict[str, Any] | None = None,
    error: str | None = None,
    artifact: Any = None,
    execution_token: UUID | None = None,
) -> dict[str, Any] | None:
    """Put a job into a terminal state and close its run if it was the last.

    Both halves are in one transaction, and the run row is locked before its
    siblings are counted. Without that lock two workers finishing their last
    jobs at the same moment both see "no non-terminal siblings", both compute a
    summary, and the run's recorded outcome is whichever committed second.
    """
    if state not in TERMINAL:
        raise ValueError(f"{state!r} is not a terminal job state")

    async with connection.transaction():
        await connection.execute(
            "select pg_advisory_xact_lock(hashtextextended(%(id)s::text, 0))", {"id": job_id}
        )
        row = await _one(
            connection,
            """
            update catalogue.jobs
               set state = %(state)s,
                   finished_at = now(),
                   error = %(error)s,
                   summary = coalesce(%(summary)s, summary),
                   artifact_path = coalesce(%(path)s, artifact_path),
                   artifact_sha256 = coalesce(%(sha)s, artifact_sha256),
                   artifact_size = coalesce(%(size)s, artifact_size),
                   lease_owner = null,
                   lease_expires_at = null,
                   execution_token = null,
                   pause_requested = false,
                   resume_without_attempt = false
             where id = %(id)s
               and (%(token)s::uuid is null or execution_token = %(token)s)
            returning run_id, source_id, attempt, max_attempts
            """,
            {
                "id": job_id,
                "token": execution_token,
                "state": state,
                "error": error,
                "summary": Jsonb(summary) if summary is not None else None,
                "path": str(artifact.path) if artifact else None,
                "sha": getattr(artifact, "sha256", None) or None,
                "size": getattr(artifact, "size", None),
            },
        )
        if row is None:
            return None

        # Progress is a live level while a crawl runs, but the terminal summary
        # is authoritative. A scraper that gathers records concurrently can
        # finish between progress flushes; without this final write the UI may
        # show a succeeded 2,555-row load as `records=0, phase=discovering`.
        final = summary or {}
        await _execute(
            connection,
            """
            insert into catalogue.job_progress
                   (job_id, phase, discovered, records, requests, rendered_pages,
                    error_count, truncated, in_flight)
            values (%(id)s, %(phase)s, coalesce(%(discovered)s, 0),
                    coalesce(%(records)s, 0), coalesce(%(requests)s, 0),
                    coalesce(%(rendered)s, 0), coalesce(%(errors)s, 0),
                    coalesce(%(truncated)s, false), '[]'::jsonb)
            on conflict (job_id) do update
              set phase = excluded.phase,
                  discovered = coalesce(%(discovered)s, catalogue.job_progress.discovered),
                  records = coalesce(%(records)s, catalogue.job_progress.records),
                  requests = coalesce(%(requests)s, catalogue.job_progress.requests),
                  rendered_pages = coalesce(%(rendered)s, catalogue.job_progress.rendered_pages),
                  error_count = coalesce(%(errors)s, catalogue.job_progress.error_count),
                  truncated = coalesce(%(truncated)s, catalogue.job_progress.truncated),
                  in_flight = '[]'::jsonb,
                  updated_at = now()
            """,
            {
                "id": job_id,
                "phase": state,
                "discovered": final.get("discovered"),
                "records": final.get("records"),
                "requests": final.get("requests"),
                "rendered": final.get("rendered_pages"),
                "errors": final.get("error_count"),
                "truncated": final.get("truncated"),
            },
        )

        # Release any host slot this job still holds, before the run is
        # considered: a finished job must never keep a shop's slot occupied.
        await _execute(
            connection,
            "update catalogue.host_leases set job_id = null, leased_by = null, leased_until = null, "
            "execution_token = null where job_id = %(id)s "
            "and (%(token)s::uuid is null or execution_token = %(token)s)",
            {"id": job_id, "token": execution_token},
        )

        await events.emit(
            connection,
            events.Topic.JOB,
            f"job.{state}",
            run_id=row["run_id"],
            job_id=job_id,
            source_id=row["source_id"],
            payload={
                "state": state,
                "records": (summary or {}).get("records"),
                "error": error,
                "attempt": row["attempt"],
            },
        )
        return await _close_run_if_done(connection, row["run_id"])


async def _close_run_if_done(connection: Connection, run_id: UUID) -> dict[str, Any] | None:
    """Lock the run and finish it when no non-terminal sibling remains."""
    await _one(connection, "select id from catalogue.runs where id = %(id)s for update", {"id": run_id})

    outstanding = await _one(
        connection,
        "select count(*) as remaining from catalogue.jobs "
        "where run_id = %(id)s "
        "and state not in ('succeeded','degraded','failed','cancelled','skipped')",
        {"id": run_id},
    )
    if outstanding is None or outstanding["remaining"]:
        return None

    tally = await _one(
        connection,
        """
        select count(*) filter (where state = 'succeeded')          as succeeded,
               count(*) filter (where state = 'degraded')           as degraded,
               count(*) filter (where state = 'failed')             as failed,
               count(*) filter (where state = 'cancelled')          as cancelled,
               count(*) filter (where state = 'skipped')            as skipped,
               coalesce(sum((summary->>'records')::bigint), 0)      as records,
               coalesce(sum((summary->>'requests')::bigint), 0)     as requests
          from catalogue.jobs where run_id = %(id)s
        """,
        {"id": run_id},
    )
    assert tally is not None
    summary = {key: int(value) for key, value in tally.items()}

    # `degraded` rather than `failed` when some sources worked: a run that
    # collected 79 of 80 catalogues did not fail, and calling it failed is how
    # an alert stops being believed.
    usable = summary["succeeded"] + summary["degraded"]
    if summary["cancelled"] and not usable and not summary["failed"]:
        status = "cancelled"
    elif usable and (summary["degraded"] or summary["failed"] or summary["cancelled"]):
        status = "degraded"
    elif summary["failed"]:
        status = "failed"
    else:
        status = "complete"

    await _execute(
        connection,
        "update catalogue.runs set status = %(status)s, finished_at = now(), summary = %(summary)s "
        "where id = %(id)s",
        {"id": run_id, "status": status, "summary": Jsonb(summary)},
    )
    await events.emit(
        connection,
        events.Topic.RUN,
        f"run.{status}",
        run_id=run_id,
        payload=summary,
    )
    LOGGER.info("run.finished", run_id=str(run_id), status=status, **summary)
    await _promote_canonicals(connection, run_id)
    return {"status": status, **summary}


async def close_run_if_done(connection: Connection, run_id: UUID) -> dict[str, Any] | None:
    """Close a run after an operator terminally cancels its last queued job."""
    return await _close_run_if_done(connection, run_id)


async def _promote_canonicals(connection: Connection, run_id: UUID) -> None:
    """Fold what this run collected into the cross-supplier product identities.

    A loaded source is only half the point. `catalogue.promote_canonical_products`
    is what turns "les-cousins sells PRAI" and "sio-2 sells PRAI" into one
    product two shops quote a price for, and until it runs the catalogue holds
    the same clay as several unrelated rows. It existed and nothing called it,
    so the join it builds was as old as whenever somebody last ran it by hand.

    Here, rather than after each load: it is one statement over the whole table
    either way, so eighty jobs would do the same three seconds of work eighty
    times to reach the same answer. Once per run, when the last job lands, is
    the first moment the answer can be complete.

    A failure is logged and swallowed. The run's own outcome is already
    committed above and is not in question because a derived table could not be
    rebuilt — and a database that predates the promotion schema has no such
    function at all, which must not turn every run into a failed one.
    """
    eligible = await _one(
        connection,
        """select exists (
               select 1 from catalogue.jobs j
                where j.run_id = %(run)s and j.state in ('succeeded', 'degraded')
                  and (exists (
                    select 1 from catalogue.job_datasets d
                     where d.job_id = j.id
                       and d.dataset in ('ceramics.catalogue_item.v2',
                                         'ceramics.catalogue_identity.v2')
                       and d.state in ('succeeded', 'degraded') and d.complete
                  ) or (not exists (
                    select 1 from catalogue.job_datasets d where d.job_id = j.id
                  ) and (j.state = 'succeeded' or (
                    j.summary->>'write_status' = 'replaced'
                    and not coalesce((j.summary->>'truncated')::boolean, false)
                    and not coalesce((j.summary->>'interrupted')::boolean, false)
                  ))))
             ) as eligible""",
        {"run": run_id},
    )
    if eligible is None or not eligible["eligible"]:
        return
    started = time.monotonic()
    try:
        # A savepoint, since this runs inside the transaction that finished the
        # job. Without it a failed statement would poison that transaction and
        # take the run's own closure down with it.
        async with connection.transaction():
            row = await _one(connection, "select * from catalogue.promote_canonical_products()")
    except psycopg.Error as error:
        LOGGER.warning(
            "run.promotion_failed", run_id=str(run_id), error=str(error).splitlines()[0]
        )
        return

    if row is None:
        return
    result: dict[str, Any] = {key: int(value) for key, value in row.items()}
    result["promotion_seconds"] = round(time.monotonic() - started, 6)
    await events.emit(
        connection,
        events.Topic.RUN,
        "run.promoted",
        run_id=run_id,
        payload=result,
    )
    LOGGER.info("run.promoted", run_id=str(run_id), **result)


async def _one(connection: Connection, sql: str, params: Any = None) -> dict[str, Any] | None:
    async with connection.cursor() as cursor:
        await cursor.execute(sql, params)
        return await cursor.fetchone()


async def _all(connection: Connection, sql: str, params: Any = None) -> list[dict[str, Any]]:
    async with connection.cursor() as cursor:
        await cursor.execute(sql, params)
        return await cursor.fetchall()


async def _execute(connection: Connection, sql: str, params: Any = None) -> int:
    async with connection.cursor() as cursor:
        await cursor.execute(sql, params)
        return cursor.rowcount
