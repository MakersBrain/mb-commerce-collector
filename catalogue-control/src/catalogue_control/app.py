"""`catalogue-control`: the write path, kept out of the read API on purpose.

`catalogue-service`'s docstring states its contract plainly: "there is no write
path here at all rather than a write path behind a permission." Putting run
control in a separate service preserves that property rather than arguing with
it — a tenant reading the catalogue cannot reach a cancel-run endpoint, because
it is not in the service they can reach.

Every `/v1` route, including the stream, requires a bearer token. `/health` and
`/metrics` are the only exemptions, and the service is additionally not
published on the host — which is defence in depth, not the authentication
boundary.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import psycopg
from mb_ceramics_catalogue.config.settings import CrawlParams
from mb_ceramics_catalogue.config.sources import SourcesFile, default_path
from mb_ceramics_catalogue.observability.http import RequestTelemetry
from mb_ceramics_catalogue.ops import events, outbox, runs
from mb_ceramics_catalogue.ops import schedule as scheduling
from starlette.applications import Starlette
from starlette.datastructures import Headers
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, PlainTextResponse, Response, StreamingResponse
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send

from catalogue_control import proxy_api, queries
from catalogue_control.auth import load_public_keys
from catalogue_control.broker import Broker, Subscriber, parse_topics
from catalogue_control.proxy_control import ReconciliationScheduler
from catalogue_control.settings import Settings
from catalogue_control.telemetry import get_logger, render_metrics

LOGGER = get_logger("catalogue.control")

#: SSE keepalive. Without it an idle proxy closes the connection after a minute
#: and the browser reconnects in a loop it never reports.
KEEPALIVE_SECONDS = 15


def problem(status: int, title: str, detail: str | None = None, kind: str = "about:blank") -> Response:
    """RFC 9457 `application/problem+json`.

    `{"error": "..."}` was an undocumented string that every client had to
    guess at. One error schema, referenced from every operation, is the thing
    that makes the generated spec worth reading (§10.3).
    """
    body: dict[str, Any] = {"type": kind, "title": title, "status": status}
    if detail:
        body["detail"] = detail
    return JSONResponse(body, status_code=status, media_type="application/problem+json")


class BearerToken:
    """Default deny on `/v1`, including the stream.

    Raw ASGI middleware rather than `BaseHTTPMiddleware`, and that is not a
    style choice: `BaseHTTPMiddleware` consumes a `StreamingResponse` through an
    intermediate task and does not forward it incrementally, so wrapping the app
    in one makes `/v1/events` deliver nothing until the stream ends — which, for
    a stream designed to stay open for hours, means never.

    The symptom is the worst kind: every JSON route works, the SSE handshake
    succeeds, and the browser simply sits there receiving no events.
    """

    def __init__(self, app: ASGIApp, token: str) -> None:
        self.app = app
        self.token = token

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not scope["path"].startswith("/v1"):
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        header = headers.get("authorization", "")
        supplied = header[7:] if header.lower().startswith("bearer ") else ""
        # A query-string token is deliberately not accepted, even though
        # `EventSource` cannot set headers: it would land in access logs and in
        # `Referer` headers. The explorer proxies the stream through a
        # SvelteKit route that adds the header server-side instead (§6.5).
        if not supplied or not _constant_time_equal(supplied, self.token):
            response = problem(401, "Unauthorized", "a bearer token is required on /v1 routes")
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)


def _constant_time_equal(left: str, right: str) -> bool:
    import hmac

    return hmac.compare_digest(left.encode(), right.encode())


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


async def health(request: Request) -> Response:
    try:
        async with request.app.state.pool.connection() as connection:
            await connection.execute("select 1")
    except psycopg.Error:
        return JSONResponse({"status": "unavailable"}, status_code=503)
    return JSONResponse({"status": "ok"})


async def metrics_endpoint(request: Request) -> Response:
    """Prometheus text. Live gauges are read from the database, not cached.

    The queue depth is authoritative in Postgres and cheap to count; keeping a
    process-local copy would mean each replica reported a different number.
    """
    from mb_ceramics_catalogue.observability import metrics as instruments

    try:
        async with request.app.state.pool.connection() as connection:
            states = await queries.all_rows(connection, queries.METRIC_JOB_STATES)
            oldest = await queries.one(connection, queries.METRIC_QUEUE_OLDEST)
            workers = await queries.one(connection, queries.METRIC_WORKERS)
            outbox_stats = await queries.one(connection, queries.QUEUE_OUTBOX_STATS) or {}
            source_history = await queries.all_rows(connection, queries.SOURCES)
            schedules = await queries.all_rows(connection, queries.SCHEDULES)
            instruments.jobs_snapshot({str(row["state"]): int(row["n"]) for row in states})
            instruments.queue_oldest_age(float((oldest or {}).get("seconds") or 0))
            instruments.workers_snapshot(
                int((workers or {}).get("healthy") or 0), int((workers or {}).get("lost") or 0)
            )
            instruments.REGISTRY.replace_gauge(
                "catalogue_queue_outbox_pending",
                "Committed outbox rows not yet confirmed published.",
                [(float(outbox_stats.get("pending") or 0), {})],
            )
            instruments.REGISTRY.replace_gauge(
                "catalogue_queue_outbox_oldest_age_seconds",
                "Age of the oldest unpublished outbox row.",
                [(float(outbox_stats.get("oldest_age_seconds") or 0), {})],
            )
            instruments.sources_snapshot(
                _source_metric_snapshot(request.app.state.sources, source_history, schedules)
            )
    except psycopg.Error:
        LOGGER.warning("metrics.snapshot_failed", exc_info=True)
        return PlainTextResponse(
            "database metric snapshot unavailable\n", status_code=503, media_type="text/plain"
        )
    try:
        snapshot = await request.app.state.queue_stats.get()
        _publish_queue_metrics(instruments, snapshot, request.app.state.queue_stats.age(snapshot))
    except Exception:
        LOGGER.warning("metrics.queue_snapshot_failed", exc_info=True)
        instruments.REGISTRY.replace_gauge(
            "catalogue_queue_provider_up",
            "Whether the selected delivery provider can be observed.",
            [(0, {"provider": request.app.state.settings.queue_provider})],
        )
    return PlainTextResponse(render_metrics(), media_type="text/plain; version=0.0.4")


def _publish_queue_metrics(instruments: Any, snapshot: Any, age_seconds: float) -> None:
    provider = snapshot.provider
    instruments.REGISTRY.replace_gauge(
        "catalogue_queue_provider_up",
        "Whether the selected delivery provider can be observed.",
        [(1.0 if snapshot.available else 0.0, {"provider": provider})],
    )
    instruments.REGISTRY.replace_gauge(
        "catalogue_queue_snapshot_age_seconds",
        "Age of the most recent successful provider queue snapshot.",
        [(age_seconds, {"provider": provider})],
    )

    def measurement(name: str, help_text: str, found: Any) -> None:
        series = (
            []
            if found.value is None
            else [
                (
                    float(found.value),
                    {"provider": provider, "accuracy": found.accuracy.value},
                )
            ]
        )
        instruments.REGISTRY.replace_gauge(name, help_text, series)

    measurement(
        "catalogue_queue_backlog_messages",
        "Unacknowledged messages in the selected delivery provider.",
        snapshot.backlog_messages,
    )
    measurement(
        "catalogue_queue_backlog_bytes",
        "Bytes held by the selected delivery provider.",
        snapshot.backlog_bytes,
    )
    measurement(
        "catalogue_queue_consumers",
        "Consumers reported by the selected delivery provider.",
        snapshot.consumer_count,
    )
    route_fields = {
        "ready": "Messages ready for delivery by route.",
        "in_flight": "Messages delivered but not acknowledged by route.",
        "redelivered": "Messages currently marked redelivered by route.",
        "delivered": "Provider delivery sequence by route.",
        "oldest_age_seconds": "Age of the oldest unacknowledged message by route.",
    }
    for field, help_text in route_fields.items():
        series = [
            (
                float(value.value),
                {
                    "provider": provider,
                    "route": route.route,
                    "accuracy": value.accuracy.value,
                },
            )
            for route in snapshot.routes
            if (value := getattr(route, field)).value is not None
        ]
        instruments.REGISTRY.replace_gauge(f"catalogue_queue_route_{field}", help_text, series)
    recovery = snapshot.recovery_dlq
    instruments.REGISTRY.replace_gauge(
        "catalogue_queue_recovery_backlog_messages",
        "Messages awaiting authoritative redrive from the provider recovery DLQ.",
        []
        if recovery is None or recovery.backlog_messages.value is None
        else [
            (
                float(recovery.backlog_messages.value),
                {
                    "provider": provider,
                    "accuracy": recovery.backlog_messages.accuracy.value,
                },
            )
        ],
    )
    instruments.REGISTRY.replace_gauge(
        "catalogue_queue_recovery_oldest_age_seconds",
        "Age of the oldest provider recovery DLQ message.",
        []
        if recovery is None or recovery.oldest_age_seconds.value is None
        else [
            (
                float(recovery.oldest_age_seconds.value),
                {
                    "provider": provider,
                    "accuracy": recovery.oldest_age_seconds.accuracy.value,
                },
            )
        ],
    )


def _source_metric_snapshot(
    sources: SourcesFile,
    history_rows: list[dict[str, Any]],
    schedules: list[dict[str, Any]],
) -> list[dict[str, str | float | int | None]]:
    """Build schedule-aware freshness and usable-output gauges."""
    now = datetime.now(UTC)
    history = {str(row["source_id"]): row for row in history_rows}
    snapshot: list[dict[str, str | float | int | None]] = []
    for source in sources.names():
        row = history.get(source, {})
        if not bool(row.get("enabled", True)) or bool(row.get("paused", False)):
            continue
        selected = [
            schedule
            for schedule in schedules
            if schedule.get("enabled") and _schedule_selects(source, schedule.get("source_filter"))
        ]
        if explicit := row.get("schedule_id"):
            selected = [schedule for schedule in selected if schedule["id"] == explicit]
        if not selected:
            continue

        config = sources.get(source)
        if config is None:  # names() and get() share one validated mapping
            continue
        grace_seconds = max(900.0, float(config.timeout_seconds or 3600.0))
        last_success = row.get("last_success_at")
        overdue = 0.0
        for schedule in selected:
            expected = _expected_fire(schedule, now)
            if expected is None or (last_success is not None and last_success >= expected):
                continue
            overdue = max(
                overdue,
                (now - (expected + timedelta(seconds=grace_seconds))).total_seconds(),
            )
        records = row.get("last_records")
        previous = row.get("previous_records")
        ratio = (
            float(records) / float(previous)
            if records is not None and previous is not None and int(previous) > 0
            else None
        )
        snapshot.append(
            {
                "source": source,
                "overdue": max(0.0, overdue),
                "succeeded": int(last_success is not None),
                "records": int(records) if records is not None else None,
                "record_ratio": ratio,
            }
        )
    return snapshot


def _expected_fire(schedule: dict[str, Any], now: datetime) -> datetime | None:
    """Schedule occurrence whose successful completion is currently owed."""
    last_fired = schedule.get("last_fired_at")
    expected = last_fired.astimezone(UTC) if last_fired is not None else None
    next_fire = schedule.get("next_fire_at")
    if next_fire is not None:
        due = next_fire.astimezone(UTC)
        return expected if due > now else due

    # A null cursor is how a newly enabled schedule asks the leader to fire its
    # latest occurrence immediately. Derive that occurrence from the cron,
    # rather than treating "not materialised" as "not expected". A non-null
    # past cursor was returned above as the earliest occurrence still owed.
    return scheduling.previous_fire(str(schedule["cron"]), str(schedule["timezone"]), now)


def _schedule_selects(source: str, source_filter: Any) -> bool:
    selection = source_filter if isinstance(source_filter, dict) else {}
    only = selection.get("only")
    if only and source not in only:
        return False
    return source not in (selection.get("except") or ())


async def create_run(request: Request) -> Response:
    body = await _json(request)
    if body is None:
        return problem(400, "Bad Request", "a JSON object is required")

    sources: SourcesFile = request.app.state.sources
    try:
        # The same model the CLI and the scheduler validate against, so a run
        # created here cannot mean something a run created there would not.
        params = CrawlParams.model_validate(body.get("params") or {})
    except Exception as error:  # noqa: BLE001 - pydantic's message is the useful part
        return problem(422, "Invalid run parameters", str(error))

    selection = body.get("sources") or "all"
    if not isinstance(selection, str):
        return problem(422, "Invalid source selection", "sources must be 'all' or a comma-separated string")
    try:
        selected = sources.select(selection)
    except ValueError as error:
        return problem(422, "Unknown source", str(error))

    async with request.app.state.pool.connection() as connection, connection.transaction():
        run_id = await runs.create_run(
            connection,
            kind=body.get("kind", "manual"),
            requested_by=body.get("requested_by"),
            params=params.model_dump(mode="json"),
        )
        if run_id is None:
            return problem(409, "Conflict", "this scheduled occurrence already exists")
        jobs = await runs.create_jobs(connection, run_id, sources, selected)

    return JSONResponse({"run_id": str(run_id), "jobs": len(jobs), "sources": sorted(jobs)}, status_code=202)


async def list_runs(request: Request) -> Response:
    limit = _limit(request, default=25, maximum=200)
    async with request.app.state.pool.connection() as connection:
        rows = await queries.all_rows(
            connection, queries.RUNS, {"limit": limit, "cursor": request.query_params.get("cursor")}
        )
    next_cursor = rows[-1]["created_at"].isoformat() if len(rows) == limit else None
    return _json_response({"runs": rows, "next_cursor": next_cursor})


async def get_run(request: Request) -> Response:
    run_id = _uuid(request, "id")
    if run_id is None:
        return problem(400, "Bad Request", "id must be a uuid")
    async with request.app.state.pool.connection() as connection:
        run = await queries.one(connection, queries.RUN, {"id": run_id})
        if run is None:
            return problem(404, "Not Found", "no such run")
        jobs = await queries.all_rows(connection, queries.RUN_JOBS, {"run": run_id})
    return _json_response({"run": run, "jobs": jobs})


async def cancel_run(request: Request) -> Response:
    run_id = _uuid(request, "id")
    if run_id is None:
        return problem(400, "Bad Request", "id must be a uuid")
    async with request.app.state.pool.connection() as connection:
        cancelled = await queries.all_rows(connection, queries.CANCEL_RUN, {"run": run_id})
        for row in cancelled:
            await events.emit(
                connection,
                events.Topic.JOB,
                "job.cancel_requested",
                run_id=run_id,
                job_id=row["id"],
                source_id=row["source_id"],
            )
        await runs.close_run_if_done(connection, run_id)
    return JSONResponse({"cancelled": len(cancelled)}, status_code=202)


async def job_action(request: Request) -> Response:
    """pause, resume, cancel or retry one source.

    Four separate controls rather than one, because they have different safety
    properties: a pause keeps the job resumable and spends no attempt, a cancel
    is terminal and keeps the partial artifact, and a retry is a new attempt the
    operator explicitly asked for.
    """
    job_id = _uuid(request, "id")
    action = request.path_params["action"]
    statements = {
        "pause": queries.PAUSE_JOB,
        "resume": queries.RESUME_JOB,
        "cancel": queries.CANCEL_JOB,
        "retry": queries.RETRY_JOB,
    }
    if job_id is None:
        return problem(400, "Bad Request", "id must be a uuid")
    if action not in statements:
        return problem(404, "Not Found", f"unknown action {action!r}")

    async with request.app.state.pool.connection() as connection, connection.transaction():
        row = await queries.one(connection, statements[action], {"id": job_id})
        if row is None:
            # Conditional on the current state, so this is "not in a state where
            # that means anything" rather than a failure. Pressing the same
            # button twice is a no-op.
            return problem(409, "Conflict", f"this job cannot be {action}ed in its current state")
        await events.emit(
            connection,
            events.Topic.JOB,
            f"job.{action}_requested",
            run_id=row["run_id"],
            job_id=job_id,
            source_id=row["source_id"],
        )
        if action == "cancel":
            await runs.close_run_if_done(connection, row["run_id"])
        elif action in ("resume", "retry") and row["state"] == "queued":
            await outbox.enqueue_job(connection, job_id)
    return JSONResponse({"job_id": str(job_id), "action": action}, status_code=202)


async def job_logs(request: Request) -> Response:
    job_id = _uuid(request, "id")
    if job_id is None:
        return problem(400, "Bad Request", "id must be a uuid")
    async with request.app.state.pool.connection() as connection:
        rows = await queries.all_rows(
            connection,
            queries.JOB_LOG,
            {
                "job": job_id,
                "after": int(request.query_params.get("after", 0) or 0),
                "level": request.query_params.get("level"),
                "search": request.query_params.get("q"),
                "limit": _limit(request, default=500, maximum=2000),
            },
        )
    return _json_response({"lines": rows, "next_after": rows[-1]["id"] if rows else None})


async def get_job(request: Request) -> Response:
    job_id = _uuid(request, "id")
    if job_id is None:
        return problem(400, "Bad Request", "id must be a uuid")
    async with request.app.state.pool.connection() as connection:
        row = await queries.one(connection, queries.JOB, {"id": job_id})
    if row is None:
        return problem(404, "Not Found", "no such job")
    return _json_response({"job": row})


async def get_job_changes(request: Request) -> Response:
    """Compare one completed source artifact with its previous successful one."""
    from catalogue_control.changes import ArtifactError, compare, read_artifact

    job_id = _uuid(request, "id")
    if job_id is None:
        return problem(400, "Bad Request", "id must be a uuid")
    kind = request.query_params.get("kind") or None
    if kind not in {None, "added", "removed", "changed"}:
        return problem(400, "Bad Request", "kind must be added, removed, or changed")

    async with request.app.state.pool.connection() as connection:
        job = await queries.one(connection, queries.JOB, {"id": job_id})
        if job is None:
            return problem(404, "Not Found", "no such job")
        artifact = await queries.one(connection, queries.JOB_CERAMICS_ARTIFACT, {"id": job_id})
        if artifact is None:
            return problem(409, "Comparison unavailable", "this job has no completed artifact")
        previous = await queries.one(
            connection,
            queries.PREVIOUS_SUCCESSFUL_JOB,
            {"source": job["source_id"], "finished": job["finished_at"]},
        )
    if previous is None:
        return problem(409, "Comparison unavailable", "this is the first successful scrape")

    try:
        before, after = await asyncio.gather(
            asyncio.to_thread(
                read_artifact,
                request.app.state.settings.artifacts_dir,
                previous["artifact_path"],
                previous["artifact_sha256"],
            ),
            asyncio.to_thread(
                read_artifact,
                request.app.state.settings.artifacts_dir,
                artifact["artifact_path"],
                artifact["artifact_sha256"],
            ),
        )
    except ArtifactError as error:
        return problem(409, "Comparison unavailable", str(error))

    result = await asyncio.to_thread(
        compare,
        before,
        after,
        kind=kind,
        search=request.query_params.get("q"),
        limit=_limit(request, default=200, maximum=1000),
    )
    return _json_response(
        {
            "job_id": job_id,
            "previous_job_id": previous["id"],
            "previous_run_id": previous["run_id"],
            "previous_finished_at": previous["finished_at"],
            **result,
        }
    )


async def download_job_artifact(request: Request) -> Response:
    """Serve one usable job dataset artifact through the authenticated API."""
    from catalogue_control.changes import ArtifactError, resolve_artifact

    job_id = _uuid(request, "id")
    if job_id is None:
        return problem(400, "Bad Request", "id must be a uuid")
    dataset = request.query_params.get("dataset")
    if dataset is not None and (not dataset.strip() or len(dataset) > 200):
        return problem(400, "Bad Request", "dataset must be a non-empty dataset name")
    async with request.app.state.pool.connection() as connection:
        if dataset is None:
            artifact = await queries.one(connection, queries.JOB_CERAMICS_ARTIFACT, {"id": job_id})
        else:
            matches = await queries.all_rows(
                connection,
                queries.JOB_DATASET_ARTIFACTS,
                {"id": job_id, "dataset": dataset},
            )
            if len(matches) > 1:
                return problem(
                    409,
                    "Artifact ambiguous",
                    "multiple versions or artifact kinds match this dataset",
                )
            artifact = matches[0] if matches else None
    if artifact is None:
        return problem(404, "Not Found", "this job has no completed artifact for that dataset")
    try:
        path = resolve_artifact(
            request.app.state.settings.artifacts_dir,
            artifact["artifact_path"],
            artifact["artifact_sha256"],
        )
    except ArtifactError as error:
        return problem(409, "Artifact unavailable", str(error))
    return FileResponse(path, filename=path.name, media_type="application/octet-stream")


async def list_workers(request: Request) -> Response:
    """The roster, in exactly the shape the stream pushes it.

    Shaped through the same `_worker` projection the `worker.roster` event uses.
    They were two hand-written shapes that differed only in calling the
    identifier `id` here and `worker_id` there, which is precisely the kind of
    difference that makes a client merge two records into one worker and show
    the fleet as twice its size.
    """
    from catalogue_control.broker import _worker

    async with request.app.state.pool.connection() as connection:
        rows = await queries.all_rows(connection, queries.WORKERS)
    return _json_response(
        {"workers": [{**_worker(row), "heartbeat_age_seconds": row["heartbeat_age_seconds"]} for row in rows]}
    )


async def queue_status(request: Request) -> Response:
    """PostgreSQL authority and provider-neutral delivery state."""
    async with request.app.state.pool.connection() as connection:
        states = await queries.all_rows(connection, queries.METRIC_JOB_STATES)
        eligible = await queries.one(connection, queries.QUEUE_ELIGIBLE) or {}
        outbox_stats = await queries.one(connection, queries.QUEUE_OUTBOX_STATS) or {}

    try:
        snapshot = await request.app.state.queue_stats.get()
        broker = snapshot.json()
        broker_error = snapshot.error
    except Exception as error:  # noqa: BLE001 - broker failure is response data
        broker = None
        broker_error = f"{type(error).__name__}: queue statistics unavailable"
        LOGGER.warning("queue.snapshot_broker_unavailable", error=broker_error)

    integer_fields = ("pending", "ready", "delayed", "errored", "publish_attempts", "published_last_hour")
    outbox_payload: dict[str, int | float] = {
        field: int(outbox_stats.get(field) or 0) for field in integer_fields
    }
    outbox_payload["oldest_age_seconds"] = float(outbox_stats.get("oldest_age_seconds") or 0)
    return _json_response(
        {
            "at": datetime.now(UTC),
            "jobs": {str(row["state"]): int(row["n"]) for row in states},
            "eligible": int(eligible.get("eligible") or 0),
            "oldest_queued_age_seconds": float(eligible.get("oldest_age_seconds") or 0),
            "outbox": outbox_payload,
            "broker": broker,
            "broker_error": broker_error,
        }
    )


async def worker_action(request: Request) -> Response:
    """Pause, resume, drain or stop a worker, or hide a lost registration.

    This controls the registered process, not the deployment's replica count. A
    restart policy may well create a new worker afterwards, so persistently
    removing capacity is a scale operation and this API does not pretend
    otherwise.
    """
    worker_id = _uuid(request, "id")
    action = request.path_params["action"]
    desired = {"pause": "paused", "resume": "running", "drain": "draining", "stop": "stopping"}
    if worker_id is None:
        return problem(400, "Bad Request", "id must be a uuid")
    if action not in {*desired, "hide"}:
        return problem(404, "Not Found", f"unknown action {action!r}")

    async with request.app.state.pool.connection() as connection:
        if action == "hide":
            row = await queries.one(connection, queries.HIDE_LOST_WORKER, {"id": worker_id})
            if row is None:
                return problem(409, "Conflict", "only a lost worker can be hidden")
            await events.emit(
                connection,
                events.Topic.WORKER,
                "worker.changed",
                worker_id=worker_id,
                payload={"status": "stopped", "hidden": True},
            )
            return JSONResponse(
                {"worker_id": str(worker_id), "status": "stopped", "hidden": True},
                status_code=202,
            )

        row = await queries.one(
            connection, queries.SET_WORKER_STATE, {"id": worker_id, "desired": desired[action]}
        )
        if row is None:
            return problem(409, "Conflict", "no such worker, or it has already stopped")
        await events.emit(
            connection,
            events.Topic.WORKER,
            "worker.changed",
            worker_id=worker_id,
            payload={"desired_state": desired[action]},
        )
    return JSONResponse({"worker_id": str(worker_id), "desired_state": desired[action]}, status_code=202)


async def list_sources(request: Request) -> Response:
    """sources.json joined to what actually happened to each source."""
    sources: SourcesFile = request.app.state.sources
    async with request.app.state.pool.connection() as connection:
        observed = {row["source_id"]: row for row in await queries.all_rows(connection, queries.SOURCES)}

    payload = []
    for name, config in sources.items():
        row = observed.get(name, {})
        last = row.get("last_records")
        previous = row.get("previous_records")
        payload.append(
            {
                "source_id": name,
                "label": config.label,
                "url": config.url,
                "scraper": config.scraper,
                "country": config.country,
                "enabled": row.get("enabled", True),
                "paused": row.get("paused", False),
                "schedule_id": row.get("schedule_id"),
                "params": row.get("params", {}),
                "last_success_at": row.get("last_success_at"),
                "last_records": last,
                "previous_records": previous,
                # The single most useful number on the page: a source that has
                # quietly halved is the failure this whole plan exists to catch.
                "delta": (last - previous) if last is not None and previous is not None else None,
                "staleness_seconds": row.get("staleness_seconds"),
                "runs_7d": row.get("runs_7d", 0),
                "failures_7d": row.get("failures_7d", 0),
                "last_job_id": row.get("last_job_id"),
                "last_run_id": row.get("last_run_id"),
            }
        )
    return _json_response({"sources": payload})


async def update_source(request: Request) -> Response:
    name = request.path_params["id"]
    sources: SourcesFile = request.app.state.sources
    if name not in sources:
        return problem(404, "Not Found", f"unknown source {name!r}")
    body = await _json(request) or {}

    if body.get("params"):
        try:
            CrawlParams.model_validate(body["params"])
        except Exception as error:  # noqa: BLE001
            return problem(422, "Invalid source parameters", str(error))

    async with request.app.state.pool.connection() as connection, connection.transaction():
        current_cursor = await connection.execute(
            "select * from catalogue.source_settings where source_id = %(id)s",
            {"id": name},
        )
        current = await current_cursor.fetchone() or {}
        proxy_policy = None
        if "proxy" in body:
            proxy_policy = await proxy_api.apply_source_policy(request, connection, name, body.get("proxy"))
            if isinstance(proxy_policy, Response):
                return proxy_policy
        row = await queries.one(
            connection,
            queries.UPSERT_SOURCE,
            {
                "id": name,
                "enabled": bool(body.get("enabled", current.get("enabled", True))),
                "paused": bool(body.get("paused", current.get("paused", False))),
                "schedule": body.get("schedule_id", current.get("schedule_id")),
                "params": queries.as_jsonb(body.get("params", current.get("params", {}))),
                "by": body.get("updated_by", current.get("updated_by")),
            },
        )
        desired_paused = bool(body.get("paused", current.get("paused", False)))
        desired_enabled = bool(body.get("enabled", current.get("enabled", True)))
        if not desired_enabled:
            disabled = await queries.all_rows(connection, queries.DISABLE_SOURCE_JOBS, {"id": name})
            for job in disabled:
                await events.emit(
                    connection,
                    events.Topic.JOB,
                    "job.skipped" if job["state"] == "skipped" else "job.cancel_requested",
                    run_id=job["run_id"],
                    job_id=job["id"],
                    source_id=name,
                    payload={"reason": "source disabled"},
                )
                if job["state"] == "skipped":
                    await runs.close_run_if_done(connection, job["run_id"])
        if desired_enabled and desired_paused:
            # Pausing a source also pauses the jobs it already has in flight.
            # Resuming does not automatically resume individually paused jobs.
            await queries.all_rows(connection, queries.PAUSE_SOURCE_JOBS, {"id": name})
        elif desired_enabled and current.get("paused"):
            resumed = await queries.all_rows(connection, queries.RESUME_SOURCE_JOBS, {"id": name})
            for job in resumed:
                await outbox.enqueue_job(connection, job["id"])
        await events.emit(
            connection, events.Topic.SOURCE, "source.changed", source_id=name, payload=dict(body)
        )
    return _json_response({"source": row, "proxy": proxy_policy})


async def list_notifications(request: Request) -> Response:
    async with request.app.state.pool.connection() as connection:
        rows = await queries.all_rows(
            connection,
            queries.NOTIFICATIONS,
            {
                "unacknowledged": request.query_params.get("unacknowledged") == "true",
                "severity": request.query_params.get("severity"),
                "limit": _limit(request, default=100, maximum=500),
            },
        )
    return _json_response({"notifications": rows})


async def acknowledge_notification(request: Request) -> Response:
    try:
        notification_id = int(request.path_params["id"])
    except ValueError:
        return problem(400, "Bad Request", "id must be a number")
    body = await _json(request) or {}
    async with request.app.state.pool.connection() as connection:
        done = await events.acknowledge(connection, notification_id, body.get("by") or "operator")
    if not done:
        return problem(409, "Conflict", "already acknowledged, or no such notification")
    return JSONResponse({"id": notification_id, "acknowledged": True})


async def acknowledge_notifications(request: Request) -> Response:
    body = await _json(request) or {}
    raw_ids = body.get("ids")
    if not isinstance(raw_ids, list) or not raw_ids:
        return problem(400, "Bad Request", "ids must be a non-empty list")
    if len(raw_ids) > 500:
        return problem(400, "Bad Request", "at most 500 notifications may be acknowledged at once")
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in raw_ids):
        return problem(400, "Bad Request", "every notification id must be a positive integer")
    notification_ids = list(dict.fromkeys(raw_ids))
    async with request.app.state.pool.connection() as connection:
        acknowledged = await events.acknowledge_many(
            connection, notification_ids, body.get("by") or "operator"
        )
    return JSONResponse({"ids": acknowledged, "acknowledged": len(acknowledged)})


async def list_schedules(request: Request) -> Response:
    async with request.app.state.pool.connection() as connection:
        rows = await queries.all_rows(connection, queries.SCHEDULES)
    return _json_response({"schedules": rows})


async def update_schedule(request: Request) -> Response:
    body = await _json(request) or {}
    name = request.path_params["id"]
    async with request.app.state.pool.connection() as connection:
        row = await queries.one(
            connection,
            queries.UPSERT_SCHEDULE,
            {
                "id": name,
                "enabled": bool(body.get("enabled", True)),
                "cron": body.get("cron", "0 3 * * *"),
                "timezone": body.get("timezone", "Europe/Paris"),
                "filter": queries.as_jsonb(body.get("source_filter") or {"all": True}),
                "params": queries.as_jsonb(body.get("params")),
            },
        )
        await events.emit(connection, events.Topic.SCHEDULE, "schedule.changed", payload={"id": name})
    return _json_response({"schedule": row})


# ---------------------------------------------------------------------------
# The stream
# ---------------------------------------------------------------------------


async def stream(request: Request) -> Response:
    """One multiplexed stream, filtered by topic.

    One endpoint rather than one per concern, because HTTP/1.1 caps a browser at
    roughly six connections per origin: three `EventSource` objects per tab
    means two tabs deadlock the app against its own streams.
    """
    broker: Broker = request.app.state.broker
    topics = parse_topics(request.query_params.get("topics"))
    run_id = request.query_params.get("run_id")

    last_event_id: int | None = None
    header = request.headers.get("last-event-id") or request.query_params.get("last_event_id")
    if header:
        with contextlib.suppress(ValueError):
            last_event_id = int(header)

    async def body() -> AsyncIterator[bytes]:
        async with request.app.state.pool.connection() as connection:
            snapshot = await queries.bootstrap(
                connection, UUID(run_id) if run_id and "progress" in topics else None
            )
        # The stream opens with everything needed to render the first frame, so
        # a client never has to make a second request to draw anything.
        yield _sse("bootstrap", {**snapshot, "watermark": broker.watermark}).encode()

        agen = broker.subscribe(topics, run_id, last_event_id)
        subscriber: Subscriber = await agen.__anext__()
        try:
            while True:
                try:
                    message = await asyncio.wait_for(subscriber.queue.get(), KEEPALIVE_SECONDS)
                except TimeoutError:
                    # A comment, not an event: it keeps proxies from closing an
                    # idle connection without appearing in the client's handler.
                    yield b": keepalive\n\n"
                    continue
                if message is None:
                    return
                yield message.encode().encode()
                if subscriber.resync:
                    return
        finally:
            with contextlib.suppress(StopAsyncIteration):
                await agen.__anext__()

    return StreamingResponse(
        body(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            # nginx and Caddy will otherwise buffer the stream into blocks and
            # it arrives in bursts, or not at all. Caddy additionally needs
            # `flush_interval -1` on the reverse_proxy.
            "X-Accel-Buffering": "no",
        },
    )


def _sse(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


# ---------------------------------------------------------------------------
# Helpers and wiring
# ---------------------------------------------------------------------------


async def _json(request: Request) -> dict[str, Any] | None:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - a malformed body is a 400, not a 500
        return None
    return body if isinstance(body, dict) else None


def _uuid(request: Request, name: str) -> UUID | None:
    try:
        return UUID(request.path_params[name])
    except (ValueError, KeyError):
        return None


def _limit(request: Request, *, default: int, maximum: int) -> int:
    try:
        return min(int(request.query_params.get("limit", default)), maximum)
    except ValueError:
        return default


def _json_response(payload: dict[str, Any]) -> Response:
    return Response(json.dumps(payload, default=str), media_type="application/json")


def _build_providers(settings: Settings) -> dict[str, Any]:
    """Construct every enabled provider from the registry.

    A provider whose credential file is absent is skipped rather than raising:
    paid proxying is opt-in, and the common case for a fresh deployment is that
    none is configured at all. An *unknown* provider name is a different thing
    and does raise -- that is a typo in the environment, and skipping it would
    look exactly like a credential that failed to load.
    """
    from mb_ceramics_catalogue.observability import logging as obs
    from mb_ceramics_catalogue.providers.registry import spec
    from mb_ceramics_catalogue.proxy import load_api_key

    built: dict[str, Any] = {}
    for name in settings.enabled_providers():
        provider_spec = spec(name)
        secret_file = settings.proxy_provider_secret_files.get(name)
        if secret_file is None and name == settings.proxy_default_provider:
            secret_file = settings.proxy_api_secret_file
        if secret_file is None:
            continue
        api_key = load_api_key(secret_file)
        obs.register_secrets({api_key})
        options: dict[str, Any] = {}
        if name == "decodo":
            options["limit_unit"] = settings.proxy_provider_limit_unit
        if name == "iproyal":
            options["traffic_writes"] = settings.proxy_iproyal_traffic_writes
        if name == "proxyscrape":
            options["sub_account_id"] = settings.proxy_proxyscrape_sub_account_id
        base_url = settings.proxy_provider_base_urls.get(name)
        if base_url is None and name == settings.proxy_default_provider:
            base_url = settings.proxy_provider_base_url
        built[name] = provider_spec.build(
            api_key, base_url=base_url or provider_spec.default_base_url, **options
        )
    return built


def create_app(settings: Settings | None = None, *, proxy_provider: Any = None) -> Starlette:
    settings = settings or Settings()
    if settings.require_token and not settings.control_token:
        raise ValueError(
            "CATALOGUE_CONTROL_TOKEN is not set. This service can cancel runs and "
            "disable sources; it refuses to start without authentication."
        )

    routes = [
        Route("/health", health),
        Route("/metrics", metrics_endpoint),
        Route("/v1/runs", create_run, methods=["POST"]),
        Route("/v1/runs", list_runs, methods=["GET"]),
        Route("/v1/runs/{id}", get_run),
        Route("/v1/runs/{id}/cancel", cancel_run, methods=["POST"]),
        Route("/v1/jobs/{id}", get_job),
        Route("/v1/jobs/{id}/changes", get_job_changes),
        Route("/v1/jobs/{id}/artifact", download_job_artifact),
        Route("/v1/jobs/{id}/logs", job_logs),
        Route("/v1/jobs/{id}/{action}", job_action, methods=["POST"]),
        Route("/v1/workers", list_workers),
        Route("/v1/queue", queue_status),
        Route("/v1/workers/{id}/{action}", worker_action, methods=["POST"]),
        Route("/v1/sources", list_sources),
        Route("/v1/sources/{id}", update_source, methods=["PUT"]),
        Route("/v1/schedules", list_schedules),
        Route("/v1/schedules/{id}", update_schedule, methods=["PUT"]),
        Route("/v1/notifications", list_notifications),
        Route("/v1/notifications/ack", acknowledge_notifications, methods=["POST"]),
        Route("/v1/notifications/{id}/ack", acknowledge_notification, methods=["POST"]),
        Route("/v1/proxy/overview", proxy_api.overview),
        Route("/v1/proxy/cycles", proxy_api.cycles),
        Route("/v1/proxy/usage", proxy_api.usage),
        Route("/v1/proxy/reservations", proxy_api.reservations),
        Route("/v1/proxy/profiles", proxy_api.profiles, methods=["GET"]),
        Route("/v1/proxy/profiles", proxy_api.create_profile, methods=["POST"]),
        Route("/v1/proxy/profiles/refresh", proxy_api.refresh_profiles, methods=["POST"]),
        Route("/v1/proxy/profiles/{id}/{action}", proxy_api.profile_action, methods=["POST", "PUT"]),
        Route("/v1/proxy/profiles/{id}", proxy_api.retire_profile, methods=["DELETE"]),
        Route("/v1/proxy/routes", proxy_api.routes, methods=["GET"]),
        Route("/v1/proxy/routes", proxy_api.create_route, methods=["POST"]),
        Route("/v1/proxy/routes/{id}/probe", proxy_api.probe_route, methods=["POST"]),
        Route("/v1/proxy/routes/{id}", proxy_api.update_or_delete_route, methods=["PUT", "DELETE"]),
        Route("/v1/proxy/probes", proxy_api.probes),
        Route("/v1/proxy/audit", proxy_api.audit),
        Route("/v1/proxy/candidates", proxy_api.candidates),
        Route("/v1/proxy/reconcile", proxy_api.reconcile_action, methods=["POST"]),
        Route("/v1/proxy/kill-switch/{action}", proxy_api.kill_switch, methods=["POST"]),
        Route("/v1/proxy/pilot/{action}", proxy_api.pilot_action, methods=["POST"]),
        Route("/v1/proxy/cycles/propose", proxy_api.propose_cycle, methods=["POST"]),
        Route("/v1/proxy/cycles/{id}/{action}", proxy_api.open_or_close_cycle, methods=["POST"]),
        Route("/v1/events", stream),
    ]

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        from mb_ceramics_catalogue.ops.providers.factory import stats_reader
        from mb_ceramics_catalogue.storage import db

        from catalogue_control.queue_stats import QueueSnapshotCache

        async with db.pool(settings.dsn, minimum=1, maximum=8) as pool:
            app.state.pool = pool
            app.state.settings = settings
            app.state.sources = SourcesFile.load(default_path())
            app.state.actor_keys = load_public_keys(settings.proxy_actor_public_keys_file)
            app.state.queue_stats = QueueSnapshotCache(
                stats_reader(settings),
                max_age_seconds=settings.queue_snapshot_cache_seconds,
                timeout_seconds=settings.queue_snapshot_timeout_seconds,
            )
            # An injected provider is the test seam and stays single: it becomes
            # the default provider and the only one.
            providers: dict[str, Any] = {}
            if proxy_provider is not None:
                providers[settings.proxy_default_provider] = proxy_provider
            else:
                providers = _build_providers(settings)
            app.state.providers = providers
            #: The default, kept as `state.provider` because most call sites want
            #: "the one provider this request is about" and resolve it earlier.
            app.state.provider = providers.get(settings.proxy_default_provider)
            app.state.broker = Broker(settings)
            await app.state.broker.start()
            # One scheduler per provider. Reconciliation reads a provider's usage
            # and writes that provider's cycle; two providers share no rows, so
            # running them on one loop would only make the slower one delay the
            # other's accounting.
            app.state.proxy_schedulers = {}
            if settings.proxy_enabled:
                for name, built in providers.items():
                    scheduler = ReconciliationScheduler(
                        pool,
                        built,
                        settings.proxy_reconcile_interval_seconds,
                        settings.proxy_secret_file,
                        provider_name=name,
                    )
                    app.state.proxy_schedulers[name] = scheduler
                    await scheduler.start()
            # Kept for the tests and call sites that predate multi-provider.
            app.state.proxy_scheduler = app.state.proxy_schedulers.get(settings.proxy_default_provider)
            try:
                yield
            finally:
                for scheduler in app.state.proxy_schedulers.values():
                    await scheduler.stop()
                await app.state.queue_stats.close()
                await app.state.broker.stop()

    return Starlette(
        routes=routes,
        lifespan=lifespan,
        middleware=[
            Middleware(RequestTelemetry, service="control", routes=routes),
            Middleware(BearerToken, token=settings.control_token),
        ],
    )
