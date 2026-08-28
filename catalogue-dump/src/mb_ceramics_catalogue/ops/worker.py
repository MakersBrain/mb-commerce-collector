"""The worker: claim a source, crawl it, load it, report it, repeat.

This is what turns the scrape and the load from two manual steps joined by files
on disk into one automatic step, and what makes "start a run" mean "insert a
row" rather than "have a terminal open".

The conventions deliberately mirror `ateliera-app/apps/api/src/workers/
lifecycle.ts`: the same event vocabulary (`worker.starting`, `worker.ready`,
`worker.tick`, `worker.stopping`), the same heartbeat-and-backoff shape. An
operator who knows one knows the other.

The loop, in order, and each step is in this order for a reason:

    register                       durable, so a dead worker is a stale
                                   heartbeat rather than an absence
    heartbeat every 5s             in its own task on its own connection, so a
                                   busy job cannot block the liveness signal
    loop:
      observe desired_state        claim only while it is running
      reap expired leases          recover work from workers that died
      consume a delivery           provider route matches capabilities
      reserve its generation       PostgreSQL fences duplicates and stale work
      acquire a host slot          else release with a short backoff, no
                                   attempt burnt
      mark running                 and consume the attempt, not before
      crawl                        the existing run_source, with the PG sink
      load                         in-process, one transaction
      release, finish, summarise   which closes the run if it was the last job
    on SIGTERM: drain, then requeue anything unfinished, then mark stopped
"""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import hashlib
import json
import os
import signal
import socket
import time
from collections.abc import AsyncIterator, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg
from mb_commerce_scraper import sanitize_diagnostic_text
from psycopg.types.json import Jsonb

from mb_ceramics_catalogue import __version__, scrapers
from mb_ceramics_catalogue.config.settings import CrawlParams, Settings
from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.connectors import BrowserBackendName
from mb_ceramics_catalogue.crawl import artifacts
from mb_ceramics_catalogue.crawl.progress import Progress
from mb_ceramics_catalogue.crawl.runner import barren as run_source_barren
from mb_ceramics_catalogue.crawl.runner import run_source
from mb_ceramics_catalogue.crawl.runner import transient as run_source_transient
from mb_ceramics_catalogue.crawl.session import open_session
from mb_ceramics_catalogue.datasets import built_in_registry
from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.observability import metrics, tracing
from mb_ceramics_catalogue.ops import events, health, leases, monitor, queue, runs, schedule
from mb_ceramics_catalogue.ops import outputs as ops_outputs
from mb_ceramics_catalogue.ops.commerce_scraper_proxy_runtime import (
    resolve_native_proxy_runtime,
)
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    TRANSPORT_TOTAL_NAMES,
    BorrowedBrowserBinding,
    CatalogueCommerceRuntime,
)
from mb_ceramics_catalogue.ops.connector_adapters import (
    runtime_plan,
)
from mb_ceramics_catalogue.ops.delivery import Delivery, routes_for
from mb_ceramics_catalogue.ops.library_lineages import (
    LibraryLineageSpec,
    resolve_library_lineage,
)
from mb_ceramics_catalogue.ops.providers.factory import consumer
from mb_ceramics_catalogue.ops.sink import THROTTLE_SECONDS, JobLogHandler, PostgresSink
from mb_ceramics_catalogue.pipeline.outputs import LocalArtifactStore
from mb_ceramics_catalogue.pipeline.runner import ConnectorPipeline, DatasetPageState, PipelineResult
from mb_ceramics_catalogue.proxy import (
    ProxyDenied,
    ProxyLease,
    close_reservation,
    load_profiles,
    reserve,
    secret_values,
)
from mb_ceramics_catalogue.scrapers.activity import CURRENT_JOB
from mb_ceramics_catalogue.scrapers.base import BrowserRenderer
from mb_ceramics_catalogue.scrapers.record import RecordBuilder
from mb_ceramics_catalogue.storage import postgres
from mb_ceramics_catalogue.storage.db import DictPool
from mb_ceramics_catalogue.transports.browser import (
    BrowserBackend,
    BrowserJobContext,
    BrowserUnavailable,
)
from mb_ceramics_catalogue.transports.cdp_extension_proxy import CdpExtensionProxyBackend

LOGGER = obs.get_logger("catalogue.worker")


class _BorrowedConnectionPool:
    """Expose the worker's fenced job connection to the proxy adapter.

    The native proxy pool serializes its reservation and attempt-accounting
    operations. Reusing this connection prevents a one-slot worker pool from
    deadlocking while a canary already owns its job connection.
    """

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    @contextlib.asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        yield self._connection


#: How often the worker reports that it is alive and renews its leases.
HEARTBEAT_SECONDS = 5.0

#: How long to wait after finding no compatible broker delivery. Long enough to
#: avoid a hot loop, short enough that "Run now" in the UI feels immediate.
IDLE_SECONDS = 2.0

#: How long a drain waits for the current source before giving up on it.
DRAIN_GRACE_SECONDS = 120.0

#: How often a worker attempts the leader duties. Every tick would mean eighty
#: advisory-lock attempts a second across a busy pool for work that is only
#: meaningful once a minute.
LEADER_SECONDS = 30.0

#: Retention runs far less often than the notification rules; deleting a month
#: of rows is not something to do every half minute.
PRUNE_SECONDS = 3600.0

#: One open `job.failed` row per source, raised on the failure and cleared by
#: the next success. Keyed on the source alone: a key carrying the job id makes
#: a new unresolved row every night for a source that fails every night.
_JOB_FAILED_KEY = "job.failed:{source}"


@dataclass
class WorkerState:
    """Everything about this process that the database also knows."""

    id: UUID = field(default_factory=uuid4)
    hostname: str = field(default_factory=socket.gethostname)
    pid: int = field(default_factory=os.getpid)
    capabilities: list[str] = field(default_factory=list)
    status: str = "starting"
    desired_state: str = "running"
    #: Jobs this process is running right now, by job id. A worker may hold
    #: several: they are different sources on different hosts, and the thing
    #: that stops two of them hammering one shop is `catalogue.host_leases`,
    #: not the fact that a process happened to do one at a time.
    current_jobs: dict[UUID, queue.ClaimedJob] = field(default_factory=dict)
    stopping: bool = False

    @property
    def current_job(self) -> queue.ClaimedJob | None:
        """The job to name in single-valued places, e.g. `workers.current_job_id`."""
        return next(iter(self.current_jobs.values()), None)


class Worker:
    """One worker process."""

    def __init__(
        self,
        pool: DictPool,
        sources: SourcesFile,
        settings: Settings,
        *,
        capabilities: list[str] | None = None,
        browser_backends: Mapping[BrowserBackendName, BrowserBackend] | None = None,
        once: bool = False,
    ) -> None:
        self.pool = pool
        self.sources = sources
        self.settings = settings
        # The connector registry and transport factories are application
        # composition. Keep one immutable root for the worker process while
        # every opened connector and route remains collection-scoped.
        self._commerce_runtime = CatalogueCommerceRuntime()
        self.state = WorkerState(capabilities=capabilities or [])
        self.once = once
        self._task: asyncio.Task[Any] | None = None
        self._heartbeat: asyncio.Task[None] | None = None
        #: One cancel flag per running job: cancelling one source must not
        #: tear down the others this process is carrying.
        self._cancels: dict[UUID, asyncio.Event] = {}
        self._job_started: dict[UUID, float] = {}
        self._deliveries: dict[UUID, Delivery] = {}
        self._broker = consumer(settings)
        self._slots = asyncio.Semaphore(max(1, settings.job_slots))
        #: One camoufox for this process, shared by every job that renders and
        #: started on the first one that does. Per job it was sixteen across the
        #: fleet; see `BrowserRenderer`.
        self._browser_backends: dict[BrowserBackendName, BrowserBackend] = dict(browser_backends or {})
        for name, backend in self._browser_backends.items():
            if backend.backend != name.value:
                raise ValueError(
                    f"browser backend registry key {name.value!r} does not match "
                    f"implementation {backend.backend!r}"
                )
        cdp_capability = f"browser:{BrowserBackendName.CDP_EXTENSION_PROXY.value}"
        if (
            cdp_capability in self.state.capabilities
            and BrowserBackendName.CDP_EXTENSION_PROXY not in self._browser_backends
        ):
            raise ValueError("worker cannot advertise cdp_extension_proxy without a ready backend")
        # Negative, so the first tick leads immediately rather than waiting out
        # the interval: a worker starting after downtime should notice a missed
        # schedule now, not in thirty seconds.
        self._last_lead = -LEADER_SECONDS
        self._last_prune = -PRUNE_SECONDS
        self._last_probe = -health.PROBE_SECONDS

    def describe(self) -> dict[str, Any]:
        """What `/health` reports. Read from a thread, so it touches no I/O."""
        return {
            "status": self.state.status,
            "worker_id": str(self.state.id),
            "desired_state": self.state.desired_state,
            "capabilities": self.state.capabilities,
            "current_source": getattr(self.state.current_job, "source_id", None),
            "version": __version__,
        }

    # -- lifecycle --------------------------------------------------------

    async def register(self) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                insert into catalogue.workers
                       (id, hostname, pid, version, capabilities, status)
                values (%(id)s, %(host)s, %(pid)s, %(version)s, %(caps)s, 'starting')
                on conflict (id) do update
                   set status = 'starting', last_heartbeat_at = now()
                """,
                {
                    "id": self.state.id,
                    "host": self.state.hostname,
                    "pid": self.state.pid,
                    "version": __version__,
                    "caps": self.state.capabilities,
                },
            )
            await events.emit(
                connection,
                events.Topic.WORKER,
                "worker.starting",
                worker_id=self.state.id,
                payload={
                    "hostname": self.state.hostname,
                    "pid": self.state.pid,
                    "version": __version__,
                    "capabilities": self.state.capabilities,
                },
            )
        obs.bind(worker_id=str(self.state.id))
        LOGGER.info(
            "worker.starting",
            hostname=self.state.hostname,
            pid=self.state.pid,
            capabilities=self.state.capabilities,
            version=__version__,
        )

    async def set_status(self, status: str, *, job_id: UUID | None = None, force: bool = False) -> None:
        """Record a status change, and emit it as an edge.

        Status is an edge — `idle -> busy` is discrete and worth pushing to a
        browser. Liveness is a level and is carried by `last_heartbeat_at`,
        which the UI turns into an age locally. A worker that has silently died
        therefore goes stale on its own, without any event arriving — which is
        precisely the case where waiting for an event cannot work.
        """
        if (
            not force
            and self.state.status == status
            and job_id == getattr(self.state.current_job, "id", None)
        ):
            return
        self.state.status = status
        async with self.pool.connection() as connection:
            await connection.execute(
                "update catalogue.workers set status = %(status)s, current_job_id = %(job)s, "
                "last_heartbeat_at = now() where id = %(id)s",
                {"status": status, "job": job_id, "id": self.state.id},
            )
            await events.emit(
                connection,
                events.Topic.WORKER,
                "worker.changed",
                worker_id=self.state.id,
                job_id=job_id,
                payload={"status": status, "current_job_id": str(job_id) if job_id else None},
            )

    async def _beat(self) -> None:
        """Report liveness, renew leases, and read the control flags.

        On its own connection from the pool, in its own task. If this shared the
        job's connection it would stop while the job held it, and a healthy
        worker crawling a slow shop would look dead — after which its job would
        be reaped and run twice.
        """
        while not self.state.stopping:
            try:
                for delivery in list(self._deliveries.values()):
                    await delivery.extend(HEARTBEAT_SECONDS)
            except Exception:
                # Broker liveness and database liveness are independent. A
                # failed progress ACK is retried next heartbeat and must not
                # prevent lease/control polling.
                LOGGER.warning("worker.delivery_heartbeat_failed", exc_info=True)
            try:
                async with self.pool.connection() as connection:
                    row = await _one(
                        connection,
                        "update catalogue.workers set last_heartbeat_at = now() "
                        "where id = %(id)s returning desired_state",
                        {"id": self.state.id},
                    )
                    if row is not None:
                        await self._observe_desired_state(str(row["desired_state"]))

                    jobs = list(self.state.current_jobs.values())
                    held = await queue.renew(connection, jobs, self.state.id)
                    await leases.renew(connection, self.state.id, [job.execution_token for job in jobs])
                    for job in held:
                        if job["cancel_requested"]:
                            LOGGER.info("job.cancel_requested", job_id=str(job["id"]))
                            self._cancel(job["id"])
                        elif job["pause_requested"]:
                            # Pause is implemented as a cancel that keeps the
                            # partial artifact and leaves the job resumable,
                            # rather than as a held-open in-memory session: a
                            # worker holding a browser and a half-read catalogue
                            # for an unbounded time is a much worse failure than
                            # restarting the source.
                            LOGGER.info("job.pause_requested", job_id=str(job["id"]))
                            self._cancel(job["id"])
                    revoked = await connection.execute(
                        """
                        select r.job_id
                          from catalogue.proxy_reservations r
                          join catalogue.jobs j on j.id = r.job_id
                         where j.lease_owner = %(worker)s
                           and r.state in ('active', 'revocation_requested')
                           and r.revocation_requested
                        """,
                        {"worker": self.state.id},
                    )
                    for reservation in await revoked.fetchall():
                        LOGGER.warning("proxy.lease_revoked", job_id=str(reservation["job_id"]))
                        self._cancel(reservation["job_id"])
            except psycopg.Error:
                # A heartbeat that cannot reach the database is exactly when the
                # worker must not fall over: the database may be restarting, and
                # the lease has minutes left on it.
                LOGGER.warning("worker.heartbeat_failed", exc_info=True)

            await asyncio.sleep(HEARTBEAT_SECONDS)

    async def _observe_desired_state(self, desired: str) -> None:
        if desired == self.state.desired_state:
            return
        self.state.desired_state = desired
        LOGGER.info("worker.desired_state", desired=desired)
        if desired == "stopping":
            # Cancels every source in flight through the same safe
            # partial-artifact path a per-job cancel uses, then exits.
            self._cancel_all()
            self.state.stopping = True
        elif desired == "draining":
            self.state.stopping = True
        elif desired == "paused":
            await self.set_status("paused")
        elif desired == "running" and self.state.status == "paused":
            await self.set_status("idle")

    # -- the loop ---------------------------------------------------------

    async def run(self) -> int:
        await self._broker.connect()
        await self.register()
        self._heartbeat = asyncio.create_task(self._beat(), name="worker-heartbeat")
        await self.set_status("idle")
        LOGGER.info("worker.ready", capabilities=self.state.capabilities)

        completed = 0
        running: set[asyncio.Task[bool]] = set()
        try:
            while not self.state.stopping:
                if self.state.desired_state in ("paused",):
                    await asyncio.sleep(IDLE_SECONDS)
                    continue

                # Fill the free slots before waiting on any of them. Sources are
                # independent and mostly waiting on someone else's network, so a
                # worker that runs them one at a time is idle for most of a run;
                # `catalogue.host_leases` is what keeps two jobs off one shop,
                # and it works within a process exactly as it does between two.
                while len(running) < self.settings.job_slots and not self.state.stopping:
                    claimed = await self.tick(spawn=running)
                    if not claimed:
                        break

                if not running:
                    if self.once:
                        break
                    await asyncio.sleep(IDLE_SECONDS)
                    continue

                done, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                worked = False
                for task in done:
                    try:
                        worked = task.result() or worked
                    except Exception:
                        # execute() already recorded the job's own outcome; this
                        # is the task machinery failing, which is ours not the
                        # source's, and must not stop the other slots.
                        LOGGER.exception("worker.job_task_failed")
                if worked:
                    completed += 1
                    if self.once:
                        break
                    if self.settings.max_jobs and completed >= self.settings.max_jobs:
                        # A clean exit between jobs, not a crash during one. The
                        # restart policy brings up a fresh process with a fresh
                        # browser; nothing is requeued because nothing is in
                        # flight at this point.
                        LOGGER.info("worker.recycling", completed=completed, max_jobs=self.settings.max_jobs)
                        self.state.desired_state = "draining"
                        break
        finally:
            # Let whatever is still in flight finish its own cancellation path
            # before the connection pool goes away underneath it.
            for task in running:
                task.cancel()
            if running:
                await asyncio.gather(*running, return_exceptions=True)
            await self.shutdown()
        return completed

    async def lead(self) -> None:
        """Do the things exactly one worker should do, if this one may.

        Scheduling, the notification rules and retention all need a single
        actor. Rather than a scheduler container — one more thing to deploy and
        a single point of failure — every worker tries a transaction-scoped
        advisory lock each tick and whichever gets it does the work. A
        transaction-scoped lock cannot leak or accumulate on a pooled session,
        which a session-scoped one can.
        """
        now = time.monotonic()
        if now - self._last_lead < LEADER_SECONDS:
            return
        self._last_lead = now
        try:
            async with self.pool.connection() as connection, connection.transaction():
                if not await schedule.try_become_leader(connection):
                    return
                await schedule.fire_due(connection, self.sources)
                await monitor.check_all(connection)
                if now - self._last_prune > PRUNE_SECONDS:
                    self._last_prune = now
                    await monitor.prune(connection)
                if now - self._last_probe > health.PROBE_SECONDS:
                    self._last_probe = now
                    await self._probe_disabled_sources()
        except psycopg.Error:
            # Leading is maintenance. A worker that cannot do it should keep
            # crawling; another worker will hold the lock next tick.
            LOGGER.warning("worker.lead_failed", exc_info=True)

    async def _probe_disabled_sources(self) -> None:
        """Ask the sites of disabled sources whether they are back.

        On its own connection, outside the leader transaction: these are
        network calls to somebody else's server, and holding an advisory lock
        and an open transaction across a twenty-second timeout would block
        scheduling behind a supplier's outage.
        """
        try:
            async with self.pool.connection() as connection:
                recovered = await health.check_recovered(connection)
            if recovered:
                LOGGER.warning("source.recovered_count", sources=recovered)
        except Exception:
            # A probe is somebody else's server; it must never stop a worker.
            LOGGER.warning("worker.probe_failed", exc_info=True)

    async def tick(self, spawn: set[asyncio.Task[bool]] | None = None) -> bool:
        """One pass: lead, recover, claim, run.

        With `spawn`, the claimed job is started as a task in that set and this
        returns whether one was claimed; the caller decides when to wait. Without
        it the job is run to completion inline, which is what `--once` and the
        tests want.
        """
        await self.lead()
        delivery = await self._broker.next_delivery(routes_for(self.state.capabilities))
        if delivery is None:
            return False
        try:
            async with self.pool.connection() as connection:
                reservation = await queue.reserve(
                    connection, delivery.envelope, self.state.id, self.state.capabilities
                )
        except psycopg.Error:
            LOGGER.warning("job.reservation_database_failed", exc_info=True)
            await delivery.retry(5.0)
            return True
        if reservation.disposition == "ack":
            await delivery.acknowledge()
            return True
        if reservation.disposition == "retry":
            await delivery.retry(reservation.retry_after)
            return True
        job = reservation.job
        assert job is not None
        self._deliveries[job.id] = delivery

        async def run_one() -> bool:
            # Set inside the task, so every line logged under this job — and
            # under any task it starts — carries the job it belongs to and no
            # other job's log sink accepts it. See `sink.JobLogHandler.emit`.
            CURRENT_JOB.set(str(job.id))
            with obs.bound(job_id=str(job.id), run_id=str(job.run_id), source=job.source_id):
                return await self.execute(job)

        if spawn is None:
            return await run_one()
        spawn.add(asyncio.create_task(run_one(), name=f"job:{job.source_id}"))
        return True

    async def execute(self, job: queue.ClaimedJob) -> bool:
        """Take a claimed job all the way to a terminal state."""
        self.state.current_jobs[job.id] = job
        self._cancels[job.id] = asyncio.Event()

        # Both keys this job has to be polite under: its own shop, and — for a
        # storefront family that answers from one provider's edge — that edge.
        # Taken in a fixed order, shop first, so two workers claiming the same
        # pair cannot deadlock against each other.
        # Asked of the source only if this worker still has one for it. A job
        # queued before a source was removed from sources.json is a job that
        # fails, further down and where a failure is recorded — not one that
        # raises out here, before the try, holding a lease nobody releases.
        keys = [job.host]
        known = self.sources.get(job.source_id)
        if known and (edge := scrapers.shared_edge(known.scraper)):
            keys.append(edge)

        async with self.pool.connection() as connection:
            for key in keys:
                if await leases.acquire(connection, key, job.id, self.state.id, job.execution_token) is None:
                    # Another worker is crawling this shop, or another shop on
                    # the same edge. Not an attempt: being polite must not spend
                    # a source's retry budget. Anything already taken for this
                    # job goes back, or the second key's contention would leak
                    # the first key's slot until its lease expired.
                    await leases.release_all(connection, job.id, job.execution_token)
                    await queue.release(connection, job, self.state.id, reason="host busy")
                    await self._deliveries[job.id].retry(queue.HOST_BACKOFF_SECONDS)
                    self._forget(job)
                    return False

        started = time.monotonic()
        with tracing.span(
            "job",
            **{
                "catalogue.source": job.source_id,
                "catalogue.run_id": str(job.run_id),
                "catalogue.attempt": job.attempt,
            },
        ):
            trace_id = tracing.trace_id()
            async with self.pool.connection() as connection:
                if not await queue.start(connection, job, self.state.id, trace_id=trace_id):
                    await leases.release_all(connection, job.id, job.execution_token)
                    await self._deliveries[job.id].acknowledge()
                    self._forget(job)
                    return False
                self._job_started[job.id] = started
                # Whichever worker gets there first moves the run out of
                # `queued`. It is conditional on the current status, so the
                # other seventy-nine jobs starting are no-ops rather than a
                # stream of redundant `run.started` edges.
                await runs.start_run(connection, job.run_id)
            await self.set_status("busy", job_id=job.id)

            try:
                await self._crawl_and_load(job)
            except BrowserUnavailable as error:
                if not await self._requeue_for_browser(job, error):
                    # Already required a browser and still could not get one, so
                    # rerouting it again would only find the same wall. This is
                    # the image being wrong, not the source, but a failed job an
                    # operator can see beats one circling the queue unnoticed.
                    LOGGER.exception("job.failed", source=job.source_id)
                    await self._finish(job, "failed", error=_durable_error(error))
            except Exception as error:
                LOGGER.exception("job.failed", source=job.source_id)
                await self._finish(job, "failed", error=_durable_error(error))
            finally:
                async with self.pool.connection() as connection:
                    await leases.release_all(connection, job.id, job.execution_token)
                delivery = self._deliveries.get(job.id)
                if delivery is not None:
                    # Terminal completion and capability rerouting both make
                    # this exact generation obsolete. CAS fencing makes a late
                    # ACK harmless if ownership was lost.
                    await delivery.acknowledge()
                self._job_started.pop(job.id, None)
                self._forget(job)
                # Another slot may still be crawling. A completed job used to
                # unconditionally publish `idle`, and the in-memory shortcut in
                # set_status could also leave current_job_id pointing at the job
                # that just finished. Re-project the aggregate process state.
                remaining = self.state.current_job
                await self.set_status(
                    "busy" if remaining else "idle",
                    job_id=remaining.id if remaining else None,
                    force=True,
                )
        return True

    def _browser_for_job(
        self,
        job: queue.ClaimedJob,
        params: CrawlParams,
        proxy_active: bool,
    ) -> tuple[BrowserBackend | None, BrowserJobContext | None]:
        """Resolve the snapshotted backend without inspecting scraper/platform names.

        Jobs created before exact backend routing default to Camoufox. Once
        `selected_browser_backend` exists it is lineage: an unavailable backend
        fails/requeues on that exact requirement rather than silently changing
        browser identity midway through a collection.
        """
        if params.browser == "never":
            return None, None
        try:
            selected = (
                BrowserBackendName(job.selected_browser_backend)
                if job.selected_browser_backend
                else BrowserBackendName.CAMOUFOX
            )
        except ValueError as error:
            raise BrowserUnavailable(
                f"unsupported selected browser backend {job.selected_browser_backend!r}"
            ) from error

        if selected == BrowserBackendName.CDP_EXTENSION_PROXY and proxy_active:
            raise BrowserUnavailable(
                "cdp_extension_proxy is direct-route only and cannot use a paid proxy lease"
            )
        backend = self._browser_backends.get(selected)
        if backend is None and selected == BrowserBackendName.CAMOUFOX:
            backend = BrowserRenderer(True, pages=self.settings.browser_pages)
            self._browser_backends[selected] = backend
        if backend is None:
            raise BrowserUnavailable(f"selected browser backend {selected.value!r} is unavailable")

        logical_profile: str | None = None
        if isinstance(backend, CdpExtensionProxyBackend):
            if backend.profile.allowed_worker_pool != self.settings.cdp_worker_pool:
                raise BrowserUnavailable("CDP profile is not allowed in this worker pool")
            logical_profile = backend.profile.name
        return backend, BrowserJobContext(str(job.id), logical_profile)

    async def _close_browser(self) -> None:
        """Shut every process-owned backend down. Idempotent and never fatal."""
        backends, self._browser_backends = self._browser_backends, {}
        for name, browser in backends.items():
            try:
                await browser.shutdown()
            except Exception:
                # A stuck browser must not block a drain: the jobs are already
                # back on the queue and the process is exiting regardless.
                LOGGER.warning("worker.browser_close_failed", backend=name.value, exc_info=True)

    async def _crawl_and_load(self, job: queue.ClaimedJob) -> None:
        """Collect one source, write its artifact, load it, and finish the job."""
        params = CrawlParams.from_job(job.params)
        config = self.sources[job.source_id]
        output = artifacts.job_directory(self.settings.dumps_dir, str(job.run_id), str(job.id))

        log_handler = JobLogHandler(job.id)
        obs.attach(log_handler)
        log_flusher = asyncio.create_task(self._flush_job_logs(log_handler), name=f"job-log:{job.source_id}")

        cancelled = False
        try:
            if params.pipeline == "connector_canary":
                await self._crawl_connector_canary(job, params, config)
                return
            async with self.pool.connection() as connection:
                proxy_lease = await self._proxy_lease(connection, job, config, params)
                browser_backend, browser_job = self._browser_for_job(
                    job, params, proxy_lease is not None
                )
                sink = PostgresSink(connection, job.run_id, {job.source_id: job.id})
                with RecordBuilder(self.sources.as_scraper_configs()):
                    try:
                        async with (
                            open_session(
                                params,
                                self.settings.cache_dir,
                                browser=None if proxy_lease else browser_backend,
                                proxy_lease=proxy_lease,
                                proxy_policy=str(job.proxy_snapshot.get("policy", "never")),
                                browser_job=browser_job,
                            ) as session,
                            Progress(1, [sink]) as progress,
                        ):
                            task = asyncio.create_task(
                                run_source(job.source_id, config, session, params, progress, None),
                                name=f"job:{job.source_id}",
                            )
                            watcher = asyncio.create_task(self._watch_for_cancel(job.id, task))
                            try:
                                outcome = await task
                            except asyncio.CancelledError:
                                cancelled = True
                                outcome = None
                            finally:
                                watcher.cancel()
                                with contextlib.suppress(asyncio.CancelledError):
                                    await watcher

                            if cancelled:
                                records = list(getattr(progress.results.get(job.source_id), "records", []))
                                artifact = artifacts.write_partial(output, job.source_id, records)
                                await self._finish(
                                    job,
                                    "cancelled",
                                    artifact=artifact,
                                    summary={"records": len(records), "interrupted": True},
                                )
                                return
                    finally:
                        if proxy_lease:
                            await close_reservation(connection, proxy_lease)

                assert outcome is not None
                # Asked before the artifact is written and before the load,
                # because both are the work of recording an outcome and a job
                # going back to the queue does not have one yet.
                if run_source_transient(outcome.summary) and await self._retry_transient(
                    connection, job, outcome.summary
                ):
                    return
                artifact = artifacts.write_source(output, job.source_id, outcome.records, params.allow_empty)
                outcome.summary["write_status"] = artifact.status
                await events.log(
                    connection,
                    job.id,
                    f"wrote {artifact.size} bytes to {artifact.path.name}",
                    event="job.artifact",
                    data={"sha256": artifact.sha256},
                )
                await log_handler.flush_to(connection)

            # The same question `plan_load` asks of a manifest entry, asked of
            # the source this worker has just collected. Decide the terminal
            # error before loading: a zero-record extraction failure is not a
            # complete empty catalogue and must never retire the last good run.
            whole, why_not, error = _legacy_load_plan(outcome.summary, artifact.status)
            if why_not:
                outcome.summary["retirement_withheld"] = why_not
                LOGGER.warning("job.adds_only", source=job.source_id, reason=why_not)
            load_started = time.monotonic()
            loaded = await self._load(job, outcome, whole=whole)
            outcome.summary["load_seconds"] = round(time.monotonic() - load_started, 6)
            outcome.summary["loaded"] = loaded.records
            outcome.summary["retired"] = loaded.retired
            if loaded.rejected:
                outcome.summary["rejected"] = loaded.rejected
                outcome.summary["rejects"] = loaded.rejects

            await self._finish(
                job,
                _legacy_terminal_state(error, loaded.rejected),
                summary=outcome.summary,
                artifact=artifact,
                error=error,
            )
        finally:
            obs.detach(log_handler)
            log_flusher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await log_flusher
            try:
                async with self.pool.connection() as connection:
                    await log_handler.flush_to(connection)
            except psycopg.Error:
                # Observability is best-effort. Losing the final buffered log
                # lines must never change a successfully loaded job into a
                # failure merely because the database was briefly unavailable.
                LOGGER.debug("sink.final_log_flush_failed", job_id=str(job.id), exc_info=True)

    async def _flush_job_logs(self, handler: JobLogHandler) -> None:
        """Persist a running job's buffered logs on a bounded cadence."""
        while True:
            await asyncio.sleep(THROTTLE_SECONDS)
            try:
                async with self.pool.connection() as connection:
                    await handler.flush_to(connection)
            except psycopg.Error:
                # The handler still owns its bounded pending rows. A transient
                # pool/database failure is retried on the next cadence.
                LOGGER.debug("sink.log_flush_failed", job_id=str(handler.job_id), exc_info=True)

    async def _crawl_connector_canary(self, job: queue.ClaimedJob, params: CrawlParams, config: Any) -> None:
        """Run a registered reusable connector only when explicitly selected."""
        adapter = runtime_plan(config)
        library_registry = self._commerce_runtime.registry

        registry = built_in_registry()
        current_ceramics = (
            "ceramics.catalogue_identity.v2" if config.identity_only else "ceramics.catalogue_item.v2"
        )
        selected = tuple(current_ceramics if name == "ceramics" else name for name in params.datasets)
        requested_fields = registry.collection_requirements(selected)[0]
        collection_plan = self._commerce_runtime.plan_collection(
            job.source_id,
            config,
            run=params,
            datasets=selected,
            requested_fields=requested_fields,
            result_limit=params.limit,
            cancelled=self._cancels[job.id].is_set,
            connector_plan=adapter,
        )
        configuration = collection_plan.configuration
        library_source = configuration.source
        route = collection_plan.route
        library_dynamic_partitions = route.dynamic_partitions
        definitions = tuple(registry.get(name) for name in selected)
        keys = {
            definition.name: ops_outputs.DatasetKey(
                definition.name, definition.version, definition.projector_version
            )
            for definition in definitions
        }
        ceramics_projection = configuration.projection_options
        # Library connectors enumerate a coherent neutral snapshot in FULL
        # mode. PRICE is application projection intent: the compatibility
        # record marker makes the loader preserve weekly descriptive fields.
        request = collection_plan.request
        library_request = collection_plan.library_request
        connector_name = library_source.connector
        connector_version = library_registry.connector_version(connector_name)
        connector_options = library_source.connector_options
        projection_configuration = {
            name: ceramics_projection if name.startswith("ceramics.") else {} for name in selected
        }
        dataset_selection = [
            {
                "dataset": key.dataset,
                "contract_version": key.contract_version,
                "projector_version": key.projector_version,
                "projection": projection_configuration[key.dataset],
            }
            for key in keys.values()
        ]
        dataset_fingerprint = _fingerprint(dataset_selection)
        store = LocalArtifactStore(self.settings.dumps_dir / "pipeline")

        async with self.pool.connection() as connection:
            resolved = await resolve_library_lineage(
                connection,
                job.id,
                spec=LibraryLineageSpec(
                    request=library_request,
                    connector=connector_name,
                    connector_version=connector_version,
                    connector_options=connector_options,
                    dataset_fingerprint=dataset_fingerprint,
                    dataset_selection=dataset_selection,
                    connector_configuration=collection_plan.connector_configuration,
                    budget_state={"request_budget": request.request_budget},
                ),
                datasets=list(keys.values()),
            )
            lineage = resolved.lineage
            library_checkpoint = resolved.checkpoint
            collection_limited = (
                resolved.progress is ops_outputs.LineageProgressState.TERMINAL_LIMITED
            )
            collection_complete = collection_limited or (
                resolved.progress is ops_outputs.LineageProgressState.TERMINAL_INTACT
            )
            restart_reason = (
                resolved.restart_reason.value if resolved.restart_reason else None
            )
            restored_states = await ops_outputs.pipeline_dataset_states(
                connection, job.id, list(keys.values())
            )

            initial_states = {
                name: DatasetPageState(state) for name, state in restored_states.items()
            }
            if collection_complete:
                result = PipelineResult(
                    pages=0,
                    terminal=True,
                    enumeration_intact=not collection_limited,
                    limited=collection_limited,
                    datasets=initial_states,
                )
                traffic_requests = 0
                rendered_pages = 0
                transport_totals = dict.fromkeys(TRANSPORT_TOTAL_NAMES, 0)
            else:
                native_proxy = resolve_native_proxy_runtime(
                    _BorrowedConnectionPool(connection),
                    job_id=job.id,
                    proxy_snapshot=job.proxy_snapshot,
                    settings=self.settings,
                    run_proxy_policy=params.proxy_policy,
                    run_proxy_max_megabytes=params.proxy_max_megabytes,
                    source=configuration.source,
                    source_policy=configuration.proxy,
                )
                def browser_binding() -> BorrowedBrowserBinding | None:
                    browser_backend, browser_job = self._browser_for_job(
                        job,
                        params,
                        native_proxy is not None,
                    )
                    return (
                        BorrowedBrowserBinding(browser_backend, browser_job)
                        if browser_backend is not None and browser_job is not None
                        else None
                    )

                assembly = self._commerce_runtime.assemble_collection(
                    collection_plan,
                    checkpoint=library_checkpoint,
                    cache_directory=self.settings.cache_dir,
                    collection_id=str(lineage),
                    proxy=native_proxy,
                    browser_factory=browser_binding,
                )
                async with self._commerce_runtime.open_collection(
                    assembly.spec,
                    assembly.routes,
                ) as opened:
                    native_connector = opened.connector
                    committer = ops_outputs.PostgresPageCommitter(
                        connection,
                        job.id,
                        lineage,
                        native_connector.version,
                        keys,
                        dynamic_partitions=library_dynamic_partitions,
                    )
                    async with asyncio.timeout(
                        configuration.fetch.timeout_seconds
                    ):
                        result = await ConnectorPipeline(
                            registry, store, committer
                        ).run(
                            job_id=str(job.id),
                            checkpoint_lineage=str(lineage),
                            connector=native_connector,
                            request=request,
                            datasets=selected,
                            checkpoint=None,
                            projection_configuration=projection_configuration,
                            initial_states=initial_states,
                        )
                transport_totals = opened.telemetry.transport_totals()
                traffic_requests = transport_totals["physical_requests"]
                rendered_pages = transport_totals["browser_requests"]

            summary: dict[str, Any] = {
                "source": job.source_id,
                "label": config.label,
                "scraper": f"{connector_name}-connector-canary",
                "runtime_format": ops_outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1.value,
                "connector": connector_name,
                "connector_version": connector_version,
                "checkpoint_lineage": str(lineage),
                "checkpoint_restart_reason": restart_reason,
                "terminal_recovery": collection_complete,
                "extraction_method": adapter.extraction_method,
                "records": 0,
                "discovered": 0,
                "requests": traffic_requests,
                "connector_pages": result.pages,
                "rendered_pages": rendered_pages,
                "transport": transport_totals,
                "truncated": not result.enumeration_intact,
                "error_count": 0,
                "errors": [],
                "notes": ["reusable connector canary"],
                "refresh_mode": params.refresh_mode,
                "field_coverage": {},
            }
            if self._cancels[job.id].is_set() or not result.terminal:
                for key in keys.values():
                    await ops_outputs.finish_dataset(
                        connection, job.id, key, state="cancelled", complete=False
                    )
                summary["interrupted"] = True
                await self._finish(job, "cancelled", summary=summary)
                return
            if not result.enumeration_intact and not result.limited:
                for key in keys.values():
                    await ops_outputs.finish_dataset(
                        connection,
                        job.id,
                        key,
                        state="failed",
                        complete=False,
                        error="connector enumeration incomplete",
                    )
                summary["error_count"] = 1
                await self._finish(job, "failed", summary=summary, error="connector enumeration incomplete")
                return

            checksum = await ops_outputs.lineage_checksum(connection, job.id, lineage)
            if result.limited:
                await ops_outputs.complete_limited_lineage(
                    connection,
                    job.id,
                    lineage,
                    checksum=checksum,
                )
            else:
                await ops_outputs.complete_lineage(
                    connection,
                    job.id,
                    lineage,
                    expected_partitions=(
                        await ops_outputs.declared_partitions(connection, job.id, lineage)
                        if library_dynamic_partitions
                        else library_request.partitions or ("main",)
                    ),
                    checksum=checksum,
                )
            published_by_dataset: dict[str, Any] = {}
            failed = [name for name, state in result.datasets.items() if state == DatasetPageState.FAILED]
            for name in failed:
                await ops_outputs.finish_dataset(
                    connection,
                    job.id,
                    keys[name],
                    state="failed",
                    complete=False,
                    error=f"{name} projector failed",
                )
            for name, key in keys.items():
                if name in failed:
                    continue
                published_by_dataset[name] = await ops_outputs.publish_dataset(
                    connection, store, job.id, lineage, key
                )
                if not name.startswith("ceramics."):
                    await ops_outputs.finish_dataset(
                        connection, job.id, key, state="succeeded", complete=True
                    )
            summary["datasets"] = {
                name: {
                    "state": "failed" if name in failed else "published",
                    "records": getattr(published_by_dataset.get(name), "records", 0),
                }
                for name in selected
            }
            summary["error_count"] = len(failed)

        ceramics_name = next((name for name in selected if name.startswith("ceramics.")), None)
        compatibility_artifact = None
        loaded = None
        if ceramics_name is not None and ceramics_name in published_by_dataset:
            published = published_by_dataset[ceramics_name]
            summary.update(
                {
                    "records": published.records,
                    "discovered": published.records,
                    "write_status": "replaced",
                    "artifact_path": published.location,
                    "artifact_sha256": published.sha256,
                    "artifact_size": published.size,
                }
            )
            if published.records == 0 and not params.allow_empty:
                async with self.pool.connection() as connection:
                    await ops_outputs.finish_dataset(
                        connection,
                        job.id,
                        keys[ceramics_name],
                        state="failed",
                        complete=True,
                        error="connector canary produced no ceramics records",
                    )
                await self._finish(
                    job, "failed", summary=summary, error="connector canary produced no ceramics records"
                )
                return
            try:
                loaded = await asyncio.to_thread(
                    self._load_connector_artifact,
                    job,
                    published.location,
                    _connector_load_is_whole(result),
                )
            except Exception as error:
                async with self.pool.connection() as connection:
                    await ops_outputs.finish_dataset(
                        connection,
                        job.id,
                        keys[ceramics_name],
                        state="failed",
                        complete=True,
                        error=_durable_error(error),
                    )
                raise
            summary.update(loaded=loaded.records, retired=loaded.retired, rejected=loaded.rejected)
            async with self.pool.connection() as connection:
                await ops_outputs.finish_dataset(
                    connection,
                    job.id,
                    keys[ceramics_name],
                    state="degraded" if loaded.rejected else "succeeded",
                    complete=True,
                    rejected=loaded.rejected,
                    error="database rejected projected records" if loaded.rejected else None,
                )
            compatibility_artifact = artifacts.Artifact(
                Path(published.location), published.sha256, published.size, "replaced"
            )

        succeeded = len(published_by_dataset)
        job_state = (
            "failed"
            if not succeeded
            else "degraded"
            if failed or (loaded and loaded.rejected)
            else "succeeded"
        )
        await self._finish(
            job,
            job_state,
            summary=summary,
            artifact=compatibility_artifact,
            error="all selected dataset projectors failed" if job_state == "failed" else None,
        )

    def _load_connector_artifact(
        self, job: queue.ClaimedJob, location: str, whole: bool
    ) -> postgres.SourceReport:
        from psycopg.rows import dict_row

        def records() -> Iterator[dict[str, Any]]:
            with gzip.open(location, "rt", encoding="utf-8") as source:
                for line in source:
                    yield json.loads(line)

        with psycopg.connect(self.settings.dsn, row_factory=dict_row, autocommit=True) as connection:
            return self._load_fenced(connection, job, records(), whole=whole)

    async def _proxy_lease(
        self,
        connection: Any,
        job: queue.ClaimedJob,
        config: Any,
        params: CrawlParams,
    ) -> ProxyLease | None:
        """Resolve operator policy without ever accepting a credential URL."""
        snapshot = job.proxy_snapshot
        policy = str(snapshot.get("policy", "never"))
        if policy == "never" or params.proxy_policy == "never" or not self.settings.proxy_enabled:
            return None
        if policy == "fallback":
            LOGGER.info("proxy.fallback_ready", source=job.source_id)
        secret_file = self.settings.proxy_secret_file
        if not secret_file:
            raise ProxyDenied("proxy is enabled but its secret file is absent")
        profiles = load_profiles(secret_file)
        obs.register_secrets(secret_values(profiles))
        logical_name = str(snapshot.get("profile", ""))
        profile = profiles.get(logical_name)
        if profile is None:
            raise ProxyDenied(f"unknown logical proxy profile {logical_name!r}")
        snapshotted_generation = snapshot.get("secret_generation")
        if (
            isinstance(snapshotted_generation, bool)
            or not isinstance(snapshotted_generation, int)
            or snapshotted_generation < 0
            or profile.generation != snapshotted_generation
        ):
            raise ProxyDenied(
                "proxy secret generation does not match the immutable job snapshot"
            )
        configured_maximum = int(snapshot.get("max_bytes", 0))
        if configured_maximum <= 0:
            raise ProxyDenied("job proxy snapshot has no valid byte maximum")
        maximum = min(
            configured_maximum,
            (params.proxy_max_megabytes * 1_000_000) if params.proxy_max_megabytes else configured_maximum,
        )
        reservation_id = await reserve(
            connection,
            job_id=job.id,
            profile=logical_name,
            profile_id=UUID(str(snapshot["profile_id"])),
            route_id=UUID(str(snapshot["route_id"])),
            requested_bytes=maximum,
            pilot=bool(snapshot.get("pilot", False)),
            secret_generation=profile.generation,
        )
        country = str(snapshot["country"]) if snapshot.get("country") else None
        return ProxyLease.build(
            reservation_id,
            job.id,
            profile,
            country,
            int(snapshot.get("session_minutes", 30)),
            maximum,
            str(snapshot.get("protocol", "http")),
        )

    async def _requeue_for_browser(self, job: queue.ClaimedJob, error: BrowserUnavailable) -> bool:
        """Send a job that turned out to need a browser to a worker that has one.

        Static adapter metadata reserves browser-only sources up front, but a
        plain source can escalate too: `pagecrawl` retries a page through the
        browser when it parses to nothing, which depends on what the shop
        served today. So the requirement cannot be fully known when the job is
        enqueued, and the honest place to discover it is here.

        Nothing collected so far is kept. The browser worker starts the source
        again from the beginning, which the response cache makes cheap, and a
        partial artifact from an aborted attempt would otherwise have to be
        reconciled with the complete one that replaces it.
        """
        async with self.pool.connection() as connection:
            return await queue.require_capability(
                connection,
                job,
                self.state.id,
                "browser",
                reason=_durable_error(error, max_length=200),
            )

    async def _retry_transient(
        self, connection: Any, job: queue.ClaimedJob, summary: dict[str, Any]
    ) -> bool:
        """Spend one of a job's remaining attempts on a host that refused it.

        `max_attempts` was only ever reachable through lease expiry — a worker
        that crashed or hung. A source that ran to completion and collected
        nothing because the host answered 429 finished normally, so it was
        recorded terminally on attempt 1 with two attempts unspent. On
        2026-08-19 that described every failure in the run: two Shopify stores
        rate-limiting the first request and one site whose database was down.

        The in-request backoff is the wrong instrument for this. It gets four
        tries inside about seven seconds, which is the right shape for a host
        pacing a crawl and the wrong one for a host refusing it outright; a
        429 that persists through the seventh second is usually still there at
        the thirtieth and usually gone at the three-hundredth. So the retry
        that matters is the whole source, later, and that is an attempt.

        Returns whether the job was requeued. False leaves the caller to record
        the failure exactly as before — the attempt budget being spent is a
        normal answer here, not an error.
        """
        if job.attempt >= job.max_attempts:
            return False
        # Widening with each attempt, because a host that was still refusing
        # five minutes later is evidence the wait was short, not that waiting
        # is futile.
        delay = queue.TRANSIENT_BACKOFF_SECONDS * job.attempt
        reason = _first_error(summary) or "host refused the request"
        await leases.release_all(connection, job.id, job.execution_token)
        if not await queue.release(
            connection, job, self.state.id, delay=delay, reason=reason[:200]
        ):
            # The lease moved on under us, so this worker no longer owns the
            # job and must not also ack its delivery.
            return False
        await events.log(
            connection,
            job.id,
            f"collected nothing and will retry in {delay}s "
            f"(attempt {job.attempt} of {job.max_attempts}): {reason}",
            event="job.retry_scheduled",
            level="warning",
            data={"attempt": job.attempt, "max_attempts": job.max_attempts, "delay": delay},
        )
        # Through `events.log` alone, as the rest of the job lifecycle is.
        # `JobLogHandler` also copies this module's logger into `job_events`,
        # so logging it as well would write the same retry to the job twice.
        delivery = self._deliveries.get(job.id)
        if delivery is not None:
            # Redelivery has to come from the broker; `release` only moved the
            # row. Held until after the row is queued so a redelivery cannot
            # arrive at a job that is still marked running.
            await delivery.retry(delay)
        # Ahead of `_run_job`'s finally, whose unconditional acknowledge would
        # otherwise consume the delivery this job still needs.
        self._forget(job)
        return True

    async def _watch_for_cancel(self, job_id: UUID, task: asyncio.Task[Any]) -> None:
        """Cancel one running source once the heartbeat sees its flag."""
        await self._cancels[job_id].wait()
        if not task.done():
            task.cancel()

    def _cancel(self, job_id: Any) -> None:
        """Raise the cancel flag for one job, if this process is running it."""
        event = self._cancels.get(UUID(str(job_id)))
        if event is not None:
            event.set()

    def _cancel_all(self) -> None:
        """Stop every source this process is carrying, e.g. on a second signal."""
        for event in self._cancels.values():
            event.set()

    def _forget(self, job: queue.ClaimedJob) -> None:
        self.state.current_jobs.pop(job.id, None)
        self._cancels.pop(job.id, None)
        self._deliveries.pop(job.id, None)

    async def _load(self, job: queue.ClaimedJob, outcome: Any, *, whole: bool) -> postgres.SourceReport:
        """Load this source's records, in a thread so the loop keeps beating.

        `storage.postgres` is synchronous — one transaction, a COPY and a
        `load_record` per row — and a 4,000-record source takes seconds. Running
        it inline would stall the event loop, and with it the heartbeat.
        """
        dsn = self.settings.dsn

        def load() -> postgres.SourceReport:
            from psycopg.rows import dict_row

            with psycopg.connect(dsn, row_factory=dict_row, autocommit=True) as connection:
                return self._load_fenced(
                    connection,
                    job,
                    outcome.records,
                    whole=whole,
                    stock_trends_enabled=self.settings.stock_trends_enabled,
                )

        return await asyncio.to_thread(load)

    @staticmethod
    def _load_fenced(
        connection: Any,
        job: queue.ClaimedJob,
        records: Any,
        *,
        whole: bool,
        stock_trends_enabled: bool = False,
    ) -> postgres.SourceReport:
        """Keep token replacement out of the material catalogue transaction."""
        with connection.transaction():
            connection.execute(
                "select pg_advisory_xact_lock(hashtextextended(%(id)s::text, 0))",
                {"id": job.id},
            )
            owned = connection.execute(
                "select 1 from catalogue.jobs where id=%(id)s and execution_token=%(token)s "
                "and state='running'",
                {"id": job.id, "token": job.execution_token},
            ).fetchone()
            if owned is None:
                raise RuntimeError("job execution token was lost before catalogue load")
            postgres.ensure_staging(connection)
            return postgres.load_source(
                connection,
                job.source_id,
                records,
                whole=whole,
                run_id=None,
                stock_trends_enabled=stock_trends_enabled,
            )

    async def _finish(
        self,
        job: queue.ClaimedJob,
        state: str,
        *,
        summary: dict[str, Any] | None = None,
        error: str | None = None,
        artifact: Any = None,
    ) -> None:
        async with self.pool.connection() as connection:
            flags = await queue.cancel_requested(connection, job.id)
            if state == "cancelled" and flags["pause"]:
                row = await connection.execute(
                    """update catalogue.jobs
                          set state = 'paused', pause_requested = false,
                              lease_owner = null, lease_expires_at = null,
                              execution_token = null,
                              summary = coalesce(%(summary)s, summary),
                              artifact_path = coalesce(%(path)s, artifact_path),
                              artifact_sha256 = coalesce(%(sha)s, artifact_sha256),
                              artifact_size = coalesce(%(size)s, artifact_size)
                        where id = %(id)s and execution_token = %(token)s
                      returning run_id, source_id""",
                    {
                        "id": job.id,
                        "token": job.execution_token,
                        "summary": Jsonb(summary) if summary is not None else None,
                        "path": str(artifact.path) if artifact else None,
                        "sha": getattr(artifact, "sha256", None) or None,
                        "size": getattr(artifact, "size", None),
                    },
                )
                paused = await row.fetchone()
                if paused is not None:
                    await events.emit(
                        connection,
                        events.Topic.JOB,
                        "job.paused",
                        run_id=paused["run_id"],
                        job_id=job.id,
                        source_id=paused["source_id"],
                    )
                return
            await runs.finish_job(
                connection,
                job.id,
                state=state,
                summary=summary,
                error=error,
                artifact=artifact,
                execution_token=job.execution_token,
            )
            snapshot = job.proxy_snapshot
            if snapshot.get("pilot") and snapshot.get("route_id") and snapshot.get("policy") != "never":
                evidence = await connection.execute(
                    """insert into catalogue.proxy_pilot_evidence
                               (job_id, source_id, route_id, succeeded, estimated_bytes, details)
                        select %(job)s, %(source)s, %(route)s, %(succeeded)s,
                               coalesce(r.estimated_bytes, 0), %(details)s
                          from catalogue.proxy_reservations r where r.job_id = %(job)s
                        on conflict (job_id) do nothing returning job_id""",
                    {
                        "job": job.id,
                        "source": job.source_id,
                        "route": UUID(str(snapshot["route_id"])),
                        "succeeded": state == "succeeded",
                        "details": Jsonb(
                            {
                                "records": int((summary or {}).get("records", 0)),
                                "error_count": int((summary or {}).get("error_count", 0)),
                            }
                        ),
                    },
                )
                if await evidence.fetchone() is not None:
                    await connection.execute(
                        # Promotion also ends the source's pilot enrolment. It
                        # did not before, so a promoted source went on
                        # reserving against the pilot ledger; when the pilot
                        # was later stopped, the-ceramic-shop's `always` policy
                        # had no fallback and the job died in nine
                        # milliseconds, every night, for five nights.
                        """update catalogue.source_proxy_policies p
                              set evidence_count = e.successes,
                                  evidence_state = case when e.successes >= 3 then 'promoted'
                                                        else 'eligible' end,
                                  pilot = p.pilot and e.successes < 3,
                                  updated_at = now()
                              from (select count(*) filter (where succeeded) as successes
                                      from catalogue.proxy_pilot_evidence
                                     where source_id = %(source)s) e
                             where p.source_id = %(source)s""",
                        {"source": job.source_id},
                    )
            if state == "failed":
                # `failed` is terminal on the first attempt — the retry budget
                # is spent by the browser requeue and lease-contention paths,
                # not by a source that simply failed. Gating this on
                # `attempt >= max_attempts` therefore meant the alert could
                # never fire: five sources failed nightly from 2026-08-13 and
                # not one raised a notification.
                await events.notify(
                    connection,
                    "job.failed",
                    f"{job.source_id} failed on attempt {job.attempt} of {job.max_attempts}",
                    body=error,
                    dedup_key=_JOB_FAILED_KEY.format(source=job.source_id),
                    run_id=job.run_id,
                    job_id=job.id,
                    source_id=job.source_id,
                )
            elif state == "succeeded":
                # The condition has ended, so the warning should stop being
                # shown. An alert that never clears is one nobody reads. The
                # key has to be the one `notify` stored, and the trailing colon
                # this used to carry matched nothing `notify` can produce.
                await events.resolve(
                    connection, _JOB_FAILED_KEY.format(source=job.source_id), source_id=job.source_id
                )
        # Emit only after every terminal side effect completed. If a database
        # write above fails, execute() records the job as failed; counting the
        # earlier intended outcome as well would double-count one completion.
        metrics.job_completed(job.source_id, state)
        if started := self._job_started.get(job.id):
            metrics.job_duration(job.source_id, time.monotonic() - started, state)

    # -- shutdown ---------------------------------------------------------

    async def shutdown(self) -> None:
        """Stop cleanly, giving back anything not finished."""
        self.state.stopping = True
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat

        try:
            async with self.pool.connection() as connection:
                for job in list(self.state.current_jobs.values()):
                    # Requeued rather than failed: the worker is going away, and
                    # that is not the source's fault, so no attempt is spent.
                    await leases.release_all(connection, job.id, job.execution_token)
                    await queue.release(connection, job, self.state.id, delay=0, reason="worker stopping")
                    delivery = self._deliveries.get(job.id)
                    if delivery is not None:
                        await delivery.retry(0)
                await connection.execute(
                    "update catalogue.workers set status = 'stopped', current_job_id = null, "
                    "last_heartbeat_at = now() where id = %(id)s",
                    {"id": self.state.id},
                )
                await events.emit(
                    connection,
                    events.Topic.WORKER,
                    "worker.stopped",
                    worker_id=self.state.id,
                    payload={"reason": self.state.desired_state},
                )
        except psycopg.Error:
            # Nothing left to do about it. The lease will expire and another
            # worker will pick the job up, which is the whole reason leases
            # expire rather than being released.
            LOGGER.warning("worker.shutdown_incomplete", exc_info=True)

        # After the jobs are back on the queue, and outside the database's
        # error path: a browser this process leaves running outlives the
        # container's stop grace period as an orphan.
        await self._close_browser()
        await self._broker.close()

        obs.unbind("worker_id")
        LOGGER.info("worker.stopping", reason=self.state.desired_state)

    def install_signal_handlers(self) -> None:
        """SIGTERM behaves like drain, bounded by the shutdown grace period.

        Containers and systemd send SIGTERM. Without this the process is killed
        mid-`write_source`, and the job stays `running` until its lease expires
        rather than going straight back on the queue.
        """
        loop = asyncio.get_running_loop()
        for name in ("SIGTERM", "SIGINT"):
            signum = getattr(signal, name, None)
            if signum is None:  # pragma: no cover - Windows
                continue
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(signum, self._on_signal, name)

    def _on_signal(self, name: str) -> None:
        if self.state.stopping:
            # A second signal means "now". Cancel the source through the safe
            # partial path rather than waiting out the grace period.
            LOGGER.warning("worker.stop_forced", signal=name)
            self._cancel_all()
            return
        LOGGER.info("worker.stopping", signal=name, grace=DRAIN_GRACE_SECONDS)
        self.state.stopping = True
        self.state.desired_state = "draining"
        loop = asyncio.get_running_loop()
        loop.call_later(DRAIN_GRACE_SECONDS, self._cancel_all)


def _first_error(summary: dict[str, Any]) -> str | None:
    errors = summary.get("errors") or []
    return (
        sanitize_diagnostic_text(
            str(errors[0].get("error")),
            max_length=2_000,
        )
        if errors
        else None
    )


def _durable_error(error: BaseException, *, max_length: int = 2_000) -> str:
    return sanitize_diagnostic_text(str(error), max_length=max_length)


def _legacy_terminal_state(error: str | None, rejected: int) -> str:
    if error:
        return "failed"
    return "degraded" if rejected else "succeeded"


def _legacy_load_plan(
    summary: dict[str, Any], write_status: str,
) -> tuple[bool, str | None, str | None]:
    """Plan retirement and failure together for the legacy worker path.

    A barren result used to be classified only after `_load`, which allowed an
    empty replacement artifact to retire every active product before the job
    was finally marked failed. The failure classification is therefore part of
    the load plan: failed zero-record outcomes are always adds-only.
    """
    error = None
    if summary.get("error_count") and not summary.get("records"):
        error = _first_error(summary) or "source collected no records after errors"
    elif nothing := run_source_barren(summary):
        error = nothing

    whole, why_not = postgres.may_retire(write_status, summary.get("truncated"))
    if error is not None:
        return False, "zero-record outcome failed validation", error
    return whole, why_not, None


def _fingerprint(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _connector_load_is_whole(result: PipelineResult) -> bool:
    """Retirement requires both a terminal cursor and intact enumeration."""
    return result.terminal and result.enumeration_intact


async def _one(
    connection: psycopg.AsyncConnection[dict[str, Any]], sql: str, params: Any = None
) -> dict[str, Any] | None:
    async with connection.cursor() as cursor:
        await cursor.execute(sql, params)
        return await cursor.fetchone()


def dumps_root(settings: Settings) -> Path:
    return settings.dumps_dir
