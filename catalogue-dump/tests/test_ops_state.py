"""Runs, jobs, edges and progress against a real PostgreSQL.

These need a database because what they are testing *is* the database: an
`on conflict do nothing` against a partial unique index, a `for update` that
serialises two finishers, a trigger that fires a notify. None of that can be
checked against a mock, and all of it is what the operations UI depends on.

Run them with `CATALOGUE_TEST_DSN` pointed at a throwaway PostgreSQL.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest
from mb_commerce_scraper import CollectionRequest as LibraryCollectionRequest

from mb_ceramics_catalogue import scrapers
from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.connectors.base import CollectionRequest, EntityPage, RefreshMode, SnapshotField
from mb_ceramics_catalogue.connectors.prestashop import (
    PrestaShopConnector,
    PrestaShopOptions,
    declared_partition_keys,
)
from mb_ceramics_catalogue.connectors.shopify import ShopifyConnector, ShopifyOptions
from mb_ceramics_catalogue.ops import events, leases, library_lineages, outputs, runs, worker
from mb_ceramics_catalogue.ops.sink import JobLogHandler, PostgresSink
from mb_ceramics_catalogue.pipeline.outputs import BatchIdentity, LocalArtifactStore, StoredBatch
from mb_ceramics_catalogue.pipeline.runner import DatasetPageOutcome, DatasetPageState
from mb_ceramics_catalogue.scrapers.activity import CURRENT_JOB
from mb_ceramics_catalogue.storage import db as storage_db

from .conftest import EXTENSIONS, requires_postgres
from .test_prestashop_connector import Transport as PrestaTransport
from .test_shopify_connector import FakeFetcher as ShopifyFetcher

pytestmark = [pytest.mark.postgres, requires_postgres]


SOURCES = SourcesFile.model_validate(
    {
        "les-cousins": {"label": "Les Cousins", "url": "https://lescousins.fr/", "scraper": "woocommerce"},
        "ceradel": {"label": "Ceradel", "url": "https://ceradel.fr/", "scraper": "shopify"},
        # Same host as les-cousins, to exercise the per-host stagger.
        "les-cousins-two": {"label": "LC2", "url": "https://lescousins.fr/other", "scraper": "woocommerce"},
        "browser-shop": {
            "label": "Ceramicolours",
            "url": "https://www.ceramicolours.it/",
            "scraper": "ceramicolours",
        },
    }
)


class FakeResult:
    """Stands in for a live `ScrapeResult`, which is all a sink ever reads."""

    def __init__(self, records: int = 0, requests: int = 0, errors: int = 0) -> None:
        self.records = [{"n": index} for index in range(records)]
        self.requests = requests
        self.errors = [{"url": "x", "error": "y"}] * errors
        self.rendered_pages = 0
        self.discovered = records
        self.truncated = False


async def register_worker(connection) -> UUID:
    """A worker row, because `host_leases.leased_by` references one."""
    worker_id = uuid4()
    await connection.execute(
        "insert into catalogue.workers (id, hostname, pid, capabilities, status) "
        "values (%(id)s, 'test', 1, '{}', 'idle')",
        {"id": worker_id},
    )
    return worker_id


async def rows(connection, sql, params=None):
    async with connection.cursor() as cursor:
        await cursor.execute(sql, params)
        return await cursor.fetchall()


class TestRunsAndJobs:
    async def test_a_run_fans_out_to_one_job_per_source(self, db):
        run_id = await runs.create_run(db, kind="manual", requested_by="tests")
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["les-cousins", "ceradel"])
        assert set(jobs) == {"les-cousins", "ceradel"}

        stored = {row["source_id"]: row for row in await rows(db, "select * from catalogue.jobs")}
        assert stored["ceradel"]["host"] == "ceradel.fr"
        assert stored["ceradel"]["state"] == "queued"
        assert stored["ceradel"]["attempt"] == 0

    async def test_browser_sources_declare_the_capability_they_need(self, db):
        """Only a worker started with `--capabilities browser` may claim these."""
        run_id = await runs.create_run(db)
        await runs.create_jobs(db, run_id, SOURCES, ["browser-shop", "ceradel"])
        stored = {row["source_id"]: row for row in await rows(db, "select * from catalogue.jobs")}
        assert stored["browser-shop"]["requires"] == ["browser"]
        assert stored["ceradel"]["requires"] == []

    async def test_two_sources_on_one_host_are_staggered(self, db):
        """Eighty jobs at 03:00:00 on eighty hosts is fine; two on one is not."""
        run_id = await runs.create_run(db)
        await runs.create_jobs(db, run_id, SOURCES, ["les-cousins", "les-cousins-two", "ceradel"])
        stored = {row["source_id"]: row for row in await rows(db, "select * from catalogue.jobs")}
        gap = stored["les-cousins-two"]["scheduled_for"] - stored["les-cousins"]["scheduled_for"]
        assert 25 <= gap.total_seconds() <= 35

        # A host with one source is not delayed at all. Compared as a tolerance
        # rather than an ordering: each insert evaluates its own `now()`, so the
        # row written last is a few milliseconds later without being staggered.
        undelayed = abs(
            (stored["ceradel"]["scheduled_for"] - stored["les-cousins"]["scheduled_for"]).total_seconds()
        )
        assert undelayed < 1

    async def test_a_disabled_source_gets_no_job(self, db):
        await db.execute(
            "insert into catalogue.source_settings (source_id, enabled) values ('ceradel', false)"
        )
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["les-cousins", "ceradel"])
        assert set(jobs) == {"les-cousins"}

    async def test_a_paused_source_still_gets_a_job(self, db):
        """Paused means 'not right now', not 'not part of runs'.

        The job exists and simply will not be claimed, so resuming the source
        lets the work it already had proceed.
        """
        await db.execute(
            "insert into catalogue.source_settings (source_id, paused) values ('ceradel', true)"
        )
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel"])
        assert set(jobs) == {"ceradel"}


class TestRunClosure:
    async def test_the_run_closes_when_its_last_job_finishes(self, db):
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["les-cousins", "ceradel"])
        await runs.start_run(db, run_id)

        first = await runs.finish_job(db, jobs["les-cousins"], state="succeeded",
                                      summary={"records": 10, "requests": 3})
        assert first is None, "the run must not close while a sibling is outstanding"

        last = await runs.finish_job(db, jobs["ceradel"], state="succeeded",
                                     summary={"records": 5, "requests": 2})
        assert last is not None
        assert last["status"] == "complete"
        assert last["records"] == 15

        progress = await rows(
            db,
            "select phase, records, requests, in_flight from catalogue.job_progress "
            "where job_id = %(id)s",
            {"id": jobs["ceradel"]},
        )
        assert progress == [{
            "phase": "succeeded", "records": 5, "requests": 2, "in_flight": [],
        }]

    async def test_a_run_with_one_failure_is_degraded_not_failed(self, db):
        """79 of 80 catalogues collected is not a failed run.

        Calling it failed is how an alert stops being believed.
        """
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["les-cousins", "ceradel"])
        await runs.finish_job(db, jobs["les-cousins"], state="succeeded", summary={"records": 10})
        result = await runs.finish_job(db, jobs["ceradel"], state="failed",
                                       summary={"records": 0}, error="the shop refused us")
        assert result["status"] == "degraded"

    async def test_a_run_where_everything_failed_is_failed(self, db):
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel"])
        result = await runs.finish_job(db, jobs["ceradel"], state="failed", summary={"records": 0})
        assert result["status"] == "failed"

    async def test_a_degraded_job_is_terminal_and_counted_separately(self, db):
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel"])
        result = await runs.finish_job(
            db, jobs["ceradel"], state="degraded", summary={"records": 9, "requests": 2}
        )
        assert result == {
            "status": "degraded", "succeeded": 0, "degraded": 1, "failed": 0,
            "cancelled": 0, "skipped": 0, "records": 9, "requests": 2,
        }
        stored = await rows(db, "select state from catalogue.jobs where id = %s", (jobs["ceradel"],))
        assert stored[0]["state"] == "degraded"

    async def test_a_degraded_job_does_not_leave_a_run_open(self, db):
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["les-cousins", "ceradel"])
        await runs.finish_job(db, jobs["les-cousins"], state="succeeded", summary={"records": 1})
        result = await runs.finish_job(db, jobs["ceradel"], state="degraded", summary={"records": 1})
        assert result is not None
        assert result["status"] == "degraded"
        assert result["succeeded"] == 1
        assert result["degraded"] == 1

    async def test_closing_a_run_promotes_what_it_collected(self, db):
        """A loaded source is only half the point.

        Until promotion runs, "les-cousins sells PRAI" and "sio-2 sells PRAI"
        are unrelated rows rather than one product two shops price. Nothing
        called it, so the cross-supplier join was as old as the last time a
        person ran it by hand.
        """
        await db.execute(
            "insert into catalogue.manufacturers (id, name) values ('sio-2', 'SIO-2') "
            "on conflict do nothing"
        )
        await db.execute(
            "insert into catalogue.manufacturer_aliases (alias, manufacturer_id) "
            "values ('sio-2', 'sio-2') on conflict do nothing"
        )
        for source in ("les-cousins", "ceradel"):
            await db.execute(
                "insert into catalogue.sources (id, label) values (%(source)s, %(source)s) "
                "on conflict do nothing",
                {"source": source},
            )
            await db.execute(
                """
                insert into catalogue.source_products
                       (source_id, external_id, record_format, product_url, name,
                        brand, manufacturer_sku, active, first_seen_at, last_seen_at)
                values (%(source)s, %(source)s || ':prai', 'ceramics.catalogue_item.v2',
                        'https://example.test/' || %(source)s || '/prai',
                        'white stoneware', 'SiO-2', 'PRAI', true, now(), now())
                """,
                {"source": source},
            )

        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["les-cousins", "ceradel"])
        await runs.finish_job(db, jobs["les-cousins"], state="succeeded", summary={"records": 1})
        await runs.finish_job(db, jobs["ceradel"], state="succeeded", summary={"records": 1})

        cursor = await db.execute(
            """
            select c.sku_key, count(distinct sp.source_id) as shops
              from catalogue.canonical_products c
              join catalogue.source_products sp on sp.canonical_product_id = c.id
             where c.manufacturer_id = 'sio-2'
             group by 1
            """
        )
        row = await cursor.fetchone()
        assert row is not None, "the run closed without promoting anything"
        assert row["sku_key"] == "PRAI"
        assert row["shops"] == 2, "both shops must land on the one product"

    async def test_a_run_closes_even_when_promotion_cannot(self, db):
        """A database predating the promotion schema has no such function.

        The run's outcome is already committed by then and is not in question
        because a derived table could not be rebuilt.
        """
        await db.execute("drop function if exists catalogue.promote_canonical_products(text)")
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel"])
        result = await runs.finish_job(db, jobs["ceradel"], state="succeeded",
                                       summary={"records": 1})
        assert result is not None
        assert result["status"] == "complete"

    async def test_concurrent_finishers_close_the_run_exactly_once(self, db):
        """Two workers finishing their last jobs at the same moment.

        Without the `for update` on the run row both see "no outstanding
        siblings", both compute a summary, and the recorded outcome is whichever
        committed second. The `run.complete` edge would also appear twice.
        """
        import psycopg
        from psycopg.rows import dict_row

        from .conftest import postgres_dsn

        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["les-cousins", "ceradel"])

        dsn = postgres_dsn()
        assert dsn is not None

        async def finish(source: str):
            async with await psycopg.AsyncConnection.connect(
                dsn, row_factory=dict_row, autocommit=True
            ) as connection:
                return await runs.finish_job(
                    connection, jobs[source], state="succeeded", summary={"records": 1}
                )

        results = await asyncio.gather(finish("les-cousins"), finish("ceradel"))
        closed = [result for result in results if result is not None]
        assert len(closed) == 1, "the run closed more than once"

        completions = await rows(
            db, "select id from catalogue.event_log where type like 'run.complete%'"
        )
        assert len(completions) == 1


class TestEdgesAndLevels:
    async def test_progress_never_reaches_the_event_log(self, db):
        """The load-bearing part of the SSE design (§3.1).

        If progress is ever written to `event_log` "for consistency", a
        three-hour run puts ~860,000 rows in it and replay stops working.
        """
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel"])
        sink = PostgresSink(db, run_id, jobs, throttle=0)

        await sink.started("ceradel", FakeResult(), "shopify", "api_json")
        for count in range(1, 40):
            await sink.progress("ceradel", FakeResult(records=count, requests=count))

        logged = await rows(db, "select type from catalogue.event_log")
        assert not any("progress" in row["type"] for row in logged)

        current = await rows(db, "select * from catalogue.job_progress")
        assert len(current) == 1, "progress is one row per job, updated in place"
        assert current[0]["records"] == 39

    async def test_progress_writes_are_throttled(self, db):
        """3,000 requests must not become 3,000 writes."""
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel"])
        sink = PostgresSink(db, run_id, jobs, throttle=60)

        await sink.started("ceradel", FakeResult(), "shopify", "api_json")
        for count in range(1, 100):
            await sink.progress("ceradel", FakeResult(records=count))

        current = await rows(db, "select records from catalogue.job_progress")
        # The `started` write went through; every subsequent one inside the
        # window was dropped. The counters are cumulative, so nothing is lost.
        assert current[0]["records"] == 0

    async def test_an_edge_is_ordered_by_one_sequence(self, db):
        first = await events.emit(db, events.Topic.WORKER, "worker.ready")
        second = await events.emit(db, events.Topic.JOB, "job.leased")
        assert second > first

    async def test_the_event_log_trigger_notifies_with_the_id_alone(self, db):
        """A payload carrying `in_flight` would exceed the 8000-byte notify cap
        and fail the insert; the id is a hint to go and read."""
        await db.execute("listen catalogue_ops")
        event_id = await events.emit(
            db, events.Topic.JOB, "job.failed", payload={"big": "x" * 9000}
        )
        received = [note.payload async for note in db.notifies(timeout=2, stop_after=1)]
        assert received == [str(event_id)]

    async def test_job_progress_notifies_on_its_own_channel(self, db):
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel"])
        await db.execute("listen catalogue_progress")
        sink = PostgresSink(db, run_id, jobs, throttle=0)
        await sink.started("ceradel", FakeResult(), "shopify", "api_json")
        received = [note.payload async for note in db.notifies(timeout=2, stop_after=1)]
        assert received == [str(jobs["ceradel"])]


class TestNotifications:
    async def test_the_same_open_condition_is_raised_once(self, db):
        """Three retries of one source at 03:00 are one notification."""
        first = await events.notify(db, "job.failed", "ceradel failed", source_id="ceradel")
        second = await events.notify(db, "job.failed", "ceradel failed again", source_id="ceradel")
        assert first is not None
        assert second is None

    async def test_a_resolved_condition_may_recur(self, db):
        """A dedup key is a deduplicator, not a mute."""
        await events.notify(db, "source.stale", "ceradel is stale", source_id="ceradel")
        assert await events.resolve(db, "source.stale:ceradel", source_id="ceradel")
        again = await events.notify(db, "source.stale", "ceradel is stale", source_id="ceradel")
        assert again is not None

    async def test_a_failed_job_alert_is_cleared_by_the_next_success(self, db):
        """Raise and clear have to agree on the key, and they did not.

        The worker raised on the default key and cleared `job.failed:<source>:`,
        a string `notify` cannot produce, so the alert — when it fired at all —
        stayed on the operator's screen for ever.
        """
        key = worker._JOB_FAILED_KEY.format(source="ceradel")
        raised = await events.notify(
            db, "job.failed", "ceradel failed on attempt 1 of 3",
            dedup_key=key, source_id="ceradel",
        )
        assert raised is not None
        assert await events.resolve(db, key, source_id="ceradel")

    async def test_acknowledging_is_idempotent(self, db):
        notification_id = await events.notify(db, "worker.lost", "worker gone",
                                              severity=events.Severity.CRITICAL)
        assert await events.acknowledge(db, notification_id, "rick")
        assert not await events.acknowledge(db, notification_id, "rick")

    async def test_selected_notifications_are_acknowledged_with_edges(self, db):
        first = await events.notify(db, "source.stale", "one", source_id="one")
        second = await events.notify(db, "source.stale", "two", source_id="two")
        assert first is not None and second is not None

        acknowledged = await events.acknowledge_many(db, [second, first, second], "rick")

        assert acknowledged == sorted([first, second])
        logged = await rows(
            db,
            "select source_id, payload from catalogue.event_log "
            "where type = 'notification.acknowledged' order by source_id",
        )
        assert [row["source_id"] for row in logged] == ["one", "two"]
        assert {row["payload"]["id"] for row in logged} == {first, second}

    async def test_raising_one_emits_an_edge(self, db):
        await events.notify(db, "host.blocking", "ceradel.fr is refusing us",
                            severity=events.Severity.CRITICAL)
        logged = await rows(db, "select type, payload from catalogue.event_log")
        assert any(row["type"] == "notification.raised" for row in logged)


def line(message: Any, level: int = logging.INFO) -> logging.LogRecord:
    return logging.LogRecord("catalogue", level, __file__, 1, message, None, None)


class TestJobLog:
    async def test_lines_reach_job_events(self, db):
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel"])
        handler = JobLogHandler(jobs["ceradel"])

        CURRENT_JOB.set(str(jobs["ceradel"]))
        handler.emit(line("host=ceradel.fr failed (429)", logging.WARNING))
        assert await handler.flush_to(db) == 1

        stored = await rows(db, "select level, message from catalogue.job_events")
        assert stored[0]["level"] == "warning"
        assert "429" in stored[0]["message"]

    async def test_structured_context_reaches_job_events_scrubbed(self, db):
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel"])
        handler = JobLogHandler(jobs["ceradel"])

        CURRENT_JOB.set(str(jobs["ceradel"]))
        record = line(
            {
                "event": "fetch.failed",
                "source": "ceradel",
                "host": "ceradel.fr",
                "request_id": "request-1",
                "authorization": "Bearer should-not-be-stored",
            },
            logging.ERROR,
        )
        handler.emit(record)
        assert await handler.flush_to(db) == 1

        stored = await rows(db, "select event, data from catalogue.job_events")
        assert stored[0]["event"] == "fetch.failed"
        assert stored[0]["data"] == {
            "host": "ceradel.fr",
            "request_id": "request-1",
            "source": "ceradel",
        }

    async def test_a_runaway_job_cannot_fill_the_queue(self, db):
        job_id = uuid4()
        handler = JobLogHandler(job_id, capacity=5)
        CURRENT_JOB.set(str(job_id))
        for index in range(50):
            handler.emit(line(f"line {index}"))
        drained = handler.drain()
        assert len(drained) == 6, "five lines plus one saying what was dropped"
        assert "dropped" in drained[-1][2]

    async def test_a_handler_takes_only_its_own_job_s_lines(self):
        """A worker with four job slots has four of these on the root logger.

        Every one of them was offered every record, so each job's log page
        showed all four jobs' lines — with the other jobs' ids inside the
        messages, which is at its most misleading exactly when someone is
        reading the page to find out why a job failed.
        """
        mine, theirs = uuid4(), uuid4()
        handler = JobLogHandler(mine)

        CURRENT_JOB.set(str(theirs))
        handler.emit(line("something the other job did"))
        assert handler.drain() == []

        CURRENT_JOB.set(str(mine))
        handler.emit(line("something this job did"))
        assert [entry[2] for entry in handler.drain()] == ["something this job did"]

    async def test_a_line_belonging_to_no_job_reaches_no_job_s_log(self):
        """The heartbeat and the queue are the worker's, not any one job's."""
        handler = JobLogHandler(uuid4())
        CURRENT_JOB.set("")
        handler.emit(line("worker.tick"))
        assert handler.drain() == []


class TestHostSlots:
    async def test_reconciling_creates_one_slot_per_unit_of_concurrency(self, db):
        await db.execute("insert into catalogue.hosts (host, max_concurrency) values ('ceradel.fr', 3)")
        await db.execute("select catalogue.reconcile_host_slots('ceradel.fr')")
        slots = await rows(db, "select slot from catalogue.host_leases order by slot")
        assert [row["slot"] for row in slots] == [1, 2, 3]

    async def test_an_unknown_host_gets_a_default_of_one(self, db):
        """One request at a time is the right default for a shop."""
        await db.execute("select catalogue.reconcile_host_slots('new-shop.test')")
        slots = await rows(db, "select slot from catalogue.host_leases where host='new-shop.test'")
        assert len(slots) == 1

    async def test_lowering_the_limit_never_takes_a_slot_from_a_running_job(self, db):
        """Taking the slot away does not stop the requests already in flight."""
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel"])
        await db.execute("insert into catalogue.hosts (host, max_concurrency) values ('ceradel.fr', 3)")
        await db.execute("select catalogue.reconcile_host_slots('ceradel.fr')")
        await db.execute(
            "update catalogue.host_leases set job_id = %(job)s where host='ceradel.fr' and slot = 3",
            {"job": jobs["ceradel"]},
        )

        await db.execute("update catalogue.hosts set max_concurrency = 1 where host='ceradel.fr'")
        await db.execute("select catalogue.reconcile_host_slots('ceradel.fr')")

        remaining = await rows(
            db, "select slot, job_id from catalogue.host_leases where host='ceradel.fr' order by slot"
        )
        assert [row["slot"] for row in remaining] == [1, 3], "the occupied slot survived"

    async def test_two_shops_on_one_edge_cannot_run_at_once(self, db):
        """Politeness is per host, and a Shopify shop's host is not the whole story.

        Nineteen of these shops are Shopify storefronts on custom domains, all
        answering from one edge that meters by client address across every shop
        on it. Two of them crawled concurrently from one machine is the shape of
        the 2026-08-12 failure, and it looks perfectly polite per host.
        """
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel", "les-cousins"])
        edge = scrapers.shared_edge("shopify")
        assert edge is not None

        first, second = await register_worker(db), await register_worker(db)
        first_token, second_token = uuid4(), uuid4()
        assert await leases.acquire(
            db, "ceradel.fr", jobs["ceradel"], first, first_token
        ) is not None
        assert await leases.acquire(db, edge, jobs["ceradel"], first, first_token) is not None

        # A different shop, its own host free, but the edge is taken.
        assert await leases.acquire(
            db, "lescousins.fr", jobs["les-cousins"], second, second_token
        ) is not None
        assert await leases.acquire(db, edge, jobs["les-cousins"], second, second_token) is None

        # The first job going away frees both of its keys, whichever it holds.
        assert set(await leases.release_all(db, jobs["ceradel"], first_token)) == {
            "ceradel.fr", edge
        }
        assert await leases.acquire(
            db, edge, jobs["les-cousins"], second, second_token
        ) is not None

    async def test_an_operator_can_widen_the_edge_without_a_deploy(self):
        """It is an ordinary row in `catalogue.hosts`, so it tunes like one."""
        assert scrapers.shared_edge("shopify") == "edge:shopify"
        assert scrapers.shared_edge("woocommerce") is None
        # `sio2` is a PrestaShop under another name: the class decides, not the key.
        assert scrapers.shared_edge("sio2") is None


class TestImportRunLink:
    async def test_a_load_can_be_traced_to_the_crawl_that_produced_it(self, db):
        run_id = await runs.create_run(db)
        await db.execute(
            "insert into catalogue.import_runs (status, importer_version, run_id) "
            "values ('complete', 'tests', %(run)s)",
            {"run": run_id},
        )
        found = await rows(
            db, "select run_id from catalogue.import_runs where run_id = %(run)s", {"run": run_id}
        )
        assert len(found) == 1


class TestCheckpointOutputs:
    async def connector_lineage(self, db, connector, partitions):
        run_id = await runs.create_run(db)
        job_id = (await runs.create_jobs(db, run_id, SOURCES, ["ceradel"]))["ceradel"]
        lineage = await outputs.create_lineage(
            db, job_id, source_id="ceradel", source_url="https://shop.test/",
            connector=connector, connector_version="1",
            connector_configuration={"partitions": list(partitions)},
            connector_config_fingerprint="a" * 64, dataset_fingerprint="b" * 64,
            dataset_selection=[],
        )
        return job_id, lineage

    async def test_library_lineage_identity_round_trips_and_never_matches_legacy(
        self, db
    ):
        run_id = await runs.create_run(db)
        job_id = (await runs.create_jobs(db, run_id, SOURCES, ["ceradel"]))[
            "ceradel"
        ]
        durable_request = {
            "source_id": "ceradel",
            "base_url": "https://shop.test/",
            "refresh_mode": "full",
            "requested_fields": ["identity"],
        }
        durable_options = {"currency": "EUR", "page_limit": 50}
        lineage = await outputs.create_lineage(
            db,
            job_id,
            source_id="ceradel",
            source_url="https://shop.test/",
            connector="shopify",
            connector_version="1",
            connector_config_fingerprint="a" * 64,
            dataset_fingerprint="b" * 64,
            dataset_selection=[],
            runtime_format=outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
            collection_request=durable_request,
            connector_options=durable_options,
        )

        assert await outputs.lineage_runtime_configuration(
            db, job_id, lineage
        ) == outputs.LineageRuntimeConfiguration(
            runtime_format=outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
            collection_request=durable_request,
            connector_options=durable_options,
        )
        lookup = {
            "source_url": "https://shop.test/",
            "connector": "shopify",
            "connector_version": "1",
            "connector_config_fingerprint": "a" * 64,
            "dataset_fingerprint": "b" * 64,
        }
        assert await outputs.find_compatible_lineage(db, job_id, **lookup) is None
        assert await outputs.find_compatible_lineage(
            db,
            job_id,
            **lookup,
            runtime_format=outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
        ) == lineage

        assert await outputs.reject_lineage(db, job_id, lineage)
        assert not await outputs.reject_lineage(db, job_id, lineage)
        assert (
            await outputs.find_compatible_lineage(
                db,
                job_id,
                **lookup,
                runtime_format=outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
            )
            is None
        )

    async def test_library_resolver_resumes_cursor_and_fences_option_drift(self, db):
        run_id = await runs.create_run(db)
        job_id = (await runs.create_jobs(db, run_id, SOURCES, ["ceradel"]))[
            "ceradel"
        ]
        request = LibraryCollectionRequest(
            source_id="ceradel",
            base_url="https://shop.test/",
        )
        dataset = outputs.DatasetKey("ceramics", "2", "projector-1")

        def spec(page_limit: int) -> library_lineages.LibraryLineageSpec:
            return library_lineages.LibraryLineageSpec(
                request=request,
                connector="shopify",
                connector_version="1",
                connector_options={"page_limit": page_limit},
                connector_configuration={"partitions": ["main"]},
                dataset_fingerprint="d" * 64,
                dataset_selection=[
                    {
                        "dataset": dataset.dataset,
                        "contract_version": dataset.contract_version,
                        "projector_version": dataset.projector_version,
                    }
                ],
            )

        original_spec = spec(50)
        created = await library_lineages.resolve_library_lineage(
            db,
            job_id,
            spec=original_spec,
            datasets=[dataset],
        )
        assert not created.resuming
        assert created.checkpoint is None
        assert created.progress is outputs.LineageProgressState.EMPTY

        await outputs.commit_page(
            db,
            job_id,
            created.lineage,
            partition_key="main",
            page_id="main:1",
            page_sequence=0,
            resume_after={"partition": "main", "page": 2},
            terminal=False,
            enumeration_intact=True,
            connector_version="1",
            batches=[],
        )
        await db.execute(
            """update catalogue.job_datasets
                  set state = 'degraded', records = 7, rejected = 2,
                      error = 'interrupted'
                where job_id = %s and dataset = %s and contract_version = %s
                  and projector_version = %s""",
            (
                job_id,
                dataset.dataset,
                dataset.contract_version,
                dataset.projector_version,
            ),
        )

        resumed = await library_lineages.resolve_library_lineage(
            db,
            job_id,
            spec=original_spec,
            datasets=[dataset],
        )
        assert resumed.lineage == created.lineage
        assert resumed.resuming
        assert resumed.restart_reason is None
        assert resumed.progress is outputs.LineageProgressState.RESUMABLE
        assert resumed.checkpoint is not None
        assert resumed.checkpoint.collection_fingerprint == (
            original_spec.connector_config_fingerprint
        )
        assert resumed.checkpoint.resume_after == {"partition": "main", "page": 2}
        assert await rows(
            db,
            """select state, records, rejected, error
                 from catalogue.job_datasets
                where job_id = %(job)s and dataset = %(dataset)s""",
            {"job": job_id, "dataset": dataset.dataset},
        ) == [{"state": "staged", "records": 7, "rejected": 2, "error": None}]

        drifted_spec = spec(51)
        restarted = await library_lineages.resolve_library_lineage(
            db,
            job_id,
            spec=drifted_spec,
            datasets=[dataset],
        )
        assert restarted.lineage != created.lineage
        assert not restarted.resuming
        assert restarted.checkpoint is None
        assert restarted.progress is outputs.LineageProgressState.EMPTY
        assert await rows(
            db,
            """select checkpoint_lineage, status
                 from catalogue.job_checkpoint_lineages
                where job_id = %(job)s
                order by created_at""",
            {"job": job_id},
        ) == [
            {"checkpoint_lineage": created.lineage, "status": "rejected"},
            {"checkpoint_lineage": restarted.lineage, "status": "active"},
        ]
        assert await outputs.lineage_runtime_configuration(
            db, job_id, restarted.lineage
        ) == outputs.LineageRuntimeConfiguration(
            runtime_format=outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
            collection_request=request.model_dump(mode="json"),
            connector_options=drifted_spec.connector_options,
        )
        assert await rows(
            db,
            """select state, records, rejected, error
                 from catalogue.job_datasets
                where job_id = %(job)s and dataset = %(dataset)s""",
            {"job": job_id, "dataset": dataset.dataset},
        ) == [{"state": "pending", "records": 0, "rejected": 0, "error": None}]

    async def test_completed_library_lineage_remains_recoverable_for_publication(
        self, db
    ):
        run_id = await runs.create_run(db)
        job_id = (await runs.create_jobs(db, run_id, SOURCES, ["ceradel"]))[
            "ceradel"
        ]
        lookup = {
            "source_url": "https://shop.test/",
            "connector": "shopify",
            "connector_version": "1",
            "connector_config_fingerprint": "a" * 64,
            "dataset_fingerprint": "b" * 64,
        }
        lineage = await outputs.create_lineage(
            db,
            job_id,
            source_id="ceradel",
            connector_configuration={"partitions": ["main"]},
            dataset_selection=[],
            runtime_format=outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
            collection_request={
                "source_id": "ceradel",
                "base_url": "https://shop.test/",
                "requested_fields": ["identity"],
            },
            connector_options={"page_limit": 50},
            **lookup,
        )
        await outputs.commit_page(
            db,
            job_id,
            lineage,
            partition_key="main",
            page_id="main:1",
            page_sequence=0,
            resume_after=None,
            terminal=True,
            enumeration_intact=True,
            connector_version="1",
            batches=[],
        )
        checksum = await outputs.lineage_checksum(db, job_id, lineage)
        await outputs.complete_lineage(
            db, job_id, lineage, expected_partitions=("main",), checksum=checksum
        )

        assert await outputs.find_compatible_lineage(
            db,
            job_id,
            runtime_format=outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
            **lookup,
        ) is None
        assert await outputs.find_recoverable_library_lineage(
            db, job_id, **lookup
        ) == lineage
        assert await outputs.lineage_progress(
            db, job_id, lineage
        ) == outputs.LineageProgress(outputs.LineageProgressState.TERMINAL_INTACT)

    async def test_shopify_crash_between_partitions_resumes_without_refetch(self, db):
        partitions = ("zeta", "alpha")
        job_id, lineage = await self.connector_lineage(db, "shopify", partitions)
        request = CollectionRequest(
            source_id="ceradel", base_url="https://shop.test/", refresh_mode=RefreshMode.FULL,
            requested_fields=frozenset({SnapshotField.IDENTITY}), collections=partitions,
        )
        first_fetcher = ShopifyFetcher([{"products": []}])
        first = await anext(ShopifyConnector(first_fetcher, ShopifyOptions()).collect(request))
        assert first.partition_key == "zeta" and first.partition_terminal and not first.terminal
        committer = outputs.PostgresPageCommitter(db, job_id, lineage, "1", {})
        await committer.commit_page(first, [], [])
        checkpoint = await outputs.resume_checkpoint(db, job_id, lineage)
        assert checkpoint is not None
        assert checkpoint.resume_after == {"partition": "alpha", "page": 1}

        resumed_fetcher = ShopifyFetcher([{"products": []}])
        resumed = [
            page async for page in ShopifyConnector(
                resumed_fetcher, ShopifyOptions()
            ).collect(request, checkpoint)
        ]
        assert [page.partition_key for page in resumed] == ["alpha"]
        await committer.commit_page(resumed[0], [], [])
        checksum = await outputs.lineage_checksum(db, job_id, lineage)
        await outputs.complete_lineage(
            db, job_id, lineage, expected_partitions=partitions, checksum=checksum
        )

    async def test_prestashop_crash_between_hashed_roots_resumes_exactly(self, db):
        categories = ("https://shop.test/zeta", "https://shop.test/alpha")
        options = PrestaShopOptions(
            category_urls=categories, use_advertised_sitemaps=False,
            product_pattern=r"/product/", variant_combinations=False,
        )
        partitions = declared_partition_keys(options, "https://shop.test/")
        job_id, lineage = await self.connector_lineage(db, "prestashop", partitions)
        documents = {
            categories[0]: '<a href="/product/1.html">one</a>',
            categories[1]: '<a href="/product/2.html">two</a>',
            "https://shop.test/product/1.html": "<html></html>",
            "https://shop.test/product/2.html": "<html></html>",
        }
        request = CollectionRequest(
            source_id="ceradel", base_url="https://shop.test/", refresh_mode=RefreshMode.FULL,
            requested_fields=frozenset({SnapshotField.IDENTITY}),
        )
        first_transport = PrestaTransport(documents)
        first = await anext(PrestaShopConnector(first_transport, options).collect(request))
        assert first.partition_key == partitions[0] and first.partition_terminal
        committer = outputs.PostgresPageCommitter(db, job_id, lineage, "1", {})
        await committer.commit_page(first, [], [])
        checkpoint = await outputs.resume_checkpoint(db, job_id, lineage)
        assert checkpoint is not None
        assert checkpoint.resume_after == {
            "partition": partitions[1], "offset": 0, "sequence": 1
        }

        resumed_transport = PrestaTransport(documents)
        resumed = [
            page async for page in PrestaShopConnector(
                resumed_transport, options
            ).collect(request, checkpoint)
        ]
        assert [page.partition_key for page in resumed] == [partitions[1]]
        assert not any(call[0].endswith("/product/1.html") for call in resumed_transport.calls)
        await committer.commit_page(resumed[0], [], [])
        checksum = await outputs.lineage_checksum(db, job_id, lineage)
        await outputs.complete_lineage(
            db, job_id, lineage, expected_partitions=partitions, checksum=checksum
        )

    async def prepared(self, db):
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel"])
        job_id = jobs["ceradel"]
        key = outputs.DatasetKey("ceramics", "v2", "projector-1")
        await outputs.declare_dataset(db, job_id, key)
        lineage = await outputs.create_lineage(
            db,
            job_id,
            source_id="ceradel",
            source_url="https://ceradel.fr/",
            connector="shopify",
            connector_version="1",
            connector_config_fingerprint="c" * 64,
            dataset_fingerprint="d" * 64,
            dataset_selection=[{"dataset": "ceramics", "contract_version": "v2"}],
        )
        return job_id, key, lineage

    async def test_replaying_an_identical_page_is_idempotent(self, db):
        job_id, key, lineage = await self.prepared(db)
        batch = outputs.PageBatch(key, "stage/page-0", "e" * 64, 20, 2)
        arguments = {
            "partition_key": "catalogue",
            "page_id": "page-0",
            "page_sequence": 0,
            "resume_after": {"cursor": 1},
            "terminal": False,
            "enumeration_intact": True,
            "connector_version": "1",
            "batches": [batch],
        }
        assert await outputs.commit_page(db, job_id, lineage, **arguments)
        assert not await outputs.commit_page(db, job_id, lineage, **arguments)
        stored = await rows(db, "select records from catalogue.job_datasets where job_id = %s", (job_id,))
        assert stored[0]["records"] == 2

    async def test_a_replayed_page_with_different_output_is_rejected(self, db):
        job_id, key, lineage = await self.prepared(db)
        common = {
            "partition_key": "catalogue", "page_id": "page-0", "page_sequence": 0,
            "resume_after": None, "terminal": True, "enumeration_intact": True,
            "connector_version": "1",
        }
        await outputs.commit_page(
            db, job_id, lineage,
            batches=[outputs.PageBatch(key, "stage/page-0", "e" * 64, 20, 2)], **common,
        )
        with pytest.raises(ValueError, match="differs"):
            await outputs.commit_page(
                db, job_id, lineage,
                batches=[outputs.PageBatch(key, "stage/page-0", "f" * 64, 20, 2)], **common,
            )

    async def test_page_sequence_must_advance_and_terminal_stops_the_partition(self, db):
        job_id, _, lineage = await self.prepared(db)
        await outputs.commit_page(
            db, job_id, lineage, partition_key="catalogue", page_id="page-2",
            page_sequence=2, resume_after={"cursor": 2}, terminal=False,
            enumeration_intact=True, connector_version="1", batches=[],
        )
        with pytest.raises(ValueError, match="monotonically"):
            await outputs.commit_page(
                db, job_id, lineage, partition_key="catalogue", page_id="page-1",
                page_sequence=1, resume_after={"cursor": 1}, terminal=False,
                enumeration_intact=True, connector_version="1", batches=[],
            )
        await outputs.commit_page(
            db, job_id, lineage, partition_key="catalogue", page_id="page-3",
            page_sequence=3, resume_after=None, terminal=True,
            enumeration_intact=True, connector_version="1", batches=[],
        )
        with pytest.raises(ValueError, match="terminal"):
            await outputs.commit_page(
                db, job_id, lineage, partition_key="catalogue", page_id="page-4",
                page_sequence=4, resume_after=None, terminal=True,
                enumeration_intact=True, connector_version="1", batches=[],
            )
        assert await outputs.complete_lineage(
            db, job_id, lineage, expected_partitions={"catalogue"}, checksum="f" * 64
        )
        assert not await outputs.complete_lineage(
            db, job_id, lineage, expected_partitions={"catalogue"}, checksum="f" * 64
        )

    def test_dataset_states_aggregate_without_hiding_partial_success(self):
        assert outputs.aggregate_job_state(["succeeded", "succeeded"]) == "succeeded"
        assert outputs.aggregate_job_state(["succeeded", "failed"]) == "degraded"
        assert outputs.aggregate_job_state(["failed", "failed"]) == "failed"
        assert outputs.aggregate_job_state(["succeeded"], cancelled=True) == "cancelled"

    async def test_pipeline_committer_supports_resume_and_ordered_reconstruction(self, db):
        job_id, key, lineage = await self.prepared(db)
        committer = outputs.PostgresPageCommitter(
            db, job_id, lineage, "1", {key.dataset: key}
        )
        first = StoredBatch(
            "stage/page-0", "1" * 64, 10, 1,
            BatchIdentity(
                str(job_id), str(lineage), "main", "page-0", 0,
                key.dataset, key.contract_version, key.projector_version,
            ),
        )
        page = EntityPage(
            page_id="page-0", partition_key="main", sequence=0, items=(),
            resume_after={"page": 2}, terminal=False, discovered=0,
        )
        outcome = DatasetPageOutcome(key.dataset, DatasetPageState.SUCCEEDED, records=1)
        await committer.commit_page(page, [first], [outcome])
        checkpoint = await outputs.resume_checkpoint(db, job_id, lineage)
        assert checkpoint is not None and checkpoint.resume_after == {"page": 2}
        assert await outputs.reconstruct_batches(db, job_id, lineage, key) == [first]

        # A crash after commit replays the same page. It must neither duplicate
        # records nor accept a changed projector outcome.
        await committer.commit_page(page, [first], [outcome])
        stored = await rows(db, "select records from catalogue.job_datasets where job_id = %s", (job_id,))
        assert stored[0]["records"] == 1
        with pytest.raises(ValueError, match="outcomes"):
            await committer.commit_page(
                page, [first],
                [DatasetPageOutcome(key.dataset, DatasetPageState.SUCCEEDED, records=0)],
            )

    async def test_resume_uses_the_connector_owned_next_partition_cursor(self, db):
        job_id, key, lineage = await self.prepared(db)
        await db.execute(
            "update catalogue.job_checkpoint_lineages "
            "set connector_configuration = '{\"partitions\":[\"first\",\"second\"]}'::jsonb "
            "where job_id = %s and checkpoint_lineage = %s",
            (job_id, lineage),
        )
        committer = outputs.PostgresPageCommitter(
            db, job_id, lineage, "1", {key.dataset: key}
        )
        page = EntityPage(
            page_id="first:1", partition_key="first", sequence=0, items=(),
            resume_after={"partition": "second", "offset": 0, "sequence": 1},
            terminal=False, partition_terminal=True, discovered=0,
        )
        batch = StoredBatch(
            "stage/first", "2" * 64, 10, 0,
            BatchIdentity(
                str(job_id), str(lineage), "first", "first:1", 0,
                key.dataset, key.contract_version, key.projector_version,
            ),
        )
        await committer.commit_page(
            page, [batch],
            [DatasetPageOutcome(key.dataset, DatasetPageState.SUCCEEDED, records=0)],
        )
        checkpoint = await outputs.resume_checkpoint(db, job_id, lineage)
        assert checkpoint is not None
        assert checkpoint.resume_after == {
            "partition": "second", "offset": 0, "sequence": 1
        }

    async def test_failed_projector_is_persisted_and_later_skip_is_sticky(self, db):
        job_id, key, lineage = await self.prepared(db)
        committer = outputs.PostgresPageCommitter(
            db, job_id, lineage, "1", {key.dataset: key}
        )
        failed = EntityPage(
            page_id="page-0", sequence=0, items=(), resume_after={"page": 2},
            terminal=False, discovered=0,
        )
        await committer.commit_page(
            failed, [],
            [DatasetPageOutcome(key.dataset, DatasetPageState.FAILED, error="boom")],
        )
        later = EntityPage(
            page_id="page-1", sequence=1, items=(), resume_after=None,
            terminal=True, discovered=0,
        )
        await committer.commit_page(
            later, [], [DatasetPageOutcome(key.dataset, DatasetPageState.SKIPPED)]
        )
        stored = await rows(
            db,
            "select state, error from catalogue.job_datasets where job_id = %s",
            (job_id,),
        )
        assert stored == [{"state": "failed", "error": "boom"}]

    async def test_publication_recovers_after_object_write_before_database_registration(
        self, db, tmp_path
    ):
        job_id, key, lineage = await self.prepared(db)
        store = LocalArtifactStore(tmp_path)
        identity = BatchIdentity(
            str(job_id), str(lineage), "main", "page-0", 0,
            key.dataset, key.contract_version, key.projector_version,
        )
        staged = store.stage_batch(identity, [{"value": "durable"}])
        committer = outputs.PostgresPageCommitter(
            db, job_id, lineage, "1", {key.dataset: key}
        )
        page = EntityPage(
            page_id="page-0", sequence=0, items=(), resume_after=None,
            terminal=True, discovered=0,
        )
        await committer.commit_page(
            page, [staged],
            [DatasetPageOutcome(key.dataset, DatasetPageState.SUCCEEDED, records=1)],
        )
        checksum = await outputs.lineage_checksum(db, job_id, lineage)
        await outputs.complete_lineage(
            db, job_id, lineage, expected_partitions={"main"}, checksum=checksum
        )

        # Simulated crash boundary: publication reached the artifact store but
        # no job_artifacts row was committed. Retry compacts to the same object.
        orphan = store.publish_dataset(str(job_id), key.dataset, key.contract_version, [staged])
        published = await outputs.publish_dataset(db, store, job_id, lineage, key)
        assert published == orphan
        assert await outputs.publish_dataset(db, store, job_id, lineage, key) == orphan
        registered = await rows(db, "select location from catalogue.job_artifacts where job_id = %s", (job_id,))
        assert registered == [{"location": orphan.location}]

    async def test_reconstruction_preserves_nonlexical_declared_partition_order(
        self, db, tmp_path
    ):
        job_id, key, lineage = await self.prepared(db)
        await db.execute(
            "update catalogue.job_checkpoint_lineages "
            "set connector_configuration = '{\"partitions\":[\"zeta\",\"alpha\"]}'::jsonb "
            "where job_id = %s and checkpoint_lineage = %s",
            (job_id, lineage),
        )
        store = LocalArtifactStore(tmp_path)
        committer = outputs.PostgresPageCommitter(
            db, job_id, lineage, "1", {key.dataset: key}
        )
        committed = []
        for partition, value, terminal in (
            ("zeta", "first", False), ("alpha", "second", True)
        ):
            identity = BatchIdentity(
                str(job_id), str(lineage), partition, f"{partition}:0", 0,
                key.dataset, key.contract_version, key.projector_version,
            )
            batch = store.stage_batch(identity, [{"value": value}])
            committed.append(batch)
            await committer.commit_page(
                EntityPage(
                    page_id=f"{partition}:0", partition_key=partition, sequence=0,
                    items=(),
                    resume_after=(
                        {"partition": "alpha", "offset": 0, "sequence": 0}
                        if not terminal else None
                    ),
                    terminal=terminal, partition_terminal=True, discovered=0,
                ),
                [batch],
                [DatasetPageOutcome(key.dataset, DatasetPageState.SUCCEEDED, records=1)],
            )
        rebuilt = await outputs.reconstruct_batches(db, job_id, lineage, key)
        assert [batch.location for batch in rebuilt] == [batch.location for batch in committed]
        checksum = await outputs.lineage_checksum(db, job_id, lineage)
        await outputs.complete_lineage(
            db, job_id, lineage, expected_partitions=("zeta", "alpha"), checksum=checksum
        )


class TestScheduleDefault:
    async def test_the_daily_run_refreshes_rather_than_replaying(self, db):
        """A daily price run under the old seven-day cache default would replay
        yesterday's pages and report success while changing no prices."""
        found = await rows(db, "select cron, timezone, params from catalogue.schedules where id='daily-prices'")
        assert found[0]["cron"] == "0 3 * * *"
        assert found[0]["timezone"] == "Europe/Paris"
        params = found[0]["params"]
        if isinstance(params, str):
            params = json.loads(params)
        assert params["cache_mode"] == "refresh"
        assert params["refresh_mode"] == "price"
        weekly = await rows(
            db,
            "select cron, timezone, source_filter, params from catalogue.schedules "
            "where id='weekly-full'",
        )
        assert weekly[0]["cron"] == "0 2 * * 0"
        assert weekly[0]["params"]["refresh_mode"] == "full"


class TestSchemaMigration:
    async def install_before(self, db, migration: str) -> None:
        await db.execute("drop schema catalogue cascade")
        await db.execute(EXTENSIONS.read_text(encoding="utf-8"))
        directory = storage_db.schema_directory()
        before = storage_db.SCHEMA_FILES[:storage_db.SCHEMA_FILES.index(migration)]
        for name in before:
            await db.execute((directory / name).read_text(encoding="utf-8"))
        await db.execute(
            """create table catalogue.schema_migrations (
                 filename text primary key,
                 applied_at timestamptz not null default now()
               )"""
        )
        for name in before:
            await db.execute(
                "insert into catalogue.schema_migrations(filename) values (%s)",
                (name,),
            )

    async def test_an_existing_schema_is_adopted_rather_than_reapplied(self, db):
        # The fixture preloads every DDL file without a migration ledger. The
        # baseline is adopted; the idempotent incremental migration is recorded
        # through the ordinary application path.
        assert await storage_db.apply_schema(db) == list(storage_db.SCHEMA_FILES[1:])
        assert await storage_db.apply_schema(db) == []
        applied = await rows(db, "select filename from catalogue.schema_migrations")
        assert {row["filename"] for row in applied} == set(storage_db.SCHEMA_FILES)

    async def test_a_database_short_of_head_is_refused_rather_than_adopted(self, db):
        # A database that stopped part-way through the pre-squash sequence has
        # the early tables and not the late ones. Stamping the baseline over it
        # would record a schema it does not have.
        await db.execute(f"drop table {storage_db.HEAD_SENTINEL}")
        with pytest.raises(RuntimeError, match="stopped part-way"):
            await storage_db.apply_schema(db)

    async def test_an_empty_database_gets_the_baseline(self, db):
        await db.execute("drop schema catalogue cascade")
        assert await storage_db.apply_schema(db) == list(storage_db.SCHEMA_FILES)
        assert await storage_db.apply_schema(db) == []

    async def test_provider_integrity_migration_backfills_existing_routes(self, db):
        await self.install_before(db, storage_db.PROXY_PROVIDER_INTEGRITY_MIGRATION)
        profile = await db.execute(
            """insert into catalogue.proxy_profiles
                 (provider, logical_name, display_name, created_by, updated_by)
                 values ('webshare', 'existing-webshare', 'Existing Webshare', 'test', 'test')
                 returning id"""
        )
        profile_id = (await profile.fetchone())["id"]
        route = await db.execute(
            """insert into catalogue.proxy_routes
                 (label, profile_id, created_by, updated_by)
                 values ('Existing route', %s, 'test', 'test') returning id""",
            (profile_id,),
        )
        route_id = (await route.fetchone())["id"]

        assert await storage_db.apply_schema(db) == [
            storage_db.PROXY_PROVIDER_INTEGRITY_MIGRATION,
            storage_db.PROXY_PROFILE_SECRET_INTENT_MIGRATION,
            storage_db.OFFER_STOCK_TRENDS_MIGRATION,
            storage_db.PURCHASED_PRODUCT_CURATION_MIGRATION,
        ]
        assert await storage_db.apply_schema(db) == []
        migrated = await rows(
            db,
            "select provider from catalogue.proxy_routes where id = %s",
            (route_id,),
        )
        assert migrated == [{"provider": "webshare"}]

    async def test_provider_integrity_migration_refuses_dirty_existing_allocations(self, db):
        await self.install_before(db, storage_db.PROXY_PROVIDER_INTEGRITY_MIGRATION)
        start = datetime.now(UTC) - timedelta(days=1)
        end = datetime.now(UTC) + timedelta(days=1)
        await db.execute(
            """insert into catalogue.proxy_budget_cycles
                 (provider, cycle_start, cycle_end)
                 values ('webshare', %s, %s)""",
            (start, end),
        )
        profile = await db.execute(
            """insert into catalogue.proxy_profiles
                 (provider, logical_name, display_name, created_by, updated_by)
                 values ('decodo', 'dirty-decodo', 'Dirty Decodo', 'test', 'test')
                 returning id"""
        )
        profile_id = (await profile.fetchone())["id"]
        await db.execute(
            """insert into catalogue.proxy_profile_allocations
                 (provider, cycle_start, profile_id, allocated_bytes, updated_by)
                 values ('webshare', %s, %s, 1000, 'test')""",
            (start, profile_id),
        )

        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            await storage_db.apply_schema(db)
        applied = await rows(
            db,
            "select filename from catalogue.schema_migrations where filename = %s",
            (storage_db.PROXY_PROVIDER_INTEGRITY_MIGRATION,),
        )
        assert applied == []

    async def test_profile_secret_intent_migration_is_ordered_and_idempotent(self, db):
        await self.install_before(db, storage_db.PROXY_PROFILE_SECRET_INTENT_MIGRATION)

        assert await storage_db.apply_schema(db) == [
            storage_db.PROXY_PROFILE_SECRET_INTENT_MIGRATION,
            storage_db.OFFER_STOCK_TRENDS_MIGRATION,
            storage_db.PURCHASED_PRODUCT_CURATION_MIGRATION,
        ]
        assert await storage_db.apply_schema(db) == []
        relation = await rows(
            db,
            """select n.nspname as schema_name, c.relname as table_name
                 from pg_class c
                 join pg_namespace n on n.oid = c.relnamespace
                where n.nspname = 'catalogue'
                  and c.relname = 'proxy_profile_secret_intents'""",
        )
        assert relation == [
            {
                "schema_name": "catalogue",
                "table_name": "proxy_profile_secret_intents",
            }
        ]
        columns = await rows(
            db,
            """select column_name from information_schema.columns
                 where table_schema = 'catalogue'
                   and table_name = 'proxy_profile_secret_intents'""",
        )
        assert {row["column_name"] for row in columns} == {
            "operation_id", "provider", "profile_id", "logical_name", "cycle_start",
            "expected_generation", "target_generation", "created_profile", "state",
            "error_code", "created_at", "updated_at", "installed_at", "completed_at",
        }

    async def test_offer_stock_trends_migration_is_ordered_and_idempotent(self, db):
        await self.install_before(db, storage_db.OFFER_STOCK_TRENDS_MIGRATION)

        assert await storage_db.apply_schema(db) == [
            storage_db.OFFER_STOCK_TRENDS_MIGRATION,
            storage_db.PURCHASED_PRODUCT_CURATION_MIGRATION,
        ]
        assert await storage_db.apply_schema(db) == []
        columns = await rows(
            db,
            """select column_name, data_type, column_default
                 from information_schema.columns
                where table_schema = 'catalogue'
                  and table_name = 'offer_observations'
                  and column_name in (
                    'stock_quantity', 'stock_quantity_kind', 'context_version'
                  )
                order by column_name""",
        )
        assert [row["column_name"] for row in columns] == [
            "context_version",
            "stock_quantity",
            "stock_quantity_kind",
        ]
        constraints = await rows(
            db,
            """select conname from pg_constraint
                where conrelid = 'catalogue.offer_observations'::regclass
                  and conname like 'offer_observations_%stock%'
                order by conname""",
        )
        assert {row["conname"] for row in constraints} == {
            "offer_observations_stock_quantity_check",
            "offer_observations_stock_quantity_kind_check",
            "offer_observations_stock_quantity_pair_check",
        }

    async def test_profile_secret_intent_constraints_bind_every_durable_identity(self, db):
        start = datetime.now(UTC) - timedelta(hours=1)
        end = start + timedelta(days=30)
        await db.execute(
            """insert into catalogue.proxy_budget_cycles
                     (provider, cycle_start, cycle_end)
                 values ('webshare', %s, %s), ('decodo', %s, %s)""",
            (start, end, start, end),
        )
        profile_cursor = await db.execute(
            """insert into catalogue.proxy_profiles
                     (provider, logical_name, display_name, created_by, updated_by)
                 values ('webshare', 'intent-webshare', 'Intent Webshare', 'test', 'test')
                 returning id"""
        )
        profile_id = (await profile_cursor.fetchone())["id"]

        async def operation(key: str) -> UUID:
            operation_id = uuid4()
            await db.execute(
                """insert into catalogue.proxy_mutation_requests
                         (operation_id, actor, action, idempotency_key)
                     values (%s, 'test', 'profile.secret.install', %s)""",
                (operation_id, key),
            )
            return operation_id

        create_operation = await operation("valid-create")
        await db.execute(
            """insert into catalogue.proxy_profile_secret_intents
                     (operation_id, provider, profile_id, logical_name, cycle_start,
                      expected_generation, target_generation, created_profile)
                 values (%s, 'webshare', %s, 'intent-webshare', %s, null, 1, true)""",
            (create_operation, profile_id, start),
        )
        await db.execute(
            """update catalogue.proxy_profile_secret_intents
                   set state = 'completed', installed_at = now(), completed_at = now(),
                       updated_at = now()
                 where operation_id = %s""",
            (create_operation,),
        )

        rotation_operation = await operation("valid-rotation")
        await db.execute(
            """insert into catalogue.proxy_profile_secret_intents
                     (operation_id, provider, profile_id, logical_name, cycle_start,
                      expected_generation, target_generation, created_profile)
                 values (%s, 'webshare', %s, 'intent-webshare', %s, 1, 2, false)""",
            (rotation_operation, profile_id, start),
        )
        await db.execute(
            """update catalogue.proxy_profile_secret_intents
                   set state = 'completed', installed_at = now(), completed_at = now(),
                       updated_at = now()
                 where operation_id = %s""",
            (rotation_operation,),
        )

        wrong_operation = uuid4()
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            await db.execute(
                """insert into catalogue.proxy_profile_secret_intents
                         (operation_id, provider, profile_id, logical_name, cycle_start,
                          expected_generation, target_generation, created_profile)
                     values (%s, 'webshare', %s, 'intent-webshare', %s, 2, 3, false)""",
                (wrong_operation, profile_id, start),
            )

        wrong_profile_operation = await operation("wrong-profile-provider")
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            await db.execute(
                """insert into catalogue.proxy_profile_secret_intents
                         (operation_id, provider, profile_id, logical_name, cycle_start,
                          expected_generation, target_generation, created_profile)
                     values (%s, 'decodo', %s, 'intent-webshare', %s, 2, 3, false)""",
                (wrong_profile_operation, profile_id, start),
            )

        wrong_name_operation = await operation("wrong-profile-name")
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            await db.execute(
                """insert into catalogue.proxy_profile_secret_intents
                         (operation_id, provider, profile_id, logical_name, cycle_start,
                          expected_generation, target_generation, created_profile)
                     values (%s, 'webshare', %s, 'other-name', %s, 2, 3, false)""",
                (wrong_name_operation, profile_id, start),
            )

        wrong_cycle = start + timedelta(minutes=1)
        wrong_cycle_operation = await operation("wrong-cycle")
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            await db.execute(
                """insert into catalogue.proxy_profile_secret_intents
                         (operation_id, provider, profile_id, logical_name, cycle_start,
                          expected_generation, target_generation, created_profile)
                     values (%s, 'webshare', %s, 'intent-webshare', %s, 2, 3, false)""",
                (wrong_cycle_operation, profile_id, wrong_cycle),
            )

    @pytest.mark.parametrize(
        ("expected", "target", "created"),
        [(None, 2, True), (1, 2, True), (None, 1, False), (0, 1, False), (1, 3, False)],
    )
    async def test_profile_secret_intent_rejects_invalid_generation_transitions(
        self, db, expected, target, created
    ):
        start = datetime.now(UTC) - timedelta(hours=1)
        await db.execute(
            """insert into catalogue.proxy_budget_cycles
                     (provider, cycle_start, cycle_end)
                 values ('webshare', %s, %s)""",
            (start, start + timedelta(days=30)),
        )
        profile_cursor = await db.execute(
            """insert into catalogue.proxy_profiles
                     (provider, logical_name, display_name, created_by, updated_by)
                 values ('webshare', 'invalid-intent', 'Invalid intent', 'test', 'test')
                 returning id"""
        )
        profile_id = (await profile_cursor.fetchone())["id"]
        operation_id = uuid4()
        await db.execute(
            """insert into catalogue.proxy_mutation_requests
                     (operation_id, actor, action, idempotency_key)
                 values (%s, 'test', 'profile.secret.install', %s)""",
            (operation_id, str(operation_id)),
        )

        with pytest.raises(psycopg.errors.CheckViolation):
            await db.execute(
                """insert into catalogue.proxy_profile_secret_intents
                         (operation_id, provider, profile_id, logical_name, cycle_start,
                          expected_generation, target_generation, created_profile)
                     values (%s, 'webshare', %s, 'invalid-intent', %s, %s, %s, %s)""",
                (operation_id, profile_id, start, expected, target, created),
            )

    async def test_profile_secret_intent_allows_only_one_active_intent_and_valid_states(self, db):
        start = datetime.now(UTC) - timedelta(hours=1)
        await db.execute(
            """insert into catalogue.proxy_budget_cycles
                     (provider, cycle_start, cycle_end)
                 values ('webshare', %s, %s)""",
            (start, start + timedelta(days=30)),
        )
        profile_cursor = await db.execute(
            """insert into catalogue.proxy_profiles
                     (provider, logical_name, display_name, created_by, updated_by)
                 values ('webshare', 'unique-intent', 'Unique intent', 'test', 'test')
                 returning id"""
        )
        profile_id = (await profile_cursor.fetchone())["id"]

        async def insert_intent(key: str, expected: int, target: int) -> UUID:
            operation_id = uuid4()
            await db.execute(
                """insert into catalogue.proxy_mutation_requests
                         (operation_id, actor, action, idempotency_key)
                     values (%s, 'test', 'profile.secret.install', %s)""",
                (operation_id, key),
            )
            await db.execute(
                """insert into catalogue.proxy_profile_secret_intents
                         (operation_id, provider, profile_id, logical_name, cycle_start,
                          expected_generation, target_generation, created_profile)
                     values (%s, 'webshare', %s, 'unique-intent', %s, %s, %s, false)""",
                (operation_id, profile_id, start, expected, target),
            )
            return operation_id

        first = await insert_intent("first-active", 1, 2)
        with pytest.raises(psycopg.errors.UniqueViolation):
            await insert_intent("second-active", 2, 3)

        with pytest.raises(psycopg.errors.CheckViolation):
            await db.execute(
                """update catalogue.proxy_profile_secret_intents
                       set state = 'unknown' where operation_id = %s""",
                (first,),
            )
        with pytest.raises(psycopg.errors.CheckViolation):
            await db.execute(
                """update catalogue.proxy_profile_secret_intents
                       set state = 'installed' where operation_id = %s""",
                (first,),
            )

        await db.execute(
            """update catalogue.proxy_profile_secret_intents
                   set state = 'failed', completed_at = now(), updated_at = now()
                 where operation_id = %s""",
            (first,),
        )
        second = await insert_intent("third-after-terminal", 2, 3)
        assert second is not None

    async def test_multi_dataset_page_and_artifact_schema_enforces_identity(self, db):
        run_id = await runs.create_run(db)
        jobs = await runs.create_jobs(db, run_id, SOURCES, ["ceradel"])
        job_id = jobs["ceradel"]
        lineage = uuid4()
        digest = "a" * 64
        await db.execute(
            "insert into catalogue.job_datasets "
            "(job_id, dataset, contract_version, projector_version) "
            "values (%s, 'ceramics', 'v2', 'legacy')",
            (job_id,),
        )
        await db.execute(
            """insert into catalogue.job_checkpoint_lineages
                         (job_id, checkpoint_lineage, source_id, connector, connector_version,
                          source_url, connector_config_fingerprint, dataset_fingerprint)
                  values (%s, %s, 'ceradel', 'shopify', '1', 'https://ceradel.fr/', %s, %s)""",
            (job_id, lineage, digest, digest),
        )
        await db.execute(
            """insert into catalogue.job_pages
                         (job_id, checkpoint_lineage, partition_key, page_sequence, page_id,
                          resume_after, terminal, enumeration_intact, connector_version)
                  values (%s, %s, 'catalogue', 0, 'page-0', '{"cursor":1}', false, true, '1')""",
            (job_id, lineage),
        )
        await db.execute(
            """insert into catalogue.job_page_batches
                         (job_id, checkpoint_lineage, partition_key, page_id, page_sequence, dataset,
                          contract_version, projector_version, object_key, sha256, size, records)
                  values (%s, %s, 'catalogue', 'page-0', 0, 'ceramics', 'v2', 'legacy',
                          'staged/job/page-0', %s, 12, 1)""",
            (job_id, lineage, digest),
        )
        await db.execute(
            """insert into catalogue.job_artifacts
                         (job_id, dataset, contract_version, projector_version, kind,
                          location, sha256, size)
                  values (%s, 'ceramics', 'v2', 'legacy', 'ndjson',
                          'published/job/ceramics.ndjson', %s, 12)""",
            (job_id, digest),
        )

        stored = await rows(
            db,
            "select p.partition_key, p.page_sequence, b.object_key, a.location "
            "from catalogue.job_pages p join catalogue.job_page_batches b using "
            "(job_id, checkpoint_lineage, partition_key, page_id) "
            "join catalogue.job_artifacts a using (job_id) where p.job_id = %s",
            (job_id,),
        )
        assert stored == [{
            "partition_key": "catalogue", "page_sequence": 0,
            "object_key": "staged/job/page-0", "location": "published/job/ceramics.ndjson",
        }]
