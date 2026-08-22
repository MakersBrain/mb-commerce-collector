"""NATS delivery reservation, generations, execution fencing and politeness."""

from __future__ import annotations

from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb

from mb_ceramics_catalogue.config.settings import CrawlParams
from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.ops import leases, outbox, queue, runs

from .conftest import requires_postgres

pytestmark = [pytest.mark.postgres, requires_postgres]

SOURCES = SourcesFile.model_validate(
    {
        "plain": {"label": "Plain", "url": "https://plain.test/", "scraper": "shopify"},
        "ceramicolours": {"label": "Browser", "url": "https://browser.test/", "scraper": "ceramicolours"},
        "same-host": {"label": "Same", "url": "https://plain.test/other", "scraper": "shopify"},
    }
)

PROXY_SOURCES = SourcesFile.model_validate(
    {
        "proxy-shop": {
            "label": "Proxy shop",
            "url": "https://proxy-shop.test/",
            "scraper": "shopify",
            "country": "fr",
            "proxy_eligible": True,
        }
    }
)


async def planned(connection, source: str = "plain"):
    run_id = await runs.create_run(connection)
    assert run_id is not None
    jobs = await runs.create_jobs(connection, run_id, SOURCES, [source])
    await connection.execute(
        "update catalogue.jobs set scheduled_for = now() where id = %(id)s", {"id": jobs[source]}
    )
    row = await one(
        connection,
        "select * from catalogue.queue_outbox where job_id = %(id)s order by generation desc limit 1",
        {"id": jobs[source]},
    )
    assert row is not None
    return jobs[source], outbox.envelope(row)


async def one(connection, sql: str, params=None):
    cursor = await connection.execute(sql, params)
    return await cursor.fetchone()


async def registered(connection):
    worker_id = uuid4()
    await connection.execute(
        "insert into catalogue.workers (id, hostname, pid, capabilities, status) "
        "values (%(id)s, 'test', 1, '{}', 'idle')",
        {"id": worker_id},
    )
    return worker_id


async def test_job_creation_and_outbox_are_atomic(db):
    job_id, envelope = await planned(db)
    assert envelope.job_id == job_id
    assert envelope.generation == 1
    assert envelope.route == "plain.normal"


async def test_proxy_snapshot_is_immutable_after_control_plane_changes(db):
    profile = await one(
        db,
        """
        insert into catalogue.proxy_profiles
               (provider, logical_name, display_name, enabled, lifecycle,
                secret_generation, created_by, updated_by)
        values ('decodo', 'snapshot-profile', 'Snapshot profile', true, 'enabled',
                4, 'test', 'test')
        returning id
        """,
    )
    assert profile is not None
    route = await one(
        db,
        """
        insert into catalogue.proxy_routes
               (label, profile_id, protocol, country, state, city, session_mode,
                session_minutes, max_bytes, pilot, enabled, created_by, updated_by)
        values ('Snapshot route', %(profile_id)s, 'https', 'FR', 'IDF', 'Paris', 'sticky',
                45, 9000, false, true, 'test', 'test')
        returning id
        """,
        {"profile_id": profile["id"]},
    )
    assert route is not None
    await db.execute(
        """
        insert into catalogue.source_proxy_policies
               (source_id, policy, route_id, max_bytes, pilot, evidence_state, revision, updated_by)
        values ('proxy-shop', 'fallback', %(route_id)s, 7000, true, 'eligible', 7, 'test')
        """,
        {"route_id": route["id"]},
    )

    run_id = await runs.create_run(db)
    assert run_id is not None
    job_id = (await runs.create_jobs(db, run_id, PROXY_SOURCES, ["proxy-shop"]))["proxy-shop"]
    await db.execute(
        "update catalogue.jobs set scheduled_for = now() where id = %(id)s",
        {"id": job_id},
    )
    stored = await one(db, "select proxy_snapshot from catalogue.jobs where id = %(id)s", {"id": job_id})
    assert stored is not None
    original = stored["proxy_snapshot"]
    assert original == {
        "policy": "fallback",
        "policy_revision": 7,
        "route_id": str(route["id"]),
        "profile_id": str(profile["id"]),
        "provider": "decodo",
        "profile": "snapshot-profile",
        "secret_generation": 4,
        "protocol": "https",
        "country": "FR",
        "state": "IDF",
        "city": "Paris",
        "session_mode": "sticky",
        "session_minutes": 45,
        "max_bytes": 7000,
        "pilot": True,
    }

    await db.execute(
        "update catalogue.proxy_profiles set provider = 'replacement', "
        "logical_name = 'replacement-profile', secret_generation = 5, "
        "enabled = false, lifecycle = 'disabled' "
        "where id = %(id)s",
        {"id": profile["id"]},
    )
    await db.execute(
        "update catalogue.proxy_routes set protocol = 'socks5', country = 'DE', state = null, "
        "city = 'Berlin', session_mode = 'random', session_minutes = 10, max_bytes = 5000, "
        "pilot = true, enabled = false where id = %(id)s",
        {"id": route["id"]},
    )
    await db.execute(
        "update catalogue.source_proxy_policies set policy = 'never', route_id = null, "
        "max_bytes = 3000, pilot = false, revision = 8 where source_id = 'proxy-shop'"
    )

    stored_after = await one(
        db, "select proxy_snapshot from catalogue.jobs where id = %(id)s", {"id": job_id}
    )
    assert stored_after is not None
    assert stored_after["proxy_snapshot"] == original

    outbox_row = await one(
        db,
        "select * from catalogue.queue_outbox where job_id = %(id)s order by generation desc limit 1",
        {"id": job_id},
    )
    assert outbox_row is not None
    reservation = await queue.reserve(
        db, outbox.envelope(outbox_row), await registered(db), []
    )
    assert reservation.disposition == "run"
    assert reservation.job is not None
    assert reservation.job.proxy_snapshot == original


async def test_provider_retry_exhaustion_redrives_only_the_current_generation(db):
    job_id, exhausted = await planned(db)
    assert await outbox.redrive_exhausted(db, exhausted)
    row = await one(
        db,
        "select j.delivery_generation, o.route, o.deduplication_key "
        "from catalogue.jobs j join catalogue.queue_outbox o "
        "on o.job_id=j.id and o.generation=j.delivery_generation "
        "where j.id=%(id)s",
        {"id": job_id},
    )
    assert row == {
        "delivery_generation": 2,
        "route": "plain.normal",
        "deduplication_key": f"{job_id}:2",
    }
    assert not await outbox.redrive_exhausted(db, exhausted)


async def test_reservation_does_not_consume_attempt_and_start_does(db):
    _, envelope = await planned(db)
    worker = await registered(db)
    result = await queue.reserve(db, envelope, worker, [])
    assert result.disposition == "run"
    assert result.job is not None
    assert result.job.attempt == 0
    assert await queue.start(db, result.job, worker)
    row = await one(
        db, "select state, attempt from catalogue.jobs where id = %(id)s", {"id": envelope.job_id}
    )
    assert row == {"state": "running", "attempt": 1}


@pytest.mark.parametrize(
    "rollback_params",
    ({"pipeline": "legacy"}, {}),
    ids=("explicit-legacy", "remove-override"),
)
async def test_source_pipeline_override_is_isolated_and_rollback_applies_on_reservation(
    db, rollback_params
):
    run_id = await runs.create_run(db, params={"pipeline": "legacy"})
    assert run_id is not None
    jobs = await runs.create_jobs(db, run_id, SOURCES, ["plain", "same-host"])
    await db.execute(
        "update catalogue.jobs set scheduled_for = now() where run_id = %(run)s",
        {"run": run_id},
    )
    await db.execute(
        "insert into catalogue.source_settings (source_id, params) "
        "values ('plain', %(params)s)",
        {"params": Jsonb({"pipeline": "connector_canary"})},
    )

    envelopes = {}
    for source, job_id in jobs.items():
        row = await one(
            db,
            "select * from catalogue.queue_outbox "
            "where job_id = %(id)s order by generation desc limit 1",
            {"id": job_id},
        )
        assert row is not None
        envelopes[source] = outbox.envelope(row)

    canary_owner = await registered(db)
    canary = await queue.reserve(db, envelopes["plain"], canary_owner, [])
    unaffected = await queue.reserve(
        db, envelopes["same-host"], await registered(db), []
    )
    assert canary.job is not None
    assert unaffected.job is not None
    assert CrawlParams.from_job(canary.job.params).pipeline == "connector_canary"
    assert CrawlParams.from_job(unaffected.job.params).pipeline == "legacy"

    assert await queue.release(
        db,
        canary.job,
        canary_owner,
        delay=0,
        reason="pipeline rollback test",
    )
    await db.execute(
        "update catalogue.source_settings set params = %(params)s "
        "where source_id = 'plain'",
        {"params": Jsonb(rollback_params)},
    )

    rolled_back = await queue.reserve(
        db, envelopes["plain"], await registered(db), []
    )
    assert rolled_back.job is not None
    assert CrawlParams.from_job(rolled_back.job.params).pipeline == "legacy"


async def test_a_released_job_spends_an_attempt_when_it_starts_again(db):
    """What the worker's transient retry relies on to stay bounded.

    A source whose host answered 429 is released back to the queue rather than
    failed, so the budget has to be spent by the restart. If it were not, a
    permanently throttled host would circle the queue forever.
    """
    _, envelope = await planned(db)
    worker = await registered(db)
    first = await queue.reserve(db, envelope, worker, [])
    assert first.job is not None
    assert await queue.start(db, first.job, worker)
    assert await queue.release(db, first.job, worker, delay=0, reason="429 Too Many Requests")

    row = await one(
        db, "select state, attempt from catalogue.jobs where id = %(id)s", {"id": envelope.job_id}
    )
    assert row == {"state": "queued", "attempt": 1}

    second = await queue.reserve(db, envelope, worker, [])
    assert second.job is not None
    # Carried out of the row, so the worker can see the budget it is spending.
    assert second.job.attempt == 1
    assert second.job.max_attempts == 3
    assert await queue.start(db, second.job, worker)
    row = await one(
        db, "select state, attempt from catalogue.jobs where id = %(id)s", {"id": envelope.job_id}
    )
    assert row == {"state": "running", "attempt": 2}


async def test_a_released_job_is_not_offered_before_its_backoff_elapses(db):
    """The delay is the whole point: coming straight back re-asks a busy host."""
    _, envelope = await planned(db)
    worker = await registered(db)
    reserved = await queue.reserve(db, envelope, worker, [])
    assert reserved.job is not None
    assert await queue.start(db, reserved.job, worker)
    assert await queue.release(
        db, reserved.job, worker, delay=queue.TRANSIENT_BACKOFF_SECONDS, reason="429"
    )

    again = await queue.reserve(db, envelope, worker, [])
    assert again.disposition == "retry"
    assert again.job is None
    assert again.retry_after > 0


async def test_live_duplicate_is_delayed_not_run(db):
    _, envelope = await planned(db)
    first = await registered(db)
    second = await registered(db)
    winner = await queue.reserve(db, envelope, first, [])
    assert winner.job is not None
    duplicate = await queue.reserve(db, envelope, second, [])
    assert duplicate.disposition == "retry"
    assert duplicate.retry_after > 0


async def test_stale_generation_is_acknowledged(db):
    job_id, envelope = await planned(db)
    await db.execute(
        "update catalogue.jobs set delivery_generation = delivery_generation + 1 where id = %(id)s",
        {"id": job_id},
    )
    result = await queue.reserve(db, envelope, uuid4(), [])
    assert result.disposition == "ack"


async def test_execution_token_fences_start_and_release(db):
    _, envelope = await planned(db)
    owner = await registered(db)
    result = await queue.reserve(db, envelope, owner, [])
    assert result.job is not None
    result.job.execution_token = uuid4()
    assert not await queue.start(db, result.job, owner)
    assert not await queue.release(db, result.job, owner, delay=0)


async def test_host_release_is_scoped_to_execution_token(db):
    job_id, envelope = await planned(db)
    owner = await registered(db)
    result = await queue.reserve(db, envelope, owner, [])
    assert result.job is not None
    token = result.job.execution_token
    assert await leases.acquire(db, "plain.test", job_id, owner, token) == 1
    assert await leases.release_all(db, job_id, uuid4()) == []
    assert await leases.release_all(db, job_id, token) == ["plain.test"]


async def test_dynamic_browser_escalation_creates_new_generation(db):
    job_id, envelope = await planned(db)
    owner = await registered(db)
    result = await queue.reserve(db, envelope, owner, [])
    assert result.job is not None
    assert await queue.start(db, result.job, owner)
    assert await queue.require_capability(db, result.job, owner, "browser", reason="render required")
    row = await one(
        db,
        "select j.delivery_generation, o.route from catalogue.jobs j "
        "join catalogue.queue_outbox o on o.job_id=j.id and o.generation=j.delivery_generation "
        "where j.id=%(id)s",
        {"id": job_id},
    )
    assert row["delivery_generation"] == 2
    assert row["route"] == "browser.auto.normal"


async def test_static_browser_route_persists_exact_lineage_before_host_slots(db):
    _, envelope = await planned(db, "ceramicolours")
    result = await queue.reserve(db, envelope, await registered(db), ["browser", "browser:camoufox"])
    assert result.job is not None
    assert result.job.selected_browser_backend == "camoufox"


async def test_auto_route_selects_and_republishes_exact_backend(db):
    job_id, first = await planned(db)
    owner = await registered(db)
    reserved = await queue.reserve(db, first, owner, [])
    assert reserved.job is not None
    assert await queue.start(db, reserved.job, owner)
    assert await queue.require_capability(db, reserved.job, owner, "browser", reason="render")
    auto_row = await one(
        db,
        "select * from catalogue.queue_outbox where job_id=%(id)s and generation=2",
        {"id": job_id},
    )
    selected = await queue.reserve(
        db, outbox.envelope(auto_row), await registered(db), ["browser", "browser:camoufox"]
    )
    assert selected.disposition == "ack"
    exact = await one(
        db,
        "select route from catalogue.queue_outbox where job_id=%(id)s and generation=3",
        {"id": job_id},
    )
    assert exact["route"] == "browser.camoufox.normal"


async def test_reconciler_requeues_expired_execution_without_discovery_scan(db):
    job_id, envelope = await planned(db)
    result = await queue.reserve(db, envelope, await registered(db), [])
    assert result.job is not None
    await db.execute(
        "update catalogue.jobs set lease_expires_at = now() - interval '1 second' where id=%(id)s",
        {"id": job_id},
    )
    assert await queue.reconcile(db) == 1
    row = await one(db, "select state, execution_token from catalogue.jobs where id=%(id)s", {"id": job_id})
    assert row == {"state": "queued", "execution_token": None}


async def test_reconciler_terminalizes_exhausted_execution(db):
    job_id, envelope = await planned(db)
    result = await queue.reserve(db, envelope, await registered(db), [])
    assert result.job is not None
    await db.execute(
        "update catalogue.jobs set attempt=max_attempts, lease_expires_at=now()-interval '1 second' "
        "where id=%(id)s",
        {"id": job_id},
    )
    await queue.reconcile(db)
    row = await one(db, "select state from catalogue.jobs where id=%(id)s", {"id": job_id})
    assert row["state"] == "failed"
