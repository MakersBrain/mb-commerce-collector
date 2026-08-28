"""The in-process loader, against a real PostgreSQL.

`plan_load` decides *whether* a file may be grounds for retirement and is tested
without a database in test_load_planning.py. This is the other half: given that
decision, does the load actually retire the right rows, in one transaction, and
does a bad record cost one source rather than the whole load.

Retirement is the dangerous operation in this codebase. Marking a live catalogue
withdrawn is invisible until somebody notices a supplier mysteriously stopped
stocking anything, so it is worth testing at the level where it really happens.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import dict_row

from mb_ceramics_catalogue.storage import postgres

from .conftest import postgres_dsn, requires_postgres

pytestmark = [pytest.mark.postgres, requires_postgres]

SOURCES = {
    "ceradel": {"label": "Ceradel", "url": "https://ceradel.fr/", "country": "FR", "scope": "materials"},
}


def record(external: str, name: str, price: float = 12.5) -> dict:
    """A minimal `ceramics.catalogue_item.v2` row the loader will accept."""
    return {
        "format": "ceramics.catalogue_item.v2",
        "source": "ceradel",
        "external_id": f"ceradel:{external}",
        "parent_external_id": f"ceradel:{external}",
        "product_url": f"https://ceradel.fr/p/{external}",
        "extraction_method": "api_json",
        "source_detail_level": "api",
        "fetched_at": "2026-08-08T00:00:00Z",
        "name": name,
        "name_raw": name,
        "price": price,
        "currency": "EUR",
        "vat_status": "inclusive",
    }


def at(row: dict, stamp: str, *, price: float | None = None) -> dict:
    changed = {**row, "fetched_at": stamp}
    if price is not None:
        changed["price"] = price
    return changed


def stocked(row: dict, quantity: int | None) -> dict:
    changed = {**row}
    changed["stock_quantity"] = quantity
    changed["availability"] = (
        "https://schema.org/OutOfStock" if quantity == 0 else "https://schema.org/InStock"
    )
    return changed


@pytest.fixture
def sync_db(db):
    """A synchronous connection to the schema the async `db` fixture built.

    The loader is synchronous — it is called from a worker's thread pool and
    from a CLI, neither of which needs it to be async — so it needs its own
    connection rather than the async one the fixture yields.
    """
    dsn = postgres_dsn()
    assert dsn is not None
    with psycopg.connect(dsn, row_factory=dict_row, autocommit=True) as connection:
        yield connection


def active_products(connection) -> dict[str, bool]:
    with connection.cursor() as cursor:
        cursor.execute("select external_id, active from catalogue.source_products order by external_id")
        return {row["external_id"]: row["active"] for row in cursor.fetchall()}


class TestLoadSource:
    def test_stock_history_gate_preserves_raw_value_without_publishing_it(self, sync_db):
        postgres.ensure_staging(sync_db)
        row = stocked(record("stock-gated", "Stock gated"), 9)

        postgres.load_source(sync_db, "ceradel", [row], whole=True)

        with sync_db.cursor() as cursor:
            cursor.execute(
                "select stock_quantity, stock_quantity_kind from catalogue.offer_observations"
            )
            assert cursor.fetchone() == {
                "stock_quantity": None,
                "stock_quantity_kind": "unknown",
            }
            cursor.execute("select record->>'stock_quantity' value from catalogue.raw_records")
            assert cursor.fetchone()["value"] == "9"

    def test_stock_change_appends_combined_offer_state(self, sync_db):
        postgres.ensure_staging(sync_db)
        first = stocked(record("stock-change", "Stock change", 10), 7)
        second = stocked(at(first, "2026-08-09T00:00:00Z"), 3)

        postgres.load_source(
            sync_db, "ceradel", [first], whole=True, stock_trends_enabled=True
        )
        postgres.load_source(
            sync_db, "ceradel", [second], whole=True, stock_trends_enabled=True
        )

        with sync_db.cursor() as cursor:
            cursor.execute(
                "select price::float8 price, stock_quantity, stock_quantity_kind, context_version "
                "from catalogue.offer_observations order by observed_at"
            )
            assert cursor.fetchall() == [
                {
                    "price": 10.0,
                    "stock_quantity": 7,
                    "stock_quantity_kind": "exact",
                    "context_version": 2,
                },
                {
                    "price": 10.0,
                    "stock_quantity": 3,
                    "stock_quantity_kind": "exact",
                    "context_version": 2,
                },
            ]

    def test_non_exact_stock_kind_is_preserved_not_promoted(self, sync_db):
        postgres.ensure_staging(sync_db)
        row = stocked(record("stock-lower-bound", "Stock lower bound"), 12)
        row["stock_quantity_kind"] = "lower_bound"

        postgres.load_source(
            sync_db, "ceradel", [row], whole=True, stock_trends_enabled=True
        )

        with sync_db.cursor() as cursor:
            cursor.execute(
                "select stock_quantity, stock_quantity_kind "
                "from catalogue.offer_observations"
            )
            assert cursor.fetchone() == {
                "stock_quantity": 12,
                "stock_quantity_kind": "lower_bound",
            }

    def test_unchanged_stock_extends_interval_and_exact_zero_is_retained(self, sync_db):
        postgres.ensure_staging(sync_db)
        first = stocked(record("stock-zero", "Stock zero"), 0)
        again = at(first, "2026-08-09T00:00:00Z")

        for row in (first, again):
            postgres.load_source(
                sync_db, "ceradel", [row], whole=True, stock_trends_enabled=True
            )

        with sync_db.cursor() as cursor:
            cursor.execute(
                "select count(*) n, min(stock_quantity) stock, "
                "min(stock_quantity_kind) kind, max(last_seen_at) last_seen "
                "from catalogue.offer_observations"
            )
            offer = cursor.fetchone()
        assert offer["n"] == 1
        assert offer["stock"] == 0
        assert offer["kind"] == "exact"
        assert str(offer["last_seen"]).startswith("2026-08-09")

    def test_context_v1_unknown_stock_does_not_create_deployment_duplicate(self, sync_db):
        postgres.ensure_staging(sync_db)
        first = record("context-v1", "Context v1")
        postgres.load_source(sync_db, "ceradel", [first], whole=True)
        with sync_db.cursor() as cursor:
            cursor.execute(
                "update catalogue.offer_observations set context_version=1, "
                "context_sha256=decode(repeat('00', 32), 'hex')"
            )

        postgres.load_source(
            sync_db,
            "ceradel",
            [at(first, "2026-08-09T00:00:00Z")],
            whole=True,
            stock_trends_enabled=True,
        )

        with sync_db.cursor() as cursor:
            cursor.execute(
                "select count(*) n, min(context_version) version, max(last_seen_at) last_seen "
                "from catalogue.offer_observations"
            )
            offer = cursor.fetchone()
        assert offer["n"] == 1
        assert offer["version"] == 1
        assert str(offer["last_seen"]).startswith("2026-08-09")

    def test_context_v1_gains_a_real_row_when_numeric_stock_begins(self, sync_db):
        postgres.ensure_staging(sync_db)
        first = record("context-stock", "Context stock")
        postgres.load_source(sync_db, "ceradel", [first], whole=True)
        with sync_db.cursor() as cursor:
            cursor.execute("update catalogue.offer_observations set context_version=1")

        current = stocked(at(first, "2026-08-09T00:00:00Z"), 4)
        postgres.load_source(
            sync_db, "ceradel", [current], whole=True, stock_trends_enabled=True
        )

        with sync_db.cursor() as cursor:
            cursor.execute(
                "select context_version, stock_quantity from catalogue.offer_observations "
                "order by observed_at"
            )
            assert cursor.fetchall() == [
                {"context_version": 1, "stock_quantity": None},
                {"context_version": 2, "stock_quantity": 4},
            ]

    def test_concurrent_first_observation_creates_one_raw_and_offer_row(self, sync_db):
        postgres.ensure_staging(sync_db)
        dsn = postgres_dsn()
        assert dsn
        first = record("same", "Concurrent blue", 10)

        def load_once():
            with psycopg.connect(dsn, row_factory=dict_row, autocommit=True) as connection:
                postgres.ensure_staging(connection)
                return postgres.load_source(connection, "ceradel", [first], whole=False)

        with ThreadPoolExecutor(max_workers=2) as executor:
            reports = list(executor.map(lambda _index: load_once(), range(2)))
        assert all(report.ok for report in reports)
        with sync_db.cursor() as cursor:
            cursor.execute("select count(*) n from catalogue.raw_records")
            assert cursor.fetchone()["n"] == 1
            cursor.execute("select count(*) n from catalogue.offer_observations")
            assert cursor.fetchone()["n"] == 1

    def test_unchanged_records_extend_intervals_without_new_rows(self, sync_db):
        postgres.ensure_staging(sync_db)
        first = record("1", "Blue")
        postgres.load_source(sync_db, "ceradel", [first], whole=True)
        postgres.load_source(
            sync_db, "ceradel", [at(first, "2026-08-09T00:00:00Z")], whole=True
        )

        with sync_db.cursor() as cursor:
            cursor.execute(
                "select count(*) n, min(first_seen_at) first_seen, max(last_seen_at) last_seen "
                "from catalogue.raw_records"
            )
            raw = cursor.fetchone()
            cursor.execute(
                "select count(*) n, min(observed_at) observed, max(last_seen_at) last_seen "
                "from catalogue.offer_observations"
            )
            offer = cursor.fetchone()
        assert raw["n"] == 1
        assert str(raw["first_seen"]).startswith("2026-08-08")
        assert str(raw["last_seen"]).startswith("2026-08-09")
        assert offer["n"] == 1
        assert str(offer["observed"]).startswith("2026-08-08")
        assert str(offer["last_seen"]).startswith("2026-08-09")

    def test_offer_history_preserves_a_to_b_to_a(self, sync_db):
        postgres.ensure_staging(sync_db)
        first = record("1", "Blue", 10)
        for row in (
            first,
            at(first, "2026-08-09T00:00:00Z", price=12),
            at(first, "2026-08-10T00:00:00Z", price=10),
        ):
            postgres.load_source(sync_db, "ceradel", [row], whole=True)

        with sync_db.cursor() as cursor:
            cursor.execute(
                "select price::float8 price from catalogue.offer_observations order by observed_at"
            )
            assert [row["price"] for row in cursor.fetchall()] == [10, 12, 10]

    def test_older_different_state_is_quarantined_and_does_not_regress_current(self, sync_db):
        postgres.ensure_staging(sync_db)
        current = at(record("1", "Blue", 12), "2026-08-10T00:00:00Z")
        current["availability"] = "in_stock"
        older = at(current, "2026-08-09T00:00:00Z", price=10)
        older["name"] = "Stale name"
        older["availability"] = "out_of_stock"
        postgres.load_source(sync_db, "ceradel", [current], whole=True)
        postgres.load_source(sync_db, "ceradel", [older], whole=True)

        with sync_db.cursor() as cursor:
            cursor.execute("select price::float8 price, last_seen_at from catalogue.latest_offers")
            latest = cursor.fetchone()
            cursor.execute("select count(*) n from catalogue.out_of_order_observations")
            quarantined = cursor.fetchone()["n"]
            cursor.execute("select name, availability from catalogue.source_products")
            product = cursor.fetchone()
        assert latest["price"] == 12
        assert str(latest["last_seen_at"]).startswith("2026-08-10")
        assert quarantined == 1
        assert product == {"name": "Blue", "availability": "in_stock"}

    def test_price_refresh_does_not_erase_weekly_enrichment(self, sync_db):
        postgres.ensure_staging(sync_db)
        full = {
            **record("1", "Blue", 10),
            "collection_mode": "full",
            "description": "A carefully documented glaze",
            "image_url": "https://ceradel.fr/blue.jpg",
            "firing": {"min_celsius": 1180, "max_celsius": 1240, "evidence": "1180-1240 C"},
            "colour": {"name": "Blue"},
        }
        price = at(full, "2026-08-09T00:00:00Z", price=11)
        price["collection_mode"] = "price"
        for key in ("description", "image_url", "firing", "colour"):
            price.pop(key, None)
        postgres.load_source(sync_db, "ceradel", [full], whole=True)
        postgres.load_source(sync_db, "ceradel", [price], whole=True)

        with sync_db.cursor() as cursor:
            cursor.execute(
                "select description, image_url, firing_range, attributes->'colour' colour "
                "from catalogue.source_products"
            )
            product = cursor.fetchone()
        assert product["description"] == "A carefully documented glaze"
        assert product["image_url"] == "https://ceradel.fr/blue.jpg"
        assert product["firing_range"] == "1180-1240 C"
        assert product["colour"] == {"name": "Blue"}

    def test_records_become_source_products(self, sync_db):
        postgres.ensure_staging(sync_db)
        report = postgres.load_source(
            sync_db, "ceradel", [record("1", "Blue glaze"), record("2", "Red glaze")], whole=True
        )
        assert report.records == 2
        assert report.ok
        assert set(active_products(sync_db)) == {"ceradel:1", "ceradel:2"}

    def test_a_whole_dump_retires_what_it_no_longer_lists(self, sync_db):
        """A product that was there last time and is absent now was withdrawn."""
        postgres.ensure_staging(sync_db)
        postgres.load_source(
            sync_db, "ceradel", [record("1", "Blue"), record("2", "Red")], whole=True
        )
        report = postgres.load_source(sync_db, "ceradel", [record("1", "Blue")], whole=True)

        assert report.retired == 1
        assert active_products(sync_db) == {"ceradel:1": True, "ceradel:2": False}

    def test_a_partial_dump_only_ever_adds(self, sync_db):
        """Retiring against an unfinished run withdraws products still for sale."""
        postgres.ensure_staging(sync_db)
        postgres.load_source(
            sync_db, "ceradel", [record("1", "Blue"), record("2", "Red")], whole=True
        )
        report = postgres.load_source(sync_db, "ceradel", [record("1", "Blue")], whole=False)

        assert report.retired == 0
        assert active_products(sync_db) == {"ceradel:1": True, "ceradel:2": True}

    def test_retirement_is_scoped_to_one_source(self, sync_db):
        """Loading one shop must not withdraw another's catalogue."""
        postgres.ensure_staging(sync_db)
        other = {**record("9", "Other shop"), "source": "mayco", "external_id": "mayco:9",
                 "parent_external_id": "mayco:9"}
        postgres.load_source(sync_db, "mayco", [other], whole=True)
        postgres.load_source(sync_db, "ceradel", [record("1", "Blue")], whole=True)
        postgres.load_source(sync_db, "ceradel", [], whole=True)

        products = active_products(sync_db)
        assert products["mayco:9"] is True

    def test_a_retired_product_comes_back_when_the_shop_lists_it_again(self, sync_db):
        postgres.ensure_staging(sync_db)
        postgres.load_source(sync_db, "ceradel", [record("1", "Blue"), record("2", "Red")], whole=True)
        postgres.load_source(sync_db, "ceradel", [record("1", "Blue")], whole=True)
        assert active_products(sync_db)["ceradel:2"] is False

        postgres.load_source(sync_db, "ceradel", [record("1", "Blue"), record("2", "Red")], whole=True)
        assert active_products(sync_db)["ceradel:2"] is True

    def test_staging_is_left_empty(self, sync_db):
        """A source that leaves rows staged would have the next one retire
        against the union of both."""
        postgres.ensure_staging(sync_db)
        postgres.load_source(sync_db, "ceradel", [record("1", "Blue")], whole=True)
        with sync_db.cursor() as cursor:
            cursor.execute("select count(*) as n from import_staging")
            assert cursor.fetchone()["n"] == 0

    def test_the_four_steps_are_still_one_transaction(self, sync_db):
        """Either all four steps happen for a source or none do.

        The `psql` version could be interrupted between staging, loading,
        retiring and truncating, leaving a source counted as loaded with its
        retirement half-applied. Rejecting a single record is now done on a
        savepoint inside this transaction rather than by failing it, so what is
        under test here is that the savepoints did not cost the outer guarantee:
        the load either happened or it did not, and staging is clean either way.
        """
        postgres.ensure_staging(sync_db)
        postgres.load_source(sync_db, "ceradel", [record("1", "Blue"), record("2", "Red")], whole=True)

        broken = record("3", "Broken")
        del broken["product_url"]  # not null in catalogue.source_products

        report = postgres.load_source(
            sync_db, "ceradel", [record("1", "Blue"), broken], whole=True
        )

        # The row the schema refused is refused; the one beside it is not, and
        # the product missing from this dump is retired as it should be.
        assert report.rejected == 1
        assert active_products(sync_db) == {"ceradel:1": True, "ceradel:2": False}
        with sync_db.cursor() as cursor:
            cursor.execute("select count(*) as n from import_staging")
            assert cursor.fetchone()["n"] == 0

    def test_the_retire_statement_takes_the_source_as_a_parameter(self, sync_db):
        """It used to be `RETIRE.replace("%(source)s", f"\'{source}\'")`.

        Source ids come from a checked-in file, so this was never an injection
        surface in practice — but it blocked running the load in-process and it
        was one careless caller away from being one. Passing a quote-laden name
        now reaches the database as a value that matches nothing, rather than as
        SQL that ends the string and starts a new statement.

        (`catalogue.sources` separately constrains ids to `^[a-z0-9][a-z0-9-]*$`,
        so this exercises the statement directly: the point under test is the
        parameterisation, not the check constraint behind it.)
        """
        postgres.ensure_staging(sync_db)
        postgres.load_source(sync_db, "ceradel", [record("1", "Blue")], whole=True)

        hostile = "ceradel'; drop table catalogue.source_products; --"
        with sync_db.cursor() as cursor:
            cursor.execute(postgres.RETIRE, {"source": hostile})
            assert cursor.rowcount == 0

        # The table is still there, and ceradel's own row was not touched.
        assert active_products(sync_db) == {"ceradel:1": True}

    def test_a_record_the_database_refuses_costs_only_itself(self, sync_db):
        """One bad row used to discard the source it arrived with.

        `catalogue.load_record` is right to refuse a price with no currency —
        it is not a weaker fact, it is a meaningless one — but the refusal
        happened inside one statement covering every record, so the whole
        source went with it. art-academy-direct lost all 4,980 rows that way on
        2026-08-11, and gwn-pottery and sheffield-pottery a run each.
        """
        bad = record("2", "Priceless") | {"currency": None}
        postgres.ensure_staging(sync_db)
        report = postgres.load_source(
            sync_db, "ceradel", [record("1", "Blue"), bad, record("3", "Red")], whole=True
        )

        assert report.ok
        assert report.rejected == 1
        assert "currency" in report.rejects[0]
        assert set(active_products(sync_db)) == {"ceradel:1", "ceradel:3"}

    def test_a_refused_record_is_not_grounds_for_retiring_it(self, sync_db):
        """It was listed. Failing to load it is our problem, not the shop's."""
        postgres.ensure_staging(sync_db)
        postgres.load_source(
            sync_db, "ceradel", [record("1", "Blue"), record("2", "Red")], whole=True
        )
        assert active_products(sync_db) == {"ceradel:1": True, "ceradel:2": True}

        # Same two products, but one now arrives unloadable.
        postgres.load_source(
            sync_db,
            "ceradel",
            [record("1", "Blue"), record("2", "Red") | {"currency": None}],
            whole=True,
        )
        assert active_products(sync_db) == {"ceradel:1": True, "ceradel:2": True}


class TestLoadDump:
    def write(self, directory: Path, source: str, rows: list[dict], partial: bool = False) -> None:
        suffix = ".partial.ndjson" if partial else ".ndjson"
        (directory / f"{source}{suffix}").write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
        )

    def test_a_directory_loads_and_reports(self, sync_db, tmp_path):
        self.write(tmp_path, "ceradel", [record("1", "Blue"), record("2", "Red")])
        plans, _ = postgres.plan_load(tmp_path)
        report = postgres.load_dump(sync_db, plans, SOURCES, {"ceradel"})

        assert report.ok
        assert report.records == 2
        assert report.run_id is not None
        assert report.products == 2

    def broken_file(self, directory: Path, source: str) -> None:
        """A dump that cannot be read at all — the source-level kind of failure.

        A single record the schema refuses is no longer this: it is rejected on
        its own and the rest of the source loads. What still fails a whole
        source is a file that cannot be turned into records in the first place.
        """
        (directory / f"{source}.ndjson").write_text('{"format": "truncated\n', encoding="utf-8")

    def test_one_bad_source_does_not_cost_the_others(self, sync_db, tmp_path):
        """A defect in the third of sixty-three sources used to cost the other sixty."""
        self.write(tmp_path, "ceradel", [record("1", "Blue")])
        self.broken_file(tmp_path, "mayco")
        plans, _ = postgres.plan_load(tmp_path)

        report = postgres.load_dump(sync_db, plans, SOURCES, {"ceradel", "mayco"})

        assert [source.source for source in report.loaded] == ["ceradel"]
        assert [source.source for source in report.failures] == ["mayco"]
        assert not report.ok
        assert report.products == 1

    def test_the_import_run_records_the_outcome(self, sync_db, tmp_path):
        self.write(tmp_path, "ceradel", [record("1", "Blue")])
        plans, _ = postgres.plan_load(tmp_path)
        report = postgres.load_dump(sync_db, plans, SOURCES, {"ceradel"})

        with sync_db.cursor() as cursor:
            cursor.execute(
                "select status, record_count from catalogue.import_runs where id = %s",
                (report.run_id,),
            )
            row = cursor.fetchone()
        assert row["status"] == "complete"
        assert row["record_count"] == 1

    def test_a_failed_load_marks_the_import_run_failed(self, sync_db, tmp_path):
        self.broken_file(tmp_path, "ceradel")
        plans, _ = postgres.plan_load(tmp_path)
        report = postgres.load_dump(sync_db, plans, SOURCES, {"ceradel"})

        with sync_db.cursor() as cursor:
            cursor.execute("select status from catalogue.import_runs where id = %s", (report.run_id,))
            assert cursor.fetchone()["status"] == "failed"

    def test_keep_stale_suppresses_retirement(self, sync_db, tmp_path):
        self.write(tmp_path, "ceradel", [record("1", "Blue"), record("2", "Red")])
        plans, _ = postgres.plan_load(tmp_path)
        postgres.load_dump(sync_db, plans, SOURCES, {"ceradel"})

        self.write(tmp_path, "ceradel", [record("1", "Blue")])
        plans, _ = postgres.plan_load(tmp_path)
        postgres.load_dump(sync_db, plans, SOURCES, {"ceradel"}, keep_stale=True)

        assert active_products(sync_db)["ceradel:2"] is True

    def test_sources_are_described_from_the_configuration(self, sync_db, tmp_path):
        """`load_record` creates a source row from the id alone; the label, shop
        URL and country come from sources.json."""
        self.write(tmp_path, "ceradel", [record("1", "Blue")])
        plans, _ = postgres.plan_load(tmp_path)
        postgres.load_dump(sync_db, plans, SOURCES, {"ceradel"})

        with sync_db.cursor() as cursor:
            cursor.execute("select label, homepage_url, metadata from catalogue.sources where id='ceradel'")
            row = cursor.fetchone()
        assert row["label"] == "Ceradel"
        assert row["homepage_url"] == "https://ceradel.fr/"
        assert row["metadata"]["country"] == "FR"

    def test_the_load_is_traceable_to_the_crawl_that_produced_it(self, sync_db, tmp_path):
        from mb_ceramics_catalogue.ops import runs as ops_runs

        with sync_db.cursor() as cursor:
            cursor.execute(
                "insert into catalogue.runs (kind, status) values ('manual', 'running') returning id"
            )
            run_id = cursor.fetchone()["id"]
        del ops_runs

        self.write(tmp_path, "ceradel", [record("1", "Blue")])
        plans, _ = postgres.plan_load(tmp_path)
        report = postgres.load_dump(sync_db, plans, SOURCES, {"ceradel"}, run_id=run_id)

        with sync_db.cursor() as cursor:
            cursor.execute("select run_id from catalogue.import_runs where id = %s", (report.run_id,))
            assert cursor.fetchone()["run_id"] == run_id


class TestConcurrency:
    def test_two_connections_stage_independently(self, sync_db):
        """The reason staging is a temp table rather than a shared unlogged one.

        Two workers loading at once into one table would each retire against the
        union of both dumps — which, for two sources, withdraws everything
        neither of them happened to list.
        """
        dsn = postgres_dsn()
        assert dsn is not None
        postgres.ensure_staging(sync_db)

        with psycopg.connect(dsn, row_factory=dict_row, autocommit=True) as other:
            postgres.ensure_staging(other)
            with sync_db.cursor() as cursor:
                cursor.execute(
                    "insert into import_staging (source_file, record) values ('a', '{}'::jsonb)"
                )
            with other.cursor() as cursor:
                cursor.execute("select count(*) as n from import_staging")
                assert cursor.fetchone()["n"] == 0, "one connection saw the other's staged rows"
