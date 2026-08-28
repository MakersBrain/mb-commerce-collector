"""`catalogue-load`: load a dump directory into the PostgreSQL catalogue.

A thin wrapper over `storage.postgres`, which is now a library the worker calls
directly. This stays a command because backfills and loading an old dump
directory are real operations that have nothing to do with a worker, and because
`docker compose run --rm loader` points at it.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from mb_ceramics_catalogue import __version__
from mb_ceramics_catalogue.config.settings import Settings
from mb_ceramics_catalogue.config.sources import SourcesFile, default_path
from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.storage import db, postgres


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="catalogue-load", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--dsn", default="",
                        help="libpq connection string; defaults to $CATALOGUE_DSN")
    parser.add_argument("--data", type=Path, required=True, help="a dump directory")
    parser.add_argument("--sources-file", type=Path, default=None)
    parser.add_argument("--source", default="all",
                        help="a source name, a comma-separated list, or 'all'")
    parser.add_argument("--run-id", type=UUID, default=None,
                        help="the catalogue.runs id this dump came from, so the load is "
                             "traceable to the crawl that produced it")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would load, touch nothing")
    parser.add_argument("--keep-stale", action="store_true",
                        help="leave rows this dump no longer lists marked active")
    parser.add_argument("--describe-only", action="store_true",
                        help="copy labels, URLs and countries from sources.json, load nothing")
    parser.add_argument("--complete-only", action="store_true",
                        help="ignore .partial.ndjson files even where they are a source's only data")
    parser.add_argument("--log-level", default="INFO")
    return parser


def run(options: argparse.Namespace) -> int:
    obs.configure(options.log_level)
    sources = SourcesFile.load(options.sources_file or default_path())
    settings = Settings()

    plans, skipped = postgres.plan_load(options.data)
    if options.complete_only:
        skipped += [
            (plan.source, f"{plan.records} partial records, --complete-only")
            for plan in plans
            if not plan.whole
        ]
        plans = [plan for plan in plans if plan.whole]
    if options.source != "all":
        wanted = set(sources.select(options.source))
        plans = [plan for plan in plans if plan.source in wanted]
        skipped = [entry for entry in skipped if entry[0] in wanted]
    if not plans:
        print(f"no source in {options.data} has any records to load", file=sys.stderr)
        return 1

    for plan in plans:
        # A file can be complete in shape and still not be grounds for
        # retirement, so the note says what it does, not what it is called.
        shape = "" if plan.whole else "  adds only"
        print(f"  {plan.source:24s} {plan.records:6d} records{shape}")
    total = sum(plan.records for plan in plans)
    print(f"  {'total':24s} {total:6d} records from {len(plans)} sources")
    # Never silently: a source left out of a load is the kind of thing that is
    # noticed weeks later as a supplier that mysteriously stopped stocking.
    for source, why in sorted(skipped):
        print(f"  skipped {source:22s} {why}")
    if options.dry_run:
        return 0

    # Only the sources this load covers, so the shops that are configured but not
    # yet crawled — and the ones whose scrape came back empty — do not appear in
    # the database as suppliers with no stock.
    described = {plan.source for plan in plans}
    raw_sources = {name: config.as_scraper_config() for name, config in sources.items()}
    dsn = db.dsn_from_environment(options.dsn)

    with psycopg.connect(dsn, row_factory=dict_row, autocommit=True) as connection:
        if options.describe_only:
            count = postgres.describe_sources(connection, raw_sources, described)
            print(f"described {count} sources from sources.json")
            return 0

        report = postgres.load_dump(
            connection,
            plans,
            raw_sources,
            described,
            keep_stale=options.keep_stale,
            run_id=options.run_id,
            stock_trends_enabled=settings.stock_trends_enabled,
        )

    print(f"\nimport run {report.run_id}")
    for loaded in report.loaded:
        note = f" ({loaded.retired} withdrawn)" if loaded.retired else ""
        print(f"  loaded {loaded.source}{note}")
    print(
        f"\n{report.products} source products, {report.offers} offer observations "
        f"({report.identities} identity rows carry no offer)"
    )
    if report.failures:
        print(f"\n{len(report.failures)} source(s) did not load:")
        for failure in report.failures:
            print(f"  {failure.source:24s} {failure.error}")
        return 1
    return 0


def main() -> int:
    options = build_parser().parse_args()
    try:
        return run(options)
    except ValueError as error:
        print(f"catalogue-load: {error}", file=sys.stderr)
        return 2
    except psycopg.Error as error:
        print(f"catalogue-load: database error: {error}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
