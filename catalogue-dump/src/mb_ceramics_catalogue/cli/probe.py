"""`catalogue-probe`: run one scraper and report what it actually yields.

Development aid. It never writes a dump, so it is safe to point at any source
while a scraper is being tuned — and with `--cache-mode replay` it is safe to
point at one repeatedly without asking the shop anything at all.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from mb_ceramics_catalogue import scrapers
from mb_ceramics_catalogue.config.settings import CrawlParams
from mb_ceramics_catalogue.config.sources import SourcesFile, default_path
from mb_ceramics_catalogue.crawl.session import open_session
from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.observability import tracing
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    open_local_commerce_session,
)
from mb_ceramics_catalogue.scrapers.cache import MODES as CACHE_MODES
from mb_ceramics_catalogue.scrapers.record import RecordBuilder, coverage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="catalogue-probe", description=__doc__)
    parser.add_argument("source")
    parser.add_argument("--sources-file", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=40)
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--browser", choices=("never", "auto", "always"), default="auto")
    parser.add_argument(
        "--impersonate", choices=("never", "auto"), default="auto",
        help="retry a refused request with a browser TLS handshake (needs the impersonate extra)",
    )
    parser.add_argument(
        "--robots", choices=("obey", "ignore"), default="ignore",
        help="whether robots.txt Disallow binds; pace comes from the rate limiter either way",
    )
    parser.add_argument("--cache", nargs="?", const=".cache", default=None, metavar="DIR")
    parser.add_argument("--cache-mode", choices=CACHE_MODES, default="auto")
    parser.add_argument(
        "--pipeline",
        choices=("legacy", "connector_canary"),
        default="legacy",
        help="explicitly select the reusable connector canary",
    )
    parser.add_argument("--show", type=int, default=2, help="how many sample rows to print")
    parser.add_argument("--log-level", default="WARNING")
    return parser


async def run(options: argparse.Namespace) -> int:
    obs.configure(options.log_level)
    tracing.configure("catalogue-probe")
    sources = SourcesFile.load(options.sources_file or default_path())
    if options.source not in sources:
        raise ValueError(f"unknown source; known: {', '.join(sorted(sources.names()))}")
    config = sources[options.source]

    params = CrawlParams(
        limit=options.limit,
        delay=options.delay,
        concurrency=4,
        browser=options.browser,
        impersonate=options.impersonate,
        robots=options.robots,
        cache_mode=options.cache_mode if options.cache else "off",
        pipeline=options.pipeline,
        # A probe wants what is on the site now, not what was there last week.
        cache_max_age_hours=0 if options.cache_mode == "replay" else 1,
    )

    with RecordBuilder(sources.as_scraper_configs()):
        cache_directory = Path(options.cache) if options.cache else None
        if params.pipeline == "connector_canary":
            async with open_local_commerce_session(
                params, cache_directory
            ) as local_session:
                local_scraper = local_session.build(
                    config.scraper,
                    options.source,
                    config.as_scraper_config(),
                    None,
                )
                result = await local_scraper.run(options.limit)
                scraper_method = local_scraper.method
        else:
            async with open_session(params, cache_directory) as legacy_session:
                legacy_scraper = scrapers.build(
                    config.scraper,
                    options.source,
                    config.as_scraper_config(),
                    legacy_session.fetcher,
                )
                result = await legacy_scraper.run(options.limit)
                scraper_method = legacy_scraper.method

    records = result.records
    print(f"\nsource   : {options.source} ({config.label})")
    print(f"scraper  : {config.scraper} / method={scraper_method}")
    print(f"records  : {len(records)}   requests={result.requests}   rendered={result.rendered_pages}")
    print(f"discovered={result.discovered}  truncated={result.truncated}  errors={len(result.errors)}")
    for note in result.notes:
        print(f"  note   : {note}")
    for error in result.errors[:5]:
        print(f"  error  : {error['url']} -> {error['error'][:160]}")

    if not records:
        print("\nno records")
        return 1

    print("\nfield coverage (rows carrying the field):")
    for field, count in sorted(coverage(records).items(), key=lambda item: -item[1]):
        share = 100 * count / len(records)
        print(f"  {field:22s} {count:5d} {share:5.1f}% {'#' * round(share / 5)}")

    def summarise(value: Any) -> Any:
        if isinstance(value, dict | list):
            return json.dumps(value, ensure_ascii=False)[:180]
        return value

    for row in records[: options.show]:
        print("\n" + "-" * 70)
        for key, value in row.items():
            if key == "raw" or value in (None, [], {}):
                continue
            print(f"  {key:22s} {summarise(value)}")
    return 0


def main() -> int:
    options = build_parser().parse_args()
    try:
        return asyncio.run(run(options))
    except KeyboardInterrupt:  # pragma: no cover
        return 130
    except ValueError as error:
        print(f"catalogue-probe: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
