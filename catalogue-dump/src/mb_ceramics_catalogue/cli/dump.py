"""`catalogue-dump`: collect the configured catalogues into an auditable dump.

An entry point parses arguments and delegates. That is all this does now — the
164-line `main()` that also configured logging, validated sources, built the
manifest, chose a display, constructed the fetch stack, orchestrated and
cancelled tasks, persisted SQLite history and printed a report has been split
along those lines (§4.3), and what is left here is the part that is genuinely
about the command line.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from uuid import UUID

from mb_ceramics_catalogue import __version__
from mb_ceramics_catalogue.config.settings import DEFAULT_SOURCE_TIMEOUT, CrawlParams, Settings
from mb_ceramics_catalogue.config.sources import SourcesFile, default_path
from mb_ceramics_catalogue.crawl import artifacts
from mb_ceramics_catalogue.crawl import progress as progress_module
from mb_ceramics_catalogue.crawl.runner import CrawlRunner, crawl
from mb_ceramics_catalogue.crawl.session import open_session
from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.observability import tracing
from mb_ceramics_catalogue.ops import recording
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    open_local_commerce_session,
)
from mb_ceramics_catalogue.scrapers.cache import MODES as CACHE_MODES
from mb_ceramics_catalogue.scrapers.record import RecordBuilder
from mb_ceramics_catalogue.storage.history import persist_history

DEFAULT_CACHE = Path(".cache")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="catalogue-dump", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--source", default="all", help="a source name, a comma-separated list, or 'all'")
    parser.add_argument("--sources-file", type=Path, default=None,
                        help="a sources.json to use instead of the packaged one")
    parser.add_argument("--out", default="catalogue-dumps")
    parser.add_argument("--limit", type=int, help="maximum products per source (for sampling)")
    parser.add_argument("--delay", type=float, default=0.0,
                        help="seconds one crawling slot waits between its requests to a host "
                             "(default 0: no wait). A source's own delay, and the backoff a host "
                             "earns by failing, still apply")
    parser.add_argument("--concurrency", type=int, default=8,
                        help="most requests in flight per host. Slots start at 2, climb while the "
                             "host answers and halve when it errors; a failing host also earns a "
                             "gap between requests until it recovers")
    parser.add_argument("--sources", type=int, default=4, help="sources crawled at once")
    parser.add_argument("--browser", choices=("never", "auto", "always"), default="auto")
    parser.add_argument(
        "--impersonate", choices=("never", "auto"), default="auto",
        help="retry a refused request with a browser TLS handshake (needs the impersonate extra)",
    )
    parser.add_argument(
        "--robots", choices=("obey", "ignore"), default="ignore",
        help="whether robots.txt Disallow binds; pace comes from the rate limiter either way",
    )
    parser.add_argument("--source-timeout", type=float, default=DEFAULT_SOURCE_TIMEOUT,
                        metavar="SECONDS",
                        help="how long one source may run before the crawl gives up on it "
                             f"(default {DEFAULT_SOURCE_TIMEOUT:.0f}). A source may set a stricter "
                             "one of its own; it may not set a looser one")
    parser.add_argument("--history-db", help="SQLite file for append-only price history")
    parser.add_argument("--cache", nargs="?", const=str(DEFAULT_CACHE), default=None, metavar="DIR",
                        help=f"record every response under DIR so a later run can replay it "
                             f"(default {DEFAULT_CACHE})")
    parser.add_argument("--cache-mode", choices=CACHE_MODES, default="auto",
                        help="auto: replay what is stored and fetch the rest; replay: never touch "
                             "the network, so parsing can be reworked offline; refresh: fetch "
                             "everything and overwrite what is stored")
    parser.add_argument("--cache-max-age", type=float, default=20.0, metavar="HOURS",
                        help="how old a stored response may be before auto refetches it "
                             "(0 = never stale). The default is under a day on purpose: a daily "
                             "price run with a week-long max age replays yesterday's pages and "
                             "reports success while changing no prices")
    parser.add_argument(
        "--stale-on-error", action="store_true",
        help="reuse an expired cached GET only after transient live-fetch failure",
    )
    parser.add_argument(
        "--refresh-mode", choices=("price", "full"), default="full",
        help="price keeps API identity/offer fields; full also refreshes enrichment",
    )
    parser.add_argument(
        "--pipeline", choices=("legacy", "connector_canary"), default="legacy",
        help="explicitly select the reusable connector canary; legacy remains the default",
    )
    parser.add_argument(
        "--dataset", dest="datasets", action="append",
        choices=(
            "ceramics", "ceramics.catalogue_item.v2", "ceramics.catalogue_identity.v2",
            "commerce.price_observation.v1", "commerce.stock_observation.v1",
            "commerce.document.v1",
        ),
        help="connector dataset to publish (repeatable; default: current ceramics output)",
    )
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    parser.add_argument("--log-json", action="store_true",
                        help="one JSON object per line even on a terminal")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--plain-progress", action="store_true",
                        help="the redrawing table instead of the interactive view")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-empty", action="store_true",
                        help="let an empty result replace an existing dump")
    record_group = parser.add_argument_group(
        "recording",
        "Leave a queryable record of this crawl in catalogue.runs / jobs / job_progress, "
        "so a hand-run crawl is visible in the operations UI exactly like a scheduled one",
    )
    record_group.add_argument("--record", action="store_true",
                           help="create a run and its jobs, and report progress to the database")
    record_group.add_argument("--run-id", type=UUID, default=None,
                           help="attach to an existing run instead of creating one (implies --record)")
    record_group.add_argument("--dsn", default="",
                           help="libpq connection string; defaults to $CATALOGUE_DSN")
    return parser


def report(outcomes: list, cache_line: str, interrupted: bool, history: dict[str, int] | None) -> None:
    """The human-readable summary the command has always printed on stdout.

    Kept on `print` rather than moved to the logger: this is the command's
    result, and it should survive `--log-level ERROR` and land on stdout where a
    pipe can read it, not on stderr among the diagnostics.
    """
    for outcome in outcomes:
        summary = outcome.summary
        flags = []
        if summary["truncated"]:
            flags.append("truncated")
        if summary["robots_ignored"]:
            flags.append("robots-ignored")
        if summary.get("interrupted"):
            flags.append("interrupted")
        print(
            f"{outcome.source:22s} {summary['scraper']:14s} "
            f"{summary.get('extraction_method', ''):10s} "
            f"{summary['records']:6d} records  "
            f"{summary['requests']:5d} requests  {summary['rendered_pages']:4d} rendered  "
            f"{summary['error_count']:4d} errors  {' '.join(flags)}"
        )
    total = sum(len(outcome.records) for outcome in outcomes)
    print(f"{'total':22s} {total:6d} records across {len(outcomes)} sources")
    if cache_line:
        print(cache_line)
    if interrupted:
        print("interrupted: partial rows written as <source>.partial.ndjson; rerun to resume")
    if history is not None:
        print(
            f"history: {history['new_products']} new products, "
            f"{history['new_price_observations']} new observations"
        )


async def run(options: argparse.Namespace) -> int:
    settings = Settings()
    obs.configure(options.log_level, json=options.log_json or settings.log_json)
    tracing.configure("catalogue-dump")
    log = obs.get_logger("catalogue.dump")

    params = CrawlParams.from_namespace(options)
    sources = SourcesFile.load(options.sources_file or default_path())
    selected = sources.select(options.source)
    if params.pipeline == "connector_canary" and params.datasets != ("ceramics",):
        raise ValueError(
            "local connector_canary currently writes only the ceramics compatibility "
            "dataset; multi-dataset publication requires the PostgreSQL worker"
        )

    output = Path(options.out)
    if not params.dry_run:
        output.mkdir(parents=True, exist_ok=True)

    manifest = artifacts.Manifest()
    log.info("run.started", sources=len(selected), out=str(output), version=__version__)

    runner_holder: list[CrawlRunner] = []

    def request_stop() -> None:
        """The interactive display's stop button.

        Wired to the same graceful path SIGTERM uses rather than to a second,
        subtly different one. It is a list because the runner does not exist
        until `crawl` builds it, and the sinks are constructed before that.
        """
        for runner in runner_holder:
            runner.stop()

    sinks = progress_module.terminal_sinks(
        len(selected),
        force=options.progress,
        disabled=options.no_progress,
        plain=options.plain_progress,
        on_stop=request_stop,
    )
    for sink in sinks:
        opener = getattr(sink, "open", None)
        if opener is not None:
            await opener()

    cache_dir = Path(options.cache) if options.cache else None

    # Source traits bound for this whole crawl. This was a module-level dict
    # mutated by `learn_sources`; a worker binds one of these per job instead.
    with RecordBuilder(sources.as_scraper_configs()):
        async with (
            recording.record_run(options, params, sources, selected) as record,
            progress_module.Progress(len(selected), sinks + record.sinks) as progress,
        ):
            if params.pipeline == "connector_canary":
                async with open_local_commerce_session(params, cache_dir) as local_session:
                    outcomes, interrupted = await crawl(
                        sources,
                        selected,
                        None,
                        params,
                        progress,
                        None if params.dry_run else output,
                        on_runner=runner_holder.append,
                        scraper_factory=local_session.build,
                    )
                    cache_line = (
                        local_session.cache_summary()
                        if local_session.cache_enabled
                        else ""
                    )
            else:
                async with open_session(params, cache_dir) as legacy_session:
                    outcomes, interrupted = await crawl(
                        sources,
                        selected,
                        legacy_session,
                        params,
                        progress,
                        None if params.dry_run else output,
                        on_runner=runner_holder.append,
                    )
                    cache_line = (
                        legacy_session.cache_summary()
                        if legacy_session.cache.enabled
                        else ""
                    )
            await record.finish(outcomes)

    # Anything the crawl did not already write, because it was cancelled before
    # `run_source` reached its write. A partial never replaces a complete file.
    for outcome in outcomes:
        summary = outcome.summary
        if not params.dry_run and "write_status" not in summary:
            artifact = (
                artifacts.write_partial(output, outcome.source, outcome.records)
                if summary.get("interrupted")
                else artifacts.write_source(output, outcome.source, outcome.records, params.allow_empty)
            )
            summary["write_status"] = artifact.status
        manifest.record(outcome.source, summary)

    history = None
    if options.history_db and not params.dry_run:
        history = persist_history(
            options.history_db, [(o.source, o.as_payload()) for o in outcomes]
        )

    report(outcomes, cache_line, interrupted, history)

    manifest.finish(sum(len(outcome.records) for outcome in outcomes))
    if not params.dry_run:
        manifest.write(output)

    failed = sum(1 for o in outcomes if o.summary["error_count"] and not o.summary["records"])
    log.info("run.complete", sources=len(outcomes), failed=failed, interrupted=interrupted)
    # Interrupted is not a failure: the partials are written and the manifest
    # says so. A source that collected nothing but errored is.
    return 1 if failed else 0


def main() -> int:
    options = build_parser().parse_args()
    try:
        return asyncio.run(run(options))
    except KeyboardInterrupt:  # pragma: no cover - the signal handler normally wins
        return 130
    except ValueError as error:
        # An unknown source, or a sources.json that does not validate. Both are
        # the operator's input, so they get a message rather than a traceback.
        print(f"catalogue-dump: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
