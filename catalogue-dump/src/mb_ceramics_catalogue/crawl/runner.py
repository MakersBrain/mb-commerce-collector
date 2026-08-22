"""Deciding what to crawl, running it, and stopping cleanly. Nothing else.

This is what `main()` was actually about, under the argument parsing, the
logging setup, the manifest building, the display selection, the SQLite history
and the report printing. Those are all elsewhere now; what is left is the ~120
lines that orchestrate eighty concurrent sources and cancel them properly.

Four things it does that the original did not (§4.5):

* **`asyncio.TaskGroup`** instead of `gather` plus a manual cancel loop. A fatal
  error in the orchestration used to leave the other seventy-nine tasks to be
  cancelled by hand in an `except` clause. Per-source handles are still kept,
  because cancelling *one* source is a requirement (§5.6) and
  `tg.create_task()` returns them.
* **A per-source deadline.** There was none. A source that hung — a slow origin,
  a browser that never settles — held its slot indefinitely, which on a 03:00
  schedule means the run is still going at 09:00 with nobody watching.
* **A real SIGTERM handler.** `main()` caught `KeyboardInterrupt`, which is what
  Ctrl-C raises. Containers and systemd send SIGTERM, which is neither caught
  nor a `KeyboardInterrupt`, so `docker stop` could land in the middle of
  `write_source`.
* **Cancellation keeps what was collected**, through the same partial path the
  stop button uses, rather than only on Ctrl-C.
"""

from __future__ import annotations

import asyncio
import signal
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from mb_ceramics_catalogue import scrapers
from mb_ceramics_catalogue.config.settings import CrawlParams
from mb_ceramics_catalogue.config.sources import SourceConfig, SourcesFile
from mb_ceramics_catalogue.crawl import artifacts
from mb_ceramics_catalogue.crawl.progress import Progress
from mb_ceramics_catalogue.crawl.session import CrawlSession
from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.observability import metrics, tracing
from mb_ceramics_catalogue.scrapers.activity import CURRENT_SOURCE
from mb_ceramics_catalogue.scrapers.base import BrowserUnavailable
from mb_ceramics_catalogue.scrapers.record import coverage

LOGGER = obs.get_logger("catalogue.runner")


class RunnableScraper(Protocol):
    """The small scraper surface required by crawl orchestration."""

    method: str
    result: Any

    async def run(self, limit: int | None = None) -> Any: ...


ScraperFactory = Callable[[str, str, dict[str, Any], Any], RunnableScraper]
"""Construct one scraper without owning its session or orchestration."""


@dataclass
class SourceOutcome:
    """One source's records and the summary describing how they were collected."""

    source: str
    records: list[dict[str, Any]] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def interrupted(self) -> bool:
        return bool(self.summary.get("interrupted"))

    def as_payload(self) -> dict[str, Any]:
        """The `{records, summary}` shape the loader and the manifest expect."""
        return {"records": self.records, "summary": self.summary}


def barren(summary: dict[str, Any]) -> str | None:
    """Why this source's work produced nothing, if that is what happened.

    A source that listed products and then kept none of them has not succeeded
    at anything, but it raises nothing either: discovery worked, every page
    fetched, and no row survived. Ten sources were in that state on 2026-08-12
    and every one of their jobs was green — countrylove spent eighty-four
    minutes finding 18,883 product URLs, read zero, and reported success.

    The two ways to get here are not the same failure, and saying so matters:
    an extractor that recognised nothing is a parser to fix, while rows that
    were read and then dropped by the materials scope are a *discovery* problem
    — the crawl is walking the wrong part of the shop. artequipment and
    mayco-glasuren spent five nights reported as broken parsers when both
    parsers were reading every page correctly.

    Read by the worker to decide the job's outcome, and returned as a sentence
    because it goes straight into `catalogue.jobs.error` for an operator.
    """
    if summary.get("records") or summary.get("interrupted"):
        return None
    discovered = int(summary.get("discovered") or 0)
    if not discovered:
        return None
    filtered = int(summary.get("filtered") or 0)
    invalid = int(summary.get("invalid") or 0)
    if filtered and not invalid:
        return (
            f"listed {discovered} products, extracted {filtered}, and kept none: "
            "every row fell outside this source's materials scope, so the crawl "
            "is listing the wrong pages"
        )
    if invalid and not filtered:
        return (
            f"listed {discovered} products, extracted {invalid}, and kept none: "
            "every extracted row lacked a usable identity or price"
        )
    if filtered and invalid:
        return (
            f"listed {discovered} products, extracted {filtered + invalid}, and kept none: "
            f"{filtered} fell outside this source's materials scope and {invalid} "
            "lacked a usable identity or price"
        )
    return (
        f"listed {discovered} products and extracted none: "
        f"the {summary.get('scraper')} scraper recognised nothing on any of them"
    )


#: Outcomes that say the host refused the pace or the network faltered, rather
#: than that the crawl asked the wrong thing. `_outcome` in `scrapers.base`
#: names 429 and 403 individually and buckets the rest by class, so a server
#: fault arrives as "5xx".
THROTTLED_OUTCOMES = ("429", "5xx", "timeout", "transport_error")


def transient(summary: dict[str, Any]) -> bool:
    """Whether this source collected nothing for a reason that may clear.

    The worker asks before it spends a job's terminal state. Three of the
    eighty-two sources in the 2026-08-19 run failed this way — two Shopify
    stores answering 429 to the very first request and one WooCommerce site
    whose database was down — and every one of them had two of its three
    attempts still unspent, because a failure recorded from a summary never
    reached the retry budget at all.

    The distinction that matters is against `barren`: a scraper that listed
    products and recognised none of them will recognise none of them again in
    five minutes, and retrying it three times only triples the crawl. So the
    answer is yes only when nothing was extracted *and* the failures on record
    are the host's refusals rather than our parsing.

    A source stopped by its own deadline is excluded for the same reason in a
    different shape: the retry would not fail faster, it would fail an hour
    later, three times over.
    """
    if summary.get("records") or summary.get("interrupted"):
        return False
    if summary.get("deadline_exceeded"):
        return False
    if not summary.get("error_count"):
        return False
    outcomes = summary.get("outcome_counts") or {}
    return any(int(outcomes.get(name) or 0) for name in THROTTLED_OUTCOMES)


def summarise(
    name: str,
    config: SourceConfig,
    result: Any,
    *,
    method: str = "",
    interrupted: bool = False,
) -> dict[str, Any]:
    """The per-source summary, identical in shape to the one `dump.py` wrote.

    `plan_load` reads `write_status` and `truncated` off this via the manifest
    to decide whether a file may be treated as a whole catalogue, so the field
    names here are a contract with the loader, not a display choice.
    """
    records = list(getattr(result, "records", []))
    summary: dict[str, Any] = {
        "source": name,
        "label": config.label,
        "scraper": config.scraper,
        "records": len(records),
        "discovered": getattr(result, "discovered", 0),
        "requests": getattr(result, "requests", 0),
        "rendered_pages": getattr(result, "rendered_pages", 0),
        "http_tx_bytes_estimated": getattr(result, "http_tx_bytes_estimated", 0),
        "http_rx_bytes_estimated": getattr(result, "http_rx_bytes_estimated", 0),
        "browser_tx_bytes_estimated": getattr(result, "browser_tx_bytes_estimated", 0),
        "browser_rx_bytes_estimated": getattr(result, "browser_rx_bytes_estimated", 0),
        "cache_bytes_read": getattr(result, "cache_bytes_read", 0),
        "direct_requests": getattr(result, "direct_requests", 0),
        "impersonated_requests": getattr(result, "impersonated_requests", 0),
        "browser_requests": getattr(result, "browser_requests", 0),
        "proxy_requests": getattr(result, "proxy_requests", 0),
        "proxy_bytes_reserved": getattr(result, "proxy_bytes_reserved", 0),
        "proxy_bytes_estimated": getattr(result, "proxy_bytes_estimated", 0),
        "browser_gain": getattr(result, "browser_gain", 0),
        "filtered": getattr(result, "filtered", 0),
        "invalid": getattr(result, "invalid", 0),
        "browser_zero_gain": getattr(result, "browser_zero_gain", 0),
        "outcome_counts": dict(getattr(result, "outcome_counts", {})),
        "truncated": getattr(result, "truncated", False),
        "robots_ignored": config.ignore_robots,
        "error_count": len(getattr(result, "errors", [])),
        "errors": list(getattr(result, "errors", []))[:25],
        "notes": list(getattr(result, "notes", [])),
        "field_coverage": coverage(records),
    }
    if interrupted:
        summary["interrupted"] = True
    else:
        # Only a completed run states its extraction method and scope; a
        # partial one has not necessarily reached the code path that decides.
        summary["extraction_method"] = method
        summary["scope"] = config.scope
    return summary


async def run_source(
    name: str,
    config: SourceConfig,
    session: CrawlSession | None,
    params: CrawlParams,
    progress: Progress,
    output: Path | None = None,
    *,
    scraper_factory: ScraperFactory | None = None,
) -> SourceOutcome:
    """Collect one source, write it, and report it. The unit the worker runs.

    Survives nearly intact from `dump.py`: it was already the right shape, and
    the worker calls exactly this.
    """
    CURRENT_SOURCE.set(name)
    scraper_config = config.as_scraper_config()

    with obs.bound(source=name, scraper=config.scraper), tracing.span(
        "job", **{"catalogue.source": name, "catalogue.scraper": config.scraper}
    ):
        factory: ScraperFactory
        fetcher: Any
        if scraper_factory is None:
            if session is None:
                raise ValueError("default scraper factory requires a crawl session")
            factory = scrapers.build
            fetcher = session.fetcher
        else:
            factory = scraper_factory
            fetcher = session.fetcher if session is not None else None
        scraper = factory(config.scraper, name, scraper_config, fetcher)
        await progress.started(name, scraper.result, config.scraper, scraper.method)
        started = time.monotonic()

        deadline_exceeded = False
        try:
            # The deadline the original had nowhere. A source that never
            # finishes is treated as a failed source rather than as a run that
            # never ends.
            async with asyncio.timeout(params.timeout_for(config.timeout_seconds)):
                result = await scraper.run(params.limit)
        except TimeoutError:
            deadline_exceeded = True
            result = scraper.result
            deadline = params.timeout_for(config.timeout_seconds)
            result.errors.append(
                {"url": config.url, "error": f"source exceeded its {deadline:.0f}s deadline"}
            )
            # A source stopped by the clock has not been listed to the end, and
            # that is exactly what `truncated` means. Without this, a crawl that
            # ran out of time holding two thirds of a shop reported a complete
            # catalogue, and the loader retired the third it never reached — the
            # same withdrawal `truncated` exists to prevent, arriving through a
            # different door. It cost nothing so far only because the sources
            # that time out here (hobbyland, mestrebras) time out having
            # collected nothing at all, and an empty file is refused earlier.
            result.truncated = True
            LOGGER.warning("job.timeout", source=name, seconds=deadline)
        except asyncio.CancelledError:
            # Keep what was collected, then let the cancellation continue: the
            # TaskGroup is entitled to know this task did not finish.
            LOGGER.info("job.cancelled", source=name, records=len(scraper.result.records))
            raise
        except BrowserUnavailable:
            # Not this source's failure, so it must not be recorded as one. The
            # worker catches this and requeues the job for a worker that has a
            # browser; recording it below would instead spend an attempt and,
            # with no records collected, fail the source for an environment
            # fault it had no part in.
            LOGGER.warning("job.browser_unavailable", source=name)
            raise
        except Exception as error:
            LOGGER.exception("job.failed", source=name)
            result = scraper.result
            result.errors.append({"url": config.url, "error": str(error)})

        if params.refresh_mode == "price" and scrapers.adapter_capabilities(
            config.scraper
        ).price_refresh:
            for record in result.records:
                for field in (
                    "description", "firing", "firing_range", "surface", "effects", "colour",
                    "claims", "documents", "technical_attributes", "coats",
                    "application_methods", "category_path", "all_image_urls",
                ):
                    record.pop(field, None)
                record["collection_mode"] = "price"
        collection_seconds = time.monotonic() - started
        summary = summarise(name, config, result, method=scraper.method)
        summary["collection_seconds"] = round(collection_seconds, 6)
        if deadline_exceeded:
            # Said plainly rather than left to be inferred from the error text,
            # because `transient` reads it to refuse a retry: a source that
            # spent its whole hour and stopped would spend another on every
            # remaining attempt, and the clock is not a condition that clears.
            summary["deadline_exceeded"] = True
        metrics.records(name, summary["records"])

        # Written as soon as this source is done rather than at the end of the
        # run: a twenty-source crawl otherwise holds every finished source
        # hostage to the slowest one, and a run that dies in hour two leaves
        # nothing behind.
        if output is not None and not params.dry_run:
            write_started = time.monotonic()
            artifact = artifacts.write_source(output, name, result.records, params.allow_empty)
            summary["artifact_write_seconds"] = round(time.monotonic() - write_started, 6)
            # Recorded on the summary rather than only returned, because
            # `catalogue.jobs` stores all three and the manifest is what
            # carries them out of a process that has already exited.
            summary["write_status"] = artifact.status
            summary["artifact_path"] = str(artifact.path)
            summary["artifact_sha256"] = artifact.sha256 or None
            summary["artifact_size"] = artifact.size

        await progress.finished(name, summary)
        return SourceOutcome(source=name, records=list(result.records), summary=summary)


def partial_outcome(name: str, config: SourceConfig, progress: Progress) -> SourceOutcome:
    """What a cancelled or never-started source had collected when it stopped."""
    result = progress.results.get(name)
    records = list(getattr(result, "records", []))
    return SourceOutcome(
        source=name,
        records=records,
        summary=summarise(name, config, result, interrupted=True),
    )


class CrawlRunner:
    """Runs a set of sources concurrently, and can be stopped.

    Stopping has two granularities, because they are different requests:
    `stop()` ends the whole run, `cancel(source)` ends one source and leaves the
    rest going. The worker needs the second for `POST /v1/jobs/{id}/cancel`; the
    interactive display's stop button uses the first.
    """

    def __init__(
        self,
        sources: SourcesFile,
        session: CrawlSession | None,
        params: CrawlParams,
        progress: Progress,
        output: Path | None = None,
        *,
        scraper_factory: ScraperFactory | None = None,
    ) -> None:
        if session is None and scraper_factory is None:
            raise ValueError("default scraper factory requires a crawl session")
        self.sources = sources
        self.session = session
        self.params = params
        self.progress = progress
        self.output = output
        self.scraper_factory = scraper_factory
        self.tasks: dict[str, asyncio.Task[SourceOutcome]] = {}
        self.stopping = False
        self.interrupted = False
        self._gate = asyncio.Semaphore(max(1, params.sources))

    # -- control ----------------------------------------------------------

    def stop(self) -> None:
        """End the whole run, keeping whatever each source has collected."""
        self.stopping = True
        for task in self.tasks.values():
            task.cancel()

    def cancel(self, source: str) -> bool:
        """End one source. Returns whether there was one to end."""
        task = self.tasks.get(source)
        if task is None or task.done():
            return False
        task.cancel()
        return True

    def install_signal_handlers(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Route SIGTERM and SIGINT into the same graceful stop.

        SIGTERM is the one that mattered and was missing: `docker stop`,
        `systemctl stop` and a Kubernetes eviction all send it, and none of them
        raise `KeyboardInterrupt`. Without this, a container being replaced
        could be killed halfway through writing a source's NDJSON.
        """
        loop = loop or asyncio.get_running_loop()
        for name in ("SIGTERM", "SIGINT"):
            signum = getattr(signal, name, None)
            if signum is None:  # pragma: no cover - Windows
                continue
            try:
                loop.add_signal_handler(signum, self._on_signal, name)
            except (NotImplementedError, RuntimeError):  # pragma: no cover
                # No signal support (Windows, or a non-main thread). The
                # KeyboardInterrupt path in the entry point still applies.
                LOGGER.debug("runner.signal_handler_unavailable", signal=name)

    def _on_signal(self, name: str) -> None:
        if self.stopping:
            LOGGER.warning("runner.signal_repeated", signal=name)
            return
        LOGGER.info("runner.stopping", signal=name, reason="signal")
        self.stop()

    # -- running ----------------------------------------------------------

    async def run(self, selected: Sequence[str]) -> list[SourceOutcome]:
        """Crawl every named source, returning one outcome each, in order.

        A TaskGroup rather than `gather`: if the orchestration itself fails, the
        remaining sources are cancelled by the structure rather than by an
        `except` clause that has to remember to. Cancelling one *child* does not
        abort the group — which is what makes `cancel(source)` a per-source
        operation rather than a way to end the run by accident.
        """
        self.tasks = {}
        interrupted = False

        async def guarded(name: str) -> SourceOutcome:
            async with self._gate:
                if self.stopping:
                    # Queued behind the gate when the stop arrived; it never
                    # started, so it is a partial with nothing in it rather than
                    # a source that failed.
                    raise asyncio.CancelledError
                return await run_source(
                    name,
                    self.sources[name],
                    self.session,
                    self.params,
                    self.progress,
                    self.output,
                    scraper_factory=self.scraper_factory,
                )

        try:
            async with asyncio.TaskGroup() as group:
                for name in selected:
                    self.tasks[name] = group.create_task(guarded(name), name=f"source:{name}")
        except* asyncio.CancelledError:
            # Reached only when *this* task was cancelled from outside, since a
            # cancelled child is absorbed by the group. If we did not ask for
            # it, something above us is shutting down and swallowing it here
            # would hold the process open.
            if not self.stopping:
                raise
            interrupted = True
        except* Exception as group_error:
            # A source's own failure is caught inside `run_source`, so anything
            # arriving here is the orchestration failing. Report it and keep the
            # partial results rather than losing the whole run.
            interrupted = True
            for error in group_error.exceptions:
                LOGGER.error("runner.task_failed", error=str(error), exc_info=error)

        self.interrupted = interrupted or self.stopping
        return [self._outcome(name) for name in selected]

    def _outcome(self, name: str) -> SourceOutcome:
        task = self.tasks.get(name)
        if task is not None and task.done() and not task.cancelled() and task.exception() is None:
            return task.result()
        return partial_outcome(name, self.sources[name], self.progress)


async def crawl(
    sources: SourcesFile,
    selected: Sequence[str],
    session: CrawlSession | None,
    params: CrawlParams,
    progress: Progress,
    output: Path | None = None,
    on_runner: Callable[[CrawlRunner], None] | None = None,
    *,
    scraper_factory: ScraperFactory | None = None,
) -> tuple[list[SourceOutcome], bool]:
    """Run a crawl end to end. Returns the outcomes and whether it was cut short."""
    runner = CrawlRunner(
        sources,
        session,
        params,
        progress,
        output,
        scraper_factory=scraper_factory,
    )
    runner.install_signal_handlers()
    if on_runner is not None:
        on_runner(runner)
    started = time.monotonic()
    with tracing.span("run", **{"catalogue.sources": len(selected)}):
        outcomes = await runner.run(selected)
    metrics.run_duration(time.monotonic() - started)
    return outcomes, runner.interrupted
