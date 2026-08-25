"""The orchestrator: concurrency, deadlines, and the two kinds of stop.

These are the behaviours phase 1 added rather than moved, so the golden files
say nothing about them — a source that never hangs never exercises a timeout.
They are also the behaviours that only show up at three in the morning, which is
the argument for testing them rather than trying them.

The scrapers are stubbed. What is under test is `CrawlRunner`, and a real
scraper would only add a network to something that is about task control.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from mb_ceramics_catalogue.config.settings import CrawlParams
from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.crawl.progress import Progress, ProgressSink
from mb_ceramics_catalogue.crawl.runner import CrawlRunner, barren, transient
from mb_ceramics_catalogue.scrapers.base import ScrapeResult


def make_sources(*names: str) -> SourcesFile:
    return SourcesFile.model_validate(
        {
            name: {"label": name, "url": f"https://{name}.test/", "scraper": "shopify"}
            for name in names
        }
    )


class StubScraper:
    """A scraper whose only behaviour is how long it takes and how it ends."""

    method = "api_json"

    def __init__(self, name: str, config: dict[str, Any], fetcher: Any) -> None:
        self.name = name
        self.config = config
        self.result = ScrapeResult()
        self.started = asyncio.Event()

    async def run(self, limit: int | None = None) -> ScrapeResult:
        self.started.set()
        behaviour = BEHAVIOUR.get(self.name, ("records", 2))
        kind, value = behaviour
        if kind == "records":
            for index in range(value):
                self.result.records.append(
                    {"external_id": f"{self.name}:{index}", "name": f"row {index}"}
                )
            self.result.requests = value
            return self.result
        if kind == "hang":
            await asyncio.sleep(value)
            return self.result
        if kind == "raise":
            raise RuntimeError(value)
        raise AssertionError(kind)


#: name -> (kind, value). Set per test; read inside the stub.
BEHAVIOUR: dict[str, tuple[str, Any]] = {}


class StubSession:
    """Stands in for `CrawlSession`. The runner only ever reads `.fetcher`."""

    fetcher = object()


async def until(predicate, timeout: float = 5.0) -> None:
    """Wait for a condition, and fail rather than hang if it never holds.

    A bare `while not predicate(): await sleep(...)` in a test turns any bug in
    the code under test into a suite that never finishes, which is a much worse
    failure than a red assertion.
    """
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


@pytest.fixture(autouse=True)
def stub_scrapers(monkeypatch):
    BEHAVIOUR.clear()
    monkeypatch.setattr(
        "mb_ceramics_catalogue.crawl.runner.scrapers.build",
        lambda scraper, name, config, fetcher: StubScraper(name, config, fetcher),
    )
    yield
    BEHAVIOUR.clear()


class RecordingSink:
    """Captures the sink protocol calls, so ordering can be asserted."""

    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    async def started(self, source: str, result: Any, scraper: str = "", method: str = "") -> None:
        self.events.append(("started", source))

    async def progress(self, source: str, result: Any) -> None:
        self.events.append(("progress", source))

    async def finished(self, source: str, summary: dict[str, Any]) -> None:
        self.events.append(("finished", source))

    async def close(self) -> None:
        self.events.append(("close", ""))


async def run_with(
    sources: SourcesFile, selected: list[str], params: CrawlParams, **kwargs: Any
) -> tuple[list, CrawlRunner]:
    progress = Progress(len(selected))
    runner = CrawlRunner(sources, StubSession(), params, progress, None)  # type: ignore[arg-type]
    for key, value in kwargs.items():
        setattr(runner, key, value)
    outcomes = await runner.run(selected)
    return outcomes, runner


class TestOrdinaryRun:
    async def test_every_source_produces_an_outcome_in_order(self):
        sources = make_sources("alpha", "beta", "gamma")
        outcomes, runner = await run_with(sources, ["alpha", "beta", "gamma"], CrawlParams())
        assert [o.source for o in outcomes] == ["alpha", "beta", "gamma"]
        assert all(o.summary["records"] == 2 for o in outcomes)
        assert not runner.interrupted

    async def test_a_source_that_raises_is_recorded_and_does_not_stop_the_run(self):
        """One supplier's site being broken is not the run failing."""
        BEHAVIOUR["beta"] = ("raise", "the shop returned nonsense")
        sources = make_sources("alpha", "beta", "gamma")
        outcomes, runner = await run_with(sources, ["alpha", "beta", "gamma"], CrawlParams())

        by_source = {o.source: o for o in outcomes}
        assert by_source["beta"].summary["error_count"] == 1
        assert "nonsense" in by_source["beta"].summary["errors"][0]["error"]
        assert by_source["alpha"].summary["records"] == 2
        assert by_source["gamma"].summary["records"] == 2
        assert not runner.interrupted

    async def test_the_concurrency_gate_is_respected(self):
        """`--sources 2` means two at a time, not two eventually."""
        names = [f"s{index}" for index in range(6)]
        for name in names:
            BEHAVIOUR[name] = ("hang", 0.05)
        sources = make_sources(*names)
        progress = Progress(len(names))
        runner = CrawlRunner(sources, StubSession(), CrawlParams(sources=2), progress, None)

        peak = 0

        async def watch() -> None:
            nonlocal peak
            while True:
                peak = max(peak, len(progress.running))
                await asyncio.sleep(0.005)

        watcher = asyncio.create_task(watch())
        await runner.run(names)
        watcher.cancel()
        assert peak <= 2, f"{peak} sources ran at once with sources=2"


class TestScraperFactory:
    async def test_default_factory_remains_scrapers_build(self, monkeypatch):
        calls: list[tuple[str, str, dict[str, Any], Any]] = []

        def build(scraper: str, name: str, config: dict[str, Any], fetcher: Any) -> Any:
            calls.append((scraper, name, config, fetcher))
            return StubScraper(name, config, fetcher)

        monkeypatch.setattr(
            "mb_ceramics_catalogue.crawl.runner.scrapers.build",
            build,
        )
        sources = make_sources("alpha")
        outcomes, _ = await run_with(sources, ["alpha"], CrawlParams())

        assert outcomes[0].summary["records"] == 2
        assert len(calls) == 1
        scraper, name, config, fetcher = calls[0]
        assert scraper == "shopify"
        assert name == "alpha"
        assert config["url"] == "https://alpha.test/"
        assert fetcher is StubSession.fetcher

    async def test_injected_factory_replaces_only_scraper_construction(self, monkeypatch):
        def default_factory(*_: Any, **__: Any) -> Any:
            raise AssertionError("default scraper factory was called")

        monkeypatch.setattr(
            "mb_ceramics_catalogue.crawl.runner.scrapers.build",
            default_factory,
        )
        calls: list[tuple[str, str, dict[str, Any], Any]] = []

        def injected(scraper: str, name: str, config: dict[str, Any], fetcher: Any) -> Any:
            calls.append((scraper, name, config, fetcher))
            return StubScraper(name, config, fetcher)

        BEHAVIOUR["alpha"] = ("records", 3)
        sources = make_sources("alpha")
        progress = Progress(1)
        runner = CrawlRunner(
            sources,
            None,
            CrawlParams(),
            progress,
            scraper_factory=injected,
        )

        outcomes = await runner.run(["alpha"])

        assert outcomes[0].summary["records"] == 3
        assert outcomes[0].summary["extraction_method"] == "api_json"
        assert calls == [
            (
                "shopify",
                "alpha",
                sources["alpha"].as_scraper_config(),
                None,
            )
        ]
        assert not runner.interrupted

    def test_default_factory_refuses_an_absent_session(self):
        sources = make_sources("alpha")

        with pytest.raises(
            ValueError,
            match="default scraper factory requires a crawl session",
        ):
            CrawlRunner(sources, None, CrawlParams(), Progress(1))


class TestDeadline:
    async def test_a_hanging_source_is_given_up_on(self):
        """There was no deadline at all before this.

        A source that never returns held its slot until someone noticed, which
        on a 03:00 schedule means the morning.
        """
        BEHAVIOUR["slow"] = ("hang", 30)
        sources = make_sources("slow")
        outcomes, _ = await run_with(
            sources, ["slow"], CrawlParams(source_timeout_seconds=0.05)
        )
        summary = outcomes[0].summary
        assert summary["error_count"] == 1
        assert "deadline" in summary["errors"][0]["error"]

    async def test_a_source_stopped_by_the_clock_is_truncated(self):
        """It was not listed to the end, which is what the flag means.

        `plan_load` and the worker both read `truncated` as their permission to
        retire what a dump does not contain. A crawl that ran out of time
        holding two thirds of a shop and reported a complete catalogue would
        have the last third withdrawn — the same withdrawal the flag exists to
        prevent, arriving through the deadline instead of through a cap.
        """
        BEHAVIOUR["slow"] = ("hang", 30)
        sources = make_sources("slow")
        outcomes, _ = await run_with(
            sources, ["slow"], CrawlParams(source_timeout_seconds=0.05)
        )
        assert outcomes[0].summary["truncated"] is True

    async def test_a_source_stopped_by_the_clock_says_so_in_the_summary(self):
        """`transient` reads this to refuse the source another attempt.

        The clock is not a condition that clears, so retrying a source that
        spent its whole deadline spends the next one too.
        """
        BEHAVIOUR["slow"] = ("hang", 30)
        sources = make_sources("slow")
        outcomes, _ = await run_with(
            sources, ["slow"], CrawlParams(source_timeout_seconds=0.05)
        )
        assert outcomes[0].summary["deadline_exceeded"] is True
        assert not transient(outcomes[0].summary)

    async def test_a_source_that_finished_carries_no_deadline_flag(self):
        BEHAVIOUR["quick"] = ("records", 3)
        sources = make_sources("quick")
        outcomes, _ = await run_with(sources, ["quick"], CrawlParams())
        assert "deadline_exceeded" not in outcomes[0].summary

    async def test_one_source_timing_out_does_not_affect_the_others(self):
        BEHAVIOUR["slow"] = ("hang", 30)
        sources = make_sources("slow", "quick")
        outcomes, _ = await run_with(
            sources, ["slow", "quick"], CrawlParams(source_timeout_seconds=0.05, sources=2)
        )
        by_source = {o.source: o for o in outcomes}
        assert by_source["quick"].summary["records"] == 2
        assert by_source["slow"].summary["error_count"] == 1

    async def test_a_source_may_tighten_the_deadline_but_not_loosen_it(self):
        params = CrawlParams(source_timeout_seconds=100)
        assert params.timeout_for(10) == 10
        assert params.timeout_for(1000) == 100
        assert params.timeout_for(None) == 100


class TestStopping:
    async def test_cancelling_one_source_leaves_the_others_running(self):
        """The §5.6 requirement: `POST /v1/jobs/{id}/cancel` is per source."""
        for name in ("alpha", "beta", "gamma"):
            BEHAVIOUR[name] = ("hang", 0.4)
        sources = make_sources("alpha", "beta", "gamma")
        progress = Progress(3)
        runner = CrawlRunner(sources, StubSession(), CrawlParams(sources=3), progress, None)

        async def cancel_beta() -> None:
            await until(lambda: "beta" in progress.running)
            assert runner.cancel("beta")

        canceller = asyncio.create_task(cancel_beta())
        outcomes = await runner.run(["alpha", "beta", "gamma"])
        await canceller

        by_source = {o.source: o for o in outcomes}
        assert by_source["beta"].interrupted
        assert not by_source["alpha"].interrupted
        assert not by_source["gamma"].interrupted

    async def test_cancelling_an_unknown_or_finished_source_reports_it(self):
        sources = make_sources("alpha")
        progress = Progress(1)
        runner = CrawlRunner(sources, StubSession(), CrawlParams(), progress, None)
        assert runner.cancel("alpha") is False  # never started
        await runner.run(["alpha"])
        assert runner.cancel("alpha") is False  # already done

    async def test_stopping_the_run_keeps_what_each_source_collected(self):
        for name in ("alpha", "beta"):
            BEHAVIOUR[name] = ("hang", 5)
        sources = make_sources("alpha", "beta")
        progress = Progress(2)
        runner = CrawlRunner(sources, StubSession(), CrawlParams(sources=2), progress, None)

        async def stop_soon() -> None:
            await until(lambda: len(progress.running) >= 2)
            runner.stop()

        stopper = asyncio.create_task(stop_soon())
        outcomes = await runner.run(["alpha", "beta"])
        await stopper

        assert runner.interrupted
        assert all(o.interrupted for o in outcomes)
        # An interrupted summary must not claim an extraction method it never
        # reached, and must say it was interrupted so `plan_load` refuses to
        # retire against it.
        for outcome in outcomes:
            assert outcome.summary["interrupted"] is True
            assert "extraction_method" not in outcome.summary

    async def test_a_source_still_queued_when_the_stop_arrives_is_a_partial(self):
        """It never started, so it is an empty partial rather than a failure."""
        BEHAVIOUR["alpha"] = ("hang", 5)
        sources = make_sources("alpha", "beta")
        progress = Progress(2)
        runner = CrawlRunner(sources, StubSession(), CrawlParams(sources=1), progress, None)

        async def stop_soon() -> None:
            await until(lambda: "alpha" in progress.running)
            runner.stop()

        stopper = asyncio.create_task(stop_soon())
        outcomes = await runner.run(["alpha", "beta"])
        await stopper

        beta = next(o for o in outcomes if o.source == "beta")
        assert beta.interrupted
        assert beta.records == []
        assert beta.summary["error_count"] == 0


class TestProgress:
    async def test_sinks_see_started_then_finished_for_every_source(self):
        sink = RecordingSink()
        sources = make_sources("alpha", "beta")
        progress = Progress(2, [sink])
        runner = CrawlRunner(sources, StubSession(), CrawlParams(sources=1), progress, None)
        async with progress:
            await runner.run(["alpha", "beta"])

        pairs = [event for event in sink.events if event[0] in ("started", "finished")]
        assert pairs == [
            ("started", "alpha"), ("finished", "alpha"),
            ("started", "beta"), ("finished", "beta"),
        ]
        assert sink.events[-1] == ("close", "")

    async def test_a_failing_sink_never_stops_the_crawl(self):
        """A display is never worth a run."""

        class BrokenSink:
            async def started(self, *_: Any, **__: Any) -> None:
                raise RuntimeError("the terminal went away")

            async def progress(self, *_: Any) -> None:
                raise RuntimeError("still gone")

            async def finished(self, *_: Any) -> None:
                raise RuntimeError("gone")

            async def close(self) -> None:
                raise RuntimeError("gone")

        sources = make_sources("alpha")
        progress = Progress(1, [BrokenSink()])
        runner = CrawlRunner(sources, StubSession(), CrawlParams(), progress, None)
        async with progress:
            outcomes = await runner.run(["alpha"])
        assert outcomes[0].summary["records"] == 2

    def test_the_protocol_is_satisfied_by_the_shipped_sinks(self):
        from mb_ceramics_catalogue.crawl.progress import BarSink, LogSink

        assert isinstance(LogSink(), ProgressSink)
        assert isinstance(BarSink(1), ProgressSink)


class TestSummary:
    async def test_a_completed_summary_carries_what_the_loader_reads(self):
        sources = make_sources("alpha")
        outcomes, _ = await run_with(sources, ["alpha"], CrawlParams())
        summary = outcomes[0].summary
        for key in ("source", "label", "scraper", "records", "truncated", "robots_ignored",
                    "field_coverage", "extraction_method", "scope"):
            assert key in summary, key
        # A completed source must not be marked interrupted: `plan_load` reads
        # that through the manifest to decide whether it may retire against it.
        assert "interrupted" not in summary


class TestBarren:
    """A source that listed products and read none of them has not succeeded.

    Ten sources were in that state on 2026-08-12 and every one of their jobs was
    green. countrylove spent eighty-four minutes finding 18,883 product URLs,
    extracted zero, raised nothing, and reported success.
    """

    def test_listing_products_and_extracting_none_is_a_failure(self):
        assert barren({"records": 0, "discovered": 18883, "scraper": "pagecrawl"})

    def test_a_source_that_found_nothing_to_read_is_not_this(self):
        """Discovery finding nothing is its own problem, and a different one."""
        assert barren({"records": 0, "discovered": 0, "scraper": "pagecrawl"}) is None

    def test_a_source_that_extracted_something_is_fine(self):
        assert barren({"records": 1, "discovered": 2054, "scraper": "pagecrawl"}) is None

    def test_rows_dropped_by_the_scope_are_reported_as_a_discovery_problem(self):
        """Blaming the parser sent an operator to read two working parsers.

        artequipment and mayco-glasuren extracted every page they listed. The
        rows were art supplies and giftware, dropped by the materials scope, so
        what is broken is which pages the crawl lists.
        """
        message = barren(
            {"records": 0, "discovered": 33, "filtered": 33, "scraper": "pagecrawl"}
        )
        assert message is not None
        assert "extracted 33" in message
        assert "materials scope" in message
        assert "recognised nothing" not in message

    def test_an_extractor_that_read_nothing_still_says_so(self):
        message = barren({"records": 0, "discovered": 33, "filtered": 0, "scraper": "pagecrawl"})
        assert message is not None
        assert "recognised nothing" in message

    def test_invalid_rows_are_reported_as_extraction_not_discovery_failures(self):
        message = barren(
            {"records": 0, "discovered": 33, "invalid": 33, "scraper": "pagecrawl"}
        )
        assert message is not None
        assert "lacked a usable identity or price" in message
        assert "listing the wrong pages" not in message

    def test_mixed_rejections_report_both_causes(self):
        message = barren({
            "records": 0, "discovered": 33, "filtered": 20, "invalid": 13,
            "scraper": "pagecrawl",
        })
        assert message is not None
        assert "20 fell outside" in message
        assert "13 lacked" in message

    def test_an_interrupted_source_is_judged_on_nothing(self):
        """It was stopped, so what it did not reach says nothing about it."""
        summary = {"records": 0, "discovered": 900, "scraper": "shopify", "interrupted": True}
        assert barren(summary) is None


class TestTransient:
    """Which empty-handed sources deserve another of their three attempts.

    Every failure in the 2026-08-19 run was a host refusing the crawl — two
    Shopify stores answering 429 to the first request, one WooCommerce site
    reporting its database was down — and each was recorded terminally on
    attempt 1 with two attempts unspent.
    """

    def test_a_rate_limited_source_is_worth_another_attempt(self):
        assert transient({
            "records": 0, "discovered": 0, "error_count": 1,
            "outcome_counts": {"429": 9},
        })

    def test_a_source_whose_host_is_erroring_is_worth_another_attempt(self):
        assert transient({
            "records": 0, "discovered": 0, "error_count": 2,
            "outcome_counts": {"2xx": 1, "5xx": 8},
        })

    def test_timeouts_and_dropped_connections_count_the_same_way(self):
        assert transient({
            "records": 0, "error_count": 1, "outcome_counts": {"timeout": 3},
        })
        assert transient({
            "records": 0, "error_count": 1, "outcome_counts": {"transport_error": 3},
        })

    def test_a_scraper_that_recognised_nothing_is_not(self):
        """The parser will recognise nothing again in five minutes.

        artequipment listed 33 products over clean 2xx responses and kept none.
        Retrying that three times triples the crawl and changes no outcome.
        """
        assert not transient({
            "records": 0, "discovered": 33, "error_count": 0,
            "outcome_counts": {"2xx": 34, "4xx": 1},
        })

    def test_a_source_that_ran_out_of_clock_is_not(self):
        """It would not fail faster, it would fail an hour later, three times."""
        assert not transient({
            "records": 0, "error_count": 1, "deadline_exceeded": True,
            "outcome_counts": {"2xx": 40, "timeout": 1},
        })

    def test_a_source_that_collected_records_is_not(self):
        assert not transient({
            "records": 12, "error_count": 1, "outcome_counts": {"429": 4},
        })

    def test_an_interrupted_source_is_judged_on_nothing(self):
        assert not transient({
            "records": 0, "error_count": 1, "interrupted": True,
            "outcome_counts": {"429": 4},
        })

    def test_a_forbidden_source_is_not_retried_as_throttling(self):
        """403 is a decision about who we are, not about our pace."""
        assert not transient({
            "records": 0, "error_count": 1, "outcome_counts": {"403": 6},
        })
