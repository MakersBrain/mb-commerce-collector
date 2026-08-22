from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from mb_ceramics_catalogue.cli import dump
from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.crawl.runner import SourceOutcome


def _sources() -> SourcesFile:
    return SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "shopify",
            }
        }
    )


def _outcome() -> SourceOutcome:
    return SourceOutcome(
        source="shop",
        records=[],
        summary={
            "scraper": "shopify",
            "extraction_method": "api_json",
            "records": 0,
            "requests": 0,
            "rendered_pages": 0,
            "error_count": 0,
            "truncated": False,
            "robots_ignored": False,
        },
    )


class _Record:
    def __init__(self) -> None:
        self.sinks: list[Any] = []
        self.finished: list[list[SourceOutcome]] = []

    async def finish(self, outcomes: list[SourceOutcome]) -> None:
        self.finished.append(outcomes)


@asynccontextmanager
async def _record_run(*args: Any, **kwargs: Any) -> AsyncIterator[_Record]:
    del args, kwargs
    yield _Record()


def _options(tmp_path: Path, pipeline: str) -> Any:
    return dump.build_parser().parse_args(
        [
            "--source",
            "shop",
            "--out",
            str(tmp_path / "out"),
            "--pipeline",
            pipeline,
            "--dry-run",
            "--no-progress",
        ]
    )


async def test_dump_connector_canary_passes_native_factory_without_legacy_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[dict[str, Any]] = []
    traced: list[str] = []

    class LocalSession:
        cache_enabled = False

        def build(self, *args: Any, **kwargs: Any) -> Any:
            del args, kwargs
            raise AssertionError("the fake crawl only inspects the native factory")

        def cache_summary(self) -> str:
            return ""

    local = LocalSession()

    @asynccontextmanager
    async def open_native(*args: Any, **kwargs: Any) -> AsyncIterator[LocalSession]:
        del args, kwargs
        yield local

    @asynccontextmanager
    async def forbidden_legacy(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        del args, kwargs
        raise AssertionError("connector canary must not open the legacy session")
        yield  # pragma: no cover

    async def fake_crawl(
        sources: SourcesFile,
        selected: list[str],
        session: Any,
        params: Any,
        progress: Any,
        output: Path | None,
        on_runner: Any = None,
        *,
        scraper_factory: Any = None,
    ) -> tuple[list[SourceOutcome], bool]:
        del sources, progress, on_runner
        calls.append(
            {
                "selected": selected,
                "session": session,
                "pipeline": params.pipeline,
                "output": output,
                "factory": scraper_factory,
            }
        )
        return [_outcome()], False

    monkeypatch.setattr(dump, "Settings", lambda: SimpleNamespace(log_json=False))
    monkeypatch.setattr(dump.SourcesFile, "load", lambda _path: _sources())
    monkeypatch.setattr(dump.tracing, "configure", traced.append)
    monkeypatch.setattr(dump.progress_module, "terminal_sinks", lambda *args, **kwargs: [])
    monkeypatch.setattr(dump.recording, "record_run", _record_run)
    monkeypatch.setattr(dump, "open_local_commerce_session", open_native)
    monkeypatch.setattr(dump, "open_session", forbidden_legacy)
    monkeypatch.setattr(dump, "crawl", fake_crawl)

    assert await dump.run(_options(tmp_path, "connector_canary")) == 0

    assert traced == ["catalogue-dump"]
    assert len(calls) == 1
    assert calls[0]["selected"] == ["shop"]
    assert calls[0]["session"] is None
    assert calls[0]["pipeline"] == "connector_canary"
    assert calls[0]["output"] is None
    assert calls[0]["factory"].__self__ is local


async def test_dump_legacy_keeps_established_session_route(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[Any, Any]] = []
    legacy = SimpleNamespace(
        cache=SimpleNamespace(enabled=False),
        cache_summary=lambda: "",
    )

    @asynccontextmanager
    async def open_legacy(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        del args, kwargs
        yield legacy

    @asynccontextmanager
    async def forbidden_native(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        del args, kwargs
        raise AssertionError("legacy mode must not open the native local session")
        yield  # pragma: no cover

    async def fake_crawl(*args: Any, **kwargs: Any) -> tuple[list[SourceOutcome], bool]:
        calls.append((args[2], kwargs.get("scraper_factory")))
        return [_outcome()], False

    monkeypatch.setattr(dump, "Settings", lambda: SimpleNamespace(log_json=False))
    monkeypatch.setattr(dump.SourcesFile, "load", lambda _path: _sources())
    monkeypatch.setattr(dump.progress_module, "terminal_sinks", lambda *args, **kwargs: [])
    monkeypatch.setattr(dump.recording, "record_run", _record_run)
    monkeypatch.setattr(dump, "open_local_commerce_session", forbidden_native)
    monkeypatch.setattr(dump, "open_session", open_legacy)
    monkeypatch.setattr(dump, "crawl", fake_crawl)

    assert await dump.run(_options(tmp_path, "legacy")) == 0
    assert calls == [(legacy, None)]
