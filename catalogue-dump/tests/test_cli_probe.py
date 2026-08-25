from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from mb_ceramics_catalogue.cli import probe
from mb_ceramics_catalogue.cli.probe import build_parser
from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.scrapers.base import ScrapeResult


def test_probe_pipeline_selection_is_explicit_and_legacy_by_default() -> None:
    parser = build_parser()

    assert parser.parse_args(["shop"]).pipeline == "legacy"
    assert (
        parser.parse_args(["shop", "--pipeline", "connector_canary"]).pipeline
        == "connector_canary"
    )


async def test_probe_connector_canary_uses_native_local_session_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sources = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "shopify",
            }
        }
    )
    traced: list[str] = []
    built: list[tuple[str, str, dict[str, Any], Any]] = []
    opened: list[tuple[Any, Path | None]] = []

    class Scraper:
        method = "api_json"

        async def run(self, limit: int | None = None) -> ScrapeResult:
            assert limit == 40
            return ScrapeResult(records=[{"name": "Glaze"}])

    class Session:
        def build(
            self,
            scraper: str,
            name: str,
            config: dict[str, Any],
            fetcher: Any,
        ) -> Scraper:
            built.append((scraper, name, config, fetcher))
            return Scraper()

    @asynccontextmanager
    async def open_native(params: Any, cache: Path | None) -> AsyncIterator[Session]:
        opened.append((params, cache))
        yield Session()

    @asynccontextmanager
    async def forbidden_session(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        del args, kwargs
        raise AssertionError("connector canary must not open the legacy session")
        yield  # pragma: no cover

    def forbidden_build(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise AssertionError("connector canary must not use the legacy scraper registry")

    monkeypatch.setattr(probe.SourcesFile, "load", lambda _path: sources)
    monkeypatch.setattr(probe.tracing, "configure", traced.append)
    monkeypatch.setattr(probe, "open_local_commerce_session", open_native)
    monkeypatch.setattr(probe, "open_session", forbidden_session)
    monkeypatch.setattr(probe.scrapers, "build", forbidden_build)
    options = build_parser().parse_args(
        [
            "shop",
            "--pipeline",
            "connector_canary",
            "--cache",
            str(tmp_path),
        ]
    )

    assert await probe.run(options) == 0

    assert traced == ["catalogue-probe"]
    assert len(opened) == 1
    assert opened[0][0].pipeline == "connector_canary"
    assert opened[0][1] == tmp_path
    assert len(built) == 1
    assert built[0][0:2] == ("shopify", "shop")
    assert built[0][3] is None
