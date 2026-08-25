from __future__ import annotations

import gc
import json
import weakref
from datetime import UTC, datetime
from pathlib import Path

import pytest
from mb_commerce_scraper.connectors import ConnectorContext
from mb_commerce_scraper.testing import FakeTransport

from mb_ceramics_catalogue import scrapers
from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.connectors import (
    CollectionRequest,
    ConnectorCheckpoint,
    DiagnosticCode,
    PageCommerceConnector,
    PageCrawlOptions,
    RefreshMode,
    SnapshotField,
)
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import source_definition
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import application_connector_registry
from mb_ceramics_catalogue.ops.connector_adapters import RUNTIME_ADAPTERS, runtime_plan

NOW = datetime(2026, 8, 15, 18, 0, tzinfo=UTC)
SOURCES = Path(__file__).parents[1] / "sources.json"


def test_every_checked_in_legacy_scraper_has_runtime_canary_and_rollback_key() -> None:
    """Make completion of a migration explicit for every production scraper family."""
    sources = SourcesFile.load(SOURCES)
    legacy_keys = {source.scraper for _, source in sources.items()}
    registry = application_connector_registry()

    assert legacy_keys <= RUNTIME_ADAPTERS.keys()
    assert all(
        runtime_plan(source).library_canary is not None
        for _, source in sources.items()
    )
    for key in sorted(legacy_keys):
        examples = [source for _, source in sources.items() if source.scraper == key]
        assert examples
        plan = runtime_plan(examples[0])
        capabilities = scrapers.adapter_capabilities(key)
        assert capabilities.canary_adapter is not None
        assert key in scrapers.REGISTRY, f"{key} lost its rollback selector"
        assert capabilities.canary_adapter != key
        assert capabilities.canary_adapter in scrapers.REGISTRY
        library_alias = scrapers.library_canary_alias(key)
        assert library_alias == f"library_{capabilities.canary_adapter}"
        assert scrapers.LIBRARY_CANARY_SCRAPERS[library_alias] == key
        assert scrapers.REGISTRY[library_alias] == (
            ".library_connector:LibraryConnectorScraper"
        )
        assert plan.connector in registry.names()
        assert registry.connector_version(plan.connector)

    for source_id, source in sources.items():
        plan = runtime_plan(source)
        definition = source_definition(source_id, source, connector_plan=plan)
        connector = registry.build(
            definition.connector,
            transport=FakeTransport(),
            options=definition.connector_options,
            context=ConnectorContext(),
        )
        assert connector.name == definition.connector == plan.connector


class StreamingTransport:
    def __init__(self, count: int) -> None:
        self.count = count
        self.sitemap_url = "https://stream.test/products.xml"

    async def advertised_sitemaps(self, base_url: str) -> tuple[str, ...]:
        return ()

    async def document(self, url: str, *, rendered=False, accept=None) -> str:
        if url == self.sitemap_url:
            return "<urlset>" + "".join(
                f"<url><loc>https://stream.test/product/{index}</loc></url>"
                for index in range(self.count)
            ) + "</urlset>"
        identifier = url.rsplit("/", 1)[-1]
        product = {
            "@context": "https://schema.org",
            "@type": "Product",
            "name": f"Glaze {identifier}",
            "sku": f"G-{identifier}",
            "offers": {
                "price": "12.50",
                "priceCurrency": "EUR",
                "availability": "https://schema.org/InStock",
            },
        }
        return f'<script type="application/ld+json">{json.dumps(product)}</script>'


def streaming_request(*, limit: int | None = None) -> CollectionRequest:
    return CollectionRequest(
        source_id="stream",
        base_url="https://stream.test/",
        refresh_mode=RefreshMode.FULL,
        requested_fields=frozenset(SnapshotField),
        result_limit=limit,
    )


def streaming_connector(count: int) -> PageCommerceConnector:
    transport = StreamingTransport(count)
    return PageCommerceConnector(
        transport,
        PageCrawlOptions(
            sitemaps=(transport.sitemap_url,),
            use_advertised_sitemaps=False,
            product_pattern=r"^/product/",
            page_limit=count,
            render=False,
        ),
        clock=lambda: NOW,
    )


async def peak_retained_entities(total: int) -> int:
    references: list[weakref.ReferenceType[object]] = []
    peak = 0
    async for page in streaming_connector(total).collect(streaming_request()):
        assert len(page.items) == 1
        references.append(weakref.ref(page.items[0]))
        if len(references) % 50 == 0:
            gc.collect()
        peak = max(peak, sum(reference() is not None for reference in references))
    del page
    gc.collect()
    assert not any(reference() is not None for reference in references)
    return peak


@pytest.mark.asyncio
async def test_peak_retained_entities_does_not_grow_with_total_page_count() -> None:
    small_peak = await peak_retained_entities(100)
    large_peak = await peak_retained_entities(2_000)

    assert small_peak <= 2
    assert large_peak <= small_peak + 1


@pytest.mark.asyncio
async def test_result_limit_and_checkpoint_page_invariants_hold_under_streaming() -> None:
    connector = streaming_connector(25)
    limited = [page async for page in connector.collect(streaming_request(limit=7))]

    assert sum(len(page.items) for page in limited) == 7
    assert limited[-1].terminal and not limited[-1].enumeration_intact
    assert limited[-1].resume_after == {"partition": "sitemap", "index": 7, "sequence": 7}
    assert limited[-1].diagnostics[0].code == DiagnosticCode.RESULT_LIMIT_REACHED
    assert all(page.discovered >= len(page.items) for page in limited)
    assert [page.sequence for page in limited] == list(range(7))
    assert len({page.page_id for page in limited}) == len(limited)
    assert all(page.resume_after is not None for page in limited[:-1])

    complete = [page async for page in streaming_connector(25).collect(streaming_request())]
    checkpoint = ConnectorCheckpoint(
        connector="pagecommerce",
        connector_version="1",
        source_id="stream",
        lineage="scale-test",
        resume_after=complete[9].resume_after,
    )
    resumed = [
        page
        async for page in streaming_connector(25).collect(streaming_request(), checkpoint)
    ]

    assert resumed[0].model_dump_json() == complete[10].model_dump_json()
    assert resumed[-1].terminal and resumed[-1].resume_after is None
