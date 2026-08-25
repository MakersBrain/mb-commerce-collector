"""Synthetic end-to-end parity at the first library migration boundary.

The response script deliberately enters through the same Fetcher-shaped seam
on both paths. This is not a substitute for the production response archive;
it protects the registry, transport bridge, neutral model, and production
ceramics projector while that larger recorded replay gate remains open.
"""

from __future__ import annotations

import gzip
import json
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlsplit

import httpx
import pytest
from mb_commerce_scraper import (
    CollectionRequest as LibraryCollectionRequest,
)
from mb_commerce_scraper import (
    ConnectorRegistry as LibraryConnectorRegistry,
)
from mb_commerce_scraper import (
    RefreshMode as LibraryRefreshMode,
)
from mb_commerce_scraper import (
    SnapshotField as LibrarySnapshotField,
)

from mb_ceramics_catalogue import scrapers
from mb_ceramics_catalogue.config.sources import SourceConfig
from mb_ceramics_catalogue.connectors import CollectionRequest, RefreshMode
from mb_ceramics_catalogue.datasets import built_in_registry
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import (
    ceramics_projection_configuration,
    source_definition,
)
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    build_library_pipeline_connector,
)
from mb_ceramics_catalogue.pipeline.outputs import LocalArtifactStore
from mb_ceramics_catalogue.pipeline.runner import ConnectorPipeline

NOW = datetime(2026, 8, 22, 12, tzinfo=UTC)


class Limiter:
    def join_group(self, url: str, group: str) -> None:
        del url, group

    def set_delay(self, url: str, delay: float) -> None:
        del url, delay


class ScriptedFetcher:
    """Independent immutable playback for one runtime path."""

    proxy_lease = None

    def __init__(self, script: dict[str, Any]) -> None:
        self._script = deepcopy(script)
        self.calls: list[tuple[str, str]] = []
        self.limiter = Limiter()
        self.stats = SimpleNamespace(proxy_requests=0, direct_requests=0)

    async def response(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        method: str = "GET",
        json_body: Any = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        del json_body, headers, kwargs
        request = httpx.Request(method, url, params=params)
        target = str(request.url)
        self.calls.append((method, target))
        self.stats.direct_requests += 1
        path = urlsplit(url).path
        if path not in self._script:
            raise AssertionError(f"unrecorded synthetic request: {target}")
        return httpx.Response(
            200,
            json=deepcopy(self._script[path]),
            request=request,
        )

    async def json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        return (await self.response(url, params=params, headers=headers)).json()

    async def render(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None
    ) -> str:
        del url, wait_ms, wait_for
        raise AssertionError("Shopify parity must not use a browser")

    async def request_json_in_browser(
        self,
        page_url: str,
        endpoint: str,
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        body: Any = None,
    ) -> Any:
        del page_url, endpoint, method, headers, body
        raise AssertionError("Shopify parity must not use browser JSON")

    async def rotate_client(self) -> None:
        raise AssertionError("a one-page Shopify replay must not rotate")

    async def may_fetch(
        self, url: str, ignore_robots: bool = False, obey_robots: bool = False
    ) -> bool:
        del url, ignore_robots, obey_robots
        return True


class CapturingCommitter:
    def __init__(self) -> None:
        self.pages: list[tuple[Any, Any, Any]] = []

    async def commit_page(self, page: Any, batches: Any, outcomes: Any) -> None:
        self.pages.append((page, batches, outcomes))


def product() -> dict[str, Any]:
    return {
        "id": 42,
        "handle": "stoneware-glaze",
        "title": "Stoneware Glaze",
        "body_html": '<p>Durable.</p><a href="/docs/sds.pdf">SDS</a>',
        "vendor": "Test Ceramics",
        "product_type": "Glazes",
        "tags": ["ceramic", "transparent"],
        "updated_at": "2026-08-20T10:00:00Z",
        "options": [{"name": "Size"}, {"name": "Colour"}],
        "images": [
            {"id": 7, "src": "https://cdn.test/glaze.jpg", "alt": "Glaze"}
        ],
        "variants": [
            {
                "id": 420,
                "title": "500 ml / Blue",
                "sku": "GL-500",
                "barcode": "1234567890123",
                "price": "12.50",
                "compare_at_price": "15.00",
                "available": True,
                "inventory_quantity": 4,
                "inventory_management": "shopify",
                "inventory_policy": "deny",
                "option1": "500 ml",
                "option2": "Blue",
                "grams": 700,
                "featured_image": {"src": "https://cdn.test/variant.jpg"},
            }
        ],
    }


@pytest.mark.asyncio
async def test_registry_bridge_and_projection_match_legacy_shopify(tmp_path) -> None:
    script = {
        "/meta.json": {"currency": "EUR"},
        "/products.json": {"products": [product()]},
    }
    config = SourceConfig.model_validate(
        {
            "label": "Shop",
            "url": "https://shop.test/path",
            "scraper": "shopify",
            "scope": "all",
            "vat_status": "inclusive",
        }
    )

    legacy_fetcher = ScriptedFetcher(script)
    legacy = scrapers.build(
        "shopify", "shop", config.as_scraper_config(), legacy_fetcher
    )
    legacy_result = await legacy.scrape()

    datasets = built_in_registry()
    dataset_name = "ceramics.catalogue_item.v2"
    requested_fields = datasets.collection_requirements((dataset_name,))[0]
    projection_request = CollectionRequest(
        source_id="shop",
        base_url=config.url,
        refresh_mode=RefreshMode.FULL,
        requested_fields=requested_fields,
    )
    library_request = LibraryCollectionRequest(
        source_id="shop",
        base_url=config.url,
        refresh_mode=LibraryRefreshMode.FULL,
        requested_fields=frozenset(
            LibrarySnapshotField(field.value) for field in requested_fields
        ),
    )
    library_fetcher = ScriptedFetcher(script)
    source = source_definition("shop", config)
    connector = build_library_pipeline_connector(
        registry=LibraryConnectorRegistry.with_builtins(),
        source=source,
        request=library_request,
        checkpoint=None,
        fetcher=library_fetcher,
        cancelled=lambda: False,
        clock=lambda: NOW,
    )
    committer = CapturingCommitter()
    pipeline_result = await ConnectorPipeline(
        datasets,
        LocalArtifactStore(tmp_path),
        committer,
    ).run(
        job_id="synthetic-shopify-parity",
        checkpoint_lineage="lineage",
        connector=connector,
        request=projection_request,
        datasets=(dataset_name,),
        projection_configuration={
            dataset_name: ceramics_projection_configuration(config)
        },
    )

    assert pipeline_result.pages == 1
    assert pipeline_result.terminal and pipeline_result.enumeration_intact
    [(page, batches, outcomes)] = committer.pages
    assert page.terminal and page.enumeration_intact
    [snapshot] = page.items
    assert snapshot.connector == "shopify"
    assert snapshot.observed_at == NOW
    [batch] = batches
    assert batch.records == outcomes[0].records == 1
    with gzip.open(batch.location, "rt", encoding="utf-8") as stream:
        projected = [json.loads(line) for line in stream]

    assert len(legacy_result.records) == len(projected) == 1
    expected = {**legacy_result.records[0], "fetched_at": "2026-08-22T12:00:00Z"}
    assert projected[0] == expected
    expected_calls = [
        ("GET", "https://shop.test/meta.json"),
        ("GET", "https://shop.test/products.json?limit=250&page=1"),
    ]
    assert legacy_fetcher.calls == library_fetcher.calls == expected_calls
    assert legacy_result.requests == len(legacy_fetcher.calls) == 2
    assert len(library_fetcher.calls) == 2
