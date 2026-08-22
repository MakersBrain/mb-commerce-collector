"""Canary adapter from the neutral Shopify connector to legacy ScrapeResult."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import httpx
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

from mb_ceramics_catalogue.config.sources import SourceConfig
from mb_ceramics_catalogue.connectors import CollectionRequest, RefreshMode
from mb_ceramics_catalogue.datasets import ProjectionContext, built_in_registry
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import (
    ceramics_projection_configuration,
    source_definition,
)
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    build_library_pipeline_connector,
)
from mb_ceramics_catalogue.proxy import ProxyDenied

from .base import Blocked, Scraper


class _CountingFetcher:
    """Count successful legacy fetches while preserving its transport policy."""

    def __init__(self, fetcher: Any) -> None:
        self.fetcher = fetcher
        self.requests = 0
        self.failures: list[tuple[str, Exception]] = []

    @property
    def proxy_lease(self) -> Any:
        return self.fetcher.proxy_lease

    @property
    def stats(self) -> Any:
        return self.fetcher.stats

    @property
    def limiter(self) -> Any:
        return self.fetcher.limiter

    async def response(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        method: str = "GET",
        json_body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        try:
            value = await self.fetcher.response(
                url,
                params=params,
                method=method,
                json_body=json_body,
                headers=headers,
            )
        except (Blocked, ProxyDenied) as error:
            self.failures.append((url, error))
            raise
        except Exception as error:
            self.failures.append((url, error))
            raise
        self.requests += 1
        return value

    async def render(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None
    ) -> str:
        try:
            value = await self.fetcher.render(url, wait_ms=wait_ms, wait_for=wait_for)
        except (Blocked, ProxyDenied) as error:
            self.failures.append((url, error))
            raise
        except Exception as error:
            self.failures.append((url, error))
            raise
        self.requests += 1
        return value

    async def rotate_client(self) -> None:
        await self.fetcher.rotate_client()

    async def may_fetch(
        self, url: str, ignore_robots: bool = False, obey_robots: bool = False
    ) -> bool:
        return await self.fetcher.may_fetch(url, ignore_robots, obey_robots)


class ShopifyConnectorScraper(Scraper):
    """Explicit opt-in path; the production ``shopify`` key remains legacy."""

    platform = "shopify"
    method = "api_json"

    def __init__(self, name: str, config: dict[str, Any], fetcher: Any) -> None:
        super().__init__(name, config, fetcher)
        self._cancel_requested = False

    def cancel(self) -> None:
        """Cooperatively stop before the next remote page."""
        self._cancel_requested = True

    async def scrape(self, limit: int | None = None) -> Any:
        collection_id = f"local:{uuid4().hex}"
        counting = _CountingFetcher(self.fetcher)
        config = SourceConfig.model_validate(self.config)
        registry = built_in_registry()
        dataset = "ceramics.catalogue_item.v2"
        definition = registry.get(dataset)
        requested_fields = registry.collection_requirements((dataset,))[0]
        request = CollectionRequest(
            source_id=self.name,
            base_url=self.base_url,
            refresh_mode=RefreshMode.FULL,
            requested_fields=requested_fields,
            result_limit=limit,
            collections=tuple(self.config.get("collections") or ()),
            cancellation_check=lambda: self._cancel_requested,
        )
        library_request = LibraryCollectionRequest(
            source_id=self.name,
            base_url=self.base_url,
            refresh_mode=LibraryRefreshMode.FULL,
            requested_fields=frozenset(
                LibrarySnapshotField(field.value) for field in requested_fields
            ),
            result_limit=limit,
            partitions=tuple(self.config.get("collections") or ()),
        )
        connector = build_library_pipeline_connector(
            registry=LibraryConnectorRegistry.with_builtins(),
            source=source_definition(self.name, config),
            request=library_request,
            checkpoint=None,
            fetcher=counting,
            cancelled=lambda: self._cancel_requested,
            collection_id=collection_id,
            ignore_robots=bool(config.ignore_robots),
            obey_robots=bool(config.obey_robots),
        )
        projection = {
            **ceramics_projection_configuration(config),
            # Legacy Scraper.add still owns category allowlists and exclusions
            # for this compatibility output path.
            "apply_scope": False,
        }
        context = ProjectionContext(
            collection_id=collection_id,
            source_id=self.name,
            dataset=dataset,
            dataset_version=definition.version,
            projector_version=definition.projector_version,
            configuration=projection,
        )

        priceless = 0
        try:
            async for page in connector.collect(request):
                self.result.discovered += page.discovered
                if not page.enumeration_intact:
                    self.result.truncated = True
                self._diagnostics(page.diagnostics)
                for snapshot in page.items:
                    category_match = self._category_match(snapshot)
                    for variant in snapshot.variants:
                        if not variant.offers:
                            priceless += 1
                    for typed in registry.project_validated(dataset, snapshot, context):
                        self.add(typed.model_dump(mode="json"), category_match)
        except asyncio.CancelledError:
            self.result.truncated = True
            raise
        finally:
            self.result.requests = counting.requests

        if self._cancel_requested:
            self.result.truncated = True
        if limit is not None and self.result.discovered >= limit:
            self.result.truncated = True
        for url, error in counting.failures:
            if url.endswith("/meta.json"):
                self.note(f"shop currency unavailable from meta.json ({error})")
        if priceless:
            self.note(
                f"{priceless} variants dropped without a price: "
                "the shop's currency could not be read from meta.json"
            )
        return self.result

    def _diagnostics(self, diagnostics: tuple[Any, ...]) -> None:
        for diagnostic in diagnostics:
            if diagnostic.affects_completeness:
                self.result.truncated = True
            if diagnostic.severity == "error":
                self.result.errors.append(
                    {"url": diagnostic.url or self.base_url, "error": diagnostic.message}
                )
            else:
                self.note(diagnostic.message)

    def _category_match(self, snapshot: Any) -> bool | None:
        extensions = snapshot.platform_extensions
        tags = extensions.get("tags") or []
        tags = tags if isinstance(tags, list) else [tags]
        partition = snapshot.categories[0].name if len(snapshot.categories) > 1 else ""
        product_type = snapshot.categories[-1].name if snapshot.categories else ""
        return self.category_allows(
            product_type,
            " ".join(str(item) for item in tags),
            partition,
            extensions.get("handle") or "",
        )
