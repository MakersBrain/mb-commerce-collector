"""One local CLI compatibility shell over the reusable connector registry."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import httpx
from mb_commerce_scraper import (
    CollectionRequest as LibraryCollectionRequest,
)
from mb_commerce_scraper import RefreshMode as LibraryRefreshMode
from mb_commerce_scraper import SnapshotField as LibrarySnapshotField

from mb_ceramics_catalogue.config.sources import SourceConfig
from mb_ceramics_catalogue.connectors import CollectionRequest, RefreshMode
from mb_ceramics_catalogue.datasets import ProjectionContext, built_in_registry
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import (
    ceramics_projection_configuration,
    source_definition,
)
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    application_connector_registry,
    build_library_pipeline_connector,
)
from mb_ceramics_catalogue.ops.connector_adapters import (
    library_canary_route,
    runtime_plan,
)

from . import CONNECTOR_CANARY_SCRAPERS, LIBRARY_CANARY_SCRAPERS
from .base import Scraper


class _CountingFetcher:
    """Observe compatibility usage without taking ownership from Fetcher."""

    def __init__(self, fetcher: Any) -> None:
        self.fetcher = fetcher
        self.requests = 0
        self.rendered_pages = 0

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
        browser_user_agent: bool = False,
    ) -> httpx.Response:
        fetch_options: dict[str, Any] = {
            "params": params,
            "method": method,
            "json_body": json_body,
            "headers": headers,
        }
        if browser_user_agent:
            fetch_options["browser_user_agent"] = True
        response = await self.fetcher.response(
            url,
            **fetch_options,
        )
        self.requests += 1
        return response

    async def render(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None
    ) -> str:
        document = await self.fetcher.render(
            url, wait_ms=wait_ms, wait_for=wait_for
        )
        self.requests += 1
        self.rendered_pages += 1
        return document

    async def request_json_in_browser(
        self,
        page_url: str,
        endpoint: str,
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        body: Any = None,
    ) -> Any:
        value = await self.fetcher.request_json_in_browser(
            page_url,
            endpoint,
            method=method,
            headers=headers,
            body=body,
        )
        self.requests += 1
        self.rendered_pages += 1
        return value

    async def evaluate_in_browser(
        self,
        url: str,
        script: str,
        wait_ms: int = 2000,
        wait_for: str | None = None,
        *,
        action_id: str = "legacy-evaluate.v1",
    ) -> Any:
        value = await self.fetcher.evaluate_in_browser(
            url,
            script,
            wait_ms=wait_ms,
            wait_for=wait_for,
            action_id=action_id,
        )
        self.requests += 1
        self.rendered_pages += 1
        return value

    async def rotate_client(self) -> None:
        await self.fetcher.rotate_client()

    async def may_fetch(
        self, url: str, ignore_robots: bool = False, obey_robots: bool = False
    ) -> bool:
        return await self.fetcher.may_fetch(url, ignore_robots, obey_robots)


class LibraryConnectorScraper(Scraper):
    """Project any explicitly approved library connector into legacy CLI rows."""

    platform = "commerce-library"
    method = "connector"

    def __init__(self, name: str, config: dict[str, Any], fetcher: Any) -> None:
        alias = str(config.get("scraper", ""))
        original = LIBRARY_CANARY_SCRAPERS.get(alias) or CONNECTOR_CANARY_SCRAPERS.get(alias)
        if original is None:
            raise ValueError("unsupported local library connector alias") from None
        source = SourceConfig.model_validate({**config, "scraper": original})
        plan = runtime_plan(source)
        definition = source_definition(name, source, connector_plan=plan)
        route = library_canary_route(plan, definition.connector)
        if route is None:
            raise ValueError("connector is not approved for the local library canary")
        self._source = source
        self._plan = plan
        self._definition = definition
        self._route = route
        self.platform = definition.connector
        self.method = plan.extraction_method
        super().__init__(name, config, fetcher)
        self._cancel_requested = False

    def cancel(self) -> None:
        self._cancel_requested = True

    async def scrape(self, limit: int | None = None) -> Any:
        collection_id = f"local:{uuid4().hex}"
        counting = _CountingFetcher(self.fetcher)
        registry = built_in_registry()
        dataset = (
            "ceramics.catalogue_identity.v2"
            if self._source.identity_only
            else "ceramics.catalogue_item.v2"
        )
        dataset_definition = registry.get(dataset)
        requested_fields = registry.collection_requirements((dataset,))[0]
        request = CollectionRequest(
            source_id=self.name,
            base_url=self.base_url,
            refresh_mode=RefreshMode.FULL,
            requested_fields=requested_fields,
            result_limit=limit,
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
            partitions=self._route.request_partitions,
        )
        connector = build_library_pipeline_connector(
            registry=application_connector_registry(),
            source=self._definition,
            request=library_request,
            checkpoint=None,
            fetcher=counting,
            cancelled=lambda: self._cancel_requested,
            collection_id=collection_id,
            ignore_robots=bool(self._source.ignore_robots),
            obey_robots=bool(self._source.obey_robots),
        )
        context = ProjectionContext(
            collection_id=collection_id,
            source_id=self.name,
            dataset=dataset,
            dataset_version=dataset_definition.version,
            projector_version=dataset_definition.projector_version,
            configuration={
                **ceramics_projection_configuration(self._source),
                # Scraper.add retains the compatibility category/scope filter.
                "apply_scope": False,
            },
        )

        try:
            async for page in connector.collect(request):
                self.result.discovered += page.discovered
                if not page.enumeration_intact:
                    self.result.truncated = True
                self._diagnostics(page.diagnostics)
                for snapshot in page.items:
                    category_match = self._category_match(snapshot)
                    for typed in registry.project_validated(
                        dataset, snapshot, context
                    ):
                        self.add(typed.model_dump(mode="json"), category_match)
        except asyncio.CancelledError:
            self.result.truncated = True
            raise
        finally:
            self.result.requests = counting.requests
            self.result.rendered_pages = counting.rendered_pages

        if self._cancel_requested:
            self.result.truncated = True
        if limit is not None and self.result.discovered >= limit:
            self.result.truncated = True
        return self.result

    def _diagnostics(self, diagnostics: tuple[Any, ...]) -> None:
        for diagnostic in diagnostics:
            if diagnostic.affects_completeness:
                self.result.truncated = True
            if diagnostic.severity == "error":
                self.result.errors.append(
                    {
                        "url": diagnostic.url or self.base_url,
                        "error": diagnostic.message,
                    }
                )

    def _category_match(self, snapshot: Any) -> bool | None:
        """Apply the legacy material allowlist to normalized library fields."""
        extensions = snapshot.platform_extensions
        tags = extensions.get("tags") or []
        tags = tags if isinstance(tags, list) else [tags]
        return self.category_allows(
            *(category.name for category in snapshot.categories),
            *(str(tag) for tag in tags),
            extensions.get("handle") or "",
        )
