from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from mb_commerce_scraper import (
    CollectionRequest as LibraryCollectionRequest,
)
from mb_commerce_scraper import (
    ConnectorRegistry as LibraryConnectorRegistry,
)
from mb_commerce_scraper import (
    ProxyMode,
    ProxyPolicyConfig,
)
from mb_commerce_scraper import (
    SnapshotField as LibrarySnapshotField,
)
from mb_commerce_scraper.runtime import CommerceScraper
from mb_commerce_scraper.testing import FakeTransport

from mb_ceramics_catalogue.config.settings import CrawlParams
from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.connectors import (
    CollectionRequest,
    EntityPage,
    RefreshMode,
    SnapshotField,
)
from mb_ceramics_catalogue.ops import commerce_scraper_runtime as runtime_module
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import (
    CatalogueSourceConfig,
    layered_source_config,
)
from mb_ceramics_catalogue.ops.commerce_scraper_proxy_runtime import NativeProxyRuntimeSpec
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    CatalogueCachePolicy,
    CatalogueCacheStats,
    CatalogueCommerceRuntime,
    LibraryDebugTelemetry,
    LocalCommerceSession,
    NativeCollectionSpec,
    NativeRouteBindings,
    OpenCatalogueCollection,
    application_connector_registry,
    open_local_commerce_session,
)


def _configuration() -> CatalogueSourceConfig:
    source = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "shopify",
            }
        }
    )["shop"]
    return layered_source_config(
        "shop",
        source,
        run=CrawlParams(cache_mode="off", stale_on_error=True),
    )


def _spec(tmp_path: Path, **request_changes: Any) -> NativeCollectionSpec:
    configuration = _configuration()
    request = LibraryCollectionRequest(
        source_id="shop",
        base_url="https://shop.test/",
        requested_fields=frozenset({LibrarySnapshotField.IDENTITY}),
    ).model_copy(update=request_changes)
    return NativeCollectionSpec(
        configuration=configuration,
        request=request,
        checkpoint=None,
        cache=CatalogueCachePolicy(
            directory=tmp_path,
            mode="off",
            maximum_age_seconds=None,
            stale_on_error=True,
        ),
        cancelled=lambda: False,
        collection_id="collection-1",
    )


def _forbid_resource_construction(*args: Any, **kwargs: Any) -> Any:
    del args, kwargs
    raise AssertionError("identity validation must precede resource construction")


def test_collection_plan_owns_dynamic_partition_declaration() -> None:
    source = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "woocommerce",
                "store_categories": ["glazes"],
            }
        }
    )["shop"]
    runtime = CatalogueCommerceRuntime(application_connector_registry())

    plan = runtime.plan_collection(
        "shop",
        source,
        run=CrawlParams(cache_mode="off"),
        datasets=("ceramics.catalogue_item.v2",),
        requested_fields=frozenset({SnapshotField.IDENTITY}),
        result_limit=25,
        cancelled=lambda: False,
    )

    assert plan.request.categories == ("glazes",)
    assert plan.library_request.partitions == ("glazes",)
    assert plan.route.dynamic_partitions
    assert plan.connector_configuration == {"partitions": []}


def test_collection_assembly_applies_shared_browser_gate(tmp_path: Path) -> None:
    source = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "ceramicolours",
                "category_ids": ["5101"],
            }
        }
    )["shop"]
    runtime = CatalogueCommerceRuntime(application_connector_registry())
    run = CrawlParams(cache_mode="off", browser="never")
    plan = runtime.plan_collection(
        "shop",
        source,
        run=run,
        datasets=("ceramics.catalogue_item.v2",),
        requested_fields=frozenset({SnapshotField.IDENTITY}),
        result_limit=None,
        cancelled=lambda: False,
    )

    assembly = runtime.assemble_collection(
        plan,
        checkpoint=None,
        cache_directory=tmp_path,
        collection_id="collection-browser-disabled",
        browser_factory=_forbid_resource_construction,
    )

    assert plan.route.uses_browser_transport
    assert assembly.routes.browser is None
    assert assembly.spec.cache.mode == run.cache_mode
    assert assembly.spec.request is plan.library_request


def _shopify_product(
    identifier: int,
    title: str,
    product_type: str,
) -> dict[str, object]:
    return {
        "id": identifier,
        "handle": f"product-{identifier}",
        "title": title,
        "product_type": product_type,
        "tags": [product_type],
        "variants": [
            {
                "id": identifier * 10,
                "title": "Default Title",
                "price": "12.00",
                "available": True,
            }
        ],
    }


@pytest.mark.parametrize(
    "request_changes",
    [
        {"source_id": "other"},
        {"base_url": "https://other.test/"},
    ],
)
async def test_request_identity_mismatch_fails_before_resource_construction(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    request_changes: dict[str, str],
) -> None:
    monkeypatch.setattr(runtime_module, "DiskResponseCache", _forbid_resource_construction)
    monkeypatch.setattr(runtime_module, "build_http_scraper", _forbid_resource_construction)
    runtime = CatalogueCommerceRuntime(LibraryConnectorRegistry.with_builtins())

    with pytest.raises(ValueError, match="request identity"):
        async with runtime.open_collection(_spec(tmp_path, **request_changes)):
            pytest.fail("an invalid collection must not open")


@pytest.mark.parametrize(
    ("source_id", "base_url"),
    [
        ("other", "https://shop.test/"),
        ("shop", "https://other.test/"),
    ],
)
async def test_proxy_identity_mismatch_fails_before_resource_construction(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    source_id: str,
    base_url: str,
) -> None:
    monkeypatch.setattr(runtime_module, "DiskResponseCache", _forbid_resource_construction)
    monkeypatch.setattr(runtime_module, "build_http_scraper", _forbid_resource_construction)
    proxy = NativeProxyRuntimeSpec(
        source_id=source_id,
        base_url=base_url,
        pool=object(),  # type: ignore[arg-type]
        policy=ProxyPolicyConfig(mode=ProxyMode.FALLBACK),
    )
    runtime = CatalogueCommerceRuntime(LibraryConnectorRegistry.with_builtins())

    with pytest.raises(ValueError, match="proxy runtime identity"):
        async with runtime.open_collection(
            _spec(tmp_path), NativeRouteBindings(proxy=proxy)
        ):
            pytest.fail("an invalid proxy binding must not open")


async def test_runtime_composes_collects_and_closes_one_collection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/meta.json", json_body={"currency": "EUR"})
    backend.add("https://shop.test/products.json", json_body={"products": []})
    lifecycle: list[str] = []
    telemetry_instances: list[RecordingTelemetry] = []

    class RecordingTelemetry(LibraryDebugTelemetry):
        def __init__(self) -> None:
            super().__init__()
            self.events: list[tuple[str, dict[str, Any]]] = []
            telemetry_instances.append(self)

        def emit(self, event: str, fields: dict[str, Any]) -> None:
            self.events.append((event, dict(fields)))
            super().emit(event, fields)

    class LifecycleScraper(CommerceScraper):
        async def __aenter__(self) -> CommerceScraper:
            lifecycle.append("open")
            return await super().__aenter__()

        async def __aexit__(self, *args: object) -> None:
            try:
                await super().__aexit__(*args)
            finally:
                lifecycle.append("close")

    def build_scraper(**kwargs: Any) -> CommerceScraper:
        return LifecycleScraper(
            registry=kwargs["registry"],
            transport=backend,
            fetch_policy=kwargs["fetch_policy"],
            cache=kwargs["cache"],
            stale_on_error=kwargs["stale_on_error"],
            telemetry=kwargs["telemetry"],
            retries=kwargs["retries"],
        )

    monkeypatch.setattr(runtime_module, "LibraryDebugTelemetry", RecordingTelemetry)
    monkeypatch.setattr(runtime_module, "build_http_scraper", build_scraper)
    runtime = CatalogueCommerceRuntime(LibraryConnectorRegistry.with_builtins())
    pipeline_request = CollectionRequest(
        source_id="shop",
        base_url="https://shop.test/",
        refresh_mode=RefreshMode.FULL,
        requested_fields=frozenset({SnapshotField.IDENTITY}),
    )

    async with runtime.open_collection(_spec(tmp_path)) as opened:
        pages = [page async for page in opened.connector.collect(pipeline_request)]
        assert lifecycle == ["open"]
        assert opened.telemetry is telemetry_instances[0]

    assert pages[-1].terminal
    assert lifecycle == ["open", "close"]
    assert [request.url for request in backend.requests] == [
        "https://shop.test/meta.json",
        "https://shop.test/products.json",
    ]
    correlated = [
        fields
        for event, fields in telemetry_instances[0].events
        if event
        in {
            "collection.started",
            "request.started",
            "catalogue.library_connector.collection.started",
            "catalogue.library_connector.page.completed",
            "catalogue.library_connector.collection.completed",
        }
    ]
    assert correlated
    assert {fields["collection_id"] for fields in correlated} == {"collection-1"}


async def test_local_scraper_uses_native_runtime_with_direct_policy_and_canonical_scope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/meta.json", json_body={"currency": "EUR"})
    backend.add(
        "https://shop.test/products.json",
        json_body={
            "products": [
                _shopify_product(1, "Transparent Glaze", "Glazes"),
                _shopify_product(2, "Kiln Shelf", "Equipment"),
                _shopify_product(3, "Clearance Glaze", "Glazes Clearance"),
            ]
        },
    )
    calls: list[tuple[NativeCollectionSpec, NativeRouteBindings | None]] = []

    class RecordingRuntime(CatalogueCommerceRuntime):
        @asynccontextmanager
        async def open_collection(
            self,
            spec: NativeCollectionSpec,
            routes: NativeRouteBindings | None = None,
        ) -> AsyncIterator[OpenCatalogueCollection]:
            calls.append((spec, routes))
            async with super().open_collection(spec, routes) as opened:
                yield opened

    def build_scraper(**kwargs: Any) -> CommerceScraper:
        return CommerceScraper(
            registry=kwargs["registry"],
            transport=backend,
            fetch_policy=kwargs["fetch_policy"],
            cache=kwargs["cache"],
            stale_on_error=kwargs["stale_on_error"],
            telemetry=kwargs["telemetry"],
            retries=kwargs["retries"],
        )

    monkeypatch.setattr(runtime_module, "build_http_scraper", build_scraper)
    source = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "shopify",
                "proxy_eligible": True,
                "scope": "materials",
                "material_categories": ["Glazes"],
                "excluded_categories": ["Clearance"],
            }
        }
    )["shop"]
    runtime = RecordingRuntime(LibraryConnectorRegistry.with_builtins())
    session = LocalCommerceSession(
        CrawlParams(cache_mode="auto"),
        tmp_path,
        runtime=runtime,
        browser=cast(Any, SimpleNamespace(shutdown=None)),
    )

    scraper = session.build(
        source.scraper,
        "shop",
        source.as_scraper_config(),
        fetcher=object(),
    )
    first = await scraper.run()
    replayed = await session.build(
        source.scraper,
        "shop",
        source.as_scraper_config(),
        fetcher=object(),
    ).run()

    assert (scraper.name, scraper.platform, scraper.method) == (
        "shop",
        "shopify",
        "api_json",
    )
    assert [row["name"] for row in first.records] == ["Transparent Glaze"]
    assert first.filtered == 2
    assert first.requests == first.direct_requests == 2
    assert first.proxy_requests == 0
    assert replayed.requests == replayed.direct_requests == 0
    assert replayed.cache_bytes_read > 0
    assert session.cache_summary() == (
        "cache mode=auto hits=2 (50%) misses=2 stored=2"
    )
    assert len(calls) == 2
    for spec, routes in calls:
        assert spec.configuration.proxy.mode is ProxyMode.NEVER
        assert spec.configuration.source.id == spec.request.source_id == "shop"
        assert spec.configuration.source.connector == "shopify"
        assert routes is not None and routes.proxy is None
        assert spec.collection_id.startswith("local:")


async def test_local_browser_route_binds_collection_and_closes_owned_backend(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class Browser:
        def __init__(self) -> None:
            self.shutdowns = 0

        async def shutdown(self) -> None:
            self.shutdowns += 1

    class EmptyConnector:
        async def collect(self, request: Any) -> AsyncIterator[Any]:
            del request
            yield EntityPage[Any](
                page_id="empty",
                sequence=0,
                items=(),
                terminal=True,
                discovered=0,
            )

    calls: list[tuple[NativeCollectionSpec, NativeRouteBindings | None]] = []

    class BindingRuntime(CatalogueCommerceRuntime):
        @asynccontextmanager
        async def open_collection(
            self,
            spec: NativeCollectionSpec,
            routes: NativeRouteBindings | None = None,
        ) -> AsyncIterator[OpenCatalogueCollection]:
            calls.append((spec, routes))
            yield OpenCatalogueCollection(
                connector=cast(Any, EmptyConnector()),
                telemetry=LibraryDebugTelemetry(),
                cache=cast(
                    Any,
                    SimpleNamespace(stats=lambda: CatalogueCacheStats()),
                ),
            )

    browser = Browser()
    monkeypatch.setattr(runtime_module, "BrowserRenderer", lambda enabled: browser)
    source = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "ceramicolours",
                "category_ids": ["5101"],
            }
        }
    )["shop"]
    runtime = BindingRuntime(application_connector_registry())

    async with open_local_commerce_session(
        CrawlParams(cache_mode="off"),
        None,
        runtime=runtime,
    ) as session:
        scraper = session.build(
            source.scraper,
            "shop",
            source.as_scraper_config(),
            fetcher=object(),
        )
        result = await scraper.run()
        assert not result.truncated
        assert browser.shutdowns == 0

    assert browser.shutdowns == 1
    [(spec, routes)] = calls
    assert routes is not None and routes.proxy is None
    assert routes.browser is not None
    assert cast(Any, routes.browser.backend) is browser
    assert routes.browser.job.job_id == spec.collection_id
    assert routes.browser.job.logical_profile == "shop"
