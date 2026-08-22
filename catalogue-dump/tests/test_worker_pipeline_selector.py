import asyncio
from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import httpx
import pytest
from mb_commerce_scraper import (
    CollectionRequest as LibraryCollectionRequest,
)
from mb_commerce_scraper import (
    ConnectorRegistry as LibraryConnectorRegistry,
)
from mb_commerce_scraper import SnapshotField as LibrarySnapshotField
from mb_commerce_scraper.runtime import CommerceScraper
from mb_commerce_scraper.testing import FakeTransport

from mb_ceramics_catalogue import scrapers
from mb_ceramics_catalogue.config.settings import Settings
from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.connectors import CollectionRequest, RefreshMode, SnapshotField
from mb_ceramics_catalogue.observability import metrics
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import (
    layered_source_config,
    source_definition,
)
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    LibraryDebugTelemetry,
    apply_library_fetch_policy,
    build_library_pipeline_connector,
    fetcher_transport_totals,
    local_canary_source_config,
    open_native_library_pipeline_connector,
)
from mb_ceramics_catalogue.ops.queue import ClaimedJob
from mb_ceramics_catalogue.ops.worker import (
    Worker,
    _durable_error,
    _legacy_terminal_state,
)
from mb_ceramics_catalogue.pipeline.runner import PipelineResult


class Pool:
    @asynccontextmanager
    async def connection(self):
        yield object()


class ShopifyFetcher:
    proxy_lease = None

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.stats = SimpleNamespace(proxy_requests=0)
        self.limiter = RecordingLimiter()
        self.policy_calls: list[tuple[str, bool, bool]] = []
        self.allowed = True

    async def response(self, url: str, **kwargs: Any) -> httpx.Response:
        self.calls.append(url)
        return httpx.Response(
            200,
            json={"products": []},
            request=httpx.Request("GET", url, params=kwargs.get("params")),
        )

    async def render(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None
    ) -> str:
        del url, wait_ms, wait_for
        raise AssertionError("Shopify canary must not render")

    async def rotate_client(self) -> None:
        raise AssertionError("empty Shopify feed must not rotate")

    async def may_fetch(
        self, url: str, ignore_robots: bool = False, obey_robots: bool = False
    ) -> bool:
        self.policy_calls.append((url, ignore_robots, obey_robots))
        return self.allowed


class RecordingLimiter:
    def __init__(self) -> None:
        self.groups: list[tuple[str, str]] = []
        self.delays: list[tuple[str, float]] = []

    def join_group(self, url: str, group: str) -> None:
        self.groups.append((url, group))

    def set_delay(self, url: str, delay: float) -> None:
        self.delays.append((url, delay))


def test_legacy_database_rejections_are_terminally_degraded() -> None:
    assert _legacy_terminal_state(None, 1) == "degraded"
    assert _legacy_terminal_state(None, 0) == "succeeded"
    assert _legacy_terminal_state("crawl failed", 1) == "failed"


def test_durable_worker_error_is_bounded_and_redacted() -> None:
    secret = "worker-error-secret"

    retained = _durable_error(
        RuntimeError(
            f"failed Authorization: Bearer {secret} " + "x" * 4_000
        )
    )

    assert secret not in retained
    assert len(retained) <= 2_000


def job(pipeline: str) -> ClaimedJob:
    return ClaimedJob(
        id=uuid4(),
        run_id=uuid4(),
        source_id="shop",
        host="shop.test",
        attempt=1,
        max_attempts=3,
        requires=[],
        requires_any=[],
        params={"pipeline": pipeline},
        proxy_snapshot={},
        delivery_generation=1,
        execution_token=uuid4(),
    )


@pytest.mark.asyncio
async def test_connector_pipeline_runs_only_for_an_explicit_canary(monkeypatch, tmp_path):
    sources = SourcesFile.model_validate(
        {"shop": {"label": "Shop", "url": "https://shop.test/", "scraper": "shopify"}}
    )
    worker = Worker(Pool(), sources, Settings(dumps_dir=tmp_path))
    selected: list[str] = []

    async def canary(claimed, params, config):
        selected.append(params.pipeline)

    monkeypatch.setattr(worker, "_crawl_connector_canary", canary)
    claimed = job("connector_canary")
    worker._cancels[claimed.id] = asyncio.Event()

    await worker._crawl_and_load(claimed)

    assert selected == ["connector_canary"]


def test_legacy_remains_the_default_pipeline():
    from mb_ceramics_catalogue.config.settings import CrawlParams

    assert CrawlParams().pipeline == "legacy"
    assert CrawlParams().datasets == ("ceramics",)


def test_local_shopify_canary_selects_library_compatibility_shell():
    config = SourcesFile.model_validate(
        {"shop": {"label": "Shop", "url": "https://shop.test/", "scraper": "shopify"}}
    )["shop"]

    selected = local_canary_source_config(config)

    assert selected.scraper == "library_shopify_connector"
    assert config.scraper == "shopify"


@pytest.mark.parametrize(
    ("scraper", "options"),
    [
        ("woocommerce", {"store_categories": ["glazes"]}),
        ("bigcommerce", {}),
        ("prestashop", {"sitemaps": ["https://shop.test/products.xml"]}),
        (
            "sio2",
            {
                "category_urls": ["https://shop.test/clay"],
                "use_advertised_sitemaps": False,
            },
        ),
        ("wix", {"sitemaps": ["https://shop.test/store-products.xml"]}),
        *(
            (
                name,
                {
                    "category_urls": ["https://shop.test/category"],
                    "use_advertised_sitemaps": False,
                    **({"render": False} if name == "nitrosell" else {}),
                },
            )
            for name in ("shopware", "starweb", "nitrosell")
        ),
        ("sumup", {}),
        ("pagecrawl", {"sitemaps": ["https://shop.test/products.xml"]}),
        ("axner", {"render": False}),
        (
            "keramik_kraft",
            {"category_paths": ["de/Glasuren.html"], "render": False},
        ),
        ("ceramicolours", {}),
    ],
)
def test_local_native_canaries_select_one_shared_library_shell(
    scraper: str, options: dict[str, Any]
) -> None:
    config = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": scraper,
                **options,
            }
        }
    )["shop"]

    selected = local_canary_source_config(config)

    assert selected.scraper == f"library_{scraper}_connector"
    assert scrapers.load(selected.scraper).__name__ == "LibraryConnectorScraper"


@pytest.mark.parametrize(
    ("scraper", "options"),
    [
        (
            "shopware",
            {
                "category_urls": ["https://shop.test/category"],
                "use_advertised_sitemaps": False,
            },
        ),
        ("sumup", {}),
        (
            "starweb",
            {
                "category_urls": ["https://shop.test/category"],
                "use_advertised_sitemaps": False,
            },
        ),
        (
            "nitrosell",
            {
                "category_urls": ["https://shop.test/category"],
                "use_advertised_sitemaps": False,
                "render": False,
            },
        ),
    ],
)
def test_specialized_canary_aliases_use_shared_composition_and_keep_stable_rollback(
    scraper: str, options: dict[str, Any]
) -> None:
    source = SourcesFile.model_validate(
        {
            "stable-source": {
                "label": "Stable source",
                "url": "https://shop.test/",
                "scraper": scraper,
                **options,
            }
        }
    )["stable-source"]

    selected = local_canary_source_config(source)
    generated_alias = f"library_{scraper}_connector"
    direct_alias = f"{scraper}_connector"
    library_type = scrapers.load(generated_alias)
    direct_type = scrapers.load(direct_alias)
    rollback_type = scrapers.load(scraper)
    shell = library_type(
        "stable-source",
        selected.as_scraper_config(),
        cast(Any, SimpleNamespace(limiter=RecordingLimiter())),
    )
    library_shell = cast(Any, shell)
    direct_shell = direct_type(
        "stable-source",
        {**source.as_scraper_config(), "scraper": direct_alias},
        cast(Any, SimpleNamespace(limiter=RecordingLimiter())),
    )

    assert selected.scraper == generated_alias
    assert library_type.__name__ == "LibraryConnectorScraper"
    assert library_type.__module__.endswith(".library_connector")
    assert library_shell.name == library_shell._definition.id == "stable-source"
    assert library_shell._source.scraper == scraper
    assert scrapers.LIBRARY_CANARY_SCRAPERS[generated_alias] == scraper
    assert scrapers.CONNECTOR_CANARY_SCRAPERS[direct_alias] == scraper
    assert direct_type is library_type
    assert cast(Any, direct_shell)._source.scraper == scraper
    assert scrapers.REGISTRY[scraper] != scrapers.REGISTRY[direct_alias]
    assert not rollback_type.__module__.endswith(".library_connector")


def test_local_bespoke_rollback_adapter_remains_registered() -> None:
    config = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "ceramicolours",
            }
        }
    )["shop"]

    selected = local_canary_source_config(config)

    assert selected.scraper == "library_ceramicolours_connector"
    assert scrapers.load("ceramicolours_connector").__name__ == (
        "CeramicoloursConnectorScraper"
    )


def test_transport_summary_aggregates_direct_and_fallback_without_mutation():
    fallback = SimpleNamespace(
        stats=SimpleNamespace(proxy_requests=2, http_rx_bytes_estimated=300),
        proxy_fallback=None,
    )
    direct = SimpleNamespace(
        stats=SimpleNamespace(direct_requests=1, http_rx_bytes_estimated=100),
        proxy_fallback=fallback,
    )

    totals = fetcher_transport_totals(cast(Any, direct))

    assert totals["direct_requests"] == 1
    assert totals["proxy_requests"] == 2
    assert totals["http_rx_bytes_estimated"] == 400


def test_native_telemetry_accumulates_terminal_attempt_accounting_once():
    telemetry = LibraryDebugTelemetry()

    telemetry.emit(
        "request.retry",
        {
            "route": "direct",
            "status": 503,
            "physical_requests": 1,
            "transmitted_bytes": 20,
            "received_bytes": 30,
        },
    )
    telemetry.emit(
        "request.completed",
        {
            "route": "browser",
            "provider": "decodo",
            "status": 200,
            "physical_requests": 3,
            "transmitted_bytes": 100,
            "received_bytes": 200,
        },
    )
    telemetry.emit(
        "request.failed",
        {
            "physical_requests": 1,
            "transmitted_bytes": 5,
            "received_bytes": 0,
        },
    )
    # The classification-only retry following a TransportFailure has no
    # accounting and must not count the failed attempt for a second time.
    telemetry.emit("request.retry", {"classification": "transport_failure"})

    totals = telemetry.transport_totals()
    assert totals["physical_requests"] == 5
    assert totals["direct_requests"] == 1
    assert totals["browser_requests"] == 3
    assert totals["proxy_requests"] == 3
    assert totals["unclassified_requests"] == 1
    assert totals["http_tx_bytes_estimated"] == 25
    assert totals["http_rx_bytes_estimated"] == 30
    assert totals["browser_tx_bytes_estimated"] == 100
    assert totals["browser_rx_bytes_estimated"] == 200
    assert telemetry.outcome_counts() == {
        "5xx": 1,
        "2xx": 3,
        "transport_error": 1,
    }


@pytest.mark.parametrize(("status", "outcome"), ((403, "403"), (429, "429")))
def test_native_retry_telemetry_preserves_dedicated_http_outcomes(
    status: int, outcome: str
) -> None:
    telemetry = LibraryDebugTelemetry()

    telemetry.emit(
        "request.retry",
        {"status": status, "physical_requests": 1, "route": "direct"},
    )

    assert telemetry.outcome_counts() == {outcome: 1}


def test_native_telemetry_projects_declared_levels_and_defaults_to_debug(
    monkeypatch: pytest.MonkeyPatch,
):
    from mb_ceramics_catalogue.ops import commerce_scraper_runtime as runtime_module

    emitted: list[tuple[str, str]] = []

    class Logger:
        def debug(self, event: str, **_fields: Any) -> None:
            emitted.append(("debug", event))

        def info(self, event: str, **_fields: Any) -> None:
            emitted.append(("info", event))

        def warning(self, event: str, **_fields: Any) -> None:
            emitted.append(("warning", event))

        def error(self, event: str, **_fields: Any) -> None:
            emitted.append(("error", event))

    monkeypatch.setattr(runtime_module, "LOGGER", Logger())
    telemetry = LibraryDebugTelemetry()

    telemetry.emit("collection.started", {"level": "info"})
    telemetry.emit("request.retry", {"level": "debug"})
    telemetry.emit("collection.interrupted", {"level": "warning"})
    telemetry.emit("operation.failed", {"level": "error"})
    telemetry.emit("connector.page", {})

    assert emitted == [
        ("info", "collection.started"),
        ("debug", "request.retry"),
        ("warning", "collection.interrupted"),
        ("error", "operation.failed"),
        ("debug", "connector.page"),
    ]


def test_native_telemetry_adds_scalar_trace_context_without_coupling_accounting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mb_ceramics_catalogue.ops import commerce_scraper_runtime as runtime_module

    traced: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        runtime_module.tracing,
        "event",
        lambda event, **fields: traced.append((event, fields)),
    )
    telemetry = LibraryDebugTelemetry()
    telemetry.emit(
        "request.completed",
        {
            "collection_id": "collection-1",
            "source_id": "shop",
            "route": "direct",
            "status": 200,
            "elapsed_ms": 12.5,
            "physical_requests": 1,
            "transmitted_bytes": 20,
            "received_bytes": 30,
            "nested": {"not": "an OpenTelemetry scalar"},
        },
    )

    assert traced == [
        (
            "request.completed",
            {
                "collection_id": "collection-1",
                "source_id": "shop",
                "route": "direct",
                "status": 200,
                "elapsed_ms": 12.5,
                "physical_requests": 1,
                "transmitted_bytes": 20,
                "received_bytes": 30,
            },
        )
    ]

    def broken_trace(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("observer unavailable")

    monkeypatch.setattr(runtime_module.tracing, "event", broken_trace)
    telemetry.emit(
        "request.failed",
        {
            "route": "direct",
            "physical_requests": 1,
            "transmitted_bytes": 5,
            "received_bytes": 0,
        },
    )
    assert telemetry.transport_totals()["physical_requests"] == 2


def test_native_telemetry_scopes_request_span_and_projects_bounded_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mb_ceramics_catalogue.ops import commerce_scraper_runtime as runtime_module

    lifecycle: list[tuple[str, str, dict[str, Any]]] = []

    class ActiveSpan:
        def set_attribute(self, name: str, value: Any) -> None:
            lifecycle.append(("attribute", name, {"value": value}))

    @contextmanager
    def request_span(name: str, **fields: Any):
        lifecycle.append(("enter", name, fields))
        try:
            yield ActiveSpan()
        finally:
            lifecycle.append(("exit", name, {}))

    traced: list[str] = []
    monkeypatch.setattr(runtime_module.tracing, "span", request_span)
    monkeypatch.setattr(
        runtime_module.tracing,
        "event",
        lambda event, **_fields: traced.append(event),
    )
    metrics.REGISTRY.clear()
    telemetry = LibraryDebugTelemetry()
    common = {
        "request_id": "request-1",
        "attempt": 1,
        "source_id": "shop",
        "method": "GET",
        "url": "https://shop.test/products.json?cursor=secret-free",
        "purpose": "entity",
    }

    telemetry.emit("request.started", {**common, "level": "debug"})
    telemetry.emit(
        "request.completed",
        {
            **common,
            "level": "debug",
            "status": 200,
            "route": "browser",
            "elapsed_ms": 12.5,
            "physical_requests": 1,
            "transmitted_bytes": 20,
            "received_bytes": 30,
        },
    )

    assert lifecycle[0][0:2] == ("enter", "commerce.request")
    assert lifecycle[-1] == ("exit", "commerce.request", {})
    assert traced == ["request.started", "request.completed"]
    rendered = metrics.render()
    assert (
        'catalogue_requests_total{host="shop.test",outcome="2xx",source="shop"} 1'
        in rendered
    )
    assert 'catalogue_request_duration_seconds_count{host="shop.test"} 1' in rendered
    assert 'catalogue_browser_renders_total{source="shop"} 1' in rendered
    assert "cursor=secret-free" not in rendered


@pytest.mark.asyncio
async def test_native_pipeline_composition_uses_runtime_without_legacy_fetcher(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    from mb_ceramics_catalogue.config.settings import CrawlParams
    from mb_ceramics_catalogue.ops import commerce_scraper_runtime as runtime_module

    config = SourcesFile.model_validate(
        {"shop": {"label": "Shop", "url": "https://shop.test/", "scraper": "shopify"}}
    )["shop"]
    params = CrawlParams(cache_mode="off", stale_on_error=True)
    configuration = layered_source_config("shop", config, run=params)
    library_request = LibraryCollectionRequest(
        source_id="shop",
        base_url=config.url,
        requested_fields=frozenset({LibrarySnapshotField.IDENTITY}),
    )
    pipeline_request = CollectionRequest(
        source_id="shop",
        base_url=config.url,
        refresh_mode=RefreshMode.FULL,
        requested_fields=frozenset({SnapshotField.IDENTITY}),
    )
    backend = FakeTransport()
    backend.add("https://shop.test/meta.json", json_body={"currency": "EUR"})
    backend.add("https://shop.test/products.json", json_body={"products": []})
    captured: list[dict[str, Any]] = []

    class RecordingTelemetry(runtime_module.LibraryDebugTelemetry):
        def __init__(self) -> None:
            super().__init__()
            self.events: list[tuple[str, dict[str, Any]]] = []

        def emit(self, event: str, fields: dict[str, Any]) -> None:
            self.events.append((event, dict(fields)))
            super().emit(event, fields)

    def fake_builder(**kwargs: Any) -> CommerceScraper:
        captured.append(kwargs)
        return CommerceScraper(
            registry=kwargs["registry"],
            transport=backend,
            fetch_policy=kwargs["fetch_policy"],
            cache=kwargs["cache"],
            telemetry=kwargs["telemetry"],
            retries=kwargs["retries"],
        )

    monkeypatch.setattr(runtime_module, "build_http_scraper", fake_builder)
    monkeypatch.setattr(runtime_module, "LibraryDebugTelemetry", RecordingTelemetry)
    async with open_native_library_pipeline_connector(
        registry=LibraryConnectorRegistry.with_builtins(),
        configuration=configuration,
        request=library_request,
        checkpoint=None,
        params=params,
        cache_directory=tmp_path,
        proxy=None,
        cancelled=lambda: False,
        collection_id="job-1",
    ) as (connector, telemetry):
        pages = [page async for page in connector.collect(pipeline_request)]

    assert pages[-1].terminal
    assert isinstance(telemetry, RecordingTelemetry)
    assert len(captured) == 1
    assert captured[0]["robots_transport_failure_policy"].value == "allow"
    assert captured[0]["robots_server_failure_policy"].value == "deny"
    assert captured[0]["stale_on_error"] is True
    assert captured[0]["require_proxy_browser_subrequest_authorization"] is True
    assert telemetry.transport_totals()["direct_requests"] == 2
    assert [request.url for request in backend.requests] == [
        "https://shop.test/meta.json",
        "https://shop.test/products.json",
    ]
    correlated = {
        event: fields
        for event, fields in telemetry.events
        if event
        in {
            "collection.started",
            "request.started",
            "catalogue.library_connector.collection.started",
            "catalogue.library_connector.page.completed",
            "catalogue.library_connector.collection.completed",
        }
    }
    assert set(correlated) == {
        "collection.started",
        "request.started",
        "catalogue.library_connector.collection.started",
        "catalogue.library_connector.page.completed",
        "catalogue.library_connector.collection.completed",
    }
    assert {fields["collection_id"] for fields in correlated.values()} == {"job-1"}
    assert correlated["catalogue.library_connector.collection.started"]["level"] == "info"
    assert correlated["catalogue.library_connector.page.completed"]["level"] == "debug"
    assert correlated["catalogue.library_connector.collection.completed"]["level"] == "info"


@pytest.mark.asyncio
async def test_shopify_canary_composes_registry_without_duplicate_transport_calls():
    config = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "shopify",
                "delay": 0.25,
                "obey_robots": True,
            }
        }
    )["shop"]
    source = source_definition("shop", config)
    library_request = LibraryCollectionRequest(
        source_id="shop",
        base_url=config.url,
        requested_fields=frozenset({LibrarySnapshotField.IDENTITY}),
    )
    pipeline_request = CollectionRequest(
        source_id="shop",
        base_url=config.url,
        refresh_mode=RefreshMode.FULL,
        requested_fields=frozenset({SnapshotField.IDENTITY}),
    )
    fetcher = ShopifyFetcher()

    connector = build_library_pipeline_connector(
        registry=LibraryConnectorRegistry.with_builtins(),
        source=source,
        request=library_request,
        checkpoint=None,
        fetcher=fetcher,
        cancelled=lambda: False,
    )
    await apply_library_fetch_policy(fetcher, config, connector)
    pages = [page async for page in connector.collect(pipeline_request)]

    assert connector.name == "shopify"
    assert connector.version == LibraryConnectorRegistry.with_builtins().connector_version(
        "shopify"
    )
    assert fetcher.calls == [
        "https://shop.test/meta.json",
        "https://shop.test/products.json",
    ]
    assert fetcher.limiter.groups == [(config.url, "edge:shopify")]
    assert fetcher.limiter.delays == [(config.url, 0.25)]
    assert fetcher.policy_calls == [(config.url, False, True)]
    assert len(pages) == 1
    assert pages[0].terminal


@pytest.mark.asyncio
async def test_library_policy_denies_robots_before_connector_network_calls():
    config = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "shopify",
                "obey_robots": True,
            }
        }
    )["shop"]
    source = source_definition("shop", config)
    request = LibraryCollectionRequest(
        source_id="shop",
        base_url=config.url,
        requested_fields=frozenset({LibrarySnapshotField.IDENTITY}),
    )
    fetcher = ShopifyFetcher()
    fetcher.allowed = False
    connector = build_library_pipeline_connector(
        registry=LibraryConnectorRegistry.with_builtins(),
        source=source,
        request=request,
        checkpoint=None,
        fetcher=fetcher,
        cancelled=lambda: False,
    )

    with pytest.raises(RuntimeError, match=r"robots\.txt disallows"):
        await apply_library_fetch_policy(fetcher, config, connector)

    assert fetcher.calls == []
    assert fetcher.policy_calls == [(config.url, False, True)]


def test_limited_connector_outcome_can_never_authorize_retirement():
    from mb_ceramics_catalogue.ops.worker import _connector_load_is_whole

    limited = PipelineResult(
        pages=1,
        terminal=True,
        enumeration_intact=False,
        limited=True,
        datasets={},
    )
    assert not _connector_load_is_whole(limited)


def test_canary_adapters_and_price_refresh_are_capability_driven():
    assert scrapers.adapter_capabilities("woocommerce").canary_adapter == (
        "woocommerce_connector"
    )
    assert scrapers.adapter_capabilities("bigcommerce_connector").price_refresh
    assert scrapers.adapter_capabilities("wix").canary_adapter == "wix_connector"
    assert scrapers.adapter_capabilities("pagecrawl").canary_adapter == "pagecrawl_connector"
    assert scrapers.adapter_capabilities("ceramicolours").required_worker_capabilities == (
        "browser",
    )
    assert scrapers.adapter_capabilities("shopify").required_worker_capabilities == ()


def test_dataset_selection_is_explicit_validated_and_ordered():
    from pydantic import ValidationError

    from mb_ceramics_catalogue.config.settings import CrawlParams

    params = CrawlParams.model_validate({
        "pipeline": "connector_canary",
        "datasets": ["commerce.stock_observation.v1", "commerce.price_observation.v1"]
    })
    assert params.datasets == (
        "commerce.stock_observation.v1", "commerce.price_observation.v1"
    )
    with pytest.raises(ValidationError, match="duplicates"):
        CrawlParams.model_validate({"datasets": ["ceramics", "ceramics"]})
    with pytest.raises(ValidationError):
        CrawlParams.model_validate({"datasets": ["unknown.v1"]})
