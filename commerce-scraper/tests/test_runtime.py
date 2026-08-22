from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from pydantic import BaseModel, ConfigDict, JsonValue

from mb_commerce_scraper import (
    BrowserPolicy,
    CollectionRequest,
    CommerceProductSnapshot,
    CommerceVariant,
    ConnectorCheckpoint,
    Diagnostic,
    DiagnosticCode,
    DiagnosticSeverity,
    EntityPage,
    FetchPolicy,
    ProxyMode,
    ProxyPolicyConfig,
    RefreshMode,
    RobotsPolicy,
    SnapshotField,
    SourceDefinition,
)
from mb_commerce_scraper.connectors import (
    CommerceConnector,
    ConnectorCapabilities,
    ConnectorContext,
    ConnectorRegistry,
)
from mb_commerce_scraper.proxy import (
    BrowserSubrequestAuthorizer,
    HttpxProxyTransportFactory,
    ProxyLease,
    ProxyRequest,
    ProxyRouting,
    RoutingMode,
    StaticProxyLease,
)
from mb_commerce_scraper.runtime import CommerceScraper, build_http_scraper
from mb_commerce_scraper.testing import FakeTransport, fake_proxy_pool
from mb_commerce_scraper.transports import (
    BrowserBackendUnavailable,
    BudgetExhausted,
    CommerceTransport,
    MemoryRequestBudget,
    MemoryResponseCache,
    ProxyBrowserRoutingUnsupported,
    RequestPriority,
    RequestPurpose,
    RobotsDenied,
    RotationReason,
    TransportRequest,
    TransportResponse,
)
from mb_commerce_scraper.transports.httpx import HttpxTransport


class Factory:
    def __init__(self, transport: CommerceTransport) -> None:
        self.transport = transport

    def build(self, lease: ProxyLease) -> CommerceTransport:
        del lease
        return self.transport


class LeaseBrowserFactory:
    def __init__(self, transport: CommerceTransport) -> None:
        self.transport = transport
        self.leases: list[ProxyLease] = []

    def build(
        self,
        lease: ProxyLease,
        authorizer: BrowserSubrequestAuthorizer,
    ) -> CommerceTransport:
        del authorizer
        self.leases.append(lease)
        return self.transport


def test_http_builder_projects_one_response_limit_to_every_runtime_layer() -> None:
    proxy_policy = ProxyPolicyConfig(
        mode=ProxyMode.ALWAYS,
        country="FR",
        provider_preferences=("one",),
        maximum_requests=7,
        maximum_bytes=8_000,
    )
    scraper = build_http_scraper(
        allowed_origins=("https://shop.test",),
        proxy_pool=fake_proxy_pool("one"),
        proxy_policy=proxy_policy,
        maximum_response_bytes=1234,
    )

    assert scraper.maximum_response_bytes == 1234
    assert isinstance(scraper.transport, HttpxTransport)
    assert scraper.transport._maximum_response_bytes == 1234
    assert isinstance(scraper.proxy_transport_factory, HttpxProxyTransportFactory)
    assert scraper.proxy_transport_factory.maximum_response_bytes == 1234
    assert scraper.proxy_policy == proxy_policy
    assert scraper.routing == ProxyRouting(
        mode=RoutingMode.ALWAYS,
        country="FR",
        provider_preferences=("one",),
    )
    assert scraper.proxy_maximum_requests == 7
    assert scraper.proxy_maximum_bytes == 8_000


@pytest.mark.parametrize(
    "legacy",
    [
        {"routing": ProxyRouting(mode=RoutingMode.ALWAYS)},
        {"proxy_maximum_requests": 1},
        {"proxy_maximum_bytes": 1},
    ],
)
def test_runtime_rejects_mixed_canonical_and_legacy_proxy_configuration(
    legacy: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="cannot be combined"):
        CommerceScraper(
            registry=ConnectorRegistry.with_builtins(),
            transport=FakeTransport(),
            proxy_policy=ProxyPolicyConfig(),
            **legacy,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "legacy",
    [{"proxy_maximum_requests": 1}, {"proxy_maximum_bytes": 1}],
)
def test_runtime_rejects_inert_legacy_proxy_caps(
    legacy: dict[str, int],
) -> None:
    with pytest.raises(ValueError, match="require active proxy routing"):
        CommerceScraper(
            registry=ConnectorRegistry.with_builtins(),
            transport=FakeTransport(),
            **legacy,  # type: ignore[arg-type]
        )


def test_runtime_rejects_active_policy_without_proxy_backend() -> None:
    with pytest.raises(ValueError, match="active proxy_policy requires"):
        CommerceScraper(
            registry=ConnectorRegistry.with_builtins(),
            transport=FakeTransport(),
            proxy_policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
        )


async def test_never_proxy_policy_keeps_configured_backend_idle() -> None:
    pool = fake_proxy_pool("one")
    direct = FakeTransport()
    direct.add(
        "https://shop.test/products.json",
        json_body={"products": []},
    )
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=direct,
        proxy_pool=pool,
        proxy_policy=ProxyPolicyConfig(mode=ProxyMode.NEVER),
        proxy_transport_factory=Factory(FakeTransport()),
    )

    pages = [page async for page in scraper.collect(source())]

    assert pages[-1].terminal
    assert len(direct.requests) == 1
    assert pool.active_leases == 0


async def test_collection_never_policy_overrides_active_default() -> None:
    pool = fake_proxy_pool("one")
    direct = FakeTransport()
    direct.add(
        "https://shop.test/products.json",
        json_body={"products": []},
    )
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=direct,
        proxy_pool=pool,
        proxy_policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
        proxy_transport_factory=Factory(FakeTransport()),
    )

    pages = [
        page
        async for page in scraper.collect(
            source(),
            proxy_policy=ProxyPolicyConfig(mode=ProxyMode.NEVER),
        )
    ]

    assert pages[-1].terminal
    assert len(direct.requests) == 1
    assert pool.active_leases == 0


async def test_collection_proxy_policy_projects_all_fields_to_pool_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = fake_proxy_pool("one")
    requests: list[ProxyRequest] = []
    acquire = pool.acquire

    async def recording_acquire(request: ProxyRequest) -> ProxyLease:
        requests.append(request)
        return await acquire(request)

    monkeypatch.setattr(pool, "acquire", recording_acquire)
    proxy = FakeTransport()
    proxy.add(
        "https://shop.test/products.json",
        json_body={"products": []},
    )
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=FakeTransport(),
        proxy_pool=pool,
        proxy_transport_factory=Factory(proxy),
    )
    policy = ProxyPolicyConfig(
        mode=ProxyMode.ALWAYS,
        country="FR",
        provider_preferences=("one",),
        maximum_requests=7,
        maximum_bytes=8_000_000,
    )

    pages = [page async for page in scraper.collect(source(), proxy_policy=policy)]

    assert pages[-1].terminal
    assert len(requests) == 1
    assert requests[0].country == "FR"
    assert requests[0].preferred_providers == ("one",)
    assert requests[0].maximum_requests == 7
    assert requests[0].maximum_bytes == 8_000_000
    assert pool.active_leases == 0


def test_http_builder_enables_browser_policy_for_proxy_browser_factory() -> None:
    browser_factory = LeaseBrowserFactory(FakeTransport())

    scraper = build_http_scraper(
        allowed_origins=("https://shop.test",),
        proxy_pool=fake_proxy_pool("one"),
        routing=ProxyRouting(mode=RoutingMode.ALWAYS),
        proxy_browser_transport_factory=browser_factory,
    )

    assert scraper.proxy_browser_transport_factory is browser_factory
    assert scraper.fetch_policy is not None
    assert scraper.fetch_policy.browser is BrowserPolicy.ALLOW


async def test_runtime_can_require_physical_proxy_browser_authorization() -> None:
    pool = fake_proxy_pool("one")
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=FakeTransport(),
        proxy_pool=pool,
        routing=ProxyRouting(mode=RoutingMode.ALWAYS),
        proxy_transport_factory=Factory(FakeTransport()),
        proxy_browser_transport_factory=LeaseBrowserFactory(FakeTransport()),
        require_proxy_browser_subrequest_authorization=True,
    )

    with pytest.raises(
        ProxyBrowserRoutingUnsupported,
        match="authorize every physical subrequest",
    ):
        async with scraper.open_connector(source()):
            pytest.fail("an unmarked browser factory must not enter composition")
    assert pool.active_leases == 0


class UnsafeOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class UnsafeConnector:
    name = "unsafe-plugin"
    platform = "unsafe"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
    )

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        del checkpoint
        secret = "unsafe-plugin-secret"
        variant = CommerceVariant.model_construct(
            external_id="variant-1",
            platform_extensions={"access_token": secret},
        )
        snapshot = CommerceProductSnapshot.model_construct(
            contract_version="commerce.product_snapshot.v1",
            connector="unsafe-plugin",
            source_id=request.source_id,
            external_id="product-1",
            canonical_url=f"{request.base_url}/product-1",
            title="Clay",
            observed_at=datetime.now(UTC),
            variants=(variant,),
            platform_extensions={"proxy": {"password": secret}},
        )
        yield EntityPage(
            page_id="unsafe:0",
            sequence=0,
            items=(snapshot,),
            terminal=True,
            discovered=1,
            diagnostics=(
                Diagnostic.model_construct(
                    code=DiagnosticCode.ENTITY_FETCH_FAILED,
                    severity=DiagnosticSeverity.ERROR,
                    message=(f"unsafe Authorization: Bearer {secret} " + "x" * 4_000),
                    retryable=True,
                    affects_completeness=False,
                    url=f"https://user:{secret}@shop.test?token={secret}",
                    metadata={"raw_error": {"password": secret}, "stage": "plugin"},
                ),
            ),
        )


class UnsafeFactory:
    name = "unsafe-plugin"
    version = UnsafeConnector.version
    options_model: type[BaseModel] = UnsafeOptions

    def build(
        self,
        *,
        transport: CommerceTransport,
        options: BaseModel,
        context: ConnectorContext,
    ) -> CommerceConnector:
        del transport, options, context
        return UnsafeConnector()


class OpenConnector:
    name = "open-test"
    platform = "test-platform"
    version = "7"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
    )

    def __init__(self, context: ConnectorContext) -> None:
        self.context = context
        self.calls: list[tuple[CollectionRequest, ConnectorCheckpoint | None]] = []

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        self.calls.append((request, checkpoint))
        self.context.telemetry.emit(
            "connector.protocol",
            {
                "level": "debug",
                "source_id": "plugin-cannot-override-runtime-identity",
                "operation": "enumerate",
            },
        )
        yield EntityPage(
            page_id="open:0",
            sequence=0,
            items=(),
            terminal=True,
            discovered=0,
        )


class OpenConnectorFactory:
    name = OpenConnector.name
    version = OpenConnector.version
    options_model: type[BaseModel] = UnsafeOptions

    def __init__(self) -> None:
        self.connector: OpenConnector | None = None

    def build(
        self,
        *,
        transport: CommerceTransport,
        options: BaseModel,
        context: ConnectorContext,
    ) -> CommerceConnector:
        del transport, options
        self.connector = OpenConnector(context)
        return self.connector


def source() -> SourceDefinition:
    return SourceDefinition(
        id="shop",
        label="Shop",
        base_url="https://shop.test",
        connector="shopify",
        connector_options={"currency": "EUR"},
    )


def bigcommerce_source() -> SourceDefinition:
    return SourceDefinition(
        id="big",
        label="Big",
        base_url="https://shop.test",
        connector="bigcommerce",
    )


def storefront_token() -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').decode().rstrip("=")
    claims = (
        base64.urlsafe_b64encode(json.dumps({"cors": ["https://shop.test"]}).encode()).decode().rstrip("=")
    )
    return f"{header}.{claims}.{'x' * 40}"


def empty_graphql_page() -> dict[str, object]:
    return {
        "data": {
            "site": {
                "products": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "edges": [],
                }
            }
        }
    }


class RecordingTelemetry:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, JsonValue]]] = []

    def emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        self.events.append((event, fields))


class RecordingLimiter:
    def __init__(self) -> None:
        self.waited: list[TransportRequest] = []
        self.released: list[TransportRequest] = []

    async def wait(self, request: TransportRequest) -> None:
        self.waited.append(request)

    async def release(self, request: TransportRequest) -> None:
        self.released.append(request)


async def test_open_connector_preserves_metadata_and_accepts_caller_request() -> None:
    registry = ConnectorRegistry()
    factory = OpenConnectorFactory()
    registry.register(factory)
    telemetry = RecordingTelemetry()
    scraper = CommerceScraper(
        registry=registry,
        transport=FakeTransport(),
        telemetry=telemetry,
    )
    selected_source = SourceDefinition(
        id="open-source",
        label="Open source",
        base_url="https://shop.test",
        connector="open-test",
    )
    request = CollectionRequest(
        source_id="caller-source",
        base_url="https://caller.test",
        partitions=("summer",),
        result_limit=3,
    )
    checkpoint = ConnectorCheckpoint(
        connector="open-test",
        connector_version="7",
        source_id="caller-source",
        lineage="lineage-1",
        collection_fingerprint="a" * 64,
        resume_after={"page": 2},
    )
    cancelled = True

    async with scraper.open_connector(
        selected_source,
        collection_id="open-123",
        cancelled=lambda: cancelled,
    ) as connector:
        assert connector.name == "open-test"
        assert connector.platform == "test-platform"
        assert connector.version == "7"
        assert connector.capabilities is OpenConnector.capabilities
        pages = [page async for page in connector.collect(request, checkpoint)]

    assert pages[0].terminal
    assert factory.connector is not None
    assert factory.connector.calls == [(request, checkpoint)]
    assert factory.connector.context.cancelled()
    [protocol] = [fields for event, fields in telemetry.events if event == "connector.protocol"]
    assert protocol == {
        "level": "debug",
        "source_id": "open-source",
        "operation": "enumerate",
        "collection_id": "open-123",
        "connector": "open-test",
        "connector_version": "7",
    }
    assert [event for event, _ in telemetry.events][-2:] == [
        "connector.page_emitted",
        "collection.completed",
    ]
    assert {fields["collection_id"] for _, fields in telemetry.events if "collection_id" in fields} == {
        "open-123"
    }


async def test_open_connector_keeps_sanitization_and_deadline_boundary() -> None:
    registry = ConnectorRegistry()
    registry.register(UnsafeFactory())
    telemetry = RecordingTelemetry()
    scraper = CommerceScraper(
        registry=registry,
        transport=FakeTransport(),
        telemetry=telemetry,
    )
    unsafe = SourceDefinition(
        id="unsafe",
        label="Unsafe plugin",
        base_url="https://shop.test",
        connector="unsafe-plugin",
    )

    async with scraper.open_connector(unsafe) as connector:
        [page] = [
            page
            async for page in connector.collect(
                CollectionRequest(
                    source_id="unsafe",
                    base_url="https://shop.test",
                )
            )
        ]
        with pytest.raises(ValueError, match="timezone"):
            _ = [
                item
                async for item in connector.collect(
                    CollectionRequest(
                        source_id="unsafe",
                        base_url="https://shop.test",
                        deadline=datetime.now(),
                    )
                )
            ]

    assert "unsafe-plugin-secret" not in page.model_dump_json()
    assert [event for event, _ in telemetry.events].count("collection.completed") == 1
    interrupted = [fields for event, fields in telemetry.events if event == "collection.interrupted"]
    assert interrupted[-1]["error_type"] == "ValueError"


async def test_open_connector_releases_routed_lease_exactly_once() -> None:
    pool = fake_proxy_pool("one")
    releases = 0
    release = pool.release

    async def counted_release(lease: ProxyLease) -> None:
        nonlocal releases
        releases += 1
        await release(lease)

    pool.release = counted_release  # type: ignore[method-assign]
    proxy = FakeTransport()
    proxy.add("https://shop.test/products.json", json_body={"products": []})
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=FakeTransport(),
        proxy_pool=pool,
        routing=ProxyRouting(mode=RoutingMode.ALWAYS),
        proxy_transport_factory=Factory(proxy),
    )

    async with scraper.open_connector(source()) as connector:
        pages = [
            page
            async for page in connector.collect(
                CollectionRequest(
                    source_id="shop",
                    base_url="https://shop.test",
                )
            )
        ]
        assert pages[-1].terminal
        assert pool.active_leases == 1

    assert releases == 1
    assert pool.active_leases == 0


async def test_runtime_releases_proxy_lease_on_success_and_failure() -> None:
    pool = fake_proxy_pool("one")
    direct = FakeTransport()
    direct.add("https://shop.test/products.json", status=403)
    proxy = FakeTransport()
    proxy.add("https://shop.test/products.json", json_body={"products": []})
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=direct,
        proxy_pool=pool,
        routing=ProxyRouting.fallback(),
        proxy_transport_factory=Factory(proxy),
    )
    assert [page async for page in scraper.collect(source())][-1].terminal
    assert pool.active_leases == 0

    failing_direct = FakeTransport()
    failing_direct.add("https://shop.test/products.json", status=403)
    failing = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=failing_direct,
        proxy_pool=pool,
        routing=ProxyRouting.fallback(),
        proxy_transport_factory=Factory(FakeTransport()),
    )
    with pytest.raises(RuntimeError, match="no fake response"):
        _ = [page async for page in failing.collect(source())]
    assert pool.active_leases == 0


async def test_runtime_sanitizes_unvalidated_plugin_extensions_at_egress() -> None:
    registry = ConnectorRegistry()
    registry.register(UnsafeFactory())
    telemetry = RecordingTelemetry()
    scraper = CommerceScraper(
        registry=registry,
        transport=FakeTransport(),
        telemetry=telemetry,
    )
    unsafe = SourceDefinition(
        id="unsafe",
        label="Unsafe plugin",
        base_url="https://shop.test",
        connector="unsafe-plugin",
    )

    [page] = [page async for page in scraper.collect(unsafe)]

    dumped = page.model_dump_json()
    assert "unsafe-plugin-secret" not in dumped
    assert page.items[0].platform_extensions == {"proxy": {"password": "[redacted]"}}
    assert page.items[0].variants[0].platform_extensions == {"access_token": "[redacted]"}
    assert len(page.diagnostics[0].message) <= 2_048
    assert page.diagnostics[0].code is DiagnosticCode.ENTITY_FETCH_FAILED
    assert page.diagnostics[0].metadata["stage"] == "plugin"
    diagnostic = next(fields for event, fields in telemetry.events if event == "connector.diagnostic")
    assert diagnostic["level"] == "error"


class BlockingTransport:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def request(self, request: TransportRequest) -> TransportResponse:
        del request
        self.started.set()
        await asyncio.Future()
        raise AssertionError("unreachable")

    async def rotate_identity(self, reason: RotationReason) -> None:
        del reason


async def test_runtime_releases_proxy_lease_on_cancellation() -> None:
    pool = fake_proxy_pool("one")
    blocking = BlockingTransport()
    direct = FakeTransport()
    direct.add("https://shop.test/products.json", status=403)
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=direct,
        proxy_pool=pool,
        routing=ProxyRouting.fallback(),
        proxy_transport_factory=Factory(blocking),
    )

    async def consume() -> None:
        _ = [page async for page in scraper.collect(source())]

    task = asyncio.create_task(consume())
    await blocking.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert pool.active_leases == 0


async def test_runtime_enforces_shared_budget_for_connectors_without_preflight() -> None:
    backend = FakeTransport()
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=backend,
        budget=MemoryRequestBudget(maximum_requests=0),
    )
    bigcommerce = SourceDefinition(
        id="big",
        label="Big",
        base_url="https://shop.test",
        connector="bigcommerce",
    )

    with pytest.raises(BudgetExhausted):
        _ = [page async for page in scraper.collect(bigcommerce)]
    assert backend.requests == []


async def test_per_call_robots_policy_overrides_default_and_denies_before_entity() -> None:
    backend = FakeTransport()
    backend.add(
        "https://shop.test/robots.txt",
        body="User-agent: *\nDisallow: /products.json\n",
    )
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=backend,
        fetch_policy=FetchPolicy(robots=RobotsPolicy.IGNORE),
    )

    with pytest.raises(RobotsDenied):
        _ = [
            page
            async for page in scraper.collect(source(), fetch_policy=FetchPolicy(robots=RobotsPolicy.OBEY))
        ]

    assert [request.purpose for request in backend.requests] == [RequestPurpose.ROBOTS]


async def test_per_call_ignore_policy_skips_default_robots_fetch() -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/products.json", json_body={"products": []})
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=backend,
        fetch_policy=FetchPolicy(robots=RobotsPolicy.OBEY),
    )

    pages = [
        page async for page in scraper.collect(source(), fetch_policy=FetchPolicy(robots=RobotsPolicy.IGNORE))
    ]

    assert pages[-1].terminal
    assert [request.purpose for request in backend.requests] == [RequestPurpose.DISCOVERY]


async def test_runtime_composes_policy_limiter_and_correlated_telemetry() -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/robots.txt", body="User-agent: *\nAllow: /\n")
    backend.add("https://shop.test/products.json", json_body={"products": []})
    telemetry = RecordingTelemetry()
    limiter = RecordingLimiter()
    selected_policies: list[FetchPolicy] = []

    def limiter_factory(policy: FetchPolicy) -> RecordingLimiter:
        selected_policies.append(policy)
        return limiter

    policy = FetchPolicy(delay=0.125, concurrency=3, robots=RobotsPolicy.OBEY)
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=backend,
        telemetry=telemetry,
        fetch_policy=policy,
        rate_limiter_factory=limiter_factory,
    )

    pages = [page async for page in scraper.collect(source(), collection_id="collection-123")]

    assert pages[-1].terminal
    assert selected_policies == [policy]
    assert [request.purpose for request in limiter.waited] == [
        RequestPurpose.ROBOTS,
        RequestPurpose.DISCOVERY,
    ]
    assert limiter.released == limiter.waited
    event_names = [name for name, _ in telemetry.events]
    assert "request.completed" in event_names
    assert "connector.page_emitted" in event_names
    assert event_names[-1] == "collection.completed"
    assert {fields["collection_id"] for _, fields in telemetry.events if "collection_id" in fields} == {
        "collection-123"
    }
    assert {
        fields["connector_version"] for _, fields in telemetry.events if "connector_version" in fields
    } == {"1"}


async def test_fallback_uses_independent_direct_and_proxy_rate_gates() -> None:
    direct = FakeTransport()
    direct.add("https://shop.test/products.json", status=429)
    proxy = FakeTransport()
    proxy.add("https://shop.test/products.json", json_body={"products": []})
    telemetry = RecordingTelemetry()
    limiters: list[RecordingLimiter] = []

    def limiter_factory(policy: FetchPolicy) -> RecordingLimiter:
        del policy
        limiter = RecordingLimiter()
        limiters.append(limiter)
        return limiter

    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=direct,
        proxy_pool=fake_proxy_pool("one"),
        routing=ProxyRouting.fallback(),
        proxy_transport_factory=Factory(proxy),
        fetch_policy=FetchPolicy(robots=RobotsPolicy.IGNORE),
        telemetry=telemetry,
        rate_limiter_factory=limiter_factory,
        retries=1,
        backoff=lambda _: 0,
    )

    pages = [page async for page in scraper.collect(source())]

    assert pages[-1].terminal
    assert len(limiters) == 2
    assert sorted(len(limiter.waited) for limiter in limiters) == [1, 1]
    assert all(limiter.released == limiter.waited for limiter in limiters)
    route_waits = {fields["route"] for event, fields in telemetry.events if event == "rate_limit.wait"}
    assert route_waits == {"direct", "proxy"}
    started = [fields for event, fields in telemetry.events if event == "request.started"]
    assert [fields["attempt"] for fields in started] == [1, 2]
    assert len({fields["request_id"] for fields in started}) == 1
    request_id = started[0]["request_id"]
    correlated = [
        (event, fields)
        for event, fields in telemetry.events
        if event
        in {
            "browser.dispatch",
            "rate_limit.wait",
            "proxy.acquire.started",
            "proxy.acquire.completed",
            "proxy.outcome",
        }
    ]
    assert correlated
    assert {fields["request_id"] for _, fields in correlated} == {request_id}
    assert {fields["attempt"] for event, fields in correlated if event == "proxy.outcome"} == {2}
    assert {fields["attempt"] for event, fields in correlated if event.startswith("proxy.acquire")} == {1}
    assert len(direct.requests) == len(proxy.requests) == 1


async def test_fallback_rejects_a_shared_direct_proxy_limiter() -> None:
    shared = RecordingLimiter()
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=FakeTransport(),
        proxy_pool=fake_proxy_pool("one"),
        routing=ProxyRouting.fallback(),
        proxy_transport_factory=Factory(FakeTransport()),
        fetch_policy=FetchPolicy(robots=RobotsPolicy.IGNORE),
        rate_limiter_factory=lambda _: shared,
    )

    with pytest.raises(ValueError, match="independent direct and proxy"):
        _ = [page async for page in scraper.collect(source())]


async def test_runtime_cache_hit_skips_network_budget_and_rate_gate() -> None:
    backend = FakeTransport()
    cache = MemoryResponseCache()
    cached_request = TransportRequest(
        url="https://shop.test/products.json",
        query={"limit": 250, "page": 1},
        purpose=RequestPurpose.DISCOVERY,
        priority=RequestPriority.DISCOVERY,
    )
    await cache.put(
        cached_request,
        TransportResponse(
            status=200,
            content=b'{"products": []}',
            final_url=cached_request.url,
        ),
    )
    budget = MemoryRequestBudget(maximum_requests=0)
    limiter = RecordingLimiter()
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=backend,
        fetch_policy=FetchPolicy(robots=RobotsPolicy.IGNORE),
        cache=cache,
        budget=budget,
        rate_limiter_factory=lambda _: limiter,
    )

    pages = [page async for page in scraper.collect(source())]

    assert pages[-1].terminal
    assert backend.requests == []
    assert budget.requests == 0
    assert limiter.waited == []


async def test_per_call_budget_takes_precedence_over_scraper_default() -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/products.json", json_body={"products": []})
    default_budget = MemoryRequestBudget(maximum_requests=0)
    call_budget = MemoryRequestBudget(maximum_requests=1)
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=backend,
        fetch_policy=FetchPolicy(robots=RobotsPolicy.IGNORE),
        budget=default_budget,
    )

    pages = [page async for page in scraper.collect(source(), budget=call_budget)]

    assert pages[-1].terminal
    assert default_budget.requests == 0
    assert call_budget.requests == 1


async def test_runtime_composes_borrowed_browser_for_required_requests() -> None:
    http = FakeTransport()
    browser = FakeTransport()
    http.add("https://shop.test", body="<html></html>")
    browser.add(
        "https://shop.test",
        body=f'local_token="{storefront_token()}"',
    )
    browser.add("https://shop.test/graphql", json_body=empty_graphql_page())
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=http,
        browser_transport=browser,
    )

    pages = [page async for page in scraper.collect(bigcommerce_source())]

    assert pages[-1].terminal
    assert [request.browser.value for request in http.requests] == ["never"]
    assert [request.browser.value for request in browser.requests] == [
        "required",
        "required",
    ]


async def test_runtime_browser_never_policy_blocks_required_dispatch() -> None:
    http = FakeTransport()
    browser = FakeTransport()
    http.add("https://shop.test", body="<html></html>")
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=http,
        browser_transport=browser,
        fetch_policy=FetchPolicy(
            robots=RobotsPolicy.IGNORE,
            browser=BrowserPolicy.NEVER,
        ),
    )

    with pytest.raises(BrowserBackendUnavailable, match="policy forbids"):
        _ = [page async for page in scraper.collect(bigcommerce_source())]

    assert [request.browser.value for request in http.requests] == ["never"]
    assert browser.requests == []


async def test_runtime_browser_require_policy_needs_configured_backend() -> None:
    backend = FakeTransport()
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=backend,
        fetch_policy=FetchPolicy(
            robots=RobotsPolicy.IGNORE,
            browser=BrowserPolicy.REQUIRE,
        ),
    )

    with pytest.raises(ValueError, match="requires a browser"):
        _ = [page async for page in scraper.collect(source())]

    assert backend.requests == []


async def test_runtime_fails_required_request_without_browser_backend() -> None:
    http = FakeTransport()
    http.add("https://shop.test", body="<html></html>")
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=http,
    )

    with pytest.raises(BrowserBackendUnavailable):
        _ = [page async for page in scraper.collect(bigcommerce_source())]


async def test_runtime_rejects_browser_bypass_of_active_proxy_routing() -> None:
    pool = fake_proxy_pool("one")
    http = FakeTransport()
    http.add("https://shop.test", body="<html></html>")
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=http,
        browser_transport=FakeTransport(),
        proxy_pool=pool,
        routing=ProxyRouting.fallback(),
        proxy_transport_factory=Factory(FakeTransport()),
    )

    with pytest.raises(ProxyBrowserRoutingUnsupported, match="active proxy lease"):
        _ = [page async for page in scraper.collect(bigcommerce_source())]
    assert pool.active_leases == 0


async def test_runtime_binds_browser_and_http_to_the_same_proxy_lease() -> None:
    pool = fake_proxy_pool("one")
    proxy_http = ClosingTransport()
    proxy_http.add("https://shop.test", body="<html></html>")
    proxy_browser = ClosingTransport()
    proxy_browser.add(
        "https://shop.test",
        body=f'local_token="{storefront_token()}"',
    )
    proxy_browser.add("https://shop.test/graphql", json_body=empty_graphql_page())
    browser_factory = LeaseBrowserFactory(proxy_browser)
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=FakeTransport(),
        proxy_pool=pool,
        routing=ProxyRouting(mode=RoutingMode.ALWAYS),
        proxy_transport_factory=Factory(proxy_http),
        proxy_browser_transport_factory=browser_factory,
    )

    pages = [page async for page in scraper.collect(bigcommerce_source())]

    assert pages[-1].terminal
    assert len(browser_factory.leases) == 1
    assert [request.browser.value for request in proxy_http.requests] == ["never"]
    assert [request.browser.value for request in proxy_browser.requests] == [
        "required",
        "required",
    ]
    # One HTTP request plus two calls through a custom browser factory that
    # does not advertise per-subrequest tokens retain outer authorization.
    assert isinstance(browser_factory.leases[0], StaticProxyLease)
    assert browser_factory.leases[0].used_requests == 3
    assert pool.active_leases == 0
    assert proxy_http.closed
    assert proxy_browser.closed


class ClosingTransport(FakeTransport):
    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


async def test_runtime_closes_browser_only_when_explicitly_owned() -> None:
    borrowed_http = ClosingTransport()
    borrowed_browser = ClosingTransport()
    async with CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=borrowed_http,
        browser_transport=borrowed_browser,
        owns_transport=True,
    ):
        pass
    assert borrowed_http.closed
    assert not borrowed_browser.closed

    owned_browser = ClosingTransport()
    async with CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=FakeTransport(),
        browser_transport=owned_browser,
        owns_browser_transport=True,
    ):
        pass
    assert owned_browser.closed
