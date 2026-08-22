"""Clean consumer exercise for connector and proxy extension contracts."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from pydantic import JsonValue, SecretStr

import mb_commerce_scraper
from mb_commerce_scraper import (
    FetchPolicy,
    RobotsPolicy,
    SnapshotField,
    SourceDefinition,
)
from mb_commerce_scraper.connectors import ConnectorRegistry
from mb_commerce_scraper.proxy import (
    ProxyCredentials,
    ProxyEndpoint,
    ProxyKind,
    ProxyLease,
    ProxyRouting,
    RoutingMode,
    StaticProxyPool,
    StaticRoute,
)
from mb_commerce_scraper.runtime import CommerceScraper
from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    CommerceTransport,
    RouteMetadata,
    TransportAccounting,
    TransportRequest,
    TransportResponse,
)


class RecordingTelemetry:
    """Minimal application-owned telemetry sink."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, JsonValue]]] = []

    def emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        self.events.append((event, fields))


class ExampleProxyTransport:
    """Consumer backend bound to one lease without retaining credentials."""

    def __init__(self, factory: ExampleProxyTransportFactory, lease: ProxyLease) -> None:
        self._factory = factory
        self._provider = lease.provider
        self._endpoint_id = lease.route.endpoint_id
        self.closed = False
        self.close_calls = 0

    async def request(self, request: TransportRequest) -> TransportResponse:
        if self.closed:
            raise RuntimeError("example proxy transport is closed")
        self._factory.requests.append((self._provider, request.url))
        if self._factory.fail_next:
            self._factory.fail_next = False
            return self._response(request, status=429, payload={"error": "rate limited"})
        if request.url.endswith("/products.json"):
            payload: Any = {
                "products": [
                    {
                        "id": 101,
                        "handle": "wheel-cup",
                        "title": "Wheel cup",
                        "variants": [{"id": 102, "price": "11.00", "available": True}],
                    }
                ]
            }
        elif request.url.endswith("/catalog.json"):
            payload = {
                "products": [
                    {
                        "id": "cup-1",
                        "title": "Plugin cup",
                        "path": "/products/plugin-cup",
                        "price": "12.50",
                    }
                ]
            }
        else:
            raise AssertionError(f"unexpected proxy request: {request.url}")
        return self._response(request, status=200, payload=payload)

    async def aclose(self) -> None:
        self.close_calls += 1
        self.closed = True

    def _response(self, request: TransportRequest, *, status: int, payload: Any) -> TransportResponse:
        content = json.dumps(payload, separators=(",", ":")).encode()
        return TransportResponse(
            status=status,
            content=content,
            final_url=request.url,
            route=RouteMetadata(
                kind="proxy",
                provider=self._provider,
                endpoint_id=self._endpoint_id,
            ),
            accounting=TransportAccounting(
                physical_requests=1,
                transmitted_bytes=128,
                received_bytes=len(content),
            ),
        )


class ExampleProxyTransportFactory:
    """External factory proving the public ``ProxyTransportFactory`` seam."""

    def __init__(self, credential_digest: str) -> None:
        self._credential_digest = credential_digest
        self.transports: list[ExampleProxyTransport] = []
        self.requests: list[tuple[str, str]] = []
        self.fail_next = True

    def build(self, lease: ProxyLease) -> CommerceTransport:
        credentials = lease.http_credentials()
        digest = _credential_digest(
            credentials.username.get_secret_value(),
            credentials.password.get_secret_value(),
        )
        if digest != self._credential_digest:
            raise RuntimeError("proxy lease supplied unexpected credentials")
        transport = ExampleProxyTransport(self, lease)
        self.transports.append(transport)
        return transport


def _credential_digest(username: str, password: str) -> str:
    return hashlib.sha256(f"{username}\0{password}".encode()).hexdigest()


def _assert_library_is_installed_outside_consumer() -> None:
    package_path = Path(mb_commerce_scraper.__file__).resolve()
    assert Path(__file__).resolve().parent not in package_path.parents


async def _collect(
    scraper: CommerceScraper,
    source: SourceDefinition,
    *,
    requested_fields: frozenset[SnapshotField] = frozenset(SnapshotField),
) -> list[Any]:
    return [
        page
        async for page in scraper.collect(
            source,
            requested_fields=requested_fields,
            collection_id=f"external-{source.id}",
        )
    ]


async def main() -> None:
    _assert_library_is_installed_outside_consumer()
    username = os.environ["EXAMPLE_PROXY_USERNAME"]
    password = os.environ["EXAMPLE_PROXY_PASSWORD"]
    digest = _credential_digest(username, password)
    routes = tuple(
        StaticRoute(
            endpoint=ProxyEndpoint(
                provider=f"example-{index}",
                endpoint_id=f"route-{index}",
                protocol="http",
                host="proxy.invalid",
                port=8_000 + index,
                kind=ProxyKind.RESIDENTIAL,
            ),
            credentials=ProxyCredentials(
                username=SecretStr(username),
                password=SecretStr(password),
            ),
        )
        for index in (1, 2)
    )
    del username, password

    pool = StaticProxyPool(routes)
    proxy_factory = ExampleProxyTransportFactory(digest)
    telemetry = RecordingTelemetry()
    registry = ConnectorRegistry.with_builtins()
    assert registry.load_entry_points(strict=True) == ()
    assert "shopify" in registry.names()
    assert "example-feed" in registry.names()

    direct = FakeTransport()
    scraper = CommerceScraper(
        registry=registry,
        transport=direct,
        proxy_pool=pool,
        routing=ProxyRouting(mode=RoutingMode.ALWAYS),
        proxy_transport_factory=proxy_factory,
        fetch_policy=FetchPolicy(robots=RobotsPolicy.IGNORE),
        telemetry=telemetry,
        retries=1,
        backoff=lambda _attempt: 0,
    )
    async with scraper:
        shopify_pages = await _collect(
            scraper,
            SourceDefinition(
                id="builtin",
                label="Built-in connector",
                base_url="https://shop.test",
                connector="shopify",
                connector_options={"currency": "EUR"},
            ),
        )
        plugin_pages = await _collect(
            scraper,
            SourceDefinition(
                id="plugin",
                label="Packaged plugin",
                base_url="https://shop.test",
                connector="example-feed",
                connector_options={"currency": "EUR"},
            ),
            requested_fields=frozenset(
                {SnapshotField.IDENTITY, SnapshotField.VARIANTS, SnapshotField.OFFERS}
            ),
        )

    assert shopify_pages[-1].terminal
    assert shopify_pages[0].items[0].title == "Wheel cup"
    assert plugin_pages[-1].terminal
    assert plugin_pages[0].items[0].title == "Plugin cup"
    assert [provider for provider, _url in proxy_factory.requests[:2]] == [
        "example-1",
        "example-2",
    ]
    assert [name for name, _fields in telemetry.events].count("request.retry") == 1
    assert direct.requests == []
    assert all(transport.closed for transport in proxy_factory.transports)
    assert all(transport.close_calls == 1 for transport in proxy_factory.transports)
    assert pool.active_leases == 0

    retained = repr(
        (
            routes,
            pool,
            proxy_factory,
            telemetry.events,
            shopify_pages,
            plugin_pages,
        )
    )
    assert os.environ["EXAMPLE_PROXY_USERNAME"] not in retained
    assert os.environ["EXAMPLE_PROXY_PASSWORD"] not in retained


asyncio.run(main())
