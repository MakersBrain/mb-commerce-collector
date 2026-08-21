from __future__ import annotations

import asyncio
import base64
import json

import pytest

from mb_commerce_scraper import SourceDefinition
from mb_commerce_scraper.connectors import ConnectorRegistry
from mb_commerce_scraper.proxy import ProxyLease, ProxyRouting
from mb_commerce_scraper.runtime import CommerceScraper
from mb_commerce_scraper.testing import FakeTransport, fake_proxy_pool
from mb_commerce_scraper.transports import (
    BrowserBackendUnavailable,
    BudgetExhausted,
    CommerceTransport,
    MemoryRequestBudget,
    ProxyBrowserRoutingUnsupported,
    RotationReason,
    TransportRequest,
    TransportResponse,
)


class Factory:
    def __init__(self, transport: CommerceTransport) -> None:
        self.transport = transport

    def build(self, lease: ProxyLease) -> CommerceTransport:
        del lease
        return self.transport


def source() -> SourceDefinition:
    return SourceDefinition(id="shop", label="Shop", base_url="https://shop.test", connector="shopify", connector_options={"currency": "EUR"})


def bigcommerce_source() -> SourceDefinition:
    return SourceDefinition(
        id="big",
        label="Big",
        base_url="https://shop.test",
        connector="bigcommerce",
    )


def storefront_token() -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').decode().rstrip("=")
    claims = base64.urlsafe_b64encode(
        json.dumps({"cors": ["https://shop.test"]}).encode()
    ).decode().rstrip("=")
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


async def test_runtime_releases_proxy_lease_on_success_and_failure() -> None:
    pool = fake_proxy_pool("one")
    direct = FakeTransport()
    direct.add("https://shop.test/products.json", status=403)
    proxy = FakeTransport()
    proxy.add("https://shop.test/products.json", json_body={"products": []})
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(), transport=direct,
        proxy_pool=pool, routing=ProxyRouting.fallback(), proxy_transport_factory=Factory(proxy),
    )
    assert [page async for page in scraper.collect(source())][-1].terminal
    assert pool.active_leases == 0

    failing_direct = FakeTransport()
    failing_direct.add("https://shop.test/products.json", status=403)
    failing = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(), transport=failing_direct,
        proxy_pool=pool, routing=ProxyRouting.fallback(), proxy_transport_factory=Factory(FakeTransport()),
    )
    with pytest.raises(RuntimeError, match="no fake response"):
        _ = [page async for page in failing.collect(source())]
    assert pool.active_leases == 0


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
        registry=ConnectorRegistry.with_builtins(), transport=direct,
        proxy_pool=pool, routing=ProxyRouting.fallback(), proxy_transport_factory=Factory(blocking),
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
