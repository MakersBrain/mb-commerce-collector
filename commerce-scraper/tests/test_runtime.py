from __future__ import annotations

import asyncio

import pytest

from mb_commerce_scraper import SourceDefinition
from mb_commerce_scraper.connectors import ConnectorRegistry
from mb_commerce_scraper.proxy import ProxyLease, ProxyRouting
from mb_commerce_scraper.runtime import CommerceScraper
from mb_commerce_scraper.testing import FakeTransport, fake_proxy_pool
from mb_commerce_scraper.transports import (
    CommerceTransport,
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
