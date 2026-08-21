from __future__ import annotations

from mb_commerce_scraper.proxy import (
    ProxyBudgetExhausted,
    ProxyLease,
    ProxyRouting,
    RoutedTransport,
    RoutingMode,
)
from mb_commerce_scraper.testing import FakeTransport, fake_proxy_pool
from mb_commerce_scraper.transports import RequestPriority, RequestPurpose, TransportRequest


class FakeProxyTransportFactory:
    def __init__(self, transports: dict[str, FakeTransport]) -> None:
        self.transports = transports

    def build(self, lease: ProxyLease) -> FakeTransport:
        return self.transports[lease.provider]


def request(*, estimated_bytes: int = 0) -> TransportRequest:
    return TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        estimated_bytes=estimated_bytes,
    )


async def test_fallback_stays_direct_after_success() -> None:
    direct = FakeTransport()
    direct.add("https://shop.test/data", body="direct")
    pool = fake_proxy_pool("one")
    proxy = FakeTransport()
    routed = RoutedTransport(
        direct,
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        routing=ProxyRouting.fallback(country="FR"),
        source_id="shop",
        base_url="https://shop.test",
    )
    response = await routed.request(request())
    assert response.text() == "direct"
    assert pool.active_leases == 0


async def test_fallback_acquires_sticky_proxy_only_for_typed_block() -> None:
    direct = FakeTransport()
    direct.add("https://shop.test/data", status=403)
    pool = fake_proxy_pool("one")
    proxy = FakeTransport()
    proxy.add("https://shop.test/data", body="proxy")
    proxy.add("https://shop.test/data", body="sticky")
    routed = RoutedTransport(
        direct,
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        routing=ProxyRouting.fallback(country="FR"),
        source_id="shop",
        base_url="https://shop.test",
    )
    first = await routed.request(request())
    second = await routed.request(request())
    assert first.route.provider == "one" and second.text() == "sticky"
    assert len(direct.requests) == 1
    await routed.aclose()
    assert pool.active_leases == 0


async def test_fallback_does_not_route_programming_errors_through_proxy() -> None:
    pool = fake_proxy_pool("one")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": FakeTransport()}),
        routing=ProxyRouting.fallback(country="FR"),
        source_id="shop",
        base_url="https://shop.test",
    )

    import pytest

    with pytest.raises(RuntimeError, match="no fake response"):
        await routed.request(request())
    assert pool.active_leases == 0


async def test_failover_rotates_provider_after_block() -> None:
    direct = FakeTransport()
    pool = fake_proxy_pool("one", "two")
    first = FakeTransport()
    first.add("https://shop.test/data", status=403)
    second = FakeTransport()
    second.add("https://shop.test/data", body="ok")
    routed = RoutedTransport(
        direct,
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": first, "two": second}),
        routing=ProxyRouting(mode=RoutingMode.FAILOVER, provider_preferences=("one", "two")),
        source_id="shop",
        base_url="https://shop.test",
    )
    response = await routed.request(request())
    assert response.text() == "ok" and response.route.provider == "two"
    await routed.aclose()


async def test_proxy_byte_cap_prevents_request_from_starting() -> None:
    import pytest

    pool = fake_proxy_pool("one")
    proxy = FakeTransport()
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        routing=ProxyRouting(mode=RoutingMode.ALWAYS),
        source_id="shop",
        base_url="https://shop.test",
        maximum_bytes=10,
    )
    with pytest.raises(ProxyBudgetExhausted):
        await routed.request(request(estimated_bytes=11))
    assert proxy.requests == []
    await routed.aclose()
