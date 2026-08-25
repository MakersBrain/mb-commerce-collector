from __future__ import annotations

import asyncio

import pytest
from pydantic import JsonValue

from mb_commerce_scraper import ProxyMode, ProxyPolicyConfig
from mb_commerce_scraper.proxy import (
    BrowserSubrequestAuthorizer,
    BrowserSubrequestOutcome,
    PoolBrowserSubrequestAuthorizer,
    ProxyAttemptAuthorization,
    ProxyBudgetExhausted,
    ProxyLease,
    ProxyOutcome,
    ProxyRequest,
    RoutedTransport,
)
from mb_commerce_scraper.testing import FakeTransport, fake_proxy_pool
from mb_commerce_scraper.transports import (
    BrowserDispatchTransport,
    BrowserHint,
    CommerceTransport,
    MemoryRequestBudget,
    MiddlewareTransport,
    RequestPriority,
    RequestPurpose,
    ResponseBodyTooLarge,
    RotationReason,
    TransportAccounting,
    TransportFailure,
    TransportRequest,
    TransportResponse,
    estimated_transmitted_bytes,
)


class FakeProxyTransportFactory:
    def __init__(self, transports: dict[str, CommerceTransport]) -> None:
        self.transports = transports

    def build(self, lease: ProxyLease) -> CommerceTransport:
        return self.transports[lease.provider]


class AuthorizedBrowserTransport:
    browser_subrequests_authorized = True

    def __init__(self, authorizer: BrowserSubrequestAuthorizer) -> None:
        self.authorizer = authorizer

    async def request(self, request: TransportRequest) -> TransportResponse:
        for transmitted, received in ((40, 100), (50, 200)):
            token = await self.authorizer.authorize(transmitted)
            assert token is not None
            await token.reconcile(
                BrowserSubrequestOutcome(
                    status=200,
                    transmitted_bytes=transmitted,
                    received_bytes=received,
                    classification="success",
                )
            )
        return TransportResponse(
            status=200,
            content=b"rendered",
            final_url=request.url,
            accounting=TransportAccounting(
                physical_requests=2,
                transmitted_bytes=90,
                received_bytes=300,
            ),
        )

    async def rotate_identity(self, reason: RotationReason) -> None:
        del reason


class AuthorizedBrowserFactory:
    def __init__(self, pool: RecordingProxyPool) -> None:
        self.pool = pool

    def build(self, lease: ProxyLease) -> CommerceTransport:
        return AuthorizedBrowserTransport(
            PoolBrowserSubrequestAuthorizer(self.pool, lease, "shop.test")
        )


class RecordingTelemetry:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, JsonValue]]] = []

    def emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        self.events.append((event, fields))


class StaleOnlyCache:
    def __init__(self, response: TransportResponse) -> None:
        self.response = response

    async def get(self, request: TransportRequest) -> TransportResponse | None:
        del request
        return None

    async def stale(self, request: TransportRequest) -> TransportResponse | None:
        del request
        return self.response

    async def get_with_stale(self, request: TransportRequest) -> StaleOnlyCacheLookup:
        del request
        return StaleOnlyCacheLookup(self.response)

    async def put(
        self, request: TransportRequest, response: TransportResponse
    ) -> None:
        del request, response


class StaleOnlyCacheLookup:
    fresh: TransportResponse | None = None

    def __init__(self, stale: TransportResponse) -> None:
        self.stale = stale

    async def put(self, response: TransportResponse) -> None:
        del response


class RecordingProxyPool:
    def __init__(self, *providers: str) -> None:
        self.inner = fake_proxy_pool(*providers)
        self.outcomes: list[ProxyOutcome] = []
        self.authorized_estimates: list[int] = []
        self.acquire_calls = 0
        self.rotate_calls = 0

    async def acquire(self, request: ProxyRequest) -> ProxyLease:
        self.acquire_calls += 1
        return await self.inner.acquire(request)

    async def rotate(
        self,
        lease: ProxyLease,
        reason: RotationReason,
    ) -> ProxyLease:
        self.rotate_calls += 1
        return await self.inner.rotate(lease, reason)

    async def report(self, lease: ProxyLease, outcome: ProxyOutcome) -> None:
        self.outcomes.append(outcome)
        await self.inner.report(lease, outcome)

    async def authorize(
        self, lease: ProxyLease, estimated_bytes: int
    ) -> ProxyAttemptAuthorization | None:
        self.authorized_estimates.append(estimated_bytes)
        return await self.inner.authorize(lease, estimated_bytes)

    async def release(self, lease: ProxyLease) -> None:
        await self.inner.release(lease)


class GatedProxyPool(RecordingProxyPool):
    def __init__(self, *providers: str) -> None:
        super().__init__(*providers)
        self.acquire_started = asyncio.Event()
        self.allow_acquire = asyncio.Event()

    async def acquire(self, request: ProxyRequest) -> ProxyLease:
        self.acquire_started.set()
        await self.allow_acquire.wait()
        return await super().acquire(request)


class GatedRotationPool(RecordingProxyPool):
    def __init__(self, *providers: str) -> None:
        super().__init__(*providers)
        self.rotation_started = asyncio.Event()
        self.allow_rotation = asyncio.Event()

    async def rotate(
        self,
        lease: ProxyLease,
        reason: RotationReason,
    ) -> ProxyLease:
        self.rotation_started.set()
        await self.allow_rotation.wait()
        return await super().rotate(lease, reason)


class FailingRotationPool(RecordingProxyPool):
    async def rotate(
        self,
        lease: ProxyLease,
        reason: RotationReason,
    ) -> ProxyLease:
        del lease, reason
        self.rotate_calls += 1
        raise RuntimeError("rotation failed")


class GatedTransport(FakeTransport):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.allow_response = asyncio.Event()
        self.closed = False

    async def request(self, request: TransportRequest) -> TransportResponse:
        self.started.set()
        await self.allow_response.wait()
        return await super().request(request)

    async def aclose(self) -> None:
        self.closed = True


class IndependentlyGatedTransport(FakeTransport):
    def __init__(self, responses: tuple[TransportResponse, ...]) -> None:
        super().__init__()
        self._prepared = responses
        self.started = tuple(asyncio.Event() for _ in responses)
        self.allow_response = tuple(asyncio.Event() for _ in responses)

    async def request(self, request: TransportRequest) -> TransportResponse:
        index = len(self.requests)
        self.requests.append(request)
        self.started[index].set()
        await self.allow_response[index].wait()
        return self._prepared[index]


def request(*, estimated_bytes: int = 0) -> TransportRequest:
    return TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        estimated_bytes=estimated_bytes,
    )


async def test_browser_subrequests_replace_outer_logical_authorization() -> None:
    pool = RecordingProxyPool("one")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=AuthorizedBrowserFactory(pool),
        policy=ProxyPolicyConfig(
            mode=ProxyMode.ALWAYS,
            maximum_requests=2,
            maximum_bytes=1_000,
        ),
        source_id="shop",
        base_url="https://shop.test",
    )

    response = await routed.request(
        request().model_copy(update={"browser": BrowserHint.REQUIRED})
    )

    assert response.accounting is not None
    assert response.accounting.physical_requests == 2
    assert pool.authorized_estimates == [40, 50]
    lease = next(iter(pool.inner._leases.values()))
    assert lease.used_requests == 2
    assert lease.used_bytes == 390
    await routed.aclose()


async def test_fallback_stays_direct_after_success() -> None:
    direct = FakeTransport()
    direct.add("https://shop.test/data", body="direct")
    pool = fake_proxy_pool("one")
    proxy = FakeTransport()
    routed = RoutedTransport(
        direct,
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig.fallback(country="FR"),
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
        policy=ProxyPolicyConfig.fallback(country="FR"),
        source_id="shop",
        base_url="https://shop.test",
    )
    first = await routed.request(request())
    await routed.rotate_identity(RotationReason.BLOCKED)
    second = await routed.request(request())
    assert first.status == 403 and first.route.kind == "direct"
    assert second.text() == "proxy" and second.route.provider == "one"
    assert len(direct.requests) == 1
    assert len(proxy.requests) == 1
    await routed.aclose()
    assert pool.active_leases == 0


async def test_fallback_does_not_route_programming_errors_through_proxy() -> None:
    pool = fake_proxy_pool("one")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": FakeTransport()}),
        policy=ProxyPolicyConfig.fallback(country="FR"),
        source_id="shop",
        base_url="https://shop.test",
    )

    import pytest

    with pytest.raises(RuntimeError, match="no fake response"):
        await routed.request(request())
    assert pool.active_leases == 0


async def test_failover_rotates_provider_after_block() -> None:
    direct = FakeTransport()
    pool = RecordingProxyPool("one", "two")
    first = FakeTransport()
    first.add("https://shop.test/data", status=403)
    second = FakeTransport()
    second.add("https://shop.test/data", body="ok")
    routed = RoutedTransport(
        direct,
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": first, "two": second}),
        policy=ProxyPolicyConfig(mode=ProxyMode.FAILOVER, provider_preferences=("one", "two")),
        source_id="shop",
        base_url="https://shop.test",
    )
    blocked = await routed.request(request())
    assert blocked.status == 403 and blocked.route.provider == "one"
    assert pool.outcomes[0].classification == "blocked"
    assert len(first.requests) == 1 and second.requests == []

    response = await routed.request(request())
    assert response.text() == "ok" and response.route.provider == "two"
    assert len(first.requests) == 1 and len(second.requests) == 1
    await routed.aclose()


async def test_proxy_lifecycle_telemetry_tracks_route_without_credentials() -> None:
    pool = RecordingProxyPool("one", "two")
    first = FakeTransport()
    first.add("https://shop.test/data", status=403)
    second = FakeTransport()
    second.add("https://shop.test/data", body="ok")
    telemetry = RecordingTelemetry()
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": first, "two": second}),
        policy=ProxyPolicyConfig(
            mode=ProxyMode.FAILOVER,
            provider_preferences=("one", "two"),
        ),
        source_id="shop",
        base_url="https://shop.test",
        telemetry=telemetry,
        telemetry_context={"collection_id": "collection-1"},
    )

    assert (await routed.request(request())).status == 403
    assert (await routed.request(request())).text() == "ok"
    await routed.aclose()

    names = [event for event, _ in telemetry.events]
    assert names == [
        "proxy.acquire.started",
        "proxy.acquire.completed",
        "proxy.outcome",
        "proxy.rotate.started",
        "proxy.rotate.completed",
        "proxy.outcome",
        "proxy.release.started",
        "proxy.release.completed",
    ]
    assert {
        fields["collection_id"] for _, fields in telemetry.events
    } == {"collection-1"}
    outcomes = [fields for event, fields in telemetry.events if event == "proxy.outcome"]
    assert [fields["classification"] for fields in outcomes] == [
        "blocked",
        "success",
    ]
    assert outcomes[0]["level"] == "warning"
    assert outcomes[1]["level"] == "debug"
    serialized = repr(telemetry.events).casefold()
    assert "password" not in serialized
    assert "username" not in serialized


async def test_fallback_transport_failure_defers_proxy_to_next_attempt() -> None:
    import pytest

    direct = FakeTransport()
    direct.add("https://shop.test/data", error=TransportFailure("connect failed"))
    pool = fake_proxy_pool("one")
    proxy = FakeTransport()
    proxy.add("https://shop.test/data", body="proxy")
    routed = RoutedTransport(
        direct,
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig.fallback(country="FR"),
        source_id="shop",
        base_url="https://shop.test",
    )

    with pytest.raises(TransportFailure, match="connect failed"):
        await routed.request(request())
    assert len(direct.requests) == 1 and proxy.requests == []

    await routed.rotate_identity(RotationReason.TRANSPORT_FAILURE)
    response = await routed.request(request())
    assert response.text() == "proxy" and response.route.provider == "one"
    assert len(direct.requests) == 1 and len(proxy.requests) == 1
    await routed.aclose()


async def test_failover_transport_failure_is_reported_before_next_provider_attempt() -> None:
    import pytest

    pool = RecordingProxyPool("one", "two")
    first = FakeTransport()
    first.add("https://shop.test/data", error=TransportFailure("proxy failed"))
    second = FakeTransport()
    second.add("https://shop.test/data", body="ok")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": first, "two": second}),
        policy=ProxyPolicyConfig(
            mode=ProxyMode.FAILOVER,
            provider_preferences=("one", "two"),
        ),
        source_id="shop",
        base_url="https://shop.test",
    )

    with pytest.raises(TransportFailure, match="proxy failed"):
        await routed.request(request())
    assert pool.outcomes[0].classification == "transport_failure"
    assert pool.outcomes[0].transmitted_bytes == estimated_transmitted_bytes(request())
    assert len(first.requests) == 1 and second.requests == []

    response = await routed.request(request())
    assert response.text() == "ok" and response.route.provider == "two"
    assert len(first.requests) == 1 and len(second.requests) == 1
    await routed.aclose()


async def test_proxy_oversized_body_reports_received_bytes_without_rotation() -> None:
    pool = RecordingProxyPool("one")

    class OversizedTransport(FakeTransport):
        async def request(self, request: TransportRequest) -> TransportResponse:
            self.requests.append(request)
            raise ResponseBodyTooLarge(maximum_bytes=5, received_bytes=6)

    proxy = OversizedTransport()
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
        source_id="shop",
        base_url="https://shop.test",
    )

    with pytest.raises(ResponseBodyTooLarge):
        await routed.request(request())

    assert pool.outcomes[0].classification == "response_body_too_large"
    assert pool.outcomes[0].transmitted_bytes == estimated_transmitted_bytes(request())
    assert pool.outcomes[0].received_bytes == 6
    assert pool.rotate_calls == 0
    await routed.aclose()


async def test_middleware_retry_is_the_single_fallback_proxy_attempt() -> None:
    direct = FakeTransport()
    direct.add("https://shop.test/data", status=429)
    pool = fake_proxy_pool("one")
    proxy = FakeTransport()
    proxy.add("https://shop.test/data", body="proxy")
    routed = RoutedTransport(
        direct,
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig.fallback(country="FR"),
        source_id="shop",
        base_url="https://shop.test",
    )
    budget = MemoryRequestBudget(maximum_requests=2)
    middleware = MiddlewareTransport(
        routed,
        budget=budget,
        retries=1,
        backoff=lambda attempt: 0,
    )

    response = await middleware.request(request())

    assert response.text() == "proxy" and response.route.provider == "one"
    assert len(direct.requests) == 1 and len(proxy.requests) == 1
    assert budget.requests == 2
    await routed.aclose()


async def test_stale_on_error_does_not_acquire_or_taint_fallback_route() -> None:
    direct = FakeTransport()
    for _ in range(4):
        direct.add("https://shop.test/data", status=429, body="limited")
    direct.add("https://shop.test/data", body="fresh")
    pool = RecordingProxyPool("one")
    proxy = FakeTransport()
    routed = RoutedTransport(
        direct,
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig.fallback(country="FR"),
        source_id="shop",
        base_url="https://shop.test",
    )
    stale = StaleOnlyCache(
        TransportResponse(
            status=200,
            content=b"stale",
            final_url="https://shop.test/data",
            from_cache=True,
        )
    )
    middleware = MiddlewareTransport(
        routed,
        cache=stale,
        stale_on_error=True,
        retries=3,
        backoff=lambda _attempt: 0,
    )

    fallback = await middleware.request(request())
    fresh = await middleware.request(request())

    assert fallback.text() == "stale" and fallback.route.kind == "cache"
    assert fresh.text() == "fresh" and fresh.route.kind == "direct"
    assert len(direct.requests) == 5
    assert proxy.requests == []
    assert pool.acquire_calls == 0
    await routed.aclose()


async def test_each_proxy_retry_reports_transmitted_bytes_once() -> None:
    pool = RecordingProxyPool("one")
    proxy = FakeTransport()
    proxy.add("https://shop.test/data", status=503, body="retry")
    proxy.add("https://shop.test/data", body="ok")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
        source_id="shop",
        base_url="https://shop.test",
    )
    transport = MiddlewareTransport(
        routed,
        retries=1,
        backoff=lambda attempt: 0,
    )

    assert (await transport.request(request())).text() == "ok"

    expected = estimated_transmitted_bytes(request())
    assert len(proxy.requests) == 2
    assert [outcome.transmitted_bytes for outcome in pool.outcomes] == [
        expected,
        expected,
    ]
    assert [outcome.received_bytes for outcome in pool.outcomes] == [5, 2]
    await routed.aclose()


async def test_proxy_outcome_prefers_backend_physical_accounting() -> None:
    class AccountingTransport(FakeTransport):
        async def request(self, request: TransportRequest) -> TransportResponse:
            self.requests.append(request)
            return TransportResponse(
                status=200,
                content=b"ok",
                final_url=request.url,
                accounting=TransportAccounting(
                    physical_requests=3,
                    transmitted_bytes=123,
                    received_bytes=456,
                ),
            )

    pool = RecordingProxyPool("one")
    proxy = AccountingTransport()
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
        source_id="shop",
        base_url="https://shop.test",
    )

    await routed.request(request())

    assert pool.outcomes[0].physical_requests == 3
    assert pool.outcomes[0].transmitted_bytes == 123
    assert pool.outcomes[0].received_bytes == 456
    await routed.aclose()


async def test_proxy_accounting_rejects_backend_values_below_observed_minimums() -> None:
    class UnderreportingTransport(FakeTransport):
        async def request(self, request: TransportRequest) -> TransportResponse:
            self.requests.append(request)
            return TransportResponse(
                status=200,
                content=b"retained",
                final_url=request.url,
                accounting=TransportAccounting(
                    physical_requests=0,
                    transmitted_bytes=0,
                    received_bytes=0,
                ),
            )

    pool = RecordingProxyPool("one")
    proxy = UnderreportingTransport()
    attempted = request()
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS, maximum_requests=1),
        source_id="shop",
        base_url="https://shop.test",
    )

    assert (await routed.request(attempted)).content == b"retained"

    assert pool.outcomes[0].physical_requests == 1
    assert pool.outcomes[0].transmitted_bytes == estimated_transmitted_bytes(
        attempted
    )
    assert pool.outcomes[0].received_bytes == len(b"retained")
    with pytest.raises(ProxyBudgetExhausted):
        await routed.request(attempted)
    await routed.aclose()


async def test_proxy_oversize_accounting_cannot_underreport_observed_bytes() -> None:
    class UnderreportingOversizeTransport(FakeTransport):
        async def request(self, request: TransportRequest) -> TransportResponse:
            self.requests.append(request)
            raise ResponseBodyTooLarge(
                maximum_bytes=5,
                received_bytes=6,
                accounting=TransportAccounting(
                    physical_requests=0,
                    transmitted_bytes=0,
                    received_bytes=1,
                ),
            )

    pool = RecordingProxyPool("one")
    attempted = request()
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory(
            {"one": UnderreportingOversizeTransport()}
        ),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
        source_id="shop",
        base_url="https://shop.test",
    )

    with pytest.raises(ResponseBodyTooLarge):
        await routed.request(attempted)

    assert pool.outcomes[0].physical_requests == 1
    assert pool.outcomes[0].transmitted_bytes == estimated_transmitted_bytes(
        attempted
    )
    assert pool.outcomes[0].received_bytes == 6
    await routed.aclose()


async def test_backend_redirect_count_reconciles_against_proxy_request_cap() -> None:
    class RedirectingTransport(FakeTransport):
        async def request(self, request: TransportRequest) -> TransportResponse:
            self.requests.append(request)
            return TransportResponse(
                status=200,
                content=b"ok",
                final_url=request.url,
                accounting=TransportAccounting(
                    physical_requests=2,
                    transmitted_bytes=100,
                    received_bytes=2,
                ),
            )

    pool = RecordingProxyPool("one")
    proxy = RedirectingTransport()
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS, maximum_requests=1),
        source_id="shop",
        base_url="https://shop.test",
    )

    with pytest.raises(ProxyBudgetExhausted) as caught:
        await routed.request(request())

    assert caught.value.used_requests == 2
    assert len(proxy.requests) == 1
    assert pool.outcomes[0].physical_requests == 2
    await routed.aclose()


async def test_proxy_byte_cap_prevents_request_from_starting() -> None:
    import pytest

    pool = fake_proxy_pool("one")
    proxy = FakeTransport()
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS, maximum_bytes=10),
        source_id="shop",
        base_url="https://shop.test",
    )
    with pytest.raises(ProxyBudgetExhausted):
        await routed.request(request(estimated_bytes=11))
    assert proxy.requests == []
    assert pool.active_leases == 1
    await routed.aclose()


async def test_proxy_authorization_reserves_expected_receive_and_transmit_bytes() -> None:
    pool = RecordingProxyPool("one")
    proxy = FakeTransport()
    proxy.add("https://shop.test/data", body="ok")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS, maximum_bytes=1_000),
        source_id="shop",
        base_url="https://shop.test",
    )
    attempted = request(estimated_bytes=60)

    await routed.request(attempted)

    assert pool.authorized_estimates == [
        attempted.estimated_bytes + estimated_transmitted_bytes(attempted)
    ]
    await routed.aclose()


async def test_proxy_browser_aggregate_uses_one_lease_and_one_outcome() -> None:
    pool = RecordingProxyPool("one")
    browser = IndependentlyGatedTransport(
        (
            TransportResponse(
                status=200,
                content=b"rendered",
                final_url="https://shop.test/data",
                accounting=TransportAccounting(
                    physical_requests=3,
                    transmitted_bytes=400,
                    received_bytes=900,
                ),
            ),
        )
    )
    browser.allow_response[0].set()
    proxy_route = BrowserDispatchTransport(FakeTransport(), browser)
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy_route}),
        policy=ProxyPolicyConfig(
            mode=ProxyMode.ALWAYS,
            maximum_requests=10,
            maximum_bytes=2_000,
        ),
        source_id="shop",
        base_url="https://shop.test",
    )
    rendered = request().model_copy(update={"browser": BrowserHint.REQUIRED})

    response = await routed.request(rendered)

    assert response.route.kind == "browser"
    assert response.route.provider == "one"
    assert response.route.lease_id == "static-1"
    assert len(pool.outcomes) == 1
    assert pool.outcomes[0].physical_requests == 3
    assert pool.outcomes[0].transmitted_bytes == 400
    assert pool.outcomes[0].received_bytes == 900
    await routed.aclose()


async def test_concurrent_proxy_attempts_authorize_the_collection_cap_atomically() -> None:
    pool = fake_proxy_pool("one")
    proxy = GatedTransport()
    proxy.add("https://shop.test/data", body="ok")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS, maximum_bytes=100),
        source_id="shop",
        base_url="https://shop.test",
    )

    first = asyncio.create_task(routed.request(request(estimated_bytes=60)))
    await proxy.started.wait()
    with pytest.raises(ProxyBudgetExhausted) as raised:
        await routed.request(request(estimated_bytes=60))

    assert raised.value.maximum_bytes == 100
    assert len(proxy.requests) == 0
    proxy.allow_response.set()
    assert (await first).text() == "ok"
    assert len(proxy.requests) == 1
    await routed.aclose()


async def test_proxy_collection_cap_survives_identity_rotation() -> None:
    pool = fake_proxy_pool("one", "two")
    first_proxy = FakeTransport()
    first_proxy.add("https://shop.test/data", body="123456")
    second_proxy = FakeTransport()
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory(
            {"one": first_proxy, "two": second_proxy}
        ),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS, maximum_bytes=100),
        source_id="shop",
        base_url="https://shop.test",
    )

    assert (await routed.request(request(estimated_bytes=60))).text() == "123456"
    await routed.rotate_identity(RotationReason.EXPLICIT)
    with pytest.raises(ProxyBudgetExhausted):
        await routed.request(request(estimated_bytes=56))

    assert second_proxy.requests == []
    await routed.aclose()


async def test_cancelled_dispatched_proxy_attempt_counts_conservatively() -> None:
    pool = fake_proxy_pool("one")
    proxy = GatedTransport()
    proxy.add("https://shop.test/data", body="ok")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(
            mode=ProxyMode.ALWAYS,
            maximum_requests=2,
            maximum_bytes=200,
        ),
        source_id="shop",
        base_url="https://shop.test",
    )

    cancelled = asyncio.create_task(routed.request(request(estimated_bytes=60)))
    await proxy.started.wait()
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled

    proxy.allow_response.set()
    assert (await routed.request(request(estimated_bytes=60))).text() == "ok"
    assert len(proxy.requests) == 1
    with pytest.raises(ProxyBudgetExhausted):
        await routed.request(request())
    assert len(proxy.requests) == 1
    await routed.aclose()


async def test_cancelled_proxy_acquisition_releases_cap_authorization() -> None:
    pool = GatedProxyPool("one")
    proxy = FakeTransport()
    proxy.add("https://shop.test/data", body="ok")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS, maximum_bytes=100),
        source_id="shop",
        base_url="https://shop.test",
    )

    cancelled = asyncio.create_task(routed.request(request(estimated_bytes=60)))
    await pool.acquire_started.wait()
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled

    pool.allow_acquire.set()
    assert (await routed.request(request(estimated_bytes=60))).text() == "ok"
    assert pool.acquire_calls == 1
    assert len(proxy.requests) == 1
    await routed.aclose()


async def test_concurrent_initial_requests_share_one_lease_acquisition() -> None:
    pool = GatedProxyPool("one")
    proxy = FakeTransport()
    proxy.add("https://shop.test/data", body="first")
    proxy.add("https://shop.test/data", body="second")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
        source_id="shop",
        base_url="https://shop.test",
    )

    first = asyncio.create_task(routed.request(request()))
    await pool.acquire_started.wait()
    second = asyncio.create_task(routed.request(request()))
    await asyncio.sleep(0)
    pool.allow_acquire.set()
    responses = await asyncio.gather(first, second)

    assert pool.acquire_calls == 1
    assert len(proxy.requests) == 2
    assert {response.text() for response in responses} == {"first", "second"}
    assert {response.route.lease_id for response in responses} == {"static-1"}
    await routed.aclose()


async def test_rotation_waits_for_in_flight_route_without_blocking_state_updates() -> None:
    pool = RecordingProxyPool("one", "two")
    first_proxy = GatedTransport()
    first_proxy.add("https://shop.test/data", body="first")
    second_proxy = FakeTransport()
    second_proxy.add("https://shop.test/data", body="second")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory(
            {"one": first_proxy, "two": second_proxy}
        ),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
        source_id="shop",
        base_url="https://shop.test",
    )

    in_flight = asyncio.create_task(routed.request(request()))
    await first_proxy.started.wait()
    rotation = asyncio.create_task(routed.rotate_identity(RotationReason.EXPLICIT))
    await asyncio.sleep(0)

    assert not rotation.done()
    assert pool.rotate_calls == 0
    assert not first_proxy.closed

    first_proxy.allow_response.set()
    first_response = await in_flight
    await rotation
    second_response = await routed.request(request())

    assert first_response.route.provider == "one"
    assert second_response.route.provider == "two"
    assert pool.rotate_calls == 1
    assert first_proxy.closed
    assert len(first_proxy.requests) == 1
    assert len(second_proxy.requests) == 1
    await routed.aclose()


async def test_concurrent_rotation_requests_coalesce_to_one_transition() -> None:
    pool = GatedRotationPool("one", "two")
    first_proxy = FakeTransport()
    first_proxy.add("https://shop.test/data", body="first")
    second_proxy = FakeTransport()
    second_proxy.add("https://shop.test/data", body="second")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory(
            {"one": first_proxy, "two": second_proxy}
        ),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
        source_id="shop",
        base_url="https://shop.test",
    )
    await routed.request(request())

    first_rotation = asyncio.create_task(
        routed.rotate_identity(RotationReason.EXPLICIT)
    )
    await pool.rotation_started.wait()
    second_rotation = asyncio.create_task(
        routed.rotate_identity(RotationReason.EXPLICIT)
    )
    await asyncio.sleep(0)
    pool.allow_rotation.set()
    await asyncio.gather(first_rotation, second_rotation)
    response = await routed.request(request())

    assert pool.rotate_calls == 1
    assert response.route.provider == "two"
    await routed.aclose()


async def test_close_waits_for_in_flight_request_before_releasing_lease() -> None:
    pool = RecordingProxyPool("one")
    proxy = GatedTransport()
    proxy.add("https://shop.test/data", body="ok")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
        source_id="shop",
        base_url="https://shop.test",
    )

    in_flight = asyncio.create_task(routed.request(request()))
    await proxy.started.wait()
    closing = asyncio.create_task(routed.aclose())
    await asyncio.sleep(0)

    assert not closing.done()
    assert not proxy.closed
    assert pool.inner.active_leases == 1

    proxy.allow_response.set()
    assert (await in_flight).text() == "ok"
    await closing

    assert proxy.closed
    assert pool.inner.active_leases == 0


async def test_cancelled_request_returns_route_checkout_before_close() -> None:
    import pytest

    pool = RecordingProxyPool("one")
    proxy = GatedTransport()
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
        source_id="shop",
        base_url="https://shop.test",
    )

    in_flight = asyncio.create_task(routed.request(request()))
    await proxy.started.wait()
    in_flight.cancel()
    with pytest.raises(asyncio.CancelledError):
        await in_flight

    await asyncio.wait_for(routed.aclose(), timeout=1)
    assert proxy.closed
    assert pool.inner.active_leases == 0


async def test_failed_rotation_releases_detached_old_lease() -> None:
    import pytest

    pool = FailingRotationPool("one")
    proxy = FakeTransport()
    proxy.add("https://shop.test/data", body="ok")
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
        source_id="shop",
        base_url="https://shop.test",
    )
    await routed.request(request())
    assert pool.inner.active_leases == 1

    with pytest.raises(RuntimeError, match="rotation failed"):
        await routed.rotate_identity(RotationReason.EXPLICIT)

    assert pool.rotate_calls == 1
    assert pool.inner.active_leases == 0
    await routed.aclose()


async def test_stale_direct_block_does_not_rotate_newly_acquired_proxy() -> None:
    blocked = TransportResponse(
        status=403,
        content=b"blocked",
        final_url="https://shop.test/data",
    )
    direct = IndependentlyGatedTransport((blocked, blocked))
    pool = RecordingProxyPool("one")
    proxy = FakeTransport()
    proxy.add("https://shop.test/data", body="proxy-first")
    proxy.add("https://shop.test/data", body="proxy-sticky")
    routed = RoutedTransport(
        direct,
        pool=pool,
        proxy_factory=FakeProxyTransportFactory({"one": proxy}),
        policy=ProxyPolicyConfig.fallback(),
        source_id="shop",
        base_url="https://shop.test",
    )

    first_direct = asyncio.create_task(routed.request(request()))
    second_direct = asyncio.create_task(routed.request(request()))
    await asyncio.gather(direct.started[0].wait(), direct.started[1].wait())
    direct.allow_response[0].set()
    assert (await first_direct).status == 403

    await routed.rotate_identity(RotationReason.BLOCKED)
    first_proxy = await routed.request(request())
    direct.allow_response[1].set()
    assert (await second_direct).status == 403
    sticky_proxy = await routed.request(request())

    assert first_proxy.text() == "proxy-first"
    assert sticky_proxy.text() == "proxy-sticky"
    assert pool.acquire_calls == 1
    assert pool.rotate_calls == 0
    await routed.aclose()
