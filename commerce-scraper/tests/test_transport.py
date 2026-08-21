import pytest

from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    BrowserBackendUnavailable,
    BrowserDispatchTransport,
    BrowserHint,
    MemoryRequestBudget,
    MemoryResponseCache,
    MiddlewareTransport,
    RequestPriority,
    RequestPurpose,
    RotationReason,
    TransportRequest,
    TransportResponse,
)
from mb_commerce_scraper.transports.httpx import HttpxTransport
from mb_commerce_scraper.transports.middleware import BudgetExhausted, RobotsDenied
from mb_commerce_scraper.transports.url_policy import URLPolicy


async def test_retries_are_charged_per_attempt() -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/data", status=503)
    backend.add("https://shop.test/data", body="ok")
    budget = MemoryRequestBudget(maximum_requests=2)
    transport = MiddlewareTransport(backend, budget=budget, retries=1, backoff=lambda _: 0)
    response = await transport.request(TransportRequest(url="https://shop.test/data", purpose=RequestPurpose.ENTITY, priority=RequestPriority.IDENTITY))
    assert response.text() == "ok"
    assert budget.requests == 2


async def test_budget_prevents_next_attempt() -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/data", status=503)
    transport = MiddlewareTransport(backend, budget=MemoryRequestBudget(maximum_requests=1), retries=1, backoff=lambda _: 0)
    with pytest.raises(BudgetExhausted):
        await transport.request(TransportRequest(url="https://shop.test/data", purpose=RequestPurpose.ENTITY, priority=RequestPriority.IDENTITY))


async def test_url_policy_rejects_private_and_cross_origin_destinations() -> None:
    policy = URLPolicy(("https://shop.test",), resolver=lambda _: _addresses("127.0.0.1"))
    with pytest.raises(ValueError, match="non-public"):
        await policy.validate("https://shop.test/products")
    public = URLPolicy(("https://shop.test",), resolver=lambda _: _addresses("93.184.216.34"))
    with pytest.raises(ValueError, match="not allowed"):
        await public.validate("https://other.test/products")


async def _addresses(*values: str) -> tuple[str, ...]:
    return values


class Robots:
    def __init__(self, allowed: bool, events: list[str]) -> None:
        self.result = allowed
        self.events = events

    async def allowed(self, url: str) -> bool:
        del url
        self.events.append("robots")
        return self.result


class Limiter:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def wait(self, request: TransportRequest) -> None:
        del request
        self.events.append("rate")


async def test_robots_precedes_cache_and_paid_attempt_layers() -> None:
    events: list[str] = []
    backend = FakeTransport()
    cache = MemoryResponseCache()
    cached_request = TransportRequest(url="https://shop.test/data", purpose=RequestPurpose.ENTITY, priority=RequestPriority.IDENTITY)
    await cache.put(cached_request, _response("cached"))
    transport = MiddlewareTransport(
        backend,
        robots=Robots(False, events),
        cache=cache,
        rate_limiter=Limiter(events),
    )
    with pytest.raises(RobotsDenied):
        await transport.request(cached_request)
    assert events == ["robots"]
    assert backend.requests == []


async def test_cache_hit_skips_budget_rate_limit_and_network() -> None:
    events: list[str] = []
    backend = FakeTransport()
    cache = MemoryResponseCache()
    cached_request = TransportRequest(url="https://shop.test/data", purpose=RequestPurpose.ENTITY, priority=RequestPriority.IDENTITY)
    await cache.put(cached_request, _response("cached"))
    budget = MemoryRequestBudget(maximum_requests=0)
    transport = MiddlewareTransport(backend, cache=cache, budget=budget, rate_limiter=Limiter(events))
    response = await transport.request(cached_request)
    assert response.from_cache and response.text() == "cached"
    assert budget.requests == 0 and events == [] and backend.requests == []


async def test_every_retry_is_independently_rate_limited_and_rotates_blocks() -> None:
    events: list[str] = []
    backend = FakeTransport()
    backend.add("https://shop.test/data", status=403)
    backend.add("https://shop.test/data", body="ok")
    transport = MiddlewareTransport(backend, retries=1, backoff=lambda _: 0, rate_limiter=Limiter(events))
    response = await transport.request(TransportRequest(url="https://shop.test/data", purpose=RequestPurpose.ENTITY, priority=RequestPriority.IDENTITY))
    assert response.text() == "ok"
    assert events == ["rate", "rate"]
    assert backend.rotations


def test_cache_key_ignores_credentials_but_distinguishes_rendering() -> None:
    ordinary = TransportRequest(
        url="https://shop.test/data", purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY, headers={"Authorization": "secret-one"},
    )
    changed_secret = ordinary.model_copy(update={"headers": {"Authorization": "secret-two"}})
    rendered = ordinary.model_copy(update={"browser": BrowserHint.REQUIRED})
    assert MemoryResponseCache.key(ordinary) == MemoryResponseCache.key(changed_secret)
    assert MemoryResponseCache.key(ordinary) != MemoryResponseCache.key(rendered)


async def test_http_transport_rejects_browser_required_requests() -> None:
    transport = HttpxTransport(allowed_origins=("https://shop.test",))
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        browser=BrowserHint.REQUIRED,
    )
    with pytest.raises(BrowserBackendUnavailable, match="browser transport"):
        await transport.request(request)
    await transport.aclose()


async def test_browser_dispatch_routes_only_required_requests() -> None:
    http = FakeTransport()
    browser = FakeTransport()
    http.add("https://shop.test/plain", body="http")
    browser.add("https://shop.test/rendered", body="browser")
    transport = BrowserDispatchTransport(http, browser)

    plain = await transport.request(
        TransportRequest(
            url="https://shop.test/plain",
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
        )
    )
    rendered = await transport.request(
        TransportRequest(
            url="https://shop.test/rendered",
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
            browser=BrowserHint.REQUIRED,
        )
    )

    assert plain.text() == "http" and plain.route.kind == "direct"
    assert rendered.text() == "browser" and rendered.route.kind == "browser"
    assert [request.url for request in http.requests] == ["https://shop.test/plain"]
    assert [request.url for request in browser.requests] == [
        "https://shop.test/rendered"
    ]


async def test_browser_dispatch_fails_required_request_without_backend() -> None:
    transport = BrowserDispatchTransport(FakeTransport())

    with pytest.raises(BrowserBackendUnavailable, match="no browser backend"):
        await transport.request(
            TransportRequest(
                url="https://shop.test/rendered",
                purpose=RequestPurpose.ENTITY,
                priority=RequestPriority.IDENTITY,
                browser=BrowserHint.REQUIRED,
            )
        )


async def test_browser_dispatch_rotates_both_identities() -> None:
    http = FakeTransport()
    browser = FakeTransport()
    transport = BrowserDispatchTransport(http, browser)

    await transport.rotate_identity(RotationReason.CAPTCHA)

    assert http.rotations == [RotationReason.CAPTCHA]
    assert browser.rotations == [RotationReason.CAPTCHA]


def _response(body: str) -> TransportResponse:
    return TransportResponse(status=200, content=body.encode(), final_url="https://shop.test/data")
