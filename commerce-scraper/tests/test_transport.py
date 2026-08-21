import pytest

from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    BrowserHint,
    MemoryRequestBudget,
    MemoryResponseCache,
    MiddlewareTransport,
    RequestPriority,
    RequestPurpose,
    TransportRequest,
    TransportResponse,
)
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


def _response(body: str) -> TransportResponse:
    return TransportResponse(status=200, content=body.encode(), final_url="https://shop.test/data")
