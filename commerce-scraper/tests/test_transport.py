import pytest

from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    MemoryRequestBudget,
    MiddlewareTransport,
    RequestPriority,
    RequestPurpose,
    TransportRequest,
)
from mb_commerce_scraper.transports.middleware import BudgetExhausted
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

