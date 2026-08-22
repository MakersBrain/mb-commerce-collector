import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import SecretStr, ValidationError

from mb_commerce_scraper.proxy import (
    BrowserSubrequestOutcome,
    InMemoryProxyHealth,
    ProxyBudgetExhausted,
    ProxyCredentials,
    ProxyEndpoint,
    ProxyFailureReason,
    ProxyKind,
    ProxyOutcome,
    ProxyRequest,
    StaticProxyPool,
    StaticRoute,
)
from mb_commerce_scraper.testing import fake_proxy_pool
from mb_commerce_scraper.transports import RotationReason


@pytest.mark.parametrize(
    "build",
    [
        lambda: ProxyRequest(
            source_id="shop", target_host="shop.test", session_ttl_seconds=True
        ),
        lambda: ProxyRequest(
            source_id="shop", target_host="shop.test", maximum_requests=True
        ),
        lambda: ProxyRequest(
            source_id="shop", target_host="shop.test", maximum_bytes=True
        ),
        lambda: ProxyEndpoint(
            provider="one",
            endpoint_id="edge",
            protocol="http",
            host="proxy.test",
            port=True,
            kind=ProxyKind.RESIDENTIAL,
        ),
        lambda: ProxyOutcome(
            target_host="shop.test", physical_requests=True, classification="success"
        ),
        lambda: BrowserSubrequestOutcome(
            transmitted_bytes=True, classification="success"
        ),
    ],
)
def test_proxy_integer_boundaries_reject_booleans(
    build: Callable[[], object],
) -> None:
    with pytest.raises(ValidationError):
        build()


async def test_static_pool_round_robin_rotation_and_idempotent_release() -> None:
    pool = fake_proxy_pool("one", "two")
    request = ProxyRequest(source_id="shop", target_host="shop.test", country="FR")
    first = await pool.acquire(request)
    assert first.provider == "one"
    second = await pool.rotate(first, RotationReason.BLOCKED)
    assert second.provider == "two"
    await pool.release(second)
    await pool.release(second)


async def test_proxy_accounting_fails_on_exhaustion_and_credentials_are_redacted() -> None:
    pool = fake_proxy_pool("one")
    lease = await pool.acquire(ProxyRequest(source_id="shop", target_host="shop.test", maximum_bytes=10))
    dumped = lease.http_credentials().model_dump_json()
    assert "one-user" not in dumped and "one-password" not in dumped
    authorization = await pool.authorize(lease, 0)
    assert authorization is not None
    with pytest.raises(ProxyBudgetExhausted, match="byte limit"):
        await authorization.reconcile(
            ProxyOutcome(
                target_host="shop.test",
                transmitted_bytes=5,
                received_bytes=6,
                classification="success",
            )
        )


async def test_static_cap_and_health_accounting_survive_rotation_once() -> None:
    class CountingHealth(InMemoryProxyHealth):
        def __init__(self) -> None:
            super().__init__()
            self.failure_calls = 0

        def failure(
            self,
            provider: str,
            endpoint_id: str,
            target_host: str,
            reason: ProxyFailureReason = ProxyFailureReason.TRANSPORT_FAILURE,
        ) -> None:
            self.failure_calls += 1
            super().failure(provider, endpoint_id, target_host, reason)

    health = CountingHealth()
    pool = StaticProxyPool((route("one"), route("two")), health=health)
    first = await pool.acquire(
        ProxyRequest(source_id="shop", target_host="shop.test", maximum_bytes=10)
    )
    authorization = await pool.authorize(first, 6)
    assert authorization is not None
    blocked = ProxyOutcome(
        target_host="shop.test",
        received_bytes=6,
        classification="blocked",
    )
    await authorization.reconcile(blocked)
    await pool.report(
        first,
        blocked,
    )
    second = await pool.rotate(first, RotationReason.BLOCKED)

    assert health.failure_calls == 1
    assert second.used_bytes == 6
    assert not second.can_start(5)
    second_authorization = await pool.authorize(second, 0)
    assert second_authorization is not None
    with pytest.raises(ProxyBudgetExhausted) as raised:
        await second_authorization.reconcile(
            ProxyOutcome(
                target_host="shop.test",
                received_bytes=5,
                classification="success",
            )
        )
    assert raised.value.used_bytes == 11

    explicit_health = CountingHealth()
    explicit_pool = StaticProxyPool(
        (route("explicit-one"), route("explicit-two")), health=explicit_health
    )
    explicit = await explicit_pool.acquire(
        ProxyRequest(source_id="shop", target_host="shop.test")
    )
    await explicit_pool.rotate(explicit, RotationReason.EXPLICIT)
    assert explicit_health.failure_calls == 0


def test_proxy_health_bounds_lru_state_and_retains_reason_counters() -> None:
    now = datetime(2026, 8, 22, tzinfo=UTC)
    health = InMemoryProxyHealth(
        initial_cooldown=5,
        maximum_cooldown=10,
        maximum_entries=2,
        clock=lambda: now,
    )

    health.failure("one", "one-1", "shop.test", ProxyFailureReason.BLOCKED)
    health.failure("one", "one-1", "shop.test", ProxyFailureReason.CAPTCHA)
    first = health.snapshot("one", "one-1", "shop.test")
    assert first is not None
    assert first.consecutive_failures == 2
    assert first.reason_failures == {
        ProxyFailureReason.BLOCKED: 1,
        ProxyFailureReason.CAPTCHA: 1,
    }
    assert first.cooldown_until == now + timedelta(seconds=10)

    health.failure("two", "two-1", "shop.test", ProxyFailureReason.RATE_LIMITED)
    assert not health.available("one", "one-1", "shop.test")  # refresh LRU position
    health.failure(
        "three",
        "three-1",
        "shop.test",
        ProxyFailureReason.TRANSPORT_FAILURE,
    )

    assert health.entry_count == 2
    assert health.snapshot("two", "two-1", "shop.test") is None
    health.success("one", "one-1", "shop.test")
    recovered = health.snapshot("one", "one-1", "shop.test")
    assert recovered is not None
    assert recovered.consecutive_failures == 0
    assert recovered.reason_failures == first.reason_failures
    assert recovered.cooldown_until is None


async def test_static_pool_reports_typed_failure_reason_to_health_selection() -> None:
    health = InMemoryProxyHealth()
    pool = StaticProxyPool((route("one"), route("two")), health=health)
    request = ProxyRequest(source_id="shop", target_host="shop.test")
    failed = await pool.acquire(request)
    assert failed.provider == "one"

    await pool.report(
        failed,
        ProxyOutcome(target_host="shop.test", classification="rate_limited"),
    )
    await pool.release(failed)
    replacement = await pool.acquire(request)

    state = health.snapshot("one", "one-1", "shop.test")
    assert state is not None
    assert state.reason_failures == {ProxyFailureReason.RATE_LIMITED: 1}
    assert replacement.provider == "two"


async def test_static_request_cap_authorization_is_atomic_and_releasable() -> None:
    pool = fake_proxy_pool("one")
    lease = await pool.acquire(
        ProxyRequest(
            source_id="shop",
            target_host="shop.test",
            maximum_requests=1,
            maximum_bytes=10,
        )
    )

    first, second = await asyncio.gather(
        pool.authorize(lease, 6),
        pool.authorize(lease, 6),
    )
    granted = first or second
    assert granted is not None
    assert (first is None) != (second is None)

    await granted.release()
    replacement = await pool.authorize(lease, 10)
    assert replacement is not None
    await replacement.reconcile(
        ProxyOutcome(
            target_host="shop.test",
            physical_requests=1,
            received_bytes=10,
            classification="success",
        )
    )
    assert await pool.authorize(lease, 0) is None


async def test_physical_request_cap_survives_rotation() -> None:
    pool = fake_proxy_pool("one", "two")
    lease = await pool.acquire(
        ProxyRequest(
            source_id="shop",
            target_host="shop.test",
            maximum_requests=2,
        )
    )
    authorization = await pool.authorize(lease, 0)
    assert authorization is not None
    await authorization.reconcile(
        ProxyOutcome(
            target_host="shop.test",
            physical_requests=2,
            classification="success",
        )
    )

    rotated = await pool.rotate(lease, RotationReason.EXPLICIT)
    assert rotated.used_requests == 2
    assert await pool.authorize(rotated, 0) is None


def route(provider: str, *, weight: int = 1) -> StaticRoute:
    return StaticRoute(
        endpoint=ProxyEndpoint(
            provider=provider,
            endpoint_id=f"{provider}-1",
            protocol="http",
            host=f"{provider}.proxy.test",
            port=8080,
            kind=ProxyKind.RESIDENTIAL,
        ),
        credentials=ProxyCredentials(
            username=SecretStr("user"), password=SecretStr("password")
        ),
        weight=weight,
    )


async def test_static_pool_applies_smooth_route_weights() -> None:
    pool = StaticProxyPool((route("primary", weight=3), route("secondary")))
    request = ProxyRequest(source_id="shop", target_host="shop.test")

    providers: list[str] = []
    for _ in range(8):
        lease = await pool.acquire(request)
        providers.append(lease.provider)
        await pool.release(lease)

    assert providers.count("primary") == 6
    assert providers.count("secondary") == 2
    assert "secondary" in providers[:3]


async def test_provider_preference_is_independent_of_round_robin_state() -> None:
    pool = StaticProxyPool((route("one"), route("two")))
    ordinary = ProxyRequest(source_id="ordinary", target_host="shop.test")
    for _ in range(3):
        lease = await pool.acquire(ordinary)
        await pool.release(lease)

    preferred = await pool.acquire(
        ProxyRequest(
            source_id="preferred",
            target_host="shop.test",
            preferred_providers=("two", "one"),
        )
    )

    assert preferred.provider == "two"


@pytest.mark.parametrize("weight", [0, -1, True, 1.5])
def test_static_route_rejects_invalid_weight(weight: int) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        route("invalid", weight=weight)


def test_static_pool_rejects_duplicate_route_identity() -> None:
    with pytest.raises(ValueError, match="unique provider/endpoint"):
        StaticProxyPool((route("duplicate"), route("duplicate")))
