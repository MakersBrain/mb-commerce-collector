from __future__ import annotations

import asyncio

import pytest
from pydantic import JsonValue

from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    MiddlewareTransport,
    PerOriginRateLimiter,
    RateLimitedTransport,
    RequestPriority,
    RequestPurpose,
    RotationReason,
    TransportRequest,
)


class FakeTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay


class RecordingTelemetry:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, JsonValue]]] = []

    def emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        self.events.append((event, fields))


class RequestScopedRotationTransport(FakeTransport):
    def __init__(self) -> None:
        super().__init__()
        self.request_rotations: list[tuple[RotationReason, TransportRequest]] = []

    async def rotate_identity_for_request(
        self, reason: RotationReason, request: TransportRequest
    ) -> None:
        self.request_rotations.append((reason, request))


class AuthorizedBrowserTransport(FakeTransport):
    browser_subrequests_authorized = True


def request(url: str) -> TransportRequest:
    return TransportRequest(
        url=url,
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
    )


def test_rejects_invalid_policy_values() -> None:
    with pytest.raises(ValueError, match="delay"):
        PerOriginRateLimiter(delay=float("inf"))
    with pytest.raises(ValueError, match="concurrency"):
        PerOriginRateLimiter(concurrency=0)


async def test_spaces_starts_for_the_same_normalized_origin() -> None:
    time = FakeTime()
    limiter = PerOriginRateLimiter(delay=0.25, clock=time.clock, sleeper=time.sleep)

    first = request("https://SHOP.test/products/1")
    second = request("https://shop.test:443/products/2")
    await limiter.wait(first)
    await limiter.release(first)
    await limiter.wait(second)
    await limiter.release(second)

    assert time.sleeps == [0.25]
    assert time.now == 0.25


async def test_origins_have_independent_pacing_and_concurrency() -> None:
    time = FakeTime()
    limiter = PerOriginRateLimiter(delay=1, concurrency=1, clock=time.clock, sleeper=time.sleep)
    first_origin = request("https://one.test/products/1")
    other_origin = request("https://two.test/products/1")

    await limiter.wait(first_origin)
    await limiter.wait(other_origin)

    assert time.sleeps == []
    await limiter.release(first_origin)
    await limiter.release(other_origin)


async def test_concurrency_waits_until_the_attempt_releases_its_permit() -> None:
    limiter = PerOriginRateLimiter(concurrency=1)
    first = request("https://shop.test/products/1")
    second = request("https://shop.test/products/2")
    await limiter.wait(first)

    waiting = asyncio.create_task(limiter.wait(second))
    await asyncio.sleep(0)
    assert not waiting.done()

    await limiter.release(first)
    await waiting
    await limiter.release(second)


async def test_cancelled_pacing_wait_does_not_leak_a_concurrency_permit() -> None:
    pacing_started = asyncio.Event()
    now = 0.0

    async def blocked_sleep(delay: float) -> None:
        del delay
        pacing_started.set()
        await asyncio.Event().wait()

    limiter = PerOriginRateLimiter(
        delay=1,
        concurrency=1,
        clock=lambda: now,
        sleeper=blocked_sleep,
    )
    value = request("https://shop.test/products")
    await limiter.wait(value)
    await limiter.release(value)

    cancelled = asyncio.create_task(limiter.wait(value))
    await pacing_started.wait()
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled

    # A cancelled reservation keeps its pacing slot as a conservative
    # politeness gap, but it must release the in-flight concurrency permit.
    now = 2.0
    acquired = asyncio.create_task(limiter.wait(value))
    await asyncio.sleep(0)
    assert acquired.done()
    await acquired
    await limiter.release(value)


async def test_middleware_releases_permit_between_retry_attempts() -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/data", status=503)
    backend.add("https://shop.test/data", body="ok")
    limiter = PerOriginRateLimiter(concurrency=1)
    telemetry = RecordingTelemetry()
    transport = MiddlewareTransport(
        backend,
        rate_limiter=limiter,
        retries=1,
        backoff=lambda _: 0,
        telemetry=telemetry,
    )

    response = await transport.request(request("https://shop.test/data"))

    assert response.text() == "ok"
    rate_events = [fields for event, fields in telemetry.events if event == "rate_limit.wait"]
    assert len(rate_events) == 2
    assert {fields["request_id"] for fields in rate_events} == {
        rate_events[0]["request_id"]
    }
    for fields in rate_events:
        elapsed_ms = fields["elapsed_ms"]
        assert fields["level"] == "debug"
        assert isinstance(elapsed_ms, (int, float)) and elapsed_ms >= 0


async def test_middleware_releases_permit_after_backend_failure() -> None:
    backend = FakeTransport()
    limiter = PerOriginRateLimiter(concurrency=1)
    transport = MiddlewareTransport(backend, rate_limiter=limiter, retries=0)
    value = request("https://shop.test/data")

    with pytest.raises(RuntimeError, match="no fake response"):
        await transport.request(value)
    backend.add("https://shop.test/data", body="recovered")
    response = await transport.request(value)

    assert response.text() == "recovered"


async def test_rate_limited_wrapper_forwards_request_scoped_rotation() -> None:
    backend = RequestScopedRotationTransport()
    backend.add("https://shop.test/data", status=429)
    backend.add("https://shop.test/data", body="ok")
    wrapped = RateLimitedTransport(
        backend,
        PerOriginRateLimiter(),
        route="proxy",
    )
    transport = MiddlewareTransport(
        wrapped,
        retries=1,
        backoff=lambda _: 0,
        telemetry=RecordingTelemetry(),
    )

    response = await transport.request(request("https://shop.test/data"))

    assert response.text() == "ok"
    [(reason, triggering_request)] = backend.request_rotations
    assert reason is RotationReason.RATE_LIMITED
    assert triggering_request.trace_request_id is not None
    assert triggering_request.trace_attempt == 1
    assert backend.rotations == []


def test_rate_limited_wrapper_forwards_browser_authorization_marker() -> None:
    limiter = PerOriginRateLimiter()
    authorized = RateLimitedTransport(
        AuthorizedBrowserTransport(), limiter, route="proxy"
    )
    ordinary = RateLimitedTransport(FakeTransport(), limiter, route="proxy")

    assert authorized.browser_subrequests_authorized is True
    assert ordinary.browser_subrequests_authorized is False
