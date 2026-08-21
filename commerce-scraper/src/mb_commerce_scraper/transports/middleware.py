from __future__ import annotations

import asyncio
from collections.abc import Callable

from .base import (
    CommerceTransport,
    NullTelemetry,
    RateLimiter,
    RequestBudget,
    ResponseCache,
    RobotsChecker,
    RotationReason,
    TelemetryHooks,
    TransportRequest,
    TransportResponse,
)


class BudgetExhausted(RuntimeError):
    pass


class RobotsDenied(RuntimeError):
    pass


class MiddlewareTransport(CommerceTransport):
    """Cache and retry controller around the complete per-attempt layer."""

    def __init__(
        self,
        backend: CommerceTransport,
        *,
        budget: RequestBudget | None = None,
        cache: ResponseCache | None = None,
        robots: RobotsChecker | None = None,
        rate_limiter: RateLimiter | None = None,
        retries: int = 2,
        backoff: Callable[[int], float] = lambda attempt: 0.1 * 2**attempt,
        telemetry: TelemetryHooks | None = None,
    ) -> None:
        self._backend = backend
        self._budget = budget
        self._cache = cache
        self._robots = robots
        self._rate_limiter = rate_limiter
        self._retries = retries
        self._backoff = backoff
        self._telemetry = telemetry or NullTelemetry()

    async def request(self, request: TransportRequest) -> TransportResponse:
        if (
            self._robots is not None
            and request.purpose.value != "robots"
            and not await self._robots.allowed(request.url)
        ):
            raise RobotsDenied(f"robots policy denies {request.url}")
        if self._cache is not None and request.cache.value == "default":
            cached = await self._cache.get(request)
            if cached is not None:
                self._telemetry.emit("cache.hit", {"url": request.url})
                return cached.model_copy(update={"from_cache": True})
            self._telemetry.emit("cache.miss", {"url": request.url})
        for attempt in range(self._retries + 1):
            if self._budget is not None and not self._budget.affordable(request):
                raise BudgetExhausted("request budget cannot authorize another attempt")
            if self._rate_limiter is not None:
                await self._rate_limiter.wait(request)
            self._telemetry.emit("request.started", {"url": request.url, "attempt": attempt})
            response = await self._backend.request(request)
            if self._budget is not None:
                self._budget.charge(request, len(response.content))
            if response.status not in {403, 429, 500, 502, 503, 504} or attempt == self._retries:
                if self._cache is not None and response.status < 400:
                    await self._cache.put(request, response)
                    self._telemetry.emit("cache.write", {"url": request.url})
                self._telemetry.emit(
                    "request.completed",
                    {"url": request.url, "status": response.status, "attempt": attempt},
                )
                return response
            self._telemetry.emit("request.retry", {"url": request.url, "attempt": attempt + 1})
            if response.status in {403, 429}:
                await self._backend.rotate_identity(
                    RotationReason.BLOCKED
                    if response.status == 403
                    else RotationReason.RATE_LIMITED
                )
            await asyncio.sleep(self._backoff(attempt))
        raise AssertionError("unreachable")

    async def rotate_identity(self, reason: RotationReason) -> None:
        await self._backend.rotate_identity(reason)
