from __future__ import annotations

import asyncio
from collections.abc import Callable

from .base import (
    CommerceTransport,
    NullTelemetry,
    RequestBudget,
    ResponseCache,
    RotationReason,
    TelemetryHooks,
    TransportRequest,
    TransportResponse,
)


class BudgetExhausted(RuntimeError):
    pass


class MiddlewareTransport(CommerceTransport):
    """Cache and retry controller around the complete per-attempt layer."""

    def __init__(
        self,
        backend: CommerceTransport,
        *,
        budget: RequestBudget | None = None,
        cache: ResponseCache | None = None,
        retries: int = 2,
        backoff: Callable[[int], float] = lambda attempt: 0.1 * 2**attempt,
        telemetry: TelemetryHooks | None = None,
    ) -> None:
        self._backend = backend
        self._budget = budget
        self._cache = cache
        self._retries = retries
        self._backoff = backoff
        self._telemetry = telemetry or NullTelemetry()

    async def request(self, request: TransportRequest) -> TransportResponse:
        if self._cache is not None and request.cache.value == "default":
            cached = await self._cache.get(request)
            if cached is not None:
                self._telemetry.emit("cache.hit", {"url": request.url})
                return cached.model_copy(update={"from_cache": True})
        for attempt in range(self._retries + 1):
            if self._budget is not None and not self._budget.affordable(request):
                raise BudgetExhausted("request budget cannot authorize another attempt")
            self._telemetry.emit("request.started", {"url": request.url, "attempt": attempt})
            response = await self._backend.request(request)
            if self._budget is not None:
                self._budget.charge(request, len(response.content))
            if response.status not in {429, 500, 502, 503, 504} or attempt == self._retries:
                if self._cache is not None and response.status < 400:
                    await self._cache.put(request, response)
                return response
            self._telemetry.emit("request.retry", {"url": request.url, "attempt": attempt + 1})
            await asyncio.sleep(self._backoff(attempt))
        raise AssertionError("unreachable")

    async def rotate_identity(self, reason: RotationReason) -> None:
        await self._backend.rotate_identity(reason)

