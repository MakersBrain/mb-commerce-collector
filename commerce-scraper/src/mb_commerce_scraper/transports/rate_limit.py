from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from time import monotonic
from typing import Literal
from urllib.parse import urlsplit

from pydantic import JsonValue

from .base import (
    CommerceTransport,
    RateLimiter,
    RotationReason,
    TelemetryHooks,
    TransportRequest,
    TransportResponse,
    transport_trace_fields,
)
from .telemetry import safe_telemetry

Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]


@dataclass(slots=True)
class _OriginState:
    concurrency: asyncio.BoundedSemaphore
    pacing: asyncio.Lock = field(default_factory=asyncio.Lock)
    next_start: float = 0.0


class PerOriginRateLimiter:
    """Limit request start rate and in-flight work independently per origin.

    ``wait`` acquires an in-flight permit and reserves the next start time.
    Callers must pair every successful ``wait`` with ``release`` in a
    ``finally`` block. ``MiddlewareTransport`` provides that lifecycle.

    The clock and sleeper are injectable so policy behavior can be tested
    without wall-clock sleeps.
    """

    def __init__(
        self,
        *,
        delay: float = 0.0,
        concurrency: int = 1,
        clock: Clock = monotonic,
        sleeper: Sleeper = asyncio.sleep,
    ) -> None:
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("delay must be a finite non-negative number")
        if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
            raise ValueError("concurrency must be a positive integer")
        self.delay = delay
        self.concurrency = concurrency
        self._clock = clock
        self._sleeper = sleeper
        self._states: dict[tuple[str, str, int], _OriginState] = {}
        self._states_lock = asyncio.Lock()

    async def wait(self, request: TransportRequest) -> None:
        state = await self._state_for(request)
        await state.concurrency.acquire()
        try:
            async with state.pacing:
                remaining = state.next_start - self._clock()
                state.next_start = max(state.next_start, self._clock()) + self.delay
            if remaining > 0:
                await self._sleeper(remaining)
        except BaseException:
            state.concurrency.release()
            raise

    async def release(self, request: TransportRequest) -> None:
        state = await self._state_for(request)
        try:
            state.concurrency.release()
        except ValueError as error:
            raise RuntimeError(
                f"rate limiter permit released without a matching wait for {_origin(request.url)!r}"
            ) from error

    async def _state_for(self, request: TransportRequest) -> _OriginState:
        origin = _origin(request.url)
        async with self._states_lock:
            state = self._states.get(origin)
            if state is None:
                state = _OriginState(asyncio.BoundedSemaphore(self.concurrency))
                self._states[origin] = state
            return state


class RateLimitedTransport(CommerceTransport):
    """Apply one limiter to one physical route.

    Separate instances around direct and proxy backends prevent a paid route
    from consuming the target origin's direct-route concurrency or pacing
    allowance. The wrapper borrows its limiter and backend; closing it delegates
    to the wrapped backend for proxy-factory lifecycle compatibility.
    """

    def __init__(
        self,
        backend: CommerceTransport,
        limiter: RateLimiter,
        *,
        route: Literal["direct", "proxy"],
        telemetry: TelemetryHooks | None = None,
        telemetry_context: dict[str, JsonValue] | None = None,
    ) -> None:
        self._backend = backend
        self._limiter = limiter
        self._route = route
        self._telemetry = safe_telemetry(telemetry) if telemetry is not None else None
        self._telemetry_context = dict(telemetry_context or {})

    async def request(self, request: TransportRequest) -> TransportResponse:
        started = monotonic()
        await self._limiter.wait(request)
        if self._telemetry is not None:
            self._telemetry.emit(
                "rate_limit.wait",
                {
                    **self._telemetry_context,
                    **transport_trace_fields(request),
                    "level": "debug",
                    "route": self._route,
                    "url": request.url,
                    "purpose": request.purpose.value,
                    "elapsed_ms": round((monotonic() - started) * 1_000, 3),
                },
            )
        try:
            return await self._backend.request(request)
        finally:
            await self._limiter.release(request)

    async def rotate_identity(self, reason: RotationReason) -> None:
        await self._backend.rotate_identity(reason)

    @property
    def browser_subrequests_authorized(self) -> bool:
        """Preserve an authoritative browser backend's accounting marker."""

        return getattr(self._backend, "browser_subrequests_authorized", False) is True

    async def aclose(self) -> None:
        if hasattr(self._backend, "aclose"):
            await self._backend.aclose()  # type: ignore[attr-defined, unused-ignore]


def _origin(url: str) -> tuple[str, str, int]:
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as error:
        raise ValueError(f"request URL has an invalid origin: {url!r}") from error
    scheme = parsed.scheme.casefold()
    if scheme not in {"http", "https"} or hostname is None:
        raise ValueError(f"request URL must have an HTTP(S) origin: {url!r}")
    try:
        normalized_host = hostname.encode("idna").decode("ascii").casefold()
    except UnicodeError as error:
        raise ValueError(f"request URL has an invalid hostname: {url!r}") from error
    return scheme, normalized_host, port or (443 if scheme == "https" else 80)
