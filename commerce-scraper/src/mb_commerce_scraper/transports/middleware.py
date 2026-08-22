from __future__ import annotations

import asyncio
from collections.abc import Callable
from time import monotonic
from uuid import uuid4

from pydantic import JsonValue

from .base import (
    DEFAULT_MAXIMUM_RESPONSE_BYTES,
    BudgetExhausted,
    CommerceTransport,
    NullTelemetry,
    RateLimiter,
    RequestBudget,
    ResponseBodyTooLarge,
    ResponseCache,
    RobotsChecker,
    RotationReason,
    RouteMetadata,
    StaleResponseCache,
    TelemetryHooks,
    TransportAccounting,
    TransportFailure,
    TransportRequest,
    TransportResponse,
    enforce_response_body_limit,
    estimated_transmitted_bytes,
)
from .telemetry import safe_telemetry


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
        stale_on_error: bool = False,
        robots: RobotsChecker | None = None,
        rate_limiter: RateLimiter | None = None,
        retries: int = 2,
        backoff: Callable[[int], float] = lambda attempt: 0.1 * 2**attempt,
        telemetry: TelemetryHooks | None = None,
        telemetry_context: dict[str, JsonValue] | None = None,
        maximum_response_bytes: int = DEFAULT_MAXIMUM_RESPONSE_BYTES,
    ) -> None:
        self._backend = backend
        self._budget = budget
        self._cache = cache
        self._stale_on_error = stale_on_error
        self._robots = robots
        self._rate_limiter = rate_limiter
        if retries < 0:
            raise ValueError("retries must be non-negative")
        self._retries = retries
        self._backoff = backoff
        self._telemetry = safe_telemetry(telemetry or NullTelemetry())
        self._telemetry_context = dict(telemetry_context or {})
        if maximum_response_bytes < 1:
            raise ValueError("maximum_response_bytes must be positive")
        self._maximum_response_bytes = maximum_response_bytes

    async def request(self, request: TransportRequest) -> TransportResponse:
        cache_request = request
        stale: TransportResponse | None = None
        request_id = uuid4().hex
        common = {
            **self._telemetry_context,
            "request_id": request_id,
            "method": request.method.upper(),
            "url": request.url,
            "purpose": request.purpose.value,
            "priority": request.priority.name.casefold(),
            "required": request.required,
            "browser": request.browser.value,
            "estimated_bytes": request.estimated_bytes,
            "browser_action": (
                request.evaluation.action_id
                if request.evaluation is not None
                else None
            ),
        }
        if (
            self._robots is not None
            and request.purpose.value != "robots"
            and not await self._robots.allowed(request.url)
        ):
            self._telemetry.emit(
                "robots.denied", {**common, "level": "warning"}
            )
            raise RobotsDenied("robots policy denies request")
        if self._cache is not None and request.cache.value == "default":
            cached = await self._cache.get(request)
            if cached is not None:
                try:
                    cached = enforce_response_body_limit(
                        cached, self._maximum_response_bytes
                    )
                except ResponseBodyTooLarge as error:
                    self._telemetry.emit(
                        "cache.rejected",
                        {
                            **common,
                            "level": "warning",
                            "reason": "response_body_too_large",
                            "received_bytes": error.received_bytes,
                            "maximum_bytes": error.maximum_bytes,
                        },
                    )
                    raise
                self._telemetry.emit(
                    "cache.hit", {**common, "level": "debug", "route": "cache"}
                )
                return cached.model_copy(update={"from_cache": True})
            self._telemetry.emit("cache.miss", {**common, "level": "debug"})
            if isinstance(self._cache, StaleResponseCache):
                stale = await self._cache.stale(cache_request)
                if stale is not None:
                    try:
                        stale = enforce_response_body_limit(
                            stale, self._maximum_response_bytes
                        )
                    except ResponseBodyTooLarge as error:
                        self._telemetry.emit(
                            "cache.rejected",
                            {
                                **common,
                                "level": "warning",
                                "reason": "stale_response_body_too_large",
                                "received_bytes": error.received_bytes,
                                "maximum_bytes": error.maximum_bytes,
                            },
                        )
                        raise
                    request = self._conditional_request(request, stale)
        for attempt in range(self._retries + 1):
            authorization = (
                await self._budget.authorize(request)
                if self._budget is not None
                else None
            )
            if self._budget is not None and authorization is None:
                self._telemetry.emit(
                    "budget.denied",
                    {**common, "level": "warning", "attempt": attempt + 1},
                )
                raise BudgetExhausted("request budget cannot authorize another attempt")
            attempt_number = attempt + 1
            rate_limit_acquired = False
            dispatched = False
            response_bytes = 0
            accounting = TransportAccounting(physical_requests=0)
            started = monotonic()
            transport_failure: TransportFailure | None = None
            try:
                if self._rate_limiter is not None:
                    rate_limit_started = monotonic()
                    await self._rate_limiter.wait(request)
                    rate_limit_acquired = True
                    self._telemetry.emit(
                        "rate_limit.wait",
                        {
                            **common,
                            "level": "debug",
                            "elapsed_ms": round(
                                (monotonic() - rate_limit_started) * 1_000, 3
                            ),
                        },
                    )
                started = monotonic()
                self._telemetry.emit(
                    "request.started",
                    {**common, "level": "debug", "attempt": attempt_number},
                )
                dispatched = True
                response = await self._backend.request(request)
                response_bytes = len(response.content)
                accounting = response.accounting or TransportAccounting(
                    transmitted_bytes=estimated_transmitted_bytes(request),
                    received_bytes=response_bytes,
                )
                response = enforce_response_body_limit(
                    response, self._maximum_response_bytes
                )
            except ResponseBodyTooLarge as error:
                if not dispatched:
                    raise
                response_bytes = error.received_bytes
                accounting = error.accounting or TransportAccounting(
                    transmitted_bytes=estimated_transmitted_bytes(request),
                    received_bytes=error.received_bytes,
                )
                self._telemetry.emit(
                    "request.failed",
                    {
                        **common,
                        "level": "warning",
                        "attempt": attempt_number,
                        "elapsed_ms": round((monotonic() - started) * 1_000, 3),
                        "error_type": type(error).__name__,
                        "retryable": False,
                        "received_bytes": error.received_bytes,
                        "transmitted_bytes": accounting.transmitted_bytes,
                        "physical_requests": accounting.physical_requests,
                        "maximum_bytes": error.maximum_bytes,
                    },
                )
                raise
            except TransportFailure as error:
                if not dispatched:
                    raise
                transport_failure = error
                accounting = error.accounting or TransportAccounting(
                    transmitted_bytes=estimated_transmitted_bytes(request),
                )
                self._telemetry.emit(
                    "request.failed",
                    {
                        **common,
                        "level": "warning",
                        "attempt": attempt_number,
                        "elapsed_ms": round((monotonic() - started) * 1_000, 3),
                        "error_type": type(error).__name__,
                        "retryable": True,
                        "transmitted_bytes": accounting.transmitted_bytes,
                        "received_bytes": accounting.received_bytes,
                        "physical_requests": accounting.physical_requests,
                    },
                )
            except asyncio.CancelledError as error:
                if dispatched:
                    accounting = getattr(
                        error, "accounting", None
                    ) or TransportAccounting(
                        transmitted_bytes=estimated_transmitted_bytes(request),
                    )
                    self._telemetry.emit(
                        "request.failed",
                        {
                            **common,
                            "level": "warning",
                            "attempt": attempt_number,
                            "elapsed_ms": round(
                                (monotonic() - started) * 1_000, 3
                            ),
                            "error_type": type(error).__name__,
                            "cancelled": True,
                            "retryable": False,
                            "transmitted_bytes": accounting.transmitted_bytes,
                            "received_bytes": accounting.received_bytes,
                            "physical_requests": accounting.physical_requests,
                        },
                    )
                raise
            except Exception as error:
                if dispatched:
                    accounting = getattr(error, "accounting", None) or TransportAccounting(
                        transmitted_bytes=estimated_transmitted_bytes(request),
                    )
                    self._telemetry.emit(
                        "request.failed",
                        {
                            **common,
                            "level": "warning",
                            "attempt": attempt_number,
                            "elapsed_ms": round(
                                (monotonic() - started) * 1_000, 3
                            ),
                            "error_type": type(error).__name__,
                            "transmitted_bytes": accounting.transmitted_bytes,
                            "received_bytes": accounting.received_bytes,
                            "physical_requests": accounting.physical_requests,
                        },
                    )
                raise
            finally:
                try:
                    if self._rate_limiter is not None and rate_limit_acquired:
                        await self._rate_limiter.release(request)
                finally:
                    if authorization is not None:
                        if dispatched:
                            await authorization.reconcile(response_bytes)
                        else:
                            await authorization.release()
            if transport_failure is not None:
                if attempt == self._retries:
                    if self._can_use_stale(request, stale):
                        return self._stale_fallback(
                            common,
                            stale,
                            reason="transport_failure",
                        )
                    raise transport_failure
                self._telemetry.emit(
                    "request.retry",
                    {
                        **common,
                        "level": "debug",
                        "attempt": attempt_number,
                        "next_attempt": attempt_number + 1,
                        "classification": "transport_failure",
                    },
                )
                if not self._can_use_stale(request, stale):
                    await self._backend.rotate_identity(
                        RotationReason.TRANSPORT_FAILURE
                    )
                await asyncio.sleep(self._backoff(attempt))
                continue
            if response.status == 304 and stale is not None:
                revalidated = self._revalidated_response(stale, response)
                if self._cache is not None:
                    await self._cache.put(cache_request, revalidated)
                self._telemetry.emit(
                    "cache.revalidated",
                    {
                        **common,
                        "level": "debug",
                        "status": 304,
                        "saved_bytes": len(stale.content),
                    },
                )
                self._emit_completed(
                    common,
                    response=response,
                    accounting=accounting,
                    attempt=attempt_number,
                    started=started,
                )
                return revalidated
            if response.status not in {403, 429, 500, 502, 503, 504} or attempt == self._retries:
                if self._cache is not None and response.status < 400:
                    await self._cache.put(cache_request, response)
                    self._telemetry.emit(
                        "cache.write",
                        {**common, "level": "debug", "status": response.status},
                    )
                self._emit_completed(
                    common,
                    response=response,
                    accounting=accounting,
                    attempt=attempt_number,
                    started=started,
                )
                if self._can_use_stale(request, stale, response.status):
                    return self._stale_fallback(
                        common,
                        stale,
                        reason="transient_status",
                        status=response.status,
                    )
                return response
            self._telemetry.emit(
                "request.retry",
                {
                    **common,
                    "level": "debug",
                    "status": response.status,
                    "attempt": attempt_number,
                    "next_attempt": attempt_number + 1,
                    "elapsed_ms": round((monotonic() - started) * 1_000, 3),
                    "route": response.route.kind,
                    "provider": response.route.provider,
                    "transmitted_bytes": accounting.transmitted_bytes,
                    "received_bytes": accounting.received_bytes,
                    "physical_requests": accounting.physical_requests,
                },
            )
            if response.status in {403, 429} and not self._can_use_stale(
                request, stale, response.status
            ):
                await self._backend.rotate_identity(
                    RotationReason.BLOCKED
                    if response.status == 403
                    else RotationReason.RATE_LIMITED
                )
            await asyncio.sleep(self._backoff(attempt))
        raise AssertionError("unreachable")

    async def rotate_identity(self, reason: RotationReason) -> None:
        await self._backend.rotate_identity(reason)

    @staticmethod
    def _conditional_request(
        request: TransportRequest, stale: TransportResponse
    ) -> TransportRequest:
        if request.method.upper() not in {"GET", "HEAD"} or request.browser.value != "never":
            return request
        headers = dict(request.headers)
        existing = {name.casefold() for name in headers}
        for cached_name, request_name in (
            ("etag", "if-none-match"),
            ("last-modified", "if-modified-since"),
        ):
            value = next(
                (
                    value
                    for name, value in stale.headers.items()
                    if name.casefold() == cached_name
                ),
                None,
            )
            if value is not None and request_name not in existing:
                headers[request_name] = value
        return request.model_copy(update={"headers": headers})

    def _can_use_stale(
        self,
        request: TransportRequest,
        stale: TransportResponse | None,
        status: int | None = None,
    ) -> bool:
        return (
            self._stale_on_error
            and stale is not None
            and request.method.upper() == "GET"
            and request.browser.value == "never"
            and (
                status is None
                or status in {408, 425, 429}
                or status >= 500
            )
        )

    def _stale_fallback(
        self,
        common: dict[str, JsonValue],
        stale: TransportResponse | None,
        *,
        reason: str,
        status: int | None = None,
    ) -> TransportResponse:
        assert stale is not None
        fields: dict[str, JsonValue] = {
            **common,
            "level": "warning",
            "reason": reason,
        }
        if status is not None:
            fields["status"] = status
        self._telemetry.emit("cache.stale_used", fields)
        return stale.model_copy(
            update={"route": RouteMetadata(kind="cache"), "from_cache": True}
        )

    @staticmethod
    def _revalidated_response(
        stale: TransportResponse, response: TransportResponse
    ) -> TransportResponse:
        headers = dict(stale.headers)
        retained = {"content-type", "content-language", "last-modified", "etag"}
        headers.update(
            {
                name: value
                for name, value in response.headers.items()
                if name.casefold() in retained
            }
        )
        return TransportResponse(
            status=stale.status,
            headers=headers,
            content=stale.content,
            final_url=stale.final_url,
            route=response.route,
            accounting=response.accounting,
            from_cache=True,
        )

    def _emit_completed(
        self,
        common: dict[str, JsonValue],
        *,
        response: TransportResponse,
        accounting: TransportAccounting,
        attempt: int,
        started: float,
    ) -> None:
        self._telemetry.emit(
            "request.completed",
            {
                **common,
                "level": "debug" if response.status < 400 else "warning",
                "status": response.status,
                "attempt": attempt,
                "elapsed_ms": round((monotonic() - started) * 1_000, 3),
                "route": response.route.kind,
                "provider": response.route.provider,
                "transmitted_bytes": accounting.transmitted_bytes,
                "received_bytes": accounting.received_bytes,
                "physical_requests": accounting.physical_requests,
            },
        )
