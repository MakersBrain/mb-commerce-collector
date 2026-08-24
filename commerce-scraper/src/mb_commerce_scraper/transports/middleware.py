from __future__ import annotations

import asyncio
from collections.abc import Callable
from time import monotonic
from urllib.parse import urlsplit
from uuid import uuid4

from pydantic import JsonValue

from .base import (
    DEFAULT_MAXIMUM_RESPONSE_BYTES,
    BudgetExhausted,
    CommerceTransport,
    RateLimiter,
    RequestBudget,
    RequestObservation,
    RequestObservationPhase,
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
        self._telemetry = safe_telemetry(telemetry) if telemetry is not None else None
        self._telemetry_context = dict(telemetry_context or {})
        if maximum_response_bytes < 1:
            raise ValueError("maximum_response_bytes must be positive")
        self._maximum_response_bytes = maximum_response_bytes

    async def request(self, request: TransportRequest) -> TransportResponse:
        cache_request = request
        stale: TransportResponse | None = None
        request_id = request.trace_request_id or (uuid4().hex if self._telemetry is not None else None)
        common: dict[str, JsonValue] = {
            **self._telemetry_context,
            "method": request.method.upper(),
            "url": request.url,
            "purpose": request.purpose.value,
            "priority": request.priority.name.casefold(),
            "required": request.required,
            "browser": request.browser.value,
            "estimated_bytes": request.estimated_bytes,
            "browser_action": (request.evaluation.action_id if request.evaluation is not None else None),
        }
        if request_id is not None:
            common["request_id"] = request_id
        self._emit("request.accepted", {**common, "level": "debug"})
        if (
            self._robots is not None
            and request.purpose.value != "robots"
            and not await self._robots.allowed(request.url)
        ):
            self._emit("robots.denied", {**common, "level": "warning"})
            raise RobotsDenied("robots policy denies request")
        if self._cache is not None and request.cache.value == "default":
            cached = await self._cache.get(request)
            if cached is not None:
                try:
                    cached = enforce_response_body_limit(cached, self._maximum_response_bytes)
                except ResponseBodyTooLarge as error:
                    self._emit(
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
                self._emit("cache.hit", {**common, "level": "debug", "route": "cache"})
                return cached.model_copy(update={"from_cache": True})
            self._emit("cache.miss", {**common, "level": "debug"})
            if isinstance(self._cache, StaleResponseCache):
                stale = await self._cache.stale(cache_request)
                if stale is not None:
                    try:
                        stale = enforce_response_body_limit(stale, self._maximum_response_bytes)
                    except ResponseBodyTooLarge as error:
                        self._emit(
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
            attempt_number = attempt + 1
            attempt_request = (
                request
                if request_id is None
                else request.model_copy(
                    update={
                        "trace_request_id": request_id,
                        "trace_attempt": attempt_number,
                    }
                )
            )
            authorization = (
                await self._budget.authorize(attempt_request) if self._budget is not None else None
            )
            if self._budget is not None and authorization is None:
                self._emit(
                    "budget.denied",
                    {**common, "level": "warning", "attempt": attempt + 1},
                )
                raise BudgetExhausted("request budget cannot authorize another attempt")
            rate_limit_acquired = False
            dispatched = False
            response_bytes = 0
            accounting = TransportAccounting(physical_requests=0)
            started = monotonic()
            transport_failure: TransportFailure | None = None
            try:
                if self._rate_limiter is not None:
                    rate_limit_started = monotonic()
                    await self._rate_limiter.wait(attempt_request)
                    rate_limit_acquired = True
                    self._emit(
                        "rate_limit.wait",
                        {
                            **common,
                            "level": "debug",
                            "elapsed_ms": round((monotonic() - rate_limit_started) * 1_000, 3),
                        },
                    )
                started = monotonic()
                self._emit(
                    "request.started",
                    {**common, "level": "debug", "attempt": attempt_number},
                )
                self._observe_request(
                    RequestObservationPhase.STARTED,
                    attempt_request,
                    attempt=attempt_number,
                )
                dispatched = True
                response = await self._backend.request(attempt_request)
                response_bytes = len(response.content)
                accounting = response.accounting or TransportAccounting(
                    transmitted_bytes=estimated_transmitted_bytes(attempt_request),
                    received_bytes=response_bytes,
                )
                response = enforce_response_body_limit(response, self._maximum_response_bytes)
            except ResponseBodyTooLarge as error:
                if not dispatched:
                    raise
                response_bytes = error.received_bytes
                accounting = error.accounting or TransportAccounting(
                    transmitted_bytes=estimated_transmitted_bytes(attempt_request),
                    received_bytes=error.received_bytes,
                )
                self._emit(
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
                self._observe_request(
                    RequestObservationPhase.FAILED,
                    attempt_request,
                    attempt=attempt_number,
                    accounting=accounting,
                    elapsed_seconds=monotonic() - started,
                    classification="response_body_too_large",
                )
                raise
            except TransportFailure as error:
                if not dispatched:
                    raise
                transport_failure = error
                accounting = error.accounting or TransportAccounting(
                    transmitted_bytes=estimated_transmitted_bytes(attempt_request),
                )
                self._emit(
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
                self._observe_request(
                    RequestObservationPhase.FAILED,
                    attempt_request,
                    attempt=attempt_number,
                    accounting=accounting,
                    elapsed_seconds=monotonic() - started,
                    classification="transport_failure",
                )
            except asyncio.CancelledError as error:
                if dispatched:
                    accounting = getattr(error, "accounting", None) or TransportAccounting(
                        transmitted_bytes=estimated_transmitted_bytes(attempt_request),
                    )
                    self._emit(
                        "request.failed",
                        {
                            **common,
                            "level": "warning",
                            "attempt": attempt_number,
                            "elapsed_ms": round((monotonic() - started) * 1_000, 3),
                            "error_type": type(error).__name__,
                            "cancelled": True,
                            "retryable": False,
                            "transmitted_bytes": accounting.transmitted_bytes,
                            "received_bytes": accounting.received_bytes,
                            "physical_requests": accounting.physical_requests,
                        },
                    )
                    self._observe_request(
                        RequestObservationPhase.FAILED,
                        attempt_request,
                        attempt=attempt_number,
                        accounting=accounting,
                        elapsed_seconds=monotonic() - started,
                        classification="cancelled",
                    )
                raise
            except Exception as error:
                if dispatched:
                    accounting = getattr(error, "accounting", None) or TransportAccounting(
                        transmitted_bytes=estimated_transmitted_bytes(attempt_request),
                    )
                    self._emit(
                        "request.failed",
                        {
                            **common,
                            "level": "warning",
                            "attempt": attempt_number,
                            "elapsed_ms": round((monotonic() - started) * 1_000, 3),
                            "error_type": type(error).__name__,
                            "transmitted_bytes": accounting.transmitted_bytes,
                            "received_bytes": accounting.received_bytes,
                            "physical_requests": accounting.physical_requests,
                        },
                    )
                    self._observe_request(
                        RequestObservationPhase.FAILED,
                        attempt_request,
                        attempt=attempt_number,
                        accounting=accounting,
                        elapsed_seconds=monotonic() - started,
                        classification="backend_failure",
                    )
                raise
            finally:
                cleanup_error: Exception | None = None
                cleanup_stage: str | None = None
                if self._rate_limiter is not None and rate_limit_acquired:
                    try:
                        await self._rate_limiter.release(attempt_request)
                    except Exception as error:  # noqa: BLE001 - terminal telemetry boundary
                        cleanup_error = error
                        cleanup_stage = "rate_limit_release"
                if authorization is not None:
                    try:
                        if dispatched:
                            await authorization.reconcile(response_bytes)
                        else:
                            await authorization.release()
                    except Exception as error:  # noqa: BLE001 - terminal telemetry boundary
                        cleanup_error = error
                        cleanup_stage = "budget_reconcile" if dispatched else "budget_release"
                if cleanup_error is not None:
                    if dispatched:
                        self._emit_attempt_failure(
                            common,
                            request=attempt_request,
                            error=cleanup_error,
                            accounting=accounting,
                            attempt=attempt_number,
                            started=started,
                            stage=cleanup_stage or "cleanup",
                        )
                    raise cleanup_error
            if transport_failure is not None:
                if attempt == self._retries:
                    if self._can_use_stale(request, stale):
                        return self._stale_fallback(
                            common,
                            stale,
                            reason="transport_failure",
                        )
                    raise transport_failure
                backoff_seconds = self._backoff(attempt)
                self._emit(
                    "request.retry",
                    {
                        **common,
                        "level": "debug",
                        "attempt": attempt_number,
                        "next_attempt": attempt_number + 1,
                        "classification": "transport_failure",
                        "backoff_ms": round(backoff_seconds * 1_000, 3),
                    },
                )
                self._observe_request(
                    RequestObservationPhase.RETRY,
                    attempt_request,
                    attempt=attempt_number,
                    classification="transport_failure",
                )
                if not self._can_use_stale(request, stale):
                    await self._rotate_identity(
                        RotationReason.TRANSPORT_FAILURE,
                        attempt_request,
                    )
                await asyncio.sleep(backoff_seconds)
                continue
            if response.status == 304 and stale is not None:
                revalidated = self._revalidated_response(stale, response)
                if self._cache is not None:
                    await self._cache_put(
                        cache_request,
                        revalidated,
                        common=common,
                        accounting=accounting,
                        attempt=attempt_number,
                        started=started,
                    )
                self._emit(
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
                    request=attempt_request,
                    response=response,
                    accounting=accounting,
                    attempt=attempt_number,
                    started=started,
                )
                return revalidated
            if response.status not in {403, 429, 500, 502, 503, 504} or attempt == self._retries:
                if self._cache is not None and response.status < 400:
                    await self._cache_put(
                        cache_request,
                        response,
                        common=common,
                        accounting=accounting,
                        attempt=attempt_number,
                        started=started,
                    )
                    self._emit(
                        "cache.write",
                        {**common, "level": "debug", "status": response.status},
                    )
                self._emit_completed(
                    common,
                    request=attempt_request,
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
            backoff_seconds = self._backoff(attempt)
            self._emit(
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
                    "backoff_ms": round(backoff_seconds * 1_000, 3),
                },
            )
            self._observe_request(
                RequestObservationPhase.RETRY,
                attempt_request,
                attempt=attempt_number,
                response=response,
                accounting=accounting,
                elapsed_seconds=monotonic() - started,
                classification="transient_status",
            )
            if response.status in {403, 429} and not self._can_use_stale(request, stale, response.status):
                await self._rotate_identity(
                    (RotationReason.BLOCKED if response.status == 403 else RotationReason.RATE_LIMITED),
                    attempt_request,
                )
            await asyncio.sleep(backoff_seconds)
        raise AssertionError("unreachable")

    async def rotate_identity(self, reason: RotationReason) -> None:
        await self._backend.rotate_identity(reason)

    def _emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        if self._telemetry is not None:
            self._telemetry.emit(event, fields)

    async def _cache_put(
        self,
        request: TransportRequest,
        response: TransportResponse,
        *,
        common: dict[str, JsonValue],
        accounting: TransportAccounting,
        attempt: int,
        started: float,
    ) -> None:
        assert self._cache is not None
        try:
            await self._cache.put(request, response)
        except BaseException as error:
            self._emit_attempt_failure(
                common,
                request=request,
                error=error,
                accounting=accounting,
                attempt=attempt,
                started=started,
                stage="cache_write",
            )
            raise

    def _emit_attempt_failure(
        self,
        common: dict[str, JsonValue],
        *,
        request: TransportRequest,
        error: BaseException,
        accounting: TransportAccounting,
        attempt: int,
        started: float,
        stage: str,
    ) -> None:
        self._emit(
            "request.failed",
            {
                **common,
                "level": "warning",
                "attempt": attempt,
                "elapsed_ms": round((monotonic() - started) * 1_000, 3),
                "error_type": type(error).__name__,
                "failure_stage": stage,
                "retryable": False,
                "transmitted_bytes": accounting.transmitted_bytes,
                "received_bytes": accounting.received_bytes,
                "physical_requests": accounting.physical_requests,
            },
        )
        self._observe_request(
            RequestObservationPhase.FAILED,
            request,
            attempt=attempt,
            accounting=accounting,
            elapsed_seconds=monotonic() - started,
            classification=stage,
        )

    async def _rotate_identity(
        self,
        reason: RotationReason,
        request: TransportRequest,
    ) -> None:
        traced_rotation = getattr(
            self._backend,
            "rotate_identity_for_request",
            None,
        )
        if traced_rotation is not None:
            await traced_rotation(reason, request)
            return
        await self._backend.rotate_identity(reason)

    @staticmethod
    def _conditional_request(request: TransportRequest, stale: TransportResponse) -> TransportRequest:
        if request.method.upper() not in {"GET", "HEAD"} or request.browser.value != "never":
            return request
        headers = dict(request.headers)
        existing = {name.casefold() for name in headers}
        for cached_name, request_name in (
            ("etag", "if-none-match"),
            ("last-modified", "if-modified-since"),
        ):
            value = next(
                (value for name, value in stale.headers.items() if name.casefold() == cached_name),
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
            and (status is None or status in {408, 425, 429} or status >= 500)
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
        self._emit("cache.stale_used", fields)
        return stale.model_copy(update={"route": RouteMetadata(kind="cache"), "from_cache": True})

    @staticmethod
    def _revalidated_response(stale: TransportResponse, response: TransportResponse) -> TransportResponse:
        headers = dict(stale.headers)
        retained = {"content-type", "content-language", "last-modified", "etag"}
        headers.update(
            {name: value for name, value in response.headers.items() if name.casefold() in retained}
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
        request: TransportRequest,
        response: TransportResponse,
        accounting: TransportAccounting,
        attempt: int,
        started: float,
    ) -> None:
        self._emit(
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
        self._observe_request(
            RequestObservationPhase.COMPLETED,
            request,
            attempt=attempt,
            response=response,
            accounting=accounting,
            elapsed_seconds=monotonic() - started,
        )

    def _observe_request(
        self,
        phase: RequestObservationPhase,
        request: TransportRequest,
        *,
        attempt: int,
        accounting: TransportAccounting | None = None,
        response: TransportResponse | None = None,
        elapsed_seconds: float | None = None,
        classification: str | None = None,
    ) -> None:
        if self._telemetry is None:
            return
        source = self._telemetry_context.get("source_id")
        self._telemetry.observe_request(
            RequestObservation(
                phase=phase,
                request_id=request.trace_request_id,
                attempt=attempt,
                source_id=source if isinstance(source, str) and source else None,
                target_host=(urlsplit(request.url).hostname or "").casefold(),
                method=request.method.upper(),
                purpose=request.purpose,
                status=response.status if response is not None else None,
                elapsed_seconds=elapsed_seconds,
                route=response.route if response is not None else None,
                accounting=accounting
                or TransportAccounting(physical_requests=0),
                classification=classification,
            )
        )
