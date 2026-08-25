from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from time import monotonic
from typing import Protocol
from urllib.parse import urlsplit

from pydantic import JsonValue

from mb_commerce_scraper.models import ProxyMode, ProxyPolicyConfig
from mb_commerce_scraper.transports import (
    CommerceTransport,
    ResponseBodyTooLarge,
    RotationReason,
    RouteMetadata,
    TransportAccounting,
    TransportFailure,
    TransportRequest,
    TransportResponse,
    estimated_transmitted_bytes,
    safe_telemetry,
)
from mb_commerce_scraper.transports.base import (
    TelemetryHooks,
    browser_subrequests_authorized,
    transport_trace_fields,
)

from .base import (
    BrowserSubrequestAuthorization,
    BrowserSubrequestAuthorizer,
    BrowserSubrequestOutcome,
    ProxyAttemptAuthorization,
    ProxyBudgetExhausted,
    ProxyLease,
    ProxyOutcome,
    ProxyPool,
    ProxyRequest,
)


class ProxyTransportFactory(Protocol):
    def build(self, lease: ProxyLease) -> CommerceTransport: ...


class ProxyBrowserTransportFactory(Protocol):
    """Build one browser transport bound to the supplied sticky lease."""

    def build(
        self,
        lease: ProxyLease,
        authorizer: BrowserSubrequestAuthorizer,
    ) -> CommerceTransport: ...


@dataclass(slots=True)
class _PoolBrowserSubrequestAuthorization:
    authorization: ProxyAttemptAuthorization
    target_host: str

    async def reconcile(self, outcome: BrowserSubrequestOutcome) -> None:
        await self.authorization.reconcile(
            ProxyOutcome(
                target_host=self.target_host,
                status=outcome.status,
                physical_requests=1,
                transmitted_bytes=outcome.transmitted_bytes,
                received_bytes=outcome.received_bytes,
                classification=outcome.classification,
            )
        )

    async def release(self) -> None:
        await self.authorization.release()


@dataclass(slots=True)
class PoolBrowserSubrequestAuthorizer:
    """Bind a neutral pool and lease to the browser callback contract."""

    pool: ProxyPool
    lease: ProxyLease
    target_host: str

    async def authorize(self, estimated_bytes: int) -> BrowserSubrequestAuthorization | None:
        authorization = await self.pool.authorize(self.lease, estimated_bytes)
        if authorization is None:
            return None
        return _PoolBrowserSubrequestAuthorization(authorization, self.target_host)


@dataclass(frozen=True)
class _CheckedOutRoute:
    generation: int
    lease: ProxyLease
    transport: CommerceTransport


class RoutedTransport(CommerceTransport):
    """Attempt-scoped direct/proxy selection with one sticky owned lease."""

    def __init__(
        self,
        direct: CommerceTransport,
        *,
        pool: ProxyPool,
        proxy_factory: ProxyTransportFactory,
        policy: ProxyPolicyConfig,
        source_id: str,
        base_url: str,
        telemetry: TelemetryHooks | None = None,
        telemetry_context: dict[str, JsonValue] | None = None,
    ) -> None:
        host = urlsplit(base_url).hostname
        if host is None:
            raise ValueError("proxy routing requires an absolute base URL")
        self._direct = direct
        self._pool = pool
        self._proxy_factory = proxy_factory
        self._policy = policy
        self._proxy_request = ProxyRequest(
            source_id=source_id,
            target_host=host,
            country=policy.country,
            maximum_requests=policy.maximum_requests,
            maximum_bytes=policy.maximum_bytes,
            preferred_providers=policy.provider_preferences,
        )
        self._telemetry = safe_telemetry(telemetry) if telemetry is not None else None
        self._telemetry_context = dict(telemetry_context or {})
        self._lease: ProxyLease | None = None
        self._proxy: CommerceTransport | None = None
        self._pending_rotation: RotationReason | None = None
        self._state_changed = asyncio.Condition()
        self._active_proxy_requests = 0
        self._transitioning = False
        self._generation = 0
        self._closed = False

    async def request(self, request: TransportRequest) -> TransportResponse:
        if self._policy.mode == ProxyMode.NEVER:
            await self._ensure_open()
            return await self._direct.request(request)
        direct_generation = await self._direct_route_generation()
        if direct_generation is not None:
            # Selection and dispatch stay attempt-scoped. The retry middleware
            # alone decides whether a typed outcome warrants another attempt;
            # a terminal response must not silently change the route used by
            # the connector's next independent request.
            return await self._direct.request(request)
        return await self._proxy_request_attempt(request)

    async def rotate_identity(self, reason: RotationReason) -> None:
        await self._rotate_identity(reason, request=None)

    async def rotate_identity_for_request(
        self,
        reason: RotationReason,
        request: TransportRequest,
    ) -> None:
        """Rotate while retaining the triggering attempt's trace identity."""

        await self._rotate_identity(reason, request=request)

    async def _rotate_identity(
        self,
        reason: RotationReason,
        *,
        request: TransportRequest | None,
    ) -> None:
        async with self._state_changed:
            if self._closed:
                raise RuntimeError("routed transport is closed")
            if self._transitioning:
                while self._transitioning and not self._closed:
                    await self._state_changed.wait()
                if self._closed:
                    raise RuntimeError("routed transport is closed")
                if self._lease is not None:
                    return
            if self._pending_rotation is None:
                self._pending_rotation = reason

        await self._checkout_proxy_route(request)
        await asyncio.shield(self._checkin_proxy_route())

    async def aclose(self) -> None:
        async with self._state_changed:
            if self._closed:
                return
            self._closed = True
            self._state_changed.notify_all()
            while self._transitioning or self._active_proxy_requests:
                await self._state_changed.wait()
            self._transitioning = True
            lease = self._lease
            proxy = self._proxy
            self._lease = None
            self._proxy = None
        try:
            try:
                await self._close_transport(proxy)
            finally:
                if lease is not None:
                    started = monotonic()
                    self._emit("proxy.release.started", self._route_fields(lease))
                    try:
                        await self._pool.release(lease)
                    except BaseException as error:
                        self._emit_failure("proxy.release.failed", error, lease=lease, started=started)
                        raise
                    self._emit(
                        "proxy.release.completed",
                        {
                            **self._route_fields(lease),
                            "elapsed_ms": self._elapsed_ms(started),
                        },
                    )
        finally:
            async with self._state_changed:
                self._transitioning = False
                self._state_changed.notify_all()

    async def _proxy_request_attempt(
        self,
        request: TransportRequest,
    ) -> TransportResponse:
        route = await self._checkout_proxy_route(request)
        authorization = None
        owns_browser_authorization = request.browser.value == "required" and (
            browser_subrequests_authorized(route.transport)
        )
        dispatched = False
        outcome: ProxyOutcome | None = None
        try:
            if not owns_browser_authorization:
                authorization = await self._pool.authorize(
                    route.lease,
                    request.estimated_bytes + estimated_transmitted_bytes(request),
                )
                if authorization is None:
                    raise ProxyBudgetExhausted(
                        maximum_requests=self._proxy_request.maximum_requests,
                        maximum_bytes=self._proxy_request.maximum_bytes,
                    )
            dispatched = True
            try:
                response = await route.transport.request(request)
            except asyncio.CancelledError as error:
                cancelled_accounting = getattr(error, "accounting", None)
                accounting = self._attempt_accounting(
                    request,
                    (cancelled_accounting if isinstance(cancelled_accounting, TransportAccounting) else None),
                )
                outcome = ProxyOutcome(
                    target_host=self._proxy_request.target_host,
                    physical_requests=accounting.physical_requests,
                    transmitted_bytes=accounting.transmitted_bytes,
                    received_bytes=accounting.received_bytes,
                    classification="cancelled",
                )
                raise
            except ResponseBodyTooLarge as error:
                accounting = self._attempt_accounting(
                    request,
                    error.accounting,
                    received_bytes=error.received_bytes,
                )
                outcome = ProxyOutcome(
                    target_host=self._proxy_request.target_host,
                    physical_requests=accounting.physical_requests,
                    transmitted_bytes=accounting.transmitted_bytes,
                    received_bytes=accounting.received_bytes,
                    classification="response_body_too_large",
                )
                await self._report(route, outcome, request=request)
                raise
            except TransportFailure as error:
                accounting = self._attempt_accounting(request, error.accounting)
                outcome = ProxyOutcome(
                    target_host=self._proxy_request.target_host,
                    physical_requests=accounting.physical_requests,
                    transmitted_bytes=accounting.transmitted_bytes,
                    received_bytes=accounting.received_bytes,
                    classification="transport_failure",
                )
                await self._report(route, outcome, request=request)
                if self._policy.mode == ProxyMode.FAILOVER:
                    await self._schedule_rotation(
                        RotationReason.TRANSPORT_FAILURE,
                        generation=route.generation,
                    )
                raise
            accounting = self._attempt_accounting(
                request,
                response.accounting,
                received_bytes=len(response.content),
            )
            classification = (
                "blocked"
                if response.status == 403
                else "rate_limited"
                if response.status == 429
                else "success"
                if response.status < 400
                else "http_error"
            )
            outcome = ProxyOutcome(
                target_host=self._proxy_request.target_host,
                status=response.status,
                physical_requests=accounting.physical_requests,
                transmitted_bytes=accounting.transmitted_bytes,
                received_bytes=accounting.received_bytes,
                classification=classification,
            )
            await self._report(route, outcome, request=request)
            if self._policy.mode == ProxyMode.FAILOVER:
                await self._remember_blocked_route(response, generation=route.generation)
            return response.model_copy(
                update={
                    "route": RouteMetadata(
                        kind=("browser" if response.route.kind == "browser" else "proxy"),
                        provider=route.lease.provider,
                        endpoint_id=route.lease.route.endpoint_id,
                        lease_id=route.lease.lease_id,
                    )
                }
            )
        finally:
            try:
                if authorization is not None:
                    if dispatched:
                        if outcome is None:
                            accounting = self._attempt_accounting(request, None)
                            outcome = ProxyOutcome(
                                target_host=self._proxy_request.target_host,
                                physical_requests=accounting.physical_requests,
                                transmitted_bytes=accounting.transmitted_bytes,
                                received_bytes=accounting.received_bytes,
                                classification="backend_failure",
                            )
                        await asyncio.shield(authorization.reconcile(outcome))
                    else:
                        await asyncio.shield(authorization.release())
            finally:
                await asyncio.shield(self._checkin_proxy_route())

    @staticmethod
    def _attempt_accounting(
        request: TransportRequest,
        accounting: TransportAccounting | None,
        *,
        received_bytes: int = 0,
    ) -> TransportAccounting:
        minimum_transmitted_bytes = estimated_transmitted_bytes(request)
        if accounting is not None:
            return TransportAccounting(
                physical_requests=max(1, accounting.physical_requests),
                transmitted_bytes=max(minimum_transmitted_bytes, accounting.transmitted_bytes),
                received_bytes=max(received_bytes, accounting.received_bytes),
            )
        return TransportAccounting(
            physical_requests=1,
            transmitted_bytes=minimum_transmitted_bytes,
            received_bytes=received_bytes,
        )

    async def _direct_route_generation(self) -> int | None:
        async with self._state_changed:
            if self._closed:
                raise RuntimeError("routed transport is closed")
            if (
                self._policy.mode == ProxyMode.FALLBACK
                and self._lease is None
                and self._pending_rotation is None
                and not self._transitioning
            ):
                return self._generation
            return None

    async def _checkout_proxy_route(
        self,
        request: TransportRequest | None = None,
    ) -> _CheckedOutRoute:
        while True:
            async with self._state_changed:
                if self._closed:
                    raise RuntimeError("routed transport is closed")
                if self._transitioning:
                    await self._state_changed.wait()
                    continue
                if self._lease is not None and self._pending_rotation is None:
                    assert self._proxy is not None
                    self._active_proxy_requests += 1
                    return _CheckedOutRoute(self._generation, self._lease, self._proxy)
                if self._active_proxy_requests:
                    await self._state_changed.wait()
                    continue
                self._transitioning = True
                old_lease = self._lease
                old_proxy = self._proxy
                reason = self._pending_rotation
                self._lease = None
                self._proxy = None
                self._pending_rotation = None

            try:
                lease, proxy = await self._replace_route(
                    old_lease,
                    old_proxy,
                    reason,
                    request=request,
                )
            except BaseException:
                async with self._state_changed:
                    self._transitioning = False
                    self._state_changed.notify_all()
                raise

            async with self._state_changed:
                self._lease = lease
                self._proxy = proxy
                self._generation += 1
                self._transitioning = False
                self._state_changed.notify_all()

    async def _replace_route(
        self,
        old_lease: ProxyLease | None,
        old_proxy: CommerceTransport | None,
        reason: RotationReason | None,
        *,
        request: TransportRequest | None,
    ) -> tuple[ProxyLease, CommerceTransport]:
        started = monotonic()
        trace_fields = transport_trace_fields(request) if request is not None else {}
        if old_lease is None:
            self._emit(
                "proxy.acquire.started",
                {
                    **trace_fields,
                    "target_host": self._proxy_request.target_host,
                },
            )
            try:
                lease = await self._pool.acquire(self._proxy_request)
            except BaseException as error:
                self._emit_failure(
                    "proxy.acquire.failed",
                    error,
                    started=started,
                    request=request,
                )
                raise
            self._emit(
                "proxy.acquire.completed",
                {
                    **trace_fields,
                    **self._route_fields(lease),
                    "elapsed_ms": self._elapsed_ms(started),
                },
            )
        else:
            rotation_reason = reason or RotationReason.EXPLICIT
            self._emit(
                "proxy.rotate.started",
                {
                    **trace_fields,
                    **self._route_fields(old_lease),
                    "reason": rotation_reason.value,
                    "level": self._rotation_level(rotation_reason),
                },
            )
            try:
                await self._close_transport(old_proxy)
                lease = await self._pool.rotate(
                    old_lease,
                    rotation_reason,
                )
            except BaseException as error:
                self._emit_failure(
                    "proxy.rotate.failed",
                    error,
                    lease=old_lease,
                    started=started,
                    reason=rotation_reason,
                    request=request,
                )
                with suppress(Exception):
                    await asyncio.shield(self._pool.release(old_lease))
                raise
            self._emit(
                "proxy.rotate.completed",
                {
                    **trace_fields,
                    **self._route_fields(lease),
                    "previous_provider": old_lease.provider,
                    "reason": rotation_reason.value,
                    "level": self._rotation_level(rotation_reason),
                    "elapsed_ms": self._elapsed_ms(started),
                },
            )
        try:
            proxy = self._proxy_factory.build(lease)
        except BaseException:
            with suppress(Exception):
                await asyncio.shield(self._pool.release(lease))
            raise
        return lease, proxy

    async def _checkin_proxy_route(self) -> None:
        async with self._state_changed:
            self._active_proxy_requests -= 1
            if self._active_proxy_requests < 0:
                raise AssertionError("proxy route checkout count became negative")
            self._state_changed.notify_all()

    async def _report(
        self,
        route: _CheckedOutRoute,
        outcome: ProxyOutcome,
        *,
        request: TransportRequest,
    ) -> None:
        started = monotonic()
        try:
            await self._pool.report(route.lease, outcome)
        except BaseException as error:
            self._emit_failure(
                "proxy.report.failed",
                error,
                lease=route.lease,
                started=started,
                request=request,
            )
            raise
        self._emit(
            "proxy.outcome",
            {
                **transport_trace_fields(request),
                **self._route_fields(route.lease),
                "level": ("debug" if outcome.classification == "success" else "warning"),
                "classification": outcome.classification,
                "status": outcome.status,
                "physical_requests": outcome.physical_requests,
                "transmitted_bytes": outcome.transmitted_bytes,
                "received_bytes": outcome.received_bytes,
                "elapsed_ms": self._elapsed_ms(started),
            },
        )

    async def _remember_blocked_route(
        self,
        response: TransportResponse,
        *,
        generation: int | None = None,
    ) -> None:
        if response.status == 403:
            await self._schedule_rotation(RotationReason.BLOCKED, generation=generation)
        elif response.status == 429:
            await self._schedule_rotation(RotationReason.RATE_LIMITED, generation=generation)

    async def _schedule_rotation(
        self,
        reason: RotationReason,
        *,
        generation: int | None = None,
    ) -> None:
        async with self._state_changed:
            if self._closed or self._transitioning:
                return
            if generation is not None and generation != self._generation:
                return
            if self._pending_rotation is None:
                self._pending_rotation = reason
            self._state_changed.notify_all()

    async def _ensure_open(self) -> None:
        async with self._state_changed:
            if self._closed:
                raise RuntimeError("routed transport is closed")

    def _emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        if self._telemetry is not None:
            self._telemetry.emit(
                event,
                {**self._telemetry_context, "level": "debug", **fields},
            )

    def _emit_failure(
        self,
        event: str,
        error: BaseException,
        *,
        lease: ProxyLease | None = None,
        started: float | None = None,
        reason: RotationReason | None = None,
        request: TransportRequest | None = None,
    ) -> None:
        fields: dict[str, JsonValue] = {
            **(transport_trace_fields(request) if request is not None else {}),
            "level": "warning",
            "error_type": type(error).__name__,
        }
        if lease is not None:
            fields.update(self._route_fields(lease))
        if started is not None:
            fields["elapsed_ms"] = self._elapsed_ms(started)
        if reason is not None:
            fields["reason"] = reason.value
        self._emit(event, fields)

    @staticmethod
    def _route_fields(lease: ProxyLease) -> dict[str, JsonValue]:
        return {
            "provider": lease.provider,
            "endpoint_id": lease.route.endpoint_id,
            "lease_id": lease.lease_id,
        }

    @staticmethod
    def _rotation_level(reason: RotationReason) -> str:
        return "debug" if reason is RotationReason.EXPLICIT else "warning"

    @staticmethod
    def _elapsed_ms(started: float) -> float:
        return round((monotonic() - started) * 1_000, 3)

    @staticmethod
    async def _close_transport(transport: CommerceTransport | None) -> None:
        if transport is not None and hasattr(transport, "aclose"):
            await transport.aclose()  # type: ignore[attr-defined, unused-ignore]
