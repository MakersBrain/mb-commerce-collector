from __future__ import annotations

from typing import Protocol
from urllib.parse import urlsplit

from mb_commerce_scraper.transports import (
    CommerceTransport,
    RotationReason,
    RouteMetadata,
    TransportFailure,
    TransportRequest,
    TransportResponse,
)

from .base import ProxyLease, ProxyOutcome, ProxyPool, ProxyRequest
from .routing import ProxyRouting, RoutingMode


class ProxyTransportFactory(Protocol):
    def build(self, lease: ProxyLease) -> CommerceTransport: ...


class ProxyBudgetExhausted(RuntimeError):
    pass


class RoutedTransport(CommerceTransport):
    """Attempt-scoped direct/proxy selection with one sticky owned lease."""

    def __init__(
        self,
        direct: CommerceTransport,
        *,
        pool: ProxyPool,
        proxy_factory: ProxyTransportFactory,
        routing: ProxyRouting,
        source_id: str,
        base_url: str,
        maximum_bytes: int | None = None,
    ) -> None:
        host = urlsplit(base_url).hostname
        if host is None:
            raise ValueError("proxy routing requires an absolute base URL")
        self._direct = direct
        self._pool = pool
        self._proxy_factory = proxy_factory
        self._routing = routing
        self._proxy_request = ProxyRequest(
            source_id=source_id,
            target_host=host,
            country=routing.country,
            maximum_bytes=maximum_bytes,
            preferred_providers=routing.provider_preferences,
        )
        self._lease: ProxyLease | None = None
        self._proxy: CommerceTransport | None = None
        self._closed = False

    async def request(self, request: TransportRequest) -> TransportResponse:
        if self._closed:
            raise RuntimeError("routed transport is closed")
        if self._routing.mode == RoutingMode.NEVER:
            return await self._direct.request(request)
        if self._routing.mode == RoutingMode.FALLBACK and self._lease is None:
            try:
                direct = await self._direct.request(request)
            except TransportFailure:
                return await self._proxy_request_attempt(request, RotationReason.TRANSPORT_FAILURE)
            if direct.status not in {403, 429}:
                return direct
            reason = RotationReason.BLOCKED if direct.status == 403 else RotationReason.RATE_LIMITED
            return await self._proxy_request_attempt(request, reason)
        try:
            response = await self._proxy_request_attempt(request)
        except TransportFailure:
            if self._routing.mode != RoutingMode.FAILOVER:
                raise
            await self.rotate_identity(RotationReason.TRANSPORT_FAILURE)
            return await self._proxy_request_attempt(request)
        if self._routing.mode == RoutingMode.FAILOVER and response.status in {403, 429}:
            reason = RotationReason.BLOCKED if response.status == 403 else RotationReason.RATE_LIMITED
            await self.rotate_identity(reason)
            return await self._proxy_request_attempt(request)
        return response

    async def rotate_identity(self, reason: RotationReason) -> None:
        if self._lease is None:
            await self._acquire()
            return
        await self._close_proxy_transport()
        self._lease = await self._pool.rotate(self._lease, reason)
        self._proxy = self._proxy_factory.build(self._lease)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._close_proxy_transport()
        if self._lease is not None:
            await self._pool.release(self._lease)
            self._lease = None
            self._proxy = None

    async def _proxy_request_attempt(
        self,
        request: TransportRequest,
        acquisition_reason: RotationReason | None = None,
    ) -> TransportResponse:
        if self._lease is None:
            await self._acquire()
        assert self._lease is not None and self._proxy is not None
        if not self._lease.can_start(request.estimated_bytes):
            raise ProxyBudgetExhausted("proxy lease byte budget cannot authorize another request")
        try:
            response = await self._proxy.request(request)
        except TransportFailure:
            await self._pool.report(
                self._lease,
                ProxyOutcome(
                    target_host=self._proxy_request.target_host,
                    classification="transport_failure",
                ),
            )
            raise
        classification = (
            "blocked"
            if response.status == 403
            else "rate_limited"
            if response.status == 429
            else "success"
            if response.status < 400
            else "http_error"
        )
        await self._pool.report(
            self._lease,
            ProxyOutcome(
                target_host=self._proxy_request.target_host,
                status=response.status,
                received_bytes=len(response.content),
                classification=classification,
            ),
        )
        del acquisition_reason
        return response.model_copy(
            update={
                "route": RouteMetadata(
                    kind="proxy",
                    provider=self._lease.provider,
                    endpoint_id=self._lease.route.endpoint_id,
                    lease_id=self._lease.lease_id,
                )
            }
        )

    async def _acquire(self) -> None:
        self._lease = await self._pool.acquire(self._proxy_request)
        self._proxy = self._proxy_factory.build(self._lease)

    async def _close_proxy_transport(self) -> None:
        if self._proxy is not None and hasattr(self._proxy, "aclose"):
            await self._proxy.aclose()  # type: ignore[attr-defined, unused-ignore]
