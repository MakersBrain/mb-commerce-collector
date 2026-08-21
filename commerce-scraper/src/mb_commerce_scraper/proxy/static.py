from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from itertools import count

from mb_commerce_scraper.transports import RotationReason

from .base import (
    BrowserProxyCredentials,
    ProxyCredentials,
    ProxyEndpoint,
    ProxyLease,
    ProxyOutcome,
    ProxyPool,
    ProxyRequest,
)
from .health import InMemoryProxyHealth


@dataclass
class StaticRoute:
    endpoint: ProxyEndpoint
    credentials: ProxyCredentials


@dataclass
class StaticProxyLease:
    lease_id: str
    provider: str
    route: ProxyEndpoint
    _credentials: ProxyCredentials
    request: ProxyRequest
    expires_at: datetime | None = None
    maximum_bytes: int | None = None
    released: bool = False
    used_bytes: int = 0

    def http_credentials(self) -> ProxyCredentials:
        return self._credentials

    def browser_credentials(self) -> BrowserProxyCredentials:
        return BrowserProxyCredentials(
            server=f"{self.route.protocol}://{self.route.host}:{self.route.port}",
            username=self._credentials.username,
            password=self._credentials.password,
        )


class StaticProxyPool(ProxyPool):
    """Deterministic multi-provider pool suitable for config and local tests."""

    def __init__(self, routes: tuple[StaticRoute, ...], *, health: InMemoryProxyHealth | None = None) -> None:
        self._routes = routes
        self._health = health or InMemoryProxyHealth()
        self._counter = count(1)
        self._round_robin = 0
        self._lock = asyncio.Lock()
        self._leases: dict[str, StaticProxyLease] = {}

    async def acquire(self, request: ProxyRequest) -> StaticProxyLease:
        async with self._lock:
            candidates = [route for route in self._routes if self._eligible(route, request)]
            if not candidates:
                raise RuntimeError("no healthy proxy route satisfies the request")
            if request.preferred_providers:
                order = {name: index for index, name in enumerate(request.preferred_providers)}
                candidates.sort(key=lambda item: order.get(item.endpoint.provider, len(order)))
            selected = candidates[self._round_robin % len(candidates)]
            self._round_robin += 1
            lease_id = f"static-{next(self._counter)}"
            lease = StaticProxyLease(
                lease_id=lease_id, provider=selected.endpoint.provider, route=selected.endpoint,
                _credentials=selected.credentials, request=request, maximum_bytes=request.maximum_bytes,
            )
            self._leases[lease_id] = lease
            return lease

    async def rotate(self, lease: ProxyLease, reason: RotationReason) -> StaticProxyLease:
        current = self._owned(lease)
        if reason in {RotationReason.BLOCKED, RotationReason.RATE_LIMITED, RotationReason.CAPTCHA, RotationReason.TRANSPORT_FAILURE}:
            self._health.failure(current.provider, current.route.endpoint_id, current.request.target_host)
        await self.release(current)
        return await self.acquire(current.request)

    async def report(self, lease: ProxyLease, outcome: ProxyOutcome) -> None:
        current = self._owned(lease)
        current.used_bytes += outcome.transmitted_bytes + outcome.received_bytes
        if current.maximum_bytes is not None and current.used_bytes > current.maximum_bytes:
            raise RuntimeError("proxy lease byte limit exhausted")
        if outcome.classification == "success":
            self._health.success(current.provider, current.route.endpoint_id, outcome.target_host)
        elif outcome.classification in {"blocked", "rate_limited", "captcha", "transport_failure"}:
            self._health.failure(current.provider, current.route.endpoint_id, outcome.target_host)

    async def release(self, lease: ProxyLease) -> None:
        current = self._leases.get(lease.lease_id)
        if current is None or current.released:
            return
        current.released = True
        self._leases.pop(current.lease_id, None)

    def _eligible(self, route: StaticRoute, request: ProxyRequest) -> bool:
        endpoint = route.endpoint
        return (
            endpoint.provider not in request.excluded_providers
            and endpoint.kind == request.kind
            and (request.country is None or not endpoint.countries or request.country in endpoint.countries)
            and self._health.available(endpoint.provider, endpoint.endpoint_id, request.target_host)
        )

    def _owned(self, lease: ProxyLease) -> StaticProxyLease:
        current = self._leases.get(lease.lease_id)
        if current is None or current.released:
            raise RuntimeError("proxy lease is not active")
        return current

