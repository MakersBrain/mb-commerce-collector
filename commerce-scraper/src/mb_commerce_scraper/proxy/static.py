from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from itertools import count

from mb_commerce_scraper.transports import RotationReason

from .base import (
    BrowserProxyCredentials,
    ProxyBudgetExhausted,
    ProxyCredentials,
    ProxyEndpoint,
    ProxyLease,
    ProxyOutcome,
    ProxyPool,
    ProxyRequest,
)
from .health import InMemoryProxyHealth, ProxyFailureReason


@dataclass
class StaticRoute:
    endpoint: ProxyEndpoint
    credentials: ProxyCredentials
    weight: int = 1

    def __post_init__(self) -> None:
        if (
            not isinstance(self.weight, int)
            or isinstance(self.weight, bool)
            or self.weight < 1
        ):
            raise ValueError("proxy route weight must be a positive integer")


@dataclass
class _StaticProxyUsage:
    maximum_requests: int | None
    maximum_bytes: int | None
    requests: int = 0
    used_bytes: int = 0
    reserved_bytes: int = 0
    next_authorization_id: int = 0
    authorizations: dict[int, int] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class StaticProxyLease:
    lease_id: str
    provider: str
    route: ProxyEndpoint
    _credentials: ProxyCredentials
    request: ProxyRequest
    _usage: _StaticProxyUsage
    expires_at: datetime | None = None
    maximum_bytes: int | None = None
    released: bool = False

    @property
    def used_bytes(self) -> int:
        return self._usage.used_bytes

    @property
    def used_requests(self) -> int:
        return self._usage.requests

    def can_start(self, estimated_bytes: int = 0) -> bool:
        return (
            not self.released
            and (self.expires_at is None or self.expires_at > datetime.now(self.expires_at.tzinfo))
            and (
                self._usage.maximum_requests is None
                or self._usage.requests < self._usage.maximum_requests
            )
            and (
                self.maximum_bytes is None
                or self._usage.used_bytes
                + self._usage.reserved_bytes
                + estimated_bytes
                <= self.maximum_bytes
            )
        )

    def http_credentials(self) -> ProxyCredentials:
        return self._credentials

    def browser_credentials(self) -> BrowserProxyCredentials:
        return BrowserProxyCredentials(
            server=f"{self.route.protocol}://{self.route.host}:{self.route.port}",
            username=self._credentials.username,
            password=self._credentials.password,
        )


@dataclass(slots=True)
class _StaticProxyAuthorization:
    usage: _StaticProxyUsage
    authorization_id: int

    async def reconcile(self, outcome: ProxyOutcome) -> None:
        async with self.usage.lock:
            estimated_bytes = self._resolve()
            self.usage.reserved_bytes -= estimated_bytes
            self.usage.requests += outcome.physical_requests - 1
            self.usage.used_bytes += (
                outcome.transmitted_bytes + outcome.received_bytes
            )
            if (
                self.usage.maximum_requests is not None
                and self.usage.requests > self.usage.maximum_requests
            ) or (
                self.usage.maximum_bytes is not None
                and self.usage.used_bytes > self.usage.maximum_bytes
            ):
                raise ProxyBudgetExhausted(
                    maximum_requests=self.usage.maximum_requests,
                    maximum_bytes=self.usage.maximum_bytes,
                    used_requests=self.usage.requests,
                    used_bytes=self.usage.used_bytes,
                )

    async def release(self) -> None:
        async with self.usage.lock:
            estimated_bytes = self._resolve()
            self.usage.requests -= 1
            self.usage.reserved_bytes -= estimated_bytes

    def _resolve(self) -> int:
        try:
            return self.usage.authorizations.pop(self.authorization_id)
        except KeyError as error:
            raise RuntimeError("proxy attempt authorization already resolved") from error


class StaticProxyPool(ProxyPool):
    """Deterministic multi-provider pool suitable for config and local tests."""

    def __init__(self, routes: tuple[StaticRoute, ...], *, health: InMemoryProxyHealth | None = None) -> None:
        route_keys = [
            (route.endpoint.provider, route.endpoint.endpoint_id) for route in routes
        ]
        if len(route_keys) != len(set(route_keys)):
            raise ValueError("proxy routes must have unique provider/endpoint identities")
        self._routes = routes
        self._health = health or InMemoryProxyHealth()
        self._counter = count(1)
        self._routing_scores: dict[
            tuple[tuple[str, str], ...], dict[tuple[str, str], int]
        ] = {}
        self._lock = asyncio.Lock()
        self._leases: dict[str, StaticProxyLease] = {}

    @property
    def active_leases(self) -> int:
        return len(self._leases)

    async def acquire(self, request: ProxyRequest) -> StaticProxyLease:
        unsupported = tuple(
            name
            for name, value in (
                ("region", request.region),
                ("city", request.city),
                ("session_ttl_seconds", request.session_ttl_seconds),
            )
            if value is not None
        )
        if unsupported:
            raise ValueError(
                "static proxy routes cannot prove requested constraints: "
                + ", ".join(unsupported)
            )
        async with self._lock:
            candidates = [route for route in self._routes if self._eligible(route, request)]
            if not candidates:
                raise RuntimeError("no healthy proxy route satisfies the request")
            candidates = self._preferred_tier(candidates, request)
            selected = self._select_weighted(candidates)
            lease_id = f"static-{next(self._counter)}"
            lease = StaticProxyLease(
                lease_id=lease_id, provider=selected.endpoint.provider, route=selected.endpoint,
                _credentials=selected.credentials,
                request=request,
                _usage=_StaticProxyUsage(
                    request.maximum_requests, request.maximum_bytes
                ),
                maximum_bytes=request.maximum_bytes,
            )
            self._leases[lease_id] = lease
            return lease

    async def rotate(self, lease: ProxyLease, reason: RotationReason) -> StaticProxyLease:
        current = self._owned(lease)
        usage = current._usage
        await self.release(current)
        replacement = await self.acquire(current.request)
        replacement._usage = usage
        return replacement

    async def authorize(
        self, lease: ProxyLease, estimated_bytes: int
    ) -> _StaticProxyAuthorization | None:
        if estimated_bytes < 0:
            raise ValueError("estimated_bytes must be non-negative")
        current = self._owned(lease)
        usage = current._usage
        async with usage.lock:
            if (
                usage.maximum_requests is not None
                and usage.requests >= usage.maximum_requests
            ) or (
                usage.maximum_bytes is not None
                and usage.used_bytes + usage.reserved_bytes + estimated_bytes
                > usage.maximum_bytes
            ):
                return None
            authorization_id = usage.next_authorization_id
            usage.next_authorization_id += 1
            usage.requests += 1
            usage.reserved_bytes += estimated_bytes
            usage.authorizations[authorization_id] = estimated_bytes
            return _StaticProxyAuthorization(usage, authorization_id)

    async def report(self, lease: ProxyLease, outcome: ProxyOutcome) -> None:
        current = self._owned(lease)
        if outcome.classification == "success":
            self._health.success(current.provider, current.route.endpoint_id, outcome.target_host)
        else:
            try:
                reason = ProxyFailureReason(outcome.classification)
            except ValueError:
                return
            self._health.failure(
                current.provider,
                current.route.endpoint_id,
                outcome.target_host,
                reason,
            )

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

    @staticmethod
    def _preferred_tier(
        candidates: list[StaticRoute], request: ProxyRequest
    ) -> list[StaticRoute]:
        for provider in request.preferred_providers:
            preferred = [
                route for route in candidates if route.endpoint.provider == provider
            ]
            if preferred:
                return preferred
        return candidates

    def _select_weighted(self, candidates: list[StaticRoute]) -> StaticRoute:
        """Select with smooth weighted round-robin for this eligible route set."""
        signature = tuple(
            (route.endpoint.provider, route.endpoint.endpoint_id)
            for route in candidates
        )
        scores = self._routing_scores.setdefault(
            signature, dict.fromkeys(signature, 0)
        )
        total_weight = 0
        selected = candidates[0]
        selected_key = signature[0]
        selected_score: int | None = None
        for route, route_key in zip(candidates, signature, strict=True):
            scores[route_key] += route.weight
            total_weight += route.weight
            if selected_score is None or scores[route_key] > selected_score:
                selected = route
                selected_key = route_key
                selected_score = scores[route_key]
        scores[selected_key] -= total_weight
        return selected

    def _owned(self, lease: ProxyLease) -> StaticProxyLease:
        current = self._leases.get(lease.lease_id)
        if current is None or current.released:
            raise RuntimeError("proxy lease is not active")
        return current
