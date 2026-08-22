"""Catalogue-owned PostgreSQL budget adapter for the neutral proxy protocol."""

from __future__ import annotations

import asyncio
import math
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Protocol
from uuid import UUID

from mb_commerce_scraper.proxy import (
    BrowserProxyCredentials,
    ProxyAttemptAuthorization,
    ProxyBudgetExhausted,
    ProxyCredentials,
    ProxyEndpoint,
    ProxyKind,
    ProxyLease,
    ProxyOutcome,
    ProxyRequest,
)
from mb_commerce_scraper.transports import RotationReason
from pydantic import SecretStr

from mb_ceramics_catalogue.proxy import (
    ProxyDenied,
    ProxyProfile,
    ProxyReservationUsage,
    authorize_reservation_attempt,
    close_reservation,
    reconcile_reservation_attempt,
    release_reservation_attempt,
    reserve,
)
from mb_ceramics_catalogue.proxy import (
    ProxyLease as LegacyProxyLease,
)


class ConnectionPool(Protocol):
    def connection(self) -> AbstractAsyncContextManager[Any]: ...


@dataclass(slots=True)
class _ReservationState:
    legacy: LegacyProxyLease
    pending_authorizations: set[UUID] = field(default_factory=set)
    reporting_failed: bool = False
    released: bool = False


@dataclass(slots=True)
class CatalogueProxyLease:
    """Neutral lease view backed by one durable catalogue reservation."""

    lease_id: str
    provider: str
    route: ProxyEndpoint
    request: ProxyRequest
    _state: _ReservationState
    maximum_bytes: int | None
    _generation: int = 0
    _invalidated: bool = False
    expires_at: datetime | None = None

    def can_start(self, estimated_bytes: int = 0) -> bool:
        if self._invalidated or estimated_bytes < 0:
            return False
        state = self._state
        return (
            not state.reporting_failed
            and not state.released
            and (
                self.request.maximum_requests is None
                or state.legacy.requests < self.request.maximum_requests
            )
            and state.legacy.used_bytes + estimated_bytes <= state.legacy.max_bytes
        )

    def http_credentials(self) -> ProxyCredentials:
        legacy = self._state.legacy
        return ProxyCredentials(
            username=SecretStr(legacy.username),
            password=SecretStr(legacy.profile.password),
        )

    def browser_credentials(self) -> BrowserProxyCredentials:
        legacy = self._state.legacy
        return BrowserProxyCredentials(
            server=f"{legacy.protocol}://{legacy.profile.host}:{legacy.profile.port}",
            username=SecretStr(legacy.username),
            password=SecretStr(legacy.profile.password),
        )


@dataclass(slots=True)
class _PostgresAttemptAuthorization:
    pool: PostgresDecodoProxyPool
    lease: CatalogueProxyLease
    authorization_id: UUID
    resolved: bool = False

    async def reconcile(self, outcome: ProxyOutcome) -> None:
        if self.resolved:
            raise RuntimeError("proxy attempt authorization already resolved")
        usage = await self.pool._reconcile(self.lease, self.authorization_id, outcome)
        self.resolved = True
        self.pool._raise_if_exhausted(self.lease, usage)

    async def release(self) -> None:
        if self.resolved:
            raise RuntimeError("proxy attempt authorization already resolved")
        await self.pool._release_authorization(self.lease, self.authorization_id)
        self.resolved = True


class PostgresDecodoProxyPool:
    """Reserve Decodo spend in PostgreSQL before exposing a usable lease.

    PostgreSQL atomically reserves the complete lease allowance against the
    billing cycle. Each physical attempt then receives a durable authorization
    token immediately before dispatch, and reconciles actual counters exactly
    once. Any authorization or reconciliation failure disables further starts.
    """

    def __init__(
        self,
        database: ConnectionPool,
        *,
        job_id: UUID,
        profile: ProxyProfile,
        profile_id: UUID,
        route_id: UUID,
        maximum_bytes: int,
        route_country: str | None = None,
        protocol: Literal["http", "https", "socks5"] = "http",
        session_minutes: int = 30,
        pilot: bool = False,
    ) -> None:
        if maximum_bytes < 1:
            raise ValueError("maximum_bytes must be positive")
        if protocol not in {"http", "https", "socks5"}:
            raise ValueError("proxy protocol must be http, https, or socks5")
        if not 1 <= session_minutes <= 1_440:
            raise ValueError("session_minutes must be between 1 and 1440")
        self._database = database
        self._job_id = job_id
        self._profile = profile
        self._profile_id = profile_id
        self._route_id = route_id
        self._maximum_bytes = maximum_bytes
        self._route_country = route_country
        self._protocol = protocol
        self._session_minutes = session_minutes
        self._pilot = pilot
        self._leases: dict[str, CatalogueProxyLease] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, request: ProxyRequest) -> CatalogueProxyLease:
        self._validate_request(request)
        maximum_bytes = min(
            self._maximum_bytes,
            request.maximum_bytes or self._maximum_bytes,
        )
        country = request.country or self._route_country
        session_minutes = min(
            self._session_minutes,
            math.ceil(request.session_ttl_seconds / 60)
            if request.session_ttl_seconds is not None
            else self._session_minutes,
        )
        async with self._database.connection() as connection:
            reservation_id = await reserve(
                connection,
                job_id=self._job_id,
                profile=self._profile.name,
                profile_id=self._profile_id,
                route_id=self._route_id,
                requested_bytes=maximum_bytes,
                pilot=self._pilot,
                secret_generation=self._profile.generation,
            )
        legacy = LegacyProxyLease.build(
            reservation_id,
            self._job_id,
            self._profile,
            country,
            session_minutes,
            maximum_bytes,
            self._protocol,
        )
        state = _ReservationState(legacy)
        lease = self._lease(request, state, generation=0)
        async with self._lock:
            self._leases[lease.lease_id] = lease
        return lease

    async def rotate(
        self,
        lease: ProxyLease,
        reason: RotationReason,
    ) -> CatalogueProxyLease:
        del reason
        async with self._lock:
            current = self._owned(lease)
            if current._state.pending_authorizations:
                raise ProxyDenied("proxy identity cannot rotate with authorized requests")
            current._invalidated = True
            current._state.legacy.rotate_session()
            replacement = self._lease(
                current.request,
                current._state,
                generation=current._generation + 1,
            )
            self._leases.pop(current.lease_id)
            self._leases[replacement.lease_id] = replacement
            return replacement

    async def authorize(
        self, lease: ProxyLease, estimated_bytes: int
    ) -> ProxyAttemptAuthorization | None:
        if estimated_bytes < 0:
            raise ValueError("estimated_bytes must be non-negative")
        async with self._lock:
            current = self._owned(lease)
            if current._state.reporting_failed:
                return None
            try:
                async with self._database.connection() as connection:
                    authorization_id = await authorize_reservation_attempt(
                        connection,
                        reservation_id=current._state.legacy.reservation_id,
                        estimated_bytes=estimated_bytes,
                        maximum_requests=current.request.maximum_requests,
                    )
            except BaseException:
                current._state.reporting_failed = True
                raise
            if authorization_id is None:
                return None
            current._state.pending_authorizations.add(authorization_id)
            return _PostgresAttemptAuthorization(self, current, authorization_id)

    async def report(
        self,
        lease: ProxyLease,
        outcome: ProxyOutcome,
    ) -> None:
        async with self._lock:
            current = self._owned(lease)
            if outcome.target_host != current.request.target_host:
                raise ProxyDenied("proxy outcome target does not match the lease")

    async def release(self, lease: ProxyLease) -> None:
        async with self._lock:
            if isinstance(lease, CatalogueProxyLease) and lease._state.released:
                return
            current = self._owned(lease)
            if current._state.pending_authorizations:
                raise ProxyDenied("proxy lease has unreconciled authorized requests")
            try:
                async with self._database.connection() as connection:
                    await close_reservation(connection, current._state.legacy)
            except BaseException:
                current._state.reporting_failed = True
                raise
            current._state.released = True
            current._invalidated = True
            self._leases.pop(current.lease_id, None)

    async def _reconcile(
        self,
        lease: CatalogueProxyLease,
        authorization_id: UUID,
        outcome: ProxyOutcome,
    ) -> ProxyReservationUsage:
        async with self._lock:
            current = self._owned(lease)
            self._pending(current, authorization_id)
            if outcome.target_host != current.request.target_host:
                raise ProxyDenied("proxy outcome target does not match the lease")
            try:
                async with self._database.connection() as connection:
                    usage = await reconcile_reservation_attempt(
                        connection,
                        authorization_id=authorization_id,
                        actual_bytes=outcome.transmitted_bytes + outcome.received_bytes,
                        physical_requests=outcome.physical_requests,
                    )
            except BaseException:
                current._state.reporting_failed = True
                raise
            current._state.pending_authorizations.remove(authorization_id)
            current._state.legacy.used_bytes = usage.estimated_bytes
            current._state.legacy.requests = usage.request_count
            return usage

    async def _release_authorization(
        self, lease: CatalogueProxyLease, authorization_id: UUID
    ) -> None:
        async with self._lock:
            current = self._owned(lease)
            self._pending(current, authorization_id)
            try:
                async with self._database.connection() as connection:
                    await release_reservation_attempt(
                        connection, authorization_id=authorization_id
                    )
            except BaseException:
                current._state.reporting_failed = True
                raise
            current._state.pending_authorizations.remove(authorization_id)

    @staticmethod
    def _pending(current: CatalogueProxyLease, authorization_id: UUID) -> None:
        if authorization_id not in current._state.pending_authorizations:
            raise ProxyDenied("proxy attempt authorization is not active")

    @staticmethod
    def _raise_if_exhausted(
        current: CatalogueProxyLease, usage: ProxyReservationUsage
    ) -> None:
        maximum_requests = current.request.maximum_requests
        if usage.revoked:
            current._state.reporting_failed = True
            raise ProxyDenied("proxy reservation was revoked")
        if usage.exhausted or (
            maximum_requests is not None and usage.request_count > maximum_requests
        ):
            current._state.reporting_failed = True
            raise ProxyBudgetExhausted(
                maximum_requests=maximum_requests,
                maximum_bytes=current.maximum_bytes,
                used_requests=usage.request_count,
                used_bytes=usage.estimated_bytes,
            )

    def _lease(
        self,
        request: ProxyRequest,
        state: _ReservationState,
        *,
        generation: int,
    ) -> CatalogueProxyLease:
        reservation = state.legacy.reservation_id
        return CatalogueProxyLease(
            lease_id=f"{reservation}:{generation}",
            provider="decodo",
            route=ProxyEndpoint(
                provider="decodo",
                endpoint_id=str(self._route_id),
                protocol=self._protocol,
                host=self._profile.host,
                port=self._profile.port,
                kind=ProxyKind.RESIDENTIAL,
                countries=(
                    frozenset({self._route_country})
                    if self._route_country is not None
                    else frozenset()
                ),
            ),
            request=request,
            _state=state,
            maximum_bytes=state.legacy.max_bytes,
            _generation=generation,
        )

    def _owned(self, lease: ProxyLease) -> CatalogueProxyLease:
        current = self._leases.get(lease.lease_id)
        if current is not lease or current._invalidated or current._state.released:
            raise ProxyDenied("proxy lease is not active")
        return current

    def _validate_request(self, request: ProxyRequest) -> None:
        if request.kind is not ProxyKind.RESIDENTIAL:
            raise ProxyDenied("Decodo route supports residential requests only")
        if request.region is not None or request.city is not None:
            raise ProxyDenied("configured Decodo route does not support region or city selection")
        if request.country and self._route_country and request.country != self._route_country:
            raise ProxyDenied("proxy request country does not match the configured route")
        if "decodo" in request.excluded_providers:
            raise ProxyDenied("proxy request excludes the configured provider")
        if request.preferred_providers and "decodo" not in request.preferred_providers:
            raise ProxyDenied("proxy request does not select the configured provider")
