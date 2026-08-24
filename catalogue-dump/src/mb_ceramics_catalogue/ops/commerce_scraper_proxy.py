"""Catalogue-owned PostgreSQL budget adapter for the neutral proxy protocol."""

from __future__ import annotations

import asyncio
import re
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from mb_commerce_scraper.proxy import (
    BrowserProxyCredentials,
    ProxyAttemptAuthorization,
    ProxyBudgetExhausted,
    ProxyCredentials,
    ProxyEndpoint,
    ProxyLease,
    ProxyOutcome,
    ProxyPool,
    ProxyRequest,
)
from mb_commerce_scraper.transports import RotationReason

from mb_ceramics_catalogue.proxy import (
    ProxyDenied,
    ProxyReservationUsage,
    authorize_reservation_attempt,
    close_reservation,
    reconcile_reservation_attempt,
    release_reservation_attempt,
    reserve,
)


class ConnectionPool(Protocol):
    def connection(self) -> AbstractAsyncContextManager[Any]: ...


@dataclass(frozen=True, slots=True)
class DurableProxyIdentity:
    """Immutable database identity for one provider-owned gateway route."""

    provider: str
    profile: str
    profile_id: UUID
    route_id: UUID
    secret_generation: int

    def __post_init__(self) -> None:
        if re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", self.provider) is None:
            raise ValueError("proxy provider is invalid")
        if not self.profile:
            raise ValueError("proxy profile name must not be empty")
        if self.profile_id.int == 0 or self.route_id.int == 0:
            raise ValueError("proxy profile and route identities must not be nil")
        if (
            isinstance(self.secret_generation, bool)
            or not isinstance(self.secret_generation, int)
            or self.secret_generation < 0
        ):
            raise ValueError("proxy secret generation must be non-negative")


@dataclass(slots=True)
class _DurableReservationState:
    reservation_id: UUID
    max_bytes: int
    used_bytes: int = 0
    requests: int = 0
    pending_authorizations: set[UUID] = field(default_factory=set)
    #: Authorizations past validation but not yet durably recorded. The lock is
    #: released across that database round trip, so rotation and release must
    #: treat an in-flight attempt exactly as they treat a pending one.
    in_flight: int = 0
    reporting_failed: bool = False
    durable_closed: bool = False
    inner_released: bool = False
    released: bool = False


@dataclass(slots=True)
class ReservedProxyLease:
    """Neutral lease whose provider behavior and durable accounting are composed."""

    lease_id: str
    provider: str
    route: ProxyEndpoint
    request: ProxyRequest
    _inner: ProxyLease
    _state: _DurableReservationState
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
            and not state.durable_closed
            and state.used_bytes + estimated_bytes <= state.max_bytes
            and self._inner.can_start(estimated_bytes)
        )

    def http_credentials(self) -> ProxyCredentials:
        return self._inner.http_credentials()

    def browser_credentials(self) -> BrowserProxyCredentials:
        return self._inner.browser_credentials()


@dataclass(slots=True)
class _ReservedAttemptAuthorization:
    pool: PostgresReservedProxyPool
    lease: ReservedProxyLease
    authorization_id: UUID
    inner: ProxyAttemptAuthorization
    resolved: bool = False

    async def reconcile(self, outcome: ProxyOutcome) -> None:
        if self.resolved:
            raise RuntimeError("proxy attempt authorization already resolved")
        usage = await self.pool._reconcile(self.lease, self.authorization_id, outcome)
        try:
            await self.inner.reconcile(outcome)
        except BaseException:
            self.lease._state.reporting_failed = True
            raise
        finally:
            # Inner authorization objects are single-use even when accounting
            # raises after retaining the actual counters.
            self.resolved = True
        self.pool._raise_if_exhausted(self.lease, usage)

    async def release(self) -> None:
        if self.resolved:
            raise RuntimeError("proxy attempt authorization already resolved")
        await self.pool._release_authorization(self.lease, self.authorization_id)
        try:
            await self.inner.release()
        except BaseException:
            self.lease._state.reporting_failed = True
            raise
        finally:
            self.resolved = True


class PostgresReservedProxyPool:
    """Add fail-closed PostgreSQL accounting to a provider data-plane pool.

    The inner pool retains provider capability, credential, identity rotation,
    and health behavior. This decorator owns the paid-traffic boundary: every
    returned attempt token has both an inner authorization and a durable
    PostgreSQL authorization, acquired in that order so the database decision
    remains the final operation before dispatch.
    """

    def __init__(
        self,
        database: ConnectionPool,
        inner: ProxyPool,
        *,
        job_id: UUID,
        identity: DurableProxyIdentity,
        maximum_bytes: int,
        pilot: bool = False,
    ) -> None:
        if job_id.int == 0:
            raise ValueError("proxy job identity must not be nil")
        if isinstance(maximum_bytes, bool) or not isinstance(maximum_bytes, int) or maximum_bytes < 1:
            raise ValueError("maximum_bytes must be positive")
        if not isinstance(pilot, bool):
            raise ValueError("proxy pilot flag must be boolean")
        self._database = database
        self._inner = inner
        self._job_id = job_id
        self._identity = identity
        self._maximum_bytes = maximum_bytes
        self._pilot = pilot
        self._leases: dict[str, ReservedProxyLease] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, request: ProxyRequest) -> ReservedProxyLease:
        inner = await self._inner.acquire(request)
        try:
            self._validate_inner(inner)
            maximum_bytes = min(
                self._maximum_bytes,
                request.maximum_bytes or self._maximum_bytes,
            )
            async with self._database.connection() as connection:
                reservation_id = await reserve(
                    connection,
                    job_id=self._job_id,
                    profile=self._identity.profile,
                    profile_id=self._identity.profile_id,
                    route_id=self._identity.route_id,
                    requested_bytes=maximum_bytes,
                    pilot=self._pilot,
                    secret_generation=self._identity.secret_generation,
                    provider=self._identity.provider,
                )
        except BaseException:
            with suppress(BaseException):
                await asyncio.shield(self._inner.release(inner))
            raise
        state = _DurableReservationState(reservation_id, maximum_bytes)
        lease = self._lease(request, inner, state, generation=0)
        async with self._lock:
            self._leases[lease.lease_id] = lease
        return lease

    async def rotate(
        self,
        lease: ProxyLease,
        reason: RotationReason,
    ) -> ReservedProxyLease:
        async with self._lock:
            current = self._owned(lease)
            if current._state.pending_authorizations or current._state.in_flight:
                raise ProxyDenied("proxy identity cannot rotate with authorized requests")
            try:
                inner = await self._inner.rotate(current._inner, reason)
            except BaseException:
                # Keep the wrapper ownership record so RoutedTransport's
                # rotation-failure cleanup can still close the reservation.
                raise
            try:
                self._validate_inner(inner)
            except BaseException:
                with suppress(BaseException):
                    await asyncio.shield(self._inner.release(inner))
                # The old inner lease was invalidated by rotate, but retaining
                # its wrapper lets caller cleanup close the durable reservation.
                raise
            current._invalidated = True
            replacement = self._lease(
                current.request,
                inner,
                current._state,
                generation=current._generation + 1,
            )
            self._leases.pop(current.lease_id)
            self._leases[replacement.lease_id] = replacement
            return replacement

    async def authorize(self, lease: ProxyLease, estimated_bytes: int) -> ProxyAttemptAuthorization | None:
        """Authorize one physical attempt, durably and last.

        A browser page load routes many subrequests concurrently, so the pool
        lock is held only for the in-memory ownership and budget decisions and
        is released across the database round trip. ``in_flight`` keeps the
        ordering guarantee that made the wider lock correct: an identity cannot
        rotate, and a reservation cannot close, while an attempt is being
        authorized against it.
        """

        if estimated_bytes < 0:
            raise ValueError("estimated_bytes must be non-negative")
        async with self._lock:
            current = self._owned(lease)
            state = current._state
            if state.reporting_failed:
                return None
            inner_authorization = await self._inner.authorize(current._inner, estimated_bytes)
            if inner_authorization is None:
                return None
            state.in_flight += 1

        try:
            async with self._database.connection() as connection:
                authorization_id = await authorize_reservation_attempt(
                    connection,
                    reservation_id=state.reservation_id,
                    estimated_bytes=estimated_bytes,
                    maximum_requests=current.request.maximum_requests,
                )
        except BaseException:
            async with self._lock:
                state.in_flight -= 1
                state.reporting_failed = True
            await self._discard_inner_authorization(inner_authorization, state)
            raise
        if authorization_id is None:
            async with self._lock:
                state.in_flight -= 1
            await self._discard_inner_authorization(inner_authorization, state)
            return None
        async with self._lock:
            # Record the durable authorization even if another concurrent
            # subrequest failed meanwhile: dropping it here would orphan a
            # database row that nothing could reconcile or release.
            state.pending_authorizations.add(authorization_id)
            state.in_flight -= 1
        return _ReservedAttemptAuthorization(
            self,
            current,
            authorization_id,
            inner_authorization,
        )

    async def report(self, lease: ProxyLease, outcome: ProxyOutcome) -> None:
        async with self._lock:
            current = self._owned(lease)
            self._validate_outcome(current, outcome)
            await self._inner.report(current._inner, outcome)

    async def release(self, lease: ProxyLease) -> None:
        async with self._lock:
            if isinstance(lease, ReservedProxyLease) and lease._state.released:
                return
            current = self._owned(lease)
            state = current._state
            if state.pending_authorizations or state.in_flight:
                raise ProxyDenied("proxy lease has unreconciled authorized requests")
            if not state.durable_closed:
                try:
                    async with self._database.connection() as connection:
                        await close_reservation(connection, state)
                except BaseException:
                    state.reporting_failed = True
                    raise
                state.durable_closed = True
            if not state.inner_released:
                try:
                    await self._inner.release(current._inner)
                except BaseException:
                    state.reporting_failed = True
                    raise
                state.inner_released = True
            state.released = True
            current._invalidated = True
            self._leases.pop(current.lease_id, None)

    async def _reconcile(
        self,
        lease: ReservedProxyLease,
        authorization_id: UUID,
        outcome: ProxyOutcome,
    ) -> ProxyReservationUsage:
        async with self._lock:
            current = self._owned(lease)
            self._pending(current, authorization_id)
            self._validate_outcome(current, outcome)
        # The authorization stays pending across the round trip, which is what
        # keeps release and rotation from running underneath it.
        try:
            async with self._database.connection() as connection:
                usage = await reconcile_reservation_attempt(
                    connection,
                    authorization_id=authorization_id,
                    actual_bytes=(outcome.transmitted_bytes + outcome.received_bytes),
                    physical_requests=outcome.physical_requests,
                )
        except BaseException:
            current._state.reporting_failed = True
            raise
        async with self._lock:
            current._state.pending_authorizations.discard(authorization_id)
            current._state.used_bytes = usage.estimated_bytes
            current._state.requests = usage.request_count
        return usage

    async def _release_authorization(
        self,
        lease: ReservedProxyLease,
        authorization_id: UUID,
    ) -> None:
        async with self._lock:
            current = self._owned(lease)
            self._pending(current, authorization_id)
        try:
            async with self._database.connection() as connection:
                await release_reservation_attempt(
                    connection,
                    authorization_id=authorization_id,
                )
        except BaseException:
            current._state.reporting_failed = True
            raise
        async with self._lock:
            current._state.pending_authorizations.discard(authorization_id)

    async def _discard_inner_authorization(
        self,
        authorization: ProxyAttemptAuthorization,
        state: _DurableReservationState,
    ) -> None:
        # Stay fail-closed if cancellation or an inner accounting failure makes
        # cleanup uncertain. Restore the prior state only after proven release.
        previous_failure = state.reporting_failed
        state.reporting_failed = True
        await asyncio.shield(authorization.release())
        state.reporting_failed = previous_failure

    def _validate_inner(self, lease: ProxyLease) -> None:
        identity = self._identity
        if lease.provider != identity.provider or lease.route.provider != identity.provider:
            raise ProxyDenied("proxy data-plane provider does not match durable identity")
        if lease.route.endpoint_id != str(identity.route_id):
            raise ProxyDenied("proxy data-plane route does not match durable identity")

    @staticmethod
    def _validate_outcome(current: ReservedProxyLease, outcome: ProxyOutcome) -> None:
        if outcome.target_host != current.request.target_host:
            raise ProxyDenied("proxy outcome target does not match the lease")

    @staticmethod
    def _pending(current: ReservedProxyLease, authorization_id: UUID) -> None:
        if authorization_id not in current._state.pending_authorizations:
            raise ProxyDenied("proxy attempt authorization is not active")

    @staticmethod
    def _raise_if_exhausted(
        current: ReservedProxyLease,
        usage: ProxyReservationUsage,
    ) -> None:
        maximum_requests = current.request.maximum_requests
        if usage.revoked:
            current._state.reporting_failed = True
            raise ProxyDenied("proxy reservation was revoked")
        if usage.exhausted or (maximum_requests is not None and usage.request_count > maximum_requests):
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
        inner: ProxyLease,
        state: _DurableReservationState,
        *,
        generation: int,
    ) -> ReservedProxyLease:
        return ReservedProxyLease(
            lease_id=f"{state.reservation_id}:{generation}",
            provider=inner.provider,
            route=inner.route,
            request=request,
            _inner=inner,
            _state=state,
            maximum_bytes=state.max_bytes,
            _generation=generation,
            expires_at=inner.expires_at,
        )

    def _owned(self, lease: ProxyLease) -> ReservedProxyLease:
        current = self._leases.get(lease.lease_id)
        if current is not lease or current._invalidated or current._state.released:
            raise ProxyDenied("proxy lease is not active")
        return current
