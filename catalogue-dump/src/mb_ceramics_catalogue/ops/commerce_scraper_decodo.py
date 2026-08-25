"""Decodo residential adapter for the neutral proxy data plane.

This module deliberately lives in the catalogue application: Decodo's username
grammar, session-token format, and per-session duration are vendor details.
The scraper core sees only its ``ProxyPool`` and ``ProxyLease`` contracts.

The adapter intentionally does not open catalogue billing reservations.  It is
composed with :class:`PostgresReservedProxyPool`, which owns the fail-closed
spend authorization, exactly as the Webshare gateway adapter is.  Keeping the
durable accounting in one decorator means authorization ordering, revocation,
and split-release semantics are implemented and tested once for every
provider rather than reimplemented per vendor.
"""

from __future__ import annotations

import math
import secrets
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from mb_commerce_scraper.proxy import (
    BrowserProxyCredentials,
    ProxyAttemptAuthorization,
    ProxyCredentials,
    ProxyEndpoint,
    ProxyKind,
    ProxyLease,
    ProxyOutcome,
    ProxyRequest,
    StaticProxyLease,
    StaticProxyPool,
    StaticRoute,
)
from mb_commerce_scraper.transports import RotationReason
from pydantic import SecretStr

from mb_ceramics_catalogue.proxy import ProxyDenied, ProxyProfile

_PROVIDER = "decodo"


@dataclass(frozen=True, slots=True)
class DecodoDataPlaneConfig:
    """Operator-verified credentials and capabilities for one Decodo route."""

    profile: ProxyProfile
    endpoint_id: str
    country: str | None = None
    protocol: Literal["http", "https", "socks5"] = "http"
    session_minutes: int = 30

    def __post_init__(self) -> None:
        if not self.endpoint_id:
            raise ValueError("Decodo endpoint id must not be empty")
        if self.protocol not in {"http", "https", "socks5"}:
            raise ValueError("proxy protocol must be http, https, or socks5")
        if not 1 <= self.session_minutes <= 1_440:
            raise ValueError("session_minutes must be between 1 and 1440")


@dataclass(slots=True)
class DecodoDataPlaneLease:
    """Provider projection over an accounting-owning neutral static lease."""

    lease_id: str
    provider: str
    route: ProxyEndpoint
    _inner: StaticProxyLease
    _credentials: ProxyCredentials
    maximum_bytes: int | None
    expires_at: datetime | None = None

    def can_start(self, estimated_bytes: int = 0) -> bool:
        return self._inner.can_start(estimated_bytes)

    def http_credentials(self) -> ProxyCredentials:
        return self._credentials

    def browser_credentials(self) -> BrowserProxyCredentials:
        return BrowserProxyCredentials(
            server=f"{self.route.protocol}://{self.route.host}:{self.route.port}",
            username=self._credentials.username,
            password=self._credentials.password,
        )


class DecodoDataPlanePool:
    """Application-owned Decodo adapter satisfying the neutral pool protocol.

    The composed ``StaticProxyPool`` retains the library's tested accounting,
    health, and ownership semantics.  This class adds only Decodo capability
    validation and per-lease credential projection.
    """

    def __init__(self, config: DecodoDataPlaneConfig) -> None:
        self._config = config
        profile = config.profile
        self._inner = StaticProxyPool(
            (
                StaticRoute(
                    endpoint=ProxyEndpoint(
                        provider=_PROVIDER,
                        endpoint_id=config.endpoint_id,
                        protocol=config.protocol,
                        host=profile.host,
                        port=profile.port,
                        kind=ProxyKind.RESIDENTIAL,
                        countries=(
                            frozenset({config.country})
                            if config.country is not None
                            else frozenset()
                        ),
                    ),
                    credentials=ProxyCredentials(
                        username=SecretStr(profile.username),
                        password=SecretStr(profile.password),
                    ),
                ),
            )
        )
        self._leases: dict[str, DecodoDataPlaneLease] = {}
        self._sessions: dict[str, tuple[str | None, int]] = {}

    @property
    def active_leases(self) -> int:
        return len(self._leases)

    async def acquire(self, request: ProxyRequest) -> DecodoDataPlaneLease:
        self._validate_request(request)
        country = request.country or self._config.country
        session_minutes = self._session_minutes(request)
        inner = await self._inner.acquire(self._discharged(request))
        try:
            lease = self._project(inner, country, session_minutes)
        except BaseException:
            await self._inner.release(inner)
            raise
        self._leases[lease.lease_id] = lease
        return lease

    async def rotate(
        self, lease: ProxyLease, reason: RotationReason
    ) -> DecodoDataPlaneLease:
        current = self._owned(lease)
        country, session_minutes = self._sessions[current.lease_id]
        try:
            inner = await self._inner.rotate(current._inner, reason)
        except BaseException:
            # The composed pool invalidates the old lease before attempting to
            # acquire its replacement.  Mirror that ownership transition even
            # when no healthy replacement is available.
            self._forget(current.lease_id)
            raise
        self._forget(current.lease_id)
        try:
            # A fresh session token is what Decodo treats as a new exit
            # identity; the durable reservation is unchanged by rotation.
            replacement = self._project(inner, country, session_minutes)
        except BaseException:
            await self._inner.release(inner)
            raise
        self._leases[replacement.lease_id] = replacement
        return replacement

    async def authorize(
        self, lease: ProxyLease, estimated_bytes: int
    ) -> ProxyAttemptAuthorization | None:
        return await self._inner.authorize(self._owned(lease)._inner, estimated_bytes)

    async def report(self, lease: ProxyLease, outcome: ProxyOutcome) -> None:
        # One configured Decodo route serves the whole job, so cooling it down
        # on an exit-specific classification would deny the only path forward
        # while rotation -- a new session token on the same route -- is the
        # operation that actually recovers.
        if outcome.classification in {"blocked", "rate_limited", "captcha"}:
            self._owned(lease)
            return
        await self._inner.report(self._owned(lease)._inner, outcome)

    async def release(self, lease: ProxyLease) -> None:
        current = self._leases.get(lease.lease_id)
        if current is None:
            return
        if current is not lease:
            raise ProxyDenied("Decodo proxy lease is not active")
        self._forget(current.lease_id)
        await self._inner.release(current._inner)

    def _session_minutes(self, request: ProxyRequest) -> int:
        configured = self._config.session_minutes
        if request.session_ttl_seconds is None:
            return configured
        return min(configured, math.ceil(request.session_ttl_seconds / 60))

    @staticmethod
    def _discharged(request: ProxyRequest) -> ProxyRequest:
        """Drop constraints this adapter has already proven.

        The composed pool owns accounting, health, and ownership; it routes to
        one static endpoint and therefore refuses any constraint a static route
        cannot prove.  Session duration is proven here instead: it is clamped
        to the configured route capability and encoded into the Decodo
        username, which is where the provider reads it.

        Region and city are not discharged: ``_validate_request`` rejects them
        outright, so they never reach the inner pool.
        """

        if request.session_ttl_seconds is None:
            return request
        return request.model_copy(update={"session_ttl_seconds": None})

    def _project(
        self,
        inner: StaticProxyLease,
        country: str | None,
        session_minutes: int,
    ) -> DecodoDataPlaneLease:
        profile = self._config.profile
        username = profile.username_for(country, secrets.token_hex(12), session_minutes)
        lease = DecodoDataPlaneLease(
            lease_id=inner.lease_id,
            provider=inner.provider,
            route=inner.route,
            _inner=inner,
            _credentials=ProxyCredentials(
                username=SecretStr(username),
                password=SecretStr(profile.password),
            ),
            maximum_bytes=inner.maximum_bytes,
        )
        self._sessions[lease.lease_id] = (country, session_minutes)
        return lease

    def _forget(self, lease_id: str) -> None:
        self._leases.pop(lease_id, None)
        self._sessions.pop(lease_id, None)

    def _owned(self, lease: ProxyLease) -> DecodoDataPlaneLease:
        current = self._leases.get(lease.lease_id)
        if current is not lease:
            raise ProxyDenied("Decodo proxy lease is not active")
        return current

    def _validate_request(self, request: ProxyRequest) -> None:
        if request.kind is not ProxyKind.RESIDENTIAL:
            raise ProxyDenied("Decodo route supports residential requests only")
        if request.region is not None or request.city is not None:
            raise ProxyDenied(
                "configured Decodo route does not support region or city selection"
            )
        if (
            request.country
            and self._config.country
            and request.country != self._config.country
        ):
            raise ProxyDenied("proxy request country does not match the configured route")
        if _PROVIDER in request.excluded_providers:
            raise ProxyDenied("proxy request excludes the configured provider")
        if request.preferred_providers and _PROVIDER not in request.preferred_providers:
            raise ProxyDenied("proxy request does not select the configured provider")
