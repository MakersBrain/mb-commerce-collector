"""Webshare residential gateway adapter for the neutral proxy data plane.

This module deliberately lives in the catalogue application: Webshare's
username grammar and backbone endpoint are vendor details.  The scraper core
sees only its ``ProxyPool`` and ``ProxyLease`` contracts.

Gateway semantics verified against Webshare's official Proxy Connection and
Proxy List documentation on 2026-08-22:

* residential backbone connections use ``p.webshare.io``;
* a two-letter country code is appended to the provider-issued username;
* a numeric session id selects a sticky exit, while ``rotate`` requests a new
  exit for every physical request;
* sticky duration is configured in the Webshare dashboard, not in the
  username.  It is therefore an explicit operator-provided capability here.

The adapter intentionally does not open catalogue billing reservations.
Applications must compose it with their own fail-closed spend authorization
before enabling paid traffic.
"""

from __future__ import annotations

import secrets
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

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

from mb_ceramics_catalogue.proxy import ProxyDenied

_PROVIDER = "webshare"
_DEFAULT_HOST = "p.webshare.io"
_DEFAULT_PORT = 80


@dataclass(frozen=True, slots=True)
class WebshareGatewayConfig:
    """Operator-verified credentials and capabilities for one backbone route."""

    username: SecretStr
    password: SecretStr
    endpoint_id: str = "webshare-residential-backbone"
    host: str = _DEFAULT_HOST
    port: int = _DEFAULT_PORT
    countries: frozenset[str] = frozenset()
    sticky_session_ttl_seconds: int = 30 * 60

    def __post_init__(self) -> None:
        if not self.username.get_secret_value() or not self.password.get_secret_value():
            raise ValueError("Webshare gateway credentials must not be empty")
        if not self.endpoint_id:
            raise ValueError("Webshare gateway endpoint id must not be empty")
        if "://" in self.host or "@" in self.host or not self.host:
            raise ValueError("Webshare gateway host must not contain URL user information")
        if self.port not in {80, 1_080, 3_128} and not 9_999 <= self.port <= 19_999:
            raise ValueError("Webshare HTTP gateway port is not documented for backbone access")
        if not 60 <= self.sticky_session_ttl_seconds <= 24 * 60 * 60:
            raise ValueError("Webshare sticky session duration must be between 1 minute and 24 hours")
        invalid_countries = [
            country
            for country in self.countries
            if (
                len(country) != 2
                or not country.isascii()
                or country != country.upper()
                or not country.isalpha()
            )
        ]
        if invalid_countries:
            raise ValueError("Webshare gateway countries must be uppercase ISO alpha-2 codes")


@dataclass(slots=True)
class WebshareGatewayLease:
    """Provider projection over an accounting-owning neutral static lease."""

    lease_id: str
    provider: str
    route: ProxyEndpoint
    _inner: StaticProxyLease
    _credentials: ProxyCredentials
    expires_at: datetime | None
    maximum_bytes: int | None

    def can_start(self, estimated_bytes: int = 0) -> bool:
        return (
            (self.expires_at is None or self.expires_at > datetime.now(UTC))
            and self._inner.can_start(estimated_bytes)
        )

    def http_credentials(self) -> ProxyCredentials:
        return self._credentials

    def browser_credentials(self) -> BrowserProxyCredentials:
        return BrowserProxyCredentials(
            server=f"{self.route.protocol}://{self.route.host}:{self.route.port}",
            username=self._credentials.username,
            password=self._credentials.password,
        )


class WebshareGatewayPool:
    """Application-owned Webshare adapter satisfying the neutral pool protocol.

    The composed ``StaticProxyPool`` retains the library's tested accounting,
    health, and ownership semantics.  This class adds only Webshare capability
    validation and per-lease credential projection.
    """

    def __init__(
        self,
        config: WebshareGatewayConfig,
        *,
        session_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._config = config
        self._session_id_factory = session_id_factory or _numeric_session_id
        base_credentials = ProxyCredentials(
            username=config.username,
            password=config.password,
        )
        self._inner = StaticProxyPool(
            (
                StaticRoute(
                    endpoint=ProxyEndpoint(
                        provider=_PROVIDER,
                        endpoint_id=config.endpoint_id,
                        protocol="http",
                        host=config.host,
                        port=config.port,
                        kind=ProxyKind.RESIDENTIAL,
                        countries=config.countries,
                    ),
                    credentials=base_credentials,
                ),
            )
        )
        self._leases: dict[str, WebshareGatewayLease] = {}

    @property
    def active_leases(self) -> int:
        return len(self._leases)

    async def acquire(self, request: ProxyRequest) -> WebshareGatewayLease:
        self._validate_request(request)
        inner = await self._inner.acquire(request)
        try:
            lease = self._project(inner)
        except BaseException:
            await self._inner.release(inner)
            raise
        self._leases[lease.lease_id] = lease
        return lease

    async def rotate(
        self, lease: ProxyLease, reason: RotationReason
    ) -> WebshareGatewayLease:
        current = self._owned(lease)
        try:
            inner = await self._inner.rotate(current._inner, reason)
        except BaseException:
            # The composed pool invalidates the old lease before attempting to
            # acquire its replacement.  Mirror that ownership transition even
            # when no healthy replacement is available.
            self._leases.pop(current.lease_id, None)
            raise
        self._leases.pop(current.lease_id, None)
        try:
            replacement = self._project(inner)
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
        # These classifications describe the selected residential exit, not
        # the shared Webshare backbone.  Rotation creates a new numeric sticky
        # identity, so cooling down the only gateway endpoint would prevent the
        # provider operation that can recover.  Transport failures still flow
        # to the neutral endpoint-health policy unchanged.
        if outcome.classification in {"blocked", "rate_limited", "captcha"}:
            self._owned(lease)
            return
        await self._inner.report(self._owned(lease)._inner, outcome)

    async def release(self, lease: ProxyLease) -> None:
        current = self._leases.get(lease.lease_id)
        if current is None:
            return
        if current is not lease:
            raise ProxyDenied("Webshare proxy lease is not active")
        self._leases.pop(lease.lease_id)
        await self._inner.release(current._inner)

    def _project(self, inner: StaticProxyLease) -> WebshareGatewayLease:
        request = inner.request
        username = self._config.username.get_secret_value()
        if request.country is not None:
            username = f"{username}-{request.country.lower()}"
        if request.sticky:
            session_id = self._session_id_factory()
            if not session_id.isascii() or not session_id.isdigit():
                raise ProxyDenied("Webshare sticky session ids must contain ASCII digits only")
            username = f"{username}-{session_id}"
            expires_at = datetime.now(UTC) + timedelta(
                seconds=self._config.sticky_session_ttl_seconds
            )
        else:
            username = f"{username}-rotate"
            expires_at = None
        return WebshareGatewayLease(
            lease_id=inner.lease_id,
            provider=inner.provider,
            route=inner.route,
            _inner=inner,
            _credentials=ProxyCredentials(
                username=SecretStr(username),
                password=self._config.password,
            ),
            expires_at=expires_at,
            maximum_bytes=inner.maximum_bytes,
        )

    def _owned(self, lease: ProxyLease) -> WebshareGatewayLease:
        current = self._leases.get(lease.lease_id)
        if current is not lease:
            raise ProxyDenied("Webshare proxy lease is not active")
        assert current is not None
        return current

    def _validate_request(self, request: ProxyRequest) -> None:
        if request.kind is not ProxyKind.RESIDENTIAL:
            raise ProxyDenied("Webshare backbone adapter supports residential requests only")
        if request.country is not None and (
            len(request.country) != 2
            or not request.country.isascii()
            or request.country != request.country.upper()
            or not request.country.isalpha()
        ):
            raise ProxyDenied("Webshare country must be an uppercase ISO alpha-2 code")
        if request.region is not None or request.city is not None:
            raise ProxyDenied(
                "Webshare state and city targeting require separately verified normalization"
            )
        if request.session_ttl_seconds is not None and (
            not request.sticky
            or request.session_ttl_seconds > self._config.sticky_session_ttl_seconds
        ):
            raise ProxyDenied(
                "requested Webshare session duration exceeds the configured gateway capability"
            )


def _numeric_session_id() -> str:
    # Webshare documents a numeric id.  A 63-bit value is compact while making
    # accidental identity reuse negligible without retaining process state.
    return str(secrets.randbits(63))
