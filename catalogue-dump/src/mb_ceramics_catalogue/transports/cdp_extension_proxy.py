"""Safe Playwright adapter for an operator-managed cdp-extension-proxy.

This module never provisions Chromium itself.  A provider hands it an attested,
short-lived endpoint for one job; cleanup always returns that lease with
``destroy=True``.  Production validation deliberately rejects the proxy's
persistent-profile development mode and every non-direct egress route.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
from collections import defaultdict
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Protocol
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, field_validator

from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.transports.browser import (
    BrowserEvaluationResult,
    BrowserFetchResponse,
    BrowserJobContext,
    BrowserUnavailable,
    TransportBlocked,
)

LOGGER = obs.get_logger("catalogue.cdp_extension_proxy")


class CdpReadinessError(BrowserUnavailable):
    """The endpoint cannot safely be advertised as a worker capability."""

    retryable = True


class CdpOperatorProfile(BaseModel):
    """Worker-owned logical profile; never accepted in a crawl request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,62}$")
    endpoint: str
    token_secret_ref: str
    health_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    capacity: int = Field(default=1, ge=1)
    allowed_worker_pool: str
    network_scope: Literal["loopback", "private"] = "loopback"
    trusted_private_hostnames: frozenset[str] = frozenset()
    route: Literal["direct", "proxy"] = "direct"
    isolation: Literal["ephemeral_profile", "isolated_context", "persistent_default"]
    expected_service_version: str
    expected_profile_generation: str

    @field_validator("endpoint")
    @classmethod
    def _http_endpoint(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https", "ws", "wss"} or not parsed.hostname:
            raise ValueError("endpoint must be an HTTP(S) or WebSocket CDP endpoint")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("endpoint must not contain credentials, query parameters, or fragments")
        return value.rstrip("/")

    def safe_projection(self) -> dict[str, Any]:
        """Non-secret fields suitable for diagnostics and job snapshots."""
        return {
            "name": self.name,
            "capacity": self.capacity,
            "allowed_worker_pool": self.allowed_worker_pool,
            "network_scope": self.network_scope,
            "route": self.route,
            "isolation": self.isolation,
            "expected_service_version": self.expected_service_version,
            "expected_profile_generation": self.expected_profile_generation,
        }


class CdpEndpointAttestation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    instance_id: str
    service_version: str
    route: Literal["direct", "proxy"]
    isolation: Literal["ephemeral_profile", "isolated_context", "persistent_default"]
    profile_generation: str
    clean_profile: bool
    capacity: int = Field(default=1, ge=1)


class CdpEndpointLease(BaseModel):
    """Short-lived connection material. Secret values are redacted by repr/dump."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    lease_id: str
    endpoint: str
    token: SecretStr
    expires_at: datetime
    attestation: CdpEndpointAttestation

    @field_validator("expires_at")
    @classmethod
    def _timezone_required(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("expires_at must include a timezone")
        return value

    def connection_url(self) -> str:
        parsed = urlsplit(self.endpoint)
        query = parse_qsl(parsed.query, keep_blank_values=True)
        query.append(("token", self.token.get_secret_value()))
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), ""))


class CdpSecretResolver(Protocol):
    def resolve(self, reference: str) -> SecretStr: ...


class MappingSecretResolver:
    """Small resolver for mounted-secret loaders and deterministic tests."""

    def __init__(self, secrets: Mapping[str, str]) -> None:
        self._secrets = secrets

    def resolve(self, reference: str) -> SecretStr:
        try:
            value = self._secrets[reference]
        except KeyError as error:
            raise CdpReadinessError(f"CDP token secret {reference!r} is unavailable") from error
        if not value:
            raise CdpReadinessError(f"CDP token secret {reference!r} is empty")
        return SecretStr(value)


class CdpEndpointProvider(Protocol):
    development_only: bool

    async def acquire(self, job_id: str, logical_profile: str) -> CdpEndpointLease: ...

    async def release(self, lease: CdpEndpointLease, *, destroy: bool) -> None: ...

    async def shutdown(self) -> None: ...


class StaticCdpEndpointProvider:
    """Single fixed endpoint for local diagnostics; categorically non-production."""

    development_only = True

    def __init__(
        self, profile: CdpOperatorProfile, resolver: CdpSecretResolver,
        attestation: CdpEndpointAttestation,
    ) -> None:
        self.profile = profile
        self.resolver = resolver
        self.attestation = attestation

    async def acquire(self, job_id: str, logical_profile: str) -> CdpEndpointLease:
        if logical_profile != self.profile.name:
            raise CdpReadinessError(f"unknown CDP operator profile {logical_profile!r}")
        return CdpEndpointLease(
            lease_id=f"static:{self.attestation.instance_id}:{job_id}",
            endpoint=self.profile.endpoint,
            token=self.resolver.resolve(self.profile.token_secret_ref),
            expires_at=datetime.max.replace(tzinfo=UTC),
            attestation=self.attestation,
        )

    async def release(self, lease: CdpEndpointLease, *, destroy: bool) -> None:
        return None

    async def shutdown(self) -> None:
        return None


class FileSecretResolver(MappingSecretResolver):
    """Resolve CDP tokens from one operator-mounted, read-only JSON object."""

    def __init__(self, path: Path) -> None:
        try:
            mode = path.stat().st_mode & 0o777
        except OSError as error:
            raise CdpReadinessError(f"cannot read CDP secret file {path}") from error
        if mode & 0o077:
            raise CdpReadinessError("CDP secret file must not be accessible by group or others")
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CdpReadinessError(f"invalid CDP secret file {path}") from error
        if not isinstance(raw, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in raw.items()
        ):
            raise CdpReadinessError("CDP secret file must contain a string-to-string object")
        super().__init__(raw)
        obs.register_secrets(set(raw.values()))


def load_cdp_profiles(path: Path) -> dict[str, CdpOperatorProfile]:
    """Load logical, non-secret profiles from worker-owned configuration."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CdpReadinessError(f"invalid CDP profiles file {path}") from error
    if not isinstance(raw, dict):
        raise CdpReadinessError("CDP profiles file must contain an object")
    profiles: dict[str, CdpOperatorProfile] = {}
    for logical_name, value in raw.items():
        if not isinstance(value, dict):
            raise CdpReadinessError(f"CDP profile {logical_name!r} must be an object")
        profile = CdpOperatorProfile.model_validate({"name": logical_name, **value})
        profiles[logical_name] = profile
    return profiles


class HttpCdpEndpointProvider:
    """Client for a distributed operator-owned disposable-browser pool.

    The pool allocates and destroys instances; this client supplies endpoint
    authentication from the worker's mounted secret file rather than accepting
    it in an API response.  Capacity and exclusivity are enforced by the pool's
    lease, so multiple catalogue workers can safely share it.
    """

    development_only = False

    def __init__(
        self, pool_endpoint: str, pool_token: SecretStr,
        profiles: Mapping[str, CdpOperatorProfile], resolver: CdpSecretResolver, *,
        timeout_seconds: float = 10.0,
        trusted_private_hostnames: frozenset[str] = frozenset(),
        client: httpx.AsyncClient | None = None,
    ) -> None:
        endpoint = pool_endpoint.rstrip("/")
        parsed = urlsplit(endpoint)
        if parsed.scheme not in {"https", "http"} or not parsed.hostname:
            raise CdpReadinessError("CDP pool endpoint must be an HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise CdpReadinessError("CDP pool endpoint must not contain credentials or a query")
        try:
            pool_address = ipaddress.ip_address(parsed.hostname)
        except ValueError:
            pool_address = None
        loopback = parsed.hostname == "localhost" or bool(pool_address and pool_address.is_loopback)
        private = bool(pool_address and pool_address.is_private)
        trusted_name = parsed.hostname in trusted_private_hostnames
        if not (loopback or private or trusted_name):
            raise CdpReadinessError("CDP pool endpoint is not operator-trusted as private")
        if parsed.scheme != "https" and not loopback:
            raise CdpReadinessError("non-loopback CDP pool endpoints require TLS")
        if not pool_token.get_secret_value():
            raise CdpReadinessError("CDP pool authentication is required")
        self.pool_endpoint = endpoint
        self.pool_token = pool_token
        self.profiles = dict(profiles)
        self.resolver = resolver
        self._client = client or httpx.AsyncClient(timeout=timeout_seconds)
        self._owns_client = client is None
        obs.register_secrets({pool_token.get_secret_value()})

    def _headers(self) -> dict[str, str]:
        return {"authorization": f"Bearer {self.pool_token.get_secret_value()}"}

    async def acquire(self, job_id: str, logical_profile: str) -> CdpEndpointLease:
        profile = self.profiles.get(logical_profile)
        if profile is None:
            raise CdpReadinessError(f"unknown CDP operator profile {logical_profile!r}")
        try:
            response = await self._client.post(
                f"{self.pool_endpoint}/v1/cdp/leases",
                headers=self._headers(),
                json={"job_id": job_id, "logical_profile": logical_profile},
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as error:
            # Response bodies may contain connection material. Never include
            # one in an exception that becomes a job diagnostic or log line.
            raise CdpReadinessError("CDP endpoint pool acquisition failed") from error
        if not isinstance(payload, dict):
            raise CdpReadinessError("CDP endpoint pool returned an invalid lease")
        try:
            return CdpEndpointLease.model_validate(
                {**payload, "token": self.resolver.resolve(profile.token_secret_ref)}
            )
        except ValidationError as error:
            raise CdpReadinessError("CDP endpoint pool returned an invalid lease") from error

    async def release(self, lease: CdpEndpointLease, *, destroy: bool) -> None:
        last_error: httpx.HTTPError | None = None
        for attempt in range(3):
            try:
                response = await self._client.post(
                    f"{self.pool_endpoint}/v1/cdp/leases/{quote(lease.lease_id, safe='')}/release",
                    headers=self._headers(),
                    json={"destroy": destroy},
                )
                response.raise_for_status()
                return
            except httpx.HTTPError as error:
                last_error = error
                if attempt < 2:
                    await asyncio.sleep(0.2 * (attempt + 1))
        raise CdpReadinessError("CDP endpoint pool cleanup failed") from last_error

    async def shutdown(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def validate_cdp_readiness(
    profile: CdpOperatorProfile, lease: CdpEndpointLease, *, production: bool,
    provider_development_only: bool = False,
) -> None:
    """Fail closed unless route, isolation and generation match operator intent."""
    attestation = lease.attestation
    if datetime.now(UTC) >= lease.expires_at:
        raise CdpReadinessError("CDP endpoint lease has expired")
    if not lease.token.get_secret_value():
        raise CdpReadinessError("CDP endpoint authentication is required")
    if lease.endpoint.rstrip("/") != profile.endpoint:
        raise CdpReadinessError("CDP provider returned an endpoint outside the logical profile")
    if attestation.service_version != profile.expected_service_version:
        raise CdpReadinessError("CDP service version does not match the logical profile")
    if attestation.profile_generation != profile.expected_profile_generation:
        raise CdpReadinessError("CDP clean-profile generation does not match the logical profile")
    if attestation.route != profile.route or attestation.isolation != profile.isolation:
        raise CdpReadinessError("CDP route/isolation attestation does not match the logical profile")
    if attestation.capacity > profile.capacity:
        raise CdpReadinessError("CDP endpoint capacity exceeds the operator-approved capacity")

    if not production:
        return
    if provider_development_only:
        raise CdpReadinessError("static CDP endpoints are development-only")
    if profile.route != "direct":
        raise CdpReadinessError("production CDP traffic must use the direct route")
    if profile.isolation not in {"ephemeral_profile", "isolated_context"}:
        raise CdpReadinessError("a persistent default browser profile is not a job isolation boundary")
    if not attestation.clean_profile:
        raise CdpReadinessError("CDP endpoint did not attest a clean job profile")
    if profile.capacity != 1 or attestation.capacity != 1:
        raise CdpReadinessError("the current CDP proxy contract requires capacity one")
    _validate_private_endpoint(profile)


def _validate_private_endpoint(profile: CdpOperatorProfile) -> None:
    parsed = urlsplit(profile.endpoint)
    hostname = parsed.hostname or ""
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        if hostname != "localhost" and profile.network_scope == "loopback":
            raise CdpReadinessError(
                "loopback CDP profiles must use localhost or a loopback IP"
            ) from None
        if profile.network_scope == "private":
            if parsed.scheme not in {"https", "wss"}:
                raise CdpReadinessError("named private-network CDP endpoints require TLS") from None
            if hostname not in profile.trusted_private_hostnames:
                raise CdpReadinessError(
                    "named private-network CDP endpoint is not operator-trusted"
                ) from None
        return
    if profile.network_scope == "loopback" and not address.is_loopback:
        raise CdpReadinessError("loopback CDP profile resolved to a non-loopback address")
    if profile.network_scope == "private" and not (address.is_private or address.is_loopback):
        raise CdpReadinessError("CDP endpoint must be on a private network")


class CdpExtensionProxyBackend:
    backend: Literal["cdp_extension_proxy"] = "cdp_extension_proxy"

    def __init__(
        self, profile: CdpOperatorProfile, provider: CdpEndpointProvider, *,
        production: bool = True, playwright_factory: Any = None,
    ) -> None:
        self.profile = profile
        self.provider = provider
        self.production = production
        self._playwright_factory = playwright_factory

    @asynccontextmanager
    async def open_session(
        self, job: BrowserJobContext | None = None,
    ) -> AsyncIterator[CdpExtensionProxySession]:
        if job is None or not job.job_id:
            raise CdpReadinessError("CDP sessions require a job identity")
        logical_profile = job.logical_profile or self.profile.name
        if logical_profile != self.profile.name:
            raise CdpReadinessError("job selected a different logical CDP profile")
        lease = await self.provider.acquire(job.job_id, logical_profile)
        try:
            validate_cdp_readiness(
                self.profile, lease, production=self.production,
                provider_development_only=self.provider.development_only,
            )
            obs.register_secrets({lease.token.get_secret_value(), lease.connection_url()})
            session = await CdpExtensionProxySession.connect(
                lease, self.provider, self.profile, self._playwright_factory,
            )
        except BaseException:
            await self.provider.release(lease, destroy=True)
            raise
        try:
            yield session
        finally:
            await session.close()

    async def shutdown(self) -> None:
        """Close provider resources after all job leases have been released."""
        await self.provider.shutdown()

    async def probe(self) -> None:
        """Exercise allocation, attestation and the browser-level CDP attach."""
        context = BrowserJobContext(f"readiness-{uuid4()}", self.profile.name)
        async with self.open_session(context):
            pass


class CdpExtensionProxySession:
    def __init__(
        self, lease: CdpEndpointLease, provider: CdpEndpointProvider, playwright: Any,
        browser: Any, context: Any, *, owns_context: bool,
    ) -> None:
        self.lease = lease
        self.provider = provider
        self.playwright = playwright
        self.browser = browser
        self.context = context
        self.owns_context = owns_context
        self._pages: set[Any] = set()
        self._origin_pages: dict[str, Any] = {}
        self._origin_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._closed = False

    @classmethod
    async def connect(
        cls, lease: CdpEndpointLease, provider: CdpEndpointProvider,
        profile: CdpOperatorProfile, playwright_factory: Any,
    ) -> CdpExtensionProxySession:
        if playwright_factory is None:
            try:
                from playwright.async_api import async_playwright
            except ImportError as error:  # pragma: no cover - optional environment
                raise CdpReadinessError("Playwright is not installed for the CDP backend") from error
            playwright_factory = async_playwright
        playwright = await playwright_factory().start()
        try:
            browser = await playwright.chromium.connect_over_cdp(lease.connection_url())
            if profile.isolation == "isolated_context":
                context = await browser.new_context()
                owns_context = True
            else:
                if len(browser.contexts) != 1:
                    raise CdpReadinessError("ephemeral CDP instance exposed an unexpected context set")
                context = browser.contexts[0]
                owns_context = False
            return cls(lease, provider, playwright, browser, context, owns_context=owns_context)
        except BaseException:
            await playwright.stop()
            raise

    async def _new_page(self) -> Any:
        if self._closed:
            raise CdpReadinessError("CDP browser session is closed")
        page = await self.context.new_page()
        self._pages.add(page)
        return page

    async def render(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None,
    ) -> str:
        page = await self._new_page()
        try:
            await self._load(page, url, wait_ms, wait_for)
            return await page.content()
        finally:
            await self._close_page(page)

    async def evaluate(
        self, url: str, script: str, wait_ms: int = 2000,
        wait_for: str | None = None,
    ) -> Any:
        return (await self.evaluate_result(url, script, wait_ms, wait_for)).value

    async def evaluate_result(
        self, url: str, script: str, wait_ms: int = 2000,
        wait_for: str | None = None,
    ) -> BrowserEvaluationResult:
        page = await self._new_page()
        try:
            await self._load(page, url, wait_ms, wait_for)
            return BrowserEvaluationResult(
                value=await page.evaluate(script),
                final_url=str(page.url),
            )
        finally:
            await self._close_page(page)

    async def _load(self, page: Any, url: str, wait_ms: int, wait_for: str | None) -> None:
        await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
        if wait_for:
            try:
                await page.wait_for_selector(wait_for, timeout=15_000)
            except Exception:  # noqa: BLE001 - same non-fatal selector semantics as Camoufox
                LOGGER.debug("cdp.selector_missing", selector=wait_for, url=url)
        await page.wait_for_timeout(wait_ms)

    async def request_json(
        self, page_url: str, endpoint: str, *, method: str = "POST",
        headers: dict[str, str] | None = None, body: Any = None,
    ) -> Any:
        response = await self.request(
            page_url,
            endpoint,
            method=method,
            headers=headers,
            json_body=body,
        )
        if response.status >= 400:
            raise TransportBlocked(
                f"{endpoint} returned {response.status} in the browser context"
            )
        try:
            return json.loads(response.content)
        except json.JSONDecodeError as error:
            raise TransportBlocked(
                f"{endpoint} did not return JSON in the browser context"
            ) from error

    async def request(
        self,
        page_url: str,
        endpoint: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        json_body: Any = None,
    ) -> BrowserFetchResponse:
        origin = urlsplit(page_url).netloc
        async with self._origin_locks[origin]:
            page = self._origin_pages.get(origin)
            if page is None or page.is_closed():
                page = await self._new_page()
                await page.goto(page_url, wait_until="domcontentloaded", timeout=45_000)
                self._origin_pages[origin] = page
            result = await page.evaluate(
                """async ({endpoint, method, headers, body}) => {
                    const response = await fetch(endpoint, {
                        method, headers,
                        body: body === null ? undefined : JSON.stringify(body),
                        credentials: 'include',
                    });
                    return {
                        status: response.status,
                        headers: Object.fromEntries(response.headers.entries()),
                        text: await response.text(),
                        url: response.url,
                    };
                }""",
                {
                    "endpoint": endpoint,
                    "method": method,
                    "headers": headers or {},
                    "body": json_body,
                },
            )
        return BrowserFetchResponse(
            status=int(result["status"]),
            headers=MappingProxyType(
                {str(key): str(value) for key, value in result["headers"].items()}
            ),
            content=str(result["text"]).encode("utf-8"),
            final_url=str(result["url"]),
        )

    async def _close_page(self, page: Any) -> None:
        self._pages.discard(page)
        if not page.is_closed():
            await page.close()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        cleanup_error: BaseException | None = None
        try:
            for page in list(self._pages):
                try:
                    await self._close_page(page)
                except BaseException as error:  # noqa: BLE001 - cancellation must still destroy lease
                    cleanup_error = cleanup_error or error
            if self.owns_context:
                try:
                    await self.context.close()
                except BaseException as error:  # noqa: BLE001 - cancellation must still destroy lease
                    cleanup_error = cleanup_error or error
            # Do not call browser.close(): this client does not own operator Chromium.
            try:
                await self.playwright.stop()
            except BaseException as error:  # noqa: BLE001 - cancellation must still destroy lease
                cleanup_error = cleanup_error or error
        finally:
            try:
                await self.provider.release(self.lease, destroy=True)
            except BaseException as error:  # noqa: BLE001 - cancellation must still destroy lease
                cleanup_error = cleanup_error or error
        if cleanup_error is not None:
            raise cleanup_error
