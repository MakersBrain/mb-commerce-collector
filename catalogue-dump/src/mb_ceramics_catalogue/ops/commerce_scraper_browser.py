"""Catalogue Camoufox projection for a neutral sticky proxy lease."""

from __future__ import annotations

import asyncio
import json
from contextlib import AbstractAsyncContextManager
from typing import Any, Literal, Protocol, cast
from urllib.parse import urlsplit, urlunsplit

import httpx
from mb_commerce_scraper.proxy import (
    BrowserProxyCredentials,
    BrowserSubrequestAuthorizer,
    ProxyLease,
)
from mb_commerce_scraper.transports import (
    DEFAULT_MAXIMUM_RESPONSE_BYTES,
    BrowserBackendUnavailable,
    BrowserHint,
    RotationReason,
    RouteMetadata,
    TransportAccounting,
    TransportFailure,
    TransportRequest,
    TransportResponse,
    enforce_response_body_limit,
    estimated_transmitted_bytes,
)

from mb_ceramics_catalogue.scrapers.base import BrowserRenderer
from mb_ceramics_catalogue.transports.browser import (
    BrowserBackend,
    BrowserJobContext,
    BrowserNetworkAccounting,
    BrowserSession,
    BrowserUnavailable,
    TransportBlocked,
)


class BrowserOriginDenied(ValueError):
    """A browser request escaped the application-approved source origins."""

    def __init__(self) -> None:
        super().__init__("browser request origin is not allowed")
        self.accounting = TransportAccounting(physical_requests=0)


class _BrowserOriginPolicy:
    def __init__(self, origins: tuple[str, ...]) -> None:
        if not origins:
            raise ValueError("browser transport requires at least one allowed origin")
        self._origins = frozenset(self._origin(value) for value in origins)

    def validate(self, url: str) -> None:
        self._validate(url, reject_fragment=True)

    def validate_response(self, url: str) -> None:
        self._validate(url, reject_fragment=False)

    def _validate(self, url: str, *, reject_fragment: bool) -> None:
        try:
            parsed = urlsplit(url)
            origin = self._origin(url)
        except ValueError:
            raise BrowserOriginDenied() from None
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.password is not None
            or (reject_fragment and parsed.fragment)
            or origin not in self._origins
        ):
            raise BrowserOriginDenied()

    @staticmethod
    def _origin(url: str) -> tuple[str, str, int]:
        parsed = urlsplit(url)
        scheme = parsed.scheme.casefold()
        default_port = 443 if scheme == "https" else 80 if scheme == "http" else 0
        return scheme, (parsed.hostname or "").casefold(), parsed.port or default_port


class BorrowedBrowserTransport:
    """Adapt one application-owned browser backend to the neutral transport.

    The backend remains process-owned. This collection-scoped adapter owns only
    the job session it opens and replaces that session when identity rotates.
    """

    def __init__(
        self,
        backend: BrowserBackend,
        job: BrowserJobContext,
        *,
        allowed_origins: tuple[str, ...],
        maximum_response_bytes: int = DEFAULT_MAXIMUM_RESPONSE_BYTES,
    ) -> None:
        if maximum_response_bytes < 1:
            raise ValueError("maximum_response_bytes must be positive")
        self._backend = backend
        self._job = job
        self._origin_policy = _BrowserOriginPolicy(allowed_origins)
        self._maximum_response_bytes = maximum_response_bytes
        self._session_context: AbstractAsyncContextManager[BrowserSession] | None = None
        self._session: BrowserSession | None = None
        self._lock = asyncio.Lock()
        self._closed = False

    async def request(self, request: TransportRequest) -> TransportResponse:
        if request.browser is not BrowserHint.REQUIRED:
            raise ValueError("borrowed browser transport accepts required-browser requests only")
        if request.body is not None:
            raise ValueError(
                "borrowed browser transport cannot send an opaque byte body; use json_body"
            )
        async with self._lock:
            if self._closed:
                raise BrowserBackendUnavailable("borrowed browser transport is closed")
            endpoint = (
                str(httpx.URL(request.url, params=request.query))
                if request.query
                else request.url
            )
            page_url = _browser_page_url(request.headers, endpoint)
            self._origin_policy.validate(endpoint)
            self._origin_policy.validate(page_url)
            dispatched = False
            try:
                session = await self._session_for_request()
                method = request.method.upper()
                dispatched = True
                if request.evaluation is not None:
                    evaluation = request.evaluation
                    result = await session.evaluate_result(
                        endpoint,
                        evaluation.script,
                        evaluation.wait_milliseconds,
                        evaluation.wait_for,
                    )
                    content = json.dumps(
                        result.value, ensure_ascii=False, separators=(",", ":")
                    ).encode()
                    status = 200
                    headers = {"content-type": "application/json"}
                    final_url = result.final_url
                    self._origin_policy.validate_response(final_url)
                elif method == "GET" and not request.headers and request.json_body is None:
                    document = await session.render(endpoint)
                    status = 200
                    headers = {"content-type": "text/html; charset=utf-8"}
                    content = document.encode()
                    final_url = endpoint
                else:
                    browser_response = await session.request(
                        page_url,
                        endpoint,
                        method=method,
                        headers=request.headers or None,
                        json_body=request.json_body,
                    )
                    status = browser_response.status
                    headers = dict(browser_response.headers)
                    content = browser_response.content
                    final_url = browser_response.final_url
                    self._origin_policy.validate_response(final_url)
            except asyncio.CancelledError as error:
                error.accounting = self._accounting(  # type: ignore[attr-defined]
                    request, physical_requests=1 if dispatched else 0
                )
                raise
            except BrowserUnavailable:
                # This is an application placement failure. Preserve its exact
                # type so Worker can durably requeue onto a browser-capable
                # process rather than treating it as a remote transport error.
                unavailable = BrowserUnavailable("browser backend is unavailable")
                cast(Any, unavailable).accounting = self._accounting(
                    request, physical_requests=1 if dispatched else 0
                )
                raise unavailable from None
            except TransportBlocked:
                raise TransportFailure(
                    "browser transport was blocked",
                    accounting=self._accounting(
                        request, physical_requests=1 if dispatched else 0
                    ),
                ) from None
            except Exception as error:  # noqa: BLE001 - detach backend exception data
                raise TransportFailure(
                    f"browser transport failed: {type(error).__name__}",
                    accounting=self._accounting(
                        request, physical_requests=1 if dispatched else 0
                    ),
                ) from None

            accounting = self._accounting(request, received_bytes=len(content))
            return enforce_response_body_limit(
                TransportResponse(
                    status=status,
                    headers=headers,
                    content=content,
                    final_url=final_url,
                    route=RouteMetadata(kind="browser"),
                    accounting=accounting,
                ),
                self._maximum_response_bytes,
            )

    async def rotate_identity(self, reason: RotationReason) -> None:
        del reason
        async with self._lock:
            if self._closed:
                raise BrowserBackendUnavailable("borrowed browser transport is closed")
            await self._close_session()

    async def aclose(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            await self._close_session()

    async def _session_for_request(self) -> BrowserSession:
        if self._session is None:
            context = self._backend.open_session(self._job)
            session = await context.__aenter__()
            self._session_context = context
            self._session = session
        return self._session

    async def _close_session(self) -> None:
        context = self._session_context
        self._session_context = None
        self._session = None
        if context is not None:
            await context.__aexit__(None, None, None)

    @staticmethod
    def _accounting(
        request: TransportRequest,
        *,
        physical_requests: int = 1,
        received_bytes: int = 0,
    ) -> TransportAccounting:
        # The legacy browser abstraction exposes one logical operation, not its
        # network waterfall. This is therefore a conservative top-level request
        # estimate; proxy browsers use callback-derived exact subrequest totals.
        return TransportAccounting(
            physical_requests=physical_requests,
            transmitted_bytes=estimated_transmitted_bytes(request),
            received_bytes=received_bytes,
        )


class ProxyBrowserBackendFactory(Protocol):
    def __call__(
        self,
        credentials: BrowserProxyCredentials,
        accounting: BrowserNetworkAccounting,
        authorizer: BrowserSubrequestAuthorizer,
    ) -> BrowserBackend: ...


class CamoufoxProxyBrowserTransportFactory:
    """Build one lazy, route-generation-owned browser per neutral lease."""

    browser_subrequests_authorized: Literal[True] = True

    def __init__(
        self,
        backend_factory: ProxyBrowserBackendFactory | None = None,
        *,
        allowed_origins: tuple[str, ...],
    ) -> None:
        self._backend_factory = backend_factory or _camoufox_backend
        self._allowed_origins = allowed_origins

    def build(
        self,
        lease: ProxyLease,
        authorizer: BrowserSubrequestAuthorizer,
    ) -> CamoufoxProxyBrowserTransport:
        return CamoufoxProxyBrowserTransport(
            lease.browser_credentials(),
            authorizer,
            backend_factory=self._backend_factory,
            allowed_origins=self._allowed_origins,
        )


class CamoufoxProxyBrowserTransport:
    """Render requests and expose aggregate allowed-subrequest accounting."""

    browser_subrequests_authorized: Literal[True] = True

    def __init__(
        self,
        credentials: BrowserProxyCredentials,
        authorizer: BrowserSubrequestAuthorizer,
        *,
        backend_factory: ProxyBrowserBackendFactory,
        allowed_origins: tuple[str, ...],
    ) -> None:
        self._accounting = BrowserNetworkAccounting()
        self._backend = backend_factory(credentials, self._accounting, authorizer)
        self._origin_policy = _BrowserOriginPolicy(allowed_origins)
        self._session_context: AbstractAsyncContextManager[BrowserSession] | None = None
        self._session: BrowserSession | None = None
        # Browser page callbacks update a shared monotonic counter. Serializing
        # logical renders keeps each response's delta attributable to one
        # RoutedTransport authorization.
        self._request_lock = asyncio.Lock()
        self._closed = False

    async def request(self, request: TransportRequest) -> TransportResponse:
        if request.browser is not BrowserHint.REQUIRED:
            raise ValueError("proxy browser transport accepts required-browser requests only")
        if request.body is not None:
            raise ValueError(
                "proxy browser transport cannot send an opaque byte body; use json_body"
            )
        async with self._request_lock:
            if self._closed:
                raise BrowserBackendUnavailable("proxy browser transport is closed")
            endpoint = (
                str(httpx.URL(request.url, params=request.query))
                if request.query
                else request.url
            )
            page_url = _browser_page_url(request.headers, endpoint)
            self._origin_policy.validate(endpoint)
            self._origin_policy.validate(page_url)
            before = self._accounting.snapshot()
            failure: tuple[str, str] | None = None
            cancelled: asyncio.CancelledError | None = None
            try:
                session = await self._session_for_request()
                method = request.method.upper()
                if request.evaluation is not None:
                    evaluation = request.evaluation
                    result = await session.evaluate_result(
                        endpoint,
                        evaluation.script,
                        evaluation.wait_milliseconds,
                        evaluation.wait_for,
                    )
                    content = json.dumps(
                        result.value, ensure_ascii=False, separators=(",", ":")
                    ).encode()
                    status = 200
                    headers = {"content-type": "application/json"}
                    final_url = result.final_url
                    self._origin_policy.validate_response(final_url)
                elif method == "GET" and not request.headers and request.json_body is None:
                    document = await session.render(endpoint)
                    status = 200
                    headers = {"content-type": "text/html; charset=utf-8"}
                    content = document.encode("utf-8")
                    final_url = endpoint
                else:
                    browser_response = await session.request(
                        page_url,
                        endpoint,
                        method=method,
                        headers=request.headers or None,
                        json_body=request.json_body,
                    )
                    status = browser_response.status
                    headers = dict(browser_response.headers)
                    content = browser_response.content
                    final_url = browser_response.final_url
                    self._origin_policy.validate_response(final_url)
            except asyncio.CancelledError as error:
                cancelled = error
            except BrowserUnavailable:
                failure = ("unavailable", "browser backend is unavailable")
            except TransportBlocked:
                failure = ("blocked", "browser transport was blocked")
            except Exception as error:  # noqa: BLE001 - detach provider exception data
                failure = ("backend", f"browser transport failed: {type(error).__name__}")

            if cancelled is not None:
                accounting = self._delta(before)
                # RoutedTransport consumes accounting attached to cancellation
                # without retaining browser exception details.
                cancelled.accounting = accounting  # type: ignore[attr-defined]
                raise cancelled
            if failure is not None:
                kind, message = failure
                if kind == "unavailable":
                    raise BrowserBackendUnavailable(message) from None
                raise TransportFailure(message, accounting=self._delta(before)) from None

            accounting = self._delta(before, retained_bytes=len(content))
            return TransportResponse(
                status=status,
                headers=headers,
                content=content,
                final_url=final_url,
                route=RouteMetadata(kind="browser"),
                accounting=accounting,
            )

    async def rotate_identity(self, reason: RotationReason) -> None:
        # RoutedTransport closes this generation before rotating the lease and
        # constructs a fresh browser bound to the replacement credentials.
        del reason

    async def aclose(self) -> None:
        async with self._request_lock:
            if self._closed:
                return
            self._closed = True
            try:
                if self._session_context is not None:
                    await self._session_context.__aexit__(None, None, None)
            finally:
                self._session = None
                self._session_context = None
                await self._backend.shutdown()

    async def _session_for_request(self) -> BrowserSession:
        if self._session is None:
            context = self._backend.open_session()
            session = await context.__aenter__()
            self._session_context = context
            self._session = session
        return self._session

    def _delta(
        self,
        before: tuple[int, int, int],
        *,
        retained_bytes: int = 0,
    ) -> TransportAccounting:
        after = self._accounting.snapshot()
        physical_requests = after[0] - before[0]
        transmitted_bytes = after[1] - before[1]
        received_bytes = after[2] - before[2]
        # Content-Length is not guaranteed for browser responses. DOM bytes are
        # a conservative fallback only when observed response lengths are lower.
        return TransportAccounting(
            physical_requests=physical_requests,
            transmitted_bytes=transmitted_bytes,
            received_bytes=max(received_bytes, retained_bytes),
        )


def _browser_page_url(headers: dict[str, str], endpoint: str) -> str:
    for name, value in headers.items():
        if name.casefold() == "referer":
            return value
    parsed = urlsplit(endpoint)
    return urlunsplit((parsed.scheme, parsed.netloc, "/", "", ""))


def _camoufox_backend(
    credentials: BrowserProxyCredentials,
    accounting: BrowserNetworkAccounting,
    authorizer: BrowserSubrequestAuthorizer,
) -> BrowserRenderer:
    return BrowserRenderer(
        True,
        pages=1,
        proxy_configuration={
            "server": credentials.server,
            "username": credentials.username.get_secret_value(),
            "password": credentials.password.get_secret_value(),
        },
        network_accounting=accounting,
        subrequest_authorizer=authorizer,
    )
