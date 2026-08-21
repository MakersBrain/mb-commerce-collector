from __future__ import annotations

from typing import Protocol

from .base import (
    BrowserHint,
    CommerceTransport,
    RotationReason,
    RouteMetadata,
    TransportRequest,
    TransportResponse,
)


class BrowserBackendUnavailable(RuntimeError):
    """A browser-required request cannot be downgraded to plain HTTP."""


class ProxyBrowserRoutingUnsupported(RuntimeError):
    """The configured proxy lease cannot be projected into the browser backend."""


class BrowserTransport(Protocol):
    """Dependency-free contract implemented by an optional browser backend."""

    async def request(self, request: TransportRequest) -> TransportResponse: ...

    async def rotate_identity(self, reason: RotationReason) -> None: ...


class BrowserDispatchTransport(CommerceTransport):
    """Route required browser work without changing ordinary HTTP behavior.

    Backends are borrowed. Their owner remains responsible for closing them.
    """

    def __init__(
        self,
        http: CommerceTransport,
        browser: BrowserTransport | None = None,
        *,
        proxy_browser_supported: bool = True,
    ) -> None:
        self._http = http
        self._browser = browser
        self._proxy_browser_supported = proxy_browser_supported

    async def request(self, request: TransportRequest) -> TransportResponse:
        if request.browser != BrowserHint.REQUIRED:
            return await self._http.request(request)
        if self._browser is None:
            raise BrowserBackendUnavailable(
                "request requires a browser transport, but no browser backend is configured"
            )
        if not self._proxy_browser_supported:
            raise ProxyBrowserRoutingUnsupported(
                "browser-required requests cannot use the active proxy lease with the "
                "configured browser backend"
            )
        response = await self._browser.request(request)
        route = response.route
        return response.model_copy(
            update={
                "route": RouteMetadata(
                    kind="browser",
                    provider=route.provider,
                    endpoint_id=route.endpoint_id,
                    lease_id=route.lease_id,
                )
            }
        )

    async def rotate_identity(self, reason: RotationReason) -> None:
        await self._http.rotate_identity(reason)
        if self._browser is not None and self._browser is not self._http:
            await self._browser.rotate_identity(reason)
