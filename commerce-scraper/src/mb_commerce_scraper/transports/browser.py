from __future__ import annotations

from typing import Protocol

from pydantic import JsonValue

from ..models.policies import BrowserPolicy
from .base import (
    DEFAULT_MAXIMUM_RESPONSE_BYTES,
    BrowserHint,
    CommerceTransport,
    RotationReason,
    RouteMetadata,
    TelemetryHooks,
    TransportRequest,
    TransportResponse,
    enforce_response_body_limit,
    transport_trace_fields,
)
from .telemetry import safe_telemetry


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
        policy: BrowserPolicy = BrowserPolicy.ALLOW,
        proxy_browser_supported: bool = True,
        telemetry: TelemetryHooks | None = None,
        telemetry_context: dict[str, JsonValue] | None = None,
        maximum_response_bytes: int = DEFAULT_MAXIMUM_RESPONSE_BYTES,
    ) -> None:
        self._http = http
        self._browser = browser
        self._policy = policy
        self._proxy_browser_supported = proxy_browser_supported
        self._telemetry = safe_telemetry(telemetry) if telemetry is not None else None
        self._telemetry_context = dict(telemetry_context or {})
        if maximum_response_bytes < 1:
            raise ValueError("maximum_response_bytes must be positive")
        self._maximum_response_bytes = maximum_response_bytes

    async def request(self, request: TransportRequest) -> TransportResponse:
        if self._policy == BrowserPolicy.NEVER:
            if request.browser == BrowserHint.REQUIRED:
                self._emit(
                    "browser.denied",
                    request,
                    reason="policy_never",
                    requested=request.browser.value,
                    level="warning",
                )
                raise BrowserBackendUnavailable(
                    "request requires a browser transport, but browser policy forbids it"
                )
            self._emit(
                "browser.dispatch",
                request,
                selected="http",
                requested=request.browser.value,
                policy=self._policy.value,
            )
            return self._bounded(await self._http.request(request))

        browser_request = request
        if self._policy == BrowserPolicy.REQUIRE and request.browser == BrowserHint.OPTIONAL:
            browser_request = request.model_copy(update={"browser": BrowserHint.REQUIRED})

        if browser_request.browser != BrowserHint.REQUIRED:
            self._emit(
                "browser.dispatch",
                browser_request,
                selected="http",
                requested=request.browser.value,
                policy=self._policy.value,
            )
            return self._bounded(await self._http.request(browser_request))
        if self._browser is None:
            self._emit(
                "browser.denied",
                browser_request,
                reason="backend_unavailable",
                requested=request.browser.value,
                level="warning",
            )
            raise BrowserBackendUnavailable(
                "request requires a browser transport, but no browser backend is configured"
            )
        if not self._proxy_browser_supported:
            self._emit(
                "browser.denied",
                browser_request,
                reason="proxy_browser_unsupported",
                requested=request.browser.value,
                level="warning",
            )
            raise ProxyBrowserRoutingUnsupported(
                "browser-required requests cannot use the active proxy lease with the "
                "configured browser backend"
            )
        self._emit(
            "browser.dispatch",
            browser_request,
            selected="browser",
            requested=request.browser.value,
            policy=self._policy.value,
            promoted=(browser_request.browser is not request.browser),
        )
        response = await self._browser.request(browser_request)
        route = response.route
        return self._bounded(
            response.model_copy(
                update={
                    "route": RouteMetadata(
                        kind="browser",
                        provider=route.provider,
                        endpoint_id=route.endpoint_id,
                        lease_id=route.lease_id,
                    )
                }
            )
        )

    def _bounded(self, response: TransportResponse) -> TransportResponse:
        return enforce_response_body_limit(response, self._maximum_response_bytes)

    def _emit(
        self,
        event: str,
        request: TransportRequest,
        **fields: JsonValue,
    ) -> None:
        if self._telemetry is not None:
            self._telemetry.emit(
                event,
                {
                    **self._telemetry_context,
                    **transport_trace_fields(request),
                    "level": "debug",
                    "url": request.url,
                    "purpose": request.purpose.value,
                    **fields,
                },
            )

    async def rotate_identity(self, reason: RotationReason) -> None:
        await self._http.rotate_identity(reason)
        if self._browser is not None and self._browser is not self._http:
            await self._browser.rotate_identity(reason)
