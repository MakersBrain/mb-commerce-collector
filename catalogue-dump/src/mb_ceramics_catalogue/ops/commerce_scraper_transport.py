"""Application-owned bridge from the legacy fetch runtime to library connectors.

This adapter is deliberately kept out of :mod:`mb_commerce_scraper`: the
library owns the neutral transport protocol, while catalogue owns the legacy
``Fetcher`` lifecycle, response cache, browser, proxy lease, and retry policy.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from typing import Any, Literal, Protocol, cast

import httpx
from mb_commerce_scraper import sanitize_diagnostic_text
from mb_commerce_scraper.transports import (
    DEFAULT_MAXIMUM_RESPONSE_BYTES,
    BrowserHint,
    NullTelemetry,
    ResponseBodyTooLarge,
    RobotsDenied,
    RotationReason,
    RouteMetadata,
    TransportFailure,
    TransportRequest,
    TransportResponse,
)
from mb_commerce_scraper.transports.base import TelemetryHooks
from mb_commerce_scraper.transports.telemetry import safe_telemetry
from pydantic import JsonValue

from mb_ceramics_catalogue.proxy import ProxyDenied
from mb_ceramics_catalogue.scrapers.base import Blocked


class LegacyFetcher(Protocol):
    """The narrow part of ``scrapers.base.Fetcher`` used during migration."""

    proxy_lease: Any
    stats: Any

    @property
    def limiter(self) -> LegacyLimiter: ...

    async def response(
        self,
        url: str,
        *,
        params: dict[str, str | int | float | bool] | None = None,
        method: str = "GET",
        json_body: JsonValue | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response: ...

    async def render(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None
    ) -> str: ...

    async def request_json_in_browser(
        self,
        page_url: str,
        endpoint: str,
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        body: Any = None,
    ) -> Any: ...

    async def rotate_client(self) -> None: ...

    async def may_fetch(
        self, url: str, ignore_robots: bool = False, obey_robots: bool = False
    ) -> bool: ...


class BrowserEvaluationFetcher(Protocol):
    """Optional legacy capability used only by typed evaluation requests."""

    async def evaluate_in_browser(
        self,
        url: str,
        script: str,
        wait_ms: int = 2000,
        wait_for: str | None = None,
        *,
        action_id: str = "legacy-evaluate.v1",
    ) -> Any: ...


class LegacyLimiter(Protocol):
    def join_group(self, url: str, group: str) -> None: ...

    def set_delay(self, url: str, delay: float) -> None: ...


class LegacyFetcherTransport:
    """Run neutral connectors through catalogue's existing fetch policy.

    It is a transitional composition adapter, not a second middleware stack.
    The wrapped fetcher remains the sole owner of retries, caching, robots,
    rate limiting, proxy fallback, and browser dispatch. Consequently a
    library runtime must not wrap this object in equivalent middleware.
    """

    def __init__(
        self,
        fetcher: LegacyFetcher,
        *,
        telemetry: TelemetryHooks | None = None,
        telemetry_context: Mapping[str, JsonValue] | None = None,
        maximum_response_bytes: int = DEFAULT_MAXIMUM_RESPONSE_BYTES,
        ignore_robots: bool = False,
        obey_robots: bool = False,
    ) -> None:
        if maximum_response_bytes < 1:
            raise ValueError("maximum_response_bytes must be positive")
        self._fetcher = fetcher
        self._telemetry = safe_telemetry(telemetry or NullTelemetry())
        self._telemetry_context = dict(telemetry_context or {})
        self._maximum_response_bytes = maximum_response_bytes
        self._ignore_robots = ignore_robots
        self._obey_robots = obey_robots

    async def request(self, request: TransportRequest) -> TransportResponse:
        if request.body is not None:
            raise ValueError("legacy Fetcher transport cannot send an opaque byte body")
        requested_url = (
            str(httpx.URL(request.url, params=request.query))
            if request.query
            else request.url
        )
        if not await self._fetcher.may_fetch(
            requested_url,
            self._ignore_robots,
            self._obey_robots,
        ):
            self._telemetry.emit(
                "catalogue.legacy_fetcher.robots.denied",
                self._request_fields(request, url=requested_url),
            )
            raise RobotsDenied("robots policy denies request")
        if request.browser == BrowserHint.REQUIRED:
            return await self._render(request)

        fields = self._request_fields(request)
        self._telemetry.emit("catalogue.legacy_fetcher.request.started", fields)
        started = time.monotonic()
        proxy_before = self._proxy_requests()
        failure: TransportFailure | None = None
        try:
            response = await self._fetcher.response(
                request.url,
                params=request.query or None,
                method=request.method,
                json_body=request.json_body,
                headers=request.headers or None,
            )
        except httpx.HTTPStatusError as error:
            # Fetcher raises a final status after applying its retry/fallback
            # policy. Library connectors own status classification, so retain
            # the response instead of disguising it as a transport failure.
            response = error.response
        except (httpx.TransportError, Blocked, ProxyDenied, UnicodeError) as error:
            self._telemetry.emit(
                "catalogue.legacy_fetcher.request.failed",
                {
                    **fields,
                    "duration_seconds": time.monotonic() - started,
                    "error_type": type(error).__name__,
                },
            )
            failure = _safe_transport_failure(error)
        except BaseException as error:
            self._telemetry.emit(
                "catalogue.legacy_fetcher.request.failed",
                {
                    **fields,
                    "duration_seconds": time.monotonic() - started,
                    "error_type": type(error).__name__,
                },
            )
            raise

        if failure is not None:
            raise failure
        content = bytes(response.content)
        received_bytes = len(content)
        if received_bytes > self._maximum_response_bytes:
            del content, response
            _raise_response_body_too_large(
                maximum_bytes=self._maximum_response_bytes,
                received_bytes=received_bytes,
            )

        cache_provenance = _cache_provenance(response)
        route: Literal["direct", "proxy", "cache"] = (
            "cache"
            if cache_provenance is not None
            else "proxy"
            if self._proxy_requests() > proxy_before
            else "direct"
        )
        elapsed = time.monotonic() - started
        self._telemetry.emit(
            "catalogue.legacy_fetcher.request.completed",
            {
                **fields,
                "status": response.status_code,
                "duration_seconds": elapsed,
                "response_bytes": received_bytes,
                "route": route,
                "cache_provenance": cache_provenance,
            },
        )
        return TransportResponse(
            status=response.status_code,
            headers=dict(response.headers),
            content=content,
            final_url=str(response.url),
            route=RouteMetadata(kind=route),
            from_cache=cache_provenance is not None,
            elapsed_seconds=elapsed,
        )

    async def _render(self, request: TransportRequest) -> TransportResponse:
        url = str(httpx.URL(request.url, params=request.query)) if request.query else request.url
        fields = self._request_fields(request, url=url)
        self._telemetry.emit("catalogue.legacy_fetcher.request.started", fields)
        started = time.monotonic()
        failure: TransportFailure | None = None
        try:
            if request.evaluation is not None:
                evaluation = request.evaluation
                payload = await cast(
                    BrowserEvaluationFetcher, self._fetcher
                ).evaluate_in_browser(
                    url,
                    evaluation.script,
                    wait_ms=evaluation.wait_milliseconds,
                    wait_for=evaluation.wait_for,
                    action_id=evaluation.action_id,
                )
                content = json.dumps(payload, separators=(",", ":")).encode()
            elif request.method.upper() == "GET" and request.json_body is None:
                content = (await self._fetcher.render(url)).encode()
            else:
                page_url = next(
                    (
                        value
                        for name, value in request.headers.items()
                        if name.casefold() == "referer"
                    ),
                    request.url,
                )
                payload = await self._fetcher.request_json_in_browser(
                    page_url,
                    url,
                    method=request.method.upper(),
                    headers=request.headers or None,
                    body=request.json_body,
                )
                content = json.dumps(payload, separators=(",", ":")).encode()
        except (httpx.TransportError, Blocked, ProxyDenied, UnicodeError) as error:
            self._telemetry.emit(
                "catalogue.legacy_fetcher.request.failed",
                {
                    **fields,
                    "duration_seconds": time.monotonic() - started,
                    "error_type": type(error).__name__,
                },
            )
            failure = _safe_transport_failure(error)
        except BaseException as error:
            self._telemetry.emit(
                "catalogue.legacy_fetcher.request.failed",
                {
                    **fields,
                    "duration_seconds": time.monotonic() - started,
                    "error_type": type(error).__name__,
                },
            )
            raise
        if failure is not None:
            raise failure
        received_bytes = len(content)
        if received_bytes > self._maximum_response_bytes:
            del content
            _raise_response_body_too_large(
                maximum_bytes=self._maximum_response_bytes,
                received_bytes=received_bytes,
            )
        elapsed = time.monotonic() - started
        self._telemetry.emit(
            "catalogue.legacy_fetcher.request.completed",
            {
                **fields,
                "status": 200,
                "duration_seconds": elapsed,
                "response_bytes": received_bytes,
                "route": "browser",
            },
        )
        return TransportResponse(
            status=200,
            headers={
                "content-type": (
                    "text/html; charset=utf-8"
                    if request.evaluation is None
                    and request.method.upper() == "GET"
                    and request.json_body is None
                    else "application/json"
                )
            },
            content=content,
            final_url=url,
            route=RouteMetadata(kind="browser"),
            elapsed_seconds=elapsed,
        )

    async def rotate_identity(self, reason: RotationReason) -> None:
        self._telemetry.emit(
            "catalogue.legacy_fetcher.identity.rotate",
            {"reason": reason.value, **self._telemetry_context},
        )
        await self._fetcher.rotate_client()

    def _proxy_requests(self) -> int:
        value = getattr(self._fetcher.stats, "proxy_requests", 0)
        return value if isinstance(value, int) else 0

    def _request_fields(
        self,
        request: TransportRequest, *, url: str | None = None
    ) -> dict[str, JsonValue]:
        return {
            "method": request.method.upper(),
            "url": url or request.url,
            "purpose": request.purpose.value,
            "priority": request.priority.name.casefold(),
            "required": request.required,
            "browser": request.browser.value,
            "estimated_bytes": request.estimated_bytes,
            "browser_action": (
                request.evaluation.action_id
                if request.evaluation is not None
                else None
            ),
            # Application-owned identity is immutable for this composed
            # transport and wins over protocol fields with the same name.
            **self._telemetry_context,
        }


def _safe_transport_failure(error: BaseException) -> TransportFailure:
    message = sanitize_diagnostic_text(
        f"{type(error).__name__}: {error}",
        max_length=2_048,
    )
    return TransportFailure(message)


def _cache_provenance(response: httpx.Response) -> Literal["fresh", "replayed", "stale"] | None:
    value = response.extensions.get("catalogue_cache_provenance")
    provenances: dict[str, Literal["fresh", "replayed", "stale"]] = {
        "fresh": "fresh",
        "replayed": "replayed",
        "stale": "stale",
    }
    return provenances.get(value) if isinstance(value, str) else None


def _raise_response_body_too_large(
    *, maximum_bytes: int, received_bytes: int
) -> None:
    raise ResponseBodyTooLarge(
        maximum_bytes=maximum_bytes,
        received_bytes=received_bytes,
    )
