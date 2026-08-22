"""Async neutral cache projection over the catalogue response archive."""

from __future__ import annotations

import asyncio
import hashlib
import time

from mb_commerce_scraper.transports import (
    BrowserHint,
    CachePolicy,
    RouteMetadata,
    TransportFailure,
    TransportRequest,
    TransportResponse,
)

from mb_ceramics_catalogue.scrapers.cache import CachedResponse, ResponseCache


class CatalogueResponseCache:
    """Preserve legacy cache keys and replay semantics for native connectors."""

    def __init__(self, cache: ResponseCache) -> None:
        self._cache = cache

    async def get(self, request: TransportRequest) -> TransportResponse | None:
        if request.cache is not CachePolicy.DEFAULT:
            return None
        key = self._key(request)
        entry = await asyncio.to_thread(self._cache.read, key, request.url)
        if entry is None:
            if self._cache.mode == "replay":
                raise TransportFailure("replay cache entry is unavailable")
            return None
        return self._response(entry, request)

    async def stale(self, request: TransportRequest) -> TransportResponse | None:
        """Return an expired HTTP entry for validators or explicit fallback."""
        if request.cache is not CachePolicy.DEFAULT:
            return None
        entry = await asyncio.to_thread(
            self._cache.stale_read, self._key(request), request.url
        )
        return self._response(entry, request) if entry is not None else None

    @staticmethod
    def _response(
        entry: CachedResponse, request: TransportRequest
    ) -> TransportResponse:
        return TransportResponse(
            status=entry.status,
            headers=entry.headers,
            content=entry.body.encode("utf-8"),
            final_url=entry.url or request.url,
            route=RouteMetadata(kind="cache"),
            from_cache=True,
        )

    async def put(
        self, request: TransportRequest, response: TransportResponse
    ) -> None:
        if request.cache is not CachePolicy.DEFAULT:
            return
        entry = CachedResponse(
            status=response.status,
            url=response.final_url,
            body=response.content.decode("utf-8", errors="replace"),
            headers=dict(response.headers),
            fetched_at=time.time(),
            kind=self._kind(request),
        )
        await asyncio.to_thread(self._cache.write, self._key(request), entry)

    def _key(self, request: TransportRequest) -> str:
        if request.evaluation is not None:
            evaluation = request.evaluation
            return self._cache.key(
                "browser_evaluation",
                request.url,
                action_id=evaluation.action_id,
                script_sha256=hashlib.sha256(evaluation.script.encode()).hexdigest(),
                wait_for=evaluation.wait_for,
                wait_milliseconds=evaluation.wait_milliseconds,
            )
        if self._is_simple_render(request):
            return self._cache.key(
                "render", request.url, wait_ms=1500, wait_for=None
            )
        kind = self._kind(request)
        return self._cache.key(
            kind,
            request.url,
            method=request.method,
            params=request.query or None,
            body=request.json_body,
            agent=False,
        )

    @staticmethod
    def _is_simple_render(request: TransportRequest) -> bool:
        return (
            request.browser is BrowserHint.REQUIRED
            and request.method.upper() == "GET"
            and not request.query
            and not request.headers
            and request.json_body is None
            and request.body is None
            and request.evaluation is None
        )

    @classmethod
    def _kind(cls, request: TransportRequest) -> str:
        if request.evaluation is not None:
            return "browser_evaluation"
        if cls._is_simple_render(request):
            return "render"
        return (
            "browser_request"
            if request.browser is BrowserHint.REQUIRED
            else "http"
        )
