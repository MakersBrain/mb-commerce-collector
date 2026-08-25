"""Async neutral cache projection over the catalogue response archive."""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass

from mb_commerce_scraper.transports import (
    BrowserHint,
    CachePolicy,
    ResponseCacheLookup,
    RouteMetadata,
    TransportFailure,
    TransportRequest,
    TransportResponse,
)

from mb_ceramics_catalogue.scrapers.cache import CachedResponse, ResponseCache


@dataclass(frozen=True, slots=True)
class CatalogueCacheStats:
    """Immutable counters retained after one collection closes."""

    hits: int = 0
    misses: int = 0
    writes: int = 0
    bytes_read: int = 0

    def __add__(self, other: CatalogueCacheStats) -> CatalogueCacheStats:
        return CatalogueCacheStats(
            hits=self.hits + other.hits,
            misses=self.misses + other.misses,
            writes=self.writes + other.writes,
            bytes_read=self.bytes_read + other.bytes_read,
        )

    def summary(self, mode: str) -> str:
        total = self.hits + self.misses
        share = self.hits / total * 100 if total else 0.0
        return (
            f"cache mode={mode} hits={self.hits} ({share:.0f}%) "
            f"misses={self.misses} stored={self.writes}"
        )


@dataclass(frozen=True, slots=True)
class _CatalogueCacheLookup(ResponseCacheLookup):
    fresh: TransportResponse | None
    stale: TransportResponse | None
    _cache: CatalogueResponseCache
    _request: TransportRequest
    _key: str | None

    async def put(self, response: TransportResponse) -> None:
        if self._key is not None:
            await self._cache._put(self._request, response, self._key)


class CatalogueResponseCache:
    """Preserve legacy cache keys and replay semantics for native connectors."""

    def __init__(self, cache: ResponseCache) -> None:
        self._cache = cache

    def stats(self) -> CatalogueCacheStats:
        """Snapshot counters without exposing the mutable legacy archive."""
        return CatalogueCacheStats(
            hits=self._cache.hits,
            misses=self._cache.misses,
            writes=self._cache.writes,
            bytes_read=self._cache.bytes_read,
        )

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

    async def get_with_stale(self, request: TransportRequest) -> ResponseCacheLookup:
        """Classify one archive read and retain its key for a later write."""
        if request.cache is not CachePolicy.DEFAULT:
            return _CatalogueCacheLookup(None, None, self, request, None)
        key = self._key(request)
        fresh, stale = await asyncio.to_thread(
            self._cache.read_with_stale,
            key,
            request.url,
        )
        if fresh is None and self._cache.mode == "replay":
            raise TransportFailure("replay cache entry is unavailable")
        return _CatalogueCacheLookup(
            self._response(fresh, request) if fresh is not None else None,
            self._response(stale, request) if stale is not None else None,
            self,
            request,
            key,
        )

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
        await self._put(request, response, self._key(request))

    async def _put(
        self,
        request: TransportRequest,
        response: TransportResponse,
        key: str,
    ) -> None:
        entry = CachedResponse(
            status=response.status,
            url=response.final_url,
            body=response.content.decode("utf-8", errors="replace"),
            headers=dict(response.headers),
            fetched_at=time.time(),
            kind=self._kind(request),
        )
        await asyncio.to_thread(self._cache.write, key, entry)

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
            agent=_has_user_agent(request.headers),
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


def _has_user_agent(headers: dict[str, str]) -> bool:
    return any(name.casefold() == "user-agent" for name in headers)
