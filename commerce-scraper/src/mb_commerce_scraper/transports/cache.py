"""Application-neutral bounded response caches."""

from __future__ import annotations

import asyncio
import base64
import binascii
import gzip
import hashlib
import json
import math
import os
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from mb_commerce_scraper.models.sanitization import sanitize_url

from .base import (
    DEFAULT_MAXIMUM_RESPONSE_BYTES,
    CachePolicy,
    ResponseCacheLookup,
    RouteMetadata,
    StaleResponseCache,
    TransportRequest,
    TransportResponse,
    enforce_response_body_limit,
)

_CACHE_SCHEMA = 1
_CACHE_SUFFIX = ".response.json.gz"
_SAFE_RESPONSE_HEADERS = frozenset(
    {
        "cache-control",
        "content-language",
        "content-type",
        "etag",
        "expires",
        "last-modified",
    }
)
_KEYED_REQUEST_HEADERS = frozenset({"accept", "accept-language", "content-type"})
_CREDENTIAL_MARKERS = (
    "authorization",
    "cookie",
    "password",
    "passwd",
    "passphrase",
    "secret",
    "apikey",
    "accesskey",
    "privatekey",
    "clientkey",
    "credential",
    "signature",
    "token",
    "proxyuser",
)
_MAXIMUM_FINAL_URL_LENGTH = 2_048
_MAXIMUM_CLOCK_SKEW_SECONDS = 300


@dataclass(frozen=True, slots=True)
class _RequestCacheIdentity:
    key: str
    eligible: bool


@dataclass(frozen=True, slots=True)
class _FileCacheLookup(ResponseCacheLookup):
    fresh: TransportResponse | None
    stale: TransportResponse | None
    _cache: FileResponseCache
    _identity: _RequestCacheIdentity
    _writable: bool

    async def put(self, response: TransportResponse) -> None:
        if self._writable:
            await self._cache._put(self._identity, response)


@dataclass(frozen=True, slots=True)
class _MemoryCacheLookup(ResponseCacheLookup):
    fresh: TransportResponse | None
    stale: TransportResponse | None
    _cache: MemoryResponseCache
    _identity: _RequestCacheIdentity
    _writable: bool

    async def put(self, response: TransportResponse) -> None:
        if self._writable:
            self._cache._put(self._identity, response)


def _normalized_name(value: str) -> str:
    return "".join(character for character in value.casefold() if character.isalnum())


def _credential_name(value: str) -> bool:
    normalized = _normalized_name(value)
    return normalized in {"auth", "key", "sig"} or any(marker in normalized for marker in _CREDENTIAL_MARKERS)


def _normalized_url_and_query(
    request: TransportRequest,
) -> tuple[str, tuple[tuple[str, str], ...]] | None:
    try:
        parsed = urlsplit(request.url)
        port = parsed.port
    except ValueError:
        return None
    scheme = parsed.scheme.casefold()
    host = parsed.hostname
    if (
        scheme not in {"http", "https"}
        or host is None
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    try:
        normalized_host = host.encode("idna").decode("ascii").casefold()
    except UnicodeError:
        return None
    if ":" in normalized_host:
        normalized_host = f"[{normalized_host}]"
    default_port = (scheme == "http" and port in {None, 80}) or (scheme == "https" and port in {None, 443})
    authority = normalized_host if default_port else f"{normalized_host}:{port}"
    query = [*parse_qsl(parsed.query, keep_blank_values=True)]
    query.extend(
        (key, str(value).lower() if isinstance(value, bool) else str(value))
        for key, value in request.query.items()
    )
    if any(_credential_name(key) for key, _ in query):
        return None
    normalized_query = tuple(sorted(query))
    url = urlunsplit(
        (
            scheme,
            authority,
            parsed.path or "/",
            urlencode(normalized_query, doseq=True),
            "",
        )
    )
    return url, normalized_query


def _request_cache_identity(request: TransportRequest) -> _RequestCacheIdentity:
    normalized = _normalized_url_and_query(request)
    has_credential_header = any(_credential_name(name) for name in request.headers)
    eligible = normalized is not None and not has_credential_header
    normalized_url, normalized_query = normalized or ("[ineligible]", ())
    relevant_headers = {
        name.casefold(): value
        for name, value in request.headers.items()
        if name.casefold() in _KEYED_REQUEST_HEADERS
    }
    body = request.body or b""
    if request.json_body is not None:
        body = json.dumps(
            request.json_body,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    value = {
        "schema": 3,
        "method": request.method.upper(),
        "url": normalized_url,
        "query": normalized_query,
        "headers": relevant_headers,
        "body": hashlib.sha256(body).hexdigest(),
        "browser": request.browser.value,
        "evaluation": (
            {
                "action_id": request.evaluation.action_id,
                "script_sha256": hashlib.sha256(request.evaluation.script.encode()).hexdigest(),
                "wait_for": request.evaluation.wait_for,
                "wait_milliseconds": request.evaluation.wait_milliseconds,
            }
            if request.evaluation is not None
            else None
        ),
    }
    serialized = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return _RequestCacheIdentity(
        key=hashlib.sha256(serialized.encode()).hexdigest(),
        eligible=eligible,
    )


class MemoryResponseCache(StaleResponseCache):
    """Bounded-process cache useful for embedding and deterministic tests."""

    def __init__(
        self,
        *,
        maximum_entries: int = 1_000,
        maximum_response_bytes: int = DEFAULT_MAXIMUM_RESPONSE_BYTES,
    ) -> None:
        if maximum_entries < 1:
            raise ValueError("maximum_entries must be positive")
        if maximum_response_bytes < 1:
            raise ValueError("maximum_response_bytes must be positive")
        self.maximum_entries = maximum_entries
        self.maximum_response_bytes = maximum_response_bytes
        self._entries: dict[str, TransportResponse] = {}

    async def get(self, request: TransportRequest) -> TransportResponse | None:
        return (await self.get_with_stale(request)).fresh

    async def get_with_stale(self, request: TransportRequest) -> ResponseCacheLookup:
        identity = _request_cache_identity(request)
        if not identity.eligible or request.cache is not CachePolicy.DEFAULT:
            return _MemoryCacheLookup(
                None,
                None,
                self,
                identity,
                identity.eligible and request.cache is not CachePolicy.BYPASS,
            )
        response = self._entries.get(identity.key)
        if response is None:
            return _MemoryCacheLookup(None, None, self, identity, True)
        fresh = response.model_copy(
            update={
                "route": RouteMetadata(kind="cache"),
                "from_cache": True,
                "elapsed_seconds": 0,
                "accounting": None,
            }
        )
        return _MemoryCacheLookup(fresh, None, self, identity, True)

    async def put(self, request: TransportRequest, response: TransportResponse) -> None:
        identity = _request_cache_identity(request)
        if not identity.eligible or request.cache is CachePolicy.BYPASS:
            return
        self._put(identity, response)

    def _put(
        self,
        identity: _RequestCacheIdentity,
        response: TransportResponse,
    ) -> None:
        enforce_response_body_limit(response, self.maximum_response_bytes)
        if identity.key not in self._entries and len(self._entries) >= self.maximum_entries:
            self._entries.pop(next(iter(self._entries)))
        self._entries[identity.key] = response

    @staticmethod
    def key(request: TransportRequest) -> str:
        return _request_cache_identity(request).key


class FileResponseCache(StaleResponseCache):
    """Bounded binary-safe filesystem response cache with no eager I/O."""

    def __init__(
        self,
        directory: Path | str,
        *,
        maximum_age_seconds: float | None = None,
        maximum_response_bytes: int = DEFAULT_MAXIMUM_RESPONSE_BYTES,
        maximum_artifact_bytes: int | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if maximum_age_seconds is not None and (
            not math.isfinite(maximum_age_seconds) or maximum_age_seconds < 0
        ):
            raise ValueError("maximum_age_seconds must be finite and non-negative")
        if maximum_response_bytes < 1:
            raise ValueError("maximum_response_bytes must be positive")
        if maximum_artifact_bytes is None:
            maximum_artifact_bytes = maximum_response_bytes * 2 + 65_536
        if maximum_artifact_bytes < 1:
            raise ValueError("maximum_artifact_bytes must be positive")
        self.directory = Path(directory)
        self.maximum_age_seconds = maximum_age_seconds
        self.maximum_response_bytes = maximum_response_bytes
        self.maximum_artifact_bytes = maximum_artifact_bytes
        self._clock = clock

    async def get(self, request: TransportRequest) -> TransportResponse | None:
        return (await self.get_with_stale(request)).fresh

    async def stale(self, request: TransportRequest) -> TransportResponse | None:
        identity = _request_cache_identity(request)
        if not identity.eligible or request.cache is not CachePolicy.DEFAULT:
            return None
        stored = await asyncio.to_thread(self._read, identity.key)
        if stored is None or self._age(stored[1]) is None:
            return None
        return stored[0]

    async def get_with_stale(self, request: TransportRequest) -> ResponseCacheLookup:
        identity = _request_cache_identity(request)
        writable = identity.eligible and request.cache is not CachePolicy.BYPASS
        if not identity.eligible or request.cache is not CachePolicy.DEFAULT:
            return _FileCacheLookup(None, None, self, identity, writable)
        stored = await asyncio.to_thread(self._read, identity.key)
        if stored is None:
            return _FileCacheLookup(None, None, self, identity, writable)
        response, stored_at = stored
        age = self._age(stored_at)
        if age is None:
            return _FileCacheLookup(None, None, self, identity, writable)
        if self.maximum_age_seconds is not None and age > self.maximum_age_seconds:
            return _FileCacheLookup(None, response, self, identity, writable)
        return _FileCacheLookup(response, None, self, identity, writable)

    async def put(self, request: TransportRequest, response: TransportResponse) -> None:
        identity = _request_cache_identity(request)
        if not identity.eligible or request.cache is CachePolicy.BYPASS:
            return
        await self._put(identity, response)

    async def _put(
        self,
        identity: _RequestCacheIdentity,
        response: TransportResponse,
    ) -> None:
        enforce_response_body_limit(response, self.maximum_response_bytes)
        stored_at = self._clock()
        if not math.isfinite(stored_at) or stored_at < 0:
            raise ValueError("cache clock must return a finite non-negative timestamp")
        await asyncio.to_thread(self._write, identity.key, response, stored_at)

    @staticmethod
    def key(request: TransportRequest) -> str:
        return _request_cache_identity(request).key

    def _path(self, key: str) -> Path:
        return self.directory / key[:2] / f"{key}{_CACHE_SUFFIX}"

    def _age(self, stored_at: float) -> float | None:
        now = self._clock()
        if not math.isfinite(now) or now < 0:
            return None
        age = now - stored_at
        if age < -_MAXIMUM_CLOCK_SKEW_SECONDS:
            return None
        return max(0.0, age)

    def _read(self, key: str) -> tuple[TransportResponse, float] | None:
        path = self._path(key)
        try:
            if path.stat().st_size > self.maximum_artifact_bytes:
                return None
            with gzip.open(path, "rb") as handle:
                artifact = handle.read(self.maximum_artifact_bytes + 1)
            if len(artifact) > self.maximum_artifact_bytes:
                return None
            payload = json.loads(artifact)
            if not isinstance(payload, dict) or payload.get("schema") != _CACHE_SCHEMA:
                return None
            stored_at = payload["stored_at"]
            status = payload["status"]
            headers = payload["headers"]
            final_url = payload["final_url"]
            encoded = payload["content_base64"]
            if (
                not isinstance(stored_at, (int, float))
                or isinstance(stored_at, bool)
                or not math.isfinite(stored_at)
                or stored_at < 0
                or not isinstance(status, int)
                or isinstance(status, bool)
                or not isinstance(headers, dict)
                or not isinstance(final_url, str)
                or len(final_url) > _MAXIMUM_FINAL_URL_LENGTH
                or not isinstance(encoded, str)
            ):
                return None
            safe_headers = self._safe_headers(headers)
            if safe_headers is None:
                return None
            content = base64.b64decode(encoded, validate=True)
            if len(content) > self.maximum_response_bytes:
                return None
            final_url = sanitize_url(final_url)[:_MAXIMUM_FINAL_URL_LENGTH]
            response = TransportResponse(
                status=status,
                headers=safe_headers,
                content=content,
                final_url=final_url,
                route=RouteMetadata(kind="cache"),
                from_cache=True,
            )
        except (
            OSError,
            EOFError,
            UnicodeError,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
            binascii.Error,
        ):
            return None
        return response, float(stored_at)

    @staticmethod
    def _safe_headers(value: dict[Any, Any]) -> dict[str, str] | None:
        headers: dict[str, str] = {}
        for name, item in value.items():
            if not isinstance(name, str) or not isinstance(item, str):
                return None
            normalized = name.casefold()
            if normalized in _SAFE_RESPONSE_HEADERS:
                headers[normalized] = item
        return headers

    def _write(
        self,
        key: str,
        response: TransportResponse,
        stored_at: float,
    ) -> None:
        final_url = sanitize_url(response.final_url)
        if len(final_url) > _MAXIMUM_FINAL_URL_LENGTH:
            return
        payload = {
            "schema": _CACHE_SCHEMA,
            "stored_at": stored_at,
            "status": response.status,
            "headers": self._safe_headers(dict(response.headers)) or {},
            "final_url": final_url,
            "content_base64": base64.b64encode(response.content).decode("ascii"),
        }
        artifact = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        if len(artifact) > self.maximum_artifact_bytes:
            return
        path = self._path(key)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = -1
        temporary: str | None = None
        try:
            descriptor, temporary = tempfile.mkstemp(
                prefix=f".{path.name}.",
                suffix=".tmp",
                dir=path.parent,
            )
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as raw:
                descriptor = -1
                with gzip.GzipFile(fileobj=raw, mode="wb") as compressed:
                    compressed.write(artifact)
                raw.flush()
                os.fsync(raw.fileno())
            os.replace(temporary, path)
            temporary = None
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if temporary is not None:
                Path(temporary).unlink(missing_ok=True)
