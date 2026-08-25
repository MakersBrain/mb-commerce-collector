from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from .base import (
    DEFAULT_MAXIMUM_RESPONSE_BYTES,
    BrowserHint,
    CommerceTransport,
    ResponseBodyTooLarge,
    RotationReason,
    Timer,
    TransportAccounting,
    TransportFailure,
    TransportRequest,
    TransportResponse,
    estimated_transmitted_bytes,
)
from .browser import BrowserBackendUnavailable
from .url_policy import URLPolicy


class HTTPStatusError(RuntimeError):
    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"HTTP request failed with status {status}")
        self.status = status
        self.url = url


@dataclass(slots=True)
class _ClientEntry:
    client: Any
    active_requests: int = 0
    ready: bool = True


class HttpxTransport(CommerceTransport):
    """Optional HTTP backend; importing the package root does not import httpx."""

    def __init__(
        self,
        *,
        allowed_origins: tuple[str, ...],
        timeout: float = 30.0,
        client: Any | None = None,
        proxy: str | None = None,
        url_policy: URLPolicy | None = None,
        maximum_redirects: int = 10,
        maximum_response_bytes: int = DEFAULT_MAXIMUM_RESPONSE_BYTES,
        maximum_clients: int = 32,
    ) -> None:
        try:
            import httpx
        except ImportError as error:  # pragma: no cover - clean environment contract
            raise RuntimeError("HttpxTransport requires mb-commerce-scraper[http]") from error
        self._httpx = httpx
        self._client = client
        if (
            not isinstance(maximum_clients, int)
            or isinstance(maximum_clients, bool)
            or maximum_clients < 1
        ):
            raise ValueError("maximum_clients must be a positive integer")
        self._maximum_clients = maximum_clients
        self._clients: OrderedDict[tuple[str, str], _ClientEntry] = OrderedDict()
        self._client_condition = asyncio.Condition()
        self._close_lock = asyncio.Lock()
        self._external_active_requests = 0
        self._closed = False
        self._client_options: dict[str, Any] = {
            "timeout": timeout,
            "follow_redirects": False,
            "proxy": proxy,
        }
        self._policy = url_policy or URLPolicy(allowed_origins)
        if maximum_redirects < 0:
            raise ValueError("maximum_redirects must be non-negative")
        self._maximum_redirects = maximum_redirects
        if maximum_response_bytes < 1:
            raise ValueError("maximum_response_bytes must be positive")
        self._maximum_response_bytes = maximum_response_bytes

    async def request(self, request: TransportRequest) -> TransportResponse:
        if request.browser == BrowserHint.REQUIRED:
            raise BrowserBackendUnavailable(
                "request requires a browser transport, but only HTTP is configured"
            )
        current, addresses = await self._policy.validate_with_addresses(request.url)
        headers = dict(request.headers)
        timer = Timer()
        method = request.method
        json_body = request.json_body
        body = request.body
        physical_requests = 0
        transmitted_bytes = 0
        for redirect in range(self._maximum_redirects + 1):
            parsed = urlsplit(current)
            address = addresses[0]
            endpoint = self._endpoint_url(current, address)
            headers["Host"] = self._host_header(parsed)
            query = request.query if redirect == 0 else None
            logical_response_url = (
                str(self._httpx.URL(current, params=query)) if query else current
            )
            physical_request = request.model_copy(
                update={
                    "method": method,
                    "url": current,
                    "query": query or {},
                    "headers": headers,
                    "json_body": json_body,
                    "body": body,
                }
            )
            physical_requests += 1
            transmitted_bytes += estimated_transmitted_bytes(physical_request)
            http_failure_type: str | None = None
            try:
                async with self._client_for(current, address) as client, client.stream(
                    method,
                    endpoint,
                    params=query,
                    headers=headers,
                    json=json_body,
                    content=body,
                    extensions={"sni_hostname": self._sni_hostname(parsed)},
                ) as response:
                    if response.is_redirect:
                        if redirect == self._maximum_redirects:
                            raise RuntimeError("maximum redirect count exceeded")
                        location = response.headers.get("location")
                        if not location:
                            raise RuntimeError("redirect response omitted Location")
                        target, target_addresses = await self._policy.validate_with_addresses(
                            location, previous_url=current
                        )
                        if URLPolicy._origin(target) != URLPolicy._origin(current):
                            if json_body is not None or body is not None:
                                raise RuntimeError(
                                    "cross-origin redirect with a request body is refused"
                                )
                            self._strip_sensitive_headers(headers)
                        if response.status_code == 303 or (
                            response.status_code in {301, 302} and method.upper() == "POST"
                        ):
                            method = "GET"
                            json_body = None
                            body = None
                        current = target
                        addresses = target_addresses
                        continue
                    response_body = await self._read_bounded(response)
                    accounting = TransportAccounting(
                        physical_requests=physical_requests,
                        transmitted_bytes=transmitted_bytes,
                        received_bytes=len(response_body),
                    )
                    return TransportResponse(
                        status=response.status_code,
                        headers=dict(response.headers),
                        content=response_body,
                        final_url=logical_response_url,
                        elapsed_seconds=timer.elapsed,
                        accounting=accounting,
                    )
            except ResponseBodyTooLarge as error:
                raise ResponseBodyTooLarge(
                    maximum_bytes=error.maximum_bytes,
                    received_bytes=error.received_bytes,
                    accounting=TransportAccounting(
                        physical_requests=physical_requests,
                        transmitted_bytes=transmitted_bytes,
                        received_bytes=error.received_bytes,
                    ),
                ) from None
            except self._httpx.HTTPError as error:
                # HTTPX errors may retain Request headers and bodies. Keep only
                # the type while inside the handler and raise after leaving it.
                http_failure_type = type(error).__name__
            if http_failure_type is not None:
                raise TransportFailure(
                    f"HTTP transport failed: {http_failure_type}",
                    accounting=TransportAccounting(
                        physical_requests=physical_requests,
                        transmitted_bytes=transmitted_bytes,
                    ),
                ) from None
        raise AssertionError("unreachable")

    async def _read_bounded(self, response: Any) -> bytes:
        chunks: list[bytes] = []
        received_bytes = 0
        async for chunk in response.aiter_bytes(64 * 1024):
            received_bytes += len(chunk)
            if received_bytes > self._maximum_response_bytes:
                raise ResponseBodyTooLarge(
                    maximum_bytes=self._maximum_response_bytes,
                    received_bytes=received_bytes,
                )
            chunks.append(chunk)
        return b"".join(chunks)

    async def rotate_identity(self, reason: RotationReason) -> None:
        del reason

    async def aclose(self) -> None:
        async with self._close_lock:
            async with self._client_condition:
                self._closed = True
                self._client_condition.notify_all()
                await self._client_condition.wait_for(
                    lambda: self._external_active_requests == 0
                    and all(
                        entry.active_requests == 0
                        for entry in self._clients.values()
                    )
                )
                clients = tuple(entry.client for entry in self._clients.values())
                self._clients.clear()
            for client in clients:
                await client.aclose()

    async def __aenter__(self) -> HttpxTransport:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    @asynccontextmanager
    async def _client_for(
        self,
        logical_url: str,
        address: str,
    ) -> AsyncIterator[Any]:
        if self._client is not None:
            async with self._client_condition:
                if self._closed:
                    raise RuntimeError("HTTP transport is closed")
                self._external_active_requests += 1
            try:
                yield self._client
            finally:
                async with self._client_condition:
                    self._external_active_requests -= 1
                    self._client_condition.notify_all()
            return

        key = (URLPolicy._origin(logical_url), address)
        evicted: Any | None = None
        async with self._client_condition:
            while True:
                if self._closed:
                    raise RuntimeError("HTTP transport is closed")
                entry = self._clients.get(key)
                if entry is not None:
                    if not entry.ready:
                        await self._client_condition.wait()
                        continue
                    self._clients.move_to_end(key)
                    entry.active_requests += 1
                    break
                idle_key = next(
                    (
                        candidate
                        for candidate, candidate_entry in self._clients.items()
                        if candidate_entry.active_requests == 0
                    ),
                    None,
                )
                if len(self._clients) < self._maximum_clients or idle_key is not None:
                    if len(self._clients) >= self._maximum_clients:
                        assert idle_key is not None
                        evicted = self._clients.pop(idle_key).client
                    entry = _ClientEntry(
                        self._httpx.AsyncClient(**self._client_options),
                        active_requests=1,
                        ready=evicted is None,
                    )
                    self._clients[key] = entry
                    break
                await self._client_condition.wait()
        try:
            if evicted is not None:
                try:
                    await evicted.aclose()
                except BaseException:
                    async with self._client_condition:
                        if self._clients.get(key) is entry:
                            self._clients.pop(key)
                        self._client_condition.notify_all()
                    await entry.client.aclose()
                    raise
                async with self._client_condition:
                    if self._clients.get(key) is entry:
                        entry.ready = True
                    self._client_condition.notify_all()
            yield entry.client
        finally:
            async with self._client_condition:
                retained = self._clients.get(key)
                if retained is entry:
                    retained.active_requests -= 1
                self._client_condition.notify_all()

    @staticmethod
    def _endpoint_url(logical_url: str, address: str) -> str:
        parsed = urlsplit(logical_url)
        host = f"[{address}]" if ":" in address else address
        port = f":{parsed.port}" if parsed.port else ""
        return urlunsplit(
            (parsed.scheme, f"{host}{port}", parsed.path, parsed.query, "")
        )

    @staticmethod
    def _host_header(parsed: Any) -> str:
        host = parsed.hostname or ""
        host = f"[{host}]" if ":" in host else host
        default = (parsed.scheme == "https" and parsed.port in {None, 443}) or (
            parsed.scheme == "http" and parsed.port in {None, 80}
        )
        return host if default else f"{host}:{parsed.port}"

    @staticmethod
    def _sni_hostname(parsed: Any) -> str:
        return (parsed.hostname or "").encode("idna").decode("ascii")

    @staticmethod
    def _strip_sensitive_headers(headers: dict[str, str]) -> None:
        sensitive = {"authorization", "proxy-authorization", "cookie"}
        for name in tuple(headers):
            if name.casefold() in sensitive:
                headers.pop(name)
