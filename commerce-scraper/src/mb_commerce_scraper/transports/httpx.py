from __future__ import annotations

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
    ) -> None:
        try:
            import httpx
        except ImportError as error:  # pragma: no cover - clean environment contract
            raise RuntimeError("HttpxTransport requires mb-commerce-scraper[http]") from error
        self._httpx = httpx
        self._client = client
        self._clients: dict[tuple[str, str], Any] = {}
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
            client = self._client or self._client_for(current, address)
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
                async with client.stream(
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
        clients, self._clients = tuple(self._clients.values()), {}
        for client in clients:
            await client.aclose()

    async def __aenter__(self) -> HttpxTransport:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    def _client_for(self, logical_url: str, address: str) -> Any:
        key = (URLPolicy._origin(logical_url), address)
        client = self._clients.get(key)
        if client is None:
            client = self._httpx.AsyncClient(**self._client_options)
            self._clients[key] = client
        return client

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
