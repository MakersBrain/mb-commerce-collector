from __future__ import annotations

from typing import Any

from .base import (
    BrowserHint,
    CommerceTransport,
    RotationReason,
    Timer,
    TransportFailure,
    TransportRequest,
    TransportResponse,
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
    ) -> None:
        try:
            import httpx
        except ImportError as error:  # pragma: no cover - clean environment contract
            raise RuntimeError("HttpxTransport requires mb-commerce-scraper[http]") from error
        self._httpx = httpx
        self._client = client or httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=False,
            proxy=proxy,
        )
        self._owns_client = client is None
        self._policy = url_policy or URLPolicy(allowed_origins)
        self._maximum_redirects = maximum_redirects

    async def request(self, request: TransportRequest) -> TransportResponse:
        if request.browser == BrowserHint.REQUIRED:
            raise BrowserBackendUnavailable(
                "request requires a browser transport, but only HTTP is configured"
            )
        current = await self._policy.validate(request.url)
        headers = dict(request.headers)
        timer = Timer()
        for redirect in range(self._maximum_redirects + 1):
            try:
                response = await self._client.request(
                    request.method,
                    current,
                    params=request.query,
                    headers=headers,
                    json=request.json_body,
                    content=request.body,
                )
            except self._httpx.HTTPError as error:
                raise TransportFailure(f"HTTP transport failed: {type(error).__name__}") from error
            if response.is_redirect:
                if redirect == self._maximum_redirects:
                    raise RuntimeError("maximum redirect count exceeded")
                target = await self._policy.validate(response.headers["location"], previous_url=current)
                if URLPolicy._origin(target) != URLPolicy._origin(current):
                    headers.pop("authorization", None)
                    headers.pop("Authorization", None)
                current = target
                continue
            return TransportResponse(
                status=response.status_code,
                headers=dict(response.headers),
                content=response.content,
                final_url=str(response.url),
                elapsed_seconds=timer.elapsed,
            )
        raise AssertionError("unreachable")

    async def rotate_identity(self, reason: RotationReason) -> None:
        del reason

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> HttpxTransport:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()
