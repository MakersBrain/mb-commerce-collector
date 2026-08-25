"""Live Camoufox callback ordering through an authenticated local HTTP proxy."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import cast
from urllib.parse import urlsplit

import pytest
from mb_commerce_scraper.proxy import (
    BrowserProxyCredentials,
    BrowserSubrequestOutcome,
    ProxyLease,
)
from mb_commerce_scraper.transports import (
    BrowserHint,
    RequestPriority,
    RequestPurpose,
    TransportRequest,
)
from pydantic import SecretStr

from mb_ceramics_catalogue.ops.commerce_scraper_browser import (
    CamoufoxProxyBrowserTransportFactory,
)


@dataclass(slots=True)
class _Ordering:
    events: list[str] = field(default_factory=list)
    authorizations: int = 0
    local_forwards: list[tuple[str, int]] = field(default_factory=list)
    outcomes: list[BrowserSubrequestOutcome] = field(default_factory=list)


class _Authorization:
    def __init__(self, ordering: _Ordering) -> None:
        self._ordering = ordering
        self._resolved = False

    async def reconcile(self, outcome: BrowserSubrequestOutcome) -> None:
        assert not self._resolved
        self._resolved = True
        self._ordering.outcomes.append(outcome)
        self._ordering.events.append("reconcile")

    async def release(self) -> None:
        assert not self._resolved
        self._resolved = True
        self._ordering.events.append("release")


class _Authorizer:
    def __init__(self, ordering: _Ordering) -> None:
        self._ordering = ordering

    async def authorize(self, estimated_bytes: int) -> _Authorization:
        assert estimated_bytes > 0
        self._ordering.authorizations += 1
        self._ordering.events.append("authorize")
        return _Authorization(self._ordering)


@dataclass(frozen=True, slots=True)
class _Lease:
    credentials: BrowserProxyCredentials

    def browser_credentials(self) -> BrowserProxyCredentials:
        return self.credentials


async def _read_headers(reader: asyncio.StreamReader) -> tuple[str, list[tuple[str, str]]]:
    first = (await reader.readline()).decode("latin-1").rstrip("\r\n")
    headers: list[tuple[str, str]] = []
    while True:
        line = await reader.readline()
        if line in {b"", b"\r\n"}:
            break
        name, value = line.decode("latin-1").rstrip("\r\n").split(":", 1)
        headers.append((name.strip(), value.strip()))
    return first, headers


async def _origin(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        request_line, _headers = await _read_headers(reader)
        path = request_line.split(" ", 2)[1]
        if path == "/":
            body = (
                b"<!doctype html><script src='/app.js'></script>"
                b"<img src='/blocked.png'>"
            )
            content_type = "text/html"
        elif path == "/app.js":
            body = (
                b"fetch('/data.json').then(r => r.json()).then(() => {"
                b"const done=document.createElement('div'); done.id='done';"
                b"document.body.appendChild(done);});"
            )
            content_type = "text/javascript"
        elif path == "/data.json":
            body = b'{"ok":true}'
            content_type = "application/json"
        else:
            body = b"not found"
            content_type = "text/plain"
        status = "200 OK" if path in {"/", "/app.js", "/data.json"} else "404 Not Found"
        writer.write(
            f"HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
            + body
        )
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


def _header(headers: list[tuple[str, str]], name: str) -> str | None:
    return next((value for key, value in headers if key.casefold() == name.casefold()), None)


async def _copy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while chunk := await reader.read(65_536):
            writer.write(chunk)
            await writer.drain()
    finally:
        with suppress(ConnectionError):
            writer.close()


@asynccontextmanager
async def _live_servers(
    ordering: _Ordering,
    *,
    username: str,
    password: str,
) -> AsyncIterator[tuple[str, str]]:
    origin = await asyncio.start_server(_origin, "127.0.0.1", 0)
    origin_port = origin.sockets[0].getsockname()[1]
    expected_auth = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()

    async def proxy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream_writer: asyncio.StreamWriter | None = None
        try:
            request_line, headers = await _read_headers(reader)
            method, target, version = request_line.split(" ", 2)
            if _header(headers, "proxy-authorization") != expected_auth:
                writer.write(
                    b"HTTP/1.1 407 Proxy Authentication Required\r\n"
                    b'Proxy-Authenticate: Basic realm="local-integration"\r\n'
                    b"Content-Length: 0\r\nConnection: close\r\n\r\n"
                )
                await writer.drain()
                return
            parsed = urlsplit(target)
            if method == "CONNECT":
                host, raw_port = target.rsplit(":", 1)
                port = int(raw_port)
                if host != "shop.test" or port != origin_port:
                    raise ValueError("proxy target escaped local integration origin")
                upstream_reader, upstream_writer = await asyncio.open_connection(
                    "127.0.0.1", port
                )
                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await writer.drain()
                await asyncio.gather(
                    _copy(reader, upstream_writer),
                    _copy(upstream_reader, writer),
                )
                return
            if (
                parsed.scheme != "http"
                or parsed.hostname != "shop.test"
                or parsed.port != origin_port
            ):
                raise ValueError("proxy target escaped local integration origin")
            ordering.events.append("forward")
            ordering.local_forwards.append((parsed.path or "/", ordering.authorizations))
            upstream_reader, upstream_writer = await asyncio.open_connection(
                "127.0.0.1", parsed.port
            )
            forwarded_headers = [
                (key, value)
                for key, value in headers
                if key.casefold() not in {"proxy-authorization", "proxy-connection"}
            ]
            upstream_writer.write(
                f"{method} {parsed.path or '/'}{('?' + parsed.query) if parsed.query else ''} "
                f"{version}\r\n".encode()
                + b"".join(f"{key}: {value}\r\n".encode() for key, value in forwarded_headers)
                + b"Connection: close\r\n\r\n"
            )
            await upstream_writer.drain()
            while chunk := await upstream_reader.read(65_536):
                writer.write(chunk)
                await writer.drain()
        except (ConnectionError, ValueError):
            if not writer.is_closing():
                writer.write(
                    b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n"
                    b"Connection: close\r\n\r\n"
                )
                await writer.drain()
        finally:
            if upstream_writer is not None:
                upstream_writer.close()
            writer.close()
            await writer.wait_closed()

    proxy_server = await asyncio.start_server(proxy, "127.0.0.1", 0)
    proxy_port = proxy_server.sockets[0].getsockname()[1]
    try:
        yield f"http://shop.test:{origin_port}/", f"http://127.0.0.1:{proxy_port}"
    finally:
        proxy_server.close()
        origin.close()
        await proxy_server.wait_closed()
        await origin.wait_closed()


async def run_live_camoufox_callback_gate() -> None:
    """Exercise real Playwright route/requestfinished ordering, not callback doubles."""
    ordering = _Ordering()
    username = "local-issued-user"
    password = "local-issued-password"
    async with _live_servers(ordering, username=username, password=password) as (
        origin_url,
        proxy_url,
    ):
        lease = _Lease(
            BrowserProxyCredentials(
                server=proxy_url,
                username=SecretStr(username),
                password=SecretStr(password),
            )
        )
        transport = CamoufoxProxyBrowserTransportFactory(
            allowed_origins=(origin_url,)
        ).build(cast(ProxyLease, lease), _Authorizer(ordering))
        try:
            response = await transport.request(
                TransportRequest(
                    url=origin_url,
                    purpose=RequestPurpose.ENRICHMENT,
                    priority=RequestPriority.DETAIL,
                    browser=BrowserHint.REQUIRED,
                )
            )
        finally:
            await transport.aclose()

    assert b'id="done"' in response.content
    forwarded_paths = [path for path, _seen in ordering.local_forwards]
    assert forwarded_paths == ["/", "/app.js", "/data.json"]
    assert "/blocked.png" not in forwarded_paths
    assert all(
        authorizations_seen >= ordinal
        for ordinal, (_path, authorizations_seen) in enumerate(
            ordering.local_forwards, start=1
        )
    )
    assert ordering.authorizations == 3
    assert len(ordering.outcomes) == 3
    # Firefox performs one 407 credential handshake beneath the navigation's
    # Playwright route callback. The authenticated origin forward still occurs
    # after that callback and the page succeeds, but request.response() retains
    # the protocol challenge for that token. Do not misreport it as an origin
    # success; the two application subrequests expose their final 200s.
    assert [outcome.status for outcome in ordering.outcomes] == [407, 200, 200]
    assert [outcome.classification for outcome in ordering.outcomes] == [
        "http_error",
        "success",
        "success",
    ]
    assert response.accounting is not None
    assert response.accounting.physical_requests == 3
    assert response.accounting.transmitted_bytes > 0
    assert response.accounting.received_bytes > 0
    assert username not in repr(ordering)
    assert password not in repr(ordering)


@pytest.mark.camoufox
async def test_live_camoufox_authorizes_before_each_local_proxy_forward() -> None:
    await run_live_camoufox_callback_gate()
