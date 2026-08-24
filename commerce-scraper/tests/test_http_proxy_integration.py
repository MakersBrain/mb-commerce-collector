from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from pydantic import SecretStr

from mb_commerce_scraper import ProxyMode, ProxyPolicyConfig
from mb_commerce_scraper.proxy import (
    HttpxProxyTransportFactory,
    ProxyCredentials,
    ProxyEndpoint,
    ProxyKind,
    ProxyLease,
    ProxyOutcome,
    RoutedTransport,
    StaticProxyPool,
    StaticRoute,
)
from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    MiddlewareTransport,
    RequestPriority,
    RequestPurpose,
    TransportRequest,
    estimated_transmitted_bytes,
)


class RecordingStaticProxyPool(StaticProxyPool):
    def __init__(self, routes: tuple[StaticRoute, ...]) -> None:
        super().__init__(routes)
        self.outcomes: list[ProxyOutcome] = []

    async def report(self, lease: ProxyLease, outcome: ProxyOutcome) -> None:
        self.outcomes.append(outcome)
        await super().report(lease, outcome)


class LocalHTTPProxy:
    def __init__(self, response_body: bytes, *, status: int = 200) -> None:
        self.response_body = response_body
        self.status = status
        self.requests: list[bytes] = []
        self.closed_connections = 0

    async def handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            self.requests.append(request)
            reason = b"OK" if self.status == 200 else b"Too Many Requests"
            writer.write(
                b"HTTP/1.1 "
                + str(self.status).encode()
                + b" "
                + reason
                + b"\r\n"
                + f"Content-Length: {len(self.response_body)}\r\n".encode()
                + b"Content-Type: application/json\r\n"
                + b"Connection: close\r\n\r\n"
                + self.response_body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            self.closed_connections += 1


class LocalSOCKS5Proxy:
    """Minimal authenticated SOCKS5 endpoint terminating HTTP locally."""

    def __init__(self, response_body: bytes) -> None:
        self.response_body = response_body
        self.credentials: list[tuple[bytes, bytes]] = []
        self.connect_targets: list[tuple[str, int]] = []
        self.requests: list[bytes] = []
        self.closed_connections = 0

    async def handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            version, method_count = await reader.readexactly(2)
            methods = await reader.readexactly(method_count)
            assert version == 5
            assert 2 in methods
            writer.write(b"\x05\x02")
            await writer.drain()

            auth_version, username_length = await reader.readexactly(2)
            username = await reader.readexactly(username_length)
            (password_length,) = await reader.readexactly(1)
            password = await reader.readexactly(password_length)
            assert auth_version == 1
            self.credentials.append((username, password))
            writer.write(b"\x01\x00")
            await writer.drain()

            request_version, command, reserved, address_type = await reader.readexactly(4)
            assert (request_version, command, reserved) == (5, 1, 0)
            target = await self._target(reader, address_type)
            port = int.from_bytes(await reader.readexactly(2))
            self.connect_targets.append((target, port))
            writer.write(b"\x05\x00\x00\x01\x7f\x00\x00\x01\x00\x00")
            await writer.drain()

            request = await reader.readuntil(b"\r\n\r\n")
            self.requests.append(request)
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                + f"Content-Length: {len(self.response_body)}\r\n".encode()
                + b"Content-Type: application/json\r\n"
                + b"Connection: close\r\n\r\n"
                + self.response_body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            self.closed_connections += 1

    @staticmethod
    async def _target(reader: asyncio.StreamReader, address_type: int) -> str:
        if address_type == 1:
            return ".".join(str(part) for part in await reader.readexactly(4))
        if address_type == 3:
            (length,) = await reader.readexactly(1)
            return (await reader.readexactly(length)).decode("idna")
        if address_type == 4:
            packed = await reader.readexactly(16)
            return ":".join(
                f"{int.from_bytes(packed[offset : offset + 2]):x}"
                for offset in range(0, len(packed), 2)
            )
        raise AssertionError(f"unsupported SOCKS5 address type: {address_type}")


@asynccontextmanager
async def local_http_proxy(proxy: LocalHTTPProxy) -> AsyncIterator[int]:
    server = await asyncio.start_server(proxy.handle, "127.0.0.1", 0)
    socket = server.sockets[0]
    try:
        yield int(socket.getsockname()[1])
    finally:
        server.close()
        await server.wait_closed()


@asynccontextmanager
async def local_socks5_proxy(proxy: LocalSOCKS5Proxy) -> AsyncIterator[int]:
    server = await asyncio.start_server(proxy.handle, "127.0.0.1", 0)
    socket = server.sockets[0]
    try:
        yield int(socket.getsockname()[1])
    finally:
        server.close()
        await server.wait_closed()


async def test_real_http_proxy_preserves_credentials_routing_and_accounting() -> None:
    body = b'{"products":[1,2]}'
    proxy = LocalHTTPProxy(body)
    username = "merchant/user"
    password = "p@ ss:word"

    async with local_http_proxy(proxy) as proxy_port:
        pool = RecordingStaticProxyPool(
            (
                StaticRoute(
                    endpoint=ProxyEndpoint(
                        provider="local-gate",
                        endpoint_id="local-gate-1",
                        protocol="http",
                        host="127.0.0.1",
                        port=proxy_port,
                        kind=ProxyKind.RESIDENTIAL,
                    ),
                    credentials=ProxyCredentials(
                        username=SecretStr(username),
                        password=SecretStr(password),
                    ),
                ),
            )
        )
        factory = HttpxProxyTransportFactory(
            allowed_origins=("http://93.184.216.34",),
            timeout=2.0,
        )
        transport = RoutedTransport(
            FakeTransport(),
            pool=pool,
            proxy_factory=factory,
            policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
            source_id="local-proxy-gate",
            base_url="http://93.184.216.34",
        )
        request = TransportRequest(
            url="http://93.184.216.34/catalog",
            query={"cursor": "next"},
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
        )

        try:
            response = await transport.request(request)
        finally:
            await transport.aclose()

    assert response.status == 200
    assert response.content == body
    assert response.route.kind == "proxy"
    assert response.route.provider == "local-gate"
    assert response.accounting is not None
    assert response.accounting.physical_requests == 1
    assert response.accounting.transmitted_bytes == estimated_transmitted_bytes(request)
    assert response.accounting.received_bytes == len(body)

    assert len(proxy.requests) == 1
    request_head = proxy.requests[0]
    assert request_head.startswith(
        b"GET http://93.184.216.34/catalog?cursor=next HTTP/1.1\r\n"
    )
    expected_credentials = base64.b64encode(f"{username}:{password}".encode())
    assert b"Proxy-Authorization: Basic " + expected_credentials + b"\r\n" in request_head
    assert b"Host: 93.184.216.34\r\n" in request_head

    assert len(pool.outcomes) == 1
    outcome = pool.outcomes[0]
    assert outcome.classification == "success"
    assert outcome.physical_requests == response.accounting.physical_requests
    assert outcome.transmitted_bytes == response.accounting.transmitted_bytes
    assert outcome.received_bytes == response.accounting.received_bytes
    assert pool.active_leases == 0
    assert proxy.closed_connections == 1


async def test_real_socks5_proxy_preserves_credentials_target_and_cleanup() -> None:
    body = b'{"products":["socks"]}'
    proxy = LocalSOCKS5Proxy(body)
    username = "merchant/user"
    password = "p@ss:word"

    async with local_socks5_proxy(proxy) as proxy_port:
        pool = RecordingStaticProxyPool(
            (
                StaticRoute(
                    endpoint=ProxyEndpoint(
                        provider="local-socks",
                        endpoint_id="local-socks-1",
                        protocol="socks5",
                        host="127.0.0.1",
                        port=proxy_port,
                        kind=ProxyKind.RESIDENTIAL,
                    ),
                    credentials=ProxyCredentials(
                        username=SecretStr(username),
                        password=SecretStr(password),
                    ),
                ),
            )
        )
        transport = RoutedTransport(
            FakeTransport(),
            pool=pool,
            proxy_factory=HttpxProxyTransportFactory(
                allowed_origins=("http://93.184.216.34",),
                timeout=2.0,
            ),
            policy=ProxyPolicyConfig(mode=ProxyMode.ALWAYS),
            source_id="local-socks-gate",
            base_url="http://93.184.216.34",
        )
        request = TransportRequest(
            url="http://93.184.216.34/catalog",
            query={"cursor": "socks-next"},
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
        )

        try:
            response = await transport.request(request)
        finally:
            await transport.aclose()

    assert response.status == 200
    assert response.content == body
    assert response.route.kind == "proxy"
    assert response.route.provider == "local-socks"
    assert response.accounting is not None
    assert response.accounting.physical_requests == 1
    assert response.accounting.transmitted_bytes == estimated_transmitted_bytes(request)
    assert response.accounting.received_bytes == len(body)
    assert proxy.credentials == [(username.encode(), password.encode())]
    assert proxy.connect_targets == [("93.184.216.34", 80)]
    assert len(proxy.requests) == 1
    request_head = proxy.requests[0]
    assert request_head.startswith(
        b"GET /catalog?cursor=socks-next HTTP/1.1\r\n"
    )
    assert b"Host: 93.184.216.34\r\n" in request_head
    assert b"Proxy-Authorization:" not in request_head
    assert [outcome.classification for outcome in pool.outcomes] == ["success"]
    assert pool.active_leases == 0
    assert proxy.closed_connections == 1


async def test_real_http_proxies_retry_across_providers_and_reconcile_attempts() -> None:
    limited = LocalHTTPProxy(b'{"error":"limited"}', status=429)
    successful = LocalHTTPProxy(b'{"products":["second-provider"]}')
    first_credentials = ProxyCredentials(
        username=SecretStr("first-user"),
        password=SecretStr("first-password"),
    )
    second_credentials = ProxyCredentials(
        username=SecretStr("second-user"),
        password=SecretStr("second-password"),
    )

    async with (
        local_http_proxy(limited) as limited_port,
        local_http_proxy(successful) as successful_port,
    ):
        pool = RecordingStaticProxyPool(
            (
                StaticRoute(
                    endpoint=ProxyEndpoint(
                        provider="provider-one",
                        endpoint_id="provider-one-1",
                        protocol="http",
                        host="127.0.0.1",
                        port=limited_port,
                        kind=ProxyKind.RESIDENTIAL,
                    ),
                    credentials=first_credentials,
                ),
                StaticRoute(
                    endpoint=ProxyEndpoint(
                        provider="provider-two",
                        endpoint_id="provider-two-1",
                        protocol="http",
                        host="127.0.0.1",
                        port=successful_port,
                        kind=ProxyKind.RESIDENTIAL,
                    ),
                    credentials=second_credentials,
                ),
            )
        )
        routed = RoutedTransport(
            FakeTransport(),
            pool=pool,
            proxy_factory=HttpxProxyTransportFactory(
                allowed_origins=("http://93.184.216.34",),
                timeout=2.0,
            ),
            policy=ProxyPolicyConfig(mode=ProxyMode.FAILOVER),
            source_id="local-multi-provider-gate",
            base_url="http://93.184.216.34",
        )
        transport = MiddlewareTransport(routed, retries=1, backoff=lambda _: 0)
        request = TransportRequest(
            url="http://93.184.216.34/catalog",
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
        )

        try:
            response = await transport.request(request)
        finally:
            await routed.aclose()

    assert response.status == 200
    assert response.content == successful.response_body
    assert response.route.provider == "provider-two"
    assert response.route.endpoint_id == "provider-two-1"
    assert len(limited.requests) == 1
    assert len(successful.requests) == 1
    assert (
        b"Proxy-Authorization: Basic "
        + base64.b64encode(b"first-user:first-password")
        + b"\r\n"
        in limited.requests[0]
    )
    assert (
        b"Proxy-Authorization: Basic "
        + base64.b64encode(b"second-user:second-password")
        + b"\r\n"
        in successful.requests[0]
    )
    assert [outcome.classification for outcome in pool.outcomes] == [
        "rate_limited",
        "success",
    ]
    assert [outcome.physical_requests for outcome in pool.outcomes] == [1, 1]
    assert [outcome.transmitted_bytes for outcome in pool.outcomes] == [
        estimated_transmitted_bytes(request),
        estimated_transmitted_bytes(request),
    ]
    assert [outcome.received_bytes for outcome in pool.outcomes] == [
        len(limited.response_body),
        len(successful.response_body),
    ]
    assert pool.active_leases == 0
    assert limited.closed_connections == 1
    assert successful.closed_connections == 1
