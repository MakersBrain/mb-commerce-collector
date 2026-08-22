"""Local native Webshare resolver-to-wire integration without provider traffic."""

from __future__ import annotations

import asyncio
import base64
import json
import re
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from mb_commerce_scraper import ProxyMode, ProxyPolicyConfig, SourceDefinition
from mb_commerce_scraper.proxy import (
    HttpxProxyTransportFactory,
    ProxyRouting,
    RoutedTransport,
    RoutingMode,
)
from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    RequestPriority,
    RequestPurpose,
    TransportRequest,
)

from mb_ceramics_catalogue.ops import commerce_scraper_proxy as durable
from mb_ceramics_catalogue.ops.commerce_scraper_proxy_runtime import (
    resolve_native_proxy_runtime,
)
from mb_ceramics_catalogue.ops.commerce_scraper_webshare import WebshareGatewayPool
from mb_ceramics_catalogue.proxy import ProxyDenied, ProxyReservationUsage


class Database:
    def __init__(self) -> None:
        self.connections = 0

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        self.connections += 1
        yield object()


@dataclass
class Settings:
    proxy_enabled: bool = True
    proxy_secret_file: Path | None = None
    proxy_webshare_data_plane_enabled: bool = False
    proxy_webshare_gateway_secret_file: Path | None = None


class LoopbackHTTPGateway:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.requests: list[bytes] = []
        self.peers: list[str] = []
        self.closed_connections = 0

    async def handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            peer = writer.get_extra_info("peername")
            self.peers.append(str(peer[0]))
            self.requests.append(await reader.readuntil(b"\r\n\r\n"))
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                + f"Content-Length: {len(self.body)}\r\n".encode()
                + b"Content-Type: application/json\r\n"
                + b"Connection: close\r\n\r\n"
                + self.body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            self.closed_connections += 1


@asynccontextmanager
async def _local_verified_port(
    gateway: LoopbackHTTPGateway,
) -> AsyncIterator[int]:
    server: asyncio.Server | None = None
    for port in range(19_999, 18_999, -1):
        try:
            server = await asyncio.start_server(gateway.handle, "127.0.0.1", port)
            break
        except OSError:
            continue
    if server is None:
        raise RuntimeError("no verified Webshare test port is available on loopback")
    try:
        yield port
    finally:
        server.close()
        await server.wait_closed()


def _write_secret(path: Path, *, port: int) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "profiles": {
                    "webshare/primary": {
                        "provider": "webshare",
                        "logical_name": "primary",
                        "generation": 7,
                        "gateway": {
                            "endpoint_id": "webshare-residential-backbone",
                            "protocol": "http",
                            "host": "p.webshare.io",
                            "port": port,
                        },
                        "credentials": {
                            "username": "issued-user",
                            "password": "issued-p@ss",
                        },
                        "capabilities": {
                            "countries": ["FR"],
                            "sticky_session_ttl_seconds": 1_800,
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)


def _snapshot(profile_id: UUID, route_id: UUID) -> dict[str, Any]:
    return {
        "policy": "always",
        "provider": "webshare",
        "profile_id": str(profile_id),
        "route_id": str(route_id),
        "profile": "primary",
        "secret_generation": 7,
        "protocol": "http",
        "country": "FR",
        "state": None,
        "city": None,
        "session_mode": "sticky",
        "session_minutes": 30,
        "max_bytes": 5_000,
        "pilot": False,
    }


async def test_mounted_secret_resolver_routes_one_durably_accounted_loopback_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = b'{"products":[1]}'
    gateway = LoopbackHTTPGateway(body)
    reservation_id = uuid4()
    authorization_id = uuid4()
    reservations: list[dict[str, Any]] = []
    authorizations: list[tuple[UUID, int, int | None]] = []
    reconciliations: list[tuple[UUID, int, int]] = []
    closed: list[tuple[UUID, int, int]] = []

    async def reserve(_connection: Any, **values: Any) -> UUID:
        reservations.append(values)
        return reservation_id

    async def authorize(
        _connection: Any,
        *,
        reservation_id: UUID,
        estimated_bytes: int,
        maximum_requests: int | None,
    ) -> UUID:
        authorizations.append(
            (reservation_id, estimated_bytes, maximum_requests)
        )
        return authorization_id

    async def reconcile(
        _connection: Any,
        *,
        authorization_id: UUID,
        actual_bytes: int,
        physical_requests: int,
    ) -> ProxyReservationUsage:
        reconciliations.append(
            (authorization_id, actual_bytes, physical_requests)
        )
        return ProxyReservationUsage(
            estimated_bytes=actual_bytes,
            request_count=physical_requests,
            revoked=False,
            exhausted=False,
        )

    async def close(_connection: Any, lease: Any) -> None:
        closed.append((lease.reservation_id, lease.used_bytes, lease.requests))

    monkeypatch.setattr(durable, "reserve", reserve)
    monkeypatch.setattr(durable, "authorize_reservation_attempt", authorize)
    monkeypatch.setattr(durable, "reconcile_reservation_attempt", reconcile)
    monkeypatch.setattr(durable, "close_reservation", close)

    async with _local_verified_port(gateway) as port:
        secret_file = tmp_path / "webshare-gateway.json"
        _write_secret(secret_file, port=port)
        profile_id = uuid4()
        route_id = uuid4()
        snapshot = _snapshot(profile_id, route_id)
        database = Database()
        source = SourceDefinition(
            id="shop",
            label="Shop",
            base_url="http://shop.test/",
            connector="shopify",
        )
        source_policy = ProxyPolicyConfig(
            mode=ProxyMode.ALWAYS,
            country="FR",
            provider_preferences=("webshare",),
            maximum_requests=1,
            maximum_bytes=4_000,
        )

        with pytest.raises(ProxyDenied, match="not enabled"):
            resolve_native_proxy_runtime(
                database,
                job_id=uuid4(),
                proxy_snapshot=snapshot,
                settings=Settings(
                    proxy_webshare_gateway_secret_file=secret_file,
                ),
                run_proxy_policy=None,
                run_proxy_max_megabytes=None,
                source=source,
                source_policy=source_policy,
            )
        assert database.connections == 0

        job_id = uuid4()
        spec = resolve_native_proxy_runtime(
            database,
            job_id=job_id,
            proxy_snapshot=snapshot,
            settings=Settings(
                proxy_webshare_data_plane_enabled=True,
                proxy_webshare_gateway_secret_file=secret_file,
            ),
            run_proxy_policy=None,
            run_proxy_max_megabytes=None,
            source=source,
            source_policy=source_policy,
        )
        assert spec is not None
        assert isinstance(spec.pool, durable.PostgresReservedProxyPool)
        inner_pool = spec.pool._inner
        assert isinstance(inner_pool, WebshareGatewayPool)
        assert database.connections == 0

        resolved_hosts: list[str] = []
        real_getaddrinfo = socket.getaddrinfo

        def loopback_only(
            host: str | bytes,
            service: str | int | None,
            *args: Any,
            **kwargs: Any,
        ) -> list[tuple[Any, ...]]:
            name = host.decode("ascii") if isinstance(host, bytes) else host
            resolved_hosts.append(name)
            if name == "p.webshare.io":
                resolved = "127.0.0.1"
            elif name == "shop.test":
                # URL policy still resolves the target before proxy dispatch.
                # A public address passes the SSRF gate; the terminating proxy
                # records the absolute target and never connects to it.
                resolved = "93.184.216.34"
            else:
                raise socket.gaierror(f"unexpected resolution: {name}")
            return real_getaddrinfo(resolved, service, *args, **kwargs)

        monkeypatch.setattr(socket, "getaddrinfo", loopback_only)
        direct = FakeTransport()
        transport = RoutedTransport(
            direct,
            pool=spec.pool,
            proxy_factory=HttpxProxyTransportFactory(
                allowed_origins=("http://shop.test",),
                timeout=2.0,
            ),
            routing=ProxyRouting(
                mode=RoutingMode.ALWAYS,
                country=spec.policy.country,
                provider_preferences=spec.policy.provider_preferences,
            ),
            source_id=spec.source_id,
            base_url=spec.base_url,
            maximum_requests=spec.policy.maximum_requests,
            maximum_bytes=spec.policy.maximum_bytes,
        )
        request = TransportRequest(
            url="http://shop.test/catalog",
            query={"cursor": "next"},
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
            estimated_bytes=300,
        )
        try:
            response = await transport.request(request)
        finally:
            await transport.aclose()

    assert response.status == 200
    assert response.content == body
    assert response.route.provider == "webshare"
    assert direct.requests == []
    assert set(resolved_hosts) == {"shop.test", "p.webshare.io"}
    assert gateway.peers == ["127.0.0.1"]
    assert gateway.closed_connections == 1
    assert len(gateway.requests) == 1
    request_head = gateway.requests[0]
    assert request_head.startswith(
        b"GET http://93.184.216.34/catalog?cursor=next HTTP/1.1\r\n"
    )
    assert b"Host: shop.test\r\n" in request_head
    authorization_header = next(
        line for line in request_head.split(b"\r\n")
        if line.lower().startswith(b"proxy-authorization: basic ")
    )
    username, password = base64.b64decode(
        authorization_header.split(maxsplit=2)[2]
    ).decode("utf-8").split(":", 1)
    assert re.fullmatch(r"issued-user-fr-[0-9]+", username)
    assert password == "issued-p@ss"

    assert reservations == [
        {
            "job_id": job_id,
            "profile": "primary",
            "profile_id": profile_id,
            "route_id": route_id,
            "requested_bytes": 4_000,
            "pilot": False,
            "secret_generation": 7,
            "provider": "webshare",
        }
    ]
    assert len(authorizations) == 1
    assert authorizations[0][0] == reservation_id
    assert authorizations[0][1] > 300
    assert authorizations[0][2] == 1
    assert len(reconciliations) == 1
    assert reconciliations[0][0] == authorization_id
    assert reconciliations[0][2] == 1
    assert response.accounting is not None
    assert reconciliations[0][1] == (
        response.accounting.transmitted_bytes + response.accounting.received_bytes
    )
    assert closed == [
        (
            reservation_id,
            reconciliations[0][1],
            1,
        )
    ]
    assert database.connections == 4
    assert inner_pool.active_leases == 0
