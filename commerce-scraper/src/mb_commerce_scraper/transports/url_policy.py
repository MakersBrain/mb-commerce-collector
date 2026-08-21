from __future__ import annotations

import ipaddress
import socket
from collections.abc import Awaitable, Callable
from urllib.parse import urljoin, urlsplit

Resolver = Callable[[str], Awaitable[tuple[str, ...]]]


async def system_resolver(host: str) -> tuple[str, ...]:
    loopback_safe = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    return tuple(dict.fromkeys(str(item[4][0]) for item in loopback_safe))


class URLPolicy:
    """SSRF policy applied before connection and to every redirect."""

    def __init__(self, origins: tuple[str, ...], *, resolver: Resolver = system_resolver) -> None:
        self._origins = frozenset(self._origin(value) for value in origins)
        self._resolver = resolver

    async def validate(self, url: str, *, previous_url: str | None = None) -> str:
        absolute = urljoin(previous_url, url) if previous_url else url
        parsed = urlsplit(absolute)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("URL policy permits only absolute HTTP(S) URLs")
        if parsed.username or parsed.password:
            raise ValueError("URL policy rejects embedded user information")
        if self._origin(absolute) not in self._origins:
            raise ValueError(f"URL origin is not allowed: {self._origin(absolute)}")
        addresses = await self._resolver(parsed.hostname)
        if not addresses:
            raise ValueError("URL host did not resolve")
        for address in addresses:
            ip = ipaddress.ip_address(address)
            if not ip.is_global:
                raise ValueError(f"URL host resolves to a non-public address: {ip}")
        return absolute

    @staticmethod
    def _origin(url: str) -> str:
        parsed = urlsplit(url)
        port = f":{parsed.port}" if parsed.port else ""
        return f"{parsed.scheme.lower()}://{(parsed.hostname or '').lower()}{port}"
