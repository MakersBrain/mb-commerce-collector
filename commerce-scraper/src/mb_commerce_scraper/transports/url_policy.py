from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable
from urllib.parse import urljoin, urlsplit

Resolver = Callable[[str], Awaitable[tuple[str, ...]]]


async def system_resolver(host: str) -> tuple[str, ...]:
    loop = asyncio.get_running_loop()
    loopback_safe = await loop.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    return tuple(dict.fromkeys(str(item[4][0]) for item in loopback_safe))


class URLPolicy:
    """SSRF policy applied before connection and to every redirect."""

    def __init__(self, origins: tuple[str, ...], *, resolver: Resolver = system_resolver) -> None:
        self._origins = frozenset(self._origin(value) for value in origins)
        self._resolver = resolver

    async def validate(self, url: str, *, previous_url: str | None = None) -> str:
        absolute, _ = await self.validate_with_addresses(
            url, previous_url=previous_url
        )
        return absolute

    async def validate_with_addresses(
        self, url: str, *, previous_url: str | None = None
    ) -> tuple[str, tuple[str, ...]]:
        """Return the logical URL and the exact public addresses it resolved to."""
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
        return absolute, addresses

    @staticmethod
    def _origin(url: str) -> str:
        parsed = urlsplit(url)
        port = f":{parsed.port}" if parsed.port else ""
        return f"{parsed.scheme.lower()}://{(parsed.hostname or '').lower()}{port}"
