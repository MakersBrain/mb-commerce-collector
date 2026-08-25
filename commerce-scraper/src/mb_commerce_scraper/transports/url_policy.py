from __future__ import annotations

import asyncio
import ipaddress
import math
import socket
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit

Resolver = Callable[[str], Awaitable[tuple[str, ...]]]


async def system_resolver(host: str) -> tuple[str, ...]:
    loop = asyncio.get_running_loop()
    loopback_safe = await loop.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    return tuple(dict.fromkeys(str(item[4][0]) for item in loopback_safe))


@dataclass(frozen=True, slots=True)
class _ResolutionEntry:
    addresses: tuple[str, ...]
    expires_at: float


def _consume_resolution_result(task: asyncio.Task[tuple[str, ...]]) -> None:
    if not task.cancelled():
        task.exception()


class URLPolicy:
    """SSRF policy applied before connection and to every redirect."""

    def __init__(
        self,
        origins: tuple[str, ...],
        *,
        resolver: Resolver = system_resolver,
        resolver_ttl_seconds: float = 60.0,
        maximum_resolved_hosts: int = 256,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not math.isfinite(resolver_ttl_seconds) or resolver_ttl_seconds < 0:
            raise ValueError("resolver_ttl_seconds must be finite and non-negative")
        if (
            not isinstance(maximum_resolved_hosts, int)
            or isinstance(maximum_resolved_hosts, bool)
            or maximum_resolved_hosts < 1
        ):
            raise ValueError("maximum_resolved_hosts must be a positive integer")
        self._origins = frozenset(self._origin(value) for value in origins)
        self._resolver = resolver
        self._resolver_ttl_seconds = resolver_ttl_seconds
        self._maximum_resolved_hosts = maximum_resolved_hosts
        self._clock = clock
        self._resolutions: OrderedDict[str, _ResolutionEntry] = OrderedDict()
        self._resolution_tasks: dict[str, asyncio.Task[tuple[str, ...]]] = {}
        self._resolution_lock = asyncio.Lock()

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
        addresses = await self._resolve(parsed.hostname)
        return absolute, addresses

    async def _resolve(self, host: str) -> tuple[str, ...]:
        key = host.casefold()
        async with self._resolution_lock:
            now = self._now()
            entry = self._resolutions.get(key)
            if entry is not None:
                if now < entry.expires_at:
                    self._resolutions.move_to_end(key)
                    return entry.addresses
                self._resolutions.pop(key)
            task = self._resolution_tasks.get(key)
            if task is None:
                task = asyncio.create_task(self._resolve_and_cache(key, host))
                task.add_done_callback(_consume_resolution_result)
                self._resolution_tasks[key] = task
        return await asyncio.shield(task)

    async def _resolve_and_cache(
        self,
        key: str,
        host: str,
    ) -> tuple[str, ...]:
        task = asyncio.current_task()
        try:
            addresses = await self._resolver(host)
            self._validate_addresses(addresses)
            async with self._resolution_lock:
                self._resolutions[key] = _ResolutionEntry(
                    addresses,
                    self._now() + self._resolver_ttl_seconds,
                )
                self._resolutions.move_to_end(key)
                while len(self._resolutions) > self._maximum_resolved_hosts:
                    self._resolutions.popitem(last=False)
            return addresses
        finally:
            async with self._resolution_lock:
                if self._resolution_tasks.get(key) is task:
                    self._resolution_tasks.pop(key)

    @staticmethod
    def _validate_addresses(addresses: tuple[str, ...]) -> None:
        if not addresses:
            raise ValueError("URL host did not resolve")
        for address in addresses:
            ip = ipaddress.ip_address(address)
            if not ip.is_global:
                raise ValueError(f"URL host resolves to a non-public address: {ip}")

    def _now(self) -> float:
        value = self._clock()
        if not math.isfinite(value):
            raise ValueError("resolver cache clock must return a finite value")
        return value

    @staticmethod
    def _origin(url: str) -> str:
        parsed = urlsplit(url)
        port = f":{parsed.port}" if parsed.port else ""
        return f"{parsed.scheme.lower()}://{(parsed.hostname or '').lower()}{port}"
