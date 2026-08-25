"""Optional HTTPX projection for neutral proxy leases."""

from __future__ import annotations

from urllib.parse import quote

from mb_commerce_scraper.transports import DEFAULT_MAXIMUM_RESPONSE_BYTES
from mb_commerce_scraper.transports.httpx import HttpxTransport

from .base import ProxyLease


class HttpxProxyTransportFactory:
    def __init__(
        self,
        *,
        allowed_origins: tuple[str, ...],
        timeout: float = 30.0,
        maximum_response_bytes: int = DEFAULT_MAXIMUM_RESPONSE_BYTES,
    ) -> None:
        self.allowed_origins = allowed_origins
        self.timeout = timeout
        if maximum_response_bytes < 1:
            raise ValueError("maximum_response_bytes must be positive")
        self.maximum_response_bytes = maximum_response_bytes

    def build(self, lease: ProxyLease) -> HttpxTransport:
        credentials = lease.http_credentials()
        username = quote(credentials.username.get_secret_value(), safe="")
        password = quote(credentials.password.get_secret_value(), safe="")
        route = lease.route
        proxy_url = f"{route.protocol}://{username}:{password}@{route.host}:{route.port}"
        return HttpxTransport(
            allowed_origins=self.allowed_origins,
            timeout=self.timeout,
            proxy=proxy_url,
            maximum_response_bytes=self.maximum_response_bytes,
        )
