from __future__ import annotations

from mb_commerce_scraper.connectors import ConnectorRegistry
from mb_commerce_scraper.proxy import (
    HttpxProxyTransportFactory,
    ProxyPool,
    ProxyRouting,
)
from mb_commerce_scraper.transports.httpx import HttpxTransport

from .client import CommerceScraper


def build_http_scraper(
    *,
    allowed_origins: tuple[str, ...],
    registry: ConnectorRegistry | None = None,
    timeout: float = 30.0,
    proxy_pool: ProxyPool | None = None,
    routing: ProxyRouting | None = None,
    proxy_maximum_bytes: int | None = None,
) -> CommerceScraper:
    transport = HttpxTransport(allowed_origins=allowed_origins, timeout=timeout)
    return CommerceScraper(
        registry=registry or ConnectorRegistry.with_builtins(),
        transport=transport,
        proxy_pool=proxy_pool,
        routing=routing,
        proxy_transport_factory=(
            HttpxProxyTransportFactory(
                allowed_origins=allowed_origins,
                timeout=timeout,
            )
            if proxy_pool is not None
            else None
        ),
        proxy_maximum_bytes=proxy_maximum_bytes,
        owns_transport=True,
    )
