from __future__ import annotations

from mb_commerce_scraper.connectors import ConnectorRegistry
from mb_commerce_scraper.transports.httpx import HttpxTransport

from .client import CommerceScraper


def build_http_scraper(*, allowed_origins: tuple[str, ...], registry: ConnectorRegistry | None = None, timeout: float = 30.0) -> CommerceScraper:
    transport = HttpxTransport(allowed_origins=allowed_origins, timeout=timeout)
    return CommerceScraper(registry=registry or ConnectorRegistry.with_builtins(), transport=transport, owns_transport=True)

