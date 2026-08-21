from __future__ import annotations

import re
from collections.abc import AsyncIterator
from urllib.parse import urljoin

from mb_commerce_scraper.transports import (
    CommerceTransport,
    RequestPriority,
    RequestPurpose,
    TransportRequest,
)

LOC = re.compile(r"<loc\b[^>]*>\s*(.*?)\s*</loc>", re.IGNORECASE | re.DOTALL)


class SitemapDiscovery:
    def __init__(self, transport: CommerceTransport, roots: tuple[str, ...], *, product_pattern: str | None = None, limit: int = 100) -> None:
        self.transport = transport
        self.roots = roots
        self.product_pattern = re.compile(product_pattern) if product_pattern else None
        self.limit = limit

    async def discover(self, base_url: str) -> AsyncIterator[str]:
        queue = [urljoin(base_url, value) for value in self.roots]
        seen: set[str] = set()
        emitted: set[str] = set()
        while queue:
            url = queue.pop(0)
            if url in seen:
                continue
            if len(seen) >= self.limit:
                raise RuntimeError(f"sitemap traversal limit {self.limit} reached")
            seen.add(url)
            response = await self.transport.request(TransportRequest(
                url=url, purpose=RequestPurpose.DISCOVERY, priority=RequestPriority.DISCOVERY,
                estimated_bytes=500_000,
            ))
            if response.status >= 400:
                raise RuntimeError(f"sitemap request failed with status {response.status}")
            document = response.text()
            locations = [urljoin(url, value.strip()) for value in LOC.findall(document)]
            if "<sitemapindex" in document.lower():
                queue.extend(locations)
                continue
            for location in locations:
                if location not in emitted and (self.product_pattern is None or self.product_pattern.search(location)):
                    emitted.add(location)
                    yield location

