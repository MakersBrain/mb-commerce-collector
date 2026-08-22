from __future__ import annotations

import re
from collections.abc import AsyncIterator
from urllib.parse import urljoin, urlsplit

from mb_commerce_scraper.transports import (
    CommerceTransport,
    RequestPriority,
    RequestPurpose,
    ResponseBodyTooLarge,
    TransportFailure,
    TransportRequest,
)

LOC = re.compile(r"<loc\b[^>]*>\s*(.*?)\s*</loc>", re.IGNORECASE | re.DOTALL)
SITEMAP_DIRECTIVE = re.compile(
    r"^\s*Sitemap\s*:\s*(\S+)\s*$", re.IGNORECASE | re.MULTILINE
)


class DiscoveryFailure(RuntimeError):
    """A bounded remote discovery failure safe to expose as a diagnostic."""

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


async def advertised_sitemaps(
    transport: CommerceTransport, base_url: str
) -> tuple[str, ...]:
    """Read and normalize sitemap directives from the origin robots file."""
    parsed = urlsplit(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    try:
        response = await transport.request(
            TransportRequest(
                url=robots_url,
                purpose=RequestPurpose.ROBOTS,
                priority=RequestPriority.DISCOVERY,
                required=False,
                estimated_bytes=100_000,
            )
        )
    except (ResponseBodyTooLarge, TransportFailure) as error:
        raise DiscoveryFailure(
            f"robots sitemap request failed: {type(error).__name__}",
            retryable=not isinstance(error, ResponseBodyTooLarge),
        ) from error
    if response.status >= 400:
        return ()
    return tuple(
        dict.fromkeys(
            urljoin(robots_url, match.strip())
            for match in SITEMAP_DIRECTIVE.findall(response.text())
        )
    )


class SitemapDiscovery:
    name = "sitemap"
    version = "1"

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
                raise DiscoveryFailure(
                    f"sitemap traversal limit {self.limit} reached"
                )
            seen.add(url)
            try:
                response = await self.transport.request(TransportRequest(
                    url=url, purpose=RequestPurpose.DISCOVERY, priority=RequestPriority.DISCOVERY,
                    estimated_bytes=500_000,
                ))
            except (ResponseBodyTooLarge, TransportFailure) as error:
                raise DiscoveryFailure(
                    f"sitemap request failed: {type(error).__name__}",
                    retryable=not isinstance(error, ResponseBodyTooLarge),
                ) from error
            if response.status >= 400:
                raise DiscoveryFailure(
                    f"sitemap request failed with status {response.status}"
                )
            document = response.text()
            locations = [urljoin(url, value.strip()) for value in LOC.findall(document)]
            if "<sitemapindex" in document.lower():
                queue.extend(locations)
                continue
            for location in locations:
                if location not in emitted and (self.product_pattern is None or self.product_pattern.search(location)):
                    emitted.add(location)
                    yield location
