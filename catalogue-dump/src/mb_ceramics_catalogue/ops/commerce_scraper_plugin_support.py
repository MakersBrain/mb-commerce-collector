"""Shared support for application-owned commerce scraper plugins."""

from __future__ import annotations

import hashlib
import re
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlparse, urlunparse

from mb_commerce_scraper.discovery import DiscoveryFailure
from mb_commerce_scraper.models import Evidence
from mb_commerce_scraper.transports import (
    BrowserHint,
    CommerceTransport,
    RequestPriority,
    RequestPurpose,
    ResponseBodyTooLarge,
    TransportFailure,
    TransportRequest,
    TransportResponse,
)

from mb_ceramics_catalogue.scrapers.domain import clean


def canonical_url(url: str) -> str:
    """Drop fragments and known traversal-only query strings."""

    parsed = urlparse(url)
    query = (
        ""
        if re.search(
            r"(?:^|&)(?:order|tag|id_currency|search_query|back|q|sort)=",
            parsed.query,
        )
        else parsed.query
    )
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, ""))


def match_text(document: str, pattern: str) -> str:
    """Return cleaned text from the first capture group of an HTML pattern."""

    match = re.search(pattern, document, re.IGNORECASE | re.DOTALL)
    return clean(match.group(1)) if match else ""


def decimal(value: Any) -> Decimal | None:
    """Parse a localized finite, non-negative decimal value."""

    if value is None or isinstance(value, bool):
        return None
    number = re.sub(r"[^0-9.,-]", "", clean(value))
    number = re.sub(r"[.,](?=\d{3}(?:\D|$))", "", number).replace(",", ".")
    try:
        result = Decimal(number)
    except InvalidOperation:
        return None
    return result if result.is_finite() and result >= 0 else None


def url_id(url: str) -> str:
    """Build the stable URL-derived identity used by page plugins."""

    return hashlib.sha256(canonical_url(url).encode()).hexdigest()[:24]


def evidence(url: str, observed_at: Any, source_field: str) -> Evidence:
    """Build published HTML evidence for a plugin observation."""

    return Evidence(
        method="html",
        source_url=url,
        source_field=source_field,
        observed_at=observed_at,
        confidence="published",
    )


async def discovery_response(
    transport: CommerceTransport,
    url: str,
    *,
    render: bool | None,
    label: str,
) -> TransportResponse:
    """Fetch discovery content with the plugins' optional browser fallback."""

    async def request(*, browser: bool) -> TransportResponse:
        response = await transport.request(
            TransportRequest(
                url=url,
                purpose=RequestPurpose.DISCOVERY,
                priority=RequestPriority.DISCOVERY,
                estimated_bytes=1_000_000 if browser else 500_000,
                browser=BrowserHint.REQUIRED if browser else BrowserHint.NEVER,
            )
        )
        if response.status >= 400:
            raise DiscoveryFailure(
                f"{label} discovery request failed with status {response.status}",
                retryable=response.status >= 500,
            )
        return response

    required = render is True
    try:
        return await request(browser=required)
    except ResponseBodyTooLarge:
        raise
    except (DiscoveryFailure, TransportFailure):
        if render is not None:
            raise
    return await request(browser=True)
