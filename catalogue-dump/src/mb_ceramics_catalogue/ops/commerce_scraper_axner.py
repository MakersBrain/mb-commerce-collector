"""Application-owned Axner plugin composed from public library contracts."""

from __future__ import annotations

import hashlib
import re
from collections.abc import AsyncIterator
from decimal import Decimal, InvalidOperation
from html import unescape
from typing import Any, Literal, cast
from urllib.parse import urljoin, urlparse, urlunparse

from mb_commerce_scraper.connectors import (
    BrowserRequirement,
    CommerceConnector,
    ConnectorCapabilities,
    ConnectorContext,
    ConnectorPlan,
    DiscoveryOptions,
    GenericPagesConnector,
    GenericPagesOptions,
)
from mb_commerce_scraper.discovery import DiscoveryFailure
from mb_commerce_scraper.models import (
    Availability,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    DocumentRef,
    Evidence,
    MediaRef,
    Money,
    RefreshMode,
    SnapshotField,
    StockQuantityKind,
    StockState,
)
from mb_commerce_scraper.transports import (
    BrowserHint,
    CommerceTransport,
    RequestPriority,
    RequestPurpose,
    ResponseBodyTooLarge,
    TransportFailure,
    TransportRequest,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue


class AxnerOptions(BaseModel):
    """Bounded source options owned by the catalogue application plugin."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    category_url: str | None = None
    category_page_limit: int = Field(default=400, ge=1, le=10_000)
    page_limit: int = Field(default=500, ge=1, le=100_000)
    brand: str | None = None
    currency: str = Field(default="USD", pattern=r"^[A-Z]{3}$")
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None = None
    vat_rate: Decimal | None = Field(default=None, ge=0, le=1)
    render: bool | None = None

    def generic_options(self) -> GenericPagesOptions:
        return GenericPagesOptions(
            discovery=DiscoveryOptions(
                category_urls=(self.category_url or "/sitemap.aspx",),
                use_advertised_sitemaps=False,
                category_page_limit=self.category_page_limit,
            ),
            currency=self.currency,
            brand=self.brand,
            vat_status=self.vat_status,
            vat_rate=self.vat_rate,
            page_limit=self.page_limit,
            render=self.render,
        )


class AxnerConnector(GenericPagesConnector):
    """Use the shared page engine with Axner-specific discovery and parsing."""

    name = "axner"
    platform = "axner"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.UNKNOWN}),
        supports_documents=True,
        browser=BrowserRequirement.OPTIONAL,
    )

    def __init__(
        self,
        transport: CommerceTransport,
        options: AxnerOptions,
        context: ConnectorContext,
    ) -> None:
        self.axner_options = options
        super().__init__(
            transport,
            options.generic_options(),
            context,
            parser=_AxnerParser(options, context),
            discovery=_AxnerDiscovery(transport, options, context),
        )

    def _partition_key(self) -> str:
        return "main"


class AxnerFactory:
    """Explicit application factory registered beside library built-ins."""

    name = AxnerConnector.name
    version = AxnerConnector.version
    options_model: type[BaseModel] = AxnerOptions

    def plan(
        self,
        options: BaseModel,
        *,
        base_url: str,
        request_partitions: tuple[str, ...] = (),
    ) -> ConnectorPlan:
        del base_url, request_partitions
        if not isinstance(options, AxnerOptions):
            raise TypeError("axner factory requires validated AxnerOptions options")
        validated = options
        return ConnectorPlan(
            partitions=("main",),
            browser=(
                BrowserRequirement.NEVER
                if validated.render is False
                else BrowserRequirement.OPTIONAL
            ),
        )

    def build(
        self,
        *,
        transport: CommerceTransport,
        options: BaseModel,
        context: ConnectorContext,
    ) -> CommerceConnector:
        validated = AxnerOptions.model_validate(options)
        return AxnerConnector(transport, validated, context)


class _AxnerDiscovery:
    name = "axner-departments"
    version = "1"
    _LISTING_LINK = re.compile(
        r'class="[^"]*product-list-link[^"]*"[^>]*>\s*<a[^>]+href="([^"]+)"',
        re.IGNORECASE,
    )
    _NEXT_PAGE = re.compile(r'href="([^"]*\?page=\d+)"', re.IGNORECASE)

    def __init__(
        self,
        transport: CommerceTransport,
        options: AxnerOptions,
        context: ConnectorContext,
    ) -> None:
        self.transport = transport
        self.options = options
        self.context = context

    async def discover(self, base_url: str) -> AsyncIterator[str]:
        index = self.options.category_url or urljoin(base_url, "/sitemap.aspx")
        document = await self._document(index)
        origin = urlparse(base_url).netloc
        queue = [
            _canonical(urljoin(index, href))
            for href in dict.fromkeys(
                re.findall(r'href="(/[A-Za-z0-9\-]+\.aspx)"', document)
            )
            if urlparse(urljoin(index, href)).netloc == origin
        ]
        seen: set[str] = set()
        emitted: set[str] = set()
        while queue:
            if self.context.cancelled():
                return
            url = queue.pop(0)
            if url in seen:
                continue
            if len(seen) >= self.options.category_page_limit:
                raise DiscoveryFailure(
                    f"Axner category page limit {self.options.category_page_limit} reached",
                    retryable=False,
                )
            seen.add(url)
            listing = await self._document(url)
            for href in self._LISTING_LINK.findall(listing):
                candidate = _canonical(urljoin(url, unescape(href)))
                if urlparse(candidate).netloc == origin and candidate not in emitted:
                    emitted.add(candidate)
                    yield candidate
            for href in self._NEXT_PAGE.findall(listing):
                page = _canonical(urljoin(url, unescape(href)))
                if page not in seen and page not in queue:
                    queue.append(page)

    async def _document(self, url: str) -> str:
        required = self.options.render is True
        try:
            return await self._request(url, browser=required)
        except ResponseBodyTooLarge:
            raise
        except (DiscoveryFailure, TransportFailure):
            if self.options.render is not None:
                raise
        return await self._request(url, browser=True)

    async def _request(self, url: str, *, browser: bool) -> str:
        response = await self.transport.request(
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
                f"Axner discovery request failed with status {response.status}",
                retryable=response.status >= 500,
            )
        return response.text()


class _AxnerParser:
    name = "axner-product"
    version = "1"

    def __init__(self, options: AxnerOptions, context: ConnectorContext) -> None:
        self.options = options
        self.context = context

    def parse(
        self, document: str, *, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        observed_at = self.context.clock()
        name = _match_text(document, r"<h1[^>]*>(.*?)</h1>") or _meta(
            document, "og:title"
        )
        price_text = _match_text(
            document,
            r'class="[^"]*product-list-cost-value[^"]*"[^>]*>\s*\$?\s*([\d,]+\.\d{2})',
        )
        amount, currency = _price(price_text)
        if not name or amount is None:
            return ()
        details = {
            _clean(label).rstrip(":"): _clean(value)
            for label, value in re.findall(
                r'class="prod-detail-part-label"[^>]*>(.*?)</span>\s*<span[^>]*class="prod-detail-part-value"[^>]*>(.*?)</span>',
                document,
                re.IGNORECASE | re.DOTALL,
            )
        }
        reference = details.get("Axner Number") or None
        brand = _match_text(
            document, r'class="prod-detail-man-name-value"[^>]*>(.*?)</span>'
        ) or self.options.brand
        external_id = reference or _url_id(url)
        evidence = _evidence(url, observed_at, "axner_data_item")
        offer = CommerceOffer(
            price=Money(amount=amount, currency=currency or self.options.currency),
            observed_at=observed_at,
            evidence=(evidence,),
            vat_status=cast(Any, self.options.vat_status or "unknown"),
            vat_rate=self.options.vat_rate,
        )
        snapshot = CommerceProductSnapshot(
            connector="axner",
            source_id=source_id,
            external_id=external_id,
            canonical_url=_canonical(url),
            title=name,
            observed_at=observed_at,
            description=_match_text(
                document, r'class="prod-detail-desc"[^>]*>(.*?)</div>'
            )
            or None,
            vendor=brand,
            images=tuple(
                MediaRef(url=urljoin(url, value))
                for value in dict.fromkeys(
                    re.findall(
                        r'src="(/ProductImages/[^"]+)"', document, re.IGNORECASE
                    )
                )
                if "thumb" not in value.rsplit("/", 1)[-1].lower()
            ),
            documents=tuple(
                DocumentRef(
                    url=document_url,
                    title=label or None,
                    media_type="application/pdf",
                    observed_at=observed_at,
                    evidence=(_evidence(url, observed_at, "a[href*=.pdf]"),),
                )
                for document_url, label in _pdf_links(document, url)
            ),
            variants=(
                CommerceVariant(
                    external_id=external_id,
                    is_default=True,
                    canonical_url=_canonical(url),
                    sku=reference,
                    offers=(offer,),
                    stock=StockState(
                        availability=Availability.UNKNOWN,
                        observed_at=observed_at,
                        evidence=(evidence,),
                    ),
                    published_attributes={
                        **cast(
                            dict[str, JsonValue],
                            {
                                key: value
                                for key, value in details.items()
                                if key != "Axner Number"
                            },
                        ),
                        "price_text": f"{price_text} {currency or self.options.currency}".strip(),
                    },
                ),
            ),
            platform_extensions={
                "raw": {
                    "details": cast(JsonValue, details),
                    "options_available": bool(
                        re.search(r"options[- ]available", document, re.IGNORECASE)
                    ),
                }
            },
        )
        return (snapshot,)


def _canonical(url: str) -> str:
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


def _match_text(document: str, pattern: str) -> str:
    match = re.search(pattern, document, re.IGNORECASE | re.DOTALL)
    return _clean(match.group(1)) if match else ""


def _meta(document: str, key: str) -> str:
    escaped = re.escape(key)
    for pattern in (
        rf'<meta[^>]+(?:property|name)=["\']{escaped}["\'][^>]+content=["\']([^"\']*)',
        rf'<meta[^>]+content=["\']([^"\']*)["\'][^>]+(?:property|name)=["\']{escaped}["\']',
    ):
        match = re.search(pattern, document, re.IGNORECASE)
        if match:
            return unescape(match.group(1)).strip()
    return ""


def _price(value: Any) -> tuple[Decimal | None, str | None]:
    if value is None or isinstance(value, bool):
        return None, None
    text = _clean(value)
    currency = next(
        (
            mapped
            for symbol, mapped in (("€", "EUR"), ("$", "USD"), ("£", "GBP"))
            if symbol in text
        ),
        None,
    )
    number = re.sub(r"[^0-9.,-]", "", text)
    number = re.sub(r"[.,](?=\d{3}(?:\D|$))", "", number).replace(",", ".")
    try:
        result = Decimal(number)
    except InvalidOperation:
        return None, currency
    return (result, currency) if result.is_finite() and result >= 0 else (None, currency)


def _evidence(url: str, observed_at: Any, source_field: str) -> Evidence:
    return Evidence(
        method="html",
        source_url=url,
        source_field=source_field,
        observed_at=observed_at,
        confidence="published",
    )


def _url_id(url: str) -> str:
    return hashlib.sha256(_canonical(url).encode()).hexdigest()[:24]


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(
        unescape(re.sub(r"<[^>]+>", " ", str(value))).split()
    )


def _pdf_links(document: str, page_url: str) -> tuple[tuple[str, str], ...]:
    return tuple(
        (urljoin(page_url, unescape(match.group(1))), _clean(match.group(2)))
        for match in re.finditer(
            r'<a[^>]+href=["\']([^"\']+\.pdf[^"\']*)["\'][^>]*>(.*?)</a>',
            document,
            re.IGNORECASE | re.DOTALL,
        )
    )
