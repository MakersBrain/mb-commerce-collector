"""Application-owned Keramik Kraft plugin using the library page lifecycle."""

from __future__ import annotations

import hashlib
import re
from collections.abc import AsyncIterator
from decimal import Decimal, InvalidOperation
from html import unescape
from typing import Any
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
    CategoryRef,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
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
    RotationReason,
    TransportFailure,
    TransportRequest,
    TransportResponse,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue


class KeramikKraftOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    category_paths: tuple[str, ...] = ()
    category_page_limit: int = Field(default=150, ge=1, le=10_000)
    page_limit: int = Field(default=500, ge=1, le=100_000)
    brand: str | None = None
    vat_rate: Decimal | None = Field(default=None, ge=0, le=1)
    render: bool | None = None

    def generic_options(self) -> GenericPagesOptions:
        return GenericPagesOptions(
            discovery=DiscoveryOptions(
                category_urls=self.category_paths or ("/",),
                use_advertised_sitemaps=False,
                category_page_limit=self.category_page_limit,
            ),
            brand=self.brand,
            currency="EUR",
            vat_status="inclusive",
            vat_rate=self.vat_rate,
            page_limit=self.page_limit,
            render=self.render,
        )


class KeramikKraftConnector(GenericPagesConnector):
    name = "keramik-kraft"
    platform = "keramik_kraft"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.UNKNOWN}),
        browser=BrowserRequirement.OPTIONAL,
    )

    def __init__(
        self,
        transport: CommerceTransport,
        options: KeramikKraftOptions,
        context: ConnectorContext,
    ) -> None:
        handoff = _ListingDocumentHandoff(transport)
        super().__init__(
            handoff,
            options.generic_options(),
            context,
            parser=_KeramikKraftParser(options, context),
            discovery=_KeramikKraftDiscovery(handoff, options, context),
        )

    def _partition_key(self) -> str:
        return "main"


class KeramikKraftFactory:
    name = KeramikKraftConnector.name
    version = KeramikKraftConnector.version
    options_model: type[BaseModel] = KeramikKraftOptions

    def plan(
        self,
        options: BaseModel,
        *,
        base_url: str,
        request_partitions: tuple[str, ...] = (),
    ) -> ConnectorPlan:
        del base_url, request_partitions
        if not isinstance(options, KeramikKraftOptions):
            raise TypeError(
                "keramik-kraft factory requires validated KeramikKraftOptions options"
            )
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
        return KeramikKraftConnector(
            transport, KeramikKraftOptions.model_validate(options), context
        )


class _ListingDocumentHandoff:
    """Reuse bounded discovery responses once during entity projection."""

    def __init__(self, transport: CommerceTransport) -> None:
        self.transport = transport
        self._documents: dict[str, TransportResponse] = {}

    def remember(self, url: str, response: TransportResponse) -> None:
        self._documents[url] = response

    async def request(self, request: TransportRequest) -> TransportResponse:
        if request.purpose == RequestPurpose.ENTITY:
            response = self._documents.pop(request.url, None)
            if response is not None:
                return response
        return await self.transport.request(request)

    async def rotate_identity(self, reason: RotationReason) -> None:
        await self.transport.rotate_identity(reason)


class _KeramikKraftDiscovery:
    name = "keramik-kraft-categories"
    version = "1"
    _CARD = re.compile(
        r'<div[^>]+class="product\b[^"]*"[^>]*>(.*?)<!--\s*/product',
        re.IGNORECASE | re.DOTALL,
    )

    def __init__(
        self,
        transport: _ListingDocumentHandoff,
        options: KeramikKraftOptions,
        context: ConnectorContext,
    ) -> None:
        self.transport = transport
        self.options = options
        self.context = context

    async def discover(self, base_url: str) -> AsyncIterator[str]:
        queue = [urljoin(base_url, value) for value in self.options.category_paths] or [
            base_url
        ]
        seen: set[str] = set()
        origin = urlparse(base_url).netloc
        while queue:
            if self.context.cancelled():
                return
            url = queue.pop(0)
            if url in seen:
                continue
            if len(seen) >= self.options.category_page_limit:
                raise DiscoveryFailure(
                    f"Keramik-Kraft category page limit {self.options.category_page_limit} reached",
                    retryable=False,
                )
            seen.add(url)
            response = await self._document(url)
            document = response.text()
            if any(
                _usable_card(match.group(1))
                for match in self._CARD.finditer(document)
            ):
                self.transport.remember(url, response)
                yield url
            for href in re.findall(r'href="([^"]+)"', document):
                candidate = _canonical(urljoin(url, unescape(href)))
                if (
                    urlparse(candidate).netloc == origin
                    and _category(candidate)
                    and candidate not in seen
                    and candidate not in queue
                ):
                    queue.append(candidate)

    async def _document(self, url: str) -> TransportResponse:
        required = self.options.render is True
        try:
            return await self._request(url, browser=required)
        except ResponseBodyTooLarge:
            raise
        except (DiscoveryFailure, TransportFailure):
            if self.options.render is not None:
                raise
        return await self._request(url, browser=True)

    async def _request(self, url: str, *, browser: bool) -> TransportResponse:
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
                f"Keramik-Kraft discovery request failed with status {response.status}",
                retryable=response.status >= 500,
            )
        return response


class _KeramikKraftParser:
    name = "keramik-kraft-listing-cards"
    version = "1"
    _CARD = _KeramikKraftDiscovery._CARD

    def __init__(
        self, options: KeramikKraftOptions, context: ConnectorContext
    ) -> None:
        self.options = options
        self.context = context

    def parse(
        self, document: str, *, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        observed_at = self.context.clock()
        categories = _breadcrumb(url)
        snapshots: list[CommerceProductSnapshot] = []
        for match in self._CARD.finditer(document):
            card = match.group(1)
            if not _usable_card(card):
                continue
            name_markup = re.search(
                r'<p[^>]+class="text-sm[^"]*"[^>]*>(.*?)</p>',
                card,
                re.IGNORECASE | re.DOTALL,
            )
            price_match = re.search(
                r'([0-9]{1,3}(?:[.\s][0-9]{3})*,[0-9]{2})\s*(?:&euro;|€)'
                r'(?:\s*<i[^>]*>\s*\(?\s*([0-9]{1,3}(?:[.\s][0-9]{3})*,[0-9]{2})\s*(?:&euro;|€)\s*HT)?',
                card,
                re.IGNORECASE,
            )
            if not name_markup or not price_match:
                continue
            parts = [
                _clean(value).lstrip("=>").strip()
                for value in re.split(r"<br\s*/?>", name_markup.group(1))
            ]
            parts = [value for value in parts if value]
            gross = _decimal(price_match.group(1))
            if not parts or gross is None:
                continue
            name, variant_title = parts[0], " ".join(parts[1:])
            net = _decimal(price_match.group(2)) if price_match.group(2) else None
            code = _match_text(card, r'<p[^>]+class="p mb-1"[^>]*>(.*?)</p>')
            link = re.search(
                r'href="([^"]*_[A-Za-z0-9.\-]+\.html[^"]*)"',
                card,
                re.IGNORECASE,
            )
            product_url = (
                _canonical(urljoin(url, unescape(link.group(1)))) if link else url
            )
            variant_title = variant_title or _variant(product_url)
            image = re.search(r'<img[^>]+src="([^"]+)"', card, re.IGNORECASE)
            image_url = urljoin(url, unescape(image.group(1))) if image else None
            external_id = code or _url_id(product_url)
            evidence = Evidence(
                method="html",
                source_url=url,
                source_field="keramik_kraft_listing_card",
                observed_at=observed_at,
                confidence="published",
            )
            attributes: dict[str, JsonValue] = {
                "price_text": _clean(price_match.group(0)) or None,
            }
            if net is not None:
                attributes["Netto-Preis EUR"] = float(net)
            snapshots.append(
                CommerceProductSnapshot(
                    connector="keramik-kraft",
                    source_id=source_id,
                    external_id=external_id,
                    canonical_url=product_url,
                    title=name,
                    observed_at=observed_at,
                    vendor=_brand(name) or self.options.brand,
                    categories=tuple(CategoryRef(name=value) for value in categories),
                    images=(MediaRef(url=image_url),) if image_url else (),
                    variants=(
                        CommerceVariant(
                            external_id=external_id,
                            is_default=True,
                            canonical_url=product_url,
                            title=variant_title or None,
                            sku=code or None,
                            offers=(
                                CommerceOffer(
                                    price=Money(amount=gross, currency="EUR"),
                                    observed_at=observed_at,
                                    evidence=(evidence,),
                                    vat_status="inclusive",
                                    vat_rate=self.options.vat_rate,
                                ),
                            ),
                            stock=StockState(
                                availability=Availability.IN_STOCK,
                                observed_at=observed_at,
                                evidence=(evidence,),
                            ),
                            published_attributes=attributes,
                        ),
                    ),
                    platform_extensions={
                        "raw": {
                            "page": url,
                            "code": code,
                            "gross": float(gross),
                            "net": float(net) if net is not None else None,
                            "variant": variant_title,
                        }
                    },
                )
            )
        return tuple(snapshots)


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


def _usable_card(card: str) -> bool:
    name = re.search(
        r'<p[^>]+class="text-sm[^"]*"[^>]*>(.*?)</p>',
        card,
        re.IGNORECASE | re.DOTALL,
    )
    price = re.search(
        r'([0-9]{1,3}(?:[.\s][0-9]{3})*,[0-9]{2})\s*(?:&euro;|€)',
        card,
        re.IGNORECASE,
    )
    if name is None or price is None or _decimal(price.group(1)) is None:
        return False
    return any(
        _clean(value).lstrip("=>").strip()
        for value in re.split(r"<br\s*/?>", name.group(1))
    )


def _category(url: str) -> bool:
    path = urlparse(url).path
    if not path.endswith(".html") or re.search(r"_[A-Za-z0-9.\-]+\.html", url):
        return False
    if re.search(r"/(?:error_page|menu|index\d*)\.html$", path, re.IGNORECASE):
        return False
    return bool(re.match(r"^/[a-z]{2}/", path))


def _variant(product_url: str) -> str:
    stem = urlparse(product_url).path.rsplit("/", 1)[-1].removesuffix(".html")
    parts = stem.split("_")
    if len(parts) < 3:
        return ""
    return " ".join(
        part for part in "_".join(parts[1:-1]).split("-") if part
    ).strip()


def _breadcrumb(page_url: str) -> tuple[str, ...]:
    parts = urlparse(page_url).path.rsplit("/", 1)[0].strip("/").split("/")
    return tuple(
        part.replace("--", " - ").replace("-", " ") for part in parts[1:] if part
    )


def _brand(name: str) -> str | None:
    for maker in (
        "Botz",
        "Mayco",
        "Duncan",
        "Amaco",
        "Terracolor",
        "Ceraline",
        "Wolbring",
    ):
        if re.search(rf"\b{maker}\b", name, re.IGNORECASE):
            return maker
    return None


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    number = re.sub(r"[^0-9.,-]", "", _clean(value))
    number = re.sub(r"[.,](?=\d{3}(?:\D|$))", "", number).replace(",", ".")
    try:
        result = Decimal(number)
    except InvalidOperation:
        return None
    return result if result.is_finite() and result >= 0 else None


def _match_text(document: str, pattern: str) -> str:
    match = re.search(pattern, document, re.IGNORECASE | re.DOTALL)
    return _clean(match.group(1)) if match else ""


def _url_id(url: str) -> str:
    return hashlib.sha256(_canonical(url).encode()).hexdigest()[:24]


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(
        unescape(re.sub(r"<[^>]+>", " ", str(value))).split()
    )
