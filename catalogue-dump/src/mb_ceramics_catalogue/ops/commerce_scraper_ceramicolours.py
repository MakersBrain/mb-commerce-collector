"""Application-owned Ceramicolours plugin using the library page lifecycle."""

from __future__ import annotations

import html
import re
from collections.abc import AsyncIterator
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, cast
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

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
    CollectionRequest,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    ConnectorCheckpoint,
    EntityPage,
    Evidence,
    MediaRef,
    Money,
    RefreshMode,
    SnapshotField,
    StockQuantityKind,
    StockState,
)
from mb_commerce_scraper.transports import (
    BrowserBackendUnavailable,
    BrowserEvaluation,
    BrowserHint,
    BudgetExhausted,
    CommerceTransport,
    RequestPriority,
    RequestPurpose,
    ResponseBodyTooLarge,
    ResponseDecodeFailure,
    RotationReason,
    TransportFailure,
    TransportRequest,
    TransportResponse,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from mb_ceramics_catalogue.transports.browser import BrowserUnavailable

PACK_PRICE_SCRIPT = """
async () => {
  const select = document.querySelector('#product-pack-field');
  const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
  const read = id => document.querySelector(id)?.textContent.trim() || '';
  if (!select) return [];
  const results = [];
  for (const option of Array.from(select.options)) {
    select.value = option.value; select.dispatchEvent(new Event('change'));
    if (typeof updatePrice === 'function') { try { updatePrice(); } catch (error) {} }
    await wait(500);
    results.push({pack: option.textContent.trim(), value: option.value,
      price: read('#product-price'), unit_price: read('#product-unit-price')});
  }
  return results;
}
"""


class CeramicoloursOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    category_ids: tuple[str, ...] = ()
    category_page_limit: int = Field(default=25, ge=1, le=10_000)
    page_limit: int = Field(default=500, ge=1, le=100_000)
    brand: str | None = None
    vat_status: Literal["inclusive", "exclusive", "unknown"] = "inclusive"
    render: bool | None = None

    def generic_options(self) -> GenericPagesOptions:
        return GenericPagesOptions(
            discovery=DiscoveryOptions(
                use_advertised_sitemaps=False,
                product_pattern=r"(?:^|/)Articolo\.php$",
                category_page_limit=self.category_page_limit,
            ),
            currency="EUR",
            brand=self.brand,
            vat_status=self.vat_status,
            page_limit=self.page_limit,
            render=self.render,
        )


class CeramicoloursConnector(GenericPagesConnector):
    name = "ceramicolours"
    platform = "ceramicolours"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset(
            {StockQuantityKind.EXACT, StockQuantityKind.UNKNOWN}
        ),
        browser=BrowserRequirement.OPTIONAL,
    )

    def __init__(
        self,
        transport: CommerceTransport,
        options: CeramicoloursOptions,
        context: ConnectorContext,
    ) -> None:
        handoff = _PackEvaluationHandoff(transport, context)
        self._pack_handoff = handoff
        super().__init__(
            handoff,
            options.generic_options(),
            context,
            parser=_CeramicoloursParser(options, context, handoff),
            discovery=_CeramicoloursDiscovery(transport, options, context),
        )

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        self._pack_handoff.configure(
            required=SnapshotField.OFFERS in request.requested_fields
        )
        async for page in super().collect(request, checkpoint):
            yield page

    def _partition_key(self) -> str:
        return "main"


class CeramicoloursFactory:
    name = CeramicoloursConnector.name
    version = CeramicoloursConnector.version
    options_model: type[BaseModel] = CeramicoloursOptions

    def plan(
        self,
        options: BaseModel,
        *,
        base_url: str,
        request_partitions: tuple[str, ...] = (),
    ) -> ConnectorPlan:
        del base_url, request_partitions
        if not isinstance(options, CeramicoloursOptions):
            raise TypeError(
                "ceramicolours factory requires validated CeramicoloursOptions options"
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
        return CeramicoloursConnector(
            transport, CeramicoloursOptions.model_validate(options), context
        )


class _PackEvaluationHandoff:
    """Evaluate dynamic pack totals through the same neutral transport stack."""

    def __init__(
        self, transport: CommerceTransport, context: ConnectorContext
    ) -> None:
        self.transport = transport
        self.context = context
        self._packs: dict[str, list[dict[str, JsonValue]]] = {}
        self._required = False

    def configure(self, *, required: bool) -> None:
        self._packs.clear()
        self._required = required

    def take(self, url: str) -> list[dict[str, JsonValue]]:
        return self._packs.pop(url, [])

    async def request(self, request: TransportRequest) -> TransportResponse:
        response = await self.transport.request(request)
        if (
            request.purpose is not RequestPurpose.ENTITY
            or request.evaluation is not None
            or response.status >= 400
            or b"product-pack-field" not in response.content
        ):
            return response
        if self.context.cancelled():
            return response
        try:
            evaluated = await self.transport.request(
                TransportRequest(
                    url=request.url,
                    purpose=RequestPurpose.ENRICHMENT,
                    priority=(
                        RequestPriority.DATASET_REQUIRED
                        if self._required
                        else RequestPriority.OPTIONAL
                    ),
                    required=self._required,
                    estimated_bytes=1_000_000,
                    browser=BrowserHint.REQUIRED,
                    evaluation=BrowserEvaluation(
                        action_id="ceramicolours.pack-prices.v1",
                        script=PACK_PRICE_SCRIPT,
                        wait_for="#product-pack-field",
                        wait_milliseconds=1_500,
                    ),
                )
            )
            self._packs[request.url] = _pack_rows(evaluated.json_value())
        except BudgetExhausted:
            if self._required:
                raise
            self._emit_fallback("BudgetExhausted", request.url)
        except (
            BrowserBackendUnavailable,
            BrowserUnavailable,
            ResponseBodyTooLarge,
            ResponseDecodeFailure,
            TransportFailure,
        ) as error:
            self._emit_fallback(type(error).__name__, request.url)
        return response

    def _emit_fallback(self, reason: str, url: str) -> None:
        self.context.telemetry.emit(
            "ceramicolours.pack_evaluation_fallback",
            {"level": "warning", "url": url, "reason": reason},
        )

    async def rotate_identity(self, reason: RotationReason) -> None:
        await self.transport.rotate_identity(reason)


class _CeramicoloursDiscovery:
    name = "ceramicolours-categories"
    version = "1"

    def __init__(
        self,
        transport: CommerceTransport,
        options: CeramicoloursOptions,
        context: ConnectorContext,
    ) -> None:
        self.transport = transport
        self.options = options
        self.context = context

    async def discover(self, base_url: str) -> AsyncIterator[str]:
        home = await self._document(base_url)
        wanted = set(self.options.category_ids)
        categories: list[str] = []
        for href in re.findall(
            r'href="([^\"]*Articoli\.php\?[^\"]+)"', home, re.I
        ):
            url = urljoin(base_url, html.unescape(href))
            identifier = parse_qs(urlparse(url).query).get("Id", [""])[0]
            if not wanted or identifier in wanted:
                categories.append(re.sub(r"&page=\d+", "", url))
        emitted: set[str] = set()
        for category in dict.fromkeys(categories):
            exhausted = False
            for page in range(1, self.options.category_page_limit + 1):
                if self.context.cancelled():
                    return
                listing_url = f"{category}&page={page}"
                document = await self._document(listing_url)
                found = [
                    _canonical(urljoin(base_url, html.unescape(href)))
                    for href in re.findall(
                        r'<a href="([^\"]*Articolo\.php\?[^\"]+)"[^>]*class="product-name">',
                        document,
                        re.I,
                    )
                ]
                for product_url in found:
                    if product_url not in emitted:
                        emitted.add(product_url)
                        yield product_url
                if not found:
                    exhausted = True
                    break
            if not exhausted:
                raise DiscoveryFailure(
                    "Ceramicolours category page limit "
                    f"{self.options.category_page_limit} reached",
                    retryable=False,
                )

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
                f"Ceramicolours discovery request failed with status {response.status}",
                retryable=response.status >= 500,
            )
        return response.text()


class _CeramicoloursParser:
    name = "ceramicolours-product"
    version = "1"

    def __init__(
        self,
        options: CeramicoloursOptions,
        context: ConnectorContext,
        handoff: _PackEvaluationHandoff,
    ) -> None:
        self.options = options
        self.context = context
        self.handoff = handoff

    def parse(
        self, document: str, *, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        name = _match_text(document, r"<h1[^>]*>(.*?)</h1>") or _match_text(
            document, r'class="product-name"[^>]*>(.*?)</a>'
        )
        if not name:
            return ()
        observed_at = self.context.clock()
        code = _clean(parse_qs(urlparse(url).query).get("cod", [""])[0])
        temperature = _match_text(document, r"Temp\.\s*</span>\s*(.*?)</p>")
        evidence = Evidence(
            method="html",
            source_url=url,
            source_field="ceramicolours_product",
            observed_at=observed_at,
            confidence="published",
        )
        variants = self._pack_variants(
            document, url, code, temperature, observed_at, evidence
        )
        parser = "browser"
        if not variants:
            parser = "dom"
            price_text = _match_text(document, r"Prezzo:\s*</span>\s*(.*?)</p>")
            amount = _price(price_text)
            if amount is None:
                return ()
            variants = (
                CommerceVariant(
                    external_id=code or _url_id(url),
                    is_default=True,
                    canonical_url=_canonical(url),
                    offers=(
                        CommerceOffer(
                            price=Money(amount=amount, currency="EUR"),
                            observed_at=observed_at,
                            evidence=(evidence,),
                            vat_status=self.options.vat_status,
                        ),
                    ),
                    stock=_stock(None, observed_at, evidence),
                    published_attributes={
                        **({"Temperatura": temperature} if temperature else {}),
                        "price_text": price_text or None,
                    },
                    platform_extensions={
                        "raw": {"url": url, "code": code, "temperature": temperature}
                    },
                ),
            )
        images = tuple(
            MediaRef(url=urljoin(url, html.unescape(value)))
            for value in dict.fromkeys(
                re.findall(
                    r'<img[^>]+src="([^\"]*upload-immagini[^\"]*)"',
                    document,
                    re.I,
                )
            )
        )
        categories = tuple(
            CategoryRef(name=value)
            for value in (
                _clean(markup)
                for markup in re.findall(
                    r'<li[^>]*class="breadcrumb[^\"]*"[^>]*>.*?<a[^>]*>(.*?)</a>',
                    document,
                    re.I | re.S,
                )
            )
            if value
        )
        return (
            CommerceProductSnapshot(
                connector="ceramicolours",
                source_id=source_id,
                external_id=code or _url_id(url),
                canonical_url=_canonical(url),
                title=name,
                observed_at=observed_at,
                description=_match_text(
                    document, r'class="product-description"[^>]*>(.*?)</div>'
                )
                or None,
                vendor=self.options.brand,
                categories=categories,
                images=images,
                variants=variants,
                platform_extensions={"page_parser": parser},
            ),
        )

    def _pack_variants(
        self,
        document: str,
        url: str,
        code: str,
        temperature: str,
        observed_at: Any,
        evidence: Evidence,
    ) -> tuple[CommerceVariant, ...]:
        variants: list[CommerceVariant] = []
        for pack in self.handoff.take(url):
            amount = _price(pack.get("price"))
            pack_size = _decimal(pack.get("value"))
            if amount is None or pack_size is None or pack_size <= 0:
                continue
            identifier = _clean(pack.get("value"))
            published_pack = _clean(pack.get("pack"))
            label = (
                published_pack
                if re.search(r"\bkg\b", published_pack, re.I)
                else f"{published_pack} kg".strip()
            )
            quantity = _ceramicolours_stock(document, pack_size)
            variants.append(
                CommerceVariant(
                    external_id=identifier or f"{code}:{label}",
                    title=label,
                    canonical_url=_canonical(url),
                    offers=(
                        CommerceOffer(
                            price=Money(amount=amount, currency="EUR"),
                            observed_at=observed_at,
                            evidence=(evidence,),
                            vat_status=self.options.vat_status,
                            unit="kg",
                            pack_size=pack_size,
                        ),
                    ),
                    stock=_stock(quantity, observed_at, evidence),
                    options={"Confezione": label},
                    published_attributes={
                        **({"Temperatura": temperature} if temperature else {}),
                        "Confezione": label,
                        "Prezzo unitario": _clean(pack.get("unit_price")),
                        "price_text": _clean(pack.get("price")) or None,
                    },
                    platform_extensions={
                        "raw": {
                            "url": url,
                            "code": code,
                            "temperature": temperature,
                            "pack": cast(JsonValue, pack),
                        }
                    },
                )
            )
        return tuple(variants)


def _pack_rows(value: Any) -> list[dict[str, JsonValue]]:
    if not isinstance(value, list):
        return []
    return [
        cast(dict[str, JsonValue], row)
        for row in value[:100]
        if isinstance(row, dict)
    ]


def _canonical(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ""))


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", str(value)))).strip()


def _match_text(document: str, pattern: str) -> str:
    match = re.search(pattern, document, re.I | re.S)
    return _clean(match.group(1)) if match else ""


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


def _price(value: Any) -> Decimal | None:
    return _decimal(value)


def _ceramicolours_stock(document: str, pack_size: Decimal) -> int | None:
    match = re.search(
        r'<input[^>]+id=["\']icaOrdinabile["\'][^>]+value=["\']([0-9.,]+)["\']',
        document,
        re.I,
    )
    available = _decimal(match.group(1)) if match else None
    if available is None or pack_size <= 0:
        return None
    return int(available // pack_size)


def _stock(
    quantity: int | None, observed_at: Any, evidence: Evidence
) -> StockState:
    return StockState(
        availability=(
            Availability.OUT_OF_STOCK
            if quantity == 0
            else Availability.IN_STOCK
            if quantity is not None
            else Availability.UNKNOWN
        ),
        quantity=quantity,
        quantity_kind=(
            StockQuantityKind.EXACT
            if quantity is not None
            else StockQuantityKind.UNKNOWN
        ),
        observed_at=observed_at,
        evidence=(evidence,),
    )


def _url_id(url: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", urlparse(url).path).strip("-") or "product"
