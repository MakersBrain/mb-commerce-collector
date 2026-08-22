"""Generic page-commerce connector composed from discovery and parser strategies."""

from __future__ import annotations

import hashlib
import re
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Literal, Protocol, cast
from urllib.parse import urljoin, urlparse

import httpx
from mb_commerce_scraper.models import (
    Availability,
    CategoryRef,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    DocumentRef,
    Evidence,
    MediaRef,
    Money,
    StockQuantityKind,
    StockState,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from .base import (
    BrowserBackendName,
    BrowserRequirement,
    CollectionRequest,
    CommerceConnector,
    ConnectorCapabilities,
    ConnectorCheckpoint,
    Diagnostic,
    DiagnosticCode,
    DiagnosticSeverity,
    EntityPage,
    RefreshMode,
    SnapshotField,
    result_limit_diagnostic,
)
from .budget import (
    BudgetExhausted,
    ConnectorBudget,
    RequestBudgetProtocol,
    RequestPriority,
    budget_diagnostic,
)
from .page import (
    VerifiedDomRules,
    brand,
    breadcrumbs,
    canonical,
    clean,
    gtin,
    images,
    jsonld_products,
    links,
    meta,
    microdata_products,
    offer,
    pdf_links,
    probable_javascript_shell,
    select,
    sitemap_locations,
    specification_table,
)


class PageTransport(Protocol):
    async def advertised_sitemaps(self, base_url: str) -> tuple[str, ...]: ...

    async def document(
        self,
        url: str,
        *,
        rendered: bool = False,
        accept: str | None = None,
    ) -> str: ...


class ParserDisposition(StrEnum):
    PARSED = "parsed"
    UNSUPPORTED = "unsupported"
    BROWSER_REQUIRED = "browser_required"


class PageParseOutcome(BaseModel):
    """Typed parser decision; parser-empty and browser-needed are deliberately distinct."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    disposition: ParserDisposition
    snapshots: tuple[CommerceProductSnapshot, ...] = ()
    parsers_tried: tuple[str, ...]
    reason: str | None = None


class PageCrawlOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    sitemaps: tuple[str, ...] = ()
    use_advertised_sitemaps: bool = True
    category_urls: tuple[str, ...] = ()
    product_pattern: str | None = None
    pagination_patterns: tuple[str, ...] = ()
    card_links_only: bool = False
    page_limit: int = Field(default=500, ge=1)
    sitemap_limit: int = Field(default=100, ge=1)
    category_page_limit: int = Field(default=120, ge=1)
    render: bool | None = None
    browser_zero_gain_limit: int = Field(default=10, ge=1)
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld",
        "microdata",
        "opengraph",
        "dom",
    )
    dom_rules: VerifiedDomRules | None = None
    brand: str | None = None
    currency: str | None = None
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None = None
    vat_rate: Decimal | None = Field(default=None, ge=0, le=1)
    stock_from_quantity_maximum: bool = False


class _Discovery(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    partition: Literal["sitemap", "category"]
    urls: tuple[str, ...]


class PageCommerceConnector(CommerceConnector):
    name = "pagecommerce"
    platform = "pagecrawl"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset(
            {
                StockQuantityKind.EXACT,
                StockQuantityKind.ORDER_LIMIT,
                StockQuantityKind.UNKNOWN,
            }
        ),
        supports_incremental_cursor=False,
        supports_category_filter=False,
        supports_documents=True,
        browser=BrowserRequirement.OPTIONAL,
        browser_backends=frozenset(
            {BrowserBackendName.CAMOUFOX, BrowserBackendName.CDP_EXTENSION_PROXY}
        ),
    )

    def __init__(
        self,
        transport: PageTransport,
        options: PageCrawlOptions | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
        budget: RequestBudgetProtocol | None = None,
    ) -> None:
        self.transport = transport
        self.options = options or PageCrawlOptions()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._budget = ConnectorBudget(budget)
        self._product_pattern = (
            re.compile(self.options.product_pattern) if self.options.product_pattern else None
        )
        self._pagination = tuple(re.compile(value) for value in self.options.pagination_patterns)

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        self._validate_request(request, checkpoint)
        try:
            discovery, diagnostic = await self._discover(request.base_url)
        except BudgetExhausted as error:
            yield self._failure_page(
                "discovery", 0, 0, budget_diagnostic(error.priority, error.url)
            )
            return
        if diagnostic is not None:
            yield self._failure_page("sitemap", 0, 0, diagnostic)
            return
        if discovery is None or not discovery.urls:
            yield self._failure_page(
                "category",
                0,
                0,
                self._diagnostic(
                    DiagnosticCode.ENUMERATION_INCOMPLETE,
                    "page discovery yielded no product URLs",
                    request.base_url,
                    retryable=True,
                ),
            )
            return

        index, sequence = self._resume(checkpoint, discovery.partition)
        if index > len(discovery.urls):
            raise ValueError("PageCommerce checkpoint index is beyond discovered products")
        stop = min(len(discovery.urls), self.options.page_limit)
        result_stop = None
        if request.result_limit is not None:
            result_stop = index + request.result_limit
            stop = min(stop, result_stop)
        if index == stop:
            yield EntityPage(
                page_id=_page_id(discovery.partition, sequence, index),
                partition_key=discovery.partition,
                sequence=sequence,
                items=(),
                terminal=True,
                enumeration_intact=True,
                discovered=0,
            )
            return

        zero_gain = 0
        for current in range(index, stop):
            if request.cancelled():
                return
            url = discovery.urls[current]
            loaded_rendered = self.options.render is True
            try:
                self._budget.require(
                    RequestPriority.IDENTITY,
                    url,
                    browser=self.options.render is True,
                )
                document = await self.transport.document(
                    url, rendered=self.options.render is True
                )
            except BudgetExhausted as error:
                yield self._failure_page(
                    discovery.partition,
                    sequence,
                    current,
                    budget_diagnostic(error.priority, error.url),
                )
                return
            except (httpx.HTTPError, RuntimeError) as error:
                if self.options.render is None:
                    try:
                        self._budget.require(RequestPriority.IDENTITY, url, browser=True)
                        document = await self.transport.document(url, rendered=True)
                        loaded_rendered = True
                    except BudgetExhausted as budget_error:
                        yield self._failure_page(
                            discovery.partition,
                            sequence,
                            current,
                            budget_diagnostic(budget_error.priority, budget_error.url),
                        )
                        return
                    except (httpx.HTTPError, RuntimeError) as browser_error:
                        yield self._failure_page(
                            discovery.partition,
                            sequence,
                            current,
                            self._diagnostic(
                                DiagnosticCode.ENTITY_FETCH_FAILED,
                                "product page and browser fallback failed: "
                                f"{type(error).__name__}/{type(browser_error).__name__}",
                                url,
                                retryable=True,
                            ),
                        )
                        return
                else:
                    yield self._failure_page(
                        discovery.partition,
                        sequence,
                        current,
                        self._diagnostic(
                            DiagnosticCode.ENTITY_FETCH_FAILED,
                            f"product page fetch failed: {type(error).__name__}",
                            url,
                            retryable=True,
                        ),
                    )
                    return
            outcome = self.parse(document, url, request.source_id, self._clock())
            if (
                outcome.disposition == ParserDisposition.BROWSER_REQUIRED
                and self.options.render is None
                and not loaded_rendered
                and zero_gain < self.options.browser_zero_gain_limit
            ):
                try:
                    self._budget.require(RequestPriority.IDENTITY, url, browser=True)
                    rendered = await self.transport.document(url, rendered=True)
                except BudgetExhausted as budget_error:
                    yield self._failure_page(
                        discovery.partition,
                        sequence,
                        current,
                        budget_diagnostic(budget_error.priority, budget_error.url),
                    )
                    return
                except (httpx.HTTPError, RuntimeError) as error:
                    yield self._failure_page(
                        discovery.partition,
                        sequence,
                        current,
                        self._diagnostic(
                            DiagnosticCode.ENTITY_FETCH_FAILED,
                            f"browser fallback failed: {type(error).__name__}",
                            url,
                            retryable=True,
                        ),
                    )
                    return
                outcome = self.parse(rendered, url, request.source_id, self._clock())
                zero_gain = 0 if outcome.disposition == ParserDisposition.PARSED else zero_gain + 1
            if outcome.disposition != ParserDisposition.PARSED:
                yield self._failure_page(
                    discovery.partition,
                    sequence,
                    current,
                    self._diagnostic(
                        DiagnosticCode.PARSER_UNSUPPORTED,
                        outcome.reason or "product page markup is unsupported",
                        url,
                        retryable=False,
                    ),
                )
                return

            next_index = current + 1
            limited = (
                result_stop is not None
                and result_stop < len(discovery.urls)
                and next_index >= result_stop
            )
            terminal = next_index == len(discovery.urls) or limited
            diagnostics = (
                (result_limit_diagnostic(request.result_limit, url),)
                if limited and request.result_limit is not None
                else ()
            )
            yield EntityPage(
                page_id=_page_id(discovery.partition, sequence, current),
                partition_key=discovery.partition,
                sequence=sequence,
                items=outcome.snapshots,
                resume_after=(
                    None
                    if terminal and not limited
                    else {
                        "partition": discovery.partition,
                        "index": next_index,
                        "sequence": sequence + 1,
                    }
                ),
                terminal=terminal,
                enumeration_intact=not limited,
                discovered=1,
                diagnostics=diagnostics,
            )
            sequence += 1
            if terminal:
                return

        if stop < len(discovery.urls):
            yield self._failure_page(
                discovery.partition,
                sequence,
                stop,
                self._diagnostic(
                    DiagnosticCode.ENUMERATION_INCOMPLETE,
                    f"product page limit {self.options.page_limit} reached",
                    request.base_url,
                    retryable=False,
                ),
            )

    def parse(
        self, document: str, url: str, source_id: str, observed_at: datetime
    ) -> PageParseOutcome:
        tried: list[str] = []
        for parser in self.options.parsers:
            tried.append(parser)
            raw_items: list[dict[str, Any]] = []
            method: Literal["jsonld", "html"] = "html"
            if parser == "jsonld":
                raw_items = jsonld_products(document)
                method = "jsonld"
            elif parser == "microdata":
                raw_items = microdata_products(document)
            elif parser == "opengraph":
                if raw := self._opengraph(document):
                    raw_items = [raw]
            elif parser == "dom" and self.options.dom_rules is not None:
                if raw := self._dom(document):
                    raw_items = [raw]
            snapshots = tuple(
                snapshot
                for item in raw_items
                if (
                    snapshot := self._normalize(
                        item, document, url, source_id, observed_at, method, parser
                    )
                )
                is not None
            )
            if snapshots:
                return PageParseOutcome(
                    disposition=ParserDisposition.PARSED,
                    snapshots=snapshots,
                    parsers_tried=tuple(tried),
                )
        if probable_javascript_shell(document) and self.options.render is not False:
            return PageParseOutcome(
                disposition=ParserDisposition.BROWSER_REQUIRED,
                parsers_tried=tuple(tried),
                reason="page is a probable JavaScript shell",
            )
        return PageParseOutcome(
            disposition=ParserDisposition.UNSUPPORTED,
            parsers_tried=tuple(tried),
            reason="no configured parser recognized a product",
        )

    async def _discover(
        self, base_url: str
    ) -> tuple[_Discovery | None, Diagnostic | None]:
        sitemap_roots = list(self.options.sitemaps)
        if not sitemap_roots and self.options.use_advertised_sitemaps:
            try:
                self._budget.require(RequestPriority.DISCOVERY, base_url)
                sitemap_roots.extend(await self.transport.advertised_sitemaps(base_url))
            except BudgetExhausted:
                raise
            except (httpx.HTTPError, RuntimeError):
                pass
        if sitemap_roots:
            urls, diagnostic = await self._sitemap_urls(sitemap_roots, base_url)
            if diagnostic is not None:
                return None, diagnostic
            if urls:
                return _Discovery(partition="sitemap", urls=tuple(urls)), None
            if not self.options.category_urls:
                return None, None
        if self.options.category_urls:
            urls, diagnostic = await self._category_urls(base_url)
            if diagnostic is not None:
                return None, diagnostic
            return _Discovery(partition="category", urls=tuple(urls)), None
        return None, None

    async def _sitemap_urls(
        self, roots: list[str], base_url: str
    ) -> tuple[list[str], Diagnostic | None]:
        queue = list(dict.fromkeys(roots))
        seen: set[str] = set()
        found: list[str] = []
        while queue:
            url = queue.pop(0)
            if url in seen:
                continue
            if len(seen) >= self.options.sitemap_limit:
                return [], self._diagnostic(
                    DiagnosticCode.ENUMERATION_INCOMPLETE,
                    f"sitemap traversal limit {self.options.sitemap_limit} reached",
                    url,
                    retryable=False,
                )
            seen.add(url)
            try:
                self._budget.require(RequestPriority.DISCOVERY, url)
                document = await self.transport.document(
                    url, accept="application/xml,text/xml"
                )
            except BudgetExhausted:
                raise
            except (httpx.HTTPError, RuntimeError, UnicodeError) as error:
                return [], self._diagnostic(
                    DiagnosticCode.ENUMERATION_INCOMPLETE,
                    f"sitemap fetch failed: {type(error).__name__}",
                    url,
                    retryable=True,
                )
            is_index, locations = sitemap_locations(document)
            if is_index:
                queue.extend(locations)
            else:
                found.extend(
                    canonical(value)
                    for value in locations
                    if self._is_product(value, base_url)
                )
        return list(dict.fromkeys(found)), None

    async def _category_urls(
        self, base_url: str
    ) -> tuple[list[str], Diagnostic | None]:
        queue = list(self.options.category_urls)
        seen: set[str] = set()
        products: list[str] = []
        while queue:
            url = queue.pop(0)
            if url in seen:
                continue
            if len(seen) >= self.options.category_page_limit:
                return [], self._diagnostic(
                    DiagnosticCode.ENUMERATION_INCOMPLETE,
                    f"category page limit {self.options.category_page_limit} reached",
                    url,
                    retryable=False,
                )
            seen.add(url)
            try:
                self._budget.require(RequestPriority.DISCOVERY, url)
                document = await self.transport.document(url)
            except BudgetExhausted:
                raise
            except (httpx.HTTPError, RuntimeError) as error:
                return [], self._diagnostic(
                    DiagnosticCode.ENUMERATION_INCOMPLETE,
                    f"category fetch failed: {type(error).__name__}",
                    url,
                    retryable=True,
                )
            for candidate in links(document, url, cards_only=self.options.card_links_only):
                if self._is_product(candidate, base_url):
                    if candidate not in products:
                        products.append(candidate)
                elif self._is_pagination(candidate) and candidate not in seen and candidate not in queue:
                    queue.append(candidate)
        return products, None

    def _normalize(
        self,
        item: dict[str, Any],
        document: str,
        url: str,
        source_id: str,
        observed_at: datetime,
        method: Literal["jsonld", "html"],
        parser: str,
    ) -> CommerceProductSnapshot | None:
        raw_offer = offer(item)
        if not raw_offer and any(key in item for key in ("price", "priceCurrency", "availability")):
            raw_offer = item
        title = clean(item.get("name")) or meta(document, "og:title")
        if not title:
            return None
        item_url = canonical(urljoin(url, clean(item.get("url")) or url))
        raw_price = raw_offer.get("price", raw_offer.get("lowPrice"))
        amount, parsed_currency = _price(raw_price)
        currency = (
            clean(raw_offer.get("priceCurrency"))
            or parsed_currency
            or self.options.currency
        )
        currency = currency.upper() if currency else None
        evidence = Evidence(
            method=method,
            source_url=item_url,
            source_field=parser,
            observed_at=observed_at,
            confidence="published",
        )
        availability = _availability(raw_offer.get("availability"))
        quantity, quantity_kind = self._stock(raw_offer, document)
        stock = StockState(
            availability=availability,
            quantity=quantity,
            quantity_kind=quantity_kind,
            observed_at=observed_at,
            evidence=(evidence,),
        )
        offers: tuple[CommerceOffer, ...] = ()
        if amount is not None and currency and re.fullmatch(r"[A-Z]{3}", currency):
            offers = (
                CommerceOffer(
                    price=Money(amount=amount, currency=currency),
                    observed_at=observed_at,
                    evidence=(evidence,),
                    vat_status=self.options.vat_status or "unknown",
                    vat_rate=self.options.vat_rate,
                    availability=availability,
                    availability_evidence=(evidence,),
                ),
            )
        external_id = (
            clean(item.get("productID") or item.get("sku") or item.get("mpn"))
            or hashlib.sha256(item_url.encode()).hexdigest()[:24]
        )
        page_images = images(item, item_url)
        if not page_images and (og_image := meta(document, "og:image")):
            page_images = [urljoin(item_url, og_image)]
        category_names = breadcrumbs(document) or [clean(item.get("category"))]
        attributes = specification_table(document)
        return CommerceProductSnapshot(
            connector=self.name,
            source_id=source_id,
            external_id=external_id,
            canonical_url=item_url,
            title=title,
            observed_at=observed_at,
            description=clean(item.get("description")) or meta(document, "og:description"),
            vendor=brand(item) or clean(item.get("brand")) or self.options.brand,
            categories=tuple(CategoryRef(name=value) for value in category_names if value),
            images=tuple(MediaRef(url=value) for value in page_images),
            documents=tuple(
                DocumentRef(
                    url=document_url,
                    title=label or None,
                    media_type="application/pdf",
                    observed_at=observed_at,
                    evidence=(
                        Evidence(
                            method="html",
                            source_url=item_url,
                            source_field="a[href*=.pdf]",
                            observed_at=observed_at,
                            confidence="published",
                        ),
                    ),
                )
                for document_url, label in pdf_links(document, item_url)
            ),
            variants=(
                CommerceVariant(
                    external_id=external_id,
                    is_default=True,
                    canonical_url=item_url,
                    sku=clean(item.get("sku") or item.get("mpn")) or None,
                    gtin=gtin(item),
                    offers=offers,
                    stock=stock,
                    published_attributes={
                        **cast(dict[str, JsonValue], attributes),
                        "price_text": f"{raw_price} {currency or ''}".strip() or None,
                    },
                    platform_extensions={"legacy_raw_variant": None},
                ),
            ),
            platform_extensions={
                "raw": cast(JsonValue, item),
                "page_parser": parser,
            },
        )

    def _opengraph(self, document: str) -> dict[str, Any] | None:
        title = meta(document, "og:title")
        price = meta(document, "product:price:amount")
        if not title or price is None:
            return None
        return {
            "name": title,
            "description": meta(document, "og:description"),
            "brand": meta(document, "og:brand"),
            "sku": meta(document, "og:upc"),
            "image": meta(document, "og:image"),
            "offers": {
                "price": price,
                "priceCurrency": meta(document, "product:price:currency"),
                "availability": meta(document, "og:availability"),
            },
            "_opengraph": True,
        }

    def _dom(self, document: str) -> dict[str, Any] | None:
        rules = self.options.dom_rules
        if rules is None or not all(select(document, rule) for rule in rules.verification):
            return None
        name = select(document, rules.name)
        if not name:
            return None
        return {
            "name": name,
            "description": select(document, rules.description) if rules.description else None,
            "sku": select(document, rules.sku) if rules.sku else None,
            "image": select(document, rules.image) if rules.image else None,
            "offers": {
                "price": select(document, rules.price) if rules.price else None,
                "priceCurrency": (
                    select(document, rules.currency) if rules.currency else self.options.currency
                ),
                "availability": (
                    select(document, rules.availability) if rules.availability else None
                ),
            },
            "_verified_dom": True,
        }

    def _stock(
        self, raw_offer: dict[str, Any], document: str
    ) -> tuple[int | None, StockQuantityKind]:
        level = raw_offer.get("inventoryLevel")
        value = level.get("value") if isinstance(level, dict) else level
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value, StockQuantityKind.EXACT
        if self.options.stock_from_quantity_maximum:
            for tag in re.findall(r"<input\b[^>]*>", document, re.IGNORECASE | re.DOTALL):
                if not re.search(
                    r'\bname=["\'](?:quantity|qty|Quantity)["\']', tag, re.IGNORECASE
                ):
                    continue
                match = re.search(r'\bmax=["\'](\d+)["\']', tag, re.IGNORECASE)
                if match and int(match.group(1)) < 9999:
                    return int(match.group(1)), StockQuantityKind.ORDER_LIMIT
        return None, StockQuantityKind.UNKNOWN

    def _is_product(self, url: str, base_url: str) -> bool:
        if urlparse(url).netloc != urlparse(base_url).netloc:
            return False
        if self._product_pattern is None:
            return True
        return bool(self._product_pattern.search(urlparse(url).path) or self._product_pattern.search(url))

    def _is_pagination(self, url: str) -> bool:
        if self._pagination:
            return any(pattern.search(url) for pattern in self._pagination)
        return bool(re.search(r"[?&](?:p|page|start)=\d+|/page/\d+", url))

    def _validate_request(
        self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None
    ) -> None:
        if not self.capabilities.supports(request.requested_fields, request.refresh_mode):
            raise ValueError("PageCommerce connector does not support the requested collection")
        if request.categories or request.collections:
            raise ValueError("PageCommerce connector does not support server-side filters")
        if checkpoint is not None and (
            checkpoint.connector != self.name
            or checkpoint.connector_version != self.version
            or checkpoint.source_id != request.source_id
        ):
            raise ValueError("PageCommerce checkpoint does not match this collection")

    @staticmethod
    def _resume(
        checkpoint: ConnectorCheckpoint | None, partition: str
    ) -> tuple[int, int]:
        if checkpoint is None:
            return 0, 0
        cursor = checkpoint.resume_after
        if not isinstance(cursor, dict) or cursor.get("partition") != partition:
            raise ValueError("PageCommerce checkpoint partition is not the declared discovery partition")
        index, sequence = cursor.get("index"), cursor.get("sequence")
        if (
            not isinstance(index, int)
            or isinstance(index, bool)
            or index < 0
            or not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence < 0
        ):
            raise ValueError("PageCommerce checkpoint cursor is invalid")
        return index, sequence

    @staticmethod
    def _diagnostic(
        code: DiagnosticCode, message: str, url: str, *, retryable: bool
    ) -> Diagnostic:
        return Diagnostic(
            code=code,
            severity=DiagnosticSeverity.ERROR,
            message=message,
            retryable=retryable,
            affects_completeness=True,
            url=url,
        )

    @staticmethod
    def _failure_page(
        partition: str,
        sequence: int,
        index: int,
        diagnostic: Diagnostic,
    ) -> EntityPage[CommerceProductSnapshot]:
        return EntityPage(
            page_id=_page_id(partition, sequence, index),
            partition_key=partition,
            sequence=sequence,
            items=(),
            resume_after={"partition": partition, "index": index, "sequence": sequence},
            terminal=True,
            enumeration_intact=False,
            discovered=0,
            diagnostics=(diagnostic,),
        )


def _availability(value: Any) -> Availability:
    text = clean(value).rsplit("/", 1)[-1].casefold().replace(" ", "")
    return {
        "instock": Availability.IN_STOCK,
        "limitedavailability": Availability.IN_STOCK,
        "outofstock": Availability.OUT_OF_STOCK,
        "soldout": Availability.OUT_OF_STOCK,
        "preorder": Availability.PREORDER,
        "backorder": Availability.BACKORDER,
    }.get(text, Availability.UNKNOWN)


def _price(value: Any) -> tuple[Decimal | None, str | None]:
    if isinstance(value, bool) or value is None:
        return None, None
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value)), None
        except InvalidOperation:
            return None, None
    text = clean(value)
    code = re.search(r"\b(EUR|USD|GBP|CHF|SEK|NOK|DKK|PLN|CZK|HUF|BGN|RON)\b", text, re.I)
    currency = code.group(1).upper() if code else None
    if currency is None:
        for symbol, mapped in {"€": "EUR", "$": "USD", "£": "GBP", "zł": "PLN"}.items():
            if symbol in text:
                currency = mapped
                break
    number = re.sub(r"[^0-9.,-]", "", text)
    number = re.sub(r"[.,](?=\d{3}(?:\D|$))", "", number).replace(",", ".")
    try:
        amount = Decimal(number)
    except InvalidOperation:
        return None, currency
    return (amount, currency) if amount.is_finite() and amount >= 0 else (None, currency)


def _page_id(partition: str, sequence: int, index: int) -> str:
    digest = hashlib.sha256(f"pagecommerce:{partition}:{sequence}:{index}".encode()).hexdigest()[:16]
    return f"pagecommerce-{partition}-{sequence}-{digest}"
