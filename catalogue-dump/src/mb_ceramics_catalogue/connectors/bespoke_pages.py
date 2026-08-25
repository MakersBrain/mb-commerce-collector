"""Neutral connectors for three intentionally bespoke page storefronts."""

from __future__ import annotations

import hashlib
import html
import re
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Protocol, cast
from urllib.parse import parse_qs, urljoin, urlparse

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
)
from .page import canonical, clean, pdf_links
from .pagecommerce import PageTransport

BROWSER_BACKENDS = frozenset(
    {BrowserBackendName.CAMOUFOX, BrowserBackendName.CDP_EXTENSION_PROXY}
)


class InteractivePageTransport(PageTransport, Protocol):
    async def evaluate(
        self, url: str, script: str, *, wait_for: str | None = None
    ) -> JsonValue: ...


class AxnerOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    category_url: str | None = None
    category_page_limit: int = Field(default=400, ge=1)
    page_limit: int = Field(default=500, ge=1)
    brand: str | None = None
    currency: str = "USD"
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None = None
    vat_rate: Decimal | None = Field(default=None, ge=0, le=1)
    render: bool | None = None


class CeramicoloursOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    category_ids: tuple[str, ...] = ()
    category_page_limit: int = Field(default=25, ge=1)
    page_limit: int = Field(default=500, ge=1)
    brand: str | None = None
    vat_status: Literal["inclusive", "exclusive", "unknown"] = "inclusive"
    render: bool | None = None


class KeramikKraftOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    category_paths: tuple[str, ...] = ()
    category_page_limit: int = Field(default=150, ge=1)
    page_limit: int = Field(default=500, ge=1)
    brand: str | None = None
    vat_rate: Decimal | None = Field(default=None, ge=0, le=1)
    render: bool | None = None


class _ConnectorBase:
    name: str
    version = "1"
    capabilities: ConnectorCapabilities

    def _validate(
        self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None
    ) -> tuple[int, int]:
        if not self.capabilities.supports(request.requested_fields, request.refresh_mode):
            raise ValueError(f"{self.name} does not support the requested collection")
        if request.categories or request.collections:
            raise ValueError(f"{self.name} does not support server-side filters")
        if checkpoint is None:
            return 0, 0
        if (
            checkpoint.connector != self.name
            or checkpoint.connector_version != self.version
            or checkpoint.source_id != request.source_id
        ):
            raise ValueError(f"{self.name} checkpoint does not match this collection")
        cursor = checkpoint.resume_after
        if not isinstance(cursor, dict) or cursor.get("partition") != "main":
            raise ValueError(f"{self.name} checkpoint partition is invalid")
        index, sequence = cursor.get("index"), cursor.get("sequence")
        if (
            not isinstance(index, int)
            or isinstance(index, bool)
            or index < 0
            or not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence < 0
        ):
            raise ValueError(f"{self.name} checkpoint cursor is invalid")
        return index, sequence

    def _failure(
        self,
        sequence: int,
        index: int,
        code: DiagnosticCode,
        message: str,
        url: str,
        *,
        retryable: bool,
    ) -> EntityPage[CommerceProductSnapshot]:
        return EntityPage(
            page_id=_page_id(self.name, sequence, index),
            sequence=sequence,
            items=(),
            resume_after={"partition": "main", "index": index, "sequence": sequence},
            terminal=True,
            enumeration_intact=False,
            discovered=0,
            diagnostics=(
                Diagnostic(
                    code=code,
                    severity=DiagnosticSeverity.ERROR,
                    message=message,
                    retryable=retryable,
                    affects_completeness=True,
                    url=url,
                ),
            ),
        )


class AxnerConnector(_ConnectorBase):
    name = "axner"
    platform = "axner"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.UNKNOWN}),
        supports_documents=True,
        browser=BrowserRequirement.OPTIONAL,
        browser_backends=BROWSER_BACKENDS,
    )

    LISTING_LINK = re.compile(
        r'class="[^"]*product-list-link[^"]*"[^>]*>\s*<a[^>]+href="([^"]+)"', re.I
    )
    NEXT_PAGE = re.compile(r'href="([^"]*\?page=\d+)"', re.I)

    def __init__(
        self,
        transport: PageTransport,
        options: AxnerOptions | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
        budget: RequestBudgetProtocol | None = None,
    ) -> None:
        self.transport = transport
        self.options = options or AxnerOptions()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._budget = ConnectorBudget(budget)

    async def collect(
        self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None = None
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        start, sequence = self._validate(request, checkpoint)
        urls, error = await self._discover(request.base_url)
        if error:
            yield self._failure(
                sequence, start, error[0], error[1], error[2], retryable=error[3]
            )
            return
        async for page in self._products(request, urls, start, sequence):
            yield page

    async def _discover(
        self, base_url: str
    ) -> tuple[list[str], tuple[DiagnosticCode, str, str, bool] | None]:
        index = self.options.category_url or urljoin(base_url, "/sitemap.aspx")
        try:
            document = await _load(
                self.transport, index, self.options.render, self._budget, RequestPriority.DISCOVERY
            )
        except (httpx.HTTPError, RuntimeError) as error:
            return [], (
                _budget_code(error, DiagnosticCode.ENUMERATION_INCOMPLETE),
                f"Axner department index failed: {type(error).__name__}",
                index,
                True,
            )
        origin = urlparse(base_url).netloc
        queue = [
            canonical(urljoin(index, href))
            for href in dict.fromkeys(
                re.findall(r'href="(/[A-Za-z0-9\-]+\.aspx)"', document)
            )
            if urlparse(urljoin(index, href)).netloc == origin
        ]
        products: list[str] = []
        seen: set[str] = set()
        while queue:
            url = queue.pop(0)
            if url in seen:
                continue
            if len(seen) >= self.options.category_page_limit:
                return [], (
                    DiagnosticCode.ENUMERATION_INCOMPLETE,
                    f"Axner category page limit {self.options.category_page_limit} reached",
                    url,
                    False,
                )
            seen.add(url)
            try:
                listing = await _load(
                    self.transport, url, self.options.render, self._budget, RequestPriority.DISCOVERY
                )
            except (httpx.HTTPError, RuntimeError) as error:
                return [], (
                    _budget_code(error, DiagnosticCode.ENUMERATION_INCOMPLETE),
                    f"Axner listing failed: {type(error).__name__}",
                    url,
                    True,
                )
            for href in self.LISTING_LINK.findall(listing):
                candidate = canonical(urljoin(url, html.unescape(href)))
                if urlparse(candidate).netloc == origin and candidate not in products:
                    products.append(candidate)
            for href in self.NEXT_PAGE.findall(listing):
                page = canonical(urljoin(url, html.unescape(href)))
                if page not in seen and page not in queue:
                    queue.append(page)
        return products, None

    async def _products(
        self,
        request: CollectionRequest,
        urls: list[str],
        start: int,
        sequence: int,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        stop = min(len(urls), self.options.page_limit)
        result_stop = None
        if request.result_limit is not None:
            result_stop = start + request.result_limit
            stop = min(stop, result_stop)
        if start == stop:
            yield _success(self.name, sequence, start, (), True)
            return
        for index in range(start, stop):
            url = urls[index]
            try:
                document = await _load(
                    self.transport, url, self.options.render, self._budget, RequestPriority.IDENTITY
                )
            except (httpx.HTTPError, RuntimeError) as error:
                yield self._failure(
                    sequence,
                    index,
                    _budget_code(error, DiagnosticCode.ENTITY_FETCH_FAILED),
                    f"Axner product failed: {type(error).__name__}",
                    url,
                    retryable=True,
                )
                return
            snapshot = self._parse(document, url, request.source_id, self._clock())
            if snapshot is None:
                yield self._failure(
                    sequence,
                    index,
                    DiagnosticCode.PARSER_UNSUPPORTED,
                    "Axner product markup is unsupported or has no published price",
                    url,
                    retryable=False,
                )
                return
            next_index = index + 1
            limited = result_stop is not None and result_stop < len(urls) and next_index >= result_stop
            terminal = next_index == len(urls) or limited
            yield _success(
                self.name, sequence, index, (snapshot,), terminal,
                result_limit=request.result_limit if limited else None, url=url,
            )
            sequence += 1
            if terminal:
                return
        if stop < len(urls):
            yield self._failure(
                sequence,
                stop,
                DiagnosticCode.ENUMERATION_INCOMPLETE,
                f"Axner product page limit {self.options.page_limit} reached",
                request.base_url,
                retryable=False,
            )

    def _parse(
        self, document: str, url: str, source_id: str, observed_at: datetime
    ) -> CommerceProductSnapshot | None:
        name = _match_text(document, r"<h1[^>]*>(.*?)</h1>") or _meta(document, "og:title")
        price_text = _match_text(
            document,
            r'class="[^"]*product-list-cost-value[^"]*"[^>]*>\s*\$?\s*([\d,]+\.\d{2})',
        )
        amount, currency = _price(price_text)
        if not name or amount is None:
            return None
        details = {
            clean(label).rstrip(":"): clean(value)
            for label, value in re.findall(
                r'class="prod-detail-part-label"[^>]*>(.*?)</span>\s*<span[^>]*class="prod-detail-part-value"[^>]*>(.*?)</span>',
                document,
                re.I | re.S,
            )
        }
        reference = details.get("Axner Number") or None
        brand = _match_text(
            document, r'class="prod-detail-man-name-value"[^>]*>(.*?)</span>'
        ) or self.options.brand
        images = [
            urljoin(url, value)
            for value in dict.fromkeys(re.findall(r'src="(/ProductImages/[^"]+)"', document, re.I))
            if "thumb" not in value.rsplit("/", 1)[-1].lower()
        ]
        attributes = {key: value for key, value in details.items() if key != "Axner Number"}
        evidence = _evidence(url, observed_at, "axner_data_item")
        external_id = reference or _url_id(url)
        offer = _offer(
            amount,
            currency or self.options.currency,
            observed_at,
            evidence,
            self.options.vat_status,
            self.options.vat_rate,
        )
        return CommerceProductSnapshot(
            connector=self.name,
            source_id=source_id,
            external_id=external_id,
            canonical_url=canonical(url),
            title=name,
            observed_at=observed_at,
            description=_match_text(document, r'class="prod-detail-desc"[^>]*>(.*?)</div>') or None,
            vendor=brand,
            images=tuple(MediaRef(url=value) for value in images),
            documents=_documents(document, url, observed_at),
            variants=(
                CommerceVariant(
                    external_id=external_id,
                    is_default=True,
                    canonical_url=canonical(url),
                    sku=reference,
                    offers=(offer,),
                    stock=_unknown_stock(observed_at, evidence),
                    published_attributes={
                        **cast(dict[str, JsonValue], attributes),
                        "price_text": f"{price_text} {currency or self.options.currency}".strip(),
                    },
                ),
            ),
            platform_extensions={
                "raw": {
                    "details": cast(JsonValue, details),
                    "options_available": bool(re.search(r"options[- ]available", document, re.I)),
                }
            },
        )


class CeramicoloursConnector(_ConnectorBase):
    name = "ceramicolours"
    platform = "ceramicolours"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.EXACT, StockQuantityKind.UNKNOWN}),
        browser=BrowserRequirement.OPTIONAL,
        browser_backends=BROWSER_BACKENDS,
    )
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

    def __init__(
        self,
        transport: InteractivePageTransport,
        options: CeramicoloursOptions | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
        budget: RequestBudgetProtocol | None = None,
    ) -> None:
        self.transport = transport
        self.options = options or CeramicoloursOptions()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._budget = ConnectorBudget(budget)

    async def collect(
        self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None = None
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        start, sequence = self._validate(request, checkpoint)
        urls, error = await self._discover(request.base_url)
        if error:
            yield self._failure(
                sequence, start, error[0], error[1], error[2], retryable=error[3]
            )
            return
        stop = min(len(urls), self.options.page_limit)
        result_stop = None
        if request.result_limit is not None:
            result_stop = start + request.result_limit
            stop = min(stop, result_stop)
        if start == stop:
            yield _success(self.name, sequence, start, (), True)
            return
        for index in range(start, stop):
            url = urls[index]
            try:
                document = await _load(
                    self.transport, url, self.options.render, self._budget, RequestPriority.IDENTITY
                )
            except (httpx.HTTPError, RuntimeError) as fetch_error:
                yield self._failure(
                    sequence,
                    index,
                    _budget_code(fetch_error, DiagnosticCode.ENTITY_FETCH_FAILED),
                    f"Ceramicolours product failed: {type(fetch_error).__name__}",
                    url,
                    retryable=True,
                )
                return
            packs: list[dict[str, Any]] = []
            if "product-pack-field" in document:
                try:
                    detail_priority = self._budget.required_detail_priority(
                        request.requested_fields, frozenset({SnapshotField.OFFERS})
                    )
                    if detail_priority == RequestPriority.DATASET_REQUIRED:
                        self._budget.require(detail_priority, url, browser=True)
                    elif not self._budget.optional(detail_priority, browser=True):
                        raise RuntimeError("optional pack-price evaluation deferred")
                    value = await self.transport.evaluate(
                        url, self.PACK_PRICE_SCRIPT, wait_for="#product-pack-field"
                    )
                    if isinstance(value, list):
                        packs = [item for item in value if isinstance(item, dict)]
                except BudgetExhausted as error:
                    yield self._failure(
                        sequence,
                        index,
                        DiagnosticCode.REQUEST_BUDGET_EXHAUSTED,
                        f"Ceramicolours required offer detail deferred: {type(error).__name__}",
                        url,
                        retryable=True,
                    )
                    return
                except RuntimeError:
                    packs = []
            snapshot = self._parse(document, url, packs, request.source_id, self._clock())
            if snapshot is None:
                yield self._failure(
                    sequence,
                    index,
                    DiagnosticCode.PARSER_UNSUPPORTED,
                    "Ceramicolours page has no usable published price",
                    url,
                    retryable=False,
                )
                return
            next_index = index + 1
            limited = result_stop is not None and result_stop < len(urls) and next_index >= result_stop
            terminal = next_index == len(urls) or limited
            yield _success(
                self.name, sequence, index, (snapshot,), terminal,
                result_limit=request.result_limit if limited else None, url=url,
            )
            sequence += 1
            if terminal:
                return
        if stop < len(urls):
            yield self._failure(
                sequence,
                stop,
                DiagnosticCode.ENUMERATION_INCOMPLETE,
                f"Ceramicolours product page limit {self.options.page_limit} reached",
                request.base_url,
                retryable=False,
            )

    async def _discover(
        self, base_url: str
    ) -> tuple[list[str], tuple[DiagnosticCode, str, str, bool] | None]:
        try:
            home = await _load(
                self.transport, base_url, self.options.render, self._budget, RequestPriority.DISCOVERY
            )
        except (httpx.HTTPError, RuntimeError) as error:
            return [], (
                _budget_code(error, DiagnosticCode.ENUMERATION_INCOMPLETE),
                f"Ceramicolours category index failed: {type(error).__name__}",
                base_url,
                True,
            )
        wanted = set(self.options.category_ids)
        categories = []
        for href in re.findall(r'href="(Articoli\.php\?[^"]+)"', home, re.I):
            url = urljoin(base_url, html.unescape(href))
            identifier = parse_qs(urlparse(url).query).get("Id", [""])[0]
            if not wanted or identifier in wanted:
                categories.append(re.sub(r"&page=\d+", "", url))
        products: list[str] = []
        for category in dict.fromkeys(categories):
            exhausted = False
            for page in range(1, self.options.category_page_limit + 1):
                url = f"{category}&page={page}"
                try:
                    document = await _load(
                        self.transport, url, self.options.render,
                        self._budget, RequestPriority.DISCOVERY,
                    )
                except (httpx.HTTPError, RuntimeError) as error:
                    return [], (
                        _budget_code(error, DiagnosticCode.ENUMERATION_INCOMPLETE),
                        f"Ceramicolours listing failed: {type(error).__name__}",
                        url,
                        True,
                    )
                found = [
                    canonical(urljoin(base_url, html.unescape(href)))
                    for href in re.findall(
                        r'<a href="(Articolo\.php\?[^"]+)"[^>]*class="product-name">',
                        document,
                        re.I,
                    )
                ]
                products.extend(value for value in found if value not in products)
                if not found:
                    exhausted = True
                    break
            if not exhausted:
                return [], (
                    DiagnosticCode.ENUMERATION_INCOMPLETE,
                    f"Ceramicolours category page limit {self.options.category_page_limit} reached",
                    category,
                    False,
                )
        return products, None

    def _parse(
        self,
        document: str,
        url: str,
        packs: list[dict[str, Any]],
        source_id: str,
        observed_at: datetime,
    ) -> CommerceProductSnapshot | None:
        name = _match_text(document, r"<h1[^>]*>(.*?)</h1>") or _match_text(
            document, r'class="product-name"[^>]*>(.*?)</a>'
        )
        if not name:
            return None
        code = clean(parse_qs(urlparse(url).query).get("cod", [""])[0])
        temperature = _match_text(document, r"Temp\.\s*</span>\s*(.*?)</p>")
        images = list(
            dict.fromkeys(
                urljoin(url, html.unescape(value))
                for value in re.findall(
                    r'<img[^>]+src="([^"]*upload-immagini[^"]*)"', document, re.I
                )
            )
        )
        categories = [
            clean(value)
            for value in re.findall(
                r'<li[^>]*class="breadcrumb[^"]*"[^>]*>.*?<a[^>]*>(.*?)</a>',
                document,
                re.I | re.S,
            )
            if clean(value)
        ]
        evidence = _evidence(url, observed_at, "ceramicolours_product")
        variants: list[CommerceVariant] = []
        for pack in packs:
            amount, _ = _price(pack.get("price"))
            if amount is None:
                continue
            identifier = clean(pack.get("value"))
            label = f"{clean(pack.get('pack'))} kg".strip()
            quantity = _ceramicolours_stock(document, pack.get("value"))
            variants.append(
                CommerceVariant(
                    external_id=identifier or f"{code}:{label}",
                    title=label,
                    canonical_url=canonical(url),
                    offers=(
                        _offer(
                            amount,
                            "EUR",
                            observed_at,
                            evidence,
                            self.options.vat_status,
                            None,
                        ),
                    ),
                    stock=_stock(quantity, observed_at, evidence),
                    options={"Confezione": label},
                    published_attributes={
                        **({"Temperatura": temperature} if temperature else {}),
                        "Confezione": label,
                        "Prezzo unitario": clean(pack.get("unit_price")),
                        "price_text": clean(pack.get("price")) or None,
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
        if not variants:
            price_text = _match_text(document, r"Prezzo:\s*</span>\s*(.*?)</p>")
            amount, _ = _price(price_text)
            if amount is None:
                return None
            variants.append(
                CommerceVariant(
                    external_id=code or _url_id(url),
                    is_default=True,
                    canonical_url=canonical(url),
                    offers=(
                        _offer(
                            amount,
                            "EUR",
                            observed_at,
                            evidence,
                            self.options.vat_status,
                            None,
                        ),
                    ),
                    stock=_unknown_stock(observed_at, evidence),
                    published_attributes={
                        **({"Temperatura": temperature} if temperature else {}),
                        "price_text": price_text or None,
                    },
                    platform_extensions={
                        "raw": {"url": url, "code": code, "temperature": temperature}
                    },
                )
            )
        return CommerceProductSnapshot(
            connector=self.name,
            source_id=source_id,
            external_id=code or _url_id(url),
            canonical_url=canonical(url),
            title=name,
            observed_at=observed_at,
            description=_match_text(
                document, r'class="product-description"[^>]*>(.*?)</div>'
            )
            or None,
            vendor=self.options.brand,
            categories=tuple(CategoryRef(name=value) for value in categories),
            images=tuple(MediaRef(url=value) for value in images),
            variants=tuple(variants),
            platform_extensions={
                "page_parser": "browser" if packs else "dom",
            },
        )


class KeramikKraftConnector(_ConnectorBase):
    name = "keramik_kraft"
    platform = "keramik_kraft"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.UNKNOWN}),
        browser=BrowserRequirement.OPTIONAL,
        browser_backends=BROWSER_BACKENDS,
    )
    CARD = re.compile(r'<div[^>]+class="product\b[^"]*"[^>]*>(.*?)<!--\s*/product', re.I | re.S)

    def __init__(
        self,
        transport: PageTransport,
        options: KeramikKraftOptions | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
        budget: RequestBudgetProtocol | None = None,
    ) -> None:
        self.transport = transport
        self.options = options or KeramikKraftOptions()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._budget = ConnectorBudget(budget)

    async def collect(
        self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None = None
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        start, sequence = self._validate(request, checkpoint)
        card_offset = 0
        if checkpoint is not None and isinstance(checkpoint.resume_after, dict):
            raw_offset = checkpoint.resume_after.get("card_offset", 0)
            if not isinstance(raw_offset, int) or isinstance(raw_offset, bool) or raw_offset < 0:
                raise ValueError("keramik_kraft checkpoint card offset is invalid")
            card_offset = raw_offset
        roots = [urljoin(request.base_url, value) for value in self.options.category_paths] or [
            request.base_url
        ]
        pages, error = await self._listing_pages(roots, request.base_url)
        if error:
            yield self._failure(
                sequence, start, error[0], error[1], error[2], retryable=error[3]
            )
            return
        stop = min(len(pages), self.options.page_limit)
        emitted = 0
        for index in range(start, stop):
            url, document = pages[index]
            snapshots = self._cards(document, url, request.source_id, self._clock())
            offset = card_offset if index == start else 0
            available = snapshots[offset:]
            remaining = None if request.result_limit is None else request.result_limit - emitted
            selected = available if remaining is None else available[: max(remaining, 0)]
            emitted += len(selected)
            limited = (
                request.result_limit is not None
                and emitted >= request.result_limit
                and (len(selected) < len(available) or index + 1 < len(pages))
            )
            terminal = index + 1 == len(pages) or limited
            yield _success(
                self.name, sequence, index, tuple(selected), terminal, len(snapshots),
                result_limit=request.result_limit if limited else None, url=url,
                resume_index=index if limited and len(selected) < len(available) else index + 1,
                resume_offset=(offset + len(selected)) if limited and len(selected) < len(available) else 0,
            )
            sequence += 1
            if terminal:
                return
        if stop < len(pages):
            yield self._failure(
                sequence,
                stop,
                DiagnosticCode.ENUMERATION_INCOMPLETE,
                f"Keramik-Kraft listing page limit {self.options.page_limit} reached",
                request.base_url,
                retryable=False,
            )

    async def _listing_pages(
        self, roots: list[str], base_url: str
    ) -> tuple[
        list[tuple[str, str]], tuple[DiagnosticCode, str, str, bool] | None
    ]:
        queue = list(roots)
        seen: set[str] = set()
        pages: list[tuple[str, str]] = []
        origin = urlparse(base_url).netloc
        while queue:
            url = queue.pop(0)
            if url in seen:
                continue
            if len(seen) >= self.options.category_page_limit:
                return [], (
                    DiagnosticCode.ENUMERATION_INCOMPLETE,
                    f"Keramik-Kraft category page limit {self.options.category_page_limit} reached",
                    url,
                    False,
                )
            seen.add(url)
            try:
                document = await _load(
                    self.transport, url, self.options.render, self._budget, RequestPriority.DISCOVERY
                )
            except (httpx.HTTPError, RuntimeError) as error:
                return [], (
                    _budget_code(error, DiagnosticCode.ENUMERATION_INCOMPLETE),
                    f"Keramik-Kraft listing failed: {type(error).__name__}",
                    url,
                    True,
                )
            pages.append((url, document))
            for href in re.findall(r'href="([^"]+)"', document):
                candidate = canonical(urljoin(url, html.unescape(href)))
                if (
                    urlparse(candidate).netloc == origin
                    and _kraft_category(candidate)
                    and candidate not in seen
                    and candidate not in queue
                ):
                    queue.append(candidate)
        return pages, None

    def _cards(
        self, document: str, page_url: str, source_id: str, observed_at: datetime
    ) -> list[CommerceProductSnapshot]:
        categories = _kraft_breadcrumb(page_url)
        snapshots: list[CommerceProductSnapshot] = []
        for match in self.CARD.finditer(document):
            card = match.group(1)
            name_markup = re.search(r'<p[^>]+class="text-sm[^"]*"[^>]*>(.*?)</p>', card, re.I | re.S)
            price_match = re.search(
                r'([0-9]{1,3}(?:[.\s][0-9]{3})*,[0-9]{2})\s*(?:&euro;|€)'
                r'(?:\s*<i[^>]*>\s*\(?\s*([0-9]{1,3}(?:[.\s][0-9]{3})*,[0-9]{2})\s*(?:&euro;|€)\s*HT)?',
                card,
                re.I,
            )
            if not name_markup or not price_match:
                continue
            parts = [clean(value).lstrip("=>").strip() for value in re.split(r"<br\s*/?>", name_markup.group(1))]
            parts = [value for value in parts if value]
            if not parts:
                continue
            name, variant_title = parts[0], " ".join(parts[1:])
            gross, _ = _price(price_match.group(1))
            net, _ = _price(price_match.group(2)) if price_match.group(2) else (None, None)
            if gross is None:
                continue
            code = _match_text(card, r'<p[^>]+class="p mb-1"[^>]*>(.*?)</p>')
            link = re.search(r'href="([^"]*_[A-Za-z0-9.\-]+\.html[^"]*)"', card, re.I)
            product_url = canonical(urljoin(page_url, html.unescape(link.group(1)))) if link else page_url
            variant_title = variant_title or _kraft_variant(product_url)
            image = re.search(r'<img[^>]+src="([^"]+)"', card, re.I)
            image_url = urljoin(page_url, html.unescape(image.group(1))) if image else None
            brand = _kraft_brand(name) or self.options.brand
            external_id = code or _url_id(product_url)
            evidence = _evidence(page_url, observed_at, "keramik_kraft_listing_card")
            attributes: dict[str, JsonValue] = {
                "price_text": clean(price_match.group(0)) or None,
            }
            if net is not None:
                attributes["Netto-Preis EUR"] = float(net)
            snapshots.append(
                CommerceProductSnapshot(
                    connector=self.name,
                    source_id=source_id,
                    external_id=external_id,
                    canonical_url=product_url,
                    title=name,
                    observed_at=observed_at,
                    vendor=brand,
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
                                _offer(
                                    gross,
                                    "EUR",
                                    observed_at,
                                    evidence,
                                    "inclusive",
                                    self.options.vat_rate,
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
                            "page": page_url,
                            "code": code,
                            "gross": float(gross),
                            "net": float(net) if net is not None else None,
                            "variant": variant_title,
                        }
                    },
                )
            )
        return snapshots


def _success(
    name: str,
    sequence: int,
    index: int,
    items: tuple[CommerceProductSnapshot, ...],
    terminal: bool,
    discovered: int | None = None,
    *,
    result_limit: int | None = None,
    url: str | None = None,
    resume_index: int | None = None,
    resume_offset: int = 0,
) -> EntityPage[CommerceProductSnapshot]:
    limited = result_limit is not None
    return EntityPage(
        page_id=_page_id(name, sequence, index),
        sequence=sequence,
        items=items,
        resume_after=(
            None
            if terminal and not limited
            else {
                "partition": "main",
                "index": index + 1 if resume_index is None else resume_index,
                "sequence": sequence + 1,
                **({"card_offset": resume_offset} if resume_offset else {}),
            }
        ),
        terminal=terminal,
        enumeration_intact=not limited,
        discovered=discovered if discovered is not None else len(items),
        diagnostics=(
            (result_limit_diagnostic(result_limit, url or "unknown"),)
            if result_limit is not None
            else ()
        ),
    )


async def _load(
    transport: PageTransport,
    url: str,
    render: bool | None,
    budget: ConnectorBudget,
    priority: RequestPriority,
) -> str:
    try:
        budget.require(priority, url, browser=render is True)
        return await transport.document(url, rendered=render is True)
    except BudgetExhausted:
        raise
    except (httpx.HTTPError, RuntimeError):
        if render is None:
            budget.require(priority, url, browser=True)
            return await transport.document(url, rendered=True)
        raise


def _budget_code(error: Exception, fallback: DiagnosticCode) -> DiagnosticCode:
    return (
        DiagnosticCode.REQUEST_BUDGET_EXHAUSTED
        if isinstance(error, BudgetExhausted)
        else fallback
    )


def _offer(
    amount: Decimal,
    currency: str,
    observed_at: datetime,
    evidence: Evidence,
    vat_status: str | None,
    vat_rate: Decimal | None,
) -> CommerceOffer:
    return CommerceOffer(
        price=Money(amount=amount, currency=currency),
        observed_at=observed_at,
        evidence=(evidence,),
        vat_status=cast(Any, vat_status or "unknown"),
        vat_rate=vat_rate,
    )


def _stock(quantity: int | None, observed_at: datetime, evidence: Evidence) -> StockState:
    return StockState(
        availability=(
            Availability.OUT_OF_STOCK if quantity == 0 else Availability.IN_STOCK
        )
        if quantity is not None
        else Availability.UNKNOWN,
        quantity=quantity,
        quantity_kind=StockQuantityKind.EXACT if quantity is not None else StockQuantityKind.UNKNOWN,
        observed_at=observed_at,
        evidence=(evidence,),
    )


def _unknown_stock(observed_at: datetime, evidence: Evidence) -> StockState:
    return StockState(
        availability=Availability.UNKNOWN,
        observed_at=observed_at,
        evidence=(evidence,),
    )


def _documents(document: str, url: str, observed_at: datetime) -> tuple[DocumentRef, ...]:
    return tuple(
        DocumentRef(
            url=document_url,
            title=label or None,
            media_type="application/pdf",
            observed_at=observed_at,
            evidence=(_evidence(url, observed_at, "a[href*=.pdf]"),),
        )
        for document_url, label in pdf_links(document, url)
    )


def _evidence(url: str, observed_at: datetime, field: str) -> Evidence:
    return Evidence(
        method="html",
        source_url=url,
        source_field=field,
        observed_at=observed_at,
        confidence="published",
    )


def _price(value: Any) -> tuple[Decimal | None, str | None]:
    if value is None or isinstance(value, bool):
        return None, None
    text = clean(value)
    currency = next(
        (mapped for symbol, mapped in (("€", "EUR"), ("$", "USD"), ("£", "GBP")) if symbol in text),
        None,
    )
    number = re.sub(r"[^0-9.,-]", "", text)
    number = re.sub(r"[.,](?=\d{3}(?:\D|$))", "", number).replace(",", ".")
    try:
        result = Decimal(number)
    except InvalidOperation:
        return None, currency
    return (result, currency) if result.is_finite() and result >= 0 else (None, currency)


def _match_text(document: str, pattern: str) -> str:
    match = re.search(pattern, document, re.I | re.S)
    return clean(match.group(1)) if match else ""


def _meta(document: str, key: str) -> str:
    match = re.search(
        rf'<meta[^>]+(?:property|name)=["\']{re.escape(key)}["\'][^>]+content=["\']([^"\']*)',
        document,
        re.I,
    )
    return html.unescape(match.group(1)).strip() if match else ""


def _ceramicolours_stock(document: str, pack: Any) -> int | None:
    match = re.search(
        r'<input[^>]+id=["\']icaOrdinabile["\'][^>]+value=["\']([0-9.,]+)["\']',
        document,
        re.I,
    )
    try:
        available = float(match.group(1).replace(",", ".")) if match else None
        pack_size = float(str(pack).replace(",", "."))
    except (TypeError, ValueError):
        return None
    if available is None or available < 0 or pack_size <= 0:
        return None
    return int((available + 1e-9) // pack_size)


def _kraft_category(url: str) -> bool:
    path = urlparse(url).path
    if not path.endswith(".html") or re.search(r"_[A-Za-z0-9.\-]+\.html", url):
        return False
    if re.search(r"/(?:error_page|menu|index\d*)\.html$", path, re.I):
        return False
    return bool(re.match(r"^/[a-z]{2}/", path))


def _kraft_variant(product_url: str) -> str:
    stem = urlparse(product_url).path.rsplit("/", 1)[-1].removesuffix(".html")
    parts = stem.split("_")
    if len(parts) < 3:
        return ""
    return " ".join(part for part in "_".join(parts[1:-1]).split("-") if part).strip()


def _kraft_breadcrumb(page_url: str) -> list[str]:
    parts = urlparse(page_url).path.rsplit("/", 1)[0].strip("/").split("/")
    return [part.replace("--", " - ").replace("-", " ") for part in parts[1:] if part]


def _kraft_brand(name: str) -> str | None:
    for maker in ("Botz", "Mayco", "Duncan", "Amaco", "Terracolor", "Ceraline", "Wolbring"):
        if re.search(rf"\b{maker}\b", name, re.I):
            return maker
    return None


def _url_id(url: str) -> str:
    return hashlib.sha256(canonical(url).encode()).hexdigest()[:24]


def _page_id(name: str, sequence: int, index: int) -> str:
    digest = hashlib.sha256(f"{name}:{sequence}:{index}".encode()).hexdigest()[:16]
    return f"{name}-{sequence}-{digest}"
