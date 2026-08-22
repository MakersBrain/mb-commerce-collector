"""Wix Stores sitemap and embedded warmup-data connector."""

from __future__ import annotations

import hashlib
import html
import json
import re
from collections.abc import AsyncIterator
from contextlib import suppress
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, cast
from urllib.parse import unquote, urljoin, urlparse, urlunparse

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from mb_commerce_scraper.models import (
    Availability,
    CategoryRef,
    CollectionRequest,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    ConnectorCheckpoint,
    Diagnostic,
    DiagnosticCode,
    DiagnosticSeverity,
    DocumentRef,
    EntityPage,
    Evidence,
    MediaRef,
    Money,
    RefreshMode,
    SnapshotField,
    StockQuantityKind,
    StockState,
    collection_fingerprint,
    result_limit_diagnostic,
    validate_checkpoint,
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

from .base import BrowserRequirement, CommerceConnector, ConnectorCapabilities, ConnectorContext


class HTTPStatusFailure(RuntimeError):
    """An unsuccessful Wix document response."""

    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"Wix request to {url} failed with status {status}")
        self.status = status
        self.url = url


class SitemapTraversalFailure(RuntimeError):
    pass


class _WixTransport:
    """Project the public transport contract into Wix document operations."""

    def __init__(self, transport: CommerceTransport) -> None:
        self.transport = transport

    async def advertised_sitemaps(self, base_url: str) -> tuple[str, ...]:
        robots_url = f"{_origin(base_url)}/robots.txt"
        response = await self.transport.request(
            TransportRequest(
                url=robots_url,
                purpose=RequestPurpose.ROBOTS,
                priority=RequestPriority.DISCOVERY,
                required=False,
                estimated_bytes=100_000,
            )
        )
        if response.status >= 400:
            return ()
        return tuple(
            dict.fromkeys(
                match.strip()
                for match in re.findall(
                    r"^\s*Sitemap\s*:\s*(\S+)\s*$", response.text(), re.IGNORECASE | re.MULTILINE
                )
                if match.strip()
            )
        )

    async def document(self, url: str, *, rendered: bool = False, accept: str | None = None) -> str:
        response = await self.transport.request(
            TransportRequest(
                url=url,
                headers={"Accept": accept} if accept else {},
                purpose=(RequestPurpose.DISCOVERY if accept else RequestPurpose.ENTITY),
                priority=(RequestPriority.DISCOVERY if accept else RequestPriority.IDENTITY),
                estimated_bytes=2_000_000,
                browser=BrowserHint.REQUIRED if rendered else BrowserHint.NEVER,
            )
        )
        if response.status >= 400:
            raise HTTPStatusFailure(response.status, url)
        return response.text()


class WixOptions(BaseModel):
    """Connector-owned projection of Wix source configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    sitemaps: tuple[str, ...] = ()
    use_advertised_sitemaps: bool = True
    product_pattern: str | None = None
    page_limit: int = Field(default=500, ge=1)
    sitemap_limit: int = Field(default=100, ge=1)
    brand: str | None = None
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None = None
    render: bool | None = None


class WixConnector(CommerceConnector):
    name = "wix"
    platform = "wix"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.EXACT, StockQuantityKind.UNKNOWN}),
        supports_incremental_cursor=False,
        supports_category_filter=False,
        supports_documents=True,
        browser=BrowserRequirement.OPTIONAL,
    )

    def __init__(
        self,
        transport: CommerceTransport,
        options: WixOptions | None = None,
        context: ConnectorContext | None = None,
    ) -> None:
        self.transport = _WixTransport(transport)
        self.options = options or WixOptions()
        self.context = context or ConnectorContext()
        self._product_pattern = (
            re.compile(self.options.product_pattern) if self.options.product_pattern else None
        )

    async def collect(
        self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None = None
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        self._validate_request(request, checkpoint)
        start, sequence = self._resume(checkpoint)
        if self.context.cancelled():
            return
        try:
            urls, discovery_error = await self._discover(request.base_url)
        except (
            HTTPStatusFailure,
            ResponseBodyTooLarge,
            SitemapTraversalFailure,
            TransportFailure,
            UnicodeError,
        ) as error:
            yield self._failure_page(
                sequence,
                start,
                Diagnostic(
                    code=DiagnosticCode.ENUMERATION_INCOMPLETE,
                    severity=DiagnosticSeverity.ERROR,
                    message=f"Wix sitemap discovery failed: {type(error).__name__}",
                    retryable=not isinstance(error, ResponseBodyTooLarge),
                    affects_completeness=True,
                    url=request.base_url,
                ),
            )
            return
        if discovery_error is not None:
            yield self._failure_page(sequence, start, discovery_error)
            return
        if not urls:
            yield self._failure_page(
                sequence,
                start,
                Diagnostic(
                    code=DiagnosticCode.ENUMERATION_INCOMPLETE,
                    severity=DiagnosticSeverity.ERROR,
                    message="Wix sitemaps yielded no product URLs",
                    retryable=True,
                    affects_completeness=True,
                    url=request.base_url,
                ),
            )
            return
        if start > len(urls):
            raise ValueError("CHECKPOINT_INVALID: Wix index is beyond discovered products")

        stop = min(len(urls), self.options.page_limit)
        result_stop = None
        if request.result_limit is not None:
            result_stop = start + request.result_limit
            stop = min(stop, result_stop)
        if start == stop:
            yield EntityPage(
                page_id=_page_id(sequence, start),
                sequence=sequence,
                items=(),
                terminal=True,
                enumeration_intact=True,
                discovered=0,
            )
            return

        for index in range(start, stop):
            if self.context.cancelled():
                return
            url = urls[index]
            try:
                document = await self.transport.document(url, rendered=self.options.render is True)
            except (HTTPStatusFailure, ResponseBodyTooLarge, TransportFailure) as error:
                yield self._entity_failure(sequence, index, url, error)
                return
            snapshot = self._normalize(document, url, request.source_id)
            if snapshot is None and self.options.render is None:
                try:
                    rendered = await self.transport.document(url, rendered=True)
                except (HTTPStatusFailure, ResponseBodyTooLarge, TransportFailure):
                    rendered = ""
                snapshot = self._normalize(rendered, url, request.source_id)
            if snapshot is None:
                yield self._failure_page(
                    sequence,
                    index,
                    Diagnostic(
                        code=DiagnosticCode.PARSER_UNSUPPORTED,
                        severity=DiagnosticSeverity.ERROR,
                        message="Wix page exposed no usable warmup product payload",
                        retryable=False,
                        affects_completeness=True,
                        url=url,
                    ),
                )
                return

            next_index = index + 1
            limited = result_stop is not None and result_stop < len(urls) and next_index >= result_stop
            terminal = next_index == len(urls) or limited
            yield EntityPage(
                page_id=_page_id(sequence, index),
                sequence=sequence,
                items=(snapshot,),
                resume_after=(
                    None if terminal and not limited else {"index": next_index, "sequence": sequence + 1}
                ),
                terminal=terminal,
                enumeration_intact=not limited,
                discovered=1,
                diagnostics=(
                    (result_limit_diagnostic(request.result_limit, url),)
                    if limited and request.result_limit is not None
                    else ()
                ),
            )
            sequence += 1
            if terminal:
                return

        if stop < len(urls):
            yield self._failure_page(
                sequence,
                stop,
                Diagnostic(
                    code=DiagnosticCode.ENUMERATION_INCOMPLETE,
                    severity=DiagnosticSeverity.ERROR,
                    message=f"Wix product page limit {self.options.page_limit} reached",
                    retryable=False,
                    affects_completeness=True,
                    url=request.base_url,
                ),
            )

    async def _discover(self, base_url: str) -> tuple[list[str], Diagnostic | None]:
        configured = list(self.options.sitemaps)
        if not configured and self.options.use_advertised_sitemaps:
            with suppress(ResponseBodyTooLarge, TransportFailure):
                configured.extend(await self.transport.advertised_sitemaps(base_url))
        fallback = f"{_origin(base_url)}/store-products-sitemap.xml"
        first_error: tuple[str, str] | None = None
        for initial in (configured, [fallback]):
            if not initial:
                continue
            try:
                urls = await self._walk_sitemaps(list(dict.fromkeys(initial)))
            except (
                HTTPStatusFailure,
                ResponseBodyTooLarge,
                SitemapTraversalFailure,
                TransportFailure,
                UnicodeError,
            ) as error:
                first_error = first_error or (initial[0], type(error).__name__)
                continue
            selected = [url for url in urls if self._is_product_url(url, base_url)]
            if selected:
                return list(dict.fromkeys(selected)), None
        if first_error is not None:
            url, error_name = first_error
            return [], Diagnostic(
                code=DiagnosticCode.ENUMERATION_INCOMPLETE,
                severity=DiagnosticSeverity.ERROR,
                message=f"Wix sitemap fetch failed: {error_name}",
                retryable=error_name != ResponseBodyTooLarge.__name__,
                affects_completeness=True,
                url=url,
            )
        return [], None

    async def _walk_sitemaps(self, queue: list[str]) -> list[str]:
        found: list[str] = []
        seen: set[str] = set()
        while queue:
            if self.context.cancelled():
                return found
            url = queue.pop(0)
            if url in seen:
                continue
            if len(seen) >= self.options.sitemap_limit:
                raise SitemapTraversalFailure("Wix sitemap traversal limit reached")
            seen.add(url)
            document = await self.transport.document(url, accept="application/xml,text/xml")
            locations = [
                _clean(value)
                for value in re.findall(
                    r"<loc>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</loc>",
                    document,
                    re.IGNORECASE,
                )
            ]
            if re.search(r"<sitemapindex\b", document, re.IGNORECASE):
                queue.extend(value for value in locations if value)
            else:
                found.extend(value for value in locations if value)
        return list(dict.fromkeys(found))

    def _is_product_url(self, url: str, base_url: str) -> bool:
        if urlparse(url).netloc != urlparse(base_url).netloc:
            return False
        if self._product_pattern is None:
            return True
        return bool(self._product_pattern.search(urlparse(url).path) or self._product_pattern.search(url))

    def _normalize(self, document: str, url: str, source_id: str) -> CommerceProductSnapshot | None:
        product = _warmup_product(document, url)
        if product is None:
            return None
        jsonld_product = next(iter(_jsonld_products(document)), {})
        title = (
            _clean(product.get("name")) or _clean(jsonld_product.get("name")) or _meta(document, "og:title")
        )
        product_id = _clean(product.get("id"))
        if not title or not product_id:
            return None
        canonical_url = _canonical(url)
        images = _jsonld_images(jsonld_product, canonical_url) or _media_images(product)
        currency = _currency(document) or _clean(product.get("currency"))
        if currency:
            currency = currency.upper()
        observed_at = self.context.clock()
        evidence = Evidence(
            method="html",
            source_url=canonical_url,
            source_field="wix_warmup_data",
            observed_at=observed_at,
            confidence="published",
        )
        variants = tuple(
            variant
            for raw in _variants(product)
            if (variant := self._variant(raw, product_id, canonical_url, currency, evidence)) is not None
        )
        product_raw = {key: value for key, value in product.items() if key != "productItems"}
        return CommerceProductSnapshot(
            connector=self.name,
            source_id=source_id,
            external_id=product_id,
            canonical_url=canonical_url,
            title=title,
            observed_at=observed_at,
            description=_clean(product.get("description")) or None,
            vendor=_clean(product.get("brand")) or self.options.brand,
            categories=tuple(CategoryRef(name=name) for name in _breadcrumbs(document)),
            images=tuple(MediaRef(url=image) for image in images),
            documents=tuple(
                DocumentRef(
                    url=document_url,
                    title=label or None,
                    media_type="application/pdf",
                    observed_at=observed_at,
                    evidence=(
                        Evidence(
                            method="html",
                            source_url=canonical_url,
                            source_field="a[href$='.pdf']",
                            observed_at=observed_at,
                            confidence="published",
                        ),
                    ),
                )
                for document_url, label in _pdf_links(document, canonical_url)
            ),
            variants=variants,
            platform_extensions={"legacy_raw_product": cast(JsonValue, product_raw)},
        )

    def _variant(
        self,
        raw: dict[str, Any],
        product_id: str,
        canonical_url: str,
        currency: str | None,
        evidence: Evidence,
    ) -> CommerceVariant | None:
        price = _decimal(raw.get("price"))
        if price is None or not currency or not re.fullmatch(r"[A-Z]{3}", currency):
            return None
        compare = _compare_price(raw)
        offers = [
            CommerceOffer(
                price=Money(amount=price, currency=currency),
                role="sale" if compare is not None else "regular",
                vat_status=self.options.vat_status or "unknown",
                observed_at=evidence.observed_at,
                evidence=(evidence,),
            )
        ]
        if compare is not None:
            offers.append(
                CommerceOffer(
                    price=Money(amount=compare, currency=currency),
                    role="regular",
                    vat_status=self.options.vat_status or "unknown",
                    observed_at=evidence.observed_at,
                    evidence=(evidence,),
                )
            )
        in_stock = raw.get("in_stock", True) is not False
        quantity = raw.get("stock_quantity")
        exact_quantity = quantity if isinstance(quantity, int) and not isinstance(quantity, bool) else None
        return CommerceVariant(
            external_id=_clean(raw.get("id")) or product_id,
            is_default=not bool(_clean(raw.get("id"))),
            canonical_url=canonical_url,
            title=_clean(raw.get("title")) or None,
            sku=_clean(raw.get("sku")) or None,
            options=(
                {
                    _clean(key): _clean(value)
                    for key, value in raw["options"].items()
                    if _clean(key) and _clean(value)
                }
                if isinstance(raw.get("options"), dict)
                else {}
            ),
            offers=tuple(offers),
            stock=StockState(
                availability=(Availability.IN_STOCK if in_stock else Availability.OUT_OF_STOCK),
                quantity=exact_quantity,
                quantity_kind=(
                    StockQuantityKind.EXACT if exact_quantity is not None else StockQuantityKind.UNKNOWN
                ),
                observed_at=evidence.observed_at,
                evidence=(evidence,),
            ),
            published_attributes={"price_text": _clean(raw.get("formattedPrice")) or None},
            platform_extensions={"legacy_raw_variant": cast(JsonValue, raw)},
        )

    def checkpoint(
        self, request: CollectionRequest, lineage: str, resume_after: JsonValue
    ) -> ConnectorCheckpoint:
        options = cast(dict[str, JsonValue], self.options.model_dump(mode="json"))
        return ConnectorCheckpoint(
            connector=self.name,
            connector_version=self.version,
            source_id=request.source_id,
            lineage=lineage,
            collection_fingerprint=collection_fingerprint(request, self.name, options),
            resume_after=resume_after,
        )

    def _validate_request(self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None) -> None:
        if not self.capabilities.supports(request.requested_fields, request.refresh_mode):
            raise ValueError("Wix connector does not support the requested collection")
        if request.partitions:
            raise ValueError("Wix connector does not support server-side filters")
        options = cast(dict[str, JsonValue], self.options.model_dump(mode="json"))
        validate_checkpoint(
            checkpoint,
            connector=self.name,
            connector_version=self.version,
            request=request,
            options=options,
        )

    @staticmethod
    def _resume(checkpoint: ConnectorCheckpoint | None) -> tuple[int, int]:
        if checkpoint is None:
            return 0, 0
        cursor = checkpoint.resume_after
        if not isinstance(cursor, dict):
            raise ValueError("CHECKPOINT_INVALID: Wix cursor must be an object")
        index = cursor.get("index")
        sequence = cursor.get("sequence")
        if (
            not isinstance(index, int)
            or isinstance(index, bool)
            or index < 0
            or not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence < 0
        ):
            raise ValueError("CHECKPOINT_INVALID: Wix cursor values are invalid")
        return index, sequence

    @staticmethod
    def _failure_page(
        sequence: int, index: int, diagnostic: Diagnostic
    ) -> EntityPage[CommerceProductSnapshot]:
        return EntityPage(
            page_id=_page_id(sequence, index),
            sequence=sequence,
            items=(),
            resume_after={"index": index, "sequence": sequence},
            terminal=True,
            enumeration_intact=False,
            discovered=0,
            diagnostics=(diagnostic,),
        )

    def _entity_failure(
        self, sequence: int, index: int, url: str, error: Exception
    ) -> EntityPage[CommerceProductSnapshot]:
        return self._failure_page(
            sequence,
            index,
            Diagnostic(
                code=DiagnosticCode.ENTITY_FETCH_FAILED,
                severity=DiagnosticSeverity.ERROR,
                message=f"Wix product fetch failed: {type(error).__name__}",
                retryable=not isinstance(error, ResponseBodyTooLarge),
                affects_completeness=True,
                url=url,
            ),
        )


class WixFactory:
    name = "wix"
    version = WixConnector.version
    options_model: type[BaseModel] = WixOptions

    def build(
        self, *, transport: CommerceTransport, options: BaseModel, context: ConnectorContext
    ) -> WixConnector:
        return WixConnector(transport, WixOptions.model_validate(options), context)


def _warmup_product(document: str, url: str) -> dict[str, Any] | None:
    slug = unquote(urlparse(url).path.rstrip("/").rsplit("/", 1)[-1])
    marker = f'"{slug}":{{"product":{{'
    index = document.find(marker)
    if index >= 0:
        return _balanced_object(document, index + len(f'"{slug}":{{"product":'))
    match = re.search(r'"product":\{"id":"[0-9a-f-]{36}"', document)
    return _balanced_object(document, match.start() + len('"product":')) if match else None


def _balanced_object(document: str, start: int) -> dict[str, Any] | None:
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(document)):
        character = document[index]
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                try:
                    value = json.loads(document[start : index + 1])
                except json.JSONDecodeError:
                    return None
                return value if isinstance(value, dict) else None
    return None


def _jsonld_blocks(document: str) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def flatten(value: Any) -> None:
        if isinstance(value, list):
            for child in value:
                flatten(child)
        elif isinstance(value, dict):
            if "@graph" in value:
                flatten(value["@graph"])
            else:
                found.append(value)

    for raw in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        document,
        re.IGNORECASE | re.DOTALL,
    ):
        try:
            flatten(json.loads(html.unescape(raw.strip())))
        except (json.JSONDecodeError, TypeError):
            continue
    return found


def _jsonld_products(document: str) -> list[dict[str, Any]]:
    return [
        item
        for item in _jsonld_blocks(document)
        if any(
            str(value).casefold() == "product"
            for value in (
                item.get("@type", []) if isinstance(item.get("@type", []), list) else [item.get("@type")]
            )
        )
    ]


def _breadcrumbs(document: str) -> list[str]:
    for item in _jsonld_blocks(document):
        types = item.get("@type", [])
        types = types if isinstance(types, list) else [types]
        if not any(str(value).casefold() == "breadcrumblist" for value in types):
            continue
        names: list[str] = []
        for element in item.get("itemListElement") or []:
            if not isinstance(element, dict):
                continue
            entry = element.get("item")
            name = entry.get("name") if isinstance(entry, dict) else element.get("name")
            if cleaned := _clean(name):
                names.append(cleaned)
        if names:
            return names
    return []


def _jsonld_images(item: dict[str, Any], page_url: str) -> list[str]:
    value = item.get("image")
    values = value if isinstance(value, list) else [value]
    found: list[str] = []
    for entry in values:
        if isinstance(entry, dict):
            entry = entry.get("url") or entry.get("contentUrl")
        if cleaned := _clean(entry):
            found.append(urljoin(page_url, cleaned))
    return list(dict.fromkeys(found))


def _media_images(product: dict[str, Any]) -> list[str]:
    found = [
        _clean(item.get("fullUrl"))
        for item in product.get("media") or []
        if isinstance(item, dict) and item.get("fullUrl")
    ]
    return list(dict.fromkeys(value for value in found if value))


def _pdf_links(document: str, page_url: str) -> list[tuple[str, str]]:
    return [
        (urljoin(page_url, html.unescape(match.group(1))), _clean(match.group(2)))
        for match in re.finditer(
            r'<a[^>]+href=["\']([^"\']+\.pdf[^"\']*)["\'][^>]*>(.*?)</a>',
            document,
            re.IGNORECASE | re.DOTALL,
        )
    ]


def _variants(product: dict[str, Any]) -> list[dict[str, Any]]:
    items = [item for item in product.get("productItems") or [] if isinstance(item, dict)]
    variants: list[dict[str, Any]] = []
    for item in items:
        options = (
            {_clean(key): _clean(value) for key, value in item["optionsSelections"].items()}
            if isinstance(item.get("optionsSelections"), dict)
            else {}
        )
        variants.append(
            {
                "id": item.get("id"),
                "title": ", ".join(value for value in options.values() if value) or None,
                "sku": item.get("sku") or product.get("sku"),
                "price": item.get("price", product.get("price")),
                "comparePrice": item.get("comparePrice"),
                "formattedPrice": item.get("formattedPrice"),
                "in_stock": item.get("isInStock", item.get("inStock", True)),
                "stock_quantity": _stock_quantity(item, product),
                "options": options or None,
            }
        )
    if len(variants) <= 1:
        return [
            {
                "id": None,
                "title": None,
                "sku": product.get("sku"),
                "price": product.get("price"),
                "comparePrice": product.get("comparePrice"),
                "formattedPrice": product.get("formattedPrice"),
                "in_stock": product.get("isInStock", True),
                "stock_quantity": _stock_quantity(product),
                "options": None,
            }
        ]
    return variants


def _stock_quantity(item: dict[str, Any], product: dict[str, Any] | None = None) -> int | None:
    inventory = item.get("inventory")
    if not isinstance(inventory, dict) and product is not None:
        inventory = product.get("inventory")
    if not isinstance(inventory, dict):
        inventory = {}
    in_stock = item.get("isInStock", item.get("inStock"))
    if in_stock is False or str(inventory.get("status") or "").lower() == "out_of_stock":
        return 0
    tracking = item.get("isTrackingInventory")
    if tracking is None and product is not None:
        tracking = product.get("isTrackingInventory")
    quantity = inventory.get("quantity")
    if tracking is True and isinstance(quantity, int) and not isinstance(quantity, bool) and quantity >= 0:
        return quantity
    return None


def _compare_price(variant: dict[str, Any]) -> Decimal | None:
    value = _decimal(variant.get("comparePrice"))
    price = _decimal(variant.get("price"))
    return value if value is not None and value != 0 and value != price else None


def _decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() and result >= 0 else None


def _currency(document: str) -> str | None:
    match = re.search(r'"currency":"([A-Z]{3})"', document)
    return match.group(1) if match else None


def _meta(document: str, key: str) -> str | None:
    escaped = re.escape(key)
    for pattern in (
        rf'<meta[^>]+(?:property|name)=["\']{escaped}["\'][^>]+content=["\']([^"\']*)',
        rf'<meta[^>]+content=["\']([^"\']*)["\'][^>]+(?:property|name)=["\']{escaped}["\']',
    ):
        if match := re.search(pattern, document, re.IGNORECASE):
            return html.unescape(match.group(1)).strip()
    return None


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", str(value))).split())


def _canonical(url: str) -> str:
    parsed = urlparse(url)
    query = (
        ""
        if re.search(r"(?:^|&)(?:order|tag|id_currency|search_query|back|q|sort)=", parsed.query)
        else parsed.query
    )
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, ""))


def _origin(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def _page_id(sequence: int, index: int) -> str:
    digest = hashlib.sha256(f"wix:{sequence}:{index}".encode()).hexdigest()[:16]
    return f"wix-{sequence}-{digest}"
