"""Specialized sitemap/page connectors for common hosted commerce frameworks."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import AsyncIterator
from decimal import Decimal, InvalidOperation
from html import unescape
from typing import Any, Literal, cast
from urllib.parse import unquote, urljoin, urlparse

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from mb_commerce_scraper.discovery import (
    DiscoveryFailure,
    SitemapDiscovery,
    advertised_sitemaps,
)
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
    sanitize_commerce_snapshot,
    validate_checkpoint,
)
from mb_commerce_scraper.parsing import JsonLdProductParser
from mb_commerce_scraper.parsing._structured import (
    VerifiedDomRules,
    dom_product,
    jsonld_products,
    microdata_products,
    opengraph_product,
    pdf_links,
    probable_javascript_shell,
    specification_table,
)
from mb_commerce_scraper.transports import (
    BrowserHint,
    BudgetExhausted,
    CommerceTransport,
    RequestPriority,
    RequestPurpose,
    ResponseBodyTooLarge,
    TransportFailure,
    TransportRequest,
)

from .base import BrowserRequirement, CommerceConnector, ConnectorCapabilities, ConnectorContext


class SpecializedPageOptions(BaseModel):
    """Bounded, data-only page discovery and platform parsing options."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    sitemaps: tuple[str, ...] = ()
    use_advertised_sitemaps: bool = True
    category_urls: tuple[str, ...] = ()
    product_pattern: str | None = None
    pagination_patterns: tuple[str, ...] = ()
    card_links_only: bool = False
    page_limit: int = Field(default=10_000, ge=1)
    sitemap_limit: int = Field(default=100, ge=1, le=10_000)
    category_page_limit: int = Field(default=120, ge=1, le=10_000)
    render: bool | None = None
    browser_zero_gain_limit: int = Field(default=10, ge=1)
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld",
        "opengraph",
    )
    dom_rules: VerifiedDomRules | None = None
    currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    brand: str | None = None
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None = None
    vat_rate: Decimal | None = Field(default=None, ge=0, le=1)
    stock_from_quantity_maximum: bool = False


class ShopwareOptions(SpecializedPageOptions):
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld",
        "microdata",
        "opengraph",
    )
    stock_from_quantity_maximum: bool = True


class StarwebOptions(SpecializedPageOptions):
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld",
        "opengraph",
    )


class NitroSellOptions(SpecializedPageOptions):
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "opengraph",
    )


class SumUpOptions(SpecializedPageOptions):
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld",
    )


class SpecializedPageConnector(CommerceConnector):
    """Shared bounded sitemap collection while platform classes own parsing."""

    name = "specialized-pages"
    platform = "specialized"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.EXACT, StockQuantityKind.UNKNOWN}),
        supports_documents=True,
        browser=BrowserRequirement.OPTIONAL,
    )

    def __init__(
        self,
        transport: CommerceTransport,
        options: SpecializedPageOptions,
        context: ConnectorContext | None = None,
    ) -> None:
        self.transport = transport
        self.options = options
        self.context = context or ConnectorContext()
        self._jsonld = JsonLdProductParser(currency=options.currency)

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        if not self.capabilities.supports(
            request.requested_fields, request.refresh_mode
        ):
            raise ValueError(
                f"{self.name} does not support the requested collection contract"
            )
        if self.context.cancelled():
            return
        options = self._checkpoint_options()
        validate_checkpoint(
            checkpoint,
            connector=self.name,
            connector_version=self.version,
            request=request,
            options=options,
        )
        try:
            discovered_urls = tuple(
                [url async for url in self._discover(request.base_url)]
            )
        except DiscoveryFailure as error:
            yield self._failure_page(
                0,
                request.base_url,
                DiagnosticCode.ENUMERATION_INCOMPLETE,
                str(error),
                retryable=error.retryable,
            )
            return
        if self.context.cancelled():
            return
        if not discovered_urls:
            yield self._failure_page(
                0,
                request.base_url,
                DiagnosticCode.ENUMERATION_INCOMPLETE,
                "page discovery yielded no product URLs",
                resume_after={
                    "index": 0, "url": request.base_url,
                    "snapshot_offset": 0, "sequence": 0,
                },
            )
            return
        index, snapshot_offset, sequence = self._resume(checkpoint, discovered_urls)
        emitted = 0
        zero_gain = 0
        stop = min(len(discovered_urls), self.options.page_limit)
        for current in range(index, stop):
            if self.context.cancelled():
                return
            url = discovered_urls[current]
            initial_browser = self.options.render is True
            try:
                response = await self.transport.request(
                    TransportRequest(
                        url=url,
                        purpose=RequestPurpose.ENTITY,
                        priority=RequestPriority.IDENTITY,
                        estimated_bytes=500_000,
                        browser=(
                            BrowserHint.REQUIRED
                            if initial_browser
                            else BrowserHint.NEVER
                        ),
                    )
                )
            except (BudgetExhausted, ResponseBodyTooLarge, TransportFailure) as error:
                budget_exhausted = isinstance(error, BudgetExhausted)
                yield self._failure_page(
                    sequence,
                    url,
                    (
                        DiagnosticCode.REQUEST_BUDGET_EXHAUSTED
                        if budget_exhausted
                        else DiagnosticCode.ENTITY_FETCH_FAILED
                    ),
                    f"product transport failed: {type(error).__name__}",
                    retryable=(
                        budget_exhausted
                        or not isinstance(error, ResponseBodyTooLarge)
                    ),
                    resume_after={
                        "index": current,
                        "url": url,
                        "snapshot_offset": snapshot_offset,
                        "sequence": sequence,
                    },
                    metadata={"stage": "browser" if initial_browser else "http"},
                )
                return
            if response.status >= 400:
                yield self._failure_page(
                    sequence,
                    url,
                    DiagnosticCode.ENTITY_FETCH_FAILED,
                    f"product request failed with status {response.status}",
                    retryable=response.status >= 500,
                    resume_after={
                        "index": current,
                        "url": url,
                        "snapshot_offset": snapshot_offset,
                        "sequence": sequence,
                    },
                    metadata={"stage": "browser" if initial_browser else "http"},
                )
                return
            snapshots = self.parse(response.text(), url, request.source_id)
            shell = probable_javascript_shell(response.text())
            browser_attempted = initial_browser
            if (
                not snapshots
                and shell
                and self.options.render is None
                and zero_gain < self.options.browser_zero_gain_limit
            ):
                if self.context.cancelled():
                    return
                browser_attempted = True
                try:
                    rendered = await self.transport.request(
                        TransportRequest(
                            url=url,
                            purpose=RequestPurpose.ENTITY,
                            priority=RequestPriority.IDENTITY,
                            estimated_bytes=1_000_000,
                            browser=BrowserHint.REQUIRED,
                        )
                    )
                except (
                    BudgetExhausted,
                    ResponseBodyTooLarge,
                    TransportFailure,
                ) as error:
                    budget_exhausted = isinstance(error, BudgetExhausted)
                    yield self._failure_page(
                        sequence,
                        url,
                        (
                            DiagnosticCode.REQUEST_BUDGET_EXHAUSTED
                            if budget_exhausted
                            else DiagnosticCode.ENTITY_FETCH_FAILED
                        ),
                        f"browser transport failed: {type(error).__name__}",
                        retryable=(
                            budget_exhausted
                            or not isinstance(error, ResponseBodyTooLarge)
                        ),
                        resume_after={
                            "index": current,
                            "url": url,
                            "snapshot_offset": snapshot_offset,
                            "sequence": sequence,
                        },
                        metadata={"stage": "browser"},
                    )
                    return
                if rendered.status >= 400:
                    yield self._failure_page(
                        sequence,
                        url,
                        DiagnosticCode.ENTITY_FETCH_FAILED,
                        f"browser request failed with status {rendered.status}",
                        retryable=rendered.status >= 500,
                        resume_after={
                            "index": current,
                            "url": url,
                            "snapshot_offset": snapshot_offset,
                            "sequence": sequence,
                        },
                        metadata={"stage": "browser"},
                    )
                    return
                snapshots = self.parse(rendered.text(), url, request.source_id)
                zero_gain = 0 if snapshots else zero_gain + 1
            if not snapshots:
                code = (
                    DiagnosticCode.BROWSER_REQUIRED
                    if shell and not browser_attempted
                    else DiagnosticCode.PARSER_UNSUPPORTED
                )
                yield self._failure_page(
                    sequence,
                    url,
                    code,
                    f"{self.platform} product markup was not recognized",
                    retryable=code == DiagnosticCode.BROWSER_REQUIRED,
                    resume_after={
                        "index": current,
                        "url": url,
                        "snapshot_offset": snapshot_offset,
                        "sequence": sequence,
                    },
                    metadata={
                        "browser_attempted": browser_attempted,
                        "render_policy": (
                            "require"
                            if self.options.render is True
                            else "never"
                            if self.options.render is False
                            else "allow"
                        ),
                    },
                )
                return
            if snapshot_offset >= len(snapshots):
                raise ValueError(
                    "CHECKPOINT_INVALID: snapshot offset is outside the resumed page"
                )
            available = snapshots[snapshot_offset:]
            remaining = None if request.result_limit is None else request.result_limit - emitted
            selected = available if remaining is None else available[:remaining]
            selected = tuple(sanitize_commerce_snapshot(item) for item in selected)
            emitted += len(selected)
            next_offset = snapshot_offset + len(selected)
            has_more_on_page = next_offset < len(snapshots)
            has_more_pages = current + 1 < len(discovered_urls)
            limited = (
                request.result_limit is not None
                and emitted >= request.result_limit
                and (has_more_on_page or has_more_pages)
            )
            terminal = limited or (not has_more_on_page and not has_more_pages)
            cursor: JsonValue | None
            if terminal and not limited:
                cursor = None
            elif has_more_on_page:
                cursor = {
                    "index": current,
                    "url": url,
                    "snapshot_offset": next_offset,
                    "sequence": sequence + 1,
                }
            else:
                cursor = {
                    "index": current + 1,
                    "url": discovered_urls[current + 1],
                    "snapshot_offset": 0,
                    "sequence": sequence + 1,
                }
            diagnostics = (
                (result_limit_diagnostic(request.result_limit, url),)
                if limited and request.result_limit is not None
                else ()
            )
            yield EntityPage(
                page_id=f"product:{sequence}",
                partition_key=self._partition_key(),
                sequence=sequence,
                items=selected,
                resume_after=cursor,
                terminal=terminal,
                enumeration_intact=not limited,
                discovered=len(snapshots),
                diagnostics=diagnostics,
            )
            sequence += 1
            if limited:
                return
            snapshot_offset = 0
        if stop < len(discovered_urls):
            yield self._failure_page(
                sequence,
                discovered_urls[stop],
                DiagnosticCode.ENUMERATION_INCOMPLETE,
                f"page limit {self.options.page_limit} reached",
                resume_after={
                    "index": stop, "url": discovered_urls[stop],
                    "snapshot_offset": 0, "sequence": sequence,
                },
            )

    def parse(
        self, document: str, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        for parser in self.options.parsers:
            if parser == "jsonld":
                snapshots = self._parse_raw(
                    jsonld_products(document), url=url, source_id=source_id
                )
            elif parser == "microdata":
                snapshots = self._parse_raw(
                    microdata_products(document), url=url, source_id=source_id
                )
            elif parser == "opengraph":
                raw = opengraph_product(document)
                snapshots = self._parse_raw(
                    [raw] if raw is not None else [], url=url, source_id=source_id
                )
            else:
                raw = (
                    dom_product(document, self.options.dom_rules, self.options.currency)
                    if self.options.dom_rules is not None
                    else None
                )
                snapshots = self._parse_raw(
                    [raw] if raw is not None else [], url=url, source_id=source_id
                )
            if snapshots:
                return self._normalize_page(
                    snapshots, document=document, url=url, parser=parser
                )
        return ()

    def checkpoint(
        self, request: CollectionRequest, lineage: str, resume_after: JsonValue
    ) -> ConnectorCheckpoint:
        return ConnectorCheckpoint(
            connector=self.name,
            connector_version=self.version,
            source_id=request.source_id,
            lineage=lineage,
            collection_fingerprint=collection_fingerprint(
                request, self.name, self._checkpoint_options()
            ),
            resume_after=resume_after,
        )

    def _retag(
        self, snapshots: tuple[CommerceProductSnapshot, ...]
    ) -> tuple[CommerceProductSnapshot, ...]:
        return tuple(
            snapshot.model_copy(
                update={
                    "connector": self.name,
                    "vendor": snapshot.vendor or self.options.brand,
                }
            )
            for snapshot in snapshots
        )

    def _parse_raw(
        self, raw_items: list[dict[str, Any]], *, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        if not raw_items:
            return ()
        document = "".join(
            '<script type="application/ld+json">'
            + json.dumps(raw, separators=(",", ":"))
            + "</script>"
            for raw in raw_items
        )
        snapshots = self._jsonld.parse(document, url=url, source_id=source_id)
        output: list[CommerceProductSnapshot] = []
        for index, snapshot in enumerate(snapshots):
            raw = raw_items[index] if index < len(raw_items) else {}
            variants = tuple(
                variant.model_copy(
                    update={
                        "is_default": True,
                        "canonical_url": snapshot.canonical_url,
                        "platform_extensions": {
                            **variant.platform_extensions,
                            "legacy_raw_variant": None,
                        },
                    }
                )
                for variant in snapshot.variants
            )
            output.append(
                snapshot.model_copy(
                    update={
                        "variants": variants,
                        "platform_extensions": {
                            **snapshot.platform_extensions,
                            "raw": cast(JsonValue, raw),
                        },
                    }
                )
            )
        return tuple(output)

    def _normalize_page(
        self,
        snapshots: tuple[CommerceProductSnapshot, ...],
        *,
        document: str,
        url: str,
        parser: Literal["jsonld", "microdata", "opengraph", "dom"],
    ) -> tuple[CommerceProductSnapshot, ...]:
        method: Literal["jsonld", "html"] = "jsonld" if parser == "jsonld" else "html"
        specifications = cast(dict[str, JsonValue], specification_table(document))
        output: list[CommerceProductSnapshot] = []
        for snapshot in self._retag(snapshots):
            platform_extensions = dict(snapshot.platform_extensions)
            if parser == "opengraph":
                raw_value = platform_extensions.get("raw")
                if isinstance(raw_value, dict):
                    platform_extensions["raw"] = {
                        **{
                            key: (value if value != "" else None)
                            for key, value in raw_value.items()
                            if key != "@type"
                        },
                        "_opengraph": True,
                    }
            evidence = Evidence(
                method=method,
                source_url=url,
                source_field=parser,
                observed_at=snapshot.observed_at,
            )
            variants = []
            for variant in snapshot.variants:
                offers = tuple(
                    offer.model_copy(
                        update={
                            "evidence": (evidence,),
                            "vat_status": self.options.vat_status or "unknown",
                            "vat_rate": self.options.vat_rate,
                            "availability_evidence": (
                                (evidence,) if offer.availability is not None else ()
                            ),
                        }
                    )
                    for offer in variant.offers
                )
                stock = (
                    variant.stock.model_copy(update={"evidence": (evidence,)})
                    if variant.stock is not None
                    else None
                )
                published_attributes = {
                    **variant.published_attributes,
                    **specifications,
                }
                if offers:
                    published_attributes.setdefault(
                        "price_text",
                        f"{offers[0].price.amount} {offers[0].price.currency}",
                    )
                variants.append(
                    variant.model_copy(
                        update={
                            "offers": offers,
                            "stock": stock,
                            "published_attributes": published_attributes,
                        }
                    )
                )
            documents = tuple(
                DocumentRef(
                    url=document_url,
                    title=label or None,
                    media_type="application/pdf",
                    observed_at=snapshot.observed_at,
                    evidence=(evidence,),
                )
                for document_url, label in pdf_links(document, url)
            )
            output.append(
                snapshot.model_copy(
                    update={
                        "variants": tuple(variants),
                        "documents": (*snapshot.documents, *documents),
                        "platform_extensions": {
                            **platform_extensions,
                            "page_parser": parser,
                        },
                    }
                )
            )
        return tuple(output)

    def _checkpoint_options(self) -> dict[str, JsonValue]:
        return cast(dict[str, JsonValue], self.options.model_dump(mode="json"))

    async def _discover(self, base_url: str) -> AsyncIterator[str]:
        if self.context.cancelled():
            return
        if self.options.category_urls:
            async for url in self._category_discover(base_url):
                if self.context.cancelled():
                    return
                yield url
            return
        roots = self.options.sitemaps
        if not roots and self.options.use_advertised_sitemaps:
            roots = await advertised_sitemaps(self.transport, base_url)
        if not roots:
            roots = ("/sitemap.xml",)
        discovery = SitemapDiscovery(
            self.transport, roots, product_pattern=None, limit=self.options.sitemap_limit
        )
        async for url in discovery.discover(base_url):
            if self.context.cancelled():
                return
            if self._is_product(url, base_url):
                yield url

    async def _category_discover(self, base_url: str) -> AsyncIterator[str]:
        queue = [urljoin(base_url, value) for value in self.options.category_urls]
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
                    f"category page limit {self.options.category_page_limit} reached"
                )
            seen.add(url)
            response = await self.transport.request(
                TransportRequest(
                    url=url,
                    purpose=RequestPurpose.DISCOVERY,
                    priority=RequestPriority.DISCOVERY,
                    estimated_bytes=500_000,
                )
            )
            if response.status >= 400:
                raise DiscoveryFailure(
                    f"category request failed with status {response.status}"
                )
            if self.context.cancelled():
                return
            for candidate in _links(
                response.text(), url, cards_only=self.options.card_links_only
            ):
                if self._is_product(candidate, base_url):
                    if candidate not in emitted:
                        emitted.add(candidate)
                        yield candidate
                elif (
                    self._is_pagination(candidate)
                    and candidate not in seen
                    and candidate not in queue
                ):
                    queue.append(candidate)

    def _is_product(self, url: str, base_url: str) -> bool:
        if urlparse(url).netloc != urlparse(base_url).netloc:
            return False
        if self.options.product_pattern is None:
            return True
        pattern = re.compile(self.options.product_pattern)
        return bool(pattern.search(urlparse(url).path) or pattern.search(url))

    def _is_pagination(self, url: str) -> bool:
        if self.options.pagination_patterns:
            return any(re.search(pattern, url) for pattern in self.options.pagination_patterns)
        return bool(re.search(r"[?&](?:p|page|start)=\d+|/page/\d+", url))

    @staticmethod
    def _resume(
        checkpoint: ConnectorCheckpoint | None, discovered_urls: tuple[str, ...]
    ) -> tuple[int, int, int]:
        if checkpoint is None:
            return 0, 0, 0
        cursor = checkpoint.resume_after
        if not isinstance(cursor, dict):
            raise ValueError("CHECKPOINT_INVALID: specialized page cursor is invalid")
        index = cursor.get("index")
        resume_url = cursor.get("url")
        snapshot_offset = cursor.get("snapshot_offset")
        sequence = cursor.get("sequence")
        values = (index, snapshot_offset, sequence)
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in values
        ):
            raise ValueError("CHECKPOINT_INVALID: specialized page cursor values are invalid")
        assert isinstance(index, int)
        assert isinstance(snapshot_offset, int)
        assert isinstance(sequence, int)
        if index >= len(discovered_urls):
            raise ValueError("CHECKPOINT_INVALID: discovery position is out of range")
        if not isinstance(resume_url, str) or discovered_urls[index] != resume_url:
            raise ValueError("CHECKPOINT_INVALID: resume target is missing from discovery")
        return index, snapshot_offset, sequence

    def _failure_page(
        self,
        sequence: int,
        url: str,
        code: DiagnosticCode,
        message: str,
        *,
        retryable: bool = False,
        resume_after: JsonValue | None = None,
        metadata: dict[str, JsonValue] | None = None,
    ) -> EntityPage[CommerceProductSnapshot]:
        diagnostic = Diagnostic(
            code=code,
            severity=DiagnosticSeverity.WARNING,
            message=message,
            retryable=retryable,
            affects_completeness=True,
            url=url,
            metadata=metadata or {},
        )
        return EntityPage(
            page_id=f"product:{sequence}",
            partition_key=self._partition_key(),
            sequence=sequence,
            items=(),
            resume_after=resume_after,
            terminal=True,
            enumeration_intact=False,
            discovered=sequence,
            diagnostics=(diagnostic,),
        )

    def _partition_key(self) -> str:
        """Name the configured discovery mechanism bound into the fingerprint."""
        return "category" if self.options.category_urls else "sitemap"


class ShopwareConnector(SpecializedPageConnector):
    name = "shopware"
    platform = "shopware"

    def __init__(
        self,
        transport: CommerceTransport,
        options: ShopwareOptions | None = None,
        context: ConnectorContext | None = None,
    ) -> None:
        super().__init__(transport, options or ShopwareOptions(), context)

    def parse(
        self, document: str, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        snapshots = super().parse(document, url, source_id)
        unit = _class_content(document, "product-detail-price-unit")
        number = re.search(
            r"(?:product-detail-ordernumber|Artikel-?Nr\.?|Bestellnummer)[^>]*>\s*"
            r"([A-Za-z0-9][\w.\-/]*)",
            document,
            re.IGNORECASE,
        )
        quantity = re.search(
            r"<(?:input|select)[^>]*(?:name=[\"']quantity[\"']|class=[\"'][^\"']*quantity-selector)"
            r"[^>]*\bmax=[\"']?(\d+)",
            document,
            re.IGNORECASE,
        )
        attributes = _definition_attributes(document)
        output: list[CommerceProductSnapshot] = []
        for snapshot in snapshots:
            variants: list[CommerceVariant] = []
            for variant in snapshot.variants:
                published = dict(variant.published_attributes)
                published.update(attributes)
                if unit:
                    published["published_unit_price"] = unit
                stock = variant.stock
                if quantity and cast(ShopwareOptions, self.options).stock_from_quantity_maximum:
                    assert stock is not None
                    stock = stock.model_copy(
                        update={
                            "quantity": int(quantity.group(1)),
                            "quantity_kind": StockQuantityKind.EXACT,
                        }
                    )
                variants.append(
                    variant.model_copy(
                        update={
                            "sku": variant.sku or (_clean(number.group(1)) if number else None),
                            "published_attributes": published,
                            "stock": stock,
                        }
                    )
                )
            output.append(snapshot.model_copy(update={"variants": tuple(variants)}))
        return tuple(output)


class StarwebConnector(SpecializedPageConnector):
    name = "starweb"
    platform = "starweb"

    def __init__(
        self,
        transport: CommerceTransport,
        options: StarwebOptions | None = None,
        context: ConnectorContext | None = None,
    ) -> None:
        super().__init__(transport, options or StarwebOptions(), context)

    def parse(
        self, document: str, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        snapshots = super().parse(document, url, source_id)
        vat = (
            "inclusive"
            if re.search(r'class=["\'][^"\']*\bincl-vat\b', document, re.IGNORECASE)
            else (
                "exclusive"
                if re.search(r'class=["\'][^"\']*\bexcl-vat\b', document, re.IGNORECASE)
                else None
            )
        )
        attributes = {
            _clean(match.group(1)).rstrip(":"): _clean(match.group(2))
            for match in re.finditer(
                r'<(?:label|span)[^>]*class=["\'][^"\']*(?:variant|attribute)-name[^"\']*["\'][^>]*>(.*?)</(?:label|span)>\s*'
                r'<(?:span|div)[^>]*class=["\'][^"\']*(?:variant|attribute)-value[^"\']*["\'][^>]*>(.*?)</(?:span|div)>',
                document,
                re.IGNORECASE | re.DOTALL,
            )
            if _clean(match.group(1)) and _clean(match.group(2))
        }
        output: list[CommerceProductSnapshot] = []
        for snapshot in snapshots:
            variants = []
            for variant in snapshot.variants:
                offers = tuple(
                    offer.model_copy(update={"vat_status": vat}) if vat else offer
                    for offer in variant.offers
                )
                published = {**variant.published_attributes, **attributes}
                if vat:
                    published["vat_basis"] = "page_markup"
                variants.append(
                    variant.model_copy(
                        update={"offers": offers, "published_attributes": published}
                    )
                )
            output.append(snapshot.model_copy(update={"variants": tuple(variants)}))
        return tuple(output)


class NitroSellConnector(SpecializedPageConnector):
    name = "nitrosell"
    platform = "nitrosell"

    def __init__(
        self,
        transport: CommerceTransport,
        options: NitroSellOptions | None = None,
        context: ConnectorContext | None = None,
    ) -> None:
        super().__init__(transport, options or NitroSellOptions(), context)

    def parse(
        self, document: str, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        snapshots = super().parse(document, url, source_id)
        if not snapshots:
            return ()
        list_price = _decimal(_class_content(document, "text-pricestrike"))
        description = _class_content(document, "product-description")
        breadcrumb = re.search(
            r'<ol[^>]*class=["\'][^"\']*breadcrumb[^"\']*["\'][^>]*>(.*?)</ol>',
            document,
            re.IGNORECASE | re.DOTALL,
        )
        categories: tuple[CategoryRef, ...] = ()
        if breadcrumb:
            crumbs = tuple(
                _clean(value)
                for value in re.findall(
                    r"<li[^>]*>(.*?)</li>", breadcrumb.group(1), re.IGNORECASE | re.DOTALL
                )
                if _clean(value)
            )
            categories = tuple(CategoryRef(name=value) for value in crumbs[1:-1])
        image_values = [_meta(document, "og:image")]
        image_values.extend(
            re.findall(
                r'https://cdn\.powered-by-nitrosell\.com/product_images/[^"\'\s<>]+',
                document,
                re.IGNORECASE,
            )
        )
        images = tuple(
            MediaRef(url=urljoin(url, value))
            for value in dict.fromkeys(value for value in image_values if value and "/thumb" not in value)
        )
        output: list[CommerceProductSnapshot] = []
        for snapshot in snapshots:
            variants = []
            for variant in snapshot.variants:
                offers = list(variant.offers)
                if list_price is not None and offers and list_price != offers[0].price.amount:
                    current = offers[0].model_copy(update={"role": "sale"})
                    offers = [
                        current,
                        current.model_copy(
                            update={
                                "role": "regular",
                                "price": Money(
                                    amount=list_price,
                                    currency=current.price.currency,
                                ),
                            }
                        ),
                    ]
                variants.append(variant.model_copy(update={"offers": tuple(offers)}))
            output.append(
                snapshot.model_copy(
                    update={
                        "description": description or snapshot.description,
                        "categories": categories or snapshot.categories,
                        "images": images or snapshot.images,
                        "variants": tuple(variants),
                    }
                )
            )
        return tuple(output)


_FLIGHT_CHUNK = re.compile(r"self\.__next_f\.push\((\[.*?\])\)</script>", re.DOTALL)
_PRODUCT_MARKER = re.compile(r'"product":\{"id":"[0-9a-f-]{8}-')
_CURRENCY = re.compile(r'"currency":"([A-Z]{3})"')


class SumUpConnector(SpecializedPageConnector):
    name = "sumup"
    platform = "sumup"

    def __init__(
        self,
        transport: CommerceTransport,
        options: SumUpOptions | None = None,
        context: ConnectorContext | None = None,
    ) -> None:
        super().__init__(transport, options or SumUpOptions(), context)

    def parse(
        self, document: str, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        payload = _flight_payload(document)
        product = self._product(payload, url) if payload else None
        if product is None:
            return super().parse(document, url, source_id)
        title = _clean(product.get("name")) or _meta(document, "og:title")
        currency_match = _CURRENCY.search(payload)
        currency = currency_match.group(1) if currency_match else self.options.currency
        if not title or not currency:
            return ()
        observed_at = self.context.clock()
        evidence = Evidence(
            method="html",
            source_url=url,
            source_field="next_rsc",
            observed_at=observed_at,
        )
        raw_variants = product.get("variants")
        candidates = (
            [
                {
                    **value,
                    "uuid": value.get("uuid") or key,
                    "name": value.get("name"),
                    "sku": value.get("sku"),
                    "price": value.get("price", product.get("price")),
                    "basePrice": value.get("basePrice", product.get("basePrice")),
                    "hasDiscount": value.get("hasDiscount", product.get("hasDiscount")),
                    "isAvailable": value.get(
                        "isAvailable", product.get("isAvailable", True)
                    ),
                    "options": value.get("options"),
                }
                for key, value in raw_variants.items()
                if isinstance(value, dict) and value
            ]
            if isinstance(raw_variants, dict)
            else []
        )
        if not candidates:
            candidates = [{**product, "uuid": product.get("id")}]
        variants: list[CommerceVariant] = []
        for candidate in candidates:
            amount = self._amount(candidate.get("price", product.get("price")))
            if amount is None:
                continue
            available = (
                Availability.IN_STOCK
                if candidate.get("isAvailable", True)
                else Availability.OUT_OF_STOCK
            )
            quantity = self._quantity(candidate, product)
            stock = StockState(
                availability=available,
                quantity=quantity,
                quantity_kind=(
                    StockQuantityKind.EXACT
                    if quantity is not None
                    else StockQuantityKind.UNKNOWN
                ),
                observed_at=observed_at,
                evidence=(evidence,),
            )
            role: Literal["regular", "sale"] = (
                "sale"
                if candidate.get("hasDiscount", product.get("hasDiscount"))
                else "regular"
            )
            current = CommerceOffer(
                price=Money(amount=amount, currency=currency),
                observed_at=observed_at,
                evidence=(evidence,),
                role=role,
                vat_status=self.options.vat_status or "unknown",
                vat_rate=self.options.vat_rate,
                availability=available,
                availability_evidence=(evidence,),
            )
            offers = [current]
            base_amount = self._amount(
                candidate.get("basePrice", product.get("basePrice"))
            )
            if role == "sale" and base_amount is not None and base_amount != amount:
                offers.append(
                    current.model_copy(
                        update={
                            "role": "regular",
                            "price": Money(amount=base_amount, currency=currency),
                        }
                    )
                )
            variants.append(
                CommerceVariant(
                    external_id=str(candidate.get("uuid") or product.get("id")),
                    canonical_url=url,
                    title=_clean(candidate.get("name")) or None,
                    sku=_clean(candidate.get("sku") or product.get("sku")) or None,
                    offers=tuple(offers),
                    stock=stock,
                    platform_extensions={"legacy_raw_variant": candidate},
                )
            )
        if not variants:
            return ()
        images = tuple(
            MediaRef(url=value)
            for value in dict.fromkeys(
                _clean(value)
                for value in (product.get("allImages") or [product.get("image")])
                if _clean(value)
            )
        )
        category_raw = product.get("category")
        category = _clean(category_raw.get("name")) if isinstance(category_raw, dict) else ""
        return (
            CommerceProductSnapshot(
                connector=self.name,
                source_id=source_id,
                external_id=str(
                    product.get("id") or hashlib.sha256(url.encode()).hexdigest()[:24]
                ),
                canonical_url=url,
                title=title,
                observed_at=observed_at,
                description=_clean(product.get("description")) or None,
                vendor=self.options.brand,
                categories=(CategoryRef(name=category),) if category else (),
                images=images,
                variants=tuple(variants),
                platform_extensions={
                    "legacy_raw_product": {
                        key: value for key, value in product.items() if key != "variants"
                    }
                },
            ),
        )

    @staticmethod
    def _amount(value: Any) -> Decimal | None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return Decimal(str(value)) / Decimal(100)

    @staticmethod
    def _quantity(variant: dict[str, Any], product: dict[str, Any]) -> int | None:
        if variant.get("isAvailable") is False:
            return 0
        tracking = variant.get("isTrackingEnabled", product.get("isTrackingEnabled"))
        quantity = variant.get("quantity")
        if (
            tracking is True
            and isinstance(quantity, int)
            and not isinstance(quantity, bool)
            and quantity >= 0
        ):
            return quantity
        return None

    @staticmethod
    def _product(payload: str, url: str) -> dict[str, Any] | None:
        slug = unquote(urlparse(url).path.rstrip("/").rsplit("/", 1)[-1])
        detailed: list[dict[str, Any]] = []
        for match in _PRODUCT_MARKER.finditer(payload):
            product = _balanced_object(payload, match.start() + len('"product":'))
            if product is None:
                continue
            if slug and product.get("slug") == slug:
                return product
            variants = product.get("variants")
            if isinstance(variants, dict) and any(
                isinstance(value, dict) and value for value in variants.values()
            ):
                detailed.append(product)
        return detailed[0] if len(detailed) == 1 else None


class _SpecializedFactory:
    name: str
    version: str
    options_model: type[BaseModel]
    connector_type: type[SpecializedPageConnector]

    def build(
        self,
        *,
        transport: CommerceTransport,
        options: BaseModel,
        context: ConnectorContext,
    ) -> CommerceConnector:
        return self.connector_type(
            transport,
            cast(SpecializedPageOptions, self.options_model.model_validate(options)),
            context,
        )


class ShopwareFactory(_SpecializedFactory):
    name = "shopware"
    version = ShopwareConnector.version
    options_model: type[BaseModel] = ShopwareOptions
    connector_type = ShopwareConnector


class StarwebFactory(_SpecializedFactory):
    name = "starweb"
    version = StarwebConnector.version
    options_model: type[BaseModel] = StarwebOptions
    connector_type = StarwebConnector


class NitroSellFactory(_SpecializedFactory):
    name = "nitrosell"
    version = NitroSellConnector.version
    options_model: type[BaseModel] = NitroSellOptions
    connector_type = NitroSellConnector


class SumUpFactory(_SpecializedFactory):
    name = "sumup"
    version = SumUpConnector.version
    options_model: type[BaseModel] = SumUpOptions
    connector_type = SumUpConnector


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", str(value)))).strip()


def _meta(document: str, key: str) -> str:
    escaped = re.escape(key)
    for pattern in (
        rf'<meta[^>]*(?:property|name)=["\']{escaped}["\'][^>]*content=["\']([^"\']*)',
        rf'<meta[^>]*content=["\']([^"\']*)["\'][^>]*(?:property|name)=["\']{escaped}["\']',
    ):
        match = re.search(pattern, document, re.IGNORECASE)
        if match:
            return _clean(match.group(1))
    return ""


def _class_content(document: str, class_name: str) -> str:
    match = re.search(
        rf'<[^>]+class=["\'][^"\']*\b{re.escape(class_name)}\b[^"\']*["\'][^>]*>(.*?)</[^>]+>',
        document,
        re.IGNORECASE | re.DOTALL,
    )
    return _clean(match.group(1)) if match else ""


def _definition_attributes(document: str) -> dict[str, JsonValue]:
    return {
        _clean(match.group(1)).rstrip(":"): _clean(match.group(2))
        for match in re.finditer(
            r'<dt[^>]*class=["\'][^"\']*properties-label[^"\']*["\'][^>]*>(.*?)</dt>\s*'
            r'<dd[^>]*class=["\'][^"\']*properties-value[^"\']*["\'][^>]*>(.*?)</dd>',
            document,
            re.IGNORECASE | re.DOTALL,
        )
        if _clean(match.group(1)) and _clean(match.group(2))
    }


def _decimal(value: str) -> Decimal | None:
    match = re.search(r"\d+(?:[.,]\d+)?", value.replace(",", "."))
    if not match:
        return None
    try:
        return Decimal(match.group())
    except InvalidOperation:
        return None


def _availability(value: str) -> Availability:
    normalized = value.rsplit("/", 1)[-1].replace("_", "").casefold()
    return {
        "instock": Availability.IN_STOCK,
        "available": Availability.IN_STOCK,
        "outofstock": Availability.OUT_OF_STOCK,
        "soldout": Availability.OUT_OF_STOCK,
        "backorder": Availability.BACKORDER,
        "preorder": Availability.PREORDER,
    }.get(normalized, Availability.UNKNOWN)


def _flight_payload(document: str) -> str:
    parts: list[str] = []
    for raw in _FLIGHT_CHUNK.findall(document):
        try:
            chunk = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if len(chunk) > 1 and isinstance(chunk[1], str):
            parts.append(chunk[1])
    return "".join(parts)


def _balanced_object(payload: str, start: int) -> dict[str, Any] | None:
    decoder = json.JSONDecoder()
    try:
        value, _ = decoder.raw_decode(payload[start:])
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _links(document: str, page_url: str, *, cards_only: bool) -> tuple[str, ...]:
    origin = urlparse(page_url).netloc
    scope = document
    if cards_only:
        cards = re.findall(
            r'<(?:article|li|div)[^>]*class=["\'][^"\']*'
            r'(?:product-miniature|product-item|product-card|productbox)[^"\']*["\']*'
            r'[\s\S]*?</(?:article|li|div)>',
            document,
            re.IGNORECASE,
        )
        pagination = re.findall(
            r'<a[^>]+(?:rel=["\']next["\']|class=["\'][^"\']*'
            r'(?:next|pagination)[^"\']*["\'])[^>]*>',
            document,
            re.IGNORECASE,
        )
        scope = "".join((*cards, *pagination)) or document
    found: list[str] = []
    for match in re.finditer(r'href=["\']([^"\']+)["\']', scope, re.IGNORECASE):
        candidate = urljoin(page_url, unescape(match.group(1)))
        if urlparse(candidate).netloc == origin and candidate not in found:
            found.append(candidate)
    return tuple(found)
