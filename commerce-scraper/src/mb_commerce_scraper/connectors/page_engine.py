"""Shared bounded discovery and parsing engine for page-based connectors."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from decimal import Decimal
from html import unescape
from typing import Any, Literal, cast
from urllib.parse import urljoin, urlparse

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from mb_commerce_scraper.discovery import (
    DiscoveryFailure,
    SitemapDiscovery,
    advertised_sitemaps,
)
from mb_commerce_scraper.models import (
    CollectionRequest,
    CommerceProductSnapshot,
    ConnectorCheckpoint,
    Diagnostic,
    DiagnosticCode,
    DiagnosticSeverity,
    DocumentRef,
    EntityPage,
    Evidence,
    RefreshMode,
    SnapshotField,
    StockQuantityKind,
    build_checkpoint,
    result_limit_diagnostic,
    sanitize_commerce_snapshot,
)
from mb_commerce_scraper.parsing import JsonLdProductParser
from mb_commerce_scraper.parsing._structured import (
    DomFieldSelector,
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

from .base import (
    BrowserRequirement,
    CommerceConnector,
    ConnectorCapabilities,
    ConnectorContext,
    validate_connector_request,
)


class DiscoveryOptions(BaseModel):
    """Safe, bounded product-page discovery configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    sitemaps: tuple[str, ...] = ()
    use_advertised_sitemaps: bool = True
    category_urls: tuple[str, ...] = ()
    product_pattern: str | None = None
    pagination_patterns: tuple[str, ...] = ()
    card_links_only: bool = False
    sitemap_limit: int = Field(default=100, ge=1, le=10_000)
    category_page_limit: int = Field(default=120, ge=1, le=10_000)


class DomRules(BaseModel):
    """Verified, data-only selectors that never enter a CSS/JS engine."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    verification: tuple[str | DomFieldSelector, ...] = ()
    name: str | DomFieldSelector
    price: str | DomFieldSelector | None = None
    currency: str | DomFieldSelector | None = None
    description: str | DomFieldSelector | None = None
    sku: str | DomFieldSelector | None = None
    image: str | DomFieldSelector | None = None
    availability: str | DomFieldSelector | None = None

    @model_validator(mode="after")
    def _selectors_are_supported(self) -> DomRules:
        self.as_verified()
        return self

    def as_verified(self) -> VerifiedDomRules:
        name = _dom_selector(self.name)
        verification = tuple(_dom_selector(value) for value in self.verification)
        return VerifiedDomRules(
            verification=verification or (name,),
            name=name,
            price=_optional_dom_selector(self.price),
            currency=_optional_dom_selector(self.currency),
            description=_optional_dom_selector(self.description),
            sku=_optional_dom_selector(self.sku),
            image=_optional_dom_selector(self.image),
            availability=_optional_dom_selector(self.availability),
        )


class PageEngineOptions(BaseModel):
    """Bounded, data-only page discovery and platform parsing options."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    discovery: DiscoveryOptions = Field(default_factory=DiscoveryOptions)
    page_limit: int = Field(default=10_000, ge=1)
    render: bool | None = None
    browser_zero_gain_limit: int = Field(default=10, ge=1)
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld",
        "opengraph",
    )
    dom_rules: DomRules | None = None
    currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    brand: str | None = None
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None = None
    vat_rate: Decimal | None = Field(default=None, ge=0, le=1)
    stock_from_quantity_maximum: bool = False

    @model_validator(mode="after")
    def _dom_parser_is_explicit(self) -> PageEngineOptions:
        if "dom" in self.parsers and self.dom_rules is None:
            raise ValueError("the dom parser requires dom_rules")
        if self.dom_rules is not None and "dom" not in self.parsers:
            raise ValueError("dom_rules require the dom parser")
        return self


def _dom_selector(value: str | DomFieldSelector) -> DomFieldSelector:
    return value if isinstance(value, DomFieldSelector) else DomFieldSelector(selector=value)


def _optional_dom_selector(
    value: str | DomFieldSelector | None,
) -> DomFieldSelector | None:
    return _dom_selector(value) if value is not None else None


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


class PageEngineConnector(CommerceConnector):
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
        options: PageEngineOptions,
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
        validate_connector_request(
            capabilities=self.capabilities,
            unsupported_message=(
                f"{self.name} does not support the requested collection contract"
            ),
            connector=self.name,
            connector_version=self.version,
            request=request,
            checkpoint=checkpoint,
            options=options,
            capabilities_checked=True,
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
            document = response.text()
            snapshots = self.parse(document, url, request.source_id)
            shell = not snapshots and probable_javascript_shell(document)
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
                    dom_product(
                        document,
                        self.options.dom_rules.as_verified(),
                        self.options.currency,
                    )
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
        return build_checkpoint(
            connector=self.name,
            connector_version=self.version,
            request=request,
            lineage=lineage,
            resume_after=resume_after,
            options=self._checkpoint_options(),
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
        snapshots = self._jsonld.parse_products(raw_items, url=url, source_id=source_id)
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
        discovery_options = self.options.discovery
        if discovery_options.category_urls:
            async for url in self._category_discover(base_url):
                if self.context.cancelled():
                    return
                yield url
            return
        roots = discovery_options.sitemaps
        if not roots and discovery_options.use_advertised_sitemaps:
            roots = await advertised_sitemaps(self.transport, base_url)
        if not roots:
            roots = ("/sitemap.xml",)
        discovery = SitemapDiscovery(
            self.transport,
            roots,
            product_pattern=None,
            limit=discovery_options.sitemap_limit,
        )
        async for url in discovery.discover(base_url):
            if self.context.cancelled():
                return
            if self._is_product(url, base_url):
                yield url

    async def _category_discover(self, base_url: str) -> AsyncIterator[str]:
        discovery_options = self.options.discovery
        queue = [urljoin(base_url, value) for value in discovery_options.category_urls]
        seen: set[str] = set()
        emitted: set[str] = set()
        while queue:
            if self.context.cancelled():
                return
            url = queue.pop(0)
            if url in seen:
                continue
            if len(seen) >= discovery_options.category_page_limit:
                raise DiscoveryFailure(
                    f"category page limit {discovery_options.category_page_limit} reached"
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
                response.text(), url, cards_only=discovery_options.card_links_only
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
        product_pattern = self.options.discovery.product_pattern
        if product_pattern is None:
            return True
        pattern = re.compile(product_pattern)
        return bool(pattern.search(urlparse(url).path) or pattern.search(url))

    def _is_pagination(self, url: str) -> bool:
        pagination_patterns = self.options.discovery.pagination_patterns
        if pagination_patterns:
            return any(re.search(pattern, url) for pattern in pagination_patterns)
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
        return "category" if self.options.discovery.category_urls else "sitemap"
