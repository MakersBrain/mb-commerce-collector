"""PrestaShop product-page connector producing neutral commerce snapshots."""

from __future__ import annotations

import hashlib
import html
import json
import re
from collections.abc import AsyncIterator, Iterable
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, cast
from urllib.parse import urlencode, urljoin, urlparse, urlunparse

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
    build_checkpoint,
    result_limit_diagnostic,
)
from mb_commerce_scraper.parsing._structured import (
    breadcrumbs,
    hashed_page_id,
    jsonld_brand,
    jsonld_gtin,
    jsonld_images,
    meta,
    pdf_links,
    probable_javascript_shell,
    specification_table,
    stable_digest,
)
from mb_commerce_scraper.parsing._structured import (
    clean as compatibility_clean,
)
from mb_commerce_scraper.parsing._structured import (
    jsonld_product_blocks as jsonld_products,
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

from .base import (
    BrowserRequirement,
    CommerceConnector,
    ConnectorCapabilities,
    ConnectorContext,
    validate_connector_request,
)
from .factory import SimpleConnectorFactory


class PrestaShopOptions(BaseModel):
    """Connector-owned projection of the existing page-crawl source keys."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    sitemaps: tuple[str, ...] = ()
    category_urls: tuple[str, ...] = ()
    use_advertised_sitemaps: bool = True
    product_pattern: str | None = None
    card_links_only: bool = False
    pagination_patterns: tuple[str, ...] = ()
    render: bool | None = None
    variant_combinations: bool = True
    currency: str = Field(default="EUR", pattern=r"^[A-Z]{3}$")
    brand: str | None = None
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None = None
    vat_rate: Decimal | None = Field(default=None, ge=0, le=1)
    page_limit: int = Field(default=500, ge=1)
    category_page_limit: int = Field(default=120, ge=1)
    sitemap_page_limit: int = Field(default=500, ge=1)
    combination_limit: int = Field(default=30, ge=1)


class _BoundReached(RuntimeError):
    pass


class _HTTPStatusFailure(RuntimeError):
    pass


class _DiscoveryFailure(RuntimeError):
    pass


def jsonld_offer(item: dict[str, Any]) -> dict[str, Any]:
    offers = item.get("offers")
    if isinstance(offers, list):
        offers = next((value for value in offers if isinstance(value, dict)), {})
    return offers if isinstance(offers, dict) else {}


def documents(links: Iterable[tuple[str, str]], page_url: str = "") -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for raw_url, raw_label in links:
        url = urljoin(page_url, compatibility_clean(raw_url))
        label = compatibility_clean(raw_label)
        if url and url not in {str(value["url"]) for value in found}:
            found.append({"name": label or None, "url": url})
    return found


class PrestaShopConnector(CommerceConnector):
    name = "prestashop"
    platform = "prestashop"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.EXACT}),
        supports_incremental_cursor=False,
        supports_category_filter=False,
        supports_documents=True,
        browser=BrowserRequirement.OPTIONAL,
    )

    def __init__(
        self,
        transport: CommerceTransport,
        options: PrestaShopOptions | None = None,
        context: ConnectorContext | None = None,
    ) -> None:
        self.transport = transport
        self.options = options or PrestaShopOptions()
        self.context = context or ConnectorContext()
        self._requests = 0

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        self._validate_request(request, checkpoint)
        self._requests = 0
        if self.context.cancelled():
            return
        try:
            partitions = await self._discover(request)
        except _BoundReached:
            yield self._failure_page(
                "discovery",
                0,
                {"partition": "discovery", "offset": 0, "sequence": 0},
                DiagnosticCode.ENUMERATION_INCOMPLETE,
                "PrestaShop discovery exceeded its configured request bound",
                request.base_url,
            )
            return
        except (
            ResponseBodyTooLarge,
            _DiscoveryFailure,
            _HTTPStatusFailure,
            TransportFailure,
        ) as error:
            yield self._failure_page(
                "discovery",
                0,
                {"partition": "discovery", "offset": 0, "sequence": 0},
                DiagnosticCode.ENUMERATION_INCOMPLETE,
                f"PrestaShop discovery failed: {type(error).__name__}",
                request.base_url,
                retryable=not isinstance(error, ResponseBodyTooLarge),
            )
            return

        if not partitions:
            yield EntityPage(
                page_id="discovery:empty",
                partition_key="discovery",
                sequence=0,
                items=(),
                terminal=True,
                enumeration_intact=True,
                discovered=0,
            )
            return

        resume = self._resume(checkpoint)
        keys = [key for key, _ in partitions]
        if resume is not None and resume[0] not in keys:
            raise ValueError("CHECKPOINT_INVALID: PrestaShop partition is not requested")
        if resume is not None:
            partition_size = len(partitions[keys.index(resume[0])][1])
            if resume[1] >= partition_size:
                raise ValueError("CHECKPOINT_INVALID: PrestaShop offset is out of range")
        start_partition = keys.index(resume[0]) if resume else 0
        sequence = resume[2] if resume else 0
        selected: list[tuple[str, int, str]] = []
        for partition_index, (partition, urls) in enumerate(partitions):
            if partition_index < start_partition:
                continue
            offset = resume[1] if resume and partition_index == start_partition else 0
            for item_offset, url in enumerate(urls[offset:], offset):
                selected.append((partition, item_offset, url))
        maximum = self.options.page_limit
        caller_limit = False
        if request.result_limit is not None:
            maximum = min(maximum, request.result_limit)
            caller_limit = request.result_limit <= self.options.page_limit
        limited = len(selected) > maximum
        selected = selected[:maximum]

        if not selected:
            yield EntityPage(
                page_id="discovery:empty",
                partition_key="discovery",
                sequence=sequence,
                items=(),
                terminal=True,
                partition_terminal=True,
                enumeration_intact=True,
                discovered=0,
            )
            return

        for index, (partition, offset, url) in enumerate(selected):
            if self.context.cancelled():
                return
            try:
                document, method = await self._product_document(url, request)
            except (
                ResponseBodyTooLarge,
                _BoundReached,
                _HTTPStatusFailure,
                TransportFailure,
            ) as error:
                yield self._failure_page(
                    partition,
                    sequence,
                    {"partition": partition, "offset": offset, "sequence": sequence},
                    DiagnosticCode.ENTITY_FETCH_FAILED,
                    f"PrestaShop product fetch failed: {type(error).__name__}",
                    url,
                    retryable=not isinstance(error, ResponseBodyTooLarge),
                )
                return

            diagnostics: list[Diagnostic] = []
            snapshots: tuple[CommerceProductSnapshot, ...] = ()
            details = data_product(document)
            if details is not None:
                variants = [details]
                if self.options.variant_combinations:
                    extras, notices = await self._combinations(document, url, details, request)
                    variants.extend(extras)
                    diagnostics.extend(notices)
                snapshot = self._normalize(variants, document, url, request.source_id, method)
                if snapshot is not None:
                    snapshots = (snapshot,)
            else:
                snapshot = self._normalize_jsonld(document, url, request.source_id, method)
                if snapshot is not None:
                    snapshots = (snapshot,)
                else:
                    diagnostics.append(
                        Diagnostic(
                            code=DiagnosticCode.PARSER_UNSUPPORTED,
                            severity=DiagnosticSeverity.WARNING,
                            message="PrestaShop page exposed no supported product payload",
                            retryable=False,
                            affects_completeness=False,
                            url=url,
                        )
                    )

            next_cursor = self._next_cursor(partitions, partition, offset, sequence)
            final = index == len(selected) - 1
            collection_terminal = final and not limited
            if final and limited:
                if caller_limit and request.result_limit is not None:
                    diagnostics.append(result_limit_diagnostic(request.result_limit, url))
                else:
                    diagnostics.append(
                        Diagnostic(
                            code=DiagnosticCode.ENUMERATION_INCOMPLETE,
                            severity=DiagnosticSeverity.WARNING,
                            message=f"PrestaShop product limit {maximum} reached",
                            retryable=False,
                            affects_completeness=True,
                            url=url,
                        )
                    )
            yield EntityPage(
                page_id=hashed_page_id(
                    f"{stable_digest(partition, 8)}:{offset}",
                    url,
                ),
                partition_key=partition,
                sequence=sequence,
                items=snapshots,
                resume_after=None if collection_terminal else next_cursor,
                terminal=final,
                partition_terminal=(
                    final or selected[index + 1][0] != partition
                ),
                enumeration_intact=not (final and limited),
                discovered=1,
                diagnostics=tuple(diagnostics),
            )
            sequence += 1

    async def _discover(self, request: CollectionRequest) -> list[tuple[str, list[str]]]:
        sitemap_roots = list(self.options.sitemaps)
        if not sitemap_roots and self.options.use_advertised_sitemaps:
            robots = await self._fetch(
                urljoin(request.base_url, "/robots.txt"),
                request,
                priority=RequestPriority.DISCOVERY,
                purpose=RequestPurpose.ROBOTS,
            )
            sitemap_roots = re.findall(r"^\s*Sitemap:\s*(\S+)", robots, re.I | re.M)
        sitemap_partitions: list[tuple[str, list[str]]] = []
        for index, root in enumerate(dict.fromkeys(sitemap_roots)):
            urls = await self._sitemap_urls(root, request)
            sitemap_partitions.append((_partition_key("sitemap", index, root), urls))
        if any(urls for _, urls in sitemap_partitions):
            return _deduplicate_partitions(sitemap_partitions)
        if self.options.sitemaps and not self.options.category_urls:
            raise _DiscoveryFailure("configured sitemaps yielded no product URLs")

        roots = list(self.options.category_urls or (request.base_url,))
        keyed_roots = [(_partition_key("category", index, root), root) for index, root in enumerate(roots)]
        category_partitions: dict[str, list[str]] = {key: [] for key, _ in keyed_roots}
        queue = list(keyed_roots)
        seen_pages: set[str] = set()
        while queue and len(seen_pages) < self.options.category_page_limit:
            partition, page_url = queue.pop(0)
            if page_url in seen_pages:
                continue
            seen_pages.add(page_url)
            document = await self._fetch(
                page_url, request, priority=RequestPriority.DISCOVERY
            )
            for link in self._links(document, page_url):
                if self._is_product_url(link):
                    if link not in category_partitions[partition]:
                        category_partitions[partition].append(link)
                elif self._is_pagination(link) and link not in seen_pages:
                    queue.append((partition, link))
        if queue:
            raise _BoundReached("category page limit")
        return _deduplicate_partitions(list(category_partitions.items()))

    async def _sitemap_urls(self, root: str, request: CollectionRequest) -> list[str]:
        origin = urlparse(request.base_url).netloc
        queue = [root]
        seen: set[str] = set()
        found: list[str] = []
        while queue:
            if len(seen) >= self.options.sitemap_page_limit:
                raise _BoundReached("sitemap page limit")
            url = queue.pop(0)
            if url in seen:
                continue
            seen.add(url)
            document = await self._fetch(
                url,
                request,
                accept="application/xml,text/xml",
                priority=RequestPriority.DISCOVERY,
            )
            locations = [
                compatibility_clean(value)
                for value in re.findall(
                    r"<loc>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</loc>", document, re.I
                )
            ]
            if re.search(r"<sitemapindex\b", document, re.I):
                queue.extend(value for value in locations if value)
            else:
                found.extend(
                    value
                    for value in locations
                    if value and urlparse(value).netloc == origin and self._is_product_url(value)
                )
        return list(dict.fromkeys(found))

    async def _product_document(
        self, url: str, request: CollectionRequest
    ) -> tuple[str, Literal["html", "browser"]]:
        if self.options.render is True:
            return await self._fetch(
                url, request, rendered=True, priority=RequestPriority.IDENTITY
            ), "browser"
        try:
            document = await self._fetch(url, request, priority=RequestPriority.IDENTITY)
        except ResponseBodyTooLarge:
            raise
        except TransportFailure:
            if self.options.render is False:
                raise
            return await self._fetch(
                url, request, rendered=True, priority=RequestPriority.IDENTITY
            ), "browser"
        if (
            self.options.render is None
            and data_product(document) is None
            and probable_javascript_shell(document)
        ):
            return await self._fetch(
                url, request, rendered=True, priority=RequestPriority.IDENTITY
            ), "browser"
        return document, "html"

    async def _fetch(
        self,
        url: str,
        request: CollectionRequest,
        *,
        rendered: bool = False,
        accept: str | None = None,
        priority: RequestPriority,
        purpose: RequestPurpose | None = None,
    ) -> str:
        self._requests += 1
        response = await self.transport.request(
            TransportRequest(
                url=url,
                headers={"accept": accept} if accept else {},
                purpose=purpose
                or (
                    RequestPurpose.DISCOVERY
                    if priority == RequestPriority.DISCOVERY
                    else RequestPurpose.ENTITY
                ),
                priority=priority,
                estimated_bytes=1_000_000,
                browser=BrowserHint.REQUIRED if rendered else BrowserHint.NEVER,
            )
        )
        if response.status >= 400:
            raise _HTTPStatusFailure(
                f"PrestaShop fetch failed with status {response.status}"
            )
        return response.text()

    async def _combinations(
        self,
        document: str,
        url: str,
        details: dict[str, Any],
        request: CollectionRequest,
    ) -> tuple[list[dict[str, Any]], list[Diagnostic]]:
        groups = variant_groups(document)
        wanted: list[str] = []
        base = compatibility_clean(details.get("link")) or url
        for group_id, group in groups.items():
            for option in group["options"]:
                if option == group["selected"]:
                    continue
                query = {f"group[{other}]": data["selected"] for other, data in groups.items()}
                query[f"group[{group_id}]"] = option
                query |= {"ajax": "1", "action": "refresh", "quantity_wanted": "1"}
                separator = "&" if urlparse(base).query else "?"
                wanted.append(f"{base}{separator}{urlencode(query)}")
        notices: list[Diagnostic] = []
        if len(wanted) > self.options.combination_limit:
            wanted = wanted[: self.options.combination_limit]
            notices.append(
                Diagnostic(
                    code=DiagnosticCode.OPTIONAL_ENRICHMENT_SKIPPED,
                    severity=DiagnosticSeverity.WARNING,
                    message=f"PrestaShop combination limit {self.options.combination_limit} reached",
                    retryable=False,
                    affects_completeness=False,
                    url=url,
                )
            )
        seen = {str(details.get("id_product_attribute") or "")}
        found: list[dict[str, Any]] = []
        priority = (
            RequestPriority.DATASET_REQUIRED
            if request.requested_fields
            & frozenset({SnapshotField.VARIANTS, SnapshotField.OFFERS, SnapshotField.STOCK})
            else RequestPriority.OPTIONAL
        )
        for target in wanted:
            try:
                body = await self._fetch(target, request, priority=priority)
                payload = json.loads(body)
            except (
                _BoundReached,
                _HTTPStatusFailure,
                ResponseBodyTooLarge,
                TransportFailure,
                json.JSONDecodeError,
            ) as error:
                notices.append(
                    Diagnostic(
                        code=DiagnosticCode.OPTIONAL_ENRICHMENT_SKIPPED,
                        severity=DiagnosticSeverity.WARNING,
                        message=f"PrestaShop combination fetch skipped: {type(error).__name__}",
                        retryable=isinstance(error, (_HTTPStatusFailure, TransportFailure)),
                        affects_completeness=False,
                        url=_safe_url(target),
                    )
                )
                if isinstance(error, _BoundReached):
                    break
                continue
            if not isinstance(payload, dict):
                continue
            extra = data_product(payload.get("product_details") or "")
            if extra is None:
                continue
            identifier = str(extra.get("id_product_attribute") or "")
            if identifier in seen:
                continue
            seen.add(identifier)
            extra.setdefault("link", compatibility_clean(payload.get("product_url")) or url)
            found.append(extra)
        return found, notices

    def _normalize(
        self,
        details_list: list[dict[str, Any]],
        document: str,
        fetched_url: str,
        source_id: str,
        method: Literal["html", "browser"],
    ) -> CommerceProductSnapshot | None:
        primary = details_list[0]
        name = compatibility_clean(primary.get("name"))
        if not name:
            return None
        observed_at = self.context.clock()
        parent_url = _canonical(fetched_url)
        product_url = _canonical(compatibility_clean(primary.get("link")) or fetched_url)
        evidence = Evidence(method=method, source_url=product_url, observed_at=observed_at)
        features = _features(primary)
        specifications = specification_table(document)
        categories = breadcrumbs(document) or [compatibility_clean(primary.get("category_name"))]
        category_refs = tuple(CategoryRef(name=value) for value in categories if value)
        brand = (
            compatibility_clean(primary.get("manufacturer_name"))
            or features.get("Brand")
            or self.options.brand
        )
        images = _images(primary)
        documents = tuple(
            DocumentRef(
                url=item["url"],
                title=item.get("name"),
                observed_at=observed_at,
                evidence=(evidence,),
            )
            for item in _attachments(primary, document, fetched_url)
        )
        variants = tuple(
            variant
            for details in details_list
            if (
                variant := self._variant(
                    details,
                    product_url,
                    brand,
                    evidence,
                    observed_at,
                )
            )
            is not None
        )
        if not variants:
            return None
        attributes = cast(dict[str, JsonValue], {**features, **specifications})
        return CommerceProductSnapshot(
            connector=self.name,
            source_id=source_id,
            external_id=str(primary.get("id_product") or product_url),
            canonical_url=parent_url,
            title=name,
            description=compatibility_clean(primary.get("description"))
            or compatibility_clean(primary.get("description_short")),
            vendor=brand,
            observed_at=observed_at,
            categories=category_refs,
            images=tuple(MediaRef(url=value) for value in images),
            documents=documents,
            variants=variants,
            published_attributes=attributes,
            platform_extensions={"legacy_record_name": name},
        )

    def _variant(
        self,
        details: dict[str, Any],
        product_url: str,
        brand: str | None,
        evidence: Evidence,
        observed_at: datetime,
    ) -> CommerceVariant | None:
        price, currency = _price(details.get("price_amount") or details.get("price"))
        if price is None:
            return None
        currency = currency or self.options.currency
        regular, _ = _price(details.get("regular_price_amount") or details.get("regular_price"))
        quantity_value = details.get("quantity")
        quantity = (
            max(quantity_value, 0)
            if isinstance(quantity_value, int) and not isinstance(quantity_value, bool)
            else None
        )
        availability = Availability.IN_STOCK if (quantity_value or 0) > 0 else Availability.OUT_OF_STOCK
        current = CommerceOffer(
            price=Money(amount=price, currency=currency),
            role="sale" if regular is not None and regular != price else "regular",
            vat_status=self.options.vat_status or "unknown",
            vat_rate=self.options.vat_rate,
            availability=availability,
            availability_evidence=(evidence,),
            observed_at=observed_at,
            evidence=(evidence,),
        )
        offers = [current]
        if regular is not None and regular != price:
            offers.append(
                CommerceOffer(
                    price=Money(amount=regular, currency=currency),
                    role="regular",
                    vat_status=self.options.vat_status or "unknown",
                    vat_rate=self.options.vat_rate,
                    availability=availability,
                    availability_evidence=(evidence,),
                    observed_at=observed_at,
                    evidence=(evidence,),
                )
            )
        combination = _combination(details)
        identifier = str(details.get("id_product_attribute") or "")
        attributes: dict[str, JsonValue] = {
            "price_text": compatibility_clean(details.get("price")) or None,
            "legacy_source_updated_at": compatibility_clean(details.get("date_upd")) or None,
        }
        image_values = _images(details)
        return CommerceVariant(
            external_id=identifier or "default",
            is_default=not bool(identifier),
            canonical_url=_canonical(compatibility_clean(details.get("link")) or product_url),
            title=combination or None,
            sku=_reference(details),
            gtin=compatibility_clean(details.get("ean13") or details.get("upc")) or None,
            image=MediaRef(url=image_values[0]) if image_values else None,
            offers=tuple(offers),
            stock=StockState(
                availability=availability,
                quantity=quantity,
                quantity_kind=(
                    StockQuantityKind.EXACT if quantity is not None else StockQuantityKind.UNKNOWN
                ),
                observed_at=observed_at,
                evidence=(evidence,),
            ),
            published_attributes=attributes,
            platform_extensions={"legacy_raw_record": _redact_json(details)},
        )

    def _normalize_jsonld(
        self,
        document: str,
        url: str,
        source_id: str,
        method: Literal["html", "browser"],
    ) -> CommerceProductSnapshot | None:
        item = next(iter(jsonld_products(document)), None)
        if not isinstance(item, dict):
            return None
        offer = jsonld_offer(item)
        price, parsed_currency = _price(offer.get("price") or offer.get("lowPrice"))
        name = compatibility_clean(item.get("name")) or meta(document, "og:title")
        if price is None or not name:
            return None
        currency = compatibility_clean(offer.get("priceCurrency")) or parsed_currency or self.options.currency
        if not re.fullmatch(r"[A-Z]{3}", currency.upper()):
            return None
        observed_at = self.context.clock()
        canonical_url = _canonical(urljoin(url, compatibility_clean(item.get("url")) or url))
        evidence = Evidence(method="jsonld", source_url=canonical_url, observed_at=observed_at)
        availability_text = compatibility_clean(offer.get("availability")).casefold()
        availability = (
            Availability.IN_STOCK
            if "instock" in availability_text
            else Availability.OUT_OF_STOCK
            if "outofstock" in availability_text
            else Availability.UNKNOWN
        )
        images = jsonld_images(item, url)
        variant = CommerceVariant(
            external_id="default",
            is_default=True,
            sku=compatibility_clean(item.get("sku") or item.get("mpn")) or None,
            gtin=jsonld_gtin(item),
            image=MediaRef(url=images[0]) if images else None,
            offers=(
                CommerceOffer(
                    price=Money(amount=price, currency=currency.upper()),
                    observed_at=observed_at,
                    evidence=(evidence,),
                    vat_status=self.options.vat_status or "unknown",
                    vat_rate=self.options.vat_rate,
                    availability=availability,
                    availability_evidence=(evidence,),
                ),
            ),
            stock=StockState(
                availability=availability,
                observed_at=observed_at,
                evidence=(evidence,),
            ),
            platform_extensions={"legacy_raw_record": _redact_json(item)},
        )
        categories = breadcrumbs(document) or [compatibility_clean(item.get("category"))]
        return CommerceProductSnapshot(
            connector=self.name,
            source_id=source_id,
            external_id=canonical_url,
            canonical_url=_canonical(url),
            title=name,
            description=compatibility_clean(item.get("description")) or meta(document, "og:description"),
            vendor=jsonld_brand(item) or self.options.brand,
            observed_at=observed_at,
            categories=tuple(CategoryRef(name=value) for value in categories if value),
            images=tuple(MediaRef(url=value) for value in images),
            variants=(variant,),
            published_attributes=cast(
                dict[str, JsonValue], dict(specification_table(document))
            ),
            platform_extensions={"legacy_record_name": name, "legacy_jsonld": True},
        )

    def _links(self, document: str, page_url: str) -> list[str]:
        origin = urlparse(page_url).netloc
        scope = document
        if self.options.card_links_only:
            cards = re.findall(
                r'<(?:article|li|div)[^>]*class=["\'][^"\']*(?:product-miniature|product-item|product-card|productbox)[^"\']*["\'][\s\S]*?</(?:article|li|div)>',
                document,
                re.I,
            )
            pagination = re.findall(
                r'<a[^>]+(?:rel=["\']next["\']|class=["\'][^"\']*(?:next|pagination)[^"\']*["\'])[^>]*>',
                document,
                re.I,
            )
            scope = "".join([*cards, *pagination]) or document
        found = []
        for match in re.finditer(r'href=["\']([^"\']+)["\']', scope, re.I):
            candidate = _canonical(urljoin(page_url, html.unescape(match.group(1))))
            if urlparse(candidate).netloc == origin:
                found.append(candidate)
        return list(dict.fromkeys(found))

    def _is_product_url(self, url: str) -> bool:
        return self.options.product_pattern is None or bool(
            re.search(self.options.product_pattern, urlparse(url).path)
            or re.search(self.options.product_pattern, url)
        )

    def _is_pagination(self, url: str) -> bool:
        if self.options.pagination_patterns:
            return any(re.search(pattern, url) for pattern in self.options.pagination_patterns)
        return bool(re.search(r"[?&](?:p|page|start)=\d+|/page/\d+", url))

    def _validate_request(
        self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None
    ) -> None:
        options = cast(dict[str, JsonValue], self.options.model_dump(mode="json"))
        validate_connector_request(
            capabilities=self.capabilities,
            unsupported_message="PrestaShop connector does not support the requested contract",
            connector=self.name,
            connector_version=self.version,
            request=request,
            checkpoint=checkpoint,
            options=options,
        )

    def checkpoint(
        self, request: CollectionRequest, lineage: str, resume_after: JsonValue
    ) -> ConnectorCheckpoint:
        options = cast(dict[str, JsonValue], self.options.model_dump(mode="json"))
        return build_checkpoint(
            connector=self.name,
            connector_version=self.version,
            request=request,
            lineage=lineage,
            resume_after=resume_after,
            options=options,
        )

    @staticmethod
    def _resume(checkpoint: ConnectorCheckpoint | None) -> tuple[str, int, int] | None:
        if checkpoint is None:
            return None
        cursor = checkpoint.resume_after
        if not isinstance(cursor, dict):
            raise ValueError("PrestaShop checkpoint cursor must be an object")
        partition = cursor.get("partition")
        offset = cursor.get("offset")
        sequence = cursor.get("sequence")
        if (
            not isinstance(partition, str)
            or not partition
            or not isinstance(offset, int)
            or isinstance(offset, bool)
            or offset < 0
            or not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence < 0
        ):
            raise ValueError("PrestaShop checkpoint cursor is invalid")
        return partition, offset, sequence

    @staticmethod
    def _next_cursor(
        partitions: list[tuple[str, list[str]]], partition: str, offset: int, sequence: int
    ) -> dict[str, JsonValue]:
        partition_index = next(index for index, value in enumerate(partitions) if value[0] == partition)
        if offset + 1 < len(partitions[partition_index][1]):
            return {"partition": partition, "offset": offset + 1, "sequence": sequence + 1}
        for next_partition, urls in partitions[partition_index + 1 :]:
            if urls:
                return {"partition": next_partition, "offset": 0, "sequence": sequence + 1}
        return {"partition": partition, "offset": offset + 1, "sequence": sequence + 1}

    @staticmethod
    def _failure_page(
        partition: str,
        sequence: int,
        resume: dict[str, JsonValue],
        code: DiagnosticCode,
        message: str,
        url: str,
        *,
        retryable: bool = True,
    ) -> EntityPage[CommerceProductSnapshot]:
        return EntityPage(
            page_id=f"{partition}:{sequence}:failed",
            partition_key=partition,
            sequence=sequence,
            items=(),
            resume_after=resume,
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
                    url=_safe_url(url),
                ),
            ),
        )


_PRODUCT_DETAILS_TAG = re.compile(r'<[^>]*\bid=["\']product-details["\'][^>]*>', re.I)
_DATA_PRODUCT = (
    re.compile(r'\bdata-product="([^"]+)"', re.I),
    re.compile(r"\bdata-product='([^']+)'", re.I),
)


def data_product(document: str) -> dict[str, Any] | None:
    for tag_match in _PRODUCT_DETAILS_TAG.finditer(document):
        tag = tag_match.group(0)
        for pattern in _DATA_PRODUCT:
            match = pattern.search(tag)
            if not match:
                continue
            try:
                value = json.loads(html.unescape(match.group(1)))
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(value, dict):
                return value
    return None


def variant_groups(document: str) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for match in re.finditer(r'<ul[^>]+id=["\']group_(\d+)["\'](.*?)</ul>', document, re.I | re.S):
        _add_group(groups, match.group(1), match.group(2), "input")
    for match in re.finditer(
        r'<select[^>]+name=["\']group\[(\d+)\]["\'](.*?)</select>', document, re.I | re.S
    ):
        _add_group(groups, match.group(1), match.group(2), "option", replace=False)
    return groups


def _add_group(
    groups: dict[str, dict[str, Any]],
    group_id: str,
    body: str,
    tag_name: str,
    *,
    replace: bool = True,
) -> None:
    options: list[str] = []
    selected: str | None = None
    for tag in re.finditer(rf"<{tag_name}[^>]*>", body, re.I):
        value = re.search(r'\bvalue=["\'](\d+)["\']', tag.group(0))
        if not value:
            continue
        options.append(value.group(1))
        marker = "checked" if tag_name == "input" else "selected"
        if re.search(rf"\b{marker}\b", tag.group(0), re.I):
            selected = value.group(1)
    if options and (replace or group_id not in groups):
        groups[group_id] = {"selected": selected or options[0], "options": options}


def _features(details: dict[str, Any]) -> dict[str, str]:
    return {
        compatibility_clean(item.get("name")): compatibility_clean(item.get("value"))
        for item in details.get("features") or []
        if isinstance(item, dict) and item.get("name") and item.get("value")
    }


def _images(details: dict[str, Any]) -> list[str]:
    found: list[str] = []
    for image in details.get("images") or []:
        if not isinstance(image, dict):
            continue
        large = image.get("large")
        candidate = compatibility_clean(large.get("url") if isinstance(large, dict) else image.get("url"))
        if candidate:
            found.append(candidate)
    return list(dict.fromkeys(found))


def _attachments(details: dict[str, Any], document: str, url: str) -> list[dict[str, Any]]:
    links = {
        match.group(2): (urljoin(url, html.unescape(match.group(1))), compatibility_clean(match.group(3)))
        for match in re.finditer(
            r'<a[^>]+href=["\']([^"\']*id_attachment=(\d+)[^"\']*)["\'][^>]*>(.*?)</a>',
            document,
            re.I | re.S,
        )
    }
    pairs: list[tuple[str, str]] = []
    for attachment in details.get("attachments") or []:
        if not isinstance(attachment, dict):
            continue
        identifier = str(attachment.get("id_attachment") or "")
        label = compatibility_clean(attachment.get("file_name") or attachment.get("name"))
        href, anchor = links.get(identifier, ("", ""))
        if href:
            pairs.append((href, label or anchor))
    return documents(pairs or pdf_links(document, url), url)


def _reference(details: dict[str, Any]) -> str | None:
    attributes = details.get("attributes")
    if isinstance(attributes, dict):
        for entry in attributes.values():
            if isinstance(entry, dict) and (value := compatibility_clean(entry.get("reference"))):
                return value
    return compatibility_clean(details.get("reference")) or None


def _combination(details: dict[str, Any]) -> str:
    parts: list[str] = []
    attributes = details.get("attributes")
    if not isinstance(attributes, dict):
        return ""
    for entry in attributes.values():
        if not isinstance(entry, dict):
            continue
        label, value = (
            (entry.get("group"), entry.get("name"))
            if entry.get("group")
            else (entry.get("name"), entry.get("value"))
        )
        label, value = compatibility_clean(label), compatibility_clean(value)
        if value:
            parts.append(f"{label}: {value}" if label else value)
    return ", ".join(parts)


def _price(value: Any) -> tuple[Decimal | None, str | None]:
    text = compatibility_clean(value)
    if not text:
        return None, None
    match = re.search(r"\b(EUR|USD|GBP|CHF|SEK|NOK|DKK|PLN|CZK|HUF|BGN|RON)\b", text, re.I)
    currency = match.group(1).upper() if match else None
    if currency is None:
        currency = next(
            (mapped for symbol, mapped in (("€", "EUR"), ("£", "GBP"), ("$", "USD")) if symbol in text),
            None,
        )
    normalized = re.sub(r"[^0-9.,-]", "", text)
    normalized = re.sub(r"[.,](?=\d{3}(?:\D|$))", "", normalized).replace(",", ".")
    try:
        amount = Decimal(normalized)
    except (InvalidOperation, ValueError):
        return None, currency
    return (amount if amount.is_finite() and amount >= 0 else None), currency


def _canonical(url: str) -> str:
    parsed = urlparse(url)
    query = "" if re.search(
        r"(?:^|&)(?:order|tag|id_currency|search_query|back|q|sort)=", parsed.query
    ) else parsed.query
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, ""))


def _deduplicate_partitions(
    partitions: list[tuple[str, list[str]]],
) -> list[tuple[str, list[str]]]:
    seen: set[str] = set()
    result: list[tuple[str, list[str]]] = []
    for key, urls in partitions:
        unique = []
        for url in urls:
            if url not in seen:
                seen.add(url)
                unique.append(url)
        result.append((key, unique))
    return result


def _safe_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def _partition_key(kind: str, index: int, url: str) -> str:
    digest = hashlib.sha256(_safe_url(url).encode()).hexdigest()[:12]
    return f"{kind}:{index}:{digest}"


def prestashop_partition_keys(
    options: PrestaShopOptions, base_url: str
) -> tuple[str, ...]:
    """Return configured discovery roots using PrestaShop checkpoint keys."""
    roots = tuple(dict.fromkeys(options.sitemaps))
    if roots:
        return tuple(_partition_key("sitemap", index, root) for index, root in enumerate(roots))
    if not options.category_urls and options.use_advertised_sitemaps:
        return ()
    categories = tuple(dict.fromkeys(options.category_urls or (base_url,)))
    return tuple(
        _partition_key("category", index, root) for index, root in enumerate(categories)
    )

def _redact_json(value: Any) -> JsonValue:
    if isinstance(value, dict):
        return {
            str(key): (
                "[redacted]"
                if re.search(r"(?:^|_)(?:access_?token|token|secret|password|authorization|cookie)(?:$|_)", str(key), re.I)
                else _redact_json(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_json(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


class PrestaShopFactory(SimpleConnectorFactory[PrestaShopOptions, PrestaShopConnector]):
    name = "prestashop"
    version = PrestaShopConnector.version
    options_model = PrestaShopOptions
    connector_type = PrestaShopConnector
