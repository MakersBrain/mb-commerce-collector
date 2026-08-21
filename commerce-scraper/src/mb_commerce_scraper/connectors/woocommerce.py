"""WooCommerce Store API connector producing neutral commerce snapshots."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html import unescape
from typing import Any, Literal, cast
from urllib.parse import urljoin, urlparse

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
    CommerceTransport,
    RequestPriority,
    RequestPurpose,
    TransportRequest,
)

from .base import BrowserRequirement, CommerceConnector, ConnectorCapabilities, ConnectorContext
from .planning import BudgetExhausted, ConnectorBudget, budget_diagnostic

PAGE_SIZE = 100


class HTTPStatusFailure(RuntimeError):
    def __init__(self, status: int) -> None:
        super().__init__(f"HTTP request failed with status {status}")
        self.status = status


class _JsonTransport:
    def __init__(self, transport: CommerceTransport) -> None:
        self.transport = transport

    async def json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        response = await self.transport.request(
            TransportRequest(
                url=url,
                query=cast(dict[str, str | int | float | bool], params or {}),
                headers=headers or {},
                purpose=RequestPurpose.DISCOVERY,
                priority=RequestPriority.DISCOVERY,
                estimated_bytes=1_000_000,
            )
        )
        if response.status >= 400:
            raise HTTPStatusFailure(response.status)
        return response.json_value()


class WooCommerceOptions(BaseModel):
    """Connector-owned Store API behavior projected from legacy source config."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    store_categories: tuple[str, ...] = ()
    identity_only: bool = False
    brand: str | None = None
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None = None
    vat_rate: Decimal | None = Field(default=None, ge=0, le=1)
    stock_from_add_to_cart_maximum: bool = False
    page_size: int = Field(default=PAGE_SIZE, ge=1, le=PAGE_SIZE)
    page_limit: int = Field(default=100, ge=1)
    variation_page_limit: int = Field(default=200, ge=1)
    category_page_limit: int = Field(default=20, ge=1)


@dataclass(frozen=True, slots=True)
class _RawPage:
    partition: str
    page: int
    products: tuple[dict[str, Any], ...]
    next_cursor: dict[str, JsonValue] | None = None


class WooCommerceConnector(CommerceConnector):
    name = "woocommerce"
    platform = "woocommerce"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.EXACT, StockQuantityKind.UNKNOWN}),
        supports_incremental_cursor=False,
        supports_category_filter=True,
        supports_documents=True,
        browser=BrowserRequirement.NEVER,
    )

    def __init__(
        self,
        transport: CommerceTransport,
        options: WooCommerceOptions | None = None,
        context: ConnectorContext | None = None,
    ) -> None:
        self._fetcher = _JsonTransport(transport)
        self.options = options or WooCommerceOptions()
        self.context = context or ConnectorContext()
        self._budget = ConnectorBudget()

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        self._validate_request(request, checkpoint)
        origin = self._origin(request.base_url)
        resume = self._resume(checkpoint)
        wanted_categories = request.partitions or self.options.store_categories
        partitions, notices, category_failure = await self._partitions(origin, wanted_categories)
        if category_failure is not None:
            restart_partition = wanted_categories[0] if wanted_categories else "main"
            yield self._failure_page(
                restart_partition,
                0,
                category_failure,
                resume_after={"partition": restart_partition, "page": 1},
            )
            return
        if resume is not None and resume[0] not in {partition for partition, _ in partitions}:
            raise ValueError("WooCommerce checkpoint partition is not requested")

        raw_pages: list[_RawPage] = []
        failure: tuple[str, int, Diagnostic, dict[str, JsonValue]] | None = None
        seen: set[str] = set()
        collected = 0
        limited = False
        start_index = 0
        if resume is not None:
            start_index = next(
                index for index, (partition, _) in enumerate(partitions) if partition == resume[0]
            )

        for partition_index, (partition, category_id) in enumerate(partitions[start_index:], start_index):
            page = resume[1] if resume is not None and partition_index == start_index else 1
            offset = resume[2] if resume is not None and partition_index == start_index else 0
            while page <= self.options.page_limit:
                if self.context.cancelled():
                    return
                params: dict[str, Any] = {"per_page": self.options.page_size, "page": page}
                if category_id is not None:
                    params["category"] = category_id
                try:
                    self._budget.require(RequestPriority.DISCOVERY, self._store_api(origin))
                    payload = await self._fetcher.json(self._store_api(origin), params=params)
                except BudgetExhausted as error:
                    failure = (
                        partition,
                        page - 1,
                        budget_diagnostic(error.priority, error.url),
                        {"partition": partition, "page": page, "offset": offset},
                    )
                    break
                except HTTPStatusFailure as error:
                    if error.status == 400 and page > 1:
                        break
                    failure = self._enumeration_failure(partition, page, error, self._store_api(origin))
                    break
                except RuntimeError as error:
                    failure = self._enumeration_failure(partition, page, error, self._store_api(origin))
                    break
                if not isinstance(payload, list) or not payload:
                    break

                products: list[dict[str, Any]] = []
                limit_cursor: dict[str, JsonValue] | None = None
                for raw_index, raw in enumerate(payload):
                    if raw_index < offset:
                        continue
                    if not isinstance(raw, dict) or raw.get("id") is None:
                        continue
                    external_id = str(raw["id"])
                    if external_id in seen:
                        continue
                    if request.result_limit is not None and collected >= request.result_limit:
                        limited = True
                        limit_cursor = {
                            "partition": partition,
                            "page": page,
                            "offset": raw_index,
                        }
                        break
                    seen.add(external_id)
                    products.append(
                        {**raw, "_category_slug": partition if partition != "main" else None}
                    )
                    collected += 1
                if (
                    not limited
                    and request.result_limit is not None
                    and collected >= request.result_limit
                    and len(payload) >= self.options.page_size
                ):
                    limited = True
                    limit_cursor = {"partition": partition, "page": page + 1}
                raw_pages.append(_RawPage(partition, page, tuple(products), limit_cursor))
                if limited:
                    break
                if len(payload) < self.options.page_size:
                    break
                page += 1
                offset = 0
            else:
                failure = self._page_limit_failure(
                    partition, page, self.options.page_limit, self._store_api(origin), "product"
                )
            if failure is not None or limited:
                break
            resume = None

        variations: dict[str, list[dict[str, Any]]] = {}
        variation_notices: list[Diagnostic] = []
        if failure is None:
            wanted_parents = {
                str(product["id"])
                for raw_page in raw_pages
                for product in raw_page.products
                if product.get("type") == "variable"
            }
            if wanted_parents:
                variations, variation_failure = await self._variations(origin, wanted_parents, request)
                if variation_failure is not None:
                    first = raw_pages[0] if raw_pages else _RawPage("main", 1, ())
                    failure = (
                        first.partition,
                        first.page - 1,
                        variation_failure,
                        {"partition": first.partition, "page": first.page},
                    )
                missing = wanted_parents - set(variations)
                if missing and variation_failure is None:
                    variation_notices.append(
                        Diagnostic(
                            code=DiagnosticCode.OPTIONAL_ENRICHMENT_SKIPPED,
                            severity=DiagnosticSeverity.WARNING,
                            message=f"{len(missing)} variable products returned no variations",
                            retryable=False,
                            affects_completeness=False,
                            url=self._store_api(origin),
                        )
                    )

        observed_at = self.context.clock()
        all_notices = tuple(notices + variation_notices)
        if failure is not None and raw_pages:
            # No product page has been emitted yet: the bulk variation join and
            # product enumeration are both part of its deterministic content.
            # Resume from the first buffered page rather than checkpointing a
            # partial shape under a page id that a retry would later change.
            first = raw_pages[0]
            failure = (
                first.partition,
                first.page - 1,
                failure[2],
                {"partition": first.partition, "page": first.page},
            )

        for index, raw_page in enumerate(raw_pages if failure is None else []):
            next_cursor = self._next_cursor(raw_pages, index)
            snapshots = tuple(
                snapshot
                for product in raw_page.products
                if (
                    snapshot := self._normalize(
                        product,
                        variations.get(str(product.get("id")), []),
                        request.source_id,
                        origin,
                        observed_at,
                        raw_page.partition,
                    )
                )
                is not None
            )
            final_page = failure is None and index == len(raw_pages) - 1
            limit_diagnostics = (
                (result_limit_diagnostic(request.result_limit, self._store_api(origin)),)
                if final_page and limited and request.result_limit is not None
                else ()
            )
            diagnostics = (all_notices if index == 0 else ()) + limit_diagnostics
            yield EntityPage(
                page_id=f"{raw_page.partition}:{raw_page.page}",
                partition_key=raw_page.partition,
                sequence=raw_page.page - 1,
                items=snapshots,
                resume_after=None if final_page and not limited else next_cursor,
                terminal=final_page,
                enumeration_intact=not (final_page and limited),
                discovered=len(raw_page.products),
                diagnostics=diagnostics,
            )

        if failure is not None:
            partition, sequence, diagnostic, cursor = failure
            yield self._failure_page(partition, sequence, diagnostic, resume_after=cursor)
        elif not raw_pages:
            yield EntityPage(
                page_id="main:empty",
                partition_key="main",
                sequence=0,
                items=(),
                terminal=True,
                enumeration_intact=True,
                discovered=0,
                diagnostics=all_notices,
            )

    async def _partitions(
        self, origin: str, wanted: tuple[str, ...],
    ) -> tuple[list[tuple[str, Any | None]], list[Diagnostic], Diagnostic | None]:
        if not wanted:
            return [("main", None)], [], None
        endpoint = self._store_api(origin, "products/categories")
        available: dict[str, Any] = {}
        reached_end = False
        for page in range(1, self.options.category_page_limit + 1):
            try:
                self._budget.require(RequestPriority.DISCOVERY, endpoint)
                payload = await self._fetcher.json(
                    endpoint, params={"per_page": self.options.page_size, "page": page}
                )
            except BudgetExhausted as error:
                return [], [], budget_diagnostic(error.priority, error.url)
            except HTTPStatusFailure as error:
                if error.status == 400 and page > 1:
                    reached_end = True
                    break
                return [], [], self._diagnostic(endpoint, page, error, "category")
            except RuntimeError as error:
                return [], [], self._diagnostic(endpoint, page, error, "category")
            if not isinstance(payload, list) or not payload:
                reached_end = True
                break
            for entry in payload:
                if isinstance(entry, dict) and entry.get("slug") and entry.get("id") is not None:
                    available[str(entry["slug"]).strip()] = entry["id"]
            if all(slug in available for slug in wanted) or len(payload) < self.options.page_size:
                reached_end = True
                break
        if not reached_end and any(slug not in available for slug in wanted):
            return [], [], self._diagnostic(
                endpoint,
                self.options.category_page_limit + 1,
                RuntimeError(
                    f"category page limit {self.options.category_page_limit} reached"
                ),
                "category",
                retryable=False,
            )
        resolved = [(slug, available[slug]) for slug in wanted if slug in available]
        missing = [slug for slug in wanted if slug not in available]
        notices = []
        if missing:
            notices.append(
                Diagnostic(
                    code=DiagnosticCode.OPTIONAL_ENRICHMENT_SKIPPED,
                    severity=DiagnosticSeverity.WARNING,
                    message=f"categories absent from the Store API: {', '.join(missing)}",
                    retryable=False,
                    affects_completeness=False,
                    url=endpoint,
                )
            )
        return resolved, notices, None

    async def _variations(
        self, origin: str, wanted: set[str], request: CollectionRequest,
    ) -> tuple[dict[str, list[dict[str, Any]]], Diagnostic | None]:
        endpoint = self._store_api(origin)
        grouped: dict[str, list[dict[str, Any]]] = {}
        detail_priority = self._budget.required_detail_priority(
            request.requested_fields,
            frozenset({SnapshotField.VARIANTS, SnapshotField.OFFERS, SnapshotField.STOCK}),
        )
        for page in range(1, self.options.variation_page_limit + 1):
            if self.context.cancelled():
                return grouped, self._diagnostic(endpoint, page, RuntimeError("cancelled"), "variation")
            try:
                self._budget.require(detail_priority, endpoint)
                payload = await self._fetcher.json(
                    endpoint,
                    params={"per_page": self.options.page_size, "page": page, "type": "variation"},
                )
            except BudgetExhausted as error:
                return grouped, budget_diagnostic(error.priority, error.url)
            except HTTPStatusFailure as error:
                if error.status == 400 and page > 1:
                    return grouped, None
                return grouped, self._diagnostic(endpoint, page, error, "variation")
            except RuntimeError as error:
                return grouped, self._diagnostic(endpoint, page, error, "variation")
            if not isinstance(payload, list) or not payload:
                return grouped, None
            for variation in payload:
                if not isinstance(variation, dict):
                    continue
                parent = str(variation.get("parent") or "")
                if parent in wanted:
                    grouped.setdefault(parent, []).append(variation)
            if len(payload) < self.options.page_size:
                return grouped, None
        return grouped, self._diagnostic(
            endpoint,
            self.options.variation_page_limit + 1,
            RuntimeError(f"variation page limit {self.options.variation_page_limit} reached"),
            "variation",
            retryable=False,
        )

    def _normalize(
        self,
        product: dict[str, Any],
        variations: list[dict[str, Any]],
        source_id: str,
        origin: str,
        observed_at: datetime,
        partition: str,
    ) -> CommerceProductSnapshot | None:
        external_id = str(product.get("id") or "").strip()
        title = self._clean(product.get("name"))
        product_url = self._clean(product.get("permalink"))
        if not external_id or not title or not product_url:
            return None
        evidence = Evidence(
            method="api",
            source_url=product_url,
            source_field="wp-json/wc/store/v1/products",
            observed_at=observed_at,
        )
        attributes, claims = self._attributes(product)
        vendor = self._brand(product, attributes) or self.options.brand
        # Preserve the legacy Store API behavior for a variable product whose
        # bulk pass returns no child: use its published parent offer/stock as a
        # degraded variant rather than silently dropping the identity.
        raw_variants = variations if product.get("type") == "variable" and variations else [product]
        variants = tuple(
            variant
            for raw in raw_variants
            if (variant := self._variant(raw, product, evidence, observed_at)) is not None
        )
        images = tuple(
            MediaRef(
                url=str(image["src"]),
                alt_text=self._clean(image.get("alt")) or None,
                external_id=str(image.get("id") or "") or None,
            )
            for image in product.get("images") or []
            if isinstance(image, dict) and image.get("src")
        )
        api_categories = tuple(
            CategoryRef(
                name=str(entry["name"]),
                external_id=str(entry.get("id") or "") or None,
            )
            for entry in product.get("categories") or []
            if isinstance(entry, dict) and entry.get("name")
        )
        categories = (
            ((CategoryRef(name=partition),) if partition != "main" else ())
            + api_categories
        )
        documents = tuple(
            DocumentRef(
                url=urljoin(product_url, match),
                title=match,
                observed_at=observed_at,
                evidence=(evidence,),
            )
            for match in re.findall(
                r'href=["\']([^"\']+\.pdf[^"\']*)',
                str(product.get("description") or ""),
                re.IGNORECASE,
            )
        )
        published: dict[str, JsonValue] = dict(attributes)
        if claims:
            published["claims"] = cast(JsonValue, claims)
        raw_extensions: dict[str, JsonValue]
        if product.get("type") == "variable":
            raw_extensions = {
                "legacy_raw_product": {
                    key: value for key, value in product.items() if key != "variations"
                }
            }
        else:
            raw_extensions = {"raw": product}
        return CommerceProductSnapshot(
            connector=self.name,
            source_id=source_id,
            external_id=external_id,
            canonical_url=product_url,
            title=title,
            observed_at=observed_at,
            description=self._clean(product.get("description"))
            or self._clean(product.get("short_description"))
            or None,
            vendor=vendor,
            categories=categories,
            images=images,
            documents=documents,
            variants=variants,
            published_attributes=published,
            platform_extensions={
                "category_slugs": [
                    str(entry.get("slug"))
                    for entry in product.get("categories") or []
                    if isinstance(entry, dict) and entry.get("slug")
                ],
                **raw_extensions,
            },
        )

    def _variant(
        self,
        raw: dict[str, Any],
        product: dict[str, Any],
        evidence: Evidence,
        observed_at: datetime,
    ) -> CommerceVariant | None:
        external_id = str(raw.get("id") or "").strip()
        if not external_id:
            return None
        title = self._variant_title(raw) if raw is not product else ""
        availability = self._availability(raw)
        offers = self._offers(raw.get("prices"), availability, evidence, observed_at)
        stock = self._stock(raw, availability, evidence, observed_at)
        options = {
            self._clean(entry.get("name")) or f"attribute-{index}": self._clean(entry.get("value"))
            for index, entry in enumerate(raw.get("attributes") or [], 1)
            if isinstance(entry, dict) and self._clean(entry.get("value"))
        }
        images = raw.get("images")
        image = next(
            (
                MediaRef(url=str(item["src"]))
                for item in images or []
                if isinstance(item, dict) and item.get("src")
            ),
            None,
        )
        attributes: dict[str, JsonValue] = {}
        price_html = self._clean(raw.get("price_html"))
        if price_html:
            attributes["price_text"] = price_html
        return CommerceVariant(
            external_id=external_id,
            canonical_url=self._clean(raw.get("permalink")) or self._clean(product.get("permalink")),
            title=title or None,
            sku=self._clean(raw.get("sku")) or self._clean(product.get("sku")) or None,
            gtin=self._clean(raw.get("global_unique_id")) or self._clean(raw.get("gtin")) or None,
            image=image,
            options=options,
            offers=() if self.options.identity_only else offers,
            stock=stock,
            published_attributes=attributes,
            platform_extensions=(
                {"legacy_raw_variant": raw} if raw is not product else {}
            ),
        )

    def _offers(
        self,
        prices: Any,
        availability: Availability,
        evidence: Evidence,
        observed_at: datetime,
    ) -> tuple[CommerceOffer, ...]:
        if not isinstance(prices, dict):
            return ()
        currency = self._currency(prices.get("currency_code"))
        current = self._minor_amount(prices.get("price"), prices.get("currency_minor_unit"))
        regular = self._minor_amount(prices.get("regular_price"), prices.get("currency_minor_unit"))
        if current is None or currency is None:
            return ()
        sale = regular is not None and regular > current
        offer = CommerceOffer(
            price=Money(amount=current, currency=currency),
            observed_at=observed_at,
            evidence=(evidence,),
            role="sale" if sale else "regular",
            vat_status=self.options.vat_status or "unknown",
            vat_rate=self.options.vat_rate,
            availability=availability,
            availability_evidence=(evidence,),
        )
        if not sale or regular is None:
            return (offer,)
        return (
            offer,
            CommerceOffer(
                price=Money(amount=regular, currency=currency),
                observed_at=observed_at,
                evidence=(evidence,),
                role="regular",
                vat_status=self.options.vat_status or "unknown",
                vat_rate=self.options.vat_rate,
                availability=availability,
                availability_evidence=(evidence,),
            ),
        )

    def _stock(
        self,
        item: dict[str, Any],
        availability: Availability,
        evidence: Evidence,
        observed_at: datetime,
    ) -> StockState:
        quantity: int | None = None
        if availability == Availability.OUT_OF_STOCK:
            quantity = 0
        stock = item.get("stock_availability")
        if quantity is None and isinstance(stock, dict):
            quantity = self._integer(stock.get("quantity"))
        if quantity is None:
            low = self._integer(item.get("low_stock_remaining"))
            quantity = low if low is not None and low > 0 else None
        if quantity is None and self.options.stock_from_add_to_cart_maximum:
            add_to_cart = item.get("add_to_cart")
            maximum = self._integer(add_to_cart.get("maximum")) if isinstance(add_to_cart, dict) else None
            if (
                item.get("is_in_stock") is True
                and not item.get("is_on_backorder")
                and not item.get("sold_individually")
                and maximum is not None
                and 0 < maximum < 9999
            ):
                quantity = maximum
        return StockState(
            availability=availability,
            quantity=quantity,
            quantity_kind=StockQuantityKind.EXACT if quantity is not None else StockQuantityKind.UNKNOWN,
            observed_at=observed_at,
            evidence=(evidence,),
        )

    @staticmethod
    def _attributes(product: dict[str, Any]) -> tuple[dict[str, str], list[dict[str, JsonValue]]]:
        attributes: dict[str, str] = {}
        claims: list[dict[str, JsonValue]] = []
        for attribute in product.get("attributes") or []:
            if not isinstance(attribute, dict) or not attribute.get("name"):
                continue
            name = WooCommerceConnector._clean(attribute.get("name"))
            terms = [
                WooCommerceConnector._clean(term.get("name"))
                for term in attribute.get("terms") or []
                if isinstance(term, dict) and WooCommerceConnector._clean(term.get("name"))
            ]
            if not name or not terms:
                continue
            value = ", ".join(terms)
            attributes[name] = value
            if re.search(r"dinnerware|food", name, re.IGNORECASE):
                claims.append(
                    {
                        "type": "food_contact_suitability",
                        "claim": not bool(re.search(r"\bnot[-\s]", value, re.IGNORECASE)),
                        "evidence": f"{name}: {value}"[:300],
                        "basis": "product_attribute",
                    }
                )
        return attributes, claims

    @staticmethod
    def _brand(product: dict[str, Any], attributes: dict[str, str]) -> str | None:
        brands = product.get("brands")
        if (
            isinstance(brands, list)
            and brands
            and isinstance(brands[0], dict)
            and (name := WooCommerceConnector._clean(brands[0].get("name")))
        ):
            return name
        brand_names = {"brand", "marque", "marke", "merk", "marca", "marque email", "gamintojas"}
        for name, value in attributes.items():
            if name.casefold() in brand_names and value:
                return value
        return None

    @staticmethod
    def _variant_title(raw: dict[str, Any]) -> str:
        if title := WooCommerceConnector._clean(raw.get("variation")):
            return title
        return ", ".join(
            WooCommerceConnector._clean(entry.get("value"))
            for entry in raw.get("attributes") or []
            if isinstance(entry, dict) and WooCommerceConnector._clean(entry.get("value"))
        )

    @staticmethod
    def _availability(item: dict[str, Any]) -> Availability:
        if item.get("is_on_backorder"):
            return Availability.BACKORDER
        if item.get("is_in_stock") is True:
            return Availability.IN_STOCK
        if item.get("is_in_stock") is False:
            return Availability.OUT_OF_STOCK
        # Legacy Woo behavior defaults variation availability to true and
        # product availability to false when the Store API omits the field.
        return (
            Availability.IN_STOCK
            if item.get("type") == "variation" or item.get("parent") is not None
            else Availability.OUT_OF_STOCK
        )

    @staticmethod
    def _minor_amount(value: Any, minor_unit: Any) -> Decimal | None:
        try:
            digits = int(minor_unit if minor_unit is not None else 2)
            amount = Decimal(str(value)).scaleb(-digits)
        except (InvalidOperation, TypeError, ValueError):
            return None
        return amount if amount.is_finite() and amount >= 0 else None

    @staticmethod
    def _currency(value: Any) -> str | None:
        currency = str(value or "").strip().upper()
        return currency if len(currency) == 3 and currency.isalpha() else None

    @staticmethod
    def _integer(value: Any) -> int | None:
        return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None

    @staticmethod
    def _clean(value: Any) -> str:
        if value is None:
            return ""
        return " ".join(unescape(re.sub(r"<[^>]+>", " ", str(value))).split())

    def _validate_request(
        self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None,
    ) -> None:
        if not self.capabilities.supports(request.requested_fields, request.refresh_mode):
            raise ValueError("WooCommerce connector does not support the requested contract")
        if self.options.identity_only and SnapshotField.OFFERS in request.requested_fields:
            raise ValueError("identity-only WooCommerce source cannot supply offers")
        validate_checkpoint(
            checkpoint,
            connector=self.name,
            connector_version=self.version,
            request=request,
            options=cast(dict[str, JsonValue], self.options.model_dump(mode="json")),
        )

    @staticmethod
    def _origin(url: str) -> str:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("WooCommerce base_url must be an absolute HTTP(S) URL")
        return urljoin(url, "/").rstrip("/")

    @staticmethod
    def _store_api(origin: str, path: str = "products") -> str:
        return f"{origin}/wp-json/wc/store/v1/{path}"

    @staticmethod
    def _resume(checkpoint: ConnectorCheckpoint | None) -> tuple[str, int, int] | None:
        if checkpoint is None:
            return None
        cursor = checkpoint.resume_after
        if not isinstance(cursor, dict):
            raise ValueError("WooCommerce checkpoint cursor must be an object")
        partition = cursor.get("partition")
        page = cursor.get("page")
        offset = cursor.get("offset", 0)
        if not isinstance(partition, str) or not isinstance(page, int) or page < 1:
            raise ValueError("WooCommerce checkpoint cursor is invalid")
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise ValueError("WooCommerce checkpoint offset is invalid")
        return partition, page, offset

    @staticmethod
    def _next_cursor(raw_pages: list[_RawPage], index: int) -> dict[str, JsonValue]:
        if index + 1 < len(raw_pages):
            following = raw_pages[index + 1]
            return {"partition": following.partition, "page": following.page}
        current = raw_pages[index]
        if current.next_cursor is not None:
            return current.next_cursor
        return {"partition": current.partition, "page": current.page + 1}

    @staticmethod
    def _diagnostic(
        endpoint: str, page: int, error: Exception, kind: str, *, retryable: bool = True,
    ) -> Diagnostic:
        return Diagnostic(
            code=DiagnosticCode.ENUMERATION_INCOMPLETE,
            severity=DiagnosticSeverity.ERROR if retryable else DiagnosticSeverity.WARNING,
            message=f"WooCommerce {kind} page {page} failed: {error}",
            retryable=retryable,
            affects_completeness=True,
            url=endpoint,
        )

    def _enumeration_failure(
        self, partition: str, page: int, error: Exception, endpoint: str,
    ) -> tuple[str, int, Diagnostic, dict[str, JsonValue]]:
        return (
            partition,
            page - 1,
            self._diagnostic(endpoint, page, error, "product"),
            {"partition": partition, "page": page},
        )

    def _page_limit_failure(
        self, partition: str, page: int, limit: int, endpoint: str, kind: str,
    ) -> tuple[str, int, Diagnostic, dict[str, JsonValue]]:
        diagnostic = self._diagnostic(
            endpoint,
            page,
            RuntimeError(f"{kind} page limit {limit} reached"),
            kind,
            retryable=False,
        )
        return partition, page - 1, diagnostic, {"partition": partition, "page": page}

    @staticmethod
    def _failure_page(
        partition: str,
        sequence: int,
        diagnostic: Diagnostic,
        *,
        resume_after: dict[str, JsonValue],
    ) -> EntityPage[CommerceProductSnapshot]:
        return EntityPage(
            page_id=f"{partition}:failure:{sequence + 1}",
            partition_key=partition,
            sequence=sequence,
            items=(),
            resume_after=resume_after,
            terminal=True,
            enumeration_intact=False,
            discovered=0,
            diagnostics=(diagnostic,),
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


class WooCommerceFactory:
    name = "woocommerce"
    options_model: type[BaseModel] = WooCommerceOptions

    def build(
        self,
        *,
        transport: CommerceTransport,
        options: BaseModel,
        context: ConnectorContext,
    ) -> WooCommerceConnector:
        return WooCommerceConnector(
            transport, WooCommerceOptions.model_validate(options), context
        )
