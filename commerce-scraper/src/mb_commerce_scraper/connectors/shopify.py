from __future__ import annotations

import re
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, Literal, cast
from urllib.parse import urljoin

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
from mb_commerce_scraper.parsing._structured import decimal_amount, origin_of
from mb_commerce_scraper.transports import (
    BudgetExhausted,
    CommerceTransport,
    RequestPriority,
    RequestPurpose,
    ResponseBodyTooLarge,
    RotationReason,
    TransportFailure,
    TransportRequest,
)

from .base import BrowserRequirement, CommerceConnector, ConnectorCapabilities, ConnectorContext


class ShopifyOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None = None
    page_size: int = Field(default=250, ge=1, le=250)
    page_limit: int = Field(default=200, ge=1)
    inventory_method: Literal["none", "product_json", "product_html"] = "none"
    inventory_section_id: str | None = None
    inventory_batch_size: int = Field(default=10, ge=1, le=50)
    inventory_request_estimated_bytes: int = Field(default=250_000, ge=0)
    discovery_request_estimated_bytes: int = Field(default=1_000_000, ge=0)


class ShopifyConnector(CommerceConnector):
    name = "shopify"
    platform = "shopify"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.EXACT, StockQuantityKind.UNKNOWN}),
        supports_documents=True,
        browser=BrowserRequirement.NEVER,
        shared_edge="edge:shopify",
    )

    def __init__(self, transport: CommerceTransport, options: ShopifyOptions, context: ConnectorContext | None = None) -> None:
        self.transport = transport
        self.options = options
        self.context = context or ConnectorContext()

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        if not self.capabilities.supports(request.requested_fields, request.refresh_mode):
            raise ValueError("Shopify does not support the requested contract")
        options = cast(dict[str, JsonValue], self.options.model_dump(mode="json"))
        validate_checkpoint(
            checkpoint,
            connector=self.name,
            connector_version=self.version,
            request=request,
            options=options,
        )
        origin = origin_of(request.base_url)
        currency = self.options.currency or await self._currency(origin)
        partitions = request.partitions or ("main",)
        resume = self._resume(checkpoint)
        resume_index = 0
        if resume is not None:
            try:
                resume_index = partitions.index(resume[0])
            except ValueError as error:
                raise ValueError(
                    "CHECKPOINT_INVALID: Shopify partition is not requested"
                ) from error
        emitted = 0
        sequence = 0
        for partition_index, partition in enumerate(partitions):
            if resume is not None and partition_index < resume_index:
                continue
            page_number = resume[1] if resume is not None and partition_index == resume_index else 1
            offset = resume[2] if resume is not None and partition_index == resume_index else 0
            endpoint = (
                f"{origin}/products.json"
                if partition == "main"
                else f"{origin}/collections/{partition}/products.json"
            )
            while page_number <= self.options.page_limit:
                if self.context.cancelled():
                    return
                discovery_request = TransportRequest(
                    url=endpoint,
                    query={"limit": self.options.page_size, "page": page_number},
                    purpose=RequestPurpose.DISCOVERY,
                    priority=RequestPriority.DISCOVERY,
                    estimated_bytes=self.options.discovery_request_estimated_bytes,
                )
                if not self._affordable(discovery_request):
                    yield self._budget_page(partition, page_number, sequence, endpoint)
                    return
                try:
                    response = await self.transport.request(discovery_request)
                    if response.status >= 400:
                        yield self._failure_page(
                            partition,
                            page_number,
                            sequence,
                            endpoint,
                            RuntimeError(
                                f"Shopify feed failed with status {response.status}"
                            ),
                        )
                        return
                    body = response.json_value()
                except BudgetExhausted:
                    yield self._budget_page(partition, page_number, sequence, endpoint)
                    return
                except (ResponseBodyTooLarge, TransportFailure, ValueError) as error:
                    yield self._failure_page(
                        partition, page_number, sequence, endpoint, error
                    )
                    return
                products = body.get("products", []) if isinstance(body, dict) else []
                products = products if isinstance(products, list) else []
                available = products[offset:]
                remaining = None if request.result_limit is None else request.result_limit - emitted
                selected = available if remaining is None else available[: max(remaining, 0)]
                mutable_products = [item for item in selected if isinstance(item, dict)]
                inventory_diagnostics = await self._enrich_inventory(
                    mutable_products, origin, endpoint
                )
                observed = self.context.clock()
                snapshots = tuple(
                    value
                    for raw in mutable_products
                    if (
                        value := self._normalize(
                            raw,
                            request.source_id,
                            origin,
                            currency,
                            observed,
                            partition,
                        )
                    )
                    is not None
                )
                emitted += len(selected)
                limited = (
                    request.result_limit is not None
                    and emitted >= request.result_limit
                    and (
                        len(selected) < len(available)
                        or len(products) >= self.options.page_size
                    )
                )
                exhausted = len(products) < self.options.page_size
                partition_terminal = limited or exhausted
                last_partition = partition == partitions[-1]
                terminal = partition_terminal and (limited or last_partition)
                if limited and len(selected) < len(available):
                    cursor: JsonValue | None = {
                        "partition": partition,
                        "page": page_number,
                        "offset": offset + len(selected),
                    }
                elif exhausted:
                    cursor = (
                        None
                        if last_partition
                        else {
                            "partition": partitions[partition_index + 1],
                            "page": 1,
                            "offset": 0,
                        }
                    )
                else:
                    cursor = {"partition": partition, "page": page_number + 1, "offset": 0}
                yield EntityPage(
                    page_id=f"{partition}:{page_number}",
                    partition_key=partition,
                    sequence=sequence,
                    items=snapshots,
                    resume_after=cursor,
                    terminal=terminal,
                    partition_terminal=partition_terminal,
                    enumeration_intact=not limited,
                    discovered=len(products),
                    diagnostics=inventory_diagnostics
                    + (
                        (result_limit_diagnostic(request.result_limit, endpoint),)
                        if limited and request.result_limit
                        else ()
                    ),
                )
                sequence += 1
                if partition_terminal:
                    if limited:
                        return
                    break
                page_number += 1
                offset = 0
            else:
                yield self._page_limit_page(partition, page_number, sequence, endpoint)
                return
            resume = None

    async def _currency(self, origin: str) -> str | None:
        request = TransportRequest(
            url=f"{origin}/meta.json",
            purpose=RequestPurpose.ENRICHMENT,
            priority=RequestPriority.OPTIONAL,
            required=False,
            estimated_bytes=10_000,
        )
        if not self._optional_affordable(request, f"{origin}/products.json"):
            return None
        try:
            response = await self.transport.request(request)
            if response.status >= 400:
                return None
            payload = response.json_value()
        except (BudgetExhausted, RuntimeError, ValueError):
            return None
        value = payload.get("currency") if isinstance(payload, dict) else None
        currency = str(value).strip().upper() if value else ""
        return currency if len(currency) == 3 and currency.isalpha() else None

    async def _enrich_inventory(
        self, products: list[dict[str, Any]], origin: str, discovery_url: str
    ) -> tuple[Diagnostic, ...]:
        if self.options.inventory_method == "none" or not products:
            return ()
        failures = 0
        deferred = 0
        batch_size = self.options.inventory_batch_size
        for offset in range(0, len(products), batch_size):
            batch = products[offset : offset + batch_size]
            for index, product in enumerate(batch):
                request = self._inventory_request(product, origin)
                if request is None:
                    failures += 1
                    continue
                if not self._optional_affordable(request, discovery_url):
                    deferred += len(products) - offset - index
                    break
                try:
                    response = await self.transport.request(request)
                    if response.status >= 400:
                        raise RuntimeError(
                            f"inventory request failed with status {response.status}"
                        )
                    if self.options.inventory_method == "product_json":
                        detail = response.json_value()
                        detail = detail if isinstance(detail, dict) else {}
                    else:
                        variant_ids = {
                            str(item.get("id"))
                            for item in product.get("variants") or []
                            if isinstance(item, dict) and item.get("id")
                        }
                        detail = {
                            "variants": list(
                                self._inventory_from_html(
                                    response.text(), variant_ids
                                ).values()
                            )
                        }
                except BudgetExhausted:
                    deferred += len(products) - offset - index
                    break
                except (RuntimeError, ValueError):
                    failures += 1
                else:
                    self._merge_inventory(product, detail)
            if deferred:
                break
            if offset + batch_size < len(products):
                await self.transport.rotate_identity(RotationReason.EXPLICIT)
        skipped = failures + deferred
        if not skipped:
            return ()
        return (
            Diagnostic(
                code=DiagnosticCode.OPTIONAL_ENRICHMENT_SKIPPED,
                severity=DiagnosticSeverity.WARNING,
                message=(
                    f"inventory unavailable for {skipped} products; quantities remain unknown"
                ),
                retryable=failures > 0,
                affects_completeness=False,
            ),
        )

    def _inventory_request(
        self, product: dict[str, Any], origin: str
    ) -> TransportRequest | None:
        handle = str(product.get("handle") or "").strip()
        if not handle:
            return None
        product_url = f"{origin}/products/{handle}"
        query: dict[str, str | int | float | bool] = {}
        url = product_url
        if self.options.inventory_method == "product_json":
            url = f"{product_url}.js"
        elif self.options.inventory_section_id:
            query["section_id"] = self.options.inventory_section_id
        return TransportRequest(
            url=url,
            query=query,
            headers={"Cookie": ""},
            purpose=RequestPurpose.ENRICHMENT,
            priority=RequestPriority.OPTIONAL,
            required=False,
            estimated_bytes=self.options.inventory_request_estimated_bytes,
        )

    @staticmethod
    def _merge_inventory(product: dict[str, Any], detail: dict[str, Any]) -> None:
        by_id = {
            str(item.get("id")): item
            for item in detail.get("variants") or []
            if isinstance(item, dict) and item.get("id")
        }
        for variant in product.get("variants") or []:
            if not isinstance(variant, dict):
                continue
            extra = by_id.get(str(variant.get("id")))
            if extra is None:
                continue
            for key in (
                "inventory_quantity",
                "inventory_management",
                "inventory_policy",
            ):
                if key in extra:
                    variant[key] = extra[key]

    @staticmethod
    def _inventory_from_html(
        document: str, variant_ids: set[str]
    ) -> dict[str, dict[str, Any]]:
        found: dict[str, dict[str, Any]] = {}
        for identifier in variant_ids:
            escaped = re.escape(identifier)
            quantity: int | None = None
            native = re.search(
                rf'"id":{escaped}\b(?:(?!"id":\d).){{0,1800}}?'
                rf'"inventory_quantity":(-?\d+)(?:(?!"id":\d).){{0,500}}?'
                r'"inventory_management":"shopify"(?:(?!"id":\d).){0,300}?'
                r'"inventory_policy":"deny"',
                document,
                re.DOTALL,
            )
            if native:
                quantity = int(native.group(1))
            if quantity is None:
                option = re.search(
                    rf'<option\b[^>]*\bvalue=["\']{escaped}["\'][^>]*>',
                    document,
                    re.IGNORECASE,
                )
                if option and re.search(
                    r'data-inventory-policy=["\']deny["\']',
                    option.group(0),
                    re.IGNORECASE,
                ):
                    match = re.search(
                        r'data-inventory=["\'](-?\d+)["\']',
                        option.group(0),
                        re.IGNORECASE,
                    )
                    if match:
                        quantity = int(match.group(1))
            if quantity is None and re.search(
                rf'gwProductInventoryPolicy\[{escaped}\]\s*=\s*["\']deny["\']',
                document,
            ):
                match = re.search(
                    rf'gwProductInventoryQuantity\[{escaped}\]\s*=\s*["\'](-?\d+)["\']',
                    document,
                )
                if match:
                    quantity = int(match.group(1))
            if quantity is None:
                local = re.search(
                    rf"\bid\s*:\s*{escaped}\b(?:(?!\bid\s*:).){{0,700}}?"
                    r'inventory_management\s*:\s*["\']shopify["\']'
                    r"(?:(?!\bid\s*:).){0,300}?\bquantity\s*:\s*(-?\d+)",
                    document,
                    re.DOTALL,
                )
                if local:
                    quantity = int(local.group(1))
            if quantity is None:
                inventory = re.search(
                    rf'["\']{escaped}["\']\s*:\s*\{{'
                    r'(?:(?!["\']\d+["\']\s*:).){0,1000}?'
                    r'["\']inventory_management["\']\s*:\s*(?:null|["\']shopify["\'])'
                    r'(?:(?!["\']\d+["\']\s*:).){0,500}?'
                    r'["\']inventory_policy["\']\s*:\s*["\']deny["\']'
                    r'(?:(?!["\']\d+["\']\s*:).){0,500}?'
                    r'["\']inventory_quantity["\']\s*:\s*(-?\d+)',
                    document,
                    re.DOTALL,
                )
                if inventory:
                    quantity = int(inventory.group(1))
            if quantity is not None:
                found[identifier] = {
                    "id": identifier,
                    "inventory_quantity": quantity,
                    "inventory_management": "published_theme",
                    "inventory_policy": "deny",
                }
        return found

    def _affordable(self, request: TransportRequest) -> bool:
        return self.context.budget is None or self.context.budget.affordable(request)

    def _optional_affordable(
        self, request: TransportRequest, discovery_url: str
    ) -> bool:
        budget = self.context.budget
        if budget is None or not budget.affordable(request):
            return budget is None
        reserve = request.model_copy(
            update={
                "url": discovery_url,
                "purpose": RequestPurpose.DISCOVERY,
                "priority": RequestPriority.DISCOVERY,
                "required": True,
                "estimated_bytes": (
                    request.estimated_bytes
                    + self.options.discovery_request_estimated_bytes
                ),
            }
        )
        if not budget.affordable(reserve):
            return False
        maximum_requests = getattr(budget, "maximum_requests", None)
        used_requests = getattr(budget, "requests", None)
        if isinstance(maximum_requests, int) and isinstance(used_requests, int):
            return maximum_requests - used_requests >= 2
        return True

    @staticmethod
    def _failure_page(
        partition: str,
        page: int,
        sequence: int,
        endpoint: str,
        error: Exception,
    ) -> EntityPage[CommerceProductSnapshot]:
        return EntityPage(
            page_id=f"{partition}:{page}",
            partition_key=partition,
            sequence=sequence,
            items=(),
            resume_after={"partition": partition, "page": page},
            terminal=True,
            enumeration_intact=False,
            discovered=0,
            diagnostics=(
                Diagnostic(
                    code=DiagnosticCode.ENUMERATION_INCOMPLETE,
                    severity=DiagnosticSeverity.ERROR,
                    message=f"Shopify feed page {page} failed: {error}",
                    retryable=not isinstance(error, ResponseBodyTooLarge),
                    affects_completeness=True,
                    url=f"{endpoint}?page={page}",
                ),
            ),
        )

    @staticmethod
    def _budget_page(
        partition: str, page: int, sequence: int, endpoint: str
    ) -> EntityPage[CommerceProductSnapshot]:
        return EntityPage(
            page_id=f"{partition}:{page}:budget",
            partition_key=partition,
            sequence=sequence,
            items=(),
            resume_after={"partition": partition, "page": page},
            terminal=True,
            enumeration_intact=False,
            discovered=0,
            diagnostics=(
                Diagnostic(
                    code=DiagnosticCode.REQUEST_BUDGET_EXHAUSTED,
                    severity=DiagnosticSeverity.ERROR,
                    message="request budget cannot fund the next Shopify discovery page",
                    retryable=False,
                    affects_completeness=True,
                    url=f"{endpoint}?page={page}",
                ),
            ),
        )

    def _page_limit_page(
        self, partition: str, page: int, sequence: int, endpoint: str
    ) -> EntityPage[CommerceProductSnapshot]:
        return EntityPage(
            page_id=f"{partition}:limit",
            partition_key=partition,
            sequence=sequence,
            items=(),
            resume_after={"partition": partition, "page": page},
            terminal=True,
            enumeration_intact=False,
            discovered=0,
            diagnostics=(
                Diagnostic(
                    code=DiagnosticCode.ENUMERATION_INCOMPLETE,
                    severity=DiagnosticSeverity.WARNING,
                    message=f"Shopify page limit {self.options.page_limit} reached",
                    retryable=False,
                    affects_completeness=True,
                    url=endpoint,
                ),
            ),
        )

    def checkpoint(self, request: CollectionRequest, lineage: str, resume_after: JsonValue) -> ConnectorCheckpoint:
        options = cast(dict[str, JsonValue], self.options.model_dump(mode="json"))
        return ConnectorCheckpoint(
            connector=self.name, connector_version=self.version, source_id=request.source_id,
            lineage=lineage, collection_fingerprint=collection_fingerprint(request, self.name, options),
            resume_after=resume_after,
        )

    @staticmethod
    def _resume(checkpoint: ConnectorCheckpoint | None) -> tuple[str, int, int] | None:
        if checkpoint is None:
            return None
        cursor = checkpoint.resume_after
        if not isinstance(cursor, dict) or not isinstance(cursor.get("partition"), str):
            raise ValueError("CHECKPOINT_INVALID: Shopify cursor must be an object")
        page, offset = cursor.get("page"), cursor.get("offset", 0)
        if not isinstance(page, int) or isinstance(page, bool) or page < 1 or not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise ValueError("CHECKPOINT_INVALID: Shopify cursor values are invalid")
        return str(cursor["partition"]), page, offset

    def _normalize(
        self,
        raw: dict[str, Any],
        source_id: str,
        origin: str,
        currency: str | None,
        observed: datetime,
        partition: str,
    ) -> CommerceProductSnapshot | None:
        handle = str(raw.get("handle") or "").strip()
        title = str(raw.get("title") or "").strip()
        external_id = str(raw.get("id") or handle).strip()
        if not handle or not title or not external_id:
            return None
        url = f"{origin}/products/{handle}"
        evidence = Evidence(method="api", source_url=url, source_field="products.json", observed_at=observed)
        variants = tuple(
            value
            for item in raw.get("variants") or []
            if isinstance(item, dict)
            if (value := self._variant(item, raw, currency, evidence, observed)) is not None
        )
        body_html = str(raw.get("body_html") or "")
        return CommerceProductSnapshot(
            connector=self.name, source_id=source_id, external_id=external_id,
            canonical_url=url, title=title, observed_at=observed,
            description=body_html or None,
            vendor=str(raw.get("vendor") or "") or None,
            categories=tuple(CategoryRef(name=value) for value in (partition if partition != "main" else "", str(raw.get("product_type") or "")) if value),
            images=tuple(
                MediaRef(
                    url=str(item["src"]), alt_text=str(item.get("alt") or "") or None,
                    external_id=str(item.get("id") or "") or None,
                )
                for item in raw.get("images") or []
                if isinstance(item, dict) and item.get("src")
            ),
            documents=tuple(
                DocumentRef(
                    url=urljoin(url, match), title=match, observed_at=observed,
                    evidence=(evidence,),
                )
                for match in re.findall(
                    r'href=["\']([^"\']+\.pdf(?:\?[^"\']*)?)["\']',
                    body_html, re.IGNORECASE,
                )
            ),
            variants=variants,
            source_updated_at=raw.get("updated_at"),
            platform_extensions={
                "handle": handle, "tags": raw.get("tags") or [],
                "options": raw.get("options") or [],
                "legacy_raw_product": {
                    key: value for key, value in raw.items() if key != "variants"
                },
            },
        )

    def _variant(
        self,
        raw: dict[str, Any],
        product: dict[str, Any],
        currency: str | None,
        evidence: Evidence,
        observed: datetime,
    ) -> CommerceVariant | None:
        identifier = str(raw.get("id") or "").strip()
        if not identifier:
            return None
        amount = decimal_amount(raw.get("price"))
        availability = Availability.IN_STOCK if raw.get("available") is True else Availability.OUT_OF_STOCK if raw.get("available") is False else Availability.UNKNOWN
        offers: tuple[CommerceOffer, ...] = ()
        if amount is not None and currency is not None:
            compare_at = decimal_amount(raw.get("compare_at_price"))
            current = CommerceOffer(
                price=Money(amount=amount, currency=currency), observed_at=observed,
                evidence=(evidence,),
                role="sale" if compare_at is not None and compare_at > amount else "regular",
                vat_status=self.options.vat_status or "unknown", availability=availability,
                availability_evidence=(evidence,),
            )
            offers = (current,)
            if compare_at is not None and compare_at > amount:
                offers += (current.model_copy(update={
                    "role": "regular", "price": Money(amount=compare_at, currency=currency),
                }),)
        quantity = raw.get("inventory_quantity")
        exact_quantity = quantity if isinstance(quantity, int) and not isinstance(quantity, bool) else None
        exact = exact_quantity is not None and bool(raw.get("inventory_management")) and raw.get("inventory_policy") == "deny"
        stock = StockState(
            availability=availability,
            quantity=max(exact_quantity, 0) if exact and exact_quantity is not None else None,
            quantity_kind=StockQuantityKind.EXACT if exact else StockQuantityKind.UNKNOWN,
            observed_at=observed, evidence=(evidence,),
        )
        option_names = [
            str(option.get("name") or f"option{index}")
            for index, option in enumerate(product.get("options") or [], 1)
            if isinstance(option, dict)
        ]
        variant_options = {}
        for index, name in enumerate(option_names, 1):
            value = raw.get(f"option{index}")
            if value not in (None, "Default Title"):
                variant_options[name] = str(value)
        featured = raw.get("featured_image")
        image = MediaRef(url=str(featured["src"])) if isinstance(featured, dict) and featured.get("src") else None
        attributes: dict[str, JsonValue] = dict(variant_options)
        grams = raw.get("grams")
        if isinstance(grams, (int, float)) and not isinstance(grams, bool) and grams:
            attributes["shipping_weight_g"] = grams
        if amount is not None and currency is not None:
            attributes["price_text"] = f"{raw.get('price')} {currency}"
        title = str(raw.get("title") or "").strip()
        return CommerceVariant(
            external_id=identifier,
            title=title if title.casefold() != "default title" else None,
            sku=str(raw.get("sku") or "") or None,
            gtin=str(raw.get("barcode") or "") or None,
            image=image, options=variant_options, offers=offers, stock=stock,
            published_attributes=attributes,
            platform_extensions={"legacy_raw_variant": raw},
        )

class ShopifyFactory:
    name = "shopify"
    version = ShopifyConnector.version
    options_model: type[BaseModel] = ShopifyOptions

    def build(self, *, transport: CommerceTransport, options: BaseModel, context: ConnectorContext) -> ShopifyConnector:
        return ShopifyConnector(transport, ShopifyOptions.model_validate(options), context)
