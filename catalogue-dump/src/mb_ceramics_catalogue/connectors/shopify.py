"""Shopify public-feed connector producing neutral commerce snapshots."""

from __future__ import annotations

import html
import re
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Protocol
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
    BudgetDecision,
    ConnectorBudget,
    RequestBudgetProtocol,
    RequestCost,
    RequestPriority,
)

PAGE_SIZE = 250


class JsonFetcher(Protocol):
    async def json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any: ...

    async def text(self, url: str, *, headers: dict[str, str] | None = None) -> str: ...

    async def rotate_client(self) -> None: ...


class ShopifyOptions(BaseModel):
    """Connector-owned options; dataset concerns intentionally do not belong here."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None = None
    page_size: int = Field(default=PAGE_SIZE, ge=1, le=PAGE_SIZE)
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
        supports_incremental_cursor=False,
        supports_category_filter=False,
        supports_documents=True,
        browser=BrowserRequirement.NEVER,
        shared_edge="edge:shopify",
    )

    def __init__(
        self,
        fetcher: JsonFetcher,
        options: ShopifyOptions | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
        budget: RequestBudgetProtocol | None = None,
    ) -> None:
        self._fetcher = fetcher
        self.options = options or ShopifyOptions()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._budget = ConnectorBudget(budget)

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        self._validate_request(request, checkpoint)
        origin = self._origin(request.base_url)
        currency = self.options.currency or await self._currency(origin)
        partitions = request.collections or ("main",)
        resume = self._resume(checkpoint)
        resume_index = 0
        if resume is not None:
            try:
                resume_index = partitions.index(resume[0])
            except ValueError as error:
                raise ValueError("Shopify checkpoint partition is not in the request") from error
        emitted = 0

        for partition_index, partition in enumerate(partitions):
            if resume is not None and partition_index < resume_index:
                continue
            page = resume[1] if resume is not None and partition_index == resume_index else 1
            offset = resume[2] if resume is not None and partition_index == resume_index else 0
            endpoint = (
                f"{origin}/products.json"
                if partition == "main"
                else f"{origin}/collections/{partition}/products.json"
            )
            while page <= self.options.page_limit:
                if request.cancelled():
                    return
                if not self._allow_discovery():
                    yield self._budget_exhausted_page(partition, page, endpoint)
                    return
                try:
                    payload = await self._fetcher.json(
                        endpoint,
                        params={"limit": self.options.page_size, "page": page},
                    )
                    raw_products = payload.get("products") if isinstance(payload, dict) else None
                    products = raw_products if isinstance(raw_products, list) else []
                except (httpx.HTTPError, RuntimeError) as error:
                    diagnostic = Diagnostic(
                        code=DiagnosticCode.ENUMERATION_INCOMPLETE,
                        severity=DiagnosticSeverity.ERROR,
                        message=f"Shopify feed page {page} failed: {error}",
                        retryable=True,
                        affects_completeness=True,
                        url=f"{endpoint}?page={page}",
                    )
                    yield EntityPage(
                        page_id=f"{partition}:{page}",
                        partition_key=partition,
                        sequence=page - 1,
                        items=(),
                        terminal=True,
                        enumeration_intact=False,
                        discovered=0,
                        diagnostics=(diagnostic,),
                        resume_after={"partition": partition, "page": page},
                    )
                    return
                observed_at = self._clock()
                available = products[offset:]
                remaining = None if request.result_limit is None else request.result_limit - emitted
                selected = available if remaining is None else available[: max(remaining, 0)]
                inventory_diagnostics = await self._enrich_inventory(
                    [item for item in selected if isinstance(item, dict)], origin
                )
                snapshots = tuple(
                    snapshot
                    for product in selected
                    if isinstance(product, dict)
                    and (
                        snapshot := self._normalize(
                            product, request.source_id, origin, currency, observed_at, partition
                        )
                    )
                    is not None
                )
                emitted += len(selected)
                limited = (
                    request.result_limit is not None
                    and emitted >= request.result_limit
                    and (len(selected) < len(available) or len(products) >= self.options.page_size)
                )
                partition_terminal = limited or len(products) < self.options.page_size
                terminal = partition_terminal and (
                    limited or partition_index == len(partitions) - 1
                )
                if limited and len(selected) < len(available):
                    next_cursor: JsonValue | None = {
                        "partition": partition,
                        "page": page,
                        "offset": offset + len(selected),
                    }
                elif partition_terminal and not limited:
                    next_cursor = (
                        None
                        if terminal
                        else {"partition": partitions[partition_index + 1], "page": 1}
                    )
                else:
                    next_cursor = {"partition": partition, "page": page + 1}
                diagnostics = inventory_diagnostics + (
                    (result_limit_diagnostic(request.result_limit, endpoint),)
                    if limited and request.result_limit is not None
                    else ()
                )
                yield EntityPage(
                    page_id=f"{partition}:{page}",
                    partition_key=partition,
                    sequence=page - 1,
                    items=snapshots,
                    resume_after=next_cursor,
                    terminal=terminal,
                    partition_terminal=partition_terminal,
                    enumeration_intact=not limited,
                    discovered=len(products),
                    diagnostics=diagnostics,
                )
                if partition_terminal:
                    if limited:
                        return
                    break
                page += 1
                offset = 0
            else:
                diagnostic = Diagnostic(
                    code=DiagnosticCode.ENUMERATION_INCOMPLETE,
                    severity=DiagnosticSeverity.WARNING,
                    message=f"Shopify page limit {self.options.page_limit} reached",
                    retryable=False,
                    affects_completeness=True,
                    url=endpoint,
                )
                yield EntityPage(
                    page_id=f"{partition}:limit",
                    partition_key=partition,
                    sequence=self.options.page_limit,
                    items=(),
                    resume_after={"partition": partition, "page": page},
                    terminal=True,
                    enumeration_intact=False,
                    discovered=0,
                    diagnostics=(diagnostic,),
                )
                return
            resume = None

    def _validate_request(self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None) -> None:
        if not self.capabilities.supports(request.requested_fields, request.refresh_mode):
            raise ValueError("Shopify connector does not support the requested contract")
        if checkpoint is not None and (
            checkpoint.connector != self.name
            or checkpoint.connector_version != self.version
            or checkpoint.source_id != request.source_id
        ):
            raise ValueError("checkpoint does not belong to this connector configuration")

    async def _currency(self, origin: str) -> str | None:
        try:
            payload = await self._fetcher.json(f"{origin}/meta.json")
        except (httpx.HTTPError, RuntimeError):
            return None
        value = payload.get("currency") if isinstance(payload, dict) else None
        cleaned = str(value).strip().upper() if value else ""
        return cleaned if len(cleaned) == 3 and cleaned.isalpha() else None

    def _allow_discovery(self) -> bool:
        return (
            self._budget.claim(
                RequestPriority.DISCOVERY,
                required=True,
                proxy_bytes=self.options.discovery_request_estimated_bytes,
            )
            == BudgetDecision.ALLOW
        )

    def _budget_exhausted_page(
        self, partition: str, page: int, endpoint: str
    ) -> EntityPage[CommerceProductSnapshot]:
        return EntityPage(
            page_id=f"{partition}:{page}:budget",
            partition_key=partition,
            sequence=page - 1,
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

    async def _enrich_inventory(self, products: list[dict[str, Any]], origin: str) -> tuple[Diagnostic, ...]:
        if self.options.inventory_method == "none" or not products:
            return ()
        failures = 0
        deferred = 0
        batch_size = self.options.inventory_batch_size
        for offset in range(0, len(products), batch_size):
            batch = products[offset : offset + batch_size]
            for index, product in enumerate(batch):
                if not self._allow_optional_inventory():
                    deferred += len(products) - offset - index
                    break
                try:
                    detail = await self._inventory_detail(product, origin)
                except (httpx.HTTPError, RuntimeError):
                    failures += 1
                else:
                    self._merge_inventory(product, detail)
            if deferred:
                break
            if offset + batch_size < len(products):
                await self._fetcher.rotate_client()
        skipped = failures + deferred
        if not skipped:
            return ()
        return (
            Diagnostic(
                code=DiagnosticCode.OPTIONAL_ENRICHMENT_SKIPPED,
                severity=DiagnosticSeverity.WARNING,
                message=(
                    f"inventory unavailable for {skipped} product responses; those quantities remain unknown"
                ),
                retryable=failures > 0,
                affects_completeness=False,
            ),
        )

    def _allow_optional_inventory(self) -> bool:
        return (
            self._budget.claim(
                RequestPriority.OPTIONAL,
                required=False,
                proxy_bytes=self.options.inventory_request_estimated_bytes,
                future_reserve=RequestCost(
                    http_requests=1,
                    proxy_bytes=self.options.discovery_request_estimated_bytes,
                ),
            )
            == BudgetDecision.ALLOW
        )

    async def _inventory_detail(self, product: dict[str, Any], origin: str) -> dict[str, Any]:
        handle = str(product.get("handle") or "")
        if not handle:
            return {}
        product_url = f"{origin}/products/{handle}"
        headers = {"Cookie": ""}
        if self.options.inventory_method == "product_json":
            payload = await self._fetcher.json(f"{product_url}.js", headers=headers)
            return payload if isinstance(payload, dict) else {}
        endpoint = product_url
        if self.options.inventory_section_id:
            from urllib.parse import urlencode

            endpoint = f"{endpoint}?{urlencode({'section_id': self.options.inventory_section_id})}"
        document = await self._fetcher.text(endpoint, headers=headers)
        variant_ids = {
            str(item.get("id"))
            for item in product.get("variants") or []
            if isinstance(item, dict) and item.get("id")
        }
        return {"variants": list(self._inventory_from_html(document, variant_ids).values())}

    @staticmethod
    def _merge_inventory(product: dict[str, Any], detail: dict[str, Any]) -> None:
        by_id = {
            str(item.get("id")): item
            for item in detail.get("variants") or []
            if isinstance(item, dict) and item.get("id")
        }
        for variant in product.get("variants") or []:
            if not isinstance(variant, dict) or not (extra := by_id.get(str(variant.get("id")))):
                continue
            for key in ("inventory_quantity", "inventory_management", "inventory_policy"):
                if key in extra:
                    variant[key] = extra[key]

    @staticmethod
    def _inventory_from_html(document: str, variant_ids: set[str]) -> dict[str, dict[str, Any]]:
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
                    r'data-inventory-policy=["\']deny["\']', option.group(0), re.IGNORECASE
                ):
                    match = re.search(r'data-inventory=["\'](-?\d+)["\']', option.group(0), re.IGNORECASE)
                    if match:
                        quantity = int(match.group(1))

            if quantity is None and re.search(
                rf'gwProductInventoryPolicy\[{escaped}\]\s*=\s*["\']deny["\']', document
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

    def _normalize(
        self,
        product: dict[str, Any],
        source_id: str,
        origin: str,
        currency: str | None,
        observed_at: datetime,
        partition: str,
    ) -> CommerceProductSnapshot | None:
        handle = str(product.get("handle") or "").strip()
        title = str(product.get("title") or "").strip()
        external_id = str(product.get("id") or handle).strip()
        if not handle or not title or not external_id:
            return None
        product_url = f"{origin}/products/{handle}"
        evidence = Evidence(
            method="api", source_url=product_url, source_field="products.json", observed_at=observed_at
        )
        variants = tuple(
            variant
            for raw in product.get("variants") or []
            if isinstance(raw, dict)
            and (
                variant := self._variant(
                    raw, product, currency, evidence, observed_at, self.options.vat_status
                )
            )
            is not None
        )
        product_type = str(product.get("product_type") or "").strip()
        categories = tuple(
            CategoryRef(name=value)
            for value in ((partition if partition != "main" else ""), product_type)
            if value
        )
        images = tuple(
            MediaRef(
                url=str(image["src"]),
                alt_text=str(image.get("alt") or "") or None,
                external_id=str(image.get("id") or "") or None,
            )
            for image in product.get("images") or []
            if isinstance(image, dict) and image.get("src")
        )
        documents = tuple(
            DocumentRef(
                url=urljoin(product_url, match),
                title=match,
                observed_at=observed_at,
                evidence=(evidence,),
            )
            for match in re.findall(
                r'href=["\']([^"\']+\.pdf(?:\?[^"\']*)?)["\']',
                str(product.get("body_html") or ""),
                re.IGNORECASE,
            )
        )
        return CommerceProductSnapshot(
            connector=self.name,
            source_id=source_id,
            external_id=external_id,
            canonical_url=product_url,
            title=title,
            observed_at=observed_at,
            description=(
                " ".join(
                    html.unescape(
                        re.sub(
                            r"<[^>]+>",
                            " ",
                            str(product.get("body_html") or ""),
                        )
                    ).split()
                )
                or None
            ),
            vendor=str(product.get("vendor") or "") or None,
            categories=categories,
            images=images,
            documents=documents,
            variants=variants,
            source_updated_at=product.get("updated_at"),
            platform_extensions={
                "handle": handle,
                "tags": product.get("tags") or [],
                "options": product.get("options") or [],
                "legacy_raw_product": {key: value for key, value in product.items() if key != "variants"},
            },
        )

    @staticmethod
    def _variant(
        raw: dict[str, Any],
        product: dict[str, Any],
        currency: str | None,
        evidence: Evidence,
        observed_at: datetime,
        vat_status: Literal["inclusive", "exclusive", "unknown"] | None,
    ) -> CommerceVariant | None:
        external_id = str(raw.get("id") or "").strip()
        if not external_id:
            return None
        offers: tuple[CommerceOffer, ...] = ()
        amount = ShopifyConnector._decimal(raw.get("price"))
        if amount is not None and currency is not None:
            available = (
                Availability.IN_STOCK
                if raw.get("available") is True
                else Availability.OUT_OF_STOCK
                if raw.get("available") is False
                else Availability.UNKNOWN
            )
            sale = ShopifyConnector._decimal(raw.get("compare_at_price"))
            current = CommerceOffer(
                price=Money(amount=amount, currency=currency),
                observed_at=observed_at,
                evidence=(evidence,),
                role="sale" if sale is not None and sale > amount else "regular",
                vat_status=vat_status or "unknown",
                availability=available,
                availability_evidence=(evidence,),
            )
            offers = (current,)
            if sale is not None and sale > amount:
                offers += (
                    CommerceOffer(
                        price=Money(amount=sale, currency=currency),
                        observed_at=observed_at,
                        evidence=(evidence,),
                        role="regular",
                        vat_status=vat_status or "unknown",
                        availability=available,
                        availability_evidence=(evidence,),
                    ),
                )
        quantity = raw.get("inventory_quantity")
        exact_quantity = quantity if isinstance(quantity, int) and not isinstance(quantity, bool) else None
        exact = (
            exact_quantity is not None
            and bool(raw.get("inventory_management"))
            and raw.get("inventory_policy") == "deny"
        )
        stock = StockState(
            availability=(
                Availability.IN_STOCK
                if raw.get("available") is True
                else Availability.OUT_OF_STOCK
                if raw.get("available") is False
                else Availability.UNKNOWN
            ),
            quantity=max(exact_quantity, 0) if exact and exact_quantity is not None else None,
            quantity_kind=StockQuantityKind.EXACT if exact else StockQuantityKind.UNKNOWN,
            observed_at=observed_at,
            evidence=(evidence,),
        )
        option_names = [
            str(option.get("name") or f"option{index}")
            for index, option in enumerate(product.get("options") or [], 1)
            if isinstance(option, dict)
        ]
        options = {}
        for index, name in enumerate(option_names, 1):
            value = raw.get(f"option{index}")
            if value not in (None, "Default Title"):
                options[name] = str(value)
        featured = raw.get("featured_image")
        image = (
            MediaRef(url=str(featured["src"])) if isinstance(featured, dict) and featured.get("src") else None
        )
        attributes: dict[str, JsonValue] = dict(options)
        grams = raw.get("grams")
        if isinstance(grams, (int, float)) and not isinstance(grams, bool) and grams:
            attributes["shipping_weight_g"] = grams
        if amount is not None and currency is not None:
            attributes["price_text"] = f"{raw.get('price')} {currency}"
        return CommerceVariant(
            external_id=external_id,
            title=(
                title
                if (title := str(raw.get("title") or "").strip()).casefold() != "default title"
                else None
            ),
            sku=str(raw.get("sku") or "") or None,
            gtin=str(raw.get("barcode") or "") or None,
            image=image,
            options=options,
            offers=offers,
            stock=stock,
            published_attributes=attributes,
            platform_extensions={"legacy_raw_variant": raw},
        )

    @staticmethod
    def _decimal(value: Any) -> Decimal | None:
        try:
            amount = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
        return amount if amount.is_finite() and amount >= 0 else None

    @staticmethod
    def _origin(url: str) -> str:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Shopify base_url must be an absolute HTTP(S) URL")
        return urljoin(url, "/").rstrip("/")

    @staticmethod
    def _resume(checkpoint: ConnectorCheckpoint | None) -> tuple[str, int, int] | None:
        if checkpoint is None:
            return None
        cursor = checkpoint.resume_after
        if not isinstance(cursor, dict):
            raise ValueError("Shopify checkpoint cursor must be an object")
        partition = cursor.get("partition")
        page = cursor.get("page")
        offset = cursor.get("offset", 0)
        if not isinstance(partition, str) or not isinstance(page, int) or page < 1:
            raise ValueError("Shopify checkpoint cursor is invalid")
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise ValueError("Shopify checkpoint offset is invalid")
        return partition, page, offset
