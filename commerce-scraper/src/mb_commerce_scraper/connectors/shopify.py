from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, cast
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, JsonValue

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


class ShopifyOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    vat_status: Literal["inclusive", "exclusive", "unknown"] = "unknown"
    page_size: int = Field(default=250, ge=1, le=250)
    page_limit: int = Field(default=200, ge=1)


class ShopifyConnector(CommerceConnector):
    name = "shopify"
    platform = "shopify"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.UNKNOWN}),
        supports_documents=True,
        browser=BrowserRequirement.NEVER,
    )

    def __init__(self, transport: CommerceTransport, options: ShopifyOptions, context: ConnectorContext | None = None) -> None:
        self.transport = transport
        self.options = options
        self.context = context or ConnectorContext()

    async def collect(self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None = None) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        if not self.capabilities.supports(request.requested_fields, request.refresh_mode):
            raise ValueError("Shopify does not support the requested contract")
        options = cast(dict[str, JsonValue], self.options.model_dump(mode="json"))
        validate_checkpoint(checkpoint, connector=self.name, connector_version=self.version, request=request, options=options)
        origin = self._origin(request.base_url)
        currency = self.options.currency or await self._currency(origin)
        partitions = request.partitions or ("main",)
        start_partition, start_page, start_offset = self._resume(checkpoint)
        emitted = 0
        sequence = 0
        for partition in partitions:
            if start_partition is not None and partitions.index(partition) < partitions.index(start_partition):
                continue
            page_number = start_page if partition == start_partition else 1
            offset = start_offset if partition == start_partition else 0
            endpoint = f"{origin}/products.json" if partition == "main" else f"{origin}/collections/{partition}/products.json"
            while page_number <= self.options.page_limit:
                if self.context.cancelled():
                    return
                response = await self.transport.request(TransportRequest(
                    url=endpoint,
                    query={"limit": self.options.page_size, "page": page_number},
                    purpose=RequestPurpose.DISCOVERY,
                    priority=RequestPriority.DISCOVERY,
                    estimated_bytes=1_000_000,
                ))
                if response.status >= 400:
                    raise RuntimeError(f"Shopify feed failed with status {response.status}")
                body = response.json_value()
                products = body.get("products", []) if isinstance(body, dict) else []
                products = products if isinstance(products, list) else []
                available = products[offset:]
                remaining = None if request.result_limit is None else request.result_limit - emitted
                selected = available if remaining is None else available[:remaining]
                observed = self.context.clock()
                snapshots = tuple(
                    value for raw in selected if isinstance(raw, dict)
                    if (value := self._normalize(raw, request.source_id, origin, currency, observed, partition)) is not None
                )
                emitted += len(snapshots)
                limited = request.result_limit is not None and emitted >= request.result_limit and (len(selected) < len(available) or len(products) >= self.options.page_size)
                exhausted = len(products) < self.options.page_size
                partition_terminal = limited or exhausted
                last_partition = partition == partitions[-1]
                terminal = partition_terminal and (limited or last_partition)
                if limited and len(selected) < len(available):
                    cursor: JsonValue | None = {"partition": partition, "page": page_number, "offset": offset + len(selected)}
                elif exhausted:
                    cursor = None if last_partition else {"partition": partitions[partitions.index(partition) + 1], "page": 1, "offset": 0}
                else:
                    cursor = {"partition": partition, "page": page_number + 1, "offset": 0}
                yield EntityPage(
                    page_id=f"{partition}:{page_number}", partition_key=partition, sequence=sequence,
                    items=snapshots, resume_after=cursor, terminal=terminal,
                    partition_terminal=partition_terminal, enumeration_intact=not limited,
                    discovered=len(products),
                    diagnostics=((result_limit_diagnostic(request.result_limit, endpoint),) if limited and request.result_limit else ()),
                )
                sequence += 1
                if partition_terminal:
                    if limited:
                        return
                    break
                page_number += 1
                offset = 0

    async def _currency(self, origin: str) -> str | None:
        response = await self.transport.request(TransportRequest(
            url=f"{origin}/meta.json", purpose=RequestPurpose.ENRICHMENT,
            priority=RequestPriority.OPTIONAL, required=False, estimated_bytes=10_000,
        ))
        if response.status >= 400:
            return None
        payload = response.json_value()
        value = payload.get("currency") if isinstance(payload, dict) else None
        currency = str(value).strip().upper() if value else ""
        return currency if len(currency) == 3 and currency.isalpha() else None

    def checkpoint(self, request: CollectionRequest, lineage: str, resume_after: JsonValue) -> ConnectorCheckpoint:
        options = cast(dict[str, JsonValue], self.options.model_dump(mode="json"))
        return ConnectorCheckpoint(
            connector=self.name, connector_version=self.version, source_id=request.source_id,
            lineage=lineage, collection_fingerprint=collection_fingerprint(request, self.name, options),
            resume_after=resume_after,
        )

    @staticmethod
    def _origin(url: str) -> str:
        parsed = urlsplit(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    @staticmethod
    def _resume(checkpoint: ConnectorCheckpoint | None) -> tuple[str | None, int, int]:
        if checkpoint is None:
            return None, 1, 0
        cursor = checkpoint.resume_after
        if not isinstance(cursor, dict) or not isinstance(cursor.get("partition"), str):
            raise ValueError("CHECKPOINT_INVALID: Shopify cursor must be an object")
        page, offset = cursor.get("page"), cursor.get("offset", 0)
        if not isinstance(page, int) or isinstance(page, bool) or page < 1 or not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise ValueError("CHECKPOINT_INVALID: Shopify cursor values are invalid")
        return str(cursor["partition"]), page, offset

    def _normalize(self, raw: dict[str, Any], source_id: str, origin: str, currency: str | None, observed: Any, partition: str) -> CommerceProductSnapshot | None:
        handle = str(raw.get("handle") or "").strip()
        title = str(raw.get("title") or "").strip()
        external_id = str(raw.get("id") or handle).strip()
        if not handle or not title or not external_id:
            return None
        url = f"{origin}/products/{handle}"
        evidence = Evidence(method="api", source_url=url, source_field="products.json", observed_at=observed)
        variants = tuple(value for item in raw.get("variants") or [] if isinstance(item, dict) if (value := self._variant(item, currency, evidence, observed)) is not None)
        return CommerceProductSnapshot(
            connector=self.name, source_id=source_id, external_id=external_id,
            canonical_url=url, title=title, observed_at=observed,
            description=str(raw.get("body_html") or "") or None,
            vendor=str(raw.get("vendor") or "") or None,
            categories=tuple(CategoryRef(name=value) for value in (partition if partition != "main" else "", str(raw.get("product_type") or "")) if value),
            images=tuple(MediaRef(url=str(item["src"]), alt_text=str(item.get("alt") or "") or None) for item in raw.get("images") or [] if isinstance(item, dict) and item.get("src")),
            variants=variants,
            platform_extensions={"handle": handle, "tags": raw.get("tags") or []},
        )

    def _variant(self, raw: dict[str, Any], currency: str | None, evidence: Evidence, observed: Any) -> CommerceVariant | None:
        identifier = str(raw.get("id") or "").strip()
        if not identifier:
            return None
        amount = self._decimal(raw.get("price"))
        availability = Availability.IN_STOCK if raw.get("available") is True else Availability.OUT_OF_STOCK if raw.get("available") is False else Availability.UNKNOWN
        offers = () if amount is None or currency is None else (CommerceOffer(price=Money(amount=amount, currency=currency), observed_at=observed, evidence=(evidence,), vat_status=self.options.vat_status, availability=availability, availability_evidence=(evidence,)),)
        stock = StockState(availability=availability, observed_at=observed, evidence=(evidence,))
        return CommerceVariant(external_id=identifier, title=str(raw.get("title") or "") or None, sku=str(raw.get("sku") or "") or None, gtin=str(raw.get("barcode") or "") or None, offers=offers, stock=stock)

    @staticmethod
    def _decimal(value: Any) -> Decimal | None:
        try:
            amount = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
        return amount if amount.is_finite() and amount >= 0 else None


class ShopifyFactory:
    name = "shopify"
    options_model: type[BaseModel] = ShopifyOptions

    def build(self, *, transport: CommerceTransport, options: BaseModel, context: ConnectorContext) -> ShopifyConnector:
        return ShopifyConnector(transport, ShopifyOptions.model_validate(options), context)
