"""A small third-party feed connector with no import-time side effects."""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field

from mb_commerce_scraper import (
    CollectionRequest,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    ConnectorCheckpoint,
    EntityPage,
    Evidence,
    Money,
    RefreshMode,
    SnapshotField,
)
from mb_commerce_scraper.connectors import (
    ConnectorCapabilities,
    ConnectorContext,
    ConnectorPlan,
)
from mb_commerce_scraper.transports import (
    CommerceTransport,
    RequestPriority,
    RequestPurpose,
    TransportRequest,
)


class ExampleFeedOptions(BaseModel):
    """Connector-owned, data-only configuration exposed through the registry."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    feed_path: str = Field(default="/catalog.json", pattern=r"^/[A-Za-z0-9._~!$&'()*+,;=:@%/-]+$")
    currency: str = Field(default="EUR", pattern=r"^[A-Z]{3}$")


class ExampleFeedConnector:
    name = "example-feed"
    platform = "example"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(
            {SnapshotField.IDENTITY, SnapshotField.VARIANTS, SnapshotField.OFFERS}
        ),
        refresh_modes=frozenset({RefreshMode.FULL}),
    )

    def __init__(
        self,
        transport: CommerceTransport,
        options: ExampleFeedOptions,
        context: ConnectorContext,
    ) -> None:
        self._transport = transport
        self._options = options
        self._context = context

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        if not self.capabilities.supports(request.requested_fields, request.refresh_mode):
            raise ValueError("example-feed does not support the requested collection contract")
        if checkpoint is not None:
            raise ValueError("example-feed does not support checkpoints")
        if self._context.cancelled():
            return

        response = await self._transport.request(
            TransportRequest(
                url=f"{request.base_url.rstrip('/')}{self._options.feed_path}",
                purpose=RequestPurpose.ENTITY,
                priority=RequestPriority.DATASET_REQUIRED,
            )
        )
        if response.status != 200:
            raise RuntimeError(f"catalog feed returned HTTP {response.status}")
        payload = response.json_value()
        products = _product_records(payload)
        limit = request.result_limit or len(products)
        items = tuple(self._snapshot(request, product) for product in products[:limit])
        yield EntityPage(
            page_id="catalog",
            sequence=0,
            items=items,
            terminal=True,
            discovered=len(products),
        )

    def _snapshot(
        self, request: CollectionRequest, product: dict[str, Any]
    ) -> CommerceProductSnapshot:
        external_id = _required_text(product, "id")
        title = _required_text(product, "title")
        product_path = _required_path(product, "path")
        observed_at = self._context.clock()
        source_url = f"{request.base_url.rstrip('/')}{self._options.feed_path}"
        evidence = Evidence(
            method="api",
            source_url=source_url,
            source_field="price",
            observed_at=observed_at,
        )
        offer = CommerceOffer(
            price=Money(
                amount=Decimal(_required_text(product, "price")),
                currency=self._options.currency,
            ),
            observed_at=observed_at,
            evidence=(evidence,),
        )
        canonical_url = f"{request.base_url.rstrip('/')}{product_path}"
        return CommerceProductSnapshot(
            connector=self.name,
            source_id=request.source_id,
            external_id=external_id,
            canonical_url=canonical_url,
            title=title,
            observed_at=observed_at,
            variants=(
                CommerceVariant(
                    external_id=external_id,
                    is_default=True,
                    canonical_url=canonical_url,
                    title=title,
                    offers=(offer,),
                ),
            ),
        )


class ExampleFeedFactory:
    name = ExampleFeedConnector.name
    version = ExampleFeedConnector.version
    options_model: type[BaseModel] = ExampleFeedOptions

    def plan(
        self,
        options: BaseModel,
        *,
        base_url: str,
        request_partitions: tuple[str, ...] = (),
    ) -> ConnectorPlan:
        del base_url, request_partitions
        if not isinstance(options, ExampleFeedOptions):
            raise TypeError(
                "example-feed factory requires validated ExampleFeedOptions options"
            )
        return ConnectorPlan()

    def build(
        self,
        *,
        transport: CommerceTransport,
        options: BaseModel,
        context: ConnectorContext,
    ) -> ExampleFeedConnector:
        return ExampleFeedConnector(
            transport,
            ExampleFeedOptions.model_validate(options),
            context,
        )


def _product_records(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("products"), list):
        raise ValueError("catalog feed must contain a products array")
    products = payload["products"]
    if not all(isinstance(product, dict) for product in products):
        raise ValueError("every catalog product must be an object")
    return products


def _required_text(product: dict[str, Any], field: str) -> str:
    value = product.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"catalog product {field} must be a non-empty string")
    return value.strip()


def _required_path(product: dict[str, Any], field: str) -> str:
    value = _required_text(product, field)
    parsed = urlsplit(value)
    if not value.startswith("/") or parsed.scheme or parsed.netloc:
        raise ValueError(f"catalog product {field} must be an origin-relative path")
    return value


# Entry points expose a factory object. Importing this module opens no resources.
connector_factory = ExampleFeedFactory()

__all__ = [
    "ExampleFeedConnector",
    "ExampleFeedFactory",
    "ExampleFeedOptions",
    "connector_factory",
]
