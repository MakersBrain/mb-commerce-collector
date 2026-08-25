"""Immutable version-one neutral commerce entities."""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal, cast

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    field_validator,
    model_validator,
)

from .sanitization import sanitize_json_value

NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
CurrencyCode = Annotated[str, StringConstraints(pattern=r"^[A-Z]{3}$")]


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Availability(StrEnum):
    IN_STOCK = "in_stock"
    LIMITED = "limited"
    OUT_OF_STOCK = "out_of_stock"
    BACKORDER = "backorder"
    PREORDER = "preorder"
    DISCONTINUED = "discontinued"
    UNKNOWN = "unknown"


class StockQuantityKind(StrEnum):
    EXACT = "exact"
    LOWER_BOUND = "lower_bound"
    UPPER_BOUND = "upper_bound"
    ORDER_LIMIT = "order_limit"
    UNKNOWN = "unknown"


class Money(ContractModel):
    amount: Decimal = Field(strict=True, ge=0)
    currency: CurrencyCode

    @field_validator("amount")
    @classmethod
    def finite_amount(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("money amount must be finite")
        return value


class Evidence(ContractModel):
    method: Literal["api", "jsonld", "microdata", "opengraph", "html", "cart_ceiling", "browser"]
    source_url: NonEmpty
    source_field: NonEmpty | None = None
    observed_at: AwareDatetime
    confidence: Literal["published", "verified", "derived"] = "published"


class CategoryRef(ContractModel):
    name: NonEmpty
    external_id: NonEmpty | None = None
    url: NonEmpty | None = None


class MediaRef(ContractModel):
    url: NonEmpty
    media_type: NonEmpty | None = None
    alt_text: str | None = None
    external_id: NonEmpty | None = None


class DocumentRef(ContractModel):
    url: NonEmpty
    observed_at: AwareDatetime
    evidence: tuple[Evidence, ...] = Field(min_length=1)
    title: str | None = None
    media_type: NonEmpty | None = None
    external_id: NonEmpty | None = None


class StockState(ContractModel):
    availability: Availability
    quantity: int | None = Field(default=None, ge=0)
    quantity_kind: StockQuantityKind = StockQuantityKind.UNKNOWN
    observed_at: AwareDatetime
    evidence: tuple[Evidence, ...] = Field(min_length=1)

    @field_validator("quantity")
    @classmethod
    def quantity_not_boolean(cls, value: int | None) -> int | None:
        if isinstance(value, bool):
            raise ValueError("stock quantity must be an integer, not a boolean")
        return value

    @model_validator(mode="after")
    def quantity_and_kind_agree(self) -> StockState:
        if (self.quantity is None) != (self.quantity_kind == StockQuantityKind.UNKNOWN):
            raise ValueError("quantity is present exactly when quantity_kind is known")
        return self


class CommerceOffer(ContractModel):
    price: Money
    observed_at: AwareDatetime
    evidence: tuple[Evidence, ...] = Field(min_length=1)
    role: Literal["regular", "sale", "member", "quantity_tier"] = "regular"
    vat_status: Literal["inclusive", "exclusive", "unknown"] = "unknown"
    vat_rate: Decimal | None = Field(default=None, ge=0, le=1, strict=True)
    minimum_quantity: Decimal | None = Field(default=None, gt=0, strict=True)
    unit: NonEmpty | None = None
    pack_size: Decimal | None = Field(default=None, gt=0, strict=True)
    valid_from: AwareDatetime | None = None
    valid_until: AwareDatetime | None = None
    seller_id: NonEmpty | None = None
    seller_name: NonEmpty | None = None
    availability: Availability | None = None
    availability_evidence: tuple[Evidence, ...] = ()

    @model_validator(mode="after")
    def validate_offer(self) -> CommerceOffer:
        if self.valid_from and self.valid_until and self.valid_until < self.valid_from:
            raise ValueError("offer valid_until cannot precede valid_from")
        if self.availability is not None and not self.availability_evidence:
            raise ValueError("published offer availability requires evidence")
        return self


class CommerceVariant(ContractModel):
    external_id: NonEmpty
    is_default: bool = False
    canonical_url: NonEmpty | None = None
    title: str | None = None
    sku: str | None = None
    gtin: str | None = None
    image: MediaRef | None = None
    options: dict[str, str] = Field(default_factory=dict)
    offers: tuple[CommerceOffer, ...] = ()
    stock: StockState | None = None
    published_attributes: dict[str, JsonValue] = Field(default_factory=dict)
    platform_extensions: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("platform_extensions", mode="before")
    @classmethod
    def sanitize_platform_extensions(cls, value: Any) -> Any:
        return sanitize_json_value(cast(JsonValue, value))


class CommerceProductSnapshot(ContractModel):
    contract_version: Literal["commerce.product_snapshot.v1"] = "commerce.product_snapshot.v1"
    connector: NonEmpty
    source_id: NonEmpty
    external_id: NonEmpty
    canonical_url: NonEmpty
    title: NonEmpty
    observed_at: AwareDatetime
    description: str | None = None
    vendor: str | None = None
    categories: tuple[CategoryRef, ...] = ()
    images: tuple[MediaRef, ...] = ()
    documents: tuple[DocumentRef, ...] = ()
    variants: tuple[CommerceVariant, ...] = ()
    source_updated_at: AwareDatetime | None = None
    published_attributes: dict[str, JsonValue] = Field(default_factory=dict)
    platform_extensions: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("platform_extensions", mode="before")
    @classmethod
    def sanitize_platform_extensions(cls, value: Any) -> Any:
        return sanitize_json_value(cast(JsonValue, value))


def sanitize_commerce_snapshot(
    snapshot: CommerceProductSnapshot,
) -> CommerceProductSnapshot:
    """Reapply extension safety after unvalidated Pydantic copy/construct paths."""

    variants = tuple(
        variant.model_copy(
            update={
                "platform_extensions": sanitize_json_value(
                    cast(JsonValue, variant.platform_extensions)
                )
            }
        )
        for variant in snapshot.variants
    )
    return snapshot.model_copy(
        update={
            "variants": variants,
            "platform_extensions": sanitize_json_value(
                cast(JsonValue, snapshot.platform_extensions)
            ),
        }
    )
