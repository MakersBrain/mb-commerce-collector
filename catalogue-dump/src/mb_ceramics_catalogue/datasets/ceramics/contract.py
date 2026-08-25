"""Typed forms of the existing ceramics v2 rows."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, JsonValue, SerializerFunctionWrapHandler, model_serializer


class _CeramicsRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: str
    external_id: str
    parent_external_id: str
    product_url: str
    extraction_method: str
    source_detail_level: str
    fetched_at: str
    source_updated_at: str | None
    name: str
    name_raw: str
    name_parsed_from: list[str] | None
    product_name: str | None
    variant_title: str | None
    brand: str | None
    brand_basis: str | None
    manufacturer_sku_basis: str | None
    manufacturer_sku: str | None
    supplier_reference: str | None
    gtin: str | None
    description: str | None
    category_path: list[str] | None
    image_url: str | None
    all_image_urls: list[str] | None
    price: float | None
    currency: str | None
    price_text: str | None
    list_price: float | None
    vat_status: str | None
    vat_rate: float | None
    unit_price: dict[str, JsonValue] | None
    availability: str | None
    stock_quantity: int | None
    min_order_quantity: int | None
    package_size: dict[str, JsonValue] | None
    family: str | None
    form: str | None
    firing: dict[str, JsonValue] | None
    surface: str | None
    effects: list[str] | None
    colour: dict[str, JsonValue] | None
    application_methods: list[str] | None
    coats: dict[str, JsonValue] | None
    claims: list[dict[str, JsonValue]] | None
    documents: list[dict[str, JsonValue]] | None
    technical_attributes: dict[str, JsonValue] | None
    raw: JsonValue
    # Platform scrapers historically add these after record.build(). Keeping
    # them optional preserves those existing v2 rows while new projectors can
    # leave them absent rather than manufacturing null fields.
    material_kind: str | None = None
    published_unit_price: str | None = None
    vat_basis: str | None = None
    collection_mode: Literal["price"] | None = None

    @model_serializer(mode="wrap")
    def _preserve_legacy_key_presence(self, handler: SerializerFunctionWrapHandler) -> dict[str, JsonValue]:
        serialized: dict[str, JsonValue] = handler(self)
        for field in (
            "material_kind",
            "published_unit_price",
            "vat_basis",
            "collection_mode",
        ):
            if field not in self.model_fields_set:
                serialized.pop(field, None)
        return serialized

    def as_legacy_dict(self) -> dict[str, JsonValue]:
        """Convert only at the legacy boundary, preserving the old key set."""
        return self.model_dump(mode="json")


class CeramicsCatalogueRecord(_CeramicsRecord):
    format: Literal["ceramics.catalogue_item.v2"]


class CeramicsIdentityRecord(_CeramicsRecord):
    format: Literal["ceramics.catalogue_identity.v2"]
