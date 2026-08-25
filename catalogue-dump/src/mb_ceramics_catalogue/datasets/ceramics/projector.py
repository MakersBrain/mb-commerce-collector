"""Compatibility projection through the proven ceramics record builder."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict

from mb_ceramics_catalogue.connectors import (
    Availability,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    SnapshotField,
    StockQuantityKind,
)
from mb_ceramics_catalogue.datasets.base import ProjectionContext
from mb_ceramics_catalogue.scrapers import domain
from mb_ceramics_catalogue.scrapers import record as legacy_record

from .contract import CeramicsCatalogueRecord, CeramicsIdentityRecord
from .policies import Sio2ProjectionPolicy

_METHODS = {
    "api": "api_json",
    "jsonld": "jsonld",
    "html": "html",
    "cart_ceiling": "cart_ceiling",
    "browser": "browser",
}


class CeramicsProjectionOptions(BaseModel):
    """The legacy source traits and explicit v2 offer selection rule."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scope: Literal["materials", "all"] = "all"
    enrichments: tuple[str, ...] = ()
    brand: str | None = None
    is_manufacturer: bool = False
    extraction_method: str | None = None
    source_detail_level: str = "api"
    apply_scope: bool = True
    material_categories: tuple[str, ...] = ()
    excluded_categories: tuple[str, ...] = ()
    source_policy: Literal["sio2"] | None = None
    collection_mode: Literal["full", "price"] = "full"
    offer_role_priority: tuple[Literal["sale", "regular", "member", "quantity_tier"], ...] = (
        "sale",
        "regular",
    )


class _CeramicsProjector:
    version = "2"
    projector_version = "compatibility-1"
    required_snapshot_fields = frozenset(
        {
            SnapshotField.IDENTITY,
            SnapshotField.CATEGORIES,
            SnapshotField.IMAGES,
            SnapshotField.VARIANTS,
            SnapshotField.PUBLISHED_ATTRIBUTES,
        }
    )
    required_capabilities: frozenset[str] = frozenset()
    identity_only: bool
    record_model: type[CeramicsCatalogueRecord] | type[CeramicsIdentityRecord]

    def project(
        self,
        entity: CommerceProductSnapshot,
        context: ProjectionContext,
    ) -> Iterable[CeramicsCatalogueRecord | CeramicsIdentityRecord]:
        options = CeramicsProjectionOptions.model_validate(context.configuration)
        variants: tuple[CommerceVariant | None, ...]
        if entity.variants:
            variants = entity.variants
        elif self.identity_only:
            variants = (None,)
        else:
            variants = ()

        traits = {
            entity.source_id: {
                "brand": options.brand,
                "is_manufacturer": options.is_manufacturer,
                "scope": options.scope,
                "enrichments": list(options.enrichments),
            }
        }
        with legacy_record.RecordBuilder(traits):
            return tuple(self._project_variants(entity, variants, options))

    def _project_variants(
        self,
        entity: CommerceProductSnapshot,
        variants: tuple[CommerceVariant | None, ...],
        options: CeramicsProjectionOptions,
    ) -> Iterator[CeramicsCatalogueRecord | CeramicsIdentityRecord]:
        for variant in variants:
            offer = self._select_offer(variant, options)
            if offer is None and not self.identity_only:
                continue
            row = self._build(entity, variant, offer, options)
            if options.source_policy == "sio2":
                policy_row = Sio2ProjectionPolicy().apply(row, entity.canonical_url)
                if policy_row is None:
                    continue
                row = policy_row
            if not legacy_record.is_valid(row):
                continue
            if not self._in_scope(entity, row, options):
                continue
            yield self.record_model.model_validate(row)

    @staticmethod
    def _in_scope(
        entity: CommerceProductSnapshot,
        row: dict[str, object],
        options: CeramicsProjectionOptions,
    ) -> bool:
        """Preserve the legacy category overrides at the projection boundary."""
        if not options.apply_scope or options.scope != "materials":
            return True

        raw_categories = row.get("category_path")
        categories = " ".join(
            str(value)
            for value in (
                raw_categories if isinstance(raw_categories, list | tuple) else ()
            )
        )
        excluded_haystack = domain.fold(
            " ".join(
                domain.clean(value)
                for value in (categories, row.get("name"))
                if value
            )
        )
        if any(
            domain.fold(category) in excluded_haystack
            for category in options.excluded_categories
        ):
            return False

        if options.material_categories:
            extensions = entity.platform_extensions
            raw_tags = extensions.get("tags") or ()
            tags = raw_tags if isinstance(raw_tags, list | tuple) else (raw_tags,)
            allowed_haystack = domain.fold(
                " ".join(
                    domain.clean(value)
                    for value in (
                        *(category.name for category in entity.categories),
                        *(str(tag) for tag in tags),
                        extensions.get("handle"),
                    )
                    if value
                )
            )
            if not any(
                domain.fold(category) in allowed_haystack
                for category in options.material_categories
            ):
                return False
            return not domain.looks_non_material(row.get("name"))

        return legacy_record.in_scope(row, strict=True)

    def _build(
        self,
        entity: CommerceProductSnapshot,
        variant: CommerceVariant | None,
        offer: CommerceOffer | None,
        options: CeramicsProjectionOptions,
    ) -> dict[str, object]:
        variant_attributes = variant.published_attributes if variant is not None else {}
        attributes = {**entity.published_attributes, **variant_attributes}
        technical_attributes: dict[str, object] = dict(variant.options) if variant is not None else {}
        technical_attributes.update(
            {
                key: value
                for key, value in attributes.items()
                if key
                not in {
                    "manufacturer_sku",
                    "supplier_reference",
                    "gtin",
                    "price_text",
                    "published_unit_price",
                    "vat_basis",
                    "claims",
                    "legacy_raw_variant",
                    "legacy_source_updated_at",
                    "legacy_all_image_urls",
                    "legacy_currency",
                }
            }
        )
        product_url = (
            variant.canonical_url if variant is not None and variant.canonical_url else entity.canonical_url
        )
        variant_title = variant.title if variant is not None else None
        legacy_record_name = _string(entity.platform_extensions.get("legacy_record_name"))
        name = legacy_record_name or f"{entity.title} {variant_title or ''}".strip()
        images = [item.url for item in entity.images]
        legacy_variant_images = attributes.get("legacy_all_image_urls")
        if entity.connector == "prestashop" and isinstance(legacy_variant_images, list):
            images = [str(value) for value in legacy_variant_images if value]
        variant_image = variant.image.url if variant is not None and variant.image is not None else None
        if entity.connector == "woocommerce":
            variant_image = None
        documents = domain.documents(
            ((item.url, item.title or item.url) for item in entity.documents), entity.canonical_url
        )
        stock = variant.stock if variant is not None else None
        exact_stock = (
            stock.quantity if stock is not None and stock.quantity_kind == StockQuantityKind.EXACT else None
        )
        selected_availability = offer.availability if offer is not None else None
        availability = selected_availability or (stock.availability if stock is not None else None)
        list_price = self._list_price(variant, offer)
        minimum = offer.minimum_quantity if offer is not None else None
        source_updated_at = _string(attributes.get("legacy_source_updated_at")) or (
            entity.source_updated_at.isoformat().replace("+00:00", "Z") if entity.source_updated_at else None
        )
        raw = self._legacy_raw(entity, variant)
        raw_product = entity.platform_extensions.get("legacy_raw_product")
        raw_variant = attributes.get("legacy_raw_variant")
        if isinstance(raw_product, dict) and isinstance(raw_variant, dict):
            raw = {"product": raw_product, "variant": raw_variant}
        manufacturer_sku = _string(attributes.get("manufacturer_sku"))
        if manufacturer_sku is None and variant is not None:
            if entity.connector == "woocommerce":
                manufacturer_sku = domain.manufacturer_code(
                    entity.vendor or options.brand,
                    entity.title,
                    variant_title or "",
                    _string(entity.published_attributes.get("supplier_reference")),
                    variant.sku or "",
                )
            else:
                manufacturer_name = (
                    entity.title
                    if entity.connector == "prestashop"
                    else variant_title or ""
                )
                manufacturer_sku = domain.manufacturer_code(
                    entity.vendor or options.brand,
                    manufacturer_name,
                    variant.sku or "",
                    None if entity.connector == "prestashop" else entity.title,
                )

        # The legacy BigCommerce scraper substitutes its configured source
        # brand before calling record.build(), so that fallback is classified
        # as a published brand. Preserve that connector-specific output during
        # migration; other connectors retain the more precise source-default
        # classification.
        published_brand = entity.vendor
        if entity.connector == "bigcommerce" and not published_brand:
            published_brand = options.brand

        row = legacy_record.build(
            source=entity.source_id,
            product_url=product_url,
            parent_url=entity.canonical_url,
            variant_id=(variant.external_id if variant is not None and not variant.is_default else None),
            name=name,
            product_name=entity.title,
            variant_title=variant_title,
            brand=published_brand,
            manufacturer_sku=manufacturer_sku,
            supplier_reference=variant.sku
            if variant is not None
            else _string(attributes.get("supplier_reference")),
            gtin=variant.gtin if variant is not None else _string(attributes.get("gtin")),
            description=entity.description,
            category_path=[item.name for item in entity.categories],
            image_url=variant_image or (images[0] if images else None),
            all_image_urls=images,
            price=float(offer.price.amount) if offer is not None else None,
            currency=(
                offer.price.currency
                if offer is not None
                else _string(attributes.get("legacy_currency"))
            ),
            price_text=_string(attributes.get("price_text")),
            list_price=float(list_price) if list_price is not None else None,
            vat=(offer.vat_status if offer is not None and offer.vat_status != "unknown" else None),
            vat_rate=float(offer.vat_rate) if offer is not None and offer.vat_rate is not None else None,
            availability=_availability(availability),
            stock_quantity=exact_stock,
            min_order_quantity=_whole_number(minimum),
            documents=documents,
            technical_attributes=technical_attributes or None,
            source_detail_level=options.source_detail_level,
            source_updated_at=source_updated_at,
            identity_only=self.identity_only,
            claims=_dict_list(attributes.get("claims")),
            source_brand=options.brand,
            source_is_manufacturer=options.is_manufacturer,
            extraction_method=options.extraction_method or self._extraction_method(offer, stock),
            raw=raw,
        )
        # record.build() historically reads the wall clock. Neutral snapshots
        # carry the actual observation time, making replay projection stable.
        row["fetched_at"] = entity.observed_at.isoformat().replace("+00:00", "Z")
        for compatibility_field in ("published_unit_price", "vat_basis"):
            if value := _string(attributes.get(compatibility_field)):
                row[compatibility_field] = value
        if options.collection_mode == "price":
            # The PostgreSQL compatibility loader uses this marker to retain
            # descriptive enrichment while updating identity, price, stock,
            # and freshness fields from a complete daily enumeration.
            row["collection_mode"] = "price"
        return row

    @staticmethod
    def _select_offer(
        variant: CommerceVariant | None, options: CeramicsProjectionOptions
    ) -> CommerceOffer | None:
        if variant is None:
            return None
        for role in options.offer_role_priority:
            if found := next((offer for offer in variant.offers if offer.role == role), None):
                return found
        return None

    @staticmethod
    def _list_price(variant: CommerceVariant | None, selected: CommerceOffer | None) -> Decimal | None:
        if variant is None or selected is None or selected.role != "sale":
            return None
        regular = next(
            (
                offer
                for offer in variant.offers
                if offer.role == "regular" and offer.price.currency == selected.price.currency
            ),
            None,
        )
        return (
            regular.price.amount
            if regular is not None and regular.price.amount != selected.price.amount
            else None
        )

    @staticmethod
    def _extraction_method(offer: CommerceOffer | None, stock: object) -> str:
        evidence = offer.evidence if offer is not None else getattr(stock, "evidence", ())
        return _METHODS[evidence[0].method] if evidence else "connector"

    @staticmethod
    def _legacy_raw(entity: CommerceProductSnapshot, variant: CommerceVariant | None) -> object:
        if variant is not None and "raw" in variant.platform_extensions:
            return variant.platform_extensions["raw"]
        if "raw" in entity.platform_extensions:
            return entity.platform_extensions["raw"]
        if variant is not None and "legacy_raw_record" in variant.platform_extensions:
            return variant.platform_extensions["legacy_raw_record"]
        product = entity.platform_extensions.get("legacy_raw_product")
        variant_raw = variant.platform_extensions.get("legacy_raw_variant") if variant else None
        if variant is not None and variant.is_default and variant_raw is None:
            return product
        if product is not None or variant_raw is not None:
            variant_key = "variation" if entity.connector == "woocommerce" else "variant"
            return {"product": product, variant_key: variant_raw}
        return None


class CeramicsCatalogueProjector(_CeramicsProjector):
    name = legacy_record.RECORD_FORMAT
    identity_only = False
    record_model = CeramicsCatalogueRecord
    required_snapshot_fields = _CeramicsProjector.required_snapshot_fields | {
        SnapshotField.OFFERS,
    }


class CeramicsIdentityProjector(_CeramicsProjector):
    name = legacy_record.IDENTITY_FORMAT
    identity_only = True
    record_model = CeramicsIdentityRecord


def _availability(value: Availability | None) -> str | None:
    if value is None or value == Availability.UNKNOWN:
        return None
    return {
        Availability.IN_STOCK: "https://schema.org/InStock",
        Availability.LIMITED: "https://schema.org/LimitedAvailability",
        Availability.OUT_OF_STOCK: "https://schema.org/OutOfStock",
        Availability.BACKORDER: "https://schema.org/BackOrder",
        Availability.PREORDER: "https://schema.org/PreOrder",
        Availability.DISCONTINUED: "https://schema.org/Discontinued",
    }[value]


def _whole_number(value: Decimal | None) -> int | None:
    if value is None or value != value.to_integral_value():
        return None
    return int(value)


def _string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _dict_list(value: object) -> list[dict[str, object]] | None:
    if not isinstance(value, list):
        return None
    return [item for item in value if isinstance(item, dict)] or None
