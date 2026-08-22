from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from mb_ceramics_catalogue.connectors import (
    Availability,
    CategoryRef,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    Evidence,
    MediaRef,
    Money,
    StockQuantityKind,
    StockState,
)
from mb_ceramics_catalogue.datasets import DatasetRegistry, ProjectionContext
from mb_ceramics_catalogue.datasets.ceramics import (
    CeramicsCatalogueProjector,
    CeramicsCatalogueRecord,
    CeramicsIdentityProjector,
    CeramicsIdentityRecord,
)
from mb_ceramics_catalogue.scrapers import record as legacy_record

NOW = datetime(2026, 8, 15, 10, 30, tzinfo=UTC)
FETCHED_AT = "2026-08-15T10:30:00Z"


def evidence() -> Evidence:
    return Evidence(method="api", source_url="https://shop.test/products/1.json", observed_at=NOW)


def context(dataset: str, **configuration: object) -> ProjectionContext:
    return ProjectionContext.model_validate(
        {
            "collection_id": "collection-1",
            "source_id": "shop",
            "dataset": dataset,
            "dataset_version": "2",
            "projector_version": "compatibility-1",
            "configuration": configuration,
        }
    )


def priced_snapshot() -> CommerceProductSnapshot:
    regular = CommerceOffer(
        price=Money(amount=Decimal("15.00"), currency="EUR"),
        role="regular",
        observed_at=NOW,
        evidence=(evidence(),),
        vat_status="inclusive",
    )
    sale = CommerceOffer(
        price=Money(amount=Decimal("12.50"), currency="EUR"),
        role="sale",
        observed_at=NOW,
        evidence=(evidence(),),
        vat_status="inclusive",
        vat_rate=Decimal("0.20"),
        minimum_quantity=Decimal("2"),
        availability=Availability.IN_STOCK,
        availability_evidence=(evidence(),),
    )
    stock = StockState(
        availability=Availability.IN_STOCK,
        quantity=7,
        quantity_kind=StockQuantityKind.EXACT,
        observed_at=NOW,
        evidence=(evidence(),),
    )
    return CommerceProductSnapshot(
        connector="shopify",
        source_id="shop",
        external_id="product-1",
        canonical_url="https://shop.test/products/glaze",
        title="Transparent gloss glaze",
        description="A reliable transparent glaze.",
        vendor="Test Ceramics",
        observed_at=NOW,
        categories=(CategoryRef(name="Glazes"),),
        images=(MediaRef(url="https://shop.test/glaze.jpg"),),
        variants=(
            CommerceVariant(
                external_id="variant-42",
                title="500ml",
                sku="GL-500",
                offers=(regular, sale),
                stock=stock,
                published_attributes={
                    "manufacturer_sku": "TC-42",
                    "Viscosity": "brushing",
                },
            ),
        ),
        platform_extensions={"raw": {"id": 1}},
    )


def test_sio2_source_policy_is_dataset_owned_and_filters_durable_projection() -> None:
    projector = CeramicsCatalogueProjector()
    snapshot = priced_snapshot().model_copy(update={
        "canonical_url": "https://sio-2.com/en/low-fire-ceramic-clays/red-clay",
        "vendor": "SIO-2",
    })
    projected = tuple(projector.project(
        snapshot, context(projector.name, scope="all", source_policy="sio2")
    ))
    assert len(projected) == 1
    assert projected[0].family == "clay"
    assert projected[0].material_kind == "low-fire-ceramic-clays"

    unrelated = snapshot.model_copy(update={
        "canonical_url": "https://sio-2.com/en/tools/rib"
    })
    assert tuple(projector.project(
        unrelated, context(projector.name, scope="all", source_policy="sio2")
    )) == ()


def test_catalogue_projector_matches_the_direct_legacy_builder() -> None:
    snapshot = priced_snapshot()
    projector = CeramicsCatalogueProjector()
    projection_context = context(
        projector.name,
        scope="all",
        enrichments=["ceramic-materials"],
        extraction_method="api_json",
    )
    [projected] = projector.project(snapshot, projection_context)

    traits = {
        "shop": {
            "scope": "all",
            "enrichments": ["ceramic-materials"],
            "brand": None,
            "is_manufacturer": False,
        }
    }
    with legacy_record.RecordBuilder(traits):
        expected = legacy_record.build(
            source="shop",
            product_url="https://shop.test/products/glaze",
            parent_url="https://shop.test/products/glaze",
            variant_id="variant-42",
            name="Transparent gloss glaze 500ml",
            product_name="Transparent gloss glaze",
            variant_title="500ml",
            brand="Test Ceramics",
            manufacturer_sku="TC-42",
            supplier_reference="GL-500",
            gtin=None,
            description="A reliable transparent glaze.",
            category_path=["Glazes"],
            image_url="https://shop.test/glaze.jpg",
            all_image_urls=["https://shop.test/glaze.jpg"],
            price=12.5,
            currency="EUR",
            price_text=None,
            list_price=15.0,
            vat="inclusive",
            vat_rate=0.2,
            availability="https://schema.org/InStock",
            stock_quantity=7,
            min_order_quantity=2,
            documents=[],
            technical_attributes={"Viscosity": "brushing"},
            source_detail_level="api",
            source_updated_at=None,
            identity_only=False,
            claims=None,
            source_brand=None,
            source_is_manufacturer=False,
            extraction_method="api_json",
            raw={"id": 1},
        )
    expected["fetched_at"] = FETCHED_AT
    assert projected.as_legacy_dict() == expected
    assert projected.model_dump(mode="json") == expected
    assert projected.price == 12.5
    assert projected.list_price == 15.0
    assert projected.stock_quantity == 7


def test_price_projection_marks_loader_preservation_without_changing_full_rows() -> None:
    projector = CeramicsCatalogueProjector()
    [price_row] = projector.project(
        priced_snapshot(),
        context(projector.name, scope="all", collection_mode="price"),
    )
    [full_row] = projector.project(
        priced_snapshot(),
        context(projector.name, scope="all"),
    )

    assert price_row.as_legacy_dict()["collection_mode"] == "price"
    assert "collection_mode" not in full_row.as_legacy_dict()


def test_identity_projector_keeps_a_variantless_manufacturer_product() -> None:
    snapshot = CommerceProductSnapshot(
        connector="woocommerce",
        source_id="shop",
        external_id="mayco-sw229",
        canonical_url="https://shop.test/sw229-mood-ring",
        title="SW-229 Mood Ring",
        description="Stoneware glaze firing from cone 6 to cone 10.",
        vendor="Mayco",
        observed_at=NOW,
        categories=(CategoryRef(name="Glazes"), CategoryRef(name="Stoneware")),
        published_attributes={
            "manufacturer_sku": "SW229",
            "Dinnerware Safe": "No",
        },
        platform_extensions={"raw": {"id": 59987}},
    )
    projector = CeramicsIdentityProjector()
    [row] = projector.project(
        snapshot,
        context(
            projector.name,
            scope="materials",
            enrichments=["ceramic-materials"],
            brand="Mayco",
            is_manufacturer=True,
            extraction_method="api_json",
        ),
    )
    assert row.format == legacy_record.IDENTITY_FORMAT
    assert row.price is None
    assert row.currency is None
    assert row.external_id == "shop:https://shop.test/sw229-mood-ring"
    assert row.parent_external_id == row.external_id
    assert row.manufacturer_sku == "SW229"
    assert row.technical_attributes == {"Dinnerware Safe": "No"}
    assert row.family == "glaze"
    assert legacy_record.is_valid(row.as_legacy_dict())

    traits = {
        "shop": {
            "scope": "materials",
            "enrichments": ["ceramic-materials"],
            "brand": "Mayco",
            "is_manufacturer": True,
        }
    }
    with legacy_record.RecordBuilder(traits):
        expected = legacy_record.build(
            source="shop",
            product_url="https://shop.test/sw229-mood-ring",
            parent_url="https://shop.test/sw229-mood-ring",
            variant_id=None,
            name="SW-229 Mood Ring",
            product_name="SW-229 Mood Ring",
            variant_title=None,
            brand="Mayco",
            manufacturer_sku="SW229",
            supplier_reference=None,
            gtin=None,
            description="Stoneware glaze firing from cone 6 to cone 10.",
            category_path=["Glazes", "Stoneware"],
            image_url=None,
            all_image_urls=[],
            price=None,
            currency=None,
            price_text=None,
            list_price=None,
            vat=None,
            vat_rate=None,
            availability=None,
            stock_quantity=None,
            min_order_quantity=None,
            documents=[],
            technical_attributes={"Dinnerware Safe": "No"},
            source_detail_level="api",
            source_updated_at=None,
            identity_only=True,
            claims=None,
            source_brand="Mayco",
            source_is_manufacturer=True,
            extraction_method="api_json",
            raw={"id": 59987},
        )
    expected["fetched_at"] = FETCHED_AT
    assert row.as_legacy_dict() == expected


def test_catalogue_projector_drops_missing_offers_and_non_material_scope() -> None:
    no_offer = priced_snapshot().model_copy(
        update={"variants": (priced_snapshot().variants[0].model_copy(update={"offers": ()}),)}
    )
    projector = CeramicsCatalogueProjector()
    assert tuple(projector.project(no_offer, context(projector.name))) == ()

    mug = priced_snapshot().model_copy(
        update={
            "title": "Finished decorative mug",
            "description": "A finished vessel for the table.",
            "categories": (CategoryRef(name="Tableware"),),
        }
    )
    assert (
        tuple(
            projector.project(
                mug,
                context(projector.name, scope="materials", enrichments=["classification"]),
            )
        )
        == ()
    )


def test_projectors_satisfy_registry_validation() -> None:
    catalogue = CeramicsCatalogueProjector()
    identity = CeramicsIdentityProjector()
    registry = DatasetRegistry([catalogue, identity])
    rows = list(
        registry.project_validated(
            catalogue.name,
            priced_snapshot(),
            context(catalogue.name, extraction_method="api_json"),
        )
    )
    assert len(rows) == 1


def test_typed_contract_accepts_every_checked_in_golden_sample() -> None:
    validated = 0
    for path in Path("tests/golden").glob("*.json"):
        document = json.loads(path.read_text(encoding="utf-8"))
        for row in document.get("sample", []):
            model = (
                CeramicsIdentityRecord
                if row.get("format") == legacy_record.IDENTITY_FORMAT
                else CeramicsCatalogueRecord
            )
            model.model_validate(row)
            validated += 1
    assert validated >= 20
