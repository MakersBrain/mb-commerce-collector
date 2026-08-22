"""Generate or verify the frozen public contract schemas and examples."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from mb_commerce_scraper import (
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
    EntityPage,
    Evidence,
    MediaRef,
    Money,
    RefreshMode,
    SnapshotField,
    SourceDefinition,
    StockQuantityKind,
    StockState,
    collection_fingerprint,
)

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "src" / "mb_commerce_scraper" / "schemas"
SCHEMA_MODELS = {
    "collection-request.schema.json": CollectionRequest,
    "commerce-product-snapshot.schema.json": CommerceProductSnapshot,
    "connector-checkpoint.schema.json": ConnectorCheckpoint,
    "diagnostic.schema.json": Diagnostic,
    "entity-page.schema.json": EntityPage[CommerceProductSnapshot],
    "source-definition.schema.json": SourceDefinition,
}


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _schemas() -> dict[str, str]:
    rendered: dict[str, str] = {}
    for filename, model in SCHEMA_MODELS.items():
        schema = model.model_json_schema(mode="validation")
        schema["$id"] = f"https://makersbrain.com/schemas/mb-commerce-scraper/v1/{filename}"
        schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
        rendered[filename] = _json(schema)
    rendered["representative-payloads.json"] = _json(_representative_payloads())
    return rendered


def _representative_payloads() -> dict[str, object]:
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    evidence = Evidence(
        method="api",
        source_url="https://shop.example/products.json",
        source_field="products[0]",
        observed_at=observed,
    )
    snapshot = CommerceProductSnapshot(
        connector="shopify",
        source_id="example-shop",
        external_id="product-100",
        canonical_url="https://shop.example/products/stoneware-clay",
        title="Stoneware Clay",
        observed_at=observed,
        categories=(CategoryRef(name="Clay", external_id="clay"),),
        images=(
            MediaRef(
                url="https://shop.example/media/stoneware-clay.jpg",
                media_type="image/jpeg",
                alt_text="Stoneware clay bag",
            ),
        ),
        variants=(
            CommerceVariant(
                external_id="variant-500g",
                is_default=True,
                sku="CLAY-500",
                options={"weight": "500 g"},
                offers=(
                    CommerceOffer(
                        price=Money(amount=Decimal("12.30"), currency="EUR"),
                        observed_at=observed,
                        evidence=(evidence,),
                        availability=Availability.IN_STOCK,
                        availability_evidence=(evidence,),
                    ),
                ),
                stock=StockState(
                    availability=Availability.IN_STOCK,
                    quantity=8,
                    quantity_kind=StockQuantityKind.EXACT,
                    observed_at=observed,
                    evidence=(evidence,),
                ),
            ),
        ),
    )
    request = CollectionRequest(
        source_id="example-shop",
        base_url="https://shop.example/",
        refresh_mode=RefreshMode.FULL,
        requested_fields=frozenset(
            {SnapshotField.IDENTITY, SnapshotField.OFFERS, SnapshotField.STOCK}
        ),
        partitions=("main",),
    )
    checkpoint = ConnectorCheckpoint(
        connector="shopify",
        connector_version="1",
        source_id=request.source_id,
        lineage="example-lineage",
        collection_fingerprint=collection_fingerprint(
            request, "shopify", {"currency": "EUR"}
        ),
        resume_after={"partition": "main", "page": 2},
    )
    diagnostic = Diagnostic(
        code=DiagnosticCode.OPTIONAL_ENRICHMENT_SKIPPED,
        severity=DiagnosticSeverity.INFO,
        message="optional description enrichment was not requested",
        retryable=False,
        affects_completeness=False,
        entity_id=snapshot.external_id,
        metadata={"stage": "enrichment"},
    )
    page = EntityPage[CommerceProductSnapshot](
        page_id="main:0",
        sequence=0,
        items=(snapshot,),
        terminal=True,
        partition_terminal=True,
        discovered=1,
        diagnostics=(diagnostic,),
    )
    source = SourceDefinition(
        id="example-shop",
        label="Example Shop",
        base_url="https://shop.example/",
        connector="shopify",
        connector_options={"currency": "EUR"},
    )
    request_payload = request.model_dump(mode="json")
    request_payload["requested_fields"] = sorted(request_payload["requested_fields"])
    return {
        "collection_request": request_payload,
        "commerce_product_snapshot": snapshot.model_dump(mode="json"),
        "connector_checkpoint": checkpoint.model_dump(mode="json"),
        "diagnostic": diagnostic.model_dump(mode="json"),
        "entity_page": page.model_dump(mode="json"),
        "source_definition": source.model_dump(mode="json"),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    options = parser.parse_args()
    expected = _schemas()
    stale: list[str] = []
    for filename, content in expected.items():
        path = SCHEMAS / filename
        if options.check:
            if not path.is_file() or path.read_text(encoding="utf-8") != content:
                stale.append(filename)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
    if stale:
        raise SystemExit(
            "frozen schemas are stale; run scripts/generate_schemas.py: "
            + ", ".join(stale)
        )


if __name__ == "__main__":
    main()
