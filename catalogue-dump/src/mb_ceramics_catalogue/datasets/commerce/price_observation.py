"""Lossless offer observations projected from neutral snapshots."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import Literal

from mb_commerce_scraper.models import Availability, CommerceProductSnapshot, Evidence
from pydantic import AwareDatetime, BaseModel, ConfigDict

from mb_ceramics_catalogue.connectors.base import SnapshotField
from mb_ceramics_catalogue.datasets.base import ProjectionContext

from ._common import evidence_key, observation_id


class PriceObservationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    format: Literal["commerce.price_observation.v1"] = "commerce.price_observation.v1"
    observation_id: str
    connector: str
    source_id: str
    product_external_id: str
    variant_external_id: str
    product_url: str
    role: Literal["regular", "sale", "member", "quantity_tier"]
    amount: Decimal
    currency: str
    vat_status: Literal["inclusive", "exclusive", "unknown"]
    vat_rate: Decimal | None
    minimum_quantity: Decimal | None
    unit: str | None
    pack_size: Decimal | None
    seller_id: str | None
    seller_name: str | None
    availability: Availability | None
    valid_from: AwareDatetime | None
    valid_until: AwareDatetime | None
    observed_at: AwareDatetime
    evidence: tuple[Evidence, ...]


class PriceObservationProjector:
    name = "commerce.price_observation.v1"
    version = "1"
    projector_version = "1"
    required_snapshot_fields = frozenset({SnapshotField.IDENTITY, SnapshotField.OFFERS})
    required_capabilities: frozenset[str] = frozenset()
    record_model = PriceObservationRecord

    def project(
        self, entity: CommerceProductSnapshot, context: ProjectionContext
    ) -> Iterable[PriceObservationRecord]:
        del context
        for variant in entity.variants:
            for offer in variant.offers:
                identity = observation_id(
                    entity.connector,
                    entity.source_id,
                    entity.external_id,
                    variant.external_id,
                    offer.role,
                    offer.seller_id or "",
                    str(offer.minimum_quantity or ""),
                    offer.observed_at.isoformat(),
                    evidence_key(offer.evidence),
                )
                yield PriceObservationRecord(
                    observation_id=identity,
                    connector=entity.connector,
                    source_id=entity.source_id,
                    product_external_id=entity.external_id,
                    variant_external_id=variant.external_id,
                    product_url=variant.canonical_url or entity.canonical_url,
                    role=offer.role,
                    amount=offer.price.amount,
                    currency=offer.price.currency,
                    vat_status=offer.vat_status,
                    vat_rate=offer.vat_rate,
                    minimum_quantity=offer.minimum_quantity,
                    unit=offer.unit,
                    pack_size=offer.pack_size,
                    seller_id=offer.seller_id,
                    seller_name=offer.seller_name,
                    availability=offer.availability,
                    valid_from=offer.valid_from,
                    valid_until=offer.valid_until,
                    observed_at=offer.observed_at,
                    evidence=offer.evidence,
                )
