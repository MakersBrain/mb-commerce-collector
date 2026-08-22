"""Availability and quantity observations without overstating order limits."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from mb_commerce_scraper.models import (
    Availability,
    CommerceProductSnapshot,
    Evidence,
    StockQuantityKind,
)
from pydantic import AwareDatetime, BaseModel, ConfigDict

from mb_ceramics_catalogue.connectors.base import SnapshotField
from mb_ceramics_catalogue.datasets.base import ProjectionContext

from ._common import evidence_key, observation_id


class StockObservationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    format: Literal["commerce.stock_observation.v1"] = "commerce.stock_observation.v1"
    observation_id: str
    connector: str
    source_id: str
    product_external_id: str
    variant_external_id: str
    product_url: str
    availability: Availability
    quantity: int | None
    quantity_kind: StockQuantityKind
    observed_at: AwareDatetime
    evidence: tuple[Evidence, ...]


class StockObservationProjector:
    name = "commerce.stock_observation.v1"
    version = "1"
    projector_version = "1"
    required_snapshot_fields = frozenset({SnapshotField.IDENTITY, SnapshotField.STOCK})
    required_capabilities: frozenset[str] = frozenset()
    record_model = StockObservationRecord

    def project(
        self, entity: CommerceProductSnapshot, context: ProjectionContext
    ) -> Iterable[StockObservationRecord]:
        del context
        for variant in entity.variants:
            if (stock := variant.stock) is None:
                continue
            identity = observation_id(
                entity.connector,
                entity.source_id,
                entity.external_id,
                variant.external_id,
                stock.observed_at.isoformat(),
                evidence_key(stock.evidence),
            )
            yield StockObservationRecord(
                observation_id=identity,
                connector=entity.connector,
                source_id=entity.source_id,
                product_external_id=entity.external_id,
                variant_external_id=variant.external_id,
                product_url=variant.canonical_url or entity.canonical_url,
                availability=stock.availability,
                quantity=stock.quantity,
                quantity_kind=stock.quantity_kind,
                observed_at=stock.observed_at,
                evidence=stock.evidence,
            )
