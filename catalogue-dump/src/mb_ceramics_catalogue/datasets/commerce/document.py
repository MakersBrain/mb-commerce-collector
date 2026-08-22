"""Published product-document observations."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from mb_commerce_scraper.models import CommerceProductSnapshot, Evidence
from pydantic import AwareDatetime, BaseModel, ConfigDict

from mb_ceramics_catalogue.connectors.base import SnapshotField
from mb_ceramics_catalogue.datasets.base import ProjectionContext

from ._common import evidence_key, observation_id


class CommerceDocumentRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    format: Literal["commerce.document.v1"] = "commerce.document.v1"
    observation_id: str
    connector: str
    source_id: str
    product_external_id: str
    product_url: str
    document_url: str
    title: str | None
    media_type: str | None
    external_id: str | None
    observed_at: AwareDatetime
    evidence: tuple[Evidence, ...]


class CommerceDocumentProjector:
    name = "commerce.document.v1"
    version = "1"
    projector_version = "1"
    required_snapshot_fields = frozenset({SnapshotField.IDENTITY, SnapshotField.DOCUMENTS})
    required_capabilities = frozenset({"documents"})
    record_model = CommerceDocumentRecord

    def project(
        self, entity: CommerceProductSnapshot, context: ProjectionContext
    ) -> Iterable[CommerceDocumentRecord]:
        del context
        for document in entity.documents:
            identity = observation_id(
                entity.connector,
                entity.source_id,
                entity.external_id,
                document.external_id or document.url,
                document.observed_at.isoformat(),
                evidence_key(document.evidence),
            )
            yield CommerceDocumentRecord(
                observation_id=identity,
                connector=entity.connector,
                source_id=entity.source_id,
                product_external_id=entity.external_id,
                product_url=entity.canonical_url,
                document_url=document.url,
                title=document.title,
                media_type=document.media_type,
                external_id=document.external_id,
                observed_at=document.observed_at,
                evidence=document.evidence,
            )
