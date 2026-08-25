"""Protocols shared by all dataset projectors."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol, TypeVar, runtime_checkable

from mb_commerce_scraper.models import CommerceProductSnapshot
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from mb_ceramics_catalogue.connectors.base import SnapshotField

DatasetRecord = TypeVar("DatasetRecord", bound=BaseModel, covariant=True)


class ProjectionContext(BaseModel):
    """Explicit inputs that keep projection deterministic and replayable."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    collection_id: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    dataset: str = Field(min_length=1)
    dataset_version: str = Field(min_length=1)
    projector_version: str = Field(min_length=1)
    configuration: dict[str, JsonValue] = Field(default_factory=dict)


@runtime_checkable
class DatasetDefinition(Protocol[DatasetRecord]):
    name: str
    version: str
    projector_version: str
    required_snapshot_fields: frozenset[SnapshotField]
    required_capabilities: frozenset[str]

    @property
    def record_model(self) -> type[DatasetRecord]: ...

    def project(
        self,
        entity: CommerceProductSnapshot,
        context: ProjectionContext,
    ) -> Iterable[DatasetRecord]: ...
