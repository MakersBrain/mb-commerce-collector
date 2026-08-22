"""Explicit registry for versioned dataset definitions."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from mb_commerce_scraper.models import CommerceProductSnapshot
from pydantic import BaseModel

from mb_ceramics_catalogue.connectors.base import SnapshotField

from .base import DatasetDefinition, ProjectionContext

DATASET_NAMES = frozenset(
    {
        "ceramics.catalogue_item.v2",
        "ceramics.catalogue_identity.v2",
        "commerce.price_observation.v1",
        "commerce.stock_observation.v1",
        "commerce.document.v1",
    }
)


class DatasetRegistry:
    """Holds explicit definitions and validates every projected record."""

    def __init__(self, definitions: Iterable[DatasetDefinition[BaseModel]] = ()) -> None:
        self._definitions: dict[str, DatasetDefinition[BaseModel]] = {}
        for definition in definitions:
            self.register(definition)

    def register(self, definition: DatasetDefinition[BaseModel]) -> None:
        if definition.name in self._definitions:
            raise ValueError(f"dataset {definition.name!r} is already registered")
        if not definition.name or not definition.version or not definition.projector_version:
            raise ValueError("dataset name, contract version, and projector version must be non-empty")
        if not issubclass(definition.record_model, BaseModel):
            raise TypeError("dataset record_model must be a Pydantic BaseModel subclass")
        self._definitions[definition.name] = definition

    def get(self, name: str) -> DatasetDefinition[BaseModel]:
        try:
            return self._definitions[name]
        except KeyError:
            known = ", ".join(self.names()) or "(none)"
            raise KeyError(f"unknown dataset {name!r}; known: {known}") from None

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._definitions))

    def collection_requirements(
        self, names: Iterable[str]
    ) -> tuple[frozenset[SnapshotField], frozenset[str]]:
        """Union the fetch requirements for one multi-dataset collection."""
        fields: set[SnapshotField] = set()
        capabilities: set[str] = set()
        for name in names:
            definition = self.get(name)
            fields.update(definition.required_snapshot_fields)
            capabilities.update(definition.required_capabilities)
        return frozenset(fields), frozenset(capabilities)

    def project_validated(
        self,
        name: str,
        entity: CommerceProductSnapshot,
        context: ProjectionContext,
    ) -> Iterator[BaseModel]:
        definition = self.get(name)
        if context.source_id != entity.source_id:
            raise ValueError("projection context source does not match the entity source")
        if context.dataset != definition.name or context.dataset_version != definition.version:
            raise ValueError("projection context does not match the registered dataset contract")
        if context.projector_version != definition.projector_version:
            raise ValueError("projection context does not match the registered projector version")
        for record in definition.project(entity, context):
            yield definition.record_model.model_validate(record)


def built_in_registry() -> DatasetRegistry:
    """Return a fresh registry containing every supported dataset contract."""
    from .ceramics import CeramicsCatalogueProjector, CeramicsIdentityProjector
    from .commerce import (
        CommerceDocumentProjector,
        PriceObservationProjector,
        StockObservationProjector,
    )

    return DatasetRegistry(
        (
            CeramicsCatalogueProjector(),
            CeramicsIdentityProjector(),
            PriceObservationProjector(),
            StockObservationProjector(),
            CommerceDocumentProjector(),
        )
    )
