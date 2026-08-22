from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable

from pydantic import model_validator

from mb_commerce_scraper.models import (
    CollectionRequest,
    CommerceProductSnapshot,
    ConnectorCheckpoint,
    ContractModel,
    EntityPage,
    RefreshMode,
    SnapshotField,
    StockQuantityKind,
)
from mb_commerce_scraper.transports import NullTelemetry
from mb_commerce_scraper.transports.base import RequestBudget, TelemetryHooks


class BrowserRequirement(StrEnum):
    NEVER = "never"
    OPTIONAL = "optional"
    REQUIRED = "required"


class ConnectorCapabilities(ContractModel):
    snapshot_fields: frozenset[SnapshotField]
    refresh_modes: frozenset[RefreshMode]
    stock_kinds: frozenset[StockQuantityKind] = frozenset()
    supports_incremental_cursor: bool = False
    supports_category_filter: bool = False
    supports_documents: bool = False
    browser: BrowserRequirement = BrowserRequirement.NEVER
    shared_edge: str | None = None

    @model_validator(mode="after")
    def consistent(self) -> ConnectorCapabilities:
        if self.supports_incremental_cursor and RefreshMode.INCREMENTAL not in self.refresh_modes:
            raise ValueError("incremental cursor support requires incremental refresh mode")
        if self.supports_documents and SnapshotField.DOCUMENTS not in self.snapshot_fields:
            raise ValueError("document support requires the documents field")
        if self.stock_kinds and SnapshotField.STOCK not in self.snapshot_fields:
            raise ValueError("stock kinds require the stock field")
        if self.shared_edge is not None and (
            not self.shared_edge.strip() or self.shared_edge != self.shared_edge.strip()
        ):
            raise ValueError("shared edge must be a non-empty normalized name")
        return self

    def supports(self, fields: frozenset[SnapshotField], refresh_mode: RefreshMode) -> bool:
        return fields <= self.snapshot_fields and refresh_mode in self.refresh_modes

    def named_capabilities(self) -> frozenset[str]:
        """Project typed declarations into dataset requirement names."""
        names: set[str] = set()
        if self.supports_incremental_cursor:
            names.add("incremental_cursor")
        if self.supports_category_filter:
            names.add("category_filter")
        if self.supports_documents:
            names.add("documents")
        names.update(f"stock:{kind.value}" for kind in self.stock_kinds)
        if self.shared_edge:
            names.add(f"shared_edge:{self.shared_edge}")
        return frozenset(names)


class ConnectorContext:
    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
        budget: RequestBudget | None = None,
        telemetry: TelemetryHooks | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> None:
        self.clock = clock or (lambda: datetime.now(UTC))
        self.budget = budget
        self.telemetry = telemetry or NullTelemetry()
        self.cancelled = cancelled or (lambda: False)


@runtime_checkable
class CommerceConnector(Protocol):
    name: str
    platform: str
    version: str
    capabilities: ConnectorCapabilities

    def collect(
        self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None = None
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]: ...
