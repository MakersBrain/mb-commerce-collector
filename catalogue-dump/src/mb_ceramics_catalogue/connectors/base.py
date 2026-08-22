"""Connector capabilities, diagnostics, requests, and streaming protocol."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from enum import StrEnum
from typing import Generic, Protocol, TypeVar, runtime_checkable

from mb_commerce_scraper.models import (
    CommerceProductSnapshot,
    ContractModel,
    StockQuantityKind,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator


class SnapshotField(StrEnum):
    IDENTITY = "identity"
    DESCRIPTION = "description"
    CATEGORIES = "categories"
    IMAGES = "images"
    VARIANTS = "variants"
    OFFERS = "offers"
    STOCK = "stock"
    DOCUMENTS = "documents"
    PUBLISHED_ATTRIBUTES = "published_attributes"
    PLATFORM_EXTENSIONS = "platform_extensions"


class RefreshMode(StrEnum):
    FULL = "full"
    INCREMENTAL = "incremental"


class BrowserRequirement(StrEnum):
    NEVER = "never"
    OPTIONAL = "optional"
    REQUIRED = "required"


class BrowserBackendName(StrEnum):
    CAMOUFOX = "camoufox"
    CDP_EXTENSION_PROXY = "cdp_extension_proxy"


class ConnectorCapabilities(ContractModel):
    snapshot_fields: frozenset[SnapshotField]
    refresh_modes: frozenset[RefreshMode]
    stock_kinds: frozenset[StockQuantityKind] = frozenset()
    supports_incremental_cursor: bool = False
    supports_category_filter: bool = False
    supports_documents: bool = False
    browser: BrowserRequirement = BrowserRequirement.NEVER
    browser_backends: frozenset[BrowserBackendName] = frozenset()
    shared_edge: str | None = None

    @model_validator(mode="after")
    def _browser_declaration_is_consistent(self) -> ConnectorCapabilities:
        if self.browser == BrowserRequirement.NEVER and self.browser_backends:
            raise ValueError("a connector that never uses a browser cannot declare browser backends")
        if self.browser != BrowserRequirement.NEVER and not self.browser_backends:
            raise ValueError("a browser-capable connector must declare at least one browser backend")
        if self.supports_incremental_cursor and RefreshMode.INCREMENTAL not in self.refresh_modes:
            raise ValueError("incremental cursor support requires incremental refresh mode")
        if self.supports_documents and SnapshotField.DOCUMENTS not in self.snapshot_fields:
            raise ValueError("document support requires the documents snapshot field")
        if self.stock_kinds and SnapshotField.STOCK not in self.snapshot_fields:
            raise ValueError("stock kinds require the stock snapshot field")
        return self

    def supports(
        self,
        fields: frozenset[SnapshotField],
        refresh_mode: RefreshMode,
    ) -> bool:
        return fields <= self.snapshot_fields and refresh_mode in self.refresh_modes

    def named_capabilities(self) -> frozenset[str]:
        """Project typed declarations into generic dataset requirement names."""
        names: set[str] = set()
        if self.supports_incremental_cursor:
            names.add("incremental_cursor")
        if self.supports_category_filter:
            names.add("category_filter")
        if self.supports_documents:
            names.add("documents")
        names.update(f"stock:{kind.value}" for kind in self.stock_kinds)
        names.update(f"browser:{backend.value}" for backend in self.browser_backends)
        if self.shared_edge:
            names.add(f"shared_edge:{self.shared_edge}")
        return frozenset(names)


class DiagnosticCode(StrEnum):
    RESULT_LIMIT_REACHED = "result_limit_reached"
    REQUEST_BUDGET_EXHAUSTED = "request_budget_exhausted"
    ENUMERATION_INCOMPLETE = "enumeration_incomplete"
    ENTITY_FETCH_FAILED = "entity_fetch_failed"
    PARSER_UNSUPPORTED = "parser_unsupported"
    RATE_LIMITED = "rate_limited"
    PROXY_BUDGET_EXHAUSTED = "proxy_budget_exhausted"
    OPTIONAL_ENRICHMENT_SKIPPED = "optional_enrichment_skipped"
    SCHEMA_CHANGED = "schema_changed"
    CHECKPOINT_INVALID = "checkpoint_invalid"


class DiagnosticSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


def result_limit_diagnostic(limit: int, url: str) -> Diagnostic:
    """Describe an intentional, resumable caller bound without calling it an error."""
    return Diagnostic(
        code=DiagnosticCode.RESULT_LIMIT_REACHED,
        severity=DiagnosticSeverity.INFO,
        message=f"caller result limit {limit} reached",
        retryable=True,
        affects_completeness=True,
        url=url,
    )


class Diagnostic(ContractModel):
    code: DiagnosticCode
    severity: DiagnosticSeverity
    message: str = Field(min_length=1, max_length=2048)
    retryable: bool
    affects_completeness: bool
    url: str | None = None
    entity_id: str | None = None
    metadata: dict[str, JsonValue] = Field(default_factory=dict)


class ConnectorCheckpoint(ContractModel):
    connector: str = Field(min_length=1)
    connector_version: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    lineage: str = Field(min_length=1)
    resume_after: JsonValue

    @model_validator(mode="after")
    def _cursor_is_present(self) -> ConnectorCheckpoint:
        if self.resume_after is None:
            raise ValueError("a checkpoint must contain a resume cursor")
        return self


CheckpointCallback = Callable[[ConnectorCheckpoint], Awaitable[None]]
CancellationCheck = Callable[[], bool]


class CollectionRequest(BaseModel):
    """Immutable, dataset-independent intent passed to a connector."""

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    source_id: str = Field(min_length=1)
    base_url: str = Field(min_length=1)
    refresh_mode: RefreshMode
    requested_fields: frozenset[SnapshotField]
    result_limit: int | None = Field(default=None, ge=1)
    categories: tuple[str, ...] = ()
    collections: tuple[str, ...] = ()
    request_budget: int | None = Field(default=None, ge=1)
    cancellation_check: CancellationCheck | None = Field(default=None, exclude=True)
    checkpoint_callback: CheckpointCallback | None = Field(default=None, exclude=True)

    def cancelled(self) -> bool:
        return self.cancellation_check is not None and self.cancellation_check()


T = TypeVar("T")


class EntityPage(BaseModel, Generic[T]):
    model_config = ConfigDict(extra="forbid", frozen=True)

    page_id: str = Field(min_length=1)
    partition_key: str = Field(default="main", min_length=1)
    sequence: int = Field(ge=0)
    items: tuple[T, ...]
    resume_after: JsonValue | None = None
    terminal: bool
    partition_terminal: bool = False
    enumeration_intact: bool = True
    discovered: int = Field(ge=0)
    diagnostics: tuple[Diagnostic, ...] = ()

    @model_validator(mode="after")
    def _terminal_state_is_unambiguous(self) -> EntityPage[T]:
        if self.terminal and self.enumeration_intact and self.resume_after is not None:
            raise ValueError("a successful terminal page cannot have a resume cursor")
        if self.partition_terminal and not self.terminal and self.resume_after is None:
            raise ValueError("an intermediate partition terminal requires a resume cursor")
        if not self.enumeration_intact and not self.terminal:
            raise ValueError("an incomplete enumeration must be terminal")
        if not self.enumeration_intact and not any(item.affects_completeness for item in self.diagnostics):
            raise ValueError("an incomplete enumeration requires a completeness diagnostic")
        if self.discovered < len(self.items):
            raise ValueError("discovered cannot be smaller than the number of page items")
        return self


@runtime_checkable
class CommerceConnector(Protocol):
    name: str
    platform: str
    version: str
    capabilities: ConnectorCapabilities

    def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]: ...
