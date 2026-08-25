from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from .diagnostics import Diagnostic


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


class SourceDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    base_url: str = Field(pattern=r"^https?://")
    connector: str = Field(min_length=1)
    connector_options: dict[str, JsonValue] = Field(default_factory=dict)


class CollectionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source_id: str = Field(min_length=1)
    base_url: str = Field(pattern=r"^https?://")
    refresh_mode: RefreshMode = RefreshMode.FULL
    requested_fields: frozenset[SnapshotField] = frozenset(SnapshotField)
    result_limit: int | None = Field(default=None, ge=1)
    partitions: tuple[str, ...] = ()
    deadline: datetime | None = None


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
    def valid_state(self) -> EntityPage[T]:
        if self.terminal and self.enumeration_intact and self.resume_after is not None:
            raise ValueError("a successful terminal page cannot have a resume cursor")
        if self.partition_terminal and not self.terminal and self.resume_after is None:
            raise ValueError("an intermediate partition terminal requires a resume cursor")
        if not self.enumeration_intact and not self.terminal:
            raise ValueError("an incomplete enumeration must be terminal")
        if not self.enumeration_intact and not any(d.affects_completeness for d in self.diagnostics):
            raise ValueError("an incomplete enumeration requires a completeness diagnostic")
        if self.discovered < len(self.items):
            raise ValueError("discovered cannot be smaller than emitted items")
        return self

