from __future__ import annotations

import hashlib
import json
from typing import Any, Literal, cast
from urllib.parse import urlsplit, urlunsplit

from pydantic import Field, JsonValue, model_validator

from .collection import CollectionRequest
from .commerce import ContractModel
from .sanitization import sanitize_json_value


class _CredentialSafeCheckpoint(ContractModel):
    """Reject unsafe cursors before validation can retain their input value."""

    def __init__(self, /, **data: Any) -> None:
        cursor = data.get("resume_after")
        try:
            unsafe = sanitize_json_value(cast(JsonValue, cursor)) != cursor
        except (AttributeError, TypeError):
            # Let Pydantic produce the ordinary type error for non-JSON input.
            unsafe = False
        if unsafe:
            raise ValueError(
                "checkpoint cursor contains credential-bearing or unsafe data"
            )
        super().__init__(**data)


class ConnectorCheckpoint(_CredentialSafeCheckpoint):
    checkpoint_schema_version: Literal[1] = 1
    connector: str = Field(min_length=1)
    connector_version: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    lineage: str = Field(min_length=1)
    collection_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    resume_after: JsonValue

    @model_validator(mode="after")
    def cursor_present(self) -> ConnectorCheckpoint:
        if self.resume_after is None:
            raise ValueError("a checkpoint must contain a resume cursor")
        return self


def collection_fingerprint(
    request: CollectionRequest, connector: str, options: dict[str, JsonValue]
) -> str:
    parsed = urlsplit(request.base_url)
    normalized_url = urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), "", ""))
    value = {
        "base_url": normalized_url,
        "connector": connector,
        "options": options,
        "partitions": request.partitions,
        "refresh_mode": request.refresh_mode.value,
        "requested_fields": sorted(field.value for field in request.requested_fields),
    }
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def build_checkpoint(
    *,
    connector: str,
    connector_version: str,
    request: CollectionRequest,
    lineage: str,
    resume_after: JsonValue,
    options: dict[str, JsonValue],
) -> ConnectorCheckpoint:
    """Build a schema-v1 checkpoint with the canonical collection identity."""
    return ConnectorCheckpoint(
        connector=connector,
        connector_version=connector_version,
        source_id=request.source_id,
        lineage=lineage,
        collection_fingerprint=collection_fingerprint(request, connector, options),
        resume_after=resume_after,
    )


def validate_checkpoint(
    checkpoint: ConnectorCheckpoint | None,
    *,
    connector: str,
    connector_version: str,
    request: CollectionRequest,
    options: dict[str, JsonValue],
) -> None:
    if checkpoint is None:
        return
    expected = collection_fingerprint(request, connector, options)
    if (
        checkpoint.connector != connector
        or checkpoint.connector_version != connector_version
        or checkpoint.source_id != request.source_id
        or checkpoint.collection_fingerprint != expected
    ):
        raise ValueError("CHECKPOINT_INVALID: checkpoint does not belong to this collection")
