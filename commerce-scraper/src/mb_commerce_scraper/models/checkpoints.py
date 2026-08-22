from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Literal, TypeAlias, cast
from urllib.parse import urlsplit, urlunsplit

from pydantic import Field, JsonValue, ValidationError, model_validator

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


class LegacyConnectorCheckpoint(_CredentialSafeCheckpoint):
    """Checkpoint envelope persisted before collection fingerprints existed."""

    connector: str = Field(min_length=1)
    connector_version: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    lineage: str = Field(min_length=1)
    resume_after: JsonValue

    @model_validator(mode="after")
    def cursor_present(self) -> LegacyConnectorCheckpoint:
        if self.resume_after is None:
            raise ValueError("a checkpoint must contain a resume cursor")
        return self


class LegacyCheckpointRestartReason(StrEnum):
    MALFORMED_CHECKPOINT = "malformed_checkpoint"
    DURABLE_CONFIGURATION_UNAVAILABLE = "durable_configuration_unavailable"
    DURABLE_CONFIGURATION_INVALID = "durable_configuration_invalid"
    LEGACY_IDENTITY_MISMATCH = "legacy_identity_mismatch"
    COLLECTION_CONFIGURATION_CHANGED = "collection_configuration_changed"
    INCOMPLETE_TERMINAL_CHECKPOINT = "incomplete_terminal_checkpoint"


class CompatibleLegacyCheckpoint(ContractModel):
    outcome: Literal["compatible"] = "compatible"
    checkpoint: ConnectorCheckpoint


class RestartLegacyCheckpoint(ContractModel):
    outcome: Literal["restart"] = "restart"
    reason: LegacyCheckpointRestartReason
    checkpoint: None = None


LegacyCheckpointDecodeResult: TypeAlias = CompatibleLegacyCheckpoint | RestartLegacyCheckpoint


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


def decode_legacy_checkpoint(
    checkpoint: LegacyConnectorCheckpoint | Mapping[str, JsonValue],
    *,
    request: CollectionRequest,
    connector: str,
    connector_version: str,
    options: dict[str, JsonValue],
    durable_request: CollectionRequest | None,
    durable_options: dict[str, JsonValue] | None,
) -> LegacyCheckpointDecodeResult:
    """Safely add a version-1 fingerprint to a legacy checkpoint.

    ``durable_request`` and ``durable_options`` must be reconstructed from the
    immutable lineage record, not from the current mutable source definition.
    If that identity is unavailable or cannot be proven equal to the current
    collection, callers must begin a new lineage.
    """
    try:
        legacy = LegacyConnectorCheckpoint.model_validate(checkpoint)
    except (ValidationError, ValueError):
        return RestartLegacyCheckpoint(
            reason=LegacyCheckpointRestartReason.MALFORMED_CHECKPOINT
        )

    if durable_request is None or durable_options is None:
        return RestartLegacyCheckpoint(
            reason=LegacyCheckpointRestartReason.DURABLE_CONFIGURATION_UNAVAILABLE
        )

    if (
        legacy.connector != connector
        or legacy.connector_version != connector_version
        or legacy.source_id != request.source_id
        or durable_request.source_id != request.source_id
    ):
        return RestartLegacyCheckpoint(
            reason=LegacyCheckpointRestartReason.LEGACY_IDENTITY_MISMATCH
        )

    try:
        durable_fingerprint = collection_fingerprint(
            durable_request, connector, durable_options
        )
        current_fingerprint = collection_fingerprint(request, connector, options)
    except (TypeError, ValueError):
        return RestartLegacyCheckpoint(
            reason=LegacyCheckpointRestartReason.DURABLE_CONFIGURATION_INVALID
        )

    if durable_fingerprint != current_fingerprint:
        return RestartLegacyCheckpoint(
            reason=LegacyCheckpointRestartReason.COLLECTION_CONFIGURATION_CHANGED
        )

    return CompatibleLegacyCheckpoint(
        checkpoint=ConnectorCheckpoint(
            connector=legacy.connector,
            connector_version=legacy.connector_version,
            source_id=legacy.source_id,
            lineage=legacy.lineage,
            collection_fingerprint=durable_fingerprint,
            resume_after=legacy.resume_after,
        )
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
