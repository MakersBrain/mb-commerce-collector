from __future__ import annotations

from collections.abc import Callable

import pytest
from pydantic import JsonValue

from mb_commerce_scraper import (
    CollectionRequest,
    CompatibleLegacyCheckpoint,
    LegacyCheckpointDecodeResult,
    LegacyCheckpointRestartReason,
    RefreshMode,
    RestartLegacyCheckpoint,
    SnapshotField,
    collection_fingerprint,
    decode_legacy_checkpoint,
)


def request(
    *, source_id: str = "shop", base_url: str = "https://shop.test/"
) -> CollectionRequest:
    return CollectionRequest(
        source_id=source_id,
        base_url=base_url,
        refresh_mode=RefreshMode.FULL,
        requested_fields=frozenset({SnapshotField.IDENTITY}),
        partitions=("main",),
    )


def legacy_checkpoint(**changes: JsonValue) -> dict[str, JsonValue]:
    value: dict[str, JsonValue] = {
        "connector": "shopify",
        "connector_version": "1",
        "source_id": "shop",
        "lineage": "legacy-lineage",
        "resume_after": {"page": 2},
    }
    value.update(changes)
    return value


def decode(
    checkpoint: dict[str, JsonValue] | None = None,
    *,
    current_request: CollectionRequest | None = None,
    durable_request: CollectionRequest | None = None,
    current_options: dict[str, JsonValue] | None = None,
    durable_options: dict[str, JsonValue] | None = None,
    connector: str = "shopify",
    connector_version: str = "1",
) -> LegacyCheckpointDecodeResult:
    intent = current_request or request()
    return decode_legacy_checkpoint(
        checkpoint if checkpoint is not None else legacy_checkpoint(),
        request=intent,
        connector=connector,
        connector_version=connector_version,
        options=(
            current_options if current_options is not None else {"currency": "EUR"}
        ),
        durable_request=durable_request if durable_request is not None else request(),
        durable_options=(
            durable_options if durable_options is not None else {"currency": "EUR"}
        ),
    )


def test_compatible_legacy_checkpoint_is_upgraded_deterministically() -> None:
    result = decode()

    assert isinstance(result, CompatibleLegacyCheckpoint)
    assert result.checkpoint.checkpoint_schema_version == 1
    assert result.checkpoint.lineage == "legacy-lineage"
    assert result.checkpoint.resume_after == {"page": 2}
    assert result.checkpoint.collection_fingerprint == collection_fingerprint(
        request(), "shopify", {"currency": "EUR"}
    )


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        (
            lambda: {"current_options": {"currency": "USD"}},
            LegacyCheckpointRestartReason.COLLECTION_CONFIGURATION_CHANGED,
        ),
        (
            lambda: {"current_request": request(base_url="https://other.test/")},
            LegacyCheckpointRestartReason.COLLECTION_CONFIGURATION_CHANGED,
        ),
    ],
)
def test_collection_configuration_drift_requires_restart(
    change: Callable[[], dict[str, object]],
    expected: LegacyCheckpointRestartReason,
) -> None:
    result = decode(**change())  # type: ignore[arg-type]

    assert result == RestartLegacyCheckpoint(reason=expected)


@pytest.mark.parametrize(
    "changes",
    [
        {"checkpoint": legacy_checkpoint(connector="woocommerce")},
        {"checkpoint": legacy_checkpoint(connector_version="2")},
        {"checkpoint": legacy_checkpoint(source_id="other")},
        {"durable_request": request(source_id="other")},
    ],
)
def test_legacy_identity_mismatch_requires_restart(changes: dict[str, object]) -> None:
    result = decode(**changes)  # type: ignore[arg-type]

    assert result == RestartLegacyCheckpoint(
        reason=LegacyCheckpointRestartReason.LEGACY_IDENTITY_MISMATCH
    )


@pytest.mark.parametrize(
    "checkpoint",
    [
        {},
        legacy_checkpoint(resume_after=None),
        legacy_checkpoint(lineage=""),
        legacy_checkpoint(unexpected=True),
    ],
)
def test_malformed_legacy_checkpoint_requires_restart(
    checkpoint: dict[str, JsonValue],
) -> None:
    result = decode(checkpoint)

    assert result == RestartLegacyCheckpoint(
        reason=LegacyCheckpointRestartReason.MALFORMED_CHECKPOINT
    )


def test_credential_bearing_legacy_cursor_restarts_without_retaining_secret() -> None:
    secret = "legacy-checkpoint-secret-sentinel"

    result = decode(
        legacy_checkpoint(resume_after={"page": 2, "password": secret})
    )

    assert result == RestartLegacyCheckpoint(
        reason=LegacyCheckpointRestartReason.MALFORMED_CHECKPOINT
    )
    assert secret not in result.model_dump_json()


@pytest.mark.parametrize("missing", ["request", "options"])
def test_missing_durable_configuration_requires_restart(missing: str) -> None:
    intent = request()
    result = decode_legacy_checkpoint(
        legacy_checkpoint(),
        request=intent,
        connector="shopify",
        connector_version="1",
        options={"currency": "EUR"},
        durable_request=None if missing == "request" else intent,
        durable_options=None if missing == "options" else {"currency": "EUR"},
    )

    assert result == RestartLegacyCheckpoint(
        reason=LegacyCheckpointRestartReason.DURABLE_CONFIGURATION_UNAVAILABLE
    )
