"""Transactional lineage selection for the commerce-scraper runtime."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast
from uuid import UUID

from mb_commerce_scraper import (
    CollectionRequest,
    ConnectorCheckpoint,
    LegacyCheckpointRestartReason,
    collection_fingerprint,
    decode_legacy_checkpoint,
)
from pydantic import JsonValue, ValidationError

from . import outputs


@dataclass(frozen=True)
class LibraryLineageSpec:
    request: CollectionRequest
    connector: str
    connector_version: str
    connector_options: dict[str, JsonValue]
    dataset_fingerprint: str
    dataset_selection: list[dict[str, Any]]
    connector_configuration: dict[str, Any] = field(default_factory=dict)
    budget_state: dict[str, Any] = field(default_factory=dict)
    expires_at: Any = None

    @property
    def connector_config_fingerprint(self) -> str:
        return collection_fingerprint(
            self.request, self.connector, self.connector_options
        )


@dataclass(frozen=True)
class ResolvedLibraryLineage:
    lineage: UUID
    checkpoint: ConnectorCheckpoint | None
    resuming: bool
    restart_reason: LegacyCheckpointRestartReason | None = None
    progress: outputs.LineageProgressState = outputs.LineageProgressState.EMPTY


async def resolve_library_lineage(
    connection: outputs.Connection,
    job_id: UUID,
    *,
    spec: LibraryLineageSpec,
    datasets: Sequence[outputs.DatasetKey],
) -> ResolvedLibraryLineage:
    """Resolve, restart, and prepare one library lineage atomically.

    No dataset state is changed until persisted identity and any durable cursor
    have been decoded. A rejected cursor is never attached to the replacement
    lineage.
    """
    async with connection.transaction():
        active_lineages = await outputs.active_lineages_for_runtime(
            connection,
            job_id,
            outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
            lock=True,
        )
        candidate = await outputs.find_recoverable_library_lineage(
            connection,
            job_id,
            source_url=spec.request.base_url,
            connector=spec.connector,
            connector_version=spec.connector_version,
            connector_config_fingerprint=spec.connector_config_fingerprint,
            dataset_fingerprint=spec.dataset_fingerprint,
            lock=True,
        )
        checkpoint: ConnectorCheckpoint | None = None
        restart_reason: LegacyCheckpointRestartReason | None = None
        resuming = candidate is not None
        progress_state = outputs.LineageProgressState.EMPTY

        # A job's dataset state is job-scoped, not lineage-scoped. Retaining an
        # older active cursor would allow A -> B -> A configuration changes to
        # resurrect A and combine its cursor with B's dataset counters.
        for active_lineage in active_lineages:
            if active_lineage != candidate and not await outputs.reject_lineage(
                connection, job_id, active_lineage
            ):
                raise RuntimeError("checkpoint lineage was no longer active")

        if candidate is not None:
            runtime_configuration: outputs.LineageRuntimeConfiguration | None
            try:
                runtime_configuration = await outputs.lineage_runtime_configuration(
                    connection, job_id, candidate
                )
            except ValueError:
                runtime_configuration = None
                restart_reason = (
                    LegacyCheckpointRestartReason.DURABLE_CONFIGURATION_INVALID
                )
            try:
                progress = await outputs.lineage_progress(connection, job_id, candidate)
            except ValueError:
                progress = outputs.LineageProgress(outputs.LineageProgressState.EMPTY)
                restart_reason = LegacyCheckpointRestartReason.MALFORMED_CHECKPOINT
            progress_state = progress.state

            if restart_reason is None and runtime_configuration is not None:
                durable_request, restart_reason = _durable_request(
                    runtime_configuration
                )
                if durable_request is not None:
                    restart_reason = _configuration_restart_reason(
                        durable_request, runtime_configuration, spec
                    )
                    if (
                        restart_reason is None
                        and progress.state is outputs.LineageProgressState.RESUMABLE
                        and progress.checkpoint is not None
                    ):
                        decoded = decode_legacy_checkpoint(
                            cast(
                                Mapping[str, JsonValue],
                                progress.checkpoint.model_dump(mode="json"),
                            ),
                            request=spec.request,
                            connector=spec.connector,
                            connector_version=spec.connector_version,
                            options=spec.connector_options,
                            durable_request=durable_request,
                            durable_options=runtime_configuration.connector_options,
                        )
                        if decoded.outcome == "compatible":
                            checkpoint = decoded.checkpoint
                        else:
                            restart_reason = decoded.reason
                    elif progress.state is outputs.LineageProgressState.TERMINAL_INCOMPLETE:
                        restart_reason = (
                            LegacyCheckpointRestartReason.INCOMPLETE_TERMINAL_CHECKPOINT
                        )

        if candidate is None or restart_reason is not None:
            if (
                candidate is not None
                and candidate in active_lineages
                and not await outputs.reject_lineage(connection, job_id, candidate)
            ):
                raise RuntimeError("checkpoint lineage was no longer active")
            candidate = await outputs.create_lineage(
                connection,
                job_id,
                source_id=spec.request.source_id,
                source_url=spec.request.base_url,
                connector=spec.connector,
                connector_version=spec.connector_version,
                connector_configuration=spec.connector_configuration,
                connector_config_fingerprint=spec.connector_config_fingerprint,
                dataset_fingerprint=spec.dataset_fingerprint,
                dataset_selection=spec.dataset_selection,
                runtime_format=outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
                collection_request=spec.request.model_dump(mode="json"),
                connector_options=spec.connector_options,
                budget_state=spec.budget_state,
                expires_at=spec.expires_at,
            )
            checkpoint = None
            resuming = False
            progress_state = outputs.LineageProgressState.EMPTY

        for dataset in datasets:
            await outputs.prepare_dataset_for_collection(
                connection, job_id, dataset, resuming=resuming
            )

        assert candidate is not None
        return ResolvedLibraryLineage(
            lineage=candidate,
            checkpoint=checkpoint,
            resuming=resuming,
            restart_reason=restart_reason,
            progress=progress_state,
        )


def _durable_request(
    configuration: outputs.LineageRuntimeConfiguration,
) -> tuple[CollectionRequest | None, LegacyCheckpointRestartReason | None]:
    if configuration.runtime_format is not outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1:
        return None, LegacyCheckpointRestartReason.DURABLE_CONFIGURATION_UNAVAILABLE
    if configuration.collection_request is None or configuration.connector_options is None:
        return None, LegacyCheckpointRestartReason.DURABLE_CONFIGURATION_UNAVAILABLE
    try:
        return CollectionRequest.model_validate(configuration.collection_request), None
    except ValidationError:
        return None, LegacyCheckpointRestartReason.DURABLE_CONFIGURATION_INVALID


def _configuration_restart_reason(
    durable_request: CollectionRequest,
    configuration: outputs.LineageRuntimeConfiguration,
    spec: LibraryLineageSpec,
) -> LegacyCheckpointRestartReason | None:
    durable_options = configuration.connector_options
    if durable_options is None:
        return LegacyCheckpointRestartReason.DURABLE_CONFIGURATION_UNAVAILABLE
    if durable_request.source_id != spec.request.source_id:
        return LegacyCheckpointRestartReason.LEGACY_IDENTITY_MISMATCH
    try:
        durable_fingerprint = collection_fingerprint(
            durable_request,
            spec.connector,
            cast(dict[str, JsonValue], durable_options),
        )
    except (TypeError, ValueError):
        return LegacyCheckpointRestartReason.DURABLE_CONFIGURATION_INVALID
    if durable_fingerprint != spec.connector_config_fingerprint:
        return LegacyCheckpointRestartReason.COLLECTION_CONFIGURATION_CHANGED
    return None
