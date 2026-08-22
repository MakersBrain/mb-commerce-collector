"""Platform-neutral, page-bounded connector and dataset orchestration."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from pydantic import BaseModel

from mb_ceramics_catalogue.connectors.base import (
    CollectionRequest,
    CommerceConnector,
    ConnectorCheckpoint,
    DiagnosticCode,
    EntityPage,
)
from mb_ceramics_catalogue.connectors.commerce import CommerceProductSnapshot
from mb_ceramics_catalogue.datasets.base import ProjectionContext
from mb_ceramics_catalogue.datasets.registry import DatasetRegistry
from mb_ceramics_catalogue.observability import metrics

from .outputs import ArtifactStore, BatchIdentity, StoredBatch


class DatasetPageState(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class DatasetPageOutcome:
    dataset: str
    state: DatasetPageState
    records: int = 0
    error: str | None = None


@dataclass(frozen=True)
class PipelineResult:
    pages: int
    terminal: bool
    enumeration_intact: bool
    limited: bool
    datasets: Mapping[str, DatasetPageState]

    def __post_init__(self) -> None:
        if self.limited and (not self.terminal or self.enumeration_intact):
            raise ValueError(
                "a result-limited pipeline must terminate with an incomplete enumeration"
            )


class PageCommitter(Protocol):
    """Database half of the atomic page/checkpoint commit protocol."""

    async def commit_page(
        self,
        page: EntityPage[CommerceProductSnapshot],
        batches: Sequence[StoredBatch],
        outcomes: Sequence[DatasetPageOutcome],
    ) -> None: ...


class ConnectorPipeline:
    """Fan each bounded connector page into independently failing datasets."""

    def __init__(
        self,
        registry: DatasetRegistry,
        store: ArtifactStore,
        committer: PageCommitter,
    ) -> None:
        self.registry = registry
        self.store = store
        self.committer = committer

    async def run(
        self,
        *,
        job_id: str,
        checkpoint_lineage: str,
        connector: CommerceConnector,
        request: CollectionRequest,
        datasets: Sequence[str],
        checkpoint: ConnectorCheckpoint | None = None,
        projection_configuration: Mapping[str, dict[str, Any]] | None = None,
        initial_states: Mapping[str, DatasetPageState] | None = None,
    ) -> PipelineResult:
        selected = tuple(dict.fromkeys(datasets))
        if not selected:
            raise ValueError("at least one dataset is required")
        required_fields, required_capabilities = self.registry.collection_requirements(selected)
        if not connector.capabilities.supports(required_fields, request.refresh_mode):
            raise ValueError("connector capabilities do not satisfy selected datasets")
        if not required_fields <= request.requested_fields:
            raise ValueError("collection request omits fields required by selected datasets")
        available_capabilities = connector.capabilities.named_capabilities()
        if not required_capabilities <= available_capabilities:
            missing = sorted(required_capabilities - available_capabilities)
            raise ValueError(f"connector lacks dataset capabilities: {', '.join(missing)}")

        states = {name: (initial_states or {}).get(name, DatasetPageState.SUCCEEDED) for name in selected}
        pages = 0
        terminal = False
        intact = True
        limited = False
        async for page in connector.collect(request, checkpoint):
            page_limited = any(
                diagnostic.code == DiagnosticCode.RESULT_LIMIT_REACHED
                for diagnostic in page.diagnostics
            )
            if page_limited:
                if not page.terminal or page.enumeration_intact:
                    raise ValueError(
                        "a result-limit page must be terminal with an incomplete enumeration"
                    )
                if page.resume_after is None:
                    raise ValueError(
                        "a result-limit page must retain a resumable checkpoint cursor"
                    )
            metrics.pipeline_entities(connector.name, connector.version, len(page.items))
            batches: list[StoredBatch] = []
            outcomes: list[DatasetPageOutcome] = []
            for name in selected:
                definition = self.registry.get(name)
                if states[name] == DatasetPageState.FAILED:
                    outcomes.append(DatasetPageOutcome(name, DatasetPageState.SKIPPED))
                    metrics.pipeline_records(
                        name, definition.version, DatasetPageState.SKIPPED.value, 0
                    )
                    continue
                context = ProjectionContext(
                    collection_id=checkpoint_lineage,
                    source_id=request.source_id,
                    dataset=name,
                    dataset_version=definition.version,
                    projector_version=definition.projector_version,
                    configuration=(projection_configuration or {}).get(name, {}),
                )
                identity = BatchIdentity(
                    job_id=job_id,
                    checkpoint_lineage=checkpoint_lineage,
                    partition_key=page.partition_key,
                    page_id=page.page_id,
                    page_sequence=page.sequence,
                    dataset=name,
                    contract_version=definition.version,
                    projector_version=definition.projector_version,
                )
                try:
                    batch = self.store.stage_batch(
                        identity,
                        self._records(page.items, name, context),
                    )
                # A projector is an isolation boundary: one dataset must not
                # discard another dataset's valid output from the same fetch.
                except Exception as error:  # noqa: BLE001
                    states[name] = DatasetPageState.FAILED
                    outcomes.append(DatasetPageOutcome(name, DatasetPageState.FAILED, error=str(error)))
                    metrics.pipeline_records(name, definition.version, DatasetPageState.FAILED.value, 0)
                else:
                    batches.append(batch)
                    outcomes.append(
                        DatasetPageOutcome(name, DatasetPageState.SUCCEEDED, records=batch.records)
                    )
                    metrics.pipeline_records(
                        name,
                        definition.version,
                        DatasetPageState.SUCCEEDED.value,
                        batch.records,
                    )

            # The implementation of this call must persist all batch metadata and
            # the page cursor in one transaction. A failure deliberately prevents
            # the connector from advancing to another page in this attempt.
            await self.committer.commit_page(page, batches, outcomes)
            pages += 1
            terminal = page.terminal
            intact = intact and page.enumeration_intact
            limited = limited or page_limited

        return PipelineResult(
            pages=pages,
            terminal=terminal,
            enumeration_intact=intact,
            limited=limited,
            datasets=dict(states),
        )

    def _records(
        self,
        entities: Iterable[CommerceProductSnapshot],
        dataset: str,
        context: ProjectionContext,
    ) -> Iterable[Mapping[str, Any]]:
        for entity in entities:
            for record in self.registry.project_validated(dataset, entity, context):
                if not isinstance(record, BaseModel):
                    raise TypeError("validated dataset projector returned a non-model record")
                yield record.model_dump(mode="json")
