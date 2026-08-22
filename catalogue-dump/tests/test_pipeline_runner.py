from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from pydantic import BaseModel

from mb_ceramics_catalogue.connectors.base import (
    BrowserRequirement,
    CollectionRequest,
    ConnectorCapabilities,
    Diagnostic,
    DiagnosticCode,
    DiagnosticSeverity,
    EntityPage,
    RefreshMode,
    SnapshotField,
    result_limit_diagnostic,
)
from mb_ceramics_catalogue.connectors.commerce import CommerceProductSnapshot
from mb_ceramics_catalogue.datasets.base import ProjectionContext
from mb_ceramics_catalogue.datasets.registry import DatasetRegistry
from mb_ceramics_catalogue.pipeline.outputs import LocalArtifactStore
from mb_ceramics_catalogue.pipeline.runner import (
    ConnectorPipeline,
    DatasetPageState,
    PipelineResult,
)

NOW = datetime(2026, 8, 15, tzinfo=UTC)


class Row(BaseModel):
    value: str


class Definition:
    version = "1"
    projector_version = "1"
    required_snapshot_fields = frozenset({SnapshotField.IDENTITY})
    required_capabilities: frozenset[str] = frozenset()
    record_model = Row

    def __init__(self, name, fail=False):
        self.name = name
        self.fail = fail

    def project(self, entity, context: ProjectionContext):
        if self.fail:
            raise ValueError("projector exploded")
        yield Row(value=f"{context.dataset}:{entity.external_id}")


class Connector:
    name = "fake"
    platform = "fake"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset({SnapshotField.IDENTITY}),
        refresh_modes=frozenset({RefreshMode.FULL}),
        browser=BrowserRequirement.NEVER,
    )

    async def collect(self, request, checkpoint=None) -> AsyncIterator:
        for sequence in range(2):
            yield EntityPage(
                page_id=str(sequence),
                sequence=sequence,
                items=(CommerceProductSnapshot(
                    connector="fake",
                    source_id=request.source_id,
                    external_id=str(sequence),
                    canonical_url=f"https://shop.test/{sequence}",
                    title="Clay",
                    observed_at=NOW,
                ),),
                terminal=sequence == 1,
                discovered=1,
            )


class Committer:
    def __init__(self):
        self.commits = []

    async def commit_page(self, page, batches, outcomes):
        self.commits.append((page, tuple(batches), tuple(outcomes)))


class IncompleteConnector(Connector):
    def __init__(self, diagnostic: Diagnostic, *, resumable: bool = True):
        self.diagnostic = diagnostic
        self.resumable = resumable

    async def collect(self, request, checkpoint=None) -> AsyncIterator:
        yield EntityPage(
            page_id="incomplete",
            sequence=0,
            items=(),
            terminal=True,
            enumeration_intact=False,
            discovered=0,
            diagnostics=(self.diagnostic,),
            resume_after={"index": 1} if self.resumable else None,
        )


def request():
    return CollectionRequest(
        source_id="shop",
        base_url="https://shop.test",
        refresh_mode=RefreshMode.FULL,
        requested_fields=frozenset({SnapshotField.IDENTITY}),
    )


def test_pipeline_result_rejects_limited_intact_enumeration():
    with pytest.raises(ValueError, match="result-limited pipeline"):
        PipelineResult(
            pages=1,
            terminal=True,
            enumeration_intact=True,
            limited=True,
            datasets={},
        )


def test_pipeline_result_rejects_limited_non_terminal_outcome():
    with pytest.raises(ValueError, match="result-limited pipeline"):
        PipelineResult(
            pages=1,
            terminal=False,
            enumeration_intact=False,
            limited=True,
            datasets={},
        )


@pytest.mark.asyncio
async def test_pipeline_stages_and_commits_each_page(tmp_path):
    registry = DatasetRegistry([Definition("good")])
    committer = Committer()
    pipeline = ConnectorPipeline(registry, LocalArtifactStore(tmp_path), committer)

    result = await pipeline.run(
        job_id="job-1",
        checkpoint_lineage="lineage-1",
        connector=Connector(),
        request=request(),
        datasets=("good",),
    )

    assert result.pages == 2 and result.terminal and result.enumeration_intact
    assert not result.limited
    assert result.datasets == {"good": DatasetPageState.SUCCEEDED}
    assert [commit[1][0].records for commit in committer.commits] == [1, 1]
    assert [commit[1][0].identity.page_sequence for commit in committer.commits] == [0, 1]


@pytest.mark.asyncio
async def test_pipeline_marks_only_result_limit_diagnostics_as_limited(tmp_path):
    registry = DatasetRegistry([Definition("good")])
    pipeline = ConnectorPipeline(registry, LocalArtifactStore(tmp_path), Committer())

    limited = await pipeline.run(
        job_id="job-1",
        checkpoint_lineage="lineage-1",
        connector=IncompleteConnector(result_limit_diagnostic(1, "https://shop.test/")),
        request=request(),
        datasets=("good",),
    )
    incomplete = await pipeline.run(
        job_id="job-2",
        checkpoint_lineage="lineage-2",
        connector=IncompleteConnector(
            Diagnostic(
                code=DiagnosticCode.ENUMERATION_INCOMPLETE,
                severity=DiagnosticSeverity.WARNING,
                message="enumeration stopped early",
                retryable=True,
                affects_completeness=True,
            )
        ),
        request=request(),
        datasets=("good",),
    )

    assert limited.limited and not limited.enumeration_intact
    assert not incomplete.limited and not incomplete.enumeration_intact


@pytest.mark.asyncio
async def test_pipeline_rejects_a_result_limit_without_a_resume_cursor(tmp_path):
    registry = DatasetRegistry([Definition("good")])
    pipeline = ConnectorPipeline(registry, LocalArtifactStore(tmp_path), Committer())

    with pytest.raises(ValueError, match="resumable checkpoint cursor"):
        await pipeline.run(
            job_id="job-1",
            checkpoint_lineage="lineage-1",
            connector=IncompleteConnector(
                result_limit_diagnostic(1, "https://shop.test/"),
                resumable=False,
            ),
            request=request(),
            datasets=("good",),
        )


@pytest.mark.asyncio
async def test_projector_failure_isolated_and_later_pages_skip_it(tmp_path):
    registry = DatasetRegistry([Definition("good"), Definition("bad", fail=True)])
    committer = Committer()
    pipeline = ConnectorPipeline(registry, LocalArtifactStore(tmp_path), committer)

    result = await pipeline.run(
        job_id="job-1",
        checkpoint_lineage="lineage-1",
        connector=Connector(),
        request=request(),
        datasets=("good", "bad"),
    )

    assert result.datasets["good"] == DatasetPageState.SUCCEEDED
    assert result.datasets["bad"] == DatasetPageState.FAILED
    assert [outcome.state for outcome in committer.commits[0][2]] == [
        DatasetPageState.SUCCEEDED,
        DatasetPageState.FAILED,
    ]
    assert [outcome.state for outcome in committer.commits[1][2]] == [
        DatasetPageState.SUCCEEDED,
        DatasetPageState.SKIPPED,
    ]


@pytest.mark.asyncio
async def test_resumed_failed_projector_remains_skipped(tmp_path):
    registry = DatasetRegistry([Definition("bad")])
    committer = Committer()
    pipeline = ConnectorPipeline(registry, LocalArtifactStore(tmp_path), committer)

    result = await pipeline.run(
        job_id="job-1",
        checkpoint_lineage="lineage-1",
        connector=Connector(),
        request=request(),
        datasets=("bad",),
        initial_states={"bad": DatasetPageState.FAILED},
    )

    assert result.datasets == {"bad": DatasetPageState.FAILED}
    assert all(
        commit[2][0].state == DatasetPageState.SKIPPED for commit in committer.commits
    )


@pytest.mark.asyncio
async def test_pipeline_rejects_request_missing_dataset_fields(tmp_path):
    definition = Definition("good")
    definition.required_snapshot_fields = frozenset({SnapshotField.OFFERS})
    pipeline = ConnectorPipeline(
        DatasetRegistry([definition]), LocalArtifactStore(tmp_path), Committer()
    )

    with pytest.raises(ValueError, match=r"capabilities|omits"):
        await pipeline.run(
            job_id="job-1",
            checkpoint_lineage="lineage-1",
            connector=Connector(),
            request=request(),
            datasets=("good",),
        )
