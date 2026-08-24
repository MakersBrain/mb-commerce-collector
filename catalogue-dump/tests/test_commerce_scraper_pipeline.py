from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
from mb_commerce_scraper import (
    CollectionRequest as LibraryRequest,
)
from mb_commerce_scraper import (
    CommerceProductSnapshot,
    ConnectorCapabilities,
)
from mb_commerce_scraper import (
    ConnectorCheckpoint as LibraryCheckpoint,
)
from mb_commerce_scraper import (
    EntityPage as LibraryPage,
)
from mb_commerce_scraper import (
    RefreshMode as LibraryRefreshMode,
)
from mb_commerce_scraper import (
    SnapshotField as LibraryField,
)

from mb_ceramics_catalogue.connectors.base import (
    CollectionRequest,
    ConnectorCheckpoint,
    RefreshMode,
    SnapshotField,
)
from mb_ceramics_catalogue.datasets import built_in_registry
from mb_ceramics_catalogue.ops.commerce_scraper_pipeline import LibraryPipelineConnector
from mb_ceramics_catalogue.pipeline.outputs import LocalArtifactStore
from mb_ceramics_catalogue.pipeline.runner import ConnectorPipeline


class Connector:
    name = "shopify"
    platform = "shopify"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(LibraryField),
        refresh_modes=frozenset({LibraryRefreshMode.FULL}),
    )

    def __init__(self) -> None:
        self.received: tuple[LibraryRequest, LibraryCheckpoint | None] | None = None

    async def collect(
        self, request: LibraryRequest, checkpoint: LibraryCheckpoint | None = None
    ) -> AsyncIterator[LibraryPage[CommerceProductSnapshot]]:
        self.received = (request, checkpoint)
        yield LibraryPage(
            page_id="main:1",
            sequence=0,
            items=(),
            terminal=True,
            partition_terminal=True,
            discovered=0,
        )


class Telemetry:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def emit(self, event: str, fields: dict[str, Any]) -> None:
        self.events.append((event, fields))


def requests() -> tuple[LibraryRequest, CollectionRequest]:
    library = LibraryRequest(
        source_id="shop",
        base_url="https://shop.test/",
        requested_fields=frozenset({LibraryField.IDENTITY}),
    )
    pipeline = CollectionRequest(
        source_id="shop",
        base_url="https://shop.test",
        refresh_mode=RefreshMode.FULL,
        requested_fields=frozenset({SnapshotField.IDENTITY}),
    )
    return library, pipeline


@pytest.mark.asyncio
async def test_delegates_library_identity_and_revalidates_page_envelope() -> None:
    connector = Connector()
    library_request, pipeline_request = requests()
    checkpoint = LibraryCheckpoint(
        connector="shopify",
        connector_version="1",
        source_id="shop",
        lineage="lineage",
        collection_fingerprint="0" * 64,
        resume_after={"page": 2},
    )
    telemetry = Telemetry()
    adapter = LibraryPipelineConnector(
        connector,
        library_request,
        checkpoint,
        telemetry=telemetry,
        telemetry_context={"collection_id": "job-1"},
    )

    pages = [page async for page in adapter.collect(pipeline_request)]

    assert connector.received == (library_request, checkpoint)
    assert pages[0].terminal
    assert pages[0].page_id == "main:1"
    assert adapter.capabilities.named_capabilities() == frozenset()
    assert [event for event, _ in telemetry.events] == [
        "catalogue.library_connector.collection.started",
        "catalogue.library_connector.page.completed",
        "catalogue.library_connector.collection.completed",
    ]
    assert telemetry.events[0][1] == {
        "collection_id": "job-1",
        "connector": "shopify",
        "connector_version": "1",
        "source_id": "shop",
        "level": "info",
        "resuming": True,
    }
    assert telemetry.events[1][1]["level"] == "debug"
    assert telemetry.events[-1][1]["level"] == "info"
    assert telemetry.events[-1][1]["terminal"] is True


@pytest.mark.asyncio
async def test_rejects_catalogue_checkpoint_and_mismatched_projection_identity() -> None:
    connector = Connector()
    library_request, pipeline_request = requests()
    adapter = LibraryPipelineConnector(connector, library_request, None)
    catalogue_checkpoint = ConnectorCheckpoint(
        connector="shopify",
        connector_version="1",
        source_id="shop",
        lineage="old",
        resume_after={"page": 2},
    )

    with pytest.raises(ValueError, match="cannot enter"):
        _ = [page async for page in adapter.collect(pipeline_request, catalogue_checkpoint)]
    with pytest.raises(ValueError, match="source identities"):
        _ = [
            page
            async for page in adapter.collect(
                pipeline_request.model_copy(update={"source_id": "other"})
            )
        ]


class Committer:
    def __init__(self) -> None:
        self.pages: list[tuple[Any, Any, Any]] = []

    async def commit_page(self, page, batches, outcomes) -> None:
        self.pages.append((page, batches, outcomes))


@pytest.mark.asyncio
async def test_library_connector_runs_through_atomic_projection_pipeline(tmp_path) -> None:
    fields = frozenset({LibraryField.IDENTITY, LibraryField.OFFERS})
    library_request = LibraryRequest(
        source_id="shop", base_url="https://shop.test", requested_fields=fields
    )
    pipeline_request = CollectionRequest(
        source_id="shop",
        base_url="https://shop.test/",
        refresh_mode=RefreshMode.FULL,
        requested_fields=frozenset(
            {SnapshotField.IDENTITY, SnapshotField.OFFERS}
        ),
    )
    committer = Committer()
    connector = LibraryPipelineConnector(Connector(), library_request, None)

    result = await ConnectorPipeline(
        built_in_registry(), LocalArtifactStore(tmp_path), committer
    ).run(
        job_id="job",
        checkpoint_lineage="lineage",
        connector=connector,
        request=pipeline_request,
        datasets=("commerce.price_observation.v1",),
    )

    assert result.pages == 1
    assert result.terminal
    assert len(committer.pages) == 1
