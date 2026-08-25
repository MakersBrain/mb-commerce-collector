from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from typing import Protocol, cast

from pydantic import JsonValue

from mb_commerce_scraper.connectors.base import CommerceConnector
from mb_commerce_scraper.models import (
    CollectionRequest,
    CommerceProductSnapshot,
    ConnectorCheckpoint,
    DiagnosticCode,
    EntityPage,
    collection_fingerprint,
    sanitize_commerce_snapshot,
)


class _CheckpointingConnector(Protocol):
    def checkpoint(
        self, request: CollectionRequest, lineage: str, resume_after: JsonValue
    ) -> ConnectorCheckpoint: ...


async def assert_connector_pages(
    pages: AsyncIterator[EntityPage[CommerceProductSnapshot]],
    *,
    connector: CommerceConnector | None = None,
    request: CollectionRequest | None = None,
    start_sequence: int = 0,
    forbidden_values: tuple[str, ...] = (),
    reopen: Callable[[], CommerceConnector] | None = None,
) -> tuple[EntityPage[CommerceProductSnapshot], ...]:
    """Assert page invariants and, when requested, fresh-connector resume safety."""
    if (connector is None) != (request is None):
        raise AssertionError("connector and request conformance context must be supplied together")
    if reopen is not None and (connector is None or request is None):
        raise AssertionError("resume conformance requires connector and request context")
    collected = tuple([page async for page in pages])
    _assert_collected_pages(
        collected,
        connector=connector,
        request=request,
        start_sequence=start_sequence,
        forbidden_values=forbidden_values,
    )
    limited = [
        page
        for page in collected
        if any(
            diagnostic.code == DiagnosticCode.RESULT_LIMIT_REACHED
            for diagnostic in page.diagnostics
        )
    ]
    if reopen is not None and limited:
        assert connector is not None and request is not None
        terminal = limited[-1]
        checkpoint_method = getattr(connector, "checkpoint", None)
        assert callable(checkpoint_method), (
            "result-limit resume conformance requires connector.checkpoint"
        )
        checkpoint = cast(_CheckpointingConnector, connector).checkpoint(
            request, "conformance-result-limit", terminal.resume_after
        )
        resumed_connector = reopen()
        assert resumed_connector is not connector, "reopen must return a fresh connector"
        assert resumed_connector.name == connector.name
        assert resumed_connector.version == connector.version
        resumed = tuple(
            [
                page
                async for page in resumed_connector.collect(request, checkpoint)
            ]
        )
        assert resumed, "resumed connector emitted no pages"
        _assert_collected_pages(
            resumed,
            connector=resumed_connector,
            request=request,
            start_sequence=resumed[0].sequence,
            forbidden_values=forbidden_values,
        )
        prior_identities = {
            (item.source_id, item.external_id)
            for page in collected
            for item in page.items
        }
        next_item = next(
            (item for page in resumed for item in page.items),
            None,
        )
        assert next_item is not None, "result-limit checkpoint did not resume to a next entity"
        assert (next_item.source_id, next_item.external_id) not in prior_identities, (
            "result-limit checkpoint resumed with a duplicate entity"
        )
    return collected


def _assert_collected_pages(
    collected: tuple[EntityPage[CommerceProductSnapshot], ...],
    *,
    connector: CommerceConnector | None,
    request: CollectionRequest | None,
    start_sequence: int,
    forbidden_values: tuple[str, ...],
) -> None:
    assert collected, "connector emitted no pages"
    assert [page.sequence for page in collected] == list(
        range(start_sequence, start_sequence + len(collected))
    )
    assert collected[-1].terminal
    assert not any(page.terminal for page in collected[:-1])
    identities = {
        (page.partition_key, page.page_id, page.sequence) for page in collected
    }
    assert len(identities) == len(collected), "connector emitted duplicate page identity"
    if connector is not None and request is not None:
        assert re.fullmatch(r"[a-z][a-z0-9-]*", connector.name)
        assert connector.platform and connector.version
        assert connector.capabilities.supports(
            request.requested_fields, request.refresh_mode
        )
        emitted = sum(len(page.items) for page in collected)
        assert request.result_limit is None or emitted <= request.result_limit
    for page in collected:
        limit_reached = any(
            diagnostic.code == DiagnosticCode.RESULT_LIMIT_REACHED
            for diagnostic in page.diagnostics
        )
        if limit_reached:
            assert page.terminal and not page.enumeration_intact
            assert page.resume_after is not None, (
                "result-limit terminal page must carry a resume cursor"
            )
        for item in page.items:
            assert item.source_id and item.external_id and item.canonical_url
            if connector is not None and request is not None:
                assert item.connector == connector.name
                assert item.source_id == request.source_id
            assert item == sanitize_commerce_snapshot(item), (
                "connector emitted unbounded or unsanitized platform extensions"
            )
        if forbidden_values:
            serialized = json.dumps(page.model_dump(mode="json"), sort_keys=True)
            assert not any(value and value in serialized for value in forbidden_values)


async def assert_cancelled_without_requests(
    pages: AsyncIterator[EntityPage[CommerceProductSnapshot]],
    requests: Sequence[object],
) -> None:
    assert [page async for page in pages] == []
    assert not requests, "cancelled connector performed transport I/O"


def assert_checkpoint_matches(
    checkpoint: ConnectorCheckpoint,
    *,
    connector: CommerceConnector,
    request: CollectionRequest,
    options: Mapping[str, JsonValue],
) -> None:
    assert checkpoint.connector == connector.name
    assert checkpoint.connector_version == connector.version
    assert checkpoint.source_id == request.source_id
    assert checkpoint.collection_fingerprint == collection_fingerprint(
        request, connector.name, dict(options)
    )
