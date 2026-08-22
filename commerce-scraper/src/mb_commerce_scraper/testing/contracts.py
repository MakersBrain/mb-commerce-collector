from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator, Mapping, Sequence

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


async def assert_connector_pages(
    pages: AsyncIterator[EntityPage[CommerceProductSnapshot]],
    *,
    connector: CommerceConnector | None = None,
    request: CollectionRequest | None = None,
    start_sequence: int = 0,
    forbidden_values: tuple[str, ...] = (),
) -> tuple[EntityPage[CommerceProductSnapshot], ...]:
    """Assert page invariants, optionally including connector/request context."""
    if (connector is None) != (request is None):
        raise AssertionError("connector and request conformance context must be supplied together")
    collected = tuple([page async for page in pages])
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
    return collected


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
