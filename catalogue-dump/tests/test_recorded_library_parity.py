"""Recorded parity for selected stable and extracted collection paths.

The production response archive is an external golden-suite input.  A checkout
without it proves no replay parity, so this module skips instead of manufacturing
responses.  When the archive is present, each path opens its own replay-only
session over the same bytes and neither path can fall through to live I/O.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from . import golden_support as support


def _recorded_shopify_sources() -> list[str]:
    configured = support.sources()
    return [
        name
        for name in support.cached_sources()
        if configured[name].scraper == "shopify"
        and support.golden_path(name).is_file()
    ]


RECORDED_SHOPIFY = _recorded_shopify_sources()
CASES: list[Any] = RECORDED_SHOPIFY or [
    pytest.param(
        None,
        marks=pytest.mark.skip(
            reason="no recorded Shopify responses checked out; parity is unavailable"
        ),
        id="archive-unavailable",
    )
]


def _recorded_source_case(name: str) -> list[Any]:
    if name in support.cached_sources() and support.golden_path(name).is_file():
        return [name]
    return [
        pytest.param(
            None,
            marks=pytest.mark.skip(
                reason=f"no recorded {name} responses checked out; parity is unavailable"
            ),
            id="archive-unavailable",
        )
    ]


RECORDED_KERAMIKBEDARF = _recorded_source_case("keramikbedarf-online")


@pytest.mark.golden
@pytest.mark.parametrize("source", CASES, ids=RECORDED_SHOPIFY or None)
def test_recorded_shopify_responses_have_legacy_library_projection_parity(
    source: str,
) -> None:
    legacy = asyncio.run(support.collect(source))
    library = asyncio.run(
        support.collect(source, scraper="library_shopify_connector")
    )

    legacy_frozen = support.freeze(source, legacy)
    library_frozen = support.freeze(source, library)
    expected = json.loads(support.golden_path(source).read_text(encoding="utf-8"))

    # Identical failures are not parity.  Anchor the legacy side to the frozen
    # production outcome so a partial host directory or replay gaps cannot let
    # two empty/erroring paths agree with each other.
    assert legacy_frozen["records"] == expected["records"]
    assert legacy_frozen["field_coverage"] == expected["field_coverage"]
    assert legacy_frozen["sample"] == expected["sample"]
    assert legacy_frozen["digest"] == expected["digest"]

    # Keep failures useful without asking pytest to render a multi-megabyte
    # record-list diff.  The sample identifies field drift and the digest
    # retains the complete normalized-row equality claim.
    assert library_frozen["records"] == legacy_frozen["records"]
    assert library_frozen["discovered"] == legacy_frozen["discovered"]
    assert library_frozen["truncated"] == legacy_frozen["truncated"]
    assert library_frozen["error_count"] == legacy_frozen["error_count"]
    assert library_frozen["errors"] == legacy_frozen["errors"]
    assert library_frozen["field_coverage"] == legacy_frozen["field_coverage"]
    assert library_frozen["requests"] == legacy_frozen["requests"]
    assert library_frozen["rendered_pages"] == legacy_frozen["rendered_pages"] == 0
    assert library_frozen["sample"] == legacy_frozen["sample"]
    assert library_frozen["digest"] == legacy_frozen["digest"]


@pytest.mark.golden
@pytest.mark.parametrize("source", RECORDED_KERAMIKBEDARF)
def test_recorded_keramikbedarf_responses_have_shopware_projection_parity(
    source: str,
) -> None:
    legacy = asyncio.run(support.collect(source))
    library = asyncio.run(
        support.collect(source, scraper="library_shopware_connector")
    )

    legacy_frozen = support.freeze(source, legacy)
    library_frozen = support.freeze(source, library)
    expected = json.loads(support.golden_path(source).read_text(encoding="utf-8"))

    # Prove the archive is complete enough to reproduce the reviewed production
    # outcome before comparing the extracted path.  A partial cache must fail,
    # not turn matching empty/error results into parity evidence.
    for field in (
        "records",
        "field_coverage",
        "sample",
        "digest",
        "truncated",
        "error_count",
        "errors",
    ):
        assert legacy_frozen[field] == expected[field]

    # Shopware's projected catalogue output and terminal semantics are the
    # migration gate.  Discovery and physical-request totals remain review
    # evidence until a restored recording has characterized both paths.
    for field in (
        "records",
        "field_coverage",
        "sample",
        "digest",
        "truncated",
        "error_count",
        "errors",
    ):
        assert library_frozen[field] == legacy_frozen[field]
    assert not legacy["summary"].get("interrupted", False)
    assert not library["summary"].get("interrupted", False)
