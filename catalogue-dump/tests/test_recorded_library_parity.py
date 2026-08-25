"""Recorded parity for selected stable and extracted collection paths.

The production response archive is an external golden-suite input.  A checkout
without it proves no replay parity, so this module skips instead of manufacturing
responses.  When the archive is present, each path opens its own replay-only
session over the same bytes and neither path can fall through to live I/O.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

import pytest
from mb_commerce_scraper import sanitize_json_value

from . import golden_support as support


def _bounded_shopify_legacy(payload: dict[str, Any]) -> dict[str, Any]:
    """Project legacy raw values through the library's public safety contract."""
    records: list[dict[str, Any]] = []
    for original in payload["records"]:
        raw = original["raw"]
        product = raw["product"]
        variant = raw["variant"]
        bounded_product = sanitize_json_value(
            sanitize_json_value(
                {
                    "handle": product.get("handle") or "",
                    "tags": product.get("tags") or [],
                    "options": product.get("options") or [],
                    "legacy_raw_product": product,
                }
            )
        )
        bounded_variant = sanitize_json_value(
            sanitize_json_value({"legacy_raw_variant": variant})
        )
        assert isinstance(bounded_product, dict)
        assert isinstance(bounded_variant, dict)
        records.append(
            {
                **original,
                "raw": {
                    "product": bounded_product["legacy_raw_product"],
                    "variant": bounded_variant["legacy_raw_variant"],
                },
            }
        )
    return {**payload, "records": records}


def _bounded_bigcommerce_legacy(payload: dict[str, Any]) -> dict[str, Any]:
    """Apply the library's public extension bound to legacy raw payloads."""
    records: list[dict[str, Any]] = []
    for original in payload["records"]:
        bounded_raw = sanitize_json_value(original["raw"])
        assert isinstance(bounded_raw, dict)
        records.append({**original, "raw": bounded_raw})
    return {**payload, "records": records}


def _bounded_prestashop_legacy(payload: dict[str, Any]) -> dict[str, Any]:
    """Project legacy raw records through the neutral variant extension."""
    records: list[dict[str, Any]] = []
    for original in payload["records"]:
        extension: Any = {"legacy_raw_record": original["raw"]}
        extension = sanitize_json_value(sanitize_json_value(extension))
        assert isinstance(extension, dict)
        bounded_raw = extension["legacy_raw_record"]
        assert isinstance(bounded_raw, dict)
        records.append({**original, "raw": bounded_raw})
    return {**payload, "records": records}


def _bounded_woocommerce_legacy(payload: dict[str, Any]) -> dict[str, Any]:
    """Project Woo raw values through their neutral extension wrappers."""
    records: list[dict[str, Any]] = []
    for original in payload["records"]:
        raw = original["raw"]
        if "product" in raw and "variation" in raw:
            product: Any = {"legacy_raw_product": raw["product"]}
            variation: Any = {"legacy_raw_variant": raw["variation"]}
            product = sanitize_json_value(sanitize_json_value(product))
            variation = sanitize_json_value(sanitize_json_value(variation))
            assert isinstance(product, dict)
            assert isinstance(variation, dict)
            bounded_raw = {
                "product": product["legacy_raw_product"],
                "variation": variation["legacy_raw_variant"],
            }
        else:
            slugs = [
                str(category["slug"])
                for category in raw.get("categories") or []
                if isinstance(category, dict) and category.get("slug")
            ]
            extension: Any = {"category_slugs": slugs, "raw": raw}
            extension = sanitize_json_value(sanitize_json_value(extension))
            assert isinstance(extension, dict)
            bounded_raw = extension["raw"]
        assert isinstance(bounded_raw, dict)
        records.append({**original, "raw": bounded_raw})
    return {**payload, "records": records}


def _bounded_wix_legacy(payload: dict[str, Any]) -> dict[str, Any]:
    """Project Wix product/variant raw values through neutral extensions."""
    records: list[dict[str, Any]] = []
    for original in payload["records"]:
        raw = original["raw"]
        product: Any = {"legacy_raw_product": raw["product"]}
        variant: Any = {"legacy_raw_variant": raw["variant"]}
        product = sanitize_json_value(sanitize_json_value(product))
        variant = sanitize_json_value(sanitize_json_value(variant))
        assert isinstance(product, dict)
        assert isinstance(variant, dict)
        records.append(
            {
                **original,
                "raw": {
                    "product": product["legacy_raw_product"],
                    "variant": variant["legacy_raw_variant"],
                },
            }
        )
    return {**payload, "records": records}


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
RECORDED_BIGCOMMERCE = [
    source
    for name in ("amaco", "speedball")
    for source in _recorded_source_case(name)
]
RECORDED_SIO2 = _recorded_source_case("sio-2")
RECORDED_PRESTASHOP = _recorded_source_case("1240-design")
RECORDED_WOOCOMMERCE = _recorded_source_case("mayco")
RECORDED_WIX = _recorded_source_case("e-cibas")
RECORDED_STARWEB = _recorded_source_case("art4fun")
RECORDED_NITROSELL = _recorded_source_case("the-ceramic-shop")
RECORDED_SUMUP = _recorded_source_case("emily-alarcon")


@pytest.mark.golden
def test_ci_archive_preflight_requires_connector_parity_inputs() -> None:
    support.require_ci_recordings()


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
    bounded_legacy_frozen = support.freeze(
        source, _bounded_shopify_legacy(legacy)
    )
    library_frozen = support.freeze(source, library)
    expected = json.loads(support.golden_path(source).read_text(encoding="utf-8"))

    # Identical failures are not parity.  Anchor the legacy side to the frozen
    # production outcome so a partial host directory or replay gaps cannot let
    # two empty/erroring paths agree with each other.
    assert legacy_frozen["records"] == expected["records"]
    assert legacy_frozen["field_coverage"] == expected["field_coverage"]
    assert legacy_frozen["sample"] == expected["sample"]
    assert legacy_frozen["digest"] == expected["digest"]

    # Semantic output remains exact. Raw upstream payloads are the sole accepted
    # difference: the library applies its public, bounded extension sanitizer at
    # model validation and egress. Projecting legacy raw through those same two
    # boundaries retains a complete normalized-row equality claim.
    assert library_frozen["records"] == legacy_frozen["records"]
    assert library_frozen["discovered"] == legacy_frozen["discovered"]
    assert library_frozen["truncated"] == legacy_frozen["truncated"]
    assert library_frozen["error_count"] == legacy_frozen["error_count"]
    assert library_frozen["errors"] == legacy_frozen["errors"]
    assert library_frozen["field_coverage"] == legacy_frozen["field_coverage"]
    assert library_frozen["requests"] == legacy_frozen["requests"]
    assert library_frozen["rendered_pages"] == legacy_frozen["rendered_pages"] == 0
    assert library_frozen["sample"] == bounded_legacy_frozen["sample"]
    assert library_frozen["digest"] == bounded_legacy_frozen["digest"]


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


@pytest.mark.golden
@pytest.mark.parametrize("source", RECORDED_BIGCOMMERCE)
def test_recorded_bigcommerce_responses_have_legacy_library_projection_parity(
    source: str,
) -> None:
    legacy = asyncio.run(support.collect(source))
    library = asyncio.run(
        support.collect(source, scraper="library_bigcommerce_connector")
    )

    legacy_frozen = support.freeze(source, legacy)
    bounded_legacy_frozen = support.freeze(
        source, _bounded_bigcommerce_legacy(legacy)
    )
    library_frozen = support.freeze(source, library)
    expected = json.loads(support.golden_path(source).read_text(encoding="utf-8"))

    for field in (
        "records",
        "discovered",
        "requests",
        "rendered_pages",
        "field_coverage",
        "sample",
        "digest",
        "truncated",
        "error_count",
        "errors",
    ):
        assert legacy_frozen[field] == expected[field]

    for field in (
        "records",
        "discovered",
        "requests",
        "rendered_pages",
        "field_coverage",
        "truncated",
        "error_count",
        "errors",
    ):
        assert library_frozen[field] == legacy_frozen[field]
    assert library_frozen["sample"] == bounded_legacy_frozen["sample"]
    assert library_frozen["digest"] == bounded_legacy_frozen["digest"]
    assert not legacy["summary"].get("interrupted", False)
    assert not library["summary"].get("interrupted", False)


def _assert_recorded_keyed_projection_parity(
    source: str,
    library_scraper: str,
    bounder: Callable[[dict[str, Any]], dict[str, Any]],
) -> None:
    legacy = asyncio.run(support.collect(source))
    library = asyncio.run(
        support.collect(source, scraper=library_scraper)
    )

    legacy_frozen = support.freeze(source, legacy)
    bounded_legacy = bounder(legacy)
    library_frozen = support.freeze(source, library)
    expected = json.loads(support.golden_path(source).read_text(encoding="utf-8"))

    for field in (
        "records",
        "discovered",
        "requests",
        "rendered_pages",
        "field_coverage",
        "sample",
        "digest",
        "truncated",
        "error_count",
        "errors",
    ):
        assert legacy_frozen[field] == expected[field]

    for field in (
        "records",
        "discovered",
        "requests",
        "rendered_pages",
        "field_coverage",
        "truncated",
        "error_count",
        "errors",
    ):
        assert library_frozen[field] == legacy_frozen[field]
    # Some library paths retain explicit partitions while their legacy crawler
    # flattens discovery before collection. Order is not catalogue identity;
    # every complete normalized row must still be identical by stable key.
    assert {
        row["external_id"]: support.normalise(row)
        for row in library["records"]
    } == {
        row["external_id"]: support.normalise(row)
        for row in bounded_legacy["records"]
    }
    assert not legacy["summary"].get("interrupted", False)
    assert not library["summary"].get("interrupted", False)


@pytest.mark.golden
@pytest.mark.parametrize("source", RECORDED_SIO2)
def test_recorded_sio2_responses_have_legacy_library_projection_parity(
    source: str,
) -> None:
    _assert_recorded_keyed_projection_parity(
        source, "library_sio2_connector", _bounded_prestashop_legacy
    )


@pytest.mark.golden
@pytest.mark.parametrize("source", RECORDED_PRESTASHOP)
def test_recorded_prestashop_responses_have_legacy_library_projection_parity(
    source: str,
) -> None:
    _assert_recorded_keyed_projection_parity(
        source, "library_prestashop_connector", _bounded_prestashop_legacy
    )


@pytest.mark.golden
@pytest.mark.parametrize("source", RECORDED_WOOCOMMERCE)
def test_recorded_woocommerce_responses_have_legacy_library_projection_parity(
    source: str,
) -> None:
    _assert_recorded_keyed_projection_parity(
        source, "library_woocommerce_connector", _bounded_woocommerce_legacy
    )


@pytest.mark.golden
@pytest.mark.parametrize("source", RECORDED_WIX)
def test_recorded_wix_responses_have_legacy_library_projection_parity(
    source: str,
) -> None:
    _assert_recorded_keyed_projection_parity(
        source, "library_wix_connector", _bounded_wix_legacy
    )


@pytest.mark.golden
@pytest.mark.parametrize("source", RECORDED_STARWEB)
def test_recorded_starweb_responses_have_legacy_library_projection_parity(
    source: str,
) -> None:
    legacy = asyncio.run(support.collect(source))
    library = asyncio.run(
        support.collect(source, scraper="library_starweb_connector")
    )

    legacy_frozen = support.freeze(source, legacy)
    library_frozen = support.freeze(source, library)
    expected = json.loads(support.golden_path(source).read_text(encoding="utf-8"))

    for field in (
        "records",
        "discovered",
        "requests",
        "rendered_pages",
        "field_coverage",
        "sample",
        "digest",
        "truncated",
        "error_count",
        "errors",
    ):
        assert legacy_frozen[field] == expected[field]

    for field in (
        "records",
        "requests",
        "rendered_pages",
        "field_coverage",
        "sample",
        "digest",
        "truncated",
        "error_count",
        "errors",
    ):
        assert library_frozen[field] == legacy_frozen[field]
    # Legacy counts unique discovered product URLs. The neutral page engine
    # counts every parsed Product entity, including multiple JSON-LD products
    # on one URL; characterize both without weakening complete row equality.
    assert legacy_frozen["discovered"] == 2238
    assert library_frozen["discovered"] == 2675


@pytest.mark.golden
@pytest.mark.parametrize("source", RECORDED_NITROSELL)
def test_recorded_nitrosell_responses_have_legacy_library_projection_parity(
    source: str,
) -> None:
    _assert_recorded_keyed_projection_parity(
        source, "library_nitrosell_connector", lambda payload: payload
    )


@pytest.mark.golden
@pytest.mark.parametrize("source", RECORDED_SUMUP)
def test_recorded_sumup_responses_have_legacy_library_projection_parity(
    source: str,
) -> None:
    _assert_recorded_keyed_projection_parity(
        source, "library_sumup_connector", lambda payload: payload
    )
