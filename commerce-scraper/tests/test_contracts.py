from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from pydantic import ValidationError

from mb_commerce_scraper import (
    CollectionRequest,
    CommerceProductSnapshot,
    ConnectorCapabilities,
    ConnectorCheckpoint,
    Diagnostic,
    DiagnosticCode,
    DiagnosticSeverity,
    EntityPage,
    Money,
    RefreshMode,
    ShopifyConnector,
    SnapshotField,
    build_checkpoint,
    collection_fingerprint,
)
from mb_commerce_scraper.testing import assert_connector_pages


def request(*, limit: int | None = None) -> CollectionRequest:
    return CollectionRequest(source_id="shop", base_url="https://shop.test/", refresh_mode=RefreshMode.FULL, requested_fields=frozenset({SnapshotField.IDENTITY}), result_limit=limit)


def test_money_is_decimal_strict_and_contracts_are_frozen() -> None:
    money = Money(amount=Decimal("12.30"), currency="EUR")
    assert money.amount == Decimal("12.30")
    with pytest.raises(ValidationError):
        Money.model_validate({"amount": 12.3, "currency": "EUR"})
    with pytest.raises(ValidationError):
        money.currency = "USD"


def test_fingerprint_excludes_operational_bounds() -> None:
    first = collection_fingerprint(request(limit=1), "shopify", {"currency": "EUR"})
    second = collection_fingerprint(request(limit=200), "shopify", {"currency": "EUR"})
    assert first == second
    assert first != collection_fingerprint(request(), "shopify", {"currency": "USD"})


def test_checkpoint_is_versioned_and_cursor_required() -> None:
    fingerprint = collection_fingerprint(request(), "shopify", {})
    checkpoint = ConnectorCheckpoint(connector="shopify", connector_version="1", source_id="shop", lineage="run-1", collection_fingerprint=fingerprint, resume_after={"page": 2})
    assert checkpoint.checkpoint_schema_version == 1
    with pytest.raises(ValidationError):
        ConnectorCheckpoint(connector="shopify", connector_version="1", source_id="shop", lineage="run-1", collection_fingerprint=fingerprint, resume_after=None)


def test_checkpoint_builder_uses_the_canonical_collection_identity() -> None:
    checkpoint = build_checkpoint(
        connector="shopify",
        connector_version="1",
        request=request(),
        lineage="run-1",
        resume_after={"page": 2},
        options={"currency": "EUR"},
    )

    assert checkpoint.source_id == "shop"
    assert checkpoint.resume_after == {"page": 2}
    assert checkpoint.collection_fingerprint == collection_fingerprint(
        request(), "shopify", {"currency": "EUR"}
    )


def test_checkpoint_rejects_credentials_without_retaining_the_value() -> None:
    secret = "checkpoint-secret-sentinel"
    fingerprint = collection_fingerprint(request(), "shopify", {})

    with pytest.raises(ValueError) as caught:
        ConnectorCheckpoint(
            connector="shopify",
            connector_version="1",
            source_id="shop",
            lineage="run-1",
            collection_fingerprint=fingerprint,
            resume_after={"page": 2, "access_token": secret},
        )

    assert "credential-bearing or unsafe data" in str(caught.value)
    assert secret not in str(caught.value)
    assert caught.value.__context__ is None


def test_shared_edge_is_typed_and_exposed_as_a_named_capability() -> None:
    assert ShopifyConnector.capabilities.shared_edge == "edge:shopify"
    assert "shared_edge:edge:shopify" in ShopifyConnector.capabilities.named_capabilities()
    with pytest.raises(ValidationError, match="shared edge"):
        ConnectorCapabilities(
            snapshot_fields=frozenset({SnapshotField.IDENTITY}),
            refresh_modes=frozenset({RefreshMode.FULL}),
            shared_edge="  ",
        )


async def test_result_limit_page_requires_resume_cursor() -> None:
    async def pages() -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        yield EntityPage(
            page_id="main:limit",
            sequence=0,
            items=(),
            terminal=True,
            enumeration_intact=False,
            discovered=1,
            diagnostics=(
                Diagnostic(
                    code=DiagnosticCode.RESULT_LIMIT_REACHED,
                    severity=DiagnosticSeverity.INFO,
                    message="result limit reached",
                    retryable=False,
                    affects_completeness=True,
                ),
            ),
        )

    with pytest.raises(AssertionError, match="must carry a resume cursor"):
        await assert_connector_pages(pages())
