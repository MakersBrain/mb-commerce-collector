from decimal import Decimal

import pytest
from pydantic import ValidationError

from mb_commerce_scraper import (
    CollectionRequest,
    ConnectorCheckpoint,
    Money,
    RefreshMode,
    SnapshotField,
    collection_fingerprint,
)


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
