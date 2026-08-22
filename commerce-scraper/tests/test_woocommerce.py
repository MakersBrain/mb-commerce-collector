from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from mb_commerce_scraper import CollectionRequest, SnapshotField
from mb_commerce_scraper.connectors import (
    ConnectorContext,
    WooCommerceConnector,
    WooCommerceOptions,
)
from mb_commerce_scraper.testing import FakeTransport, assert_connector_pages

NOW = datetime(2026, 8, 15, 10, 0, tzinfo=UTC)
API = "https://shop.test/wp-json/wc/store/v1/products"


def prices(price: str = "1250", regular: str = "1500") -> dict[str, Any]:
    return {
        "price": price,
        "regular_price": regular,
        "currency_code": "EUR",
        "currency_minor_unit": 2,
    }


def product(identifier: int = 10, *, variable: bool = False) -> dict[str, Any]:
    return {
        "id": identifier,
        "type": "variable" if variable else "simple",
        "name": "Transparent &amp; Gloss Glaze",
        "permalink": f"https://shop.test/product/glaze-{identifier}",
        "description": '<p>A gloss glaze.</p><a href="/docs/sds.pdf">SDS</a>',
        "sku": f"GL-{identifier}",
        "prices": prices(),
        "is_in_stock": True,
        "is_on_backorder": False,
        "sold_individually": False,
        "add_to_cart": {"maximum": 7},
        "brands": [{"name": "Test Ceramics"}],
        "categories": [{"id": 3, "name": "Glazes", "slug": "glazes"}],
        "images": [{"id": 5, "src": "https://cdn.test/glaze.jpg", "alt": "Blue"}],
        "attributes": [{"name": "Firing range", "terms": [{"name": "Cone 6"}]}],
    }


def variation(parent: int = 10) -> dict[str, Any]:
    return {
        "id": parent * 10,
        "parent": parent,
        "type": "variation",
        "permalink": f"https://shop.test/product/glaze-{parent}?size=500ml",
        "variation": "500 ml / Blue",
        "sku": f"GL-{parent}-500",
        "prices": prices(),
        "is_in_stock": True,
        "stock_availability": {"quantity": 4},
        "attributes": [{"name": "Size", "value": "500 ml"}],
    }


def intent(**values: Any) -> CollectionRequest:
    return CollectionRequest(source_id="woo-shop", base_url="https://shop.test/catalogue", **values)


async def test_simple_product_preserves_neutral_fields() -> None:
    transport = FakeTransport()
    transport.add(API, json_body=[product()])
    connector = WooCommerceConnector(
        transport,
        WooCommerceOptions(vat_status="inclusive", stock_from_add_to_cart_maximum=True),
        ConnectorContext(clock=lambda: NOW),
    )
    request = intent()
    pages = await assert_connector_pages(
        connector.collect(request), connector=connector, request=request
    )
    snapshot = pages[0].items[0]
    assert snapshot.title == "Transparent & Gloss Glaze"
    assert snapshot.documents[0].url == "https://shop.test/docs/sds.pdf"
    assert snapshot.variants[0].offers[0].price.amount == 12.50
    stock = snapshot.variants[0].stock
    assert stock is not None and stock.quantity == 7


async def test_variable_product_joins_bulk_variations() -> None:
    transport = FakeTransport()
    transport.add(API, json_body=[product(variable=True)])
    transport.add(API, json_body=[variation()])
    connector = WooCommerceConnector(transport, context=ConnectorContext(clock=lambda: NOW))
    pages = await assert_connector_pages(connector.collect(intent()))
    variant = pages[0].items[0].variants[0]
    assert variant.external_id == "100"
    assert variant.options == {"Size": "500 ml"}
    assert transport.requests[1].query["type"] == "variation"


async def test_categories_are_collection_partitions() -> None:
    transport = FakeTransport()
    transport.add(f"{API}/categories", json_body=[{"id": 3, "slug": "glazes"}])
    transport.add(API, json_body=[product()])
    connector = WooCommerceConnector(transport, context=ConnectorContext(clock=lambda: NOW))
    pages = await assert_connector_pages(connector.collect(intent(partitions=("glazes",))))
    assert pages[0].partition_key == "glazes"
    assert transport.requests[1].query["category"] == 3


async def test_category_boundaries_are_explicit_partition_terminals() -> None:
    transport = FakeTransport()
    transport.add(
        f"{API}/categories",
        json_body=[{"id": 3, "slug": "glazes"}, {"id": 4, "slug": "clay"}],
    )
    transport.add(API, json_body=[product(10)])
    transport.add(API, json_body=[product(20)])
    connector = WooCommerceConnector(
        transport, context=ConnectorContext(clock=lambda: NOW)
    )

    pages = tuple(
        [
            page
            async for page in connector.collect(
                intent(partitions=("glazes", "clay"))
            )
        ]
    )

    assert [(page.partition_key, page.partition_terminal, page.terminal) for page in pages] == [
        ("glazes", True, False),
        ("clay", False, True),
    ]


async def test_limit_checkpoint_rejects_changed_options() -> None:
    transport = FakeTransport()
    transport.add(API, json_body=[product(1), product(2)])
    connector = WooCommerceConnector(transport, WooCommerceOptions(page_size=2))
    request = intent(result_limit=1, requested_fields=frozenset(SnapshotField))
    page = await anext(connector.collect(request))
    checkpoint = connector.checkpoint(request, "lineage", page.resume_after)
    changed = WooCommerceConnector(FakeTransport(), WooCommerceOptions(page_size=1))
    with pytest.raises(ValueError, match="CHECKPOINT_INVALID"):
        await anext(changed.collect(request, checkpoint))
