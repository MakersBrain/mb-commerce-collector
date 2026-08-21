from datetime import UTC, datetime
from decimal import Decimal

import pytest

from mb_commerce_scraper import CollectionRequest, RefreshMode, SnapshotField
from mb_commerce_scraper.connectors import ConnectorContext, ShopifyConnector, ShopifyOptions
from mb_commerce_scraper.testing import FakeTransport, assert_connector_pages
from mb_commerce_scraper.transports import MemoryRequestBudget, MiddlewareTransport

NOW = datetime(2026, 8, 15, tzinfo=UTC)


def product(identifier: int = 1) -> dict[str, object]:
    return {
        "id": identifier,
        "handle": f"clay-{identifier}",
        "title": "Clay",
        "body_html": '<p>Body</p><a href="/files/spec.pdf">Spec</a>',
        "updated_at": "2026-08-14T12:00:00Z",
        "options": [{"name": "Size"}],
        "images": [{"id": 5, "src": "https://cdn.test/clay.jpg"}],
        "variants": [
            {
                "id": identifier * 10,
                "title": "10 kg",
                "price": "12.30",
                "compare_at_price": "15.00",
                "available": True,
                "option1": "10 kg",
                "grams": 10_000,
            }
        ],
    }


async def test_shopify_collects_from_fake_transport_and_resumes() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/meta.json", json_body={"currency": "EUR"})
    transport.add("https://shop.test/products.json", json_body={"products": [{"id": 1, "handle": "clay", "title": "Clay", "variants": [{"id": 2, "price": "12.30", "available": True}]}]})
    connector = ShopifyConnector(transport, ShopifyOptions(), ConnectorContext())
    request = CollectionRequest(source_id="shop", base_url="https://shop.test", refresh_mode=RefreshMode.FULL, requested_fields=frozenset(SnapshotField))
    pages = await assert_connector_pages(connector.collect(request))
    assert pages[0].items[0].variants[0].offers[0].price.currency == "EUR"
    assert [item.url for item in transport.requests] == ["https://shop.test/meta.json", "https://shop.test/products.json"]


async def test_shopify_limit_produces_compatible_checkpoint() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/products.json", json_body={"products": [{"id": 1, "handle": "one", "title": "One"}, {"id": 2, "handle": "two", "title": "Two"}]})
    connector = ShopifyConnector(transport, ShopifyOptions(currency="EUR", page_size=2))
    request = CollectionRequest(source_id="shop", base_url="https://shop.test", result_limit=1)
    page = await anext(connector.collect(request))
    assert page.terminal and not page.enumeration_intact
    checkpoint = connector.checkpoint(request, "lineage", page.resume_after)
    assert checkpoint.resume_after == {"partition": "main", "page": 1, "offset": 1}


async def test_product_json_inventory_produces_exact_stock_and_full_snapshot() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/products.json", json_body={"products": [product()]})
    transport.add(
        "https://shop.test/products/clay-1.js",
        json_body={
            "variants": [
                {
                    "id": 10,
                    "inventory_quantity": 7,
                    "inventory_management": "shopify",
                    "inventory_policy": "deny",
                }
            ]
        },
    )
    connector = ShopifyConnector(
        transport,
        ShopifyOptions(currency="EUR", inventory_method="product_json"),
        ConnectorContext(clock=lambda: NOW),
    )

    [page] = await assert_connector_pages(connector.collect(CollectionRequest(
        source_id="shop", base_url="https://shop.test"
    )))
    snapshot = page.items[0]
    variant = snapshot.variants[0]
    assert ShopifyConnector.capabilities.stock_kinds == {"exact", "unknown"}
    assert variant.stock is not None
    assert variant.stock.quantity == 7
    assert variant.stock.quantity_kind == "exact"
    assert [(offer.role, offer.price.amount) for offer in variant.offers] == [
        ("sale", Decimal("12.30")),
        ("regular", Decimal("15.00")),
    ]
    assert variant.options == {"Size": "10 kg"}
    assert variant.published_attributes["shipping_weight_g"] == 10_000
    assert snapshot.documents[0].url == "https://shop.test/files/spec.pdf"
    assert snapshot.images[0].external_id == "5"
    assert snapshot.source_updated_at == datetime(2026, 8, 14, 12, tzinfo=UTC)
    inventory_request = transport.requests[1]
    assert inventory_request.priority.name == "OPTIONAL"
    assert not inventory_request.required
    assert inventory_request.estimated_bytes == 250_000
    assert inventory_request.headers == {"Cookie": ""}


async def test_product_html_inventory_uses_section_and_parses_exact_stock() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/products.json", json_body={"products": [product()]})
    transport.add(
        "https://shop.test/products/clay-1",
        body=(
            '<script>{"id":10,"inventory_quantity":6,'
            '"inventory_management":"shopify","inventory_policy":"deny"}</script>'
        ),
    )
    connector = ShopifyConnector(
        transport,
        ShopifyOptions(
            currency="EUR",
            inventory_method="product_html",
            inventory_section_id="product-inventory",
            inventory_request_estimated_bytes=1234,
        ),
        ConnectorContext(clock=lambda: NOW),
    )

    [page] = await assert_connector_pages(
        connector.collect(CollectionRequest(source_id="shop", base_url="https://shop.test"))
    )
    assert page.items[0].variants[0].stock is not None
    assert page.items[0].variants[0].stock.quantity == 6
    assert transport.requests[1].query == {"section_id": "product-inventory"}
    assert transport.requests[1].estimated_bytes == 1234


async def test_inventory_batches_rotate_between_configured_groups() -> None:
    transport = FakeTransport()
    products = [product(index) for index in range(1, 4)]
    transport.add("https://shop.test/products.json", json_body={"products": products})
    for index in range(1, 4):
        transport.add(
            f"https://shop.test/products/clay-{index}.js",
            json_body={
                "variants": [
                    {
                        "id": index * 10,
                        "inventory_quantity": index,
                        "inventory_management": "shopify",
                        "inventory_policy": "deny",
                    }
                ]
            },
        )
    connector = ShopifyConnector(
        transport,
        ShopifyOptions(
            currency="EUR", inventory_method="product_json", inventory_batch_size=2
        ),
        ConnectorContext(clock=lambda: NOW),
    )

    [page] = await assert_connector_pages(
        connector.collect(CollectionRequest(source_id="shop", base_url="https://shop.test"))
    )
    stocks = [item.variants[0].stock for item in page.items]
    assert all(stock is not None for stock in stocks)
    assert [stock.quantity for stock in stocks if stock is not None] == [1, 2, 3]
    assert [reason.value for reason in transport.rotations] == ["explicit"]


async def test_inventory_budget_reserves_the_next_discovery_page() -> None:
    backend = FakeTransport()
    products = [product(index) for index in range(1, 4)]
    backend.add("https://shop.test/products.json", json_body={"products": products})
    backend.add(
        "https://shop.test/products/clay-1.js",
        json_body={
            "variants": [
                {
                    "id": 10,
                    "inventory_quantity": 9,
                    "inventory_management": "shopify",
                    "inventory_policy": "deny",
                }
            ]
        },
    )
    backend.add("https://shop.test/products.json", json_body={"products": []})
    budget = MemoryRequestBudget(maximum_requests=3, maximum_bytes=3_000_000)
    transport = MiddlewareTransport(backend, budget=budget, retries=0)
    connector = ShopifyConnector(
        transport,
        ShopifyOptions(
            currency="EUR", page_size=3, inventory_method="product_json"
        ),
        ConnectorContext(clock=lambda: NOW, budget=budget),
    )

    pages = await assert_connector_pages(
        connector.collect(CollectionRequest(source_id="shop", base_url="https://shop.test"))
    )
    assert len(pages) == 2
    assert budget.requests == 3
    first_stock = pages[0].items[0].variants[0].stock
    second_stock = pages[0].items[1].variants[0].stock
    assert first_stock is not None and first_stock.quantity == 9
    assert second_stock is not None and second_stock.quantity is None
    assert pages[0].diagnostics[0].code == "optional_enrichment_skipped"


async def test_feed_failure_and_budget_exhaustion_are_resumable_diagnostics() -> None:
    failed = FakeTransport()
    failed.add("https://shop.test/products.json", status=503)
    connector = ShopifyConnector(failed, ShopifyOptions(currency="EUR"))
    [failure] = await assert_connector_pages(
        connector.collect(CollectionRequest(source_id="shop", base_url="https://shop.test"))
    )
    assert not failure.enumeration_intact
    assert failure.resume_after == {"partition": "main", "page": 1}
    assert failure.diagnostics[0].code == "enumeration_incomplete"

    budget = MemoryRequestBudget(maximum_requests=0)
    budgeted = ShopifyConnector(
        FakeTransport(),
        ShopifyOptions(currency="EUR"),
        ConnectorContext(budget=budget),
    )
    [exhausted] = await assert_connector_pages(
        budgeted.collect(CollectionRequest(source_id="shop", base_url="https://shop.test"))
    )
    assert exhausted.diagnostics[0].code == "request_budget_exhausted"


async def test_cancellation_and_checkpoint_option_drift() -> None:
    cancelled_transport = FakeTransport()
    connector = ShopifyConnector(
        cancelled_transport,
        ShopifyOptions(currency="EUR"),
        ConnectorContext(cancelled=lambda: True),
    )
    request = CollectionRequest(source_id="shop", base_url="https://shop.test")
    assert [page async for page in connector.collect(request)] == []
    assert cancelled_transport.requests == []

    original = ShopifyConnector(FakeTransport(), ShopifyOptions(currency="EUR"))
    checkpoint = original.checkpoint(
        request, "lineage", {"partition": "main", "page": 1}
    )
    changed = ShopifyConnector(
        FakeTransport(),
        ShopifyOptions(currency="EUR", inventory_method="product_json"),
    )
    with pytest.raises(ValueError, match="CHECKPOINT_INVALID"):
        await anext(changed.collect(request, checkpoint))
