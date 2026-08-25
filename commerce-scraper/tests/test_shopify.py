import asyncio
import json
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest

import mb_commerce_scraper.connectors.shopify as shopify_module
from mb_commerce_scraper import CollectionRequest, RefreshMode, SnapshotField
from mb_commerce_scraper.connectors import ConnectorContext, ShopifyConnector, ShopifyOptions
from mb_commerce_scraper.testing import (
    FakeTransport,
    assert_cancelled_without_requests,
    assert_checkpoint_matches,
    assert_connector_pages,
)
from mb_commerce_scraper.transports import (
    MemoryRequestBudget,
    MiddlewareTransport,
    PerOriginRateLimiter,
    RotationReason,
    TransportRequest,
    TransportResponse,
)

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
    pages = await assert_connector_pages(
        connector.collect(request), connector=connector, request=request
    )
    assert pages[0].items[0].variants[0].offers[0].price.currency == "EUR"
    assert [item.url for item in transport.requests] == ["https://shop.test/meta.json", "https://shop.test/products.json"]


async def test_shopify_limit_produces_compatible_checkpoint() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/products.json", json_body={"products": [{"id": 1, "handle": "one", "title": "One"}, {"id": 2, "handle": "two", "title": "Two"}]})
    connector = ShopifyConnector(transport, ShopifyOptions(currency="EUR", page_size=2))
    request = CollectionRequest(source_id="shop", base_url="https://shop.test", result_limit=1)
    [page] = await assert_connector_pages(
        connector.collect(request), connector=connector, request=request
    )
    assert page.terminal and not page.enumeration_intact
    checkpoint = connector.checkpoint(request, "lineage", page.resume_after)
    assert checkpoint.resume_after == {"partition": "main", "page": 1, "offset": 1}
    assert_checkpoint_matches(
        checkpoint,
        connector=connector,
        request=request,
        options=connector.options.model_dump(mode="json"),
    )


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
    assert snapshot.description == "Body Spec"
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


@pytest.mark.parametrize(
    ("document", "identifier", "quantity"),
    (
        (
            '{"id":101,"inventory_quantity":12,"inventory_management":"shopify",'
            '"inventory_policy":"deny"}',
            "101",
            12,
        ),
        (
            '<option value="102" data-inventory="14" data-inventory-policy="deny" '
            'data-inventory-management="shopify">',
            "102",
            14,
        ),
        (
            'gwProductInventoryPolicy[103]="deny";'
            'gwProductInventoryQuantity[103]="6";',
            "103",
            6,
        ),
        ('{id:105,inventory_management:"shopify",quantity:12}', "105", 12),
        (
            '{"inventory":{"106":{"inventory_management":null,'
            '"inventory_policy":"deny","inventory_quantity":72}}}',
            "106",
            72,
        ),
    ),
)
def test_html_inventory_matches_recorded_theme_shape(
    document: str,
    identifier: str,
    quantity: int,
) -> None:
    found = ShopifyConnector._inventory_from_html(document, {identifier})

    assert found[identifier]["inventory_quantity"] == quantity


def test_html_inventory_preserves_first_shape_precedence() -> None:
    document = (
        '{"id":60,"inventory_quantity":6,"inventory_management":"shopify",'
        '"inventory_policy":"deny"}'
        '<option value="60" data-inventory="66" data-inventory-policy="deny">'
        'gwProductInventoryPolicy[60]="deny";'
        'gwProductInventoryQuantity[60]="666";'
    )

    found = ShopifyConnector._inventory_from_html(document, {"60"})

    assert found["60"]["inventory_quantity"] == 6


def test_html_inventory_preserves_first_option_match_semantics() -> None:
    document = (
        '<option value="60" data-inventory="6" data-inventory-policy="continue">'
        '<option value="60" data-inventory="66" data-inventory-policy="deny">'
        'gwProductInventoryPolicy[60]="deny";'
        'gwProductInventoryQuantity[60]="666";'
    )

    found = ShopifyConnector._inventory_from_html(document, {"60"})

    assert found["60"]["inventory_quantity"] == 666


def test_html_inventory_scans_each_precompiled_pattern_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class CountingPattern:
        def __init__(self, name: str, pattern: re.Pattern[str]) -> None:
            self.name = name
            self.pattern = pattern

        def finditer(self, document: str) -> Iterator[re.Match[str]]:
            calls.append(self.name)
            return self.pattern.finditer(document)

    names = (
        "_NATIVE_INVENTORY",
        "_OPTION_INVENTORY",
        "_GATEWAY_INVENTORY",
        "_LOCAL_INVENTORY",
        "_KEYED_INVENTORY",
    )
    for name in names:
        monkeypatch.setattr(
            shopify_module,
            name,
            CountingPattern(name, getattr(shopify_module, name)),
        )

    found = ShopifyConnector._inventory_from_html(
        '{"id":500,"inventory_quantity":8,"inventory_management":"shopify",'
        '"inventory_policy":"deny"}',
        {str(identifier) for identifier in range(1, 1_001)},
    )

    assert found["500"]["inventory_quantity"] == 8
    assert calls == list(names)


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


async def test_inventory_batches_run_concurrently_and_merge_in_product_order() -> None:
    class CoordinatedTransport:
        def __init__(self) -> None:
            self.active = 0
            self.maximum_active = 0
            self.first_started = asyncio.Event()
            self.second_finished = asyncio.Event()
            self.completions: list[int] = []
            self.rotations: list[RotationReason] = []

        async def request(self, request: TransportRequest) -> TransportResponse:
            identifier = int(request.url.removesuffix(".js").rsplit("-", 1)[-1])
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
            try:
                if identifier == 1:
                    self.first_started.set()
                    await self.second_finished.wait()
                elif identifier == 2:
                    await self.first_started.wait()
                    self.second_finished.set()
                self.completions.append(identifier)
                return TransportResponse(
                    status=200,
                    content=json.dumps(
                        {
                            "variants": [
                                {
                                    "id": identifier * 10,
                                    "inventory_quantity": identifier,
                                    "inventory_management": "shopify",
                                    "inventory_policy": "deny",
                                }
                            ]
                        }
                    ).encode(),
                    final_url=request.url,
                )
            finally:
                self.active -= 1

        async def rotate_identity(self, reason: RotationReason) -> None:
            assert self.active == 0
            self.rotations.append(reason)

    backend = CoordinatedTransport()
    transport = MiddlewareTransport(
        backend,
        rate_limiter=PerOriginRateLimiter(concurrency=2),
        retries=0,
    )
    connector = ShopifyConnector(
        transport,
        ShopifyOptions(
            currency="EUR", inventory_method="product_json", inventory_batch_size=2
        ),
    )
    products = [product(index) for index in range(1, 4)]

    diagnostics = await connector._enrich_inventory(
        products, "https://shop.test", "https://shop.test/products.json"
    )

    assert diagnostics == ()
    assert backend.maximum_active == 2
    assert backend.completions == [2, 1, 3]
    assert backend.rotations == [RotationReason.EXPLICIT]
    assert [
        cast(list[dict[str, object]], item["variants"])[0]["inventory_quantity"]
        for item in products
    ] == [1, 2, 3]


async def test_inventory_batch_cancellation_cancels_in_flight_requests() -> None:
    class BlockingTransport:
        def __init__(self) -> None:
            self.started = 0
            self.cancelled = 0
            self.all_started = asyncio.Event()
            self.block = asyncio.Event()

        async def request(self, request: TransportRequest) -> TransportResponse:
            self.started += 1
            if self.started == 3:
                self.all_started.set()
            try:
                await self.block.wait()
            except asyncio.CancelledError:
                self.cancelled += 1
                raise
            raise AssertionError("blocking request unexpectedly released")

        async def rotate_identity(self, reason: RotationReason) -> None:
            raise AssertionError(f"unexpected rotation: {reason}")

    transport = BlockingTransport()
    connector = ShopifyConnector(
        transport,
        ShopifyOptions(
            currency="EUR", inventory_method="product_json", inventory_batch_size=3
        ),
    )
    task = asyncio.create_task(
        connector._enrich_inventory(
            [product(index) for index in range(1, 4)],
            "https://shop.test",
            "https://shop.test/products.json",
        )
    )
    await transport.all_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert transport.cancelled == 3


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
    await assert_cancelled_without_requests(
        connector.collect(request), cancelled_transport.requests
    )

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
