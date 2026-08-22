from __future__ import annotations

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from mb_ceramics_catalogue import scrapers


class Limiter:
    def join_group(self, url, group):
        pass

    def set_delay(self, url, delay):
        pass


class FakeFetcher:
    def __init__(self, responses, on_products=None):
        self.responses = list(responses)
        self.calls = []
        self.limiter = Limiter()
        self.on_products = on_products
        self.rotations = 0
        self.proxy_lease = None
        self.stats = SimpleNamespace(proxy_requests=0)

    async def response(
        self,
        url,
        *,
        params=None,
        method="GET",
        json_body=None,
        headers=None,
    ):
        del json_body
        self.calls.append((url, params, headers))
        if url.endswith("products.json") and self.on_products is not None:
            self.on_products()
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return httpx.Response(
            200,
            json=deepcopy(value),
            request=httpx.Request(method, url, params=params),
        )

    async def json(self, url, *, params=None, headers=None):
        return (await self.response(url, params=params, headers=headers)).json()

    async def text(self, url, *, headers=None):
        return await self.json(url, headers=headers)

    async def rotate_client(self):
        self.rotations += 1

    async def render(self, url, wait_ms=1500, wait_for=None):
        del url, wait_ms, wait_for
        raise AssertionError("Shopify connector must not render")

    async def may_fetch(self, url, ignore_robots=False, obey_robots=False):
        del url, ignore_robots, obey_robots
        return True


class BlockingSecondPageFetcher(FakeFetcher):
    def __init__(self, first):
        super().__init__([first])
        self.second_started = asyncio.Event()

    async def response(
        self,
        url,
        *,
        params=None,
        method="GET",
        json_body=None,
        headers=None,
    ):
        if self.calls:
            self.calls.append((url, params, headers))
            self.second_started.set()
            await asyncio.Event().wait()
        return await super().response(
            url,
            params=params,
            method=method,
            json_body=json_body,
            headers=headers,
        )


def product(identifier: int = 10) -> dict:
    return {
        "id": identifier,
        "handle": f"glaze-{identifier}",
        "title": "Transparent Glaze",
        "body_html": '<p>A gloss glaze.</p><a href="/files/sds.pdf">SDS</a>',
        "vendor": "Test Ceramics",
        "product_type": "Glazes",
        "tags": ["ceramic", "transparent"],
        "updated_at": "2026-08-14T12:00:00Z",
        "options": [{"name": "Size"}, {"name": "Colour"}],
        "images": [
            {"id": 1, "src": "https://cdn.test/product.jpg"},
            {"id": 2, "src": "https://cdn.test/second.jpg"},
        ],
        "variants": [
            {
                "id": identifier * 10,
                "title": "500 ml / Blue",
                "option1": "500 ml",
                "option2": "Blue",
                "price": "12.50",
                "compare_at_price": "15.00",
                "available": True,
                "inventory_management": "shopify",
                "inventory_policy": "deny",
                "inventory_quantity": 7,
                "sku": f"SHOP-{identifier}",
                "barcode": "1234567890123",
                "grams": 650,
                "featured_image": {"src": "https://cdn.test/variant.jpg"},
            }
        ],
    }


def config(**updates) -> dict:
    values = {
        "label": "Shop",
        "url": "https://shop.test/path",
        "scraper": "shopify",
        "scope": "all",
        "currency": "EUR",
        "vat_status": "inclusive",
    }
    values.update(updates)
    return values


async def scrape(kind: str, responses: list, **configuration):
    fetcher = FakeFetcher(responses)
    scraper = scrapers.build(kind, "shop", config(**configuration), fetcher)
    result = await scraper.scrape()
    return result, fetcher


def stable(row: dict) -> dict:
    return {**row, "fetched_at": "<volatile>"}


@pytest.mark.asyncio
async def test_canary_is_explicit_and_preserves_sale_options_images_raw_and_stock() -> None:
    payload = {"products": [product()]}
    legacy, _ = await scrape("shopify", [payload])
    canary, _ = await scrape("shopify_connector", [payload])

    assert scrapers.load("shopify").__name__ == "ShopifyScraper"
    assert scrapers.load("shopify_connector").__name__ == "ShopifyConnectorScraper"
    assert len(legacy.records) == len(canary.records) == 1
    assert stable(canary.records[0]) == stable(legacy.records[0])
    row = canary.records[0]
    assert row["price"] == 12.5 and row["list_price"] == 15.0
    assert row["technical_attributes"] == {
        "Size": "500 ml",
        "Colour": "Blue",
        "shipping_weight_g": 650,
    }
    assert row["image_url"] == "https://cdn.test/variant.jpg"
    assert row["all_image_urls"] == [
        "https://cdn.test/product.jpg",
        "https://cdn.test/second.jpg",
    ]
    assert row["stock_quantity"] == 7
    assert row["raw"] == {
        "product": {key: value for key, value in product().items() if key != "variants"},
        "variant": product()["variants"][0],
    }


@pytest.mark.asyncio
async def test_local_library_shell_uses_shared_registry_and_projection_root() -> None:
    payload = {"products": [product()]}
    canary, _ = await scrape("shopify_connector", [payload])
    shared, fetcher = await scrape(
        "library_shopify_connector",
        [payload],
        scraper="library_shopify_connector",
    )

    assert scrapers.load("library_shopify_connector").__name__ == (
        "LibraryConnectorScraper"
    )
    assert [stable(row) for row in shared.records] == [
        stable(row) for row in canary.records
    ]
    assert shared.requests == 1
    assert fetcher.calls[0][0].endswith("products.json")


@pytest.mark.asyncio
async def test_regular_offer_and_default_title_match_legacy() -> None:
    item = product()
    item["variants"][0]["title"] = "Default Title"
    item["variants"][0]["option1"] = "Default Title"
    item["variants"][0]["compare_at_price"] = None
    payload = {"products": [item]}

    legacy, _ = await scrape("shopify", [payload])
    canary, _ = await scrape("shopify_connector", [payload])

    assert stable(canary.records[0]) == stable(legacy.records[0])
    assert canary.records[0]["variant_title"] is None
    assert canary.records[0]["list_price"] is None


@pytest.mark.asyncio
async def test_missing_currency_drops_rows_and_keeps_legacy_note_and_counts() -> None:
    payload = {"products": [product()]}
    legacy_fetcher = FakeFetcher([httpx.ReadTimeout("meta"), payload])
    canary_fetcher = FakeFetcher([httpx.ReadTimeout("meta"), payload])
    legacy_scraper = scrapers.build("shopify", "shop", config(currency=None), legacy_fetcher)
    canary_scraper = scrapers.build("shopify_connector", "shop", config(currency=None), canary_fetcher)

    legacy = await legacy_scraper.scrape()
    canary = await canary_scraper.scrape()

    assert canary.records == legacy.records == []
    assert canary.discovered == legacy.discovered == 1
    assert canary.requests == legacy.requests == 1
    assert "shop currency unavailable" in canary.notes[0]
    assert "1 variants dropped without a price" in canary.notes[-1]


@pytest.mark.asyncio
async def test_product_json_inventory_enrichment_matches_legacy() -> None:
    item = product()
    variant = item["variants"][0]
    variant.pop("inventory_quantity")
    variant.pop("inventory_management")
    variant.pop("inventory_policy")
    detail = {
        "variants": [
            {
                "id": variant["id"],
                "inventory_quantity": 11,
                "inventory_management": "shopify",
                "inventory_policy": "deny",
            }
        ]
    }
    payload = {"products": [item]}
    legacy, _ = await scrape("shopify", [payload, detail], inventory_product_json=True)
    canary, _ = await scrape("shopify_connector", [payload, detail], inventory_product_json=True)
    assert stable(canary.records[0]) == stable(legacy.records[0])
    assert canary.records[0]["stock_quantity"] == 11
    assert canary.requests == legacy.requests == 2


@pytest.mark.asyncio
async def test_pagination_counts_and_limit_truncation_match_legacy() -> None:
    first = {"products": [product(index) for index in range(1, 251)]}
    second = {"products": [product(251)]}
    legacy, legacy_fetcher = await scrape("shopify", [first, second])
    canary, canary_fetcher = await scrape("shopify_connector", [first, second])
    assert len(canary.records) == len(legacy.records) == 251
    assert canary.discovered == legacy.discovered == 251
    assert canary.requests == legacy.requests == 2
    assert [call[1]["page"] for call in canary_fetcher.calls] == [1, 2]
    assert [call[1]["page"] for call in legacy_fetcher.calls] == [1, 2]

    limited_fetcher = FakeFetcher([first])
    limited = scrapers.build("shopify_connector", "shop", config(), limited_fetcher)
    result = await limited.scrape(limit=3)
    assert len(result.records) == 3
    assert result.discovered == 250
    assert result.truncated
    assert result.requests == 1


@pytest.mark.asyncio
async def test_failure_maps_typed_diagnostic_to_legacy_error() -> None:
    fetcher = FakeFetcher([httpx.ReadTimeout("page slow")])
    scraper = scrapers.build("shopify_connector", "shop", config(), fetcher)
    result = await scraper.scrape()
    assert result.truncated
    assert result.records == []
    assert result.requests == 0
    assert result.errors and "page slow" in result.errors[0]["error"]
    assert result.errors[0]["url"].endswith("/products.json?page=1")


@pytest.mark.asyncio
async def test_cooperative_cancellation_keeps_partial_rows_and_marks_truncated() -> None:
    first = {"products": [product(index) for index in range(1, 251)]}
    holder: dict[str, Any] = {}
    fetcher = FakeFetcher([first], on_products=lambda: holder["scraper"].cancel())
    scraper = scrapers.build("shopify_connector", "shop", config(), fetcher)
    holder["scraper"] = scraper

    result = await scraper.scrape()

    assert len(result.records) == 250
    assert result.discovered == 250
    assert result.requests == 1
    assert result.truncated


@pytest.mark.asyncio
async def test_cancellation_between_collection_partitions_is_truncated() -> None:
    payload = {"products": [product()]}
    holder: dict[str, Any] = {}
    fetcher = FakeFetcher([payload], on_products=lambda: holder["scraper"].cancel())
    scraper = scrapers.build(
        "shopify_connector",
        "shop",
        config(collections=["clay", "glaze"]),
        fetcher,
    )
    holder["scraper"] = scraper
    result = await scraper.scrape()
    assert len(result.records) == 1
    assert result.truncated
    assert len(fetcher.calls) == 1


@pytest.mark.asyncio
async def test_task_cancellation_preserves_partial_result_and_propagates() -> None:
    first = {"products": [product(index) for index in range(1, 251)]}
    fetcher = BlockingSecondPageFetcher(first)
    scraper = scrapers.build("shopify_connector", "shop", config(), fetcher)
    task = asyncio.create_task(scraper.scrape())
    await fetcher.second_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(scraper.result.records) == 250
    assert scraper.result.discovered == 250
    assert scraper.result.requests == 1
    assert scraper.result.truncated
