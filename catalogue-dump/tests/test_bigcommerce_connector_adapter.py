from __future__ import annotations

import base64
import json

import httpx
import pytest

from mb_ceramics_catalogue import scrapers

from .test_bigcommerce_connector import node, payload


def token(origin: str) -> str:
    claims = base64.urlsafe_b64encode(json.dumps({"cors": [origin]}).encode()).decode().rstrip("=")
    return f"{'a' * 20}.{claims}.{'b' * 24}"


class Limiter:
    def join_group(self, url, group):
        pass

    def set_delay(self, url, delay):
        pass


class Fetcher:
    def __init__(self, graph):
        self.graph = graph
        self.limiter = Limiter()
        self.calls = []

    async def text(self, url, *, browser_user_agent=False):
        self.calls.append(("text", url))
        return f'window.storefront_token = "{token("https://shop.test")}"'

    async def render(self, url, *, wait_ms=0):
        raise AssertionError("rendering should not be needed")

    async def response(self, url, **kwargs):
        self.calls.append(("response", url, kwargs))
        return httpx.Response(200, json=self.graph, request=httpx.Request("POST", url))

    async def request_json_in_browser(self, *args, **kwargs):
        raise AssertionError("browser GraphQL should not be needed")


def config(kind: str) -> dict:
    return {
        "url": "https://shop.test/",
        "category_url": "https://shop.test/glazes/",
        "scraper": kind,
        "scope": "all",
        "vat_status": "exclusive",
    }


def stable(row: dict) -> dict:
    return {**row, "fetched_at": "<volatile>"}


@pytest.mark.asyncio
async def test_bigcommerce_canary_is_explicit_and_matches_legacy_variant_row() -> None:
    graph = payload([node()])
    legacy_fetcher = Fetcher(graph)
    canary_fetcher = Fetcher(graph)
    legacy = scrapers.build("bigcommerce", "shop", config("bigcommerce"), legacy_fetcher)
    canary = scrapers.build("bigcommerce_connector", "shop", config("bigcommerce_connector"), canary_fetcher)

    legacy_result = await legacy.scrape()
    canary_result = await canary.scrape()

    assert scrapers.load("bigcommerce").__name__ == "BigCommerceScraper"
    assert scrapers.load("bigcommerce_connector").__name__ == "BigCommerceConnectorScraper"
    assert len(legacy_result.records) == len(canary_result.records) == 1
    assert stable(canary_result.records[0]) == stable(legacy_result.records[0])
    assert canary_result.discovered == legacy_result.discovered == 1
    assert canary_result.requests == legacy_result.requests == 2


@pytest.mark.asyncio
async def test_bigcommerce_canary_matches_legacy_product_without_variants() -> None:
    product = node()
    product["variants"] = {"edges": []}
    graph = payload([product])
    legacy = scrapers.build("bigcommerce", "shop", config("bigcommerce"), Fetcher(graph))
    canary = scrapers.build(
        "bigcommerce_connector",
        "shop",
        config("bigcommerce_connector"),
        Fetcher(graph),
    )

    legacy_result = await legacy.scrape()
    canary_result = await canary.scrape()

    assert len(legacy_result.records) == len(canary_result.records) == 1
    assert stable(canary_result.records[0]) == stable(legacy_result.records[0])


@pytest.mark.asyncio
async def test_bigcommerce_canary_preserves_legacy_configured_brand_basis() -> None:
    product = node()
    product["brand"] = None
    graph = payload([product])
    legacy_config = {**config("bigcommerce"), "brand": "Source Brand"}
    canary_config = {**config("bigcommerce_connector"), "brand": "Source Brand"}
    legacy = scrapers.build("bigcommerce", "shop", legacy_config, Fetcher(graph))
    canary = scrapers.build(
        "bigcommerce_connector",
        "shop",
        canary_config,
        Fetcher(graph),
    )

    legacy_result = await legacy.scrape()
    canary_result = await canary.scrape()

    assert legacy_result.records[0]["brand_basis"] == "published"
    assert stable(canary_result.records[0]) == stable(legacy_result.records[0])
