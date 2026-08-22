from __future__ import annotations

import html
import json
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from mb_commerce_scraper import CollectionRequest
from mb_commerce_scraper.connectors import (
    ConnectorContext,
    PrestaShopConnector,
    PrestaShopOptions,
    prestashop_partition_keys,
)
from mb_commerce_scraper.proxy import ProxyBudgetExhausted
from mb_commerce_scraper.testing import FakeTransport, assert_connector_pages
from mb_commerce_scraper.transports import (
    BrowserHint,
    BudgetExhausted,
    RequestPurpose,
    RobotsDenied,
    TransportFailure,
    TransportRequest,
    TransportResponse,
)

NOW = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
ORIGIN = "https://shop.test"
SITEMAP = f"{ORIGIN}/product-sitemap.xml"
PRODUCT = f"{ORIGIN}/12-stoneware-glaze.html"


class RaisingTransport(FakeTransport):
    def __init__(self, error: RuntimeError) -> None:
        super().__init__()
        self.error = error

    async def request(self, request: TransportRequest) -> TransportResponse:
        self.requests.append(request)
        raise self.error


class ProductTransportFailureOnce(FakeTransport):
    def __init__(self, target: str) -> None:
        super().__init__()
        self.target = target
        self.failed = False

    async def request(self, request: TransportRequest) -> TransportResponse:
        if request.url == self.target and not self.failed:
            self.requests.append(request)
            self.failed = True
            raise TransportFailure("connection reset")
        return await super().request(request)


def details(*, attribute: int = 0, price: str = "12,50 EUR") -> dict[str, Any]:
    return {
        "id_product": 12,
        "id_product_attribute": attribute,
        "name": "Stoneware &amp; Glaze",
        "link": PRODUCT,
        "description_short": "A durable glaze.",
        "manufacturer_name": "Test Ceramics",
        "reference": "GL-12" if not attribute else f"GL-12-{attribute}",
        "ean13": "1234567890123",
        "price": price,
        "regular_price": "15,00 EUR",
        "quantity": 4,
        "category_name": "Glazes",
        "features": [{"name": "Firing", "value": "Cone 6"}],
        "images": [{"large": {"url": "https://cdn.test/glaze.jpg"}}],
        "attachments": [{"id_attachment": 7, "name": "Safety data sheet"}],
    }


def page(value: dict[str, Any], *, combinations: bool = False) -> str:
    groups = (
        '<ul id="group_1"><li><input value="1" checked></li>'
        '<li><input value="2"></li></ul>'
        if combinations
        else ""
    )
    encoded = html.escape(json.dumps(value), quote=True)
    return (
        f'<html><div id="product-details" data-product="{encoded}"></div>{groups}'
        '<a href="/index.php?id_attachment=7">Safety data sheet</a>'
        '<table><tr><th>Application</th><td>Brush</td></tr></table></html>'
    )


def request(**values: Any) -> CollectionRequest:
    return CollectionRequest(source_id="presta-shop", base_url=f"{ORIGIN}/catalogue", **values)


async def test_sitemap_product_preserves_neutral_snapshot() -> None:
    transport = FakeTransport()
    transport.add(SITEMAP, body=f"<urlset><url><loc>{PRODUCT}</loc></url></urlset>")
    transport.add(PRODUCT, body=page(details()))
    connector = PrestaShopConnector(
        transport,
        PrestaShopOptions(sitemaps=(SITEMAP,), variant_combinations=False),
        ConnectorContext(clock=lambda: NOW),
    )

    intent = request()
    pages = await assert_connector_pages(
        connector.collect(intent), connector=connector, request=intent
    )

    snapshot = pages[0].items[0]
    assert snapshot.connector == "prestashop"
    assert snapshot.title == "Stoneware & Glaze"
    assert snapshot.vendor == "Test Ceramics"
    assert snapshot.documents[0].url == f"{ORIGIN}/index.php?id_attachment=7"
    assert snapshot.published_attributes == {"Firing": "Cone 6", "Application": "Brush"}
    assert snapshot.variants[0].offers[0].price.amount == 12.50
    assert snapshot.variants[0].stock is not None
    assert snapshot.variants[0].stock.quantity == 4


async def test_variant_combinations_use_prestashop_refresh_endpoint() -> None:
    transport = FakeTransport()
    transport.add(SITEMAP, body=f"<urlset><url><loc>{PRODUCT}</loc></url></urlset>")
    transport.add(PRODUCT, body=page(details(), combinations=True))
    refresh = f"{PRODUCT}?group%5B1%5D=2&ajax=1&action=refresh&quantity_wanted=1"
    response = {
        "product_details": (
            f'<div id="product-details" data-product="'
            f'{html.escape(json.dumps(details(attribute=2, price="13,50 EUR")), quote=True)}"></div>'
        ),
        "product_url": PRODUCT,
    }
    transport.add(refresh, json_body=response)
    connector = PrestaShopConnector(
        transport,
        PrestaShopOptions(sitemaps=(SITEMAP,)),
        ConnectorContext(clock=lambda: NOW),
    )

    pages = await assert_connector_pages(connector.collect(request()))

    assert [variant.external_id for variant in pages[0].items[0].variants] == ["default", "2"]
    assert transport.requests[-1].url == refresh


async def test_result_limit_checkpoint_is_option_fingerprinted() -> None:
    second = f"{ORIGIN}/13-second.html"
    transport = FakeTransport()
    transport.add(
        SITEMAP,
        body=f"<urlset><url><loc>{PRODUCT}</loc></url><url><loc>{second}</loc></url></urlset>",
    )
    transport.add(PRODUCT, body=page(details()))
    connector = PrestaShopConnector(
        transport, PrestaShopOptions(sitemaps=(SITEMAP,), variant_combinations=False)
    )
    intent = request(result_limit=1)

    result = await anext(connector.collect(intent))
    checkpoint = connector.checkpoint(intent, "lineage", result.resume_after)

    assert not result.enumeration_intact
    changed = PrestaShopConnector(
        FakeTransport(),
        PrestaShopOptions(sitemaps=(SITEMAP,), page_limit=10),
    )
    with pytest.raises(ValueError, match="CHECKPOINT_INVALID"):
        await anext(changed.collect(intent, checkpoint))


async def test_sio2_category_projection_and_cancellation() -> None:
    options = PrestaShopOptions(
        category_urls=(f"{ORIGIN}/gb/14-glazes",),
        use_advertised_sitemaps=False,
        product_pattern=r"/\d+-[^/]+\.html$",
        card_links_only=True,
        pagination_patterns=(r"[?&]page=\d+",),
        render=False,
        variant_combinations=False,
        currency="GBP",
    )
    assert prestashop_partition_keys(options, ORIGIN)[0].startswith("category:0:")
    with pytest.raises(ValidationError):
        PrestaShopOptions.model_validate({"category_urls": [], "unknown": True})

    transport = FakeTransport()
    connector = PrestaShopConnector(
        transport, options, ConnectorContext(cancelled=lambda: True)
    )
    assert [value async for value in connector.collect(request())] == []
    assert transport.requests == []


async def test_advertised_sitemap_uses_robots_request_purpose() -> None:
    transport = FakeTransport()
    transport.add(
        f"{ORIGIN}/robots.txt",
        body=f"User-agent: *\nSitemap: {SITEMAP}\n",
    )
    transport.add(SITEMAP, body="<urlset></urlset>")
    transport.add(f"{ORIGIN}/catalogue", body="<html></html>")

    pages = await assert_connector_pages(
        PrestaShopConnector(transport).collect(request())
    )

    assert pages[-1].terminal
    assert transport.requests[0].purpose == RequestPurpose.ROBOTS


@pytest.mark.parametrize(
    "error",
    [
        BudgetExhausted("request budget exhausted"),
        RobotsDenied("robots denied"),
        ProxyBudgetExhausted("proxy budget exhausted"),
    ],
)
async def test_policy_failures_propagate_without_browser_retry(error: RuntimeError) -> None:
    transport = RaisingTransport(error)
    connector = PrestaShopConnector(
        transport,
        PrestaShopOptions(sitemaps=(SITEMAP,)),
    )

    with pytest.raises(type(error)):
        await anext(connector.collect(request()))

    assert len(transport.requests) == 1


async def test_only_transport_failure_uses_browser_retry() -> None:
    transport = ProductTransportFailureOnce(PRODUCT)
    transport.add(SITEMAP, body=f"<urlset><url><loc>{PRODUCT}</loc></url></urlset>")
    transport.add(PRODUCT, body=page(details()))
    connector = PrestaShopConnector(
        transport,
        PrestaShopOptions(sitemaps=(SITEMAP,), variant_combinations=False),
    )

    pages = await assert_connector_pages(connector.collect(request()))

    assert pages[0].items
    product_requests = [item for item in transport.requests if item.url == PRODUCT]
    assert [item.browser for item in product_requests] == [
        BrowserHint.NEVER,
        BrowserHint.REQUIRED,
    ]


async def test_checkpoint_offset_must_reference_a_discovered_product() -> None:
    transport = FakeTransport()
    transport.add(SITEMAP, body=f"<urlset><url><loc>{PRODUCT}</loc></url></urlset>")
    connector = PrestaShopConnector(
        transport,
        PrestaShopOptions(sitemaps=(SITEMAP,), variant_combinations=False),
    )
    intent = request()
    checkpoint = connector.checkpoint(
        intent,
        "lineage",
        {
            "partition": prestashop_partition_keys(connector.options, ORIGIN)[0],
            "offset": 1,
            "sequence": 1,
        },
    )

    with pytest.raises(ValueError, match="CHECKPOINT_INVALID"):
        await anext(connector.collect(intent, checkpoint))
