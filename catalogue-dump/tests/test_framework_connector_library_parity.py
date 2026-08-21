"""Deterministic neutral-snapshot parity for extracted framework connectors."""

from __future__ import annotations

import html
import json
from datetime import UTC, datetime
from typing import Any

import pytest
from mb_commerce_scraper.connectors.base import ConnectorContext
from mb_commerce_scraper.connectors.bigcommerce import BigCommerceConnector, BigCommerceOptions
from mb_commerce_scraper.connectors.prestashop import PrestaShopConnector, PrestaShopOptions
from mb_commerce_scraper.connectors.shopify import ShopifyConnector, ShopifyOptions
from mb_commerce_scraper.connectors.specialized import (
    NitroSellConnector,
    NitroSellOptions,
    ShopwareConnector,
    ShopwareOptions,
    StarwebConnector,
    StarwebOptions,
    SumUpConnector,
    SumUpOptions,
)
from mb_commerce_scraper.connectors.wix import WixConnector, WixOptions
from mb_commerce_scraper.testing import FakeTransport

from mb_ceramics_catalogue.connectors.bigcommerce import (
    BigCommerceConnector as LegacyBigCommerceConnector,
)
from mb_ceramics_catalogue.connectors.bigcommerce import (
    BigCommerceOptions as LegacyBigCommerceOptions,
)
from mb_ceramics_catalogue.connectors.prestashop import (
    PrestaShopConnector as LegacyPrestaShopConnector,
)
from mb_ceramics_catalogue.connectors.prestashop import (
    PrestaShopOptions as LegacyPrestaShopOptions,
)
from mb_ceramics_catalogue.connectors.shopify import ShopifyConnector as LegacyShopifyConnector
from mb_ceramics_catalogue.connectors.shopify import ShopifyOptions as LegacyShopifyOptions
from mb_ceramics_catalogue.connectors.specialized import (
    NitroSellConnector as LegacyNitroSellConnector,
)
from mb_ceramics_catalogue.connectors.specialized import (
    NitroSellOptions as LegacyNitroSellOptions,
)
from mb_ceramics_catalogue.connectors.specialized import (
    ShopwareConnector as LegacyShopwareConnector,
)
from mb_ceramics_catalogue.connectors.specialized import (
    ShopwareOptions as LegacyShopwareOptions,
)
from mb_ceramics_catalogue.connectors.specialized import (
    StarwebConnector as LegacyStarwebConnector,
)
from mb_ceramics_catalogue.connectors.specialized import (
    StarwebOptions as LegacyStarwebOptions,
)
from mb_ceramics_catalogue.connectors.specialized import (
    SumUpConnector as LegacySumUpConnector,
)
from mb_ceramics_catalogue.connectors.specialized import (
    SumUpOptions as LegacySumUpOptions,
)
from mb_ceramics_catalogue.connectors.wix import WixConnector as LegacyWixConnector
from mb_ceramics_catalogue.connectors.wix import WixOptions as LegacyWixOptions

NOW = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)


class LegacyTransport:
    """No-I/O legacy transport for parser-level parity fixtures."""

    async def allowed(self, url: str) -> bool:
        del url
        return True

    async def advertised_sitemaps(self, base_url: str) -> tuple[str, ...]:
        del base_url
        return ()

    async def document(self, url: str, *, rendered: bool = False, accept: str | None = None) -> str:
        del url, rendered, accept
        raise AssertionError("parser-level parity must not perform I/O")

    async def request_json(
        self,
        url: str,
        *,
        headers: dict[str, str],
        body: dict[str, Any],
        browser_context_url: str | None = None,
    ) -> Any:
        del url, headers, body, browser_context_url
        raise AssertionError("parser-level parity must not perform I/O")

    async def json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        del url, params, headers
        raise AssertionError("parser-level parity must not perform I/O")

    async def text(self, url: str, *, headers: dict[str, str] | None = None) -> str:
        del url, headers
        raise AssertionError("parser-level parity must not perform I/O")

    async def rotate_client(self) -> None:
        raise AssertionError("parser-level parity must not perform I/O")


def _commerce_data(snapshot: Any) -> Any:
    """Remove observation time only, retaining every commerce field and extension."""

    def normalize(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: normalize(child) for key, child in value.items() if key != "observed_at"}
        if isinstance(value, list):
            return [normalize(child) for child in value]
        return value

    return normalize(snapshot.model_dump(mode="json"))


def _assert_snapshot_parity(legacy: Any, extracted: Any) -> None:
    assert legacy is not None
    assert extracted is not None
    assert _commerce_data(extracted) == _commerce_data(legacy)


def _presta_details() -> dict[str, Any]:
    return {
        "id_product": 12,
        "id_product_attribute": 0,
        "name": "Stoneware &amp; Glaze",
        "link": "https://shop.test/12-stoneware-glaze.html",
        "description_short": "A durable glaze.",
        "manufacturer_name": "Test Ceramics",
        "reference": "GL-12",
        "ean13": "1234567890123",
        "price": "12,50 EUR",
        "regular_price": "15,00 EUR",
        "quantity": 4,
        "category_name": "Glazes",
        "features": [{"name": "Firing", "value": "Cone 6"}],
        "images": [{"large": {"url": "https://cdn.test/glaze.jpg"}}],
        "attachments": [{"id_attachment": 7, "name": "Safety data sheet"}],
    }


def test_shopify_neutral_snapshot_matches_legacy() -> None:
    product = {
        "id": 42,
        "handle": "stoneware-glaze",
        "title": "Stoneware Glaze",
        "body_html": '<p>Durable.</p><a href="/docs/sds.pdf">SDS</a>',
        "vendor": "Test Ceramics",
        "product_type": "Glazes",
        "updated_at": "2026-08-20T10:00:00Z",
        "options": [{"name": "Size"}],
        "images": [{"id": 7, "src": "https://cdn.test/glaze.jpg", "alt": "Glaze"}],
        "variants": [
            {
                "id": 420,
                "title": "500 ml",
                "sku": "GL-500",
                "barcode": "1234567890123",
                "price": "12.50",
                "compare_at_price": "15.00",
                "available": True,
                "inventory_quantity": 4,
                "inventory_management": "shopify",
                "inventory_policy": "deny",
                "option1": "500 ml",
                "grams": 700,
            }
        ],
    }
    legacy = LegacyShopifyConnector(
        LegacyTransport(),
        LegacyShopifyOptions(vat_status="inclusive"),
        clock=lambda: NOW,
    )
    extracted = ShopifyConnector(
        FakeTransport(),
        ShopifyOptions(vat_status="inclusive"),
        ConnectorContext(clock=lambda: NOW),
    )

    _assert_snapshot_parity(
        legacy._normalize(product, "shop", "https://shop.test", "EUR", NOW, "main"),
        extracted._normalize(product, "shop", "https://shop.test", "EUR", NOW, "main"),
    )


def test_prestashop_neutral_snapshot_matches_legacy() -> None:
    details = _presta_details()
    document = (
        f'<div id="product-details" data-product="{html.escape(json.dumps(details), quote=True)}"></div>'
        '<a href="/index.php?id_attachment=7">Safety data sheet</a>'
        "<table><tr><th>Application</th><td>Brush</td></tr></table>"
    )
    url = "https://shop.test/12-stoneware-glaze.html"
    legacy = LegacyPrestaShopConnector(
        LegacyTransport(),
        LegacyPrestaShopOptions(variant_combinations=False),
        clock=lambda: NOW,
    )
    extracted = PrestaShopConnector(
        FakeTransport(),
        PrestaShopOptions(variant_combinations=False),
        ConnectorContext(clock=lambda: NOW),
    )

    _assert_snapshot_parity(
        legacy._normalize([details], document, url, "shop", "html"),
        extracted._normalize([details], document, url, "shop", "html"),
    )


def _bigcommerce_product() -> dict[str, Any]:
    return {
        "entityId": 42,
        "name": "Stoneware Glaze",
        "path": "/stoneware-glaze/",
        "sku": "GL-42",
        "description": "A durable glaze.",
        "brand": {"name": "Test Ceramics"},
        "availabilityV2": {"status": "Available"},
        "defaultImage": {"urlOriginal": "https://cdn.test/glaze.jpg"},
        "images": {"edges": []},
        "prices": {
            "price": {"value": "12.50", "currencyCode": "EUR"},
            "retailPrice": {"value": "15.00"},
        },
        "categories": {"edges": [{"node": {"name": "Glazes"}}]},
        "customFields": {"edges": [{"node": {"name": "SDS", "value": "/docs/sds.pdf"}}]},
        "variants": {"edges": []},
    }


def test_bigcommerce_neutral_snapshot_matches_legacy() -> None:
    product = _bigcommerce_product()
    legacy = LegacyBigCommerceConnector(
        LegacyTransport(), LegacyBigCommerceOptions(vat_status="inclusive"), clock=lambda: NOW
    )
    extracted = BigCommerceConnector(
        FakeTransport(),
        BigCommerceOptions(vat_status="inclusive"),
        ConnectorContext(clock=lambda: NOW),
    )

    _assert_snapshot_parity(
        legacy._normalize(product, "shop", "https://shop.test", NOW),
        extracted._normalize(product, "shop", "https://shop.test", NOW),
    )


def _wix_page() -> str:
    product = {
        "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "name": "Transparent <b>Glaze</b>",
        "description": "A glossy &amp; durable glaze.",
        "brand": "Test Ceramics",
        "sku": "GL-500",
        "price": 11,
        "comparePrice": 14,
        "formattedPrice": "€11.00",
        "isInStock": True,
        "isTrackingInventory": True,
        "inventory": {"quantity": 3},
        "media": [{"fullUrl": "https://cdn.test/glaze.jpg"}],
    }
    jsonld = {
        "@type": "Product",
        "image": ["https://cdn.test/published.jpg"],
    }
    return (
        f'<script type="application/ld+json">{json.dumps(jsonld)}</script>'
        f'<script>window.warmup={{"glaze":{{"product":{json.dumps(product)}}}}};</script>'
        '<script>const locale={"currency":"EUR"};</script>'
        '<a href="/docs/sds.pdf">Safety data sheet</a>'
    )


def test_wix_neutral_snapshot_matches_legacy() -> None:
    document = _wix_page()
    url = "https://shop.test/product-page/glaze"
    legacy = LegacyWixConnector(
        LegacyTransport(), LegacyWixOptions(vat_status="inclusive"), clock=lambda: NOW
    )
    extracted = WixConnector(
        FakeTransport(),
        WixOptions(vat_status="inclusive"),
        ConnectorContext(clock=lambda: NOW),
    )

    _assert_snapshot_parity(
        legacy._normalize(document, url, "shop", NOW),
        extracted._normalize(document, url, "shop"),
    )


JSONLD = """<html><script type="application/ld+json">{
 "@type":"Product","name":"Clay","sku":"CL-1","image":"https://shop.test/a.jpg",
 "offers":{"price":"12.50","priceCurrency":"EUR","availability":"InStock"}
}</script></html>"""


def _shopware_page() -> str:
    return JSONLD.replace(
        "</html>",
        '<dl><dt class="properties-label">Firing:</dt><dd class="properties-value">1200 C</dd></dl>'
        '<span class="product-detail-ordernumber">CL-99</span>'
        '<span class="product-detail-price-unit">2.50 / kg</span>'
        '<input class="quantity-selector" name="quantity" max="7"></html>',
    )


def _starweb_page() -> str:
    return JSONLD.replace("<html>", '<html class="incl-vat">').replace(
        "</html>",
        '<label class="variant-name">Size:</label><span class="variant-value">1 kg</span></html>',
    )


NITROSELL_PAGE = """<html><head>
<meta property="og:title" content="Blue glaze"><meta property="og:upc" content="BG-1">
<meta property="product:price:amount" content="14.25">
<meta property="product:price:currency" content="USD">
<meta property="og:availability" content="instock"></head>
<span class="text-pricestrike">$18.00</span>
<ol class="breadcrumb"><li>Home</li><li>Glazes</li><li>Blue glaze</li></ol>
<div class="product-description">Long blue description</div>
<img src="https://cdn.powered-by-nitrosell.com/product_images/blue-large.jpg"></html>"""


def _sumup_page() -> str:
    product = {
        "id": "ebec5308-b80d-4c0c-ae84-26ce26a341b4",
        "name": "Tasse bleue",
        "slug": "tasse-bleue",
        "price": 2500,
        "basePrice": 3000,
        "hasDiscount": True,
        "isAvailable": True,
        "image": "https://images.test/one.jpg",
        "allImages": ["https://images.test/one.jpg"],
        "category": {"name": "Ceramiques"},
        "variants": {
            "1f7ac7e9-8bb0-4998-b88f-077b7a249862": {
                "uuid": "1f7ac7e9-8bb0-4998-b88f-077b7a249862",
                "price": 2500,
                "quantity": 3,
                "isAvailable": True,
                "isTrackingEnabled": True,
            }
        },
    }
    payload = '{"currency":"EUR","product":' + json.dumps(product, separators=(",", ":")) + "}"
    return f"<script>self.__next_f.push([1,{json.dumps(payload)}])</script>"


@pytest.mark.parametrize(
    ("legacy_type", "legacy_options", "extracted_type", "extracted_options", "document", "url"),
    [
        (
            LegacyShopwareConnector,
            LegacyShopwareOptions(currency="EUR"),
            ShopwareConnector,
            ShopwareOptions(currency="EUR"),
            _shopware_page(),
            "https://shop.test/a",
        ),
        (
            LegacyStarwebConnector,
            LegacyStarwebOptions(currency="EUR"),
            StarwebConnector,
            StarwebOptions(currency="EUR"),
            _starweb_page(),
            "https://shop.test/a",
        ),
        (
            LegacyNitroSellConnector,
            LegacyNitroSellOptions(vat_status="exclusive"),
            NitroSellConnector,
            NitroSellOptions(vat_status="exclusive"),
            NITROSELL_PAGE,
            "https://shop.test/product/blue",
        ),
        (
            LegacySumUpConnector,
            LegacySumUpOptions(),
            SumUpConnector,
            SumUpOptions(),
            _sumup_page(),
            "https://shop.test/article/tasse-bleue",
        ),
    ],
    ids=("shopware", "starweb", "nitrosell", "sumup"),
)
def test_specialized_neutral_snapshot_matches_legacy(
    legacy_type: Any,
    legacy_options: Any,
    extracted_type: Any,
    extracted_options: Any,
    document: str,
    url: str,
) -> None:
    legacy_connector = legacy_type(LegacyTransport(), legacy_options, clock=lambda: NOW)
    extracted_connector = extracted_type(
        FakeTransport(), extracted_options, ConnectorContext(clock=lambda: NOW)
    )
    legacy_outcome = legacy_connector.parse(document, url, "shop", NOW)
    extracted_snapshots = extracted_connector.parse(document, url, "shop")

    assert len(legacy_outcome.snapshots) == len(extracted_snapshots) == 1
    _assert_snapshot_parity(legacy_outcome.snapshots[0], extracted_snapshots[0])
