from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import pytest
from pydantic import JsonValue

from mb_commerce_scraper import CollectionRequest
from mb_commerce_scraper.connectors.base import ConnectorContext
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
from mb_commerce_scraper.connectors.specialized_parsing import VerifiedDomRules
from mb_commerce_scraper.models import SnapshotField
from mb_commerce_scraper.testing import FakeTransport, assert_connector_pages
from mb_commerce_scraper.transports import BrowserHint

NOW = datetime(2026, 8, 15, tzinfo=UTC)
JSONLD = """<html><script type="application/ld+json">{
 "@type":"Product","name":"Clay","sku":"CL-1","image":"https://shop.test/a.jpg",
 "offers":{"price":"12.50","priceCurrency":"EUR","availability":"InStock"}
}</script></html>"""


def _request(*, limit: int | None = None) -> CollectionRequest:
    return CollectionRequest(
        source_id="shop",
        base_url="https://shop.test/",
        requested_fields=frozenset(SnapshotField),
        result_limit=limit,
    )


def _context(*, cancelled: bool = False) -> ConnectorContext:
    return ConnectorContext(clock=lambda: NOW, cancelled=lambda: cancelled)


def test_shopware_parser_preserves_platform_fields() -> None:
    document = JSONLD.replace(
        "</html>",
        """
        <dl><dt class="properties-label">Firing:</dt>
        <dd class="properties-value">1200 C</dd></dl>
        <span class="product-detail-ordernumber">CL-99</span>
        <span class="product-detail-price-unit">2.50 / kg</span>
        <input class="quantity-selector" name="quantity" max="7"></html>""",
    )
    connector = ShopwareConnector(
        FakeTransport(), ShopwareOptions(currency="EUR"), _context()
    )

    snapshot = connector.parse(document, "https://shop.test/a", "shop")[0]
    variant = snapshot.variants[0]
    assert snapshot.connector == "shopware"
    assert variant.sku == "CL-1"
    assert variant.published_attributes == {
        "price_text": "12.50 EUR",
        "Firing": "1200 C",
        "published_unit_price": "2.50 / kg",
    }
    assert variant.stock is not None
    assert variant.stock.quantity == 7
    assert variant.stock.quantity_kind == "exact"


def test_starweb_parser_preserves_vat_and_variant_markup() -> None:
    document = JSONLD.replace("<html>", '<html class="incl-vat">').replace(
        "</html>",
        '<label class="variant-name">Size:</label>'
        '<span class="variant-value">1 kg</span></html>',
    )
    connector = StarwebConnector(FakeTransport(), StarwebOptions(), _context())

    variant = connector.parse(document, "https://shop.test/a", "shop")[0].variants[0]
    assert variant.offers[0].vat_status == "inclusive"
    assert variant.published_attributes == {
        "price_text": "12.50 EUR", "Size": "1 kg", "vat_basis": "page_markup",
    }


NITRO = """<html><head>
<meta property="og:title" content="Blue glaze"><meta property="og:upc" content="BG-1">
<meta property="product:price:amount" content="14.25">
<meta property="product:price:currency" content="USD">
<meta property="og:availability" content="instock">
</head>
<span class="text-pricestrike">$18.00</span>
<ol class="breadcrumb"><li>Home</li><li>Glazes</li><li>Blue glaze</li></ol>
<div class="product-description">Long blue description</div>
<img src="https://cdn.powered-by-nitrosell.com/product_images/blue-large.jpg"></html>"""


def test_nitrosell_parser_preserves_opengraph_and_extended_fields() -> None:
    connector = NitroSellConnector(
        FakeTransport(), NitroSellOptions(vat_status="exclusive"), _context()
    )

    snapshot = connector.parse(NITRO, "https://shop.test/product/blue", "shop")[0]
    variant = snapshot.variants[0]
    assert snapshot.connector == "nitrosell"
    assert snapshot.description == "Long blue description"
    assert [category.name for category in snapshot.categories] == ["Glazes"]
    assert [image.url for image in snapshot.images] == [
        "https://cdn.powered-by-nitrosell.com/product_images/blue-large.jpg"
    ]
    assert variant.sku == "BG-1"
    assert [(offer.role, offer.price.amount) for offer in variant.offers] == [
        ("sale", Decimal("14.25")),
        ("regular", Decimal("18")),
    ]
    assert variant.offers[0].vat_status == "exclusive"


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
    payload = '{"currency":"EUR","product":' + json.dumps(
        product, separators=(",", ":")
    ) + "}"
    return f"<script>self.__next_f.push([1,{json.dumps(payload)}])</script>"


def test_sumup_parser_preserves_minor_units_variants_and_exact_stock() -> None:
    connector = SumUpConnector(FakeTransport(), SumUpOptions(), _context())

    snapshot = connector.parse(
        _sumup_page(), "https://shop.test/article/tasse-bleue", "shop"
    )[0]
    variant = snapshot.variants[0]
    assert snapshot.connector == "sumup"
    assert snapshot.categories[0].name == "Ceramiques"
    assert variant.offers[0].price.amount == 25
    assert variant.offers[1].price.amount == 30
    assert variant.stock is not None
    assert variant.stock.quantity == 3
    assert variant.stock.quantity_kind == "exact"


@pytest.mark.parametrize(
    ("connector_type", "options_type"),
    [
        (ShopwareConnector, ShopwareOptions),
        (StarwebConnector, StarwebOptions),
    ],
)
async def test_collection_is_ordered_bounded_and_checkpoint_resumable(
    connector_type: Any,
    options_type: Any,
) -> None:
    transport = FakeTransport()
    sitemap = (
        "<urlset><url><loc>https://shop.test/a</loc></url>"
        "<url><loc>https://shop.test/b</loc></url></urlset>"
    )
    transport.add("https://shop.test/s.xml", body=sitemap)
    transport.add("https://shop.test/a", body=JSONLD)
    transport.add("https://shop.test/b", body=JSONLD)
    options = options_type(sitemaps=("/s.xml",), product_pattern=r"/[ab]$")
    connector = connector_type(transport, options, _context())

    pages = await assert_connector_pages(connector.collect(_request()))
    assert [page.sequence for page in pages] == [0, 1]
    assert pages[0].resume_after == {
        "index": 1, "url": "https://shop.test/b",
        "snapshot_offset": 0, "sequence": 1,
    }

    resume_transport = FakeTransport()
    resume_transport.add("https://shop.test/s.xml", body=sitemap)
    resume_transport.add("https://shop.test/b", body=JSONLD)
    resumed_connector = connector_type(resume_transport, options, _context())
    checkpoint = resumed_connector.checkpoint(
        _request(), "lineage-1", pages[0].resume_after
    )
    resumed = tuple([page async for page in resumed_connector.collect(_request(), checkpoint)])
    assert resumed[-1].terminal
    assert resumed[0].sequence == 1
    assert resumed[0].items[0].canonical_url == "https://shop.test/b"


async def test_result_limit_is_typed_and_does_not_fetch_next_product() -> None:
    transport = FakeTransport()
    transport.add(
        "https://shop.test/s.xml",
        body=(
            "<urlset><url><loc>https://shop.test/a</loc></url>"
            "<url><loc>https://shop.test/b</loc></url></urlset>"
        ),
    )
    transport.add("https://shop.test/a", body=NITRO)
    connector = NitroSellConnector(
        transport,
        NitroSellOptions(sitemaps=("/s.xml",), product_pattern=r"/[ab]$"),
        _context(),
    )

    pages = await assert_connector_pages(connector.collect(_request(limit=1)))
    assert len(pages) == 1
    assert not pages[0].enumeration_intact
    assert pages[0].diagnostics[0].code == "result_limit_reached"
    assert [request.url for request in transport.requests] == [
        "https://shop.test/s.xml",
        "https://shop.test/a",
    ]


async def test_cancellation_stops_before_entity_fetch() -> None:
    transport = FakeTransport()
    transport.add(
        "https://shop.test/s.xml",
        body="<urlset><url><loc>https://shop.test/a</loc></url></urlset>",
    )
    connector = ShopwareConnector(
        transport,
        ShopwareOptions(sitemaps=("/s.xml",)),
        _context(cancelled=True),
    )

    assert [page async for page in connector.collect(_request())] == []
    assert transport.requests == []


async def test_category_discovery_preserves_path_patterns_cards_and_pagination() -> None:
    transport = FakeTransport()
    transport.add(
        "https://shop.test/clay/",
        body="""
        <div class="product-card"><a href="/Clay/123-red">Red</a></div>
        <a class="pagination next" href="/clay/?page=2">Next</a>
        <a href="/not-a-product">Noise</a>""",
    )
    transport.add(
        "https://shop.test/clay/?page=2",
        body='<div class="product-card"><a href="/Clay/456-blue">Blue</a></div>',
    )
    transport.add("https://shop.test/Clay/123-red", body=JSONLD)
    transport.add("https://shop.test/Clay/456-blue", body=JSONLD)
    connector = ShopwareConnector(
        transport,
        ShopwareOptions(
            use_advertised_sitemaps=False,
            category_urls=("/clay/",),
            product_pattern=r"^/Clay/\d+-",
            pagination_patterns=(r"[?&]page=",),
            card_links_only=True,
        ),
        _context(),
    )

    pages = await assert_connector_pages(connector.collect(_request()))
    assert [item.canonical_url for page in pages for item in page.items] == [
        "https://shop.test/Clay/123-red",
        "https://shop.test/Clay/456-blue",
    ]
    assert [request.url for request in transport.requests] == [
        "https://shop.test/clay/",
        "https://shop.test/clay/?page=2",
        "https://shop.test/Clay/123-red",
        "https://shop.test/Clay/456-blue",
    ]


async def test_checkpoint_fingerprint_rejects_option_drift() -> None:
    connector = SumUpConnector(
        FakeTransport(), SumUpOptions(sitemaps=("/products.xml",)), _context()
    )
    checkpoint = connector.checkpoint(
        _request(), "lineage-1", {"after_url": "https://shop.test/a"}
    )
    changed = SumUpConnector(
        FakeTransport(), SumUpOptions(sitemaps=("/changed.xml",)), _context()
    )

    with pytest.raises(ValueError, match="CHECKPOINT_INVALID"):
        await anext(changed.collect(_request(), checkpoint))


def test_configured_microdata_and_verified_dom_parser_chain() -> None:
    microdata = """<div itemscope itemtype="https://schema.org/Product">
      <span itemprop="name">Micro Clay</span><span itemprop="sku">MIC-1</span>
      <div itemprop="offers" itemscope itemtype="https://schema.org/Offer">
        <meta itemprop="price" content="9.25"><meta itemprop="priceCurrency" content="EUR">
      </div></div>"""
    micro = ShopwareConnector(
        FakeTransport(), ShopwareOptions(parsers=("microdata",)), _context()
    ).parse(microdata, "https://shop.test/micro", "shop")[0]
    assert micro.title == "Micro Clay"
    assert micro.variants[0].offers[0].evidence[0].method == "html"
    assert micro.platform_extensions["page_parser"] == "microdata"

    rules = VerifiedDomRules.model_validate({
        "verification": [{"selector": "#product"}],
        "name": {"selector": "h1.name"},
        "price": {"selector": "span.price"},
        "currency": {"selector": "meta.currency", "attribute": "content"},
        "sku": {"selector": "span.sku"},
    })
    document = """<main id="product"><h1 class="name">DOM Clay</h1>
      <span class="price">7.50</span><meta class="currency" content="EUR">
      <span class="sku">DOM-1</span></main>"""
    dom = StarwebConnector(
        FakeTransport(), StarwebOptions(parsers=("dom",), dom_rules=rules), _context()
    ).parse(document, "https://shop.test/dom", "shop")[0]
    assert dom.title == "DOM Clay"
    assert dom.variants[0].offers[0].evidence[0].method == "html"


def test_common_page_normalization_keeps_documents_specs_and_legacy_shape() -> None:
    document = JSONLD.replace(
        "</html>",
        '<table><tr><th>Firing</th><td>1200 C</td></tr></table>'
        '<a href="/docs/spec.pdf">Technical sheet</a></html>',
    )
    connector = StarwebConnector(FakeTransport(), StarwebOptions(), _context())
    snapshot = connector.parse(document, "https://shop.test/a", "shop")[0]
    variant = snapshot.variants[0]
    assert connector.capabilities.supports_documents
    assert snapshot.documents[0].url == "https://shop.test/docs/spec.pdf"
    assert variant.published_attributes["Firing"] == "1200 C"
    assert variant.is_default
    assert variant.canonical_url == "https://shop.test/a"
    assert variant.platform_extensions["legacy_raw_variant"] is None
    raw = cast(dict[str, JsonValue], snapshot.platform_extensions["raw"])
    assert raw["sku"] == "CL-1"


async def test_browser_required_hint_and_rendered_fallback() -> None:
    transport = FakeTransport()
    transport.add(
        "https://shop.test/s.xml",
        body="<urlset><url><loc>https://shop.test/a</loc></url></urlset>",
    )
    transport.add(
        "https://shop.test/a",
        body='<html><div id="root"></div><script>enable javascript</script></html>',
    )
    transport.add("https://shop.test/a", body=JSONLD)
    connector = ShopwareConnector(
        transport, ShopwareOptions(sitemaps=("/s.xml",)), _context()
    )
    [page] = await assert_connector_pages(connector.collect(_request()))
    assert page.items[0].title == "Clay"
    assert [request.browser for request in transport.requests[1:]] == [
        BrowserHint.NEVER, BrowserHint.REQUIRED,
    ]


async def test_multi_snapshot_limit_resumes_without_loss_and_preserves_sequence() -> None:
    document = """<script type="application/ld+json">[
      {"@type":"Product","name":"One","sku":"ONE","offers":{"price":"1","priceCurrency":"EUR"}},
      {"@type":"Product","name":"Two","sku":"TWO","offers":{"price":"2","priceCurrency":"EUR"}}
    ]</script>"""
    sitemap = "<urlset><url><loc>https://shop.test/a</loc></url></urlset>"
    transport = FakeTransport()
    transport.add("https://shop.test/s.xml", body=sitemap)
    transport.add("https://shop.test/a", body=document)
    connector = ShopwareConnector(
        transport, ShopwareOptions(sitemaps=("/s.xml",)), _context()
    )
    [limited] = await assert_connector_pages(connector.collect(_request(limit=1)))
    assert [item.title for item in limited.items] == ["One"]
    assert limited.resume_after == {
        "index": 0, "url": "https://shop.test/a",
        "snapshot_offset": 1, "sequence": 1,
    }

    resumed_transport = FakeTransport()
    resumed_transport.add("https://shop.test/s.xml", body=sitemap)
    resumed_transport.add("https://shop.test/a", body=document)
    resumed_connector = ShopwareConnector(
        resumed_transport, ShopwareOptions(sitemaps=("/s.xml",)), _context()
    )
    checkpoint = resumed_connector.checkpoint(
        _request(limit=1), "lineage", limited.resume_after
    )
    resumed = tuple([page async for page in resumed_connector.collect(
        _request(limit=1), checkpoint
    )])
    assert len(resumed) == 1
    assert resumed[0].sequence == 1
    assert resumed[0].terminal and resumed[0].enumeration_intact
    assert [item.title for item in resumed[0].items] == ["Two"]


async def test_resume_rejects_missing_out_of_range_and_intra_page_positions() -> None:
    sitemap = "<urlset><url><loc>https://shop.test/a</loc></url></urlset>"

    async def resume(cursor: JsonValue) -> None:
        transport = FakeTransport()
        transport.add("https://shop.test/s.xml", body=sitemap)
        transport.add("https://shop.test/a", body=JSONLD)
        connector = ShopwareConnector(
            transport, ShopwareOptions(sitemaps=("/s.xml",)), _context()
        )
        checkpoint = connector.checkpoint(_request(), "lineage", cursor)
        await anext(connector.collect(_request(), checkpoint))

    with pytest.raises(ValueError, match="cursor values"):
        await resume({"index": 0, "url": "https://shop.test/a", "sequence": 1})
    with pytest.raises(ValueError, match="out of range"):
        await resume({
            "index": 4, "url": "https://shop.test/missing",
            "snapshot_offset": 0, "sequence": 1,
        })
    with pytest.raises(ValueError, match="resume target"):
        await resume({
            "index": 0, "url": "https://shop.test/removed",
            "snapshot_offset": 0, "sequence": 1,
        })
    with pytest.raises(ValueError, match="snapshot offset"):
        await resume({
            "index": 0, "url": "https://shop.test/a",
            "snapshot_offset": 3, "sequence": 1,
        })


async def test_cancellation_during_discovery_stops_before_entity_request() -> None:
    transport = FakeTransport()
    transport.add(
        "https://shop.test/s.xml",
        body="<urlset><url><loc>https://shop.test/a</loc></url></urlset>",
    )
    context = ConnectorContext(
        clock=lambda: NOW, cancelled=lambda: bool(transport.requests)
    )
    connector = ShopwareConnector(
        transport, ShopwareOptions(sitemaps=("/s.xml",)), context
    )
    assert [page async for page in connector.collect(_request())] == []
    assert [request.url for request in transport.requests] == [
        "https://shop.test/s.xml"
    ]
