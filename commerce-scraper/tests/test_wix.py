from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from mb_commerce_scraper import CollectionRequest, DiagnosticCode, SnapshotField
from mb_commerce_scraper.connectors.base import ConnectorContext
from mb_commerce_scraper.connectors.wix import WixConnector, WixFactory, WixOptions
from mb_commerce_scraper.proxy import ProxyBudgetExhausted
from mb_commerce_scraper.testing import FakeTransport, assert_connector_pages
from mb_commerce_scraper.transports import (
    BrowserHint,
    BudgetExhausted,
    RequestPurpose,
    RobotsDenied,
    TransportRequest,
    TransportResponse,
)

NOW = datetime(2026, 8, 15, 12, 0, tzinfo=UTC)
SITEMAP = "https://shop.test/store-products-sitemap.xml"


class RaisingTransport(FakeTransport):
    def __init__(self, error: RuntimeError) -> None:
        super().__init__()
        self.error = error

    async def request(self, request: TransportRequest) -> TransportResponse:
        self.requests.append(request)
        raise self.error


def request(*, limit: int | None = None) -> CollectionRequest:
    return CollectionRequest(
        source_id="wix-shop",
        base_url="https://shop.test/",
        requested_fields=frozenset(SnapshotField),
        result_limit=limit,
    )


def sitemap(*urls: str) -> str:
    return "<urlset>" + "".join(f"<url><loc>{url}</loc></url>" for url in urls) + "</urlset>"


def product_page(slug: str = "glaze", identifier: str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa") -> str:
    product: dict[str, Any] = {
        "id": identifier,
        "name": "Transparent <b>Glaze</b>",
        "description": "A glossy &amp; durable glaze.",
        "brand": "Test Ceramics",
        "sku": "PARENT",
        "price": 12.5,
        "comparePrice": 15,
        "formattedPrice": "€12.50",
        "isInStock": True,
        "isTrackingInventory": True,
        "inventory": {"quantity": 7, "status": "in_stock"},
        "media": [{"fullUrl": "https://cdn.test/fallback.jpg"}],
        "productItems": [
            {
                "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "sku": "GL-500",
                "price": 11,
                "comparePrice": 14,
                "formattedPrice": "€11.00",
                "isInStock": True,
                "isTrackingInventory": True,
                "inventory": {"quantity": 3},
                "optionsSelections": {"Size": "500 ml"},
            },
            {
                "id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
                "sku": "GL-1K",
                "price": 20,
                "comparePrice": 0,
                "formattedPrice": "€20.00",
                "isInStock": False,
                "inventory": {"status": "out_of_stock", "quantity": 99},
                "optionsSelections": {"Size": "1 kg"},
            },
        ],
    }
    jsonld = {
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": "Product",
                "name": "JSON-LD name",
                "image": ["https://cdn.test/published.jpg"],
            },
            {
                "@type": "BreadcrumbList",
                "itemListElement": [
                    {"item": {"name": "Store"}},
                    {"item": {"name": "Glazes"}},
                ],
            },
        ],
    }
    return (
        f'<script type="application/ld+json">{json.dumps(jsonld)}</script>'
        f'<script>window.warmup={{"{slug}":{{"product":{json.dumps(product)}}}}};</script>'
        '<script>const locale={"currency":"EUR"};</script>'
        '<a href="/docs/sds.pdf">Safety data sheet</a>'
    )


async def test_warmup_payload_emits_neutral_price_stock_and_documents() -> None:
    url = "https://shop.test/product-page/glaze"
    transport = FakeTransport()
    transport.add(SITEMAP, body=sitemap(url))
    transport.add(url, body=product_page())
    connector = WixConnector(
        transport,
        WixOptions(sitemaps=(SITEMAP,), render=False, vat_status="inclusive"),
        ConnectorContext(clock=lambda: NOW),
    )

    intent = request()
    pages = await assert_connector_pages(
        connector.collect(intent), connector=connector, request=intent
    )

    snapshot = pages[0].items[0]
    assert snapshot.title == "Transparent Glaze"
    assert [category.name for category in snapshot.categories] == ["Store", "Glazes"]
    assert snapshot.images[0].url == "https://cdn.test/published.jpg"
    assert snapshot.documents[0].url == "https://shop.test/docs/sds.pdf"
    first, second = snapshot.variants
    assert first.options == {"Size": "500 ml"}
    assert [offer.role for offer in first.offers] == ["sale", "regular"]
    assert [offer.price.amount for offer in first.offers] == [11, 14]
    assert first.stock is not None and first.stock.quantity == 3
    assert second.stock is not None and second.stock.quantity == 0
    assert [
        (item.url, item.purpose, item.browser, item.estimated_bytes)
        for item in transport.requests
    ] == [
        (SITEMAP, RequestPurpose.DISCOVERY, BrowserHint.NEVER, 2_000_000),
        (url, RequestPurpose.ENTITY, BrowserHint.NEVER, 2_000_000),
    ]


async def test_browser_fallback_uses_required_browser_hint_and_checks_cancellation() -> None:
    url = "https://shop.test/product-page/glaze"
    transport = FakeTransport()
    transport.add(SITEMAP, body=sitemap(url))
    transport.add(url, body="<html><div id='root'></div></html>")
    transport.add(url, body=product_page())
    connector = WixConnector(
        transport,
        WixOptions(sitemaps=(SITEMAP,)),
        ConnectorContext(clock=lambda: NOW),
    )

    pages = await assert_connector_pages(connector.collect(request()))

    assert pages[0].enumeration_intact
    assert [item.browser for item in transport.requests[-2:]] == [
        BrowserHint.NEVER,
        BrowserHint.REQUIRED,
    ]
    assert [
        (item.purpose, item.browser, item.estimated_bytes)
        for item in transport.requests
    ] == [
        (RequestPurpose.DISCOVERY, BrowserHint.NEVER, 2_000_000),
        (RequestPurpose.ENTITY, BrowserHint.NEVER, 2_000_000),
        (RequestPurpose.ENTITY, BrowserHint.REQUIRED, 2_000_000),
    ]

    cancelled_transport = FakeTransport()
    cancelled_transport.add(SITEMAP, body=sitemap(url))
    cancelled = WixConnector(
        cancelled_transport,
        WixOptions(sitemaps=(SITEMAP,)),
        ConnectorContext(cancelled=lambda: True),
    )
    assert [page async for page in cancelled.collect(request())] == []
    assert cancelled_transport.requests == []


async def test_result_limit_checkpoint_resumes_and_rejects_option_changes() -> None:
    urls = (
        "https://shop.test/product-page/z-last",
        "https://shop.test/product-page/a-first",
    )
    transport = FakeTransport()
    transport.add(SITEMAP, body=sitemap(*urls))
    transport.add(urls[0], body=product_page("z-last"))
    connector = WixConnector(
        transport,
        WixOptions(sitemaps=(SITEMAP,), render=False),
        ConnectorContext(clock=lambda: NOW),
    )
    limited_request = request(limit=1)
    [page] = await assert_connector_pages(connector.collect(limited_request))
    assert page.diagnostics[0].code == DiagnosticCode.RESULT_LIMIT_REACHED
    assert page.resume_after == {"index": 1, "sequence": 1}
    checkpoint = connector.checkpoint(limited_request, "lineage", page.resume_after)

    resumed_transport = FakeTransport()
    resumed_transport.add(SITEMAP, body=sitemap(*urls))
    resumed_transport.add(urls[1], body=product_page("a-first", "dddddddd-dddd-dddd-dddd-dddddddddddd"))
    resumed = WixConnector(
        resumed_transport,
        WixOptions(sitemaps=(SITEMAP,), render=False),
        ConnectorContext(clock=lambda: NOW),
    )
    resumed_page = await anext(resumed.collect(limited_request, checkpoint))
    assert resumed_page.items[0].canonical_url == urls[1]

    changed = WixConnector(FakeTransport(), WixOptions(sitemaps=(SITEMAP,), render=True))
    with pytest.raises(ValueError, match="CHECKPOINT_INVALID"):
        await anext(changed.collect(limited_request, checkpoint))


async def test_advertised_sitemap_and_typed_fetch_failure() -> None:
    url = "https://shop.test/product-page/glaze"
    transport = FakeTransport()
    transport.add("https://shop.test/robots.txt", body=f"User-agent: *\nSitemap: {SITEMAP}\n")
    transport.add(SITEMAP, body=sitemap(url))
    transport.add(url, status=503)
    connector = WixConnector(transport, WixOptions(render=False))

    [page] = await assert_connector_pages(connector.collect(request()))

    assert not page.enumeration_intact
    assert page.resume_after == {"index": 0, "sequence": 0}
    assert page.diagnostics[0].code == DiagnosticCode.ENTITY_FETCH_FAILED
    assert transport.requests[0].purpose == RequestPurpose.ROBOTS
    assert transport.requests[1].headers == {"Accept": "application/xml,text/xml"}


def test_strict_options_and_factory_contract() -> None:
    with pytest.raises(ValidationError):
        WixOptions.model_validate({"unknown": True})

    factory = WixFactory()
    connector = factory.build(
        transport=FakeTransport(),
        options=WixOptions(render=False),
        context=ConnectorContext(clock=lambda: NOW),
    )
    assert connector.name == factory.name == "wix"
    assert connector.capabilities.browser.value == "optional"


@pytest.mark.parametrize(
    "error",
    [
        BudgetExhausted("request budget exhausted"),
        RobotsDenied("robots denied"),
        ProxyBudgetExhausted("proxy budget exhausted"),
    ],
)
async def test_sitemap_discovery_does_not_swallow_policy_failures(
    error: RuntimeError,
) -> None:
    connector = WixConnector(RaisingTransport(error))

    with pytest.raises(type(error)):
        await anext(connector.collect(request()))
