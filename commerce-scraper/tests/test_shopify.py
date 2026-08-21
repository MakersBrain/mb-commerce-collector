from mb_commerce_scraper import CollectionRequest, RefreshMode, SnapshotField
from mb_commerce_scraper.connectors import ConnectorContext, ShopifyConnector, ShopifyOptions
from mb_commerce_scraper.testing import FakeTransport, assert_connector_pages


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

