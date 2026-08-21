from mb_commerce_scraper import CollectionRequest
from mb_commerce_scraper.connectors import GenericPagesConnector, GenericPagesOptions
from mb_commerce_scraper.testing import FakeTransport, assert_connector_pages


async def test_sitemap_jsonld_collection() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/sitemap.xml", body="<urlset><url><loc>https://shop.test/products/clay</loc></url></urlset>")
    transport.add("https://shop.test/products/clay", body='''<script type="application/ld+json">{"@type":"Product","name":"Clay","sku":"CLAY-1","offers":{"price":"8.50","priceCurrency":"EUR","availability":"https://schema.org/InStock"}}</script>''')
    connector = GenericPagesConnector(transport, GenericPagesOptions.model_validate({"discovery": {"sitemaps": ["/sitemap.xml"], "product_pattern": "/products/"}}))
    pages = await assert_connector_pages(connector.collect(CollectionRequest(source_id="shop", base_url="https://shop.test")))
    assert pages[0].items[0].title == "Clay"
    assert pages[-1].page_id == "sitemap:terminal"


def test_declarative_config_rejects_code_execution_fields() -> None:
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        GenericPagesOptions.model_validate({"python_expression": "__import__('os')"})
    with pytest.raises(ValidationError):
        GenericPagesOptions.model_validate({"parsers": ["javascript"]})

