from __future__ import annotations

import gzip

import httpx
import pytest
from mb_commerce_scraper.transports import ResponseBodyTooLarge

from mb_ceramics_catalogue.config.sources import SourceConfig
from mb_ceramics_catalogue.connectors import (
    BigCommerceConnector,
    NitroSellConnector,
    ShopifyConnector,
    ShopwareConnector,
    StarwebConnector,
    SumUpConnector,
    WixConnector,
    WooCommerceConnector,
)
from mb_ceramics_catalogue.ops.connector_adapters import (
    RUNTIME_ADAPTERS,
    BigCommerceFetcherTransport,
    ConnectorRuntimePlan,
    LibraryCanaryRoute,
    WixFetcherTransport,
    library_canary_route,
    runtime_plan,
)
from mb_ceramics_catalogue.pipeline.budget import RequestBudget, RequestCost


class Fetcher:
    async def response(self, url, **kwargs):
        del kwargs
        return httpx.Response(200, content=gzip.compress(b"<urlset/>"), request=httpx.Request("GET", url))


@pytest.mark.parametrize(
    ("scraper", "fields", "connector_type", "partitions"),
    [
        ("shopify", {"collections": ["clay"]}, ShopifyConnector, ("clay",)),
        (
            "woocommerce",
            {"store_categories": ["glazes"], "variation_page_limit": 17},
            WooCommerceConnector,
            ("glazes",),
        ),
        ("bigcommerce", {"category_url": "https://shop.test/token"}, BigCommerceConnector, ("main",)),
        ("wix", {"sitemaps": ["https://shop.test/sitemap.xml"]}, WixConnector, ("main",)),
        (
            "shopware",
            {"category_urls": ["https://shop.test/clay"], "use_advertised_sitemaps": False},
            ShopwareConnector, ("category",),
        ),
        (
            "starweb",
            {"category_urls": ["https://shop.test/clay"], "use_advertised_sitemaps": False},
            StarwebConnector, ("category",),
        ),
        (
            "nitrosell",
            {"category_urls": ["https://shop.test/clay"], "use_advertised_sitemaps": False},
            NitroSellConnector, ("category",),
        ),
        ("sumup", {}, SumUpConnector, ("sitemap",)),
    ],
)
def test_runtime_registry_constructs_every_connector(
    scraper, fields, connector_type, partitions
):
    config = SourceConfig(
        label="Shop", url="https://shop.test/", scraper=scraper, **fields
    )
    plan = runtime_plan(config)
    connector = plan.build(Fetcher(), None)

    assert set(RUNTIME_ADAPTERS) >= {
        "shopify", "woocommerce", "bigcommerce", "wix",
        "shopware", "starweb", "nitrosell", "sumup",
    }
    assert plan.name == scraper
    assert plan.connector_version == connector.version
    assert plan.partitions == partitions
    assert plan.legacy_scraper_adapter == f"{scraper}_connector"
    assert isinstance(connector, connector_type)
    assert plan.options


@pytest.mark.asyncio
async def test_wix_transport_preserves_compressed_sitemap_decoding():
    document = await WixFetcherTransport(Fetcher()).document(
        "https://shop.test/sitemap.xml.gz", accept="application/xml"
    )
    assert document == "<urlset/>"


@pytest.mark.asyncio
async def test_wix_transport_bounds_compressed_and_decompressed_documents():
    class LargeCompressedFetcher:
        async def response(self, url, **kwargs):
            del kwargs
            return httpx.Response(
                200,
                content=gzip.compress(b"x" * 128),
                request=httpx.Request("GET", url),
            )

    with pytest.raises(ResponseBodyTooLarge, match="32-byte retention limit"):
        await WixFetcherTransport(
            LargeCompressedFetcher(),
            maximum_response_bytes=32,
        ).document("https://shop.test/sitemap.xml.gz", accept="application/xml")


@pytest.mark.asyncio
async def test_bigcommerce_json_failure_does_not_retain_raw_document():
    secret = "json-secret-sentinel"

    class InvalidJsonFetcher:
        async def response(self, url, **kwargs):
            del kwargs
            return httpx.Response(
                200,
                content=f'{{"token":"{secret}"'.encode(),
                request=httpx.Request("POST", url),
            )

    with pytest.raises(ValueError, match="not valid JSON") as caught:
        await BigCommerceFetcherTransport(InvalidJsonFetcher()).request_json(
            "https://shop.test/graphql",
            headers={},
            body={},
        )

    assert secret not in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_unknown_runtime_adapter_fails_with_registered_names():
    config = SourceConfig(label="Shop", url="https://shop.test/", scraper="shopify_connector")
    with pytest.raises(ValueError, match=r"shopify.*wix"):
        runtime_plan(config)


@pytest.mark.parametrize("scraper", sorted(RUNTIME_ADAPTERS))
def test_every_registered_projector_returns_a_complete_plan(scraper):
    config = SourceConfig(label="Shop", url="https://shop.test/", scraper=scraper)
    assert isinstance(runtime_plan(config), ConnectorRuntimePlan)


@pytest.mark.parametrize("scraper", sorted(RUNTIME_ADAPTERS))
def test_every_runtime_forwards_the_shared_request_budget(scraper):
    config = SourceConfig(label="Shop", url="https://shop.test/", scraper=scraper)
    budget = RequestBudget(RequestCost(http_requests=10, browser_requests=10))

    connector = runtime_plan(config).build(Fetcher(), budget)

    assert connector._budget.budget is budget


def test_library_canary_routes_are_explicit_application_metadata() -> None:
    shopify = runtime_plan(
        SourceConfig(
            label="Shop",
            url="https://shop.test/",
            scraper="shopify",
            collections=["clay", "kilns"],
        )
    )
    woocommerce = runtime_plan(
        SourceConfig(
            label="Shop",
            url="https://shop.test/",
            scraper="woocommerce",
            store_categories=["glazes"],
        )
    )
    bigcommerce = runtime_plan(
        SourceConfig(
            label="Shop",
            url="https://shop.test/",
            scraper="bigcommerce",
        )
    )
    prestashop = runtime_plan(
        SourceConfig(
            label="Shop",
            url="https://shop.test/",
            scraper="prestashop",
            sitemaps=["https://shop.test/product-sitemap.xml"],
        )
    )
    sio2 = runtime_plan(
        SourceConfig(
            label="Shop",
            url="https://shop.test/gb/",
            scraper="sio2",
            category_urls=["https://shop.test/gb/66-low-fire-ceramic-clays"],
            use_advertised_sitemaps=False,
            product_pattern=r"^/gb/[a-z0-9-]+/[0-9]+-[^/]+\.html$",
            card_links_only=True,
        )
    )
    wix = runtime_plan(
        SourceConfig(
            label="Shop",
            url="https://shop.test/",
            scraper="wix",
            sitemaps=["https://shop.test/store-products-sitemap.xml"],
        )
    )
    specialized = {
        name: runtime_plan(
            SourceConfig(
                label="Shop",
                url="https://shop.test/",
                scraper=name,
                category_urls=["https://shop.test/category"],
                use_advertised_sitemaps=False,
                render=False if name == "nitrosell" else None,
            )
        )
        for name in ("shopware", "starweb", "nitrosell")
    }
    sumup = runtime_plan(
        SourceConfig(
            label="Shop",
            url="https://shop.test/",
            scraper="sumup",
        )
    )
    generic_pages = runtime_plan(
        SourceConfig(
            label="Shop",
            url="https://shop.test/",
            scraper="pagecrawl",
            sitemaps=["https://shop.test/products.xml"],
            product_pattern=r"/product/",
        )
    )
    axner = runtime_plan(
        SourceConfig(
            label="Shop",
            url="https://shop.test/",
            scraper="axner",
            category_url="https://shop.test/sitemap.aspx",
            render=False,
        )
    )
    kraft = runtime_plan(
        SourceConfig(
            label="Shop",
            url="https://shop.test/",
            scraper="keramik_kraft",
            category_paths=["de/Glasuren.html"],
            render=False,
        )
    )
    ceramicolours = runtime_plan(
        SourceConfig(
            label="Shop",
            url="https://shop.test/",
            scraper="ceramicolours",
        )
    )

    assert library_canary_route(shopify, "shopify") == LibraryCanaryRoute(
        connector="shopify",
        request_partitions=("clay", "kilns"),
    )
    assert library_canary_route(woocommerce, "woocommerce") == LibraryCanaryRoute(
        connector="woocommerce",
        request_partitions=("glazes",),
        dynamic_partitions=True,
    )
    assert library_canary_route(bigcommerce, "bigcommerce") == LibraryCanaryRoute(
        connector="bigcommerce",
        uses_browser_transport=True,
    )
    assert library_canary_route(prestashop, "prestashop") == LibraryCanaryRoute(
        connector="prestashop",
        request_partitions=("sitemap:0:d30089dfe4db",),
    )
    assert library_canary_route(sio2, "prestashop") == LibraryCanaryRoute(
        connector="prestashop",
        request_partitions=("category:0:9bc2b475c709",),
    )
    assert sio2.ceramics_projection == {"source_policy": "sio2"}
    assert library_canary_route(wix, "wix") == LibraryCanaryRoute(
        connector="wix",
        uses_browser_transport=True,
    )
    assert {
        name: library_canary_route(plan, name)
        for name, plan in specialized.items()
    } == {
        "shopware": LibraryCanaryRoute(
            connector="shopware",
            request_partitions=("category",),
            uses_browser_transport=True,
        ),
        "starweb": LibraryCanaryRoute(
            connector="starweb",
            request_partitions=("category",),
            uses_browser_transport=True,
        ),
        "nitrosell": LibraryCanaryRoute(
            connector="nitrosell",
            request_partitions=("category",),
        ),
    }
    assert library_canary_route(sumup, "sumup") == LibraryCanaryRoute(
        connector="sumup",
        request_partitions=("sitemap",),
        uses_browser_transport=True,
    )
    assert library_canary_route(
        generic_pages, "generic-pages"
    ) == LibraryCanaryRoute(
        connector="generic-pages",
        request_partitions=("sitemap",),
        uses_browser_transport=True,
    )
    assert library_canary_route(axner, "axner") == LibraryCanaryRoute(
        connector="axner",
        request_partitions=("main",),
    )
    assert library_canary_route(kraft, "keramik-kraft") == LibraryCanaryRoute(
        connector="keramik-kraft",
        request_partitions=("main",),
    )
    assert library_canary_route(
        ceramicolours, "ceramicolours"
    ) == LibraryCanaryRoute(
        connector="ceramicolours",
        request_partitions=("main",),
        uses_browser_transport=True,
    )


def test_library_canary_route_rejects_projection_drift_and_invalid_metadata() -> None:
    plan = runtime_plan(
        SourceConfig(label="Shop", url="https://shop.test/", scraper="shopify")
    )

    with pytest.raises(ValueError, match="does not match"):
        library_canary_route(plan, "woocommerce")
    with pytest.raises(ValueError, match="configured partitions"):
        LibraryCanaryRoute(connector="shopify", dynamic_partitions=True)
