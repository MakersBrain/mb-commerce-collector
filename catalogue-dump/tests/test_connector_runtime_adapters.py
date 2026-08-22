from __future__ import annotations

import pytest
from mb_commerce_scraper.connectors import ConnectorContext
from mb_commerce_scraper.testing import FakeTransport

from mb_ceramics_catalogue.config.sources import SourceConfig
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import source_definition
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import application_connector_registry
from mb_ceramics_catalogue.ops.connector_adapters import (
    RUNTIME_ADAPTERS,
    ConnectorRuntimePlan,
    LibraryCanaryRoute,
    library_canary_route,
    runtime_plan,
)


@pytest.mark.parametrize(
    ("scraper", "fields", "connector_name", "partitions"),
    [
        ("shopify", {"collections": ["clay"]}, "shopify", ("clay",)),
        (
            "woocommerce",
            {"store_categories": ["glazes"], "variation_page_limit": 17},
            "woocommerce",
            ("glazes",),
        ),
        ("bigcommerce", {"category_url": "https://shop.test/token"}, "bigcommerce", ()),
        ("wix", {"sitemaps": ["https://shop.test/sitemap.xml"]}, "wix", ()),
        (
            "shopware",
            {"category_urls": ["https://shop.test/clay"], "use_advertised_sitemaps": False},
            "shopware", ("category",),
        ),
        (
            "starweb",
            {"category_urls": ["https://shop.test/clay"], "use_advertised_sitemaps": False},
            "starweb", ("category",),
        ),
        (
            "nitrosell",
            {"category_urls": ["https://shop.test/clay"], "use_advertised_sitemaps": False},
            "nitrosell", ("category",),
        ),
        ("sumup", {}, "sumup", ("sitemap",)),
    ],
)
def test_runtime_registry_projects_native_connector_metadata(
    scraper, fields, connector_name, partitions
):
    config = SourceConfig(
        label="Shop", url="https://shop.test/", scraper=scraper, **fields
    )
    plan = runtime_plan(config)
    definition = source_definition("shop", config, connector_plan=plan)
    registry = application_connector_registry()
    connector = registry.build(
        definition.connector,
        transport=FakeTransport(),
        options=definition.connector_options,
        context=ConnectorContext(),
    )

    assert set(RUNTIME_ADAPTERS) >= {
        "shopify", "woocommerce", "bigcommerce", "wix",
        "shopware", "starweb", "nitrosell", "sumup",
    }
    assert definition.connector == plan.connector == connector_name
    assert connector.name == connector_name
    assert connector.version == registry.connector_version(connector_name)
    assert plan.connector_options
    assert plan.library_canary is not None
    assert plan.library_canary.request_partitions == partitions


def test_unknown_runtime_adapter_fails_with_registered_names():
    config = SourceConfig(label="Shop", url="https://shop.test/", scraper="shopify_connector")
    with pytest.raises(ValueError, match=r"shopify.*wix"):
        runtime_plan(config)


@pytest.mark.parametrize("scraper", sorted(RUNTIME_ADAPTERS))
def test_every_registered_projector_returns_a_complete_plan(scraper):
    config = SourceConfig(label="Shop", url="https://shop.test/", scraper=scraper)
    assert isinstance(runtime_plan(config), ConnectorRuntimePlan)


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
