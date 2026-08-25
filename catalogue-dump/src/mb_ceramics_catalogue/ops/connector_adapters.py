"""Project validated catalogue sources into native connector runtime metadata."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse

from mb_commerce_scraper.connectors import (
    BigCommerceOptions,
    DiscoveryOptions,
    GenericPagesOptions,
    NitroSellOptions,
    PrestaShopOptions,
    ShopifyOptions,
    ShopwareOptions,
    StarwebOptions,
    SumUpOptions,
    WixOptions,
    WooCommerceOptions,
    prestashop_partition_keys,
)

from mb_ceramics_catalogue.config.sources import SourceConfig

from .commerce_scraper_axner import AxnerOptions
from .commerce_scraper_ceramicolours import CeramicoloursOptions
from .commerce_scraper_keramik_kraft import KeramikKraftOptions


@dataclass(frozen=True, slots=True)
class LibraryCanaryRoute:
    """Application approval for one connector's native migration route."""

    connector: str
    request_partitions: tuple[str, ...] = ()
    dynamic_partitions: bool = False
    uses_browser_transport: bool = False

    def __post_init__(self) -> None:
        if not self.connector or self.connector != self.connector.strip().lower():
            raise ValueError(
                "library canary connector must be a normalized non-empty name"
            )
        if any(not value or value != value.strip() for value in self.request_partitions):
            raise ValueError(
                "library canary partitions must be normalized non-empty strings"
            )
        if len(set(self.request_partitions)) != len(self.request_partitions):
            raise ValueError("library canary partitions must be unique")
        if self.dynamic_partitions and not self.request_partitions:
            raise ValueError(
                "dynamic library canary partitions require configured partitions"
            )


@dataclass(frozen=True)
class ConnectorRuntimePlan:
    """Data-only application projection for one stable source scraper family."""

    connector: str
    connector_options: dict[str, Any]
    extraction_method: str
    source_detail_level: str
    ceramics_projection: dict[str, Any] = field(default_factory=dict)
    categories: tuple[str, ...] = ()
    collections: tuple[str, ...] = ()
    library_canary: LibraryCanaryRoute | None = None


RuntimeProjector = Callable[[SourceConfig], ConnectorRuntimePlan]
RUNTIME_ADAPTERS: dict[str, RuntimeProjector] = {}


def register_runtime_adapter(name: str, projector: RuntimeProjector) -> None:
    if name in RUNTIME_ADAPTERS:
        raise ValueError(f"runtime adapter {name!r} is already registered")
    RUNTIME_ADAPTERS[name] = projector


def runtime_plan(config: SourceConfig) -> ConnectorRuntimePlan:
    try:
        return RUNTIME_ADAPTERS[config.scraper](config)
    except KeyError:
        known = ", ".join(sorted(RUNTIME_ADAPTERS))
        raise ValueError(
            f"connector_canary has no runtime adapter for {config.scraper!r}; known: {known}"
        ) from None


def library_canary_route(
    plan: ConnectorRuntimePlan,
    projected_connector: str,
) -> LibraryCanaryRoute | None:
    """Return explicitly approved native metadata, rejecting projection drift."""
    route = plan.library_canary
    if route is not None and route.connector != projected_connector:
        raise ValueError(
            "library canary route does not match the projected source connector"
        )
    return route


def _shopify(config: SourceConfig) -> ConnectorRuntimePlan:
    options = ShopifyOptions(
        currency=config.currency, vat_status=config.vat_status,
        page_limit=config.page_limit or 200,
        inventory_method=("product_html" if config.inventory_product_html else
                          "product_json" if config.inventory_product_json else "none"),
        inventory_section_id=config.inventory_section_id,
    )
    collections = tuple(config.collections or ())
    return ConnectorRuntimePlan(
        connector="shopify",
        connector_options=options.model_dump(mode="json"),
        extraction_method="api_json",
        source_detail_level="api",
        collections=collections,
        library_canary=LibraryCanaryRoute(
            connector="shopify",
            request_partitions=collections,
        ),
    )


def _woocommerce(config: SourceConfig) -> ConnectorRuntimePlan:
    options = WooCommerceOptions(
        store_categories=tuple(config.store_categories or ()),
        identity_only=bool(config.identity_only), brand=config.brand,
        vat_status=config.vat_status,
        vat_rate=Decimal(str(config.vat_rate)) if config.vat_rate is not None else None,
        stock_from_add_to_cart_maximum=bool(config.stock_from_add_to_cart_maximum),
        page_limit=config.page_limit or 100,
        variation_page_limit=config.variation_page_limit or 200,
        category_page_limit=config.category_page_limit or 20,
    )
    categories = tuple(config.store_categories or ())
    return ConnectorRuntimePlan(
        connector="woocommerce",
        connector_options=options.model_dump(mode="json"),
        extraction_method="api_json",
        source_detail_level="api",
        categories=categories,
        library_canary=LibraryCanaryRoute(
            connector="woocommerce",
            request_partitions=categories,
            dynamic_partitions=bool(categories),
        ),
    )


def _bigcommerce(config: SourceConfig) -> ConnectorRuntimePlan:
    options = BigCommerceOptions(
        token_page=config.category_url, page_limit=config.page_limit or 200,
        allow_rendered_token_fallback=config.render is not False,
        vat_status=config.vat_status,
    )
    return ConnectorRuntimePlan(
        connector="bigcommerce",
        connector_options=options.model_dump(mode="json"),
        extraction_method="graphql",
        source_detail_level="api",
        library_canary=LibraryCanaryRoute(
            connector="bigcommerce",
            uses_browser_transport=True,
        ),
    )


def _wix(config: SourceConfig) -> ConnectorRuntimePlan:
    options = WixOptions(
        sitemaps=tuple(config.sitemaps or ()),
        use_advertised_sitemaps=(True if config.use_advertised_sitemaps is None
                                else config.use_advertised_sitemaps),
        product_pattern=config.product_pattern, page_limit=config.page_limit or 500,
        brand=config.brand, vat_status=config.vat_status, render=config.render,
    )
    return ConnectorRuntimePlan(
        connector="wix",
        connector_options=options.model_dump(mode="json"),
        extraction_method="dom",
        source_detail_level="product_page",
        library_canary=LibraryCanaryRoute(
            connector="wix",
            uses_browser_transport=True,
        ),
    )


def _page_partitions(config: SourceConfig, *, sitemaps: tuple[str, ...]) -> tuple[str, ...]:
    return ("sitemap",) if sitemaps or config.use_advertised_sitemaps is not False else ("category",)


def _specialized_options(
    model: type[ShopwareOptions] | type[StarwebOptions] | type[NitroSellOptions],
    config: SourceConfig,
) -> ShopwareOptions | StarwebOptions | NitroSellOptions:
    return model(
        discovery=DiscoveryOptions(
            sitemaps=tuple(config.sitemaps or ()),
            use_advertised_sitemaps=(
                True
                if config.use_advertised_sitemaps is None
                else config.use_advertised_sitemaps
            ),
            category_urls=tuple(config.category_urls or ()),
            product_pattern=config.product_pattern,
            pagination_patterns=tuple(config.pagination_patterns or ()),
            card_links_only=bool(config.card_links_only),
            category_page_limit=config.category_page_limit or 120,
        ),
        page_limit=config.page_limit or 500,
        render=config.render, brand=config.brand, currency=config.currency,
        vat_status=config.vat_status,
        vat_rate=Decimal(str(config.vat_rate)) if config.vat_rate is not None else None,
        stock_from_quantity_maximum=(
            True if model is ShopwareOptions else bool(config.stock_from_quantity_maximum)
        ),
    )


def _specialized(config: SourceConfig, name: str) -> ConnectorRuntimePlan:
    definitions: dict[str, tuple[Any, str]] = {
        "shopware": (ShopwareOptions, "dom"),
        "starweb": (StarwebOptions, "dom"),
        "nitrosell": (NitroSellOptions, "opengraph"),
    }
    options_type, method = definitions[name]
    options = _specialized_options(options_type, config)
    sitemaps = tuple(options.discovery.sitemaps)
    partitions = _page_partitions(config, sitemaps=sitemaps)
    return ConnectorRuntimePlan(
        connector=name,
        connector_options=options.model_dump(mode="json"),
        extraction_method=method,
        source_detail_level="product_page",
        library_canary=LibraryCanaryRoute(
            connector=name,
            request_partitions=partitions,
            uses_browser_transport=options.render is not False,
        ),
    )


def _sumup(config: SourceConfig) -> ConnectorRuntimePlan:
    origin = f"{urlparse(config.url).scheme}://{urlparse(config.url).netloc}"
    sitemaps = tuple(config.sitemaps or (f"{origin}/sitemap.products.xml",))
    options = SumUpOptions(
        discovery=DiscoveryOptions(
            sitemaps=sitemaps,
            use_advertised_sitemaps=False,
            product_pattern=config.product_pattern,
        ),
        page_limit=config.page_limit or 500,
        render=config.render, brand=config.brand, currency=config.currency,
        vat_status=config.vat_status,
        vat_rate=Decimal(str(config.vat_rate)) if config.vat_rate is not None else None,
    )
    return ConnectorRuntimePlan(
        connector="sumup",
        connector_options=options.model_dump(mode="json"),
        extraction_method="dom",
        source_detail_level="product_page",
        library_canary=LibraryCanaryRoute(
            connector="sumup",
            request_partitions=("sitemap",),
            uses_browser_transport=options.render is not False,
        ),
    )


def _prestashop(config: SourceConfig, *, sio2: bool = False) -> ConnectorRuntimePlan:
    options = PrestaShopOptions(
        sitemaps=tuple(config.sitemaps or ()), category_urls=tuple(config.category_urls or ()),
        use_advertised_sitemaps=(True if config.use_advertised_sitemaps is None
                                else config.use_advertised_sitemaps),
        product_pattern=config.product_pattern, card_links_only=bool(config.card_links_only),
        pagination_patterns=tuple(config.pagination_patterns or ()), render=config.render,
        variant_combinations=(True if config.variant_combinations is None
                              else config.variant_combinations),
        currency=config.currency or "EUR", brand=config.brand, vat_status=config.vat_status,
        vat_rate=Decimal(str(config.vat_rate)) if config.vat_rate is not None else None,
        page_limit=config.page_limit or 500,
        category_page_limit=config.category_page_limit or 120,
    )
    partitions = prestashop_partition_keys(options, config.url)
    return ConnectorRuntimePlan(
        connector="prestashop",
        connector_options=options.model_dump(mode="json"),
        extraction_method="dom",
        source_detail_level="product_page",
        ceramics_projection={"source_policy": "sio2"} if sio2 else {},
        library_canary=LibraryCanaryRoute(
            connector="prestashop",
            request_partitions=partitions,
        ),
    )


def _pagecommerce(config: SourceConfig) -> ConnectorRuntimePlan:
    discovery = DiscoveryOptions(
        sitemaps=tuple(config.sitemaps or ()),
        category_urls=tuple(config.category_urls or ()),
        use_advertised_sitemaps=(
            True
            if config.use_advertised_sitemaps is None
            else config.use_advertised_sitemaps
        ),
        product_pattern=config.product_pattern,
        pagination_patterns=tuple(config.pagination_patterns or ()),
        card_links_only=bool(config.card_links_only),
        category_page_limit=config.category_page_limit or 120,
        sitemap_limit=100,
    )
    options = GenericPagesOptions(
        discovery=discovery,
        page_limit=config.page_limit or 500,
        render=config.render,
        brand=config.brand, currency=config.currency, vat_status=config.vat_status,
        vat_rate=Decimal(str(config.vat_rate)) if config.vat_rate is not None else None,
        stock_from_quantity_maximum=bool(config.stock_from_quantity_maximum),
        parsers=("jsonld", "microdata", "opengraph"),
        browser_zero_gain_limit=10,
    )
    partitions = _page_partitions(config, sitemaps=tuple(discovery.sitemaps))
    return ConnectorRuntimePlan(
        connector="generic-pages",
        connector_options=options.model_dump(mode="json"),
        extraction_method="structured",
        source_detail_level="product_page",
        library_canary=LibraryCanaryRoute(
            connector="generic-pages",
            request_partitions=partitions,
            uses_browser_transport=options.render is not False,
        ),
    )


def _axner(config: SourceConfig) -> ConnectorRuntimePlan:
    options = AxnerOptions(
        category_url=config.category_url,
        category_page_limit=config.category_page_limit or 400,
        page_limit=config.page_limit or 500,
        brand=config.brand,
        currency=config.currency or "USD",
        vat_status=config.vat_status,
        vat_rate=(
            Decimal(str(config.vat_rate))
            if config.vat_rate is not None
            else None
        ),
        render=config.render,
    )
    return ConnectorRuntimePlan(
        connector="axner",
        connector_options=options.model_dump(mode="json"),
        extraction_method="dom",
        source_detail_level="product_page",
        library_canary=LibraryCanaryRoute(
            connector="axner",
            request_partitions=("main",),
            uses_browser_transport=options.render is not False,
        ),
    )


def _ceramicolours(config: SourceConfig) -> ConnectorRuntimePlan:
    options = CeramicoloursOptions(
        category_ids=tuple(str(value) for value in (config.category_ids or ())),
        category_page_limit=config.category_page_limit or 25,
        page_limit=config.page_limit or 500,
        brand=config.brand,
        vat_status=config.vat_status or "inclusive",
        render=config.render,
    )
    return ConnectorRuntimePlan(
        connector="ceramicolours",
        connector_options=options.model_dump(mode="json"),
        extraction_method="dom",
        source_detail_level="product_page",
        library_canary=LibraryCanaryRoute(
            connector="ceramicolours",
            request_partitions=("main",),
            uses_browser_transport=options.render is not False,
        ),
    )


def _keramik_kraft(config: SourceConfig) -> ConnectorRuntimePlan:
    options = KeramikKraftOptions(
        category_paths=tuple(config.category_paths or ()),
        category_page_limit=config.category_page_limit or 150,
        page_limit=config.page_limit or 500,
        brand=config.brand,
        vat_rate=(
            Decimal(str(config.vat_rate))
            if config.vat_rate is not None
            else None
        ),
        render=config.render,
    )
    return ConnectorRuntimePlan(
        connector="keramik-kraft",
        connector_options=options.model_dump(mode="json"),
        extraction_method="dom",
        source_detail_level="product_page",
        library_canary=LibraryCanaryRoute(
            connector="keramik-kraft",
            request_partitions=("main",),
            uses_browser_transport=options.render is not False,
        ),
    )


for _name, _projector in {
    "shopify": _shopify,
    "woocommerce": _woocommerce,
    "bigcommerce": _bigcommerce,
    "wix": _wix,
    "shopware": lambda config: _specialized(config, "shopware"),
    "starweb": lambda config: _specialized(config, "starweb"),
    "nitrosell": lambda config: _specialized(config, "nitrosell"),
    "sumup": _sumup,
    "prestashop": _prestashop,
    "sio2": lambda config: _prestashop(config, sio2=True),
    "pagecrawl": _pagecommerce,
    "axner": _axner,
    "ceramicolours": _ceramicolours,
    "keramik_kraft": _keramik_kraft,
}.items():
    register_runtime_adapter(_name, _projector)
