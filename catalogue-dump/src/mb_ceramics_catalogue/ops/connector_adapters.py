"""Runtime factories adapting the legacy fetcher to neutral connectors.

Connectors remain transport-only.  This ops-owned registry is the composition
root: it projects validated source configuration, supplies transport adapters,
and is intentionally extensible for new connector canaries.
"""

from __future__ import annotations

import json
import zlib
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse

from mb_commerce_scraper.transports import (
    DEFAULT_MAXIMUM_RESPONSE_BYTES,
    ResponseBodyTooLarge,
)

from mb_ceramics_catalogue.config.sources import SourceConfig
from mb_ceramics_catalogue.connectors import (
    AxnerConnector,
    AxnerOptions,
    BigCommerceConnector,
    BigCommerceOptions,
    CeramicoloursConnector,
    CeramicoloursOptions,
    CommerceConnector,
    KeramikKraftConnector,
    KeramikKraftOptions,
    NitroSellConnector,
    NitroSellOptions,
    PageCommerceConnector,
    PageCrawlOptions,
    PrestaShopConnector,
    PrestaShopOptions,
    ShopifyConnector,
    ShopifyOptions,
    ShopwareConnector,
    ShopwareOptions,
    StarwebConnector,
    StarwebOptions,
    SumUpConnector,
    SumUpOptions,
    WixConnector,
    WixOptions,
    WooCommerceConnector,
    WooCommerceOptions,
)
from mb_ceramics_catalogue.connectors.prestashop import declared_partition_keys
from mb_ceramics_catalogue.pipeline.budget import RequestBudget


class BigCommerceFetcherTransport:
    def __init__(
        self,
        fetcher: Any,
        *,
        maximum_response_bytes: int = DEFAULT_MAXIMUM_RESPONSE_BYTES,
    ) -> None:
        self.fetcher = fetcher
        self.maximum_response_bytes = _positive_response_limit(maximum_response_bytes)

    async def document(self, url: str, *, rendered: bool = False) -> str:
        if rendered:
            document = str(await self.fetcher.render(url, wait_ms=2500))
        else:
            document = str(await self.fetcher.text(url, browser_user_agent=True))
        return _bounded_text(document, maximum_bytes=self.maximum_response_bytes)

    async def request_json(
        self, url: str, *, headers: dict[str, str], body: dict[str, Any],
        browser_context_url: str | None = None,
    ) -> Any:
        if browser_context_url is not None:
            value = await self.fetcher.request_json_in_browser(
                browser_context_url, url, headers=headers, body=body
            )
            return _bounded_json_value(
                value,
                maximum_bytes=self.maximum_response_bytes,
            )
        response = await self.fetcher.response(
            url, method="POST", json_body=body, headers=headers, browser_user_agent=True
        )
        return _decode_json_bytes(
            bytes(response.content),
            maximum_bytes=self.maximum_response_bytes,
        )


class WixFetcherTransport:
    def __init__(
        self,
        fetcher: Any,
        *,
        maximum_response_bytes: int = DEFAULT_MAXIMUM_RESPONSE_BYTES,
    ) -> None:
        self.fetcher = fetcher
        self.maximum_response_bytes = _positive_response_limit(maximum_response_bytes)

    async def advertised_sitemaps(self, base_url: str) -> tuple[str, ...]:
        _, sitemaps = await self.fetcher.robots(base_url)
        return tuple(sitemaps)

    async def document(
        self, url: str, *, rendered: bool = False, accept: str | None = None
    ) -> str:
        if rendered:
            document = str(await self.fetcher.render(url))
            return _bounded_text(
                document,
                maximum_bytes=self.maximum_response_bytes,
            )
        if accept is not None:
            response = await self.fetcher.response(url, accept=accept)
            body = _bounded_bytes(
                bytes(response.content),
                maximum_bytes=self.maximum_response_bytes,
            )
            if body[:2] == b"\x1f\x8b":
                body = _decompress_gzip_bounded(
                    body,
                    maximum_bytes=self.maximum_response_bytes,
                )
            return body.decode(response.encoding or "utf-8", errors="replace")
        document = str(await self.fetcher.text(url, browser_user_agent=True))
        return _bounded_text(
            document,
            maximum_bytes=self.maximum_response_bytes,
        )


class PageFetcherTransport(WixFetcherTransport):
    def __init__(self, fetcher: Any, config: SourceConfig) -> None:
        super().__init__(fetcher)
        self.config = config

    async def allowed(self, url: str) -> bool:
        return bool(await self.fetcher.may_fetch(
            url, self.config.ignore_robots, self.config.obey_robots
        ))


class InteractiveFetcherTransport(PageFetcherTransport):
    async def evaluate(
        self, url: str, script: str, *, wait_for: str | None = None
    ) -> Any:
        return await self.fetcher.evaluate_in_browser(url, script, wait_for=wait_for)


def _positive_response_limit(value: int) -> int:
    if value < 1:
        raise ValueError("maximum_response_bytes must be positive")
    return value


def _bounded_bytes(content: bytes, *, maximum_bytes: int) -> bytes:
    received_bytes = len(content)
    if received_bytes > maximum_bytes:
        del content
        _raise_response_body_too_large(
            maximum_bytes=maximum_bytes,
            received_bytes=received_bytes,
        )
    return content


def _bounded_text(document: str, *, maximum_bytes: int) -> str:
    received_bytes = len(document.encode())
    if received_bytes > maximum_bytes:
        del document
        _raise_response_body_too_large(
            maximum_bytes=maximum_bytes,
            received_bytes=received_bytes,
        )
    return document


def _decode_json_bytes(content: bytes, *, maximum_bytes: int) -> Any:
    content = _bounded_bytes(content, maximum_bytes=maximum_bytes)
    failure: ValueError | None = None
    try:
        value = json.loads(content)
    except json.JSONDecodeError as error:
        failure = ValueError(
            f"response body is not valid JSON at line {error.lineno} column {error.colno}"
        )
    except UnicodeDecodeError:
        failure = ValueError("response body is not valid Unicode JSON")
    if failure is not None:
        del content
        raise failure
    return value


def _bounded_json_value(value: Any, *, maximum_bytes: int) -> Any:
    failure: ValueError | None = None
    try:
        encoded = json.dumps(value, separators=(",", ":")).encode()
    except (RecursionError, TypeError, ValueError):
        failure = ValueError("browser response did not contain a JSON-compatible value")
    if failure is not None:
        del value
        raise failure
    _bounded_bytes(encoded, maximum_bytes=maximum_bytes)
    return value


def _decompress_gzip_bounded(content: bytes, *, maximum_bytes: int) -> bytes:
    failure: ValueError | None = None
    decompressed = b""
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        decompressed = decompressor.decompress(content, maximum_bytes + 1)
        if len(decompressed) <= maximum_bytes:
            decompressed += decompressor.flush(maximum_bytes + 1 - len(decompressed))
        if not decompressor.eof:
            failure = ValueError("gzip response is incomplete or exceeds its retention limit")
    except zlib.error:
        failure = ValueError("gzip response is invalid")
    received_bytes = len(decompressed)
    del content, decompressor
    if received_bytes > maximum_bytes:
        del decompressed
        _raise_response_body_too_large(
            maximum_bytes=maximum_bytes,
            received_bytes=received_bytes,
        )
    if failure is not None:
        raise failure
    return decompressed


def _raise_response_body_too_large(
    *, maximum_bytes: int, received_bytes: int
) -> None:
    raise ResponseBodyTooLarge(
        maximum_bytes=maximum_bytes,
        received_bytes=received_bytes,
    )


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
    name: str
    connector_version: str
    options: dict[str, Any]
    partitions: tuple[str, ...]
    build: Callable[[Any, RequestBudget | None], CommerceConnector]
    extraction_method: str
    source_detail_level: str
    legacy_scraper_adapter: str
    ceramics_projection: dict[str, Any] = field(default_factory=dict)
    categories: tuple[str, ...] = ()
    collections: tuple[str, ...] = ()
    library_canary: LibraryCanaryRoute | None = None

    @property
    def dynamic_partitions(self) -> bool:
        """Whether discovery must register its exact partition keys durably."""
        return not self.partitions


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
        "shopify", ShopifyConnector.version, options.model_dump(mode="json"),
        collections or ("main",),
        lambda fetcher, budget: ShopifyConnector(fetcher, options, budget=budget),
        "api_json", "api", "shopify_connector",
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
        "woocommerce", WooCommerceConnector.version, options.model_dump(mode="json"),
        categories or ("main",),
        lambda fetcher, budget: WooCommerceConnector(fetcher, options, budget=budget),
        "api_json", "api", "woocommerce_connector",
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
        "bigcommerce", BigCommerceConnector.version, options.model_dump(mode="json"), ("main",),
        lambda fetcher, budget: BigCommerceConnector(
            BigCommerceFetcherTransport(fetcher), options, budget=budget
        ),
        "graphql", "api", "bigcommerce_connector",
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
        "wix", WixConnector.version, options.model_dump(mode="json"), ("main",),
        lambda fetcher, budget: WixConnector(
            WixFetcherTransport(fetcher), options, budget=budget
        ),
        "dom", "product_page", "wix_connector",
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
        sitemaps=tuple(config.sitemaps or ()),
        use_advertised_sitemaps=(True if config.use_advertised_sitemaps is None else config.use_advertised_sitemaps),
        category_urls=tuple(config.category_urls or ()),
        product_pattern=config.product_pattern,
        pagination_patterns=tuple(config.pagination_patterns or ()),
        card_links_only=bool(config.card_links_only),
        page_limit=config.page_limit or 500,
        category_page_limit=config.category_page_limit or 120,
        render=config.render, brand=config.brand, currency=config.currency,
        vat_status=config.vat_status,
        vat_rate=Decimal(str(config.vat_rate)) if config.vat_rate is not None else None,
        stock_from_quantity_maximum=(
            True if model is ShopwareOptions else bool(config.stock_from_quantity_maximum)
        ),
    )


def _specialized(config: SourceConfig, name: str) -> ConnectorRuntimePlan:
    definitions: dict[str, tuple[Any, Any, str]] = {
        "shopware": (ShopwareConnector, ShopwareOptions, "dom"),
        "starweb": (StarwebConnector, StarwebOptions, "dom"),
        "nitrosell": (NitroSellConnector, NitroSellOptions, "opengraph"),
    }
    connector_type, options_type, method = definitions[name]
    options = _specialized_options(options_type, config)
    sitemaps = tuple(options.sitemaps)
    partitions = _page_partitions(config, sitemaps=sitemaps)
    return ConnectorRuntimePlan(
        name, connector_type.version, options.model_dump(mode="json"),
        partitions,
        lambda fetcher, budget: connector_type(
            WixFetcherTransport(fetcher), options, budget=budget
        ),
        method, "product_page", f"{name}_connector",
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
        sitemaps=sitemaps, use_advertised_sitemaps=False,
        product_pattern=config.product_pattern, page_limit=config.page_limit or 500,
        render=config.render, brand=config.brand, currency=config.currency,
        vat_status=config.vat_status,
        vat_rate=Decimal(str(config.vat_rate)) if config.vat_rate is not None else None,
    )
    return ConnectorRuntimePlan(
        "sumup", SumUpConnector.version, options.model_dump(mode="json"), ("sitemap",),
        lambda fetcher, budget: SumUpConnector(
            WixFetcherTransport(fetcher), options, budget=budget
        ),
        "dom", "product_page", "sumup_connector",
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
    partitions = declared_partition_keys(options, config.url)
    return ConnectorRuntimePlan(
        "prestashop", PrestaShopConnector.version, options.model_dump(mode="json"),
        partitions,
        lambda fetcher, budget: PrestaShopConnector(
            PageFetcherTransport(fetcher, config), options, budget=budget
        ),
        "dom", "product_page", "sio2_connector" if sio2 else "prestashop_connector",
        ceramics_projection={"source_policy": "sio2"} if sio2 else {},
        library_canary=LibraryCanaryRoute(
            connector="prestashop",
            request_partitions=partitions,
        ),
    )


def _pagecommerce(config: SourceConfig) -> ConnectorRuntimePlan:
    options = PageCrawlOptions(
        sitemaps=tuple(config.sitemaps or ()), category_urls=tuple(config.category_urls or ()),
        use_advertised_sitemaps=(True if config.use_advertised_sitemaps is None
                                else config.use_advertised_sitemaps),
        product_pattern=config.product_pattern,
        pagination_patterns=tuple(config.pagination_patterns or ()),
        card_links_only=bool(config.card_links_only), page_limit=config.page_limit or 500,
        category_page_limit=config.category_page_limit or 120, render=config.render,
        brand=config.brand, currency=config.currency, vat_status=config.vat_status,
        vat_rate=Decimal(str(config.vat_rate)) if config.vat_rate is not None else None,
        stock_from_quantity_maximum=bool(config.stock_from_quantity_maximum),
    )
    partitions = _page_partitions(config, sitemaps=tuple(options.sitemaps))
    return ConnectorRuntimePlan(
        "pagecommerce", PageCommerceConnector.version, options.model_dump(mode="json"),
        partitions,
        lambda fetcher, budget: PageCommerceConnector(
            PageFetcherTransport(fetcher, config), options, budget=budget
        ),
        "structured", "product_page", "pagecrawl_connector",
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
        "axner",
        AxnerConnector.version,
        options.model_dump(mode="json"),
        ("main",),
        lambda fetcher, budget: AxnerConnector(
            PageFetcherTransport(fetcher, config), options, budget=budget
        ),
        "dom",
        "product_page",
        "axner_connector",
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
        "ceramicolours",
        CeramicoloursConnector.version,
        options.model_dump(mode="json"),
        ("main",),
        lambda fetcher, budget: CeramicoloursConnector(
            InteractiveFetcherTransport(fetcher, config), options, budget=budget
        ),
        "dom",
        "product_page",
        "ceramicolours_connector",
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
        "keramik_kraft",
        KeramikKraftConnector.version,
        options.model_dump(mode="json"),
        ("main",),
        lambda fetcher, budget: KeramikKraftConnector(
            PageFetcherTransport(fetcher, config), options, budget=budget
        ),
        "dom",
        "product_page",
        "keramik_kraft_connector",
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
