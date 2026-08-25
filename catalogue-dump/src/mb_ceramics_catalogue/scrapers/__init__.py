"""Per-supplier scrapers for the ceramics catalogue dump.

Each source in sources.json names a scraper here. Platform scrapers cover a
whole storefront family through its public API; the named ones handle a single
supplier whose site needs its own rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from .base import Scraper


@dataclass(frozen=True)
class AdapterCapabilities:
    """Migration capabilities keyed by the stable legacy scraper name."""

    canary_adapter: str | None = None
    price_refresh: bool = False
    required_worker_capabilities: tuple[str, ...] = ()


ADAPTER_CAPABILITIES: dict[str, AdapterCapabilities] = {
    "shopify": AdapterCapabilities("shopify_connector", price_refresh=True),
    "woocommerce": AdapterCapabilities("woocommerce_connector", price_refresh=True),
    "bigcommerce": AdapterCapabilities("bigcommerce_connector", price_refresh=True),
    "wix": AdapterCapabilities("wix_connector"),
    "shopware": AdapterCapabilities("shopware_connector"),
    "sumup": AdapterCapabilities("sumup_connector"),
    "starweb": AdapterCapabilities("starweb_connector", price_refresh=True),
    "nitrosell": AdapterCapabilities("nitrosell_connector"),
    "prestashop": AdapterCapabilities("prestashop_connector"),
    "sio2": AdapterCapabilities("sio2_connector"),
    "pagecrawl": AdapterCapabilities("pagecrawl_connector"),
    "axner": AdapterCapabilities("axner_connector"),
    "ceramicolours": AdapterCapabilities(
        "ceramicolours_connector",
        required_worker_capabilities=("browser",),
    ),
    "keramik_kraft": AdapterCapabilities(
        "keramik_kraft_connector",
        price_refresh=True,
        required_worker_capabilities=("browser",),
    ),
}

# Shared local library shells are derived from the same migration metadata as
# their rollback adapters. This reverse map is also the shell's authority for
# restoring the stable source scraper identity.
LIBRARY_CANARY_SCRAPERS: dict[str, str] = {
    f"library_{capabilities.canary_adapter}": scraper
    for scraper, capabilities in ADAPTER_CAPABILITIES.items()
    if capabilities.canary_adapter is not None
}
CONNECTOR_CANARY_SCRAPERS: dict[str, str] = {
    capabilities.canary_adapter: scraper
    for scraper, capabilities in ADAPTER_CAPABILITIES.items()
    if capabilities.canary_adapter is not None
}
_LIBRARY_CONNECTOR_SCRAPER = ".library_connector:LibraryConnectorScraper"

#: scraper name -> "module:class", imported on demand.
#:
#: The module names are relative to this package. They used to be absolute
#: (`scrapers.shopify`), which resolved only because Python put the directory of
#: the running script on `sys.path` — the coincidence §4.1 of the plan is about.
#: Relative names resolve from an installed distribution instead, so a worker in
#: an image imports these the same way the CLI does.
REGISTRY: dict[str, str] = {
    "shopify": ".shopify:ShopifyScraper",
    # Explicit canary for the neutral connector/projector stack. Existing
    # sources remain on `shopify` until shadow parity is approved.
    "shopify_connector": ".shopify_connector:ShopifyConnectorScraper",
    "woocommerce": ".woocommerce:WooCommerceScraper",
    # Explicit neutral-contract canary; existing WooCommerce sources remain
    # on the legacy key until shadow parity is approved.
    "woocommerce_connector": ".woocommerce_connector:WooCommerceConnectorScraper",
    "bigcommerce": ".bigcommerce:BigCommerceScraper",
    "bigcommerce_connector": ".bigcommerce_connector:BigCommerceConnectorScraper",
    "prestashop": ".prestashop:PrestaShopScraper",
    "prestashop_connector": ".prestashop_connector:PrestaShopConnectorScraper",
    # generic json-ld page crawler, for storefronts with no public API.
    # Reads schema.org json-ld only; a microdata-only storefront yields nothing.
    "pagecrawl": ".pagecrawl:PageScraper",
    "pagecrawl_connector": ".pagecrawl_connector:PageCrawlConnectorScraper",
    # NitroSell publishes the whole product as OpenGraph meta; its
    # schema.org scope carries only the name, so pagecrawl finds no price.
    "nitrosell": ".nitrosell:NitroSellScraper",
    # AmeriCommerce with no feed and no structured data at all.
    "axner": ".axner:AxnerScraper",
    "axner_connector": ".bespoke_connectors:AxnerConnectorScraper",
    "sio2": ".prestashop:Sio2Scraper",
    "sio2_connector": ".prestashop_connector:Sio2ConnectorScraper",
    "wix": ".wix:WixScraper",
    "wix_connector": ".wix_connector:WixConnectorScraper",
    # SumUp Online Store: the listing API is disallowed, but each product page
    # streams the whole product in its React Server Components payload.
    "sumup": ".sumup:SumUpScraper",
    "shopware": ".shopware:ShopwareScraper",
    "starweb": ".starweb:StarwebScraper",
    "ceramicolours": ".ceramicolours:CeramicoloursScraper",
    "ceramicolours_connector": ".bespoke_connectors:CeramicoloursConnectorScraper",
    "keramik_kraft": ".keramik_kraft:KeramikKraftScraper",
    "keramik_kraft_connector": ".bespoke_connectors:KeramikKraftConnectorScraper",
    # These former connector-specific shells are now compatibility aliases for
    # the same registry/runtime/projection composition as local library canaries.
    "shopware_connector": _LIBRARY_CONNECTOR_SCRAPER,
    "sumup_connector": _LIBRARY_CONNECTOR_SCRAPER,
    "starweb_connector": _LIBRARY_CONNECTOR_SCRAPER,
    "nitrosell_connector": _LIBRARY_CONNECTOR_SCRAPER,
    # Local dump/probe canaries share one registry/runtime/projection shell.
    # Generated aliases remain available for parity while stable unsuffixed
    # scraper keys retain the independent rollback implementations.
    **dict.fromkeys(LIBRARY_CANARY_SCRAPERS, _LIBRARY_CONNECTOR_SCRAPER),
}


def adapter_capabilities(name: str) -> AdapterCapabilities:
    """Return declared migration behavior without branching at call sites."""
    if name.endswith("_connector"):
        name = name.removesuffix("_connector")
    return ADAPTER_CAPABILITIES.get(name, AdapterCapabilities())


def library_canary_alias(name: str) -> str:
    """Return the registered shared-library shell for a stable scraper name."""
    capabilities = ADAPTER_CAPABILITIES.get(name)
    if capabilities is None or capabilities.canary_adapter is None:
        raise KeyError(f"scraper {name!r} has no library canary adapter")
    return f"library_{capabilities.canary_adapter}"


def load(name: str) -> type[Scraper]:
    """Resolve a registry name to its Scraper class."""
    try:
        target = REGISTRY[name]
    except KeyError:
        raise KeyError(f"unknown scraper '{name}'; known: {', '.join(sorted(REGISTRY))}") from None
    module_name, class_name = target.split(":")
    return getattr(import_module(module_name, __name__), class_name)


def build(name: str, source_name: str, config: dict[str, Any], fetcher: Any) -> Scraper:
    return load(name)(source_name, config, fetcher)


def shared_edge(name: str) -> str | None:
    """The shared edge this scraper's shops answer from, or None.

    Asked by the worker before it runs a job, so that two shops behind one
    provider's edge do not both get crawled at once from the same address. The
    class attribute is the authority — `sio2` is a PrestaShop, and a caller
    reading the registry key rather than the platform would miss that.
    """
    from .base import SHARED_EDGES

    try:
        return SHARED_EDGES.get(load(name).platform)
    except KeyError:
        return None
