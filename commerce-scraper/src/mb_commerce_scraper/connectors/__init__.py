from .base import BrowserRequirement, CommerceConnector, ConnectorCapabilities, ConnectorContext
from .bigcommerce import BigCommerceConnector, BigCommerceFactory, BigCommerceOptions
from .factory import ConnectorFactory
from .generic_pages import (
    DiscoveryOptions,
    DomRules,
    GenericPagesConnector,
    GenericPagesFactory,
    GenericPagesOptions,
)
from .prestashop import PrestaShopConnector, PrestaShopFactory, PrestaShopOptions
from .registry import ConnectorRegistry, PluginLoadError, register_builtin_connectors
from .shopify import ShopifyConnector, ShopifyFactory, ShopifyOptions
from .specialized import (
    NitroSellConnector,
    NitroSellFactory,
    NitroSellOptions,
    ShopwareConnector,
    ShopwareFactory,
    ShopwareOptions,
    StarwebConnector,
    StarwebFactory,
    StarwebOptions,
    SumUpConnector,
    SumUpFactory,
    SumUpOptions,
)
from .wix import WixConnector, WixFactory, WixOptions
from .woocommerce import WooCommerceConnector, WooCommerceFactory, WooCommerceOptions

__all__ = [
    "BigCommerceConnector",
    "BigCommerceFactory",
    "BigCommerceOptions",
    "BrowserRequirement",
    "CommerceConnector",
    "ConnectorCapabilities",
    "ConnectorContext",
    "ConnectorFactory",
    "ConnectorRegistry",
    "DiscoveryOptions",
    "DomRules",
    "GenericPagesConnector",
    "GenericPagesFactory",
    "GenericPagesOptions",
    "NitroSellConnector",
    "NitroSellFactory",
    "NitroSellOptions",
    "PluginLoadError",
    "PrestaShopConnector",
    "PrestaShopFactory",
    "PrestaShopOptions",
    "ShopifyConnector",
    "ShopifyFactory",
    "ShopifyOptions",
    "ShopwareConnector",
    "ShopwareFactory",
    "ShopwareOptions",
    "StarwebConnector",
    "StarwebFactory",
    "StarwebOptions",
    "SumUpConnector",
    "SumUpFactory",
    "SumUpOptions",
    "WixConnector",
    "WixFactory",
    "WixOptions",
    "WooCommerceConnector",
    "WooCommerceFactory",
    "WooCommerceOptions",
    "register_builtin_connectors",
]
