"""Platform-neutral collection contracts.

The package is intentionally dependency-only at this stage: importing it does
not register a scraper, open a transport, or alter the legacy crawl path.
"""

from mb_commerce_scraper.models import (
    Availability,
    CategoryRef,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    DocumentRef,
    Evidence,
    MediaRef,
    Money,
    StockQuantityKind,
    StockState,
)

from .base import (
    BrowserBackendName,
    BrowserRequirement,
    CollectionRequest,
    CommerceConnector,
    ConnectorCapabilities,
    ConnectorCheckpoint,
    Diagnostic,
    DiagnosticCode,
    DiagnosticSeverity,
    EntityPage,
    RefreshMode,
    SnapshotField,
)
from .bespoke_pages import (
    AxnerConnector,
    AxnerOptions,
    CeramicoloursConnector,
    CeramicoloursOptions,
    KeramikKraftConnector,
    KeramikKraftOptions,
)
from .bigcommerce import BigCommerceConnector, BigCommerceOptions
from .page import DomFieldSelector, VerifiedDomRules
from .pagecommerce import (
    PageCommerceConnector,
    PageCrawlOptions,
    PageParseOutcome,
    ParserDisposition,
)
from .prestashop import PrestaShopConnector, PrestaShopOptions
from .shopify import ShopifyConnector, ShopifyOptions
from .specialized import (
    NitroSellConnector,
    NitroSellOptions,
    ShopwareConnector,
    ShopwareOptions,
    StarwebConnector,
    StarwebOptions,
    SumUpConnector,
    SumUpOptions,
)
from .wix import WixConnector, WixOptions
from .woocommerce import WooCommerceConnector, WooCommerceOptions

__all__ = [
    "Availability",
    "AxnerConnector",
    "AxnerOptions",
    "BigCommerceConnector",
    "BigCommerceOptions",
    "BrowserBackendName",
    "BrowserRequirement",
    "CategoryRef",
    "CeramicoloursConnector",
    "CeramicoloursOptions",
    "CollectionRequest",
    "CommerceConnector",
    "CommerceOffer",
    "CommerceProductSnapshot",
    "CommerceVariant",
    "ConnectorCapabilities",
    "ConnectorCheckpoint",
    "Diagnostic",
    "DiagnosticCode",
    "DiagnosticSeverity",
    "DocumentRef",
    "DomFieldSelector",
    "EntityPage",
    "Evidence",
    "KeramikKraftConnector",
    "KeramikKraftOptions",
    "MediaRef",
    "Money",
    "NitroSellConnector",
    "NitroSellOptions",
    "PageCommerceConnector",
    "PageCrawlOptions",
    "PageParseOutcome",
    "ParserDisposition",
    "PrestaShopConnector",
    "PrestaShopOptions",
    "RefreshMode",
    "ShopifyConnector",
    "ShopifyOptions",
    "ShopwareConnector",
    "ShopwareOptions",
    "SnapshotField",
    "StarwebConnector",
    "StarwebOptions",
    "StockQuantityKind",
    "StockState",
    "SumUpConnector",
    "SumUpOptions",
    "VerifiedDomRules",
    "WixConnector",
    "WixOptions",
    "WooCommerceConnector",
    "WooCommerceOptions",
]
