"""Public contracts. Importing this module performs no I/O or discovery."""

from .connectors import (
    BrowserRequirement,
    CommerceConnector,
    ConnectorCapabilities,
    ConnectorContext,
    ConnectorFactory,
    ConnectorRegistry,
    GenericPagesConnector,
    GenericPagesOptions,
    ShopifyConnector,
    ShopifyOptions,
    WooCommerceConnector,
    WooCommerceOptions,
)
from .models import (
    Availability,
    BrowserPolicy,
    CategoryRef,
    CollectionRequest,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    ConnectorCheckpoint,
    Diagnostic,
    DiagnosticCode,
    DiagnosticSeverity,
    DocumentRef,
    EntityPage,
    Evidence,
    FetchPolicy,
    MediaRef,
    Money,
    ProxyMode,
    ProxyPolicyConfig,
    RefreshMode,
    RobotsPolicy,
    SnapshotField,
    SourceDefinition,
    StockQuantityKind,
    StockState,
    collection_fingerprint,
)

__version__ = "0.1.0"
__all__ = [name for name in globals() if not name.startswith("_")]
