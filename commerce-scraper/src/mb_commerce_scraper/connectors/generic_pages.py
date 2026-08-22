from __future__ import annotations

import re
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Literal, cast
from urllib.parse import urljoin, urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from mb_commerce_scraper.discovery import DiscoveryFailure, DiscoveryStrategy
from mb_commerce_scraper.models import (
    CollectionRequest,
    CommerceProductSnapshot,
    ConnectorCheckpoint,
    RefreshMode,
    SnapshotField,
    StockQuantityKind,
    collection_fingerprint,
)
from mb_commerce_scraper.parsing import ProductParser
from mb_commerce_scraper.transports import CommerceTransport

from .base import BrowserRequirement, ConnectorCapabilities, ConnectorContext
from .specialized import SpecializedPageConnector, SpecializedPageOptions
from .specialized_parsing import DomFieldSelector, VerifiedDomRules


class DiscoveryOptions(BaseModel):
    """Safe, bounded product-page discovery configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    sitemaps: tuple[str, ...] = ()
    use_advertised_sitemaps: bool = True
    category_urls: tuple[str, ...] = ()
    product_pattern: str | None = None
    pagination_patterns: tuple[str, ...] = ()
    card_links_only: bool = False
    sitemap_limit: int = Field(default=100, ge=1, le=10_000)
    category_page_limit: int = Field(default=120, ge=1, le=10_000)


class DomRules(BaseModel):
    """Verified, data-only selectors that never enter a CSS/JS engine."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    verification: tuple[str | DomFieldSelector, ...] = ()
    name: str | DomFieldSelector
    price: str | DomFieldSelector | None = None
    currency: str | DomFieldSelector | None = None
    description: str | DomFieldSelector | None = None
    sku: str | DomFieldSelector | None = None
    image: str | DomFieldSelector | None = None
    availability: str | DomFieldSelector | None = None

    @model_validator(mode="after")
    def _selectors_are_supported(self) -> DomRules:
        self.as_verified()
        return self

    def as_verified(self) -> VerifiedDomRules:
        name = _dom_selector(self.name)
        verification = tuple(_dom_selector(value) for value in self.verification)
        return VerifiedDomRules(
            verification=verification or (name,),
            name=name,
            price=_optional_dom_selector(self.price),
            currency=_optional_dom_selector(self.currency),
            description=_optional_dom_selector(self.description),
            sku=_optional_dom_selector(self.sku),
            image=_optional_dom_selector(self.image),
            availability=_optional_dom_selector(self.availability),
        )


class GenericPagesOptions(BaseModel):
    """Declarative configuration for a generic structured-data shop."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    discovery: DiscoveryOptions = Field(default_factory=DiscoveryOptions)
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld",
    )
    dom_rules: DomRules | None = None
    currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    brand: str | None = None
    vat_status: Literal["inclusive", "exclusive", "unknown"] | None = None
    vat_rate: Decimal | None = Field(default=None, ge=0, le=1)
    stock_from_quantity_maximum: bool = False
    page_limit: int = Field(default=10_000, ge=1)
    render: bool | None = None
    browser_zero_gain_limit: int = Field(default=10, ge=1)

    @model_validator(mode="after")
    def _dom_parser_is_explicit(self) -> GenericPagesOptions:
        if "dom" in self.parsers and self.dom_rules is None:
            raise ValueError("the dom parser requires dom_rules")
        if self.dom_rules is not None and "dom" not in self.parsers:
            raise ValueError("dom_rules require the dom parser")
        return self

    def as_page_options(self) -> SpecializedPageOptions:
        """Project the public nested schema into the shared page-collection engine."""

        discovery = self.discovery
        return SpecializedPageOptions(
            sitemaps=discovery.sitemaps,
            use_advertised_sitemaps=discovery.use_advertised_sitemaps,
            category_urls=discovery.category_urls,
            product_pattern=discovery.product_pattern,
            pagination_patterns=discovery.pagination_patterns,
            card_links_only=discovery.card_links_only,
            sitemap_limit=discovery.sitemap_limit,
            category_page_limit=discovery.category_page_limit,
            parsers=self.parsers,
            dom_rules=self.dom_rules.as_verified() if self.dom_rules else None,
            currency=self.currency,
            brand=self.brand,
            vat_status=self.vat_status,
            vat_rate=self.vat_rate,
            stock_from_quantity_maximum=self.stock_from_quantity_maximum,
            page_limit=self.page_limit,
            render=self.render,
            browser_zero_gain_limit=self.browser_zero_gain_limit,
        )


def _dom_selector(value: str | DomFieldSelector) -> DomFieldSelector:
    return value if isinstance(value, DomFieldSelector) else DomFieldSelector(selector=value)


def _optional_dom_selector(
    value: str | DomFieldSelector | None,
) -> DomFieldSelector | None:
    return _dom_selector(value) if value is not None else None


class GenericPagesConnector(SpecializedPageConnector):
    """Generic connector using the shared, checkpoint-safe page collection engine."""

    name = "generic-pages"
    platform = "generic"
    version = "2"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        stock_kinds=frozenset({StockQuantityKind.EXACT, StockQuantityKind.UNKNOWN}),
        supports_documents=True,
        browser=BrowserRequirement.OPTIONAL,
    )

    def __init__(
        self,
        transport: CommerceTransport,
        options: GenericPagesOptions,
        context: ConnectorContext | None = None,
        *,
        parser: ProductParser | None = None,
        discovery: DiscoveryStrategy | None = None,
    ) -> None:
        self.generic_options = options
        self._parser_override = parser
        self._parser_identity = (
            _strategy_identity(parser, kind="parser") if parser is not None else None
        )
        self._discovery_override = discovery
        self._discovery_identity = (
            _strategy_identity(discovery, kind="discovery")
            if discovery is not None
            else None
        )
        super().__init__(transport, options.as_page_options(), context)

    async def _discover(self, base_url: str) -> AsyncIterator[str]:
        if self._discovery_override is None:
            async for url in super()._discover(base_url):
                yield url
            return

        emitted: set[str] = set()
        candidates = 0
        maximum_candidates = max(
            self.options.page_limit,
            self.options.sitemap_limit,
            self.options.category_page_limit,
        ) + 1
        async for candidate in self._discovery_override.discover(base_url):
            if self.context.cancelled():
                return
            candidates += 1
            if candidates > maximum_candidates:
                raise DiscoveryFailure(
                    f"custom discovery candidate limit {maximum_candidates} reached",
                    retryable=False,
                )
            url = _safe_discovered_url(base_url, candidate)
            if not self._is_product(url, base_url) or url in emitted:
                continue
            emitted.add(url)
            yield url
            # One look-ahead URL is enough for the shared page-limit path to
            # produce its established resumable incomplete page.
            if len(emitted) > self.options.page_limit:
                return

    def parse(
        self, document: str, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        if self._parser_override is not None:
            return tuple(
                snapshot.model_copy(update={"connector": self.name})
                for snapshot in self._parser_override.parse(
                    document, url=url, source_id=source_id
                )
            )
        return super().parse(document, url, source_id)

    def checkpoint(
        self, request: CollectionRequest, lineage: str, resume_after: JsonValue
    ) -> ConnectorCheckpoint:
        return ConnectorCheckpoint(
            connector=self.name,
            connector_version=self.version,
            source_id=request.source_id,
            lineage=lineage,
            collection_fingerprint=collection_fingerprint(
                request, self.name, self._checkpoint_options()
            ),
            resume_after=resume_after,
        )

    def _checkpoint_options(self) -> dict[str, JsonValue]:
        options = cast(
            dict[str, JsonValue], self.generic_options.model_dump(mode="json")
        )
        if self._parser_identity is not None:
            options["custom_parser"] = self._parser_identity
        if self._discovery_identity is not None:
            options["custom_discovery"] = self._discovery_identity
        return options

    def _partition_key(self) -> str:
        if self._discovery_identity is not None:
            return f"strategy:{self._discovery_identity['name']}"
        return super()._partition_key()


def _strategy_identity(
    strategy: ProductParser | DiscoveryStrategy, *, kind: str
) -> dict[str, JsonValue]:
    identity: dict[str, JsonValue] = {}
    for field in ("name", "version"):
        value = getattr(strategy, field, None)
        if not isinstance(value, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._+-]{0,127}", value
        ):
            raise ValueError(
                f"custom {kind} {field} must be a stable 1..128 character identifier"
            )
        identity[field] = value
    return identity


def _safe_discovered_url(base_url: str, candidate: object) -> str:
    if not isinstance(candidate, str) or not candidate:
        raise DiscoveryFailure(
            "custom discovery yielded an invalid product URL", retryable=False
        )
    url = urljoin(base_url, candidate)
    try:
        base = urlsplit(base_url)
        parsed = urlsplit(url)
        base_port = base.port or (443 if base.scheme.casefold() == "https" else 80)
        candidate_port = parsed.port or (
            443 if parsed.scheme.casefold() == "https" else 80
        )
    except ValueError:
        raise DiscoveryFailure(
            "custom discovery yielded an invalid product URL", retryable=False
        ) from None
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or (
            parsed.scheme.casefold(),
            parsed.hostname.casefold(),
            candidate_port,
        )
        != (base.scheme.casefold(), (base.hostname or "").casefold(), base_port)
    ):
        raise DiscoveryFailure(
            "custom discovery yielded an off-origin product URL", retryable=False
        )
    return urlunsplit((base.scheme, base.netloc, parsed.path or "/", parsed.query, ""))


class GenericPagesFactory:
    name = "generic-pages"
    version = GenericPagesConnector.version
    options_model: type[BaseModel] = GenericPagesOptions

    def __init__(
        self,
        *,
        parser: ProductParser | None = None,
        discovery: DiscoveryStrategy | None = None,
    ) -> None:
        # Validate before registration/build so invalid plugin composition
        # fails without opening a transport or retaining strategy details.
        if parser is not None:
            _strategy_identity(parser, kind="parser")
        if discovery is not None:
            _strategy_identity(discovery, kind="discovery")
        self._parser = parser
        self._discovery = discovery

    def build(
        self,
        *,
        transport: CommerceTransport,
        options: BaseModel,
        context: ConnectorContext,
    ) -> GenericPagesConnector:
        return GenericPagesConnector(
            transport,
            GenericPagesOptions.model_validate(options),
            context,
            parser=self._parser,
            discovery=self._discovery,
        )
