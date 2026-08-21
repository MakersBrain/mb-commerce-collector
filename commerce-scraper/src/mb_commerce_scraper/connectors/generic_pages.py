from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from mb_commerce_scraper.discovery import SitemapDiscovery
from mb_commerce_scraper.models import (
    CollectionRequest,
    CommerceProductSnapshot,
    ConnectorCheckpoint,
    Diagnostic,
    DiagnosticCode,
    DiagnosticSeverity,
    EntityPage,
    RefreshMode,
    SnapshotField,
    collection_fingerprint,
    result_limit_diagnostic,
    validate_checkpoint,
)
from mb_commerce_scraper.parsing import JsonLdProductParser, ProductParser
from mb_commerce_scraper.transports import (
    CommerceTransport,
    RequestPriority,
    RequestPurpose,
    TransportRequest,
)

from .base import BrowserRequirement, CommerceConnector, ConnectorCapabilities, ConnectorContext


class DiscoveryOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    sitemaps: tuple[str, ...] = ("/sitemap.xml",)
    product_pattern: str | None = None
    sitemap_limit: int = Field(default=100, ge=1, le=10_000)


class DomRules(BaseModel):
    """Reserved verified selectors; data-only and never executed as code."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str | None = None
    price: str | None = None
    sku: str | None = None


class GenericPagesOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    discovery: DiscoveryOptions = DiscoveryOptions()
    parsers: tuple[Literal["jsonld"], ...] = ("jsonld",)
    dom_rules: DomRules | None = None
    currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    page_limit: int = Field(default=10_000, ge=1)


class GenericPagesConnector(CommerceConnector):
    name = "generic-pages"
    platform = "generic"
    version = "1"
    capabilities = ConnectorCapabilities(
        snapshot_fields=frozenset(SnapshotField),
        refresh_modes=frozenset({RefreshMode.FULL}),
        browser=BrowserRequirement.OPTIONAL,
    )

    def __init__(self, transport: CommerceTransport, options: GenericPagesOptions, context: ConnectorContext | None = None, *, parser: ProductParser | None = None) -> None:
        self.transport = transport
        self.options = options
        self.context = context or ConnectorContext()
        self.parser = parser or JsonLdProductParser(currency=options.currency)

    async def collect(self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None = None) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        options = cast(dict[str, JsonValue], self.options.model_dump(mode="json"))
        validate_checkpoint(checkpoint, connector=self.name, connector_version=self.version, request=request, options=options)
        resume_url = self._resume(checkpoint)
        discovery = SitemapDiscovery(
            self.transport, self.options.discovery.sitemaps,
            product_pattern=self.options.discovery.product_pattern,
            limit=self.options.discovery.sitemap_limit,
        )
        emitted = 0
        sequence = 0
        skipping = resume_url is not None
        async for url in discovery.discover(request.base_url):
            if skipping:
                if url == resume_url:
                    skipping = False
                continue
            if self.context.cancelled():
                return
            if sequence >= self.options.page_limit:
                diagnostic = Diagnostic(code=DiagnosticCode.ENUMERATION_INCOMPLETE, severity=DiagnosticSeverity.WARNING, message=f"page limit {self.options.page_limit} reached", retryable=False, affects_completeness=True, url=url)
                yield EntityPage(page_id=f"product:{sequence}", sequence=sequence, items=(), resume_after={"after_url": url}, terminal=True, enumeration_intact=False, discovered=sequence, diagnostics=(diagnostic,))
                return
            response = await self.transport.request(TransportRequest(url=url, purpose=RequestPurpose.ENTITY, priority=RequestPriority.IDENTITY, estimated_bytes=500_000))
            if response.status >= 400:
                diagnostic = Diagnostic(code=DiagnosticCode.ENTITY_FETCH_FAILED, severity=DiagnosticSeverity.WARNING, message=f"product request failed with status {response.status}", retryable=response.status >= 500, affects_completeness=False, url=url)
                snapshots: tuple[CommerceProductSnapshot, ...] = ()
                diagnostics: tuple[Diagnostic, ...] = (diagnostic,)
            else:
                snapshots = self.parser.parse(response.text(), url=url, source_id=request.source_id)
                diagnostics = () if snapshots else (Diagnostic(code=DiagnosticCode.PARSER_UNSUPPORTED, severity=DiagnosticSeverity.WARNING, message="no configured parser recognized a product", retryable=False, affects_completeness=False, url=url),)
            remaining = None if request.result_limit is None else request.result_limit - emitted
            selected = snapshots if remaining is None else snapshots[:remaining]
            emitted += len(selected)
            limited = request.result_limit is not None and emitted >= request.result_limit
            yield EntityPage(
                page_id=f"product:{sequence}", partition_key="sitemap", sequence=sequence,
                items=selected, resume_after={"after_url": url}, terminal=limited,
                enumeration_intact=not limited, discovered=len(snapshots),
                diagnostics=diagnostics + ((result_limit_diagnostic(request.result_limit, url),) if limited and request.result_limit else ()),
            )
            sequence += 1
            if limited:
                return
        yield EntityPage(page_id="sitemap:terminal", partition_key="sitemap", sequence=sequence, items=(), terminal=True, partition_terminal=True, discovered=0)

    def checkpoint(self, request: CollectionRequest, lineage: str, resume_after: JsonValue) -> ConnectorCheckpoint:
        options = cast(dict[str, JsonValue], self.options.model_dump(mode="json"))
        return ConnectorCheckpoint(connector=self.name, connector_version=self.version, source_id=request.source_id, lineage=lineage, collection_fingerprint=collection_fingerprint(request, self.name, options), resume_after=resume_after)

    @staticmethod
    def _resume(checkpoint: ConnectorCheckpoint | None) -> str | None:
        if checkpoint is None:
            return None
        cursor = checkpoint.resume_after
        if not isinstance(cursor, dict) or not isinstance(cursor.get("after_url"), str):
            raise ValueError("CHECKPOINT_INVALID: generic-pages cursor is invalid")
        return str(cursor["after_url"])


class GenericPagesFactory:
    name = "generic-pages"
    options_model: type[BaseModel] = GenericPagesOptions

    def build(self, *, transport: CommerceTransport, options: BaseModel, context: ConnectorContext) -> GenericPagesConnector:
        return GenericPagesConnector(transport, GenericPagesOptions.model_validate(options), context)
