from __future__ import annotations

import re
from collections.abc import AsyncIterator
from typing import Literal, cast
from urllib.parse import urljoin, urlsplit, urlunsplit

from pydantic import BaseModel, JsonValue

from mb_commerce_scraper.discovery import DiscoveryFailure, DiscoveryStrategy
from mb_commerce_scraper.models import (
    CollectionRequest,
    CommerceProductSnapshot,
    ConnectorCheckpoint,
    RefreshMode,
    SnapshotField,
    StockQuantityKind,
    build_checkpoint,
)
from mb_commerce_scraper.parsing import ProductParser
from mb_commerce_scraper.transports import CommerceTransport

from .base import BrowserRequirement, ConnectorCapabilities, ConnectorContext
from .factory import ConnectorPlan, validated_options
from .page_engine import (
    DiscoveryOptions as DiscoveryOptions,
)
from .page_engine import (
    DomRules as DomRules,
)
from .page_engine import (
    PageEngineConnector,
    PageEngineOptions,
    page_engine_plan,
)


class GenericPagesOptions(PageEngineOptions):
    """Declarative configuration for a generic structured-data shop."""

    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld",
    )


class GenericPagesConnector(PageEngineConnector):
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
        super().__init__(transport, options, context)

    async def _discover(self, base_url: str) -> AsyncIterator[str]:
        if self._discovery_override is None:
            async for url in super()._discover(base_url):
                yield url
            return

        emitted: set[str] = set()
        candidates = 0
        maximum_candidates = max(
            self.options.page_limit,
            self.options.discovery.sitemap_limit,
            self.options.discovery.category_page_limit,
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
        return build_checkpoint(
            connector=self.name,
            connector_version=self.version,
            request=request,
            lineage=lineage,
            resume_after=resume_after,
            options=self._checkpoint_options(),
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
            validated_options(options, GenericPagesOptions, factory_name=self.name),
            context,
            parser=self._parser,
            discovery=self._discovery,
        )

    def plan(
        self,
        options: BaseModel,
        *,
        base_url: str,
        request_partitions: tuple[str, ...] = (),
    ) -> ConnectorPlan:
        del base_url, request_partitions
        validated = validated_options(
            options, GenericPagesOptions, factory_name=self.name
        )
        identity = (
            _strategy_identity(self._discovery, kind="discovery")["name"]
            if self._discovery is not None
            else None
        )
        assert isinstance(identity, str) or identity is None
        return page_engine_plan(validated, custom_discovery=identity)
