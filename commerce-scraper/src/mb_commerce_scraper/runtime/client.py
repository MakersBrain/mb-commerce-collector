from __future__ import annotations

from collections.abc import AsyncIterator

from mb_commerce_scraper.connectors import ConnectorContext, ConnectorRegistry
from mb_commerce_scraper.models import (
    CollectionRequest,
    CommerceProductSnapshot,
    ConnectorCheckpoint,
    EntityPage,
    RefreshMode,
    SnapshotField,
    SourceDefinition,
)
from mb_commerce_scraper.proxy import ProxyPool, ProxyRouting
from mb_commerce_scraper.transports import CommerceTransport
from mb_commerce_scraper.transports.base import RequestBudget, TelemetryHooks


class CommerceScraper:
    def __init__(
        self,
        *,
        registry: ConnectorRegistry,
        transport: CommerceTransport,
        proxy_pool: ProxyPool | None = None,
        routing: ProxyRouting | None = None,
        budget: RequestBudget | None = None,
        telemetry: TelemetryHooks | None = None,
        owns_transport: bool = False,
    ) -> None:
        self.registry = registry
        self.transport = transport
        self.proxy_pool = proxy_pool
        self.routing = routing
        self.budget = budget
        self.telemetry = telemetry
        self.owns_transport = owns_transport

    async def collect(
        self,
        source: SourceDefinition,
        *,
        requested_fields: frozenset[SnapshotField] = frozenset(SnapshotField),
        refresh_mode: RefreshMode = RefreshMode.FULL,
        result_limit: int | None = None,
        partitions: tuple[str, ...] = (),
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        context = ConnectorContext(budget=self.budget, telemetry=self.telemetry)
        connector = self.registry.build(source.connector, transport=self.transport, options=source.connector_options, context=context)
        request = CollectionRequest(
            source_id=source.id, base_url=source.base_url, refresh_mode=refresh_mode,
            requested_fields=requested_fields, result_limit=result_limit, partitions=partitions,
        )
        async for page in connector.collect(request, checkpoint):
            yield page

    async def __aenter__(self) -> CommerceScraper:
        return self

    async def __aexit__(self, *_: object) -> None:
        if self.owns_transport and hasattr(self.transport, "aclose"):
            await self.transport.aclose()  # type: ignore[attr-defined, unused-ignore]
