from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

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
from mb_commerce_scraper.proxy import ProxyPool, ProxyRouting, ProxyTransportFactory, RoutedTransport
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
        proxy_transport_factory: ProxyTransportFactory | None = None,
        proxy_maximum_bytes: int | None = None,
        budget: RequestBudget | None = None,
        telemetry: TelemetryHooks | None = None,
        owns_transport: bool = False,
    ) -> None:
        self.registry = registry
        self.transport = transport
        self.proxy_pool = proxy_pool
        self.routing = routing
        self.proxy_transport_factory = proxy_transport_factory
        self.proxy_maximum_bytes = proxy_maximum_bytes
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
        deadline: datetime | None = None,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        attempt_transport: CommerceTransport = self.transport
        routed: RoutedTransport | None = None
        if self.proxy_pool is not None or self.routing is not None:
            if self.proxy_pool is None or self.proxy_transport_factory is None:
                raise ValueError("proxy routing requires both proxy_pool and proxy_transport_factory")
            routed = RoutedTransport(
                self.transport,
                pool=self.proxy_pool,
                proxy_factory=self.proxy_transport_factory,
                routing=self.routing or ProxyRouting(),
                source_id=source.id,
                base_url=source.base_url,
                maximum_bytes=self.proxy_maximum_bytes,
            )
            attempt_transport = routed
        context = ConnectorContext(budget=self.budget, telemetry=self.telemetry)
        connector = self.registry.build(source.connector, transport=attempt_transport, options=source.connector_options, context=context)
        request = CollectionRequest(
            source_id=source.id, base_url=source.base_url, refresh_mode=refresh_mode,
            requested_fields=requested_fields, result_limit=result_limit, partitions=partitions,
            deadline=deadline,
        )
        try:
            if deadline is None:
                async for page in connector.collect(request, checkpoint):
                    yield page
            else:
                if deadline.tzinfo is None:
                    raise ValueError("collection deadline must include a timezone")
                remaining = max(0.0, (deadline - datetime.now(UTC)).total_seconds())
                async with asyncio.timeout(remaining):
                    async for page in connector.collect(request, checkpoint):
                        yield page
        finally:
            if routed is not None:
                await routed.aclose()

    async def __aenter__(self) -> CommerceScraper:
        return self

    async def __aexit__(self, *_: object) -> None:
        if self.owns_transport and hasattr(self.transport, "aclose"):
            await self.transport.aclose()  # type: ignore[attr-defined, unused-ignore]
