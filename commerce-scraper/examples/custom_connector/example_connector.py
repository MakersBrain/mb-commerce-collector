"""Minimal external connector/factory registered without global side effects."""

from collections.abc import AsyncIterator

from pydantic import BaseModel, ConfigDict

from mb_commerce_scraper import CollectionRequest, ConnectorCheckpoint, EntityPage
from mb_commerce_scraper.connectors import ConnectorCapabilities, ConnectorContext
from mb_commerce_scraper.models import RefreshMode, SnapshotField
from mb_commerce_scraper.transports import CommerceTransport


class ExampleOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    feed_path: str = "/products.json"


class ExampleConnector:
    name = "example"
    platform = "example"
    version = "1"
    capabilities = ConnectorCapabilities(snapshot_fields=frozenset(SnapshotField), refresh_modes=frozenset({RefreshMode.FULL}))

    async def collect(self, request: CollectionRequest, checkpoint: ConnectorCheckpoint | None = None) -> AsyncIterator[EntityPage]:
        del request, checkpoint
        yield EntityPage(page_id="terminal", sequence=0, items=(), terminal=True, discovered=0)


class ExampleFactory:
    name = "example"
    options_model = ExampleOptions

    def build(self, *, transport: CommerceTransport, options: BaseModel, context: ConnectorContext) -> ExampleConnector:
        del transport, options, context
        return ExampleConnector()


connector_factory = ExampleFactory()

