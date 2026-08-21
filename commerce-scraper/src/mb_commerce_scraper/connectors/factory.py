from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel

from mb_commerce_scraper.transports import CommerceTransport

from .base import CommerceConnector, ConnectorContext


class ConnectorFactory(Protocol):
    name: str
    options_model: type[BaseModel]

    def build(
        self, *, transport: CommerceTransport, options: BaseModel, context: ConnectorContext
    ) -> CommerceConnector: ...

