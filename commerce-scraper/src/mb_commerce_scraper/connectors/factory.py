from __future__ import annotations

from collections.abc import Callable
from typing import Generic, Protocol, TypeVar

from pydantic import BaseModel

from mb_commerce_scraper.transports import CommerceTransport

from .base import CommerceConnector, ConnectorContext


class ConnectorFactory(Protocol):
    name: str
    version: str

    @property
    def options_model(self) -> type[BaseModel]: ...

    def build(
        self, *, transport: CommerceTransport, options: BaseModel, context: ConnectorContext
    ) -> CommerceConnector: ...


OptionsT = TypeVar("OptionsT", bound=BaseModel)
ConnectorT = TypeVar("ConnectorT", bound=CommerceConnector)


def validated_options(
    options: BaseModel, options_model: type[OptionsT], *, factory_name: str
) -> OptionsT:
    """Narrow registry-validated options without validating the model twice."""
    if not isinstance(options, options_model):
        raise TypeError(
            f"{factory_name} factory requires validated {options_model.__name__} options"
        )
    return options


class SimpleConnectorFactory(Generic[OptionsT, ConnectorT]):
    """Factory for connectors whose constructor needs no plugin-owned state."""

    name: str
    version: str
    options_model: type[OptionsT]
    connector_type: Callable[[CommerceTransport, OptionsT, ConnectorContext], ConnectorT]

    def build(
        self,
        *,
        transport: CommerceTransport,
        options: BaseModel,
        context: ConnectorContext,
    ) -> ConnectorT:
        return self.connector_type(
            transport,
            validated_options(options, self.options_model, factory_name=self.name),
            context,
        )
