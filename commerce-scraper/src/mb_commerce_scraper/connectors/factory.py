from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar, cast

from pydantic import BaseModel

from mb_commerce_scraper.transports import CommerceTransport

from .base import (
    BrowserRequirement,
    CommerceConnector,
    ConnectorCapabilities,
    ConnectorContext,
)


@dataclass(frozen=True, slots=True)
class ConnectorPlan:
    """Validated collection topology and transport needs for one connector."""

    partitions: tuple[str, ...] = ()
    dynamic_partitions: bool = False
    browser: BrowserRequirement = BrowserRequirement.NEVER

    def __post_init__(self) -> None:
        if not isinstance(self.partitions, tuple) or not all(
            isinstance(value, str) for value in self.partitions
        ):
            raise TypeError("connector plan partitions must be a tuple of strings")
        if any(not value or value != value.strip() for value in self.partitions):
            raise ValueError("connector plan partitions must be normalized non-empty strings")
        if len(set(self.partitions)) != len(self.partitions):
            raise ValueError("connector plan partitions must be unique")
        if not isinstance(self.dynamic_partitions, bool):
            raise TypeError("connector plan dynamic_partitions must be a boolean")
        if not isinstance(self.browser, BrowserRequirement):
            raise TypeError("connector plan browser must be a BrowserRequirement")


class ConnectorFactory(Protocol):
    name: str
    version: str

    @property
    def options_model(self) -> type[BaseModel]: ...

    def build(
        self, *, transport: CommerceTransport, options: BaseModel, context: ConnectorContext
    ) -> CommerceConnector: ...

    def plan(
        self,
        options: BaseModel,
        *,
        base_url: str,
        request_partitions: tuple[str, ...] = (),
    ) -> ConnectorPlan: ...


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

    def plan(
        self,
        options: BaseModel,
        *,
        base_url: str,
        request_partitions: tuple[str, ...] = (),
    ) -> ConnectorPlan:
        validated = validated_options(
            options, self.options_model, factory_name=self.name
        )
        return ConnectorPlan(
            partitions=self._plan_partitions(
                validated,
                base_url=base_url,
                request_partitions=request_partitions,
            ),
            dynamic_partitions=self._plan_dynamic_partitions(
                validated, request_partitions=request_partitions
            ),
            browser=self._plan_browser(validated),
        )

    def _plan_partitions(
        self,
        options: OptionsT,
        *,
        base_url: str,
        request_partitions: tuple[str, ...],
    ) -> tuple[str, ...]:
        del options, base_url, request_partitions
        return ()

    def _plan_dynamic_partitions(
        self, options: OptionsT, *, request_partitions: tuple[str, ...]
    ) -> bool:
        del options, request_partitions
        return False

    def _plan_browser(self, options: OptionsT) -> BrowserRequirement:
        del options
        capabilities = cast(
            ConnectorCapabilities | None,
            getattr(self.connector_type, "capabilities", None),
        )
        if capabilities is None:
            raise TypeError(f"{self.name} connector type does not declare capabilities")
        return capabilities.browser

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
