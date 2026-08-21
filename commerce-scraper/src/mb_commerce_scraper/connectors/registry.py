from __future__ import annotations

import re
from importlib import metadata
from typing import Any

from mb_commerce_scraper.transports import CommerceTransport

from .base import CommerceConnector, ConnectorContext
from .factory import ConnectorFactory

NAME = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")


class PluginLoadError(RuntimeError):
    pass


class ConnectorRegistry:
    def __init__(self) -> None:
        self._factories: dict[str, ConnectorFactory] = {}
        self.plugin_errors: list[PluginLoadError] = []

    @classmethod
    def with_builtins(cls) -> ConnectorRegistry:
        registry = cls()
        register_builtin_connectors(registry)
        return registry

    def register(self, factory: ConnectorFactory) -> None:
        name = factory.name.strip().lower().replace("_", "-")
        if not NAME.fullmatch(name):
            raise ValueError(f"invalid connector name: {factory.name!r}")
        if name != factory.name:
            raise ValueError(f"connector name must already be normalized as {name!r}")
        if name in self._factories:
            raise ValueError(f"connector {name!r} is already registered")
        self._factories[name] = factory

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))

    def options_schema(self, name: str) -> dict[str, Any]:
        return self._factory(name).options_model.model_json_schema()

    def build(
        self,
        name: str,
        *,
        transport: CommerceTransport,
        options: dict[str, Any],
        context: ConnectorContext,
    ) -> CommerceConnector:
        factory = self._factory(name)
        validated = factory.options_model.model_validate(options)
        return factory.build(transport=transport, options=validated, context=context)

    def load_entry_points(self, *, strict: bool = False) -> tuple[PluginLoadError, ...]:
        errors: list[PluginLoadError] = []
        points = metadata.entry_points(group="mb_commerce_scraper.connectors")
        for point in points:
            try:
                loaded = point.load()
                factory = loaded() if isinstance(loaded, type) else loaded
                self.register(factory)
            except Exception as error:
                package = point.dist.name if point.dist is not None else "unknown package"
                failure = PluginLoadError(f"plugin {package}:{point.name} failed: {type(error).__name__}: {error}")
                errors.append(failure)
                if strict:
                    raise failure from error
        self.plugin_errors.extend(errors)
        return tuple(errors)

    def _factory(self, name: str) -> ConnectorFactory:
        normalized = name.strip().lower().replace("_", "-")
        try:
            return self._factories[normalized]
        except KeyError:
            raise KeyError(f"unknown connector {normalized!r}; known: {', '.join(self.names())}") from None


def register_builtin_connectors(registry: ConnectorRegistry) -> None:
    from .generic_pages import GenericPagesFactory
    from .shopify import ShopifyFactory
    from .woocommerce import WooCommerceFactory

    registry.register(ShopifyFactory())
    registry.register(GenericPagesFactory())
    registry.register(WooCommerceFactory())
