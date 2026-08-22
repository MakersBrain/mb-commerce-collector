from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import BaseModel

from mb_commerce_scraper.connectors import (
    ConnectorContext,
    ConnectorRegistry,
    PluginLoadError,
    ShopifyFactory,
)
from mb_commerce_scraper.connectors import registry as registry_module
from mb_commerce_scraper.testing import FakeTransport


def test_registry_is_isolated_and_does_not_instantiate_for_listing() -> None:
    first = ConnectorRegistry.with_builtins()
    second = ConnectorRegistry()
    assert first.names() == (
        "bigcommerce",
        "generic-pages",
        "nitrosell",
        "prestashop",
        "shopify",
        "shopware",
        "starweb",
        "sumup",
        "wix",
        "woocommerce",
    )
    assert second.names() == ()
    assert first.options_schema("shopify")["additionalProperties"] is False


def test_duplicate_and_non_normalized_names_fail() -> None:
    registry = ConnectorRegistry()
    registry.register(ShopifyFactory())
    with pytest.raises(ValueError, match="already registered"):
        registry.register(ShopifyFactory())

    class BadFactory:
        name = "Bad_Name"
        options_model = BaseModel

        def build(self, **_: object) -> object:
            raise AssertionError("must not build")

    with pytest.raises(ValueError, match="normalized"):
        registry.register(BadFactory())  # type: ignore[arg-type]


def test_builtin_factory_metadata_matches_built_connector_versions() -> None:
    registry = ConnectorRegistry.with_builtins()
    transport = FakeTransport()

    for name in registry.names():
        connector = registry.build(
            name,
            transport=transport,
            options={},
            context=ConnectorContext(),
        )
        assert registry.connector_version(name) == connector.version


def test_factory_version_is_required_and_checked_against_built_connector() -> None:
    class MissingVersionFactory:
        name = "missing-version"
        options_model = BaseModel

        def build(self, **_: object) -> object:
            raise AssertionError("must not build")

    with pytest.raises(ValueError, match="must declare"):
        ConnectorRegistry().register(MissingVersionFactory())  # type: ignore[arg-type]

    class DriftingShopifyFactory(ShopifyFactory):
        name = "drifting-shopify"
        version = "999"

    registry = ConnectorRegistry()
    registry.register(DriftingShopifyFactory())
    with pytest.raises(ValueError, match="factory declares"):
        registry.build(
            "drifting-shopify",
            transport=FakeTransport(),
            options={},
            context=ConnectorContext(),
        )


class EntryPoint:
    def __init__(
        self,
        name: str,
        loaded: Any = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.name = name
        self.dist = SimpleNamespace(name="fixture-package")
        self._loaded = loaded
        self._error = error

    def load(self) -> Any:
        if self._error is not None:
            raise self._error
        return self._loaded


def test_plugin_loading_is_explicit_isolated_and_secret_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    points = [
        EntryPoint("shopify", ShopifyFactory),
        EntryPoint("broken", error=RuntimeError("token=private")),
    ]
    calls: list[str] = []

    def entry_points(*, group: str) -> list[EntryPoint]:
        calls.append(group)
        return points

    monkeypatch.setattr(registry_module.metadata, "entry_points", entry_points)
    registry = ConnectorRegistry()

    errors = registry.load_entry_points()

    assert calls == ["mb_commerce_scraper.connectors"]
    assert registry.names() == ("shopify",)
    assert errors == tuple(registry.plugin_errors)
    assert len(errors) == 1
    assert str(errors[0]) == "plugin fixture-package:broken failed: RuntimeError"
    assert "private" not in str(errors[0])


def test_strict_plugin_loading_raises_at_the_broken_plugin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = RuntimeError("plugin initialization failed")

    def entry_points(*, group: str) -> list[EntryPoint]:
        del group
        return [EntryPoint("broken", error=failure)]

    monkeypatch.setattr(
        registry_module.metadata,
        "entry_points",
        entry_points,
    )

    with pytest.raises(PluginLoadError) as raised:
        ConnectorRegistry().load_entry_points(strict=True)

    assert raised.value.__cause__ is failure


def test_duplicate_and_invalid_entry_point_plugins_are_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class InvalidFactory:
        name = "Invalid Name"
        version = "1"
        options_model = BaseModel

        def build(self, **_: object) -> object:
            raise AssertionError("invalid plugin must not build")

    points = [
        EntryPoint("valid", ShopifyFactory),
        EntryPoint("duplicate", ShopifyFactory),
        EntryPoint("invalid", InvalidFactory),
    ]

    def entry_points(*, group: str) -> list[EntryPoint]:
        del group
        return points

    monkeypatch.setattr(
        registry_module.metadata,
        "entry_points",
        entry_points,
    )
    registry = ConnectorRegistry()

    errors = registry.load_entry_points()

    assert registry.names() == ("shopify",)
    assert [str(error) for error in errors] == [
        "plugin fixture-package:duplicate failed: ValueError",
        "plugin fixture-package:invalid failed: ValueError",
    ]
