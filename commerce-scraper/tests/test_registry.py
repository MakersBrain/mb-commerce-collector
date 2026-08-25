from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import BaseModel

from mb_commerce_scraper.connectors import (
    BrowserRequirement,
    ConnectorContext,
    ConnectorPlan,
    ConnectorRegistry,
    PluginLoadError,
    ShopifyFactory,
    ShopifyOptions,
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


def test_registry_validates_options_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = ShopifyOptions.model_validate
    calls = 0

    def model_validate(value: object) -> ShopifyOptions:
        nonlocal calls
        calls += 1
        return original(value)

    monkeypatch.setattr(ShopifyOptions, "model_validate", model_validate)

    connector = ConnectorRegistry.with_builtins().build(
        "shopify",
        transport=FakeTransport(),
        options={},
        context=ConnectorContext(),
    )

    assert connector.name == "shopify"
    assert calls == 1


def test_registry_validates_planning_options_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = ShopifyOptions.model_validate
    calls = 0

    def model_validate(value: object) -> ShopifyOptions:
        nonlocal calls
        calls += 1
        return original(value)

    monkeypatch.setattr(ShopifyOptions, "model_validate", model_validate)

    plan = ConnectorRegistry.with_builtins().plan(
        "shopify",
        options={},
        base_url="https://shop.test/",
        request_partitions=("clay",),
    )

    assert plan.partitions == ("clay",)
    assert calls == 1


@pytest.mark.parametrize(
    ("name", "options", "request_partitions", "partitions", "browser", "dynamic"),
    [
        ("shopify", {}, ("clay",), ("clay",), BrowserRequirement.NEVER, False),
        (
            "woocommerce",
            {"store_categories": ["glazes"]},
            (),
            ("glazes",),
            BrowserRequirement.NEVER,
            True,
        ),
        (
            "bigcommerce",
            {"allow_rendered_token_fallback": False},
            (),
            (),
            BrowserRequirement.NEVER,
            False,
        ),
        (
            "wix",
            {"render": False},
            (),
            (),
            BrowserRequirement.NEVER,
            False,
        ),
        (
            "prestashop",
            {"sitemaps": ["https://shop.test/products.xml"], "render": False},
            (),
            ("sitemap:0:a61b52bd4614",),
            BrowserRequirement.NEVER,
            False,
        ),
        (
            "generic-pages",
            {"discovery": {"category_urls": ["/clay"]}},
            (),
            ("category",),
            BrowserRequirement.OPTIONAL,
            False,
        ),
        (
            "shopware",
            {"discovery": {"category_urls": ["/clay"]}, "render": False},
            (),
            ("category",),
            BrowserRequirement.NEVER,
            False,
        ),
        ("starweb", {}, (), ("sitemap",), BrowserRequirement.OPTIONAL, False),
        ("nitrosell", {}, (), ("sitemap",), BrowserRequirement.OPTIONAL, False),
        ("sumup", {}, (), ("sitemap",), BrowserRequirement.OPTIONAL, False),
    ],
)
def test_builtin_factories_plan_collection_topology_without_building(
    name: str,
    options: dict[str, Any],
    request_partitions: tuple[str, ...],
    partitions: tuple[str, ...],
    browser: BrowserRequirement,
    dynamic: bool,
) -> None:
    plan = ConnectorRegistry.with_builtins().plan(
        name,
        options=options,
        base_url="https://shop.test/",
        request_partitions=request_partitions,
    )

    assert plan == ConnectorPlan(
        partitions=partitions,
        browser=browser,
        dynamic_partitions=dynamic,
    )


def test_connector_plan_rejects_invalid_partition_declarations() -> None:
    with pytest.raises(TypeError, match="tuple of strings"):
        ConnectorPlan(partitions=["main"])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="non-empty"):
        ConnectorPlan(partitions=("",))
    with pytest.raises(ValueError, match="unique"):
        ConnectorPlan(partitions=("main", "main"))
    with pytest.raises(TypeError, match="boolean"):
        ConnectorPlan(dynamic_partitions=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="BrowserRequirement"):
        ConnectorPlan(browser="optional")  # type: ignore[arg-type]


def test_simple_factory_requires_registry_validated_options() -> None:
    class WrongOptions(BaseModel):
        pass

    with pytest.raises(TypeError, match="requires validated ShopifyOptions"):
        ShopifyFactory().build(
            transport=FakeTransport(),
            options=WrongOptions(),
            context=ConnectorContext(),
        )


def test_factory_version_is_required_and_checked_against_built_connector() -> None:
    class MissingVersionFactory:
        name = "missing-version"
        options_model = BaseModel

        def build(self, **_: object) -> object:
            raise AssertionError("must not build")

    with pytest.raises(ValueError, match="must declare"):
        ConnectorRegistry().register(MissingVersionFactory())  # type: ignore[arg-type]

    class MissingPlanFactory:
        name = "missing-plan"
        version = "1"
        options_model = BaseModel

        def build(self, **_: object) -> object:
            raise AssertionError("must not build")

    with pytest.raises(ValueError, match="planning method"):
        ConnectorRegistry().register(MissingPlanFactory())  # type: ignore[arg-type]

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
