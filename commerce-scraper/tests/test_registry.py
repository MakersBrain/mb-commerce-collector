import pytest
from pydantic import BaseModel

from mb_commerce_scraper.connectors import ConnectorRegistry, ShopifyFactory


def test_registry_is_isolated_and_does_not_instantiate_for_listing() -> None:
    first = ConnectorRegistry.with_builtins()
    second = ConnectorRegistry()
    assert first.names() == ("generic-pages", "shopify", "woocommerce")
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
