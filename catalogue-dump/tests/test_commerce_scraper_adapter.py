from pathlib import Path

from mb_commerce_scraper import ConnectorRegistry, SourceDefinition
from mb_commerce_scraper.connectors import ConnectorContext
from mb_commerce_scraper.testing import FakeTransport

from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import (
    layered_source_config,
    source_definition,
)


def test_flat_shopify_source_projects_to_library_envelope() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    source_id, config = next(
        (source_id, config)
        for source_id, config in sources.items()
        if config.scraper == "shopify"
    )
    projected = source_definition(source_id, config)
    assert isinstance(projected, SourceDefinition)
    assert projected.id == source_id
    assert projected.connector == "shopify"
    assert "page_limit" in projected.connector_options


def test_flat_source_projects_fetch_proxy_and_dataset_layers() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    source_id, config = next(iter(sources.items()))
    layered = layered_source_config(source_id, config)
    assert layered.source.id == source_id
    assert layered.fetch.concurrency >= 1
    assert layered.fetch.timeout_seconds > 0
    assert layered.datasets
    assert layered.proxy.mode.value in {"never", "fallback"}


def test_every_framework_source_validates_with_the_library_registry() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    registry = ConnectorRegistry.with_builtins()
    frameworks = {
        "shopify",
        "woocommerce",
        "bigcommerce",
        "prestashop",
        "sio2",
        "wix",
        "shopware",
        "starweb",
        "nitrosell",
        "sumup",
    }

    validated = set()
    for source_id, config in sources.items():
        if config.scraper not in frameworks:
            continue
        definition = source_definition(source_id, config)
        connector = registry.build(
            definition.connector,
            transport=FakeTransport(),
            options=definition.connector_options,
            context=ConnectorContext(),
        )
        assert connector.name == definition.connector
        validated.add(config.scraper)

    assert validated == frameworks


def test_sio2_projection_policy_survives_layering_and_can_be_overridden() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    source_id, config = next(
        (source_id, config)
        for source_id, config in sources.items()
        if config.scraper == "sio2"
    )

    assert layered_source_config(source_id, config).projection_options == {
        "source_policy": "sio2"
    }
    assert layered_source_config(
        source_id,
        config,
        projection_options={"source_policy": "custom", "locale": "es"},
    ).projection_options == {"source_policy": "custom", "locale": "es"}
