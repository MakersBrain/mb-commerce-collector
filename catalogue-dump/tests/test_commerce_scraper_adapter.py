from pathlib import Path

from mb_commerce_scraper import SourceDefinition

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
