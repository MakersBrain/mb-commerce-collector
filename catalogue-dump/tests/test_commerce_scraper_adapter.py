from pathlib import Path

import pytest
from mb_commerce_scraper import (
    CollectionRequest,
    CommerceConnector,
    CompatibleLegacyCheckpoint,
    ConnectorRegistry,
    LegacyCheckpointRestartReason,
    RestartLegacyCheckpoint,
    SourceDefinition,
)
from mb_commerce_scraper.connectors import ConnectorContext
from mb_commerce_scraper.testing import FakeTransport
from pydantic import JsonValue

from mb_ceramics_catalogue.config.settings import CrawlParams
from mb_ceramics_catalogue.config.sources import SourceConfig, SourcesFile
from mb_ceramics_catalogue.connectors.base import (
    ConnectorCheckpoint as CatalogueConnectorCheckpoint,
)
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import (
    decode_legacy_source_checkpoint,
    layered_source_config,
    source_definition,
)
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    application_connector_registry,
)


def _checkpoint_context(
    scraper: str = "shopify",
) -> tuple[
    SourceConfig,
    CommerceConnector,
    CollectionRequest,
    dict[str, JsonValue],
    dict[str, JsonValue],
]:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    source_id, config = next(
        (source_id, config) for source_id, config in sources.items() if config.scraper == scraper
    )
    definition = source_definition(source_id, config)
    connector = ConnectorRegistry.with_builtins().build(
        definition.connector,
        transport=FakeTransport(),
        options=definition.connector_options,
        context=ConnectorContext(),
    )
    request = CollectionRequest(source_id=source_id, base_url=config.url)
    legacy: dict[str, JsonValue] = {
        "connector": connector.name,
        "connector_version": connector.version,
        "source_id": source_id,
        "lineage": "legacy-lineage",
        "resume_after": {"partition": "main", "page": 2, "offset": 0},
    }
    return config, connector, request, definition.connector_options, legacy


def test_flat_shopify_source_projects_to_library_envelope() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    source_id, config = next(
        (source_id, config) for source_id, config in sources.items() if config.scraper == "shopify"
    )
    projected = source_definition(source_id, config)
    assert isinstance(projected, SourceDefinition)
    assert projected.id == source_id
    assert projected.connector == "shopify"
    assert "page_limit" in projected.connector_options


def test_flat_source_projects_fetch_proxy_and_dataset_layers() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    source_id, config = next(iter(sources.items()))
    layered = layered_source_config(source_id, config, run=CrawlParams())
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


def test_every_checked_in_source_builds_from_the_application_registry() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    registry = application_connector_registry()
    built: dict[str, str] = {}

    for source_id, config in sources.items():
        definition = source_definition(source_id, config)
        connector = registry.build(
            definition.connector,
            transport=FakeTransport(),
            options=definition.connector_options,
            context=ConnectorContext(),
        )
        assert connector.name == definition.connector
        assert connector.version == registry.connector_version(definition.connector)
        built[source_id] = connector.name

    assert tuple(built) == tuple(sources.names())


def test_every_pagecrawl_source_validates_as_generic_pages() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    registry = ConnectorRegistry.with_builtins()
    pagecrawl_sources = {
        source_id: config for source_id, config in sources.items() if config.scraper == "pagecrawl"
    }

    assert pagecrawl_sources
    for source_id, config in pagecrawl_sources.items():
        definition = source_definition(source_id, config)
        assert definition.connector == "generic-pages"
        connector = registry.build(
            definition.connector,
            transport=FakeTransport(),
            options=definition.connector_options,
            context=ConnectorContext(),
        )
        assert connector.name == "generic-pages"


def test_pagecrawl_projection_preserves_discovery_and_collection_policy() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    source_id, config = next(
        (source_id, config)
        for source_id, config in sources.items()
        if config.scraper == "pagecrawl" and config.category_urls
    )

    projected = source_definition(source_id, config)
    options = projected.connector_options
    discovery = options["discovery"]

    assert isinstance(discovery, dict)
    assert discovery["category_urls"] == config.category_urls
    assert discovery["use_advertised_sitemaps"] is config.use_advertised_sitemaps
    assert discovery["product_pattern"] == config.product_pattern
    assert discovery["sitemap_limit"] == 100
    assert options["page_limit"] == config.page_limit
    assert options["parsers"] == ["jsonld", "microdata", "opengraph"]


def test_sio2_projection_policy_survives_layering_and_can_be_overridden() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    source_id, config = next(
        (source_id, config) for source_id, config in sources.items() if config.scraper == "sio2"
    )

    projected = layered_source_config(
        source_id, config, run=CrawlParams()
    ).projection_options
    assert projected["source_policy"] == "sio2"
    assert projected["scope"] == config.scope
    assert projected["brand"] == config.brand
    assert projected["enrichments"] == config.enrichments
    overridden = layered_source_config(
        source_id,
        config,
        run=CrawlParams(),
        projection_options={"source_policy": "custom", "locale": "es"},
    ).projection_options
    assert overridden == {**projected, "source_policy": "custom", "locale": "es"}


def test_layered_source_config_composes_exact_source_and_run_precedence() -> None:
    config = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "shopify",
                "delay": 0.5,
                "product_concurrency": 2,
                "obey_robots": True,
                "render": False,
                "timeout_seconds": 20,
                "proxy_eligible": True,
                "country": "FR",
            }
        }
    )["shop"]

    layered = layered_source_config(
        "shop",
        config,
        run=CrawlParams(
            delay=1.0,
            concurrency=8,
            browser="always",
            robots="ignore",
            source_timeout_seconds=10,
            refresh_mode="price",
        ),
        datasets=("ceramics.catalogue_item.v2",),
    )

    assert layered.fetch.delay == 1.0
    assert layered.fetch.concurrency == 2
    assert layered.fetch.robots.value == "obey"
    assert layered.fetch.browser.value == "never"
    assert layered.fetch.timeout_seconds == 10
    assert layered.proxy.mode.value == "fallback"
    assert layered.proxy.country == "FR"
    assert layered.datasets == ("ceramics.catalogue_item.v2",)
    assert layered.projection_options["collection_mode"] == "price"


def test_layered_source_config_keeps_defaults_and_explicit_disable_fail_closed() -> None:
    config = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "shopify",
                "proxy_eligible": True,
            }
        }
    )["shop"]

    defaulted = layered_source_config("shop", config, run=CrawlParams())
    disabled = layered_source_config(
        "shop", config, run=CrawlParams(proxy_policy="never")
    )

    assert defaulted.fetch.concurrency == 8
    assert defaulted.fetch.robots.value == "ignore"
    assert defaulted.proxy.mode.value == "fallback"
    assert disabled.proxy.mode.value == "never"
    with pytest.raises(ValueError, match="at least one dataset"):
        layered_source_config("shop", config, run=CrawlParams(), datasets=())


def test_compatible_legacy_checkpoint_decodes_at_catalogue_boundary() -> None:
    config, connector, request, options, legacy = _checkpoint_context()
    persisted = CatalogueConnectorCheckpoint.model_validate(legacy)

    decoded = decode_legacy_source_checkpoint(
        persisted,
        request=request,
        connector_version=connector.version,
        config=config,
        durable_request=request,
        durable_options=options,
    )

    assert isinstance(decoded, CompatibleLegacyCheckpoint)
    assert decoded.checkpoint.checkpoint_schema_version == 1
    assert decoded.checkpoint.resume_after == legacy["resume_after"]
    assert len(decoded.checkpoint.collection_fingerprint) == 64


def test_legacy_checkpoint_restarts_when_source_configuration_drifted() -> None:
    config, connector, request, durable_options, legacy = _checkpoint_context()
    changed_page_limit = 24 if config.page_limit == 23 else 23
    current_config = config.model_copy(update={"page_limit": changed_page_limit})

    decoded = decode_legacy_source_checkpoint(
        legacy,
        request=request,
        connector_version=connector.version,
        config=current_config,
        durable_request=request,
        durable_options=durable_options,
    )

    assert decoded == RestartLegacyCheckpoint(
        reason=LegacyCheckpointRestartReason.COLLECTION_CONFIGURATION_CHANGED
    )


@pytest.mark.parametrize(
    ("identity_field", "other_value"),
    (
        ("connector", "woocommerce"),
        ("connector_version", "legacy-version"),
        ("source_id", "another-source"),
    ),
)
def test_legacy_checkpoint_restarts_on_identity_mismatch(identity_field: str, other_value: str) -> None:
    config, connector, request, options, legacy = _checkpoint_context()
    legacy[identity_field] = other_value

    decoded = decode_legacy_source_checkpoint(
        legacy,
        request=request,
        connector_version=connector.version,
        config=config,
        durable_request=request,
        durable_options=options,
    )

    assert decoded == RestartLegacyCheckpoint(reason=LegacyCheckpointRestartReason.LEGACY_IDENTITY_MISMATCH)


def test_legacy_pagecrawl_checkpoint_restarts_after_generic_pages_rename() -> None:
    config, connector, request, options, legacy = _checkpoint_context("pagecrawl")
    legacy.update(connector="pagecommerce", connector_version="1")

    decoded = decode_legacy_source_checkpoint(
        legacy,
        request=request,
        connector_version=connector.version,
        config=config,
        durable_request=request,
        durable_options=options,
    )

    assert connector.name == "generic-pages"
    assert decoded == RestartLegacyCheckpoint(reason=LegacyCheckpointRestartReason.LEGACY_IDENTITY_MISMATCH)


def test_legacy_checkpoint_restarts_on_malformed_cursor() -> None:
    config, connector, request, options, legacy = _checkpoint_context()
    legacy["resume_after"] = None

    decoded = decode_legacy_source_checkpoint(
        legacy,
        request=request,
        connector_version=connector.version,
        config=config,
        durable_request=request,
        durable_options=options,
    )

    assert decoded == RestartLegacyCheckpoint(reason=LegacyCheckpointRestartReason.MALFORMED_CHECKPOINT)


@pytest.mark.parametrize("missing", ("request", "options"))
def test_legacy_checkpoint_restarts_when_durable_configuration_is_missing(
    missing: str,
) -> None:
    config, connector, request, options, legacy = _checkpoint_context()

    decoded = decode_legacy_source_checkpoint(
        legacy,
        request=request,
        connector_version=connector.version,
        config=config,
        durable_request=None if missing == "request" else request,
        durable_options=None if missing == "options" else options,
    )

    assert decoded == RestartLegacyCheckpoint(
        reason=LegacyCheckpointRestartReason.DURABLE_CONFIGURATION_UNAVAILABLE
    )
