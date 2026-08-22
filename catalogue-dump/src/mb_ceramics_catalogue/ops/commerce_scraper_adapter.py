"""Compatibility projection from flat catalogue sources to the library envelope."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from mb_commerce_scraper import (
    BrowserPolicy,
    CollectionRequest,
    FetchPolicy,
    LegacyCheckpointDecodeResult,
    ProxyMode,
    ProxyPolicyConfig,
    RobotsPolicy,
    SourceDefinition,
    decode_legacy_checkpoint,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from mb_ceramics_catalogue.config.settings import CrawlParams
from mb_ceramics_catalogue.config.sources import SourceConfig

from .connector_adapters import ConnectorRuntimePlan, runtime_plan


class CatalogueSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: SourceDefinition
    fetch: FetchPolicy
    proxy: ProxyPolicyConfig
    datasets: tuple[str, ...] = Field(min_length=1)
    projection_options: dict[str, JsonValue] = Field(default_factory=dict)


def source_definition(
    source_id: str,
    config: SourceConfig,
    *,
    connector_plan: ConnectorRuntimePlan | None = None,
) -> SourceDefinition:
    """Preserve every connector option through the existing validated projector."""
    plan = connector_plan or runtime_plan(config)
    return SourceDefinition(
        id=source_id,
        label=config.label,
        base_url=config.url,
        connector=plan.connector,
        connector_options=dict(plan.connector_options),
    )


def decode_legacy_source_checkpoint(
    checkpoint: BaseModel | Mapping[str, JsonValue],
    *,
    request: CollectionRequest,
    connector_version: str,
    config: SourceConfig,
    durable_request: CollectionRequest | None,
    durable_options: dict[str, JsonValue] | None,
) -> LegacyCheckpointDecodeResult:
    """Decode a catalogue checkpoint only when its durable identity is known.

    Existing catalogue lineages do not retain enough configuration to provide
    ``durable_request`` and ``durable_options``. Callers handling those rows
    must pass ``None`` and begin a new lineage. In particular, current mutable
    source configuration is not evidence of the configuration that produced a
    persisted cursor.
    """
    definition = source_definition(request.source_id, config)
    payload = checkpoint.model_dump(mode="json") if isinstance(checkpoint, BaseModel) else checkpoint
    return decode_legacy_checkpoint(
        payload,
        request=request,
        connector=definition.connector,
        connector_version=connector_version,
        options=definition.connector_options,
        durable_request=durable_request,
        durable_options=durable_options,
    )


def layered_source_config(
    source_id: str,
    config: SourceConfig,
    *,
    run: CrawlParams,
    datasets: tuple[str, ...] | None = None,
    projection_options: dict[str, Any] | None = None,
    connector_plan: ConnectorRuntimePlan | None = None,
) -> CatalogueSourceConfig:
    """Project one effective source/run policy into the four ownership layers."""
    selected_datasets = (
        datasets
        if datasets is not None
        else (
            "ceramics.catalogue_identity.v2"
            if config.identity_only
            else "ceramics.catalogue_item.v2",
        )
    )
    if not selected_datasets:
        raise ValueError("layered source configuration requires at least one dataset")
    browser = (
        BrowserPolicy.REQUIRE
        if config.render is True or (config.render is None and run.browser == "always")
        else BrowserPolicy.NEVER
        if config.render is False or run.browser == "never"
        else BrowserPolicy.ALLOW
    )
    obey_robots = bool(config.obey_robots) or (
        run.robots == "obey" and not config.ignore_robots
    )
    plan = connector_plan or runtime_plan(config)
    return CatalogueSourceConfig(
        source=source_definition(source_id, config, connector_plan=plan),
        fetch=FetchPolicy(
            delay=max(run.delay, config.delay or 0.0),
            concurrency=config.product_concurrency or run.concurrency,
            robots=RobotsPolicy.OBEY if obey_robots else RobotsPolicy.IGNORE,
            timeout_seconds=run.timeout_for(config.timeout_seconds),
            browser=browser,
        ),
        proxy=ProxyPolicyConfig(
            mode=(
                ProxyMode.FALLBACK
                if config.proxy_eligible and run.proxy_policy != "never"
                else ProxyMode.NEVER
            ),
            country=config.country,
        ),
        datasets=selected_datasets,
        projection_options={
            **ceramics_projection_configuration(
                config,
                collection_mode=run.refresh_mode,
            ),
            **(projection_options or {}),
        },
    )


def ceramics_projection_configuration(
    config: SourceConfig,
    *,
    collection_mode: Literal["full", "price"] = "full",
) -> dict[str, JsonValue]:
    """Build the one application-owned ceramics projection policy.

    Collection connectors stay commerce-neutral. Source traits and the
    compatibility projector's scope policy belong at this composition boundary
    and must be identical for workers, replay probes, and parity tests.
    """
    plan = runtime_plan(config)
    return {
        "scope": config.scope,
        "enrichments": list(config.enrichments or ()),
        "brand": config.brand,
        "is_manufacturer": config.is_manufacturer,
        "extraction_method": plan.extraction_method,
        "source_detail_level": plan.source_detail_level,
        "apply_scope": True,
        "material_categories": list(config.material_categories or ()),
        "excluded_categories": list(config.excluded_categories or ()),
        "collection_mode": collection_mode,
        **plan.ceramics_projection,
    }
