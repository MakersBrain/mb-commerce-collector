"""Compatibility projection from flat catalogue sources to the library envelope."""

from __future__ import annotations

from typing import Any

from mb_commerce_scraper import (
    BrowserPolicy,
    FetchPolicy,
    ProxyMode,
    ProxyPolicyConfig,
    RobotsPolicy,
    SourceDefinition,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from mb_ceramics_catalogue.config.sources import SourceConfig

from .connector_adapters import runtime_plan


class CatalogueSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: SourceDefinition
    fetch: FetchPolicy
    proxy: ProxyPolicyConfig
    datasets: tuple[str, ...] = Field(min_length=1)
    projection_options: dict[str, JsonValue] = Field(default_factory=dict)


def source_definition(source_id: str, config: SourceConfig) -> SourceDefinition:
    """Preserve every connector option through the existing validated projector."""
    plan = runtime_plan(config)
    return SourceDefinition(
        id=source_id,
        label=config.label,
        base_url=config.url,
        connector="generic-pages" if plan.name == "pagecommerce" else plan.name,
        connector_options=dict(plan.options),
    )


def legacy_checkpoint_fingerprint_options(config: SourceConfig) -> dict[str, Any]:
    """Durable option identity used when decoding a legacy checkpoint lineage."""
    return dict(runtime_plan(config).options)


def layered_source_config(
    source_id: str,
    config: SourceConfig,
    *,
    datasets: tuple[str, ...] | None = None,
    projection_options: dict[str, Any] | None = None,
) -> CatalogueSourceConfig:
    """Validate the flat source first, then split collection and application policy."""
    plan = runtime_plan(config)
    selected_datasets = datasets or (
        "ceramics.catalogue_identity.v2"
        if config.identity_only
        else "ceramics.catalogue_item.v2",
    )
    browser = (
        BrowserPolicy.REQUIRE
        if config.render is True
        else BrowserPolicy.NEVER
        if config.render is False
        else BrowserPolicy.ALLOW
    )
    return CatalogueSourceConfig(
        source=source_definition(source_id, config),
        fetch=FetchPolicy(
            delay=config.delay or 0.0,
            concurrency=config.product_concurrency or 1,
            robots=RobotsPolicy.IGNORE if config.ignore_robots else RobotsPolicy.OBEY,
            timeout_seconds=config.timeout_seconds or 30.0,
            browser=browser,
        ),
        proxy=ProxyPolicyConfig(
            mode=ProxyMode.FALLBACK if config.proxy_eligible else ProxyMode.NEVER,
            country=config.country,
        ),
        datasets=selected_datasets,
        projection_options={**plan.ceramics_projection, **(projection_options or {})},
    )
