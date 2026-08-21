"""Compatibility projection from flat catalogue sources to the library envelope."""

from __future__ import annotations

from typing import Any

from mb_commerce_scraper import SourceDefinition

from mb_ceramics_catalogue.config.sources import SourceConfig

from .connector_adapters import runtime_plan


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
