"""Typed compatibility boundary into catalogue's atomic dataset pipeline."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from types import MappingProxyType
from typing import Any

from mb_commerce_scraper import (
    CollectionRequest as LibraryCollectionRequest,
)
from mb_commerce_scraper import (
    CommerceConnector as LibraryCommerceConnector,
)
from mb_commerce_scraper import (
    ConnectorCheckpoint as LibraryConnectorCheckpoint,
)
from mb_commerce_scraper.models import CommerceProductSnapshot
from mb_commerce_scraper.transports import NullTelemetry, TelemetryHooks, safe_telemetry
from pydantic import JsonValue

from mb_ceramics_catalogue.connectors.base import (
    CollectionRequest,
    ConnectorCheckpoint,
    EntityPage,
)


class LibraryPipelineConnector:
    """Present one registry-built library connector to the legacy pipeline API.

    The pipeline request remains application-owned projection context. The
    immutable library request and decoded library checkpoint are captured at
    construction so an old catalogue checkpoint can never be passed across
    this boundary by accident.
    """

    def __init__(
        self,
        connector: LibraryCommerceConnector,
        request: LibraryCollectionRequest,
        checkpoint: LibraryConnectorCheckpoint | None,
        telemetry: TelemetryHooks | None = None,
        telemetry_context: Mapping[str, JsonValue] | None = None,
    ) -> None:
        self._connector = connector
        self._request = request
        self._checkpoint = checkpoint
        self.name = connector.name
        self.platform = connector.platform
        self.version = connector.version
        self._telemetry = safe_telemetry(telemetry or NullTelemetry())
        # Copy before freezing so caller mutation cannot change correlation for
        # an in-flight collection. Canonical connector fields below take
        # precedence over any application context with the same key.
        self._telemetry_context = MappingProxyType(dict(telemetry_context or {}))
        # Both contracts expose supports() and named_capabilities(). Their enum
        # values are deliberately identical StrEnums during this migration.
        self.capabilities: Any = connector.capabilities

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        if checkpoint is not None:
            raise ValueError("catalogue checkpoint cannot enter a library connector")
        self._validate_projection_request(request)
        common = {
            **self._telemetry_context,
            "connector": self.name,
            "connector_version": self.version,
            "source_id": self._request.source_id,
        }
        self._telemetry.emit(
            "catalogue.library_connector.collection.started",
            {
                **common,
                "level": "info",
                "resuming": self._checkpoint is not None,
            },
        )
        pages = 0
        items = 0
        discovered = 0
        terminal = False
        intact = True
        try:
            async for page in self._connector.collect(self._request, self._checkpoint):
                # Entity snapshots already share the library model. Revalidation is
                # intentional for the still-distinct page envelopes and catches
                # contract drift at this one migration boundary. Keep Python-native
                # values here: JSON mode turns strict Decimal amounts into strings.
                validated = EntityPage[CommerceProductSnapshot].model_validate(
                    page.model_dump()
                )
                pages += 1
                items += len(validated.items)
                discovered += validated.discovered
                terminal = validated.terminal
                intact = intact and validated.enumeration_intact
                self._telemetry.emit(
                    "catalogue.library_connector.page.completed",
                    {
                        **common,
                        "level": "debug",
                        "page_id": validated.page_id,
                        "partition_key": validated.partition_key,
                        "sequence": validated.sequence,
                        "items": len(validated.items),
                        "discovered": validated.discovered,
                        "terminal": validated.terminal,
                        "enumeration_intact": validated.enumeration_intact,
                        "diagnostics": len(validated.diagnostics),
                    },
                )
                for diagnostic in validated.diagnostics:
                    self._telemetry.emit(
                        "catalogue.library_connector.diagnostic",
                        {
                            **common,
                            "level": diagnostic.severity.value,
                            "page_id": validated.page_id,
                            "code": diagnostic.code.value,
                            "severity": diagnostic.severity.value,
                            "retryable": diagnostic.retryable,
                            "affects_completeness": diagnostic.affects_completeness,
                        },
                    )
                yield validated
        except asyncio.CancelledError:
            self._telemetry.emit(
                "catalogue.library_connector.collection.interrupted",
                {
                    **common,
                    "level": "warning",
                    "reason": "task_cancelled",
                    "pages": pages,
                },
            )
            raise
        except Exception as error:
            self._telemetry.emit(
                "catalogue.library_connector.collection.failed",
                {
                    **common,
                    "level": "error",
                    "error_type": type(error).__name__,
                    "pages": pages,
                },
            )
            raise
        else:
            self._telemetry.emit(
                "catalogue.library_connector.collection.completed",
                {
                    **common,
                    "level": "info",
                    "pages": pages,
                    "items": items,
                    "discovered": discovered,
                    "terminal": terminal,
                    "enumeration_intact": intact,
                },
            )

    def _validate_projection_request(self, request: CollectionRequest) -> None:
        if request.source_id != self._request.source_id:
            raise ValueError("pipeline and library source identities differ")
        if request.base_url.rstrip("/") != self._request.base_url.rstrip("/"):
            raise ValueError("pipeline and library source URLs differ")
        if request.result_limit != self._request.result_limit:
            raise ValueError("pipeline and library result limits differ")
        if request.refresh_mode.value != self._request.refresh_mode.value:
            raise ValueError("pipeline and library refresh modes differ")
        pipeline_fields = frozenset(field.value for field in request.requested_fields)
        library_fields = frozenset(field.value for field in self._request.requested_fields)
        if pipeline_fields != library_fields:
            raise ValueError("pipeline and library requested fields differ")
