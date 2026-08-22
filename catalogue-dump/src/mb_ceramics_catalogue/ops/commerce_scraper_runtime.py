"""Shared application composition for library connectors during migration."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from mb_commerce_scraper import (
    BrowserPolicy,
    CollectionRequest,
    ConnectorCheckpoint,
    ConnectorRegistry,
    SourceDefinition,
)
from mb_commerce_scraper import ConnectorContext as LibraryConnectorContext
from mb_commerce_scraper.runtime import CommerceScraper, build_http_scraper
from mb_commerce_scraper.transports import RobotsFetchFailurePolicy, safe_telemetry
from mb_commerce_scraper.transports.base import TelemetryHooks
from pydantic import JsonValue

from mb_ceramics_catalogue.config.settings import CrawlParams
from mb_ceramics_catalogue.config.sources import SourceConfig
from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.observability import metrics, tracing
from mb_ceramics_catalogue.scrapers import library_canary_alias
from mb_ceramics_catalogue.scrapers.base import USER_AGENT
from mb_ceramics_catalogue.scrapers.cache import ResponseCache as DiskResponseCache
from mb_ceramics_catalogue.transports.browser import BrowserBackend, BrowserJobContext

from .commerce_scraper_adapter import CatalogueSourceConfig, source_definition
from .commerce_scraper_axner import AxnerFactory
from .commerce_scraper_browser import (
    BorrowedBrowserTransport,
    CamoufoxProxyBrowserTransportFactory,
)
from .commerce_scraper_cache import CatalogueResponseCache
from .commerce_scraper_ceramicolours import CeramicoloursFactory
from .commerce_scraper_keramik_kraft import KeramikKraftFactory
from .commerce_scraper_pipeline import LibraryPipelineConnector
from .commerce_scraper_proxy_runtime import NativeProxyRuntimeSpec
from .commerce_scraper_transport import LegacyFetcher, LegacyFetcherTransport
from .connector_adapters import library_canary_route, runtime_plan

LOGGER = obs.get_logger("catalogue.commerce_scraper")


class _ContextualTelemetry:
    """Bind connector-originated events to one immutable local collection."""

    def __init__(
        self,
        telemetry: TelemetryHooks,
        context: dict[str, JsonValue],
    ) -> None:
        self._telemetry = safe_telemetry(telemetry)
        self._context = dict(context)

    def emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        self._telemetry.emit(event, {**fields, **self._context})


def application_connector_registry() -> ConnectorRegistry:
    """Compose library built-ins with explicitly approved application plugins."""
    registry = ConnectorRegistry.with_builtins()
    registry.register(AxnerFactory())
    registry.register(CeramicoloursFactory())
    registry.register(KeramikKraftFactory())
    return registry


def local_canary_source_config(config: SourceConfig) -> SourceConfig:
    """Select the explicit Scraper compatibility shell for local CLI tools.

    The PostgreSQL worker enters the atomic pipeline directly. Local dump and
    probe commands still speak ``ScrapeResult``, so they use a thin registered
    shell. Every explicitly approved native route delegates back into this
    module's library registry composition rather than constructing a
    connector-specific catalogue adapter.
    """
    plan = runtime_plan(config)
    projected = source_definition("local-canary", config, connector_plan=plan)
    route = library_canary_route(plan, projected.connector)
    if route is None:
        raise ValueError(
            f"connector_canary has no approved native route for {config.scraper!r}"
        )
    adapter = library_canary_alias(config.scraper)
    if adapter != f"library_{plan.legacy_scraper_adapter}":
        raise ValueError(
            f"library canary alias metadata does not match runtime plan for {config.scraper!r}"
        )
    return config.model_copy(update={"scraper": adapter})


class LibraryDebugTelemetry:
    """Adapt sanitized library events to catalogue logs, traces, and totals."""

    _TOTAL_NAMES = (
        "direct_requests",
        "impersonated_requests",
        "browser_requests",
        "proxy_requests",
        "http_tx_bytes_estimated",
        "http_rx_bytes_estimated",
        "browser_tx_bytes_estimated",
        "browser_rx_bytes_estimated",
        "physical_requests",
        "unclassified_requests",
    )

    def __init__(self) -> None:
        self._totals = dict.fromkeys(self._TOTAL_NAMES, 0)
        self._request_spans: dict[tuple[str, int], tuple[Any, Any]] = {}

    def emit(self, event: str, fields: dict[str, Any]) -> None:
        declared_level = fields.get("level")
        level = declared_level if isinstance(declared_level, str) else "debug"
        writer = {
            "info": LOGGER.info,
            "warning": LOGGER.warning,
            "error": LOGGER.error,
        }.get(level, LOGGER.debug)
        with suppress(Exception):  # observers never affect collection
            writer(event, **fields)
        if event == "request.started":
            with suppress(Exception):
                self._start_request_span(fields)
        with suppress(Exception):  # optional tracing is best-effort
            tracing.event(event, **self._trace_fields(fields))
        if event in {"request.completed", "request.failed", "request.retry"}:
            with suppress(Exception):
                self._project_request_metrics(event, fields)
            with suppress(Exception):
                self._finish_request_span(fields)
        if event not in {"request.completed", "request.failed", "request.retry"}:
            return
        physical = self._counter(fields.get("physical_requests"))
        if physical == 0:
            return
        transmitted = self._counter(fields.get("transmitted_bytes"))
        received = self._counter(fields.get("received_bytes"))
        route = fields.get("route")
        provider = fields.get("provider")
        self._totals["physical_requests"] += physical
        if route == "browser":
            self._totals["browser_requests"] += physical
            self._totals["browser_tx_bytes_estimated"] += transmitted
            self._totals["browser_rx_bytes_estimated"] += received
        else:
            self._totals["http_tx_bytes_estimated"] += transmitted
            self._totals["http_rx_bytes_estimated"] += received
        if provider is not None:
            self._totals["proxy_requests"] += physical
        elif route == "direct":
            self._totals["direct_requests"] += physical
        elif route != "browser":
            # Transport failures deliberately carry no potentially stale route
            # attribution. Retain their usage without guessing direct/proxy.
            self._totals["unclassified_requests"] += physical

    def transport_totals(self) -> dict[str, int]:
        return dict(self._totals)

    @staticmethod
    def _counter(value: Any) -> int:
        return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0

    @staticmethod
    def _trace_fields(fields: dict[str, Any]) -> dict[str, str | bool | int | float]:
        """Keep OpenTelemetry attributes bounded to its scalar value contract."""
        return {
            name: value
            for name, value in fields.items()
            if isinstance(value, (str, bool, int, float))
        }

    @staticmethod
    def _request_key(fields: dict[str, Any]) -> tuple[str, int] | None:
        request_id = fields.get("request_id")
        attempt = fields.get("attempt")
        if (
            not isinstance(request_id, str)
            or not request_id
            or not isinstance(attempt, int)
            or isinstance(attempt, bool)
            or attempt < 1
        ):
            return None
        return request_id, attempt

    def _start_request_span(self, fields: dict[str, Any]) -> None:
        key = self._request_key(fields)
        if key is None:
            return
        previous = self._request_spans.pop(key, None)
        if previous is not None:
            previous[0].__exit__(None, None, None)
        manager = tracing.span(
            "commerce.request",
            **self._trace_fields(fields),
        )
        active = manager.__enter__()
        self._request_spans[key] = manager, active

    def _finish_request_span(self, fields: dict[str, Any]) -> None:
        key = self._request_key(fields)
        if key is None:
            return
        retained = self._request_spans.pop(key, None)
        if retained is None:
            return
        manager, active = retained
        if active is not None:
            for name, value in self._trace_fields(fields).items():
                active.set_attribute(name, value)
        manager.__exit__(None, None, None)

    @staticmethod
    def _project_request_metrics(event: str, fields: dict[str, Any]) -> None:
        if event == "request.retry" and LibraryDebugTelemetry._counter(
            fields.get("physical_requests")
        ) == 0:
            # A TransportFailure already emitted and counted request.failed;
            # its following classification-only retry is a decision event, not
            # a second physical attempt.
            return
        raw_url = fields.get("url")
        if not isinstance(raw_url, str):
            return
        host = (urlsplit(raw_url).hostname or "").casefold()
        if not host:
            return
        source = fields.get("source_id")
        source_id = source if isinstance(source, str) else None
        status = fields.get("status")
        outcome = (
            "failed"
            if event == "request.failed"
            else "retry"
            if event == "request.retry"
            else f"{status // 100}xx"
            if isinstance(status, int) and not isinstance(status, bool)
            else "completed"
        )
        metrics.request(source_id, host, outcome)
        elapsed_ms = fields.get("elapsed_ms")
        if (
            isinstance(elapsed_ms, (int, float))
            and not isinstance(elapsed_ms, bool)
            and elapsed_ms >= 0
        ):
            metrics.request_duration(host, elapsed_ms / 1_000)
        if isinstance(status, int) and not isinstance(status, bool) and status >= 400:
            metrics.http_error(host, status)
        if fields.get("route") == "browser":
            metrics.browser_render(source_id)


def build_library_pipeline_connector(
    *,
    registry: ConnectorRegistry,
    source: SourceDefinition,
    request: CollectionRequest,
    checkpoint: ConnectorCheckpoint | None,
    fetcher: LegacyFetcher,
    cancelled: Callable[[], bool],
    clock: Callable[[], datetime] | None = None,
    collection_id: str | None = None,
) -> LibraryPipelineConnector:
    """Compose a registry connector at the single application boundary."""
    telemetry = LibraryDebugTelemetry()
    telemetry_context: dict[str, JsonValue] = {
        "collection_id": collection_id or uuid4().hex,
        "source_id": source.id,
        "connector": source.connector,
    }
    connector = registry.build(
        source.connector,
        transport=LegacyFetcherTransport(
            fetcher,
            telemetry=telemetry,
            telemetry_context=telemetry_context,
        ),
        options=source.connector_options,
        context=LibraryConnectorContext(
            cancelled=cancelled,
            telemetry=_ContextualTelemetry(telemetry, telemetry_context),
            clock=clock,
        ),
    )
    return LibraryPipelineConnector(
        connector,
        request,
        checkpoint,
        telemetry=telemetry,
        telemetry_context=telemetry_context,
    )


@asynccontextmanager
async def open_native_library_pipeline_connector(
    *,
    registry: ConnectorRegistry,
    configuration: CatalogueSourceConfig,
    request: CollectionRequest,
    checkpoint: ConnectorCheckpoint | None,
    params: CrawlParams,
    cache_directory: Path,
    proxy: NativeProxyRuntimeSpec | None,
    browser_backend: BrowserBackend | None = None,
    browser_job: BrowserJobContext | None = None,
    cancelled: Callable[[], bool],
    collection_id: str,
) -> AsyncIterator[tuple[LibraryPipelineConnector, LibraryDebugTelemetry]]:
    """Compose the native library stack without a legacy Fetcher/session."""
    if (browser_backend is None) != (browser_job is None):
        raise ValueError("browser backend and job context must be supplied together")
    telemetry = LibraryDebugTelemetry()
    source = configuration.source
    policy = configuration.fetch
    direct_browser = (
        BorrowedBrowserTransport(
            browser_backend,
            browser_job,
            allowed_origins=(source.base_url,),
        )
        if browser_backend is not None and browser_job is not None
        else None
    )
    cache = CatalogueResponseCache(
        DiskResponseCache(
            cache_directory,
            mode=params.cache_mode,
            max_age=params.cache_max_age_seconds,
        )
    )
    scraper: CommerceScraper = build_http_scraper(
        allowed_origins=(source.base_url,),
        registry=registry,
        timeout=policy.timeout_seconds,
        fetch_policy=policy,
        cache=cache,
        stale_on_error=params.stale_on_error,
        telemetry=telemetry,
        retries=3,
        robots_user_agent=USER_AGENT,
        robots_transport_failure_policy=RobotsFetchFailurePolicy.ALLOW,
        robots_server_failure_policy=RobotsFetchFailurePolicy.DENY,
        proxy_pool=proxy.pool if proxy is not None else None,
        routing=proxy.routing if proxy is not None else None,
        browser_transport=direct_browser,
        owns_browser_transport=direct_browser is not None,
        proxy_browser_transport_factory=(
            CamoufoxProxyBrowserTransportFactory(
                allowed_origins=(source.base_url,),
            )
            if proxy is not None and policy.browser is not BrowserPolicy.NEVER
            else None
        ),
        require_proxy_browser_subrequest_authorization=True,
        proxy_maximum_requests=(
            proxy.maximum_requests if proxy is not None else None
        ),
        proxy_maximum_bytes=proxy.maximum_bytes if proxy is not None else None,
    )
    async with scraper, scraper.open_connector(
        source,
        collection_id=collection_id,
        cancelled=cancelled,
    ) as connector:
        yield (
            LibraryPipelineConnector(
                connector,
                request,
                checkpoint,
                telemetry=telemetry,
                telemetry_context={"collection_id": collection_id},
            ),
            telemetry,
        )
async def apply_library_fetch_policy(
    fetcher: LegacyFetcher,
    config: SourceConfig,
    connector: LibraryPipelineConnector,
) -> None:
    """Restore policy formerly applied by ``Scraper.__init__`` and ``run``."""
    if connector.capabilities.shared_edge:
        fetcher.limiter.join_group(config.url, connector.capabilities.shared_edge)
    if config.delay:
        fetcher.limiter.set_delay(config.url, float(config.delay))
    if not await fetcher.may_fetch(
        config.url, bool(config.ignore_robots), bool(config.obey_robots)
    ):
        raise RuntimeError("robots.txt disallows the library connector")


def fetcher_transport_totals(fetcher: LegacyFetcher | None) -> dict[str, int]:
    """Read route/byte counters without mutating or double-merging Fetcher stats."""
    names = (
        "direct_requests",
        "impersonated_requests",
        "browser_requests",
        "proxy_requests",
        "http_tx_bytes_estimated",
        "http_rx_bytes_estimated",
        "browser_tx_bytes_estimated",
        "browser_rx_bytes_estimated",
    )
    totals = dict.fromkeys(names, 0)
    current: Any = fetcher
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        stats = current.stats
        for name in names:
            value = getattr(stats, name, 0)
            if isinstance(value, int):
                totals[name] += value
        current = getattr(current, "proxy_fallback", None)
    return totals
