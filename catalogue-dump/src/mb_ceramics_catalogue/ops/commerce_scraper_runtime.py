"""Shared application composition for library connectors during migration."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
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
    ProxyMode,
    RefreshMode,
    SnapshotField,
    SourceDefinition,
)
from mb_commerce_scraper import ConnectorContext as LibraryConnectorContext
from mb_commerce_scraper.runtime import CommerceScraper, build_http_scraper
from mb_commerce_scraper.transports import (
    RobotsFetchFailurePolicy,
    TelemetryHooks,
    safe_telemetry,
)
from pydantic import JsonValue

from mb_ceramics_catalogue.config.settings import CacheMode, CrawlParams
from mb_ceramics_catalogue.config.sources import SourceConfig
from mb_ceramics_catalogue.connectors import (
    CollectionRequest as CatalogueCollectionRequest,
)
from mb_ceramics_catalogue.connectors import RefreshMode as CatalogueRefreshMode
from mb_ceramics_catalogue.datasets import ProjectionContext, built_in_registry
from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.observability import metrics, tracing
from mb_ceramics_catalogue.scrapers import LIBRARY_CANARY_SCRAPERS, library_canary_alias
from mb_ceramics_catalogue.scrapers import record as record_module
from mb_ceramics_catalogue.scrapers.base import (
    USER_AGENT,
    BrowserRenderer,
    ScrapeResult,
)
from mb_ceramics_catalogue.scrapers.cache import ResponseCache as DiskResponseCache
from mb_ceramics_catalogue.transports.browser import BrowserBackend, BrowserJobContext

from .commerce_scraper_adapter import (
    CatalogueSourceConfig,
    layered_source_config,
    source_definition,
)
from .commerce_scraper_axner import AxnerFactory
from .commerce_scraper_browser import (
    BorrowedBrowserTransport,
    CamoufoxProxyBrowserTransportFactory,
)
from .commerce_scraper_cache import CatalogueCacheStats, CatalogueResponseCache
from .commerce_scraper_ceramicolours import CeramicoloursFactory
from .commerce_scraper_keramik_kraft import KeramikKraftFactory
from .commerce_scraper_pipeline import LibraryPipelineConnector
from .commerce_scraper_proxy_runtime import NativeProxyRuntimeSpec
from .commerce_scraper_transport import LegacyFetcher, LegacyFetcherTransport
from .connector_adapters import library_canary_route, runtime_plan

LOGGER = obs.get_logger("catalogue.commerce_scraper")


@dataclass(frozen=True, slots=True)
class CatalogueCachePolicy:
    """Collection cache inputs without coupling composition to CLI parameters."""

    directory: Path
    mode: CacheMode
    maximum_age_seconds: float | None
    stale_on_error: bool = False


@dataclass(frozen=True, slots=True)
class BorrowedBrowserBinding:
    """A process-owned browser backend bound to one collection identity."""

    backend: BrowserBackend
    job: BrowserJobContext


@dataclass(frozen=True, slots=True)
class NativeCollectionSpec:
    configuration: CatalogueSourceConfig
    request: CollectionRequest
    checkpoint: ConnectorCheckpoint | None
    cache: CatalogueCachePolicy
    cancelled: Callable[[], bool]
    collection_id: str


@dataclass(frozen=True, slots=True)
class NativeRouteBindings:
    proxy: NativeProxyRuntimeSpec | None = None
    browser: BorrowedBrowserBinding | None = None


@dataclass(frozen=True, slots=True)
class OpenCatalogueCollection:
    connector: LibraryPipelineConnector
    telemetry: LibraryDebugTelemetry
    cache: CatalogueResponseCache


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
    """Select the retained legacy-Fetcher shell for parity and rollback tests.

    Production worker, dump, and probe canary routes enter
    :class:`CatalogueCommerceRuntime` directly. This alias projection remains
    temporarily available to compare that path with the former Fetcher bridge
    without restoring connector-specific construction.
    """
    plan = runtime_plan(config)
    projected = source_definition("local-canary", config, connector_plan=plan)
    route = library_canary_route(plan, projected.connector)
    if route is None:
        raise ValueError(f"connector_canary has no approved native route for {config.scraper!r}")
    adapter = library_canary_alias(config.scraper)
    if LIBRARY_CANARY_SCRAPERS.get(adapter) != config.scraper:
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
        self._outcomes: dict[str, int] = {}
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
        outcome = self._transport_outcome(event, fields)
        self._outcomes[outcome] = self._outcomes.get(outcome, 0) + physical
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

    def outcome_counts(self) -> dict[str, int]:
        return dict(self._outcomes)

    @staticmethod
    def _transport_outcome(event: str, fields: dict[str, Any]) -> str:
        del event
        status = fields.get("status")
        if (
            isinstance(status, int)
            and not isinstance(status, bool)
            and 100 <= status <= 599
        ):
            if status in {403, 429}:
                return str(status)
            return f"{status // 100}xx"
        return "transport_error"

    @staticmethod
    def _counter(value: Any) -> int:
        return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0

    @staticmethod
    def _trace_fields(fields: dict[str, Any]) -> dict[str, str | bool | int | float]:
        """Keep OpenTelemetry attributes bounded to its scalar value contract."""
        return {name: value for name, value in fields.items() if isinstance(value, (str, bool, int, float))}

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
        if event == "request.retry" and LibraryDebugTelemetry._counter(fields.get("physical_requests")) == 0:
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
        if isinstance(elapsed_ms, (int, float)) and not isinstance(elapsed_ms, bool) and elapsed_ms >= 0:
            metrics.request_duration(host, elapsed_ms / 1_000)
        if isinstance(status, int) and not isinstance(status, bool) and status >= 400:
            metrics.http_error(host, status)
        if fields.get("route") == "browser":
            metrics.browser_render(source_id)


class CatalogueCommerceRuntime:
    """Application-scoped registry and collection-scoped transport root."""

    def __init__(self, registry: ConnectorRegistry | None = None) -> None:
        self.registry = registry or application_connector_registry()

    @asynccontextmanager
    async def open_collection(
        self,
        spec: NativeCollectionSpec,
        routes: NativeRouteBindings | None = None,
    ) -> AsyncIterator[OpenCatalogueCollection]:
        routes = routes or NativeRouteBindings()
        source = spec.configuration.source
        request = spec.request
        if request.source_id != source.id or request.base_url != source.base_url:
            raise ValueError("native collection request identity must match its source definition")
        if source.connector not in self.registry.names():
            raise ValueError(f"native collection connector {source.connector!r} is not registered")
        proxy = routes.proxy
        if proxy is not None and (proxy.source_id != source.id or proxy.base_url != source.base_url):
            raise ValueError("native proxy runtime identity must match its source definition")

        telemetry = LibraryDebugTelemetry()
        policy = spec.configuration.fetch
        browser = routes.browser
        direct_browser = (
            BorrowedBrowserTransport(
                browser.backend,
                browser.job,
                allowed_origins=(source.base_url,),
            )
            if browser is not None
            else None
        )
        cache = CatalogueResponseCache(
            DiskResponseCache(
                spec.cache.directory,
                mode=spec.cache.mode,
                max_age=spec.cache.maximum_age_seconds,
            )
        )
        scraper: CommerceScraper = build_http_scraper(
            allowed_origins=(source.base_url,),
            registry=self.registry,
            timeout=policy.timeout_seconds,
            fetch_policy=policy,
            cache=cache,
            stale_on_error=spec.cache.stale_on_error,
            telemetry=telemetry,
            retries=3,
            robots_user_agent=USER_AGENT,
            robots_transport_failure_policy=RobotsFetchFailurePolicy.ALLOW,
            robots_server_failure_policy=RobotsFetchFailurePolicy.DENY,
            proxy_pool=proxy.pool if proxy is not None else None,
            proxy_policy=proxy.policy if proxy is not None else None,
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
        )
        async with (
            scraper,
            scraper.open_connector(
                source,
                collection_id=spec.collection_id,
                cancelled=spec.cancelled,
            ) as connector,
        ):
            yield OpenCatalogueCollection(
                connector=LibraryPipelineConnector(
                    connector,
                    request,
                    spec.checkpoint,
                    telemetry=telemetry,
                    telemetry_context={"collection_id": spec.collection_id},
                ),
                telemetry=telemetry,
                cache=cache,
            )


class LocalCommerceSession:
    """Native connector session for dump/probe compatibility output.

    Local commands have no durable paid-proxy reservation. This boundary
    therefore narrows every run to direct transport and never accepts a proxy
    binding. The browser process is lazy, shared by the local run, and owned by
    this session rather than by any individual connector.
    """

    def __init__(
        self,
        params: CrawlParams,
        cache_directory: Path | None,
        *,
        runtime: CatalogueCommerceRuntime | None = None,
        browser: BrowserBackend | None = None,
    ) -> None:
        if params.cache_mode != "off" and cache_directory is None:
            raise ValueError("an enabled local commerce cache requires a directory")
        self.params = params.model_copy(
            update={"proxy_policy": "never", "proxy_max_megabytes": None}
        )
        self.cache_directory = cache_directory or Path(".cache")
        self.runtime = runtime or CatalogueCommerceRuntime()
        self.browser = browser or BrowserRenderer(True)
        self._owns_browser = browser is None
        self._cache_stats = CatalogueCacheStats()
        self._closed = False

    @property
    def cache_enabled(self) -> bool:
        return self.params.cache_mode != "off"

    def cache_summary(self) -> str:
        return self._cache_stats.summary(self.params.cache_mode)

    def build(
        self,
        scraper: str,
        name: str,
        config: dict[str, Any],
        fetcher: Any,
    ) -> LocalLibraryScraper:
        """Match the crawl runner's scraper factory without a legacy Fetcher."""
        del fetcher
        if self._closed:
            raise RuntimeError("local commerce session is closed")
        source = SourceConfig.model_validate(config)
        if scraper != source.scraper:
            raise ValueError("local native scraper identity does not match its source")
        return LocalLibraryScraper(self, name, source)

    def retain_cache_stats(self, stats: CatalogueCacheStats) -> None:
        self._cache_stats = self._cache_stats + stats

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if not self._owns_browser:
            return
        try:
            await self.browser.shutdown()
        except Exception:
            LOGGER.warning("local_commerce.browser_close_failed", exc_info=True)


class LocalLibraryScraper:
    """Project one native connector into the established local result shape."""

    platform = "commerce-library"
    method = "connector"

    def __init__(
        self,
        session: LocalCommerceSession,
        name: str,
        source: SourceConfig,
    ) -> None:
        self.name = name
        self.config = source.as_scraper_config()
        self.base_url = source.url
        self.result = ScrapeResult()
        self._session = session
        self._source = source
        self._cancel_requested = False
        self._plan = runtime_plan(source)
        self.method = self._plan.extraction_method
        self.platform = self._plan.connector

    def cancel(self) -> None:
        self._cancel_requested = True

    async def scrape(self, limit: int | None = None) -> ScrapeResult:
        return await self._collect(limit)

    async def run(self, limit: int | None = None) -> ScrapeResult:
        try:
            return await self._collect(limit)
        finally:
            merged: dict[tuple[Any, ...], dict[str, Any]] = {}
            for row in self.result.records:
                merged.setdefault(record_module.dedupe_key(row), row)
            self.result.records = list(merged.values())

    async def _collect(self, limit: int | None) -> ScrapeResult:
        collection_id = f"local:{uuid4().hex}"
        dataset_registry = built_in_registry()
        dataset = (
            "ceramics.catalogue_identity.v2"
            if self._source.identity_only
            else "ceramics.catalogue_item.v2"
        )
        definition = dataset_registry.get(dataset)
        requested_fields = dataset_registry.collection_requirements((dataset,))[0]
        configuration = layered_source_config(
            self.name,
            self._source,
            run=self._session.params,
            datasets=(dataset,),
            connector_plan=self._plan,
        )
        if configuration.proxy.mode is not ProxyMode.NEVER:
            raise ValueError("local commerce collection cannot enable a paid proxy")
        route = library_canary_route(self._plan, configuration.source.connector)
        if route is None:
            raise ValueError(
                f"connector_canary has no approved native route for {self._source.scraper!r}"
            )
        if configuration.source.connector not in self._session.runtime.registry.names():
            raise ValueError(
                f"approved native connector {configuration.source.connector!r} is not registered"
            )

        request = CatalogueCollectionRequest(
            source_id=self.name,
            base_url=self.base_url,
            refresh_mode=CatalogueRefreshMode.FULL,
            requested_fields=requested_fields,
            result_limit=limit,
            collections=self._plan.collections,
            categories=self._plan.categories,
            cancellation_check=lambda: self._cancel_requested,
        )
        library_request = CollectionRequest(
            source_id=self.name,
            base_url=self.base_url,
            refresh_mode=RefreshMode.FULL,
            requested_fields=frozenset(
                SnapshotField(field.value) for field in requested_fields
            ),
            result_limit=limit,
            partitions=route.request_partitions,
        )
        projection = ProjectionContext(
            collection_id=collection_id,
            source_id=self.name,
            dataset=dataset,
            dataset_version=definition.version,
            projector_version=definition.projector_version,
            configuration=configuration.projection_options,
        )
        unfiltered_projection = projection.model_copy(
            update={
                "configuration": {
                    **configuration.projection_options,
                    "apply_scope": False,
                }
            }
        )
        browser = (
            BorrowedBrowserBinding(
                self._session.browser,
                BrowserJobContext(
                    job_id=collection_id,
                    logical_profile=self.name,
                ),
            )
            if route.uses_browser_transport
            and configuration.fetch.browser is not BrowserPolicy.NEVER
            else None
        )
        spec = NativeCollectionSpec(
            configuration=configuration,
            request=library_request,
            checkpoint=None,
            cache=CatalogueCachePolicy(
                directory=self._session.cache_directory,
                mode=self._session.params.cache_mode,
                maximum_age_seconds=self._session.params.cache_max_age_seconds,
                stale_on_error=self._session.params.stale_on_error,
            ),
            cancelled=lambda: self._cancel_requested,
            collection_id=collection_id,
        )
        opened: OpenCatalogueCollection | None = None
        last_terminal = False
        try:
            async with self._session.runtime.open_collection(
                spec,
                NativeRouteBindings(proxy=None, browser=browser),
            ) as active:
                opened = active
                async for page in active.connector.collect(request):
                    self.result.discovered += page.discovered
                    last_terminal = page.terminal
                    if not page.enumeration_intact:
                        self.result.truncated = True
                    self._retain_diagnostics(page.diagnostics)
                    for snapshot in page.items:
                        scoped = tuple(
                            dataset_registry.project_validated(
                                dataset, snapshot, projection
                            )
                        )
                        if configuration.projection_options.get("scope") == "materials":
                            candidates = tuple(
                                dataset_registry.project_validated(
                                    dataset, snapshot, unfiltered_projection
                                )
                            )
                            self.result.filtered += max(0, len(candidates) - len(scoped))
                        self.result.records.extend(
                            typed.model_dump(mode="json") for typed in scoped
                        )
        except asyncio.CancelledError:
            self.result.truncated = True
            raise
        finally:
            if opened is not None:
                self._retain_runtime_accounting(opened)

        if self._cancel_requested or not last_terminal:
            self.result.truncated = True
        if limit is not None and self.result.discovered >= limit:
            self.result.truncated = True
        if self._source.ignore_robots:
            self.result.notes.append(
                "robots.txt intentionally not applied for this source (operator decision)"
            )
        return self.result

    def _retain_diagnostics(self, diagnostics: tuple[Any, ...]) -> None:
        for diagnostic in diagnostics:
            if diagnostic.affects_completeness:
                self.result.truncated = True
            if diagnostic.severity == "error":
                self.result.errors.append(
                    {
                        "url": diagnostic.url or self.base_url,
                        "error": diagnostic.message,
                    }
                )

    def _retain_runtime_accounting(self, opened: OpenCatalogueCollection) -> None:
        totals = opened.telemetry.transport_totals()
        self.result.requests = totals["physical_requests"]
        self.result.rendered_pages = totals["browser_requests"]
        for name in (
            "direct_requests",
            "impersonated_requests",
            "browser_requests",
            "proxy_requests",
            "http_tx_bytes_estimated",
            "http_rx_bytes_estimated",
            "browser_tx_bytes_estimated",
            "browser_rx_bytes_estimated",
        ):
            setattr(self.result, name, totals[name])
        self.result.outcome_counts = opened.telemetry.outcome_counts()
        self.result.outcome_counts["browser_gain"] = self.result.browser_gain
        self.result.outcome_counts["browser_zero_gain"] = self.result.browser_zero_gain
        cache_stats = opened.cache.stats()
        self.result.cache_bytes_read = cache_stats.bytes_read
        self._session.retain_cache_stats(cache_stats)


@asynccontextmanager
async def open_local_commerce_session(
    params: CrawlParams,
    cache_directory: Path | None,
    *,
    runtime: CatalogueCommerceRuntime | None = None,
    browser: BrowserBackend | None = None,
) -> AsyncIterator[LocalCommerceSession]:
    """Open the native local compatibility runtime and close owned resources."""
    session = LocalCommerceSession(
        params,
        cache_directory,
        runtime=runtime,
        browser=browser,
    )
    try:
        yield session
    finally:
        await session.aclose()


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
    ignore_robots: bool = False,
    obey_robots: bool = False,
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
            ignore_robots=ignore_robots,
            obey_robots=obey_robots,
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
    """Compatibility wrapper around the application composition root."""
    if (browser_backend is None) != (browser_job is None):
        raise ValueError("browser backend and job context must be supplied together")
    browser = (
        BorrowedBrowserBinding(browser_backend, browser_job)
        if browser_backend is not None and browser_job is not None
        else None
    )
    runtime = CatalogueCommerceRuntime(registry)
    spec = NativeCollectionSpec(
        configuration=configuration,
        request=request,
        checkpoint=checkpoint,
        cache=CatalogueCachePolicy(
            directory=cache_directory,
            mode=params.cache_mode,
            maximum_age_seconds=params.cache_max_age_seconds,
            stale_on_error=params.stale_on_error,
        ),
        cancelled=cancelled,
        collection_id=collection_id,
    )
    async with runtime.open_collection(
        spec,
        NativeRouteBindings(proxy=proxy, browser=browser),
    ) as opened:
        yield opened.connector, opened.telemetry


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
    if not await fetcher.may_fetch(config.url, bool(config.ignore_robots), bool(config.obey_robots)):
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
