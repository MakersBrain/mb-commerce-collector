from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from time import monotonic
from typing import Protocol, cast
from urllib.parse import urlsplit
from uuid import uuid4

from pydantic import JsonValue

from mb_commerce_scraper.connectors import (
    CommerceConnector,
    ConnectorCapabilities,
    ConnectorContext,
    ConnectorRegistry,
)
from mb_commerce_scraper.models import (
    BrowserPolicy,
    CollectionRequest,
    CommerceProductSnapshot,
    ConnectorCheckpoint,
    EntityPage,
    FetchPolicy,
    ProxyMode,
    ProxyPolicyConfig,
    RefreshMode,
    RobotsPolicy,
    SnapshotField,
    SourceDefinition,
    sanitize_commerce_snapshot,
    sanitize_diagnostic,
)
from mb_commerce_scraper.proxy import (
    PoolBrowserSubrequestAuthorizer,
    ProxyBrowserTransportFactory,
    ProxyLease,
    ProxyPool,
    ProxyRouting,
    ProxyTransportFactory,
    RoutedTransport,
    RoutingMode,
)
from mb_commerce_scraper.transports import (
    DEFAULT_MAXIMUM_RESPONSE_BYTES,
    BrowserDispatchTransport,
    BrowserTransport,
    CachedRobotsChecker,
    CommerceTransport,
    MiddlewareTransport,
    PerOriginRateLimiter,
    ProxyBrowserRoutingUnsupported,
    RateLimitedTransport,
    RotationReason,
    TransportRequest,
    TransportResponse,
    safe_telemetry,
)
from mb_commerce_scraper.transports.base import (
    RateLimiter,
    RequestBudget,
    ResponseCache,
    RobotsChecker,
    TelemetryHooks,
)

RobotsFactory = Callable[[CommerceTransport], RobotsChecker]
RateLimiterFactory = Callable[[FetchPolicy], RateLimiter]


class _AsyncCloseable(Protocol):
    async def aclose(self) -> None: ...


def _routing_from_policy(policy: ProxyPolicyConfig) -> ProxyRouting:
    return ProxyRouting(
        mode=RoutingMode(policy.mode.value),
        country=policy.country,
        provider_preferences=policy.provider_preferences,
    )


def _legacy_proxy_policy(
    routing: ProxyRouting | None,
    maximum_requests: int | None,
    maximum_bytes: int | None,
) -> ProxyPolicyConfig:
    selected = routing or ProxyRouting()
    return ProxyPolicyConfig(
        mode=ProxyMode(selected.mode.value),
        country=selected.country,
        provider_preferences=selected.provider_preferences,
        maximum_requests=maximum_requests,
        maximum_bytes=maximum_bytes,
    )


class _ContextualTelemetry:
    """Attach immutable collection identity to connector-originated events."""

    def __init__(
        self,
        telemetry: TelemetryHooks,
        context: dict[str, JsonValue],
    ) -> None:
        self._telemetry = telemetry
        self._context = dict(context)

    def emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        # Runtime-owned identity wins if a plugin accidentally reuses one of
        # these field names. The downstream SafeTelemetry boundary still owns
        # redaction, bounding, and observer-failure isolation.
        self._telemetry.emit(event, {**fields, **self._context})


class _RateLimitedProxyTransportFactory:
    def __init__(
        self,
        factory: ProxyTransportFactory,
        limiter: RateLimiter,
        *,
        telemetry: TelemetryHooks | None,
        telemetry_context: dict[str, JsonValue],
    ) -> None:
        self._factory = factory
        self._limiter = limiter
        self._telemetry = telemetry
        self._telemetry_context = telemetry_context

    def build(self, lease: ProxyLease) -> CommerceTransport:
        return RateLimitedTransport(
            self._factory.build(lease),
            self._limiter,
            route="proxy",
            telemetry=self._telemetry,
            telemetry_context=self._telemetry_context,
        )


class _BrowserProxyTransportFactory:
    """Compose HTTP and browser transports around one sticky proxy lease."""

    def __init__(
        self,
        factory: ProxyTransportFactory,
        browser_factory: ProxyBrowserTransportFactory | None,
        pool: ProxyPool,
        target_host: str,
        *,
        policy: BrowserPolicy,
        telemetry: TelemetryHooks | None,
        telemetry_context: dict[str, JsonValue],
        maximum_response_bytes: int,
    ) -> None:
        self._factory = factory
        self._browser_factory = browser_factory
        self._pool = pool
        self._target_host = target_host
        self._policy = policy
        self._telemetry = telemetry
        self._telemetry_context = telemetry_context
        self._maximum_response_bytes = maximum_response_bytes

    def build(self, lease: ProxyLease) -> CommerceTransport:
        http = self._factory.build(lease)
        browser = (
            self._browser_factory.build(
                lease,
                PoolBrowserSubrequestAuthorizer(
                    self._pool,
                    lease,
                    self._target_host,
                ),
            )
            if self._browser_factory is not None
            else None
        )
        return _OwnedProxyBrowserTransport(
            BrowserDispatchTransport(
                http,
                browser,
                policy=self._policy,
                proxy_browser_supported=browser is not None,
                telemetry=self._telemetry,
                telemetry_context=self._telemetry_context,
                maximum_response_bytes=self._maximum_response_bytes,
            ),
            http=http,
            browser=browser,
        )


class _OwnedProxyBrowserTransport:
    """Own both transports created for one proxy route generation."""

    def __init__(
        self,
        dispatch: BrowserDispatchTransport,
        *,
        http: CommerceTransport,
        browser: CommerceTransport | None,
    ) -> None:
        self._dispatch = dispatch
        self._http = http
        self._browser = browser

    async def request(self, request: TransportRequest) -> TransportResponse:
        return await self._dispatch.request(request)

    @property
    def browser_subrequests_authorized(self) -> bool:
        """Only advertise the capability implemented by the browser backend."""

        return (
            self._browser is not None
            and getattr(self._browser, "browser_subrequests_authorized", False) is True
        )

    async def rotate_identity(self, reason: RotationReason) -> None:
        await self._dispatch.rotate_identity(reason)

    async def aclose(self) -> None:
        try:
            close_http = getattr(self._http, "aclose", None)
            if close_http is not None:
                await close_http()
        finally:
            if self._browser is not None and self._browser is not self._http:
                close_browser = getattr(self._browser, "aclose", None)
                if close_browser is not None:
                    await close_browser()


class _CollectionLifecycleConnector:
    """Collection-scoped connector view enforcing the public runtime boundary."""

    def __init__(
        self,
        connector: CommerceConnector,
        *,
        scraper: CommerceScraper,
        telemetry_context: dict[str, JsonValue],
    ) -> None:
        self._connector = connector
        self._scraper = scraper
        self._telemetry_context = telemetry_context
        self.name = connector.name
        self.platform = connector.platform
        self.version = connector.version
        self.capabilities: ConnectorCapabilities = connector.capabilities

    async def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        context = self._telemetry_context
        self._scraper._emit("collection.started", {**context, "level": "info"})
        try:
            if request.deadline is None:
                async for page in self._connector.collect(request, checkpoint):
                    yield self._page(context, page)
            else:
                if request.deadline.tzinfo is None:
                    raise ValueError("collection deadline must include a timezone")
                remaining = max(
                    0.0,
                    (request.deadline - datetime.now(UTC)).total_seconds(),
                )
                async with asyncio.timeout(remaining):
                    async for page in self._connector.collect(request, checkpoint):
                        yield self._page(context, page)
            self._scraper._emit("collection.completed", {**context, "level": "info"})
        except BaseException as error:
            self._scraper._emit(
                "collection.interrupted",
                {
                    **context,
                    "level": "warning",
                    "error_type": type(error).__name__,
                },
            )
            raise

    def _page(
        self,
        context: dict[str, JsonValue],
        page: EntityPage[CommerceProductSnapshot],
    ) -> EntityPage[CommerceProductSnapshot]:
        sanitized = _sanitize_page(page)
        self._scraper._emit_page(context, sanitized)
        return sanitized


class CommerceScraper:
    def __init__(
        self,
        *,
        registry: ConnectorRegistry,
        transport: CommerceTransport,
        proxy_pool: ProxyPool | None = None,
        proxy_policy: ProxyPolicyConfig | None = None,
        routing: ProxyRouting | None = None,
        proxy_transport_factory: ProxyTransportFactory | None = None,
        proxy_browser_transport_factory: ProxyBrowserTransportFactory | None = None,
        require_proxy_browser_subrequest_authorization: bool = False,
        proxy_maximum_requests: int | None = None,
        proxy_maximum_bytes: int | None = None,
        budget: RequestBudget | None = None,
        telemetry: TelemetryHooks | None = None,
        fetch_policy: FetchPolicy | None = None,
        cache: ResponseCache | None = None,
        stale_on_error: bool = False,
        robots_factory: RobotsFactory | None = None,
        rate_limiter_factory: RateLimiterFactory | None = None,
        retries: int = 2,
        backoff: Callable[[int], float] = lambda attempt: 0.1 * 2**attempt,
        owns_transport: bool = False,
        browser_transport: BrowserTransport | None = None,
        owns_browser_transport: bool = False,
        maximum_response_bytes: int = DEFAULT_MAXIMUM_RESPONSE_BYTES,
    ) -> None:
        self.registry = registry
        self.transport = transport
        legacy_proxy_configuration = any(
            value is not None for value in (routing, proxy_maximum_requests, proxy_maximum_bytes)
        )
        if proxy_policy is not None and legacy_proxy_configuration:
            raise ValueError("proxy_policy cannot be combined with routing or legacy proxy caps")
        if proxy_policy is None and (
            (routing is None and (proxy_maximum_requests is not None or proxy_maximum_bytes is not None))
            or (
                routing is not None
                and routing.mode is RoutingMode.NEVER
                and (proxy_maximum_requests is not None or proxy_maximum_bytes is not None)
            )
        ):
            raise ValueError("legacy proxy caps require active proxy routing")
        self.proxy_pool = proxy_pool
        self._legacy_proxy_configuration = legacy_proxy_configuration
        self.proxy_policy = proxy_policy or _legacy_proxy_policy(
            routing,
            proxy_maximum_requests,
            proxy_maximum_bytes,
        )
        self.routing = _routing_from_policy(self.proxy_policy)
        self.proxy_transport_factory = proxy_transport_factory
        self.proxy_browser_transport_factory = proxy_browser_transport_factory
        self.require_proxy_browser_subrequest_authorization = require_proxy_browser_subrequest_authorization
        self.proxy_maximum_requests = self.proxy_policy.maximum_requests
        self.proxy_maximum_bytes = self.proxy_policy.maximum_bytes
        if self.proxy_policy.mode is not ProxyMode.NEVER and (
            self.proxy_pool is None or self.proxy_transport_factory is None
        ):
            raise ValueError("active proxy_policy requires both proxy_pool and proxy_transport_factory")
        self.budget = budget
        self.telemetry = safe_telemetry(telemetry) if telemetry is not None else None
        self.fetch_policy = fetch_policy
        self.cache = cache
        self.stale_on_error = stale_on_error
        self.robots_factory = robots_factory or CachedRobotsChecker
        self.rate_limiter_factory = rate_limiter_factory or (
            lambda policy: PerOriginRateLimiter(
                delay=policy.delay,
                concurrency=policy.concurrency,
            )
        )
        if retries < 0:
            raise ValueError("retries must be non-negative")
        self.retries = retries
        self.backoff = backoff
        self.owns_transport = owns_transport
        self.browser_transport = browser_transport
        self.owns_browser_transport = owns_browser_transport
        if maximum_response_bytes < 1:
            raise ValueError("maximum_response_bytes must be positive")
        self.maximum_response_bytes = maximum_response_bytes

    async def collect(
        self,
        source: SourceDefinition,
        *,
        requested_fields: frozenset[SnapshotField] = frozenset(SnapshotField),
        refresh_mode: RefreshMode = RefreshMode.FULL,
        result_limit: int | None = None,
        partitions: tuple[str, ...] = (),
        deadline: datetime | None = None,
        checkpoint: ConnectorCheckpoint | None = None,
        fetch_policy: FetchPolicy | None = None,
        proxy_policy: ProxyPolicyConfig | None = None,
        budget: RequestBudget | None = None,
        browser_transport: BrowserTransport | None = None,
        collection_id: str | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]:
        request = CollectionRequest(
            source_id=source.id,
            base_url=source.base_url,
            refresh_mode=refresh_mode,
            requested_fields=requested_fields,
            result_limit=result_limit,
            partitions=partitions,
            deadline=deadline,
        )
        async with self.open_connector(
            source,
            fetch_policy=fetch_policy,
            proxy_policy=proxy_policy,
            budget=budget,
            browser_transport=browser_transport,
            collection_id=collection_id,
            cancelled=cancelled,
        ) as connector:
            async for page in connector.collect(request, checkpoint):
                yield page

    @asynccontextmanager
    async def open_connector(
        self,
        source: SourceDefinition,
        *,
        fetch_policy: FetchPolicy | None = None,
        proxy_policy: ProxyPolicyConfig | None = None,
        budget: RequestBudget | None = None,
        browser_transport: BrowserTransport | None = None,
        collection_id: str | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> AsyncIterator[CommerceConnector]:
        """Open one connector with collection-scoped policy and route ownership.

        The yielded connector accepts caller-owned requests and checkpoints while
        retaining the same sanitization, deadline, and telemetry boundary as
        :meth:`collect`. Base transports remain borrowed; a sticky routed proxy
        created here is released exactly once when the context exits.
        """
        selected_policy = fetch_policy or self.fetch_policy
        if proxy_policy is not None and self._legacy_proxy_configuration:
            raise ValueError("per-collection proxy_policy cannot override legacy proxy configuration")
        selected_proxy_policy = proxy_policy or self.proxy_policy
        selected_routing = _routing_from_policy(selected_proxy_policy)
        selected_budget = budget if budget is not None else self.budget
        selected_browser = browser_transport if browser_transport is not None else self.browser_transport
        telemetry_context: dict[str, JsonValue] = (
            {
                "collection_id": collection_id or uuid4().hex,
                "source_id": source.id,
                "connector": source.connector,
                "connector_version": self.registry.connector_version(source.connector),
            }
            if self.telemetry is not None
            else {}
        )
        attempt_transport: CommerceTransport = self.transport
        routed: RoutedTransport | None = None
        limiter: RateLimiter | None = None
        proxy_factory = self.proxy_transport_factory
        route_uses_proxy = selected_proxy_policy.mode is not ProxyMode.NEVER
        if (
            route_uses_proxy
            and self.require_proxy_browser_subrequest_authorization
            and self.proxy_browser_transport_factory is not None
            and getattr(
                self.proxy_browser_transport_factory,
                "browser_subrequests_authorized",
                False,
            )
            is not True
        ):
            raise ProxyBrowserRoutingUnsupported(
                "proxy browser factory must authorize every physical subrequest"
            )
        browser_policy = selected_policy.browser if selected_policy is not None else BrowserPolicy.ALLOW
        attempt_transport = BrowserDispatchTransport(
            attempt_transport,
            selected_browser,
            policy=browser_policy,
            proxy_browser_supported=(
                not route_uses_proxy or self.proxy_browser_transport_factory is not None
            ),
            telemetry=self.telemetry,
            telemetry_context=telemetry_context,
            maximum_response_bytes=self.maximum_response_bytes,
        )
        if route_uses_proxy and proxy_factory is not None:
            if self.proxy_pool is None:
                raise ValueError("proxy routing requires both proxy_pool and proxy_transport_factory")
            proxy_factory = _BrowserProxyTransportFactory(
                proxy_factory,
                self.proxy_browser_transport_factory,
                self.proxy_pool,
                target_host=urlsplit(source.base_url).hostname or "",
                policy=browser_policy,
                telemetry=self.telemetry,
                telemetry_context=telemetry_context,
                maximum_response_bytes=self.maximum_response_bytes,
            )
        if selected_policy is not None:
            if route_uses_proxy:
                proxy_limiter = self.rate_limiter_factory(selected_policy)
                if selected_routing.mode is RoutingMode.FALLBACK:
                    direct_limiter = self.rate_limiter_factory(selected_policy)
                    if direct_limiter is proxy_limiter:
                        raise ValueError(
                            "rate_limiter_factory must return independent direct and proxy limiters"
                        )
                    attempt_transport = RateLimitedTransport(
                        attempt_transport,
                        direct_limiter,
                        route="direct",
                        telemetry=self.telemetry,
                        telemetry_context=telemetry_context,
                    )
                if proxy_factory is not None:
                    proxy_factory = _RateLimitedProxyTransportFactory(
                        proxy_factory,
                        proxy_limiter,
                        telemetry=self.telemetry,
                        telemetry_context=telemetry_context,
                    )
            else:
                limiter = self.rate_limiter_factory(selected_policy)
        if route_uses_proxy:
            if self.proxy_pool is None or proxy_factory is None:
                raise ValueError("proxy routing requires both proxy_pool and proxy_transport_factory")
            routed = RoutedTransport(
                attempt_transport,
                pool=self.proxy_pool,
                proxy_factory=proxy_factory,
                routing=selected_routing,
                source_id=source.id,
                base_url=source.base_url,
                maximum_requests=selected_proxy_policy.maximum_requests,
                maximum_bytes=selected_proxy_policy.maximum_bytes,
                telemetry=self.telemetry,
                telemetry_context=telemetry_context,
            )
            attempt_transport = routed
        robots: RobotsChecker | None = None
        if selected_policy is not None and selected_policy.robots is RobotsPolicy.OBEY:
            robots_attempts = MiddlewareTransport(
                attempt_transport,
                budget=selected_budget,
                rate_limiter=limiter,
                retries=self.retries,
                backoff=self.backoff,
                telemetry=self.telemetry,
                telemetry_context=telemetry_context,
                maximum_response_bytes=self.maximum_response_bytes,
            )
            robots = self.robots_factory(robots_attempts)
        if any(
            value is not None
            for value in (
                selected_budget,
                self.telemetry,
                self.cache,
                robots,
                limiter,
            )
        ) or (routed is not None and selected_routing.mode in {RoutingMode.FALLBACK, RoutingMode.FAILOVER}):
            attempt_transport = MiddlewareTransport(
                attempt_transport,
                budget=selected_budget,
                cache=self.cache,
                stale_on_error=self.stale_on_error,
                robots=robots,
                rate_limiter=limiter,
                retries=self.retries,
                backoff=self.backoff,
                telemetry=self.telemetry,
                telemetry_context=telemetry_context,
                maximum_response_bytes=self.maximum_response_bytes,
            )
        try:
            context = ConnectorContext(
                budget=selected_budget,
                telemetry=(
                    _ContextualTelemetry(self.telemetry, telemetry_context)
                    if self.telemetry is not None
                    else None
                ),
                cancelled=cancelled,
            )
            connector = self.registry.build(
                source.connector,
                transport=attempt_transport,
                options=source.connector_options,
                context=context,
            )
            if (
                selected_policy is not None
                and selected_policy.browser.value == "never"
                and connector.capabilities.browser.value == "required"
            ):
                raise ValueError(
                    f"connector {connector.name!r} requires a browser but fetch policy forbids it"
                )
            if (
                selected_policy is not None
                and selected_policy.browser.value == "require"
                and selected_browser is None
                and not (route_uses_proxy and self.proxy_browser_transport_factory is not None)
            ):
                raise ValueError("fetch policy requires a browser but no backend is configured")
            yield _CollectionLifecycleConnector(
                connector,
                scraper=self,
                telemetry_context=telemetry_context,
            )
        finally:
            if routed is not None:
                await routed.aclose()

    def _emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        if self.telemetry is not None:
            self.telemetry.emit(event, fields)

    def _emit_page(
        self,
        context: dict[str, JsonValue],
        page: EntityPage[CommerceProductSnapshot],
    ) -> None:
        self._emit(
            "connector.page_emitted",
            {
                **context,
                "level": "debug",
                "partition": page.partition_key,
                "sequence": page.sequence,
                "emitted": len(page.items),
                "discovered": page.discovered,
                "diagnostics": len(page.diagnostics),
                "terminal": page.terminal,
                "enumeration_intact": page.enumeration_intact,
            },
        )
        for diagnostic in page.diagnostics:
            self._emit(
                "connector.diagnostic",
                {
                    **context,
                    "level": diagnostic.severity.value,
                    "partition": page.partition_key,
                    "sequence": page.sequence,
                    "code": diagnostic.code.value,
                    "severity": diagnostic.severity.value,
                    "retryable": diagnostic.retryable,
                    "affects_completeness": diagnostic.affects_completeness,
                },
            )

    async def __aenter__(self) -> CommerceScraper:
        return self

    async def __aexit__(self, *_: object) -> None:
        try:
            if self.owns_transport and hasattr(self.transport, "aclose"):
                await self._close_owned_resource("http", cast(_AsyncCloseable, self.transport))
        finally:
            if (
                self.owns_browser_transport
                and self.browser_transport is not self.transport
                and hasattr(self.browser_transport, "aclose")
            ):
                await self._close_owned_resource(
                    "browser",
                    cast(_AsyncCloseable, self.browser_transport),
                )

    async def _close_owned_resource(self, resource: str, target: _AsyncCloseable) -> None:
        started = monotonic()
        self._emit(
            "runtime.cleanup_started",
            {"level": "debug", "resource": resource},
        )
        try:
            await target.aclose()
        except BaseException as error:
            self._emit(
                "runtime.cleanup_failed",
                {
                    "level": "warning",
                    "resource": resource,
                    "elapsed_ms": round((monotonic() - started) * 1_000, 3),
                    "error_type": type(error).__name__,
                    "cancelled": isinstance(error, asyncio.CancelledError),
                },
            )
            raise
        self._emit(
            "runtime.cleanup_completed",
            {
                "level": "debug",
                "resource": resource,
                "elapsed_ms": round((monotonic() - started) * 1_000, 3),
            },
        )


def _sanitize_page(
    page: EntityPage[CommerceProductSnapshot],
) -> EntityPage[CommerceProductSnapshot]:
    """Enforce extension safety at the public runtime/plugin egress boundary."""

    return page.model_copy(
        update={
            "items": tuple(sanitize_commerce_snapshot(item) for item in page.items),
            "diagnostics": tuple(sanitize_diagnostic(diagnostic) for diagnostic in page.diagnostics),
        }
    )
