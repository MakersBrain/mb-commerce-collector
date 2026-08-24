from __future__ import annotations

from mb_commerce_scraper.connectors import ConnectorRegistry
from mb_commerce_scraper.models import BrowserPolicy, FetchPolicy, ProxyPolicyConfig
from mb_commerce_scraper.proxy import (
    HttpxProxyTransportFactory,
    ProxyBrowserTransportFactory,
    ProxyPool,
)
from mb_commerce_scraper.transports import (
    DEFAULT_MAXIMUM_RESPONSE_BYTES,
    BrowserTransport,
    CachedRobotsChecker,
    RobotsFetchFailurePolicy,
)
from mb_commerce_scraper.transports.base import (
    CommerceTransport,
    RequestBudget,
    ResponseCache,
    TelemetryHooks,
)
from mb_commerce_scraper.transports.httpx import HttpxTransport

from .client import CommerceScraper


def build_http_scraper(
    *,
    allowed_origins: tuple[str, ...],
    registry: ConnectorRegistry | None = None,
    timeout: float = 30.0,
    fetch_policy: FetchPolicy | None = None,
    cache: ResponseCache | None = None,
    stale_on_error: bool = False,
    budget: RequestBudget | None = None,
    telemetry: TelemetryHooks | None = None,
    retries: int = 2,
    robots_user_agent: str = "*",
    robots_failure_policy: RobotsFetchFailurePolicy = RobotsFetchFailurePolicy.DENY,
    robots_transport_failure_policy: RobotsFetchFailurePolicy | None = None,
    robots_server_failure_policy: RobotsFetchFailurePolicy | None = None,
    robots_cache_origins: int = 1_000,
    proxy_pool: ProxyPool | None = None,
    proxy_policy: ProxyPolicyConfig | None = None,
    proxy_browser_transport_factory: ProxyBrowserTransportFactory | None = None,
    require_proxy_browser_subrequest_authorization: bool = False,
    browser_transport: BrowserTransport | None = None,
    owns_browser_transport: bool = False,
    maximum_response_bytes: int = DEFAULT_MAXIMUM_RESPONSE_BYTES,
) -> CommerceScraper:
    selected_policy = fetch_policy or FetchPolicy(
        timeout_seconds=timeout,
        browser=(
            BrowserPolicy.ALLOW
            if (browser_transport is not None or proxy_browser_transport_factory is not None)
            else BrowserPolicy.NEVER
        ),
    )
    request_timeout = selected_policy.timeout_seconds
    transport = HttpxTransport(
        allowed_origins=allowed_origins,
        timeout=request_timeout,
        maximum_response_bytes=maximum_response_bytes,
    )

    def robots_factory(raw_transport: CommerceTransport) -> CachedRobotsChecker:
        return CachedRobotsChecker(
            raw_transport,
            user_agent=robots_user_agent,
            failure_policy=robots_failure_policy,
            transport_failure_policy=robots_transport_failure_policy,
            server_failure_policy=robots_server_failure_policy,
            maximum_origins=robots_cache_origins,
        )

    return CommerceScraper(
        registry=registry or ConnectorRegistry.with_builtins(),
        transport=transport,
        proxy_pool=proxy_pool,
        proxy_policy=proxy_policy,
        proxy_transport_factory=(
            HttpxProxyTransportFactory(
                allowed_origins=allowed_origins,
                timeout=request_timeout,
                maximum_response_bytes=maximum_response_bytes,
            )
            if proxy_pool is not None
            else None
        ),
        proxy_browser_transport_factory=proxy_browser_transport_factory,
        require_proxy_browser_subrequest_authorization=(require_proxy_browser_subrequest_authorization),
        budget=budget,
        telemetry=telemetry,
        fetch_policy=selected_policy,
        cache=cache,
        stale_on_error=stale_on_error,
        robots_factory=robots_factory,
        retries=retries,
        owns_transport=True,
        browser_transport=browser_transport,
        owns_browser_transport=owns_browser_transport,
        maximum_response_bytes=maximum_response_bytes,
    )
