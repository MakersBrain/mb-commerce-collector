from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from mb_commerce_scraper.proxy import (
    BrowserSubrequestAuthorization,
    BrowserSubrequestOutcome,
    ProxyBudgetExhausted,
)
from pydantic import SecretStr

from mb_ceramics_catalogue.scrapers.base import Blocked as LegacyBlocked
from mb_ceramics_catalogue.scrapers.base import BrowserRenderer
from mb_ceramics_catalogue.scrapers.base import (
    BrowserUnavailable as LegacyBrowserUnavailable,
)
from mb_ceramics_catalogue.transports.browser import (
    BrowserJobContext,
    BrowserNetworkAccounting,
    BrowserSession,
    BrowserUnavailable,
    TransportBlocked,
)
from mb_ceramics_catalogue.transports.cdp_extension_proxy import (
    CdpEndpointAttestation,
    CdpEndpointLease,
    CdpExtensionProxyBackend,
    CdpOperatorProfile,
    CdpReadinessError,
    HttpCdpEndpointProvider,
    MappingSecretResolver,
    StaticCdpEndpointProvider,
    validate_cdp_readiness,
)


class MeterToken:
    def __init__(self) -> None:
        self.outcomes: list[BrowserSubrequestOutcome] = []
        self.released = 0

    async def reconcile(self, outcome: BrowserSubrequestOutcome) -> None:
        self.outcomes.append(outcome)

    async def release(self) -> None:
        self.released += 1


class MeterAuthorizer:
    def __init__(self, *, deny: bool = False) -> None:
        self.deny = deny
        self.estimates: list[int] = []
        self.tokens: list[MeterToken] = []

    async def authorize(
        self, estimated_bytes: int
    ) -> BrowserSubrequestAuthorization | None:
        self.estimates.append(estimated_bytes)
        if self.deny:
            return None
        token = MeterToken()
        self.tokens.append(token)
        return token


class MeterResponse:
    def __init__(self) -> None:
        self.status = 200
        self.headers = {"content-length": "321"}


class MeterRequest:
    method = "GET"
    url = "https://shop.test/app.js"
    resource_type = "script"

    async def response(self) -> MeterResponse:
        return MeterResponse()


class MeterRoute:
    def __init__(self, *, block_continue: bool = False) -> None:
        self.aborted = False
        self.continued = False
        self.block_continue = block_continue
        self.started = asyncio.Event()

    async def abort(self) -> None:
        self.aborted = True

    async def continue_(self) -> None:
        self.continued = True
        self.started.set()
        if self.block_continue:
            await asyncio.Future()


class MeterPage:
    def __init__(self) -> None:
        self.route_handler: Any = None
        self.listeners: dict[str, Any] = {}

    async def route(self, _pattern: str, handler: Any) -> None:
        self.route_handler = handler

    def on(self, event: str, handler: Any) -> None:
        self.listeners[event] = handler


async def test_camoufox_meter_authorizes_before_continue_and_reconciles_once() -> None:
    accounting = BrowserNetworkAccounting()
    authorizer = MeterAuthorizer()
    renderer = BrowserRenderer(
        True,
        proxy_configuration={"server": "http://proxy.test"},
        network_accounting=accounting,
        subrequest_authorizer=authorizer,
    )
    page = MeterPage()
    meter = await renderer._meter_page(page)
    assert meter is not None

    blocked = MeterRequest()
    blocked.resource_type = "image"
    blocked_route = MeterRoute()
    await page.route_handler(blocked_route, blocked)
    assert blocked_route.aborted and authorizer.estimates == []

    request = MeterRequest()
    route = MeterRoute()
    await page.route_handler(route, request)
    assert route.continued
    assert authorizer.estimates and len(authorizer.tokens) == 1
    page.listeners["requestfinished"](request)
    page.listeners["requestfailed"](request)
    await meter.drain()

    token = authorizer.tokens[0]
    assert token.released == 0
    assert len(token.outcomes) == 1
    assert token.outcomes[0].classification == "success"
    assert token.outcomes[0].received_bytes == 321
    assert accounting.snapshot() == (1, authorizer.estimates[0], 321)
    await meter.close("cancelled")
    assert len(token.outcomes) == 1


async def test_camoufox_meter_cancellation_reconciles_inflight_callback_once() -> None:
    accounting = BrowserNetworkAccounting()
    authorizer = MeterAuthorizer()
    renderer = BrowserRenderer(
        True,
        proxy_configuration={"server": "http://proxy.test"},
        network_accounting=accounting,
        subrequest_authorizer=authorizer,
    )
    page = MeterPage()
    meter = await renderer._meter_page(page)
    assert meter is not None
    request = MeterRequest()
    route = MeterRoute(block_continue=True)
    callback = asyncio.create_task(page.route_handler(route, request))
    await route.started.wait()

    callback.cancel()
    with pytest.raises(asyncio.CancelledError):
        await callback
    page.listeners["requestfailed"](request)
    await meter.close("cancelled")

    token = authorizer.tokens[0]
    assert token.released == 0
    assert [outcome.classification for outcome in token.outcomes] == ["cancelled"]


async def test_camoufox_meter_denial_aborts_without_continuing() -> None:
    renderer = BrowserRenderer(
        True,
        proxy_configuration={"server": "http://proxy.test"},
        network_accounting=BrowserNetworkAccounting(),
        subrequest_authorizer=MeterAuthorizer(deny=True),
    )
    page = MeterPage()
    meter = await renderer._meter_page(page)
    assert meter is not None
    route = MeterRoute()

    with pytest.raises(ProxyBudgetExhausted):
        await page.route_handler(route, MeterRequest())

    assert route.aborted and not route.continued


class FakePage:
    def __init__(self) -> None:
        self.closed = False
        self.urls: list[str] = []
        self.waits: list[int] = []

    @property
    def url(self) -> str:
        return self.urls[-1] if self.urls else "about:blank"

    async def goto(self, url: str, **kwargs: Any) -> None:
        self.urls.append(url)

    async def wait_for_selector(self, selector: str, **kwargs: Any) -> None:
        return None

    async def wait_for_timeout(self, milliseconds: int) -> None:
        self.waits.append(milliseconds)

    async def content(self) -> str:
        return f"<html>{self.urls[-1]}</html>"

    async def evaluate(self, script: str, argument: Any = None) -> Any:
        if "fetch(endpoint" in script:
            return {
                "status": 200,
                "headers": {"content-type": "application/json"},
                "text": '{"available": true}',
                "url": argument["endpoint"],
            }
        return {"script": script}

    async def close(self) -> None:
        self.closed = True

    def is_closed(self) -> bool:
        return self.closed


def test_legacy_browser_exceptions_are_transport_owned_compatibility_aliases() -> None:
    assert LegacyBlocked is TransportBlocked
    assert LegacyBrowserUnavailable is BrowserUnavailable
    assert LegacyBlocked.__name__ == "Blocked"
    assert LegacyBrowserUnavailable.__name__ == "BrowserUnavailable"


class FakeContext:
    def __init__(self) -> None:
        self.pages: list[FakePage] = []
        self.closed = False

    async def new_page(self) -> FakePage:
        page = FakePage()
        self.pages.append(page)
        return page

    async def close(self) -> None:
        self.closed = True


class FakeBrowser:
    def __init__(self) -> None:
        self.default_context = FakeContext()
        self.contexts = [self.default_context]
        self.created_context: FakeContext | None = None
        self.close_called = False

    async def new_context(self) -> FakeContext:
        self.created_context = FakeContext()
        return self.created_context

    async def close(self) -> None:
        self.close_called = True


class FakeChromium:
    def __init__(self, browser: FakeBrowser) -> None:
        self.browser = browser
        self.urls: list[str] = []

    async def connect_over_cdp(self, url: str) -> FakeBrowser:
        self.urls.append(url)
        return self.browser


class FakePlaywright:
    def __init__(self, browser: FakeBrowser) -> None:
        self.chromium = FakeChromium(browser)
        self.stopped = False

    async def stop(self) -> None:
        self.stopped = True


class FakePlaywrightManager:
    def __init__(self, playwright: FakePlaywright) -> None:
        self.playwright = playwright

    async def start(self) -> FakePlaywright:
        return self.playwright


class FakeProvider:
    development_only = False

    def __init__(self, lease: CdpEndpointLease) -> None:
        self.lease = lease
        self.acquired: list[tuple[str, str]] = []
        self.released: list[tuple[str, bool]] = []

    async def acquire(self, job_id: str, logical_profile: str) -> CdpEndpointLease:
        self.acquired.append((job_id, logical_profile))
        return self.lease

    async def release(self, lease: CdpEndpointLease, *, destroy: bool) -> None:
        self.released.append((lease.lease_id, destroy))

    async def shutdown(self) -> None:
        return None


def profile(**changes: Any) -> CdpOperatorProfile:
    values: dict[str, Any] = {
        "name": "clean_chromium",
        "endpoint": "http://127.0.0.1:9222",
        "token_secret_ref": "cdp/token",
        "allowed_worker_pool": "browser-cdp",
        "route": "direct",
        "isolation": "ephemeral_profile",
        "expected_service_version": "0.1.0",
        "expected_profile_generation": "generation-42",
    }
    values.update(changes)
    return CdpOperatorProfile.model_validate(values)


def lease(**changes: Any) -> CdpEndpointLease:
    attestation: dict[str, Any] = {
        "instance_id": "instance-7",
        "service_version": "0.1.0",
        "route": "direct",
        "isolation": "ephemeral_profile",
        "profile_generation": "generation-42",
        "clean_profile": True,
        "capacity": 1,
    }
    attestation.update(changes.pop("attestation", {}))
    values: dict[str, Any] = {
        "lease_id": "lease-9",
        "endpoint": "http://127.0.0.1:9222",
        "token": SecretStr("super-secret-token"),
        "expires_at": datetime.now(UTC) + timedelta(minutes=5),
        "attestation": CdpEndpointAttestation.model_validate(attestation),
    }
    values.update(changes)
    return CdpEndpointLease.model_validate(values)


async def test_camoufox_sessions_do_not_share_origin_pages() -> None:
    renderer = BrowserRenderer(True)
    renderer.browser = FakeContext()

    async with renderer.open_session() as first:
        assert isinstance(first, BrowserSession)
        assert await first.request_json("https://shop.test/a", "https://shop.test/api") == {
            "available": True
        }
        first_page = renderer.browser.pages[0]
    assert first_page.closed

    async with renderer.open_session() as second:
        await second.request_json("https://shop.test/b", "https://shop.test/api")
        second_page = renderer.browser.pages[1]
    assert second_page is not first_page
    assert second_page.closed


def test_operator_profile_projection_omits_endpoint_and_secret_reference() -> None:
    projected = profile().safe_projection()
    assert "endpoint" not in projected
    assert "token_secret_ref" not in projected
    assert projected["route"] == "direct"


def test_endpoint_lease_redacts_token_but_builds_playwright_url() -> None:
    item = lease()
    assert "super-secret-token" not in repr(item)
    assert item.model_dump(mode="json")["token"] == "**********"
    assert item.connection_url().endswith("?token=super-secret-token")


@pytest.mark.parametrize(
    ("profile_changes", "attestation_changes", "message"),
    [
        ({"route": "proxy"}, {"route": "proxy"}, "direct route"),
        (
            {"isolation": "persistent_default"},
            {"isolation": "persistent_default"},
            "not a job isolation boundary",
        ),
        ({}, {"profile_generation": "stale"}, "generation"),
        ({}, {"clean_profile": False}, "clean job profile"),
    ],
)
def test_production_readiness_fails_closed(
    profile_changes: dict[str, Any], attestation_changes: dict[str, Any], message: str,
) -> None:
    with pytest.raises(CdpReadinessError, match=message):
        validate_cdp_readiness(
            profile(**profile_changes), lease(attestation=attestation_changes), production=True
        )


def test_static_provider_is_rejected_in_production() -> None:
    selected = profile()
    provider = StaticCdpEndpointProvider(
        selected, MappingSecretResolver({"cdp/token": "secret"}), lease().attestation
    )
    with pytest.raises(CdpReadinessError, match="development-only"):
        validate_cdp_readiness(
            selected,
            lease(token=provider.resolver.resolve("cdp/token")),
            production=True,
            provider_development_only=provider.development_only,
        )


def test_named_private_endpoint_requires_operator_trust() -> None:
    selected = profile(
        endpoint="https://browser.private.example:9222", network_scope="private"
    )
    endpoint_lease = lease(endpoint="https://browser.private.example:9222")
    with pytest.raises(CdpReadinessError, match="operator-trusted"):
        validate_cdp_readiness(selected, endpoint_lease, production=True)

    trusted = profile(
        endpoint="https://browser.private.example:9222",
        network_scope="private",
        trusted_private_hostnames=frozenset({"browser.private.example"}),
    )
    validate_cdp_readiness(trusted, endpoint_lease, production=True)


async def test_cdp_adapter_contract_and_destructive_cleanup() -> None:
    endpoint_lease = lease()
    provider = FakeProvider(endpoint_lease)
    browser = FakeBrowser()
    playwright = FakePlaywright(browser)
    backend = CdpExtensionProxyBackend(
        profile(), provider, playwright_factory=lambda: FakePlaywrightManager(playwright)
    )

    async with backend.open_session(BrowserJobContext("job-1", "clean_chromium")) as session:
        assert await session.render("https://shop.test/product", 10) == (
            "<html>https://shop.test/product</html>"
        )
        assert await session.evaluate("https://shop.test/product", "() => ({ok: true})") == {
            "script": "() => ({ok: true})"
        }
        assert await session.request_json(
            "https://shop.test/product", "https://shop.test/api"
        ) == {"available": True}

    assert provider.acquired == [("job-1", "clean_chromium")]
    assert provider.released == [("lease-9", True)]
    assert playwright.stopped
    assert not browser.close_called, "an attached client must not shut down operator Chromium"
    assert all(page.closed for page in browser.default_context.pages)
    assert "super-secret-token" in playwright.chromium.urls[0]


async def test_cdp_validation_failure_releases_and_destroys_instance() -> None:
    provider = FakeProvider(lease(attestation={"profile_generation": "old"}))
    backend = CdpExtensionProxyBackend(profile(), provider)

    with pytest.raises(CdpReadinessError, match="generation"):
        async with backend.open_session(BrowserJobContext("job-2")):
            pytest.fail("unsafe endpoint must never be yielded")

    assert provider.released == [("lease-9", True)]


async def test_http_pool_provider_uses_mounted_endpoint_token_and_destroys_lease() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["authorization"] == "Bearer pool-control-secret"
        if request.url.path.endswith("/release"):
            return httpx.Response(204)
        return httpx.Response(
            200,
            json={
                "lease_id": "instance/lease",
                "endpoint": "http://127.0.0.1:9222",
                "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
                "attestation": lease().attestation.model_dump(mode="json"),
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    provider = HttpCdpEndpointProvider(
        "https://pool.private.example",
        SecretStr("pool-control-secret"),
        {"clean_chromium": profile()},
        MappingSecretResolver({"cdp/token": "endpoint-secret"}),
        trusted_private_hostnames=frozenset({"pool.private.example"}),
        client=client,
    )
    allocated = await provider.acquire("job-8", "clean_chromium")
    assert allocated.token.get_secret_value() == "endpoint-secret"
    await provider.release(allocated, destroy=True)

    assert requests[0].url.path == "/v1/cdp/leases"
    assert requests[1].url.raw_path == b"/v1/cdp/leases/instance%2Flease/release"
    assert requests[1].read() == b'{"destroy":true}'
    await client.aclose()


async def test_http_pool_failure_does_not_expose_response_or_tokens() -> None:
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(503, text="endpoint-secret pool-control-secret")
        )
    )
    provider = HttpCdpEndpointProvider(
        "https://pool.private.example",
        SecretStr("pool-control-secret"),
        {"clean_chromium": profile()},
        MappingSecretResolver({"cdp/token": "endpoint-secret"}),
        trusted_private_hostnames=frozenset({"pool.private.example"}),
        client=client,
    )
    with pytest.raises(CdpReadinessError) as failure:
        await provider.acquire("job-8", "clean_chromium")
    assert "endpoint-secret" not in str(failure.value)
    assert "pool-control-secret" not in str(failure.value)
    await client.aclose()
