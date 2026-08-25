"""Neutral proxy-browser composition and accounting boundaries."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Literal, cast

import pytest
from mb_commerce_scraper.proxy import (
    BrowserProxyCredentials,
    BrowserSubrequestAuthorizer,
    PoolBrowserSubrequestAuthorizer,
    ProxyBrowserTransportFactory,
    ProxyRequest,
)
from mb_commerce_scraper.testing import fake_proxy_pool
from mb_commerce_scraper.transports import (
    BrowserEvaluation,
    BrowserHint,
    RequestPriority,
    RequestPurpose,
    ResponseBodyTooLarge,
    RotationReason,
    TransportFailure,
    TransportRequest,
)

from mb_ceramics_catalogue.ops.commerce_scraper_browser import (
    BorrowedBrowserTransport,
    BrowserOriginDenied,
    CamoufoxProxyBrowserTransportFactory,
)
from mb_ceramics_catalogue.transports.browser import (
    BrowserEvaluationResult,
    BrowserFetchResponse,
    BrowserJobContext,
    BrowserNetworkAccounting,
    BrowserUnavailable,
    TransportBlocked,
)


class BorrowedSession:
    def __init__(
        self,
        content: bytes = b'{"data":{"ok":true}}',
        final_url: str | None = None,
    ) -> None:
        self.content = content
        self.final_url = final_url
        self.closed = 0
        self.renders: list[str] = []
        self.browser_requests: list[dict[str, Any]] = []
        self.evaluations: list[tuple[str, str, int, str | None]] = []

    async def render(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None
    ) -> str:
        del wait_ms, wait_for
        self.renders.append(url)
        return "<html>rendered</html>"

    async def evaluate(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("not used")

    async def evaluate_result(
        self, url: str, script: str, wait_ms: int = 2000,
        wait_for: str | None = None,
    ) -> BrowserEvaluationResult:
        self.evaluations.append((url, script, wait_ms, wait_for))
        return BrowserEvaluationResult(
            value=[{"pack": "5 kg", "price": "26.65"}],
            final_url=self.final_url or url,
        )

    async def request(
        self,
        page_url: str,
        endpoint: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        json_body: Any = None,
    ) -> BrowserFetchResponse:
        self.browser_requests.append(
            {
                "page_url": page_url,
                "endpoint": endpoint,
                "method": method,
                "headers": headers,
                "json_body": json_body,
            }
        )
        return BrowserFetchResponse(
            status=202,
            headers={"content-type": "application/json", "x-browser": "yes"},
            content=self.content,
            final_url=self.final_url or f"{endpoint}#complete",
        )

    async def request_json(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("not used")

    async def close(self) -> None:
        self.closed += 1


class BorrowedBackend:
    backend: Literal["camoufox"] = "camoufox"

    def __init__(
        self,
        content: bytes = b'{"data":{"ok":true}}',
        final_url: str | None = None,
    ) -> None:
        self.content = content
        self.final_url = final_url
        self.sessions: list[BorrowedSession] = []
        self.jobs: list[BrowserJobContext | None] = []
        self.shutdown_calls = 0
        self.unavailable: BrowserUnavailable | None = None

    @asynccontextmanager
    async def open_session(self, job: BrowserJobContext | None = None):
        if self.unavailable is not None:
            raise self.unavailable
        session = BorrowedSession(self.content, self.final_url)
        self.sessions.append(session)
        self.jobs.append(job)
        try:
            yield session
        finally:
            await session.close()

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


async def test_borrowed_browser_reuses_session_and_preserves_request_semantics() -> None:
    backend = BorrowedBackend()
    job = BrowserJobContext("job-1", "profile-1")
    transport = BorrowedBrowserTransport(
        backend, job, allowed_origins=("https://shop.test",)
    )

    rendered = await transport.request(browser_request())
    graphql = await transport.request(
        TransportRequest(
            method="POST",
            url="https://shop.test/graphql",
            query={"channel": 2},
            headers={
                "authorization": "Bearer public-token",
                "referer": "https://shop.test/catalogue",
            },
            json_body={"query": "query Products { site { products { edges } } }"},
            purpose=RequestPurpose.DISCOVERY,
            priority=RequestPriority.DISCOVERY,
            browser=BrowserHint.REQUIRED,
        )
    )

    assert backend.jobs == [job]
    assert len(backend.sessions) == 1
    assert backend.sessions[0].renders == ["https://shop.test/product"]
    assert rendered.status == 200
    assert rendered.headers == {"content-type": "text/html; charset=utf-8"}
    assert rendered.content == b"<html>rendered</html>"
    assert rendered.final_url == "https://shop.test/product"
    assert rendered.route.kind == "browser"
    assert rendered.accounting is not None
    assert rendered.accounting.physical_requests == 1
    assert rendered.accounting.received_bytes == len(rendered.content)
    assert graphql.status == 202
    assert graphql.headers == {
        "content-type": "application/json",
        "x-browser": "yes",
    }
    assert graphql.content == b'{"data":{"ok":true}}'
    assert graphql.final_url == "https://shop.test/graphql?channel=2#complete"
    assert graphql.route.kind == "browser"
    assert graphql.accounting is not None
    assert graphql.accounting.physical_requests == 1
    assert graphql.accounting.transmitted_bytes > 0
    assert graphql.accounting.received_bytes == len(graphql.content)
    assert backend.sessions[0].browser_requests == [
        {
            "page_url": "https://shop.test/catalogue",
            "endpoint": "https://shop.test/graphql?channel=2",
            "method": "POST",
            "headers": {
                "authorization": "Bearer public-token",
                "referer": "https://shop.test/catalogue",
            },
            "json_body": {
                "query": "query Products { site { products { edges } } }"
            },
        }
    ]

    await transport.aclose()
    await transport.aclose()
    assert backend.sessions[0].closed == 1
    assert backend.shutdown_calls == 0


async def test_borrowed_browser_rotation_replaces_only_the_job_session() -> None:
    backend = BorrowedBackend()
    transport = BorrowedBrowserTransport(
        backend,
        BrowserJobContext("job-1"),
        allowed_origins=("https://shop.test",),
    )

    await transport.request(browser_request())
    await transport.rotate_identity(RotationReason.BLOCKED)
    await transport.request(browser_request())
    await transport.aclose()

    assert len(backend.sessions) == 2
    assert [session.closed for session in backend.sessions] == [1, 1]
    assert backend.shutdown_calls == 0


async def test_borrowed_browser_evaluation_returns_json_and_validates_final_origin() -> None:
    backend = BorrowedBackend()
    transport = BorrowedBrowserTransport(
        backend,
        BrowserJobContext("job-1"),
        allowed_origins=("https://shop.test",),
    )
    attempted = TransportRequest(
        url="https://shop.test/product",
        purpose=RequestPurpose.ENRICHMENT,
        priority=RequestPriority.OPTIONAL,
        required=False,
        browser=BrowserHint.REQUIRED,
        evaluation=BrowserEvaluation(
            action_id="ceramicolours.pack-prices.v1",
            script="() => []",
            wait_for="#product-pack-field",
            wait_milliseconds=1500,
        ),
    )

    response = await transport.request(attempted)

    assert response.json_value() == [{"pack": "5 kg", "price": "26.65"}]
    assert response.final_url == attempted.url
    assert response.route.kind == "browser"
    assert backend.sessions[0].evaluations == [
        (
            attempted.url,
            "() => []",
            1500,
            "#product-pack-field",
        )
    ]
    assert response.accounting is not None
    assert response.accounting.physical_requests == 1
    await transport.aclose()

    redirected = BorrowedBackend(final_url="https://other.test/private")
    denied = BorrowedBrowserTransport(
        redirected,
        BrowserJobContext("job-2"),
        allowed_origins=("https://shop.test",),
    )
    with pytest.raises(TransportFailure, match="BrowserOriginDenied"):
        await denied.request(attempted)
    await denied.aclose()


async def test_borrowed_browser_enforces_the_retained_response_limit() -> None:
    backend = BorrowedBackend(b"x" * 17)
    transport = BorrowedBrowserTransport(
        backend,
        BrowserJobContext("job-1"),
        allowed_origins=("https://shop.test",),
        maximum_response_bytes=16,
    )
    request = TransportRequest(
        method="POST",
        url="https://shop.test/graphql",
        json_body={"query": "query Products { site { products { edges } } }"},
        purpose=RequestPurpose.DISCOVERY,
        priority=RequestPriority.DISCOVERY,
        browser=BrowserHint.REQUIRED,
    )

    with pytest.raises(ResponseBodyTooLarge) as caught:
        await transport.request(request)

    assert caught.value.accounting is not None
    assert caught.value.accounting is not None
    assert caught.value.accounting.physical_requests == 1
    assert caught.value.accounting.received_bytes == 17
    await transport.aclose()
    assert backend.sessions[0].closed == 1
    assert backend.shutdown_calls == 0


async def test_borrowed_browser_preserves_application_unavailability() -> None:
    backend = BorrowedBackend()
    unavailable = BrowserUnavailable("selected backend is unavailable")
    backend.unavailable = unavailable
    transport = BorrowedBrowserTransport(
        backend,
        BrowserJobContext("job-1"),
        allowed_origins=("https://shop.test",),
    )

    with pytest.raises(BrowserUnavailable) as caught:
        await transport.request(browser_request())

    assert caught.value is not unavailable
    assert str(caught.value) == "browser backend is unavailable"
    assert cast(Any, caught.value).accounting.physical_requests == 0
    await transport.aclose()
    assert backend.shutdown_calls == 0


@pytest.mark.parametrize(
    "attempted",
    (
        TransportRequest(
            url="https://other.test/product",
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
            browser=BrowserHint.REQUIRED,
        ),
        TransportRequest(
            method="POST",
            url="https://shop.test/graphql",
            headers={"referer": "https://other.test/secret"},
            json_body={"query": "products"},
            purpose=RequestPurpose.DISCOVERY,
            priority=RequestPriority.DISCOVERY,
            browser=BrowserHint.REQUIRED,
        ),
    ),
)
async def test_borrowed_browser_rejects_unapproved_endpoint_and_page_origin(
    attempted: TransportRequest,
) -> None:
    backend = BorrowedBackend()
    transport = BorrowedBrowserTransport(
        backend,
        BrowserJobContext("job-1"),
        allowed_origins=("https://shop.test",),
    )

    with pytest.raises(BrowserOriginDenied) as caught:
        await transport.request(attempted)

    assert str(caught.value) == "browser request origin is not allowed"
    assert caught.value.accounting.physical_requests == 0
    assert backend.sessions == []
    await transport.aclose()


async def test_borrowed_browser_rejects_off_origin_final_url_after_dispatch() -> None:
    backend = BorrowedBackend(final_url="https://other.test/redirected")
    transport = BorrowedBrowserTransport(
        backend,
        BrowserJobContext("job-1"),
        allowed_origins=("https://shop.test",),
    )

    with pytest.raises(TransportFailure, match="BrowserOriginDenied") as caught:
        await transport.request(
            TransportRequest(
                method="POST",
                url="https://shop.test/graphql",
                json_body={"query": "products"},
                purpose=RequestPurpose.DISCOVERY,
                priority=RequestPriority.DISCOVERY,
                browser=BrowserHint.REQUIRED,
            )
        )

    assert caught.value.accounting is not None
    assert caught.value.accounting.physical_requests == 1
    assert "other.test" not in str(caught.value)
    await transport.aclose()


class FakeSession:
    def __init__(self, accounting: BrowserNetworkAccounting) -> None:
        self.accounting = accounting
        self.closed = False
        self.blocked = False
        self.started = asyncio.Event()
        self.resume = asyncio.Event()
        self.gated = False
        self.browser_requests: list[dict[str, Any]] = []
        self.final_url: str | None = None

    async def render(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None
    ) -> str:
        del url, wait_ms, wait_for
        self.accounting.record(100, 200, 2)
        if self.blocked:
            raise TransportBlocked("provider leaked secret-value")
        if self.gated:
            self.started.set()
            await self.resume.wait()
        return "<html>rendered</html>"

    async def evaluate(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("not used")

    async def evaluate_result(
        self, url: str, script: str, wait_ms: int = 2000,
        wait_for: str | None = None,
    ) -> BrowserEvaluationResult:
        del script, wait_ms, wait_for
        self.accounting.record(100, 200, 2)
        return BrowserEvaluationResult(
            value={"ok": True}, final_url=self.final_url or url
        )

    async def request(
        self,
        page_url: str,
        endpoint: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        json_body: Any = None,
    ) -> BrowserFetchResponse:
        self.browser_requests.append(
            {
                "page_url": page_url,
                "endpoint": endpoint,
                "method": method,
                "headers": headers,
                "json_body": json_body,
            }
        )
        self.accounting.record(150, 250, 2)
        return BrowserFetchResponse(
            status=201,
            headers={"content-type": "application/json"},
            content=b'{"data":{"ok":true}}',
            final_url=self.final_url or endpoint,
        )

    async def request_json(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("not used")

    async def close(self) -> None:
        self.closed = True


class FakeBackend:
    backend = "camoufox"

    def __init__(self, accounting: BrowserNetworkAccounting) -> None:
        self.session = FakeSession(accounting)
        self.opened = 0
        self.shutdown_called = False

    @asynccontextmanager
    async def open_session(self, _job: Any = None):
        self.opened += 1
        try:
            yield self.session
        finally:
            await self.session.close()

    async def shutdown(self) -> None:
        self.shutdown_called = True


def browser_request() -> TransportRequest:
    return TransportRequest(
        url="https://shop.test/product",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        browser=BrowserHint.REQUIRED,
    )


async def build_transport():
    pool = fake_proxy_pool("one")
    lease = await pool.acquire(
        ProxyRequest(source_id="shop", target_host="shop.test")
    )
    captured: list[BrowserProxyCredentials] = []
    backends: list[FakeBackend] = []

    def backend_factory(
        credentials: BrowserProxyCredentials,
        accounting: BrowserNetworkAccounting,
        authorizer: BrowserSubrequestAuthorizer,
    ) -> FakeBackend:
        del authorizer
        captured.append(credentials)
        backend = FakeBackend(accounting)
        backends.append(backend)
        return backend

    factory = CamoufoxProxyBrowserTransportFactory(
        backend_factory,
        allowed_origins=("https://shop.test",),
    )
    _contract: ProxyBrowserTransportFactory = factory
    return (
        factory.build(
            lease,
            PoolBrowserSubrequestAuthorizer(pool, lease, "shop.test"),
        ),
        captured,
        backends,
    )


async def test_proxy_browser_projects_credentials_and_isolates_request_deltas() -> None:
    transport, captured, backends = await build_transport()
    assert len(captured) == 1
    assert "secret" not in repr(transport)

    first = await transport.request(browser_request())
    second = await transport.request(browser_request())

    assert first.route.kind == "browser"
    assert first.accounting is not None
    assert first.accounting.physical_requests == 2
    assert first.accounting.transmitted_bytes == 100
    assert first.accounting.received_bytes == 200
    assert second.accounting == first.accounting
    assert backends[0].opened == 1

    await transport.aclose()
    await transport.aclose()
    assert backends[0].session.closed
    assert backends[0].shutdown_called


async def test_proxy_browser_preserves_json_post_request_semantics() -> None:
    transport, _captured, backends = await build_transport()
    response = await transport.request(
        TransportRequest(
            method="POST",
            url="https://shop.test/graphql",
            query={"channel": 2},
            headers={
                "authorization": "Bearer public-token",
                "referer": "https://shop.test/catalogue",
            },
            json_body={"query": "query Products { site { products { edges } } }"},
            purpose=RequestPurpose.DISCOVERY,
            priority=RequestPriority.DISCOVERY,
            browser=BrowserHint.REQUIRED,
        )
    )

    assert response.status == 201
    assert response.json_value() == {"data": {"ok": True}}
    assert response.final_url == "https://shop.test/graphql?channel=2"
    assert backends[0].session.browser_requests == [
        {
            "page_url": "https://shop.test/catalogue",
            "endpoint": "https://shop.test/graphql?channel=2",
            "method": "POST",
            "headers": {
                "authorization": "Bearer public-token",
                "referer": "https://shop.test/catalogue",
            },
            "json_body": {
                "query": "query Products { site { products { edges } } }"
            },
        }
    ]
    assert response.accounting is not None
    assert response.accounting.physical_requests == 2
    assert response.accounting.transmitted_bytes == 150
    assert response.accounting.received_bytes == 250
    await transport.aclose()


async def test_proxy_browser_evaluation_preserves_accounted_session_delta() -> None:
    transport, _captured, backends = await build_transport()
    attempted = TransportRequest(
        url="https://shop.test/product",
        purpose=RequestPurpose.ENRICHMENT,
        priority=RequestPriority.OPTIONAL,
        required=False,
        browser=BrowserHint.REQUIRED,
        evaluation=BrowserEvaluation(
            action_id="shop.offer.v1",
            script="() => ({ok: true})",
        ),
    )

    response = await transport.request(attempted)

    assert response.json_value() == {"ok": True}
    assert response.final_url == attempted.url
    assert response.accounting is not None
    assert response.accounting.physical_requests == 2
    assert response.accounting.transmitted_bytes == 100
    assert response.accounting.received_bytes == 200
    assert backends[0].opened == 1
    await transport.aclose()


async def test_proxy_browser_rejects_unapproved_referer_before_session_io() -> None:
    transport, _captured, backends = await build_transport()
    attempted = TransportRequest(
        method="POST",
        url="https://shop.test/graphql",
        headers={"referer": "https://other.test/private"},
        json_body={"query": "products"},
        purpose=RequestPurpose.DISCOVERY,
        priority=RequestPriority.DISCOVERY,
        browser=BrowserHint.REQUIRED,
    )

    with pytest.raises(BrowserOriginDenied) as caught:
        await transport.request(attempted)

    assert caught.value.accounting.physical_requests == 0
    assert backends[0].opened == 0
    await transport.aclose()


async def test_proxy_browser_rejects_off_origin_final_url_after_dispatch() -> None:
    transport, _captured, backends = await build_transport()
    backends[0].session.final_url = "https://other.test/private"

    with pytest.raises(TransportFailure, match="BrowserOriginDenied") as caught:
        await transport.request(
            TransportRequest(
                method="POST",
                url="https://shop.test/graphql",
                json_body={"query": "products"},
                purpose=RequestPurpose.DISCOVERY,
                priority=RequestPriority.DISCOVERY,
                browser=BrowserHint.REQUIRED,
            )
        )

    assert "other.test" not in str(caught.value)
    assert caught.value.accounting is not None
    assert caught.value.accounting.physical_requests == 2
    await transport.aclose()


async def test_proxy_browser_detaches_blocked_provider_details() -> None:
    transport, _captured, backends = await build_transport()
    backends[0].session.blocked = True

    with pytest.raises(TransportFailure) as caught:
        await transport.request(browser_request())

    assert "secret-value" not in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert caught.value.accounting is not None
    assert caught.value.accounting.physical_requests == 2
    await transport.aclose()


async def test_proxy_browser_cancellation_carries_partial_accounting() -> None:
    transport, _captured, backends = await build_transport()
    backends[0].session.gated = True
    task = asyncio.create_task(transport.request(browser_request()))
    await backends[0].session.started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    accounting = getattr(caught.value, "accounting", None)
    assert accounting is not None
    assert accounting.physical_requests == 2
    assert accounting.transmitted_bytes == 100
    assert accounting.received_bytes == 200
    await transport.aclose()
