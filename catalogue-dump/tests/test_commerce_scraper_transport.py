from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
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

from mb_ceramics_catalogue.ops.commerce_scraper_transport import LegacyFetcherTransport
from mb_ceramics_catalogue.proxy import ProxyDenied
from mb_ceramics_catalogue.scrapers.base import (
    Blocked,
    BrowserSession,
    HostLimiter,
    NotCached,
)
from mb_ceramics_catalogue.scrapers.base import Fetcher as CatalogueFetcher
from mb_ceramics_catalogue.scrapers.cache import ResponseCache


class RecordingTelemetry:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def emit(self, event: str, fields: dict[str, Any]) -> None:
        self.events.append((event, fields))


class Fetcher:
    proxy_lease = None

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.stats = SimpleNamespace(proxy_requests=0)
        self.limiter: Any = SimpleNamespace(
            join_group=lambda *args: None, set_delay=lambda *args: None
        )
        self.rotations = 0

    async def response(self, url: str, **kwargs: Any) -> httpx.Response:
        self.calls.append((url, kwargs))
        return httpx.Response(
            201,
            content=b'{"ok":true}',
            headers={"content-type": "application/json"},
            request=httpx.Request(kwargs["method"], f"{url}?page=2"),
        )

    async def render(self, url: str, wait_ms: int = 1500, wait_for: str | None = None) -> str:
        self.calls.append((url, {"wait_ms": wait_ms, "wait_for": wait_for}))
        return "<html>rendered</html>"

    async def request_json_in_browser(
        self,
        page_url: str,
        endpoint: str,
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        body: Any = None,
    ) -> Any:
        self.calls.append(
            (
                endpoint,
                {
                    "page_url": page_url,
                    "method": method,
                    "headers": headers,
                    "body": body,
                },
            )
        )
        return {"data": {"products": [1]}}

    async def evaluate_in_browser(
        self,
        url: str,
        script: str,
        wait_ms: int = 2000,
        wait_for: str | None = None,
        *,
        action_id: str = "legacy-evaluate.v1",
    ) -> Any:
        self.calls.append(
            (
                url,
                {
                    "script": script,
                    "wait_ms": wait_ms,
                    "wait_for": wait_for,
                    "action_id": action_id,
                },
            )
        )
        return [{"pack": "5", "price": "26.65"}]

    async def rotate_client(self) -> None:
        self.rotations += 1

    async def may_fetch(
        self, url: str, ignore_robots: bool = False, obey_robots: bool = False
    ) -> bool:
        del url, ignore_robots, obey_robots
        return True


class EvaluationSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int, str | None]] = []

    async def evaluate(
        self, url: str, script: str, wait_ms: int = 2000,
        wait_for: str | None = None,
    ) -> Any:
        self.calls.append((url, script, wait_ms, wait_for))
        return [{"pack": "5", "price": "26.65"}]


def request(**changes: Any) -> TransportRequest:
    values: dict[str, Any] = {
        "url": "https://shop.test/products",
        "purpose": RequestPurpose.ENTITY,
        "priority": RequestPriority.IDENTITY,
    }
    values.update(changes)
    return TransportRequest(**values)


@pytest.mark.asyncio
async def test_maps_http_request_response_and_safe_trace_fields() -> None:
    fetcher = Fetcher()
    telemetry = RecordingTelemetry()
    transport = LegacyFetcherTransport(fetcher, telemetry=telemetry)

    response = await transport.request(
        request(
            method="POST",
            query={"page": 2},
            headers={"authorization": "secret", "accept": "application/json"},
            json_body={"query": "products"},
        )
    )

    assert fetcher.calls == [
        (
            "https://shop.test/products",
            {
                "params": {"page": 2},
                "method": "POST",
                "json_body": {"query": "products"},
                "headers": {"authorization": "secret", "accept": "application/json"},
            },
        )
    ]
    assert response.status == 201
    assert response.content == b'{"ok":true}'
    assert response.route.kind == "direct"
    assert response.final_url == "https://shop.test/products?page=2"
    assert [event for event, _ in telemetry.events] == [
        "catalogue.legacy_fetcher.request.started",
        "catalogue.legacy_fetcher.request.completed",
    ]
    assert all("headers" not in fields and "json_body" not in fields for _, fields in telemetry.events)


@pytest.mark.asyncio
async def test_required_browser_render_includes_query_and_marks_browser_route() -> None:
    fetcher = Fetcher()
    response = await LegacyFetcherTransport(fetcher).request(
        request(browser=BrowserHint.REQUIRED, query={"variant": 4})
    )

    assert fetcher.calls == [
        ("https://shop.test/products?variant=4", {"wait_ms": 1500, "wait_for": None})
    ]
    assert response.text() == "<html>rendered</html>"
    assert response.route.kind == "browser"


@pytest.mark.asyncio
async def test_browser_json_request_preserves_context_method_headers_and_body() -> None:
    fetcher = Fetcher()
    response = await LegacyFetcherTransport(fetcher).request(
        request(
            method="POST",
            browser=BrowserHint.REQUIRED,
            headers={
                "referer": "https://shop.test/catalogue",
                "authorization": "Bearer public-token",
            },
            json_body={"query": "products"},
        )
    )

    assert fetcher.calls == [
        (
            "https://shop.test/products",
            {
                "page_url": "https://shop.test/catalogue",
                "method": "POST",
                "headers": {
                    "referer": "https://shop.test/catalogue",
                    "authorization": "Bearer public-token",
                },
                "body": {"query": "products"},
            },
        )
    ]
    assert response.status == 200
    assert response.headers == {"content-type": "application/json"}
    assert response.json_value() == {"data": {"products": [1]}}
    assert response.route.kind == "browser"


@pytest.mark.asyncio
async def test_browser_evaluation_preserves_typed_action_without_trace_leak() -> None:
    fetcher = Fetcher()
    telemetry = RecordingTelemetry()
    script = "() => 'trusted private implementation'"
    response = await LegacyFetcherTransport(
        fetcher,
        telemetry=telemetry,
        telemetry_context={
            "collection_id": "local-1",
            "source_id": "shop",
            "connector": "ceramicolours",
        },
    ).request(
        request(
            purpose=RequestPurpose.ENRICHMENT,
            priority=RequestPriority.OPTIONAL,
            required=False,
            browser=BrowserHint.REQUIRED,
            evaluation=BrowserEvaluation(
                action_id="ceramicolours.pack-prices.v1",
                script=script,
                wait_for="#product-pack-field",
                wait_milliseconds=1500,
            ),
        )
    )

    assert fetcher.calls == [
        (
            "https://shop.test/products",
            {
                "script": script,
                "wait_ms": 1500,
                "wait_for": "#product-pack-field",
                "action_id": "ceramicolours.pack-prices.v1",
            },
        )
    ]
    assert response.headers == {"content-type": "application/json"}
    assert response.json_value() == [{"pack": "5", "price": "26.65"}]
    assert response.route.kind == "browser"
    assert all(
        fields["collection_id"] == "local-1"
        and fields["source_id"] == "shop"
        and fields["connector"] == "ceramicolours"
        and fields["priority"] == "optional"
        and fields["required"] is False
        and fields["browser"] == "required"
        and fields["estimated_bytes"] == 0
        and fields["browser_action"] == "ceramicolours.pack-prices.v1"
        for _, fields in telemetry.events
    )
    assert script not in repr(telemetry.events)


@pytest.mark.asyncio
async def test_fetcher_browser_evaluation_is_cacheable_and_replay_fails_closed(
    tmp_path,
) -> None:
    session = EvaluationSession()
    cache = ResponseCache(tmp_path, mode="auto")
    client = httpx.AsyncClient()
    fetcher = CatalogueFetcher(
        client,
        HostLimiter(0, 1),
        cast(BrowserSession, session),
        cache=cache,
        impersonate_policy="never",
    )
    url = "https://shop.test/product"
    script = "() => 'trusted private implementation'"

    first = await fetcher.evaluate_in_browser(
        url,
        script,
        wait_ms=1500,
        wait_for="#product-pack-field",
        action_id="ceramicolours.pack-prices.v1",
    )
    cache.mode = "replay"
    replayed = await fetcher.evaluate_in_browser(
        url,
        script,
        wait_ms=1500,
        wait_for="#product-pack-field",
        action_id="ceramicolours.pack-prices.v1",
    )

    assert first == replayed == [{"pack": "5", "price": "26.65"}]
    assert session.calls == [
        (url, script, 1500, "#product-pack-field")
    ]
    assert fetcher.stats.browser_requests == 1
    assert fetcher.stats.browser_rx_bytes_estimated > 0
    assert script not in repr(tuple(tmp_path.rglob("*")))
    with pytest.raises(NotCached, match="not in the cache"):
        await fetcher.evaluate_in_browser(
            url,
            "() => 'changed'",
            action_id="ceramicolours.pack-prices.v2",
        )
    await client.aclose()


@pytest.mark.asyncio
async def test_unsupported_opaque_body_fails_before_io() -> None:
    fetcher = Fetcher()
    transport = LegacyFetcherTransport(fetcher)

    with pytest.raises(ValueError, match="opaque byte body"):
        await transport.request(request(body=b"raw"))

    assert fetcher.calls == []


@pytest.mark.asyncio
async def test_rotation_delegates_with_reason_trace() -> None:
    fetcher = Fetcher()
    telemetry = RecordingTelemetry()
    transport = LegacyFetcherTransport(fetcher, telemetry=telemetry)

    await transport.rotate_identity(RotationReason.RATE_LIMITED)

    assert fetcher.rotations == 1
    assert telemetry.events == [
        ("catalogue.legacy_fetcher.identity.rotate", {"reason": "rate_limited"})
    ]


@pytest.mark.asyncio
async def test_returns_http_status_but_types_exhausted_network_failure() -> None:
    fetcher = Fetcher()
    transport = LegacyFetcherTransport(fetcher)
    status_response = httpx.Response(
        404,
        content=b"missing",
        request=httpx.Request("GET", "https://shop.test/missing"),
    )

    async def status(*args: Any, **kwargs: Any) -> httpx.Response:
        del args, kwargs
        raise httpx.HTTPStatusError(
            "missing", request=status_response.request, response=status_response
        )

    fetcher.response = status  # type: ignore[method-assign]
    response = await transport.request(request())
    assert response.status == 404

    async def network(*args: Any, **kwargs: Any) -> httpx.Response:
        del args, kwargs
        raise httpx.ConnectError("offline")

    fetcher.response = network  # type: ignore[method-assign]
    with pytest.raises(TransportFailure) as failure:
        await transport.request(request())
    assert str(failure.value) == "ConnectError: offline"
    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [Blocked("cache miss"), ProxyDenied("budget exhausted")])
async def test_types_application_fetch_failures_at_library_boundary(error: Exception) -> None:
    fetcher = Fetcher()

    async def fail(*args: Any, **kwargs: Any) -> httpx.Response:
        del args, kwargs
        raise error

    fetcher.response = fail  # type: ignore[method-assign]

    with pytest.raises(TransportFailure) as failure:
        await LegacyFetcherTransport(fetcher).request(request())

    assert str(failure.value) == f"{type(error).__name__}: {error}"
    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None


@pytest.mark.asyncio
async def test_bounds_http_and_browser_response_retention() -> None:
    fetcher = Fetcher()

    with pytest.raises(ResponseBodyTooLarge, match="4-byte retention limit"):
        await LegacyFetcherTransport(
            fetcher,
            maximum_response_bytes=4,
        ).request(request())

    with pytest.raises(ResponseBodyTooLarge, match="4-byte retention limit"):
        await LegacyFetcherTransport(
            fetcher,
            maximum_response_bytes=4,
        ).request(request(browser=BrowserHint.REQUIRED))


@pytest.mark.asyncio
async def test_redacts_and_detaches_raw_fetcher_error_content() -> None:
    secret = "raw-secret-sentinel"
    fetcher = Fetcher()

    async def fail(*args: Any, **kwargs: Any) -> httpx.Response:
        del args, kwargs
        raise Blocked(
            f"response Authorization: Bearer {secret} "
            + "x" * 4_000
        )

    fetcher.response = fail  # type: ignore[method-assign]

    with pytest.raises(TransportFailure) as caught:
        await LegacyFetcherTransport(fetcher).request(request())

    assert secret not in str(caught.value)
    assert len(str(caught.value)) <= 2_048
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
