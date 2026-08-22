import asyncio

import httpx
import pytest
from pydantic import JsonValue

from mb_commerce_scraper.models import BrowserPolicy
from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    BrowserBackendUnavailable,
    BrowserDispatchTransport,
    BrowserEvaluation,
    BrowserHint,
    MemoryRequestBudget,
    MemoryResponseCache,
    MiddlewareTransport,
    ProxyBrowserRoutingUnsupported,
    RequestPriority,
    RequestPurpose,
    ResponseBodyTooLarge,
    ResponseDecodeFailure,
    RotationReason,
    TransportFailure,
    TransportRequest,
    TransportResponse,
    estimated_transmitted_bytes,
    safe_telemetry,
    sanitize_fields,
    sanitize_url,
)
from mb_commerce_scraper.transports.httpx import HttpxTransport
from mb_commerce_scraper.transports.middleware import BudgetExhausted, RobotsDenied
from mb_commerce_scraper.transports.url_policy import URLPolicy


class RecordingTelemetry:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, JsonValue]]] = []

    def emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        self.events.append((event, fields))


class BrokenTelemetry:
    def emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        del event, fields
        raise RuntimeError("observer unavailable")


class FailingReleaseLimiter:
    async def wait(self, request: TransportRequest) -> None:
        del request

    async def release(self, request: TransportRequest) -> None:
        del request
        raise RuntimeError("limiter release failed")


class FailingReconcileAuthorization:
    async def reconcile(self, response_bytes: int) -> None:
        del response_bytes
        raise RuntimeError("budget reconcile failed")

    async def release(self) -> None:
        return None


class FailingReconcileBudget:
    async def authorize(
        self,
        request: TransportRequest,
    ) -> FailingReconcileAuthorization:
        del request
        return FailingReconcileAuthorization()


class FailingPutCache:
    async def get(self, request: TransportRequest) -> None:
        del request
        return None

    async def put(
        self,
        request: TransportRequest,
        response: TransportResponse,
    ) -> None:
        del request, response
        raise RuntimeError("cache write failed")


class StaleCache:
    def __init__(self, stale: TransportResponse) -> None:
        self.response = stale
        self.writes: list[tuple[TransportRequest, TransportResponse]] = []

    async def get(self, request: TransportRequest) -> TransportResponse | None:
        del request
        return None

    async def stale(self, request: TransportRequest) -> TransportResponse | None:
        del request
        return self.response

    async def put(self, request: TransportRequest, response: TransportResponse) -> None:
        self.writes.append((request, response))


class BlockingTransport(FakeTransport):
    def __init__(self, response_body: bytes = b"ok") -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.response_body = response_body

    async def request(self, request: TransportRequest) -> TransportResponse:
        self.requests.append(request)
        self.entered.set()
        await self.release.wait()
        return TransportResponse(
            status=200,
            content=self.response_body,
            final_url=request.url,
        )


class CancelOnceLimiter:
    def __init__(self) -> None:
        self.calls = 0
        self.entered = asyncio.Event()

    async def wait(self, request: TransportRequest) -> None:
        del request
        self.calls += 1
        if self.calls == 1:
            self.entered.set()
            await asyncio.Event().wait()

    async def release(self, request: TransportRequest) -> None:
        del request


async def test_retries_are_charged_per_attempt() -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/data", status=503)
    backend.add("https://shop.test/data", body="ok")
    budget = MemoryRequestBudget(maximum_requests=2)
    transport = MiddlewareTransport(backend, budget=budget, retries=1, backoff=lambda _: 0)
    response = await transport.request(
        TransportRequest(
            url="https://shop.test/data", purpose=RequestPurpose.ENTITY, priority=RequestPriority.IDENTITY
        )
    )
    assert response.text() == "ok"
    assert budget.requests == 2


async def test_budget_prevents_next_attempt() -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/data", status=503)
    transport = MiddlewareTransport(
        backend, budget=MemoryRequestBudget(maximum_requests=1), retries=1, backoff=lambda _: 0
    )
    with pytest.raises(BudgetExhausted):
        await transport.request(
            TransportRequest(
                url="https://shop.test/data", purpose=RequestPurpose.ENTITY, priority=RequestPriority.IDENTITY
            )
        )


async def test_budget_denial_telemetry_explains_browser_evaluation_policy() -> None:
    telemetry = RecordingTelemetry()
    transport = MiddlewareTransport(
        FakeTransport(),
        budget=MemoryRequestBudget(maximum_requests=0),
        telemetry=telemetry,
        retries=0,
    )
    request = TransportRequest(
        url="https://shop.test/product",
        purpose=RequestPurpose.ENRICHMENT,
        priority=RequestPriority.OPTIONAL,
        required=False,
        estimated_bytes=12_345,
        browser=BrowserHint.REQUIRED,
        evaluation=BrowserEvaluation(
            action_id="shop.offer.v1",
            script="() => []",
        ),
    )

    with pytest.raises(BudgetExhausted):
        await transport.request(request)

    assert [event for event, _ in telemetry.events] == [
        "request.accepted",
        "budget.denied",
    ]
    accepted = telemetry.events[0][1]
    fields = telemetry.events[1][1]
    assert accepted["request_id"] == fields["request_id"]
    assert fields["purpose"] == "enrichment"
    assert fields["priority"] == "optional"
    assert fields["required"] is False
    assert fields["browser"] == "required"
    assert fields["estimated_bytes"] == 12_345
    assert fields["browser_action"] == "shop.offer.v1"
    assert "script" not in repr(fields)


async def test_budget_authorization_is_atomic_across_concurrent_middleware_calls() -> None:
    backend = BlockingTransport()
    budget = MemoryRequestBudget(maximum_requests=1)
    transport = MiddlewareTransport(backend, budget=budget, retries=0)
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
    )

    first = asyncio.create_task(transport.request(request))
    await backend.entered.wait()
    with pytest.raises(BudgetExhausted):
        await transport.request(request)
    backend.release.set()

    assert (await first).text() == "ok"
    assert budget.requests == 1
    assert len(backend.requests) == 1


async def test_budget_reconciles_reserved_estimate_with_actual_response_bytes() -> None:
    backend = BlockingTransport(b"abc")
    budget = MemoryRequestBudget(maximum_bytes=10)
    transport = MiddlewareTransport(backend, budget=budget, retries=0)
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        estimated_bytes=6,
    )

    first = asyncio.create_task(transport.request(request))
    await backend.entered.wait()
    assert not budget.affordable(request)
    with pytest.raises(BudgetExhausted):
        await transport.request(request)
    backend.release.set()
    await first

    assert budget.bytes == 3
    assert budget.affordable(request)


async def test_connector_affordability_preview_does_not_reserve_budget() -> None:
    budget = MemoryRequestBudget(maximum_requests=1, maximum_bytes=10)
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        estimated_bytes=6,
    )

    assert budget.affordable(request)
    assert budget.affordable(request)
    assert budget.requests == 0
    assert budget.bytes == 0


async def test_cancelled_rate_limit_wait_releases_undispatched_authorization() -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/data", body="ok")
    limiter = CancelOnceLimiter()
    budget = MemoryRequestBudget(maximum_requests=1, maximum_bytes=6)
    transport = MiddlewareTransport(
        backend,
        budget=budget,
        rate_limiter=limiter,
        retries=0,
    )
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        estimated_bytes=6,
    )

    cancelled = asyncio.create_task(transport.request(request))
    await limiter.entered.wait()
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled

    assert budget.requests == 0
    assert budget.bytes == 0
    assert (await transport.request(request)).text() == "ok"
    assert budget.requests == 1


async def test_cancelled_backend_attempt_reconciles_before_next_authorization() -> None:
    backend = BlockingTransport()
    budget = MemoryRequestBudget(maximum_requests=2, maximum_bytes=6)
    telemetry = RecordingTelemetry()
    transport = MiddlewareTransport(backend, budget=budget, telemetry=telemetry, retries=0)
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        estimated_bytes=6,
    )

    cancelled = asyncio.create_task(transport.request(request))
    await backend.entered.wait()
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled

    assert budget.requests == 1
    assert budget.bytes == 0
    assert [event for event, _ in telemetry.events] == [
        "request.accepted",
        "request.started",
        "request.failed",
    ]
    cancellation = telemetry.events[-1][1]
    assert cancellation["error_type"] == "CancelledError"
    assert cancellation["cancelled"] is True
    assert cancellation["retryable"] is False
    assert cancellation["physical_requests"] == 1
    backend.release.set()
    assert (await transport.request(request)).text() == "ok"
    assert budget.requests == 2
    assert budget.bytes == 2


async def test_unexpected_backend_failure_counts_as_an_attempt() -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/data", error=RuntimeError("backend failed"))
    budget = MemoryRequestBudget(maximum_requests=1, maximum_bytes=6)
    transport = MiddlewareTransport(backend, budget=budget, retries=0)
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        estimated_bytes=6,
    )

    with pytest.raises(RuntimeError, match="backend failed"):
        await transport.request(request)

    assert budget.requests == 1
    assert budget.bytes == 0
    assert not budget.affordable(request)


async def test_typed_transport_failure_is_charged_rotated_and_retried() -> None:
    backend = FakeTransport()
    backend.add(
        "https://shop.test/data",
        error=TransportFailure("TLS failed for a secret-bearing backend URL"),
    )
    backend.add("https://shop.test/data", body="ok")
    budget = MemoryRequestBudget(maximum_requests=2)
    transport = MiddlewareTransport(
        backend,
        budget=budget,
        retries=1,
        backoff=lambda _: 0,
    )

    response = await transport.request(
        TransportRequest(
            url="https://shop.test/data",
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
        )
    )

    assert response.text() == "ok"
    assert budget.requests == 2
    assert backend.rotations == [RotationReason.TRANSPORT_FAILURE]


async def test_url_policy_rejects_private_and_cross_origin_destinations() -> None:
    policy = URLPolicy(("https://shop.test",), resolver=lambda _: _addresses("127.0.0.1"))
    with pytest.raises(ValueError, match="non-public"):
        await policy.validate("https://shop.test/products")
    public = URLPolicy(("https://shop.test",), resolver=lambda _: _addresses("93.184.216.34"))
    with pytest.raises(ValueError, match="not allowed"):
        await public.validate("https://other.test/products")


async def test_http_transport_connects_to_the_validated_address_with_logical_host() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"ok", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    policy = URLPolicy(
        ("https://shop.test",),
        resolver=lambda _: _addresses("93.184.216.34"),
    )
    transport = HttpxTransport(allowed_origins=("https://shop.test",), client=client, url_policy=policy)

    response = await transport.request(
        TransportRequest(
            url="https://shop.test/products",
            query={"page": 2},
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
        )
    )

    assert response.final_url == "https://shop.test/products?page=2"
    assert str(requests[0].url) == "https://93.184.216.34/products?page=2"
    assert requests[0].headers["host"] == "shop.test"
    assert requests[0].extensions["sni_hostname"] == "shop.test"
    assert response.accounting is not None
    assert response.accounting.physical_requests == 1
    assert response.accounting.transmitted_bytes == estimated_transmitted_bytes(
        TransportRequest(
            url="https://shop.test/products",
            query={"page": 2},
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
        )
    )
    assert response.accounting.received_bytes == 2
    await client.aclose()


def test_transmitted_byte_estimate_is_deterministic_and_secret_free() -> None:
    request = TransportRequest(
        method="POST",
        url="https://shop.test/data?existing=yes",
        query={"page": 2, "enabled": True},
        headers={"X-Token": "very-secret-value"},
        json_body={"name": "glaze"},
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
    )
    expected = (
        b"POST /data?existing=yes&page=2&enabled=true HTTP/1.1\r\n"
        b"X-Token: very-secret-value\r\n"
        b"Host: shop.test\r\n"
        b"Content-Length: 16\r\n"
        b"Content-Type: application/json\r\n"
        b"\r\n"
        b'{"name":"glaze"}'
    )

    measured = estimated_transmitted_bytes(request)

    assert measured == len(expected)
    assert isinstance(measured, int)


async def test_http_transport_failure_carries_numeric_request_accounting() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("token=very-secret-value", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    request = TransportRequest(
        url="https://shop.test/data",
        headers={"Authorization": "Bearer very-secret-value"},
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
    )
    transport = HttpxTransport(
        allowed_origins=("https://shop.test",),
        client=client,
        url_policy=URLPolicy(
            ("https://shop.test",),
            resolver=lambda _: _addresses("93.184.216.34"),
        ),
    )

    with pytest.raises(TransportFailure) as caught:
        await transport.request(request)

    accounting = caught.value.accounting
    assert accounting is not None
    assert accounting.physical_requests == 1
    assert accounting.transmitted_bytes == estimated_transmitted_bytes(request)
    assert "very-secret-value" not in repr(accounting.model_dump())
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert "very-secret-value" not in repr(caught.value)
    await client.aclose()


async def test_http_redirects_are_counted_as_distinct_physical_requests() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(
                302,
                headers={"location": "https://shop.test/final"},
                request=request,
            )
        return httpx.Response(200, content=b"ok", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpxTransport(
        allowed_origins=("https://shop.test",),
        client=client,
        url_policy=URLPolicy(
            ("https://shop.test",),
            resolver=lambda _: _addresses("93.184.216.34"),
        ),
    )
    original = TransportRequest(
        url="https://shop.test/start",
        query={"page": 2},
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
    )

    response = await transport.request(original)

    assert len(requests) == 2
    assert response.accounting is not None
    assert response.accounting.physical_requests == 2
    assert response.accounting.transmitted_bytes == (
        estimated_transmitted_bytes(original)
        + estimated_transmitted_bytes(
            original.model_copy(
                update={
                    "url": "https://shop.test/final",
                    "query": {},
                    "headers": {"Host": "shop.test"},
                }
            )
        )
    )
    assert response.accounting.received_bytes == 2
    await client.aclose()


async def test_http_transport_streams_and_rejects_an_oversized_body() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"123456", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    policy = URLPolicy(
        ("https://shop.test",),
        resolver=lambda _: _addresses("93.184.216.34"),
    )
    transport = HttpxTransport(
        allowed_origins=("https://shop.test",),
        client=client,
        url_policy=policy,
        maximum_response_bytes=5,
    )

    with pytest.raises(ResponseBodyTooLarge) as raised:
        await transport.request(
            TransportRequest(
                url="https://shop.test/products",
                purpose=RequestPurpose.ENTITY,
                priority=RequestPriority.IDENTITY,
            )
        )

    assert raised.value.maximum_bytes == 5
    assert raised.value.received_bytes == 6
    assert "123456" not in str(raised.value)
    await client.aclose()


async def test_redirect_is_validated_before_second_connection() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            302,
            headers={"location": "https://cdn.test/product"},
            request=request,
        )

    async def resolver(host: str) -> tuple[str, ...]:
        return ("127.0.0.1",) if host == "cdn.test" else ("93.184.216.34",)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpxTransport(
        allowed_origins=("https://shop.test", "https://cdn.test"),
        client=client,
        url_policy=URLPolicy(("https://shop.test", "https://cdn.test"), resolver=resolver),
    )

    with pytest.raises(ValueError, match="non-public"):
        await transport.request(
            TransportRequest(
                url="https://shop.test/start",
                purpose=RequestPurpose.ENTITY,
                priority=RequestPriority.IDENTITY,
            )
        )

    assert len(requests) == 1
    await client.aclose()


async def test_cross_origin_redirect_strips_sensitive_headers() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(
                302,
                headers={"location": "https://cdn.test/product?variant=2"},
                request=request,
            )
        return httpx.Response(200, content=b"ok", request=request)

    addresses = {
        "shop.test": "93.184.216.34",
        "cdn.test": "93.184.216.35",
    }

    async def resolver(host: str) -> tuple[str, ...]:
        return (addresses[host],)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    origins = ("https://shop.test", "https://cdn.test")
    transport = HttpxTransport(
        allowed_origins=origins,
        client=client,
        url_policy=URLPolicy(origins, resolver=resolver),
    )
    response = await transport.request(
        TransportRequest(
            url="https://shop.test/start",
            query={"page": 1},
            headers={
                "Authorization": "Bearer secret",
                "Cookie": "session=secret",
                "X-Correlation-ID": "safe",
            },
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
        )
    )

    assert response.final_url == "https://cdn.test/product?variant=2"
    assert str(requests[0].url).endswith("/start?page=1")
    assert str(requests[1].url) == "https://93.184.216.35/product?variant=2"
    assert "authorization" not in requests[1].headers
    assert "cookie" not in requests[1].headers
    assert requests[1].headers["x-correlation-id"] == "safe"
    assert requests[1].headers["host"] == "cdn.test"
    await client.aclose()

    body_requests: list[httpx.Request] = []

    async def body_handler(request: httpx.Request) -> httpx.Response:
        body_requests.append(request)
        return httpx.Response(
            307,
            headers={"location": "https://cdn.test/receive"},
            request=request,
        )

    body_client = httpx.AsyncClient(transport=httpx.MockTransport(body_handler))
    body_transport = HttpxTransport(
        allowed_origins=origins,
        client=body_client,
        url_policy=URLPolicy(origins, resolver=resolver),
    )
    with pytest.raises(RuntimeError, match="request body is refused"):
        await body_transport.request(
            TransportRequest(
                method="POST",
                url="https://shop.test/start",
                json_body={"api_token": "private"},
                purpose=RequestPurpose.ENTITY,
                priority=RequestPriority.IDENTITY,
            )
        )
    assert len(body_requests) == 1
    await body_client.aclose()


async def _addresses(*values: str) -> tuple[str, ...]:
    return values


class Robots:
    def __init__(self, allowed: bool, events: list[str]) -> None:
        self.result = allowed
        self.events = events

    async def allowed(self, url: str) -> bool:
        del url
        self.events.append("robots")
        return self.result


class Limiter:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def wait(self, request: TransportRequest) -> None:
        del request
        self.events.append("rate")

    async def release(self, request: TransportRequest) -> None:
        del request


async def test_robots_precedes_cache_and_paid_attempt_layers() -> None:
    events: list[str] = []
    telemetry = RecordingTelemetry()
    backend = FakeTransport()
    cache = MemoryResponseCache()
    cached_request = TransportRequest(
        url="https://shop.test/data", purpose=RequestPurpose.ENTITY, priority=RequestPriority.IDENTITY
    )
    await cache.put(cached_request, _response("cached"))
    transport = MiddlewareTransport(
        backend,
        robots=Robots(False, events),
        cache=cache,
        rate_limiter=Limiter(events),
        telemetry=telemetry,
    )
    with pytest.raises(RobotsDenied):
        await transport.request(cached_request)
    assert events == ["robots"]
    assert backend.requests == []
    assert [event for event, _ in telemetry.events] == [
        "request.accepted",
        "robots.denied",
    ]
    assert telemetry.events[0][1]["request_id"] == telemetry.events[1][1]["request_id"]


async def test_cache_hit_skips_budget_rate_limit_and_network() -> None:
    events: list[str] = []
    telemetry = RecordingTelemetry()
    backend = FakeTransport()
    cache = MemoryResponseCache()
    cached_request = TransportRequest(
        url="https://shop.test/data", purpose=RequestPurpose.ENTITY, priority=RequestPriority.IDENTITY
    )
    await cache.put(cached_request, _response("cached"))
    budget = MemoryRequestBudget(maximum_requests=0)
    transport = MiddlewareTransport(
        backend,
        cache=cache,
        budget=budget,
        rate_limiter=Limiter(events),
        telemetry=telemetry,
    )
    response = await transport.request(cached_request)
    assert response.from_cache and response.text() == "cached"
    assert budget.requests == 0 and events == [] and backend.requests == []
    assert [event for event, _ in telemetry.events] == [
        "request.accepted",
        "cache.hit",
    ]
    assert telemetry.events[0][1]["request_id"] == telemetry.events[1][1]["request_id"]


async def test_stale_cache_validator_reuses_body_after_304() -> None:
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
    )
    cache = StaleCache(
        TransportResponse(
            status=200,
            headers={"content-type": "text/plain", "etag": '"v1"'},
            content=b"previous",
            final_url=request.url,
            from_cache=True,
        )
    )
    backend = FakeTransport()
    backend.add(request.url, status=304, headers={"etag": '"v2"'})
    telemetry = RecordingTelemetry()

    response = await MiddlewareTransport(
        backend,
        cache=cache,
        telemetry=telemetry,
        retries=0,
    ).request(request)

    assert backend.requests[0].headers["if-none-match"] == '"v1"'
    assert response.status == 200
    assert response.text() == "previous"
    assert response.from_cache
    assert response.headers["etag"] == '"v2"'
    assert len(cache.writes) == 1
    assert cache.writes[0][0] == request
    assert cache.writes[0][1].text() == "previous"
    assert [event for event, _ in telemetry.events] == [
        "request.accepted",
        "cache.miss",
        "request.started",
        "cache.revalidated",
        "request.completed",
    ]


@pytest.mark.parametrize("failure", [503, TransportFailure("network unavailable")])
async def test_explicit_stale_on_error_only_masks_transient_failures(
    failure: int | TransportFailure,
) -> None:
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
    )
    cache = StaleCache(
        TransportResponse(
            status=200,
            content=b"previous",
            final_url=request.url,
            from_cache=True,
        )
    )
    backend = FakeTransport()
    if isinstance(failure, int):
        backend.add(request.url, status=failure, body="temporary")
    else:
        backend.add(request.url, error=failure)
    telemetry = RecordingTelemetry()

    response = await MiddlewareTransport(
        backend,
        cache=cache,
        stale_on_error=True,
        telemetry=telemetry,
        retries=0,
    ).request(request)

    assert response.status == 200
    assert response.text() == "previous"
    assert response.route.kind == "cache"
    assert response.from_cache
    assert [fields["reason"] for event, fields in telemetry.events if event == "cache.stale_used"] == [
        "transient_status" if isinstance(failure, int) else "transport_failure"
    ]


async def test_stale_on_error_does_not_mask_deterministic_404() -> None:
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
    )
    cache = StaleCache(TransportResponse(status=200, content=b"previous", final_url=request.url))
    backend = FakeTransport()
    backend.add(request.url, status=404, body="gone")

    response = await MiddlewareTransport(backend, cache=cache, stale_on_error=True, retries=0).request(
        request
    )

    assert response.status == 404
    assert response.text() == "gone"
    assert not response.from_cache


async def test_stale_on_error_does_not_mask_browser_failure() -> None:
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        browser=BrowserHint.REQUIRED,
    )
    cache = StaleCache(TransportResponse(status=200, content=b"previous", final_url=request.url))
    backend = FakeTransport()
    backend.add(request.url, error=TransportFailure("browser unavailable"))

    with pytest.raises(TransportFailure, match="browser unavailable"):
        await MiddlewareTransport(backend, cache=cache, stale_on_error=True, retries=0).request(request)


async def test_oversized_browser_response_is_non_retryable_and_accounted() -> None:
    direct = FakeTransport()
    browser = FakeTransport()
    browser.add("https://shop.test/data", body="123456")
    bounded = BrowserDispatchTransport(
        direct,
        browser,
        maximum_response_bytes=5,
    )
    budget = MemoryRequestBudget(maximum_requests=3, maximum_bytes=100)
    transport = MiddlewareTransport(
        bounded,
        budget=budget,
        retries=2,
        backoff=lambda _: 0,
        maximum_response_bytes=5,
    )
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        browser=BrowserHint.REQUIRED,
    )

    with pytest.raises(ResponseBodyTooLarge):
        await transport.request(request)

    assert len(browser.requests) == 1
    assert direct.requests == []
    assert budget.requests == 1
    assert budget.bytes == 6
    assert browser.rotations == []


async def test_middleware_rejects_an_oversized_generic_backend_response() -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/data", body="123456")
    budget = MemoryRequestBudget(maximum_requests=2, maximum_bytes=100)
    transport = MiddlewareTransport(
        backend,
        budget=budget,
        retries=1,
        backoff=lambda _: 0,
        maximum_response_bytes=5,
    )
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
    )

    with pytest.raises(ResponseBodyTooLarge) as raised:
        await transport.request(request)

    assert raised.value.received_bytes == 6
    assert len(backend.requests) == 1
    assert backend.rotations == []
    assert budget.requests == 1
    assert budget.bytes == 6


def test_json_decode_failure_does_not_retain_response_body() -> None:
    secret = "secret-that-must-not-survive-in-parser-context"
    response = _response(f'{{"value":"{secret}"')

    with pytest.raises(ResponseDecodeFailure) as raised:
        response.json_value()

    assert raised.value.line == 1
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert secret not in repr(raised.value)
    assert not hasattr(raised.value, "doc")


async def test_oversized_cache_entry_is_rejected_before_paid_layers() -> None:
    events: list[str] = []
    backend = FakeTransport()
    cache = MemoryResponseCache(maximum_response_bytes=10)
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
    )
    await cache.put(request, _response("123456"))
    budget = MemoryRequestBudget(maximum_requests=0)
    transport = MiddlewareTransport(
        backend,
        cache=cache,
        budget=budget,
        rate_limiter=Limiter(events),
        maximum_response_bytes=5,
    )

    with pytest.raises(ResponseBodyTooLarge):
        await transport.request(request)

    assert budget.requests == 0
    assert events == []
    assert backend.requests == []


async def test_memory_cache_refuses_to_retain_oversized_response() -> None:
    cache = MemoryResponseCache(maximum_response_bytes=5)
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
    )

    with pytest.raises(ResponseBodyTooLarge):
        await cache.put(request, _response("123456"))
    assert await cache.get(request) is None


async def test_every_retry_is_independently_rate_limited_and_rotates_blocks() -> None:
    events: list[str] = []
    backend = FakeTransport()
    backend.add("https://shop.test/data", status=403)
    backend.add("https://shop.test/data", body="ok")
    transport = MiddlewareTransport(backend, retries=1, backoff=lambda _: 0, rate_limiter=Limiter(events))
    response = await transport.request(
        TransportRequest(
            url="https://shop.test/data", purpose=RequestPurpose.ENTITY, priority=RequestPriority.IDENTITY
        )
    )
    assert response.text() == "ok"
    assert events == ["rate", "rate"]
    assert backend.rotations


def test_telemetry_sanitizes_urls_credentials_and_bodies() -> None:
    assert (
        sanitize_url("https://user:password@shop.test/products?token=secret&page=2#private")
        == "https://redacted@shop.test/products?token=%5Bredacted%5D&page=%5Bredacted%5D"
    )
    assert sanitize_fields(
        {
            "url": "https://shop.test/products?api_key=secret&locale=en",
            "headers": {"Authorization": "Bearer secret"},
            "body": "raw response",
            "context": {"password": "secret", "page": 2},
        }
    ) == {
        "url": "https://shop.test/products?api_key=%5Bredacted%5D&locale=%5Bredacted%5D",
        "headers": "[omitted]",
        "body": "[omitted]",
        "context": {"password": "[redacted]", "page": 2},
    }


def test_telemetry_replaces_invalid_event_name_without_echoing_it() -> None:
    telemetry = RecordingTelemetry()

    safe_telemetry(telemetry).emit(
        "connector.password=secret",
        {"level": "debug"},
    )

    assert telemetry.events == [("telemetry.invalid_event", {"level": "debug"})]


async def test_retry_telemetry_correlates_attempts_without_leaking_credentials() -> None:
    backend = FakeTransport()
    secret_url = "https://user:password@shop.test/data?access_token=secret&page=2"
    backend.add(secret_url, status=503)
    backend.add(secret_url, body="ok")
    telemetry = RecordingTelemetry()
    transport = MiddlewareTransport(backend, telemetry=telemetry, retries=1, backoff=lambda _: 0)

    response = await transport.request(
        TransportRequest(
            url=secret_url,
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
            headers={"Authorization": "Bearer secret"},
            body=b"private request body",
        )
    )

    assert response.text() == "ok"
    assert [event for event, _ in telemetry.events] == [
        "request.accepted",
        "request.started",
        "request.retry",
        "request.started",
        "request.completed",
    ]
    fields = [event_fields for _, event_fields in telemetry.events]
    assert {event_fields["request_id"] for event_fields in fields} == {fields[0]["request_id"]}
    assert [fields[1]["attempt"], fields[2]["next_attempt"], fields[3]["attempt"]] == [
        1,
        2,
        2,
    ]
    assert fields[2]["status"] == 503
    assert fields[2]["backoff_ms"] == 0
    expected_transmitted = estimated_transmitted_bytes(backend.requests[0])
    assert fields[2]["transmitted_bytes"] == expected_transmitted
    assert fields[2]["received_bytes"] == 0
    assert fields[2]["physical_requests"] == 1
    assert fields[-1]["route"] == "direct"
    assert fields[-1]["purpose"] == "entity"
    assert fields[-1]["transmitted_bytes"] == expected_transmitted
    assert fields[-1]["received_bytes"] == 2
    serialized = repr(telemetry.events)
    assert "password" not in serialized
    assert "Bearer secret" not in serialized
    assert "private request body" not in serialized
    assert "access_token=secret" not in serialized


async def test_telemetry_failure_does_not_fail_collection_request() -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/data", body="ok")
    transport = MiddlewareTransport(backend, telemetry=BrokenTelemetry(), retries=0)

    response = await transport.request(
        TransportRequest(
            url="https://shop.test/data",
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
        )
    )

    assert response.text() == "ok"


async def test_disabled_telemetry_avoids_trace_allocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/data", body="ok")
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
    )
    monkeypatch.setattr(
        "mb_commerce_scraper.transports.middleware.uuid4",
        lambda: (_ for _ in ()).throw(AssertionError("trace allocation")),
    )

    response = await MiddlewareTransport(backend).request(request)

    assert response.text() == "ok"
    assert backend.requests == [request]
    assert backend.requests[0].trace_request_id is None
    assert backend.requests[0].trace_attempt is None


@pytest.mark.parametrize(
    ("middleware_options", "failure_stage"),
    [
        ({"rate_limiter": FailingReleaseLimiter()}, "rate_limit_release"),
        ({"budget": FailingReconcileBudget()}, "budget_reconcile"),
        ({"cache": FailingPutCache()}, "cache_write"),
    ],
)
async def test_post_dispatch_failures_emit_terminal_request_event(
    middleware_options: dict[str, object],
    failure_stage: str,
) -> None:
    backend = FakeTransport()
    backend.add("https://shop.test/data", body="ok")
    telemetry = RecordingTelemetry()
    transport = MiddlewareTransport(
        backend,
        telemetry=telemetry,
        **middleware_options,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="failed"):
        await transport.request(
            TransportRequest(
                url="https://shop.test/data",
                purpose=RequestPurpose.ENTITY,
                priority=RequestPriority.IDENTITY,
            )
        )

    terminal = [fields for event, fields in telemetry.events if event == "request.failed"]
    assert len(terminal) == 1
    assert terminal[0]["failure_stage"] == failure_stage
    assert terminal[0]["attempt"] == 1
    assert terminal[0]["retryable"] is False


def test_cache_key_ignores_credentials_but_distinguishes_rendering() -> None:
    ordinary = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        headers={"Authorization": "secret-one"},
    )
    changed_secret = ordinary.model_copy(update={"headers": {"Authorization": "secret-two"}})
    rendered = ordinary.model_copy(update={"browser": BrowserHint.REQUIRED})
    assert MemoryResponseCache.key(ordinary) == MemoryResponseCache.key(changed_secret)
    assert MemoryResponseCache.key(ordinary) != MemoryResponseCache.key(rendered)


def test_browser_evaluation_is_bounded_and_explicit() -> None:
    evaluation = BrowserEvaluation(
        action_id="ceramicolours.pack-prices.v1",
        script="() => []",
        wait_for="#product-pack-field",
    )
    request = TransportRequest(
        url="https://shop.test/product",
        purpose=RequestPurpose.ENRICHMENT,
        priority=RequestPriority.OPTIONAL,
        required=False,
        browser=BrowserHint.REQUIRED,
        evaluation=evaluation,
    )

    assert request.evaluation == evaluation
    assert estimated_transmitted_bytes(request) == estimated_transmitted_bytes(
        request.model_copy(update={"evaluation": None})
    )
    for update in (
        {"browser": BrowserHint.NEVER},
        {"method": "POST"},
        {"headers": {"x-script": "forbidden"}},
        {"purpose": RequestPurpose.DISCOVERY},
    ):
        with pytest.raises(ValueError, match="browser evaluation"):
            TransportRequest(**(request.model_dump() | update))


def test_cache_key_hashes_browser_evaluation_identity_without_exposing_script() -> None:
    script = "() => 'private implementation detail'"
    ordinary = TransportRequest(
        url="https://shop.test/product",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.OPTIONAL,
        browser=BrowserHint.REQUIRED,
    )
    evaluated = ordinary.model_copy(
        update={
            "evaluation": BrowserEvaluation(
                action_id="shop.offer.v1",
                script=script,
            )
        }
    )
    assert evaluated.evaluation is not None
    changed = evaluated.model_copy(
        update={"evaluation": evaluated.evaluation.model_copy(update={"script": "() => 'changed'"})}
    )

    ordinary_key = MemoryResponseCache.key(ordinary)
    evaluated_key = MemoryResponseCache.key(evaluated)
    changed_key = MemoryResponseCache.key(changed)
    assert len({ordinary_key, evaluated_key, changed_key}) == 3
    assert script not in evaluated_key
    assert script not in repr(evaluated)


async def test_http_transport_rejects_browser_required_requests() -> None:
    transport = HttpxTransport(allowed_origins=("https://shop.test",))
    request = TransportRequest(
        url="https://shop.test/data",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        browser=BrowserHint.REQUIRED,
    )
    with pytest.raises(BrowserBackendUnavailable, match="browser transport"):
        await transport.request(request)
    await transport.aclose()


async def test_browser_dispatch_routes_only_required_requests() -> None:
    http = FakeTransport()
    browser = FakeTransport()
    http.add("https://shop.test/plain", body="http")
    browser.add("https://shop.test/rendered", body="browser")
    transport = BrowserDispatchTransport(http, browser)

    plain = await transport.request(
        TransportRequest(
            url="https://shop.test/plain",
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
        )
    )
    rendered = await transport.request(
        TransportRequest(
            url="https://shop.test/rendered",
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
            browser=BrowserHint.REQUIRED,
        )
    )

    assert plain.text() == "http" and plain.route.kind == "direct"
    assert rendered.text() == "browser" and rendered.route.kind == "browser"
    assert [request.url for request in http.requests] == ["https://shop.test/plain"]
    assert [request.url for request in browser.requests] == ["https://shop.test/rendered"]


async def test_browser_dispatch_fails_required_request_without_backend() -> None:
    transport = BrowserDispatchTransport(FakeTransport())

    with pytest.raises(BrowserBackendUnavailable, match="no browser backend"):
        await transport.request(
            TransportRequest(
                url="https://shop.test/rendered",
                purpose=RequestPurpose.ENTITY,
                priority=RequestPriority.IDENTITY,
                browser=BrowserHint.REQUIRED,
            )
        )


async def test_browser_never_policy_rejects_required_without_invoking_backends() -> None:
    http = FakeTransport()
    browser = FakeTransport()
    http.add("https://shop.test/optional", body="http")
    transport = BrowserDispatchTransport(http, browser, policy=BrowserPolicy.NEVER)

    with pytest.raises(BrowserBackendUnavailable, match="policy forbids"):
        await transport.request(
            TransportRequest(
                url="https://shop.test/rendered",
                purpose=RequestPurpose.ENTITY,
                priority=RequestPriority.IDENTITY,
                browser=BrowserHint.REQUIRED,
            )
        )

    optional = await transport.request(
        TransportRequest(
            url="https://shop.test/optional",
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
            browser=BrowserHint.OPTIONAL,
        )
    )

    assert optional.text() == "http"
    assert [request.url for request in http.requests] == ["https://shop.test/optional"]
    assert browser.requests == []


async def test_browser_require_policy_upgrades_only_optional_requests() -> None:
    http = FakeTransport()
    browser = FakeTransport()
    http.add("https://shop.test/robots.txt", body="robots")
    browser.add("https://shop.test/product", body="rendered")
    transport = BrowserDispatchTransport(http, browser, policy=BrowserPolicy.REQUIRE)

    robots = await transport.request(
        TransportRequest(
            url="https://shop.test/robots.txt",
            purpose=RequestPurpose.ROBOTS,
            priority=RequestPriority.DISCOVERY,
            browser=BrowserHint.NEVER,
        )
    )
    rendered = await transport.request(
        TransportRequest(
            url="https://shop.test/product",
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
            browser=BrowserHint.OPTIONAL,
        )
    )

    assert robots.text() == "robots"
    assert rendered.text() == "rendered"
    assert [request.browser for request in http.requests] == [BrowserHint.NEVER]
    assert [request.browser for request in browser.requests] == [BrowserHint.REQUIRED]


async def test_browser_require_policy_fails_optional_without_backend() -> None:
    transport = BrowserDispatchTransport(FakeTransport(), policy=BrowserPolicy.REQUIRE)

    with pytest.raises(BrowserBackendUnavailable, match="no browser backend"):
        await transport.request(
            TransportRequest(
                url="https://shop.test/product",
                purpose=RequestPurpose.ENTITY,
                priority=RequestPriority.IDENTITY,
                browser=BrowserHint.OPTIONAL,
            )
        )


async def test_browser_dispatch_traces_typed_selection_and_denial() -> None:
    telemetry = RecordingTelemetry()
    browser = FakeTransport()
    browser.add("https://shop.test/product", body="rendered")
    traced = TransportRequest(
        url="https://shop.test/product",
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        browser=BrowserHint.OPTIONAL,
        trace_request_id="request-1",
        trace_attempt=2,
    )
    dispatch = BrowserDispatchTransport(
        FakeTransport(),
        browser,
        policy=BrowserPolicy.REQUIRE,
        telemetry=telemetry,
        telemetry_context={"collection_id": "collection-1"},
    )

    assert (await dispatch.request(traced)).route.kind == "browser"

    denied = BrowserDispatchTransport(
        FakeTransport(),
        policy=BrowserPolicy.NEVER,
        telemetry=telemetry,
        telemetry_context={"collection_id": "collection-1"},
    )
    with pytest.raises(BrowserBackendUnavailable, match="policy forbids"):
        await denied.request(traced.model_copy(update={"browser": BrowserHint.REQUIRED}))

    assert [event for event, _ in telemetry.events] == [
        "browser.dispatch",
        "browser.denied",
    ]
    selected = telemetry.events[0][1]
    assert selected == {
        "collection_id": "collection-1",
        "request_id": "request-1",
        "attempt": 2,
        "level": "debug",
        "url": "https://shop.test/product",
        "purpose": "entity",
        "selected": "browser",
        "requested": "optional",
        "policy": "require",
        "promoted": True,
    }
    assert telemetry.events[1][1]["reason"] == "policy_never"
    assert telemetry.events[1][1]["level"] == "warning"


async def test_browser_require_policy_keeps_proxy_browser_fail_closed() -> None:
    http = FakeTransport()
    browser = FakeTransport()
    transport = BrowserDispatchTransport(
        http,
        browser,
        policy=BrowserPolicy.REQUIRE,
        proxy_browser_supported=False,
    )

    with pytest.raises(ProxyBrowserRoutingUnsupported):
        await transport.request(
            TransportRequest(
                url="https://shop.test/product",
                purpose=RequestPurpose.ENTITY,
                priority=RequestPriority.IDENTITY,
                browser=BrowserHint.OPTIONAL,
            )
        )

    assert http.requests == []
    assert browser.requests == []


async def test_browser_dispatch_rotates_both_identities() -> None:
    http = FakeTransport()
    browser = FakeTransport()
    transport = BrowserDispatchTransport(http, browser)

    await transport.rotate_identity(RotationReason.CAPTCHA)

    assert http.rotations == [RotationReason.CAPTCHA]
    assert browser.rotations == [RotationReason.CAPTCHA]


def _response(body: str) -> TransportResponse:
    return TransportResponse(status=200, content=body.encode(), final_url="https://shop.test/data")
