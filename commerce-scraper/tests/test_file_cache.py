from __future__ import annotations

import asyncio
import gzip
from pathlib import Path
from typing import Any

import pytest

from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    BrowserEvaluation,
    BrowserHint,
    FileResponseCache,
    MemoryRequestBudget,
    MemoryResponseCache,
    MiddlewareTransport,
    RequestPriority,
    RequestPurpose,
    ResponseBodyTooLarge,
    RouteMetadata,
    TransportRequest,
    TransportResponse,
)


def request(
    url: str = "https://shop.test/data",
    **updates: Any,
) -> TransportRequest:
    values: dict[str, Any] = {
        "url": url,
        "purpose": RequestPurpose.ENTITY,
        "priority": RequestPriority.IDENTITY,
    }
    values.update(updates)
    return TransportRequest(**values)


def response(
    content: bytes,
    *,
    status: int = 200,
    headers: dict[str, str] | None = None,
    final_url: str = "https://shop.test/data",
    route: RouteMetadata | None = None,
) -> TransportResponse:
    return TransportResponse(
        status=status,
        headers=headers or {},
        content=content,
        final_url=final_url,
        route=route or RouteMetadata(),
    )


def artifacts(root: Path) -> list[Path]:
    return [path for path in root.rglob("*") if path.is_file()]


def decoded_artifacts(root: Path) -> bytes:
    decoded = bytearray()
    for path in artifacts(root):
        encoded = path.read_bytes()
        try:
            decoded.extend(gzip.decompress(encoded))
        except (EOFError, OSError):
            decoded.extend(encoded)
    return bytes(decoded)


@pytest.mark.asyncio
async def test_binary_response_round_trips_as_fresh_then_stale(tmp_path: Path) -> None:
    now = [1_000.0]
    cache = FileResponseCache(
        tmp_path,
        maximum_age_seconds=60,
        clock=lambda: now[0],
    )
    attempted = request()
    recorded = response(
        b"\x00\xffbinary\x80",
        status=206,
        headers={"content-type": "application/octet-stream", "etag": '"v1"'},
        final_url="https://shop.test/final",
    )

    await cache.put(attempted, recorded)

    fresh = await cache.get(attempted)
    assert fresh is not None
    assert (fresh.status, fresh.content, fresh.final_url) == (
        206,
        recorded.content,
        recorded.final_url,
    )
    assert fresh.headers == recorded.headers

    now[0] += 61
    assert await cache.get(attempted) is None
    stale = await cache.stale(attempted)
    assert stale is not None
    assert stale.content == recorded.content

    now[0] = 0
    assert await cache.get(attempted) is None
    assert await cache.stale(attempted) is None


@pytest.mark.asyncio
async def test_filesystem_and_memory_cache_share_normalized_key_semantics(
    tmp_path: Path,
) -> None:
    first = request(
        "HTTPS://SHOP.TEST:443/data",
        method="post",
        query={"page": 2, "locale": "en"},
        headers={"Accept": "application/json", "X-Ignored": "one"},
        json_body={"variables": {"after": None}, "query": "products"},
    )
    equivalent = request(
        "https://shop.test/data",
        method="POST",
        query={"locale": "en", "page": 2},
        headers={"accept": "application/json", "x-ignored": "two"},
        json_body={"query": "products", "variables": {"after": None}},
    )
    assert MemoryResponseCache.key(first) == MemoryResponseCache.key(equivalent)

    cache = FileResponseCache(tmp_path)
    await cache.put(first, response(b"page"))
    replayed = await cache.get(equivalent)
    assert replayed is not None and replayed.content == b"page"

    memory = MemoryResponseCache()
    await memory.put(first, response(b"memory", route=RouteMetadata(kind="proxy")))
    memory_hit = await memory.get(equivalent)
    assert memory_hit is not None and memory_hit.content == b"memory"
    assert memory_hit.from_cache and memory_hit.route == RouteMetadata(kind="cache")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attempted",
    [
        request(headers={"Authorization": "Bearer request-secret"}),
        request("https://shop.test/data?access_token=query-secret"),
        request("https://user:password@shop.test/data"),
    ],
    ids=("authorization", "sensitive-query", "userinfo"),
)
async def test_sensitive_requests_bypass_cache_without_artifacts(
    tmp_path: Path,
    attempted: TransportRequest,
) -> None:
    cache = FileResponseCache(tmp_path)

    await cache.put(attempted, response(b"public response"))

    memory = MemoryResponseCache()
    await memory.put(attempted, response(b"public response"))

    assert await cache.get(attempted) is None
    assert await cache.stale(attempted) is None
    assert await memory.get(attempted) is None
    assert artifacts(tmp_path) == []


@pytest.mark.asyncio
async def test_artifact_omits_sensitive_metadata_and_browser_script(
    tmp_path: Path,
) -> None:
    script = "() => 'private-browser-script'"
    attempted = request(
        "https://shop.test/product",
        browser=BrowserHint.REQUIRED,
        evaluation=BrowserEvaluation(action_id="shop.offer.v1", script=script),
    )
    cache = FileResponseCache(tmp_path)
    recorded = response(
        b'{"price":"12.00"}',
        headers={
            "content-type": "application/json",
            "etag": '"offer-v1"',
            "set-cookie": "session=response-secret",
            "proxy-authorization": "proxy-secret",
        },
        final_url=attempted.url,
        route=RouteMetadata(
            kind="proxy",
            provider="provider-secret",
            endpoint_id="endpoint-secret",
            lease_id="lease-secret",
        ),
    )

    await cache.put(attempted, recorded)

    encoded = decoded_artifacts(tmp_path)
    for secret in (
        script,
        "response-secret",
        "proxy-secret",
        "provider-secret",
        "endpoint-secret",
        "lease-secret",
    ):
        assert secret.encode() not in encoded
    replayed = await cache.get(attempted)
    assert replayed is not None
    assert replayed.headers["etag"] == '"offer-v1"'
    assert not any(
        name.casefold() in {"set-cookie", "proxy-authorization"}
        for name in replayed.headers
    )
    assert replayed.route.kind == "cache"
    assert replayed.route.provider is None
    assert replayed.route.endpoint_id is None
    assert replayed.route.lease_id is None


@pytest.mark.asyncio
async def test_oversize_put_preserves_the_prior_entry(tmp_path: Path) -> None:
    cache = FileResponseCache(tmp_path, maximum_response_bytes=5)
    attempted = request()
    await cache.put(attempted, response(b"first"))

    with pytest.raises(ResponseBodyTooLarge):
        await cache.put(attempted, response(b"second"))

    await cache.put(
        attempted,
        response(b"other", final_url=f"https://shop.test/{'x' * 3_000}"),
    )

    replayed = await cache.get(attempted)
    assert replayed is not None and replayed.content == b"first"


@pytest.mark.asyncio
async def test_corrupt_entry_is_a_bounded_miss(tmp_path: Path) -> None:
    cache = FileResponseCache(tmp_path)
    attempted = request()
    await cache.put(attempted, response(b"safe"))
    [entry] = artifacts(tmp_path)
    entry.write_bytes(b"not a cache document private-corrupt-content")

    assert await cache.get(attempted) is None
    assert await cache.stale(attempted) is None


@pytest.mark.asyncio
async def test_oversize_stored_entry_is_a_bounded_miss(tmp_path: Path) -> None:
    attempted = request()
    writer = FileResponseCache(tmp_path, maximum_response_bytes=100)
    await writer.put(attempted, response(b"x" * 20))
    bounded_reader = FileResponseCache(tmp_path, maximum_response_bytes=5)

    assert await bounded_reader.get(attempted) is None
    assert await bounded_reader.stale(attempted) is None


@pytest.mark.asyncio
async def test_concurrent_same_key_writes_and_reads_remain_atomic(
    tmp_path: Path,
) -> None:
    cache = FileResponseCache(tmp_path)
    attempted = request()
    bodies = tuple(f"complete-{index}".encode() for index in range(12))
    await cache.put(attempted, response(b"initial"))

    async def read_repeatedly() -> None:
        for _ in range(40):
            replayed = await cache.get(attempted)
            assert replayed is not None
            assert replayed.content in (b"initial", *bodies)

    await asyncio.gather(
        *(cache.put(attempted, response(body)) for body in bodies),
        *(read_repeatedly() for _ in range(4)),
    )

    replayed = await cache.get(attempted)
    assert replayed is not None and replayed.content in bodies
    assert len(artifacts(tmp_path)) == 1
    assert not any("tmp" in path.name or "partial" in path.name for path in artifacts(tmp_path))


@pytest.mark.asyncio
async def test_middleware_cache_hit_skips_backend_and_budget(tmp_path: Path) -> None:
    attempted = request()
    cache = FileResponseCache(tmp_path)
    await cache.put(attempted, response(b"cached"))
    backend = FakeTransport()
    budget = MemoryRequestBudget(maximum_requests=0)
    transport = MiddlewareTransport(backend, cache=cache, budget=budget)

    replayed = await transport.request(attempted)

    assert replayed.content == b"cached"
    assert replayed.from_cache
    assert replayed.route.kind == "cache"
    assert backend.requests == []
    assert budget.requests == 0
