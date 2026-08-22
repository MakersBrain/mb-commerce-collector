"""Compatibility tests for the native library response-cache projection."""

from __future__ import annotations

import time

import pytest
from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    BrowserEvaluation,
    BrowserHint,
    MiddlewareTransport,
    RequestPriority,
    RequestPurpose,
    TransportFailure,
    TransportRequest,
    TransportResponse,
)

from mb_ceramics_catalogue.ops.commerce_scraper_cache import CatalogueResponseCache
from mb_ceramics_catalogue.scrapers.cache import CachedResponse, ResponseCache


def request(*, browser: BrowserHint = BrowserHint.NEVER) -> TransportRequest:
    return TransportRequest(
        method="POST",
        url="https://shop.test/products.json",
        query={"page": 2},
        json_body={"query": "products"},
        purpose=RequestPurpose.ENTITY,
        priority=RequestPriority.IDENTITY,
        browser=browser,
    )


async def test_native_http_cache_reads_the_existing_legacy_key(tmp_path) -> None:
    legacy = ResponseCache(tmp_path, mode="replay")
    attempted = request()
    key = legacy.key(
        "http",
        attempted.url,
        method=attempted.method,
        params=attempted.query,
        body=attempted.json_body,
        agent=False,
    )
    legacy.mode = "auto"
    legacy.write(
        key,
        CachedResponse(
            status=200,
            url="https://shop.test/products.json?page=2",
            body='{"products": []}',
            headers={"content-type": "application/json"},
            fetched_at=time.time(),
        ),
    )
    legacy.mode = "replay"

    response = await CatalogueResponseCache(legacy).get(attempted)

    assert response is not None
    assert response.route.kind == "cache"
    assert response.from_cache
    assert response.json_value() == {"products": []}


async def test_native_browser_write_uses_the_existing_render_key(tmp_path) -> None:
    legacy = ResponseCache(tmp_path, mode="auto")
    attempted = request(browser=BrowserHint.REQUIRED).model_copy(
        update={"method": "GET", "query": {}, "json_body": None}
    )
    cache = CatalogueResponseCache(legacy)

    await cache.put(
        attempted,
        TransportResponse(
            status=200,
            content=b"<html>rendered</html>",
            final_url=attempted.url,
        ),
    )

    stored = legacy.read(
        legacy.key("render", attempted.url, wait_ms=1500, wait_for=None),
        attempted.url,
    )
    assert stored is not None
    assert stored.kind == "render"
    assert stored.body == "<html>rendered</html>"


async def test_browser_evaluation_cache_key_is_replayable_and_secret_free(tmp_path) -> None:
    legacy = ResponseCache(tmp_path, mode="auto")
    cache = CatalogueResponseCache(legacy)
    script = "() => 'trusted private script'"
    attempted = TransportRequest(
        url="https://shop.test/product",
        purpose=RequestPurpose.ENRICHMENT,
        priority=RequestPriority.OPTIONAL,
        browser=BrowserHint.REQUIRED,
        evaluation=BrowserEvaluation(
            action_id="shop.offer.v1",
            script=script,
        ),
    )

    await cache.put(
        attempted,
        TransportResponse(
            status=200,
            headers={"content-type": "application/json"},
            content=b'{"price":"26.65"}',
            final_url=attempted.url,
        ),
    )
    legacy.mode = "replay"

    replayed = await cache.get(attempted)
    assert replayed is not None
    assert replayed.json_value() == {"price": "26.65"}
    assert script not in repr(tuple(tmp_path.rglob("*")))


async def test_browser_graphql_pages_have_secret_free_distinct_replay_keys(
    tmp_path,
) -> None:
    legacy = ResponseCache(tmp_path, mode="auto")
    cache = CatalogueResponseCache(legacy)
    first = TransportRequest(
        method="POST",
        url="https://shop.test/graphql",
        headers={"authorization": "Bearer first-secret"},
        json_body={"query": "products", "variables": {"after": None}},
        purpose=RequestPurpose.DISCOVERY,
        priority=RequestPriority.DISCOVERY,
        browser=BrowserHint.REQUIRED,
    )
    second = first.model_copy(
        update={
            "headers": {"authorization": "Bearer second-secret"},
            "json_body": {
                "query": "products",
                "variables": {"after": "cursor-2"},
            },
        }
    )

    await cache.put(
        first,
        TransportResponse(
            status=200,
            content=b'{"page":1}',
            final_url=first.url,
        ),
    )
    await cache.put(
        second,
        TransportResponse(
            status=200,
            content=b'{"page":2}',
            final_url=second.url,
        ),
    )
    legacy.mode = "replay"

    replacement = first.model_copy(
        update={"headers": {"authorization": "Bearer replacement-secret"}}
    )
    first_replay = await cache.get(replacement)
    second_replay = await cache.get(second)

    assert first_replay is not None and first_replay.json_value() == {"page": 1}
    assert second_replay is not None and second_replay.json_value() == {"page": 2}
    assert cache._key(first) == cache._key(replacement)
    assert cache._key(first) != cache._key(second)


async def test_replay_miss_fails_without_becoming_a_network_miss(tmp_path) -> None:
    cache = CatalogueResponseCache(ResponseCache(tmp_path, mode="replay"))

    with pytest.raises(TransportFailure, match="replay cache entry") as caught:
        await cache.get(request())

    assert caught.value.__cause__ is None
    assert "shop.test" not in str(caught.value)


async def test_native_cache_exposes_expired_entry_for_revalidation(tmp_path) -> None:
    attempted = request().model_copy(
        update={"method": "GET", "query": {}, "json_body": None}
    )
    legacy = ResponseCache(tmp_path, mode="auto", max_age=1)
    key = legacy.key(
        "http",
        attempted.url,
        method="GET",
        params=None,
        body=None,
        agent=False,
    )
    legacy.write(
        key,
        CachedResponse(
            status=200,
            url=attempted.url,
            body="previous",
            headers={"etag": '"v1"'},
            fetched_at=time.time() - 3_600,
        ),
    )
    cache = CatalogueResponseCache(legacy)

    assert await cache.get(attempted) is None
    stale = await cache.stale(attempted)

    assert stale is not None
    assert stale.text() == "previous"
    assert stale.headers["etag"] == '"v1"'


async def test_catalogue_cache_and_middleware_revalidate_the_legacy_archive(
    tmp_path,
) -> None:
    attempted = request().model_copy(
        update={"method": "GET", "query": {}, "json_body": None}
    )
    legacy = ResponseCache(tmp_path, mode="auto", max_age=1)
    key = legacy.key(
        "http",
        attempted.url,
        method="GET",
        params=None,
        body=None,
        agent=False,
    )
    legacy.write(
        key,
        CachedResponse(
            status=200,
            url=attempted.url,
            body="previous",
            headers={"content-type": "text/plain", "etag": '"v1"'},
            fetched_at=time.time() - 3_600,
        ),
    )
    backend = FakeTransport()
    backend.add(attempted.url, status=304, headers={"etag": '"v2"'})

    response = await MiddlewareTransport(
        backend,
        cache=CatalogueResponseCache(legacy),
        retries=0,
    ).request(attempted)

    assert backend.requests[0].headers["if-none-match"] == '"v1"'
    assert response.status == 200
    assert response.text() == "previous"
    assert response.from_cache
    refreshed = legacy.read(key, attempted.url)
    assert refreshed is not None
    assert refreshed.headers["etag"] == '"v2"'
