from __future__ import annotations

import asyncio

import pytest

from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import CachedRobotsChecker, RobotsFetchFailurePolicy
from mb_commerce_scraper.transports.base import TransportFailure, TransportRequest


class FailingTransport(FakeTransport):
    async def request(self, request: TransportRequest):  # type: ignore[no-untyped-def]
        self.requests.append(request)
        raise TransportFailure("failed URL https://user:secret@shop.test/robots.txt?token=secret")


class SlowTransport(FakeTransport):
    async def request(self, request: TransportRequest):  # type: ignore[no-untyped-def]
        await asyncio.sleep(0)
        return await super().request(request)


@pytest.mark.asyncio
async def test_fetches_raw_robots_once_per_origin_and_applies_user_agent_rules() -> None:
    transport = FakeTransport()
    transport.add(
        "https://shop.test/robots.txt",
        body="User-agent: commerce-bot\nDisallow: /private/item\nAllow: /private/listing\n",
    )
    checker = CachedRobotsChecker(transport, user_agent="commerce-bot")

    assert not await checker.allowed("https://shop.test/private/item?token=secret")
    assert await checker.allowed("https://shop.test/private/listing")
    assert await checker.allowed("https://shop.test/public")

    assert len(transport.requests) == 1
    request = transport.requests[0]
    assert request.url == "https://shop.test/robots.txt"
    assert request.purpose.value == "robots"
    assert request.cache.value == "bypass"
    assert request.browser.value == "never"


@pytest.mark.asyncio
async def test_cache_is_partitioned_by_normalized_origin() -> None:
    transport = FakeTransport()
    transport.add("https://one.test/robots.txt", body="User-agent: *\nDisallow: /blocked\n")
    transport.add("https://two.test:8443/robots.txt", body="User-agent: *\nAllow: /\n")
    checker = CachedRobotsChecker(transport)

    assert not await checker.allowed("HTTPS://ONE.TEST:443/blocked")
    assert await checker.allowed("https://two.test:8443/product")
    assert [request.url for request in transport.requests] == [
        "https://one.test/robots.txt",
        "https://two.test:8443/robots.txt",
    ]


@pytest.mark.asyncio
async def test_concurrent_checks_coalesce_the_origin_fetch() -> None:
    transport = SlowTransport()
    transport.add("https://shop.test/robots.txt", body="User-agent: *\nAllow: /\n")
    checker = CachedRobotsChecker(transport)

    decisions = await asyncio.gather(
        checker.allowed("https://shop.test/a"),
        checker.allowed("https://shop.test/b"),
    )

    assert list(decisions) == [True, True]
    assert len(transport.requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403])
async def test_authorization_status_denies_all(status: int) -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/robots.txt", status=status)

    assert not await CachedRobotsChecker(transport).allowed("https://shop.test/product")


@pytest.mark.asyncio
async def test_missing_robots_file_allows_access() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/robots.txt", status=404)

    assert await CachedRobotsChecker(transport).allowed("https://shop.test/product")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (RobotsFetchFailurePolicy.DENY, False),
        (RobotsFetchFailurePolicy.ALLOW, True),
    ],
)
async def test_fetch_failure_uses_explicit_cached_policy_without_leaking_error(
    policy: RobotsFetchFailurePolicy, expected: bool
) -> None:
    transport = FailingTransport()
    checker = CachedRobotsChecker(transport, failure_policy=policy)

    assert await checker.allowed("https://shop.test/product?api_key=secret") is expected
    assert await checker.allowed("https://shop.test/other") is expected
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_transport_and_server_failure_decisions_can_differ() -> None:
    transport_failure = FailingTransport()
    server_failure = FakeTransport()
    server_failure.add("https://shop.test/robots.txt", status=503)
    assert await CachedRobotsChecker(
        transport_failure,
        transport_failure_policy=RobotsFetchFailurePolicy.ALLOW,
        server_failure_policy=RobotsFetchFailurePolicy.DENY,
    ).allowed("https://shop.test/private")
    assert not await CachedRobotsChecker(
        server_failure,
        transport_failure_policy=RobotsFetchFailurePolicy.ALLOW,
        server_failure_policy=RobotsFetchFailurePolicy.DENY,
    ).allowed("https://shop.test/private")


@pytest.mark.asyncio
async def test_invalid_or_credential_only_url_follows_safe_failure_policy_without_fetch() -> None:
    transport = FakeTransport()
    deny = CachedRobotsChecker(transport)
    allow = CachedRobotsChecker(transport, failure_policy=RobotsFetchFailurePolicy.ALLOW)

    assert not await deny.allowed("file:///tmp/catalogue")
    assert await allow.allowed("not a URL")
    assert transport.requests == []
