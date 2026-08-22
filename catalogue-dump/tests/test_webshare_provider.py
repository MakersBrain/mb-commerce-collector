"""Contract tests for the Webshare adapter; no test reaches the provider."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from mb_ceramics_catalogue.providers.base import ProviderError
from mb_ceramics_catalogue.providers.webshare import WebshareProvider


def provider(handler):
    return WebshareProvider(
        "secret-api-key",
        transport=httpx.MockTransport(handler), base_url="https://provider.test",
    )


async def test_authenticates_with_the_token_scheme_not_bearer():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Token secret-api-key"
        return httpx.Response(200, json={"plan": 1, "start_date": "2026-08-01T00:00:00Z",
                                         "end_date": "2026-09-01T00:00:00Z"})

    assert await provider(handler).health() is True


async def test_subscription_joins_the_term_to_the_plan():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/subscription/":
            return httpx.Response(200, json={
                "plan": 42, "start_date": "2026-08-01T00:00:00Z",
                "end_date": "2026-09-01T00:00:00Z",
            })
        assert request.url.path == "/subscription/plan/42/"
        return httpx.Response(200, json={
            "id": 42, "bandwidth_limit": 250.0, "proxy_type": "shared", "subusers_total": 10,
        })

    result = await provider(handler).subscription()
    assert result.traffic_limit_bytes == 250_000_000_000
    assert result.valid_from == datetime(2026, 8, 1, tzinfo=UTC)
    assert result.valid_until == datetime(2026, 9, 1, tzinfo=UTC)
    assert result.users_limit == 10


async def test_unlimited_bandwidth_is_none_not_zero():
    """Zero would read as 'no traffic purchased', and a cycle built from it
    would refuse every lease -- the opposite of what unlimited means."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/subscription/":
            return httpx.Response(200, json={
                "plan": 1, "start_date": "2026-08-01T00:00:00Z",
                "end_date": "2026-09-01T00:00:00Z",
            })
        return httpx.Response(200, json={"id": 1, "bandwidth_limit": 0})

    result = await provider(handler).subscription()
    assert result.traffic_limit_bytes is None
    assert result.raw_traffic_limit == 0


async def test_usage_is_byte_native_with_no_conversion():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/stats/aggregate/"
        return httpx.Response(200, json={"bandwidth_total": 1_234_567, "requests_total": 89})

    report = await provider(handler).usage(
        datetime.now(UTC) - timedelta(days=2), datetime.now(UTC)
    )
    assert report.total_bytes == 1_234_567
    assert report.total_received_bytes == 0
    assert report.buckets[0].received_bytes == 0
    assert report.requests == 89


async def test_usage_refuses_a_window_the_provider_will_not_serve():
    p = provider(lambda _: httpx.Response(200, json={}))
    with pytest.raises(ProviderError, match="at most 90 days"):
        await p.usage(datetime.now(UTC) - timedelta(days=120), datetime.now(UTC))


async def test_usage_rejects_a_non_integer_byte_total():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"bandwidth_total": "1.5"})

    with pytest.raises(ProviderError, match="non-integer"):
        await provider(handler).usage(
            datetime.now(UTC) - timedelta(days=1), datetime.now(UTC)
        )


async def test_provisioning_is_refused_rather_than_faking_a_password():
    """A Webshare sub-user has no username or password field. Mapping one
    anyway would leave the caller believing it set a credential."""
    p = provider(lambda _: httpx.Response(200, json={}))
    with pytest.raises(ProviderError, match="no settable username or password"):
        await p.create_subuser(
            username="catalogue_test", password="Long_password_123",
            traffic_limit_bytes=100_000_000, traffic_count_from=datetime(2026, 8, 1, tzinfo=UTC),
        )


async def test_password_rotation_is_refused_for_the_same_reason():
    p = provider(lambda _: httpx.Response(200, json={}))
    with pytest.raises(ProviderError, match="no password field"):
        await p.update_subuser("7", password="Another_password_123")


async def test_list_subusers_walks_offset_pagination():
    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset", 0))
        if offset == 0:
            return httpx.Response(200, json={
                "results": [{"id": 1, "label": "first", "proxy_limit": 1.0}],
                "next": "https://provider.test/subuser/?offset=1",
            })
        return httpx.Response(200, json={
            "results": [{"id": 2, "label": "second", "proxy_limit": 2.0}], "next": None,
        })

    subusers = await provider(handler).list_subusers()
    assert [s.id for s in subusers] == ["1", "2"]
    assert subusers[0].username == "first"
    assert subusers[1].traffic_limit_bytes == 2_000_000_000


async def test_a_mutation_is_never_retried():
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return httpx.Response(503)

    with pytest.raises(ProviderError) as raised:
        await provider(handler).delete_subuser("7")
    assert calls == ["DELETE"]
    assert raised.value.ambiguous is True
