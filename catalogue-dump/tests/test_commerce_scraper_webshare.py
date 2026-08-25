"""Focused contract tests for the Webshare data-plane gateway adapter."""

from datetime import UTC, datetime

import pytest
from mb_commerce_scraper.proxy import (
    HttpxProxyTransportFactory,
    ProxyKind,
    ProxyOutcome,
    ProxyPool,
    ProxyRequest,
)
from mb_commerce_scraper.transports import RotationReason
from pydantic import SecretStr

from mb_ceramics_catalogue.ops.commerce_scraper_webshare import (
    WebshareGatewayConfig,
    WebshareGatewayPool,
)
from mb_ceramics_catalogue.proxy import ProxyDenied


def config(**changes: object) -> WebshareGatewayConfig:
    values: dict[str, object] = {
        "username": SecretStr("provider-user"),
        "password": SecretStr("provider-password"),
        "countries": frozenset({"FR", "US"}),
        "sticky_session_ttl_seconds": 1_800,
    }
    values.update(changes)
    return WebshareGatewayConfig(**values)  # type: ignore[arg-type]


def request(**changes: object) -> ProxyRequest:
    values: dict[str, object] = {
        "source_id": "shop",
        "target_host": "shop.test",
        "country": "FR",
        "sticky": True,
        "session_ttl_seconds": 900,
        "maximum_requests": 1,
        "maximum_bytes": 2_000,
        "preferred_providers": ("webshare",),
    }
    values.update(changes)
    return ProxyRequest(**values)  # type: ignore[arg-type]


async def test_sticky_lease_projects_the_same_vendor_identity_to_http_and_browser() -> None:
    pool: ProxyPool = WebshareGatewayPool(
        config(), session_id_factory=lambda: "123456"
    )

    lease = await pool.acquire(request())
    http = lease.http_credentials()
    browser = lease.browser_credentials()

    assert lease.provider == "webshare"
    assert lease.route.host == "p.webshare.io"
    assert lease.route.port == 80
    assert lease.route.kind is ProxyKind.RESIDENTIAL
    assert http.username.get_secret_value() == "provider-user-fr-123456"
    assert browser.username == http.username
    assert browser.password == http.password
    assert browser.server == "http://p.webshare.io:80"
    assert lease.expires_at is not None and lease.expires_at > datetime.now(UTC)
    assert "provider-password" not in repr(http)

    await pool.release(lease)


async def test_rotation_changes_sticky_identity_and_retains_accounting_caps() -> None:
    identities = iter(("111", "222"))
    pool = WebshareGatewayPool(config(), session_id_factory=lambda: next(identities))
    first = await pool.acquire(request())

    authorization = await pool.authorize(first, 100)
    assert authorization is not None
    await authorization.reconcile(
        ProxyOutcome(
            target_host="shop.test",
            status=429,
            transmitted_bytes=100,
            received_bytes=200,
            classification="rate_limited",
        )
    )
    await pool.report(
        first,
        ProxyOutcome(
            target_host="shop.test", status=429, classification="rate_limited"
        ),
    )

    second = await pool.rotate(first, RotationReason.RATE_LIMITED)

    assert first.http_credentials().username.get_secret_value().endswith("-111")
    assert second.http_credentials().username.get_secret_value().endswith("-222")
    assert await pool.authorize(second, 1) is None
    assert pool.active_leases == 1
    await pool.release(second)
    assert pool.active_leases == 0


async def test_pool_lease_and_http_factory_intercall_keeps_vendor_grammar_out_of_core() -> None:
    pool = WebshareGatewayPool(config(), session_id_factory=lambda: "987")
    lease = await pool.acquire(request(country="US"))

    transport = HttpxProxyTransportFactory(
        allowed_origins=("https://shop.test",)
    ).build(lease)

    assert transport._client_options["proxy"] == (
        "http://provider-user-us-987:provider-password@p.webshare.io:80"
    )
    await pool.release(lease)


async def test_rotating_route_uses_documented_rotate_suffix_without_a_ttl() -> None:
    pool = WebshareGatewayPool(config())
    lease = await pool.acquire(
        request(sticky=False, session_ttl_seconds=None, maximum_requests=None)
    )

    assert lease.http_credentials().username.get_secret_value() == "provider-user-fr-rotate"
    assert lease.expires_at is None
    await pool.release(lease)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"kind": ProxyKind.DATACENTER}, "residential"),
        ({"country": "fr"}, "uppercase ISO"),
        ({"country": "ÉX"}, "uppercase ISO"),
        ({"region": "ile_de_france"}, "normalization"),
        ({"city": "paris"}, "normalization"),
        ({"session_ttl_seconds": 1_801}, "exceeds"),
        ({"sticky": False, "session_ttl_seconds": 60}, "exceeds"),
    ],
)
async def test_unverified_or_unsupported_gateway_semantics_fail_before_a_lease(
    changes: dict[str, object], message: str
) -> None:
    pool = WebshareGatewayPool(config())

    with pytest.raises(ProxyDenied, match=message):
        await pool.acquire(request(**changes))

    assert pool.active_leases == 0


async def test_invalid_generated_session_is_rejected_and_inner_lease_is_cleaned_up() -> None:
    pool = WebshareGatewayPool(config(), session_id_factory=lambda: "not-a-number")

    with pytest.raises(ProxyDenied, match="ASCII digits"):
        await pool.acquire(request())

    assert pool.active_leases == 0
