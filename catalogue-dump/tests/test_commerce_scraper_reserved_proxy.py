"""Provider-neutral durable accounting composed with the Webshare data plane."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID, uuid4

import pytest
from mb_commerce_scraper.proxy import (
    BrowserSubrequestOutcome,
    PoolBrowserSubrequestAuthorizer,
    ProxyOutcome,
    ProxyRequest,
)
from mb_commerce_scraper.transports import RotationReason
from pydantic import SecretStr

from mb_ceramics_catalogue.ops import commerce_scraper_proxy as adapter
from mb_ceramics_catalogue.ops.commerce_scraper_webshare import (
    WebshareGatewayConfig,
    WebshareGatewayLease,
    WebshareGatewayPool,
)
from mb_ceramics_catalogue.proxy import ProxyDenied, ProxyReservationUsage


class FakeDatabase:
    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        yield object()


@pytest.fixture(autouse=True)
def _stub_close_reservation(monkeypatch: pytest.MonkeyPatch) -> None:
    async def close(_connection: Any, _lease: Any) -> None:
        return None

    monkeypatch.setattr(adapter, "close_reservation", close)


def request(**changes: object) -> ProxyRequest:
    values: dict[str, object] = {
        "source_id": "shop",
        "target_host": "shop.test",
        "country": "FR",
        "sticky": True,
        "session_ttl_seconds": 900,
        "maximum_requests": 2,
        "maximum_bytes": 1_000,
        "preferred_providers": ("webshare",),
    }
    values.update(changes)
    return ProxyRequest(**values)  # type: ignore[arg-type]


def durable_pool(
    *,
    route_id: UUID | None = None,
    endpoint_id: str | None = None,
) -> tuple[
    adapter.PostgresReservedProxyPool,
    WebshareGatewayPool,
    adapter.DurableProxyIdentity,
]:
    selected_route_id = route_id or uuid4()
    identity = adapter.DurableProxyIdentity(
        provider="webshare",
        profile="webshare-primary",
        profile_id=uuid4(),
        route_id=selected_route_id,
        secret_generation=7,
    )
    session_ids = iter(("123456", "654321", "777777"))
    inner = WebshareGatewayPool(
        WebshareGatewayConfig(
            username=SecretStr("provider-user"),
            password=SecretStr("provider-password"),
            endpoint_id=endpoint_id or str(selected_route_id),
            countries=frozenset({"FR"}),
            sticky_session_ttl_seconds=1_800,
        ),
        session_id_factory=lambda: next(session_ids),
    )
    return (
        adapter.PostgresReservedProxyPool(
            FakeDatabase(),
            inner,
            job_id=uuid4(),
            identity=identity,
            maximum_bytes=800,
            pilot=True,
        ),
        inner,
        identity,
    )


def test_identity_and_budget_reject_boolean_integer_coercion() -> None:
    with pytest.raises(ValueError, match="generation"):
        adapter.DurableProxyIdentity(
            provider="webshare",
            profile="webshare-primary",
            profile_id=uuid4(),
            route_id=uuid4(),
            secret_generation=True,
        )

    route_id = uuid4()
    identity = adapter.DurableProxyIdentity(
        provider="webshare",
        profile="webshare-primary",
        profile_id=uuid4(),
        route_id=route_id,
        secret_generation=1,
    )
    inner = WebshareGatewayPool(
        WebshareGatewayConfig(
            username=SecretStr("provider-user"),
            password=SecretStr("provider-password"),
            endpoint_id=str(route_id),
        )
    )
    with pytest.raises(ValueError, match="maximum_bytes"):
        adapter.PostgresReservedProxyPool(
            FakeDatabase(),
            inner,
            job_id=uuid4(),
            identity=identity,
            maximum_bytes=True,
        )


async def test_acquire_binds_exact_durable_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservation_id = uuid4()
    calls: list[dict[str, Any]] = []

    async def fake_reserve(_connection: Any, **kwargs: Any) -> UUID:
        calls.append(kwargs)
        return reservation_id

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    pool, inner, identity = durable_pool()

    lease = await pool.acquire(request())

    assert lease.provider == "webshare"
    assert lease.route.endpoint_id == str(identity.route_id)
    assert lease.maximum_bytes == 800
    assert calls == [
        {
            "job_id": pool._job_id,
            "profile": "webshare-primary",
            "profile_id": identity.profile_id,
            "route_id": identity.route_id,
            "requested_bytes": 800,
            "pilot": True,
            "secret_generation": 7,
            "provider": "webshare",
        }
    ]
    assert inner.active_leases == 1
    await pool.release(lease)
    assert inner.active_leases == 0


async def test_reservation_failure_releases_the_unexposed_inner_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_reservation(_connection: Any, **_kwargs: Any) -> UUID:
        raise RuntimeError("ledger unavailable")

    monkeypatch.setattr(adapter, "reserve", fail_reservation)
    pool, inner, _identity = durable_pool()

    with pytest.raises(RuntimeError, match="ledger unavailable"):
        await pool.acquire(request())

    assert inner.active_leases == 0


async def test_route_mismatch_is_rejected_before_a_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        nonlocal called
        called = True
        return uuid4()

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    pool, inner, _identity = durable_pool(endpoint_id="wrong-route")

    with pytest.raises(ProxyDenied, match="route does not match"):
        await pool.acquire(request())

    assert not called
    assert inner.active_leases == 0


async def test_attempt_authorizes_durably_last_and_reconciles_both_layers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservation_id = uuid4()
    authorization_id = uuid4()
    events: list[tuple[str, Any]] = []
    closed: list[tuple[int, int]] = []

    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        return reservation_id

    async def fake_authorize(
        _connection: Any,
        *,
        reservation_id: UUID,
        estimated_bytes: int,
        maximum_requests: int | None,
    ) -> UUID:
        events.append(("durable-authorize", (reservation_id, estimated_bytes, maximum_requests)))
        return authorization_id

    async def fake_reconcile(
        _connection: Any,
        *,
        authorization_id: UUID,
        actual_bytes: int,
        physical_requests: int,
    ) -> ProxyReservationUsage:
        events.append(("durable-reconcile", (authorization_id, actual_bytes, physical_requests)))
        return ProxyReservationUsage(actual_bytes, physical_requests, False, False)

    async def fake_close(_connection: Any, lease: Any) -> None:
        closed.append((lease.used_bytes, lease.requests))

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    monkeypatch.setattr(adapter, "authorize_reservation_attempt", fake_authorize)
    monkeypatch.setattr(adapter, "reconcile_reservation_attempt", fake_reconcile)
    monkeypatch.setattr(adapter, "close_reservation", fake_close)
    pool, _inner, _identity = durable_pool()
    lease = await pool.acquire(request())

    authorization = await pool.authorize(lease, 300)
    assert authorization is not None
    # The inner token has already retained its estimate when the durable call
    # runs, so the database remains the final gate before dispatch.
    assert not lease._inner.can_start(701)
    assert events == [("durable-authorize", (reservation_id, 300, 2))]

    await authorization.reconcile(
        ProxyOutcome(
            target_host="shop.test",
            status=200,
            transmitted_bytes=40,
            received_bytes=160,
            classification="success",
        )
    )

    assert events[-1] == ("durable-reconcile", (authorization_id, 200, 1))
    assert isinstance(lease._inner, WebshareGatewayLease)
    assert lease._inner._inner.used_bytes == 200
    assert lease._state.used_bytes == 200
    await pool.report(
        lease,
        ProxyOutcome(target_host="shop.test", status=200, classification="success"),
    )
    assert lease._state.used_bytes == 200
    await pool.release(lease)
    assert closed == [(200, 1)]


async def test_durable_denial_releases_inner_token_without_consuming_its_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        return uuid4()

    async def deny(_connection: Any, **_kwargs: Any) -> None:
        nonlocal calls
        calls += 1
        return None

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    monkeypatch.setattr(adapter, "authorize_reservation_attempt", deny)
    pool, _inner, _identity = durable_pool()
    lease = await pool.acquire(request(maximum_requests=1))

    assert await pool.authorize(lease, 100) is None
    assert await pool.authorize(lease, 100) is None
    assert calls == 2
    assert isinstance(lease._inner, WebshareGatewayLease)
    assert lease._inner._inner.used_requests == 0
    await pool.release(lease)


async def test_durable_authorization_failure_disables_all_new_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        return uuid4()

    async def fail(_connection: Any, **_kwargs: Any) -> UUID:
        raise RuntimeError("ledger unavailable")

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    monkeypatch.setattr(adapter, "authorize_reservation_attempt", fail)
    pool, _inner, _identity = durable_pool()
    lease = await pool.acquire(request())

    with pytest.raises(RuntimeError, match="ledger unavailable"):
        await pool.authorize(lease, 100)

    assert not lease.can_start(1)
    assert await pool.authorize(lease, 1) is None


async def test_rotation_reuses_reservation_and_pending_attempt_blocks_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservation_id = uuid4()

    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        return reservation_id

    async def fake_authorize(_connection: Any, **_kwargs: Any) -> UUID:
        return uuid4()

    async def fake_release(_connection: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    monkeypatch.setattr(adapter, "authorize_reservation_attempt", fake_authorize)
    monkeypatch.setattr(adapter, "release_reservation_attempt", fake_release)
    pool, _inner, _identity = durable_pool()
    first = await pool.acquire(request())
    authorization = await pool.authorize(first, 100)
    assert authorization is not None

    with pytest.raises(ProxyDenied, match="authorized requests"):
        await pool.rotate(first, RotationReason.EXPLICIT)
    with pytest.raises(ProxyDenied, match="unreconciled"):
        await pool.release(first)

    await authorization.release()
    second = await pool.rotate(first, RotationReason.EXPLICIT)
    assert second._state is first._state
    assert second.lease_id == f"{reservation_id}:1"
    assert first.http_credentials().username != second.http_credentials().username
    with pytest.raises(ProxyDenied, match="not active"):
        await pool.authorize(first, 1)
    await pool.release(second)


async def test_invalid_rotated_identity_releases_replacement_and_keeps_close_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        return uuid4()

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    pool, inner, _identity = durable_pool()
    first = await pool.acquire(request())
    original_rotate = inner.rotate

    async def rotate_with_wrong_provider(
        lease: Any, reason: RotationReason
    ) -> WebshareGatewayLease:
        replacement = await original_rotate(lease, reason)
        replacement.provider = "other-provider"
        return replacement

    monkeypatch.setattr(inner, "rotate", rotate_with_wrong_provider)

    with pytest.raises(ProxyDenied, match="provider does not match"):
        await pool.rotate(first, RotationReason.EXPLICIT)

    assert inner.active_leases == 0
    # RoutedTransport uses this same old wrapper on rotation failure. It still
    # owns the durable reservation even though Webshare invalidated its lease.
    await pool.release(first)


async def test_browser_subrequests_each_receive_a_durable_authorization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorized: list[int] = []
    reconciled: list[tuple[int, int]] = []

    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        return uuid4()

    async def fake_authorize(_connection: Any, *, estimated_bytes: int, **_kwargs: Any) -> UUID:
        authorized.append(estimated_bytes)
        return uuid4()

    async def fake_reconcile(
        _connection: Any,
        *,
        actual_bytes: int,
        physical_requests: int,
        **_kwargs: Any,
    ) -> ProxyReservationUsage:
        reconciled.append((actual_bytes, physical_requests))
        return ProxyReservationUsage(
            sum(value for value, _count in reconciled),
            sum(count for _value, count in reconciled),
            False,
            False,
        )

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    monkeypatch.setattr(adapter, "authorize_reservation_attempt", fake_authorize)
    monkeypatch.setattr(adapter, "reconcile_reservation_attempt", fake_reconcile)
    pool, _inner, _identity = durable_pool()
    lease = await pool.acquire(request())
    authorizer = PoolBrowserSubrequestAuthorizer(pool, lease, "shop.test")

    first = await authorizer.authorize(40)
    second = await authorizer.authorize(60)
    assert first is not None and second is not None
    await first.reconcile(
        BrowserSubrequestOutcome(
            status=200,
            transmitted_bytes=10,
            received_bytes=20,
            classification="success",
        )
    )
    await second.reconcile(
        BrowserSubrequestOutcome(
            status=200,
            transmitted_bytes=20,
            received_bytes=30,
            classification="success",
        )
    )

    assert authorized == [40, 60]
    assert reconciled == [(30, 1), (50, 1)]
    await pool.release(lease)
