"""Deterministic boundaries for the catalogue-owned neutral proxy adapter."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID, uuid4

import pytest
from mb_commerce_scraper.proxy import (
    ProxyKind,
    ProxyLease,
    ProxyOutcome,
    ProxyPool,
    ProxyRequest,
    ProxyRouting,
    RoutedTransport,
    RoutingMode,
)
from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    MiddlewareTransport,
    RequestPriority,
    RequestPurpose,
    RotationReason,
    TransportRequest,
    TransportResponse,
)
from mb_commerce_scraper.transports.base import CommerceTransport

from mb_ceramics_catalogue.ops import commerce_scraper_proxy as adapter
from mb_ceramics_catalogue.proxy import ProxyDenied, ProxyProfile, ProxyReservationUsage


class FakeDatabase:
    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        yield object()


class ProxyFactory:
    def __init__(self, transport: CommerceTransport) -> None:
        self.transport = transport
        self.leases: list[ProxyLease] = []

    def build(self, lease: ProxyLease) -> CommerceTransport:
        self.leases.append(lease)
        return self.transport


class BlockingProxyTransport:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.closed = False

    async def request(self, request: TransportRequest) -> TransportResponse:
        del request
        self.started.set()
        await asyncio.Future()
        raise AssertionError("unreachable")

    async def rotate_identity(self, reason: RotationReason) -> None:
        del reason

    async def aclose(self) -> None:
        self.closed = True


def request(**changes: Any) -> ProxyRequest:
    return ProxyRequest(
        source_id="shop",
        target_host="shop.test",
        country="FR",
        maximum_requests=2,
        maximum_bytes=100,
        **changes,
    )


def pool() -> adapter.PostgresDecodoProxyPool:
    return adapter.PostgresDecodoProxyPool(
        FakeDatabase(),
        job_id=uuid4(),
        profile=ProxyProfile("decodo", "gate.test", 7000, "named-user", "secret"),
        profile_id=uuid4(),
        route_id=uuid4(),
        maximum_bytes=200,
        route_country="FR",
    )


async def test_attempt_tokens_authorize_reconcile_release_and_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservation_id = uuid4()
    authorized: list[tuple[int, int | None]] = []
    reconciled: list[tuple[UUID, int, int]] = []
    released: list[UUID] = []
    closed: list[tuple[int, int]] = []

    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        return reservation_id

    async def fake_authorize(
        _connection: Any,
        *,
        reservation_id: UUID,
        estimated_bytes: int,
        maximum_requests: int | None,
    ) -> UUID | None:
        assert reservation_id == reservation_id_value
        authorized.append((estimated_bytes, maximum_requests))
        return uuid4()

    async def fake_reconcile(
        _connection: Any,
        *,
        authorization_id: UUID,
        actual_bytes: int,
        physical_requests: int,
    ) -> ProxyReservationUsage:
        reconciled.append((authorization_id, actual_bytes, physical_requests))
        return ProxyReservationUsage(40, 1, False, False)

    async def fake_release(_connection: Any, *, authorization_id: UUID) -> None:
        released.append(authorization_id)

    async def fake_close(_connection: Any, lease: Any) -> None:
        closed.append((lease.used_bytes, lease.requests))

    reservation_id_value = reservation_id
    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    monkeypatch.setattr(adapter, "authorize_reservation_attempt", fake_authorize)
    monkeypatch.setattr(adapter, "reconcile_reservation_attempt", fake_reconcile)
    monkeypatch.setattr(adapter, "release_reservation_attempt", fake_release)
    monkeypatch.setattr(adapter, "close_reservation", fake_close)

    proxy_pool = pool()
    neutral_pool: ProxyPool = proxy_pool
    assert neutral_pool is proxy_pool
    lease = await proxy_pool.acquire(request())
    assert lease.maximum_bytes == 100
    assert lease.route.host == "gate.test"
    assert lease.http_credentials().username.get_secret_value().startswith("user-named-user-")
    assert "named-user" not in repr(lease.http_credentials())

    dispatched = await proxy_pool.authorize(lease, 60)
    undispatched = await proxy_pool.authorize(lease, 20)
    assert isinstance(dispatched, adapter._PostgresAttemptAuthorization)
    assert isinstance(undispatched, adapter._PostgresAttemptAuthorization)
    assert authorized == [(60, 2), (20, 2)]
    with pytest.raises(ProxyDenied, match="unreconciled"):
        await proxy_pool.release(lease)

    await dispatched.reconcile(
        ProxyOutcome(
            target_host="shop.test",
            transmitted_bytes=10,
            received_bytes=30,
            classification="success",
        )
    )
    await undispatched.release()
    assert reconciled[0][1:] == (40, 1)
    assert released == [undispatched.authorization_id]

    await proxy_pool.report(
        lease, ProxyOutcome(target_host="shop.test", classification="success")
    )
    await proxy_pool.release(lease)
    await proxy_pool.release(lease)
    assert closed == [(40, 1)]
    assert not lease.can_start()


async def test_authorization_failure_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        return uuid4()

    async def failed_authorize(_connection: Any, **_kwargs: Any) -> UUID | None:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    monkeypatch.setattr(adapter, "authorize_reservation_attempt", failed_authorize)
    proxy_pool = pool()
    lease = await proxy_pool.acquire(request())

    with pytest.raises(RuntimeError, match="database unavailable"):
        await proxy_pool.authorize(lease, 10)
    assert not lease.can_start(1)
    assert await proxy_pool.authorize(lease, 1) is None


async def test_reconciliation_overshoot_is_retained_then_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        return uuid4()

    async def fake_authorize(_connection: Any, **_kwargs: Any) -> UUID | None:
        return uuid4()

    async def overshot(_connection: Any, **_kwargs: Any) -> ProxyReservationUsage:
        return ProxyReservationUsage(120, 1, False, True)

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    monkeypatch.setattr(adapter, "authorize_reservation_attempt", fake_authorize)
    monkeypatch.setattr(adapter, "reconcile_reservation_attempt", overshot)
    proxy_pool = pool()
    lease = await proxy_pool.acquire(request())
    authorization = await proxy_pool.authorize(lease, 50)
    assert authorization is not None

    with pytest.raises(RuntimeError, match="request or byte limit"):
        await authorization.reconcile(
            ProxyOutcome(
                target_host="shop.test",
                received_bytes=120,
                classification="success",
            )
        )
    assert lease._state.legacy.used_bytes == 120
    assert not lease.can_start()


async def test_rotation_reuses_reservation_and_rejects_pending_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservations: list[UUID] = []

    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        value = uuid4()
        reservations.append(value)
        return value

    async def fake_authorize(_connection: Any, **_kwargs: Any) -> UUID | None:
        return uuid4()

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    monkeypatch.setattr(adapter, "authorize_reservation_attempt", fake_authorize)
    proxy_pool = pool()
    first = await proxy_pool.acquire(request())
    authorization = await proxy_pool.authorize(first, 1)
    assert authorization is not None

    with pytest.raises(ProxyDenied, match="authorized requests"):
        await proxy_pool.rotate(first, RotationReason.BLOCKED)


async def test_incompatible_request_is_denied_before_database_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        nonlocal called
        called = True
        return uuid4()

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    proxy_pool = pool()

    with pytest.raises(ProxyDenied, match="residential"):
        await proxy_pool.acquire(request(kind=ProxyKind.DATACENTER))
    assert not called


async def test_middleware_fallback_uses_one_durable_catalogue_proxy_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservation_id = uuid4()
    authorization_id = uuid4()
    authorized: list[int] = []
    reconciled: list[tuple[int, int]] = []
    closed: list[UUID] = []

    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        return reservation_id

    async def fake_authorize(
        _connection: Any, *, estimated_bytes: int, **_kwargs: Any
    ) -> UUID:
        authorized.append(estimated_bytes)
        return authorization_id

    async def fake_reconcile(
        _connection: Any,
        *,
        actual_bytes: int,
        physical_requests: int,
        **_kwargs: Any,
    ) -> ProxyReservationUsage:
        reconciled.append((actual_bytes, physical_requests))
        return ProxyReservationUsage(actual_bytes, physical_requests, False, False)

    async def fake_close(_connection: Any, lease: Any) -> None:
        closed.append(lease.reservation_id)

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    monkeypatch.setattr(adapter, "authorize_reservation_attempt", fake_authorize)
    monkeypatch.setattr(adapter, "reconcile_reservation_attempt", fake_reconcile)
    monkeypatch.setattr(adapter, "close_reservation", fake_close)

    direct = FakeTransport()
    direct.add("https://shop.test/data", status=429, body="blocked")
    proxy = FakeTransport()
    proxy.add("https://shop.test/data", body="proxied")
    proxy_factory = ProxyFactory(proxy)
    routed = RoutedTransport(
        direct,
        pool=pool(),
        proxy_factory=proxy_factory,
        routing=ProxyRouting.fallback(country="FR"),
        source_id="shop",
        base_url="https://shop.test/",
        maximum_requests=2,
        maximum_bytes=100,
    )
    middleware = MiddlewareTransport(
        routed,
        retries=1,
        backoff=lambda _attempt: 0,
    )

    response = await middleware.request(
        TransportRequest(
            url="https://shop.test/data",
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
        )
    )
    await routed.aclose()

    assert response.text() == "proxied"
    assert response.route.provider == "decodo"
    assert len(direct.requests) == 1
    assert len(proxy.requests) == 1
    assert len(proxy_factory.leases) == 1
    assert len(authorized) == 1 and authorized[0] > 0
    assert len(reconciled) == 1 and reconciled[0][1] == 1
    assert closed == [reservation_id]


async def test_cancelled_dispatched_proxy_attempt_reconciles_before_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservation_id = uuid4()
    authorization_id = uuid4()
    reconciled: list[tuple[int, int]] = []
    released_authorizations: list[UUID] = []
    closed: list[UUID] = []

    async def fake_reserve(_connection: Any, **_kwargs: Any) -> UUID:
        return reservation_id

    async def fake_authorize(_connection: Any, **_kwargs: Any) -> UUID:
        return authorization_id

    async def fake_reconcile(
        _connection: Any,
        *,
        actual_bytes: int,
        physical_requests: int,
        **_kwargs: Any,
    ) -> ProxyReservationUsage:
        reconciled.append((actual_bytes, physical_requests))
        return ProxyReservationUsage(actual_bytes, physical_requests, False, False)

    async def fake_release(
        _connection: Any, *, authorization_id: UUID
    ) -> None:
        released_authorizations.append(authorization_id)

    async def fake_close(_connection: Any, lease: Any) -> None:
        closed.append(lease.reservation_id)

    monkeypatch.setattr(adapter, "reserve", fake_reserve)
    monkeypatch.setattr(adapter, "authorize_reservation_attempt", fake_authorize)
    monkeypatch.setattr(adapter, "reconcile_reservation_attempt", fake_reconcile)
    monkeypatch.setattr(adapter, "release_reservation_attempt", fake_release)
    monkeypatch.setattr(adapter, "close_reservation", fake_close)

    proxy = BlockingProxyTransport()
    routed = RoutedTransport(
        FakeTransport(),
        pool=pool(),
        proxy_factory=ProxyFactory(proxy),
        routing=ProxyRouting(mode=RoutingMode.ALWAYS, country="FR"),
        source_id="shop",
        base_url="https://shop.test/",
        maximum_requests=2,
        maximum_bytes=100,
    )
    task = asyncio.create_task(
        routed.request(
            TransportRequest(
                url="https://shop.test/data",
                purpose=RequestPurpose.ENTITY,
                priority=RequestPriority.IDENTITY,
            )
        )
    )
    await proxy.started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    await routed.aclose()

    assert len(reconciled) == 1
    assert reconciled[0][0] > 0
    assert reconciled[0][1] == 1
    assert released_authorizations == []
    assert closed == [reservation_id]
    assert proxy.closed
