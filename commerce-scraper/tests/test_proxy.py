import pytest

from mb_commerce_scraper.proxy import ProxyOutcome, ProxyRequest
from mb_commerce_scraper.testing import fake_proxy_pool
from mb_commerce_scraper.transports import RotationReason


async def test_static_pool_round_robin_rotation_and_idempotent_release() -> None:
    pool = fake_proxy_pool("one", "two")
    request = ProxyRequest(source_id="shop", target_host="shop.test", country="FR")
    first = await pool.acquire(request)
    assert first.provider == "one"
    second = await pool.rotate(first, RotationReason.BLOCKED)
    assert second.provider == "two"
    await pool.release(second)
    await pool.release(second)


async def test_proxy_accounting_fails_on_exhaustion_and_credentials_are_redacted() -> None:
    pool = fake_proxy_pool("one")
    lease = await pool.acquire(ProxyRequest(source_id="shop", target_host="shop.test", maximum_bytes=10))
    dumped = lease.http_credentials().model_dump_json()
    assert "one-user" not in dumped and "one-password" not in dumped
    with pytest.raises(RuntimeError, match="byte limit"):
        await pool.report(lease, ProxyOutcome(target_host="shop.test", transmitted_bytes=5, received_bytes=6, classification="success"))

