from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError, dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import pytest
from mb_commerce_scraper import ProxyMode, ProxyPolicyConfig, SourceDefinition
from mb_commerce_scraper.proxy import RoutingMode

from mb_ceramics_catalogue.ops import commerce_scraper_proxy_runtime as runtime
from mb_ceramics_catalogue.proxy import ProxyDenied, ProxyProfile


class Database:
    def __init__(self) -> None:
        self.connections = 0

    @asynccontextmanager
    async def connection(self):
        self.connections += 1
        raise AssertionError("resolving proxy policy must not access PostgreSQL")
        yield object()  # pragma: no cover


@dataclass
class Settings:
    proxy_enabled: bool = True
    proxy_secret_file: Path | None = Path("/mounted/proxy.json")


def snapshot(**changes: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "policy": "fallback",
        "provider": "decodo",
        "profile_id": str(uuid4()),
        "route_id": str(uuid4()),
        "profile": "primary",
        "protocol": "http",
        "country": "FR",
        "state": None,
        "city": None,
        "session_mode": "sticky",
        "session_minutes": 30,
        "max_bytes": 5_000_000,
        "pilot": False,
    }
    value.update(changes)
    return value


def resolve(
    database: Database,
    proxy_snapshot: dict[str, Any],
    *,
    settings: Settings | None = None,
    run_proxy_policy: Literal["never"] | None = None,
    run_proxy_max_megabytes: int | None = None,
    proxy_eligible: bool = True,
    base_url: str = "https://shop.test/",
) -> runtime.NativeProxyRuntimeSpec | None:
    source = SourceDefinition(
        id="shop",
        label="Shop",
        base_url=base_url,
        connector="shopify",
    )
    source_policy = ProxyPolicyConfig(
        mode=ProxyMode.FALLBACK if proxy_eligible else ProxyMode.NEVER,
    )
    return runtime.resolve_native_proxy_runtime(
        database,
        job_id=uuid4(),
        proxy_snapshot=proxy_snapshot,
        settings=settings or Settings(),
        run_proxy_policy=run_proxy_policy,
        run_proxy_max_megabytes=run_proxy_max_megabytes,
        source=source,
        source_policy=source_policy,
    )


@pytest.mark.parametrize(
    ("proxy_snapshot", "settings", "run_policy"),
    [
        ({"policy": "never"}, Settings(), None),
        (snapshot(), Settings(proxy_enabled=False), None),
        (snapshot(), Settings(), "never"),
    ],
)
def test_disabled_routes_do_not_read_secrets(
    monkeypatch: pytest.MonkeyPatch,
    proxy_snapshot: dict[str, Any],
    settings: Settings,
    run_policy: Literal["never"] | None,
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_profiles",
        lambda _path: (_ for _ in ()).throw(AssertionError("secret read")),
    )
    database = Database()

    assert resolve(
        database,
        proxy_snapshot,
        settings=settings,
        run_proxy_policy=run_policy,
    ) is None
    assert database.connections == 0


@pytest.mark.parametrize(
    ("policy", "mode"),
    [("fallback", RoutingMode.FALLBACK), ("always", RoutingMode.ALWAYS)],
)
def test_active_policy_constructs_a_lazy_frozen_runtime(
    monkeypatch: pytest.MonkeyPatch, policy: str, mode: RoutingMode
) -> None:
    profile = ProxyProfile("primary", "gate.test", 7000, "named-user", "secret")
    registered: list[set[str]] = []
    monkeypatch.setattr(runtime, "load_profiles", lambda _path: {"primary": profile})
    monkeypatch.setattr(runtime.obs, "register_secrets", lambda values: registered.append(values))
    database = Database()

    spec = resolve(database, snapshot(policy=policy))

    assert spec is not None
    assert spec.routing.mode is mode
    assert spec.routing.country == "FR"
    assert spec.routing.provider_preferences == ("decodo",)
    assert spec.maximum_requests is None
    assert spec.maximum_bytes == 5_000_000
    assert spec.source_id == "shop"
    assert spec.base_url == "https://shop.test/"
    assert registered and {"named-user", "secret"} <= registered[0]
    assert database.connections == 0
    with pytest.raises(FrozenInstanceError):
        spec.maximum_bytes = 1  # type: ignore[misc]


def test_run_byte_cap_can_only_narrow_operator_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_profiles",
        lambda _path: {
            "primary": ProxyProfile("primary", "gate.test", 7000, "user", "secret")
        },
    )
    monkeypatch.setattr(runtime.obs, "register_secrets", lambda _values: None)
    database = Database()

    narrowed = resolve(database, snapshot(), run_proxy_max_megabytes=3)
    unchanged = resolve(database, snapshot(max_bytes=2_000_000), run_proxy_max_megabytes=3)

    assert narrowed is not None and narrowed.maximum_bytes == 3_000_000
    assert unchanged is not None and unchanged.maximum_bytes == 2_000_000
    assert database.connections == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"provider": "iproyal"},
        {"provider": None},
        {"profile_id": "not-a-uuid"},
        {"profile_id": None},
        {"route_id": "00000000-0000-0000-0000-000000000000"},
        {"route_id": None},
        {"profile": ""},
        {"protocol": "ftp"},
        {"protocol": ["http"]},
        {"country": "fr"},
        {"state": "IDF"},
        {"city": "Paris"},
        {"session_mode": "random"},
        {"session_mode": None},
        {"session_minutes": 0},
        {"max_bytes": 0},
        {"pilot": "yes"},
    ],
)
def test_invalid_active_snapshot_fails_before_secret_or_database_access(
    monkeypatch: pytest.MonkeyPatch, changes: dict[str, Any]
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_profiles",
        lambda _path: (_ for _ in ()).throw(AssertionError("secret read")),
    )
    database = Database()

    with pytest.raises(ProxyDenied):
        resolve(database, snapshot(**changes))
    assert database.connections == 0


def test_noneligible_source_and_missing_secret_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_profiles",
        lambda _path: (_ for _ in ()).throw(AssertionError("secret read")),
    )
    database = Database()

    with pytest.raises(ProxyDenied, match="not checked-in"):
        resolve(database, snapshot(), proxy_eligible=False)
    with pytest.raises(ProxyDenied, match="secret file is absent"):
        resolve(database, snapshot(), settings=Settings(proxy_secret_file=None))


def test_source_url_with_userinfo_fails_before_secret_or_database_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_profiles",
        lambda _path: (_ for _ in ()).throw(AssertionError("secret read")),
    )
    database = Database()

    with pytest.raises(ProxyDenied, match="absolute HTTP source URL"):
        resolve(database, snapshot(), base_url="https://user:secret@shop.test/")
    assert database.connections == 0


def test_unknown_profile_registers_loaded_secrets_before_safe_denial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registered: list[set[str]] = []
    monkeypatch.setattr(
        runtime,
        "load_profiles",
        lambda _path: {
            "other": ProxyProfile("other", "gate.test", 7000, "sensitive-user", "sensitive-pass")
        },
    )
    monkeypatch.setattr(runtime.obs, "register_secrets", lambda values: registered.append(values))

    with pytest.raises(ProxyDenied, match="unknown logical proxy profile 'primary'"):
        resolve(Database(), snapshot())
    assert registered and {"sensitive-user", "sensitive-pass"} <= registered[0]
