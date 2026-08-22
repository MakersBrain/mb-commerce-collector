from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError, dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import pytest
from mb_commerce_scraper import ProxyMode, ProxyPolicyConfig, SourceDefinition
from mb_commerce_scraper.proxy import RoutingMode
from pydantic import SecretStr

from mb_ceramics_catalogue.ops import commerce_scraper_proxy_runtime as runtime
from mb_ceramics_catalogue.proxy import ProxyDenied, ProxyProfile
from mb_ceramics_catalogue.webshare_gateway_secrets import WebshareGatewaySecret


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
    proxy_webshare_data_plane_enabled: bool = False
    proxy_webshare_gateway_secret_file: Path | None = Path(
        "/mounted/webshare-gateway.json"
    )


def snapshot(**changes: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "policy": "fallback",
        "provider": "decodo",
        "profile_id": str(uuid4()),
        "route_id": str(uuid4()),
        "profile": "primary",
        "secret_generation": 0,
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


def webshare_secret(**changes: Any) -> WebshareGatewaySecret:
    values: dict[str, Any] = {
        "provider": "webshare",
        "logical_name": "primary",
        "generation": 7,
        "endpoint_id": "webshare-residential-backbone",
        "protocol": "http",
        "host": "p.webshare.io",
        "port": 80,
        "username": SecretStr("issued-user"),
        "password": SecretStr("issued-password"),
        "countries": frozenset({"FR", "US"}),
        "sticky_session_ttl_seconds": 1_800,
    }
    values.update(changes)
    return WebshareGatewaySecret(**values)


def resolve(
    database: Database,
    proxy_snapshot: dict[str, Any],
    *,
    settings: Settings | None = None,
    run_proxy_policy: Literal["never"] | None = None,
    run_proxy_max_megabytes: int | None = None,
    proxy_eligible: bool = True,
    base_url: str = "https://shop.test/",
    source_country: str | None = None,
    source_providers: tuple[str, ...] = (),
    source_maximum_requests: int | None = None,
    source_maximum_bytes: int | None = None,
) -> runtime.NativeProxyRuntimeSpec | None:
    source = SourceDefinition(
        id="shop",
        label="Shop",
        base_url=base_url,
        connector="shopify",
    )
    source_policy = ProxyPolicyConfig(
        mode=ProxyMode.FALLBACK if proxy_eligible else ProxyMode.NEVER,
        country=source_country,
        provider_preferences=source_providers,
        maximum_requests=source_maximum_requests,
        maximum_bytes=source_maximum_bytes,
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

    assert (
        resolve(
            database,
            proxy_snapshot,
            settings=settings,
            run_proxy_policy=run_policy,
        )
        is None
    )
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
    assert spec.policy.mode.value == mode.value
    assert spec.policy.country == "FR"
    assert spec.policy.provider_preferences == ("decodo",)
    assert spec.policy.maximum_requests is None
    assert spec.policy.maximum_bytes == 5_000_000
    assert spec.source_id == "shop"
    assert spec.base_url == "https://shop.test/"
    assert registered and {"named-user", "secret"} <= registered[0]
    assert database.connections == 0
    with pytest.raises(FrozenInstanceError):
        spec.policy = ProxyPolicyConfig()  # type: ignore[misc]


def test_webshare_is_default_off_before_secret_or_database_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_webshare_gateway_secrets",
        lambda _path: (_ for _ in ()).throw(AssertionError("secret read")),
    )
    database = Database()

    with pytest.raises(ProxyDenied, match="not enabled"):
        resolve(database, snapshot(provider="webshare", secret_generation=7))

    assert database.connections == 0


def test_enabled_webshare_constructs_one_lazy_durable_snapshotted_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected_route = uuid4()
    selected_profile = uuid4()
    secret = webshare_secret()
    registered: list[set[str]] = []
    monkeypatch.setattr(
        runtime,
        "load_profiles",
        lambda _path: (_ for _ in ()).throw(AssertionError("Decodo secret read")),
    )
    monkeypatch.setattr(
        runtime,
        "load_webshare_gateway_secrets",
        lambda path: {
            ("webshare", "primary"): secret
            if path == Path("/mounted/webshare-gateway.json")
            else pytest.fail("wrong provider secret path")
        },
    )
    monkeypatch.setattr(runtime.obs, "register_secrets", lambda values: registered.append(values))
    database = Database()

    spec = resolve(
        database,
        snapshot(
            provider="webshare",
            profile_id=str(selected_profile),
            route_id=str(selected_route),
            secret_generation=7,
        ),
        settings=Settings(proxy_webshare_data_plane_enabled=True),
        source_country="FR",
        source_providers=("decodo", "webshare"),
        source_maximum_requests=4,
    )

    assert spec is not None
    assert isinstance(spec.pool, runtime.PostgresReservedProxyPool)
    assert spec.policy == ProxyPolicyConfig(
        mode=ProxyMode.FALLBACK,
        country="FR",
        provider_preferences=("webshare",),
        maximum_requests=4,
        maximum_bytes=5_000_000,
    )
    assert spec.pool._identity == runtime.DurableProxyIdentity(
        provider="webshare",
        profile="primary",
        profile_id=selected_profile,
        route_id=selected_route,
        secret_generation=7,
    )
    assert isinstance(spec.pool._inner, runtime.WebshareGatewayPool)
    assert spec.pool._inner._config.endpoint_id == str(selected_route)
    assert spec.pool._inner._config.host == "p.webshare.io"
    assert registered == [{"issued-user", "issued-password"}]
    assert database.connections == 0


def test_enabled_webshare_requires_its_separate_gateway_secret_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_profiles",
        lambda _path: (_ for _ in ()).throw(AssertionError("Decodo secret read")),
    )
    monkeypatch.setattr(
        runtime,
        "load_webshare_gateway_secrets",
        lambda _path: (_ for _ in ()).throw(AssertionError("Webshare secret read")),
    )
    database = Database()

    with pytest.raises(ProxyDenied, match="secret file is absent"):
        resolve(
            database,
            snapshot(provider="webshare", secret_generation=7),
            settings=Settings(
                proxy_webshare_data_plane_enabled=True,
                proxy_webshare_gateway_secret_file=None,
            ),
        )

    assert database.connections == 0


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"protocol": "https"}, "verified HTTP"),
        ({"state": "IDF"}, "state or city"),
        ({"city": "Paris"}, "state or city"),
        ({"session_mode": "rotate"}, "sticky session"),
    ],
)
def test_webshare_snapshot_capabilities_fail_before_secret_access(
    monkeypatch: pytest.MonkeyPatch,
    changes: dict[str, Any],
    message: str,
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_webshare_gateway_secrets",
        lambda _path: (_ for _ in ()).throw(AssertionError("secret read")),
    )
    database = Database()

    with pytest.raises(ProxyDenied, match=message):
        resolve(
            database,
            snapshot(provider="webshare", **{"secret_generation": 7, **changes}),
            settings=Settings(proxy_webshare_data_plane_enabled=True),
        )

    assert database.connections == 0


@pytest.mark.parametrize(
    ("changes", "secret", "message"),
    [
        ({"session_minutes": 31}, webshare_secret(), "verified gateway duration"),
        ({"country": "DE"}, webshare_secret(), "not supported"),
        ({"secret_generation": 6}, webshare_secret(), "immutable job snapshot"),
        (
            {},
            webshare_secret(provider="decodo"),
            "identity does not match",
        ),
        (
            {},
            webshare_secret(logical_name="other"),
            "identity does not match",
        ),
    ],
)
def test_webshare_secret_must_match_snapshot_and_route_capabilities(
    monkeypatch: pytest.MonkeyPatch,
    changes: dict[str, Any],
    secret: WebshareGatewaySecret,
    message: str,
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_webshare_gateway_secrets",
        lambda _path: {("webshare", "primary"): secret},
    )
    monkeypatch.setattr(runtime.obs, "register_secrets", lambda _values: None)
    database = Database()

    with pytest.raises(ProxyDenied, match=message):
        resolve(
            database,
            snapshot(provider="webshare", **{"secret_generation": 7, **changes}),
            settings=Settings(proxy_webshare_data_plane_enabled=True),
        )

    assert database.connections == 0


def test_unknown_webshare_profile_registers_all_loaded_secrets_before_denial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = webshare_secret(logical_name="other")
    registered: list[set[str]] = []
    monkeypatch.setattr(
        runtime,
        "load_webshare_gateway_secrets",
        lambda _path: {("webshare", "other"): loaded},
    )
    monkeypatch.setattr(runtime.obs, "register_secrets", lambda values: registered.append(values))

    with pytest.raises(ProxyDenied, match="unknown logical Webshare"):
        resolve(
            Database(),
            snapshot(provider="webshare", secret_generation=7),
            settings=Settings(proxy_webshare_data_plane_enabled=True),
        )

    assert registered == [{"issued-user", "issued-password"}]


@pytest.mark.parametrize(
    "source_constraints",
    [
        {"source_country": "US"},
        {"source_providers": ("decodo",)},
    ],
)
def test_webshare_cannot_broaden_source_constraints_before_secret_access(
    monkeypatch: pytest.MonkeyPatch,
    source_constraints: dict[str, Any],
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_webshare_gateway_secrets",
        lambda _path: (_ for _ in ()).throw(AssertionError("secret read")),
    )
    database = Database()

    with pytest.raises(ProxyDenied, match="outside source policy"):
        resolve(
            database,
            snapshot(provider="webshare", secret_generation=7),
            settings=Settings(proxy_webshare_data_plane_enabled=True),
            **source_constraints,
        )

    assert database.connections == 0


def test_run_byte_cap_can_only_narrow_operator_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_profiles",
        lambda _path: {"primary": ProxyProfile("primary", "gate.test", 7000, "user", "secret")},
    )
    monkeypatch.setattr(runtime.obs, "register_secrets", lambda _values: None)
    database = Database()

    narrowed = resolve(database, snapshot(), run_proxy_max_megabytes=3)
    unchanged = resolve(database, snapshot(max_bytes=2_000_000), run_proxy_max_megabytes=3)

    assert narrowed is not None and narrowed.policy.maximum_bytes == 3_000_000
    assert unchanged is not None and unchanged.policy.maximum_bytes == 2_000_000
    assert database.connections == 0


def test_rotated_secret_cannot_replace_the_generation_snapshotted_for_a_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_profiles",
        lambda _path: {
            "primary": ProxyProfile(
                "primary",
                "gate.test",
                7000,
                "user",
                "rotated-secret",
                generation=2,
            )
        },
    )
    monkeypatch.setattr(runtime.obs, "register_secrets", lambda _values: None)
    database = Database()

    with pytest.raises(ProxyDenied, match="immutable job snapshot"):
        resolve(database, snapshot(secret_generation=1))

    assert database.connections == 0


def test_source_policy_constraints_narrow_effective_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_profiles",
        lambda _path: {"primary": ProxyProfile("primary", "gate.test", 7000, "user", "secret")},
    )
    monkeypatch.setattr(runtime.obs, "register_secrets", lambda _values: None)

    spec = resolve(
        Database(),
        snapshot(max_bytes=5_000_000),
        source_country="FR",
        source_providers=("decodo", "backup"),
        source_maximum_requests=9,
        source_maximum_bytes=2_000_000,
    )

    assert spec is not None
    assert spec.policy == ProxyPolicyConfig(
        mode=ProxyMode.FALLBACK,
        country="FR",
        provider_preferences=("decodo",),
        maximum_requests=9,
        maximum_bytes=2_000_000,
    )


@pytest.mark.parametrize(
    "source_constraints",
    [
        {"source_country": "US"},
        {"source_providers": ("backup",)},
    ],
)
def test_snapshot_cannot_broaden_source_route_constraints_before_secret_access(
    monkeypatch: pytest.MonkeyPatch,
    source_constraints: dict[str, Any],
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_profiles",
        lambda _path: (_ for _ in ()).throw(AssertionError("secret read")),
    )
    database = Database()

    with pytest.raises(ProxyDenied, match="outside source policy"):
        resolve(database, snapshot(), **source_constraints)
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
        lambda _path: {"other": ProxyProfile("other", "gate.test", 7000, "sensitive-user", "sensitive-pass")},
    )
    monkeypatch.setattr(runtime.obs, "register_secrets", lambda values: registered.append(values))

    with pytest.raises(ProxyDenied, match="unknown logical proxy profile 'primary'"):
        resolve(Database(), snapshot())
    assert registered and {"sensitive-user", "sensitive-pass"} <= registered[0]
