"""Resolve snapshotted catalogue policy into a lazy neutral proxy runtime."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from urllib.parse import urlsplit
from uuid import UUID

from mb_commerce_scraper import ProxyMode, ProxyPolicyConfig, SourceDefinition
from mb_commerce_scraper.proxy import ProxyPool

from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.proxy import (
    ProxyDenied,
    load_profiles,
)
from mb_ceramics_catalogue.proxy import secret_values as decodo_secret_values
from mb_ceramics_catalogue.webshare_gateway_secrets import (
    load_webshare_gateway_secrets,
)
from mb_ceramics_catalogue.webshare_gateway_secrets import (
    secret_values as webshare_secret_values,
)

from .commerce_scraper_decodo import DecodoDataPlaneConfig, DecodoDataPlanePool
from .commerce_scraper_proxy import (
    ConnectionPool,
    DurableProxyIdentity,
    PostgresReservedProxyPool,
)
from .commerce_scraper_webshare import WebshareGatewayConfig, WebshareGatewayPool

_COUNTRY = re.compile(r"^[A-Z]{2}$")
_DECODO = "decodo"
_WEBSHARE = "webshare"


class ProxyRuntimeSettings(Protocol):
    proxy_enabled: bool
    proxy_secret_file: Path | None
    proxy_webshare_data_plane_enabled: bool
    proxy_webshare_gateway_secret_file: Path | None


@dataclass(frozen=True, slots=True)
class _DataPlaneProvider:
    """One provider's data-plane entry: is it permitted, and how is it built.

    Provider knowledge lives here rather than in the resolver so that adding a
    third provider is a new registry entry, not another branch threaded through
    policy validation, capability gating, and secret plumbing.  The control
    plane keeps the same shape in ``providers/registry.py``.
    """

    #: Operator gate beyond ``proxy_enabled``. A provider whose data plane is
    #: still being qualified stays off even when a job snapshot names it.
    enabled: Callable[[ProxyRuntimeSettings], bool]
    denied_message: str
    build: Callable[..., PostgresReservedProxyPool]



@dataclass(frozen=True, slots=True)
class NativeProxyRuntimeSpec:
    source_id: str
    base_url: str
    pool: ProxyPool
    policy: ProxyPolicyConfig


def resolve_native_proxy_runtime(
    database: ConnectionPool,
    *,
    job_id: UUID,
    proxy_snapshot: Mapping[str, Any],
    settings: ProxyRuntimeSettings,
    run_proxy_policy: Literal["never"] | None,
    run_proxy_max_megabytes: int | None,
    source: SourceDefinition,
    source_policy: ProxyPolicyConfig,
) -> NativeProxyRuntimeSpec | None:
    """Validate immutable job policy and construct one non-acquired provider pool.

    Operator policy may enable a route; ordinary run parameters can only turn
    it off or lower its byte allowance. Returning a spec performs no database
    operation and therefore cannot reserve paid traffic. A job names exactly
    one immutable route: this resolver does not discover or compose fallback
    providers at execution time.
    """
    policy = str(proxy_snapshot.get("policy", "never"))
    if policy == "never" or run_proxy_policy == "never" or not settings.proxy_enabled:
        return None
    if run_proxy_policy is not None:
        raise ProxyDenied("ordinary run policy may only disable proxy routing")
    if policy not in {"fallback", "always"}:
        raise ProxyDenied(f"unsupported snapshotted proxy policy {policy!r}")
    if source_policy.mode is ProxyMode.NEVER:
        raise ProxyDenied(f"source {source.id!r} is not checked-in as proxy eligible")
    _validate_source(source.id, source.base_url)
    provider = proxy_snapshot.get("provider")
    if not isinstance(provider, str) or provider not in _PROVIDERS:
        raise ProxyDenied("native proxy runtime does not support the snapshotted provider")
    selected_provider = _PROVIDERS[provider]
    if not selected_provider.enabled(settings):
        raise ProxyDenied(selected_provider.denied_message)
    if proxy_snapshot.get("state") is not None or proxy_snapshot.get("city") is not None:
        raise ProxyDenied("native proxy routing does not support state or city selection")
    if proxy_snapshot.get("session_mode") != "sticky":
        raise ProxyDenied("native proxy routing requires a sticky session mode")

    profile_id = _uuid(proxy_snapshot, "profile_id")
    route_id = _uuid(proxy_snapshot, "route_id")
    logical_name = _required_text(proxy_snapshot, "profile")
    protocol = _protocol(proxy_snapshot)
    country = _country(proxy_snapshot)
    if source_policy.country is not None and country != source_policy.country:
        raise ProxyDenied("snapshotted proxy country is outside source policy")
    if source_policy.provider_preferences and provider not in source_policy.provider_preferences:
        raise ProxyDenied("snapshotted proxy provider is outside source policy")
    session_minutes = _bounded_int(proxy_snapshot, "session_minutes", minimum=1, maximum=1_440)
    configured_maximum = _bounded_int(proxy_snapshot, "max_bytes", minimum=1, maximum=25_000_000)
    if source_policy.maximum_bytes is not None:
        configured_maximum = min(configured_maximum, source_policy.maximum_bytes)
    if run_proxy_max_megabytes is not None:
        if (
            isinstance(run_proxy_max_megabytes, bool)
            or not isinstance(run_proxy_max_megabytes, int)
            or not 1 <= run_proxy_max_megabytes <= 25
        ):
            raise ProxyDenied("run proxy byte maximum must be between 1 and 25 MB")
        configured_maximum = min(configured_maximum, run_proxy_max_megabytes * 1_000_000)
    pilot = proxy_snapshot.get("pilot", False)
    if not isinstance(pilot, bool):
        raise ProxyDenied("snapshotted proxy pilot flag must be boolean")
    snapshotted_generation = _bounded_int(
        proxy_snapshot,
        "secret_generation",
        minimum=0,
        maximum=2_147_483_647,
    )

    pool: ProxyPool = selected_provider.build(
        database,
        settings=settings,
        job_id=job_id,
        logical_name=logical_name,
        profile_id=profile_id,
        route_id=route_id,
        secret_generation=snapshotted_generation,
        maximum_bytes=configured_maximum,
        country=country,
        protocol=protocol,
        session_minutes=session_minutes,
        pilot=pilot,
    )

    effective_policy = ProxyPolicyConfig(
        mode=ProxyMode(policy),
        country=country,
        provider_preferences=(provider,),
        maximum_requests=source_policy.maximum_requests,
        maximum_bytes=configured_maximum,
    )
    return NativeProxyRuntimeSpec(
        source_id=source.id,
        base_url=source.base_url,
        pool=pool,
        policy=effective_policy,
    )


def _decodo_pool(
    database: ConnectionPool,
    *,
    settings: ProxyRuntimeSettings,
    job_id: UUID,
    logical_name: str,
    profile_id: UUID,
    route_id: UUID,
    secret_generation: int,
    maximum_bytes: int,
    country: str | None,
    protocol: Literal["http", "https", "socks5"],
    session_minutes: int,
    pilot: bool,
) -> PostgresReservedProxyPool:
    secret_file = settings.proxy_secret_file
    if secret_file is None:
        raise ProxyDenied("proxy is enabled but its secret file is absent")
    profiles = load_profiles(secret_file)
    obs.register_secrets(decodo_secret_values(profiles))
    profile = profiles.get(logical_name)
    if profile is None:
        raise ProxyDenied(f"unknown logical proxy profile {logical_name!r}")
    if profile.generation != secret_generation:
        raise ProxyDenied("proxy secret generation does not match the immutable job snapshot")

    data_plane = DecodoDataPlanePool(
        DecodoDataPlaneConfig(
            profile=profile,
            endpoint_id=str(route_id),
            country=country,
            protocol=protocol,
            session_minutes=session_minutes,
        )
    )
    return PostgresReservedProxyPool(
        database,
        data_plane,
        job_id=job_id,
        identity=DurableProxyIdentity(
            provider=_DECODO,
            profile=logical_name,
            profile_id=profile_id,
            route_id=route_id,
            secret_generation=profile.generation,
        ),
        maximum_bytes=maximum_bytes,
        pilot=pilot,
    )


def _webshare_pool(
    database: ConnectionPool,
    *,
    settings: ProxyRuntimeSettings,
    job_id: UUID,
    logical_name: str,
    profile_id: UUID,
    route_id: UUID,
    secret_generation: int,
    maximum_bytes: int,
    country: str | None,
    protocol: Literal["http", "https", "socks5"],
    session_minutes: int,
    pilot: bool,
) -> PostgresReservedProxyPool:
    if protocol != "http":
        raise ProxyDenied("native Webshare routing requires the verified HTTP gateway")
    secret_file = settings.proxy_webshare_gateway_secret_file
    if secret_file is None:
        raise ProxyDenied("Webshare data-plane routing is enabled but its secret file is absent")
    profiles = load_webshare_gateway_secrets(secret_file)
    obs.register_secrets(webshare_secret_values(profiles))
    secret = profiles.get((_WEBSHARE, logical_name))
    if secret is None:
        raise ProxyDenied(f"unknown logical Webshare gateway profile {logical_name!r}")
    if secret.provider != _WEBSHARE or secret.logical_name != logical_name:
        raise ProxyDenied("Webshare gateway secret identity does not match the job snapshot")
    if secret.generation != secret_generation:
        raise ProxyDenied("proxy secret generation does not match the immutable job snapshot")
    if session_minutes * 60 > secret.sticky_session_ttl_seconds:
        raise ProxyDenied("snapshotted Webshare session exceeds the verified gateway duration")
    if country is not None and country not in secret.countries:
        raise ProxyDenied("snapshotted Webshare country is not supported by the gateway secret")

    gateway = WebshareGatewayPool(
        WebshareGatewayConfig(
            username=secret.username,
            password=secret.password,
            endpoint_id=str(route_id),
            host=secret.host,
            port=secret.port,
            countries=secret.countries,
            sticky_session_ttl_seconds=secret.sticky_session_ttl_seconds,
        )
    )
    return PostgresReservedProxyPool(
        database,
        gateway,
        job_id=job_id,
        identity=DurableProxyIdentity(
            provider=secret.provider,
            profile=secret.logical_name,
            profile_id=profile_id,
            route_id=route_id,
            secret_generation=secret.generation,
        ),
        maximum_bytes=maximum_bytes,
        pilot=pilot,
    )



def _data_plane_providers() -> dict[str, _DataPlaneProvider]:
    return {
        _DECODO: _DataPlaneProvider(
            enabled=lambda settings: True,
            denied_message="native Decodo data-plane routing is not enabled",
            build=_decodo_pool,
        ),
        _WEBSHARE: _DataPlaneProvider(
            enabled=lambda settings: settings.proxy_webshare_data_plane_enabled,
            denied_message="native Webshare data-plane routing is not enabled",
            build=_webshare_pool,
        ),
    }


_PROVIDERS = _data_plane_providers()


def _validate_source(source_id: str, base_url: str) -> None:
    if not source_id:
        raise ProxyDenied("native proxy runtime requires a source identity")
    parsed = urlsplit(base_url)
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ProxyDenied("native proxy runtime requires an absolute HTTP source URL")


def _required_text(snapshot: Mapping[str, Any], name: str) -> str:
    value = snapshot.get(name)
    if not isinstance(value, str) or not value:
        raise ProxyDenied(f"job proxy snapshot has no valid {name}")
    return value


def _uuid(snapshot: Mapping[str, Any], name: str) -> UUID:
    value = _required_text(snapshot, name)
    try:
        parsed = UUID(value)
    except ValueError:
        raise ProxyDenied(f"job proxy snapshot has no valid {name}") from None
    if parsed.int == 0:
        raise ProxyDenied(f"job proxy snapshot has no valid {name}")
    return parsed


def _bounded_int(snapshot: Mapping[str, Any], name: str, *, minimum: int, maximum: int) -> int:
    value = snapshot.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ProxyDenied(f"job proxy snapshot has no valid {name}")
    return value


def _protocol(snapshot: Mapping[str, Any]) -> Literal["http", "https", "socks5"]:
    value = snapshot.get("protocol")
    if not isinstance(value, str) or value not in {"http", "https", "socks5"}:
        raise ProxyDenied("job proxy snapshot has no valid protocol")
    return cast(Literal["http", "https", "socks5"], value)


def _country(snapshot: Mapping[str, Any]) -> str | None:
    value = snapshot.get("country")
    if value is None:
        return None
    if not isinstance(value, str) or _COUNTRY.fullmatch(value) is None:
        raise ProxyDenied("job proxy snapshot has no valid country")
    return value
