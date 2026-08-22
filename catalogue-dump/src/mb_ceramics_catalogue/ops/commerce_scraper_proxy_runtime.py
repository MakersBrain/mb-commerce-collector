"""Resolve snapshotted catalogue policy into a lazy neutral proxy runtime."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from urllib.parse import urlsplit
from uuid import UUID

from mb_commerce_scraper import ProxyMode, ProxyPolicyConfig, SourceDefinition

from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.proxy import (
    ProxyDenied,
    load_profiles,
    secret_values,
)

from .commerce_scraper_proxy import ConnectionPool, PostgresDecodoProxyPool

_COUNTRY = re.compile(r"^[A-Z]{2}$")
_PROVIDER = "decodo"


class ProxyRuntimeSettings(Protocol):
    proxy_enabled: bool
    proxy_secret_file: Path | None


@dataclass(frozen=True, slots=True)
class NativeProxyRuntimeSpec:
    source_id: str
    base_url: str
    pool: PostgresDecodoProxyPool
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
    """Validate immutable job policy and construct a non-acquired Decodo pool.

    Operator policy may enable a route; ordinary run parameters can only turn
    it off or lower its byte allowance. Returning a spec performs no database
    operation and therefore cannot reserve paid traffic.
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
    if proxy_snapshot.get("provider") != _PROVIDER:
        raise ProxyDenied("native proxy runtime supports snapshotted Decodo routes only")
    if proxy_snapshot.get("state") is not None or proxy_snapshot.get("city") is not None:
        raise ProxyDenied("native Decodo routing does not support state or city selection")
    if proxy_snapshot.get("session_mode") != "sticky":
        raise ProxyDenied("native Decodo routing requires a sticky session mode")

    profile_id = _uuid(proxy_snapshot, "profile_id")
    route_id = _uuid(proxy_snapshot, "route_id")
    logical_name = _required_text(proxy_snapshot, "profile")
    protocol = _protocol(proxy_snapshot)
    country = _country(proxy_snapshot)
    if source_policy.country is not None and country != source_policy.country:
        raise ProxyDenied("snapshotted proxy country is outside source policy")
    if source_policy.provider_preferences and _PROVIDER not in source_policy.provider_preferences:
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

    secret_file = settings.proxy_secret_file
    if secret_file is None:
        raise ProxyDenied("proxy is enabled but its secret file is absent")
    profiles = load_profiles(secret_file)
    obs.register_secrets(secret_values(profiles))
    profile = profiles.get(logical_name)
    if profile is None:
        raise ProxyDenied(f"unknown logical proxy profile {logical_name!r}")

    effective_policy = ProxyPolicyConfig(
        mode=ProxyMode(policy),
        country=country,
        provider_preferences=(_PROVIDER,),
        maximum_requests=source_policy.maximum_requests,
        maximum_bytes=configured_maximum,
    )
    pool = PostgresDecodoProxyPool(
        database,
        job_id=job_id,
        profile=profile,
        profile_id=profile_id,
        route_id=route_id,
        maximum_bytes=configured_maximum,
        route_country=country,
        protocol=protocol,
        session_minutes=session_minutes,
        pilot=pilot,
    )
    return NativeProxyRuntimeSpec(
        source_id=source.id,
        base_url=source.base_url,
        pool=pool,
        policy=effective_policy,
    )


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
