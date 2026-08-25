"""Where the control service runs, from the environment."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CATALOGUE_", env_file=".env", extra="ignore")

    dsn: str = ""
    #: Required on every `/v1` route including the stream. `/health` and
    #: `/metrics` are the only exemptions.
    #:
    #: The service is additionally not published on the host, which is defence
    #: in depth rather than its authentication boundary — an unauthenticated
    #: service reachable from one network is still an unauthenticated service.
    control_token: str = ""
    host: str = "0.0.0.0"
    port: int = 8687
    log_level: str = "INFO"
    log_json: bool | None = None
    artifacts_dir: Path = Path("/var/lib/catalogue/dumps")
    #: Read-only queue inspection for the operator UI. Queue availability must
    #: not gate the control service: when NATS is down, operators still need the
    #: page that explains why work is not moving.
    nats_url: str = "nats://127.0.0.1:4222"
    nats_stream: str = "CATALOGUE_JOBS"
    nats_stats_token_file: Path | None = None
    queue_provider: Literal["nats", "cloudflare"] = "nats"
    queue_snapshot_cache_seconds: float = 5.0
    queue_snapshot_timeout_seconds: float = 3.0
    cf_account_id: str = ""
    cf_stats_token_file: Path | None = None
    cf_queue_plain_id: str = ""
    cf_queue_browser_auto_id: str = ""
    cf_queue_browser_camoufox_id: str = ""
    cf_queue_browser_cdp_extension_proxy_id: str = ""
    cf_queue_recovery_dlq_id: str = ""
    cf_api_base_url: str = "https://api.cloudflare.com/client/v4"
    proxy_enabled: bool = False
    proxy_api_secret_file: Path | None = None
    proxy_secret_file: Path | None = None
    #: Control-owned writable gateway store. Workers receive the containing
    #: directory through a separate read-only mount so atomic replacements are
    #: visible without sharing provider-management credentials.
    proxy_webshare_gateway_secret_file: Path | None = None
    proxy_actor_public_keys_file: Path | None = None
    proxy_provider_limit_unit: Literal["unconfirmed", "decimal_gb"] = "unconfirmed"
    proxy_provider_base_url: str = "https://api.decodo.com"
    proxy_reconcile_interval_seconds: float = 3600
    proxy_mutations_enabled: bool = False
    proxy_paid_probe_enabled: bool = False

    # ---- multi-provider ----------------------------------------------------
    #
    # The five settings above describe one provider and are kept as the
    # configuration for whichever provider `proxy_default_provider` names, so an
    # existing deployment keeps working untouched. The rest add the others.

    #: Comma-separated provider names to construct at startup, e.g.
    #: "decodo,iproyal". Every name must exist in the provider registry; an
    #: unknown one fails startup rather than being skipped, because a silently
    #: absent provider looks identical to one whose credential failed to load.
    proxy_providers: str = "decodo"

    #: The provider used when a request does not name one. Also the provider the
    #: single-provider settings above configure.
    proxy_default_provider: str = "decodo"

    #: Per-provider API credential files, as JSON: {"iproyal": "/run/secrets/..."}.
    #: The default provider falls back to `proxy_api_secret_file`, so this only
    #: has to name the additional ones.
    proxy_provider_secret_files: dict[str, Path] = {}

    #: Per-provider API base URLs, as JSON. Omitted providers use the registry's
    #: default for that provider.
    proxy_provider_base_urls: dict[str, str] = {}

    #: Per-provider paid IP-check endpoints, as JSON. A provider with no probe
    #: URL -- in the registry or here -- refuses to probe rather than guessing an
    #: endpoint, because a probe spends real traffic.
    proxy_provider_probe_urls: dict[str, str] = {}

    #: IPRoyal only: whether a PUT carrying `traffic` sets the balance or adds
    #: to it. Left unconfirmed, traffic writes refuse. See providers/iproyal.py.
    proxy_iproyal_traffic_writes: Literal["unconfirmed", "absolute"] = "unconfirmed"

    #: ProxyScrape only: the sub-account UUID that scopes every residential
    #: path. Without it the adapter refuses before making any request.
    proxy_proxyscrape_sub_account_id: str = ""

    def enabled_providers(self) -> list[str]:
        names = [name.strip() for name in self.proxy_providers.split(",") if name.strip()]
        # The default must be constructible, or a request that names no provider
        # has nowhere to go.
        if self.proxy_default_provider not in names:
            names.insert(0, self.proxy_default_provider)
        return list(dict.fromkeys(names))

    #: Refuse to start without a token rather than serving an open control
    #: plane. The one thing worse than no run-cancel endpoint is an
    #: unauthenticated one.
    require_token: bool = True
