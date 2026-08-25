"""Run parameters and process settings, as two models rather than a Namespace.

The split matters. `CrawlParams` is *what to do* — the options a run carries,
which arrive from an `argparse.Namespace` on the command line and from a job's
`params` jsonb in the database, and must mean the same thing either way.
`Settings` is *where the process lives* — the DSN, the cache directory, the
control token. One comes from an operator's request, the other from the
deployment, and conflating them is how a run ends up able to change the database
it writes to.

`CrawlParams` is also what generates the request schema for `POST /v1/runs`
(§6), so the CLI, the API and the scheduler cannot disagree about what a valid
run is.
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BrowserPolicy = Literal["never", "auto", "always"]
#: When to fall back to a browser TLS handshake for a host that refuses ours.
#: "auto" is the last rung of the fallback ladder in `Fetcher.response`;
#: "never" leaves a refusal as a refusal.
ImpersonatePolicy = Literal["never", "auto"]
#: Whether robots.txt Disallow is treated as binding.
#:
#: "ignore" is the default, and it is a deliberate policy rather than an
#: oversight. What replaces it is pace: the limiter reads a host's own
#: `X-RateLimit-*` accounting and its `Retry-After`, halves its slots and opens
#: a gap on any error, and adopts a published `Crawl-delay` as that gap. robots
#: is still fetched under either setting, because its `Crawl-delay` and
#: `Sitemap` lines are the two most useful things in it.
RobotsPolicy = Literal["obey", "ignore"]
CacheMode = Literal["off", "auto", "replay", "refresh"]
RefreshMode = Literal["price", "full"]
PipelineMode = Literal["legacy", "connector_canary"]
DatasetSelection = Literal[
    "ceramics",
    "ceramics.catalogue_item.v2",
    "ceramics.catalogue_identity.v2",
    "commerce.price_observation.v1",
    "commerce.stock_observation.v1",
    "commerce.document.v1",
]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]

#: Seconds one source may run before the crawl gives up on it. Generous, because
#: a large storefront legitimately takes an hour; finite, because before this
#: there was no deadline at all and a hung source held its slot until someone
#: noticed — which, on a 03:00 schedule, is the morning.
DEFAULT_SOURCE_TIMEOUT = 3600.0


class CrawlParams(BaseModel):
    """Everything that decides how a run collects. Validated once, used twice."""

    model_config = ConfigDict(extra="forbid")

    #: Maximum products per source. A run that hits this is `truncated`, and a
    #: truncated run is never grounds for retiring a product (see `plan_load`).
    limit: int | None = Field(default=None, ge=1)
    #: Sources crawled at once.
    sources: int = Field(default=4, ge=1)
    #: Most requests in flight per host.
    concurrency: int = Field(default=8, ge=1)
    #: Seconds one crawling slot waits between its requests to a host.
    delay: float = Field(default=0.0, ge=0)
    browser: BrowserPolicy = "auto"
    impersonate: ImpersonatePolicy = "auto"
    robots: RobotsPolicy = "ignore"

    cache_mode: CacheMode = "auto"
    #: How old a stored response may be before `auto` refetches it. Zero means
    #: never stale.
    #:
    #: The default is deliberately **20 hours, not the 168 it used to be**. A
    #: daily price run under a seven-day max age replays yesterday's pages and
    #: reports success while changing no prices at all, which would make the
    #: whole schedule a no-op. Seven days is right for reworking a parser and
    #: wrong for the thing this pipeline exists to do (§8).
    cache_max_age_hours: float = Field(default=20.0, ge=0)
    #: Explicitly permit an expired cached GET response when every live attempt
    #: ends in a transient transport failure. Off by default: freshness must
    #: never be weakened silently.
    stale_on_error: bool = False
    #: Explicit migration selector. Legacy remains the default; the connector
    #: pipeline is enabled only on a deliberately requested canary run.
    pipeline: PipelineMode = "legacy"
    #: Versioned outputs requested from the connector pipeline. ``ceramics`` is
    #: the compatibility alias for the source's current catalogue/identity
    #: output, preserving the pre-migration default.
    datasets: tuple[DatasetSelection, ...] = ("ceramics",)
    #: Daily structured-API runs keep identity/offer fields; weekly runs own
    #: descriptive enrichment. The loader preserves enrichment on price rows.
    refresh_mode: RefreshMode = "full"
    #: Ordinary run requests may only narrow an operator-configured proxy
    #: policy or budget. They can never enable/select a paid route.
    proxy_policy: Literal["never"] | None = None
    proxy_max_megabytes: int | None = Field(default=None, ge=1, le=25)

    #: Per-source deadline; a source may lower it but not raise it.
    source_timeout_seconds: float = Field(default=DEFAULT_SOURCE_TIMEOUT, gt=0)

    log_level: LogLevel = "INFO"
    dry_run: bool = False
    #: Let an empty result replace an existing dump. Off, because an empty
    #: scrape is not a smaller catalogue.
    allow_empty: bool = False

    @property
    def cache_max_age_seconds(self) -> float | None:
        return (self.cache_max_age_hours * 3600) or None

    @model_validator(mode="after")
    def _replay_needs_no_browser(self) -> CrawlParams:
        """Replay must not be able to reach the network by another door.

        `--cache-mode replay` promises no requests. A browser render is a
        request that does not go through the response cache's HTTP path, so
        leaving the browser enabled would quietly turn an offline parser run
        into a live crawl of the two sources that need one.
        """
        if self.cache_mode == "replay" and self.browser == "always":
            raise ValueError("cache_mode=replay cannot be combined with browser=always")
        if self.pipeline == "legacy" and self.datasets != ("ceramics",):
            raise ValueError("versioned dataset selection requires pipeline=connector_canary")
        return self

    @field_validator("datasets")
    @classmethod
    def _datasets_are_nonempty_and_unique(
        cls, value: tuple[DatasetSelection, ...]
    ) -> tuple[DatasetSelection, ...]:
        if not value:
            raise ValueError("at least one dataset must be selected")
        if len(value) != len(set(value)):
            raise ValueError("datasets must not contain duplicates")
        if "ceramics" in value and any(name.startswith("ceramics.") for name in value):
            raise ValueError("ceramics alias cannot be combined with a versioned ceramics dataset")
        if sum(name.startswith("ceramics.") or name == "ceramics" for name in value) > 1:
            raise ValueError("at most one ceramics dataset may be selected")
        return value

    def timeout_for(self, source_timeout: float | None) -> float:
        """The deadline for one source: the stricter of its own and the run's."""
        if source_timeout is None:
            return self.source_timeout_seconds
        return min(source_timeout, self.source_timeout_seconds)

    @classmethod
    def from_namespace(cls, options: argparse.Namespace) -> CrawlParams:
        """Build from parsed command-line arguments.

        Named explicitly rather than by `vars(options)`: the parser also carries
        things that are not run parameters (where to write, which sources), and
        a model with `extra="forbid"` would reject them — correctly.
        """
        return cls(
            limit=options.limit,
            sources=options.sources,
            concurrency=options.concurrency,
            delay=options.delay,
            browser=options.browser,
            impersonate=options.impersonate,
            robots=options.robots,
            cache_mode=options.cache_mode if options.cache else "off",
            cache_max_age_hours=options.cache_max_age,
            stale_on_error=getattr(options, "stale_on_error", False),
            pipeline=getattr(options, "pipeline", "legacy"),
            datasets=tuple(getattr(options, "datasets", None) or ("ceramics",)),
            refresh_mode=getattr(options, "refresh_mode", "full"),
            source_timeout_seconds=options.source_timeout,
            log_level=options.log_level,
            dry_run=options.dry_run,
            allow_empty=options.allow_empty,
        )

    @classmethod
    def from_job(cls, params: dict[str, Any] | None) -> CrawlParams:
        """Build from a job's `params` jsonb, taking defaults for what it omits."""
        return cls.model_validate(params or {})


class Settings(BaseSettings):
    """Where this process runs, from the environment.

    Read once at startup. Nothing here is settable per run, which is the point:
    a run request arriving over HTTP cannot redirect the load to another
    database or move the artifact directory.
    """

    model_config = SettingsConfigDict(
        env_prefix="CATALOGUE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    #: libpq connection string for the catalogue database.
    dsn: str = ""
    #: One queue provider delivers work; PostgreSQL remains the state and
    #: fencing authority.
    queue_provider: Literal["nats", "cloudflare"] = "nats"
    queue_poll_empty_seconds: float = Field(default=2.0, gt=0)
    queue_visibility_seconds: int = Field(default=4200, gt=0, le=43_200)
    queue_pre_execution_wait_seconds: int = Field(default=60, ge=0)
    queue_finalization_seconds: int = Field(default=180, ge=0)
    queue_shutdown_ack_margin_seconds: int = Field(default=60, ge=0)
    nats_url: str = "nats://127.0.0.1:4222"
    nats_publish_token_file: Path | None = None
    nats_consume_token_file: Path | None = None
    nats_stats_token_file: Path | None = None
    nats_admin_token_file: Path | None = None
    nats_publish_credentials_file: Path | None = None
    nats_consume_credentials_file: Path | None = None
    nats_stats_credentials_file: Path | None = None
    nats_admin_credentials_file: Path | None = None
    nats_stream: str = "CATALOGUE_JOBS"
    cf_account_id: str = ""
    cf_publish_token_file: Path | None = None
    cf_consume_token_file: Path | None = None
    cf_recovery_token_file: Path | None = None
    cf_stats_token_file: Path | None = None
    cf_admin_token_file: Path | None = None
    cf_queue_plain_id: str = ""
    cf_queue_browser_auto_id: str = ""
    cf_queue_browser_camoufox_id: str = ""
    cf_queue_browser_cdp_extension_proxy_id: str = ""
    cf_queue_recovery_dlq_id: str = ""
    cf_max_retries: int = Field(default=100, ge=1, le=100)
    cf_api_base_url: str = "https://api.cloudflare.com/client/v4"
    #: Where recorded responses live. Shared between workers as a named volume,
    #: sharded per host so two of them never write one entry (§8).
    cache_dir: Path = Path(".cache")
    #: Where NDJSON artifacts are written, namespaced `<run-id>/<job-id>/`.
    dumps_dir: Path = Path("dumps")
    #: Identifies this worker's build in `catalogue.workers.version`.
    worker_capabilities: tuple[str, ...] = ()
    #: Exit cleanly after this many completed jobs, letting the restart policy
    #: start a fresh process. Zero means never.
    #:
    #: This exists for the browser worker. camoufox leaks across jobs, and a
    #: long-lived process that has rendered a few hundred pages is a process
    #: that will eventually be killed by the OOM reaper mid-write. Recycling on
    #: a count turns that into a scheduled, graceful restart between jobs.
    max_jobs: int = 0
    #: How many jobs one worker runs at once.
    #:
    #: Sources are independent — different shops, different hosts — and a worker
    #: doing one at a time spends most of a run waiting on someone else's TLS
    #: handshake. What stops two jobs pounding one shop is `catalogue.hosts` and
    #: its leases, which are per host and cross-process, so they are just as
    #: effective within a process as between two of them.
    #:
    #: The browser worker used to be the reason this defaults to 1 rather than
    #: to something ambitious: each concurrent job there was another camoufox,
    #: and that is measured in hundreds of megabytes rather than in sockets. A
    #: worker now starts one browser and shares it, so the cost of a slot there
    #: is a page rather than a program — see `browser_pages`.
    job_slots: int = 1
    #: How many pages this process will render at once, across every job it is
    #: running. One browser per worker, this many pages in it.
    #:
    #: It is a separate figure from `job_slots` because the two bound different
    #: things and the browser is by far the scarcer: raising slots to run more
    #: shops concurrently is cheap, and raising this is what actually decides
    #: how much of a machine the renders take. Four browser workers each running
    #: four jobs with a browser apiece is how a three-second render became nine
    #: minutes on 2026-08-10.
    browser_pages: int = 2
    #: Optional operator-owned CDP configuration. Crawl/job parameters carry
    #: only a logical profile name; connection URLs and tokens never enter the
    #: queue. The pool allocates a disposable instance for each job.
    cdp_profiles_file: Path | None = None
    cdp_secrets_file: Path | None = None
    cdp_pool_endpoint: str | None = None
    cdp_pool_trusted_hostnames: frozenset[str] = frozenset()
    cdp_pool_token_secret_ref: str = "cdp/pool_token"
    cdp_default_profile: str | None = None
    cdp_worker_pool: str = "browser-cdp"

    @model_validator(mode="after")
    def _queue_delivery_lifetime_fits_visibility(self) -> Settings:
        required = (
            DEFAULT_SOURCE_TIMEOUT
            + self.queue_pre_execution_wait_seconds
            + self.queue_finalization_seconds
            + self.queue_shutdown_ack_margin_seconds
        )
        if self.queue_visibility_seconds <= required:
            raise ValueError(
                "queue_visibility_seconds must exceed the complete delivery-lifetime "
                f"bound ({required:g}s)"
            )
        if self.queue_provider == "cloudflare":
            required_values = {
                "CATALOGUE_CF_ACCOUNT_ID": self.cf_account_id,
                "CATALOGUE_CF_QUEUE_PLAIN_ID": self.cf_queue_plain_id,
                "CATALOGUE_CF_QUEUE_BROWSER_AUTO_ID": self.cf_queue_browser_auto_id,
                "CATALOGUE_CF_QUEUE_BROWSER_CAMOUFOX_ID": self.cf_queue_browser_camoufox_id,
                "CATALOGUE_CF_QUEUE_BROWSER_CDP_EXTENSION_PROXY_ID": (
                    self.cf_queue_browser_cdp_extension_proxy_id
                ),
                "CATALOGUE_CF_QUEUE_RECOVERY_DLQ_ID": self.cf_queue_recovery_dlq_id,
            }
            missing = [name for name, value in required_values.items() if not value]
            if missing:
                raise ValueError(f"Cloudflare queue configuration missing: {', '.join(missing)}")
        return self

    cdp_production: bool = True
    #: Bearer token `catalogue-control` requires on every /v1 route.
    control_token: str = ""

    #: Global no-rebuild kill switch. Credentials themselves are read from the
    #: mounted JSON file and are deliberately not accepted as environment
    #: fields or run parameters.
    proxy_enabled: bool = False
    proxy_secret_file: Path | None = None
    #: Webshare paid traffic remains separately default-off until operators
    #: install a provider-bound gateway secret and explicitly enable its data
    #: plane. The Decodo secret path above remains unchanged for compatibility.
    proxy_webshare_data_plane_enabled: bool = False
    proxy_webshare_gateway_secret_file: Path | None = None
    proxy_api_secret_file: Path | None = None
    proxy_reconcile_profile: str = "decodo"
    proxy_billing_cycle_start: datetime | None = None
    proxy_billing_cycle_end: datetime | None = None

    #: Emit logs as one JSON object per line regardless of whether stdout is a
    #: terminal. Set in containers, where the console renderer is unreadable.
    log_json: bool | None = None

    def sources_path(self) -> Path:
        from .sources import default_path

        return default_path()
