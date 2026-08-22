"""The registry `catalogue-ops.openapi.json` is generated from.

A separate document from the catalogue's, deliberately (§10.1). Different
audience, different auth, different guarantees — merging them would put a
run-cancel endpoint in the document a tenant reads.

`GET /v1/events` is described as a `text/event-stream` response. OpenAPI 3.1 can
name the media type but not the schema of each named SSE event, so every payload
is defined in `components/schemas` and the operation description maps event
names onto them. The tooling does not enforce that mapping — but the schemas are
generated from the same models the service serialises with, so the payloads
cannot drift even though the association is prose.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from mb_ceramics_catalogue.config.settings import CrawlParams
from mb_ceramics_catalogue.contracts import Operation, Parameter, Registry
from pydantic import BaseModel, ConfigDict, Field

VERSION = "0.2.0"

DESCRIPTION = """
Operator control for the ceramics catalogue: start and watch runs, control
workers, enable and pause sources, and read the durable notification feed.

Internal. Every `/v1` route including the stream requires a bearer token;
`/health` and `/metrics` are the only exemptions. The service is not published
on the host, which is defence in depth rather than the authentication boundary.

### The live stream

`GET /v1/events` is one multiplexed `text/event-stream`, filtered by `topics`.
One endpoint rather than one per concern, because HTTP/1.1 caps a browser at
roughly six connections per origin.

Each message uses the SSE `event:` field, so a client writes
`stream.addEventListener('worker.changed', …)` rather than discriminating a
union. **Which messages carry an `id:` is the contract**:

| `event:` | Schema | Numbered | Replayed |
|---|---|---|---|
| `bootstrap` | `Bootstrap` | no | no |
| `worker.roster` | `WorkerRoster` | no | no |
| `worker.changed` | `WorkerChanged` | yes | yes |
| `run.started` / `run.complete` / `run.degraded` / `run.failed` | `RunEvent` | yes | yes |
| `job.leased` / `job.started` / `job.succeeded` / `job.degraded` / `job.failed` / `job.cancelled` | `JobStateChanged` | yes | yes |
| `job.progress` | `JobProgress` | no | no |
| `notification.raised` / `notification.resolved` | `NotificationEvent` | yes | yes |
| `resync` | `Resync` | no | no |

Numbered events are edges: discrete, ordered by one bigint sequence, and
replayable from `Last-Event-ID`. Unnumbered ones are levels — current values
that are meaningful only as the latest reading. A client that missed forty
progress readings does not want them, it wants the current one.

On `resync`, refetch state over the JSON endpoints; the gap was too large to
replay.
""".strip()


# ---------------------------------------------------------------------------
# Request and response models
# ---------------------------------------------------------------------------


class CreateRunRequest(BaseModel):
    sources: str = Field(default="all", description="A source id, a comma-separated list, or 'all'.")
    kind: Literal["manual", "scheduled", "retry", "backfill"] = "manual"
    requested_by: str | None = None
    params: CrawlParams = Field(
        default_factory=CrawlParams,
        description=(
            "Validated against the same model the CLI and the scheduler use, so a run "
            "created here cannot mean something a run created there would not."
        ),
    )


class CreateRunResponse(BaseModel):
    run_id: str
    jobs: int
    sources: list[str]


class RunSummary(BaseModel):
    succeeded: int = 0
    degraded: int = 0
    failed: int = 0
    cancelled: int = 0
    skipped: int = 0
    records: int = 0
    requests: int = 0


class Run(BaseModel):
    id: str
    kind: str
    status: Literal["queued", "running", "complete", "degraded", "failed", "cancelled"]
    schedule_id: str | None = None
    requested_by: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    summary: RunSummary | None = None
    jobs: int = 0
    succeeded: int = 0
    degraded: int = 0
    failed: int = 0
    active: int = 0


class RunList(BaseModel):
    runs: list[Run]
    next_cursor: str | None = None


class InFlight(BaseModel):
    seconds: float
    url: str


class JobSummary(BaseModel):
    """The per-source summary `run_source` produces, stored verbatim on the job.

    Typed rather than a free `object`: the job detail page reads
    `field_coverage` and `errors` off it, and a `Record<string, unknown>` is
    exactly the drift this whole exercise removes.
    """

    source: str
    label: str | None = None
    scraper: str
    extraction_method: str | None = None
    records: int = 0
    discovered: int = 0
    requests: int = 0
    rendered_pages: int = 0
    truncated: bool = False
    robots_ignored: bool = False
    error_count: int = 0
    errors: list[dict[str, str]] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    field_coverage: dict[str, int] = Field(
        default_factory=dict,
        description="Rows carrying each field, so a thin scraper is visible.",
    )
    write_status: str | None = None
    interrupted: bool | None = None
    loaded: int | None = None
    retired: int | None = None


class JobDataset(BaseModel):
    dataset: str
    contract_version: str
    projector_version: str
    state: Literal[
        "pending",
        "projecting",
        "staged",
        "publishing",
        "published",
        "loading",
        "succeeded",
        "degraded",
        "failed",
        "cancelled",
        "skipped",
    ]
    complete: bool = False
    records: int = 0
    rejected: int = 0
    error: str | None = None
    promoted_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class JobArtifact(BaseModel):
    id: str
    dataset: str
    contract_version: str
    projector_version: str
    kind: str
    location: str
    sha256: str
    size: int
    published_at: datetime
    available: bool = True
    retained_at: datetime | None = None


class Job(BaseModel):
    id: str
    run_id: str
    source_id: str
    host: str
    state: Literal[
        "queued", "leased", "running", "paused", "succeeded", "degraded", "failed", "cancelled", "skipped"
    ]
    attempt: int
    max_attempts: int
    priority: int
    requires: list[str] = Field(default_factory=list)
    requires_any: list[str] = Field(default_factory=list)
    selected_browser_backend: Literal["camoufox", "cdp_extension_proxy"] | None = None
    scheduled_for: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    trace_id: str | None = Field(
        default=None, description="32 lower-case hex characters when tracing is active."
    )
    artifact_path: str | None = None
    artifact_sha256: str | None = None
    artifact_size: int | None = None
    cancel_requested: bool = False
    pause_requested: bool = False
    summary: JobSummary | None = None
    datasets: list[JobDataset] = Field(default_factory=list)
    artifacts: list[JobArtifact] = Field(default_factory=list)
    phase: str | None = None
    records: int | None = None
    requests: int | None = None
    rendered_pages: int | None = None
    error_count: int | None = None
    discovered: int | None = None
    truncated: bool | None = None
    in_flight: list[InFlight] | None = None
    previous_records: int | None = Field(
        default=None,
        description="The previous successful run's record count, so a progress bar has a scale.",
    )


class RunDetail(BaseModel):
    run: Run
    jobs: list[Job]


class JobDetail(BaseModel):
    job: Job


class ChangedField(BaseModel):
    field: str
    before: Any = None
    after: Any = None


class RecordChange(BaseModel):
    kind: Literal["added", "removed", "changed"]
    external_id: str
    name: str | None = None
    fields: list[ChangedField] = Field(default_factory=list)


class JobChanges(BaseModel):
    job_id: str
    previous_job_id: str
    previous_run_id: str
    previous_finished_at: datetime
    added: int = 0
    removed: int = 0
    changed: int = 0
    unchanged: int = 0
    matched: int = 0
    items: list[RecordChange] = Field(default_factory=list)


class Accepted(BaseModel):
    """A control was applied. 202 rather than 200: the worker acts on it later."""

    job_id: str | None = None
    worker_id: str | None = None
    action: str | None = None
    desired_state: str | None = None
    cancelled: int | None = None


class LogLine(BaseModel):
    id: int
    at: datetime
    level: Literal["debug", "info", "warning", "error"]
    event: str | None = None
    message: str
    data: dict[str, Any] | None = None


class LogPage(BaseModel):
    lines: list[LogLine]
    next_after: int | None = None


class WorkerJob(BaseModel):
    job_id: str
    run_id: str
    source: str


class Worker(BaseModel):
    worker_id: str
    hostname: str
    pid: int
    version: str | None = None
    capabilities: list[str] = Field(default_factory=list)
    status: Literal["starting", "idle", "busy", "paused", "draining", "stopped"]
    desired_state: Literal["running", "paused", "draining", "stopping"]
    started_at: datetime
    last_heartbeat_at: datetime = Field(
        description=(
            "Derive the age from this locally. A worker that has silently died emits "
            "no event — nothing fires when a process stops existing — so staleness has "
            "to be computed from a clock rather than waited for."
        )
    )
    current_job_id: str | None = None
    current_source: str | None = None
    current_jobs: list[WorkerJob] = Field(
        default_factory=list,
        description="Every source currently leased to this process; a plain worker may run several.",
    )
    heartbeat_age_seconds: float | None = None


class WorkerList(BaseModel):
    workers: list[Worker]


class QueueOutbox(BaseModel):
    pending: int = 0
    ready: int = 0
    delayed: int = 0
    errored: int = 0
    publish_attempts: int = 0
    oldest_age_seconds: float = 0
    published_last_hour: int = 0


class QueueMeasurement(BaseModel):
    value: int | float | None = None
    accuracy: Literal["exact", "best_effort", "unsupported"]


class QueueRoute(BaseModel):
    route: str
    ready: QueueMeasurement
    in_flight: QueueMeasurement
    redelivered: QueueMeasurement
    delivered: QueueMeasurement
    oldest_age_seconds: QueueMeasurement


class QueueRecovery(BaseModel):
    backlog_messages: QueueMeasurement
    oldest_age_seconds: QueueMeasurement


class QueueBroker(BaseModel):
    provider: Literal["nats", "cloudflare"]
    observed_at: datetime
    last_success_at: datetime | None = None
    available: bool
    backlog_messages: QueueMeasurement
    backlog_bytes: QueueMeasurement
    consumer_count: QueueMeasurement
    routes: list[QueueRoute] = Field(default_factory=list)
    recovery_dlq: QueueRecovery | None = None
    error: str | None = None


class QueueStatus(BaseModel):
    """The authoritative job state, delivery outbox, and broker lag together."""

    at: datetime
    jobs: dict[str, int]
    eligible: int = 0
    oldest_queued_age_seconds: float = 0
    outbox: QueueOutbox
    broker: QueueBroker | None = None
    broker_error: str | None = None


class Source(BaseModel):
    source_id: str
    label: str
    url: str
    scraper: str
    country: str | None = None
    enabled: bool = True
    paused: bool = False
    schedule_id: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    last_success_at: datetime | None = None
    last_records: int | None = None
    previous_records: int | None = None
    delta: int | None = None
    staleness_seconds: float | None = None
    runs_7d: int = 0
    failures_7d: int = 0
    last_job_id: str | None = None
    last_run_id: str | None = None


class SourceList(BaseModel):
    sources: list[Source]


class SourceSettings(BaseModel):
    enabled: bool = True
    paused: bool = Field(
        default=False,
        description=(
            "Pausing also pauses this source's jobs that are in flight. Resuming does "
            "not automatically resume individually paused jobs: a broad administrative "
            "toggle must not silently restart work somebody stopped on purpose."
        ),
    )
    schedule_id: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    updated_by: str | None = None
    proxy: SourceProxyPolicyRequest | None = None


class SourceUpdated(BaseModel):
    source: dict[str, Any]
    proxy: dict[str, Any] | None = None


class SourceProxyPolicyRequest(BaseModel):
    policy: Literal["never", "fallback", "always"] = "never"
    route_id: str | None = None
    max_megabytes: int = Field(default=25, ge=1, le=25)
    pilot: bool = True


class ProxyCycle(BaseModel):
    id: str
    provider: str
    cycle_start: datetime
    cycle_end: datetime
    purchased_bytes: int
    operational_bytes: int
    daily_bytes: int
    pilot_bytes: int
    pilot_active: bool
    provider_reported_bytes: int
    application_bytes: int
    reconciled_at: datetime | None = None
    reconciliation_ok: bool
    kill_switch: bool
    lifecycle: Literal["proposed", "active", "closed", "rejected"]
    unmanaged_allocation_bytes: int = 0
    active_reserved_bytes: int = 0
    active_reservations: int = 0
    accounted_bytes: int = 0
    remaining_operational_bytes: int = 0
    provider_application_discrepancy_bytes: int = 0
    reconciliation_age_seconds: float | None = None
    daily_used_bytes: int = 0
    dynamic_daily_bytes: int = 0


class ProxySubscription(BaseModel):
    provider_resource_id: str | None = None
    service_type: str
    traffic_limit_bytes: int | None = None
    raw_traffic_limit: str | float | int | None = None
    valid_from: datetime
    valid_until: datetime
    users_limit: int | None = None


class ProxyProfileCounts(BaseModel):
    enabled: int = 0
    total: int = 0


class ProxyOverview(BaseModel):
    deployment_enabled: bool
    mutations_enabled: bool
    paid_probe_enabled: bool
    provider_configured: bool
    provider_error: str | None = None
    subscription: ProxySubscription | None = None
    cycle: ProxyCycle | None = None
    profiles: ProxyProfileCounts


class ProxyCycleList(BaseModel):
    cycles: list[ProxyCycle]


class ProxyUsageItem(BaseModel):
    key: str | datetime | None = None
    transmitted_bytes: int | None = None
    received_bytes: int | None = None
    total_bytes: int = 0
    request_count: int = 0
    last_observed_at: datetime | None = None


class ProxyUsageList(BaseModel):
    group_by: str
    usage: list[ProxyUsageItem]


class ProxyReservation(BaseModel):
    id: str
    job_id: str | None = None
    probe_id: str | None = None
    purpose: Literal["job", "probe"]
    provider: str
    profile: str
    profile_id: str | None = None
    route_id: str | None = None
    cycle_start: datetime
    reserved_bytes: int
    estimated_bytes: int
    request_count: int
    pilot: bool
    state: str
    created_at: datetime
    closed_at: datetime | None = None
    source_id: str | None = None
    run_id: str | None = None
    job_state: str | None = None
    probe_state: str | None = None


class ProxyReservationList(BaseModel):
    reservations: list[ProxyReservation]


class ProxyProfile(BaseModel):
    id: str
    provider: str
    logical_name: str
    provider_resource_id: str | None = None
    display_name: str
    username_mask: str | None = None
    provider_traffic_limit_bytes: int | None = None
    auto_disable: bool
    enabled: bool
    lifecycle: str
    secret_generation: int
    secret_installed_at: datetime | None = None
    provider_observed_at: datetime | None = None
    created_at: datetime
    updated_at: datetime
    allocated_bytes: int | None = None
    route_count: int = 0
    source_count: int = 0
    active_reservations: int = 0


class ProxyProfileList(BaseModel):
    profiles: list[ProxyProfile]


class ProxyRoute(BaseModel):
    id: str
    label: str
    profile_id: str
    profile: str | None = None
    protocol: Literal["http", "https", "socks5"]
    country: str | None = None
    state: str | None = None
    city: str | None = None
    session_mode: Literal["random", "sticky"]
    session_minutes: int
    max_bytes: int
    pilot: bool
    enabled: bool
    created_at: datetime
    updated_at: datetime
    source_count: int = 0


class ProxyRouteList(BaseModel):
    routes: list[ProxyRoute]


class ProxyProbe(BaseModel):
    id: str
    route_id: str
    profile_id: str
    reservation_id: str | None = None
    state: str
    requested_at: datetime
    completed_at: datetime | None = None
    error_category: str | None = None
    estimated_bytes: int
    provider_requests: int
    exit_country: str | None = None
    exit_ip: str | None = None
    latency_ms: int | None = None
    protocol: str
    actor: str
    request_id: str


class ProxyProbeList(BaseModel):
    probes: list[ProxyProbe]


class ProxyAudit(BaseModel):
    id: int
    operation_id: str
    actor: str
    actor_role: str
    request_id: str
    action: str
    resource_type: str
    resource_id: str | None = None
    at: datetime
    state: str
    success: bool | None = None
    error_code: str | None = None
    response_status: int | None = None


class ProxyAuditList(BaseModel):
    audit: list[ProxyAudit]


class ProxyCandidateList(BaseModel):
    candidates: list[dict[str, Any]]
    eligible_sources: list[str]


class ProxyMutationResult(BaseModel):
    status: str | None = None
    operation_id: str | None = None
    provider_reported_bytes: int | None = None
    kill_switch: bool | None = None
    revocation_requested: bool | None = None
    pilot_active: bool | None = None
    profile_id: str | None = None
    allocated_bytes: int | None = None
    provider_traffic_limit_bytes: int | None = None
    rotation: str | None = None
    active: int | None = None
    profile: ProxyProfile | None = None
    route: ProxyRoute | None = None
    cycle: ProxyCycle | None = None
    refreshed: int | None = None
    drift: list[dict[str, Any]] | None = None
    probe_id: str | None = None
    reservation_id: str | None = None
    state: str | None = None
    application_bytes: int | None = None
    reserved_bytes: int | None = None
    latency_ms: int | None = None
    exit_country: str | None = None
    exit_ip: str | None = None


class CreateProxyProfileRequest(BaseModel):
    logical_name: str
    display_name: str | None = None
    allocated_bytes: int = Field(gt=0, le=2_400_000_000)
    provider_traffic_limit_bytes: int | None = Field(default=None, gt=0, le=2_400_000_000)
    confirmation: str


class WebshareGatewayEndpoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    endpoint_id: Literal["webshare-residential-backbone"]
    protocol: Literal["http"]
    host: Literal["p.webshare.io"]
    port: int = Field(ge=1, le=65_535)


class WebshareGatewayCredentials(BaseModel):
    model_config = ConfigDict(extra="forbid")

    username: str = Field(min_length=1, max_length=512, json_schema_extra={"writeOnly": True})
    password: str = Field(min_length=1, max_length=1024, json_schema_extra={"writeOnly": True})


class WebshareGatewayCapabilities(BaseModel):
    model_config = ConfigDict(extra="forbid")

    countries: list[str]
    sticky_session_ttl_seconds: int = Field(ge=60, le=86_400)


class WebshareGatewayImportProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: Literal["webshare"]
    logical_name: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    generation: int = Field(ge=1, le=2_147_483_647)
    gateway: WebshareGatewayEndpoint
    credentials: WebshareGatewayCredentials
    capabilities: WebshareGatewayCapabilities


class WebshareGatewayImportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    profile: WebshareGatewayImportProfile
    expected_generation: int | None = Field(default=None, ge=1, le=2_147_483_646)
    display_name: str | None = Field(default=None, min_length=1, max_length=200)
    allocated_bytes: int | None = Field(default=None, gt=0, le=2_400_000_000)
    confirmation: str


class WebshareGatewayImportResult(BaseModel):
    operation_id: str
    profile_id: str | None = None
    provider: Literal["webshare"]
    logical_name: str
    generation: int
    state: Literal["draining", "installed", "completed", "failed"]
    remediation: str | None = None
    error_code: str | None = None


class ProxyProfileActionRequest(BaseModel):
    mode: Literal["drain", "blue-green"] | None = None
    allocated_bytes: int | None = Field(default=None, ge=0, le=2_400_000_000)
    provider_traffic_limit_bytes: int | None = Field(default=None, gt=0, le=2_400_000_000)
    confirmation: str


class ConfirmationRequest(BaseModel):
    confirmation: str


class OptionalConfirmationRequest(BaseModel):
    confirmation: str | None = None


class CreateProxyRouteRequest(BaseModel):
    label: str
    profile_id: str
    protocol: Literal["http", "https", "socks5"] = "http"
    country: str | None = None
    state: str | None = None
    city: str | None = None
    session_mode: Literal["random", "sticky"] = "random"
    session_minutes: int = Field(default=30, ge=1, le=1440)
    max_bytes: int = Field(default=25_000_000, ge=1, le=25_000_000)
    pilot: bool = True
    enabled: bool = False


class CycleConfirmation(BaseModel):
    confirmation: str
    cycle_start: datetime | None = None
    cycle_end: datetime | None = None
    purchased_bytes: int | None = None
    operational_bytes: int | None = None
    daily_bytes: int | None = None
    pilot_bytes: int | None = None
    unmanaged_allocation_bytes: int | None = None


class Notification(BaseModel):
    id: int
    at: datetime
    severity: Literal["info", "warning", "critical"]
    kind: str
    title: str
    body: str | None = None
    run_id: str | None = None
    job_id: str | None = None
    source_id: str | None = None
    worker_id: str | None = None
    dedup_key: str
    resolved_at: datetime | None = None
    acknowledged_at: datetime | None = None
    acknowledged_by: str | None = None


class NotificationList(BaseModel):
    notifications: list[Notification]


class Acknowledgement(BaseModel):
    id: int
    acknowledged: bool


class AcknowledgeRequest(BaseModel):
    by: str | None = None


class BulkAcknowledgeRequest(BaseModel):
    ids: list[int] = Field(min_length=1, max_length=500)
    by: str | None = None


class BulkAcknowledgement(BaseModel):
    ids: list[int]
    acknowledged: int


class Schedule(BaseModel):
    id: str
    enabled: bool = True
    cron: str = Field(description="Five-field cron, evaluated in `timezone`.")
    timezone: str = "Europe/Paris"
    source_filter: dict[str, Any] = Field(default_factory=lambda: {"all": True})
    params: dict[str, Any] = Field(default_factory=dict)
    last_fired_at: datetime | None = None
    next_fire_at: datetime | None = None


class ScheduleList(BaseModel):
    schedules: list[Schedule]


class ScheduleUpdated(BaseModel):
    schedule: Schedule


class Health(BaseModel):
    status: Literal["ok", "unavailable"]


# -- stream payloads --------------------------------------------------------


class JobProgress(BaseModel):
    """A level. Unnumbered and never replayed; the counters are cumulative."""

    job_id: str
    run_id: str
    source: str
    phase: str | None = None
    records: int = 0
    requests: int = 0
    rendered_pages: int = 0
    errors: int = 0
    discovered: int = 0
    truncated: bool = False
    in_flight: list[InFlight] = Field(default_factory=list)
    at: datetime


class WorkerRoster(BaseModel):
    """A level, pushed every five seconds."""

    workers: list[Worker]


class WorkerChanged(BaseModel):
    """An edge: `idle -> busy`, a drain, a stop."""

    id: int
    at: datetime
    type: str
    worker_id: str
    status: str | None = None
    current_job_id: str | None = None
    desired_state: str | None = None


class RunEvent(BaseModel):
    id: int
    at: datetime
    type: str
    run_id: str
    succeeded: int | None = None
    failed: int | None = None
    records: int | None = None


class JobStateChanged(BaseModel):
    id: int
    at: datetime
    type: str
    run_id: str
    job_id: str
    source: str
    state: str | None = None
    attempt: int | None = None
    records: int | None = None
    error: str | None = None


class NotificationEvent(BaseModel):
    id: int
    at: datetime
    type: str
    severity: str | None = None
    kind: str | None = None
    title: str | None = None
    source: str | None = None


class Bootstrap(BaseModel):
    """The stream's first frame, so a client renders without a second request."""

    workers: list[Worker]
    active_runs: list[dict[str, Any]]
    notifications: list[Notification]
    queue: dict[str, int]
    watermark: int
    jobs: list[Job] | None = None


class Resync(BaseModel):
    """The gap was too large to replay. Refetch over the JSON endpoints."""

    reason: str


def registry() -> Registry:
    api = Registry(
        title="Ceramics catalogue operations",
        version=VERSION,
        description=DESCRIPTION,
        servers=[{"url": "/", "description": "the control service"}],
        security=True,
    )

    api.add(
        Operation("get", "/health", "health", "Liveness", response=Health, errors=(503,), tags=("service",))
    )
    api.add(
        Operation(
            "get",
            "/metrics",
            "metrics",
            "Prometheus metrics",
            media_type="text/plain",
            errors=(),
            tags=("service",),
        )
    )

    api.add(
        Operation(
            "post",
            "/v1/runs",
            "createRun",
            "Start a run",
            description="Creates the run and one job per selected source. 202: the workers pick them up.",
            request=CreateRunRequest,
            response=CreateRunResponse,
            status=202,
            errors=(401, 409, 422),
            tags=("runs",),
        )
    )
    api.add(
        Operation(
            "get",
            "/v1/runs",
            "listRuns",
            "Run history",
            parameters=(
                Parameter("limit", schema={"type": "integer", "maximum": 200, "default": 25}),
                Parameter("cursor", description="A previous page's `next_cursor`."),
            ),
            response=RunList,
            errors=(401,),
            tags=("runs",),
        )
    )
    api.add(
        Operation(
            "get",
            "/v1/runs/{id}",
            "getRun",
            "One run with its jobs and their progress",
            parameters=(
                Parameter("id", location="path"),
                Parameter(
                    "dataset",
                    description="Exact dataset name; defaults to the ceramics compatibility output.",
                ),
            ),
            response=RunDetail,
            errors=(400, 401, 404),
            tags=("runs",),
        )
    )
    api.add(
        Operation(
            "post",
            "/v1/runs/{id}/cancel",
            "cancelRun",
            "Cancel every unfinished job in a run",
            parameters=(Parameter("id", location="path"),),
            response=Accepted,
            status=202,
            errors=(400, 401),
            tags=("runs",),
        )
    )

    api.add(
        Operation(
            "get",
            "/v1/jobs/{id}",
            "getJob",
            "One job",
            parameters=(Parameter("id", location="path"),),
            response=JobDetail,
            errors=(400, 401, 404),
            tags=("jobs",),
        )
    )
    api.add(
        Operation(
            "get",
            "/v1/jobs/{id}/changes",
            "getJobChanges",
            "Changes from this source's previous successful scrape",
            description=(
                "Compares the job's immutable NDJSON artifact with the immediately "
                "preceding successful artifact. Collection timestamps and raw scraper "
                "evidence are ignored so they do not make every record look changed."
            ),
            parameters=(
                Parameter("id", location="path"),
                Parameter("kind", schema={"type": "string", "enum": ["added", "removed", "changed"]}),
                Parameter("q", description="Substring match on product name or external id."),
                Parameter("limit", schema={"type": "integer", "maximum": 1000, "default": 200}),
            ),
            response=JobChanges,
            errors=(400, 401, 404, 409),
            tags=("jobs",),
        )
    )
    api.add(
        Operation(
            "get",
            "/v1/jobs/{id}/artifact",
            "downloadJobArtifact",
            "Download a completed ceramics artifact",
            description=(
                "Returns only an available, complete ceramics dataset artifact. "
                "Legacy job columns are considered only when the job has no dataset rows."
            ),
            parameters=(Parameter("id", location="path"),),
            response=None,
            media_type="application/octet-stream",
            errors=(400, 401, 404, 409),
            tags=("jobs",),
        )
    )
    api.add(
        Operation(
            "get",
            "/v1/jobs/{id}/logs",
            "getJobLogs",
            "A job's log, cursor-paged",
            parameters=(
                Parameter("id", location="path"),
                Parameter(
                    "after",
                    description="Return lines after this id.",
                    schema={"type": "integer", "default": 0},
                ),
                Parameter("level", description="debug, info, warning or error."),
                Parameter("q", description="Substring match on the message."),
                Parameter("limit", schema={"type": "integer", "maximum": 2000, "default": 500}),
            ),
            response=LogPage,
            errors=(400, 401),
            tags=("jobs",),
        )
    )
    api.add(
        Operation(
            "post",
            "/v1/jobs/{id}/{action}",
            "controlJob",
            "pause, resume, cancel or retry one source",
            description=(
                "Four controls rather than one, because they have different safety "
                "properties. A **pause** keeps the job resumable and consumes no "
                "attempt. A **cancel** is terminal and keeps whatever was collected as a "
                "partial artifact, which the loader will add from but never retire "
                "against. A **retry** is a new attempt the operator asked for, so the "
                "attempt budget is reset. 409 means the job is not in a state where the "
                "action means anything — pressing the same button twice is a no-op."
            ),
            parameters=(
                Parameter("id", location="path"),
                Parameter(
                    "action",
                    location="path",
                    schema={"type": "string", "enum": ["pause", "resume", "cancel", "retry"]},
                ),
            ),
            response=Accepted,
            status=202,
            errors=(400, 401, 404, 409),
            tags=("jobs",),
        )
    )

    api.add(
        Operation(
            "get",
            "/v1/workers",
            "listWorkers",
            "The worker roster with heartbeat ages",
            response=WorkerList,
            errors=(401,),
            tags=("workers",),
        )
    )
    api.add(
        Operation(
            "get",
            "/v1/queue",
            "queueStatus",
            "Queue delivery details",
            description=(
                "Combines PostgreSQL job state, the transactional outbox, and the "
                "selected provider's normalized queue measurements. PostgreSQL remains "
                "available in the response when provider statistics cannot be reached."
            ),
            response=QueueStatus,
            errors=(401,),
            tags=("workers",),
        )
    )
    api.add(
        Operation(
            "post",
            "/v1/workers/{id}/{action}",
            "controlWorker",
            "pause, resume, drain or stop a worker, or hide a lost registration",
            description=(
                "Controls the registered process, not the deployment's replica count: a "
                "restart policy may create a new worker afterwards, so persistently "
                "removing capacity is a scale operation this API does not pretend to "
                "guarantee. Hide is different: it only removes a registration from the "
                "roster after its heartbeat is already stale; the audit row is retained."
            ),
            parameters=(
                Parameter("id", location="path"),
                Parameter(
                    "action",
                    location="path",
                    schema={"type": "string", "enum": ["pause", "resume", "drain", "stop", "hide"]},
                ),
            ),
            response=Accepted,
            status=202,
            errors=(400, 401, 404, 409),
            tags=("workers",),
        )
    )

    api.add(
        Operation(
            "get",
            "/v1/sources",
            "listSources",
            "Every configured source, joined to what actually happened to it",
            response=SourceList,
            errors=(401,),
            tags=("sources",),
        )
    )
    api.add(
        Operation(
            "put",
            "/v1/sources/{id}",
            "updateSource",
            "Enable, pause, or override a source's schedule and parameters",
            parameters=(Parameter("id", location="path"),),
            request=SourceSettings,
            response=SourceUpdated,
            errors=(401, 404, 422),
            tags=("sources",),
        )
    )

    api.add(
        Operation(
            "get",
            "/v1/schedules",
            "listSchedules",
            "Schedules",
            response=ScheduleList,
            errors=(401,),
            tags=("schedules",),
        )
    )
    api.add(
        Operation(
            "put",
            "/v1/schedules/{id}",
            "updateSchedule",
            "Create or edit a schedule",
            parameters=(Parameter("id", location="path"),),
            request=Schedule,
            response=ScheduleUpdated,
            errors=(401, 422),
            tags=("schedules",),
        )
    )

    api.add(
        Operation(
            "get",
            "/v1/notifications",
            "listNotifications",
            "The durable notification feed",
            parameters=(
                Parameter(
                    "unacknowledged",
                    schema={"type": "boolean"},
                    description="Only conditions that are still open.",
                ),
                Parameter("severity", schema={"type": "string", "enum": ["info", "warning", "critical"]}),
                Parameter("limit", schema={"type": "integer", "maximum": 500, "default": 100}),
            ),
            response=NotificationList,
            errors=(401,),
            tags=("notifications",),
        )
    )
    api.add(
        Operation(
            "post",
            "/v1/notifications/ack",
            "acknowledgeNotifications",
            "Acknowledge selected notifications",
            request=BulkAcknowledgeRequest,
            response=BulkAcknowledgement,
            errors=(400, 401),
            tags=("notifications",),
        )
    )
    api.add(
        Operation(
            "post",
            "/v1/notifications/{id}/ack",
            "acknowledgeNotification",
            "Acknowledge one",
            parameters=(Parameter("id", location="path", schema={"type": "integer"}),),
            request=AcknowledgeRequest,
            response=Acknowledgement,
            errors=(400, 401, 409),
            tags=("notifications",),
        )
    )

    api.add(
        Operation(
            "post",
            "/v1/proxy/profiles/import",
            "importWebshareGatewayProfile",
            "Install or rotate an operator-issued Webshare gateway",
            description=(
                "Create returns 201 and rotation returns 200. Draining and an installed "
                "credential awaiting cycle allocation return 202; retry draining with a new "
                "Idempotency-Key after leases close, but retry cycle remediation with the same key."
            ),
            parameters=(
                Parameter(
                    "provider",
                    required=True,
                    schema={"type": "string", "const": "webshare"},
                ),
                Parameter(
                    "Idempotency-Key",
                    location="header",
                    required=True,
                    schema={"type": "string", "minLength": 1, "maxLength": 200},
                ),
            ),
            request=WebshareGatewayImportRequest,
            response=WebshareGatewayImportResult,
            status=201,
            errors=(400, 401, 403, 409, 422, 503),
            tags=("proxy",),
        )
    )

    for method, path, operation_id, summary, response, request_model, status in (
        (
            "get",
            "/v1/proxy/overview",
            "proxyOverview",
            "Proxy safety and subscription overview",
            ProxyOverview,
            None,
            200,
        ),
        ("get", "/v1/proxy/cycles", "proxyCycles", "Proxy billing cycles", ProxyCycleList, None, 200),
        ("get", "/v1/proxy/usage", "proxyUsage", "Provider and application usage", ProxyUsageList, None, 200),
        (
            "get",
            "/v1/proxy/reservations",
            "proxyReservations",
            "Proxy reservations",
            ProxyReservationList,
            None,
            200,
        ),
        (
            "get",
            "/v1/proxy/profiles",
            "proxyProfiles",
            "Safe proxy profile metadata",
            ProxyProfileList,
            None,
            200,
        ),
        (
            "post",
            "/v1/proxy/profiles",
            "createProxyProfile",
            "Create a bounded Decodo sub-user",
            ProxyMutationResult,
            CreateProxyProfileRequest,
            201,
        ),
        (
            "post",
            "/v1/proxy/profiles/refresh",
            "refreshProxyProfiles",
            "Refresh safe provider profile metadata",
            ProxyMutationResult,
            None,
            202,
        ),
        (
            "post",
            "/v1/proxy/profiles/{id}/{action}",
            "mutateProxyProfile",
            "Rotate or disable a proxy profile",
            ProxyMutationResult,
            ProxyProfileActionRequest,
            202,
        ),
        (
            "put",
            "/v1/proxy/profiles/{id}/{action}",
            "updateProxyProfile",
            "Change profile limit or allocation",
            ProxyMutationResult,
            ProxyProfileActionRequest,
            202,
        ),
        (
            "delete",
            "/v1/proxy/profiles/{id}",
            "retireProxyProfile",
            "Drain and retire a proxy profile",
            ProxyMutationResult,
            ConfirmationRequest,
            202,
        ),
        ("get", "/v1/proxy/routes", "proxyRoutes", "Non-secret managed routes", ProxyRouteList, None, 200),
        (
            "post",
            "/v1/proxy/routes",
            "createProxyRoute",
            "Create a non-secret route",
            ProxyMutationResult,
            CreateProxyRouteRequest,
            201,
        ),
        (
            "put",
            "/v1/proxy/routes/{id}",
            "updateProxyRoute",
            "Update a route",
            ProxyMutationResult,
            CreateProxyRouteRequest,
            202,
        ),
        (
            "delete",
            "/v1/proxy/routes/{id}",
            "deleteProxyRoute",
            "Retire an unused route",
            ProxyMutationResult,
            None,
            202,
        ),
        (
            "post",
            "/v1/proxy/routes/{id}/probe",
            "probeProxyRoute",
            "Run the fixed-target paid probe",
            ProxyMutationResult,
            ConfirmationRequest,
            200,
        ),
        ("get", "/v1/proxy/probes", "proxyProbes", "Bounded paid-probe history", ProxyProbeList, None, 200),
        (
            "get",
            "/v1/proxy/audit",
            "proxyAudit",
            "Append-only proxy administration audit",
            ProxyAuditList,
            None,
            200,
        ),
        (
            "get",
            "/v1/proxy/candidates",
            "proxyCandidates",
            "Proxy pilot candidates",
            ProxyCandidateList,
            None,
            200,
        ),
        (
            "post",
            "/v1/proxy/reconcile",
            "reconcileProxy",
            "Reconcile provider usage now",
            ProxyMutationResult,
            None,
            202,
        ),
        (
            "post",
            "/v1/proxy/kill-switch/{action}",
            "proxyKillSwitch",
            "Activate, clear, or revoke paid leases",
            ProxyMutationResult,
            OptionalConfirmationRequest,
            202,
        ),
        (
            "post",
            "/v1/proxy/pilot/{action}",
            "proxyPilot",
            "Start or stop the bounded pilot",
            ProxyMutationResult,
            OptionalConfirmationRequest,
            202,
        ),
        (
            "post",
            "/v1/proxy/cycles/propose",
            "proposeProxyCycle",
            "Propose a cycle from Decodo",
            ProxyMutationResult,
            None,
            201,
        ),
        (
            "post",
            "/v1/proxy/cycles/{id}/{action}",
            "mutateProxyCycle",
            "Open or close a confirmed cycle",
            ProxyMutationResult,
            CycleConfirmation,
            202,
        ),
    ):
        parameters: tuple[Parameter, ...] = ()
        if "{id}" in path:
            parameters += (Parameter("id", location="path"),)
        if "{action}" in path:
            parameters += (Parameter("action", location="path"),)
        api.add(
            Operation(
                method,
                path,
                operation_id,
                summary,
                parameters=parameters,
                request=request_model,
                response=response,
                status=status,
                errors=(400, 401, 403, 409, 422, 502, 503),
                tags=("proxy",),
            )
        )

    api.add(
        Operation(
            "get",
            "/v1/events",
            "stream",
            "The live stream",
            description=(
                "Server-sent events. See the table in the document description for which "
                "`event:` names carry which schema, and which are numbered.\n\n"
                "Reconnect with `Last-Event-ID` to resume exactly; only numbered events "
                "are replayed."
            ),
            parameters=(
                Parameter(
                    "topics",
                    description=(
                        "Comma-separated: workers, runs, jobs, progress, notifications, "
                        "schedules, sources, proxies. Omitting this subscribes to everything "
                        "except `progress`, which is the expensive one and should be "
                        "asked for deliberately."
                    ),
                ),
                Parameter("run_id", description="Narrow `jobs` and `progress` to one run."),
                Parameter("Last-Event-ID", location="header", description="Resume after this event id."),
            ),
            media_type="text/event-stream",
            errors=(401,),
            tags=("stream",),
        )
    )

    # The SSE payloads. Published as schemas without a path of their own: the
    # event table in the document description is what maps `event:` names onto
    # them, and inventing an endpoint per payload would put routes in the
    # document that do not exist.
    api.declare(
        Bootstrap,
        WorkerRoster,
        WorkerChanged,
        RunEvent,
        JobStateChanged,
        JobProgress,
        NotificationEvent,
        Resync,
    )

    return api
