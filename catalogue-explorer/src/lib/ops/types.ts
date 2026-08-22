/**
 * GENERATED FILE — do not edit.
 *
 * Produced from catalogue-ops.openapi.json by `catalogue-ops-types`, which is
 * generated in turn from the Pydantic models catalogue-control serialises with.
 * Edit those; run `make openapi` and `make types`; commit the result.
 *
 * The drift this removes is the silent kind: a renamed field compiles fine
 * against a hand-written interface and renders blank.
 */


/** A control was applied. 202 rather than 200: the worker acts on it later. */
export interface Accepted {
	job_id?: string | null;
	worker_id?: string | null;
	action?: string | null;
	desired_state?: string | null;
	cancelled?: number | null;
}

export interface AcknowledgeRequest {
	by?: string | null;
}

export interface Acknowledgement {
	id: number;
	acknowledged: boolean;
}

/** The stream's first frame, so a client renders without a second request. */
export interface Bootstrap {
	workers: Worker[];
	active_runs: Record<string, unknown>[];
	notifications: Notification[];
	queue: Record<string, number>;
	watermark: number;
	jobs?: Job[] | null;
}

export interface BulkAcknowledgeRequest {
	ids: number[];
	by?: string | null;
}

export interface BulkAcknowledgement {
	ids: number[];
	acknowledged: number;
}

export interface ChangedField {
	field: string;
	before?: unknown | null;
	after?: unknown | null;
}

export interface ConfirmationRequest {
	confirmation: string;
}

/** Everything that decides how a run collects. Validated once, used twice. */
export interface CrawlParams {
	limit?: number | null;
	sources?: number | null;
	concurrency?: number | null;
	delay?: number | null;
	browser?: 'never' | 'auto' | 'always' | null;
	impersonate?: 'never' | 'auto' | null;
	robots?: 'obey' | 'ignore' | null;
	cache_mode?: 'off' | 'auto' | 'replay' | 'refresh' | null;
	cache_max_age_hours?: number | null;
	stale_on_error?: boolean | null;
	pipeline?: 'legacy' | 'connector_canary' | null;
	datasets?: 'ceramics' | 'ceramics.catalogue_item.v2' | 'ceramics.catalogue_identity.v2' | 'commerce.price_observation.v1' | 'commerce.stock_observation.v1' | 'commerce.document.v1'[] | null;
	refresh_mode?: 'price' | 'full' | null;
	proxy_policy?: 'never' | null;
	proxy_max_megabytes?: number | null;
	source_timeout_seconds?: number | null;
	log_level?: 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR' | null;
	dry_run?: boolean | null;
	allow_empty?: boolean | null;
}

export interface CreateProxyProfileRequest {
	logical_name: string;
	display_name?: string | null;
	allocated_bytes: number;
	provider_traffic_limit_bytes?: number | null;
	confirmation: string;
}

export interface CreateProxyRouteRequest {
	label: string;
	profile_id: string;
	protocol?: 'http' | 'https' | 'socks5' | null;
	country?: string | null;
	state?: string | null;
	city?: string | null;
	session_mode?: 'random' | 'sticky' | null;
	session_minutes?: number | null;
	max_bytes?: number | null;
	pilot?: boolean | null;
	enabled?: boolean | null;
}

export interface CreateRunRequest {
	/** A source id, a comma-separated list, or 'all'. */
	sources?: string | null;
	kind?: 'manual' | 'scheduled' | 'retry' | 'backfill' | null;
	requested_by?: string | null;
	/** Validated against the same model the CLI and the scheduler use, so a run created here cannot mean something a run created there would not. */
	params?: CrawlParams | null;
}

export interface CreateRunResponse {
	run_id: string;
	jobs: number;
	sources: string[];
}

export interface CycleConfirmation {
	confirmation: string;
	cycle_start?: string | null;
	cycle_end?: string | null;
	purchased_bytes?: number | null;
	operational_bytes?: number | null;
	daily_bytes?: number | null;
	pilot_bytes?: number | null;
	unmanaged_allocation_bytes?: number | null;
}

export interface Health {
	status: 'ok' | 'unavailable';
}

export interface InFlight {
	seconds: number;
	url: string;
}

export interface Job {
	id: string;
	run_id: string;
	source_id: string;
	host: string;
	state: 'queued' | 'leased' | 'running' | 'paused' | 'succeeded' | 'degraded' | 'failed' | 'cancelled' | 'skipped';
	attempt: number;
	max_attempts: number;
	priority: number;
	requires?: string[] | null;
	requires_any?: string[] | null;
	selected_browser_backend?: 'camoufox' | 'cdp_extension_proxy' | null;
	scheduled_for: string;
	started_at?: string | null;
	finished_at?: string | null;
	error?: string | null;
	/** 32 lower-case hex characters when tracing is active. */
	trace_id?: string | null;
	artifact_path?: string | null;
	artifact_sha256?: string | null;
	artifact_size?: number | null;
	cancel_requested?: boolean | null;
	pause_requested?: boolean | null;
	summary?: JobSummary | null;
	datasets?: JobDataset[] | null;
	artifacts?: JobArtifact[] | null;
	phase?: string | null;
	records?: number | null;
	requests?: number | null;
	rendered_pages?: number | null;
	error_count?: number | null;
	discovered?: number | null;
	truncated?: boolean | null;
	in_flight?: InFlight[] | null;
	/** The previous successful run's record count, so a progress bar has a scale. */
	previous_records?: number | null;
}

export interface JobArtifact {
	id: string;
	dataset: string;
	contract_version: string;
	projector_version: string;
	kind: string;
	location: string;
	sha256: string;
	size: number;
	published_at: string;
	available?: boolean | null;
	retained_at?: string | null;
}

export interface JobChanges {
	job_id: string;
	previous_job_id: string;
	previous_run_id: string;
	previous_finished_at: string;
	added?: number | null;
	removed?: number | null;
	changed?: number | null;
	unchanged?: number | null;
	matched?: number | null;
	items?: RecordChange[] | null;
}

export interface JobDataset {
	dataset: string;
	contract_version: string;
	projector_version: string;
	state: 'pending' | 'projecting' | 'staged' | 'publishing' | 'published' | 'loading' | 'succeeded' | 'degraded' | 'failed' | 'cancelled' | 'skipped';
	complete?: boolean | null;
	records?: number | null;
	rejected?: number | null;
	error?: string | null;
	promoted_at?: string | null;
	created_at: string;
	updated_at: string;
}

export interface JobDetail {
	job: Job;
}

/** A level. Unnumbered and never replayed; the counters are cumulative. */
export interface JobProgress {
	job_id: string;
	run_id: string;
	source: string;
	phase?: string | null;
	records?: number | null;
	requests?: number | null;
	rendered_pages?: number | null;
	errors?: number | null;
	discovered?: number | null;
	truncated?: boolean | null;
	in_flight?: InFlight[] | null;
	at: string;
}

export interface JobStateChanged {
	id: number;
	at: string;
	type: string;
	run_id: string;
	job_id: string;
	source: string;
	state?: string | null;
	attempt?: number | null;
	records?: number | null;
	error?: string | null;
}

/** The per-source summary `run_source` produces, stored verbatim on the job. */
export interface JobSummary {
	source: string;
	label?: string | null;
	scraper: string;
	extraction_method?: string | null;
	records?: number | null;
	discovered?: number | null;
	requests?: number | null;
	rendered_pages?: number | null;
	truncated?: boolean | null;
	robots_ignored?: boolean | null;
	error_count?: number | null;
	errors?: Record<string, string>[] | null;
	notes?: string[] | null;
	/** Rows carrying each field, so a thin scraper is visible. */
	field_coverage?: Record<string, number> | null;
	write_status?: string | null;
	interrupted?: boolean | null;
	loaded?: number | null;
	retired?: number | null;
}

export interface LogLine {
	id: number;
	at: string;
	level: 'debug' | 'info' | 'warning' | 'error';
	event?: string | null;
	message: string;
	data?: Record<string, unknown> | null;
}

export interface LogPage {
	lines: LogLine[];
	next_after?: number | null;
}

export interface Notification {
	id: number;
	at: string;
	severity: 'info' | 'warning' | 'critical';
	kind: string;
	title: string;
	body?: string | null;
	run_id?: string | null;
	job_id?: string | null;
	source_id?: string | null;
	worker_id?: string | null;
	dedup_key: string;
	resolved_at?: string | null;
	acknowledged_at?: string | null;
	acknowledged_by?: string | null;
}

export interface NotificationEvent {
	id: number;
	at: string;
	type: string;
	severity?: string | null;
	kind?: string | null;
	title?: string | null;
	source?: string | null;
}

export interface NotificationList {
	notifications: Notification[];
}

export interface OptionalConfirmationRequest {
	confirmation?: string | null;
}

/** RFC 9457 `application/problem+json`. */
export interface Problem {
	type?: string | null;
	title: string;
	status: number;
	detail?: string | null;
}

export interface ProxyAudit {
	id: number;
	operation_id: string;
	actor: string;
	actor_role: string;
	request_id: string;
	action: string;
	resource_type: string;
	resource_id?: string | null;
	at: string;
	state: string;
	success?: boolean | null;
	error_code?: string | null;
	response_status?: number | null;
}

export interface ProxyAuditList {
	audit: ProxyAudit[];
}

export interface ProxyCandidateList {
	candidates: Record<string, unknown>[];
	eligible_sources: string[];
}

export interface ProxyCycle {
	id: string;
	provider: string;
	cycle_start: string;
	cycle_end: string;
	purchased_bytes: number;
	operational_bytes: number;
	daily_bytes: number;
	pilot_bytes: number;
	pilot_active: boolean;
	provider_reported_bytes: number;
	application_bytes: number;
	reconciled_at?: string | null;
	reconciliation_ok: boolean;
	kill_switch: boolean;
	lifecycle: 'proposed' | 'active' | 'closed' | 'rejected';
	unmanaged_allocation_bytes?: number | null;
	active_reserved_bytes?: number | null;
	active_reservations?: number | null;
	accounted_bytes?: number | null;
	remaining_operational_bytes?: number | null;
	provider_application_discrepancy_bytes?: number | null;
	reconciliation_age_seconds?: number | null;
	daily_used_bytes?: number | null;
	dynamic_daily_bytes?: number | null;
}

export interface ProxyCycleList {
	cycles: ProxyCycle[];
}

export interface ProxyMutationResult {
	status?: string | null;
	operation_id?: string | null;
	provider_reported_bytes?: number | null;
	kill_switch?: boolean | null;
	revocation_requested?: boolean | null;
	pilot_active?: boolean | null;
	profile_id?: string | null;
	allocated_bytes?: number | null;
	provider_traffic_limit_bytes?: number | null;
	rotation?: string | null;
	active?: number | null;
	profile?: ProxyProfile | null;
	route?: ProxyRoute | null;
	cycle?: ProxyCycle | null;
	refreshed?: number | null;
	drift?: Record<string, unknown>[] | null;
	probe_id?: string | null;
	reservation_id?: string | null;
	state?: string | null;
	application_bytes?: number | null;
	reserved_bytes?: number | null;
	latency_ms?: number | null;
	exit_country?: string | null;
	exit_ip?: string | null;
}

export interface ProxyOverview {
	deployment_enabled: boolean;
	mutations_enabled: boolean;
	paid_probe_enabled: boolean;
	provider_configured: boolean;
	provider_error?: string | null;
	subscription?: ProxySubscription | null;
	cycle?: ProxyCycle | null;
	profiles: ProxyProfileCounts;
}

export interface ProxyProbe {
	id: string;
	route_id: string;
	profile_id: string;
	reservation_id?: string | null;
	state: string;
	requested_at: string;
	completed_at?: string | null;
	error_category?: string | null;
	estimated_bytes: number;
	provider_requests: number;
	exit_country?: string | null;
	exit_ip?: string | null;
	latency_ms?: number | null;
	protocol: string;
	actor: string;
	request_id: string;
}

export interface ProxyProbeList {
	probes: ProxyProbe[];
}

export interface ProxyProfile {
	id: string;
	provider: string;
	logical_name: string;
	provider_resource_id?: string | null;
	display_name: string;
	username_mask?: string | null;
	provider_traffic_limit_bytes?: number | null;
	auto_disable: boolean;
	enabled: boolean;
	lifecycle: string;
	secret_generation: number;
	secret_installed_at?: string | null;
	provider_observed_at?: string | null;
	created_at: string;
	updated_at: string;
	allocated_bytes?: number | null;
	route_count?: number | null;
	source_count?: number | null;
	active_reservations?: number | null;
}

export interface ProxyProfileActionRequest {
	mode?: 'drain' | 'blue-green' | null;
	allocated_bytes?: number | null;
	provider_traffic_limit_bytes?: number | null;
	confirmation: string;
}

export interface ProxyProfileCounts {
	enabled?: number | null;
	total?: number | null;
}

export interface ProxyProfileList {
	profiles: ProxyProfile[];
}

export interface ProxyReservation {
	id: string;
	job_id?: string | null;
	probe_id?: string | null;
	purpose: 'job' | 'probe';
	provider: string;
	profile: string;
	profile_id?: string | null;
	route_id?: string | null;
	cycle_start: string;
	reserved_bytes: number;
	estimated_bytes: number;
	request_count: number;
	pilot: boolean;
	state: string;
	created_at: string;
	closed_at?: string | null;
	source_id?: string | null;
	run_id?: string | null;
	job_state?: string | null;
	probe_state?: string | null;
}

export interface ProxyReservationList {
	reservations: ProxyReservation[];
}

export interface ProxyRoute {
	id: string;
	label: string;
	profile_id: string;
	profile?: string | null;
	protocol: 'http' | 'https' | 'socks5';
	country?: string | null;
	state?: string | null;
	city?: string | null;
	session_mode: 'random' | 'sticky';
	session_minutes: number;
	max_bytes: number;
	pilot: boolean;
	enabled: boolean;
	created_at: string;
	updated_at: string;
	source_count?: number | null;
}

export interface ProxyRouteList {
	routes: ProxyRoute[];
}

export interface ProxySubscription {
	provider_resource_id?: string | null;
	service_type: string;
	traffic_limit_bytes?: number | null;
	raw_traffic_limit?: string | number | null;
	valid_from: string;
	valid_until: string;
	users_limit?: number | null;
}

export interface ProxyUsageItem {
	key?: string | null;
	transmitted_bytes?: number | null;
	received_bytes?: number | null;
	total_bytes?: number | null;
	request_count?: number | null;
	last_observed_at?: string | null;
}

export interface ProxyUsageList {
	group_by: string;
	usage: ProxyUsageItem[];
}

export interface QueueBroker {
	provider: 'nats' | 'cloudflare';
	observed_at: string;
	last_success_at?: string | null;
	available: boolean;
	backlog_messages: QueueMeasurement;
	backlog_bytes: QueueMeasurement;
	consumer_count: QueueMeasurement;
	routes?: QueueRoute[] | null;
	recovery_dlq?: QueueRecovery | null;
	error?: string | null;
}

export interface QueueMeasurement {
	value?: number | null;
	accuracy: 'exact' | 'best_effort' | 'unsupported';
}

export interface QueueOutbox {
	pending?: number | null;
	ready?: number | null;
	delayed?: number | null;
	errored?: number | null;
	publish_attempts?: number | null;
	oldest_age_seconds?: number | null;
	published_last_hour?: number | null;
}

export interface QueueRecovery {
	backlog_messages: QueueMeasurement;
	oldest_age_seconds: QueueMeasurement;
}

export interface QueueRoute {
	route: string;
	ready: QueueMeasurement;
	in_flight: QueueMeasurement;
	redelivered: QueueMeasurement;
	delivered: QueueMeasurement;
	oldest_age_seconds: QueueMeasurement;
}

/** The authoritative job state, delivery outbox, and broker lag together. */
export interface QueueStatus {
	at: string;
	jobs: Record<string, number>;
	eligible?: number | null;
	oldest_queued_age_seconds?: number | null;
	outbox: QueueOutbox;
	broker?: QueueBroker | null;
	broker_error?: string | null;
}

export interface RecordChange {
	kind: 'added' | 'removed' | 'changed';
	external_id: string;
	name?: string | null;
	fields?: ChangedField[] | null;
}

/** The gap was too large to replay. Refetch over the JSON endpoints. */
export interface Resync {
	reason: string;
}

export interface Run {
	id: string;
	kind: string;
	status: 'queued' | 'running' | 'complete' | 'degraded' | 'failed' | 'cancelled';
	schedule_id?: string | null;
	requested_by?: string | null;
	created_at: string;
	started_at?: string | null;
	finished_at?: string | null;
	params?: Record<string, unknown> | null;
	summary?: RunSummary | null;
	jobs?: number | null;
	succeeded?: number | null;
	degraded?: number | null;
	failed?: number | null;
	active?: number | null;
}

export interface RunDetail {
	run: Run;
	jobs: Job[];
}

export interface RunEvent {
	id: number;
	at: string;
	type: string;
	run_id: string;
	succeeded?: number | null;
	failed?: number | null;
	records?: number | null;
}

export interface RunList {
	runs: Run[];
	next_cursor?: string | null;
}

export interface RunSummary {
	succeeded?: number | null;
	degraded?: number | null;
	failed?: number | null;
	cancelled?: number | null;
	skipped?: number | null;
	records?: number | null;
	requests?: number | null;
}

export interface Schedule {
	id: string;
	enabled?: boolean | null;
	/** Five-field cron, evaluated in `timezone`. */
	cron: string;
	timezone?: string | null;
	source_filter?: Record<string, unknown> | null;
	params?: Record<string, unknown> | null;
	last_fired_at?: string | null;
	next_fire_at?: string | null;
}

export interface ScheduleList {
	schedules: Schedule[];
}

export interface ScheduleUpdated {
	schedule: Schedule;
}

export interface Source {
	source_id: string;
	label: string;
	url: string;
	scraper: string;
	country?: string | null;
	enabled?: boolean | null;
	paused?: boolean | null;
	schedule_id?: string | null;
	params?: Record<string, unknown> | null;
	last_success_at?: string | null;
	last_records?: number | null;
	previous_records?: number | null;
	delta?: number | null;
	staleness_seconds?: number | null;
	runs_7d?: number | null;
	failures_7d?: number | null;
	last_job_id?: string | null;
	last_run_id?: string | null;
}

export interface SourceList {
	sources: Source[];
}

export interface SourceProxyPolicyRequest {
	policy?: 'never' | 'fallback' | 'always' | null;
	route_id?: string | null;
	max_megabytes?: number | null;
	pilot?: boolean | null;
}

export interface SourceSettings {
	enabled?: boolean | null;
	/** Pausing also pauses this source's jobs that are in flight. Resuming does not automatically resume individually paused jobs: a broad administrative toggle must not silently restart work somebody stopped on purpose. */
	paused?: boolean | null;
	schedule_id?: string | null;
	params?: Record<string, unknown> | null;
	updated_by?: string | null;
	proxy?: SourceProxyPolicyRequest | null;
}

export interface SourceUpdated {
	source: Record<string, unknown>;
	proxy?: Record<string, unknown> | null;
}

export interface WebshareGatewayCapabilities {
	countries: string[];
	sticky_session_ttl_seconds: number;
}

export interface WebshareGatewayCredentials {
	username: string;
	password: string;
}

export interface WebshareGatewayEndpoint {
	endpoint_id: 'webshare-residential-backbone';
	protocol: 'http';
	host: 'p.webshare.io';
	port: number;
}

export interface WebshareGatewayImportProfile {
	provider: 'webshare';
	logical_name: string;
	generation: number;
	gateway: WebshareGatewayEndpoint;
	credentials: WebshareGatewayCredentials;
	capabilities: WebshareGatewayCapabilities;
}

export interface WebshareGatewayImportRequest {
	profile: WebshareGatewayImportProfile;
	expected_generation?: number | null;
	display_name?: string | null;
	allocated_bytes?: number | null;
	confirmation: string;
}

export interface WebshareGatewayImportResult {
	operation_id: string;
	profile_id?: string | null;
	provider: 'webshare';
	logical_name: string;
	generation: number;
	state: 'draining' | 'installed' | 'completed' | 'failed';
	remediation?: string | null;
	error_code?: string | null;
}

export interface Worker {
	worker_id: string;
	hostname: string;
	pid: number;
	version?: string | null;
	capabilities?: string[] | null;
	status: 'starting' | 'idle' | 'busy' | 'paused' | 'draining' | 'stopped';
	desired_state: 'running' | 'paused' | 'draining' | 'stopping';
	started_at: string;
	/** Derive the age from this locally. A worker that has silently died emits no event — nothing fires when a process stops existing — so staleness has to be computed from a clock rather than waited for. */
	last_heartbeat_at: string;
	current_job_id?: string | null;
	current_source?: string | null;
	/** Every source currently leased to this process; a plain worker may run several. */
	current_jobs?: WorkerJob[] | null;
	heartbeat_age_seconds?: number | null;
}

/** An edge: `idle -> busy`, a drain, a stop. */
export interface WorkerChanged {
	id: number;
	at: string;
	type: string;
	worker_id: string;
	status?: string | null;
	current_job_id?: string | null;
	desired_state?: string | null;
}

export interface WorkerJob {
	job_id: string;
	run_id: string;
	source: string;
}

export interface WorkerList {
	workers: Worker[];
}

/** A level, pushed every five seconds. */
export interface WorkerRoster {
	workers: Worker[];
}

// Names the explorer already imports, mapped onto the generated ones.
export type RunRow = Run;
export type WorkerRow = Worker;
export type SourceRow = Source;
export type NotificationRow = Notification;
