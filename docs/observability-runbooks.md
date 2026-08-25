# Catalogue observability runbooks

The optional stack starts with `docker compose --profile observability up -d`.
vmagent is on `http://127.0.0.1:8429` by default and is the only observability
service that runs here. It scrapes and remote-writes to the shared plane in
mazenet-infra; nothing is stored, evaluated or displayed on this host.

The rules these runbooks describe live in
`mazenet-infra/ansible/files/monitoring/prometheus/rules/catalogue-alerts.yml`
and are evaluated by the plane's vmalert, which delivers to its Alertmanager.
Dashboards are the plane's Grafana at `https://grafana.int.mazenet.org`. A
metric that is absent locally is normal -- there is nowhere local for it to be.
Each alert still links back to the catalogue operator pages; request, job, run,
and trace IDs remain log search keys and are never metric labels.

Changing a rule is a `mazenet-infra` converge, not a commit here. That is the
cost of the 2026-08-25 decision to evaluate centrally, taken so that no
query-API read path into the shared plane had to be created.

## SourceNeverSucceeded

Open **Operations → Sources**, select the named source, and inspect its latest
job and current log. Confirm the source is intentionally enabled and covered by
an enabled schedule. If no job exists, inspect scheduler/worker health; if a job
failed, use its bounded upstream outcome and latency metrics to distinguish a
shop refusal from extraction or data-quality failure. Pause the source only when
continuing would repeatedly contact a broken or objecting upstream.

## SourceOverdue

Open the named source and compare its last usable completion with the last
scheduled fire. Check queued age and healthy worker capacity before debugging
the scraper: an overdue source with queued work is a fleet problem, while a
completed failed job is source-specific. Inspect host outcomes, backoff, and the
job's live structured log; do not mark a truncated or partial artifact usable to
silence the alert.

## QueueWithoutCapacity

Open **Operations** and verify every non-stopped worker is marked lost or absent.
Check the worker containers/services and their database connectivity, then
restore at least one worker with the capabilities required by the queued jobs.
Do not cancel the queue as a substitute for capacity: queued jobs are durable
and should begin once a healthy compatible worker returns.

## QueueStuck

Open **Operations → Runs** and inspect the oldest queued job's requirements,
scheduled time, host, and source pause state. Healthy workers plus an old queue
usually means capability mismatch, host-lease contention, a paused source, or a
claim-query problem. Check lease/backoff metrics and worker logs, then repair the
constraint; cancel or reschedule only when the work itself is no longer wanted.

## JobFailuresHigh

Group recent terminal jobs by source and outcome before acting. If one source
dominates, inspect that source's job logs and upstream outcomes; if failures are
broad, check database, proxy, browser pool, and worker health. The alert requires
at least four completions so a single isolated failure is not a fleet incident.
Treat degraded output as failure for diagnosis because it can preserve stale or
partial catalogue data even when some records were loaded.

## MetricsTargetDown

Open vmagent **Targets** (`http://127.0.0.1:8429/targets`) and identify the
exact job and instance. If the targets are healthy but the series are absent on
the plane, the fault is delivery rather than collection: check vmagent's
remote-write queue on `http://127.0.0.1:8429/metrics`
(`vmagent_remotewrite_pending_data_bytes`) before chasing the exporter.
For control/service, check the container health and `/metrics`; for workers,
confirm Docker DNS returns every replica and port 9109 is listening. A missing
worker target also makes worker-local rates incomplete, so restore collection
before using those rates to conclude that upstream traffic or failures stopped.
