# Connector migration and rollback

Migrate one source and connector at a time. Extraction is complete only after
identical recorded inputs, projected artifact comparison, a bounded production
canary, and an observation window establish behavior—not merely because unit
tests pass.

The catalogue currently keeps `legacy` as the default and enables the library
path explicitly with `connector_canary`. Shopify and WooCommerce have native
library worker routes; other connectors may still pass through compatibility
adapters. Check the live matrix in
[`commerce-scraper-library-status.md`](../../docs/commerce-scraper-library-status.md)
before treating any connector as cut over.

## 1. Inventory the source contract

Record, in reviewable configuration:

- every legacy source field and its library connector option;
- requested datasets and required `SnapshotField` values;
- discovery partitions, category/collection filtering, browser and proxy use;
- refresh modes, result-limit behavior, cancellation, and timeout policy;
- legacy and library connector/version identity;
- checkpoint compatibility and restart behavior;
- intentional projection differences and their owner.

Use the repository
[`connector inventory`](../../docs/commerce-scraper-connector-inventory.md) and
the plan's
[`per-connector checklist`](../../docs/commerce-scraper-library-plan.md#17-per-connector-migration-checklist)
as the source-of-truth checklist. Do not move dataset projection, storage, or
queue semantics into the connector to make parity easier.

## 2. Establish deterministic replay

Acquire and scrub the raw response archive through the approved cache process.
Replay must be network-closed: cache misses fail and browser execution is
disabled. Never call a synthetic fixture “recorded parity.”

```console
make cache-pull
make test-golden
```

For focused diagnosis, run the existing path and local canary compatibility
shell against the same replay cache. Replace `SOURCE` with a checked-in source
key:

```console
uv --directory catalogue-dump run -- catalogue-probe SOURCE \
  --pipeline legacy --cache .cache --cache-mode replay --browser never \
  --limit 40 --log-level DEBUG

uv --directory catalogue-dump run -- catalogue-probe SOURCE \
  --pipeline connector_canary --cache .cache --cache-mode replay --browser never \
  --limit 40 --log-level DEBUG
```

This local shell proves source mapping, parser, and compatibility projection;
it does not prove the native worker's Postgres checkpoint or routed-proxy
lifecycle. Cover those boundaries in the worker canary gate.

Compare full projected artifacts, not only connector pages or row counts:

```console
uv --directory catalogue-dump run -- catalogue-shadow-compare \
  /path/to/legacy.ndjson /path/to/connector.ndjson --pretty
```

Exit codes are `0` for equal, `1` for a valid comparison with differences, and
`2` for invalid/unreadable inputs. If a difference is intended, check in a
bounded version-one rules file, name the affected field paths, explain the
semantic reason, and review the resulting samples. Do not regenerate a golden
solely to make extraction pass.

## 3. Verify architecture and intercall behavior

Before live traffic, run the connector conformance test plus the application
composition boundary. Essential coverage is:

- factory/options/capability validation;
- one successful connector-to-middleware-to-transport flow;
- page/checkpoint persistence and terminal recovery;
- cancellation and owned-resource cleanup;
- direct/browser/proxy selection, if the source uses it;
- dataset projection and artifact identity;
- structured trace correlation and credential containment.

```console
make scraper-check
make test
```

Add Postgres or NATS coverage only when the change crosses those boundaries;
do not multiply equivalent unit permutations.

## 4. Run an explicit canary

Canary selection must be source-scoped. In scheduled workers,
`source_settings.params.pipeline` overrides the run default for that source;
unselected siblings remain `legacy`. Apply the override through the catalogue
control plane and confirm the reserved job snapshot says
`connector_canary` before it starts.

For a local, non-publishing preview:

```console
uv --directory catalogue-dump run -- catalogue-dump \
  --source SOURCE --pipeline connector_canary --dry-run --limit 40 \
  --log-level DEBUG --log-json
```

Like the probe, this CLI path is a compatibility preview. The explicitly
selected scheduled worker route is the authoritative native canary.

Use full output only after replay parity. A production canary must have an
owner, start/end time, source and connector version, bounded request/proxy byte
budget, stable artifact destination, and written rollback criterion. Observe:

- discovered/emitted/terminal counts and completeness diagnostics;
- request, retry, browser, proxy, transmitted, and received totals;
- route/provider/rotation classifications;
- checkpoint lineage and resumption;
- projected field coverage and artifact comparison;
- source health, duration, and downstream load behavior.

Trace using `collection_id`/lineage first. Keep application lifecycle at
`info`, protocol/cursor/route detail at `debug`, and retryable failures,
budget denial, or incomplete enumeration at `warning`. Logs and artifacts must
remain free of credentials, raw headers, payload bodies, and unsafe URLs.

## 5. Roll back without mixing lineage

Rollback is configuration-only: set the source override to `pipeline=legacy`
or remove the override so the run's legacy default applies. Confirm the next
reservation contains `legacy`; do not mutate an already snapshotted running
job in place.

Library checkpoints must never be passed to the legacy implementation. Retain
connector/version and dataset projection metadata on every artifact so output
from the two paths cannot appear equivalent accidentally. Preserve the canary
artifact and comparison report for diagnosis; revoke proxy leases only if the
rollback is also intended to interrupt active requests.

Rollback immediately for credential exposure, unbounded paid traffic, missing
terminal/checkpoint progress, materially incomplete enumeration, corrupt
artifacts, or an unexplained parity regression. A retryable storefront failure
is handled by normal typed retry/health policy and is not by itself proof that
the connector mapping is wrong.

## 6. Promote and remove legacy code

After replay and canary gates pass:

1. approve or document every output difference;
2. switch only the selected stable source mapping;
3. retain the explicit legacy route for at least one normal schedule cycle;
4. compare scheduled output and resume behavior over the agreed observation
   window;
5. remove compatibility code only when every affected source has equivalent
   evidence and a release note.

Run the full relevant gate before merge. Schema changes require a deliberate
schema version and compatibility review.

```console
make check
make test-golden
```

Do not declare migration complete when the recording archive, canary approval,
or production observation evidence is absent. Record that as an open gate and
keep rollback available.
