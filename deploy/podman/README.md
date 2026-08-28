# Catalogue production release contract

This directory is the application-owned boundary invoked by Deployment
Ansible. Infrastructure transports files and selects the host; these tools
validate release semantics, render Quadlets, scope runtime credentials and
activate one immutable release.

`render.py` accepts only digest-pinned images, one exact compatible
MakersBrain release, PostgreSQL endpoint metadata and one to three worker
instances. `build_runtime_stage.py` converts the exact Infisical export into
per-process database environments and four NATS role credentials. Runtime
containers never receive the NATS administrator credential. `release.py`
verifies the signed record and every image, stages immutable content and
atomically selects the Quadlet bundle with rollback on activation failure.

OTLP/HTTP trace export is a separate, fail-closed rollout. Public values must
carry a strict `otlp_traces_enabled` boolean and an `otlp_trace_processes`
list. Disabled means an empty list and no trace credentials are read. Enabled
requires a non-empty unique subset of `worker` and `worker-browser`; these are
the only deployed processes that currently initialize tracing. The runtime
stage then reads `observability/OTLP_TRACES_ACCESS_CLIENT_ID` and
`observability/OTLP_TRACES_ACCESS_CLIENT_SECRET`, URL-encodes them into the
standard signal-specific headers, and writes the endpoint, HTTP/protobuf
protocol, 1% parent-based sampler, deployment environment, and bounded batch
and export timeouts only to the selected mode-0600 process environment files.
Service, control and dispatcher receive neither trace settings nor trace
credentials.

Price/stock history has two independent public release booleans:
`stock_trends_enabled` injects `CATALOGUE_STOCK_TRENDS_ENABLED` only into both
worker modes, and `explorer_trends_enabled` injects
`CATALOGUE_EXPLORER_TRENDS_ENABLED` only into Explorer. Both default false in
the example values. The reviewed `catalogue-explorer/config/tracked-products.json`
is copied into the runtime stage and mounted read-only at
`/srv/config/tracked-products.json`; it contains identifiers and purchase
references, not credentials.

The optional Infisical export
`catalogue/proxy/WEBSHARE_GATEWAY_V2_JSON` is copied byte-for-byte into the
private runtime stage as `secrets/webshare-gateway/webshare-gateway.json`.
Missing input leaves that file absent so control can create generation 1;
the dedicated directory still exists. Control alone receives it writable,
which permits same-directory atomic secret generation replacement; plain and
browser workers receive it read-only so a
rename is immediately visible. Service, dispatcher, explorer, and NATS never
receive gateway credentials. The directory is mode `0700` and owned by the
rootless tenant identity; a bootstrap file is mode `0600` and control-written
rotations are mode `0400`. Staging or mounting it does not enable paid traffic:
the Webshare data-plane enable setting remains absent and therefore false.
Enabling it requires a separate, qualified deployment change after the durable
runtime gate passes.

The single `tenant-runtime` Podman context owns two private networks. Catalogue
containers join `catalogue.network`; MakersBrain containers join
`makersbrain.network`; only vmagent and cloudflared join both. No Catalogue
container publishes a host port.

### Gateway rotation smoke

On a deployment host, prefer a generated Quadlet/container smoke with Podman.
When Podman is unavailable but Docker is present, the executable fallback runs
the actual worker entrypoint and store implementation as `10001:10001`, with a
read-only root filesystem, no network, no capabilities, and the gateway store
mounted read-only in the worker:

```sh
python deploy/podman/smoke_webshare_rotation.py \
  --image catalogue-ceramics-worker:<candidate-tag>
```

It installs generation 1, starts the worker entrypoint, atomically replaces the
credential with generation 2 from a separate writable container, and requires
the already-running worker process to observe the replacement. The result is
explicitly Docker rootless-identity emulation, not evidence of Quadlet
generation or activation; use it only as the strongest local fallback.

## Database transfer

The signed `database_transfer` image contains PostgreSQL 17 client tools and
the `catalogue-db-transfer` command. Supply the connection only through
`CATALOGUE_DSN`; the command converts it to libpq environment fields and never
places its password in process argv.

```sh
catalogue-db-transfer inventory
catalogue-db-transfer dump /transfer/catalogue.dump
catalogue-db-transfer verify /transfer/catalogue.dump
catalogue-db-transfer restore /transfer/catalogue.dump \
  --expected-target-database ateliera \
  --confirm restore-empty:ateliera
```

`dump` refuses existing outputs, uses custom format with ownership, ACLs and
globals excluded, and records source version/size, the migration ledger,
critical counts, timestamps, restore-list checksum and archive SHA-256. It
rejects a dump if the ledger or critical counts move during capture. `restore`
requires an exact database-bound confirmation, refuses a non-empty target,
uses one transaction and compares the restored ledger and counts to the
manifest.

After restore, run `catalogue-migrate` and `catalogue-proxy-roles` with the
production migration identity. Provision JetStream once with
`catalogue-queue-admin apply` and the admin credential; ordinary publisher,
consumer and stats clients deliberately cannot create streams or consumers.

The rehearsal archive is not the final cutover archive. The final dump is
taken only after schedules and run creation are disabled, workers drain, the
dispatcher and writers stop, and queued/running/unpublished-outbox counts are
all zero.
