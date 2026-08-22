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
