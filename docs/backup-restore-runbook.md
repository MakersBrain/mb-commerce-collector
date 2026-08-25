# Catalogue backup and restore

Status: implemented, not yet rehearsed against production storage.
Tool: `catalogue-backup` (`catalogue_control.backup`), image `docker/backup`.

## What is protected, and what deliberately is not

| Data | Backed up | Why |
| --- | --- | --- |
| PostgreSQL `catalogue` schema | yes | Runs, jobs, datasets and every artifact reference. |
| `catalogue-dumps` volume | yes | The NDJSON artifacts those references describe. |
| `catalogue-cache` volume | **no** | Reproducible by fetching. Losing it costs one slow run, not data. |
| `catalogue-nats` volume | **no** | Queue state, not a record. See "NATS" below. |

The audit trail is the **pair**: a row records `artifact_path` and
`artifact_sha256`, and the file is what those describe. A database without its
artifacts is a set of dangling references; artifacts without the database are
anonymous files in run/job directories. Neither half alone is a backup.

## Why the database is dumped before the artifacts

`catalogue-backup backup` runs two passes, database first, and the order is the
correctness argument rather than a preference.

Artifacts are **write-once**: a job writes `<run-id>/<job-id>/…` and never
rewrites it. So every row in the database dump refers to a file that already
existed when the dump was taken and cannot change afterwards. Files created
between the two passes are simply not referenced by the dump — harmless extra
data in the snapshot.

Reversing the order breaks exactly this: a job finishing between the file pass
and the dump lands a row whose artifact was never captured, producing a restore
with dangling references. That is the one failure this design exists to prevent.

## Configuration

Everything comes from the environment. No credential is accepted on the command
line, where it would land in shell history and the process table.

| Variable | Meaning |
| --- | --- |
| `RESTIC_REPOSITORY` | `s3:https://<account>.r2.cloudflarestorage.com/makersbrain-<env>-backups/collector` |
| `RESTIC_PASSWORD_FILE` | Path to the repository password. A file, never an inline value. |
| `CATALOGUE_DATABASE_URL` | PostgreSQL connection URL. |
| `CATALOGUE_ARTIFACTS_DIR` | Defaults to `/var/lib/catalogue/dumps`. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | R2 access key for that bucket. |

The repository is in Cloudflare R2. restic speaks S3 and R2 answers it, so the
endpoint and the keys are the whole of the difference; nothing in the tool
knows which provider it is talking to. R2 charges nothing to egress, which is
what makes a restore rehearsal cheap enough to actually run.

The agent renders all five of these to `/etc/catalogue/backup.env`, and the
password to its own file, from `/catalogue/backup` in Infisical
(`deploy/infisical/README.md`).

Retention is enforced by an R2 bucket lock on the `collector/` prefix, so a key
that can write a snapshot cannot delete or overwrite one:

```sh
wrangler r2 bucket lock add makersbrain-<env>-backups \
  --name collector-retention --prefix collector/ --retention-days 90
```

A lock rule is removable by an account administrator, which S3 compliance mode
is not. That is the right trade here: the threat this defends against is a
compromised backup writer — the key on the host, scoped to this prefix — and
that key cannot change lock rules. It does not defend against a compromised
Cloudflare account, which is what the offsite escrow of the password is for.

**The lock and `restic forget --prune` are in direct conflict.** Pruning
repacks and deletes pack files, and a locked object refuses deletion until its
retention expires, so a prune inside the window fails partway and leaves the
repository needing `restic repair index`. Either run `forget` without `--prune`
and let the lock window be the retention policy, or prune only against objects
older than the lock. Do not set an indefinite lock on a repository you intend
to prune.

The repository password is the encryption key. **If it is lost, the backups are
unreadable** — restic has no recovery path. It is in Infisical so the agent can
render it, and it must be escrowed somewhere Infisical is not: losing the
password and the bucket credentials together turns every snapshot into noise.

## Routine backup

```
catalogue-backup backup
```

Initialises the restic repository on first use, dumps the database with
`pg_dump --format=custom`, backs it up tagged `database`, then backs up the
artifact directory tagged `artifacts`. Both passes share an `at:<timestamp>` tag
so the halves of one run can be found together.

Retention:

```
catalogue-backup forget --prune --keep-daily 7 --keep-weekly 5 --keep-monthly 12
```

## Restore

```
catalogue-backup restore --snapshot latest --target /var/lib/catalogue-restore
pg_restore --clean --if-exists --no-owner --no-privileges \
  --dbname "$CATALOGUE_DATABASE_URL" /var/lib/catalogue-restore/…/catalogue.dump
CATALOGUE_ARTIFACTS_DIR=/var/lib/catalogue-restore/…/dumps catalogue-backup verify
```

**A restore is not finished until `verify` passes.** It re-reads every artifact
reference in the restored database and checks the file resolves within the
artifact root and its sha256 matches, reusing `changes.resolve_artifact` so the
integrity rule lives in exactly one place. It reports every broken reference in
one run rather than stopping at the first, and exits non-zero if any fail.

Artifacts marked `available = false` are excluded: that flag means the catalogue
has retired the artifact and the file is allowed to be gone. Verifying them
would report a deliberate retention decision as a corrupt restore, and a check
that cries wolf is a check that gets ignored.

## NATS

`catalogue-nats` holds queue state, not the record of what happened — that is in
PostgreSQL. A restore brings back a catalogue whose in-flight jobs are lost;
they are re-dispatched from their database rows. Backing up JetStream state
would restore a queue that disagrees with the restored database, which is worse
than an empty one.

## What still has to happen before this counts as a backup

1. **Rehearse a restore into a scratch database and prove `verify` passes.**
   Until that is done this is an untested script, not a recovery capability.
2. Create the R2 bucket and the `collector` prefix in `mb-infra`, with
   versioning on, a bucket lock over the prefix, and a scoped access key that
   can write this prefix and no other. The Terraform this expects is in the
   appendix below.
3. Add the Quadlet unit and systemd timer in `mb-infra`. Scheduling and storage
   are infrastructure-owned per the repository split; this repository owns the
   image and the data semantics, and the Infisical agent owns the credentials.
4. Alert on backup age. A silent failure is indistinguishable from success
   until the day it matters — `catalogue-backup snapshots` is the signal.
5. Escrow the restic password outside Infisical.

## Appendix: the Terraform this expects

Storage and credentials are `mb-infra`-owned; this appendix is here so the
collector side states what it expects to exist rather than leaving the other
repository to infer it. Verified against the Cloudflare provider (v5) and the
Infisical provider as of August 2026.

### The buckets and the lock

The lock resource does not take wrangler's flags. It is a rules list with a
typed condition — `Age`, `Date` or `Indefinite`:

```hcl
resource "cloudflare_r2_bucket" "backups" {
  account_id = var.account_id
  name       = "makersbrain-${var.env}-backups"
}

resource "cloudflare_r2_bucket_lock" "backups" {
  account_id  = var.account_id
  bucket_name = cloudflare_r2_bucket.backups.name
  rules = [{
    id        = "collector-retention"
    enabled   = true
    prefix    = "collector/"
    condition = { type = "Age", max_age_seconds = 90 * 24 * 60 * 60 }
  }]
}

# The response cache is a build input, not a backup: it is republished whenever
# the crawl is refreshed, so a retention lock here would only block its own
# replacement.
resource "cloudflare_r2_bucket" "cache" {
  account_id = var.account_id
  name       = "mb-catalogue-cache"
}
```

### The keys

R2 has no access-key resource. A scoped account token is created and the S3
pair is derived from it: the Access Key ID is the token's `id`, and the Secret
Access Key is the **SHA-256 of the token value**.

```hcl
data "cloudflare_account_api_token_permission_groups_list" "r2_write" {
  account_id = var.account_id
  name       = "Workers R2 Storage Bucket Item Write"
}

resource "cloudflare_account_token" "backup_writer" {
  account_id = var.account_id
  name       = "catalogue-backup-writer"
  policies = [{
    effect            = "allow"
    permission_groups = [{ id = data.cloudflare_account_api_token_permission_groups_list.r2_write.result[0].id }]
    resources = jsonencode({
      "com.cloudflare.edge.r2.bucket.${var.account_id}_default_${cloudflare_r2_bucket.backups.name}" = "*"
    })
  }]
}

locals {
  backup_access_key_id = cloudflare_account_token.backup_writer.id
  backup_secret_key    = sha256(cloudflare_account_token.backup_writer.value)
}
```

The bucket-scoped resource string is the whole of the isolation: it is what
stops the CI reader from reaching the backup bucket, and the backup writer from
reaching anything else. CI gets a second token with `Workers R2 Storage Bucket
Item Read`, scoped to the cache bucket.

Note what this does **not** give the backup writer: the lock rule is account
configuration, not bucket data, so a compromised writer key can neither delete
a snapshot nor lift the rule that stops it.

### Into Infisical

```hcl
resource "infisical_secret" "backup_secret_key" {
  name         = "AWS_SECRET_ACCESS_KEY"
  value        = local.backup_secret_key
  workspace_id = var.infisical_project_id
  env_slug     = "prod"
  folder_path  = "/catalogue/backup"
}
```

`/catalogue/backup` in `prod` is what the agent renders on the host;
`/catalogue/cache` in `ci` is what the golden job reads. The paths are the
contract between this repository and `mb-infra`, and they are the reason the
two identities can be scoped to different things.

### Three things that bite

**Terraform state becomes a secret store.** The token value, the derived S3
secret and the restic password all land in state in plaintext. Encrypted remote
state and access control on it stop being good practice and become part of the
threat model — which is the argument for this living in `mb-infra` with its
existing backend rather than anywhere more convenient.

**The lock outlives `terraform destroy`.** Once objects are retained the bucket
cannot be emptied or deleted until retention expires, so a torn-down
environment leaves its backup bucket standing for up to 90 days. That is the
feature working, but it surprises people who expect destroy to be total.

**The Infisical provider cannot bootstrap itself.** It authenticates with a
machine identity that Terraform cannot have fetched from Infisical. That one
credential is placed by hand, the same as the host's client-id and
client-secret pair. Everything downstream of it is managed.
