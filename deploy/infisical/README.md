# Secrets on the deployment host

Every secret this deployment uses is rendered by the Infisical agent into a
file, and every unit reads a file. Nothing authenticates to a secret manager on
its own behalf, and no secret is baked into an image, a unit file or this
repository.

## What the agent renders

| Destination | Contents | Consumed by |
| --- | --- | --- |
| `/etc/catalogue/catalogue.env` | Database DSN, control token, queue provider mapping | `catalogue-control`, `catalogue-service` |
| `/etc/catalogue/backup.env` | `RESTIC_REPOSITORY` and the R2 keys behind it | the scheduled backup unit |
| `/etc/catalogue/restic-password` | The repository password, alone in its own file | `restic`, which reads a path |

`agent.yaml` re-renders on a poll and restarts only the services whose inputs
changed. A rotated database password reaches the running system without a
deploy.

## Bootstrapping

The machine identity is the one credential that has to be placed by hand. Two
files, mode `0400`, owned by root:

```sh
install -d -m 0700 /etc/infisical
printf %s "$CLIENT_ID"     > /etc/infisical/client-id
printf %s "$CLIENT_SECRET" > /etc/infisical/client-secret
chmod 0400 /etc/infisical/client-id /etc/infisical/client-secret

install -d -m 0750 /etc/catalogue
install -d -m 0700 /var/lib/infisical
cp agent.yaml /etc/infisical/agent.yaml
cp -r templates /etc/infisical/templates
systemctl enable --now infisical-agent
```

The templates carry `INFISICAL_PROJECT_ID` as a placeholder. Substitute the
project's id before installing them: `listSecrets` and `getSecretByName` take
the project id, where the CI job's API call takes the project slug. They are
not interchangeable, and the failure when they are swapped is an empty render
rather than an error — which is why the units refuse to start on a missing
value rather than defaulting. (`listSecretsByProjectSlug` exists if you would
rather both sides key on the slug.)

## Scope

Give the host identity read on `/catalogue` and `/catalogue/backup`, and
nothing else. In particular it must not be able to read `/catalogue/cache`,
which is CI's, or to write anywhere: an identity that can rewrite the secret it
authenticates with is a way to lock yourself out of your own deployment.

The golden CI job uses GitHub OIDC to obtain a 15-minute Infisical token. The
identity is bound to this repository's `catalogue-cache` environment and is a
Viewer of the dedicated `makersbrain-catalogue-ci` project, whose only secrets
are the bucket-scoped R2 reader. That reader can fetch objects from
`mb-catalogue-cache`; it cannot publish, replace, or delete archives or reach
another bucket. GitHub stores only the public Infisical identity ID. See the
`golden` job in `.github/workflows/ci.yml`.

## Backups escrow

`RESTIC_PASSWORD` is in Infisical so the agent can render it, but a copy has to
live somewhere Infisical is not. The failure this protects against is not a
lost password; it is losing the password and the bucket credentials together,
which turns every snapshot into noise.

The bucket lock over the backup prefix means the host's key can write snapshots
but not remove them, so a compromised host destroys no history. It also means
`restic forget --prune` fails inside the retention window — see the
[backup runbook](../../docs/backup-restore-runbook.md).
