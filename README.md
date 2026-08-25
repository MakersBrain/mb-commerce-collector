# catalogue-ceramics

Collects public ceramic-materials listings from eighty suppliers, loads them
into PostgreSQL as a comparable reference catalogue, and runs the whole thing on
a schedule that can be watched and controlled from a browser.

```
                    ┌──────────────────────────────────────────┐
                    │  catalogue-explorer  (SvelteKit)         │
                    │  /  /explore  /compare                   │
                    │  /ops  /ops/runs  /ops/sources  …        │
                    └───────┬──────────────────────┬───────────┘
                            │ SSE + JSON           │ SQL (read)
                            │ (proxied server-side)│
                    ┌───────▼──────────┐           │
                    │ catalogue-control│           │
                    │  POST /v1/runs   │           │
                    │  GET  /v1/events │           │
                    └───────┬──────────┘           │
                            │ enqueue / LISTEN     │
                    ┌───────▼──────────────────────▼───────────┐
                    │            PostgreSQL                    │
                    │  catalogue.*        (reference data)     │
                    │  catalogue.runs / jobs / job_progress    │
                    │  catalogue.job_events / workers / hosts  │
                    └───────▲──────────────────────▲───────────┘
                            │ claim / progress     │ read
                    ┌───────┴───────┐      ┌───────┴──────────┐
                    │ catalogue-    │ ...  │ catalogue-service│
                    │ worker  (xN)  │      │ read API, under  │
                    │               │      │ a generated spec │
                    └───────────────┘      └──────────────────┘
```

| Directory | What it is |
|---|---|
| `catalogue-dump/` | the collection package: 12 scrapers, the crawl runner, the loader, the worker |
| `catalogue-control/` | operator API and the live event stream |
| `catalogue-service/` | the read API, under `catalogue.openapi.json` |
| `catalogue-explorer/` | the browser, including `/ops` |
| `deploy/quadlet/` | systemd units for production |

## Getting started

```sh
cp .env.example .env          # then set CATALOGUE_CONTROL_TOKEN
make install
docker compose up -d
docker compose --profile ui up -d     # the explorer on http://127.0.0.1:5175
docker compose --profile observability up -d  # Grafana :3001, VictoriaMetrics :8428
```

`make` on its own lists every target. `make check` is what a change has to pass.
The six initial alerts have operator steps in [the observability runbooks](docs/observability-runbooks.md).
Queue delivery, cutover, and broker recovery are covered by the
[queue runbook](docs/queue-provider-runbook.md).

## The design decisions worth knowing

**NATS JetStream delivers work; PostgreSQL is the system of record.** Job
creation writes a transactional outbox beside authoritative run state, a
dispatcher publishes compact generation-fenced references, and workers reserve
an execution token in PostgreSQL before touching a source. JetStream redelivery
is safe while `listen`/`notify` continues to provide UI live push.

**Edges go in the event log; levels do not.** A job failing is an edge:
discrete, ordered, replayable from `Last-Event-ID`, bad to miss. A job's
counters are a level: only the latest reading means anything. Progress is
therefore written in place and never to `catalogue.event_log` — putting it there
would make the log ~860,000 rows per run and destroy replay.

**LISTEN is a hint, never the queue.** Notifications carry an id and nothing
else, and the control service keeps a watermark it reconciles every five
seconds. A dropped notification costs latency, never data.

**Politeness is per host, per shared edge, and crosses processes.**
`catalogue.hosts` and `host_leases` bound concurrency per shop however many
workers are running. Getting a source blocked costs more than any feature here
is worth. A hostname is not always the whole story: nineteen of these shops are
Shopify storefronts on custom domains and all of them answer from one edge that
meters by client address across every shop on it, so those jobs claim a slot
under `edge:shopify` as well as under their own host. Both keys are ordinary
rows in `catalogue.hosts`, so either bound is an operator's to widen without a
deploy.

**The generated OpenAPI documents are never hand-edited.** Change the Pydantic
registries, run `make openapi`, commit the diff. `make openapi-check` fails the
build on drift, and a test asserts the read API's document contains no operation
other than `get`.

**The design system is a dependency, not a copy.** Colour, type, the component
layer and the mark all come from `@makersbrain/brand`, which is its own
repository and its own package. Nothing here regenerates any of it — a change to
the system is a version bump, and `package-lock.json` records which one this app
is on.

It is published to GitHub Packages rather than npmjs.com, so npm has to be told
where the scope lives. `catalogue-explorer/.npmrc` carries that mapping and no
credential; a person keeps a token with `read:packages` in `~/.npmrc`, and CI
writes one from the workflow's own `GITHUB_TOKEN` before `npm ci`.

**Two palettes meet in `app.css`, and the split is deliberate.** The interface —
surfaces, controls, type — is the brand's. The charts and the grid are not: those
values are the validated data-viz reference set, whose separations were measured
under protanopia, deuteranopia and tritanopia against these particular surfaces.
Replacing them with brand colours would throw that away, so the brand dresses the
interface and the data palette dresses the data. `--accent` belongs to the
interface and `--accent-data` to the charts; they are different colours doing
different jobs and the rename exists so one name cannot mean both.

**The golden files are how "no behaviour change" is checked rather than
claimed.** `make test-golden` replays every source the recorded response cache
covers and compares the output against a frozen digest. During the refactor they
caught a typed-config change that silently flipped two scrapers' defaults and
dropped one source from 49 records to 40.

The cache is not in the tree — it is 638 MB, published to R2, and fetched with
`make cache-pull`. Without it the suite skips rather than fails, so a golden run
on a fresh clone asserts nothing until the cache is pulled.

## Testing

| Command | Covers |
|---|---|
| `make test` | the fast suite: no network, no database |
| `make pg-up && make test-postgres` | the queue, run closure, the stream, the loader |
| `make cache-pull` | fetch the recorded response cache the golden suite replays |
| `make test-golden` | replay every cached source against its frozen dump |
| `make check-all` | all of the above |
