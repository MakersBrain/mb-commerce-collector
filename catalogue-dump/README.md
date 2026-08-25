# Catalogue dump tool

Collects public ceramic-materials listings from the configured suppliers into an
auditable local dump. It is an importer, not a checkout client and not a price
authority.

Every supplier has its own scraper. The shared code handles fetching, politeness
and the record contract; anything site-specific lives in that site's scraper.

## Layout

An installable `src`-layout package, `mb_ceramics_catalogue`. That matters because
the worker imports it from inside a container: the modules used to resolve only
because Python puts the running script's directory on `sys.path`, which works
for `python3 dump.py` and not for anything else.

```
src/mb_ceramics_catalogue/
  cli/            dump.py  load.py  probe.py  worker.py   -- console scripts
  config/         sources.py (SourceConfig)  settings.py (CrawlParams, Settings)
  crawl/          runner.py  session.py  progress.py  artifacts.py
  scrapers/       the 4,700 lines of site-specific collection
                  enrichment.py: the derived fields, as modules a source selects
  storage/        postgres.py  history.py  schema/
  observability/  logging.py  tracing.py  metrics.py
  ui/             dashboard.py (rich)  interactive.py (textual)
```

Commands, all installed by `uv sync`:

| Command | Does |
|---|---|
| `catalogue-dump` | collect the configured sources into a dump directory |
| `catalogue-load` | load a dump directory into PostgreSQL |
| `catalogue-probe` | run one scraper and report what it yields |
| `catalogue-worker` | claim jobs from the queue, crawl and load them |
| `catalogue-shadow-compare` | compare two existing NDJSON artifacts without crawling |

## Running it as a service

The pieces above are the collection. Around them:

| Piece | What it is |
|---|---|
| `catalogue.runs` / `jobs` / `job_progress` | durable run state; Postgres is the queue |
| `catalogue-worker` | claims a source, crawls it, loads it, reports it |
| `../catalogue-control` | run/job/worker/source control and the live SSE stream |
| `../catalogue-explorer/src/routes/ops` | the browser view of all of it |

```sh
docker compose up -d                      # database, read API, control, workers
docker compose up -d --scale worker=3     # more workers; the queue does not care
docker compose --profile ui up -d         # plus the explorer, on :5175/ops
```

Three sharp edges worth knowing:

- **The cache setting decides whether a run prices anything.** `--cache-mode
  auto` with a stale max age replays yesterday's pages and reports success
  while changing no prices. The default max age is 20 hours for that reason,
  and the daily schedule sets `cache_mode=refresh` explicitly.
- **`catalogue.hosts` is what stops three workers tripling the load on every
  shop.** One slot per host by default; per-host overrides live in the table so
  they can be tuned without a deploy.
- **A hand-run crawl can leave the same record a scheduled one does.**
  `catalogue-dump --record` writes to `catalogue.runs` and shows up in `/ops`
  beside the 03:00 run.

### Testing

`make check` runs ruff, mypy and the fast suite. `make cache-pull` fetches the
recorded response cache, without which the next command has nothing to replay
and skips. `make test-golden` replays every source that the response cache
covers and compares the result against a frozen digest in `tests/golden/` — that is the suite that proves a refactor changed no
output, and it is how the two `config.get(key, True)` defaults that the typed
configuration silently flipped were caught. After an intended change to
collection, `make golden-update` rewrites those files and the diff is the review.

### Shadow artifact gate

`catalogue-shadow-compare legacy.ndjson connector.ndjson.gz` compares existing
artifacts by `external_id`; it never runs collection. Exit code 0 means parity,
1 means reviewed differences remain, and 2 means invalid input or rules. Output
is deterministic JSON for CI. Optional `--legacy-metadata` and
`--connector-metadata` summary files include request and record metadata in the
gate. Volatile fields and numeric tolerances must be declared in a reviewed
version-1 rules file, for example:

```json
{
  "version": 1,
  "ignore_fields": ["fetched_at"],
  "numeric_tolerances": {"price": {"absolute": 0.01}},
  "metadata_ignore_fields": ["finished_at"]
}
```

Inputs must be `.ndjson` or `.ndjson.gz`. Line, record, field-path, and sample
bounds are configurable; differences are sorted and sample values are redacted.

## Scope

The dump covers **ceramic materials**: glazes, underglazes, engobes, clay bodies,
porcelain, oxides, stains and raw materials. Kilns, wheels, tools, brushes,
bisque ware, books and equipment are out of scope.

Scope is decided per source by a category allowlist where the site publishes
usable categories, and otherwise by a multilingual classifier over the product
name and its categories. The classifier deliberately ignores the free
description: glaze copy mentions brushes, kilns, cones and shelves constantly,
and reading it rejects the very products we want.

## Records

One row per **purchasable variant**, in `ceramics.catalogue_item.v2`. A glaze
sold in 118 ml, 473 ml and 1 pint becomes three rows sharing a
`parent_external_id`. That is the only shape in which a price per litre compares
across suppliers.

Manufacturer catalogues that publish specifications without prices (Mayco) emit
`ceramics.catalogue_identity.v2` rows instead, which carry no offer.

| Group | Fields |
|---|---|
| Provenance | `source`, `external_id`, `parent_external_id`, `product_url`, `fetched_at`, `extraction_method`, `source_detail_level`, `raw` |
| Identity | `name`, `name_raw`, `name_parsed_from`, `product_name`, `variant_title`, `brand`, `brand_basis`, `manufacturer_sku`, `manufacturer_sku_basis`, `supplier_reference`, `gtin`, `description`, `category_path`, `image_url`, `all_image_urls` |
| Commercial | `price`, `currency`, `price_text`, `list_price`, `vat_status`, `vat_rate`, `unit_price`, `availability`, `stock_quantity`, `min_order_quantity`, `package_size` |
| Ceramics | `family`, `form`, `firing`, `surface`, `effects`, `colour`, `application_methods`, `coats`, `claims`, `documents`, `technical_attributes` |

## Enrichment

The Ceramics row above is the one group nobody published. `price`, `stock` and
`name_raw` are read off the page; that a product is a glaze, fires to cone 6 and
is sold in 473 ml is **inferred**, and only means anything for a shop that sells
ceramic materials. Run over a potter selling finished mugs, the same code reads
a colour off a title and an application method out of the word "carafe".

So it is opt-in, per source, by name (`scrapers/enrichment.py`):

```json
"enrichments": ["ceramic-materials"]     the bundle every supplier here uses
"enrichments": ["classification", "firing"]   or only the parts that apply
```

| Module | Fills |
|---|---|
| `classification` | `family`, `form` — and with them the materials scope filter |
| `firing` | `firing` |
| `glaze` | `surface`, `effects`, `colour`, `application_methods`, `coats` |
| `packaging` | `package_size`, and through it `unit_price` |
| `claims` | `claims` read from prose (a spec-table claim is extraction, and always kept) |

A source that names none gets none: those fields are null and the row is only
what the shop said. `scope: materials` adds `classification` regardless, since
that is the field the scope filter reads.

## Titles

A storefront writes a title as a sentence — a description, the maker, the
maker's code, a colour and the pack size, in whatever order suits it:

```
Émail à effets pour grès Amaco - KI18 Artic Blush - 472ml 472
1050 UNDERGLAZE BASE size: $/GAL
011SF2502 - GRES BIANCO 11SF0-0,2 WITGERT
```

Stored whole, that is neither searchable nor comparable; stored parsed, it is no
longer what the supplier published. So both are kept. `name_raw` is the title
exactly as it arrived and is never edited. `name` is what remains once the
packaging wording, the firing range, the leading article number and a trailing
maker token have been lifted out, and `name_parsed_from` lists what was lifted.

Two things come out of the same reading:

- **The maker.** A retailer's own label is not the manufacturer: Ceradel sells
  AMACO and Mayco under `brand: "Harry-Ceradel"`, and on more than a thousand of
  its rows the real maker is named in the title and nowhere else. A maker named
  in the title wins, and `brand_basis` records that it came from there rather
  than from a published field.
- **The code.** A retailer's article number is still never promoted to a
  manufacturer code. It is promoted only when the shop *is* the manufacturer
  (`is_manufacturer` in `sources.json` — "1050 UNDERGLAZE BASE" on
  spectrumglazes.com is Spectrum 1050) and no other maker is named on that row,
  or when a named maker's own pattern matches. SiO-2's `303020000607` for a
  Colorobbia engobe stays SiO-2's article number, because it is one.

Measured across all 47,000 collected records, this reads a manufacturer code on
12,835 rows where 9,994 had one, and a brand on 22,473 where 16,676 had one. In
the loaded database 1,163 codes are now carried by two or more suppliers, which
is the surface every cross-supplier comparison is drawn from.

Notes on the derived fields:

- `firing` reads Celsius and Fahrenheit ranges, Orton cones (`cone 06 to cone 10`)
  and German Segerkegel (`SK 6a`), keeps the matched wording as `evidence`, and
  records whether a temperature was `derived_from_cone`.
- `package_size` normalises to millilitres or grams. A bare `oz` is read as fluid
  ounces only for liquids; otherwise it is weight and flagged `unit_ambiguous`.
- `unit_price` is per litre or per kilogram, computed from the observed price and
  package size.
- `manufacturer_sku` is only filled when a known manufacturer is actually named,
  so a retailer's internal reference is never promoted to a cross-supplier key.
- `claims` record food-contact, lead-free and similar wording as **supplier
  claims** with their evidence text. They are never a certificate, a laboratory
  result or a reviewed compliance fact. Polarity is read carefully: a
  `not-dinnerware-safe` icon yields `claim: false`.
- `vat_status` is a configured property of each storefront unless the page states
  it (Art4Fun publishes it in the page markup; Keramik-Kraft prints gross and net).

## Per-source strategy

APIs are preferred wherever a site has one. Browser instrumentation is used only
where the server will not hand over the data.

| Sources | Scraper | How |
|---|---|---|
| ceradel, penguin-pottery, ceramique-peinture | `shopify` | `/products.json`, full variants |
| les-cousins, mayco, spectrum, keramiekenglazuur, menomuza, solutions-ceramiques | `woocommerce` | Store API, with one bulk `type=variation` pass |
| amaco, speedball | `bigcommerce` | Storefront GraphQL, token read from the public page |
| 1240-design, ceram-decor, colpaert-online | `prestashop` | `data-product` JSON, every size fetched, JSON-LD fallback |
| sio-2 | `sio2` | PrestaShop, narrowed to SIO-2 materials plus all stocked glazes |
| e-cibas | `wix` | Products sitemap plus server-rendered JSON-LD |
| keramikbedarf-online | `shopware` | Category crawl, properties table |
| art4fun | `starweb` | Category crawl, VAT basis read from the markup |
| ceramicolours | `ceramicolours` | Browser-driven pack pricing (see below) |
| keramik-kraft | `keramik_kraft` | Category crawl (see the robots.txt note) |

A PrestaShop page renders one combination and hides the rest behind the variant
selector, so the scraper asks for each of the others through the shop's own
`?group[N]=X&ajax=1&action=refresh` call. Without that pass a product's second
size is missing entirely, and it is missing selectively: an out-of-stock 3.8 L
never appears while its in-stock 472 ml sibling does. Each combination carries
its own reference, price and stock, and the size the storefront publishes only
inside that selector is what makes a unit price possible at all. Set
`variant_combinations: false` on a source to skip the extra requests.

Two sites genuinely require the browser:

- **amaco** — the CDN fingerprints the TLS handshake and refuses every Python
  HTTP client while serving the same public pages to a real browser. The
  storefront token and the GraphQL calls are issued from inside a loaded page.
- **ceramicolours** — per-pack prices exist only after the page's own
  `updatePrice()` runs. Each pack option is selected in the browser and the
  displayed figure recorded. Its pricing is **not** linear (25 kg is 17.13 EUR/kg
  where 1 kg is 26.65 EUR/kg), so prices are never derived by multiplication.

## robots.txt

`robots.txt` is fetched and applied per RFC 9309: 2xx applies the published
rules, 4xx means no restrictions were published, and 5xx is treated as a
disallow. A CDN that rejects the declared research agent is asked once more as an
ordinary browser before any conclusion is drawn.

`keramik-kraft` publishes a `robots.txt` that allows only named search engines
and disallows everything else. It is collected because the operator explicitly
decided to, which is recorded in `sources.json` as `ignore_robots` together with
a reduced request rate and a note stating why. No other source sets it, and the
manifest reports `robots_ignored` for any source that does. Nothing here touches
a login, cart, CAPTCHA or any other access control.

## Usage

Install the importer and the Camoufox runtime once:

```bash
uv sync --directory catalogue-dump
uv run --directory catalogue-dump python -m camoufox fetch
```

On NixOS, run anything browser-backed through the supplied shell so Firefox
finds GTK and its other runtime libraries:

```bash
nix develop path:catalogue-dump --command \
  uv run --directory catalogue-dump catalogue-dump --source all --out catalogue-dumps
```

The opt-in callback-ordering integration uses the packaged browser-worker
runtime, synthetic local credentials, and an authenticated loopback proxy. It
does not contact Webshare or enable the durable route:

```bash
make test-camoufox-live
```

Common invocations:

```bash
catalogue-dump --source all --out catalogue-dumps
catalogue-dump --source les-cousins,spectrum --limit 50 --dry-run
catalogue-dump --source all --out catalogue-dumps --history-db catalogue-dumps/history.sqlite3
catalogue-dump --source all --browser never      # skip every browser-backed source
```

`--limit` caps products per source for sampling. `--dry-run` writes nothing.
`--browser never` disables rendering, which leaves amaco and ceramicolours empty.

## Pace

| Flag | Default | Meaning |
|---|---|---|
| `--sources` | 4 | sources crawled at the same time |
| `--concurrency` | 8 | most requests in flight per host |
| `--delay` | 0 | seconds one slot waits between its own requests |

**There is no wait between requests by default.** A host that answers is asked
again immediately, and the only limit is how many requests are in flight. A
fixed wait spends real minutes on every source to protect a host that has shown
no sign of strain, and an API returning a hundred products per call is not
helped by it.

The brake is the response instead. Slots start at 2, gain one after three
consecutive good responses up to `--concurrency`, and **halve on any error** — a
429, a 5xx, a timeout or a refused connection. A host that errors also earns a
gap between its requests, starting at 0.5 s and doubling with each further
failure up to 8 s; both the lost slots and the gap are released once it has
answered its way back to full speed. So a tolerant site runs flat out, and a
struggling one is backed off within a request or two without anyone tuning a
number by hand.

Anything jittered is jittered for a reason: an exact metronome is both easy to
fingerprint and needlessly bursty against a shop's cache.

A run's wall clock is mostly requests now, which was not always true: the claim
patterns used to be quadratic and parsing cost more than fetching. If a run ever
feels slow again, check whether it is network-bound at all — `--cache-mode
replay` re-runs the parsing with no network, so the difference between the two
timings is the answer.

**robots.txt.** `Disallow` is a rule and is always obeyed. A published
`Crawl-delay` is a pacing request, and this crawler treats it as a fallback: it
is remembered but unused while the host answers normally, and adopted the moment
that host returns an error. That is a deliberate operator decision — a site that
publishes a 10 s delay is crawled faster than it asked for as long as it shows
no sign of strain. A `delay` configured on a source (keramik-kraft asks for a
slow rate) is a hard floor from the first request and is never divided by the
slot count.

```bash
catalogue-dump --source all --sources 10 --concurrency 20   # as fast as they will take
catalogue-dump --source all --delay 0.5                     # deliberately gentle
catalogue-dump --source keramik-kraft                       # its configured floor still holds
```

To develop or tune one scraper, `catalogue-probe` runs it against the live site and
prints field coverage without writing a dump:

```bash
catalogue-probe les-cousins --limit 25 --show 1
```

## Cache and resume

Every response — HTTP, browser-rendered page, or browser-issued API call — can
be written to a cache directory and replayed later:

```bash
catalogue-dump --source all --cache                  # record while crawling
catalogue-dump --source all --cache --cache-mode replay   # reparse, no network
```

`--cache` defaults to `catalogue-dump/.cache`, one gzipped JSON per request
under `<host>/<shard>/`. The key covers the method, URL, body and whether the
request was made as a browser, so nothing is served back to a request that asked
differently.

| `--cache-mode` | Behaviour |
|---|---|
| `auto` (default) | replay entries younger than `--cache-max-age` (168 h), fetch the rest |
| `replay` | never touch the network; a request that was never recorded is reported as an error for that page and the run continues |
| `refresh` | fetch everything and overwrite what is stored |

This is what makes **reparsing** cheap: changing a regex and re-running with
`--cache-mode replay` reads a thousand pages off disk in a second instead of
asking a shop for them again. It is also what makes a run **resumable** — after
a Ctrl-C, rerun the same command and the pages already fetched come from disk
while only the missing ones are requested.

An interrupted run keeps what it had collected: each source's rows are written
to `<source>.partial.ndjson` and the manifest marks it `interrupted`. The
complete dump is never replaced by a half-finished one, because a run that
stopped early is not a smaller catalogue — it is an incomplete one, and letting
it overwrite would quietly delete products that are still for sale.

### Sharing the cache

The golden tests replay the cache, which makes it a build input: CI and every
developer have to be able to get the same one. It is 638 MB, so it is published
to Cloudflare R2 as an immutable tarball rather than committed.

```sh
catalogue-cache-archive push          # tar the local cache, upload, write the manifest
catalogue-cache-archive push --host shop.example --host other.example
catalogue-cache-archive pull          # fetch the archive this commit expects
catalogue-cache-archive verify        # the object is still there and the right size
```

`cache-archive.json` is checked in and names exactly one archive, so a commit
and the cache its golden files were frozen against travel together — an older
commit still pulls the archive it was written from. The key contains the digest
of the tar, so a push never overwrites the archive an older commit names, and a
pull verifies what it downloaded before unpacking a byte of it.

`push` needs a key that can write the bucket; `pull` needs one that can only
read it, which is all CI is given. Both come from `CATALOGUE_CACHE_BUCKET`,
`CATALOGUE_CACHE_ENDPOINT` and the standard `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`, and never from the command line. Install the extra
that carries the S3 client with `uv sync --extra archive`.

Repeat `--host` on `push` to publish a reviewed replay subset instead of every
locally recorded storefront. The manifest records only those hosts, and each
selected host must exist; omit the option to retain the full-cache behavior.

Pull replaces the local cache rather than merging into it. A cache holding some
hosts and not others is worse than none at all: `cached_sources()` selects any
source whose host directory exists, so a partial cache turns the golden suite
from honestly skipping into failing on every source it half-covers.

## Progress

On a terminal the run opens an interactive view: a scrollable table of sources
over a scrollable log.

| Key | Does |
|---|---|
| `up` / `down` / `page up` / `page down` | move through the sources |
| `enter` | open the source under the cursor |
| `l` / `s` / `tab` | focus the log, the source list, or the next pane |
| `f` | stop the log jumping to the newest line, so it can be read |
| `q` or `ctrl-c` | stop the run and keep what it collected |

Each row carries the source's scraper, its extraction method, status, records,
requests, request rate and error count. A source that falls back to the browser
shows `dom+browser`, so a silent change of strategy is visible while it happens
rather than only in the manifest afterwards.

Opening a source answers the question a table cannot: **what is it doing right
now**. The detail view lists the requests it has in flight with how long each
has been waiting, the last ten it finished with their timing and status code,
and that source's own log lines — which otherwise means tailing a log file and
grepping for a source name while the run is still going.

Requests are attributed to sources through a context variable rather than by
passing a name down every call, since each source already runs in its own task.

Two fallbacks, both automatic: `--plain-progress` gives the redrawing table,
which needs no key handling, and redirected output gives one log line per event,
since a live display in a log file is unreadable. `--no-progress` turns the
display off entirely.

## Output

```text
<out>/<source>.ndjson
<out>/manifest.json
```

Files are replaced atomically. An empty run cannot overwrite an existing
non-empty dump unless `--allow-empty` is passed; `write_status` records what
happened. The manifest reports per source: records, requests, rendered pages,
truncation, errors, notes, `robots_ignored` and a `field_coverage` count, so a
scraper that has silently gone thin is visible.

Prices are observations. They can be VAT-inclusive or exclusive, package prices,
volume prices or regional prices; the raw record and source URL are kept so an
operator can classify them before import.

Online dumps keep remote media and document URLs. The separate reference-media
ingestion workflow downloads, validates, hashes and uploads accepted immutable
masters to private R2 storage; this crawler does not upload publisher bytes.

## Tests

```bash
python -m unittest discover -s tests -t .
```

These are offline: they cover the ceramics field parsing, the scope rules, the
record contract and the source configuration. They make no network requests.

## PostgreSQL reference catalogue

[`catalogue-schema.sql`](src/mb_ceramics_catalogue/storage/schema/catalogue-schema.sql)
defines the whole shared PostgreSQL `catalogue` schema used to load the NDJSON
exports — tables, functions, the promotion rule and the seed rows, in one file.

```bash
psql -d ateliera -f catalogue-dump/src/mb_ceramics_catalogue/storage/schema/catalogue-schema.sql
```

It was eleven versioned files applied in a hand-maintained order until they were
squashed into this baseline, generated as a `pg_dump --schema-only` of a database
that had applied all of them. Regenerate it the same way rather than editing it
by hand. `catalogue-migrate` applies it to a database that has none of it and
adopts one that already does; it cannot migrate a database left part-way through
the old sequence, and says so rather than guessing.

`pgcrypto` must be reachable from the loader's `search_path`
(`pg_catalog, catalogue`), so the schema installs it into `catalogue` itself —
not into `public`, where `digest()` will not resolve.

Both the v1 and v2 record formats load unchanged. `source_products` carries
`parent_external_id`, `manufacturer_sku` and `supplier_reference`,
`offer_observations` carries `unit_price`/`unit_price_per`, and
`catalogue.offer_comparison` gives the latest offer per source product keyed by
manufacturer code. Fields not promoted to columns (form, surface, effects,
colour, claims, documents, technical_attributes, category_path) stay queryable in
`source_products.attributes`.

The normalized model deliberately separates:

- `source_products`: identities exactly as published by each supplier or PDF;
- `offer_observations`: append-only package, currency, and price observations;
- `raw_records`: the original JSON objects retained for audit and reprocessing;
- `canonical_products`: curated cross-supplier identities, never guessed by the importer;
- `source_documents` and `import_runs`: retrieval and batch provenance.

Load a whole dump with [`storage/postgres.py`](src/mb_ceramics_catalogue/storage/postgres.py), which stages each
file with `COPY` and then calls the loader over it. It needs `psql` on PATH and
no Python driver:

```bash
catalogue-load --dsn 'postgresql:///ateliera' --data catalogue-dumps-v2
catalogue-load --data catalogue-dumps-v2 --dry-run      # count only
catalogue-load --data catalogue-dumps-v2 --source amaco,mayco
```

Each invocation opens one `catalogue.import_runs` row and marks it `complete` or
`failed`, so a partial load is identifiable afterwards.

To load a single record by hand, inside one transaction:

```sql
select catalogue.load_record(
  p_record       => $1::jsonb,
  p_import_run_id => $2::uuid,
  p_document_id   => $3::uuid
);
```

`p_import_run_id` and `p_document_id` are optional. Identity-only records create
a source product and raw record without inventing an offer. Priced records create
an offer observation. Replaying an identical object is idempotent.

Search the imported reference catalogue with:

```sql
select * from catalogue.search_products('PC-20 blue rutile', 25);
```

Ateliera tenant products should link to a curated `canonical_products.id` in a
module migration. They should not foreign-key directly to a transient price
observation or copy a supplier price into product master data.

## Canonical products

`canonical_products` is the only layer with a stable cross-supplier key, and the
loader deliberately never fills it: an importer that guessed which similarly
named supplier rows are the same product would silently merge two different
glazes, and nothing downstream could tell that it had.

The schema supplies the missing curation as an explicit rule: the manufacturer
and alias seed rows, and `promote_canonical_products`. Run the promotion once the
schema is applied; it is idempotent and re-runnable.

```bash
psql -d ateliera -c 'select * from catalogue.promote_canonical_products();'
psql -d ateliera -c "select * from catalogue.promote_canonical_products('mayco');"
```

A supplier row is promoted only when its `brand` resolves, through a hand-written
alias, to a row in `catalogue.manufacturers`, **and** it carries a
`manufacturer_sku`. Both conditions are the point. `brand` is whatever the shop
printed: Ceradel sells AMACO and Mayco under `Harry-Ceradel`, and Ulster Ceramics
Pottery Supplies appears as a brand on its own listings — both with article
numbers that would pass for manufacturer codes. Only a person knows which of
those names belongs to a company that makes glaze, so `catalogue.manufacturers`
is an allowlist and `catalogue.manufacturer_aliases` collects the spellings
(`Mayco`/`MAYCO`, `Speedball`/`Speedball Art`, `Terracolor`/`Terra Color`).

On the current dump this promotes **1,748 identities across 6 manufacturers** and
links **7,463 supplier rows** to them. Nothing else changes: promotion adds a
curated identity and the link, and never edits, merges or retires a supplier row.

Fields are merged per field rather than taken from one row wholesale, because the
maker publishes a firing range and no price while a retailer publishes an image
and no range. Provenance beats completeness beats recency — except for the name,
which is ranked by length and *not* by provenance. A maker writes titles for its
own variant selector (Mayco publishes `Hot Tamale Size /Unit of Measure: Pint`),
while a retailer names the product because that is what a customer searches for.
`catalogue.clean_product_name` then strips a trailing pack or firing clause, from
a closed list of labels rather than a general "cut at the colon" rule that would
turn `Stroke & Coat: Hot Tamale` into `Stroke & Coat`.

`catalogue.canonical_catalogue` is the read contract for a tenant sync: one row
per canonical product and supplier variant, with that variant's latest offer.

```sql
select source_id, price, currency, vat_status, package_quantity, package_unit,
       round(unit_price, 2) as per_litre
  from catalogue.canonical_catalogue
 where manufacturer_sku = 'SC74'
 order by unit_price;
```

Two things it cannot tell you, and a consumer must handle: 44% of offers carry no
`vat_status`, and only 1,026 of 132,622 raw records carry a `vat_rate`. A net
price cannot be derived from those rows, so the VAT basis has to be configured
against the supplier rather than read off the offer.

## The read API

`catalogue-service/` serves the curated catalogue to tenants over HTTP. It is a
service in front of the database rather than a connection string handed to each
tenant, because this is cross-tenant reference data owned by none of them: there
is no write path at all, rather than a write path behind a permission.

```bash
docker compose up -d --build service
```

```text
GET /health
GET /v1/canonical-products?q=<text>&limit=<n>     search
GET /v1/canonical-products/<uuid>                 fetch one, with offers
GET /v1/canonical-products:batch?ids=<uuid,uuid>  fetch several, with offers
GET /v1/manufacturers
```

Search matches the name, the brand, the code, and the code with its punctuation
removed. That last one is not a nicety: AMACO stores `PC20` and prints `PC-20` on
the jar and in every catalogue it publishes, so the code a person actually types
is the one that would otherwise find nothing. Results carry a supplier count and
a unit-price range and are ordered by the count, because a code eleven shops
carry is more likely the one meant than a code one shop carries.

It is not published on the host. Consumers join the compose network and reach it
at `http://catalogue-service:8686`.

## Price history

Passing `--history-db` stores an append-only SQLite history alongside the NDJSON
dump. Each run is recorded in `catalogue_import_runs`; stable supplier-product
identities live in `catalogue_source_products`; and each distinct price context
is stored in `catalogue_price_observations`.

An identical consecutive price context is ignored, while a later price change
creates a new observation. The context includes amount, currency, VAT status,
package quantity and unit, so a package-size or VAT change is visible as a
change. Identity-only records create a product row and no observation.

```sql
select p.source, p.name, o.observed_at, o.price, o.currency,
       o.vat_status, o.quantity, o.unit, o.unit_price, o.unit_price_per
from catalogue_price_observations o
join catalogue_source_products p on p.id = o.product_id
order by o.observed_at desc;
```

## Extract PDF colour tiles

Manufacturer brochures often embed each fired colour tile as its own image.
Associate those images with the SKU coordinates in a guide and add an
`image_path` to each NDJSON record with:

```bash
nix shell nixpkgs#poppler-utils --command python3 \
  catalogue-dump/extract_pdf_tiles.py GUIDE.pdf DUMP.ndjson \
  --out catalogue-images
```

The extractor ignores grayscale masks and surrounding page artwork, converts
matched tiles to sRGB PNG, and stores them under
`catalogue-images/<source>/<sku>.png`. The PostgreSQL loader retains
`image_path` in `source_products.attributes`.
