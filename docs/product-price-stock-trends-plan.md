# Product Price and Stock Trends Plan

Status: MVP implemented; rollout gates remain disabled by default

Date: 2026-08-28

## Objective

Ship a useful dashboard for a small, reviewed set of products that shows price
and stock changes across providers. Reuse the catalogue's existing collection,
observation, canonical-product, and Explorer infrastructure.

This document separates the work into:

1. A lean dashboard MVP.
2. Optional production hardening after the MVP has real users and data.

## What the MVP provides

### Tracked-products overview

The overview contains one row per tracked canonical product:

```text
Product       Best unit price  7-day change  Stocked providers  Last checked
AMACO PC-20   18.40 EUR/l      -6.2%         3 of 4             12 min ago
Mayco SW-001  21.10 EUR/l       0.0%         2 of 3             18 min ago
Clay 10 kg     1.29 EUR/kg     +3.1%         4 of 5              1 h ago
```

Summary cards show:

- Number of tracked products.
- Price drops over the selected period.
- Products newly out of stock.
- Products newly restocked.
- Providers with stale observations.

### Product detail

Opening a tracked product shows:

- One listed-price line per provider.
- A normalized unit-price comparison when package data is trustworthy.
- A stock-quantity step chart when exact inventory is published.
- In-stock/out-of-stock history when a provider exposes only availability.
- A current provider comparison table.
- A chronological price and stock change table.
- Provider, variant, and date-range filters.
- The last successful observation and a stale-data warning.

The chart never treats missing stock as zero. Cart limits and ambiguous numbers
are not presented as exact inventory.

## Existing foundation

The catalogue already provides most of the MVP:

- `catalogue.offer_observations` stores append-only offer history.
- Unchanged states extend `observed_at` to `last_seen_at` intervals.
- Changed states preserve sequences such as A to B to A.
- Out-of-order records cannot regress current state.
- The Explorer product panel already draws price, unit-price, and availability
  history from `offer_observations`.
- Daily price refreshes use a cache age suitable for observing changes.

The MVP needs only two material additions:

1. Persist trustworthy stock quantity and quantity semantics with each offer
   state.
2. Add a curated tracked-product view that groups reviewed provider variants.

## MVP scope decisions

### Combined offer state

For the MVP, price and stock remain one combined observation stream in
`catalogue.offer_observations`.

A price or stock change creates a new row. This may repeat the same price when
only stock changes, but it is correct, easy to query, and compatible with the
current Explorer.

The MVP therefore does not support stock-only products that have no price. All
initial tracked products must have a valid priced offer.

### Primary offer only

Track the provider's selected primary/regular catalogue offer. Member,
quantity-tier, seller-specific, and overlapping validity-window offers remain
in connector dataset artifacts and are not combined into the MVP history.

### Curated identity

Start with a small reviewed canonical-product set (currently 29 products from
the purchase workbook and explicit clay additions). Map provider variants manually
using:

1. Manufacturer identity.
2. Manufacturer SKU or variant code.
3. Package quantity and unit.
4. Structured family and variant attributes.

Do not merge products from name similarity alone. Different sizes and variants
remain separate comparison series.

### Existing collection cadence

Continue using source-wide scheduled refreshes. The tracked-product list
controls presentation, not crawling. Targeted high-frequency product fetching
is deferred.

## MVP phase 1: Schema migration

Add one idempotent incremental migration after the currently registered schema
files. Do not rewrite the squashed baseline in the same change.

Add these columns to `catalogue.offer_observations`:

```sql
stock_quantity      bigint null
stock_quantity_kind text not null default 'unknown'
context_version     smallint not null default 1
```

Constraints:

- `stock_quantity >= 0` when present.
- `stock_quantity` is null exactly when kind is `unknown`.
- Allowed kinds are `exact`, `lower_bound`, `upper_bound`, `order_limit`, and
  `unknown`.
- Existing rows adopt `context_version = 1` and unknown quantity.
- New combined price/stock hashes use `context_version = 2`.

Register the migration in `storage/db.py`. Add migration-order, idempotency,
grant, and database-transfer tests. Fresh databases continue to apply the fixed
baseline followed by incremental migrations.

## MVP phase 2: Loader changes

Extend the production legacy catalogue loader first.

For each priced record:

1. Read price, currency, package data, availability, stock quantity, and stock
   quantity kind.
2. Persist an exact quantity only for scraper families whose extraction
   contract guarantees inventory. Otherwise preserve known availability while
   storing a null quantity with kind `unknown`.
3. Include price and stock facts in the version-2 combined-state hash.
4. Append a row when either price or stock changes.
5. Extend `last_seen_at` when the whole combined state is unchanged.

### Hash transition

Existing hashes include a different set of fields. The first post-deployment
collection must not create an unchanged duplicate merely because the hash
algorithm changed.

When the latest row is version 1, compare semantic columns rather than hashes.
If price, package data, and availability are unchanged and the new observation
also has unknown quantity, extend that row. If the new observation adds a
trustworthy quantity, append a version-2 row because it is the real beginning
of numeric stock history, not an artificial hash-only change. Set every other
new row to version 2.

Update offer compaction to compare semantic combined-state columns across hash
versions while continuing to preserve A to B to A transitions.

### Loader tests

- Existing row migration compatibility.
- First version-2 write without an artificial duplicate.
- Unchanged price and stock extends the interval.
- Price-only change.
- Stock-only change.
- Exact zero stock.
- Unknown stock.
- Non-exact quantity kinds are never converted to exact.
- A to B to A history.
- Concurrent and duplicate loads.
- Out-of-order input.
- Failed and partial runs.

## MVP phase 3: Tracked-product configuration

Use a reviewed configuration file rather than building watchlist CRUD.

Example:

```json
{
  "products": [
    {
      "canonical_product_id": "uuid",
      "label": "AMACO Potter's Choice PC-20"
    }
  ]
}
```

Validation must reject:

- Duplicate canonical IDs.
- Unknown canonical products.
- Products with no active provider variants.
- Provider mappings whose SKU, variant, or package evidence conflicts.

The file is deployment configuration, reviewed in Git, and mounted read-only
into Explorer. No public or control-plane mutation endpoint is required for the
MVP.

## MVP phase 4: Explorer queries

Add server-side Explorer queries; do not add a public catalogue-service endpoint
yet.

For each configured canonical product:

- Resolve active `source_products` by `canonical_product_id`.
- Read at most a bounded date range and observation count per source product.
- Return provider, variant, package, price, stock, observation interval, and
  freshness data.
- Order providers and timestamps deterministically.
- Keep all SQL parameterized.

Defaults:

- 30-day view.
- Maximum one-year range.
- Maximum 500 observations per provider series.
- A clear truncated-result indicator if a series reaches the bound.

Provider-local listed prices remain in their original currency. Cross-provider
unit-price comparison is shown only when currencies match or when a clearly
labelled conversion rate and date are available.

## MVP phase 5: Explorer dashboard

Add a `/trends` page behind
`CATALOGUE_EXPLORER_TRENDS_ENABLED=false` by default.

### Overview

- Summary cards.
- Tracked-product comparison table.
- Best current provider and price.
- 7-day and 30-day change.
- Provider stock coverage.
- Last observation and stale indicator.

### Detail

- Provider price overlay.
- Unit-price overlay.
- Stock step chart.
- Availability-only bands.
- Current offers table.
- Numerical change history.
- Provider and date filters.

Accessibility requirements:

- Every chart has an equivalent numerical table.
- Colour is not the only provider identifier.
- Unknown periods are visible gaps, not connected lines.
- Quantity-kind labels are present in tooltips and tables.

## MVP phase 6: Optional focused backfill

Starting history at deployment time is acceptable for the first release.

If historical context is required, backfill only configured tracked products.
Use `raw_records.first_seen_at` and `last_seen_at`; raw compaction may retain one
representative document whose embedded `fetched_at` is not the complete
interval.

The backfill command must:

- Default to dry-run.
- Process one reviewed provider/product mapping at a time.
- Insert observations chronologically.
- Preserve exact versus ambiguous quantity semantics.
- Be idempotent.
- Validate reconstructed latest state against the latest eligible raw record.

Run focused backfill before enabling new live stock writes for that tracked
source product. If live observations already exist, rebuild its history in a
shadow table and swap only after validation; do not feed historical rows
through ordinary out-of-order quarantine.

## MVP rollout

The reviewed initial list is derived from `~/Ceramics Purchase Overview.xlsx`:
the distinct invoice-backed Glaze entries, collapsed by canonical identity,
plus the explicitly requested PRAI and LUNA clay bodies. Supplier invoice SKUs
and descriptions are retained as `purchase_references` in the configuration.
WC-108 and LUNA require the explicit curation migration because their provider
records did not previously carry enough normalized manufacturer evidence for
automatic promotion.

1. Select and review 5–20 canonical products and provider mappings.
2. Merge and test the additive offer-observation migration.
3. Apply the migration in development.
4. Deploy loader support with stock persistence disabled by
   `CATALOGUE_STOCK_TRENDS_ENABLED=false`.
5. Optionally backfill the selected products and validate latest state.
6. Enable stock persistence globally; non-tracked products may begin collecting
   history, but only the reviewed tracked set is exposed by the dashboard.
7. Collect at least two successful scheduled observation windows.
8. Deploy the Explorer queries and feature-flagged dashboard.
9. Validate charts against the numerical history tables.
10. Enable the dashboard in development, then production after a soak period.

Rollback is additive: disable stock persistence and the Explorer feature while
leaving the new nullable columns and collected history intact.

## MVP acceptance criteria

- A tracked product shows multiple reviewed provider variants.
- Price and exact stock changes are visible over time.
- Availability-only providers remain useful without invented quantities.
- Exact zero is retained and displayed correctly.
- Bounds and order limits are never labelled exact.
- Unchanged states extend intervals rather than creating daily duplicates.
- A to B to A changes remain visible.
- The first version-2 collection does not create an artificial change.
- Different variants and package sizes are not incorrectly merged.
- Failed or partial runs cannot retire products or create valid-looking trends.
- Dashboard queries are bounded and parameterized.
- Every chart can be verified against its numerical table.

## Post-MVP hardening triggers

Do not implement the following merely in anticipation. Promote each item when
the corresponding need appears.

### Separate stock observations

Trigger: stock-only products must be tracked, price and stock require different
collection times, or combined observations grow materially because stock
changes much more frequently than price.

Then add a dedicated `stock_observations` table, `latest_stock` view, connector
evidence, stock compaction, historical migration, and an atomic read-model
cutover.

### Public trends API

Trigger: a consumer other than Explorer needs the history.

Then add bounded read-only endpoints consistent with existing naming:

```text
GET /v1/canonical-products/{id}/trends
GET /v1/source-products/{id}/trends
```

### Managed watchlist

Trigger: operators need to change tracked products without a deployment.

Then add an audited global watchlist managed through authenticated
catalogue-control endpoints. Keep the public catalogue service GET-only.

### Connector-dataset relational loading

Trigger: connector pipelines replace legacy loading for tracked production
sources.

Then add typed price/stock loaders, stable variant-to-source-product resolution,
observation IDs, evidence persistence, parity tests, and explicit primary-offer
selection. Do not silently collapse member or quantity-tier offers.

### Targeted collection

Trigger: tracked products need a higher cadence than full provider catalogues or
source-wide requests become too expensive.

Then add optional stable product selectors only to connectors that can fetch
them safely. Unsupported connectors continue source-wide refreshes.

### Alerts

Trigger: users need proactive price-drop, restock, or stale-data notifications.

Build alerts as consumers of verified history rather than coupling them to the
loader transaction.

### Full retention and compaction policy

Trigger: measured history growth or query latency requires it.

Add stock-specific compaction, raw-record FK rewiring, retention metrics, and
reviewed downsampling rules. Never collapse real A to B to A changes.
