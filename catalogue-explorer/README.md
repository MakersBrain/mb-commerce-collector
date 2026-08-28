# Catalogue explorer

A small SvelteKit app over the local PostgreSQL `catalogue` schema — the one
`../docker-compose.yml` starts and `catalogue-load` fills.

```bash
npm install
npm run dev          # http://localhost:5173
```

For a standalone run, `npm run build` emits a Node server: `node build`.

The database URL defaults to the loopback publication of the compose stack,
`postgresql://catalogue:catalogue@127.0.0.1:5434/ateliera`. Override it with
`DATABASE_URL` (a `.env` file in this directory works).

Every query runs server-side in `+page.server.ts`; the browser never talks to
PostgreSQL and no credentials reach the client.

## Pages

| Route | What it does |
|---|---|
| `/` | Dashboard: totals, products per supplier, family mix, median EUR/L per supplier, widest cross-supplier spread. Filters: pack size, brand. |
| `/explore` | Faceted browse: product type, brand, product line, colour, surface, firing, application, form, supplier, in-stock only, plus a name search. Grid or sortable table, both paged. Every state - filters, view, sort - is a shareable URL. |
| `/compare?q=` | Offers for one manufacturer code across suppliers, one chart per pack band. |
| `/trends` | Reviewed purchased products across providers, with bounded price, exact-stock, availability, and numerical histories. Disabled by default. |

## Price and stock trends

Set `CATALOGUE_EXPLORER_TRENDS_ENABLED=true` to expose `/trends`. The reviewed
list lives in `config/tracked-products.json`; production mounts the same file
read-only. Its purchase references are provenance for review, while queries use
only canonical UUIDs. Duplicate IDs, malformed entries, unknown products, and
products without active provider variants fail closed.

The default range is 30 days. The page allows 7, 30, 90, or 365 days and reads
at most 500 observations per provider variant. A capped series is labelled.
Listed currencies are not converted or compared across currencies. Missing
stock stays unknown, and only `exact` quantities are drawn as inventory.

## The honest-comparison rules, in code

- **Cross-supplier identity is the manufacturer code**, never the product name.
  Everything comparative reads `catalogue.offer_comparison`, which only contains
  rows carrying one.
- **Prices are compared inside one pack band** (`src/lib/bands.ts`). Per-litre
  pricing is not linear in pack size, so ranking a gallon against a 59 ml jar
  would crown the gallon every time. The dashboard has a band filter; the
  compare page draws one chart per band.
- **Every price is converted to EUR** at the ECB daily reference rate
  (`src/lib/server/fx.ts`), so a USD listing takes part in a median or a
  cheapest-of comparison instead of being dropped. The rate date is stated on
  every page, the listed currency and amount stay beside the converted figure,
  and the last good rate table is stored in the database so a restart without
  network still converts. A reference rate is indicative, never a transaction
  rate.
- **Application and form** come from what the supplier published: brushing,
  dipping, pouring or spraying, and liquid versus powder (the dry mix). Coverage
  is partial - roughly a third of glazes name a method - so the facet counts are
  shown and an unlabelled product is never assumed to be brush-on.
- **Stock is the supplier's own claim** at collection time, shown as in/out with
  the quantity where one was published, never inferred from a missing price.
- **Product line** (Celadon, Cosmos, Potter's Choice, Stoneware) is derived from
  the manufacturer code prefix, and its human name is harvested from the
  manufacturers that publish it as a `(PC) Potter's Choice` style category. A
  prefix nobody labels shows as the bare prefix. See `src/lib/server/explore.ts`.

## Layout

One column on a phone, wider grids from 640px up. Wide content scrolls inside
its own box - the ten-column results table has `overflow-x: auto` and a minimum
width, so the page itself never scrolls sideways.

## Charts

Hand-rolled SVG/HTML in `src/lib/charts` rather than a chart library: bar
(magnitude, single hue, with an emphasis mode), stacked bar (part-to-whole),
dumbbell (low-to-high range). They follow one palette declared as CSS custom
properties in `src/app.css`, selected for both light and dark surfaces, with a
legend and a table view under every chart so no value is reachable only by
hovering.

Styling is Tailwind CSS v4 with daisyUI available; the chart cards deliberately
use the palette's own surfaces so the colours stay valid against the surface
they were checked against.
