# Commerce scraper connector and transport inventory

Reviewed: 2026-08-22
Authoritative source configuration: `catalogue-dump/sources.json`
Target architecture: [commerce-scraper-library-plan.md](commerce-scraper-library-plan.md)

This inventory records the migration classification and transport surface of
the configured commerce sources. Counts describe the checked-in source file;
they are not production-success or replay-parity claims.

## Configured source families

| Legacy scraper key | Sources | Neutral connector | Classification | Current execution status |
|---|---:|---|---|---|
| `shopify` | 19 | `shopify` v1 | Library built-in | Native library worker canary; stable route remains legacy |
| `woocommerce` | 25 | `woocommerce` v1 | Library built-in | Library implementation available; migration gates pending |
| `prestashop` | 11 | `prestashop` v1 | Library built-in | Library implementation available; migration gates pending |
| `sio2` | 1 | `prestashop` v1 plus catalogue projection policy | Library built-in | Library implementation available; migration gates pending |
| `bigcommerce` | 2 | `bigcommerce` v1 | Library built-in | Library implementation available; migration gates pending |
| `wix` | 2 | `wix` v1 | Library built-in | Library implementation available; migration gates pending |
| `shopware` | 1 | `shopware` v1 | Library built-in | Library implementation available; migration gates pending |
| `starweb` | 1 | `starweb` v1 | Library built-in | Library implementation available; migration gates pending |
| `nitrosell` | 1 | `nitrosell` v1 | Library built-in | Library implementation available; migration gates pending |
| `sumup` | 1 | `sumup` v1 | Library built-in | Library implementation available; migration gates pending |
| `pagecrawl` | 21 | `generic-pages` v2 | Library built-in/declarative | Options translate; replay and source migration pending |
| `axner` | 1 | `axner` v1 | Application-owned plugin | Library-contract native route; legacy path retained for rollback |
| `ceramicolours` | 1 | `ceramicolours` v1 | Application-owned plugin | Native route plus retained legacy rollback path |
| `keramik_kraft` | 1 | `keramik-kraft` v1 | Application-owned plugin | Library-contract native route; legacy path retained for rollback |
| **Total** | **88** | 88 library-constructable, 0 catalogue-only |  |  |

Axner, Ceramicolours, and Keramik Kraft are registered explicitly by the catalogue application
rather than as core built-ins or external entry points. No configured source
currently requires an external entry-point plugin; that contract remains
supported for independently distributed connectors.

## Required transport features

| Connector family | Required request surface | Browser | Discovery and partitioning | Special policy/accounting needs |
|---|---|---|---|---|
| Shopify | HTTP GET JSON feeds; optional HTTP product detail | Never | Main or configured collection partitions | Shared Shopify edge limiter; exact/unknown stock; bounded pagination |
| WooCommerce | HTTP GET REST/store APIs and bounded HTML evidence | Never | Main or configured category partitions | Variation/category pagination and optional cart-ceiling evidence |
| PrestaShop/Sio2 | HTTP HTML and embedded/API JSON | Optional | Connector-owned bounded page enumeration | Sio2 projection remains catalogue-owned; browser only when requested by options |
| BigCommerce | HTTP token/bootstrap document and GraphQL POST JSON | Optional | Single logical partition | Browser-context fallback must preserve request body and sticky route |
| Wix | HTTP robots/sitemap/XML and storefront JSON/documents | Optional | Advertised or configured sitemap discovery | Bounded gzip decode and browser fallback for client-rendered data |
| Generic pages | HTTP robots, sitemap/category discovery, product documents | Optional | Declarative sitemap/category/page strategies with resumable offsets | JSON-LD, Microdata, OpenGraph, and verified DOM parsers; parser identity is checkpointed |
| Shopware/Starweb/NitroSell/SumUp | HTTP sitemap/API/product documents | Optional | Shared bounded sitemap engine with platform parsing | Platform options select parsing/API behavior without changing runtime ownership |
| Axner | HTTP sitemap/listing and product documents | Optional | Versioned application discovery with library-owned checkpoint/resume | Site-specific application parser; all I/O uses library middleware |
| Ceramicolours | HTTP category/product documents plus typed browser evaluation for pack offers | Optional, including evaluate | Versioned category discovery; library-owned checkpoint/resume | Application parser and trusted evaluation action preserve non-linear pack totals; all I/O uses library middleware |
| Keramik Kraft | HTTP category-card documents | Optional | Versioned recursive application discovery; library-owned snapshot-offset resume | Bounded response handoff avoids duplicate physical fetches; all I/O uses library middleware |

All library HTTP requests pass through the same cache, robots, rate, retry,
budget, telemetry, URL-policy, and optional direct/proxy routing middleware.
Browser-required proxy requests require a lease-bound browser factory; CDP
extension proxy profiles remain direct-route only.

## Checked-in policy dimensions

The source file currently contains:

- 8 proxy-eligible sources;
- 3 sources explicitly forbidding rendering and none forcing it globally;
- 1 source forcing robots compliance and 1 deliberately ignoring robots with
  its checked-in justification;
- 12 sources with explicit sitemap configuration;
- 2 Shopify sources with configured collection partitions; and
- 6 sources with category filters.

These are application policy, not connector identity. Run parameters may make
policy stricter or reduce a budget, but may not enable paid routing or weaken a
source-owned restriction.

## Migration evidence still required

Library construction is not migration. Every stable source switch still needs
the same raw-response replay, neutral and ceramics projection comparison,
request/byte review, limited production canary, rollback proof, and observation
window tracked in the implementation status. The first Shopify and Shopware
replay gates are implemented and skip explicitly; the repository currently
lacks the raw response archive required to produce parity evidence.
