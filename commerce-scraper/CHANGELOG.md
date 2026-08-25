# Changelog

All notable changes to `mb-commerce-scraper` are recorded here. The format is
based on Keep a Changelog, and releases follow the compatibility policy in
[`docs/public-api.md`](docs/public-api.md).

## [Unreleased]

### Added

- Dataset-neutral connector, runtime, middleware, proxy, checkpoint, telemetry,
  schema, and testing contracts.
- Built-in Shopify, WooCommerce, PrestaShop, BigCommerce, Wix, Shopware,
  Starweb, NitroSell, SumUp, and generic-page connectors.
- Explicit external connector entry points and clean installed-wheel consumer
  verification.
- Provider-neutral routing with durable authorization seams, HTTP/browser
  accounting, bounded health, and application-owned Decodo and Webshare
  adapters.
- Public API manifest, versioned schemas, authoring/integration/migration
  guides, and release verification.
- Public `ResponseCache` extension protocol and a bounded, atomic,
  standard-library `FileResponseCache` with fresh/stale lookup and borrowed,
  stateless runtime ownership.
- Canonical default and per-collection `ProxyPolicyConfig` runtime composition,
  keeping route selection, geography, provider preference, and request/byte
  caps in one validated policy.
- End-to-end request/attempt correlation across retry, rate-limit, browser, and
  proxy lifecycle events, typed browser routing decisions, observable retry
  backoff, and terminal events for cleanup/cache-write failures.
- Public telemetry-hook and PrestaShop partition-declaration composition
  contracts for applications integrating the library without private imports.
- Typed `RequestObserver` and `RequestObservation` contracts for request spans,
  metrics, status, route, and physical-attempt accounting without reconstructing
  typed values from the general event dictionary.
- Public `build_checkpoint` construction using the canonical collection
  fingerprint and schema-v1 checkpoint identity.
- Public immutable `ConnectorPlan` metadata and `ConnectorRegistry.plan()` for
  deriving connector partitions, dynamic declarations, and effective browser
  requirements without constructing a connector or performing I/O.

### Changed

- Expanded neutral availability to preserve limited and discontinued published
  states, and aligned page connectors with legacy browser-profile requests,
  breadcrumb projection, JSON-LD entity decoding, and cleaned descriptions.
- Aligned direct BigCommerce discovery and GraphQL requests with the legacy
  browser profile and stable 50-item query identity used by recorded caches.
- Aligned direct PrestaShop page requests with the legacy browser profile and
  excluded consent/tracking tables from published technical specifications.
- Preserved the established PrestaShop radio-option boundary and exposed
  per-variant image lists for compatibility projection.
- Preserved WooCommerce source category slugs, default-variant identity,
  identity-catalogue currency, empty safety attributes, and product references
  needed by compatibility projection.
- Aligned direct Wix product-page requests with the legacy browser profile used
  by recorded caches.
- Consolidated runtime and routed-transport proxy configuration on
  `ProxyPolicyConfig`; the unreleased parallel routing model and legacy request
  and byte-cap parameters were removed.
- Removed version-0 catalogue checkpoint decoding. Commerce-scraper lineages
  now reconstruct schema-v1 checkpoints directly from durable identity;
  older or malformed cursor shapes restart instead of being upgraded.
- Consolidated built-in request validation, checkpoint construction, and
  stateless connector factories; options are now validated exactly once by the
  registry before factory construction.
- Unified middleware cache-limit rejection and terminal attempt-failure
  telemetry, including a consistent failure stage across response limits,
  transport errors, cancellation, backend errors, cache writes, and cleanup.
- Replaced duck-typed optional transport capabilities with runtime-checkable
  request-scoped rotation and browser-authorization protocols, with shared
  forwarding through transport wrappers.
- Moved the shared page-collection engine out of the vendor connector module
  and consolidated generic and specialized connector configuration on one
  nested discovery options model. Vendor discovery fields now live under
  `discovery`, matching generic-page configuration.
- Precompiled Shopify inventory extractors and reduced published-theme HTML
  parsing to one scan per supported encoding, independent of variant count.
- Added request-scoped stale-cache lookups so middleware computes cache
  identity once and reads an expired artifact once through revalidation.
- Tokenized opening tags and attributes once per verified DOM product instead
  of rescanning and reparsing the document for every configured field.
- Bounded owned HTTPX connection pools to 32 origin/address clients with
  concurrency-safe LRU eviction, active-stream draining, and prompt closure.
- Added a bounded per-host DNS cache to `URLPolicy`, with successful public
  resolutions retained for 60 seconds and concurrent lookups coalesced.
- Shopify inventory enrichment now runs each configured batch concurrently,
  with the per-origin limiter enforcing the in-flight ceiling. Budget-aware
  admission retains capacity for the next discovery page, results merge in
  product order, and identity rotation waits for the batch to settle.

Before tagging the first release, move these notes under
`## [0.1.0] - YYYY-MM-DD` and retain an empty `## [Unreleased]` section above
it. The release verifier rejects an undated or missing version entry.
