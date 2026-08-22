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

Before tagging the first release, move these notes under
`## [0.1.0] - YYYY-MM-DD` and retain an empty `## [Unreleased]` section above
it. The release verifier rejects an undated or missing version entry.
