# Public API and compatibility policy

The supported Python import surface is the set of modules and names recorded in
[`public-api.toml`](../public-api.toml). The manifest is intentionally explicit
and is verified from the installed wheel. Use package imports such as:

```python
from mb_commerce_scraper import ProxyPolicyConfig, SourceDefinition
from mb_commerce_scraper.connectors import ConnectorFactory, ConnectorRegistry
from mb_commerce_scraper.proxy import ProxyPool, RoutedTransport
from mb_commerce_scraper.runtime import CommerceScraper, build_http_scraper
from mb_commerce_scraper.transports import (
    CommerceTransport,
    FileResponseCache,
    RequestObservation,
    RequestObserver,
    ResponseCache,
    TelemetryHooks,
    TransportRequest,
)
```

The root package provides the common connector and contract imports. The
subpackage surfaces have these roles:

- `connectors`: connector/factory protocols, built-in implementations and
  explicit registry composition;
- `discovery` and `parsing`: extension protocols and reusable implementations;
- `models`: validated collection, entity, diagnostic, policy and checkpoint
  contracts;
- `proxy` and `transports`: application-neutral integration protocols and
  reusable routing/middleware implementations;
- `runtime`: the high-level borrowed-resource client and owned HTTP builder;
- `testing`: supported test helpers for third-party connector conformance.

`ConnectorPlan` is the factory-owned, I/O-free declaration of effective
partition keys, dynamic partition handling, and browser requirements for one
validated options model. Applications should call `ConnectorRegistry.plan()`
rather than reproduce connector partition or browser logic. The planning call
accepts the source base URL where a stable partition key depends on it, plus
explicit request partitions such as Shopify collection scope.

`TelemetryHooks` is the supported observer protocol for application-owned
structured tracing and debugging sinks. Low-level consumers may still use
public helpers such as `prestashop_partition_keys`, but registry planning is
the canonical application-composition path.

`build_checkpoint` is the supported constructor for connector-authored
schema-v1 checkpoints. It binds source, connector version, options, and
collection intent through the same canonical fingerprint used by
`validate_checkpoint`, avoiding connector-specific checkpoint assembly.

Telemetry sinks may additionally implement `RequestObserver` to receive typed,
secret-free `RequestObservation` values for physical-attempt accounting,
metrics, and spans. The general string event channel remains available for
collection, connector, cache, proxy, and lifecycle events. Observer failures on
either channel are isolated from collection correctness.

`ResponseCache` is the supported extension protocol for application-owned
caches. `FileResponseCache` is the standard-library, directory-backed
implementation. Its operations are asynchronous so filesystem work does not
block the collection loop, but the object owns no open handle, background task,
or other resource requiring an async close. A cache instance passed to
`CommerceScraper` or `build_http_scraper` is borrowed; leaving the scraper
context neither closes it nor removes its persistent entries.

Caches supporting validators implement `StaleResponseCache.get_with_stale()`.
Its `ResponseCacheLookup` result classifies one read as fresh or stale and
retains the request-specific write identity for revalidation or replacement;
middleware therefore neither reopens an expired artifact nor recomputes its
identity before writing the result. The original `get`, `stale`, and `put`
methods remain available on `FileResponseCache` for direct cache operations.

Entries and decompressed reads are bounded, and requests carrying credential
headers, credential query fields, or URL user information bypass both standard
caches to prevent cross-identity reuse. The directory can still contain raw
response bodies and must be treated as sensitive application data. Directory
retention and total-size pruning remain application-owned policy.

The filesystem artifact format is private and independently versioned. It is
not one of the frozen JSON contracts, and callers should interact through the
cache protocol rather than reading its files directly.

`ProxyPolicyConfig` is the high-level runtime contract for proxy mode,
geography, provider preference, and collection caps. The same policy is passed
to direct `RoutedTransport` composition, so low- and high-level routing use one
validated configuration shape.

Every supported namespace remains importable from the base installation. The
HTTP-facing factory symbols are still importable there, but constructing an
HTTP transport requires the `http` extra.

Imports from implementation modules such as
`mb_commerce_scraper.connectors.shopify`, `mb_commerce_scraper.proxy.httpx`, or
`mb_commerce_scraper.transports.middleware` are intentionally private even
when an object from that module is re-exported publicly. Files, names, and
behavior absent from the manifest may change without a compatibility window.
The JSON schemas under `mb_commerce_scraper.schemas` are data resources, not a
Python import namespace; their compatibility is governed by their own schema
versions.

## Versioning

The distribution begins at `0.1.0`. Before `1.0`, a minor release may contain
a necessary breaking API change; such changes must be called out in release
notes and should provide a deprecation path when practical. Patch releases must
remain backward compatible with the documented public surface.

Starting at `1.0`, the package follows semantic versioning:

- **major** releases may remove or incompatibly change supported imports,
  callable behavior, model fields, diagnostic codes, connector names,
  capabilities, or connector entry-point contracts;
- **minor** releases may add backward-compatible API and deprecate existing API;
- **patch** releases contain backward-compatible fixes and documentation.

After `1.0`, a public name is normally deprecated for at least one minor
release before removal. An urgent security or correctness issue may require a
faster removal and must be prominent in the release notes.

Serialized model, checkpoint, and schema contracts are versioned independently
from the package. A package-major release does not silently reinterpret an old
serialized contract, and an incompatible serialized change requires a new
contract or schema version. Connector parsing changes that alter extracted data
require release notes even if the Python API is unchanged.

Adding a public name requires adding it to `public-api.toml` and documenting
its role. Removing or renaming a manifest entry is therefore an explicit API
review, while the installed-artifact gate catches accidental export drift.
