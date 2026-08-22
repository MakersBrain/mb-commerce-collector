# Reusable Commerce Scraper Library Refactor Plan

Status: in progress
Target repository: `mb-commerce-collector`  
Proposed library distribution: `mb-commerce-scraper`  
Proposed Python import package: `mb_commerce_scraper`  

## 1. Purpose

Extract the reusable commerce-collection capabilities currently embedded in
`mb_ceramics_catalogue` into a standalone Python library that can be installed
and used by other projects.

The library must make it straightforward to:

- scrape common storefront frameworks such as Shopify, WooCommerce,
  PrestaShop, BigCommerce, and Wix;
- add support for another storefront framework without changing the core;
- describe a simple custom shop with discovery and parsing rules;
- implement a fully custom connector when declarative rules are insufficient;
- use direct HTTP, browser-backed collection, or both;
- route requests through multiple residential proxy providers;
- choose proxy routes by geography, health, cost, and fallback policy;
- collect neutral commerce entities rather than ceramics-specific records;
- resume interrupted collections with durable, versioned checkpoints;
- test connectors without accessing live shops or paid proxies.

This is an extraction and stabilization project, not a rewrite of all existing
scrapers. Existing production output must remain stable throughout migration.

## 2. Current state

The repository already contains several parts of the intended design:

- `connectors/base.py` defines a dataset-neutral connector protocol,
  capabilities, requests, pages, diagnostics, and checkpoints.
- `connectors/commerce.py` defines neutral product, variant, offer, stock,
  media, document, and evidence models.
- `connectors/shopify.py`, `woocommerce.py`, `prestashop.py`,
  `bigcommerce.py`, `wix.py`, and `pagecommerce.py` provide neutral connector
  implementations.
- `pipeline/runner.py` separates collection from dataset projection.
- `datasets/` demonstrates how neutral commerce snapshots can be projected
  into ceramics and generic commerce datasets.
- `ops/connector_adapters.py` is already a composition boundary between source
  configuration, legacy transports, and connectors.
- `providers/` models the control APIs of several proxy vendors.
- recorded-response golden tests and shadow comparison provide a migration
  safety mechanism.

The main limitations are:

1. Reusable code is distributed as part of `mb-ceramics-catalogue`.
2. Package names and some dependencies expose ceramics application concerns.
3. Connector construction is hard-coded in an application-owned registry.
4. `SourceConfig` combines operational settings and options for every
   storefront platform in one large model.
5. HTTP, browser, cache, robots, throttling, and proxy behavior are concentrated
   in the legacy `Fetcher` runtime.
6. Runtime proxy credentials assume provider-specific username formatting.
7. Proxy billing/control-plane logic and request routing are not cleanly
   separated.
8. Legacy and neutral implementations coexist, which is useful for migration
   but should not become the final public architecture.

## 3. Guiding principles

### 3.1 Neutral collection model

The reusable boundary is a commerce snapshot, not a ceramics catalogue row.
Ceramics classification, enrichment, filtering, and database projection remain
application concerns.

### 3.2 Connectors depend on capabilities, not implementations

A connector may request JSON, text, browser evaluation, sitemap discovery, or
session rotation through small protocols. It must not construct `httpx`,
Playwright, Camoufox, PostgreSQL clients, log exporters, or proxy-provider API
clients itself.

### 3.3 Explicit composition

Importing the library must not discover plugins, open network clients, start a
browser, read secrets, or mutate a global registry. Built-ins and third-party
plugins are loaded by an explicit runtime builder.

### 3.4 Stable contracts, replaceable infrastructure

Product models, connector requests, pages, diagnostics, checkpoint envelopes,
and extension protocols form the public API. Concrete HTTP clients, caches,
browser engines, schedulers, databases, and metrics exporters remain
replaceable implementations.

### 3.5 Provider-neutral proxy routing

The scraping library works with leases and routes. Provider-specific account
management, billing cycles, and SQL budgets stay outside the core runtime.

### 3.6 Behavior-preserving migration

Move one connector at a time and compare it with the existing implementation.
Do not remove the legacy path until recorded replay and production shadow gates
pass.

## 4. Scope

### 4.1 In scope

- Neutral commerce contracts and validation.
- Connector and connector-factory protocols.
- Built-in storefront connectors.
- Generic sitemap/category discovery and structured-data/DOM parsing.
- HTTP transport and optional browser transport integration.
- Request rate limiting, retries, caching hooks, robots policy, and budgets.
- Provider-neutral proxy endpoints, leases, pools, selection, rotation, and
  accounting hooks.
- Plugin discovery and explicit registration.
- Testing utilities and connector conformance tests.
- Documentation and runnable examples.
- Migration of `mb_ceramics_catalogue` to use the extracted package.

### 4.2 Out of scope

- Checkout automation, cart mutation, authentication bypass, or purchase APIs.
- CAPTCHA-solving services in the initial release.
- Centralized hosted proxy brokerage as part of the library.
- PostgreSQL schemas, NATS jobs, worker leases, or catalogue scheduling.
- Ceramics enrichment, matching, classification, or projection.
- Redesigning all catalogue operational APIs during extraction.
- Guaranteeing that arbitrary shops can be scraped without a custom connector.

## 5. Target repository layout

Keep the new distribution in this monorepo until its API and migration are
stable:

```text
mb-commerce-collector/
├── commerce-scraper/
│   ├── pyproject.toml
│   ├── README.md
│   ├── src/mb_commerce_scraper/
│   │   ├── __init__.py
│   │   ├── py.typed
│   │   ├── models/
│   │   │   ├── commerce.py
│   │   │   ├── collection.py
│   │   │   ├── diagnostics.py
│   │   │   └── checkpoints.py
│   │   ├── connectors/
│   │   │   ├── base.py
│   │   │   ├── factory.py
│   │   │   ├── registry.py
│   │   │   ├── shopify.py
│   │   │   ├── woocommerce.py
│   │   │   ├── prestashop.py
│   │   │   ├── bigcommerce.py
│   │   │   ├── wix.py
│   │   │   └── generic_pages.py
│   │   ├── discovery/
│   │   │   ├── base.py
│   │   │   ├── sitemap.py
│   │   │   └── category.py
│   │   ├── parsing/
│   │   │   ├── base.py
│   │   │   ├── jsonld.py
│   │   │   ├── microdata.py
│   │   │   ├── opengraph.py
│   │   │   └── dom.py
│   │   ├── transports/
│   │   │   ├── base.py
│   │   │   ├── httpx.py
│   │   │   ├── browser.py
│   │   │   └── middleware.py
│   │   ├── proxy/
│   │   │   ├── base.py
│   │   │   ├── pool.py
│   │   │   ├── routing.py
│   │   │   ├── health.py
│   │   │   └── static.py
│   │   ├── runtime/
│   │   │   ├── client.py
│   │   │   └── builder.py
│   │   └── testing/
│   │       ├── contracts.py
│   │       ├── fake_transport.py
│   │       ├── fake_proxy.py
│   │       └── recordings.py
│   └── tests/
├── catalogue-dump/
│   └── ... application, datasets, storage, workers and compatibility adapters
└── ...
```

Initially, copy or move code only when the destination package has tests and
the catalogue application is ready to import it. Avoid a long-lived fork of the
same connector in both packages.

## 6. Dependency boundaries

The dependency direction must be:

```text
models
  ↑
connectors ← discovery/parsing
  ↑
runtime ← transports ← proxy protocols
  ↑
catalogue application adapters
  ↑
ceramics datasets, storage, workers and APIs
```

Rules enforced by tests:

- `models` imports only the standard library and Pydantic.
- `connectors` may import models, discovery/parsing helpers, and transport
  protocols; it may not import concrete runtime or application modules.
- proxy contracts may not import a concrete provider SDK or catalogue SQL.
- concrete transports may import optional dependencies, but neutral contracts
  may not.
- the new library may not import `mb_ceramics_catalogue`.
- `mb_ceramics_catalogue` may import `mb_commerce_scraper`.
- optional browser and observability dependencies must not be loaded by a basic
  `import mb_commerce_scraper`.

Add AST-based boundary tests similar to the existing connector import tests,
plus a clean-environment import test against the built wheel.

## 7. Public domain contracts

### 7.1 Commerce snapshot

Retain the existing neutral product shape, with explicit nested models for:

- product identity;
- variants;
- offers and money;
- availability and stock evidence;
- categories;
- media;
- documents;
- published attributes;
- platform extensions;
- extraction evidence and observation time.

Contract rules:

- Models are immutable and reject unknown fields unless a field is explicitly
  designated as an extension map.
- Money uses `Decimal`, never binary floating point.
- Every emitted entity carries `source_id`, external identity, canonical URL,
  and observation time.
- Exact stock, inferred availability, and order limits remain distinguishable.
- Raw provider payloads are optional and bounded; connectors must not place
  secrets or complete response bodies into diagnostics.

### 7.2 Collection request

The public request should include only collection intent:

```python
class CollectionRequest(BaseModel):
    source_id: str
    base_url: str
    refresh_mode: RefreshMode
    requested_fields: frozenset[SnapshotField]
    result_limit: int | None = None
    partitions: tuple[str, ...] = ()
    deadline: datetime | None = None
```

Cancellation and checkpoint callbacks can remain runtime-only fields or be
placed in a separate execution context so the serializable request model stays
portable. Request budgets also belong in that execution context rather than in
the request. There is one attempt-scoped `RequestBudget` shared by the
connector and transport: connectors label work with its priority and whether it
is required, while the transport records the actual HTTP, browser, and proxy
cost of every network attempt, including retries. Authorization and charging
must not be duplicated at both layers. The migration adapter maps the current
priority- and cost-aware budget protocol into this interface; it must not
reduce it to a request-count integer.

### 7.3 Page and diagnostics

Continue yielding bounded pages rather than one unbounded list. A page must
state:

- stable page identity and sequence;
- partition key;
- emitted items;
- next resume cursor;
- whether the partition and collection are terminal;
- whether enumeration remained intact;
- discovered count;
- typed diagnostics.

Add diagnostic metadata cautiously. Public diagnostic codes should be stable;
provider-specific details belong in a sanitized metadata dictionary.

### 7.4 Checkpoints

Use a versioned envelope:

```python
class ConnectorCheckpoint(BaseModel):
    checkpoint_schema_version: Literal[1] = 1
    connector: str
    connector_version: str
    source_id: str
    lineage: str
    collection_fingerprint: str
    resume_after: JsonValue
```

Rules:

- A connector validates its own cursor before performing network work.
- A cursor created by another connector/version/source or collection
  configuration is rejected.
- `collection_fingerprint` is a deterministic digest of the normalized base
  URL, connector name and options, partitions, refresh mode, and requested
  fields. Result limits and deadlines are excluded so callers can safely
  continue with different operational bounds.
- `checkpoint_schema_version` versions the envelope independently of the
  connector and package versions.
- Cursor data must be JSON-serializable and contain no credentials.
- Connector version changes must either accept the old cursor explicitly or
  reject it with `CHECKPOINT_INVALID`.

## 8. Connector extension architecture

### 8.1 Connector protocol

Keep the existing async streaming model:

```python
class CommerceConnector(Protocol):
    name: str
    platform: str
    version: str
    capabilities: ConnectorCapabilities

    def collect(
        self,
        request: CollectionRequest,
        checkpoint: ConnectorCheckpoint | None = None,
    ) -> AsyncIterator[EntityPage[CommerceProductSnapshot]]: ...
```

### 8.2 Connector factory

Registration should operate on factories, not prebuilt connector instances:

```python
class ConnectorFactory(Protocol):
    name: str
    options_model: type[BaseModel]

    def build(
        self,
        *,
        transport: CommerceTransport,
        options: BaseModel,
        context: ConnectorContext,
    ) -> CommerceConnector: ...
```

`ConnectorContext` supplies a clock, the single attempt-scoped request budget,
telemetry hooks, and other attempt-scoped capabilities. It must not expose the
catalogue database or a global service container. Connector budget checks are
non-consuming affordability checks used to choose required versus optional
work; the transport authorizes and charges each actual attempt exactly once.

### 8.3 Registry

`ConnectorRegistry` is an ordinary instance:

```python
registry = ConnectorRegistry()
register_builtin_connectors(registry)
registry.register(MyConnectorFactory())
```

Required behavior:

- duplicate names fail with a clear error;
- names are normalized and validated;
- factories expose their options JSON schema;
- registry listing does not instantiate connectors;
- explicit registration overrides are prohibited by default;
- tests can create isolated registries without clearing global state.

### 8.4 Third-party plugins

Support optional Python entry-point discovery:

```toml
[project.entry-points."mb_commerce_scraper.connectors"]
my_platform = "my_scraper_package:connector_factory"
```

Discovery must be explicit:

```python
registry.load_entry_points()
```

A broken plugin reports its own package and entry-point name without preventing
the caller from using built-in connectors, unless strict loading was requested.

### 8.5 Adding a framework connector

A new framework connector must provide:

1. An options model.
2. A connector implementation.
3. A factory.
4. Capability declarations.
5. Recorded fixtures or a fake transport suite.
6. Conformance tests.
7. Checkpoint/resume tests.
8. Documentation with a minimal configuration.

It must not require edits to the core registry, runtime, or product models
unless the platform exposes a genuinely new neutral commerce concept.

## 9. Custom-shop support

Offer two paths rather than forcing every custom store into Python code.

### 9.1 Declarative generic-page connector

Refactor `PageCommerceConnector` into composable discovery and parser
strategies. Supported discovery mechanisms:

- configured sitemap URLs;
- sitemaps advertised by `robots.txt`;
- sitemap indexes;
- configured category/listing URLs;
- pagination link/pattern discovery;
- configured product URL patterns.

Supported parser chain:

- JSON-LD;
- microdata;
- OpenGraph;
- verified CSS/DOM selectors;
- optional user-provided parser object.

Example configuration:

```yaml
connector: generic-pages
connector_options:
  discovery:
    sitemaps:
      - https://shop.example/sitemap-products.xml
    product_pattern: /products/
  parsers:
    - jsonld
    - microdata
    - dom
  dom_rules:
    name: "h1.product-title"
    price: ".product-price"
    sku: "[data-product-sku]"
  currency: EUR
```

Declarative rules must be data only. They must not permit arbitrary imports,
Python expressions, or browser JavaScript from untrusted source configuration.

### 9.2 Python custom connector

Complex stores can implement the connector/factory protocols or compose the
generic connector with custom discovery/parser strategies. Provide a complete
example package in `examples/custom_connector/`.

### 9.3 Site-specific connectors

Existing connectors such as Axner, Ceramicolours, and Keramik Kraft should not
automatically become core built-ins. Classify them after extraction:

- generally useful storefront support: library built-in;
- reusable but niche integration: optional plugin package;
- catalogue-only site logic: keep in `catalogue-dump` as an application plugin.

## 10. Source configuration redesign

Separate source configuration into four layers:

```python
class SourceDefinition(BaseModel):
    id: str
    label: str
    base_url: str
    connector: str
    connector_options: dict[str, JsonValue] = {}

class FetchPolicy(BaseModel):
    delay: float
    concurrency: int
    robots: RobotsPolicy
    timeout_seconds: float
    browser: BrowserPolicy

class ProxyPolicyConfig(BaseModel):
    mode: ProxyMode
    country: str | None
    provider_preferences: tuple[str, ...]
    maximum_bytes: int | None

class CatalogueSourceConfig(BaseModel):
    source: SourceDefinition
    fetch: FetchPolicy
    proxy: ProxyPolicyConfig
    datasets: tuple[str, ...]
    projection_options: dict[str, JsonValue]
```

The library owns `SourceDefinition` and may offer the fetch/proxy models. The
catalogue application owns dataset and projection configuration.

Migration must include an adapter from the current flat `SourceConfig` to the
new layered models. Do not rewrite `sources.json` until all existing fields are
mapped and validated by tests.

Connector options are validated in two steps:

1. Validate the generic source envelope.
2. Resolve the factory and validate `connector_options` with its options model.

This eliminates dozens of irrelevant optional fields while preserving strict
unknown-field rejection.

## 11. Transport architecture

### 11.1 Core request/response contract

Define a small transport contract that supports normal HTTP connectors:

```python
class CommerceTransport(Protocol):
    async def request(self, request: TransportRequest) -> TransportResponse: ...
    async def rotate_identity(self, reason: RotationReason) -> None: ...
```

`TransportRequest` includes method, URL, query, headers, optional JSON/body,
request purpose, priority, estimated bytes, cache policy, and browser hint.

`TransportResponse` includes status, headers, bytes, final URL, route metadata,
cache status, and timing. Convenience methods such as `json()` and `text()` can
live on the response or a thin connector-facing adapter.

Avoid exposing the raw `httpx.Response` as a public contract.

### 11.2 Middleware

Build transport behavior as two explicit layers:

```text
connector
  → cancellation/deadline
  → robots policy
  → cache lookup
  → retry/block classification
      ↳ for every network attempt:
          request-budget authorization and charge
          → rate limit
          → proxy routing/identity selection
          → direct HTTP or browser backend
  → cache write
```

The retry controller re-enters the complete per-attempt layer. The exact order
must be tested because it affects cost and politeness. In particular:

- cache hits do not consume network request/proxy budgets;
- retry attempts do consume budgets;
- every retry is independently rate-limited;
- rate limiting applies independently to direct and proxy identities;
- robots checks happen before paid routing;
- browser subrequests are accounted for when the backend can expose them.

Robots metadata may have its own bounded cache. Fetching or refreshing
`robots.txt` is ordinary network work and is accounted for, but serving an
already cached response does not enter the per-attempt layer.

### 11.3 HTTP transport

The default implementation uses `httpx` and supports HTTP(S), SOCKS, redirect
following, timeouts, compressed responses, and streamed byte accounting.

All backends share a `URLPolicy` applied before DNS connection and again after
every redirect. Its safe default permits only HTTP(S), rejects embedded
userinfo, loopback, private, link-local, and metadata-service destinations, and
prevents DNS rebinding by validating resolved addresses. Source configuration
defines allowed target origins; explicit allowlists cover legitimate
cross-origin storefront APIs and CDNs. Connectors and discovery parsers cannot
bypass this policy by constructing a URL directly.

### 11.4 Browser transport

Browser support is an optional extra. The library defines the protocol and
session lifecycle; concrete Camoufox/CDP integrations can initially be moved
with minimal change.

Browser requirements remain connector capabilities:

- never;
- optional;
- required.

The runtime must fail clearly when a required backend is unavailable. It must
not silently downgrade a browser-required connector to raw HTTP.

### 11.5 Cache

Define a `ResponseCache` protocol with get/put metadata and leave storage
implementations pluggable. Include a local filesystem implementation if it can
be extracted without application coupling. Cache keys must include the request
method, normalized URL, relevant headers/body digest, render mode, and a schema
version. Proxy credentials must never enter cache keys or artifacts.

## 12. Multi-provider residential proxy design

### 12.1 Separate control plane from data plane

Control plane responsibilities:

- query vendor subscriptions and usage;
- create or rotate sub-users;
- open/close billing cycles;
- approve spending;
- reserve organization budgets.

Data plane responsibilities:

- select a usable route;
- acquire a sticky identity;
- produce transport/browser credentials;
- enforce attempt-local limits;
- rotate or invalidate an identity;
- report requests, bytes, blocks, and failures.

The reusable scraper library owns data-plane protocols and an optional static
implementation. The catalogue application retains its PostgreSQL control plane
and adapts it to those protocols.

### 12.2 Core proxy models

```python
class ProxyKind(StrEnum):
    RESIDENTIAL = "residential"
    MOBILE = "mobile"
    DATACENTER = "datacenter"

class ProxyRequest(BaseModel):
    source_id: str
    target_host: str
    kind: ProxyKind = ProxyKind.RESIDENTIAL
    country: str | None = None
    region: str | None = None
    city: str | None = None
    sticky: bool = True
    session_ttl_seconds: int | None = None
    maximum_bytes: int | None = None
    preferred_providers: tuple[str, ...] = ()
    excluded_providers: tuple[str, ...] = ()

class ProxyEndpoint(BaseModel):
    provider: str
    endpoint_id: str
    protocol: Literal["http", "https", "socks5"]
    host: str
    port: int
    kind: ProxyKind
    countries: frozenset[str] = frozenset()

class ProxyLease(Protocol):
    lease_id: str
    provider: str
    route: ProxyEndpoint
    expires_at: datetime | None
    maximum_bytes: int | None

    def http_credentials(self) -> ProxyCredentials: ...
    def browser_credentials(self) -> BrowserProxyCredentials: ...
```

Credentials must use secret-aware types whose `repr` and serialization are
redacted. A lease should be an async context manager or be released by the
runtime in `finally`.

### 12.3 Pool protocol

```python
class ProxyPool(Protocol):
    async def acquire(self, request: ProxyRequest) -> ProxyLease: ...
    async def rotate(
        self, lease: ProxyLease, reason: RotationReason
    ) -> ProxyLease: ...
    async def report(self, lease: ProxyLease, outcome: ProxyOutcome) -> None: ...
    async def release(self, lease: ProxyLease) -> None: ...
```

Lease ownership is explicit: the runtime owns every lease it acquires and
releases it in `finally`. `rotate` atomically invalidates and releases the old
lease before returning the replacement, so callers never own two live leases
for one route. `release` is idempotent.

Rotation reasons include:

- explicit caller request;
- HTTP 403/block page;
- HTTP 429/rate limit;
- CAPTCHA detected;
- transport/TLS failure;
- session expired;
- byte/request budget exhausted.

### 12.4 Routing policies

Initial policies:

- `never`: direct only;
- `always`: proxy from the first request;
- `fallback`: use direct first, then acquire a proxy for classified failures;
- `failover`: use providers in preference order;
- `round_robin`: rotate healthy routes across jobs;
- `weighted`: choose from configured provider weights.

Do not switch identity for every request by default. A storefront collection
usually requires a stable cookie/session identity. The default lease scope is
one source attempt, with explicit rotation on classified failures.

### 12.5 Provider adapters

Vendor-specific adapters own:

- username construction;
- country/session parameters;
- gateway protocol and endpoint selection;
- provider limits and supported geography;
- translation from neutral rotation requests to provider semantics.

Move the current Decodo username template out of the neutral lease. Add
adapters for Decodo, IPRoyal, Webshare, and ProxyScrape only after each
provider's actual gateway semantics are verified. Existing control API clients
may remain in the application until their reuse case is demonstrated.

### 12.6 Health and selection

Track health by provider, endpoint, target host, and optionally geography. A
route can be healthy globally but blocked by one storefront.

Use bounded in-memory health state in the library:

- consecutive failures;
- last success/failure;
- cooldown deadline;
- exponentially increasing cooldown with a maximum;
- reason-specific counters.

Allow applications to implement a durable/shared health store. Do not require
one for library use.

Selection excludes:

- routes in cooldown for the target host;
- routes lacking requested geography/type;
- exhausted leases;
- providers rejected by application budget authorization;
- explicitly excluded providers.

### 12.7 Accounting and safety

Proxy accounting reports:

- transmitted and received bytes;
- request count;
- browser-estimated bytes when exact values are unavailable;
- target host;
- response classification;
- direct/proxy/cache route;
- lease and endpoint opaque identifiers.

Rules:

- no new request starts after the attempt-local proxy limit is exhausted;
- credentials are never logged, serialized into checkpoints, or included in
  exception messages;
- application budget authorization is fail-closed;
- failure to report usage is surfaced to the application, not silently ignored;
- direct and proxy rate limiters are independent;
- proxy fallback occurs only for typed failure classifications, not every
  parser-empty result.

## 13. Runtime API

Provide two usage levels.

### 13.1 High-level client

```python
registry = ConnectorRegistry.with_builtins()

async with CommerceScraper(
    registry=registry,
    transport=http_transport,
    browser_transport=browser_transport,
    proxy_pool=proxy_pool,
    proxy_policy=ProxyPolicyConfig(
        mode=ProxyMode.FALLBACK,
        country="FR",
    ),
) as scraper:
    async for page in scraper.collect(
        SourceDefinition(
            id="example-shop",
            label="Example Shop",
            base_url="https://shop.example",
            connector="shopify",
            connector_options={"currency": "EUR"},
        ),
        requested_fields=frozenset(SnapshotField),
    ):
        consume(page)
```

Dependencies passed as instances are borrowed and are never closed by the
scraper. Dependencies created by the runtime builder are owned by it and are
closed when the context exits. The high-level runtime guarantees cleanup of
all owned sessions and proxy leases on success, failure, timeout, and
cancellation. Factory and builder helpers are the preferred API when the
scraper should own the HTTP or browser session.

### 13.2 Low-level connector use

```python
options = ShopifyOptions(currency="EUR")
connector = ShopifyConnector(transport, options)

async for page in connector.collect(request, checkpoint):
    consume(page)
```

This level is important for applications that already own sessions, proxy
leases, retries, or orchestration.

## 14. Telemetry hooks

The core library must not require the catalogue's metrics package. Define
no-op-by-default hooks or emitted events for:

- request started/completed;
- retry and backoff;
- rate-limit wait;
- cache hit/miss/write;
- proxy acquired/rotated/released;
- connector page emitted;
- diagnostic emitted;
- collection completed/interrupted.

Event fields must be bounded and secret-safe. The catalogue application adapts
these events to its existing structured logs, metrics, and traces.

## 15. Testing strategy

### 15.1 Library unit tests

- Model validation and serialization.
- Capability validation.
- Registry duplicate and plugin behavior.
- Checkpoint validation.
- Checkpoint schema-version and collection-fingerprint compatibility.
- Parser fixtures.
- Discovery pagination and limits.
- Middleware ordering.
- Retry classification.
- URL-policy enforcement for direct requests, redirects, cross-origin links,
  private/link-local addresses, and DNS rebinding.
- Proxy selection, cooldown, rotation, and budget exhaustion.
- Credential redaction.
- Resource cleanup on success, exception, timeout, and cancellation.

### 15.2 Connector conformance suite

Provide a reusable test function every connector runs. It verifies:

- declared capabilities match behavior;
- pages have monotonically valid sequences;
- terminal and resume states are consistent;
- result limits produce resumable output;
- checkpoints reject wrong schema/source/connector/version/fingerprint;
- cancellation stops without starting additional requests;
- diagnostics correctly mark incomplete enumeration;
- normalized records validate against the neutral model;
- connector output never contains credentials.

### 15.3 Recorded-response replay

Reuse the existing response archive and golden fixtures. During migration, run
both paths against identical recordings:

```text
recording → legacy scraper → ceramics rows ┐
                                           ├→ shadow comparison
recording → library connector → projector ┘
```

No live network is used for parity tests.

### 15.4 Proxy tests

- Fake local HTTP and SOCKS proxy servers.
- Multiple fake providers with deterministic routing.
- Provider failure and fallback.
- Sticky identity reuse.
- Rotation after block classification.
- Per-target cooldown.
- Concurrent acquisition.
- Byte/request limit enforcement.
- Browser credential projection.
- Log, diagnostic, and exception redaction.

Paid-provider smoke tests remain opt-in operational tests and are never part of
the default suite.

### 15.5 Packaging tests

- Build wheel and source distribution.
- Install wheel into a clean environment.
- Import core with only base dependencies.
- Import each optional extra independently.
- Verify package data and `py.typed`.
- Run a minimal fake-transport scrape from the installed wheel.

## 16. Migration phases

### Phase 0: Baseline and decisions

Deliverables:

- Approve the distribution/import names.
- Record current fast, connector, proxy, and golden test results.
- Inventory every connector, its transport features, and configured sources.
- Classify site-specific connectors as built-in, plugin, or catalogue-only.
- Freeze neutral model JSON schemas and example payloads.
- Document currently supported Python versions.
- Add an ADR for the library boundary and plugin model.

Exit criteria:

- Every current source maps to a legacy and/or neutral connector path.
- Existing failures/skips are documented.
- No behavior change has been introduced.

### Phase 1: Create the distribution and extract contracts

Deliverables:

- Add `commerce-scraper/pyproject.toml` and package skeleton.
- Move/copy neutral commerce models, connector contracts, budgets,
  diagnostics, and checkpoints.
- Add a compatibility decoder for current application checkpoints: derive the
  version-1 collection fingerprint from durable lineage configuration when it
  is available; otherwise reject the old checkpoint and begin a new lineage
  rather than resuming without configuration identity.
- Establish public exports and `py.typed`.
- Add dependency-boundary and package-install tests.
- Point `catalogue-dump` at the workspace dependency.
- Update every worker, loader, control, backup, and maintenance image that
  installs `catalogue-dump` so its build context includes and installs the new
  distribution.
- Update lockfiles and release inputs so local development, CI wheels, and
  container builds resolve the same scraper version without relying on an
  undeclared editable checkout.
- Replace old contract imports with compatibility re-exports where needed.

Exit criteria:

- The new wheel installs with base dependencies only.
- Catalogue tests pass using contracts from the new package.
- All catalogue container images build and can import both distributions from
  their installed artifacts.
- No connector implementation has changed behavior.

### Phase 2: Extract the HTTP transport and runtime primitives

Deliverables:

- Add transport request/response protocols.
- Add `httpx` implementation and lifecycle management.
- Add the shared URL policy and SSRF/redirect/DNS-rebinding tests.
- Extract/adapt rate limiting, robots policy, cache hooks, retries, and request
  accounting as middleware.
- Add fake transport utilities.
- Map legacy `Fetcher` operations onto the new transport during migration.

Exit criteria:

- A connector can run entirely from a fake transport.
- HTTP session cleanup and cancellation tests pass.
- Direct requests have parity with the current fetch path for selected
  recordings.

### Phase 3: Extract the first vertical slice

Use Shopify first because it has a public feed, multiple configured sources,
and existing connector-canary coverage.

Deliverables:

- Move Shopify connector and options into the library.
- Add built-in factory and isolated registry.
- Run connector conformance tests.
- Adapt catalogue configuration to Shopify's options model.
- Run recorded golden and shadow comparisons.
- Run a limited production canary without replacing the legacy source key.

Exit criteria:

- Golden parity or reviewed/documented intentional differences.
- Checkpoint and result-limit behavior validated.
- At least one catalogue source runs through the extracted wheel.
- Rollback is a source configuration change, not a code revert.

### Phase 4: Generic custom-shop framework

Deliverables:

- Extract page discovery and parsers.
- Introduce composable discovery/parser strategy protocols.
- Implement safe declarative configuration.
- Migrate `pagecrawl` and at least one bespoke connector.
- Add documentation for declarative and Python custom shops.

Exit criteria:

- A new structured-data shop can be configured without editing library code.
- A custom parser can be registered without subclassing the runtime.
- Parser-empty, browser-required, blocked, and discovery-incomplete states are
  distinct and tested.

### Phase 5: Proxy data plane

Deliverables:

- Add proxy models, pool protocol, routing policies, and static pool.
- Add provider adapter protocol.
- Add direct/always/fallback/failover/round-robin selection.
- Add health/cooldown and accounting hooks.
- Adapt the current Decodo runtime lease to the neutral interface.
- Add at least one second provider gateway adapter using fake/local tests.
- Preserve application-owned fail-closed PostgreSQL budget authorization.

Exit criteria:

- One scrape can fail over between two configured providers.
- Sticky identity is retained across HTTP and browser requests.
- Byte caps prevent new requests after exhaustion.
- No credentials appear in logs, errors, checkpoints, or recordings.
- Existing catalogue proxy safety tests continue to pass.

### Phase 6: Remaining framework connectors

Suggested order:

1. WooCommerce
2. PrestaShop/Sio2
3. BigCommerce
4. Wix
5. Shopware
6. Starweb
7. NitroSell
8. SumUp

For every connector:

- move implementation and options;
- add factory registration;
- run conformance suite;
- map flat source config to connector options;
- run replay and shadow parity;
- canary selected sources;
- switch the stable source key only after approval.

Exit criteria:

- All general-purpose frameworks use the library.
- Remaining code in `scrapers/` is explicitly catalogue-specific or scheduled
  for deletion.

### Phase 7: Catalogue application cutover

Deliverables:

- Replace `ops/connector_adapters.py` hard-coded construction with the library
  registry and application adapters.
- Move dataset projection after neutral collection for all migrated sources.
- Introduce layered source configuration while retaining a compatibility
  loader for the existing file.
- Remove connector-specific conditionals from worker/runtime code.
- Map library telemetry into catalogue observability.

Exit criteria:

- All production sources run through the library connector contract.
- The worker, CLI, and probe share one composition root.
- Dataset/storage/queue code contains no storefront framework logic.

### Phase 8: Remove compatibility code and stabilize 1.0

Deliverables:

- Remove legacy scraper implementations whose parity gates passed.
- Remove `_connector` canary aliases and compatibility re-exports after a
  deprecation window.
- Publish connector authoring, proxy integration, and migration guides.
- Document supported public imports and semantic-versioning policy.
- Add changelog and release automation.
- Build final API/schema compatibility tests.

Exit criteria:

- No duplicate production connector implementations remain.
- A clean external example project can install the wheel, add a custom
  connector, and scrape through a fake or configured proxy pool.
- Public APIs are documented and covered by compatibility tests.

## 17. Per-connector migration checklist

Use this checklist for each connector:

- [ ] Identify all legacy source fields consumed by the connector.
- [ ] Define a strict connector-owned options model.
- [ ] Identify required transport operations.
- [ ] Confirm capability declarations.
- [ ] Confirm page partition and checkpoint semantics.
- [ ] Move parsing/normalization code without unrelated reformatting.
- [ ] Add deterministic fake/recorded fixtures.
- [ ] Pass connector conformance tests.
- [ ] Compare neutral snapshots.
- [ ] Compare projected ceramics rows.
- [ ] Review request counts and byte estimates.
- [ ] Review direct/browser/proxy behavior.
- [ ] Canary one or more representative sources.
- [ ] Document intentional differences.
- [ ] Switch stable registry mapping.
- [ ] Retain rollback configuration through at least one normal schedule cycle.
- [ ] Remove legacy implementation only after the agreed observation window.

## 18. CI and quality gates

Add Make targets such as:

```text
make scraper-lint
make scraper-typecheck
make scraper-test
make scraper-build
make scraper-contracts
make scraper-check
```

Update repository gates so `make check` includes the new distribution.

Required merge gates by change type:

| Change | Required gates |
|---|---|
| Contract/model | unit, schema diff, mypy, installed-wheel test |
| Connector | conformance, recording replay, relevant golden comparison |
| Transport | unit, cancellation/cleanup, recording replay |
| Proxy | fake proxy integration, redaction, budget and fallback tests |
| Catalogue cutover | fast suite, golden suite, Postgres/NATS suites as applicable |

No golden fixture is regenerated solely to make extraction pass. A golden
change requires a reviewed explanation of the behavior change.

## 19. Versioning and compatibility

- Start the new distribution at `0.1.0`.
- Use semantic versioning for the Python API after `1.0`.
- Version serialized contracts and checkpoints independently of the package.
- Treat removal or semantic changes to model fields, diagnostic codes,
  capabilities, connector names, or entry-point contracts as breaking changes.
- Connector parser improvements that change extracted data require release
  notes even when the Python API is unchanged.
- Keep compatibility re-exports in `mb_ceramics_catalogue` for one documented
  deprecation window.
- Pin the library workspace version in the application during migration.

## 20. Security and responsible collection requirements

- Respect configured robots and rate-limit policies.
- Keep per-host and shared-edge concurrency controls available to applications.
- Redact proxy credentials and authentication headers structurally.
- Reject proxy hosts containing embedded userinfo in configuration.
- Never serialize secrets into Pydantic dumps, checkpoints, cache metadata, or
  telemetry.
- Bound response bodies retained in errors and test artifacts.
- Validate redirects before forwarding sensitive headers.
- Apply the shared URL policy before connection and after every redirect;
  reject unsafe resolved addresses and require explicit allowlists for
  cross-origin APIs or CDNs.
- Make paid proxy activation explicit and fail-closed.
- Do not silently use a proxy because direct extraction returned zero records;
  fallback requires a transport/block classification.
- Document that library users are responsible for legal authorization, terms,
  privacy, and collection policies for their targets.

## 21. Operational migration and rollback

Every production source retains an explicit legacy/library route during its
canary period. Record:

- selected connector and version;
- request and response counts;
- direct, browser, and proxy bytes;
- discovered and emitted counts;
- diagnostic counts;
- checkpoint lineage;
- dataset projection versions.

Rollback must be possible by changing the source's connector route or feature
flag. Checkpoints from the library path must not be fed into the legacy path.
Artifacts retain connector/version metadata so a rollback does not make two
different collectors appear equivalent.

## 22. Risks and mitigations

### Accidental behavior changes while moving parsers

Mitigation: avoid cleanup/reformatting during extraction; use recorded replay,
golden output, and shadow comparison.

### Designing a library around one application's internals

Mitigation: keep database, queues, ceramics datasets, and provider billing
outside the package; prove usability with a clean external example.

### A transport protocol that is too generic or too narrow

Mitigation: derive the initial protocol from Shopify, WooCommerce, generic
pages, and one browser connector before declaring it stable.

### Plugin import side effects

Mitigation: instance registries and explicit entry-point loading.

### Proxy cost or credential leakage

Mitigation: fail-closed authorization, secret-aware values, structural
redaction, local fake proxy tests, and strict accounting.

### Provider behavior differences

Mitigation: capability-driven provider adapters; do not pretend all providers
support the same session, geography, usage, or sub-user operations.

### Permanent dual implementations

Mitigation: every canary has an owner, exit criteria, and legacy removal task.
Track migration state per connector.

### Overly ambitious first release

Mitigation: deliver a thin vertical slice first: neutral contracts, HTTP
transport, Shopify, generic pages, testing utilities, and a static proxy pool.

## 23. Proposed first milestone

The first useful release should contain:

- neutral commerce models;
- collection request/page/checkpoint/diagnostic contracts;
- connector factory and isolated registry;
- `httpx` transport and fake transport;
- Shopify connector;
- generic page connector with sitemap and JSON-LD support;
- static proxy pool with direct, always, fallback, and failover policies;
- fake multi-provider proxy tests;
- connector-author example;
- catalogue compatibility adapter;
- recorded replay parity for selected Shopify and page-based sources.

This milestone proves all important extension boundaries without requiring the
entire catalogue migration.

## 24. Suggested implementation sequence for the first milestone

1. Add the new project skeleton and CI targets.
2. Extract models and connector contracts with compatibility re-exports.
3. Add fake transport and conformance suite before moving a connector.
4. Extract the HTTP transport contract and implementation.
5. Move Shopify and establish replay parity.
6. Split generic page discovery from parsing.
7. Move sitemap and JSON-LD generic-page support.
8. Add connector factories, registry, and explicit built-in registration.
9. Add proxy contracts and static pool.
10. Implement two fake provider adapters and failover tests.
11. Adapt the catalogue's existing proxy lease to the pool protocol.
12. Run one Shopify and one page-source production canary.
13. Build/install the wheel from a clean example project.
14. Review the public API before tagging `0.1.0`.

## 25. Definition of done

The refactor is complete when:

- `mb-commerce-scraper` can be installed without the catalogue application;
- its core import does not require PostgreSQL, NATS, a browser, or a proxy
  provider SDK;
- an external project can use a built-in connector with a small public API;
- an external project can register a framework or fully custom connector;
- a simple custom shop can be described through safe discovery/parser rules;
- multiple residential proxy providers can be selected, failed over, rotated,
  metered, and health-checked through neutral interfaces;
- all general-purpose production connectors use the library;
- ceramics projection and catalogue orchestration remain outside the library;
- checkpoint/resume, cancellation, rate limiting, proxy safety, and cleanup are
  covered by automated tests;
- existing production outputs have passed replay/shadow migration gates;
- duplicate legacy connector paths have been removed;
- public APIs, compatibility guarantees, and extension guides are documented.
