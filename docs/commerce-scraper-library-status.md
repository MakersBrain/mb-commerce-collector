# Commerce Scraper Library Implementation Status

Plan: [commerce-scraper-library-plan.md](commerce-scraper-library-plan.md)
Branch: `feat/commerce-scraper-library`
Last reviewed: 2026-08-25
Overall status: **in progress — library foundation implemented, production cutover pending**

This document tracks implementation and migration against the refactor plan. It
is the operational status record; the plan remains the source of truth for the
target architecture and acceptance criteria.

## Status conventions

- `[x]` implemented and verified in the current branch.
- `[~]` partially implemented, or implemented without all required migration
  and verification gates.
- `[ ]` not implemented or not yet demonstrated.
- A connector is not considered migrated merely because its library
  implementation exists. Migration requires replay/shadow parity, a production
  canary, a stable source mapping, and a retained rollback route.

## Standing engineering guardrails

Apply these reminders to every implementation batch. They complement the
architecture rules in the plan and are not separate scope-expansion goals.

### Python architecture and design

- Follow established Python architecture and design-pattern best practices,
  while preferring the smallest design that satisfies a demonstrated need.
- Keep dependency direction explicit: domain models and protocols must not
  depend on concrete transports, provider SDKs, or catalogue application code.
- Use dependency injection and small typed protocols at infrastructure
  boundaries. Prefer composition over inheritance and avoid service locators,
  hidden globals, import-time registration, and unnecessary abstractions.
- Give each module one clear responsibility. Keep connector parsing,
  discovery, transport policy, proxy selection, runtime orchestration, and
  application projection separate.
- Keep public contracts strict and typed. Validate data at boundaries, use
  domain-specific exceptions or diagnostics, and preserve exception causes.
- Make ownership and lifecycle explicit for clients, browser sessions, leases,
  caches, and other resources. Cleanup and cancellation must be reliable.
- Do not add a pattern, abstraction, or extension point until it reduces
  current duplication, isolates a real boundary, or enables a planned use case.

### Testing depth

- Keep tests proportional to risk; do not pursue exhaustive combinations that
  add substantial maintenance cost without protecting important behavior.
- Prioritize essential domain invariants, public contracts, checkpoint/resume,
  cancellation, lifecycle cleanup, security boundaries, and error
  classification.
- Test module and middleware interactions, not only isolated units. Important
  call chains include connector → runtime → middleware → direct/proxy/browser
  transport and neutral snapshot → catalogue projector.
- Give middleware-order tests observable assertions for ordering, retries,
  short-circuit behavior, cleanup, and propagation of context and errors.
- Use deterministic fakes for most behavior. Add focused integration tests at
  boundaries where mocks would hide wiring errors, and reserve recorded replay
  and end-to-end tests for migration-critical paths.
- Every production defect should receive the smallest regression test that
  proves the failure and fix. Avoid tests coupled to private implementation
  details when a contract-level assertion is available.

### Tracing and debugging

- Use structured events through the telemetry protocol rather than scattered
  free-form logging. Application adapters decide the concrete logging, metrics,
  and tracing backend.
- Propagate a collection/trace identifier and stable operation context through
  connector, runtime, middleware, proxy, and transport calls. Include safe
  fields such as connector name/version, source ID, request purpose, route,
  attempt, page/partition, status class, duration, and byte counts.
- Emit `INFO` events for collection and lifecycle boundaries, `DEBUG` events
  for sanitized protocol decisions and middleware transitions, `WARNING` for
  recoverable degradation, and `ERROR` only for failed operations requiring
  attention. Avoid logging the same failure at every layer.
- At `DEBUG`, make the protocol path reconstructable: request accepted, policy
  decisions, cache outcome, rate-limit wait, route/lease selection, attempt,
  response classification, retry/rotation decision, page emission, and
  cleanup. Include bounded timings and counts rather than response bodies.
- Record typed outcome and reason codes so failures can be filtered without
  parsing message text. Preserve exception chaining and attach stack traces at
  the layer that finally handles an unexpected exception.
- Never emit credentials, authorization or cookie values, proxy userinfo,
  sensitive query parameters, complete response bodies, or unrestricted raw
  provider payloads. Redaction must be structural and applied before events
  cross the telemetry boundary.
- Tracing must remain optional and low-overhead. Disabled tracing must not
  change behavior, and telemetry failures must not fail a collection.

## Current baseline

### Implemented commits

- `f9c0401` — extract reusable commerce scraper library.
- `b49fd50` — add proxy runtime and WooCommerce connector.
- `7adda2b` — extract remaining framework connectors.
- `d79cbcb` — complete native connector integration and filesystem caching.
- `02de77b` — unify proxy policy and end-to-end request tracing.
- `f863a66` — share native catalogue composition across worker and local tools.
- `1b05488` — publish intentional bounded collections as sealed limited output.
- `de23085` — make catalogue connector runtime plans data-only.
- `bdefd58` — derive worker placement capabilities from adapter metadata.
- `be50039` — reconcile completed Phase 7 implementation status.
- `3bdd0a2` — revalidate durable proxy safety before every physical attempt.
- `e7c4662` — preserve HTTP retry outcomes in protocol telemetry.
- `38be13c` — trace logical requests and owned runtime cleanup.
- `5977d8f` — isolate provider reconciliation and control mutations.
- `ed429da` — constrain durable provider identity in PostgreSQL.
- `db25719` — authorize durable provider gateways.
- `2fd0386` — resolve durable Webshare routes.
- `1d83636` — rotate Webshare gateway secrets with local CAS.
- `a2043ea` — stage Webshare worker secrets default-off.
- `31285e3` — fence profile secret rotations and persist recovery intents.
- `0559a67` — run Podman catalogue workers rootless.
- `46a2e59` — coordinate Webshare profile imports.
- `13bd95d` — harden Webshare import recovery and stale-probe cleanup.
- `d3e15a6` — mount the control-owned writable Webshare gateway store.
- `3a84514` — fence cancelled Webshare operations and settle late usage.
- `b85a23d` — expose recent-admin Webshare profile imports.
- `df82eb4` — reject booleans at strict numeric contract boundaries.
- `cf89f58` — preserve per-request robots policy and legacy cache provenance.
- `a39176f` — exercise live Camoufox proxy callback ordering.
- `97a62e4` — align rootless worker identity and add a rotation smoke.
- `258d1fd` — verify installed catalogue and scraper wheel composition.
- `0ff6788` — type strict numeric-boundary regression factories.
- `52425ad` — route native Shopify through durable Webshare locally.
- `b1479fa` — exercise active routed worker recovery and summaries.
- `7d0f673` — verify deadline-triggered runtime cleanup.
- `cfb3add` — require provisioned recorded-replay parity inputs in CI.
- `7a3a355` — enforce supported scraper composition imports.
- `dd2093a` — reject unprovable static proxy constraints.
- `92f97ba` — exercise durable lineage resolution against PostgreSQL.
- `864e87d` — review framework connector traffic profiles.
- `0998578` — bypass the retained commerce-model compatibility shim internally.
- `aabf825` — move structured parsing helpers below the connector layer.
- `63c745b` — finalize the pre-1.0 public surface and typed telemetry contract.
- `6ac428f` — consolidate connector parsing helpers.
- `1c9c8eb` — unify checkpoint and connector-factory plumbing.
- `517db89` — unify middleware failure telemetry.

### Verification at last review

- [x] `make scraper-check`
  - Ruff passed.
  - Mypy passed for 75 source and test files.
  - 313 library tests passed. Version-0 checkpoint compatibility and its
    library test suite were removed before the first release.
  - Wheel and source distribution built.
  - 4 dependency-boundary tests passed.
  - The installed-wheel matrix verified all 224 reviewed public exports across
    nine modules, metadata/source version parity, typed package data, and
    isolated base, HTTP, and development extras; the base install remained
    free of the optional HTTPX dependency.
  - The separately packaged custom connector installed beside the built wheel,
    loaded through its entry point, and passed a public conformance flow.
  - A clean external consumer ran built-in Shopify and the custom connector
    through a consumer-owned two-route proxy pool, exercising middleware retry,
    rotation, cleanup, lease release, and credential containment.
  - The release verifier confirmed source, wheel, and source-distribution
    version parity plus the required changelog structure.
- [x] Catalogue production imports only the nine supported scraper namespaces.
  `TelemetryHooks` and PrestaShop partition declaration are reviewed public
  contracts, and an AST architecture gate rejects future imports from private
  connector or transport implementation modules. The focused catalogue
  architecture/runtime/pipeline slice passed 54 tests with Ruff and mypy.
- [x] Catalogue connectors, datasets, and projection code consume neutral model
  identities directly from the supported library namespace. The deprecated
  catalogue commerce-model shim remains available for its compatibility
  window, has exact export/identity coverage, and an AST gate prevents new
  internal dependence on it. The focused compatibility, connector, dataset,
  and pipeline slice passed 75 tests with Ruff and mypy.
- [x] Targeted catalogue composition and intercall tests passed, covering
  the layered source/run policy, explicit native-route metadata, registry and
  projection boundaries, proxy-runtime composition, and native middleware
  construction without a legacy Fetcher.
  - Canonical proxy-policy tests prove all five policy fields reach the neutral
    pool request together, `never` leaves configured infrastructure idle,
    active policies fail closed without a backend, and catalogue snapshot
    country/provider constraints are checked before secret or database access.
- [x] Full catalogue verification:
  - Ruff passed.
  - Mypy passed for 239 source and test files.
  - 936 tests passed, 2 skipped, 187 deselected, and 284 subtests passed in the
    latest fast-suite run; the wider repository fast gate also passed 32
    control-plane tests, 14 service tests, and 2 explorer tests.
  - The lower fast-test count reflects deletion of the obsolete specialized
    Fetcher composition shell and its construction/transport tests. Its native
    middleware guarantees remain covered at the library/runtime boundaries;
    an all-source invariant now builds all 88 canonical connector/options pairs
    through the application registry instead.
- [x] Durable proxy-attempt PostgreSQL integration test passed against a
  throwaway PostgreSQL 17 instance, covering concurrent authorization,
  capacity exclusion, and exactly-once reconciliation.
- [x] The native Shopify worker path and terminal recovery PostgreSQL gate
  passed together with the immutable proxy-snapshot gate. The first execution
  uses the library runtime and the completed-lineage replay performs no second
  proxy resolution, runtime construction, legacy session, or network request.
- [x] A catalogue/library interaction test proves a typed direct-route 429
  crosses the middleware retry boundary into exactly one sticky Decodo attempt,
  with one durable authorization, one reconciliation, and one reservation
  close. This is deterministic in-memory routing proof, not the still-pending
  local gateway or production canary.
- [x] Catalogue PostgreSQL suite includes registry-built Shopify and
  WooCommerce workers, completed-lineage recovery, and the source-level canary
  rollback selector.
- [x] Native worker refresh-mode PostgreSQL gate: 16 cases passed, covering
  Shopify full and scheduled daily-price collection plus WooCommerce full
  collection, BigCommerce direct-denial/browser-GraphQL collection, and
  PrestaShop sitemap/product collection plus Sio2 category/card discovery and
  durable ceramics loading. Wix covers an actual direct-to-rendered fallback;
  Shopware, Starweb, NitroSell, and SumUp cover their configured discovery and
  parser surfaces. Generic pages cover the catalogue `pagecommerce` to library
  `generic-pages` rename plus translated discovery/parser configuration. Axner
  covers the application-plugin registry, department/listing discovery,
  neutral parsing, and durable projection. Keramik Kraft covers recursive
  category-card discovery, bounded discovery-to-parser response handoff, and
  multi-snapshot listing projection. Ceramicolours covers typed, replayable
  browser evaluation, exact pack stock, and two price plus two stock records.
  Every case also proves terminal recovery
  without a second runtime or network request.
  - The Shopify result-limit case additionally proves that an intentional
    bounded prefix seals as `limited`, publishes usable adds-only output with
    `truncated=true`, never retires unseen records, and recovers idempotently
    without refetching or duplicating its artifact.
- [x] Direct browser composition gate: 21 focused browser/cache tests plus the
  PostgreSQL worker gate verify exact job-context borrowing, session reuse and
  rotation, collection-only cleanup, pre-I/O origin denial, detached placement
  failures, bounded responses, request and final-response origin enforcement,
  method/query/header/JSON preservation, and cursor-sensitive browser cache
  keys without retaining authorization headers.
- [x] The proxy-audit trigger now fails closed when the optional maintenance
  role is absent and checks the invoking session rather than the privileged
  owner of its `SECURITY DEFINER` function.
- [x] Full PostgreSQL verification after the audit migration:
  - Catalogue dump: 166 passed, 1 skipped, and 830 deselected.
  - Control plane: 71 passed and 32 deselected.
- [x] Frozen `catalogue-control` and `catalogue-service` lockfiles resolve and
  import both `mb_ceramics_catalogue` and `mb_commerce_scraper==0.1.0`.
- [x] Lineage runtime persistence tests: 5 unit tests and 5 PostgreSQL
  migration/round-trip tests passed against a throwaway database.
- [x] Current implementation batch passed the verification gates recorded
  below.
  - The pre-1.0 public-surface cleanup removed the parallel proxy-routing API
    and all version-0 checkpoint compatibility. Commerce-scraper lineages now
    reconstruct schema-v1 checkpoints directly from durable row identity.
  - Typed request observations now carry status, route, accounting, elapsed
    time, host, purpose, and attempt identity to catalogue metrics and spans;
    the general sanitized event channel remains intact for logs and lifecycle
    telemetry.
    The complete scraper release gate, full catalogue Ruff/mypy/fast suite, and
    installed two-wheel catalogue composition gate passed.
  - Parsing helpers now have one implementation below the connector layer.
    PrestaShop and Wix consume the shared structured-data helpers; decimal,
    origin, and deterministic page-ID construction are shared across the
    framework connectors. The complete scraper release gate passed with 307
    tests, Ruff, mypy over 75 files, schema generation, package builds,
    boundary checks, installed-wheel verification, external-consumer checks,
    and release verification. Catalogue Ruff, mypy over 239 files, and the
    fast suite (936 passed, 2 skipped, 187 deselected, 284 subtests) passed.
    Installed two-wheel catalogue composition passed.
  - Connector plumbing now uses one public checkpoint builder, one shared
    request-contract validator, and one generic stateless factory. The registry
    performs the sole options validation and still checks factory/connector
    version drift. The complete scraper gate passed with 310 tests and 224
    reviewed exports; the unchanged catalogue fast suite, type/lint gates, and
    installed two-wheel composition also passed.
  - Middleware cached-body rejection and dispatched-attempt failure telemetry
    now each have one implementation. All failure classes carry a consistent
    stage into general events and typed observations without changing retry,
    cancellation, or accounting behavior. The complete scraper gate passed
    with 311 tests; catalogue lint/type/fast tests and installed composition
    remained green.
  - Optional transport capabilities now use runtime-checkable protocols and a
    shared wrapper-forwarding implementation. Request-scoped rotation keeps
    attempt context through rate limiting, while browser subrequest accounting
    remains fail-closed through runtime and routed composition. The complete
    scraper gate passed with 313 tests; catalogue lint/type/fast tests and
    installed composition remained green.
  - The provider-neutral durable Webshare composition and existing Decodo and
    Webshare adapter suites passed 29 focused tests.
  - The strict Webshare gateway-secret contract passed 47 focused tests.
  - Two live PostgreSQL 17 tests passed for the durable Webshare lifecycle and
    unsafe-cycle authorization denial with Decodo ledger isolation.
  - One local native Webshare intercall loads the strict mounted secret, crosses
    resolver → routed transport → real HTTP proxy framing, and proves exact
    sticky credential grammar, one durable authorization/reconciliation/close,
    no direct request, and complete lease/connection cleanup.
  - The Podman/rootless deployment suite passed 29 tests; worker entrypoint and
    rendered Compose placement passed 4 focused tests plus Compose
    configuration validation. A combined live PostgreSQL gate passed 18
    durable-fence and schema-migration cases.
  - The hardened Webshare profile-import service passed 15 live PostgreSQL
    create, rotate, drain, crash-recovery, cycle-rebind, repeated-cancellation,
    advisory-unlock, tuple-binding, and unsafe-cycle cases. Stale-probe cleanup
    and late accounting passed 2 live cases; the authenticated HTTP boundary
    passed 4 live create/rotate/replay/remediation cases.
  - The current control fast suite passed 34 tests with 92 PostgreSQL cases
    deselected. A combined focused live control gate passed 26 cases; the
    Webshare secret and reserved-pool unit slice passed 75 cases, and the
    durable reserved-pool PostgreSQL slice passed 4 cases.
  - The expanded control-owned/worker-read-only deployment and entrypoint slice
    passed 43 tests, and Compose rendering remained valid.
  - The legacy Fetcher compatibility bridge passed 271 tests and 274 subtests,
    including per-request robots enforcement before HTTP/browser I/O and safe
    fresh/replayed/stale cache-route projection; targeted Ruff passed.
  - The opt-in live Camoufox gate passed in the packaged browser-worker image.
    A real browser crossed an authenticated loopback proxy for navigation,
    script, and fetch requests; blocked images did not dispatch, and callback
    authorization, reconciliation, request/byte accounting, and credential
    containment completed successfully.
  - The candidate worker image passed the executable Docker rootless-identity
    fallback smoke. The running worker observed an atomic Webshare generation
    `1 -> 2` replacement through its read-only mount as `10001:10001`, with a
    mode-`0400` secret, no capabilities, no provider API credential, and the
    data plane still disabled. This is not evidence of Quadlet activation.
  - The isolated installed-artifact gate builds the scraper and catalogue
    wheels with workspace source overrides disabled, installs both wheel paths
    in one strict transaction, and runs under `python -I` outside the checkout.
    It loads the packaged `sources.json` and constructs checked-in `ceradel`
    through runtime plan → source projection → application registry → legacy
    transport boundary → library pipeline on both Python versions in CI.
  - The complete scraper release gate passed: Ruff, mypy over 76 files, 317
    tests, frozen schemas, wheel/sdist builds, 4 dependency-boundary tests,
    isolated wheel/extras checks, the external connector consumer, and release
    metadata verification.
  - The native Shopify/Webshare intercall passed through catalogue runtime →
    registry-built connector → middleware → routed proxy → durable attempt
    authorization → real authenticated loopback gateway. It emitted one
    product, one proxy 2xx and no direct request, then reconciled and closed the
    reservation exactly once with no active route lease or connection left.
  - The PostgreSQL-backed outer worker intercall passed through
    `_crawl_connector_canary` with an active immutable Webshare route. It proves
    proxy-only summary accounting, exact reservation/authorization/
    reconciliation/close identity, credential-free retained summaries, no
    legacy lease or browser ownership, and terminal recovery with no second
    route resolution or physical request. The complete adjacent worker module
    passed 17 cases; the resolver-to-wire test also remained green.
  - The aware-deadline runtime gate enters a blocking routed attempt and proves
    `TimeoutError` interrupts the collection without a completion event, closes
    the route-scoped transport, releases its lease exactly once, and leaves no
    active lease. The outer owned HTTP and browser transports remain valid
    until scraper exit and then both close. The full 318-test scraper release
    gate, schemas, artifacts, isolated consumers, and release checks pass.
  - The recorded-replay CI gate now becomes strict only after a successful
    archive pull. It requires a valid manifest, the explicit Shopify and
    Shopware source identities, their frozen outputs, each declared source
    host, and at least one compressed response per host. Four focused preflight
    tests pass; the archive-free golden slice reports one preflight pass and
    two explicit parity skips instead of treating missing recordings as parity.
- [x] Source configuration inventory: all 88 configured sources can be
  constructed through the current layered/library mapping.
  - All 21 `pagecrawl` sources now validate through the explicit legacy-to-
    nested `GenericPagesOptions` translation.
  - Axner, Ceramicolours, and Keramik Kraft construct as application-owned
    connector factories using public library contracts. Ceramicolours' trusted
    DOM action is typed, cacheable, origin-bounded, and accounted through the
    same middleware pipeline as HTTP work on the native worker route. The local
    CLI/probe compatibility shell preserves equivalent policy through the
    shared native runtime. Retained direct canary aliases use the compatibility
    Fetcher bridge only for explicit parity flows.

## Phase summary

| Phase | Status | Summary |
|---|---|---|
| 0. Baseline and decisions | Complete | Baselines, grouped source/transport inventory, bespoke classifications, ADR, frozen schemas, Python policy, and known skips/failures are recorded. |
| 1. Distribution and contracts | Complete | Package, contracts, compatibility decoding, durable lineage composition, public import boundaries, workspace integration, typing, builds, and installed-wheel proof pass. |
| 2. HTTP transport and runtime | Partial | HTTP/fake transports, hardened middleware, conditional archive revalidation, per-request legacy robots/cache projection, and native Shopify policy composition exist; recorded direct-path parity and wider application rollout remain. |
| 3. Shopify vertical slice | Partial | Registry/Fetcher/projection synthetic parity exists; recorded replay, production canary, and stable source switch remain. |
| 4. Generic custom shops | Partial | Composable versioned discovery/parser strategies, sitemap/category/robots discovery, structured-page parsing, safe resume, current pagecrawl option translation, and a native worker gate exist; replay and production migration remain. |
| 5. Proxy data plane | Implementation complete, activation pending | The plan's failover, sticky identity, byte-cap, credential-containment, provider-adapter, and PostgreSQL safety criteria pass locally. Durable default-off Webshare control and live Camoufox callback gates also exist; real Quadlet activation and production routed canaries remain operational cutover gates. |
| 6. Remaining frameworks | Implementation complete, migration pending | Eight framework connectors are extracted; replay, canary, and stable source switching remain. |
| 7. Catalogue cutover | Partial | Every configured connector family now has an explicit native library/application-plugin route through the atomic projection pipeline; production replay/canary approval and stable switching remain. |
| 8. Legacy removal and 1.0 | Partial | Public API/schema manifests, SemVer policy, guides, changelog/release automation, and a clean external consumer gate exist; production observation and duplicate removal remain. |

## Phase 0 — Baseline and decisions

- [x] Distribution name: `mb-commerce-scraper`.
- [x] Import name: `mb_commerce_scraper`.
- [x] Record fast, connector, proxy, and golden test baselines.
  - Counts and focused PostgreSQL/proxy results are maintained in **Current
    baseline**. Golden replay is explicitly recorded as unavailable rather than
    passing synthetically.
- [x] Inventory connector implementations and configured sources.
  - The grouped inventory accounts for all 88 sources: 85 construct through a
    library built-in/declarative path, while Axner, Ceramicolours, and Keramik
    Kraft use checked-in application plugins.
- [x] Record each connector's required transport features.
  - [commerce-scraper-connector-inventory.md](commerce-scraper-connector-inventory.md)
    records HTTP method/surface, browser, discovery/partition, policy, and
    accounting requirements by connector family.
- [x] Classify every site-specific connector as built-in, plugin, or
  catalogue-only.
  - Axner, Ceramicolours, and Keramik Kraft are application-owned plugins
    registered explicitly beside the library built-ins; configured sources
    currently use no external entry-point plugins.
- [x] Freeze neutral-model JSON schemas and representative payloads.
  - Version-one source, request, checkpoint, diagnostic, product, and page
    schemas plus model-validated representative payloads ship as package data.
    A deterministic generator/check gate prevents silent drift, and installed-
    wheel verification proves the artifacts are distributed.
- [x] Declare supported Python versions in packaging; document the support
  policy explicitly.
  - Python 3.11–3.13 is declared and documented; range changes require the full
    verification gates and an explicit release-policy note.
- [x] Add an ADR for the library boundary and plugin model.
  - ADR 0002 fixes the application-neutral dependency direction, explicit
    built-in registration, opt-in entry-point loading, failure isolation, and
    ownership of catalogue adapters and migration gates.
- [x] Document existing test failures and skips.
  - The fast catalogue suite has four intentional archive-dependent golden
    skips: the two existing golden-output cases plus the Shopify and Shopware
    dual-path parity gates. No raw response cache is checked out. There are no
    current library or fast-suite failures.
  - The formerly failing proxy-audit role case is covered by an incremental
    fail-closed migration and focused PostgreSQL regression tests; the full
    combined PostgreSQL target is green.

Exit criterion: **met**. Every configured source has a documented legacy and/or
neutral path; missing replay evidence remains a gate in its owning phases.

## Phase 1 — Distribution and contracts

- [x] Add standalone project and package skeleton.
- [x] Add neutral immutable models using `Decimal` for money.
- [x] Add collection, diagnostics, budget, and checkpoint contracts.
- [x] Add versioned checkpoint schema and collection fingerprints.
- [x] Decode compatible legacy catalogue checkpoints or explicitly start a new
  lineage when compatibility cannot be proven.
  - The package-level decoder returns typed compatible/restart outcomes,
    validates legacy envelope identity, and derives the version-1 fingerprint
    only from separately supplied durable request and connector options.
  - The catalogue boundary maps current connector identities and explicitly
    rejects the `pagecommerce` v1 to `generic-pages` v2 rename.
  - Existing lineage rows retain only partitions and non-reversible hashes, so
    they cannot prove the durable request/options and must start a new lineage.
  - An incremental migration and typed persistence API now store a
    `catalogue-v1`/`commerce-scraper-v1` discriminator plus reconstructable
    request/options JSON for new library lineages. Existing rows safely migrate
    to `catalogue-v1` with no inferred identity.
  - A transactional library-lineage resolver now locks the job/runtime,
    rejects every stale active lineage, decodes durable identity/cursors, and
    only then creates or resumes a lineage and prepares datasets. This prevents
    A → B → A configuration changes from resurrecting an old cursor.
  - Durable progress distinguishes empty, resumable, terminal-intact,
    intentional terminal-limited, and failed terminal-incomplete states. A
    crash after the final or bounded-limit page can therefore continue to
    sealing/publication without refetching from page one. Result-limit pages
    must be terminal, incomplete, and carry a non-null resume cursor.
  - The resolver is wired into every approved native `connector_canary` worker
    path. The normal legacy worker retains its existing lineage format for
    rollback during incremental migration.
  - A PostgreSQL resolver-to-storage intercall proves first-lineage creation,
    durable nonterminal cursor recovery into a typed fingerprinted checkpoint,
    resumable dataset-state preservation, and option-drift fencing. Drift
    rejects the prior lineage, persists fresh durable identity, reuses no
    cursor, and resets dataset counters. The full adjacent PostgreSQL class
    passed 14 cases.
- [x] Add public exports and `py.typed`.
- [x] Add dependency-boundary tests.
  - AST checks prohibit catalogue imports, enforce the documented internal
    layer edges, and restrict models to the standard library, Pydantic, and
    other model modules. A clean subprocess proves the core import does not
    eagerly load HTTP or catalogue application dependencies.
- [x] Add package-install tests.
  - The contract gate installs the wheel and declared dependencies into an
    isolated environment, verifies installed `py.typed`, and performs a
    built-in Shopify scrape through `FakeTransport` outside the source tree.
- [x] Add the workspace dependency and lockfile integration.
- [x] Update relevant container build contexts to include the distribution.
- [x] Prove every catalogue image imports both installed distributions.
  - Every Python image runs the same isolated-mode build check after package
    installation, so imports cannot resolve from the copied `/src` tree.
  - CI builds all seven Python targets, including the compose-only loader and
    backup images that were previously omitted from the image job.
  - A local loader image build passed and reported both imports from installed
    `site-packages`; CI applies the same assertion to the other targets.
- [x] Add catalogue compatibility adapters; legacy contracts remain active only
  on the retained rollback route until Phase 8 observation gates permit their
  removal.

Exit criterion: **met**. Source-tree, installed-artifact, public-boundary, and
PostgreSQL checkpoint-migration verification pass. Production source promotion
remains a Phase 7 cutover gate rather than a Phase 1 contract-extraction gap.

## Phase 2 — HTTP transport and runtime primitives

- [x] Add transport request and response protocols.
- [x] Add `httpx` transport and explicit lifecycle management.
- [x] Add fake transports for deterministic connector tests.
- [x] Add URL allowlist/address policy basics.
- [x] Protect against SSRF and redirects.
  - Every direct and redirect target is allowlisted and resolved once; the
    connection is pinned to one validated public address while logical Host and
    TLS SNI are preserved, closing the DNS-rebinding gap.
  - Redirects are manually bounded and revalidated before connection.
    Cross-origin authorization, proxy-authorization, and cookie headers are
    stripped, and cross-origin redirects carrying bodies are refused.
- [x] Add middleware primitives for retries, cache, robots, rate limiting,
  budgets, and telemetry.
- [x] Add a local filesystem response cache implementation.
  - The public, transports-level `FileResponseCache` provides bounded fresh and
    stale lookups using hashed, versioned artifacts and atomic writes.
  - Cache I/O runs asynchronously per operation; the cache is stateless and
    borrowed by the runtime, so it has no lifecycle method for the scraper to
    close. Installed-wheel contracts cover a binary response round trip.
  - Memory and filesystem caches share canonical request identity and bypass
    credential-bearing headers, URL query fields, and user information rather
    than risking cross-identity response reuse. Entry/read limits are library
    policy; directory retention and total-size pruning remain application-owned.
- [x] Wire policy models and middleware through the public runtime builder.
  - Default and per-collection `FetchPolicy`, injected cache/budget/telemetry,
    retries, robots policy, per-origin pacing/concurrency, and browser policy are
    enacted.
  - Default and per-collection `ProxyPolicyConfig` is the canonical high-level
    proxy contract. Mode, country, provider preferences, request cap, and byte
    cap project together to the routed transport; `never` does not touch an
    available backend and active policies fail closed without one.
  - The guarded legacy routing/cap arguments remain for compatibility, cannot
    be mixed with the canonical policy, and reject inert cap-only use.
- [x] Provide concrete bounded/cached robots and per-origin rate-limiter
  implementations.
- [x] Retry typed transport failures and account failed network attempts.
- [x] Authorize request attempts atomically and reconcile estimated bytes to
  actual response bytes with explicit pre-dispatch release semantics.
  - `affordable()` remains a non-consuming connector planning preview.
  - Application budget implementations now provide async `authorize()` tokens
    with fail-closed `reconcile()` and `release()` operations.
- [x] Ensure each routed transport request performs exactly one physical
  direct or proxy attempt; fallback/failover transitions are driven by the
  middleware retry boundary and charged as separate attempts.
- [x] Release rate-limit concurrency permits on success, retry, cancellation,
  and backend failure.
- [x] Make sticky route acquisition/rotation concurrency-safe without holding
  a selection lock across network I/O.
  - Concurrent acquisitions and rotations coalesce, active route checkouts
    delay rotation/close, stale outcomes cannot rotate a newer generation, and
    failed transitions release detached leases best-effort.
- [x] Preserve independent direct and proxy rate limits.
  - Route-specific limiter instances wrap the physical direct and proxy
    backends and emit sanitized route-aware wait telemetry.
- [~] Map legacy `Fetcher` behavior onto the new transport.
  - An application-owned adapter maps HTTP requests/responses, required-browser
    GET rendering, identity rotation, final HTTP statuses, typed exhausted
    network/cache/proxy-budget failures, route metadata, and secret-safe
    protocol telemetry.
  - Opaque bodies fail before I/O. Browser JSON POST and typed evaluation now
    retain their method/context/action semantics through the shared local
    bridge. Evaluations use script-hashed replay keys, bounded JSON artifacts,
    browser accounting, and no script-bearing trace fields. Every connector
    URL now crosses the effective robots policy before HTTP or browser I/O, and
    fresh, replayed, and stale legacy cache hits project explicit cache-route
    metadata. Recorded direct-path parity remains.
- [x] Preserve the catalogue response archive in native runtime composition.
  - An async application adapter preserves legacy HTTP/render cache keys,
    atomic archive writes, cache route metadata, and fail-closed replay misses.
    The native Shopify worker selects this adapter. Expired HTTP entries expose
    ETag/Last-Modified validators, 304 responses atomically refresh archive
    metadata while reusing the retained body, and explicitly enabled
    stale-on-error masks only transient direct HTTP failures after the configured
    retries. Stale availability suppresses paid fallback rotation without
    changing later route state; deterministic errors and browser requests are
    never masked.
- [x] Bound response bodies retained in memory, errors, and fixtures.
  - One configurable production policy defaults to 16 MiB and is projected
    through direct/proxy HTTP streaming, browser dispatch, generic middleware,
    cache reads/writes, and the catalogue legacy bridge. Oversized bodies are
    rejected without truncation, retry, rotation, or proxy-health penalties.
  - JSON decode failures retain only line/column context. Deterministic fake
    responses and recordings have per-body, archive, response-count, and
    retained-error bounds; credential-bearing fixture URLs/headers are
    rejected.
  - Catalogue compatibility adapters bound rendered text, browser JSON, and
    compressed/decompressed sitemap content, while durable worker errors are
    centrally redacted and bounded.
- [~] Demonstrate recorded-response parity with the legacy direct path.
  - A golden-marked Shopify gate now opens independent replay-only legacy and
    library sessions over the production `ResponseCache`, anchors legacy output
    to the frozen golden, and compares complete normalized output, coverage,
    completeness, errors, and request/render counts. It skips explicitly when
    the external raw-response archive is absent; therefore the gate exists but
    recorded parity evidence is still pending.
  - A second bounded gate covers `keramikbedarf-online` through the stable and
    `library_shopware_connector` paths. It anchors the legacy result to the
    frozen golden and compares projected count, coverage, sample, full digest,
    errors, truncation, and interruption semantics. Request/discovery totals
    remain review evidence until the archive is restored and observed.
  - CI sets `CATALOGUE_GOLDEN_ARCHIVE_REQUIRED=1` only after the configured
    archive pull succeeds. In that mode, missing manifests, source hosts,
    recordings, or frozen outputs fail before the parity cases can silently
    skip. Local archive-free runs retain their explicit skips.

Exit criterion: **not met**. Native policy/cache execution works, but recorded
direct-path parity and migration-wide application composition are incomplete.

## Phase 3 — Shopify vertical slice

- [x] Extract Shopify connector and strict options model.
- [x] Register the built-in factory.
- [x] Add deterministic connector tests and synthetic parity tests.
  - One immutable two-response script now runs independently through the
    production legacy Shopify scraper and the actual library registry, Fetcher
    bridge, atomic dataset pipeline, and staged artifact writer. It proves
    neutral collection, ceramics projection equality, and identical two-request
    URL/query behavior.
  - This gate found and fixed strict Decimal corruption at the page-envelope
    boundary and non-JSON projection configuration before a non-empty canary.
- [x] Adapt catalogue configuration to the options model.
- [x] Execute Shopify through the library registry in the worker canary route.
  - The worker now composes the native library cache, policy, middleware,
    direct/proxy transport, and lifecycle boundary without a legacy
    `Fetcher`/session or a second proxy lease owner. Source/run delay,
    concurrency, robots, browser, cancellation, and deadline controls are
    projected once.
  - A PostgreSQL worker test proves initial collection and a later
    terminal-complete recovery with no second runtime, proxy resolution,
    legacy session, or network request.
  - A focused aware-deadline intercall proves the active routed request is
    interrupted with typed `TimeoutError` telemetry, its route transport and
    lease close immediately, and scraper-owned HTTP/browser resources close at
    the outer lifecycle boundary without a false completion event.
  - The local `catalogue-dump --pipeline connector_canary` compatibility shell
    now enters the same application-owned native runtime as the worker and
    probe. It constructs no legacy Fetcher/session and uses direct-only local
    policy while preserving the stable source identity and result contract.
- [~] Run the same recorded responses through legacy and library connectors.
  - The replay-only dual-path gate is implemented and fails closed against the
    production cache, but currently skips because the archive is absent.
- [x] Compare projected ceramics output for the deterministic synthetic replay.
  Recorded production-output comparison remains part of the unchecked replay
  gate above.
- [ ] Validate checkpoint/resume and result-limit behavior against production
  recordings.
  - Deterministic connector conformance now reopens a fresh connector from the
    emitted cursor and proves the next page does not repeat the bounded entity.
    The PostgreSQL worker gate proves sealed limited publication and recovery;
    production-recording evidence is still absent, so this item remains open.
- [ ] Canary at least one real Shopify source through the installed library.
- [x] Add a configuration-only rollback route.
  - Per-source pipeline overrides select the canary without changing the source
    mapping; explicit `legacy` or removing the override restores the legacy
    route on the next reservation.
- [ ] Switch a stable source mapping after approval.

Exit criterion: **not met**. The connector exists, but no catalogue source has
completed the production migration gate.

## Phase 4 — Generic custom-shop framework

- [x] Add basic sitemap discovery.
- [x] Add JSON-LD product parsing.
- [x] Add strict declarative option models.
- [x] Reuse the shared structured-page collection engine for discovery,
  parser-chain, browser-fallback, and cursor behavior.
  - Applications can inject versioned discovery and parser objects through
    `GenericPagesFactory` and a caller-owned registry without adding callables
    or imports to declarative source options or mutating a global registry.
  - Strategy identity participates in checkpoint compatibility. The connector
    retains cancellation, same-origin validation, product filtering,
    deduplication, finite candidate/page bounds, and strategy partition
    provenance; version drift rejects resume before strategy or transport I/O.
    Default declarative discovery retains its existing options fingerprint.
- [x] Discover robots-advertised sitemaps.
  - The shared page engine issues an optional origin `/robots.txt` request with
    `ROBOTS` purpose, normalizes and deduplicates `Sitemap:` directives, and
    falls back to `/sitemap.xml` when none are advertised.
- [x] Add category/listing discovery and pagination.
  - Success and failure pages retain `category` versus `sitemap` partition
    provenance from discovery through artifact/checkpoint handling; the chosen
    mechanism is already bound into the options fingerprint.
- [x] Add Microdata and OpenGraph parsers.
  - DOM selection, raw JSON-LD/Microdata/OpenGraph extraction, JavaScript-shell
    detection, specification tables, and document-link parsing now live in the
    private parsing layer rather than a connector implementation module. The
    move was byte-identical and changed only three private imports; the full
    322-test release, schema, artifact, installed-consumer, and boundary gate
    passes without expanding the public API.
- [x] Make the declared DOM rules functional.
  - The public generic options accept an explicit `dom` parser and bounded
    tag/id/class/attribute selectors, project them into the shared verified DOM
    parser, and reject missing, unused, descendant, executable, or arbitrary
    selector configuration during model validation.
- [x] Add browser fallback for browser-required pages.
- [x] Translate legacy `pagecrawl` options into `GenericPagesOptions`.
- [x] Fix resume behavior for sequence, resume URL, and multiple entities per
  page.
- [x] Bind user-provided parser identity into checkpoint compatibility.
  - Custom parsers must declare stable bounded `name` and `version` identifiers;
    same-version resume succeeds, while a parser swap rejects the cursor before
    transport I/O.
- [x] Distinguish and test parser-empty, browser-required, blocked, and
  discovery-incomplete results.
  - Bounded sitemap/category status and traversal failures now produce typed,
    retryable incomplete-enumeration pages without invalid resume cursors;
    budget/robots/proxy policy failures still propagate to their middleware
    owner.
  - Ordinary parser-empty markup is `PARSER_UNSUPPORTED`; an unrendered
    JavaScript shell is `BROWSER_REQUIRED`; HTTP and exhausted transport
    failures are `ENTITY_FETCH_FAILED` with bounded `http`/`browser` stage
    metadata. Policy and browser-environment exceptions remain application
    owned.
- [~] Validate all `pagecrawl` source configurations through the library;
  replay, canary, and stable source switching remain.
  - All 21 configurations construct through the translated nested options. A
    native PostgreSQL worker gate proves the `pagecommerce` → `generic-pages`
    identity boundary, sitemap partition, JSON-LD parser chain, atomic page
    commit, lazy optional-browser ownership, and terminal recovery.
- [x] Migrate at least one bespoke source.
  - Axner now composes a versioned application discovery strategy and parser
    with the library's generic page engine, so checkpoint fingerprints,
    cancellation, result limits, sanitization, middleware, and browser fallback
    remain library-owned. One application registry factory is shared by the
    worker and local CLI/probe shell; the legacy adapter remains available for
    rollback. Contract/resume/browser tests and the native PostgreSQL worker
    gate cover the module boundary.
  - Keramik Kraft now uses the same application-plugin boundary. Its recursive
    discovery hands each bounded listing response to the shared page engine
    exactly once, preserving physical request counts while gaining fingerprinted
    checkpoints and lossless `snapshot_offset` resume across multiple cards.
  - Ceramicolours completes the application-plugin set. Its category discovery
    and product parser reuse the generic lifecycle while a typed, origin-bound
    browser action preserves non-linear pack totals. Required offer-budget
    denial remains retryable; other browser/transport evaluation failures use
    the static published price with structured warning telemetry. Native worker
    and local shared-shell tests cover exact stock, replay identity, accounting,
    and rollback registration.
- [x] Provide a Python connector example; install and test it externally.
  - A separate src-layout distribution registers `example-feed` through the
    public entry-point group, uses only injected public contracts, and emits a
    neutral product/variant/offer snapshot through `FakeTransport`.
  - The scraper gate installs the freshly built library wheel and example into
    an isolated environment, explicitly loads the plugin, and runs the public
    connector page conformance assertions.
- [x] Publish declarative and Python custom-shop authoring documentation.
  - The guide covers safe generic-page configuration, plugin boundaries,
    explicit loading, essential intercall tests, and structured secret-safe
    debugging guidance.

Exit criterion: **not met**. Current page-source configurations now validate,
but replay/canary migration remains.

## Phase 5 — Proxy data plane

- [x] Add provider-neutral endpoints, leases, pool protocols, and factories.
  - `ProxyPool` is the provider-adapter protocol boundary. Application-owned
    pool/factory implementations translate provider behavior into neutral
    leases, authorization, rotation, accounting, and cleanup; a second
    provider-specific protocol would duplicate this boundary without adding a
    current capability.
- [x] Add direct, always, fallback, failover, and round-robin routing.
- [x] Add static pool and basic health/cooldown tracking.
  - Static routes fail closed before selection or lease allocation when a
    request requires region, city, or session TTL capabilities that the route
    model cannot prove. Three focused constraint cases and the adjacent
    23-test static proxy slice pass; empty-country semantics are unchanged.
- [x] Add received-byte cap support.
- [x] Add weighted routing.
  - Static routes use validated positive weights and deterministic smooth
    weighted round-robin within the currently eligible provider tier.
- [x] Adapt the catalogue Decodo lease to the neutral pool interface.
  - `PostgresDecodoProxyPool` implements the neutral async pool/token contract
    over the existing Decodo reservation and sticky session. The native
    Shopify worker validates the immutable provider/route/profile snapshot and
    constructs the pool lazily only for active eligible policy.
  - The catalogue now passes one effective `ProxyPolicyConfig` into the
    library. Checked-in country/provider constraints are enforced before
    secrets are read, and source/run byte caps only narrow the immutable
    operator snapshot.
- [x] Add a second provider gateway adapter.
  - An application-owned Webshare residential-backbone adapter projects the
    officially documented `p.webshare.io` HTTP endpoint, lowercase country,
    numeric sticky-session, and `rotate` username semantics into neutral leases.
    The same immutable identity reaches HTTP and browser credentials, rotation
    preserves collection accounting caps, and unsupported kind, geography,
    country casing, session duration, and generated identity fail closed.
  - The adapter intentionally accepts operator-installed secrets and verified
    sticky duration rather than reaching into the Webshare management API. It
    now composes with a durable secret/profile snapshot and fail-closed billing
    authorization, but remains default-off pending operational activation and
    an explicitly approved canary.
- [x] Preserve sticky proxy identity across HTTP and browser requests.
  - Browser dispatch now lives inside each candidate route. A neutral proxy
    browser factory receives the exact selected lease, and its route-scoped
    HTTP/browser transports close before rotation or release. The catalogue
    Camoufox adapter projects `browser_credentials()` into one lazy browser per
    route generation; CDP remains correctly rejected for paid proxy routes.
  - Native Shopify worker composition selects this factory for paid proxy
    browser routes. A production routed canary has not yet been run.
  - The proxy-browser transport preserves browser-context method, query,
    headers, JSON body, status, response headers, final URL, and accounting.
    Browser-required BigCommerce GraphQL therefore remains a POST instead of
    being silently downgraded to rendered GET/HTML; opaque byte bodies fail
    closed before dispatch.
  - Direct native browser work borrows the process-owned backend with the exact
    durable job context. The collection adapter lazily owns, rotates, and closes
    only its session; it never shuts down or silently replaces the selected
    backend. Endpoint and Referer origins are validated before session I/O.
    Direct browser accounting is explicitly a conservative logical-operation
    estimate because the legacy backend does not expose its network waterfall;
    proxy-browser accounting remains callback-derived physical subrequests.
  - Direct and proxy browser routes share fail-closed origin validation. A CDP
    backend selected for a job remains incompatible with a paid proxy route;
    composition raises a placement failure rather than switching backends.
- [x] Add request caps and transmitted/browser byte accounting.
  - Static and PostgreSQL pools authorize request count and expected receive
    plus transmitted bytes atomically before dispatch. HTTP redirects and
    responses reconcile physical requests and transmitted/received bytes;
    retries receive independent authorizations. Routed accounting also clamps
    malformed backend metadata to at least one dispatched request, the
    deterministic transmitted estimate, and observed retained response bytes.
  - The catalogue Camoufox projection obtains a provider-neutral, single-use
    pool token before every allowed physical browser subrequest continues.
    Blocked resources and denied requests do not dispatch; undispatched tokens
    release, while continued, failed, cancelled, and completed requests
    reconcile exactly once with status and byte accounting. The old logical
    browser reservation is skipped only when the concrete backend advertises
    this capability, so an unmarked custom backend retains the fail-closed
    outer authorization.
  - Deterministic callback tests cover authorization order, blocked-resource
    short-circuiting, denial, duplicate finished/failed events, cancellation,
    and close-time reconciliation. The opt-in packaged-browser gate now also
    demonstrates real Playwright callback ordering through an authenticated
    loopback proxy; late resolution remains idempotent.
  - Runtime composition can require this physical-subrequest capability and
    rejects unmarked proxy-browser factories before lease acquisition. Native
    catalogue composition enables the fail-closed requirement explicitly.
- [x] Add application-owned fail-closed PostgreSQL budget authorization.
  - Durable authorization rows reserve capacity under the reservation lock,
    release only proven-undispatched attempts, and reconcile actual counters
    exactly once. The live PostgreSQL concurrency/idempotency test passes and
    worker runtime selection is wired. A small borrowed-pool adapter exposes
    the already-fenced job connection, avoiding nested pool checkout and
    legacy/native double ownership; proxy operations remain serialized.
  - Every physical-attempt authorization now locks and revalidates its owning
    billing cycle before the reservation. Kill-switch activation, failed or
    missing reconciliation, a closed lifecycle, and an expired cycle therefore
    stop existing leases as well as new reservations, with no authorization row
    inserted after the unsafe transition.
  - Attempt authorization also locks and revalidates the exact enabled profile,
    secret generation, enabled/non-retired route, and reservation identity in
    cycle → profile → route → reservation order. Disabling or rotating a
    profile therefore stops its existing leases before attempt insertion;
    focused PostgreSQL tests prove Decodo and Webshare remain isolated.
  - Reservation SQL is provider-keyed and verifies the enabled profile, route,
    allocation, logical identity, and secret generation together. Job snapshots
    persist the profile generation, and both legacy and native workers reject a
    rotated secret rather than silently changing credentials for queued work.
    Focused PostgreSQL tests cover provider-isolated Webshare ledger rows and
    all unsafe-cycle denials; the default-off Webshare runtime now composes the
    same durable authorization boundary.
- [x] Make durable control reconciliation provider-aware.
  - Provider capabilities declare their real reconciliation dimensions:
    Decodo uses day and target snapshots, IPRoyal uses day snapshots, Webshare
    uses exactly one cycle-total snapshot, and providers without a windowed
    usage contract remain unsupported rather than receiving fabricated data.
  - Locks, active-cycle selection, ledger updates, provider snapshots,
    reconciliation requests, notifications, kill-switch revocation, profile
    refresh/actions, and asynchronous finalization are provider-scoped.
    PostgreSQL tests prove Webshare success and failure cannot change Decodo
    safety state or finalize its profiles.
  - Webshare-reported aggregate bandwidth is retained as total bytes only; it
    is not mislabeled as received traffic. Subscription windows beyond the
    provider's 90-day reporting limit and unlimited plans without an explicit
    finite ceiling are refused before a cycle proposal is written.
  - Profile creation now persists the selected provider explicitly. Webshare
    create, rotate, disable, and retirement operations fail before local intent
    because provider-issued credentials and missing status controls do not
    satisfy those mutation contracts.
- [x] Enforce provider/profile/route integrity in PostgreSQL.
  - An append-only, idempotent migration backfills an explicit route provider
    from its profile, then constrains route, allocation, reservation, probe,
    and reconciliation-request identities with composite unique and foreign
    keys. Provider changes and route reparenting can no longer silently move
    existing durable identities across billing ledgers.
  - New reservations require both profile and route IDs. Their provider,
    logical profile name, route ownership, optional probe identity, and later
    reconciliation requests must all describe the same durable graph; runtime
    reservation code rejects missing identities before opening a transaction.
  - Existing legacy reservations without IDs are not guessed or rewritten.
    The new non-null identity check is installed `NOT VALID`, so it protects
    all new writes while an operator can inventory any historical rows before
    a later validation migration.
  - PostgreSQL migration tests prove clean old-schema route backfill is
    idempotent and dirty cross-provider allocations refuse the upgrade without
    recording it. Fresh-schema regressions assert each named relationship
    constraint while retaining valid Decodo and Webshare reservations.
- [x] Add provider-neutral durable accounting composition.
  - `PostgresReservedProxyPool` decorates a provider data-plane pool without
    importing vendor behavior into the neutral library. It binds the immutable
    provider, profile, route UUID, and secret generation to one PostgreSQL
    reservation, then makes durable authorization the final gate before every
    physical HTTP or browser request.
  - Provider-local and durable attempt tokens reconcile the same outcome once;
    reporting remains health-only. Rotation preserves one reservation, pending
    attempts block rotation and release, and partial acquisition, rotation,
    authorization, reconciliation, and close failures remain fail closed with
    retryable ownership.
  - Focused intercall tests compose the decorator with the Webshare gateway and
    prove exact reservation arguments, denial cleanup, accounting without
    double counting, sticky rotation, invalid replacement cleanup, and one
    durable authorization per browser subrequest.
  - A focused PostgreSQL 17 gate proves the full Webshare acquire, authorize,
    reconcile, report, and close lifecycle retains exact provider/profile/route
    identity and changes only the Webshare ledger. Activating the Webshare kill
    switch denies an existing lease before an attempt row is inserted.
- [x] Resolve one immutable Webshare route in the native worker runtime.
  - The provider-keyed resolver retains the current single-route job snapshot;
    it does not query live alternatives or fabricate cross-provider failover.
    Webshare has a separate secret path and default-off data-plane capability,
    while Decodo keeps its existing configuration and behavior.
  - Before secret or database access, the resolver checks the selected provider
    against source preferences plus geography, protocol, state/city, sticky
    session, and bounded immutable route fields. It then binds the strict
    provider/name/generation secret, verified capabilities, route UUID, and
    durable profile UUID into the composed gateway and PostgreSQL pool. Merely
    resolving the runtime remains lazy and opens no reservation.
- [x] Define the operator-managed Webshare gateway secret contract.
  - A separate, read-only schema-v2 loader now accepts only provider-bound
    `webshare/<logical-name>` records with strict generations, secret-aware
    credentials, the verified HTTP backbone, explicit country/sticky-session
    capabilities, duplicate-key rejection, bounded private regular-file reads,
    provider-keyed lookup, and immediate redaction values.
  - The contract is deliberately isolated from the legacy Decodo secret store;
    loading a Webshare secret neither creates a durable profile nor enables
    traffic.
  - A whole-file local store now installs and removes provider-keyed records
    with explicit generation compare-and-swap. It uses a private no-follow
    lock with bounded acquisition, one validated read under that lock, canonical revalidation, a private
    same-directory temporary file, file and directory `fsync`, atomic replace,
    and cleanup. Focused tests cover stale and concurrent rotation, unrelated
    record preservation, guarded removal, replacement failure, and symlink
    refusal without exposing credential values.
  - A separate recent-admin operation coordinates strict request validation,
    non-secret idempotency metadata, durable intent state, local generation CAS,
    audit/event emission, and safe replay. It never calls a provider-management
    API and never writes credentials, fingerprints, endpoints, or capabilities
    to PostgreSQL.
- [x] Persist recoverable profile-secret operations without secret material.
  - An append-only migration adds one intent per mutation operation with exact
    provider/profile/logical-name/cycle foreign keys, create-versus-rotation
    generation constraints, prepared/installed/completed/failed states,
    timestamp consistency, and at most one active intent per profile. The table
    contains no credentials, credential fingerprints, endpoints, or provider
    capabilities and is included in proxy role ownership.
  - Migration ordering, idempotence, identity foreign keys, generation/state
    checks, timestamp rules, and active-intent uniqueness pass focused live
    PostgreSQL tests.
  - A typed control service now prepares creates or rotations under provider,
    cycle, profile, allocation, and reservation locks; disables and drains
    active reservations before creating an intent; performs strict local file
    CAS outside PostgreSQL transactions; and finalizes only after reacquiring
    locks and rechecking cycle, allocation, database generation, and installed
    file generation.
  - Generation-only recovery finalizes a file-ahead-of-database crash, safely
    completes an already-applied target generation, rebinds an expired intent
    only to one safe current cycle with the exact profile allocation, retains a
    disabled installed state with stable remediation when allocation is absent,
    and fails unrelated database/file generations without inspecting values.
  - A session advisory fence spans prepare, off-loop file CAS, and finalize.
    Repeated cancellation cannot release it while the file thread still runs;
    unlock is verified and an uncertain session is discarded. Draining is a
    terminal 202 that requires a new key after leases close, while installed
    cycle remediation resumes under the same key.
  - Reservation close, profile rotation, and stale cleanup share cycle-first
    lock order. Timed-out probes are cancelled after a bounded interval, late
    actuals settle monotonically without resurrecting terminal state, and both
    cleanup and settlement advance application accounting and the kill switch.
- [x] Add default-off control-owned Webshare secret staging.
  - The authoritative Podman runtime stage creates a private gateway directory
    and optionally bootstraps the bounded Infisical export byte-for-byte as a
    mode-`0600` file. An absent bootstrap leaves the file absent so generation
    one can be created, and later stage rebuilds preserve a control-rotated
    owner-only store instead of replacing it with stale bootstrap material.
  - Compose bootstraps the same directory through its initializer. Control is
    its sole read-write consumer; plain and browser workers mount it read-only,
    while service, dispatcher, and explorer receive no credential mount. The
    worker entrypoint uses bounded non-blocking/no-follow
    reads, strips management-secret variables, atomically stages a
    catalogue-owned mode-`0400` copy with file/directory `fsync`, and removes
    stale state for absent, empty, non-regular, symlink, FIFO, oversized, or
    failed replacements. It never changes the independent enable flag.
  - Presence remains inert and production enablement is still false. A rootless
    execution contract now maps both workers directly to catalogue UID/GID,
    retains `DropCapability=all`, and validates owner-only bootstrap mode 0600
    and atomically rotated mode 0400 without privileged calls; rootful Compose
    retains private staging and privilege drop. Directory mounts make atomic
    replacements visible without stale bind-mounted inodes. The executable
    Docker fallback proves the actual worker entrypoint can observe a live
    generation replacement with the same identity and read-only constraints.
    A real rootless Podman/Quadlet activation remains an operational gate.
- [x] Bound health state and add reason-specific health counters.
  - Process-local state is keyed per provider/endpoint/target, capped with LRU
    eviction, and exposes detached read-only snapshots. Cooldowns grow
    exponentially to a fixed ceiling without unbounded exponent work.
  - Four typed failure classifications retain cumulative counters while
    success resets only consecutive failures and cooldown. A pool intercall
    test proves a classified failure makes the affected route ineligible and
    selects a healthy replacement.
- [x] Remove possible double-counting of routed failures.
  - Outcome reporting now owns health only; authorization reconciliation owns
    usage only, and rotation does not apply a second health penalty.
- [x] Verify failover preference semantics independently of round-robin state.
  - Ordered provider tiers select the first healthy preferred provider without
    inheriting cursor state from unrelated requests.
- [x] Add local fake HTTP/SOCKS proxies, concurrency tests, browser credential
  routing, and multi-provider failover tests.
  - A dependency-free loopback HTTP proxy now exercises the real HTTPX proxy
    factory and routed pool. It proves Basic credential decoding, absolute-form
    URL/query and Host routing, route metadata, request/byte accounting,
    outcome reporting, and lease cleanup.
  - A dependency-free authenticated SOCKS5 endpoint exercises the same real
    HTTPX factory and proves credential negotiation, CONNECT target/port,
    origin-form requests, target-header containment, accounting, and cleanup.
  - A real two-provider loopback gate crosses `MiddlewareTransport` into
    `RoutedTransport`: provider one returns 429, health-driven rotation selects
    provider two, distinct credentials reach only their intended gateway, and
    both physical attempts reconcile and close independently. Sticky-route
    concurrency and browser credential projection retain focused protocol
    tests.
- [x] Prove credentials cannot enter logs, errors, checkpoints, recordings, or
  telemetry.
  - The library emits no direct logs; all observability crosses the
    secret-scrubbing telemetry boundary, with direct retry, proxy lifecycle,
    URL, header, query, and body tests.
  - Retained diagnostics and transport/fixture errors are bounded and redacted;
    recordings reject credential-bearing URLs and headers without retaining
    malformed or oversized input in exceptions.
  - Schema-v1 library and catalogue rollback checkpoints reject unsafe
    credential-bearing cursors before persistence. Validation errors retain no
    raw credential, and invalid persisted identity produces a reason-coded
    new-lineage restart without decoding a compatibility envelope.

Exit criterion: **met locally**. One scrape fails over across two provider
adapters; sticky identity crosses HTTP and browser work; request and byte caps
stop new physical dispatch; credential-containment gates pass; and the existing
catalogue PostgreSQL safety boundary remains fail closed. The adapter boundary
is the neutral `ProxyPool` protocol plus application-owned factories rather
than an additional unused provider protocol. Real Quadlet activation and
production routed canaries are deployment/cutover evidence and remain pending;
they do not reopen the Phase 5 library deliverables.

## Phase 6 — Remaining framework connectors

| Connector | Library implementation | Factory | Synthetic tests/parity | Replay | Canary | Stable source switch |
|---|---:|---:|---:|---:|---:|---:|
| WooCommerce | Yes | Yes | Yes | No | No | No |
| PrestaShop/Sio2 | Yes | Yes | Yes | No | No | No |
| BigCommerce | Yes | Yes | Yes | No | No | No |
| Wix | Yes | Yes | Yes | No | No | No |
| Shopware | Yes | Yes | Yes | No | No | No |
| Starweb | Yes | Yes | Yes | No | No | No |
| NitroSell | Yes | Yes | Yes | No | No | No |
| SumUp | Yes | Yes | Yes | No | No | No |

Cross-cutting work:

- [x] Extract implementations and strict connector-owned options.
- [x] Register built-in factories.
- [x] Add focused deterministic tests.
  - BigCommerce token discovery now preserves the configured rendered fallback
    after a direct HTTP denial. A regression proves direct 403 → rendered token
    discovery → browser-context GraphQL POST with exact origin, referer, JSON,
    and secret-containment semantics.
  - BigCommerce now has an explicit native-route capability marker and a
    PostgreSQL worker intercall gate. It proves direct 403 retries, rendered
    token discovery, browser-context GraphQL POST, neutral projection/page
    commit, secret containment, exact session cleanup, backend retention, and
    completed-lineage recovery without a second runtime or browser resolution.
    This is synthetic migration evidence; no production source was switched.
  - PrestaShop now has explicit native-route metadata carrying its stable
    declared sitemap partition. Its PostgreSQL worker gate proves sitemap and
    product requests, neutral projection/page commit, no browser resolution,
    and terminal recovery.
  - Sio2 has separate native evidence rather than inheriting approval solely
    from its PrestaShop connector alias. Its gate exercises category discovery,
    product-card link scoping, the stable category partition, neutral product
    parsing, `source_policy="sio2"`, atomic ceramics publication, the fenced
    compatibility loader, and the durable `clay_body`/material-kind row.
  - Wix proves optional browser fallback through the native worker: direct
    sitemap discovery and a direct JavaScript shell lead to exactly one
    collection-scoped rendered request with the worker-owned backend retained.
  - Shopware, Starweb, and NitroSell carry explicit category partitions through
    native lineage and exercise their JSON-LD/OpenGraph parsing surfaces.
    Optional browser capability is composed lazily for Shopware and Starweb;
    NitroSell's explicit `render=false` resolves no browser at all.
  - SumUp carries its default product sitemap partition through native lineage,
    parses the Next.js flight payload, and emits both sale and regular price
    observations. Its optional browser is borrowed but never opened when the
    server response is sufficient.
- [x] Map flat source configuration; validate every configured source.
  - One application-composition invariant loads all 88 checked-in sources,
    projects each native source definition, and constructs it through the actual
    built-in-plus-plugin registry while checking connector name/version parity.
- [x] Drive every connector through the full reusable conformance suite.
  - The shared harness now verifies connector identity/capabilities, contiguous
    and unique page sequences (including resumed sequences), terminal-page
    placement, result-limit bounds, enumeration completeness, snapshot
    sanitation, source/connector identity, checkpoint fingerprints,
    cancellation before transport I/O, and caller-supplied secret sentinels.
  - Shopify, generic-page, WooCommerce, PrestaShop, BigCommerce, Wix,
    Shopware, Starweb, NitroSell, and SumUp paths use the contextual harness.
    Focused specialized cases also prove pre-I/O cancellation and rejection of
    unsupported incremental collection without an exhaustive low-value matrix.
- [~] Run recorded-response replay and ceramics projection comparisons.
  - The first page-based gate is implemented for `keramikbedarf-online` and
    Shopware, but cannot produce evidence until the raw archive is restored.
- [~] Review request counts and byte estimates.
  - Focused traffic profiles now pin exact logical request order, purpose,
    browser hint, and byte estimate for WooCommerce, PrestaShop/Sio2,
    BigCommerce, Wix, Shopware, Starweb, NitroSell, and SumUp. BigCommerce also
    proves an origin-equal token page does not multiply direct/rendered probes.
    The compact five-module slice passed 56 tests. Empirical recorded-response
    totals remain pending with the external archive.
- [~] Review direct, browser, and proxy behavior.
  - The same profiles prove direct-only WooCommerce, PrestaShop/Sio2,
    Shopware, Starweb, NitroSell, and SumUp paths; BigCommerce direct-to-rendered
    token and browser-GraphQL fallback; and Wix direct-to-rendered entity
    fallback. Provider routing remains connector-neutral and is covered at the
    middleware/runtime boundary. Recorded and live route evidence is pending.
- [ ] Canary representative sources.
- [ ] Document intentional differences and switch stable source keys.

Exit criterion: **not met**. Code extraction and native synthetic worker gates
cover every listed framework connector; recorded replay and real source
canaries have not run.

## Phase 7 — Catalogue cutover

- [x] Replace catalogue `RUNTIME_ADAPTERS` connector construction with
  `mb_commerce_scraper.ConnectorRegistry` and application-owned adapters.
  - `RUNTIME_ADAPTERS` now projects only immutable canonical connector/options,
    routing, and ceramics metadata. Connector construction and version
    authority belong exclusively to the built-in-plus-application registry.
    A typed compatibility boundary delegates those registry-built connectors
    and decoded library checkpoints into the existing atomic dataset pipeline,
    revalidating the still-distinct page envelope and rejecting catalogue
    checkpoints at the boundary.
- [~] Run neutral collection before ceramics projection for all migrated
  sources.
  - Shopify, all eight extracted framework families, generic pages, Axner,
    Ceramicolours, and Keramik Kraft now run
    registry → neutral pages → existing atomic dataset projection when
    explicitly selected.
    Shopify and WooCommerce accept the scheduled
    daily-price intent: connectors produce a coherent full neutral snapshot and
    the application-owned projection marks ceramics rows so the compatibility
    loader preserves weekly descriptive enrichment. Ceramicolours uses a
    typed browser-evaluation action for pack offers, with static-price fallback
    and warning telemetry when optional enrichment is unavailable.
- [x] Introduce layered source configuration.
  - One frozen application-owned configuration now composes the validated
    source definition, exact effective source/run fetch policy, fail-closed
    proxy eligibility, selected datasets, and projection policy. The native
    worker, runtime opener, and proxy resolver consume this same object rather
    than independently reconstructing policy.
  - Contract tests preserve the established strictest-policy precedence,
    distinguish omitted datasets from an invalid explicit empty selection,
    and prove an ordinary run can disable but never enable paid routing.
- [x] Retain a tested compatibility loader for the current source file.
  - `SourcesFile.load()` remains the accepted strict `sources.json` entry point
    while `source_projection` deterministically splits it into typed connector,
    browser, crawl, and dataset ownership. The checked-in file validates, a
    golden projection covers every source, and lossless/default-preservation
    tests prevent the compatibility layer from changing legacy scraper input.
- [x] Remove connector-specific worker/runtime conditionals.
  - Native eligibility, browser ownership, request partitions, and
    dynamic-partition behavior are now explicit immutable metadata on the
    application runtime plan. Worker selection no longer contains a connector
    allow-list or connector-name partition branches.
  - `ConnectorRuntimePlan` is now data-only and emits canonical native registry
    names and option schemas. Executable builders, duplicated connector
    versions/partitions, legacy-adapter names, catalogue connector imports, and
    the `pagecommerce`/`keramik_kraft` name switches were removed from the
    composition metadata and source-definition boundary.
  - The former Shopware, SumUp, Starweb, and NitroSell specialized Fetcher shell
    was deleted. Both its retained direct aliases and generated `library_*`
    aliases now enter `LibraryConnectorScraper`; the stable unsuffixed legacy
    implementations remain distinct and available for production rollback.
    An AST architecture test prevents executable construction dependencies
    from returning, and an all-source test validates every checked-in canonical
    connector/options pair through the application registry.
  - Static browser-worker placement now comes from scraper-family adapter
    capabilities rather than a hard-coded pair of source IDs. A PostgreSQL
    regression uses a renamed Ceramicolours source to prove scheduling follows
    declared behavior; response-dependent browser escalation remains at the
    worker boundary.
  - Library registry membership deliberately does not grant worker
    eligibility: remaining registered connectors stay on their compatibility
    routes until explicit application metadata and their required evidence are
    added. This is a generic approval boundary rather than storefront-specific
    worker logic.
- [x] Map library telemetry to catalogue observability.
  - Sanitized Fetcher bridge protocol events map to catalogue DEBUG logging;
    worker summaries retain direct/impersonated/browser/proxy route counters
    and HTTP/browser byte estimates across direct and fallback Fetchers.
    The library-to-pipeline boundary now emits sanitized collection start,
    page completion, typed diagnostic, task interruption, failure, and
    collection completion events correlated by source, connector, connector
    version, and collection ID. The same immutable ID now crosses request,
    library lifecycle, and catalogue projection events. Native terminal request
    events also aggregate physical
    direct/proxy/browser requests and HTTP/browser byte estimates for the
    worker summary without double-counting retries. The application adapter
    projects explicit lifecycle/page/diagnostic/failure levels (`INFO`,
    `DEBUG`, `WARNING`, `ERROR`) and defaults undeclared protocol detail to
    `DEBUG`. Safe scalar protocol fields are also attached as events to the
    active catalogue job trace; trace observer failure cannot affect accounting
    or collection correctness. Each native physical attempt now owns a
    `commerce.request` child span from `request.started` through completed,
    failed, or retry classification, with terminal safe attributes and events.
    Backend cancellation emits a typed terminal failure before budget cleanup,
    preventing leaked spans. The same terminal events project bounded
    source/host/outcome counts, duration, HTTP errors, and browser-use counters
    into the existing catalogue metrics without double-counting a
    classification-only retry.
  - Connector-originated events inherit immutable collection, source, and
    connector identity, so plugin fields cannot break trace correlation.
    Middleware request events include purpose, named priority, required/browser
    policy, byte estimate, and stable browser action ID; scripts and payloads
    remain excluded.
  - Local CLI/probe native requests, connector events, lifecycle/page
    events, and ceramics projection now share one `local:<uuid>` collection ID.
    The retained parity bridge exposes the same safe policy fields and browser
    action ID while omitting scripts, selectors, headers, and payloads.
- [x] Make worker, CLI, and probes share one composition root.
  - A typed application-owned `CatalogueCommerceRuntime` now owns the connector
    registry and validates source, request, and proxy-route identity before it
    constructs collection resources. The worker retains one process-scoped
    root and opens collection-scoped cache, transport, browser, connector, and
    telemetry lifecycles through it. The former native opener is only a
    compatibility wrapper around that root, eliminating its duplicate
    transport construction path.
  - Focused contract tests cover fail-fast identity validation and the real
    connector → runtime → middleware → fake transport intercall, including
    collection correlation and exactly-once scraper lifecycle cleanup.
  - Worker construction, local dump, and `catalogue-probe --pipeline
    connector_canary` now enter the same native middleware/runtime composition.
    The local runner injects a small runnable-scraper protocol, so canary tools
    construct no legacy Fetcher/session and retain stable source keys. Local
    policy is narrowed to direct-only routing, a shared lazy browser is owned
    by the local session, and cache/transport accounting is projected back into
    the established result contract. The legacy route and the generated canary
    aliases remain available only for explicit rollback and parity tests.
  - CLI routing tests fail if canary dump/probe touch legacy session or scraper
    construction, and a reciprocal regression test proves legacy mode cannot
    enter the native local session. Direct/cache/browser integration tests
    cover canonical material scope, per-collection accounting, replay, and
    exactly-once browser cleanup.
  - `connector_canary` now fails before database work when an approved native
    route or registry entry is absent and has no unreachable legacy session or
    lineage arm. The normal legacy pipeline remains intact for rollback.
  - Local canary aliases and their reverse stable-scraper lookup are generated
    from `ADAPTER_CAPABILITIES`; an all-source invariant checks capabilities,
    aliases, registry entries, runtime plans, and rollback selectors together.
- [x] Add a per-source legacy/library route or feature flag for canary and
  rollback.
  - `source_settings.params.pipeline` overrides the run default for only that
    source. PostgreSQL tests prove both explicit `legacy` and removal of the
    override restore the legacy route on a later reservation.
- [ ] Move all production sources to the library contract.

Exit criterion: **not met**. Shopify and all extracted framework families are
migrated in the native canary route, but production parity/canary evidence is
missing. Generic pages and all three application-owned bespoke plugins are
also native; stable source keys still remain on their rollback-compatible
legacy defaults until promotion evidence exists.

## Phase 8 — Legacy removal and 1.0 stabilization

- [ ] Complete the agreed observation window for each migrated source.
- [ ] Remove duplicate legacy framework connectors.
- [ ] Remove `_connector` aliases and compatibility re-exports after the
  deprecation window.
  - The pure commerce-model shim has no remaining production consumers and is
    guarded by exact object-identity tests, but remains importable until that
    window closes. Real legacy contracts, connectors, canary aliases, Fetcher
    compatibility, and source loading remain untouched for rollback.
- [x] Publish connector-authoring, proxy-integration, and migration guides.
  - The guides define dependency/lifecycle ownership, essential intercall
    tests, structured tracing and credential boundaries, provider authorization,
    replay/shadow/canary evidence, promotion, and configuration-only rollback.
- [x] Document supported public imports and semantic-versioning policy.
  - A reviewed manifest fixes 224 exports across nine supported modules. The
    policy separates package SemVer from independently versioned serialized
    contracts and records pre-1.0, deprecation, and Python-support rules.
- [x] Add changelog and release automation.
  - The library changelog starts with an explicit Unreleased section. A
    `commerce-scraper-vX.Y.Z` tag on `main` runs every library gate, verifies
    source/package/wheel/tag/changelog parity, and publishes only the verified
    wheel and sdist to a GitHub release; no undefined package registry or
    credential flow is assumed.
- [x] Add final API and schema compatibility tests.
  - The installed-wheel verifier compares exact `__all__` and every attribute
    to `public-api.toml`, checks metadata/version parity and base/http/dev extras,
    and proves HTTPX stays optional. Frozen version-one JSON schemas and
    representative payloads retain byte-for-byte generation checks.
- [x] Validate a clean external project using a built-in connector, custom
  connector, fake transport, and configured proxy pool.
  - The isolated project installs the built wheel and external connector,
    explicitly loads its entry point, runs built-in Shopify and the plugin
    through a consumer-owned proxy factory, and crosses middleware retry and
    rotation. It proves the public fake transport remains unused under
    always-proxy routing, exact route cleanup, lease release, and credential
    containment outside the source tree.

Exit criterion: **not met**.

## Cross-cutting quality and security

- [x] Core import avoids catalogue, PostgreSQL, NATS, browser, and provider SDK
  dependencies.
- [x] Explicit built-in registration avoids import-time plugin discovery.
- [x] Test valid, duplicate, invalid, and broken entry-point plugins.
  - Discovery remains explicit; tolerant loading isolates failures while strict
    loading raises with the original cause. Failure text includes package,
    entry point, and exception type without raw plugin exception content.
- [x] Expand conformance checks to cover capabilities, cancellation, resume,
  result limits, checkpoint dimensions, enumeration completeness, and broad
  secret detection.
  - Reusable assertions cover each listed invariant, including resumed
    sequence offsets and explicit secret sentinels. Every built-in family now
    exercises the harness, with focused unsupported-capability and
    cancellation cases for the shared specialized engine.
- [x] Install the built wheel in a clean environment and run a fake scrape.
- [x] Test every optional extra independently.
  - The installed-wheel verifier compares declared and artifact extras, then
    installs base, `http`, and `dev` independently. HTTP construction/SOCKS and
    each declared development tool receive focused smoke checks; future extras
    enter the isolated matrix automatically.
- [x] Verify installed `py.typed` package data.
- [x] Add a common bounded raw-payload and diagnostics sanitizer.
  - Platform extensions are structurally redacted and bounded at model,
    shared-page connector, and runtime/plugin egress boundaries.
  - Diagnostic messages, URLs, entity IDs, and metadata are now centrally
    bounded/redacted and revalidated at runtime/plugin egress, including
    unvalidated plugin construction paths.
  - Production responses, cache retention, recording archives, fake response
    bodies/errors, JSON decoder context, and catalogue compatibility bridges
    now enforce explicit limits without placing raw bodies in diagnostics.
- [x] Redact authorization, cookies, tokens, secrets, proxy userinfo, all URL
  query values, and URL fragments structurally.
- [x] Stop emitting raw sensitive URLs, headers, and bodies in telemetry.
- [x] Isolate telemetry observer failures from collection correctness.
- [x] Add correlated request tracing with purpose, attempt, outcome, duration,
  route, and provider context across middleware retries.
  - One bounded request identity now crosses request/retry events, direct and
    proxy rate gates, typed browser dispatch decisions, proxy
    acquire/rotation/outcome, and terminal events. Retry events expose the
    selected backoff; connector version is present in child events.
  - A logical `request.accepted` event now precedes robots, cache, budget,
    rate-limit, and physical-attempt decisions. Cache hits, robots denials, and
    budget denials therefore retain the same request correlation even when no
    network span is opened.
  - Catalogue terminal accounting classifies retrying HTTP 403, 429, and 5xx
    outcomes by status instead of collapsing them into transport errors.
  - Disabled telemetry avoids UUID/context allocation. Invalid event names are
    replaced without echoing them, and limiter release, budget reconciliation,
    and cache-write failures emit terminal request failures so tracing spans
    cannot remain open.
- [x] Add lifecycle events.
  - Rate-limit wait, connector page, diagnostics, collection completion, and
    interruption are implemented with collection correlation.
  - Proxy acquire, rotation, outcome/accounting, failure, and release events
    include sanitized route/provider context, reason codes, timings, and byte
    counts without credentials or payload bodies.
  - Runtime-owned HTTP and browser cleanup emits typed start, completion, and
    failure events with resource and elapsed-time context. Browser cleanup is
    still attempted and traced when HTTP cleanup fails.

## Immediate implementation queue

1. Restore or supply the absent raw Shopify response archive, then run the new
   cache-conditional legacy/library replay gate and projected-output shadow
   comparison. The checkout currently contains output goldens, only two
   synthetic `shop.test` cache entries, and no
   `catalogue-dump/cache-archive.json`; this is not production replay evidence,
   and its explicit skip must not be reported as recorded parity.
2. Execute a limited production Shopify canary using the tested per-source
   rollback selector after recorded parity passes.
3. Restore the `keramikbedarf-online` archive, run its implemented Shopware
   projected-output parity gate, review request/discovery totals, and then run
   a limited page-based canary.
4. Run a limited, explicitly approved routed Shopify production canary. The
   local gates now cover the real outer worker, native runtime, Shopify,
   middleware, immutable Webshare route, durable attempt accounting, summary
   and artifact persistence, cleanup, and terminal recovery without legacy
   lease ownership. The live Camoufox callback gate independently covers
   physical browser authorization because Shopify declares browser capability
   `never`.
5. Run BigCommerce recorded replay and projected-output comparison, then a
   limited browser-capable canary with the tested source-level rollback route.
6. Run representative PrestaShop and Sio2 sources through recorded replay,
   projected output comparison, and limited canaries with independent rollback.
7. Run a real rootless Podman/Quadlet activation/readability smoke, then a
   default-off native worker integration and explicitly approved Webshare
   canary. The recent-admin import/rotation endpoint, durable replay,
   control-exclusive write mount, worker read mounts, HTTP resolver-to-wire
   integration, Docker rootless-identity fallback, and live Camoufox callback
   gate now pass. Keep Webshare out of production composite selection until
   those remaining operational gates pass.
8. Migrate configured production sources incrementally through the existing
   library registry route, and remove legacy implementations only after their
   observation windows.

## Update procedure

After each implementation batch:

1. Update the relevant checkboxes and connector matrix.
2. Record the verification commands and counts in **Current baseline**.
3. Record any intentional plan divergence and its rationale below.
4. Do not mark replay, canary, stable switch, or removal complete without the
   corresponding evidence.
5. Update `Last reviewed` and add the implementing commit to the baseline.

## Accepted plan deviations

- B1 resolves the previously documented parsing drift in favor of the shared
  helper behavior: specification names may contain up to 99 characters, the
  broader JavaScript-shell marker set is canonical, missing metadata returns
  `""`, booleans are never decimal amounts, and only BigCommerce and
  WooCommerce request strict absolute-HTTP(S) origin validation. These choices
  retain the intended common contract while preserving connector validation
  semantics.

- The plan names a separate provider-adapter protocol. The implemented neutral
  `ProxyPool` lifecycle plus application-owned transport/browser factories is
  the provider-adapter boundary: Decodo and Webshare both implement it without
  provider grammar entering the library. A second provider-specific protocol
  would duplicate acquisition, authorization, accounting, rotation, and
  cleanup contracts without enabling a current use case. This deviation is
  accepted under the plan's provider-neutrality and minimal-abstraction goals.
