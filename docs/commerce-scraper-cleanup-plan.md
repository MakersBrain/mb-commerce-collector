# Commerce Scraper Cleanup and Refactoring Plan

Last reviewed: 2026-08-24
Branch: `feat/commerce-scraper-library`

## What this plan is

A sequenced plan for the code-quality debt found by a four-angle review (reuse,
simplification, efficiency, altitude) of the branch diff against `main`
(273 files, ~69k changed lines). It covers duplication, wasted work, and
misplaced layering — not correctness defects.

It **complements** `commerce-scraper-library-status.md` and does not restate
it. That document owns the migration itself (Phases 0-8) and the evidence gates
for retiring legacy code. This plan owns the cleanup that the migration leaves
behind, and is explicit about which items the migration gates block.

The organizing constraint is `pyproject.toml: version = "0.1.0"` with every
entry still under `## [Unreleased]` in the changelog. Anything that changes the
library's public surface is nearly free today and becomes a breaking change
after 1.0. Anything that touches the legacy rollback path is blocked by ADR
0002 until per-source observation windows close. Those two facts set the
ordering: **public surface first, internals second, gated deletions last.**

## Status conventions

- `[ ]` not started
- `[~]` partially done
- `[x]` done, with the verifying command recorded

---

## Track 0 — Unblock the workspace (do first)

Neither item is a cleanup. Both make the rest of the plan verifiable, and
without them later tracks will produce false greens.

- [x] **0.1 Fix the 13 failing proxy tests on this branch.**
  `tests/test_commerce_scraper_reserved_proxy.py` (9) and
  `tests/test_commerce_scraper_webshare.py` (4) fail on a clean checkout of
  this branch, independent of any cleanup work. Root cause is a contract
  mismatch, not a flake: `commerce_scraper_webshare.py:162` builds a
  `ProxyRequest` carrying `session_ttl_seconds=900`, and the library's static
  route refuses it — `static.py:171`, *"static proxy routes cannot prove
  requested constraints: session_ttl_seconds"*.
  This is the paid-traffic money path and it is the same code Track D
  restructures. Fix or explicitly quarantine it before D1/D2, or the refactor
  will be landing on top of red.
  **Fixed.** `WebshareGatewayPool` proves the TTL capability itself --
  `_validate_request` checks it against the configured gateway capability and
  `_project` honors it via the sticky identity's `expires_at` -- then forwarded
  the un-discharged request to the composed pool. A new `_discharged()` drops
  constraints the adapter has already proven; the static route keeps refusing
  what it genuinely cannot prove. `DecodoDataPlanePool` carries the same seam.
  *Verified:* `uv run pytest -q tests/test_commerce_scraper_reserved_proxy.py tests/test_commerce_scraper_webshare.py` -> 22 passed.

- [x] **0.2 Make `catalogue-dump` test against the library source.**
  `catalogue-dump/.venv/.../mb_commerce_scraper/` is a *non-editable copy*
  installed on 2026-08-22. Edits to `commerce-scraper/src` do not reach the
  catalogue suite, so every cross-package refactor in Tracks A-D would be
  validated against a snapshot. Set
  `mb-commerce-scraper = { path = "../commerce-scraper", editable = true }` in
  `[tool.uv.sources]`, or re-sync the package in the pre-test step.
  This does not affect the installed-wheel gates, which deliberately test the
  built artifact and must keep doing so.
  **Fixed.** `editable = true` added to the path source. Imports now resolve to
  `commerce-scraper/src`, so the catalogue suite exercises library edits.
  *Verified:* `uv run python -c "import mb_commerce_scraper.transports.url_policy as m; print(m.__file__)"` resolves to the source tree.

---

## Track A — Public library surface (must land before 1.0)

Each item removes a compatibility affordance that has no consumer, because the
library has never been released. Each also edits `public-api.toml`, which the
installed-wheel verifier compares attribute-by-attribute — so sequence A1 and
A2 back to back and update the manifest once, rather than letting two branches
race on the same file.

- [x] **A1 Delete the legacy proxy-configuration path.**
  `runtime/client.py` accepts `proxy_policy` *and* a parallel
  `routing` / `proxy_maximum_requests` / `proxy_maximum_bytes` triple, guards
  them with three mutual-exclusion checks, threads a
  `_legacy_proxy_configuration` flag into `open_connector`, and keeps a
  `_legacy_proxy_policy()` converter plus three derived attributes that only
  the tests read. The sole in-repo caller of the legacy form is the library's
  own `build_http_scraper`, which forwards it verbatim; the application passes
  `proxy_policy`.
  Fold in the same file's twin enums: `RoutingMode` and `ProxyMode` have
  identical members and exist only to be converted into each other by `.value`,
  which is also why `client.py` and `proxy/transport.py` compare
  `.value == "never"` / `"required"` as strings the type checker cannot check.
  *Removes:* 3 constructor parameters, 3 derived attributes, 2 converter
  functions, 3 validation branches, 1 flag, 1 mirror enum, `ProxyRouting`.
  *Touches:* `runtime/client.py`, `runtime/builder.py`, `proxy/routing.py`,
  `proxy/transport.py`, `public-api.toml`, `CHANGELOG.md`,
  `tests/test_runtime.py:109-115`.
  **Completed.** `ProxyPolicyConfig` now flows unchanged through the runtime
  and `RoutedTransport`; the parallel constructor arguments, derived runtime
  attributes, conversion helpers, duplicate enum, and `ProxyRouting` model are
  removed. Direct transport tests and the external consumer use the canonical
  policy too.

- [x] **A2 Remove legacy checkpoint decoding.**
  `models/checkpoints.py` carries `LegacyConnectorCheckpoint`,
  `LegacyCheckpointRestartReason`, `CompatibleLegacyCheckpoint`,
  `LegacyCheckpointDecodeResult`, `RestartLegacyCheckpoint` and
  `decode_legacy_checkpoint` — a migration path for checkpoints "persisted
  before collection fingerprints existed" — and `public-api.toml` promotes all
  six to the permanent public API. No library user can hold version-0
  checkpoints; the only holder is the catalogue database, and the only callers
  are `ops/library_lineages.py` and `ops/commerce_scraper_adapter.py`.
  Move the models and the decoder to `catalogue-dump` beside the lineage table
  they read. `collection_fingerprint` is the genuinely shared primitive and
  stays public.
  *Removes:* 6 exports from a permanent contract surface (~120 lines).
  *Touches:* `models/checkpoints.py`, `models/__init__.py`, `public-api.toml`,
  `tests/test_checkpoint_compatibility.py` (moves), both catalogue consumers.
  **Completed.** The decoder and all six compatibility contracts were removed
  from both packages. Commerce-scraper lineages now reconstruct the library's
  schema-v1 `ConnectorCheckpoint` directly from durable row identity; a
  version-0 or malformed cursor restarts rather than being upgraded. The
  library retains only the shared schema-v1 checkpoint and
  `collection_fingerprint` contracts.
  *Verified together with A1:* `make scraper-check` -> 303 tests, Ruff, mypy
  over 74 files, schemas, build, 4 boundary tests, installed-wheel/public-API,
  external-consumer, and release gates passed. After deleting version-0
  compatibility, catalogue Ruff and mypy over 239 files passed; `pytest -q` ->
  936 passed, 2 skipped, 187 deselected, 284 subtests; 64 live PostgreSQL
  lineage/output tests and installed catalogue composition passed.

- [x] **A3 Decide the telemetry contract before it is frozen.**
  `TelemetryHooks.emit(event: str, fields: dict[str, JsonValue])` flattens
  typed data — `TransportAccounting`, `RouteMetadata`, `TransportResponse.status`
  — into strings at the boundary, so every consumer reconstructs it.
  `LibraryDebugTelemetry` (`commerce_scraper_runtime.py:160-332`) is the proof:
  ~170 lines that string-match `"request.completed"` / `"request.retry"` and
  re-validate every field out of an untyped dict with ~15
  `isinstance(..., int) and not isinstance(..., bool)` guards, re-parsing the
  host out of `fields["url"]` and re-deriving status classes.
  Every future embedder rewrites those 170 lines. A typed request-observation
  hook alongside the string channel would delete most of the class.
  **This is the only Track A item that is genuinely a design change rather than
  a deletion.** It is also the only one that gets materially more expensive
  after 1.0. Decide explicitly: adopt a typed hook now, or record accepting the
  reconstruction cost permanently, in `## Accepted plan deviations`.
  **Completed — typed hook adopted.** `RequestObserver` receives immutable,
  secret-free `RequestObservation` values alongside the retained general event
  channel. Middleware supplies typed phase, status, route, accounting, elapsed
  time, target host, purpose, and attempt identity. Catalogue request totals,
  outcomes, metrics, and spans consume that contract directly; logging and
  arbitrary lifecycle tracing continue through `TelemetryHooks.emit`.
  *Verified:* `make scraper-check` -> 303 tests, Ruff, mypy over 74 files,
  schemas, build, 4 boundary tests, installed-wheel/public-API,
  external-consumer, and release gates passed. Catalogue Ruff, mypy over 239
  files, and `pytest -q` -> 936 passed, 2 skipped, 187 deselected, 284 subtests;
  installed catalogue composition passed.

---

## Track B — Library internals (no public API change)

Independent of Tracks A, C and D. Safe to parallelize across B1-B5.

- [ ] **B1 One home for the parsing helpers.**
  The structured-data helper family exists in **four** places:
  `parsing/_structured.py` (the intended home), `connectors/prestashop.py:94-249`,
  `connectors/wix.py:612-812`, and — already fixed — `connectors/specialized.py`.
  The same pattern repeats for `_decimal` (shopify ≡ bigcommerce byte-identical;
  wix adds a bool guard), `_origin` (4 copies, 3 semantics), `_page_id`
  (3 sha256 variants). The copies are provably copy-paste: `wix.py:796`
  `_canonical` strips PrestaShop query parameters (`id_currency`, `search_query`,
  `back`) that Wix never emits.
  Extend `_structured.py` with `breadcrumbs`, `jsonld_images`, `jsonld_brand`,
  `jsonld_gtin`, `decimal_amount`, `origin_of`, and delete the per-connector
  copies.
  **The copies have drifted, so this is not a mechanical merge.** Each
  divergence needs a deliberate choice, recorded:
  - `specification_table` name cap: 100 (`_structured`) vs 60 (prestashop)
  - `probable_javascript_shell` marker lists differ between copies
  - `meta` returns `""` (`_structured`) vs `None` (others)
  - `_decimal` bool guard present only in wix
  - `_origin` absolute-HTTP(S) assertion present only in woocommerce/bigcommerce
  *Guarded by:* `test_shopify.py`, `test_wix.py`, `test_woocommerce.py`,
  `test_prestashop.py`, `test_bigcommerce.py`, `test_recordings.py`.
  *Removes:* ~250 duplicated lines on the parsing hot path.

- [ ] **B2 Collapse the connector plumbing.**
  `checkpoint()` is copy-pasted verbatim into seven connectors; four
  `_validate_request` methods share one capability-check + `validate_checkpoint`
  shape; nine `*Factory` classes have the same four-line body that
  `_SpecializedFactory` already generalizes for its own four subclasses.
  Add `build_checkpoint(connector, request, lineage, resume_after, options)`
  beside `collection_fingerprint`, and promote `_SpecializedFactory` to a
  `SimpleConnectorFactory` in `connectors/factory.py` (currently 19 lines
  holding only the Protocol). `CommerceConnector` is a `Protocol`, so the
  helper is a free function, not a base class.
  Also drop the double validation: `ConnectorRegistry.build` calls
  `options_model.model_validate(options)` and passes the validated model, then
  every factory validates it again. Keep the registry's version-drift check —
  it is tautological for the ten built-ins but earns its keep for third-party
  plugins.
  *Removes:* ~70 lines of ceremony, one redundant validate per connector build.

- [ ] **B3 Fold the middleware telemetry copies.**
  `MiddlewareTransport.request` is one 341-line method. Its
  `ResponseBodyTooLarge`, `TransportFailure`, `CancelledError` and bare
  `Exception` handlers each build the same failure payload inline — with the
  accounting keys in a different order each time, so a missing key reads as
  intentional — while `_emit_attempt_failure` already encapsulates that shape.
  Lines 99-112 and 119-132 are the same "enforce limit → emit `cache.rejected`
  → raise" block differing only in a reason string.
  Extend `_emit_attempt_failure` with `retryable`/`cancelled`/`extra`; add
  `_enforce_cached_body(response, reason)`.
  *Note:* folding in the helper adds a `failure_stage` key to those four events.
  *Removes:* ~55 lines.

- [ ] **B4 Make optional transport capabilities explicit.**
  Two capabilities propagate by duck-typed `getattr`:
  `rotate_identity_for_request` in the middleware, and
  `browser_subrequests_authorized` at four separate sites — the latter despite
  `BrowserSubrequestAuthorizedTransport` being declared `@runtime_checkable`
  and exported for exactly this purpose. Two of the four are hand-written
  forwarding properties whose only job is to re-expose the attribute through a
  wrapper, so any new wrapper silently drops the capability unless someone
  remembers to add another one.
  The same file already does this correctly one layer up
  (`StaleResponseCache(ResponseCache, Protocol)` + `isinstance`). Use that
  mechanism for both, and put wrapper forwarding in one shared base.
  *Touches:* `transports/middleware.py`, `transports/rate_limit.py`,
  `proxy/transport.py`, `runtime/client.py`.

- [ ] **B5 One options model for the page engine, in its own module.**
  `SpecializedPageConnector` takes a flat `SpecializedPageOptions` while the
  generic connector exposes a nested `GenericPagesOptions`/`DiscoveryOptions`
  and converts between them — eight discovery fields declared twice with
  duplicated defaults and bounds, so a new knob must be added in three places.
  The application already has to know which shape each connector wants.
  Worse, the shared engine lives in `specialized.py`, so the *generic*
  connector imports from the *vendor* module — the dependency points the wrong
  way. Give the engine one options model (the nested one, with vendor
  subclasses composing `DiscoveryOptions`) and move it to its own module.
  *Prerequisite for:* D9.

---

## Track C — Library hot paths

C1-C4 are contained; C5 is a small addition; **C6 is the one with real
behavioral consequences and should be scheduled on its own.**

- [ ] **C1 Shopify inventory extraction: compile once, scan once.**
  `_inventory_from_html` builds and compiles a fresh regex *per variant id* and
  rescans the whole document with it. Five of the patterns interpolate
  `re.escape(identifier)` into the pattern text, so each variant produces
  distinct pattern strings — genuine compiles, not cache hits, which also evict
  the interpreter's 512-entry regex cache for the whole process. Each then runs
  a full document scan with bounded negative-lookahead constructs
  (`(?:(?!"id":\d).){0,1800}?`) that backtrack heavily. With V variants: up to
  5V compiles and 5V full scans, once per product.
  Make the id a capture group instead of an interpolation, compile the five
  patterns once at module level, `finditer` each *once* to build `{id: quantity}`,
  then look up each variant. Five compiles and five scans per page, regardless
  of variant count.
  *Verify against recorded fixtures* — the matching semantics change even
  though the intended result does not.

- [ ] **C2 Compute the cache identity once; read the artifact once.**
  `_request_cache_identity` runs `urlsplit` + `parse_qsl` + an `idna` encode +
  `json.dumps` + two sha256 digests, and calls `_credential_name` (which
  rebuilds a normalized string per header and per query key). It is invoked
  independently by `get`, `stale` and `put` — three times per cached request.
  Separately `MiddlewareTransport.request` calls `cache.get()` then, on a miss,
  `cache.stale()`; for an expired-but-present entry both run
  `asyncio.to_thread(self._read, key)`, so the file is opened,
  gzip-decompressed, JSON-parsed and base64-decoded **twice for one request**.
  Compute the identity once in the middleware (or memoize it on the frozen
  `TransportRequest`) and give `StaleResponseCache` a single
  `get_with_stale(request)` that classifies fresh vs stale from one read.

- [ ] **C3 Tokenize the document once per `dom_product`.**
  `select()` builds an f-string pattern and `finditer`s every opening tag,
  running `_attributes()` on each until a match. `dom_product` calls it once
  per verification rule plus up to seven field rules — roughly ten full
  tag-by-tag passes over the same document, re-parsing attributes from scratch
  each pass. Tokenize opening tags and their attribute dicts once, then filter
  that list per rule.

- [ ] **C4 Bound the httpx client cache.**
  `HttpxTransport._clients` is keyed by `(origin, resolved_address)` and never
  evicts; each entry owns a live connection pool with open sockets. A worker
  crawling many origins — or one origin behind rotating DNS, where the resolved
  address changes between requests — accumulates clients and sockets for the
  process lifetime, released only at `aclose()`. Add an LRU bound (~32) with
  `await client.aclose()` on eviction, or key by origin alone and pin the
  address through httpx `extensions`.

- [ ] **C5 Cache DNS resolution per host with a TTL.**
  `system_resolver` no longer blocks the event loop, but still resolves on
  every request and every redirect hop with no caching: a 500-URL crawl of one
  origin performs 500+ lookups. Add a small per-host TTL cache.
  *Depends on:* the async-resolver change already landed.

- [ ] **C6 Actually run requests concurrently.** *(schedule separately)*
  `_enrich_inventory` issues one HTTP request per product, strictly
  sequentially — for a 250-product `products.json` page, 250 serialized round
  trips. The requests are independent, and `PerOriginRateLimiter` already has a
  `concurrency` setting to govern how many may be in flight. But **nothing in
  the library ever runs requests concurrently** — no `asyncio.gather` or
  `TaskGroup` anywhere in `commerce-scraper/src` — so `FetchPolicy.concurrency > 1`
  is effectively dead configuration and every collection is bound by serial
  latency.
  `asyncio.gather` over each batch (the batching loop is already the natural
  unit, with `rotate_identity` between batches) would let the rate limiter
  enforce the ceiling it was built for.
  **Behavior change:** budget-exhaustion "deferred" counting and failure
  ordering within a batch become non-deterministic; that accounting needs
  reworking, and D8 is the matching ceiling on the application side. Treat this
  as a feature with its own tests, not a cleanup.

---

## Track D — Application adapters

All new code on this branch, none of it gated by the migration. D1/D2 sit on
the paid-traffic path and depend on Track 0.1.

- [x] **D1 Route the data plane through the provider registry.**
  `commerce_scraper_proxy_runtime.py` re-introduces per-provider string
  comparison inside the generic native-proxy resolver: an
  `if provider == _DECODO: ... else: _webshare_pool(...)` branch, a hardcoded
  `_SUPPORTED_PROVIDERS`, and provider-named fields
  (`proxy_webshare_data_plane_enabled`, `proxy_webshare_gateway_secret_file`)
  baked into the generic `ProxyRuntimeSettings` protocol.
  The repo's own `providers/registry.py` states the principle this violates
  verbatim — *"the difference between adding a third provider and editing
  twenty-five call sites again"* — and already carries `ProviderSpec` with
  capability flags, lock keys and build callables for the control plane.
  Add the data-plane entry (secret loader, pool builder) to `ProviderSpec` or a
  parallel registry, leaving `resolve_native_proxy_runtime` as pure policy
  validation.
  **Done.** A `_DataPlaneProvider` registry now carries each provider's
  operator gate, denial message, and builder. `_SUPPORTED_PROVIDERS`, the
  `if provider == _DECODO` branch, and the Webshare-specific gate are gone;
  membership and gating are registry lookups.
  *Residual, deliberately not changed:* `ProxyRuntimeSettings` still declares
  `proxy_webshare_*` fields, because generalizing them means reshaping the
  settings schema and its config files -- a wider change than this cleanup.
  Adding a provider is now one registry entry plus its settings fields, not a
  branch threaded through policy validation.

- [x] **D2 One durable proxy pool.**
  `PostgresDecodoProxyPool` (~270 lines) and `PostgresReservedProxyPool`
  (~285 lines) are two builds of the same thing in one file, line-for-line
  parallel: `_ReservationState`/`_DurableReservationState`,
  `CatalogueProxyLease`/`ReservedProxyLease`,
  `_PostgresAttemptAuthorization`/`_ReservedAttemptAuthorization`, with
  `_pending`, `_raise_if_exhausted`, `_owned` and `_release_authorization`
  identical modulo the lease type name.
  Extract a thin `DecodoDataPlanePool` (credentials from `LegacyProxyLease.build`,
  `rotate` = `rotate_session()`) and wrap it in `PostgresReservedProxyPool`
  exactly as `_webshare_pool` already does.
  **Behavior change:** Decodo gains the inner-lease validation and
  split-release accounting Webshare already has. This is the fail-closed money
  path — it needs its own review and tests, and
  `tests/test_commerce_scraper_proxy.py` moves onto the shared pool.
  *Depends on:* 0.1. *Pairs with:* D1.

- [ ] **D3 Share the browser transport dispatch.**
  `BorrowedBrowserTransport.request` and `CamoufoxProxyBrowserTransport.request`
  share ~55 identical lines — query-merged endpoint, `_browser_page_url`, both
  `_origin_policy.validate` calls, and the three-way evaluation /
  bare-GET-render / `session.request` dispatch with response assembly. They
  differ only in accounting and error mapping. `_session_for_request` is
  duplicated verbatim apart from `open_session(self._job)` vs `open_session()`.
  A fix to origin validation or branch selection currently has to land twice —
  once in the free path, once in the paid-proxy path.
  Extract a module-level `_dispatch(...)`; each transport keeps its own
  try/except and accounting. Behavior-preserving.

- [ ] **D4 One support module for the site plugins.**
  `commerce_scraper_axner.py`, `commerce_scraper_keramik_kraft.py` and
  `commerce_scraper_ceramicolours.py` are three-way copy-paste: the
  `_document`/`_request` browser-fallback pair is 28 lines duplicated across
  all three, differing in exactly two lines; `_canonical` is byte-identical
  between axner and keramik_kraft (including the stray PrestaShop query-strip
  from B1); `_url_id`, `_clean`, `_decimal` and `_match_text` likewise.
  Add `ops/commerce_scraper_plugin_support.py` with `canonical_url`, `clean`,
  `match_text`, `url_id`, `evidence` and
  `async def discovery_response(transport, url, *, render, label)`.
  Several already exist upstream and should be imported rather than rewritten:
  `_clean` → `scrapers/domain.py:clean`, `_meta` → `_structured.meta`,
  `_pdf_links` → `_structured.pdf_links`.
  Keep Ceramicolours' deliberately different `_canonical`/`_clean`/`_url_id`
  local. *Removes:* ~110 lines.

- [ ] **D5 Delete the retired fetcher traversal.**
  `fetcher_transport_totals` walks a `LegacyFetcher` chain production never
  passes: the only non-test call site is `worker.py:984`, which passes `None`,
  so the loop never runs and it returns `dict.fromkeys(names, 0)`. The
  `proxy_fallback` walk, the `id()` cycle guard and the `stats` reads exist for
  one test. The same 8-name tuple is spelled out three times and is a subset of
  `LibraryDebugTelemetry._TOTAL_NAMES`.
  Export one `TRANSPORT_TOTAL_NAMES`; delete the traversal and its test.
  Behavior-preserving in production.

- [ ] **D6 One quarantine statement.**
  `set enabled = false, lifecycle = 'pending', pending_action = null,
  updated_at = now()` appears four times in
  `catalogue-control/src/catalogue_control/webshare_profile_import.py` (398,
  489, 612, 629); the last three are byte-identical apart from parameter-dict
  formatting, and 398 adds `updated_by`. This statement encodes a safety
  invariant — a profile must never stay enabled after a failed install — and it
  has four places to keep in sync.
  One `_quarantine_profile(connection, provider, profile_id, *, actor=None)`.

- [ ] **D7 One native-collection assembly.**
  `Worker._crawl_connector_canary` and `LocalLibraryScraper._collect`
  independently re-implement the same ~80 lines: `layered_source_config` →
  `library_canary_route` → registry-membership check → building **both** a
  catalogue `CollectionRequest` and a `LibraryCollectionRequest` from the same
  inputs → the identical browser gate → `NativeCollectionSpec` +
  `NativeRouteBindings` → `open_collection`.
  `CatalogueCommerceRuntime` is already the composition seam; this is
  collection-construction policy living in two call sites instead of behind it.
  **They have already drifted — only the worker honours `dynamic_partitions`.**
  Resolve that divergence deliberately as part of the extraction.

- [x] **D8 Narrow the proxy authorization lock.**
  `authorize` and `_reconcile` each take a process-wide `self._lock`, then
  acquire a connection and run a statement *while still holding it*. Every
  proxied request pays two connection-pool checkouts and two DB round trips,
  and because the lock spans the awaits, no two requests anywhere in the
  process can authorize or reconcile concurrently. This is the ceiling on
  anything C6 achieves.
  Hold the lock only for the in-memory `_owned`/`pending_authorizations`
  mutation; hold one connection for the lease's lifetime.
  **Done, after D2, so it landed once instead of twice.**
  *Contention confirmed before changing anything:* one pool is built per job,
  and library requests are serial, so the win is not on the HTTP path. It is on
  the browser path -- Playwright dispatches route handlers concurrently
  (`scrapers/base.py:550` collects them as separate tasks), so every subrequest
  of a page load called `authorize` on the same lease and queued behind the
  previous one's round trip.
  *The invariant the wide lock was buying:* `rotate` and `release` both refuse
  when authorizations are pending, so holding the lock across the round trip
  made authorize atomic against them. Narrowing it naively would let an
  identity rotate while an attempt was mid-authorization. `_DurableReservationState`
  now carries `in_flight`, incremented under the lock before the round trip and
  cleared under it after; `rotate` and `release` refuse on
  `pending_authorizations or in_flight`. `_reconcile` and `_release_authorization`
  are narrowed the same way -- their id stays pending across the round trip,
  which is what keeps release and rotation out.
  A durable authorization is now recorded even if another concurrent subrequest
  fails meanwhile: dropping it would orphan a row nothing could reconcile.
  *Verified:* two new tests -- one proving two authorizations genuinely overlap,
  one proving rotate and release are both refused while an attempt is in flight.
  The overlap flag was checked for sensitivity (it stays `False` when the same
  two authorizations run sequentially), so the test fails if the lock is
  re-widened.

- [ ] **D9 Derive canary routes from library capabilities.**
  `LibraryCanaryRoute` re-declares, per connector, facts the library connector
  already owns: `uses_browser_transport=options.render is not False` is written
  out eight times, and `request_partitions` is hand-derived by `_page_partitions`
  / literal `("main",)` / `("sitemap",)`. The library already publishes
  `ConnectorCapabilities.browser: BrowserRequirement`, each connector computes
  its own `_partition_key()`, and `prestashop_partition_keys` is exported
  specifically so the app does not have to guess — the mechanism exists, it was
  just generalized for one connector and left as transcription for the other
  twelve.
  Add a library-side `ConnectorFactory.plan(options) -> (partitions, browser
  requirement)` and have the app read capabilities instead of restating them.
  The validating `library_canary_route()` is a symptom: it exists only because
  two sources of truth are kept in sync by hand, and it can shrink to a
  registry lookup.
  *Depends on:* B2, B5.
  *Note:* keep `ConnectorRuntimePlan.library_canary` optional.
  `register_runtime_adapter` is a public hook, so the guards at its five call
  sites are defense against externally-registered adapters, not dead code.

- [ ] **D10 Move per-process env into the roles table.**
  `deploy/podman/build_runtime_stage.py:162-177` applies per-process
  configuration as `if process == "control"` / `if process in {"worker",
  "worker-browser"}` branches inside the loop over `DB_ROLES` — the table that
  already describes each process. Add an `extra_env` column so the table stays
  the single place to look up what a process gets, instead of the loop body
  growing a branch per process.

---

## Track E — Gated by migration evidence (do not start)

These are the largest duplications in the diff, and **all of them are
deliberate.** ADR 0002: *"Some migration adapters and duplicate legacy
connectors remain temporarily; they are application code and are removed only
after replay, canary, and rollback gates pass."* The status doc records the
stable unsuffixed scraper keys as "the independent rollback implementations".

Nothing here should be touched until the corresponding Phase 8 checkbox has its
evidence. Listing them is a scheduling statement, not an invitation.

- [ ] **E1 Delete the duplicate legacy connectors.** ~7,375 lines in
  `catalogue-dump/src/mb_ceramics_catalogue/connectors/` shadowing 6,316 lines
  of library connectors, already drifted (the old `_variant` builds a second
  `CommerceOffer` from scratch where the new one uses `model_copy`).
  *Gate:* Phase 8 — per-source observation windows.
  *Per-source, not wholesale:* each connector family is deletable only once
  **its** sources are promoted.

- [ ] **E2 Collapse the remaining `*_connector` scraper shells.**
  `LibraryConnectorScraper` is fully generic and already backs all 14
  `library_*` keys plus shopware/sumup/starweb/nitrosell. Six modules
  (`shopify_connector.py`, `woocommerce_connector.py`, `bigcommerce_connector.py`,
  `wix_connector.py`, `prestashop_connector.py`, `pagecrawl_connector.py`,
  ~992 lines) plus `bespoke_connectors.py` still do the same job per platform,
  and `_CountingFetcher` is copied into three of them with three different
  field sets. The proven collapse was applied to 4 of 10.
  *Gate:* Phase 8 — stable key promotion. The parity tests exist to license
  exactly this.

- [ ] **E3 Delete the `EntityPage` shim.**
  `LibraryPipelineConnector` round-trips every page through
  `EntityPage.model_validate(page.model_dump())` because
  `connectors/base.py:EntityPage` is a field-for-field copy of the library's,
  and carries `capabilities: Any` for the same reason. The fix is already
  demonstrated in this branch: `connectors/commerce.py` re-exports the library
  snapshot models rather than redefining them.
  *Gate:* Phase 8 — compatibility re-export removal. Re-export the library
  `EntityPage`, drop the per-page dump/validate, and `capabilities` regains its
  real type.

- [ ] **E4 Remove the commerce model shim and `_connector` aliases** after the
  deprecation window. Already tracked in Phase 8.

---

## Ordering

```
Track 0  ──────────────────────────────────────────────►  everything
   │
   ├─► Track A (A1 → A2 share public-api.toml; A3 is a decision)
   │        └─► freeze the surface before 1.0
   │
   ├─► Track B (B1‖B2‖B3‖B4‖B5, independent)
   │        └─► B2, B5 ─► D9
   │
   ├─► Track C (C1‖C2‖C3‖C4‖C5 independent; C6 separate, pairs with D8)
   │
   └─► Track D (D1+D2 together ─► D8; D3‖D4‖D5‖D6‖D7‖D10 independent)

Track E: blocked on Phase 8 evidence. Not scheduled here.
```

**Critical path:** 0.1 → D1+D2 → D8 -- **complete.** The paid-traffic path is
green, single-implementation, and no longer serializes concurrent browser
subrequests behind one another's database round trip.

**Deadline-shaped:** all of Track A, because 1.0 makes it breaking.

**Everything else** is genuinely parallel and can be picked up by size.

## Gates that must stay green

Any item above is done only when these still pass:

- `commerce-scraper`: `uv run pytest -q` (322 tests), `uv run ruff check src/`
- `catalogue-dump`: `uv run pytest -q`, plus the boundary suites —
  `test_connector_import_boundaries.py`, `test_migration_scaling.py`,
  `test_framework_connector_library_parity.py`, `test_recorded_library_parity.py`,
  `test_shadow_parity.py`
- The AST dependency tests that keep `mb_ceramics_catalogue` out of the library
- The installed-wheel verifier against `public-api.toml` (Tracks A and B2 change
  what it compares; update the manifest in the same commit)
- Frozen version-one JSON schemas — byte-for-byte generation checks

`ruff format` is **not** a gate: 27 of 54 library source files are currently
unformatted, so running it would bury a refactor in unrelated churn. Leave it
alone or reformat the tree in one dedicated commit, not inside a cleanup.

## Recording

Per the status doc's update procedure, when an item lands: tick its box here,
record the verifying command and counts, and add any intentional divergence
(notably B1's drift resolutions and D2's accounting change) to
`## Accepted plan deviations` in `commerce-scraper-library-status.md`.
