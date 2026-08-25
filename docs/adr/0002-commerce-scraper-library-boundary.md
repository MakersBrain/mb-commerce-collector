# ADR 0002: Commerce scraper library boundary and connector plugins

Status: accepted
Date: 2026-08-22

## Context

Commerce collection was embedded in the catalogue application alongside
ceramics projection, PostgreSQL lineage state, worker orchestration, browser
processes, and provider billing. That made connectors difficult to reuse and
made application infrastructure available inside parsing code even when it was
not part of the connector contract.

The extracted distribution also needs controlled extensibility. Built-in and
third-party connectors must share one typed contract without automatic plugin
imports changing a basic package import or allowing one broken plugin to hide
the built-ins.

## Decision

Maintain `mb-commerce-scraper` as an application-neutral distribution with
this dependency direction:

```text
models
  ↑
connectors ← discovery/parsing
  ↑
runtime ← transports ← proxy protocols
  ↑
catalogue application adapters
```

The library may not import `mb_ceramics_catalogue`, PostgreSQL clients, queue
clients, catalogue datasets, or provider control APIs. Models use only the
standard library and Pydantic. Connectors depend on neutral models, parsing or
discovery strategies, and transport contracts; they do not select concrete
HTTP clients, proxy providers, caches, or application services. Concrete
composition and resource ownership belong to the runtime or the embedding
application.

Built-ins are registered explicitly by `ConnectorRegistry.with_builtins()`.
External plugins use the `mb_commerce_scraper.connectors` Python entry-point
group and supply a `ConnectorFactory` with a normalized name, declared version,
Pydantic options model, and factory method. Entry points are loaded only after
an explicit `load_entry_points()` call. Tolerant loading isolates and records a
plugin failure; strict loading raises a sanitized `PluginLoadError` chained to
the original exception. Duplicate names and factory/connector version drift
are rejected.

Catalogue-specific compatibility connectors remain in the catalogue package.
A connector moves into the library only when its behavior is expressed through
neutral contracts and it has conformance coverage. Catalogue projection,
lineage persistence, proxy budget SQL, telemetry backends, and production
configuration remain application-owned adapters.

AST dependency tests, clean-import tests, installed-wheel verification, and
connector conformance tests enforce this decision in CI.

## Consequences

- Core imports remain small and do not initialize HTTP, browser, database, or
  plugin infrastructure.
- Connectors can run against deterministic fake transports outside the
  catalogue application.
- Runtime and provider implementations are replaceable behind typed protocols.
- Plugins require explicit loading and cannot silently alter the built-in
  registry.
- Some migration adapters and duplicate legacy connectors remain temporarily;
  they are application code and are removed only after replay, canary, and
  rollback gates pass.
- A new dependency edge that violates the diagram requires a new architectural
  decision, not an allowlist exception added only to make CI pass.
