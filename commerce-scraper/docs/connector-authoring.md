# Connector authoring

This guide defines the Python extension boundary. For a configured
`generic-pages` shop and the complete external-package example, start with
[`custom-shops.md`](custom-shops.md) and
[`examples/custom_connector`](../examples/custom_connector/README.md). Do not
write a connector when structured metadata plus declarative discovery is
sufficient.

## Architecture boundary

A connector translates storefront responses into dataset-neutral commerce
models. It does not choose how requests are cached, retried, rate-limited,
routed through a proxy, traced, or persisted.

```text
application composition
  -> ConnectorRegistry -> ConnectorFactory
  -> CommerceScraper -> middleware -> CommerceTransport
  -> CommerceConnector -> EntityPage[CommerceProductSnapshot]
  -> application-owned projection and storage
```

Connector packages may import public models, `mb_commerce_scraper.connectors`,
`mb_commerce_scraper.transports`, and the discovery/parsing helpers. They must
not import catalogue application modules or construct the runtime, HTTP
clients, proxy pools, caches, loggers, or global registries.

The two required contracts are:

- `ConnectorFactory`: normalized `name`, stable `version`, a strict Pydantic
  `options_model`, and `build(transport=..., options=..., context=...)`;
- `CommerceConnector`: `name`, `platform`, `version`, typed `capabilities`, and
  async `collect(request, checkpoint)` yielding ordered `EntityPage` values.

Use the public imports demonstrated by the
[`example-feed` implementation](../examples/custom_connector/src/example_commerce_connector/__init__.py).
That file is the maintained code template; this guide intentionally does not
duplicate it.

## Lifecycle and ownership

The application loads entry points explicitly, normally once while composing
the process. For each collection, the registry validates options, the runtime
builds a connector with a borrowed transport and `ConnectorContext`, and the
runtime closes any route generation it owns after collection.

Connector code should:

- perform no I/O at import or factory-registration time;
- check `context.cancelled()` before starting more I/O and let
  `asyncio.CancelledError` propagate;
- use only the injected transport for network work;
- declare every supported field, refresh mode, stock kind, category filter,
  document capability, incremental cursor, and browser requirement;
- emit monotonically ordered pages with exactly one final terminal page;
- bind checkpoints to connector name/version, source, collection fingerprint,
  partition, and next cursor; never accept a foreign or future checkpoint;
- keep raw payloads, authentication material, application state, and mutable
  global state out of pages, diagnostics, evidence, and checkpoints.

`TransportRequest` communicates intent to middleware. Set `purpose`,
`priority`, `required`, `estimated_bytes`, cache policy, and browser hint; do
not reproduce retries or proxy fallback inside the connector. A connector
request is one logical call. Middleware alone owns retry attempts and physical
accounting.

## Registration and configuration

Expose a factory object through the stable entry-point group:

```toml
[project.entry-points."mb_commerce_scraper.connectors"]
example-feed = "example_commerce_connector:connector_factory"
```

Discovery is deliberately opt-in and can be fail-closed:

```python
from mb_commerce_scraper.connectors import ConnectorRegistry

registry = ConnectorRegistry.with_builtins()
registry.load_entry_points(strict=True)
assert "example-feed" in registry.names()
schema = registry.options_schema("example-feed")
```

Names use lowercase kebab case. The factory and built connector must report the
same version. Options should use `ConfigDict(extra="forbid", frozen=True)` so a
misspelled or stale setting fails at composition rather than changing runtime
behavior silently.

## Essential tests

Test the connector/transport intercall, not every middleware combination. A
useful connector suite covers:

1. one representative successful response through `FakeTransport` and
   `assert_connector_pages`;
2. capability rejection and the connector-specific malformed/empty response;
3. meaningful resume behavior, or explicit checkpoint rejection when resume
   is unsupported;
4. cancellation before I/O;
5. absence of credentials in serialized output and diagnostics.

Use bounded response fixtures. Promote a response to a recording only when the
protocol shape matters, scrub it first, and never let replay fall through to
the network.

```console
make scraper-test
make scraper-example
make scraper-schemas
```

`make scraper-example` builds the wheel, installs it beside the external
example in an isolated environment, loads its entry point, and exercises the
real registry/factory/transport boundary.

## Tracing and debugging

Emit structured events through `context.telemetry.emit(event, fields)`. Use a
stable dotted event name and scalar, bounded, secret-free fields:

```python
self._context.telemetry.emit(
    "connector.feed.decoded",
    {"level": "debug", "page": page_number, "items": len(products)},
)
```

The runtime adds `collection_id`, `source_id`, and connector identity to its own
events. Use `debug` for protocol decisions and page/cursor detail, `info` for
collection lifecycle, and `warning` for retryable or completeness-affecting
conditions. Do not log response bodies, headers, query strings, credentials,
or exception text copied from a provider. The runtime sanitizes telemetry, but
sanitization is a final boundary—not permission to emit secrets.

## Release gate

Before publishing a connector package:

- run its unit/conformance suite against every supported library version;
- install the built library wheel and connector wheel in a clean environment;
- replay scrubbed recordings and review every intentional output difference;
- run a bounded canary with explicit ownership and rollback configuration;
- retain the previous connector mapping for at least one normal schedule cycle.

A parser change that changes emitted data needs release notes even when the
Python signatures do not change. A checkpoint or frozen envelope incompatibility
needs a new contract/schema version, not an in-place rewrite.
