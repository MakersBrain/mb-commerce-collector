# Custom shop authoring

Choose the declarative connector when the shop exposes structured product
pages. Write Python only when discovery, authentication, or parsing cannot be
expressed safely as data.

## Declarative shops

`generic-pages` combines sitemap/category discovery with JSON-LD, microdata,
OpenGraph, or verified DOM selectors:

```python
from mb_commerce_scraper import SourceDefinition

source = SourceDefinition(
    id="example",
    label="Example shop",
    base_url="https://shop.example",
    connector="generic-pages",
    connector_options={
        "discovery": {
            "sitemaps": ["https://shop.example/products-sitemap.xml"],
            "product_pattern": r"/products/",
        },
        "parsers": ["jsonld", "dom"],
        "dom_rules": {
            "name": "h1.product-title",
            "price": ".product-price",
            "sku": "[data-product-sku]",
        },
        "currency": "EUR",
    },
)
```

Keep configuration data-only: no imports, expressions, callbacks, or browser
JavaScript. Prefer JSON-LD and other published metadata; DOM rules are a
verified fallback. Test discovery-empty, parser-empty, browser-required, and
blocked responses as distinct outcomes. The runtime owns robots, retry,
rate-limit, cache, proxy, accounting, and telemetry policy, so declarative
configuration should not duplicate those concerns.

Applications that need Python discovery or parsing while retaining the shared
generic collection engine can explicitly compose versioned strategies into
their factory:

```python
from mb_commerce_scraper.connectors import (
    ConnectorRegistry,
    GenericPagesFactory,
)

registry = ConnectorRegistry()
registry.register(
    GenericPagesFactory(discovery=my_discovery, parser=my_parser)
)
```

Both objects declare stable `name` and `version` strings. Their identities are
part of checkpoint compatibility, so change the version whenever behavior or
configuration changes. Discovery may yield relative or absolute product URLs;
the connector still enforces same-origin routing, deduplication, cancellation,
product-pattern filtering, and finite bounds. Strategy objects are trusted
application/plugin code and are never loaded from declarative source data. A
factory may share them across connector builds, so keep them reentrant and put
per-collection mutable state inside `discover()` or `parse()` calls.

## Python connectors

Start from [`../examples/custom_connector`](../examples/custom_connector), a
separate installable package exercised against the built library wheel. A
plugin provides two objects:

- a connector implementing `name`, `platform`, `version`, `capabilities`, and
  async `collect(request, checkpoint)`;
- a factory implementing `name`, `version`, `options_model`, and
  `build(transport=..., options=..., context=...)`.

Publish the factory through the stable entry-point group:

```toml
[project.entry-points."mb_commerce_scraper.connectors"]
example-feed = "example_commerce_connector:connector_factory"
```

Load entry points only at application composition time:

```python
from mb_commerce_scraper import SnapshotField, SourceDefinition
from mb_commerce_scraper.connectors import ConnectorRegistry
from mb_commerce_scraper.runtime import CommerceScraper

registry = ConnectorRegistry.with_builtins()
registry.load_entry_points(strict=True)

source = SourceDefinition(
    id="example",
    label="Example feed",
    base_url="https://shop.example",
    connector="example-feed",
    connector_options={"feed_path": "/catalog.json", "currency": "EUR"},
)

async with CommerceScraper(registry=registry, transport=transport) as scraper:
    fields = frozenset(
        {SnapshotField.IDENTITY, SnapshotField.VARIANTS, SnapshotField.OFFERS}
    )
    async for page in scraper.collect(source, requested_fields=fields):
        consume(page)
```

Connector code should depend on models, the base transport contract, and
discovery/parsing helpers—not runtime, proxy, cache, or application modules.
Accept injected transport, clock, budget/telemetry context, and cancellation;
never create hidden clients or global registries. Use a strict frozen Pydantic
options model, stable connector/version identifiers, bounded parsing, neutral
output models, and credential-free diagnostics/checkpoints.

Test the essential boundary with `FakeTransport` and
`assert_connector_pages`. Cover one representative successful intercall plus
the connector-specific failure and resume/cancellation behavior that matters;
middleware behavior belongs to the library's middleware tests. During
debugging, emit structured events through `context.telemetry` with stable event
names and non-secret scalar fields. Let the runtime provide collection/route
trace context and choose debug/info/warning levels rather than logging payloads.
