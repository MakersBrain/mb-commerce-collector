# mb-commerce-scraper

Dataset-neutral, async commerce collection primitives. The base install depends
only on Pydantic; install `mb-commerce-scraper[http]` for the HTTPX backend.
Importing the package performs no plugin discovery and opens no resources.

```python
from mb_commerce_scraper import SourceDefinition
from mb_commerce_scraper.connectors import ConnectorRegistry
from mb_commerce_scraper.runtime import CommerceScraper
from mb_commerce_scraper.testing import FakeTransport

registry = ConnectorRegistry.with_builtins()
source = SourceDefinition(
    id="example",
    label="Example",
    base_url="https://shop.example",
    connector="shopify",
    connector_options={"currency": "EUR"},
)

async with CommerceScraper(registry=registry, transport=FakeTransport()) as scraper:
    async for page in scraper.collect(source):
        print(page.items)
```

Built-in connector names are `shopify`, `woocommerce`, `prestashop`,
`bigcommerce`, `wix`, `shopware`, `starweb`, `nitrosell`, `sumup`, and
`generic-pages`. Sio2 sources use the `prestashop` connector with catalogue
projection policy applied afterward. Connector options are validated by the
selected factory; unrelated platform options are rejected.

Connectors may label individual requests as browser-required. Pass a borrowed
browser transport to `CommerceScraper(browser_transport=...)` to serve those
requests; ordinary requests continue through HTTP. HTTP-only composition fails
clearly instead of silently downgrading browser-required work. Until a browser
proxy factory can bind the browser to the same sticky lease, combining an
active proxy route with browser-required work is rejected rather than bypassing
the configured proxy.

For an owned HTTP session and optional sticky residential failover:

```python
from mb_commerce_scraper.proxy import ProxyRouting, StaticProxyPool
from mb_commerce_scraper.runtime import build_http_scraper
from mb_commerce_scraper.transports import FileResponseCache

cache = FileResponseCache(".commerce-cache", maximum_age_seconds=86_400)

async with build_http_scraper(
    allowed_origins=("https://shop.example",),
    cache=cache,
    proxy_pool=StaticProxyPool(routes),
    routing=ProxyRouting.fallback(country="FR"),
    proxy_maximum_bytes=50_000_000,
) as scraper:
    async for page in scraper.collect(source):
        consume(page)
```

Explicit cache instances are borrowed. The filesystem implementation opens
files only for individual asynchronous operations, so scraper shutdown has no
cache close step and deliberately leaves the reusable entries on disk.
Requests carrying credentials bypass the standard caches. Cached response
bodies may still contain sensitive application data, so protect the directory
and apply an application-owned retention/total-size policy.

The runtime keeps direct and proxy identities distinct, acquires at most one
sticky lease at a time, fails over only on typed transport/block outcomes, and
releases leases on success, failure, timeout, or cancellation. Proxy passwords
are secret-aware and are excluded from cache keys, checkpoints, diagnostics,
and route metadata.

Applications can split their source envelope with `SourceDefinition`,
`FetchPolicy`, and `ProxyPolicyConfig`; dataset and projection settings remain
application-owned.

## Compatibility and frozen contracts

The supported interpreter range is Python 3.11 through 3.13, enforced by the
package metadata and verification matrix. A new Python minor enters the range
only after the full type, test, build, and installed-wheel gates pass. Removing
an interpreter from the declared range is a release-policy decision and must be
called out in the changelog.

Version-one JSON schemas for the public source, request, checkpoint,
diagnostic, product snapshot, and page envelopes ship in
`mb_commerce_scraper.schemas` beside validated representative payloads. They
are generated from the Pydantic contracts and checked byte-for-byte by
`make scraper-schemas`; update them deliberately with:

```console
uv --directory commerce-scraper run -- python scripts/generate_schemas.py
```

An incompatible contract change requires a new contract/schema version. Do
not regenerate these artifacts merely to hide an unintended model drift.

The supported import surface and semantic-versioning rules are documented in
[`docs/public-api.md`](docs/public-api.md). A machine-readable companion in
[`public-api.toml`](public-api.toml) is checked against the installed wheel so
accidental removals, renames, or undocumented exports fail the contract gate.
Release notes live in [`CHANGELOG.md`](CHANGELOG.md). A release is created only
from a `commerce-scraper-vX.Y.Z` tag on `main` after the full library, artifact,
public-API, and clean-consumer gates pass and the changelog contains a dated
entry for the exact package version.

Integration guides:

- [`docs/custom-shops.md`](docs/custom-shops.md) for declarative shop setup and
  the external connector example;
- [`docs/connector-authoring.md`](docs/connector-authoring.md) for the Python
  extension architecture, lifecycle, tests, and release gate;
- [`docs/proxy-integration.md`](docs/proxy-integration.md) for provider adapters,
  paid-traffic authorization, browser routing, and incident rollback;
- [`docs/migration.md`](docs/migration.md) for replay, shadow comparison,
  source-scoped canaries, promotion, and rollback.

Users are responsible for authorization, terms, privacy, robots, and
collection policies for every target.
