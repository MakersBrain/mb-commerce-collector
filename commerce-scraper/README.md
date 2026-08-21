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

Built-in connector names are `shopify`, `woocommerce`, and `generic-pages`.
Connector options are validated by the selected factory; unrelated platform
options are rejected.

For an owned HTTP session and optional sticky residential failover:

```python
from mb_commerce_scraper.proxy import ProxyRouting, StaticProxyPool
from mb_commerce_scraper.runtime import build_http_scraper

async with build_http_scraper(
    allowed_origins=("https://shop.example",),
    proxy_pool=StaticProxyPool(routes),
    routing=ProxyRouting.fallback(country="FR"),
    proxy_maximum_bytes=50_000_000,
) as scraper:
    async for page in scraper.collect(source):
        consume(page)
```

The runtime keeps direct and proxy identities distinct, acquires at most one
sticky lease at a time, fails over only on typed transport/block outcomes, and
releases leases on success, failure, timeout, or cancellation. Proxy passwords
are secret-aware and are excluded from cache keys, checkpoints, diagnostics,
and route metadata.

Applications can split their source envelope with `SourceDefinition`,
`FetchPolicy`, and `ProxyPolicyConfig`; dataset and projection settings remain
application-owned.

See `examples/custom_connector/` for explicit third-party registration. Users
are responsible for authorization, terms, privacy, robots, and collection
policies for every target.
