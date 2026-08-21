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

See `examples/custom_connector/` for explicit third-party registration. Users
are responsible for authorization, terms, privacy, robots, and collection
policies for every target.

