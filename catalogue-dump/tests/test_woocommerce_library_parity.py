from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from mb_commerce_scraper import CollectionRequest as LibraryRequest
from mb_commerce_scraper.connectors import (
    ConnectorContext,
)
from mb_commerce_scraper.connectors import (
    WooCommerceConnector as LibraryWooCommerceConnector,
)
from mb_commerce_scraper.connectors import (
    WooCommerceOptions as LibraryWooCommerceOptions,
)
from mb_commerce_scraper.testing import FakeTransport

from mb_ceramics_catalogue.connectors import (
    CollectionRequest as LegacyRequest,
)
from mb_ceramics_catalogue.connectors import (
    RefreshMode,
    SnapshotField,
)
from mb_ceramics_catalogue.connectors import (
    WooCommerceConnector as LegacyWooCommerceConnector,
)
from mb_ceramics_catalogue.connectors import (
    WooCommerceOptions as LegacyWooCommerceOptions,
)

NOW = datetime(2026, 8, 15, 10, 0, tzinfo=UTC)
API = "https://shop.test/wp-json/wc/store/v1/products"


class LegacyFetcher:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses

    async def json(self, url: str, *, params=None, headers=None):
        del url, params, headers
        return deepcopy(self.responses.pop(0))


def payload() -> dict[str, Any]:
    return {
        "id": 10,
        "type": "simple",
        "name": "Transparent &amp; Gloss Glaze",
        "permalink": "https://shop.test/product/glaze-10",
        "description": "<p>A gloss glaze.</p>",
        "sku": "GL-10",
        "prices": {"price": "1250", "regular_price": "1500", "currency_code": "EUR", "currency_minor_unit": 2},
        "is_in_stock": True,
        "is_on_backorder": False,
        "add_to_cart": {"maximum": 7},
        "categories": [{"id": 3, "name": "Glazes", "slug": "glazes"}],
        "images": [{"src": "https://cdn.test/glaze.jpg"}],
    }


async def test_extracted_woocommerce_snapshot_matches_catalogue_connector() -> None:
    legacy = LegacyWooCommerceConnector(
        LegacyFetcher([[payload()]]),
        LegacyWooCommerceOptions(
            vat_status="inclusive",
            stock_from_add_to_cart_maximum=True,
        ),
        clock=lambda: NOW,
    )
    legacy_request = LegacyRequest(
        source_id="shop", base_url="https://shop.test", refresh_mode=RefreshMode.FULL,
        requested_fields=frozenset(SnapshotField),
    )
    legacy_pages = [page async for page in legacy.collect(legacy_request)]

    transport = FakeTransport()
    transport.add(API, json_body=[payload()])
    library = LibraryWooCommerceConnector(
        transport,
        LibraryWooCommerceOptions(
            vat_status="inclusive",
            stock_from_add_to_cart_maximum=True,
        ),
        ConnectorContext(clock=lambda: NOW),
    )
    library_pages = [page async for page in library.collect(LibraryRequest(source_id="shop", base_url="https://shop.test"))]

    assert library_pages[0].items[0].model_dump(mode="json") == legacy_pages[0].items[0].model_dump(mode="json")
