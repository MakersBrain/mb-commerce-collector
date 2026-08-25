from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest
from mb_commerce_scraper import CollectionRequest, SnapshotField
from mb_commerce_scraper.connectors import ConnectorContext
from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import BrowserHint, RequestPurpose

from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import source_definition
from mb_ceramics_catalogue.ops.commerce_scraper_keramik_kraft import (
    KeramikKraftConnector,
    KeramikKraftOptions,
)
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    application_connector_registry,
)

NOW = datetime(2026, 8, 22, 14, 0, tzinfo=UTC)
CATEGORY = "https://kraft.test/de/Glasuren.html"


def _card(code: str) -> str:
    return f"""
    <div class="product card">
      <p class="text-sm">Mayco Blue Glaze<br>Bt. á 0,25 kg</p>
      <p class="p mb-1">{code}</p>
      <span>4,97 € <i>(4,18 € HT)</i></span>
      <a href="Mayco-Blue_{code}.html">detail</a><img src="/blue.jpg">
    <!-- /product
    """


def _request(*, limit: int | None = None) -> CollectionRequest:
    return CollectionRequest(
        source_id="kraft",
        base_url="https://kraft.test/",
        requested_fields=frozenset(SnapshotField),
        result_limit=limit,
    )


def _build(transport: FakeTransport) -> KeramikKraftConnector:
    return cast(
        KeramikKraftConnector,
        application_connector_registry().build(
            "keramik-kraft",
            transport=transport,
            options=KeramikKraftOptions(
                category_paths=("de/Glasuren.html",),
                vat_rate=Decimal("0.19"),
                render=False,
            ).model_dump(mode="json"),
            context=ConnectorContext(clock=lambda: NOW),
        ),
    )


def test_configured_kraft_source_constructs_with_normalized_library_identity() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    [(source_id, config)] = [
        item for item in sources.items() if item[1].scraper == "keramik_kraft"
    ]
    definition = source_definition(source_id, config)
    connector = application_connector_registry().build(
        definition.connector,
        transport=FakeTransport(),
        options=definition.connector_options,
        context=ConnectorContext(),
    )

    assert definition.connector == "keramik-kraft"
    assert connector.name == "keramik-kraft"


@pytest.mark.asyncio
async def test_kraft_plugin_reuses_listing_response_and_resumes_inside_cards() -> None:
    transport = FakeTransport()
    transport.add(CATEGORY, body=_card("MAY-1") + _card("MAY-2"))
    connector = _build(transport)

    [limited] = [page async for page in connector.collect(_request(limit=1))]
    checkpoint = connector.checkpoint(_request(limit=1), "test", limited.resume_after)
    replay_transport = FakeTransport()
    replay_transport.add(CATEGORY, body=_card("MAY-1") + _card("MAY-2"))
    replay = _build(replay_transport)
    [resumed] = [page async for page in replay.collect(_request(limit=1), checkpoint)]

    snapshot = limited.items[0]
    variant = snapshot.variants[0]
    offer = variant.offers[0]
    assert snapshot.connector == "keramik-kraft"
    assert snapshot.external_id == "MAY-1"
    assert snapshot.vendor == "Mayco"
    assert snapshot.images[0].url == "https://kraft.test/blue.jpg"
    assert offer.price.amount == Decimal("4.97")
    assert offer.price.currency == "EUR"
    assert offer.vat_status == "inclusive"
    assert offer.vat_rate == Decimal("0.19")
    assert variant.published_attributes["Netto-Preis EUR"] == 4.18
    assert variant.stock is not None and variant.stock.availability == "in_stock"
    assert offer.evidence[0].source_url == CATEGORY
    assert offer.evidence[0].source_field == "keramik_kraft_listing_card"
    raw = cast(dict[str, Any], snapshot.platform_extensions["raw"])
    assert raw["net"] == 4.18
    assert limited.resume_after == {
        "index": 0,
        "url": CATEGORY,
        "snapshot_offset": 1,
        "sequence": 1,
    }
    assert not limited.enumeration_intact
    assert resumed.items[0].external_id == "MAY-2"
    assert resumed.resume_after is None and resumed.enumeration_intact
    assert [request.url for request in transport.requests] == [CATEGORY]
    assert transport.requests[0].purpose == RequestPurpose.DISCOVERY
    assert [request.url for request in replay_transport.requests] == [CATEGORY]


@pytest.mark.asyncio
async def test_kraft_plugin_skips_empty_parent_and_browser_falls_back() -> None:
    root = "https://kraft.test/de/Glasuren--Engoben.html"
    child = "https://kraft.test/de/Glasuren.html"
    transport = FakeTransport()
    transport.add(root, body=f'<a href="{child}">Glazes</a>')
    transport.add(child, status=403)
    transport.add(child, body=_card("MAY-1"))
    connector = cast(
        KeramikKraftConnector,
        application_connector_registry().build(
            "keramik-kraft",
            transport=transport,
            options=KeramikKraftOptions(
                category_paths=("de/Glasuren--Engoben.html",)
            ).model_dump(mode="json"),
            context=ConnectorContext(clock=lambda: NOW),
        ),
    )

    [page] = [item async for item in connector.collect(_request())]

    assert page.items[0].external_id == "MAY-1"
    child_requests = [request for request in transport.requests if request.url == child]
    assert [request.browser for request in child_requests] == [
        BrowserHint.NEVER,
        BrowserHint.REQUIRED,
    ]
