from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest
from mb_commerce_scraper import CollectionRequest, SnapshotField
from mb_commerce_scraper.connectors import ConnectorContext
from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import BrowserHint, RequestPriority, RequestPurpose

from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import source_definition
from mb_ceramics_catalogue.ops.commerce_scraper_axner import (
    AxnerConnector,
    AxnerOptions,
)
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    application_connector_registry,
)

NOW = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)


def _product(reference: str) -> str:
    return f"""
    <h1>Blue Stoneware</h1>
    <span class="product-list-cost-value">$ 12.50</span>
    <span class="prod-detail-man-name-value">Axner</span>
    <div class="prod-detail-desc">Strong &amp; plastic clay</div>
    <span class="prod-detail-part-label">Axner Number:</span>
    <span class="prod-detail-part-value">{reference}</span>
    <span class="prod-detail-part-label">Cone:</span>
    <span class="prod-detail-part-value">6</span>
    <img src="/ProductImages/clay.jpg">
    <a href="/docs/sds.pdf">Safety data sheet</a>
    """


def _request(*, limit: int | None = None) -> CollectionRequest:
    return CollectionRequest(
        source_id="axner",
        base_url="https://axner.test/",
        requested_fields=frozenset(SnapshotField),
        result_limit=limit,
    )


def _transport() -> FakeTransport:
    transport = FakeTransport()
    transport.add(
        "https://axner.test/sitemap.aspx",
        body='<a href="/glazes.aspx">Glazes</a>',
    )
    transport.add(
        "https://axner.test/glazes.aspx",
        body=(
            '<h5 class="product-list-link"><a href="/one.aspx">One</a></h5>'
            '<h5 class="product-list-link"><a href="/two.aspx">Two</a></h5>'
        ),
    )
    transport.add("https://axner.test/one.aspx", body=_product("AX-1"))
    transport.add("https://axner.test/two.aspx", body=_product("AX-2"))
    return transport


def test_configured_axner_source_constructs_through_application_registry() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    configured = [
        (source_id, config)
        for source_id, config in sources.items()
        if config.scraper == "axner"
    ]

    assert len(configured) == 1
    source_id, config = configured[0]
    definition = source_definition(source_id, config)
    connector = application_connector_registry().build(
        definition.connector,
        transport=FakeTransport(),
        options=definition.connector_options,
        context=ConnectorContext(),
    )

    assert connector.name == "axner"


@pytest.mark.asyncio
async def test_application_axner_plugin_preserves_contract_and_resumes() -> None:
    registry = application_connector_registry()
    transport = _transport()
    connector = cast(
        AxnerConnector,
        registry.build(
            "axner",
            transport=transport,
            options=AxnerOptions(render=False, vat_status="exclusive").model_dump(
                mode="json"
            ),
            context=ConnectorContext(clock=lambda: NOW),
        ),
    )

    [limited] = [page async for page in connector.collect(_request(limit=1))]
    checkpoint = connector.checkpoint(_request(limit=1), "test", limited.resume_after)
    replay_transport = _transport()
    replay = registry.build(
        "axner",
        transport=replay_transport,
        options=AxnerOptions(render=False, vat_status="exclusive").model_dump(
            mode="json"
        ),
        context=ConnectorContext(clock=lambda: NOW),
    )
    [resumed] = [page async for page in replay.collect(_request(limit=1), checkpoint)]

    snapshot = limited.items[0]
    variant = snapshot.variants[0]
    assert "axner" in registry.names()
    assert snapshot.connector == "axner"
    assert snapshot.external_id == "AX-1"
    assert variant.offers[0].price.amount == Decimal("12.50")
    assert variant.offers[0].vat_status == "exclusive"
    assert variant.published_attributes["Cone"] == "6"
    assert snapshot.documents[0].url == "https://axner.test/docs/sds.pdf"
    raw = cast(dict[str, Any], snapshot.platform_extensions["raw"])
    details = cast(dict[str, Any], raw["details"])
    assert details["Cone"] == "6"
    assert not limited.enumeration_intact
    assert resumed.items[0].external_id == "AX-2"
    assert not any(
        request.url == "https://axner.test/one.aspx"
        for request in replay_transport.requests
    )
    assert [request.purpose for request in transport.requests] == [
        RequestPurpose.DISCOVERY,
        RequestPurpose.DISCOVERY,
        RequestPurpose.ENTITY,
    ]
    assert [request.priority for request in transport.requests] == [
        RequestPriority.DISCOVERY,
        RequestPriority.DISCOVERY,
        RequestPriority.IDENTITY,
    ]
    assert all(request.browser == BrowserHint.NEVER for request in transport.requests)


@pytest.mark.asyncio
async def test_application_axner_plugin_cancels_before_transport_io() -> None:
    transport = FakeTransport()
    connector = application_connector_registry().build(
        "axner",
        transport=transport,
        options=AxnerOptions().model_dump(mode="json"),
        context=ConnectorContext(cancelled=lambda: True),
    )

    pages = [page async for page in connector.collect(_request())]

    assert pages == []
    assert transport.requests == []


@pytest.mark.asyncio
async def test_application_axner_discovery_falls_back_to_browser_when_allowed() -> None:
    transport = FakeTransport()
    index = "https://axner.test/sitemap.aspx"
    transport.add(index, status=403)
    transport.add(index, body='<a href="/glazes.aspx">Glazes</a>')
    transport.add(
        "https://axner.test/glazes.aspx",
        body='<h5 class="product-list-link"><a href="/one.aspx">One</a></h5>',
    )
    transport.add("https://axner.test/one.aspx", body=_product("AX-1"))
    connector = application_connector_registry().build(
        "axner",
        transport=transport,
        options=AxnerOptions().model_dump(mode="json"),
        context=ConnectorContext(clock=lambda: NOW),
    )

    [page] = [item async for item in connector.collect(_request())]

    assert page.items[0].external_id == "AX-1"
    assert [request.browser for request in transport.requests[:2]] == [
        BrowserHint.NEVER,
        BrowserHint.REQUIRED,
    ]
