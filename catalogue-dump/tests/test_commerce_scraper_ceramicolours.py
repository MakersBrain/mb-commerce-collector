from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from mb_commerce_scraper import CollectionRequest, DiagnosticCode, SnapshotField
from mb_commerce_scraper.connectors import ConnectorContext
from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    BrowserHint,
    BudgetExhausted,
    RequestPriority,
    RequestPurpose,
    TransportFailure,
    TransportRequest,
    TransportResponse,
)

from mb_ceramics_catalogue import scrapers
from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import source_definition
from mb_ceramics_catalogue.ops.commerce_scraper_ceramicolours import (
    PACK_PRICE_SCRIPT,
    CeramicoloursConnector,
    CeramicoloursOptions,
)
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    application_connector_registry,
)
from mb_ceramics_catalogue.scrapers.record import RecordBuilder

NOW = datetime(2026, 8, 22, 16, 0, tzinfo=UTC)
BASE = "https://color.test/"
ONE = f"{BASE}Articolo.php?cod=ONE"
TWO = f"{BASE}Articolo.php?cod=TWO"


class Telemetry:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def emit(self, event: str, fields: dict[str, Any]) -> None:
        self.events.append((event, fields))


class EvaluationBudgetDenied(FakeTransport):
    async def request(self, request: TransportRequest) -> TransportResponse:
        if request.evaluation is not None:
            self.requests.append(request)
            raise BudgetExhausted("evaluation budget denied")
        return await super().request(request)


class LocalFetcher:
    proxy_lease = None

    def __init__(self) -> None:
        self.stats = SimpleNamespace(proxy_requests=0)
        self.limiter = SimpleNamespace(
            join_group=lambda *_args: None,
            set_delay=lambda *_args: None,
        )
        self.urls: list[str] = []
        self.evaluations: list[tuple[str, str]] = []

    async def response(self, url: str, **_kwargs: Any) -> httpx.Response:
        self.urls.append(url)
        documents = {
            BASE: '<a href="Articoli.php?Id=5101">Glazes</a>',
            f"{BASE}Articoli.php?Id=5101&page=1": (
                '<a href="Articolo.php?cod=ONE" class="product-name">One</a>'
            ),
            f"{BASE}Articoli.php?Id=5101&page=2": "",
            ONE: _product("ONE"),
        }
        return httpx.Response(
            200,
            text=documents[url],
            request=httpx.Request("GET", url),
        )

    async def evaluate_in_browser(
        self,
        url: str,
        script: str,
        wait_ms: int = 2000,
        wait_for: str | None = None,
        *,
        action_id: str = "legacy-evaluate.v1",
    ) -> Any:
        del wait_ms, wait_for
        self.evaluations.append((url, action_id))
        assert script
        return _packs("26.65", "99.00")

    async def may_fetch(self, *_args: Any, **_kwargs: Any) -> bool:
        return True

    async def rotate_client(self) -> None:
        return None


def _request(*, limit: int | None = None) -> CollectionRequest:
    return CollectionRequest(
        source_id="ceramicolours",
        base_url=BASE,
        requested_fields=frozenset(SnapshotField),
        result_limit=limit,
    )


def _product(code: str, *, packs: bool = True) -> str:
    selector = '<select id="product-pack-field"></select>' if packs else ""
    return f"""
    <h1>Glaze {code}</h1>
    <div class="product-description">Bright glaze</div>
    <p>Temp.</span> 1000–1050 °C</p>
    <p>Prezzo:</span> € 8,40</p>
    <input id="icaOrdinabile" value="10">
    {selector}
    <img src="/upload-immagini/{code}.jpg">
    <li class="breadcrumb"><a>Glazes</a></li>
    """


def _discovery(transport: FakeTransport) -> None:
    transport.add(
        BASE,
        body=(
            '<a href="Articoli.php?Id=5101">A</a>'
            '<a href="Articoli.php?Id=5102">B</a>'
        ),
    )
    transport.add(
        f"{BASE}Articoli.php?Id=5101&page=1",
        body='<a href="Articolo.php?cod=ONE" class="product-name">One</a>',
    )
    transport.add(f"{BASE}Articoli.php?Id=5101&page=2", body="")
    # Repeating category B's first page must not prevent walking its page 2.
    transport.add(
        f"{BASE}Articoli.php?Id=5102&page=1",
        body='<a href="Articolo.php?cod=ONE" class="product-name">One</a>',
    )
    transport.add(
        f"{BASE}Articoli.php?Id=5102&page=2",
        body='<a href="Articolo.php?cod=TWO" class="product-name">Two</a>',
    )
    transport.add(f"{BASE}Articoli.php?Id=5102&page=3", body="")


def _packs(first: str, second: str) -> list[dict[str, str]]:
    return [
        {"pack": "1", "value": "1", "price": first, "unit_price": "€ 8,40/kg"},
        {"pack": "5", "value": "5", "price": second, "unit_price": "€ 7,92/kg"},
    ]


def _transport(*, include_one: bool = True, include_two: bool = True) -> FakeTransport:
    transport = FakeTransport()
    _discovery(transport)
    if include_one:
        transport.add(ONE, body=_product("ONE"))
        transport.add(ONE, json_body=_packs("26.65", "99.00"))
    if include_two:
        transport.add(TWO, body=_product("TWO"))
        transport.add(TWO, json_body=_packs("9.50", "44.00"))
    return transport


def _build(
    transport: FakeTransport,
    *, telemetry: Telemetry | None = None,
    cancelled: Any = None,
) -> CeramicoloursConnector:
    return cast(
        CeramicoloursConnector,
        application_connector_registry().build(
            "ceramicolours",
            transport=transport,
            options=CeramicoloursOptions(render=False).model_dump(mode="json"),
            context=ConnectorContext(
                clock=lambda: NOW,
                telemetry=telemetry,
                cancelled=cancelled,
            ),
        ),
    )


def test_configured_source_constructs_through_application_registry() -> None:
    sources = SourcesFile.load(Path(__file__).parents[1] / "sources.json")
    [(source_id, config)] = [
        item for item in sources.items() if item[1].scraper == "ceramicolours"
    ]
    definition = source_definition(source_id, config)
    connector = application_connector_registry().build(
        definition.connector,
        transport=FakeTransport(),
        options=definition.connector_options,
        context=ConnectorContext(),
    )

    assert definition.connector == "ceramicolours"
    assert connector.name == "ceramicolours"
    assert definition.connector_options["vat_status"] == "inclusive"


@pytest.mark.asyncio
async def test_local_shared_shell_constructs_and_evaluates_pack_prices(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mb_ceramics_catalogue.ops import commerce_scraper_runtime as runtime_module

    telemetry_instances: list[Telemetry] = []

    class LocalTelemetry(Telemetry):
        def __init__(self) -> None:
            super().__init__()
            telemetry_instances.append(self)

    monkeypatch.setattr(runtime_module, "LibraryDebugTelemetry", LocalTelemetry)
    config = {
        "label": "Ceramicolours",
        "url": BASE,
        "scraper": "library_ceramicolours_connector",
        "category_ids": ["5101"],
        "render": False,
    }
    fetcher = LocalFetcher()
    scraper = scrapers.build(
        "library_ceramicolours_connector", "shop", config, fetcher
    )

    with RecordBuilder({"shop": config}):
        result = await scraper.scrape(limit=1)

    assert result.records
    assert result.errors == []
    assert result.requests == 5
    assert result.rendered_pages == 1
    assert fetcher.evaluations == [
        (ONE, "ceramicolours.pack-prices.v1")
    ]
    events = telemetry_instances[0].events
    correlated = [
        fields
        for event, fields in events
        if event
        in {
            "catalogue.legacy_fetcher.request.started",
            "catalogue.legacy_fetcher.request.completed",
            "catalogue.library_connector.collection.started",
            "catalogue.library_connector.page.completed",
            "catalogue.library_connector.collection.completed",
        }
    ]
    assert correlated
    [collection_id] = {fields["collection_id"] for fields in correlated}
    assert str(collection_id).startswith("local:")
    assert {fields["source_id"] for fields in correlated} == {"shop"}
    assert {fields["connector"] for fields in correlated} == {"ceramicolours"}
    evaluation_events = [
        fields
        for event, fields in events
        if event.startswith("catalogue.legacy_fetcher.request.")
        and fields.get("browser_action") == "ceramicolours.pack-prices.v1"
    ]
    assert len(evaluation_events) == 2
    assert PACK_PRICE_SCRIPT not in repr(events)


@pytest.mark.asyncio
async def test_pack_totals_and_stock_are_preserved_across_checkpoint_resume() -> None:
    first_transport = _transport(include_two=False)
    connector = _build(first_transport)

    [limited] = [page async for page in connector.collect(_request(limit=1))]
    checkpoint = connector.checkpoint(_request(limit=1), "test", limited.resume_after)
    replay_transport = _transport(include_one=False)
    replay = _build(replay_transport)
    [resumed] = [
        page async for page in replay.collect(_request(limit=1), checkpoint)
    ]

    snapshot = limited.items[0]
    assert snapshot.external_id == "ONE"
    assert [offer.price.amount for variant in snapshot.variants for offer in variant.offers] == [
        Decimal("26.65"),
        Decimal("99.00"),
    ]
    assert [variant.stock.quantity for variant in snapshot.variants if variant.stock] == [10, 2]
    assert [variant.offers[0].pack_size for variant in snapshot.variants] == [
        Decimal("1"),
        Decimal("5"),
    ]
    assert all(variant.offers[0].unit == "kg" for variant in snapshot.variants)
    assert snapshot.variants[1].published_attributes["Prezzo unitario"] == "€ 7,92/kg"
    assert snapshot.images[0].url == f"{BASE}upload-immagini/ONE.jpg"
    assert limited.resume_after == {
        "index": 1,
        "url": TWO,
        "snapshot_offset": 0,
        "sequence": 1,
    }
    assert resumed.items[0].external_id == "TWO"
    assert resumed.resume_after is None and resumed.enumeration_intact

    entity_requests = [
        request
        for request in first_transport.requests
        if request.purpose is RequestPurpose.ENTITY
    ]
    evaluations = [
        request
        for request in first_transport.requests
        if request.purpose is RequestPurpose.ENRICHMENT
    ]
    assert [request.url for request in entity_requests] == [ONE]
    assert len(evaluations) == 1
    assert evaluations[0].browser is BrowserHint.REQUIRED
    assert evaluations[0].priority is RequestPriority.DATASET_REQUIRED
    assert evaluations[0].required
    assert evaluations[0].evaluation is not None
    assert evaluations[0].evaluation.action_id == "ceramicolours.pack-prices.v1"
    assert all(request.url != ONE for request in replay_transport.requests if request.purpose is RequestPurpose.ENTITY)


@pytest.mark.asyncio
async def test_evaluation_failure_falls_back_to_static_price_with_warning() -> None:
    transport = FakeTransport()
    _discovery(transport)
    transport.add(ONE, body=_product("ONE"))
    transport.add(ONE, error=TransportFailure("provider-private-detail"))
    transport.add(TWO, body=_product("TWO", packs=False))
    telemetry = Telemetry()

    pages = [page async for page in _build(transport, telemetry=telemetry).collect(_request())]

    first, second = (page.items[0] for page in pages)
    assert first.variants[0].offers[0].price.amount == Decimal("8.40")
    assert first.variants[0].stock is not None
    assert first.variants[0].stock.availability == "unknown"
    assert second.variants[0].offers[0].price.amount == Decimal("8.40")
    assert len([request for request in transport.requests if request.purpose is RequestPurpose.ENRICHMENT]) == 1
    assert telemetry.events == [
        (
            "ceramicolours.pack_evaluation_fallback",
            {
                "level": "warning",
                "url": ONE,
                "reason": "TransportFailure",
            },
        )
    ]
    assert "provider-private-detail" not in repr(telemetry.events)


@pytest.mark.asyncio
async def test_required_offer_budget_denial_is_retryable_not_static_fallback() -> None:
    transport = EvaluationBudgetDenied()
    _discovery(transport)
    transport.add(ONE, body=_product("ONE"))
    transport.add(TWO, body=_product("TWO", packs=False))

    [page] = [page async for page in _build(transport).collect(_request())]

    assert page.items == ()
    assert page.diagnostics[0].code is DiagnosticCode.REQUEST_BUDGET_EXHAUSTED
    assert page.diagnostics[0].retryable
    evaluation = next(
        request for request in transport.requests if request.evaluation is not None
    )
    assert evaluation.required
    assert evaluation.priority is RequestPriority.DATASET_REQUIRED


@pytest.mark.asyncio
async def test_optional_offer_budget_denial_uses_static_fallback() -> None:
    transport = EvaluationBudgetDenied()
    _discovery(transport)
    transport.add(ONE, body=_product("ONE"))
    telemetry = Telemetry()
    request = CollectionRequest(
        source_id="ceramicolours",
        base_url=BASE,
        requested_fields=frozenset({SnapshotField.IDENTITY}),
        result_limit=1,
    )

    [page] = [
        page
        async for page in _build(transport, telemetry=telemetry).collect(request)
    ]

    assert page.items[0].variants[0].offers[0].price.amount == Decimal("8.40")
    evaluation = next(
        attempted
        for attempted in transport.requests
        if attempted.evaluation is not None
    )
    assert not evaluation.required
    assert evaluation.priority is RequestPriority.OPTIONAL
    assert telemetry.events[0][0] == "ceramicolours.pack_evaluation_fallback"


@pytest.mark.asyncio
async def test_cancellation_before_collection_performs_no_io() -> None:
    transport = FakeTransport()
    connector = _build(transport, cancelled=lambda: True)

    assert [page async for page in connector.collect(_request())] == []
    assert transport.requests == []
