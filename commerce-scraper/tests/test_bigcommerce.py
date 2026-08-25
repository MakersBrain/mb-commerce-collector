from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from typing import Any

import pytest

from mb_commerce_scraper import CollectionRequest
from mb_commerce_scraper.connectors._request_profiles import (
    LEGACY_BROWSER_USER_AGENT,
)
from mb_commerce_scraper.connectors.base import ConnectorContext
from mb_commerce_scraper.connectors.bigcommerce import (
    BigCommerceConnector,
    BigCommerceOptions,
)
from mb_commerce_scraper.proxy import ProxyBudgetExhausted
from mb_commerce_scraper.testing import FakeTransport, assert_connector_pages
from mb_commerce_scraper.transports import (
    BrowserHint,
    BudgetExhausted,
    RequestPurpose,
    RobotsDenied,
    TransportRequest,
    TransportResponse,
)

NOW = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
ORIGIN = "https://store.test"
GRAPHQL = f"{ORIGIN}/graphql"


class RaisingTransport(FakeTransport):
    def __init__(self, error: RuntimeError) -> None:
        super().__init__()
        self.error = error

    async def request(self, request: TransportRequest) -> TransportResponse:
        self.requests.append(request)
        raise self.error


def token(origin: str = ORIGIN) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').decode().rstrip("=")
    claims = base64.urlsafe_b64encode(
        json.dumps({"cors": [origin]}).encode()
    ).decode().rstrip("=")
    return f"{header}.{claims}.{'x' * 40}"


def product(identifier: int = 42) -> dict[str, Any]:
    return {
        "entityId": identifier,
        "name": "Stoneware Glaze",
        "path": f"/stoneware-glaze-{identifier}/",
        "sku": f"GL-{identifier}",
        "description": "A durable glaze.",
        "brand": {"name": "Test Ceramics"},
        "availabilityV2": {"status": "Available"},
        "defaultImage": {"urlOriginal": "https://cdn.test/glaze.jpg"},
        "images": {"edges": []},
        "prices": {
            "price": {"value": "12.50", "currencyCode": "EUR"},
            "retailPrice": {"value": "15.00"},
        },
        "categories": {"edges": [{"node": {"name": "Glazes"}}]},
        "customFields": {
            "edges": [{"node": {"name": "SDS", "value": "/docs/sds.pdf"}}]
        },
        "variants": {"edges": []},
    }


def payload(
    products: list[dict[str, Any]], *, has_next: bool = False, cursor: str | None = None
) -> dict[str, Any]:
    return {
        "data": {
            "site": {
                "products": {
                    "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                    "edges": [{"node": value} for value in products],
                }
            }
        }
    }


def request(**values: Any) -> CollectionRequest:
    return CollectionRequest(source_id="big-store", base_url=f"{ORIGIN}/catalogue", **values)


async def test_public_graphql_snapshot_is_neutral() -> None:
    transport = FakeTransport()
    storefront_token = token()
    transport.add(
        f"{ORIGIN}/catalogue",
        body=f'storefront_api_token: "{storefront_token}"',
    )
    transport.add(GRAPHQL, json_body=payload([product()]))
    connector = BigCommerceConnector(
        transport,
        BigCommerceOptions(vat_status="inclusive"),
        ConnectorContext(clock=lambda: NOW),
    )

    intent = request()
    pages = await assert_connector_pages(
        connector.collect(intent),
        connector=connector,
        request=intent,
        forbidden_values=(storefront_token,),
    )

    snapshot = pages[0].items[0]
    assert snapshot.connector == "bigcommerce"
    assert snapshot.title == "Stoneware Glaze"
    assert snapshot.vendor == "Test Ceramics"
    assert snapshot.documents[0].url == f"{ORIGIN}/docs/sds.pdf"
    assert snapshot.variants[0].offers[0].price.amount == 12.50
    assert transport.requests[-1].json_body is not None
    assert transport.requests[0].headers == {
        "user-agent": LEGACY_BROWSER_USER_AGENT
    }
    assert transport.requests[-1].headers["user-agent"] == LEGACY_BROWSER_USER_AGENT
    assert [
        (item.url, item.purpose, item.browser, item.estimated_bytes)
        for item in transport.requests
    ] == [
        (
            f"{ORIGIN}/catalogue",
            RequestPurpose.DISCOVERY,
            BrowserHint.NEVER,
            500_000,
        ),
        (GRAPHQL, RequestPurpose.DISCOVERY, BrowserHint.NEVER, 1_000_000),
    ]


async def test_http_denial_uses_rendered_token_and_browser_graphql() -> None:
    transport = FakeTransport()
    storefront_token = token()
    token_page = f"{ORIGIN}/catalogue"
    transport.add(token_page, status=403)
    transport.add(
        token_page,
        body=f'storefront_api_token: "{storefront_token}"',
    )
    transport.add(GRAPHQL, json_body=payload([product()]))
    connector = BigCommerceConnector(
        transport,
        context=ConnectorContext(clock=lambda: NOW),
    )
    intent = request()

    pages = await assert_connector_pages(
        connector.collect(intent),
        connector=connector,
        request=intent,
        forbidden_values=(storefront_token,),
    )

    direct, rendered, graphql = transport.requests
    assert direct.url == rendered.url == token_page
    assert direct.browser is BrowserHint.NEVER
    assert rendered.browser is BrowserHint.REQUIRED
    assert graphql.url == GRAPHQL
    assert graphql.method == "POST"
    assert graphql.browser is BrowserHint.REQUIRED
    assert graphql.headers == {
        "authorization": f"Bearer {storefront_token}",
        "content-type": "application/json",
        "origin": ORIGIN,
        "referer": token_page,
    }
    assert isinstance(graphql.json_body, dict)
    assert isinstance(graphql.json_body["query"], str)
    assert graphql.json_body["variables"] == {"after": None}
    assert pages[0].diagnostics == ()
    assert [
        (item.purpose, item.browser, item.estimated_bytes)
        for item in transport.requests
    ] == [
        (RequestPurpose.DISCOVERY, BrowserHint.NEVER, 500_000),
        (RequestPurpose.DISCOVERY, BrowserHint.REQUIRED, 500_000),
        (RequestPurpose.DISCOVERY, BrowserHint.REQUIRED, 1_000_000),
    ]


async def test_origin_token_page_is_not_retried_as_a_second_candidate() -> None:
    transport = FakeTransport()
    transport.add(ORIGIN, body="no storefront token")
    transport.add(ORIGIN, body="no storefront token")
    connector = BigCommerceConnector(
        transport,
        BigCommerceOptions(token_page=ORIGIN),
    )

    page = await anext(connector.collect(request()))

    assert page.diagnostics[0].code.value == "parser_unsupported"
    assert [
        (item.url, item.purpose, item.browser, item.estimated_bytes)
        for item in transport.requests
    ] == [
        (ORIGIN, RequestPurpose.DISCOVERY, BrowserHint.NEVER, 500_000),
        (ORIGIN, RequestPurpose.DISCOVERY, BrowserHint.REQUIRED, 500_000),
    ]


async def test_result_limit_produces_resumable_checkpoint() -> None:
    transport = FakeTransport()
    transport.add(f"{ORIGIN}/catalogue", body=f'local_token="{token()}"')
    transport.add(GRAPHQL, json_body=payload([product(1)], has_next=True, cursor="next"))
    connector = BigCommerceConnector(transport)
    intent = request(result_limit=1)

    page = await anext(connector.collect(intent))
    checkpoint = connector.checkpoint(intent, "lineage", page.resume_after)

    assert not page.enumeration_intact
    assert checkpoint.resume_after == {"after": "next", "sequence": 1}

    changed = BigCommerceConnector(FakeTransport(), BigCommerceOptions(page_size=10))
    with pytest.raises(ValueError, match="CHECKPOINT_INVALID"):
        await anext(changed.collect(intent, checkpoint))


async def test_cancelled_collection_makes_no_requests() -> None:
    transport = FakeTransport()
    connector = BigCommerceConnector(
        transport, context=ConnectorContext(cancelled=lambda: True)
    )
    pages = [page async for page in connector.collect(request())]
    assert pages == []
    assert transport.requests == []


async def test_rejects_token_scoped_to_another_origin() -> None:
    transport = FakeTransport()
    bad = token("https://other.test")
    transport.add(f"{ORIGIN}/catalogue", body=f'local_token="{bad}"')
    transport.add(f"{ORIGIN}/catalogue", body=f'local_token="{bad}"')
    transport.add(ORIGIN, body="")
    transport.add(ORIGIN, body="")

    page = await anext(BigCommerceConnector(transport).collect(request()))

    assert page.items == ()
    assert page.diagnostics[0].code.value == "parser_unsupported"


@pytest.mark.parametrize(
    "error",
    [
        BudgetExhausted("request budget exhausted"),
        RobotsDenied("robots denied"),
        ProxyBudgetExhausted("proxy budget exhausted"),
    ],
)
async def test_token_discovery_does_not_swallow_policy_failures(error: RuntimeError) -> None:
    connector = BigCommerceConnector(RaisingTransport(error))

    with pytest.raises(type(error)):
        await anext(connector.collect(request()))


async def test_checkpoint_rejects_boolean_sequence() -> None:
    connector = BigCommerceConnector(FakeTransport())
    intent = request()
    checkpoint = connector.checkpoint(
        intent,
        "lineage",
        {"after": "cursor", "sequence": True},
    )

    with pytest.raises(ValueError, match="checkpoint cursor is invalid"):
        await anext(connector.collect(intent, checkpoint))
