from __future__ import annotations

from collections.abc import AsyncIterator
from typing import cast

import pytest
from pydantic import JsonValue, ValidationError

from mb_commerce_scraper import (
    CollectionRequest,
    CommerceProductSnapshot,
    DiagnosticCode,
)
from mb_commerce_scraper.connectors import (
    ConnectorRegistry,
    GenericPagesConnector,
    GenericPagesFactory,
    GenericPagesOptions,
)
from mb_commerce_scraper.connectors.base import ConnectorContext
from mb_commerce_scraper.parsing import JsonLdProductParser
from mb_commerce_scraper.testing import FakeTransport, assert_connector_pages
from mb_commerce_scraper.transports import RequestPurpose, TransportFailure
from mb_commerce_scraper.transports.middleware import BudgetExhausted

SITEMAP = (
    "<urlset><url><loc>https://shop.test/products/clay</loc></url></urlset>"
)
TWO_PRODUCTS = """<script type="application/ld+json">[
  {"@type":"Product","name":"Clay","sku":"CLAY-1",
   "offers":{"price":"8.50","priceCurrency":"EUR"}},
  {"@type":"Product","name":"Glaze","sku":"GLAZE-1",
   "offers":{"price":"5.25","priceCurrency":"EUR"}}
]</script>"""


class VersionedParser:
    name = "fixture-parser"

    def __init__(self, version: str) -> None:
        self.version = version
        self._delegate = JsonLdProductParser()

    def parse(
        self, document: str, *, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        return self._delegate.parse(document, url=url, source_id=source_id)


class VersionedDiscovery:
    name = "fixture-discovery"

    def __init__(self, version: str, *urls: str) -> None:
        self.version = version
        self.urls = urls
        self.calls = 0
        self.yields = 0

    async def discover(self, base_url: str) -> AsyncIterator[str]:
        del base_url
        self.calls += 1
        for url in self.urls:
            self.yields += 1
            yield url


def _request(*, limit: int | None = None) -> CollectionRequest:
    return CollectionRequest(
        source_id="shop", base_url="https://shop.test/", result_limit=limit
    )


def _options(**discovery: object) -> GenericPagesOptions:
    values: dict[str, object] = {
        "sitemaps": ["/sitemap.xml"],
        "use_advertised_sitemaps": False,
        "product_pattern": r"/products/",
        **discovery,
    }
    return GenericPagesOptions.model_validate({"discovery": values})


async def test_sitemap_jsonld_collection_finishes_with_complete_terminal_page() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/sitemap.xml", body=SITEMAP)
    transport.add("https://shop.test/products/clay", body=TWO_PRODUCTS)
    connector = GenericPagesConnector(transport, _options())

    request = _request()
    pages = await assert_connector_pages(
        connector.collect(request), connector=connector, request=request
    )

    assert len(pages) == 1
    assert pages[0].partition_key == "sitemap"
    assert [item.title for item in pages[0].items] == ["Clay", "Glaze"]
    assert pages[0].sequence == 0
    assert pages[0].terminal and pages[0].enumeration_intact
    assert pages[0].resume_after is None


async def test_robots_advertised_sitemap_enters_shared_discovery() -> None:
    transport = FakeTransport()
    transport.add(
        "https://shop.test/robots.txt",
        body="User-agent: *\nSitemap: /catalogue-sitemap.xml\n",
    )
    transport.add("https://shop.test/catalogue-sitemap.xml", body=SITEMAP)
    transport.add("https://shop.test/products/clay", body=TWO_PRODUCTS)
    connector = GenericPagesConnector(
        transport,
        _options(sitemaps=[], use_advertised_sitemaps=True),
    )

    pages = await assert_connector_pages(connector.collect(_request()))

    assert pages[-1].terminal and pages[-1].enumeration_intact
    assert [request.purpose for request in transport.requests[:2]] == [
        RequestPurpose.ROBOTS,
        RequestPurpose.DISCOVERY,
    ]
    assert transport.requests[1].url == "https://shop.test/catalogue-sitemap.xml"


async def test_sitemap_http_failure_becomes_typed_incomplete_page() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/sitemap.xml", status=503)
    connector = GenericPagesConnector(transport, _options())

    [page] = await assert_connector_pages(connector.collect(_request()))

    assert page.terminal and not page.enumeration_intact
    assert page.resume_after is None
    assert page.diagnostics[0].code == DiagnosticCode.ENUMERATION_INCOMPLETE
    assert page.diagnostics[0].retryable
    assert "status 503" in page.diagnostics[0].message


async def test_robots_discovery_does_not_swallow_policy_failure() -> None:
    transport = FakeTransport()
    transport.add(
        "https://shop.test/robots.txt",
        error=BudgetExhausted("request budget exhausted"),
    )
    connector = GenericPagesConnector(
        transport,
        _options(sitemaps=[], use_advertised_sitemaps=True),
    )

    with pytest.raises(BudgetExhausted):
        await anext(connector.collect(_request()))


async def test_result_limit_cursor_preserves_snapshots_on_same_url_and_sequence() -> None:
    def open_connector() -> GenericPagesConnector:
        transport = FakeTransport()
        transport.add("https://shop.test/sitemap.xml", body=SITEMAP)
        transport.add("https://shop.test/products/clay", body=TWO_PRODUCTS)
        return GenericPagesConnector(transport, _options())

    connector = open_connector()
    request = _request(limit=1)
    [limited] = await assert_connector_pages(
        connector.collect(request),
        connector=connector,
        request=request,
        reopen=open_connector,
    )
    assert [item.title for item in limited.items] == ["Clay"]
    assert limited.sequence == 0
    assert limited.resume_after == {
        "index": 0,
        "url": "https://shop.test/products/clay",
        "snapshot_offset": 1,
        "sequence": 1,
    }
    assert limited.terminal and not limited.enumeration_intact


async def test_custom_parser_identity_is_part_of_checkpoint_fingerprint() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/sitemap.xml", body=SITEMAP)
    transport.add("https://shop.test/products/clay", body=TWO_PRODUCTS)
    connector = GenericPagesConnector(
        transport,
        _options(),
        parser=VersionedParser("1"),
    )
    request = _request(limit=1)
    [limited] = await assert_connector_pages(connector.collect(request))
    checkpoint = connector.checkpoint(request, "lineage", limited.resume_after)

    resumed_transport = FakeTransport()
    resumed_transport.add("https://shop.test/sitemap.xml", body=SITEMAP)
    resumed_transport.add("https://shop.test/products/clay", body=TWO_PRODUCTS)
    resumed = GenericPagesConnector(
        resumed_transport,
        _options(),
        parser=VersionedParser("1"),
    )
    [page] = await assert_connector_pages(
        resumed.collect(request, checkpoint),
        connector=resumed,
        request=request,
        start_sequence=1,
    )
    assert page.sequence == 1 and page.terminal
    assert [item.title for item in page.items] == ["Glaze"]

    changed_transport = FakeTransport()
    changed = GenericPagesConnector(
        changed_transport,
        _options(),
        parser=VersionedParser("2"),
    )
    with pytest.raises(ValueError, match="CHECKPOINT_INVALID"):
        await anext(changed.collect(request, checkpoint))
    assert changed_transport.requests == []


async def test_registry_composes_versioned_discovery_and_parser_with_safe_resume() -> None:
    discovery = VersionedDiscovery(
        "1", "/products/clay#first", "/products/clay#duplicate"
    )
    registry = ConnectorRegistry()
    registry.register(
        GenericPagesFactory(parser=VersionedParser("1"), discovery=discovery)
    )
    transport = FakeTransport()
    transport.add("https://shop.test/products/clay", body=TWO_PRODUCTS)
    connector = cast(
        GenericPagesConnector,
        registry.build(
            "generic-pages",
            transport=transport,
            options=_options().model_dump(mode="json"),
            context=ConnectorContext(),
        ),
    )

    request = _request(limit=1)
    [limited] = await assert_connector_pages(
        connector.collect(request), connector=connector, request=request
    )
    assert discovery.calls == 1
    assert [attempt.url for attempt in transport.requests] == [
        "https://shop.test/products/clay"
    ]
    assert limited.partition_key == "strategy:fixture-discovery"
    assert [item.title for item in limited.items] == ["Clay"]
    checkpoint = connector.checkpoint(request, "lineage", limited.resume_after)

    resumed_discovery = VersionedDiscovery("1", "/products/clay")
    resumed_registry = ConnectorRegistry()
    resumed_registry.register(
        GenericPagesFactory(
            parser=VersionedParser("1"), discovery=resumed_discovery
        )
    )
    resumed_transport = FakeTransport()
    resumed_transport.add("https://shop.test/products/clay", body=TWO_PRODUCTS)
    resumed = resumed_registry.build(
        "generic-pages",
        transport=resumed_transport,
        options=_options().model_dump(mode="json"),
        context=ConnectorContext(),
    )
    [terminal] = await assert_connector_pages(
        resumed.collect(request, checkpoint),
        connector=resumed,
        request=request,
        start_sequence=1,
    )
    assert resumed_discovery.calls == 1
    assert [item.title for item in terminal.items] == ["Glaze"]
    assert terminal.terminal and terminal.enumeration_intact

    for parser_version, discovery_version in (("2", "1"), ("1", "2")):
        drifted_discovery = VersionedDiscovery(
            discovery_version, "/products/clay"
        )
        drifted_registry = ConnectorRegistry()
        drifted_registry.register(
            GenericPagesFactory(
                parser=VersionedParser(parser_version),
                discovery=drifted_discovery,
            )
        )
        drifted_transport = FakeTransport()
        drifted = drifted_registry.build(
            "generic-pages",
            transport=drifted_transport,
            options=_options().model_dump(mode="json"),
            context=ConnectorContext(),
        )
        with pytest.raises(ValueError, match="CHECKPOINT_INVALID"):
            await anext(drifted.collect(request, checkpoint))
        assert drifted_discovery.calls == 0
        assert drifted_transport.requests == []


async def test_custom_discovery_rejects_off_origin_before_transport() -> None:
    discovery = VersionedDiscovery("1", "https://other.test/products/clay")
    transport = FakeTransport()
    connector = GenericPagesConnector(
        transport, _options(), discovery=discovery
    )

    [page] = await assert_connector_pages(connector.collect(_request()))

    assert page.terminal and not page.enumeration_intact
    assert page.diagnostics[0].code is DiagnosticCode.ENUMERATION_INCOMPLETE
    assert not page.diagnostics[0].retryable
    assert page.diagnostics[0].message == (
        "custom discovery yielded an off-origin product URL"
    )
    assert transport.requests == []


async def test_custom_discovery_stops_after_bounded_page_limit_lookahead() -> None:
    discovery = VersionedDiscovery(
        "1", "/products/one", "/products/two", "/products/never-consumed"
    )
    options = GenericPagesOptions.model_validate(
        {
            "page_limit": 1,
            "discovery": {
                "use_advertised_sitemaps": False,
                "product_pattern": r"/products/",
            },
        }
    )
    transport = FakeTransport()
    transport.add("https://shop.test/products/one", body=TWO_PRODUCTS)
    connector = GenericPagesConnector(
        transport,
        options,
        parser=VersionedParser("1"),
        discovery=discovery,
    )

    pages = await assert_connector_pages(connector.collect(_request()))

    assert discovery.yields == 2
    assert [attempt.url for attempt in transport.requests] == [
        "https://shop.test/products/one"
    ]
    assert pages[-1].terminal and not pages[-1].enumeration_intact
    assert pages[-1].resume_after == {
        "index": 1,
        "url": "https://shop.test/products/two",
        "snapshot_offset": 0,
        "sequence": 1,
    }
    assert pages[-1].diagnostics[0].code is DiagnosticCode.ENUMERATION_INCOMPLETE


@pytest.mark.parametrize(
    "cursor,match",
    [
        ({"after_url": "https://shop.test/products/clay"}, "cursor values"),
        (
            {
                "index": 0,
                "url": "https://shop.test/products/removed",
                "snapshot_offset": 0,
                "sequence": 4,
            },
            "resume target",
        ),
        (
            {
                "index": 2,
                "url": "https://shop.test/products/removed",
                "snapshot_offset": 0,
                "sequence": 4,
            },
            "out of range",
        ),
    ],
)
async def test_resume_rejects_invalid_or_missing_discovery_target(
    cursor: JsonValue, match: str
) -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/sitemap.xml", body=SITEMAP)
    transport.add("https://shop.test/products/clay", body=TWO_PRODUCTS)
    connector = GenericPagesConnector(transport, _options())
    checkpoint = connector.checkpoint(_request(), "lineage", cursor)

    with pytest.raises(ValueError, match=match):
        await anext(connector.collect(_request(), checkpoint))


async def test_category_and_pagination_discovery_feed_the_shared_engine() -> None:
    transport = FakeTransport()
    transport.add(
        "https://shop.test/clay/",
        body=(
            '<div class="product-card"><a href="/products/red">Red</a></div>'
            '<a class="next" href="/clay/?page=2">Next</a>'
            '<a href="https://elsewhere.test/products/noise">Noise</a>'
        ),
    )
    transport.add(
        "https://shop.test/clay/?page=2",
        body='<div class="product-card"><a href="/products/blue">Blue</a></div>',
    )
    transport.add(
        "https://shop.test/products/red", body=TWO_PRODUCTS.replace("Glaze", "Slip")
    )
    transport.add("https://shop.test/products/blue", body=TWO_PRODUCTS)
    connector = GenericPagesConnector(
        transport,
        _options(
            sitemaps=[],
            category_urls=["/clay/"],
            pagination_patterns=[r"[?&]page="],
            card_links_only=True,
        ),
    )

    pages = await assert_connector_pages(connector.collect(_request()))

    assert [page.sequence for page in pages] == [0, 1]
    assert {page.partition_key for page in pages} == {"category"}
    assert [request.url for request in transport.requests] == [
        "https://shop.test/clay/",
        "https://shop.test/clay/?page=2",
        "https://shop.test/products/red",
        "https://shop.test/products/blue",
    ]


async def test_verified_dom_rules_collect_without_executable_selectors() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/sitemap.xml", body=SITEMAP)
    transport.add(
        "https://shop.test/products/clay",
        body="""<main id="product"><h1 class="product-title">DOM Clay</h1>
        <span class="product-price">7.50</span>
        <span data-product-sku="DOM-1">DOM-1</span></main>""",
    )
    options = GenericPagesOptions.model_validate(
        {
            "discovery": _options().discovery.model_dump(mode="json"),
            "parsers": ["dom"],
            "dom_rules": {
                "verification": ["#product"],
                "name": "h1.product-title",
                "price": ".product-price",
                "sku": "[data-product-sku]",
            },
            "currency": "EUR",
        }
    )

    [page] = await assert_connector_pages(
        GenericPagesConnector(transport, options).collect(_request())
    )

    [snapshot] = page.items
    assert snapshot.title == "DOM Clay"
    assert snapshot.variants[0].sku == "DOM-1"
    assert str(snapshot.variants[0].offers[0].price.amount) == "7.50"
    assert snapshot.platform_extensions["page_parser"] == "dom"


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        ("<html><p>unknown product</p></html>", DiagnosticCode.PARSER_UNSUPPORTED),
        (
            '<html><div id="root"></div><script>enable javascript</script></html>',
            DiagnosticCode.BROWSER_REQUIRED,
        ),
    ],
)
async def test_empty_parser_results_distinguish_browser_requirement(
    document: str, expected: DiagnosticCode
) -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/sitemap.xml", body=SITEMAP)
    transport.add("https://shop.test/products/clay", body=document)
    # Keep the test on the classification boundary: browser execution is
    # deliberately disabled rather than faked as another parser response.
    options = _options().model_copy(update={"render": False})
    connector = GenericPagesConnector(transport, options, ConnectorContext())

    [page] = await assert_connector_pages(connector.collect(_request()))

    diagnostic = page.diagnostics[0]
    assert diagnostic.code == expected
    assert diagnostic.retryable is (expected == DiagnosticCode.BROWSER_REQUIRED)
    assert diagnostic.metadata == {
        "browser_attempted": False,
        "render_policy": "never",
    }


async def test_browser_http_failure_is_typed_with_stage_metadata() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/sitemap.xml", body=SITEMAP)
    transport.add(
        "https://shop.test/products/clay",
        body='<html><div id="root"></div><script>enable javascript</script></html>',
    )
    transport.add("https://shop.test/products/clay", status=503)

    [page] = await assert_connector_pages(
        GenericPagesConnector(transport, _options()).collect(_request())
    )

    diagnostic = page.diagnostics[0]
    assert diagnostic.code == DiagnosticCode.ENTITY_FETCH_FAILED
    assert diagnostic.retryable
    assert diagnostic.metadata == {"stage": "browser"}


async def test_entity_transport_failure_is_typed_with_http_stage() -> None:
    transport = FakeTransport()
    transport.add("https://shop.test/sitemap.xml", body=SITEMAP)
    transport.add(
        "https://shop.test/products/clay",
        error=TransportFailure("connection reset"),
    )

    [page] = await assert_connector_pages(
        GenericPagesConnector(transport, _options()).collect(_request())
    )

    diagnostic = page.diagnostics[0]
    assert diagnostic.code == DiagnosticCode.ENTITY_FETCH_FAILED
    assert diagnostic.retryable
    assert diagnostic.metadata == {"stage": "http"}

    policy_transport = FakeTransport()
    policy_transport.add("https://shop.test/sitemap.xml", body=SITEMAP)
    policy_transport.add(
        "https://shop.test/products/clay",
        error=BudgetExhausted("request budget exhausted"),
    )
    [budget_page] = await assert_connector_pages(
        GenericPagesConnector(policy_transport, _options()).collect(_request())
    )
    budget_diagnostic = budget_page.diagnostics[0]
    assert budget_diagnostic.code == DiagnosticCode.REQUEST_BUDGET_EXHAUSTED
    assert budget_diagnostic.retryable
    assert budget_diagnostic.metadata == {"stage": "http"}


async def test_cancellation_before_discovery_makes_no_requests() -> None:
    transport = FakeTransport()
    connector = GenericPagesConnector(
        transport, _options(), ConnectorContext(cancelled=lambda: True)
    )

    assert [page async for page in connector.collect(_request())] == []
    assert transport.requests == []


def test_declarative_config_rejects_code_execution_fields() -> None:
    with pytest.raises(ValidationError):
        GenericPagesOptions.model_validate({"python_expression": "__import__('os')"})
    with pytest.raises(ValidationError):
        GenericPagesOptions.model_validate({"parsers": ["javascript"]})
    with pytest.raises(ValidationError, match="one tag/id/class/attribute"):
        GenericPagesOptions.model_validate(
            {
                "parsers": ["dom"],
                "dom_rules": {"name": "main h1"},
            }
        )
    with pytest.raises(ValidationError, match="requires dom_rules"):
        GenericPagesOptions.model_validate({"parsers": ["dom"]})


def test_custom_parser_requires_explicit_stable_identity() -> None:
    class AnonymousParser:
        name = ""
        version = "1"

        def parse(
            self, document: str, *, url: str, source_id: str
        ) -> tuple[CommerceProductSnapshot, ...]:
            del document, url, source_id
            return ()

    with pytest.raises(ValueError, match="custom parser name"):
        GenericPagesConnector(FakeTransport(), _options(), parser=AnonymousParser())

    class AnonymousDiscovery:
        name = ""
        version = "1"

        async def discover(self, base_url: str) -> AsyncIterator[str]:
            del base_url
            if False:
                yield ""

    with pytest.raises(ValueError, match="custom discovery name"):
        GenericPagesFactory(discovery=AnonymousDiscovery())


def test_declarative_parser_chain_accepts_safe_markup_parsers() -> None:
    options = GenericPagesOptions.model_validate(
        {"parsers": ["jsonld", "microdata", "opengraph"]}
    )
    assert options.parsers == ("jsonld", "microdata", "opengraph")
