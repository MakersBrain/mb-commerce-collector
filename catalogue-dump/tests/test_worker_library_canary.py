from __future__ import annotations

import asyncio
import base64
import html
import json
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, Literal, cast
from uuid import uuid4

import httpx
import pytest
from mb_commerce_scraper.runtime import CommerceScraper
from mb_commerce_scraper.testing import FakeTransport

from mb_ceramics_catalogue.config.settings import (
    CrawlParams,
    DatasetSelection,
    Settings,
)
from mb_ceramics_catalogue.config.sources import SourcesFile
from mb_ceramics_catalogue.ops import outputs, runs
from mb_ceramics_catalogue.ops.queue import ClaimedJob
from mb_ceramics_catalogue.ops.worker import Worker
from mb_ceramics_catalogue.storage.db import DictPool
from mb_ceramics_catalogue.transports.browser import (
    BrowserEvaluationResult,
    BrowserFetchResponse,
    BrowserJobContext,
    BrowserSession,
)

from .conftest import requires_postgres

pytestmark = [pytest.mark.postgres, requires_postgres]


class Pool:
    def __init__(self, connection: outputs.Connection) -> None:
        self._connection = connection

    @asynccontextmanager
    async def connection(self):
        yield self._connection


class Limiter:
    def __init__(self) -> None:
        self.groups: list[tuple[str, str]] = []
        self.delays: list[tuple[str, float]] = []

    def join_group(self, url: str, group: str) -> None:
        self.groups.append((url, group))

    def set_delay(self, url: str, delay: float) -> None:
        self.delays.append((url, delay))


class Fetcher:
    proxy_lease = None

    def __init__(self) -> None:
        self.urls: list[str] = []
        self.limiter = Limiter()
        self.stats = SimpleNamespace(
            proxy_requests=0,
            direct_requests=0,
            impersonated_requests=0,
            browser_requests=0,
        )

    async def response(self, url: str, **kwargs: Any) -> httpx.Response:
        self.urls.append(url)
        self.stats.direct_requests += 1
        return httpx.Response(
            200,
            json={"products": []},
            request=httpx.Request("GET", url, params=kwargs.get("params")),
        )

    async def render(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None
    ) -> str:
        del url, wait_ms, wait_for
        raise AssertionError("Shopify library canary must not render")

    async def rotate_client(self) -> None:
        raise AssertionError("empty Shopify feed must not rotate")

    async def may_fetch(
        self, url: str, ignore_robots: bool = False, obey_robots: bool = False
    ) -> bool:
        del url, ignore_robots, obey_robots
        return True


def _bigcommerce_token() -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').decode().rstrip("=")
    claims = base64.urlsafe_b64encode(
        json.dumps({"cors": ["https://shop.test"]}).encode()
    ).decode().rstrip("=")
    return f"{header}.{claims}.{'x' * 40}"


def _sumup_document() -> str:
    product = {
        "id": "ebec5308-b80d-4c0c-ae84-26ce26a341b4",
        "name": "Tasse bleue",
        "slug": "tasse-bleue",
        "price": 2500,
        "basePrice": 3000,
        "hasDiscount": True,
        "isAvailable": True,
        "category": {"name": "Ceramiques"},
        "variants": {},
    }
    payload = '{"currency":"EUR","product":' + json.dumps(
        product, separators=(",", ":")
    ) + "}"
    return f"<script>self.__next_f.push([1,{json.dumps(payload)}])</script>"


class NativeBrowserSession:
    def __init__(self, token: str, documents: dict[str, str] | None = None) -> None:
        self.token = token
        self.documents = documents or {}
        self.rendered: list[str] = []
        self.requests: list[dict[str, Any]] = []
        self.evaluations: list[tuple[str, str, int, str | None]] = []
        self.closes = 0

    async def render(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None
    ) -> str:
        del wait_ms, wait_for
        self.rendered.append(url)
        return self.documents.get(url, f'storefront_api_token: "{self.token}"')

    async def request(
        self,
        page_url: str,
        endpoint: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        json_body: Any = None,
    ) -> BrowserFetchResponse:
        self.requests.append(
            {
                "page_url": page_url,
                "endpoint": endpoint,
                "method": method,
                "headers": headers,
                "json_body": json_body,
            }
        )
        if method == "GET":
            document = self.documents.get(
                endpoint, f'storefront_api_token: "{self.token}"'
            )
            return BrowserFetchResponse(
                status=200,
                headers={"content-type": "text/html; charset=utf-8"},
                content=document.encode(),
                final_url=endpoint,
            )
        payload = {
            "data": {
                "site": {
                    "products": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "edges": [
                            {
                                "node": {
                                    "entityId": 10,
                                    "name": "Glaze",
                                    "path": "/product/glaze",
                                    "sku": "GLAZE",
                                    "brand": {"name": "Test Ceramics"},
                                    "availabilityV2": {"status": "Available"},
                                    "defaultImage": None,
                                    "images": {"edges": []},
                                    "prices": {
                                        "price": {
                                            "value": "12.50",
                                            "currencyCode": "EUR",
                                        },
                                        "retailPrice": None,
                                    },
                                    "categories": {"edges": []},
                                    "customFields": {"edges": []},
                                    "variants": {"edges": []},
                                }
                            }
                        ],
                    }
                }
            }
        }
        return BrowserFetchResponse(
            status=200,
            headers={"content-type": "application/json"},
            content=json.dumps(payload).encode(),
            final_url=endpoint,
        )

    async def evaluate(self, *args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise AssertionError("BigCommerce canary does not evaluate scripts")

    async def evaluate_result(
        self, url: str, script: str, wait_ms: int = 2000,
        wait_for: str | None = None,
    ) -> BrowserEvaluationResult:
        self.evaluations.append((url, script, wait_ms, wait_for))
        return BrowserEvaluationResult(
            value=[
                {
                    "pack": "1",
                    "value": "1",
                    "price": "26.65",
                    "unit_price": "8.40 EUR/kg",
                },
                {
                    "pack": "5",
                    "value": "5",
                    "price": "99.00",
                    "unit_price": "7.92 EUR/kg",
                },
            ],
            final_url=url,
        )

    async def request_json(self, *args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise AssertionError("the neutral adapter uses request, not request_json")

    async def close(self) -> None:
        self.closes += 1


class NativeBrowserBackend:
    backend: Literal["camoufox"] = "camoufox"

    def __init__(self, session: NativeBrowserSession) -> None:
        self.session = session
        self.jobs: list[BrowserJobContext | None] = []
        self.shutdowns = 0

    @asynccontextmanager
    async def open_session(self, job: BrowserJobContext | None = None):
        self.jobs.append(job)
        try:
            yield cast(BrowserSession, self.session)
        finally:
            await self.session.close()

    async def shutdown(self) -> None:
        self.shutdowns += 1


async def test_native_shopify_result_limit_publishes_usable_sealed_output(
    db,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """A deliberate sample is usable output, not an enumeration failure.

    The limit is an operator-owned collection boundary.  It must retain its
    cursor and incomplete-enumeration signal for auditability while publishing
    the bounded records adds-only.  Re-entering the same job then recovers the
    sealed result instead of opening another transport or publishing it twice.
    """
    from mb_ceramics_catalogue.ops import commerce_scraper_runtime as runtime_module
    from mb_ceramics_catalogue.ops import worker as worker_module
    from mb_ceramics_catalogue.storage import postgres

    sources = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": "shopify",
                "currency": "EUR",
                "scope": "all",
            }
        }
    )
    run_id = await runs.create_run(db)
    assert run_id is not None
    job_id = (await runs.create_jobs(db, run_id, sources, ["shop"]))["shop"]
    claimed = ClaimedJob(
        id=job_id,
        run_id=run_id,
        source_id="shop",
        host="shop.test",
        attempt=1,
        max_attempts=3,
        requires=[],
        requires_any=[],
        params={},
        proxy_snapshot={"policy": "never"},
        delivery_generation=1,
        execution_token=uuid4(),
    )
    worker = Worker(
        cast(DictPool, Pool(db)),
        sources,
        Settings(
            dsn=os.environ["CATALOGUE_TEST_DSN"],
            dumps_dir=tmp_path,
            cache_dir=tmp_path / "cache",
        ),
    )
    worker._cancels[job_id] = asyncio.Event()

    backend = FakeTransport()
    backend.add(
        "https://shop.test/products.json",
        json_body={
            "products": [
                {
                    "id": 1,
                    "handle": "first-glaze",
                    "title": "First glaze",
                    "variants": [
                        {
                            "id": 11,
                            "title": "500 ml",
                            "sku": "FIRST-500",
                            "price": "12.50",
                            "available": True,
                        }
                    ],
                },
                {
                    "id": 2,
                    "handle": "second-glaze",
                    "title": "Second glaze",
                    "variants": [
                        {
                            "id": 22,
                            "title": "500 ml",
                            "sku": "SECOND-500",
                            "price": "13.50",
                            "available": True,
                        }
                    ],
                },
            ]
        },
    )
    native_runtimes = 0
    proxy_resolutions = 0
    original_resolver = worker_module.resolve_native_proxy_runtime

    def native_builder(**kwargs: Any) -> CommerceScraper:
        nonlocal native_runtimes
        native_runtimes += 1
        return CommerceScraper(
            registry=kwargs["registry"],
            transport=backend,
            fetch_policy=kwargs["fetch_policy"],
            cache=kwargs["cache"],
            telemetry=kwargs["telemetry"],
            retries=kwargs["retries"],
        )

    def resolve_native_proxy(*args: Any, **kwargs: Any) -> Any:
        nonlocal proxy_resolutions
        proxy_resolutions += 1
        return original_resolver(*args, **kwargs)

    async def forbidden_legacy_session(*args: Any, **kwargs: Any):
        del args, kwargs
        raise AssertionError("native limited collection must not open a legacy session")

    async def forbidden_legacy_proxy(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AssertionError("native limited collection must not acquire a legacy proxy")

    def forbidden_browser(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise AssertionError("Shopify limited collection must not resolve a browser")

    loads: list[tuple[str, bool]] = []

    def load_limited_artifact(
        loaded_job: ClaimedJob,
        location: str,
        whole: bool,
    ) -> postgres.SourceReport:
        assert loaded_job.id == job_id
        loads.append((location, whole))
        return postgres.SourceReport(source="shop", records=1, retired=0)

    finished: list[tuple[str, dict[str, Any]]] = []

    async def finish(
        finished_job: ClaimedJob,
        state: str,
        **kwargs: Any,
    ) -> None:
        assert finished_job.id == job_id
        finished.append((state, kwargs))

    monkeypatch.setattr(runtime_module, "build_http_scraper", native_builder)
    monkeypatch.setattr(
        worker_module,
        "resolve_native_proxy_runtime",
        resolve_native_proxy,
    )
    monkeypatch.setattr(worker_module, "open_session", forbidden_legacy_session)
    monkeypatch.setattr(worker, "_proxy_lease", forbidden_legacy_proxy)
    monkeypatch.setattr(worker, "_browser_for_job", forbidden_browser)
    monkeypatch.setattr(worker, "_load_connector_artifact", load_limited_artifact)
    monkeypatch.setattr(worker, "_finish", finish)

    params = CrawlParams(
        pipeline="connector_canary",
        datasets=("ceramics.catalogue_item.v2",),
        limit=1,
    )

    await worker._crawl_connector_canary(claimed, params, sources["shop"])
    await worker._crawl_connector_canary(claimed, params, sources["shop"])

    assert native_runtimes == 1
    assert proxy_resolutions == 1
    assert [request.url for request in backend.requests] == [
        "https://shop.test/products.json"
    ]
    assert [state for state, _ in finished] == ["succeeded", "succeeded"]
    first_summary = finished[0][1]["summary"]
    recovered_summary = finished[1][1]["summary"]
    assert first_summary["records"] == 1
    assert first_summary["truncated"] is True
    assert first_summary["retired"] == 0
    assert first_summary["datasets"] == {
        "ceramics.catalogue_item.v2": {"state": "published", "records": 1}
    }
    assert recovered_summary["records"] == 1
    assert recovered_summary["truncated"] is True
    assert recovered_summary["terminal_recovery"] is True
    assert not any(recovered_summary["transport"].values())
    assert loads and all(whole is False for _, whole in loads)

    lineage_cursor = await db.execute(
        "select status, checksum from catalogue.job_checkpoint_lineages "
        "where job_id = %s",
        (job_id,),
    )
    lineage_rows = await lineage_cursor.fetchall()
    assert len(lineage_rows) == 1
    assert lineage_rows[0]["status"] == "limited"
    assert len(lineage_rows[0]["checksum"]) == 64

    dataset_cursor = await db.execute(
        "select state, complete, records, rejected from catalogue.job_datasets "
        "where job_id = %s and dataset = 'ceramics.catalogue_item.v2'",
        (job_id,),
    )
    assert await dataset_cursor.fetchone() == {
        "state": "succeeded",
        "complete": True,
        "records": 1,
        "rejected": 0,
    }
    outcome_cursor = await db.execute(
        "select coalesce(sum(records), 0) as records "
        "from catalogue.job_page_dataset_outcomes where job_id = %s",
        (job_id,),
    )
    assert (await outcome_cursor.fetchone())["records"] == 1
    artifact_cursor = await db.execute(
        "select count(*) as count, count(distinct location) as locations "
        "from catalogue.job_artifacts where job_id = %s",
        (job_id,),
    )
    assert await artifact_cursor.fetchone() == {"count": 1, "locations": 1}


@pytest.mark.parametrize(
    ("scraper", "source_options", "refresh_mode"),
    [
        ("shopify", {}, "full"),
        ("shopify", {}, "price"),
        ("woocommerce", {"store_categories": ["glazes", "missing"]}, "full"),
        ("bigcommerce", {}, "full"),
        (
            "prestashop",
            {"sitemaps": ["https://shop.test/product-sitemap.xml"]},
            "full",
        ),
        (
            "sio2",
            {
                "scope": "materials",
                "category_urls": [
                    "https://shop.test/gb/66-low-fire-ceramic-clays"
                ],
                "use_advertised_sitemaps": False,
                "product_pattern": r"^/gb/[a-z0-9-]+/[0-9]+-[^/]+\.html$",
                "card_links_only": True,
                "brand": "SIO-2",
                "vat_status": "inclusive",
            },
            "full",
        ),
        (
            "wix",
            {"sitemaps": ["https://shop.test/store-products-sitemap.xml"]},
            "full",
        ),
        *(
            (
                name,
                {
                    "category_urls": ["https://shop.test/category"],
                    "use_advertised_sitemaps": False,
                    "product_pattern": r"/product/",
                    **({"render": False} if name == "nitrosell" else {}),
                },
                "full",
            )
            for name in ("shopware", "starweb", "nitrosell")
        ),
        ("sumup", {}, "full"),
        (
            "pagecrawl",
            {
                "sitemaps": ["https://shop.test/products.xml"],
                "product_pattern": r"/product/",
            },
            "full",
        ),
        (
            "axner",
            {
                "category_url": "https://shop.test/sitemap.aspx",
                "render": False,
                "currency": "USD",
            },
            "full",
        ),
        (
            "keramik_kraft",
            {
                "category_paths": ["de/Glasuren.html"],
                "vat_rate": 0.19,
            },
            "full",
        ),
        (
            "ceramicolours",
            {"category_ids": ["5101"]},
            "full",
        ),
    ],
)
async def test_native_worker_uses_library_lineage_and_terminal_recovery(
    db,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    scraper: str,
    source_options: dict[str, Any],
    refresh_mode: Literal["full", "price"],
) -> None:
    from mb_ceramics_catalogue.ops import commerce_scraper_runtime as runtime_module
    from mb_ceramics_catalogue.ops import worker as worker_module

    sources = SourcesFile.model_validate(
        {
            "shop": {
                "label": "Shop",
                "url": "https://shop.test/",
                "scraper": scraper,
                **source_options,
            }
        }
    )
    run_id = await runs.create_run(db)
    assert run_id is not None
    job_id = (await runs.create_jobs(db, run_id, sources, ["shop"]))["shop"]
    claimed = ClaimedJob(
        id=job_id,
        run_id=run_id,
        source_id="shop",
        host="shop.test",
        attempt=1,
        max_attempts=3,
        requires=[],
        requires_any=[],
        params={},
        proxy_snapshot={"policy": "never"},
        delivery_generation=1,
        execution_token=uuid4(),
    )
    if scraper == "sio2":
        await db.execute(
            "update catalogue.jobs set state = 'running', execution_token = %s "
            "where id = %s",
            (claimed.execution_token, job_id),
        )
    worker = Worker(
        cast(DictPool, Pool(db)),
        sources,
        Settings(
            dsn=os.environ["CATALOGUE_TEST_DSN"],
            dumps_dir=tmp_path,
            cache_dir=tmp_path / "cache",
        ),
    )
    worker._cancels[job_id] = asyncio.Event()
    backend = FakeTransport()
    browser_documents: dict[str, str] = {}
    if scraper == "shopify":
        backend.add("https://shop.test/meta.json", json_body={"currency": "EUR"})
        backend.add("https://shop.test/products.json", json_body={"products": []})
        expected_urls = [
            "https://shop.test/meta.json",
            "https://shop.test/products.json",
        ]
    elif scraper == "woocommerce":
        backend.add(
            "https://shop.test/wp-json/wc/store/v1/products/categories",
            json_body=[{"id": 3, "slug": "glazes"}],
        )
        backend.add(
            "https://shop.test/wp-json/wc/store/v1/products",
            json_body=[
                {
                    "id": 10,
                    "type": "simple",
                    "name": "Glaze",
                    "permalink": "https://shop.test/product/glaze",
                    "prices": {
                        "price": "1250",
                        "regular_price": "1250",
                        "currency_code": "EUR",
                        "currency_minor_unit": 2,
                    },
                    "is_in_stock": True,
                }
            ],
        )
        expected_urls = [
            "https://shop.test/wp-json/wc/store/v1/products/categories",
            "https://shop.test/wp-json/wc/store/v1/products",
        ]
    elif scraper == "bigcommerce":
        for _ in range(4):
            backend.add("https://shop.test/", status=403)
        expected_urls = ["https://shop.test/"] * 4
    elif scraper == "prestashop":
        product_url = "https://shop.test/12-stoneware-glaze.html"
        backend.add(
            "https://shop.test/product-sitemap.xml",
            body=f"<urlset><url><loc>{product_url}</loc></url></urlset>",
        )
        details = html.escape(
            json.dumps(
                {
                    "id_product": 12,
                    "id_product_attribute": 0,
                    "name": "Stoneware Glaze",
                    "link": product_url,
                    "reference": "GL-12",
                    "price": "12,50 EUR",
                    "quantity": 4,
                    "category_name": "Glazes",
                    "features": [],
                    "images": [],
                    "attachments": [],
                }
            ),
            quote=True,
        )
        backend.add(
            product_url,
            body=f'<html><div id="product-details" data-product="{details}"></div></html>',
        )
        expected_urls = [
            "https://shop.test/product-sitemap.xml",
            product_url,
        ]
    elif scraper == "sio2":
        category_url = "https://shop.test/gb/66-low-fire-ceramic-clays"
        product_url = (
            "https://shop.test/gb/low-fire-ceramic-clays/12-red-clay.html"
        )
        backend.add(
            category_url,
            body=(
                '<html><article class="product-miniature">'
                f'<a href="{product_url}">Red clay</a>'
                "</article></html>"
            ),
        )
        details = html.escape(
            json.dumps(
                {
                    "id_product": 12,
                    "id_product_attribute": 0,
                    "name": "Red Clay",
                    "link": product_url,
                    "reference": "CLAY-12",
                    "price": "12,50 EUR",
                    "quantity": 4,
                    "category_name": "Low fire ceramic clays",
                    "features": [],
                    "images": [],
                    "attachments": [],
                }
            ),
            quote=True,
        )
        backend.add(
            product_url,
            body=f'<html><div id="product-details" data-product="{details}"></div></html>',
        )
        expected_urls = [category_url, product_url]
    elif scraper == "wix":
        sitemap_url = "https://shop.test/store-products-sitemap.xml"
        product_url = "https://shop.test/product-page/glaze"
        backend.add(
            sitemap_url,
            body=f"<urlset><url><loc>{product_url}</loc></url></urlset>",
        )
        backend.add(product_url, body="<html><div id='root'></div></html>")
        product = {
            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "name": "Transparent Glaze",
            "brand": "Test Ceramics",
            "sku": "GL-500",
            "price": 12.5,
            "formattedPrice": "€12.50",
            "isInStock": True,
            "inventory": {"quantity": 3, "status": "in_stock"},
            "media": [],
            "productItems": [],
        }
        browser_documents[product_url] = (
            f'<script>window.warmup={{"glaze":{{"product":{json.dumps(product)}}}}};'
            "</script><script>const locale={\"currency\":\"EUR\"};</script>"
        )
        expected_urls = [sitemap_url, product_url]
    elif scraper in {"shopware", "starweb", "nitrosell"}:
        category_url = "https://shop.test/category"
        product_url = "https://shop.test/product/clay"
        backend.add(
            category_url,
            body=f'<html><a href="{product_url}">Clay</a></html>',
        )
        if scraper == "nitrosell":
            product_document = (
                '<html><head><meta property="og:title" content="Blue glaze">'
                '<meta property="og:upc" content="BG-1">'
                '<meta property="product:price:amount" content="14.25">'
                '<meta property="product:price:currency" content="EUR">'
                '<meta property="og:availability" content="instock"></head>'
                '<div class="product-description">Blue glaze</div></html>'
            )
        else:
            product_document = (
                '<html><script type="application/ld+json">'
                '{"@type":"Product","name":"Clay","sku":"CL-1",'
                '"offers":{"price":"12.50","priceCurrency":"EUR",'
                '"availability":"InStock"}}</script></html>'
            )
        backend.add(product_url, body=product_document)
        expected_urls = [category_url, product_url]
    elif scraper == "sumup":
        sitemap_url = "https://shop.test/sitemap.products.xml"
        product_url = "https://shop.test/article/tasse-bleue"
        backend.add(
            sitemap_url,
            body=f"<urlset><url><loc>{product_url}</loc></url></urlset>",
        )
        backend.add(product_url, body=_sumup_document())
        expected_urls = [sitemap_url, product_url]
    elif scraper == "axner":
        index_url = "https://shop.test/sitemap.aspx"
        category_url = "https://shop.test/clay.aspx"
        product_url = "https://shop.test/blue-clay.aspx"
        backend.add(index_url, body='<a href="/clay.aspx">Clay</a>')
        backend.add(
            category_url,
            body=(
                '<h5 class="product-list-link">'
                f'<a href="{product_url}">Blue clay</a></h5>'
            ),
        )
        backend.add(
            product_url,
            body=(
                '<h1>Blue Stoneware</h1>'
                '<span class="product-list-cost-value">$ 12.50</span>'
                '<span class="prod-detail-man-name-value">Axner</span>'
                '<span class="prod-detail-part-label">Axner Number:</span>'
                '<span class="prod-detail-part-value">AX-1</span>'
                '<span class="prod-detail-part-label">Cone:</span>'
                '<span class="prod-detail-part-value">6</span>'
                '<a href="/docs/sds.pdf">Safety data sheet</a>'
            ),
        )
        expected_urls = [index_url, category_url, product_url]
    elif scraper == "keramik_kraft":
        category_url = "https://shop.test/de/Glasuren.html"
        backend.add(
            category_url,
            body=(
                '<div class="product card">'
                '<p class="text-sm">Mayco Blue Glaze<br>Bt. á 0,25 kg</p>'
                '<p class="p mb-1">MAY-1</p>'
                '<span>4,97 € <i>(4,18 € HT)</i></span>'
                '<a href="Mayco-Blue_MAY-1.html">detail</a>'
                '<img src="/blue.jpg"><!-- /product'
            ),
        )
        expected_urls = [category_url]
    elif scraper == "ceramicolours":
        category_url = "https://shop.test/Articoli.php?Id=5101"
        listing_one = f"{category_url}&page=1"
        listing_two = f"{category_url}&page=2"
        product_url = "https://shop.test/Articolo.php?cod=GL-1"
        backend.add(
            "https://shop.test/",
            body='<a href="Articoli.php?Id=5101">Glazes</a>',
        )
        backend.add(
            listing_one,
            body=(
                f'<a href="{product_url}" class="product-name">Glaze</a>'
            ),
        )
        backend.add(listing_two, body="")
        backend.add(
            product_url,
            body=(
                "<h1>Blue Glaze</h1>"
                "<p>Prezzo:</span> € 8,40</p>"
                '<input id="icaOrdinabile" value="10">'
                '<select id="product-pack-field"></select>'
            ),
        )
        expected_urls = [
            "https://shop.test/",
            listing_one,
            listing_two,
            product_url,
        ]
    else:
        sitemap_url = "https://shop.test/products.xml"
        product_url = "https://shop.test/product/stoneware-clay"
        backend.add(
            sitemap_url,
            body=f"<urlset><url><loc>{product_url}</loc></url></urlset>",
        )
        backend.add(
            product_url,
            body=(
                '<html><script type="application/ld+json">'
                '{"@type":"Product","name":"Stoneware Clay","sku":"CL-2",'
                '"offers":{"price":"9.50","priceCurrency":"EUR",'
                '"availability":"InStock"}}</script></html>'
            ),
        )
        expected_urls = [sitemap_url, product_url]
    browser_session = NativeBrowserSession(
        _bigcommerce_token(), documents=browser_documents
    )
    browser_backend = NativeBrowserBackend(browser_session)
    browser_resolutions = 0
    native_runtimes = 0
    native_proxy_resolutions = 0

    original_resolver = worker_module.resolve_native_proxy_runtime

    def resolve_native_proxy(*args: Any, **kwargs: Any) -> Any:
        nonlocal native_proxy_resolutions
        native_proxy_resolutions += 1
        return original_resolver(*args, **kwargs)

    def native_builder(**kwargs: Any) -> CommerceScraper:
        nonlocal native_runtimes
        native_runtimes += 1
        return CommerceScraper(
            registry=kwargs["registry"],
            transport=backend,
            fetch_policy=kwargs["fetch_policy"],
            cache=kwargs["cache"],
            telemetry=kwargs["telemetry"],
            retries=kwargs["retries"],
            browser_transport=kwargs["browser_transport"],
            owns_browser_transport=kwargs["owns_browser_transport"],
        )

    async def legacy_proxy_forbidden(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AssertionError("native library canary must not acquire a legacy lease")

    def select_native_browser(
        *args: Any, **kwargs: Any
    ) -> tuple[NativeBrowserBackend, BrowserJobContext] | tuple[None, None]:
        nonlocal browser_resolutions
        del args, kwargs
        if scraper not in {
            "bigcommerce",
            "wix",
            "shopware",
            "starweb",
            "sumup",
            "pagecrawl",
            "keramik_kraft",
            "ceramicolours",
        }:
            raise AssertionError("non-browser canary must not resolve a browser")
        browser_resolutions += 1
        return browser_backend, BrowserJobContext(str(job_id))

    @asynccontextmanager
    async def legacy_session_forbidden(*args: Any, **kwargs: Any):
        del args, kwargs
        raise AssertionError("native library canary must not open a legacy session")
        yield

    finished: list[tuple[str, dict[str, Any]]] = []

    async def finish(claimed_job: ClaimedJob, state: str, **kwargs: Any) -> None:
        assert claimed_job.id == job_id
        finished.append((state, kwargs))

    monkeypatch.setattr(runtime_module, "build_http_scraper", native_builder)
    monkeypatch.setattr(
        worker_module, "resolve_native_proxy_runtime", resolve_native_proxy
    )
    monkeypatch.setattr(worker_module, "open_session", legacy_session_forbidden)
    monkeypatch.setattr(worker, "_proxy_lease", legacy_proxy_forbidden)
    monkeypatch.setattr(worker, "_browser_for_job", select_native_browser)
    monkeypatch.setattr(worker, "_finish", finish)
    datasets: tuple[DatasetSelection, ...] = (
        (
            "commerce.price_observation.v1",
            "commerce.stock_observation.v1",
        )
        if scraper == "ceramicolours"
        else ("ceramics.catalogue_item.v2",)
        if scraper == "sio2"
        else ("commerce.price_observation.v1",)
    )
    params = CrawlParams(
        pipeline="connector_canary",
        datasets=datasets,
        refresh_mode=refresh_mode,
    )

    await worker._crawl_connector_canary(claimed, params, sources["shop"])
    await worker._crawl_connector_canary(claimed, params, sources["shop"])

    assert native_runtimes == 1
    assert native_proxy_resolutions == 1
    browser_capable = {
        "bigcommerce",
        "wix",
        "shopware",
        "starweb",
        "sumup",
        "pagecrawl",
        "keramik_kraft",
        "ceramicolours",
    }
    assert browser_resolutions == (1 if scraper in browser_capable else 0)
    assert [request.url for request in backend.requests] == expected_urls
    assert [state for state, _ in finished] == ["succeeded", "succeeded"]
    first_summary = finished[0][1]["summary"]
    second_summary = finished[1][1]["summary"]
    assert first_summary["runtime_format"] == "commerce-scraper-v1"
    assert first_summary["refresh_mode"] == refresh_mode
    assert first_summary["terminal_recovery"] is False
    assert first_summary["transport"]["direct_requests"] == len(expected_urls)
    assert first_summary["transport"]["browser_requests"] == (
        2
        if scraper == "bigcommerce"
        else 1
        if scraper in {"wix", "ceramicolours"}
        else 0
    )
    assert second_summary["terminal_recovery"] is True
    assert not any(second_summary["transport"].values())
    lineages = await db.execute(
        "select runtime_format, status, connector_configuration "
        "from catalogue.job_checkpoint_lineages "
        "where job_id = %s",
        (job_id,),
    )
    lineage_rows = await lineages.fetchall()
    assert lineage_rows == [
        {
            "runtime_format": "commerce-scraper-v1",
            "status": "completed",
            "connector_configuration": {
                "partitions": [
                    "glazes"
                    if scraper == "woocommerce"
                    else "sitemap:0:d30089dfe4db"
                    if scraper == "prestashop"
                    else "category:0:9bc2b475c709"
                    if scraper == "sio2"
                    else "category"
                    if scraper in {"shopware", "starweb", "nitrosell"}
                    else "sitemap"
                    if scraper in {"sumup", "pagecrawl"}
                    else "main"
                ]
            },
        }
    ]
    pages = await db.execute(
        "select count(*) as count from catalogue.job_pages where job_id = %s",
        (job_id,),
    )
    assert (await pages.fetchone())["count"] == 1
    outcomes = await db.execute(
        "select coalesce(sum(records), 0) as records "
        "from catalogue.job_page_dataset_outcomes where job_id = %s",
        (job_id,),
    )
    expected_records = (
        0
        if scraper == "shopify"
        else 4
        if scraper == "ceramicolours"
        else 2
        if scraper == "sumup"
        else 1
    )
    assert (await outcomes.fetchone())["records"] == expected_records
    if scraper == "sio2":
        projected = await db.execute(
            "select family, attributes->>'material_kind' as material_kind "
            "from catalogue.source_products where source_id = %s",
            ("shop",),
        )
        assert await projected.fetchall() == [
            {"family": "clay_body", "material_kind": "low-fire-ceramic-clays"}
        ]
    if scraper == "bigcommerce":
        assert browser_backend.jobs == [BrowserJobContext(str(job_id))]
        assert browser_backend.shutdowns == 0
        assert browser_session.closes == 1
        browser_gets = [
            request for request in browser_session.requests if request["method"] == "GET"
        ]
        assert len(browser_session.rendered) + len(browser_gets) == 1
        assert (browser_session.rendered or [browser_gets[0]["endpoint"]]) == [
            "https://shop.test/"
        ]
        assert len(browser_session.requests) in {1, 2}
        browser_request = browser_session.requests[-1]
        assert browser_request["page_url"] == "https://shop.test/"
        assert browser_request["endpoint"] == "https://shop.test/graphql"
        assert browser_request["method"] == "POST"
        assert browser_request["headers"]["authorization"].startswith("Bearer ")
        assert browser_request["json_body"]["variables"] == {
            "after": None,
            "first": 50,
        }
        assert browser_session.token not in repr(finished)
        assert browser_session.token not in repr(lineage_rows)
    if scraper == "wix":
        assert browser_backend.jobs == [BrowserJobContext(str(job_id))]
        assert browser_backend.shutdowns == 0
        assert browser_session.closes == 1
        assert browser_session.rendered == ["https://shop.test/product-page/glaze"]
        assert browser_session.requests == []
    if scraper == "ceramicolours":
        assert browser_backend.jobs == [BrowserJobContext(str(job_id))]
        assert browser_backend.shutdowns == 0
        assert browser_session.closes == 1
        assert len(browser_session.evaluations) == 1
        evaluation = browser_session.evaluations[0]
        assert evaluation[0] == "https://shop.test/Articolo.php?cod=GL-1"
        assert evaluation[2:] == (1500, "#product-pack-field")
        assert browser_session.requests == []
    if scraper in {
        "shopware",
        "starweb",
        "sumup",
        "pagecrawl",
        "keramik_kraft",
    }:
        assert browser_backend.jobs == []
        assert browser_backend.shutdowns == 0
        assert browser_session.closes == 0
