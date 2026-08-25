import re
from collections.abc import Iterator
from decimal import Decimal

import pytest

import mb_commerce_scraper.parsing._structured as structured_module
from mb_commerce_scraper.parsing._structured import (
    DomFieldSelector,
    VerifiedDomRules,
    breadcrumbs,
    decimal_amount,
    dom_product,
    jsonld_brand,
    jsonld_gtin,
    jsonld_images,
    jsonld_product_blocks,
    meta,
    origin_of,
    probable_javascript_shell,
    specification_table,
)


def test_jsonld_product_helpers_share_graph_parsing() -> None:
    document = """
    <script type="application/ld+json">
      {"@graph": [
        {"@type": "product", "brand": {"name": "Acme &amp; Co"},
         "gtin13": "123", "image": ["/one.jpg", {"url": "/one.jpg"}]},
        {"@type": "BreadcrumbList", "itemListElement": [
          {"item": {"name": "Clay &amp; tools"}}, {"name": "Glazes"}
        ]}
      ]}
    </script>
    """

    product = jsonld_product_blocks(document)[0]
    assert jsonld_brand(product) == "Acme & Co"
    assert jsonld_gtin(product) == "123"
    assert jsonld_images(product, "https://shop.test/item") == ["https://shop.test/one.jpg"]
    assert breadcrumbs(document) == ["Clay & tools", "Glazes"]


def test_decimal_amount_rejects_boolean_and_non_finite_values() -> None:
    assert decimal_amount(True) is None
    assert decimal_amount("NaN") is None
    assert decimal_amount("-1") is None
    assert decimal_amount("12.30") == Decimal("12.30")


def test_origin_of_can_enforce_absolute_http_urls() -> None:
    assert origin_of("https://shop.test/catalogue?q=1") == "https://shop.test"
    with pytest.raises(ValueError, match="base URL"):
        origin_of("/relative", require_http=True, error_message="invalid base URL")


def test_structured_drift_choices_are_explicit() -> None:
    accepted = "a" * 99
    rejected = "b" * 100
    document = f"<dl><dt>{accepted}</dt><dd>yes</dd><dt>{rejected}</dt><dd>no</dd></dl>"
    assert specification_table(document) == {accepted: "yes"}
    assert meta("<html></html>", "og:title") == ""
    assert probable_javascript_shell(
        '<html><body><div ng-version="17"></div><script src="app.js"></script></body></html>'
    )


@pytest.mark.parametrize(
    ("document", "rule", "expected"),
    (
        ('<H1 class="name featured">Clay &amp; Tools</H1>', "h1.name", "Clay & Tools"),
        ('<main id="product">Verified</main>', "#product", "Verified"),
        ('<span data-sku="A-1">SKU text</span>', "[data-sku]", "SKU text"),
        ('<span data-kind="primary">Chosen</span>', "[data-kind=primary]", "Chosen"),
        (
            '<meta class="currency" content="EUR">',
            "meta.currency",
            "EUR",
        ),
    ),
)
def test_dom_selector_tokenization_preserves_matching_semantics(
    document: str,
    rule: str,
    expected: str,
) -> None:
    assert structured_module.select(document, DomFieldSelector(selector=rule)) == expected


def test_dom_product_tokenizes_and_parses_attributes_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = """<main id="product" data-live="yes">
      <h1 class="name">DOM Clay</h1>
      <p class="description">Smooth &amp; plastic</p>
      <span class="sku" data-code="DOM-1">DOM-1</span>
      <img class="image" src="/clay.jpg">
      <span class="price">7.50</span>
      <meta class="currency" content="EUR">
      <link class="availability" content="InStock">
    </main>"""
    rules = VerifiedDomRules(
        verification=(
            DomFieldSelector(selector="#product"),
            DomFieldSelector(selector="[data-live=yes]"),
        ),
        name=DomFieldSelector(selector="h1.name"),
        description=DomFieldSelector(selector=".description"),
        sku=DomFieldSelector(selector="[data-code]", attribute="data-code"),
        image=DomFieldSelector(selector="img.image"),
        price=DomFieldSelector(selector=".price"),
        currency=DomFieldSelector(selector="meta.currency"),
        availability=DomFieldSelector(selector="link.availability"),
    )
    scans = 0
    attribute_parses = 0
    original_pattern = structured_module._DOM_OPENING_TAG
    original_attributes = structured_module._attributes

    class CountingPattern:
        def finditer(self, value: str) -> Iterator[re.Match[str]]:
            nonlocal scans
            scans += 1
            return original_pattern.finditer(value)

    def counted_attributes(raw: str) -> dict[str, str]:
        nonlocal attribute_parses
        attribute_parses += 1
        return original_attributes(raw)

    monkeypatch.setattr(structured_module, "_DOM_OPENING_TAG", CountingPattern())
    monkeypatch.setattr(structured_module, "_attributes", counted_attributes)

    product = dom_product(document, rules, "USD")

    assert product == {
        "@type": "Product",
        "name": "DOM Clay",
        "description": "Smooth & plastic",
        "sku": "DOM-1",
        "image": "/clay.jpg",
        "offers": {
            "price": "7.50",
            "priceCurrency": "EUR",
            "availability": "InStock",
        },
    }
    assert scans == 1
    assert attribute_parses == 8
