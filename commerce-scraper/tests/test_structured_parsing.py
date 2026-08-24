from decimal import Decimal

import pytest

from mb_commerce_scraper.parsing._structured import (
    breadcrumbs,
    decimal_amount,
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
