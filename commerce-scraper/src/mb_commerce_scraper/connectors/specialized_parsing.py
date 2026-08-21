"""Dependency-free, declarative HTML helpers for specialized page connectors."""

from __future__ import annotations

import html
import json
import re
from typing import Any
from urllib.parse import urljoin

from pydantic import BaseModel, ConfigDict, Field, model_validator


def clean(value: Any) -> str:
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", str(value or ""))).split())


class DomFieldSelector(BaseModel):
    """One safe CSS-like selector; no script or arbitrary selector execution."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    selector: str = Field(min_length=1)
    attribute: str | None = None

    @model_validator(mode="after")
    def supported(self) -> DomFieldSelector:
        if not re.fullmatch(
            r"(?:[A-Za-z][\w-]*)?(?:#[\w-]+|\.[\w-]+|\[[\w:-]+(?:=[\"']?[\w:./ -]+[\"']?)?\])?",
            self.selector,
        ):
            raise ValueError("DOM rules support one tag/id/class/attribute selector only")
        if self.attribute is not None and not re.fullmatch(r"[\w:-]+", self.attribute):
            raise ValueError("DOM rule attribute is invalid")
        return self


class VerifiedDomRules(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    verification: tuple[DomFieldSelector, ...] = Field(min_length=1)
    name: DomFieldSelector
    price: DomFieldSelector | None = None
    currency: DomFieldSelector | None = None
    description: DomFieldSelector | None = None
    sku: DomFieldSelector | None = None
    image: DomFieldSelector | None = None
    availability: DomFieldSelector | None = None


def select(document: str, rule: DomFieldSelector) -> str | None:
    parsed = _selector(rule.selector)
    if parsed is None:
        return None
    tag, wanted_id, wanted_class, wanted_attr, wanted_value = parsed
    tag_pattern = tag or r"[A-Za-z][\w-]*"
    for match in re.finditer(
        rf"<(?P<tag>{tag_pattern})\b(?P<attrs>(?:[^>\"']|\"[^\"]*\"|'[^']*')*)>",
        document,
        re.IGNORECASE | re.DOTALL,
    ):
        attrs = _attributes(match.group("attrs"))
        if wanted_id and attrs.get("id") != wanted_id:
            continue
        if wanted_class and wanted_class not in (attrs.get("class") or "").split():
            continue
        if wanted_attr and wanted_attr not in attrs:
            continue
        if wanted_value is not None and attrs.get(wanted_attr or "") != wanted_value:
            continue
        if rule.attribute:
            return clean(attrs.get(rule.attribute)) or None
        if match.group("tag").casefold() in _VOID_TAGS:
            return clean(attrs.get("content") or attrs.get("src") or attrs.get("value")) or None
        close = re.search(
            rf"</{re.escape(match.group('tag'))}\s*>",
            document[match.end() :],
            re.IGNORECASE,
        )
        end = match.end() + close.start() if close else len(document)
        return clean(document[match.end() : end]) or None
    return None


def microdata_products(document: str) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    marker = re.compile(
        r'itemtype\s*=\s*["\']\s*https?://schema\.org/Product["\']', re.IGNORECASE
    )
    opening_tag = re.compile(
        r"<\s*([A-Za-z][A-Za-z0-9]*)((?:[^>\"']|\"[^\"]*\"|'[^']*')*)>",
        re.DOTALL,
    )
    for match in marker.finditer(document):
        start = document.rfind("<", 0, match.start())
        opening = opening_tag.match(document, start) if start >= 0 else None
        if opening is None:
            continue
        scope = _scope(document, start, opening.group(1))
        item = _microdata_read(scope[opening.end() - start :])
        if item:
            item.setdefault("@type", "Product")
            found.append(item)
    return found


def jsonld_products(document: str) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def walk(value: Any) -> None:
        if isinstance(value, list):
            for child in value:
                walk(child)
        elif isinstance(value, dict):
            kinds = value.get("@type")
            if kinds == "Product" or (isinstance(kinds, list) and "Product" in kinds):
                found.append(value)
            for child in value.values():
                walk(child)

    for body in re.findall(
        r'<script\b[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        document,
        re.IGNORECASE | re.DOTALL,
    ):
        try:
            walk(json.loads(body))
        except json.JSONDecodeError:
            continue
    return found


def probable_javascript_shell(document: str) -> bool:
    lower = document.casefold()
    if "<html" not in lower and "<!doctype" not in lower:
        return False
    explicit = any(
        marker in lower
        for marker in (
            "enable javascript",
            "javascript is required",
            "requires javascript",
            'id="__next"',
            "id='__next'",
            'id="root"',
            "id='root'",
            'id="app"',
            "id='app'",
            "ng-version=",
        )
    )
    visible = re.sub(
        r"<script\b[\s\S]*?</script>|<style\b[\s\S]*?</style>|<[^>]+>", " ", lower
    )
    return explicit and "<script" in lower and len(clean(visible)) < 1000


def specification_table(document: str) -> dict[str, str]:
    attributes: dict[str, str] = {}
    pairs = (
        r"<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>",
        r"<tr[^>]*>\s*<(?:th|td)[^>]*>(.*?)</(?:th|td)>\s*<td[^>]*>(.*?)</td>\s*</tr>",
    )
    for pattern in pairs:
        for match in re.finditer(pattern, document, re.IGNORECASE | re.DOTALL):
            name, value = clean(match.group(1)).rstrip(":"), clean(match.group(2))
            if name and value and len(name) < 100:
                attributes.setdefault(name, value)
    return attributes


def pdf_links(document: str, page_url: str) -> tuple[tuple[str, str], ...]:
    found: list[tuple[str, str]] = []
    for match in re.finditer(
        r'<a[^>]*href=["\']([^"\']+\.pdf(?:\?[^"\']*)?)["\'][^>]*>(.*?)</a>',
        document,
        re.IGNORECASE | re.DOTALL,
    ):
        value = (urljoin(page_url, html.unescape(match.group(1))), clean(match.group(2)))
        if value not in found:
            found.append(value)
    return tuple(found)


def opengraph_product(document: str) -> dict[str, Any] | None:
    title = meta(document, "og:title")
    price = meta(document, "product:price:amount")
    if not title or not price:
        return None
    return {
        "@type": "Product",
        "name": title,
        "description": meta(document, "og:description"),
        "brand": meta(document, "og:brand"),
        "sku": meta(document, "og:upc") or meta(document, "product:retailer_item_id"),
        "image": meta(document, "og:image"),
        "offers": {
            "price": price,
            "priceCurrency": meta(document, "product:price:currency"),
            "availability": meta(document, "og:availability"),
        },
    }


def dom_product(
    document: str, rules: VerifiedDomRules, default_currency: str | None
) -> dict[str, Any] | None:
    if not all(select(document, rule) for rule in rules.verification):
        return None
    name = select(document, rules.name)
    if not name:
        return None
    return {
        "@type": "Product",
        "name": name,
        "description": select(document, rules.description) if rules.description else None,
        "sku": select(document, rules.sku) if rules.sku else None,
        "image": select(document, rules.image) if rules.image else None,
        "offers": {
            "price": select(document, rules.price) if rules.price else None,
            "priceCurrency": (
                select(document, rules.currency) if rules.currency else default_currency
            ),
            "availability": (
                select(document, rules.availability) if rules.availability else None
            ),
        },
    }


def meta(document: str, key: str) -> str:
    escaped = re.escape(key)
    for pattern in (
        rf'<meta[^>]*(?:property|name)=["\']{escaped}["\'][^>]*content=["\']([^"\']*)',
        rf'<meta[^>]*content=["\']([^"\']*)["\'][^>]*(?:property|name)=["\']{escaped}["\']',
    ):
        match = re.search(pattern, document, re.IGNORECASE)
        if match:
            return clean(match.group(1))
    return ""


_VOID_TAGS = {"meta", "link", "img", "br", "hr", "input", "source", "area", "base", "col"}


def _scope(document: str, start: int, tag: str) -> str:
    if tag.casefold() in _VOID_TAGS:
        end = document.find(">", start)
        return document[start : end + 1 if end != -1 else len(document)]
    depth = 0
    for match in re.finditer(
        rf"<\s*(/?)\s*{re.escape(tag)}\b[^>]*>", document[start:], re.IGNORECASE
    ):
        depth += -1 if match.group(1) else 1
        if depth <= 0:
            return document[start : start + match.end()]
    return document[start:]


def _microdata_read(fragment: str) -> dict[str, Any]:
    item: dict[str, Any] = {}
    skip_to = 0
    itemprop = re.compile(r'itemprop\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)
    opening_tag = re.compile(
        r"<\s*([A-Za-z][A-Za-z0-9]*)((?:[^>\"']|\"[^\"]*\"|'[^']*')*)>",
        re.DOTALL,
    )
    for match in itemprop.finditer(fragment):
        if match.start() < skip_to:
            continue
        start = fragment.rfind("<", 0, match.start())
        opening = opening_tag.match(fragment, start) if start >= 0 else None
        if opening is None:
            continue
        tag, attributes = opening.group(1), opening.group(2)
        name = match.group(1).split()[0] if match.group(1).strip() else ""
        if not name:
            continue
        if "itemscope" in attributes.casefold() or re.search(
            r"itemtype\s*=", attributes, re.IGNORECASE
        ):
            nested = _scope(fragment, start, tag)
            skip_to = start + len(nested)
            value: Any = _microdata_read(nested[opening.end() - start :])
        else:
            value = _microdata_value(tag, attributes, fragment, start, opening.end())
        if value not in (None, "", {}):
            _assign(item, name, value)
    return item


def _microdata_value(
    tag: str, attributes: str, fragment: str, start: int, after_open: int
) -> str | None:
    attrs = _attributes(attributes)
    for attribute in ("content", "datetime"):
        if value := attrs.get(attribute):
            return value
    lowered = tag.casefold()
    if lowered in {"img", "source", "audio", "video", "embed"}:
        return attrs.get("src") or attrs.get("content")
    if lowered in {"a", "link", "area"} and attrs.get("href"):
        return attrs["href"]
    if lowered in _VOID_TAGS:
        return None
    return clean(_scope(fragment, start, tag)[after_open - start :]) or None


def _assign(item: dict[str, Any], name: str, value: Any) -> None:
    if name not in item:
        item[name] = [value] if name in {"image", "additionalProperty"} else value
    elif isinstance(item[name], list):
        if value not in item[name]:
            item[name].append(value)
    elif name in {"image", "additionalProperty"} and item[name] != value:
        item[name] = [item[name], value]


def _attributes(raw: str) -> dict[str, str]:
    return {
        match.group(1).casefold(): html.unescape(
            match.group(3) or match.group(4) or match.group(5) or ""
        )
        for match in re.finditer(
            r"([\w:-]+)\s*=\s*(\"([^\"]*)\"|'([^']*)'|([^\s>]+))", raw
        )
    }


def _selector(
    value: str,
) -> tuple[str | None, str | None, str | None, str | None, str | None] | None:
    match = re.fullmatch(
        r"(?P<tag>[A-Za-z][\w-]*)?(?:#(?P<id>[\w-]+)|\.(?P<class>[\w-]+)|"
        r"\[(?P<attr>[\w:-]+)(?:=[\"']?(?P<value>[\w:./ -]+)[\"']?)?\])?",
        value,
    )
    if not match:
        return None
    return (
        match.group("tag"), match.group("id"), match.group("class"),
        match.group("attr"), match.group("value"),
    )
