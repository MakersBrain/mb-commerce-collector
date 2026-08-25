"""Dependency-free, declarative HTML helpers for specialized page connectors."""

from __future__ import annotations

import hashlib
import html
import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urljoin, urlsplit

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


@dataclass(frozen=True, slots=True)
class _DomOpeningTag:
    name: str
    attributes: dict[str, str]
    content_start: int


_DOM_OPENING_TAG = re.compile(
    r"<(?P<tag>[A-Za-z][\w-]*)\b(?P<attrs>(?:[^>\"']|\"[^\"]*\"|'[^']*')*)>",
    re.IGNORECASE | re.DOTALL,
)


def _dom_tokens(document: str) -> tuple[_DomOpeningTag, ...]:
    return tuple(
        _DomOpeningTag(
            name=match.group("tag"),
            attributes=_attributes(match.group("attrs")),
            content_start=match.end(),
        )
        for match in _DOM_OPENING_TAG.finditer(document)
    )


def select(document: str, rule: DomFieldSelector) -> str | None:
    return _select(document, _dom_tokens(document), rule)


def _select(
    document: str,
    tokens: tuple[_DomOpeningTag, ...],
    rule: DomFieldSelector,
) -> str | None:
    parsed = _selector(rule.selector)
    if parsed is None:
        return None
    tag, wanted_id, wanted_class, wanted_attr, wanted_value = parsed
    for token in tokens:
        if tag and token.name.casefold() != tag.casefold():
            continue
        attrs = token.attributes
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
        if token.name.casefold() in _VOID_TAGS:
            return clean(attrs.get("content") or attrs.get("src") or attrs.get("value")) or None
        close = re.search(
            rf"</{re.escape(token.name)}\s*>",
            document[token.content_start :],
            re.IGNORECASE,
        )
        end = token.content_start + close.start() if close else len(document)
        return clean(document[token.content_start : end]) or None
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


def jsonld_blocks(document: str) -> list[dict[str, Any]]:
    """Return top-level JSON-LD objects, flattening lists and ``@graph`` values."""
    found: list[dict[str, Any]] = []

    def flatten(value: Any) -> None:
        if isinstance(value, list):
            for child in value:
                flatten(child)
        elif isinstance(value, dict):
            if "@graph" in value:
                flatten(value["@graph"])
            else:
                found.append(value)

    for body in re.findall(
        r'<script\b[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        document,
        re.IGNORECASE | re.DOTALL,
    ):
        try:
            flatten(json.loads(html.unescape(body.strip())))
        except (json.JSONDecodeError, TypeError):
            continue
    return found


def jsonld_has_type(item: dict[str, Any], wanted: str) -> bool:
    value = item.get("@type")
    values = value if isinstance(value, list) else [value]
    return any(str(candidate).casefold() == wanted.casefold() for candidate in values)


def jsonld_product_blocks(document: str) -> list[dict[str, Any]]:
    """Return product objects from the top-level JSON-LD graph."""
    return [item for item in jsonld_blocks(document) if jsonld_has_type(item, "Product")]


def breadcrumbs(document: str) -> list[str]:
    for item in jsonld_blocks(document):
        if not jsonld_has_type(item, "BreadcrumbList"):
            continue
        names: list[str] = []
        for element in item.get("itemListElement") or []:
            if not isinstance(element, dict):
                continue
            entry = element.get("item")
            name = entry.get("name") if isinstance(entry, dict) else element.get("name")
            if cleaned := clean(name):
                names.append(cleaned)
        if names:
            return names
    return []


def jsonld_images(item: dict[str, Any], page_url: str) -> list[str]:
    value = item.get("image")
    values = value if isinstance(value, list) else [value]
    found: list[str] = []
    for entry in values:
        if isinstance(entry, dict):
            entry = entry.get("url") or entry.get("contentUrl")
        if cleaned := clean(entry):
            found.append(urljoin(page_url, cleaned))
    return list(dict.fromkeys(found))


def jsonld_brand(item: dict[str, Any]) -> str | None:
    value = item.get("brand")
    if isinstance(value, dict):
        value = value.get("name")
    return clean(value) or None


def jsonld_gtin(item: dict[str, Any]) -> str | None:
    for key in ("gtin13", "gtin14", "gtin12", "gtin8", "gtin", "ean"):
        if value := clean(item.get(key)):
            return value
    return None


def decimal_amount(value: Any) -> Decimal | None:
    """Parse a finite, non-negative decimal without treating booleans as numbers."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return amount if amount.is_finite() and amount >= 0 else None


def origin_of(
    url: str,
    *,
    require_http: bool = False,
    error_message: str = "URL must be an absolute HTTP(S) URL",
) -> str:
    parsed = urlsplit(url)
    if require_http and (parsed.scheme not in {"http", "https"} or not parsed.netloc):
        raise ValueError(error_message)
    return f"{parsed.scheme}://{parsed.netloc}"


def stable_digest(value: str, length: int = 12) -> str:
    """Return the bounded digest used by deterministic connector page IDs."""
    return hashlib.sha256(value.encode()).hexdigest()[:length]


def hashed_page_id(
    prefix: str,
    identity: str,
    *,
    digest_length: int = 12,
    separator: str = ":",
) -> str:
    return f"{prefix}{separator}{stable_digest(identity, digest_length)}"


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
    if not explicit or "<script" not in lower:
        return False
    visible = re.sub(
        r"<script\b[\s\S]*?</script>|<style\b[\s\S]*?</style>|<[^>]+>", " ", lower
    )
    return len(clean(visible)) < 1000


def specification_table(document: str) -> dict[str, str]:
    attributes: dict[str, str] = {}
    pairs = (
        r"<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>",
        r"<tr[^>]*>\s*<t[hd][^>]*>(.*?)</t[hd]>\s*<t[hd][^>]*>(.*?)</t[hd]>\s*</tr>",
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
    tokens = _dom_tokens(document)

    def selected(rule: DomFieldSelector | None) -> str | None:
        return _select(document, tokens, rule) if rule is not None else None

    if not all(selected(rule) for rule in rules.verification):
        return None
    name = selected(rules.name)
    if not name:
        return None
    return {
        "@type": "Product",
        "name": name,
        "description": selected(rules.description),
        "sku": selected(rules.sku),
        "image": selected(rules.image),
        "offers": {
            "price": selected(rules.price),
            "priceCurrency": (
                selected(rules.currency) if rules.currency else default_currency
            ),
            "availability": selected(rules.availability),
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
