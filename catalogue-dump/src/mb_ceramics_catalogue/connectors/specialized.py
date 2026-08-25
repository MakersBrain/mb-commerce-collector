"""Specialized page-commerce connectors with shared bounded discovery."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from decimal import Decimal
from html import unescape as html_unescape
from typing import Any, Literal
from urllib.parse import unquote, urljoin, urlparse

from mb_commerce_scraper.models import (
    Availability,
    CategoryRef,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    Evidence,
    MediaRef,
    Money,
    StockQuantityKind,
    StockState,
)
from pydantic import ConfigDict

from .page import balanced_object, clean, meta
from .pagecommerce import (
    PageCommerceConnector,
    PageCrawlOptions,
    PageParseOutcome,
    ParserDisposition,
)


class ShopwareOptions(PageCrawlOptions):
    model_config = ConfigDict(extra="forbid", frozen=True)
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld", "microdata", "opengraph"
    )
    stock_from_quantity_maximum: bool = True


class StarwebOptions(PageCrawlOptions):
    model_config = ConfigDict(extra="forbid", frozen=True)
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld", "opengraph"
    )


class NitroSellOptions(PageCrawlOptions):
    model_config = ConfigDict(extra="forbid", frozen=True)
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "opengraph",
    )


class SumUpOptions(PageCrawlOptions):
    model_config = ConfigDict(extra="forbid", frozen=True)
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = ("jsonld",)


class ShopwareConnector(PageCommerceConnector):
    name = "shopware"
    platform = "shopware"
    version = "1"

    def parse(self, document: str, url: str, source_id: str, observed_at: datetime) -> PageParseOutcome:
        outcome = super().parse(document, url, source_id, observed_at)
        unit = re.search(
            r'<[^>]+class=["\'][^"\']*product-detail-price-unit[^"\']*["\'][^>]*>(.*?)</',
            document, re.I | re.S,
        )
        number = re.search(
            r'(?:product-detail-ordernumber|Artikel-?Nr\.?|Bestellnummer)[^>]*>\s*([A-Za-z0-9][\w.\-/]*)',
            document, re.I,
        )
        if not outcome.snapshots:
            return outcome
        snapshots = []
        for snapshot in outcome.snapshots:
            variants = []
            for variant in snapshot.variants:
                attributes = dict(variant.published_attributes)
                if unit and clean(unit.group(1)):
                    attributes["published_unit_price"] = clean(unit.group(1))
                stock = variant.stock
                if stock is not None and stock.quantity_kind == StockQuantityKind.ORDER_LIMIT:
                    stock = stock.model_copy(update={"quantity_kind": StockQuantityKind.EXACT})
                variants.append(variant.model_copy(update={
                    "sku": variant.sku or (clean(number.group(1)) if number else None),
                    "published_attributes": attributes,
                    "stock": stock,
                }))
            snapshots.append(snapshot.model_copy(update={"variants": tuple(variants)}))
        return outcome.model_copy(update={"snapshots": tuple(snapshots)})


class StarwebConnector(PageCommerceConnector):
    name = "starweb"
    platform = "starweb"
    version = "1"

    def parse(self, document: str, url: str, source_id: str, observed_at: datetime) -> PageParseOutcome:
        outcome = super().parse(document, url, source_id, observed_at)
        vat = "inclusive" if re.search(r'class=["\'][^"\']*\bincl-vat\b', document, re.I) else (
            "exclusive" if re.search(r'class=["\'][^"\']*\bexcl-vat\b', document, re.I) else None
        )
        if vat is None or not outcome.snapshots:
            return outcome
        snapshots = []
        attributes = {
            clean(match.group(1)).rstrip(":"): clean(match.group(2))
            for match in re.finditer(
                r'<(?:label|span)[^>]*class=["\'][^"\']*(?:variant|attribute)-name[^"\']*["\'][^>]*>(.*?)</(?:label|span)>\s*'
                r'<(?:span|div)[^>]*class=["\'][^"\']*(?:variant|attribute)-value[^"\']*["\'][^>]*>(.*?)</(?:span|div)>',
                document, re.I | re.S,
            )
            if clean(match.group(1)) and clean(match.group(2))
        }
        for snapshot in outcome.snapshots:
            variants = []
            for variant in snapshot.variants:
                offers = tuple(offer.model_copy(update={"vat_status": vat}) for offer in variant.offers)
                published = {**variant.published_attributes, **attributes, "vat_basis": "page_markup"}
                variants.append(variant.model_copy(update={
                    "offers": offers, "published_attributes": published
                }))
            snapshots.append(snapshot.model_copy(update={"variants": tuple(variants)}))
        return outcome.model_copy(update={"snapshots": tuple(snapshots)})


class NitroSellConnector(PageCommerceConnector):
    name = "nitrosell"
    platform = "nitrosell"
    version = "1"

    @staticmethod
    def _legacy_meta(document: str, key: str) -> str | None:
        escaped = re.escape(key)
        for pattern in (
            rf'<meta[^>]+(?:property|name)=["\']{escaped}["\'][^>]+content=["\']([^"\']*)',
            rf'<meta[^>]+content=["\']([^"\']*)["\'][^>]+(?:property|name)=["\']{escaped}["\']',
        ):
            if match := re.search(pattern, document, re.I):
                return html_unescape(match.group(1)).strip() or None
        return None

    def parse(self, document: str, url: str, source_id: str, observed_at: datetime) -> PageParseOutcome:
        outcome = super().parse(document, url, source_id, observed_at)
        if not outcome.snapshots:
            return outcome
        description_match = re.search(
            r'<div[^>]*class="[^"]*product-description[^"]*"[^>]*>(.*?)</div>',
            document, re.I | re.S,
        )
        breadcrumb = re.search(r'<ol class="breadcrumb">(.*?)</ol>', document, re.I | re.S)
        categories = []
        if breadcrumb:
            crumbs = [clean(value) for value in re.findall(
                r'<li[^>]*>\s*(?:<a[^>]*>)?(.*?)(?:</a>)?\s*</li>', breadcrumb.group(1), re.I | re.S
            )]
            categories = [CategoryRef(name=value) for value in crumbs[1:-1] if value]
        images = list(outcome.snapshots[0].images)
        for value in re.findall(
            r'https://cdn\.powered-by-nitrosell\.com/product_images/[^"\'\s]+', document, re.I
        ):
            if "/thumb" not in value and value not in {item.url for item in images}:
                images.append(MediaRef(url=urljoin(url, value)))
        list_match = re.search(r'class="text-pricestrike"[^>]*>([^<]+)<', document, re.I)
        snapshot = outcome.snapshots[0]
        variants = []
        for variant in snapshot.variants:
            offers = list(variant.offers)
            if list_match and offers:
                parsed = re.search(r"(\d+(?:[.,]\d+)?)", clean(list_match.group(1)))
                if parsed:
                    token = parsed.group(1)
                    amount = Decimal(
                        token.replace(".", "").replace(",", "")
                        if re.fullmatch(r"\d+[.,]\d{3}", token)
                        else token.replace(",", ".")
                    )
                    sale = offers[0].model_copy(update={"role": "sale"})
                    regular = offers[0].model_copy(update={
                        "role": "regular",
                        "price": Money(amount=amount, currency=offers[0].price.currency),
                    })
                    offers = [sale, regular]
            variants.append(variant.model_copy(update={"offers": tuple(offers)}))
        snapshot = snapshot.model_copy(update={
            "description": clean(description_match.group(1)) if description_match else snapshot.description,
            "categories": tuple(categories) if categories else snapshot.categories,
            "images": tuple(images), "variants": tuple(variants),
            "platform_extensions": {
                **snapshot.platform_extensions,
                "raw": {
                    "og": {
                        key: self._legacy_meta(document, key)
                        for key in (
                            "og:title",
                            "og:brand",
                            "og:upc",
                            "og:availability",
                            "product:price:amount",
                            "product:price:currency",
                        )
                    }
                },
            },
        })
        return outcome.model_copy(update={"snapshots": (snapshot,)})


_FLIGHT_CHUNK = re.compile(r"self\.__next_f\.push\((\[.*?\])\)</script>", re.S)
_PRODUCT_MARKER = re.compile(r'"product":\{"id":"[0-9a-f-]{8}-')
_CURRENCY = re.compile(r'"currency":"([A-Z]{3})"')


def _flight_payload(document: str) -> str:
    parts: list[str] = []
    for raw in _FLIGHT_CHUNK.findall(document):
        try:
            chunk = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if len(chunk) > 1 and isinstance(chunk[1], str):
            parts.append(chunk[1])
    return "".join(parts)


class SumUpConnector(PageCommerceConnector):
    name = "sumup"
    platform = "sumup"
    version = "1"

    def parse(self, document: str, url: str, source_id: str, observed_at: datetime) -> PageParseOutcome:
        payload = _flight_payload(document)
        product = self._product(payload, url) if payload else None
        if product is None:
            return super().parse(document, url, source_id, observed_at)
        title = clean(product.get("name")) or meta(document, "og:title")
        if not title:
            return PageParseOutcome(
                disposition=ParserDisposition.UNSUPPORTED,
                parsers_tried=("sumup_rsc",), reason="SumUp product has no title"
            )
        currency_match = _CURRENCY.search(payload)
        currency = currency_match.group(1) if currency_match else self.options.currency
        evidence = Evidence(
            method="html", source_url=url, source_field="next_rsc", observed_at=observed_at
        )
        variants: list[CommerceVariant] = []
        raw_variants = product.get("variants")
        candidates = [
            {
                "uuid": value.get("uuid") or key, "name": value.get("name"),
                "sku": value.get("sku"), "price": value.get("price", product.get("price")),
                "basePrice": value.get("basePrice", product.get("basePrice")),
                "hasDiscount": value.get("hasDiscount", product.get("hasDiscount")),
                "options": value.get("options"), "quantity": value.get("quantity"),
                "isAvailable": value.get("isAvailable", product.get("isAvailable", True)),
                "isTrackingEnabled": value.get("isTrackingEnabled"),
            }
            for key, value in raw_variants.items()
            if isinstance(value, dict) and value
        ] if isinstance(raw_variants, dict) else []
        if not candidates:
            candidates = [{
                "uuid": None, "name": None, "sku": product.get("sku"),
                "price": product.get("price"), "basePrice": product.get("basePrice"),
                "hasDiscount": product.get("hasDiscount"), "options": None,
                "quantity": None, "isAvailable": product.get("isAvailable", True),
                "isTrackingEnabled": product.get("isTrackingEnabled"),
            }]
        for candidate in candidates:
            amount = self._amount(candidate.get("price", product.get("price")))
            if amount is None or currency is None:
                continue
            available = Availability.IN_STOCK if candidate.get("isAvailable", True) else Availability.OUT_OF_STOCK
            quantity = self._quantity(candidate, product)
            stock = StockState(
                availability=available, quantity=quantity,
                quantity_kind=(StockQuantityKind.EXACT if quantity is not None else StockQuantityKind.UNKNOWN),
                observed_at=observed_at, evidence=(evidence,),
            )
            current_offer = CommerceOffer(
                price=Money(amount=amount, currency=currency), observed_at=observed_at,
                evidence=(evidence,),
                role="sale" if candidate.get("hasDiscount", product.get("hasDiscount")) else "regular",
                vat_status=self.options.vat_status or "unknown",
                vat_rate=self.options.vat_rate, availability=available,
                availability_evidence=(evidence,),
            )
            offers = [current_offer]
            base_amount = self._amount(candidate.get("basePrice", product.get("basePrice")))
            if current_offer.role == "sale" and base_amount is not None and base_amount != amount:
                offers.append(current_offer.model_copy(update={
                    "role": "regular", "price": Money(amount=base_amount, currency=currency)
                }))
            variants.append(CommerceVariant(
                external_id=str(candidate.get("uuid") or product.get("id")),
                canonical_url=url, title=clean(candidate.get("name")) or None,
                sku=clean(candidate.get("sku") or product.get("sku")) or None,
                options=self._options(candidate),
                offers=tuple(offers), stock=stock,
                platform_extensions={"legacy_raw_variant": candidate},
            ))
        if not variants:
            return PageParseOutcome(
                disposition=ParserDisposition.UNSUPPORTED,
                parsers_tried=("sumup_rsc",), reason="SumUp product has no priced variants"
            )
        images = tuple(MediaRef(url=value) for value in dict.fromkeys(
            clean(value) for value in (product.get("allImages") or [product.get("image")]) if clean(value)
        ))
        category = clean((product.get("category") or {}).get("name"))
        snapshot = CommerceProductSnapshot(
            connector=self.name, source_id=source_id,
            external_id=str(product.get("id") or hashlib.sha256(url.encode()).hexdigest()[:24]),
            canonical_url=url, title=title, observed_at=observed_at,
            description=clean(product.get("description")) or None,
            vendor=self.options.brand,
            categories=(CategoryRef(name=category),) if category else (), images=images,
            variants=tuple(variants), platform_extensions={
                "legacy_raw_product": {key: value for key, value in product.items() if key != "variants"}
            },
        )
        return PageParseOutcome(
            disposition=ParserDisposition.PARSED, snapshots=(snapshot,), parsers_tried=("sumup_rsc",)
        )

    @staticmethod
    def _amount(value: Any) -> Decimal | None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return Decimal(str(value)) / Decimal(100)

    @staticmethod
    def _quantity(variant: dict[str, Any], product: dict[str, Any]) -> int | None:
        if variant.get("isAvailable") is False:
            return 0
        tracking = variant.get("isTrackingEnabled", product.get("isTrackingEnabled"))
        quantity = variant.get("quantity")
        return quantity if tracking is True and isinstance(quantity, int) and not isinstance(quantity, bool) and quantity >= 0 else None

    @staticmethod
    def _options(variant: dict[str, Any]) -> dict[str, str]:
        options = variant.get("options")
        if not isinstance(options, list):
            return {}
        attributes: dict[str, str] = {}
        for index, option in enumerate(options):
            if isinstance(option, dict):
                key = clean(option.get("name") or option.get("label"))
                value = clean(option.get("value") or option.get("choice"))
                if key and value:
                    attributes[key] = value
            elif isinstance(option, str) and (value := clean(option)):
                attributes[f"option_{index + 1}"] = value
        return attributes

    @staticmethod
    def _product(payload: str, url: str) -> dict[str, Any] | None:
        slug = unquote(urlparse(url).path.rstrip("/").rsplit("/", 1)[-1])
        detailed: list[dict[str, Any]] = []
        for match in _PRODUCT_MARKER.finditer(payload):
            product = balanced_object(payload, match.start() + len('"product":'))
            if product is None:
                continue
            if slug and product.get("slug") == slug:
                return product
            variants = product.get("variants")
            if isinstance(variants, dict) and any(isinstance(value, dict) and value for value in variants.values()):
                detailed.append(product)
        return detailed[0] if len(detailed) == 1 else None
