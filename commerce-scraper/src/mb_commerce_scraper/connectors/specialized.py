"""Specialized sitemap/page connectors for common hosted commerce frameworks."""

from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Generic, Literal, TypeVar, cast
from urllib.parse import unquote, urljoin, urlparse

from pydantic import BaseModel, JsonValue

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
from mb_commerce_scraper.parsing._structured import (
    clean,
    meta,
)
from mb_commerce_scraper.transports import (
    CommerceTransport,
)

from .base import (
    ConnectorContext,
)
from .factory import ConnectorPlan, SimpleConnectorFactory, validated_options
from .page_engine import PageEngineConnector, PageEngineOptions, page_engine_plan


class ShopwareOptions(PageEngineOptions):
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld",
        "microdata",
        "opengraph",
    )
    stock_from_quantity_maximum: bool = True


class StarwebOptions(PageEngineOptions):
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld",
        "opengraph",
    )


class NitroSellOptions(PageEngineOptions):
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "opengraph",
    )


class SumUpOptions(PageEngineOptions):
    parsers: tuple[Literal["jsonld", "microdata", "opengraph", "dom"], ...] = (
        "jsonld",
    )


class ShopwareConnector(PageEngineConnector):
    name = "shopware"
    platform = "shopware"

    def __init__(
        self,
        transport: CommerceTransport,
        options: ShopwareOptions | None = None,
        context: ConnectorContext | None = None,
    ) -> None:
        super().__init__(transport, options or ShopwareOptions(), context)

    def parse(
        self, document: str, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        snapshots = super().parse(document, url, source_id)
        unit = _class_content(document, "product-detail-price-unit")
        number = re.search(
            r"(?:product-detail-ordernumber|Artikel-?Nr\.?|Bestellnummer)[^>]*>\s*"
            r"([A-Za-z0-9][\w.\-/]*)",
            document,
            re.IGNORECASE,
        )
        quantity = re.search(
            r"<(?:input|select)[^>]*(?:name=[\"']quantity[\"']|class=[\"'][^\"']*quantity-selector)"
            r"[^>]*\bmax=[\"']?(\d+)",
            document,
            re.IGNORECASE,
        )
        attributes = _definition_attributes(document)
        output: list[CommerceProductSnapshot] = []
        for snapshot in snapshots:
            variants: list[CommerceVariant] = []
            for variant in snapshot.variants:
                published = dict(variant.published_attributes)
                published.update(attributes)
                if unit:
                    published["published_unit_price"] = unit
                stock = variant.stock
                if quantity and cast(ShopwareOptions, self.options).stock_from_quantity_maximum:
                    assert stock is not None
                    stock = stock.model_copy(
                        update={
                            "quantity": int(quantity.group(1)),
                            "quantity_kind": StockQuantityKind.EXACT,
                        }
                    )
                variants.append(
                    variant.model_copy(
                        update={
                            "sku": variant.sku or (clean(number.group(1)) if number else None),
                            "published_attributes": published,
                            "stock": stock,
                        }
                    )
                )
            output.append(snapshot.model_copy(update={"variants": tuple(variants)}))
        return tuple(output)


class StarwebConnector(PageEngineConnector):
    name = "starweb"
    platform = "starweb"

    def __init__(
        self,
        transport: CommerceTransport,
        options: StarwebOptions | None = None,
        context: ConnectorContext | None = None,
    ) -> None:
        super().__init__(transport, options or StarwebOptions(), context)

    def parse(
        self, document: str, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        snapshots = super().parse(document, url, source_id)
        vat = (
            "inclusive"
            if re.search(r'class=["\'][^"\']*\bincl-vat\b', document, re.IGNORECASE)
            else (
                "exclusive"
                if re.search(r'class=["\'][^"\']*\bexcl-vat\b', document, re.IGNORECASE)
                else None
            )
        )
        attributes = {
            clean(match.group(1)).rstrip(":"): clean(match.group(2))
            for match in re.finditer(
                r'<(?:label|span)[^>]*class=["\'][^"\']*(?:variant|attribute)-name[^"\']*["\'][^>]*>(.*?)</(?:label|span)>\s*'
                r'<(?:span|div)[^>]*class=["\'][^"\']*(?:variant|attribute)-value[^"\']*["\'][^>]*>(.*?)</(?:span|div)>',
                document,
                re.IGNORECASE | re.DOTALL,
            )
            if clean(match.group(1)) and clean(match.group(2))
        }
        output: list[CommerceProductSnapshot] = []
        for snapshot in snapshots:
            variants = []
            for variant in snapshot.variants:
                offers = tuple(
                    offer.model_copy(update={"vat_status": vat}) if vat else offer
                    for offer in variant.offers
                )
                published = {**variant.published_attributes, **attributes}
                if vat:
                    published["vat_basis"] = "page_markup"
                variants.append(
                    variant.model_copy(
                        update={"offers": offers, "published_attributes": published}
                    )
                )
            output.append(snapshot.model_copy(update={"variants": tuple(variants)}))
        return tuple(output)


class NitroSellConnector(PageEngineConnector):
    name = "nitrosell"
    platform = "nitrosell"

    def __init__(
        self,
        transport: CommerceTransport,
        options: NitroSellOptions | None = None,
        context: ConnectorContext | None = None,
    ) -> None:
        super().__init__(transport, options or NitroSellOptions(), context)

    def parse(
        self, document: str, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        snapshots = super().parse(document, url, source_id)
        if not snapshots:
            return ()
        list_price = _decimal(_class_content(document, "text-pricestrike"))
        description = _class_content(document, "product-description")
        breadcrumb = re.search(
            r'<ol[^>]*class=["\'][^"\']*breadcrumb[^"\']*["\'][^>]*>(.*?)</ol>',
            document,
            re.IGNORECASE | re.DOTALL,
        )
        categories: tuple[CategoryRef, ...] = ()
        if breadcrumb:
            crumbs = tuple(
                clean(value)
                for value in re.findall(
                    r"<li[^>]*>(.*?)</li>", breadcrumb.group(1), re.IGNORECASE | re.DOTALL
                )
                if clean(value)
            )
            categories = tuple(CategoryRef(name=value) for value in crumbs[1:-1])
        image_values = [meta(document, "og:image")]
        image_values.extend(
            re.findall(
                r'https://cdn\.powered-by-nitrosell\.com/product_images/[^"\'\s<>]+',
                document,
                re.IGNORECASE,
            )
        )
        images = tuple(
            MediaRef(url=urljoin(url, value))
            for value in dict.fromkeys(value for value in image_values if value and "/thumb" not in value)
        )
        output: list[CommerceProductSnapshot] = []
        for snapshot in snapshots:
            variants = []
            for variant in snapshot.variants:
                offers = list(variant.offers)
                if list_price is not None and offers and list_price != offers[0].price.amount:
                    current = offers[0].model_copy(update={"role": "sale"})
                    offers = [
                        current,
                        current.model_copy(
                            update={
                                "role": "regular",
                                "price": Money(
                                    amount=list_price,
                                    currency=current.price.currency,
                                ),
                            }
                        ),
                    ]
                variants.append(variant.model_copy(update={"offers": tuple(offers)}))
            output.append(
                snapshot.model_copy(
                    update={
                        "description": description or snapshot.description,
                        "categories": categories or snapshot.categories,
                        "images": images or snapshot.images,
                        "variants": tuple(variants),
                    }
                )
            )
        return tuple(output)


_FLIGHT_CHUNK = re.compile(r"self\.__next_f\.push\((\[.*?\])\)</script>", re.DOTALL)
_PRODUCT_MARKER = re.compile(r'"product":\{"id":"[0-9a-f-]{8}-')
_CURRENCY = re.compile(r'"currency":"([A-Z]{3})"')


class SumUpConnector(PageEngineConnector):
    name = "sumup"
    platform = "sumup"

    def __init__(
        self,
        transport: CommerceTransport,
        options: SumUpOptions | None = None,
        context: ConnectorContext | None = None,
    ) -> None:
        super().__init__(transport, options or SumUpOptions(), context)

    def parse(
        self, document: str, url: str, source_id: str
    ) -> tuple[CommerceProductSnapshot, ...]:
        payload = _flight_payload(document)
        product = self._product(payload, url) if payload else None
        if product is None:
            return super().parse(document, url, source_id)
        title = clean(product.get("name")) or meta(document, "og:title")
        currency_match = _CURRENCY.search(payload)
        currency = currency_match.group(1) if currency_match else self.options.currency
        if not title or not currency:
            return ()
        observed_at = self.context.clock()
        evidence = Evidence(
            method="html",
            source_url=url,
            source_field="next_rsc",
            observed_at=observed_at,
        )
        raw_variants = product.get("variants")
        candidates = (
            [
                {
                    **value,
                    "uuid": value.get("uuid") or key,
                    "name": value.get("name"),
                    "sku": value.get("sku"),
                    "price": value.get("price", product.get("price")),
                    "basePrice": value.get("basePrice", product.get("basePrice")),
                    "hasDiscount": value.get("hasDiscount", product.get("hasDiscount")),
                    "isAvailable": value.get(
                        "isAvailable", product.get("isAvailable", True)
                    ),
                    "options": value.get("options"),
                }
                for key, value in raw_variants.items()
                if isinstance(value, dict) and value
            ]
            if isinstance(raw_variants, dict)
            else []
        )
        if not candidates:
            candidates = [{**product, "uuid": product.get("id")}]
        variants: list[CommerceVariant] = []
        for candidate in candidates:
            amount = self._amount(candidate.get("price", product.get("price")))
            if amount is None:
                continue
            available = (
                Availability.IN_STOCK
                if candidate.get("isAvailable", True)
                else Availability.OUT_OF_STOCK
            )
            quantity = self._quantity(candidate, product)
            stock = StockState(
                availability=available,
                quantity=quantity,
                quantity_kind=(
                    StockQuantityKind.EXACT
                    if quantity is not None
                    else StockQuantityKind.UNKNOWN
                ),
                observed_at=observed_at,
                evidence=(evidence,),
            )
            role: Literal["regular", "sale"] = (
                "sale"
                if candidate.get("hasDiscount", product.get("hasDiscount"))
                else "regular"
            )
            current = CommerceOffer(
                price=Money(amount=amount, currency=currency),
                observed_at=observed_at,
                evidence=(evidence,),
                role=role,
                vat_status=self.options.vat_status or "unknown",
                vat_rate=self.options.vat_rate,
                availability=available,
                availability_evidence=(evidence,),
            )
            offers = [current]
            base_amount = self._amount(
                candidate.get("basePrice", product.get("basePrice"))
            )
            if role == "sale" and base_amount is not None and base_amount != amount:
                offers.append(
                    current.model_copy(
                        update={
                            "role": "regular",
                            "price": Money(amount=base_amount, currency=currency),
                        }
                    )
                )
            variants.append(
                CommerceVariant(
                    external_id=str(candidate.get("uuid") or product.get("id")),
                    canonical_url=url,
                    title=clean(candidate.get("name")) or None,
                    sku=clean(candidate.get("sku") or product.get("sku")) or None,
                    offers=tuple(offers),
                    stock=stock,
                    platform_extensions={"legacy_raw_variant": candidate},
                )
            )
        if not variants:
            return ()
        images = tuple(
            MediaRef(url=value)
            for value in dict.fromkeys(
                clean(value)
                for value in (product.get("allImages") or [product.get("image")])
                if clean(value)
            )
        )
        category_raw = product.get("category")
        category = clean(category_raw.get("name")) if isinstance(category_raw, dict) else ""
        return (
            CommerceProductSnapshot(
                connector=self.name,
                source_id=source_id,
                external_id=str(
                    product.get("id") or hashlib.sha256(url.encode()).hexdigest()[:24]
                ),
                canonical_url=url,
                title=title,
                observed_at=observed_at,
                description=clean(product.get("description")) or None,
                vendor=self.options.brand,
                categories=(CategoryRef(name=category),) if category else (),
                images=images,
                variants=tuple(variants),
                platform_extensions={
                    "legacy_raw_product": {
                        key: value for key, value in product.items() if key != "variants"
                    }
                },
            ),
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
        if (
            tracking is True
            and isinstance(quantity, int)
            and not isinstance(quantity, bool)
            and quantity >= 0
        ):
            return quantity
        return None

    @staticmethod
    def _product(payload: str, url: str) -> dict[str, Any] | None:
        slug = unquote(urlparse(url).path.rstrip("/").rsplit("/", 1)[-1])
        detailed: list[dict[str, Any]] = []
        for match in _PRODUCT_MARKER.finditer(payload):
            product = _balanced_object(payload, match.start() + len('"product":'))
            if product is None:
                continue
            if slug and product.get("slug") == slug:
                return product
            variants = product.get("variants")
            if isinstance(variants, dict) and any(
                isinstance(value, dict) and value for value in variants.values()
            ):
                detailed.append(product)
        return detailed[0] if len(detailed) == 1 else None


PageOptionsT = TypeVar("PageOptionsT", bound=PageEngineOptions)
PageConnectorT = TypeVar("PageConnectorT", bound=PageEngineConnector)


class _SpecializedPageFactory(
    SimpleConnectorFactory[PageOptionsT, PageConnectorT],
    Generic[PageOptionsT, PageConnectorT],
):
    def plan(
        self,
        options: BaseModel,
        *,
        base_url: str,
        request_partitions: tuple[str, ...] = (),
    ) -> ConnectorPlan:
        del base_url, request_partitions
        return page_engine_plan(
            validated_options(options, self.options_model, factory_name=self.name)
        )


class ShopwareFactory(_SpecializedPageFactory[ShopwareOptions, ShopwareConnector]):
    name = "shopware"
    version = ShopwareConnector.version
    options_model = ShopwareOptions
    connector_type = ShopwareConnector


class StarwebFactory(_SpecializedPageFactory[StarwebOptions, StarwebConnector]):
    name = "starweb"
    version = StarwebConnector.version
    options_model = StarwebOptions
    connector_type = StarwebConnector


class NitroSellFactory(_SpecializedPageFactory[NitroSellOptions, NitroSellConnector]):
    name = "nitrosell"
    version = NitroSellConnector.version
    options_model = NitroSellOptions
    connector_type = NitroSellConnector


class SumUpFactory(_SpecializedPageFactory[SumUpOptions, SumUpConnector]):
    name = "sumup"
    version = SumUpConnector.version
    options_model = SumUpOptions
    connector_type = SumUpConnector

def _class_content(document: str, class_name: str) -> str:
    match = re.search(
        rf'<[^>]+class=["\'][^"\']*\b{re.escape(class_name)}\b[^"\']*["\'][^>]*>(.*?)</[^>]+>',
        document,
        re.IGNORECASE | re.DOTALL,
    )
    return clean(match.group(1)) if match else ""


def _definition_attributes(document: str) -> dict[str, JsonValue]:
    return {
        clean(match.group(1)).rstrip(":"): clean(match.group(2))
        for match in re.finditer(
            r'<dt[^>]*class=["\'][^"\']*properties-label[^"\']*["\'][^>]*>(.*?)</dt>\s*'
            r'<dd[^>]*class=["\'][^"\']*properties-value[^"\']*["\'][^>]*>(.*?)</dd>',
            document,
            re.IGNORECASE | re.DOTALL,
        )
        if clean(match.group(1)) and clean(match.group(2))
    }


def _decimal(value: str) -> Decimal | None:
    match = re.search(r"\d+(?:[.,]\d+)?", value.replace(",", "."))
    if not match:
        return None
    try:
        return Decimal(match.group())
    except InvalidOperation:
        return None


def _availability(value: str) -> Availability:
    normalized = value.rsplit("/", 1)[-1].replace("_", "").casefold()
    return {
        "instock": Availability.IN_STOCK,
        "available": Availability.IN_STOCK,
        "outofstock": Availability.OUT_OF_STOCK,
        "soldout": Availability.OUT_OF_STOCK,
        "backorder": Availability.BACKORDER,
        "preorder": Availability.PREORDER,
    }.get(normalized, Availability.UNKNOWN)


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


def _balanced_object(payload: str, start: int) -> dict[str, Any] | None:
    decoder = json.JSONDecoder()
    try:
        value, _ = decoder.raw_decode(payload[start:])
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None
