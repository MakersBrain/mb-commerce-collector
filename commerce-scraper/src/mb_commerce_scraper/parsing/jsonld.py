from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urljoin

from mb_commerce_scraper.models import (
    Availability,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    Evidence,
    MediaRef,
    Money,
    StockState,
)

SCRIPT = re.compile(r"<script\b[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)


def _walk(value: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if isinstance(value, dict):
        kinds = value.get("@type")
        if kinds == "Product" or (isinstance(kinds, list) and "Product" in kinds):
            found.append(value)
        for child in value.values():
            found.extend(_walk(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_walk(child))
    return found


class JsonLdProductParser:
    name = "jsonld"
    version = "1"

    def __init__(self, *, currency: str | None = None) -> None:
        self.currency = currency

    def parse(self, document: str, *, url: str, source_id: str) -> tuple[CommerceProductSnapshot, ...]:
        products: list[CommerceProductSnapshot] = []
        observed = datetime.now(UTC)
        for body in SCRIPT.findall(document):
            try:
                decoded = json.loads(body)
            except json.JSONDecodeError:
                continue
            for raw in _walk(decoded):
                name = str(raw.get("name") or "").strip()
                identifier = str(raw.get("sku") or raw.get("productID") or raw.get("@id") or url).strip()
                if not name or not identifier:
                    continue
                canonical_url = urljoin(url, str(raw.get("url") or url))
                evidence = Evidence(method="jsonld", source_url=url, observed_at=observed)
                offers_raw = raw.get("offers")
                offers_list = offers_raw if isinstance(offers_raw, list) else [offers_raw]
                offers: list[CommerceOffer] = []
                availability = Availability.UNKNOWN
                for offer in offers_list:
                    if not isinstance(offer, dict):
                        continue
                    availability = self._availability(offer.get("availability"))
                    try:
                        amount = Decimal(str(offer.get("price")))
                    except (InvalidOperation, ValueError):
                        continue
                    currency = str(offer.get("priceCurrency") or self.currency or "").upper()
                    if amount.is_finite() and amount >= 0 and len(currency) == 3:
                        offers.append(CommerceOffer(
                            price=Money(amount=amount, currency=currency), observed_at=observed,
                            evidence=(evidence,), availability=availability,
                            availability_evidence=(evidence,),
                        ))
                image_raw = raw.get("image")
                image_values = image_raw if isinstance(image_raw, list) else [image_raw]
                images = tuple(MediaRef(url=urljoin(url, str(value))) for value in image_values if isinstance(value, str) and value)
                products.append(CommerceProductSnapshot(
                    connector="generic-pages", source_id=source_id, external_id=identifier,
                    canonical_url=canonical_url, title=name, observed_at=observed,
                    description=str(raw.get("description") or "") or None,
                    vendor=self._brand(raw.get("brand")), images=images,
                    variants=(CommerceVariant(external_id=identifier, sku=str(raw.get("sku") or "") or None, offers=tuple(offers), stock=StockState(availability=availability, observed_at=observed, evidence=(evidence,))),),
                ))
        return tuple(products)

    @staticmethod
    def _brand(value: Any) -> str | None:
        if isinstance(value, dict):
            value = value.get("name")
        return str(value).strip() if value else None

    @staticmethod
    def _availability(value: Any) -> Availability:
        normalized = str(value or "").rsplit("/", 1)[-1].casefold()
        return {
            "instock": Availability.IN_STOCK,
            "outofstock": Availability.OUT_OF_STOCK,
            "backorder": Availability.BACKORDER,
            "preorder": Availability.PREORDER,
        }.get(normalized, Availability.UNKNOWN)
