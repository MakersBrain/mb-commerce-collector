"""Shopify storefronts, collected through the public products.json feed.

Shopify publishes the whole catalogue as JSON with every variant, option and
image already structured, so no page rendering is needed. Collections are read
first when the source declares a materials allowlist, which keeps the request
count proportional to the part of the shop we actually want.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlencode

import httpx

from mb_ceramics_catalogue.proxy import ProxyDenied

from . import domain
from . import record as record_module
from .base import Blocked, Scraper

PAGE_SIZE = 250
INVENTORY_PROXY_RESERVE = 1_000_000


class ShopifyScraper(Scraper):
    platform = "shopify"
    method = "api_json"
    currency: str | None = None
    #: Variants seen while the shop's currency was unknown. Counted rather than
    #: logged per row: it is one fact about the shop, not five thousand.
    _priceless: int = 0

    async def scrape(self, limit: int | None = None) -> Any:
        self._priceless = 0
        self._inventory_failures = 0
        await self._resolve_currency()
        collections = self.config.get("collections") or []
        if collections:
            await self._scrape_collections(collections, limit)
        else:
            await self._scrape_all(limit)
        if self._priceless:
            # Said once, and loudly enough to be the reason the job fails: a
            # shop whose currency could not be read has no usable prices at all,
            # and reporting that as an empty success is how it goes unnoticed.
            self.note(
                f"{self._priceless} variants dropped without a price: "
                "the shop's currency could not be read from meta.json"
            )
        if self._inventory_failures:
            self.note(
                f"inventory unavailable for {self._inventory_failures} product responses; "
                "those quantities remain unknown"
            )
        return self.result

    async def _resolve_currency(self) -> None:
        """products.json omits the currency, so read the shop's own meta.json."""
        self.currency = self.config.get("currency")
        if self.currency:
            return
        endpoint = f"{self.origin()}/meta.json"
        try:
            payload = await self.fetcher.json(endpoint)
            self.result.requests += 1
            self.currency = domain.clean(payload.get("currency")) or None
        except (httpx.HTTPError, Blocked, AttributeError) as error:
            self.note(f"shop currency unavailable from meta.json ({error})")
            # Currency is required to interpret every amount in products.json.
            # Record this as a failed enumeration, not merely a note: the worker
            # can then retry a transient 429/5xx and must treat any exhausted
            # zero-record attempt as adds-only instead of retiring the previous
            # catalogue against a meaningless empty artifact.
            self.fail(endpoint, error)
            self.currency = None

    async def _scrape_collections(self, collections: list[str], limit: int | None) -> None:
        for handle in collections:
            endpoint = f"{self.origin()}/collections/{handle}/products.json"
            await self._paginate(endpoint, limit, collection=handle)

    async def _scrape_all(self, limit: int | None) -> None:
        await self._paginate(f"{self.origin()}/products.json", limit)

    async def _paginate(self, endpoint: str, limit: int | None, collection: str | None = None) -> None:
        page = 1
        seen = 0
        max_pages = self.config.get("page_limit", 200)
        while page <= max_pages:
            try:
                payload = await self.fetcher.json(endpoint, params={"limit": PAGE_SIZE, "page": page})
                self.result.requests += 1
            except (httpx.HTTPError, Blocked) as error:
                # Not `fail`: everything after this page went unseen, and a
                # shop that 429s at page 8 of 14 must not read as complete.
                self.enumeration_failed(f"{endpoint}?page={page}", error)
                return
            products = payload.get("products") if isinstance(payload, dict) else None
            if not products:
                return
            self.result.discovered += len(products)
            selected = products
            if limit is not None:
                selected = products[:max(limit - seen, 0)]
            if self.config.get("inventory_product_json") or self.config.get("inventory_product_html"):
                # Product detail requests are the expensive part. Do not fetch
                # inventory for obviously out-of-scope feed entries (Art
                # Academy, for example, sells far more yarn than ceramics).
                await self._enrich_inventory([
                    product for product in selected
                    if self._inventory_candidate(product, collection)
                ])
            for product in selected:
                self._emit(product, collection)
                seen += 1
                if limit is not None and seen >= limit:
                    self.result.truncated = True
                    return
            if len(products) < PAGE_SIZE:
                return
            page += 1
        self.result.truncated = True

    async def _enrich_inventory(self, products: list[dict[str, Any]]) -> None:
        """Join exact variant inventory from shops whose compact JSON exposes it."""
        async def load(product: dict[str, Any]) -> None:
            handle = product.get("handle")
            if not handle:
                return
            suffix = "" if self.config.get("inventory_product_html") else ".js"
            endpoint = f"{self.origin()}/products/{handle}{suffix}"
            section_id = self.config.get("inventory_section_id")
            if not suffix and section_id:
                endpoint = f"{endpoint}?{urlencode({'section_id': section_id})}"
            try:
                # Do not let a storefront cookie make later inventory reads
                # stateful. Connections are deliberately reused inside the
                # bounded batch below: forcing a fresh TLS handshake for every
                # product made large catalogues needlessly slow. The whole
                # client is replaced between batches, before a Shopify theme's
                # observed pooled-session stall can escape that boundary.
                headers = {"Cookie": ""}
                if suffix:
                    detail = await self.fetcher.json(endpoint, headers=headers)
                else:
                    document = await self.fetcher.text(endpoint, headers=headers)
                    detail = {"variants": list(self._inventory_from_html(
                        document,
                        {str(variant.get("id")) for variant in product.get("variants") or []},
                    ).values())}
                self.result.requests += 1
            except (httpx.HTTPError, Blocked, ProxyDenied):
                self._inventory_failures += 1
                return
            by_id = {
                str(variant.get("id")): variant
                for variant in detail.get("variants") or []
                if isinstance(variant, dict) and variant.get("id")
            } if isinstance(detail, dict) else {}
            for variant in product.get("variants") or []:
                if isinstance(variant, dict) and (extra := by_id.get(str(variant.get("id")))):
                    for key in ("inventory_quantity", "inventory_management", "inventory_policy"):
                        if key in extra:
                            variant[key] = extra[key]

        # A few large themes silently park a storefront session after a burst:
        # no 429, just requests that stop completing while a new session works.
        # Stay below the observed window and rotate between bounded batches.
        # Ulster's theme has parked a session after only fifteen successful
        # reads, so keep the boundary below the smallest observed stall window.
        # Read a batch serially: firing ten simultaneous fallbacks at one
        # storefront made every request race through the direct 429 before the
        # circuit opened, then overloaded the shared residential proxy session.
        batch_size = 10
        for offset in range(0, len(products), batch_size):
            batch = products[offset:offset + batch_size]
            for index, product in enumerate(batch):
                remaining = getattr(self.fetcher, "proxy_bytes_remaining", None)
                if remaining is not None and remaining <= INVENTORY_PROXY_RESERVE:
                    # Product detail HTML is optional enrichment and much
                    # larger than products.json. Preserve enough of the same
                    # bounded reservation to discover every later feed page.
                    self._inventory_failures += len(products) - offset - index
                    return
                await load(product)
            if offset + batch_size < len(products):
                await self.fetcher.rotate_client()

    @staticmethod
    def _inventory_from_html(document: str, variant_ids: set[str]) -> dict[str, dict[str, Any]]:
        """Read the verified inventory shapes emitted by Shopify themes/apps."""
        found: dict[str, dict[str, Any]] = {}
        for identifier in variant_ids:
            escaped = re.escape(identifier)
            quantity: int | None = None

            native = re.search(
                rf'"id":{escaped}\b(?:(?!"id":\d).){{0,1800}}?'
                rf'"inventory_quantity":(-?\d+)(?:(?!"id":\d).){{0,500}}?'
                r'"inventory_management":"shopify"(?:(?!"id":\d).){0,300}?'
                r'"inventory_policy":"deny"',
                document, re.S,
            )
            if native:
                quantity = int(native.group(1))

            if quantity is None:
                option = re.search(rf'<option\b[^>]*\bvalue=["\']{escaped}["\'][^>]*>', document, re.I)
                if option and re.search(r'data-inventory-policy=["\']deny["\']', option.group(0), re.I):
                    match = re.search(r'data-inventory=["\'](-?\d+)["\']', option.group(0), re.I)
                    if match:
                        quantity = int(match.group(1))

            if quantity is None and re.search(
                rf'gwProductInventoryPolicy\[{escaped}\]\s*=\s*["\']deny["\']', document,
            ):
                match = re.search(
                    rf'gwProductInventoryQuantity\[{escaped}\]\s*=\s*["\'](-?\d+)["\']', document,
                )
                if match:
                    quantity = int(match.group(1))

            if quantity is None:
                info = re.search(
                    rf'variant_id\s*:\s*{escaped}\b(?:(?!variant_id\s*:).){{0,500}}?'
                    r'variant_inventory_policy\s*:\s*["\']deny["\']'
                    r'(?:(?!variant_id\s*:).){0,300}?variant_inventory_quantity\s*:\s*(-?\d+)',
                    document, re.S,
                )
                if info:
                    quantity = int(info.group(1))

            if quantity is None:
                local = re.search(
                    rf'\bid\s*:\s*{escaped}\b(?:(?!\bid\s*:).){{0,700}}?'
                    r'inventory_management\s*:\s*["\']shopify["\']'
                    r'(?:(?!\bid\s*:).){0,300}?\bquantity\s*:\s*(-?\d+)',
                    document, re.S,
                )
                if local:
                    quantity = int(local.group(1))

            if quantity is None:
                inventory = re.search(
                    rf'["\']{escaped}["\']\s*:\s*\{{'
                    r'(?:(?!["\']\d+["\']\s*:).){0,1000}?'
                    r'["\']inventory_management["\']\s*:\s*(?:null|["\']shopify["\'])'
                    r'(?:(?!["\']\d+["\']\s*:).){0,500}?'
                    r'["\']inventory_policy["\']\s*:\s*["\']deny["\']'
                    r'(?:(?!["\']\d+["\']\s*:).){0,500}?'
                    r'["\']inventory_quantity["\']\s*:\s*(-?\d+)',
                    document, re.S,
                )
                if inventory:
                    quantity = int(inventory.group(1))

            if quantity is not None:
                found[identifier] = {
                    "id": identifier,
                    "inventory_quantity": quantity,
                    "inventory_management": "published_theme",
                    "inventory_policy": "deny",
                }
        return found

    def _emit(self, product: dict[str, Any], collection: str | None) -> None:
        handle = product.get("handle")
        if not handle:
            return
        product_url = f"{self.origin()}/products/{handle}"
        description = domain.clean(product.get("body_html"))
        product_type = domain.clean(product.get("product_type"))
        tags = product.get("tags") or []
        tags = tags if isinstance(tags, list) else [domain.clean(tags)]
        category_path = [value for value in ([collection] if collection else []) + [product_type] if value]
        category_match = self._category_match(product, collection)

        images = [
            image.get("src") for image in product.get("images") or []
            if isinstance(image, dict) and image.get("src")
        ]
        documents = domain.documents(
            [(match, match) for match in re.findall(r'href=["\']([^"\']+\.pdf[^"\']*)', product.get("body_html") or "", re.I)],
            product_url,
        )
        variants = product.get("variants") or []
        for variant in variants:
            if not isinstance(variant, dict) or (variant.get("available") is None and variant.get("price") is None):
                continue
            price, currency = record_module.parse_price(variant.get("price"))
            if price is None:
                continue
            money = currency or self.currency
            if money is None:
                # products.json states an amount and never the unit it is in, so
                # without meta.json there is no currency to publish this price
                # in — and a price with no currency is not a weaker fact, it is
                # a meaningless one. `record.is_valid` says the same and drops
                # the row; what matters here is not emitting it as though the
                # number meant something, because the database refuses it and
                # the refusal used to cost the whole source's load.
                self._priceless += 1
                price = None
            compare_at, _ = record_module.parse_price(variant.get("compare_at_price"))
            variant_title = domain.clean(variant.get("title"))
            variant_title = "" if variant_title.casefold() == "default title" else variant_title
            name = f"{domain.clean(product.get('title'))} {variant_title}".strip()
            weight = self._weight(variant)
            row = record_module.build(
                source=self.name,
                product_url=product_url,
                variant_id=str(variant.get("id") or ""),
                name=name,
                product_name=product.get("title"),
                variant_title=variant_title or None,
                brand=product.get("vendor"),
                manufacturer_sku=self._manufacturer_sku(product, variant, variant_title),
                supplier_reference=domain.clean(variant.get("sku")) or None,
                description=description,
                category_path=category_path or None,
                image_url=(variant.get("featured_image") or {}).get("src") if isinstance(variant.get("featured_image"), dict) else (images[0] if images else None),
                all_image_urls=images or None,
                price=price,
                currency=money,
                price_text=f"{variant.get('price')} {money}".strip() if money else None,
                list_price=(
                    compare_at if money and compare_at and compare_at != price else None
                ),
                vat=self.config.get("vat_status"),
                availability=(
                    "https://schema.org/InStock" if variant.get("available")
                    else "https://schema.org/OutOfStock" if variant.get("available") is False
                    else None
                ),
                stock_quantity=self._stock_quantity(variant),
                gtin=domain.clean(variant.get("barcode")) or None,
                technical_attributes=self._options(product, variant) | weight,
                documents=documents or None,
                extraction_method=self.method,
                source_detail_level="api",
                source_updated_at=product.get("updated_at"),
                raw={"product": {k: v for k, v in product.items() if k != "variants"}, "variant": variant},
            )
            self.add(row, category_match)

    def _category_match(self, product: dict[str, Any], collection: str | None) -> bool | None:
        tags = product.get("tags") or []
        tags = tags if isinstance(tags, list) else [domain.clean(tags)]
        return self.category_allows(
            domain.clean(product.get("product_type")),
            " ".join(str(tag) for tag in tags),
            collection or "",
            domain.clean(product.get("handle")),
        )

    def _inventory_candidate(self, product: dict[str, Any], collection: str | None) -> bool:
        """Decide whether an expensive detail request can produce an emitted row."""
        category_match = self._category_match(product, collection)
        if category_match is not None:
            return category_match
        if not self.config.get("inventory_prefilter_materials"):
            return True

        product_type = domain.clean(product.get("product_type"))
        tags = product.get("tags") or []
        tags = tags if isinstance(tags, list) else [domain.clean(tags)]
        categories = tuple(value for value in (collection, product_type) if value)
        category_text = " ".join((*categories, *(domain.clean(tag) for tag in tags)))
        title = domain.clean(product.get("title"))
        description = domain.clean(product.get("body_html"))
        variants = product.get("variants") or [None]
        for variant in variants:
            variant_title = domain.clean(variant.get("title")) if isinstance(variant, dict) else ""
            name = f"{title} {variant_title}".strip()
            family = domain.family(name, category_text) or domain.family(
                name, description, category_text,
            )
            if domain.is_material(
                family, name, category_text,
                categories=categories, description=description,
            ):
                return True
        return False

    @staticmethod
    def _stock_quantity(variant: dict[str, Any]) -> int | None:
        """Return finite Shopify inventory, excluding continue-selling stock."""
        quantity = variant.get("inventory_quantity")
        if (
            variant.get("inventory_management")
            and variant.get("inventory_policy") == "deny"
            and isinstance(quantity, int)
            and not isinstance(quantity, bool)
        ):
            return max(quantity, 0)
        return None

    @staticmethod
    def _weight(variant: dict[str, Any]) -> dict[str, Any]:
        grams = variant.get("grams")
        return {"shipping_weight_g": grams} if isinstance(grams, (int, float)) and grams else {}

    @staticmethod
    def _options(product: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
        names = [
            domain.clean(option.get("name"))
            for option in product.get("options") or []
            if isinstance(option, dict)
        ]
        values = [variant.get(f"option{index}") for index in (1, 2, 3)]
        return {
            name: domain.clean(value)
            for name, value in zip(names, values)
            if name and value and domain.clean(value).casefold() != "default title"
        }

    def _manufacturer_sku(
        self, product: dict[str, Any], variant: dict[str, Any], variant_title: str = "",
    ) -> str | None:
        """Record a manufacturer code only when that manufacturer is named.

        Shops often sell a whole colour range as one product whose variants are
        the colours ("Ivory Specks FN061 / 473 ml"), so the variant carries the
        code and the product title does not.
        """
        return domain.manufacturer_code(
            domain.clean(product.get("vendor")) or self.config.get("brand"),
            variant_title,
            domain.clean(variant.get("sku")),
            domain.clean(product.get("title")),
        )
