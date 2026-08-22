"""Offline tests for the ceramics field parsing and the record contract."""

import asyncio
import gzip
import json
import tempfile
import time
import timeit
import unittest
import unittest.mock
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import httpx

from mb_ceramics_catalogue import scrapers
from mb_ceramics_catalogue.scrapers import (
    base,
    ceramicolours,
    domain,
    enrichment,
    jsonld,
    microdata,
    prestashop,
    shopify,
    shopware,
    sumup,
    wix,
    woocommerce,
)
from mb_ceramics_catalogue.scrapers import cache as cache_module
from mb_ceramics_catalogue.scrapers import record as record_module

ROOT = Path(__file__).resolve().parent.parent


def described(name, description="", categories=()):
    """The derived block a source selecting `ceramic-materials` gets, plus scope.

    `is_material` is not a record field — the scope decision is taken from the
    finished row — but it reads the family this block derives, so the two are
    asserted together here.
    """
    block = enrichment.apply(
        enrichment.resolve(["ceramic-materials"]),
        enrichment.Context(
            name=name, description=description, categories=tuple(categories),
        ),
    )
    block["is_material"] = domain.is_material(
        block["family"], name, " ".join(categories),
        categories=tuple(categories), description=description,
    )
    return block


class SourceConfigTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / "sources.json").read_text())

    def test_every_source_names_a_registered_scraper(self):
        # A count, not an exact number: sources are added over time, and a
        # hard-coded total fails on the addition rather than on a real fault.
        self.assertGreaterEqual(len(self.config), 20)
        for name, source in self.config.items():
            with self.subTest(source=name):
                self.assertIn("scraper", source, f"{name} declares no scraper")
                self.assertIn(source["scraper"], scrapers.REGISTRY)
                self.assertTrue(source.get("url", "").startswith("https://"))

    def test_every_registered_scraper_loads(self):
        for name in scrapers.REGISTRY:
            with self.subTest(scraper=name):
                self.assertTrue(callable(scrapers.load(name)))

    def test_art_academy_uses_its_authoritative_ceramics_categories(self):
        source = self.config["art-academy-direct"]
        self.assertEqual(["Glazes", "Pottery"], source["material_categories"])

    def test_ulster_prefilters_inventory_to_material_products(self):
        self.assertTrue(self.config["ulster-ceramics"]["inventory_prefilter_materials"])

    def test_robots_may_only_be_ignored_deliberately(self):
        """An ignore_robots source must record why and slow itself down."""
        for name, source in self.config.items():
            if not source.get("ignore_robots"):
                continue
            with self.subTest(source=name):
                self.assertIn("note", source, f"{name} ignores robots.txt without stating why")
                self.assertGreaterEqual(source.get("delay", 0), 2.0)


class WooCommerceStockTests(unittest.TestCase):
    def scraper(self, *, trust_maximum: bool = True):
        scraper = object.__new__(woocommerce.WooCommerceScraper)
        scraper.config = {"stock_from_add_to_cart_maximum": trust_maximum}
        return scraper

    def test_verified_cart_ceiling_is_stock_without_cart_mutation(self):
        item = {
            "is_in_stock": True,
            "is_on_backorder": False,
            "sold_individually": False,
            "low_stock_remaining": None,
            "add_to_cart": {"maximum": 69},
        }
        self.assertEqual(69, self.scraper()._stock_quantity(item))

    def test_low_stock_count_is_exact_without_source_opt_in(self):
        item = {"is_in_stock": True, "low_stock_remaining": 2, "add_to_cart": {"maximum": 2}}
        self.assertEqual(2, self.scraper(trust_maximum=False)._stock_quantity(item))

    def test_cart_rules_are_not_mislabeled_as_stock(self):
        backorder = {
            "is_in_stock": True,
            "is_on_backorder": True,
            "low_stock_remaining": 0,
            "add_to_cart": {"maximum": 500},
        }
        sold_individually = {
            "is_in_stock": True,
            "is_on_backorder": False,
            "sold_individually": True,
            "add_to_cart": {"maximum": 1},
        }
        self.assertIsNone(self.scraper()._stock_quantity(backorder))
        self.assertIsNone(self.scraper()._stock_quantity(sold_individually))
        self.assertIsNone(self.scraper()._stock_quantity({
            "is_in_stock": True,
            "is_on_backorder": False,
            "add_to_cart": {"maximum": 9999},
        }))
        self.assertIsNone(self.scraper(trust_maximum=False)._stock_quantity({
            "is_in_stock": True,
            "is_on_backorder": False,
            "add_to_cart": {"maximum": 14},
        }))

    def test_out_of_stock_is_zero(self):
        self.assertEqual(0, self.scraper()._stock_quantity({
            "is_in_stock": False,
            "low_stock_remaining": 0,
            "add_to_cart": {"maximum": 1},
        }))


class WixStockTests(unittest.TestCase):
    def test_tracked_inventory_is_exact(self):
        item = {
            "isInStock": True,
            "isTrackingInventory": True,
            "inventory": {"status": "in_stock", "quantity": 16},
        }
        self.assertEqual(16, wix.WixScraper._stock_quantity(item))

    def test_variant_inherits_product_tracking(self):
        item = {
            "isInStock": True,
            "isTrackingInventory": None,
            "inventory": {"status": "in_stock", "quantity": 7},
        }
        self.assertEqual(7, wix.WixScraper._stock_quantity(item, {"isTrackingInventory": True}))

    def test_disabled_inventory_counter_is_unknown(self):
        item = {
            "isInStock": True,
            "isTrackingInventory": False,
            "inventory": {"status": "in_stock", "quantity": 0},
        }
        self.assertIsNone(wix.WixScraper._stock_quantity(item))

    def test_explicit_out_of_stock_is_zero(self):
        item = {
            "isInStock": False,
            "isTrackingInventory": False,
            "inventory": {"status": "out_of_stock", "quantity": 0},
        }
        self.assertEqual(0, wix.WixScraper._stock_quantity(item))


class PublishedStockTests(unittest.TestCase):
    def test_prestashop_product_json_allows_apostrophes(self):
        payload = {"name": "Potter's glaze", "quantity": 25}
        encoded = json.dumps(payload).replace('"', "&quot;")
        document = f'<div id="product-details" class="details" data-product="{encoded}"></div>'
        self.assertEqual(payload, prestashop.data_product(document))

    def test_shopware_buy_widget_maximum_is_stock(self):
        document = (
            '<input name="lineItems[id][quantity]" '
            'class="form-control quantity-selector-group-input" min="1" max="7">'
        )
        self.assertEqual(7, shopware.ShopwareScraper.quantity_maximum(document))

    def test_ceramicolours_mass_is_converted_to_pack_units(self):
        document = '<input id="icaOrdinabile" name="icaOrdinabile" value="0.30">'
        self.assertEqual(6, ceramicolours.CeramicoloursScraper.stock_units(document, "0.05"))
        self.assertEqual(0, ceramicolours.CeramicoloursScraper.stock_units(document, "0.5"))

    def test_shopify_finite_managed_inventory_is_exact(self):
        variant = {
            "inventory_quantity": 9,
            "inventory_management": "shopify",
            "inventory_policy": "deny",
        }
        self.assertEqual(9, shopify.ShopifyScraper._stock_quantity(variant))

    def test_shopify_continue_selling_inventory_is_not_a_purchase_ceiling(self):
        variant = {
            "inventory_quantity": -3,
            "inventory_management": "shopify",
            "inventory_policy": "continue",
        }
        self.assertIsNone(shopify.ShopifyScraper._stock_quantity(variant))

    def test_shopify_inventory_enrichment_can_skip_rejected_feed_categories(self):
        scraper = shopify.ShopifyScraper.__new__(shopify.ShopifyScraper)
        scraper.category_allows = lambda *values: "yarn" not in values[0].casefold()
        self.assertFalse(scraper._category_match(
            {"product_type": "Yarn", "tags": [], "handle": "wool"}, None,
        ))
        self.assertTrue(scraper._category_match(
            {"product_type": "Ceramic glaze", "tags": [], "handle": "blue"}, None,
        ))

    def test_shopify_inventory_prefilter_rejects_uncategorised_non_materials(self):
        scraper = shopify.ShopifyScraper.__new__(shopify.ShopifyScraper)
        scraper.config = {"inventory_prefilter_materials": True}
        scraper.category_allows = lambda *values: None
        self.assertFalse(scraper._inventory_candidate({
            "title": "Set of 12 plastic crayons & eraser",
            "product_type": "", "tags": [], "variants": [{"title": "Default Title"}],
            "body_html": "Drawing supplies for children.",
        }, None))
        self.assertTrue(scraper._inventory_candidate({
            "title": "Mayco Stroke & Coat glaze",
            "product_type": "", "tags": [], "variants": [{"title": "Pint"}],
            "body_html": "Brush on and fire to cone 6.",
        }, None))

    def test_shopify_proxy_exhaustion_only_skips_that_inventory_read(self):
        class DeniedFetcher:
            async def text(self, *_args, **_kwargs):
                raise shopify.ProxyDenied("reservation exhausted")

            async def rotate_client(self):
                return None

        scraper = shopify.ShopifyScraper.__new__(shopify.ShopifyScraper)
        scraper.config = {"inventory_product_html": True}
        scraper.fetcher = DeniedFetcher()
        scraper.result = SimpleNamespace(requests=0)
        scraper._inventory_failures = 0
        scraper.origin = lambda: "https://shop.test"
        asyncio.run(scraper._enrich_inventory([{"handle": "glaze", "variants": []}]))
        self.assertEqual(1, scraper._inventory_failures)
        self.assertEqual(0, scraper.result.requests)

    def test_shopify_inventory_batches_are_serial_and_rotate_between_batches(self):
        class TrackingFetcher:
            active = peak = rotations = 0

            async def text(self, *_args, **_kwargs):
                self.active += 1
                self.peak = max(self.peak, self.active)
                await asyncio.sleep(0)
                self.active -= 1
                return ""

            async def rotate_client(self):
                self.rotations += 1

        fetcher = TrackingFetcher()
        scraper = shopify.ShopifyScraper.__new__(shopify.ShopifyScraper)
        scraper.config = {"inventory_product_html": True}
        scraper.fetcher = fetcher
        scraper.result = SimpleNamespace(requests=0)
        scraper._inventory_failures = 0
        scraper.origin = lambda: "https://shop.test"
        products = [
            {"handle": f"glaze-{index}", "variants": []}
            for index in range(11)
        ]
        asyncio.run(scraper._enrich_inventory(products))
        self.assertEqual(1, fetcher.peak)
        self.assertEqual(1, fetcher.rotations)
        self.assertEqual(11, scraper.result.requests)

    def test_shopify_inventory_preserves_proxy_bytes_for_later_feed_pages(self):
        class ReservedFetcher:
            proxy_bytes_remaining = 900_000

            async def text(self, *_args, **_kwargs):
                raise AssertionError("optional inventory must not spend the feed reserve")

            async def rotate_client(self):
                raise AssertionError("an untouched batch must not rotate")

        scraper = shopify.ShopifyScraper.__new__(shopify.ShopifyScraper)
        scraper.config = {"inventory_product_html": True}
        scraper.fetcher = ReservedFetcher()
        scraper.result = SimpleNamespace(requests=0)
        scraper._inventory_failures = 0
        scraper.origin = lambda: "https://shop.test"
        products = [
            {"handle": "glaze-one", "variants": []},
            {"handle": "glaze-two", "variants": []},
        ]
        asyncio.run(scraper._enrich_inventory(products))
        self.assertEqual(2, scraper._inventory_failures)
        self.assertEqual(0, scraper.result.requests)

    def test_verified_shopify_theme_inventory_shapes(self):
        samples = (
            ('{"id":101,"inventory_quantity":12,"inventory_management":"shopify",'
             '"inventory_policy":"deny"}', "101", 12),
            ('<option value="102" data-inventory="14" data-inventory-policy="deny" '
             'data-inventory-management="shopify">', "102", 14),
            ('gwProductInventoryPolicy[103]="deny";'
             'gwProductInventoryQuantity[103]="6";', "103", 6),
            ('{variant_id:104,variant_inventory_policy:"deny",'
             'variant_inventory_quantity:3}', "104", 3),
            ('{id:105,inventory_management:"shopify",quantity:12}', "105", 12),
            ('{"inventory":{"106":{"inventory_management":null,'
             '"inventory_policy":"deny","inventory_quantity":72}}}', "106", 72),
        )
        for document, identifier, expected in samples:
            with self.subTest(identifier=identifier):
                found = shopify.ShopifyScraper._inventory_from_html(document, {identifier})
                self.assertEqual(expected, found[identifier]["inventory_quantity"])


class SumUpTests(unittest.TestCase):
    """Reading a SumUp product out of the RSC payload its page streams."""

    def _page(self, *products):
        chunks = "".join(
            f"<script>self.__next_f.push([1,{json.dumps(part)}])</script>"
            for part in ['{"currency":"EUR","item":{', *products, "}}"]
        )
        return f"<html><body>{chunks}</body></html>"

    SHOWN = json.dumps({
        "id": "ebec5308-b80d-4c0c-ae84-26ce26a341b4",
        "name": "Tasse bleue",
        "slug": "tasse-bleue",
        "price": 2500,
        "basePrice": 3000,
        "hasDiscount": True,
        "image": "https://images.sumup.com/img_one",
        "allImages": ["https://images.sumup.com/img_one"],
        "category": {"name": "Ceramiques pour la maison"},
        "isAvailable": True,
        "variants": {
            "1f7ac7e9-8bb0-4998-b88f-077b7a249862": {
                "uuid": "1f7ac7e9-8bb0-4998-b88f-077b7a249862",
                "name": "",
                "price": 2500,
                "basePrice": 3000,
                "hasDiscount": True,
                "options": [],
                "quantity": 3,
                "isAvailable": True,
                "isTrackingEnabled": True,
            },
        },
    }, separators=(",", ":"))[1:-1]
    #: A related item, in the listing shape: no variant detail, and its own slug.
    RELATED = json.dumps({
        "id": "bc945f75-16b6-4c78-825a-73f117bfe5f9",
        "name": "Soucoupe bleue",
        "slug": "soucoupe-bleue",
        "price": 900,
        "variants": {"5157898e-6f95-4e63-99fa-5004f377bec8": {}},
    }, separators=(",", ":"))[1:-1]

    def _scraper(self):
        fetcher = SimpleNamespace(limiter=SimpleNamespace(join_group=lambda *a: None, set_delay=lambda *a: None))
        return sumup.SumUpScraper(
            "shop", {"url": "https://shop.sumupstore.com/", "scope": "all"}, fetcher,
        )

    def _rows(self, url="https://shop.sumupstore.com/article/tasse-bleue"):
        document = self._page(
            '"product":{' + self.RELATED + '},"product":{' + self.SHOWN + "}",
        )
        return self._scraper().parse(document, url)

    def test_the_product_the_page_is_about_wins_over_a_related_item(self):
        (row, _), = self._rows()
        self.assertEqual("Tasse bleue", row["product_name"])
        # Minor units: 2500 is 25.00, and the pre-discount price rides along.
        self.assertEqual(25.0, row["price"])
        self.assertEqual(30.0, row["list_price"])
        self.assertEqual("EUR", row["currency"])
        self.assertEqual(3, row["stock_quantity"])

    def test_a_slug_that_matches_nothing_falls_back_to_the_detailed_product(self):
        """A localised route we did not anticipate must not select a related item."""
        (row, _), = self._rows("https://shop.sumupstore.com/produkt/unbekannt")
        self.assertEqual("Tasse bleue", row["product_name"])

    def test_untracked_inventory_is_unknown_rather_than_zero(self):
        variant = {"isAvailable": True, "isTrackingEnabled": False, "quantity": 0}
        self.assertIsNone(sumup.SumUpScraper._stock_quantity(variant, {}))

    def test_a_variant_inherits_the_product_s_tracking(self):
        variant = {"isAvailable": True, "isTrackingEnabled": None, "quantity": 4}
        self.assertEqual(4, sumup.SumUpScraper._stock_quantity(variant, {"isTrackingEnabled": True}))

    def test_sold_out_is_zero_whatever_the_counter_says(self):
        variant = {"isAvailable": False, "isTrackingEnabled": True, "quantity": 7}
        self.assertEqual(0, sumup.SumUpScraper._stock_quantity(variant, {}))

    def test_a_listing_page_in_the_products_sitemap_yields_nothing(self):
        """The shop's own products sitemap lists its home page beside the products.

        Every object on it is a suggestion in the listing shape. Taking the
        first one published a real product's price under the shop's root URL.
        """
        document = self._page('"product":{' + self.RELATED + "}")
        self.assertEqual([], self._scraper().parse(document, "https://shop.sumupstore.com/"))

    def test_a_page_with_no_payload_falls_back_to_json_ld(self):
        rows = self._scraper().parse("<html><body>nothing</body></html>", "https://shop.sumupstore.com/x")
        self.assertEqual([], rows)


class ImpersonatedEncodingTests(unittest.IsolatedAsyncioTestCase):
    """curl_cffi returns a decompressed body; its headers still claim gzip.

    Passing those headers through made httpx inflate the body a second time and
    fail, which reads as a block rather than a bug — and it cost every gzipped
    sitemap on a host that fingerprints the handshake.
    """

    async def test_the_body_is_not_inflated_twice(self):
        client = base.ImpersonatingClient()
        body = b"<urlset><url><loc>https://shop.test/a</loc></url></urlset>"

        def blocking_get(*args, **kwargs):
            return 200, body, {"content-encoding": "gzip", "content-type": "application/xml"}, "https://shop.test/s.xml"

        with unittest.mock.patch.object(client, "_blocking_get", blocking_get):
            response = await client.request("https://shop.test/s.xml")
        self.assertIn("https://shop.test/a", response.text)
        self.assertNotIn("content-encoding", response.headers)


class FiringRangeTests(unittest.TestCase):
    def test_celsius_range_with_degree_on_the_first_value(self):
        result = domain.firing_range("BOTZ ENGOBE FLORIDA 1180° - 1280°C")
        self.assertEqual((1180, 1280), (result["min_celsius"], result["max_celsius"]))

    def test_cone_range_repeating_the_word(self):
        result = domain.firing_range("fires cone 06 to cone 10")
        self.assertEqual(("06", "10"), (result["cone_min"], result["cone_max"]))
        self.assertEqual("orton", result["cone_system"])
        self.assertEqual((999, 1305), (result["min_celsius"], result["max_celsius"]))

    def test_fahrenheit_is_converted(self):
        result = domain.firing_range("Fires 2000-2232°F")
        self.assertEqual((1093, 1222), (result["min_celsius"], result["max_celsius"]))
        self.assertEqual("F", result["published_unit"])

    def test_segerkegel_is_distinguished_from_orton(self):
        result = domain.firing_range("Glasur SK 6a")
        self.assertEqual("seger", result["cone_system"])
        self.assertEqual(1200, result["min_celsius"])

    def test_multilingual_separators(self):
        for text, expected in (
            ("entre 1220 et 1280°C", (1220, 1280)),
            ("tussen 1000 en 1100°C", (1000, 1100)),
            ("von 1020 und 1080°C", (1020, 1080)),
        ):
            with self.subTest(text=text):
                result = domain.firing_range(text)
                self.assertEqual(expected, (result["min_celsius"], result["max_celsius"]))

    def test_absent_range_is_none(self):
        self.assertIsNone(domain.firing_range("Blue glaze, 473 ml jar"))


class PackageAndUnitPriceTests(unittest.TestCase):
    def test_metric_volume(self):
        package = domain.package_size("473ml jar", liquid_hint=True)
        self.assertEqual(473.0, package["millilitres"])

    def test_fluid_ounces_for_a_liquid(self):
        package = domain.package_size("16 oz", liquid_hint=True)
        self.assertEqual("fl oz", package["unit"])
        self.assertAlmostEqual(473.176, package["millilitres"], places=2)

    def test_ounces_stay_weight_for_a_dry_product(self):
        package = domain.package_size("16 oz", liquid_hint=False)
        self.assertEqual("weight", package["dimension"])
        self.assertTrue(package["unit_ambiguous"])

    def test_named_container_without_a_number(self):
        """US suppliers name the container: "C-01 Obsidian Pint"."""
        pint = domain.package_size("C-01 Obsidian Pint", liquid_hint=True)
        self.assertAlmostEqual(473.176, pint["millilitres"], places=2)
        self.assertEqual("named_container", pint["basis"])
        gallon = domain.package_size("C-01 Obsidian Gallon", liquid_hint=True)
        self.assertAlmostEqual(3785.41, gallon["millilitres"], places=2)

    def test_a_model_number_is_not_a_quantity(self):
        """"SW-229 Pint" is one pint of SW-229, not 229 pints."""
        package = domain.package_size("SW-229 Pint", liquid_hint=True)
        self.assertAlmostEqual(473.176, package["millilitres"], places=2)
        # A genuinely sized pack still wins over the container name.
        self.assertAlmostEqual(946.35, domain.package_size("2 pint jar", liquid_hint=True)["millilitres"], places=1)

    def test_a_ratio_is_a_specification_not_a_package(self):
        """"DENSIMETRE 1000/2000 - 0.010g/ml" measures a density; it is not a 10 mg jar."""
        self.assertIsNone(domain.package_size("DENSIMETRE 1000/2000 - 0.010g/ml Tp.20C"))
        self.assertIsNone(domain.package_size("Engobe 15 g/l dilution"))

    def test_a_multipack_counts_every_unit(self):
        """"36x2,5ml" is a set of 36 pans, so the pack the buyer receives is 90 ml."""
        package = domain.package_size("Akvareliu rinkinys 36x2,5ml", liquid_hint=True)
        self.assertEqual(90.0, package["millilitres"])
        self.assertIn("36", package["evidence"])

    def test_unit_in_the_attribute_name(self):
        package = domain.package_size_from_attributes({"Volume (ml)": "200"}, liquid_hint=True)
        self.assertEqual(200.0, package["millilitres"])

    def test_unit_price_per_litre_and_kilogram(self):
        litre = domain.unit_price(11.8, "EUR", {"dimension": "volume", "millilitres": 200.0})
        self.assertEqual({"value": 59.0, "currency": "EUR", "per": "l"}, litre)
        kilo = domain.unit_price(20.0, "EUR", {"dimension": "weight", "grams": 500.0})
        self.assertEqual({"value": 40.0, "currency": "EUR", "per": "kg"}, kilo)


class ClassificationTests(unittest.TestCase):
    def test_families_across_languages(self):
        for text, expected in (
            ("Emaux transparent brillant", "glaze"),
            ("Glasur glänzend", "glaze"),
            ("Underglaze black", "underglaze"),
            ("Engobe pour grès", "engobe"),
            ("Argile de tournage", "clay_body"),
            ("Oxyde de cobalt", "oxide"),
        ):
            with self.subTest(text=text):
                self.assertEqual(expected, domain.family(text))

    def test_description_prose_does_not_reject_a_glaze(self):
        """Glaze copy mentions brushes and kilns; scope must ignore the prose."""
        block = described(
            "Penguin Pottery Underglaze - Black",
            "It can be brushed or sprayed on. Fire in a kiln to cone 6 on a kiln shelf.",
            ["underglaze-series"],
        )
        self.assertEqual("underglaze", block["family"])
        self.assertTrue(block["is_material"])

    def test_brush_on_glaze_is_a_material_but_a_glaze_brush_is_a_tool(self):
        for name in (
            "Yellow Mid Temp Glaze - Brush On",
            "Black Botz Earthenware Brush-On Glaze",
        ):
            with self.subTest(name=name):
                family = domain.family(name)
                self.assertEqual("glaze", family)
                self.assertTrue(domain.is_material(family, name))
        self.assertTrue(domain.looks_non_material("Glaze Brush No. 8"))

    def test_equipment_is_out_of_scope(self):
        self.assertTrue(domain.looks_non_material("Kiln shelf 30cm"))
        self.assertFalse(domain.is_material(None, "Rohde kiln KE 250N"))

    def test_a_kiln_is_not_a_clay_body(self):
        """A Nabertherm furnace reached the catalogue at 37 EUR/kg.

        Two independent failures had to line up. "gres" is the Italian for
        stoneware and was matched as a bare substring, so the "ingresso" of
        "valvola ingresso aria" in the specification classified the kiln as a
        clay body; and the scope filter had no Italian in it, so the word
        "forno" in the very first line was never looked for.
        """
        block = described(
            "N 100 (5 lati)",
            "Forno elettrico con apertura frontale Nabertherm N 100. "
            "Temperatura massima 1300°C - 9,0 kW. Peso: 275 kg. "
            "valvola ingresso aria; collettore per uscita fumi.",
        )
        self.assertIsNone(block["family"])
        self.assertFalse(block["is_material"])

    def test_a_short_keyword_may_not_match_inside_a_word(self):
        self.assertIsNone(domain.family("valvola ingresso aria"))
        self.assertIsNone(domain.family("lavori in progresso"))
        self.assertEqual("clay_body", domain.family("Gres blanc chamotté"))

    def test_a_material_may_be_the_tail_of_a_compound(self):
        """Germanic catalogues weld the material onto the end of the word."""
        for text, expected in (
            ("Lertøjsglasur 1925 Blågrøn", "glaze"),
            ("Penselglasur 33 Turkis", "glaze"),
            ("84210-5 Transparent Porzellanglasur", "glaze"),
            ("2S Aufbaumasse, Lederfarben, 1000-1280°C", "clay_body"),
            ("32SF40 Plattenmasse Weiß 40 %", "clay_body"),
            ("Eisenoxid rot", "oxide"),
            ("Steinzeugton weiß", "clay_body"),
        ):
            with self.subTest(text=text):
                self.assertEqual(expected, domain.family(text))

    def test_a_material_may_be_the_middle_of_a_compound(self):
        """Danish welds on both sides: under + glasur + farver."""
        self.assertEqual("underglaze", domain.family("underglasurfarver til keramik"))

    def test_polish_inflections_still_match(self):
        for text in ("szkliwo transparentne", "szkliwa białe", "szkliwie"):
            with self.subTest(text=text):
                self.assertEqual("glaze", domain.family(text))

    def test_the_scope_filter_reaches_romance_equipment(self):
        for text in (
            "Forno elettrico Nabertherm N 100",
            "Forno Kittec CL-5 330 litros",
            "HORNO PLUTON (23 a 200lt)",
            "Cuptor ROHDE Raku seria TR",
            "Tornio Elettrico RK-3E",
            "Coni Orton Self Supporting (coppia)",
            "Matita sottosmalto Chrysanthos viola",
            "Spugne Diamantate",
        ):
            with self.subTest(text=text):
                self.assertTrue(domain.looks_non_material(text))

    def test_the_scope_filter_still_reaches_germanic_compounds(self):
        """German names the equipment at the end, so these need a substring."""
        for text in ("Muffelofen 230V", "Kammerofen", "Keramikofen", "Töpferscheibe Shimpo"):
            with self.subTest(text=text):
                self.assertTrue(domain.looks_non_material(text))

    def test_the_scope_filter_reaches_slavic_and_nordic_kilns(self):
        for text in (
            "Piec do ceramiki Kittec Squadro SQ 11",
            "Piec Kittec RAKU CBR 80 T",
            "Električne peći za keramiku BC 1200/1250",
            "Drejskiva Brent CXC",
            "Kittec X-Line Toplader Modell: X 215",
            "Hobby-Frontlader Modell: N 100 E",
        ):
            with self.subTest(text=text):
                self.assertTrue(domain.looks_non_material(text))

    def test_a_piece_is_not_a_polish_kiln(self):
        """"piec" is a kiln in Polish and the first four letters of "piece"."""
        for text in ("a piece of clay", "Masterpiece glaze 500ml", "Centrepiece stoneware"):
            with self.subTest(text=text):
                self.assertFalse(domain.looks_non_material(text))

    def test_studio_machinery_is_not_a_clay_body(self):
        """A pug mill costs more than a pallet of clay and is not clay."""
        for text in (
            "Peter Pugger VPM-7 Vacuum Power Wedger",
            "Shimpo Ball Mill PTA-02",
            "3D PotterBot Scara Elite Printer",
            "LAMINADORA XLAM 1600 COLD&HOT",
            "Galletera Rohde TS 20",
            "Fieira - Extrusora SHIMPO NRA-04S",
        ):
            with self.subTest(text=text):
                self.assertTrue(domain.looks_non_material(text))
        # ...but a printed transfer is a material.
        self.assertFalse(domain.looks_non_material("Printed decal paper A4"))

    def test_the_shops_own_department_decides_scope(self):
        """A category is a classification, so it can be read more broadly."""
        for categories in (
            ["Maquinaria, accesorios y seguridad"],
            ["Machinery", "Machinery (New)"],
            ["equipment"],
            ["Ferramentas para cerâmica"],
            ["Hornos y pirometría", "Refractario"],
            ["Fours céramiques"],
            ["Inne narzędzia"],
            ["set draaigereedschap"],
            ["Utensili per modellare", "Strumenti"],
            ["Carts & Ceramic Furniture", "Equipment"],
        ):
            with self.subTest(categories=categories):
                self.assertTrue(domain.names_non_material_department(categories))

    def test_a_department_that_only_looks_like_equipment(self):
        """Each of these cost a real product to find.

        "torno" is the wheel, but "arcilla roja torno" is a clay *for* it;
        "refractaria" is fireclay, not a kiln shelf; and "peci" hides inside the
        Spanish "especiales", which is where a shop files its speciality glazes.
        """
        for categories in (
            ["arcilla roja torno pf"],
            ["pastas refractarias"],
            ["arcilla refractaria"],
            ["especiales sin plomo"],
            ["Speciality glazes"],
            ["Fournitures"],
            ["Glazes", "Stoneware"],
        ):
            with self.subTest(categories=categories):
                self.assertFalse(domain.names_non_material_department(categories))

    def test_a_published_wattage_names_a_machine_in_any_language(self):
        for description in (
            "Potenza/Consumo 15,0 kW - 400 Volt Trifase",
            "Anschluss 9,0 kW, Dreiphasen",
            "230v single phase",
            "Rated 3 phase supply",
        ):
            with self.subTest(description=description):
                self.assertTrue(domain.publishes_machine_specification(description))

    def test_a_product_code_is_not_a_voltage(self):
        """"Email grès vert EK320V" is a glaze; the V is part of the code."""
        for description in (
            "Email gres vert EK081V, 1240°C",
            "VERT D'EAU (978320) 320V",
            "Fires to 1240°C, apply 2 coats",
        ):
            with self.subTest(description=description):
                self.assertFalse(domain.publishes_machine_specification(description))

    def test_scope_reads_three_kinds_of_evidence(self):
        # A kiln whose name says nothing, caught by its specification alone.
        self.assertFalse(
            described("KMT1027", "Toploader 11,0 kW 400 Volt Trifase")["is_material"]
        )
        # Orton cones in Spanish, caught by the department alone.
        self.assertFalse(
            described("Conos ORTON SMALL(SRB)", "", ["Pirometría"])["is_material"]
        )
        # ...and a glaze filed under glazes is still a glaze.
        self.assertTrue(
            described(
                "Botz Steinzeugglasur 9101", "Brennen 1220-1250°C", ["Glasuren"]
            )["is_material"]
        )

    def test_a_maker_who_only_makes_machines(self):
        """The last machines had only a brand name left to give them away."""
        for text in (
            "Nabertherm N 140E Modell",
            "Kittec Squadro Modell: SQ 11",
            "Shimpo Whisper Economy RK 3T",
            "Skutt Relay – Solid State for Firebox",
            "Gladstone Vibratory Sifter",
            "brent Leg Extension Kit",
        ):
            with self.subTest(text=text):
                self.assertTrue(domain.looks_non_material(text))

    def test_a_maker_who_sells_both_is_no_evidence(self):
        """Rohde and Laguna sell clay as well as kilns, so the brand proves nothing.

        "Brentwood" and "Paragon" are ordinary words that a wheel maker's name
        would otherwise swallow.
        """
        for text in (
            "Rohde Tonmasse rot",
            "Laguna B-Mix 5 with grog",
            "Brentwood stoneware clay",
            "Paragon blue glaze",
        ):
            with self.subTest(text=text):
                self.assertFalse(domain.looks_non_material(text))

    def test_machines_named_in_the_remaining_languages(self):
        for text in (
            "BOUDINEUSE G51E (220V)",
            "Impastatrice Shimpo NVS-07A",
            "Vakuummischer NVS 07",
            "Ton 3D Drucker PAW42 St",
            "ECOTOP200S",
            "Cabină de glazurat ROHDE SK66",
            "Cabine de Vidragem Kittec SB1",
        ):
            with self.subTest(text=text):
                self.assertTrue(domain.looks_non_material(text))

    def test_a_print_shop_is_not_a_clay_printer(self):
        """"Drucker" sits inside "Druckerei", which is where a decal comes from."""
        self.assertFalse(domain.looks_non_material("Druckerei transfer paper A4"))

    def test_the_category_alone_can_name_the_machine(self):
        """A spray gun filed under Spray Booths is still not a glaze."""
        self.assertTrue(
            domain.looks_non_material("Paasche LXG-20 HVLP Spray Gun", "Spray Booths")
        )
        self.assertTrue(
            domain.looks_non_material("Anbauplatten Blue-Star", "Tonplattenwalze")
        )

    def test_a_glaze_may_be_named_after_the_kiln(self):
        """Amaco's Kiln Ice is a glaze, and "kiln" must not veto it.

        A kiln shelf is never called a glaze, but a glaze is quite often named
        after the kiln it goes into, so an explicit family word is the stronger
        claim of the two. Only "kiln" is overruled this way.
        """
        for text in (
            "Amaco Kiln Ice Glaze KI11 Snow Drift, Pint",
            "Amaco Kiln Ice Glaze KI46 Frozen Fern",
        ):
            with self.subTest(text=text):
                self.assertFalse(domain.looks_non_material(text))
        # The override is not a licence: these are still equipment.
        for text in ("Kiln shelf 30cm", "Rohde kiln KE 250N", "Kiln furniture set"):
            with self.subTest(text=text):
                self.assertTrue(domain.looks_non_material(text))

    def test_a_colour_range_may_be_called_elements(self):
        """Mayco Elements is fifty glazes; a kiln's element is a wire."""
        for text in (
            "Émail brilliant Mayco Elements® - EL101 Oyster Shell",
            "Assorted Mayco Elements Glazes - 4 oz Samples EL-133",
            "Masquerade CG970 Jungle Gems (Mayco) 16oz",
        ):
            with self.subTest(text=text):
                self.assertFalse(domain.looks_non_material(text))
        for text in (
            "Kiln heating element 230V",
            "Element de chauffe four 230V",
            "Heizelement fuer Brennofen",
            "masque de protection FFP2",
        ):
            with self.subTest(text=text):
                self.assertTrue(domain.looks_non_material(text))

    def test_hematite_is_a_colourant_not_a_pencil(self):
        """"matite" is Italian for pencils and hides inside "hematite"."""
        self.assertFalse(domain.looks_non_material("Hematite"))
        self.assertTrue(domain.is_material("oxide", "Hematite"))


class ManufacturerCodeTests(unittest.TestCase):
    def test_code_requires_a_named_manufacturer(self):
        self.assertEqual("SW229", domain.manufacturer_code("Mayco", "Mood Ring", "SW-229"))
        self.assertIsNone(domain.manufacturer_code("Les Cousins", "EMAIL GRES EG140-05B", "EG140-05B"))

    def test_a_firing_temperature_is_not_a_product_code(self):
        self.assertNotEqual(
            "1180", domain.manufacturer_code("Botz", "B9826 BOTZ ENGOBE FLORIDA 1180 - 1280 C", ""),
        )

    def test_zero_padding_is_normalised(self):
        """AMACO publishes C-01 where a reseller writes C-1; both are one product."""
        self.assertEqual("C1", domain.manufacturer_code("AMACO", "C-01 Obsidian Pint", ""))
        self.assertEqual("C1", domain.manufacturer_code("Amaco", "AMACO C-1 OBSIDIAN stoneware glaze", ""))
        self.assertEqual(
            domain.manufacturer_code("Mayco", "COULEUR MAYCO SC075 ORANGE A PEEL", ""),
            domain.manufacturer_code("Mayco", "COULEUR MAYCO SC75 ORANGE A PEEL", ""),
        )
        # Padding is stripped, not every zero: C-10 must not become C-1.
        self.assertEqual("C10", domain.manufacturer_code("AMACO", "C-10 Blue", ""))

    def test_a_bare_word_is_not_a_product_code(self):
        """'SIO' matched every SIO-2 clay and collapsed them into one product."""
        for name in ("SIO-2 FLUMO 1 lt", "SIO-2 ARGILA 5kg Red", "SIO-2 RAKU 12.5kg"):
            with self.subTest(name=name):
                self.assertIsNone(domain.manufacturer_code("SIO-2", name, ""))

    def test_a_curated_code_is_read_when_the_maker_is_named_too(self):
        """`PGV` and `PLV` carry no digit and are not in the PR series.

        On a shop that writes "Sio-2 Maiolica verde PLV" the maker is named and
        the code is right there, but no pattern can express it — so the curated
        vocabulary has to be consulted here as well as when inferring a maker
        from a code, or these clays join nothing.
        """
        for name, expected in (
            ("Sio-2 Maiolica verde PLV 5kg", "PLV"),
            ("Sio-2 Lut pentru veselă PGV 12.5kg", "PGV"),
            ("Pasta Cerâmica SiO-2 PLA Azul – Faiança", "PLA"),
        ):
            with self.subTest(name=name):
                self.assertEqual(expected, domain.manufacturer_code("SiO-2", name, ""))

    def test_the_makers_own_name_is_not_one_of_its_codes(self):
        """`SIO2` is letters-then-digit, exactly like a SIO-2 glaze code.

        So the brand token matched its own pattern, and sixteen unrelated
        products — a red clay, a white one, five chamotte grades, a litre of
        Flumo — promoted into a single canonical product keyed `SIO2`.
        """
        self.assertIsNone(
            domain.manufacturer_code("SiO-2", "SIO2 Chamotte 0 à 0.2mm rouge", "")
        )

    def test_a_real_code_still_wins_after_the_makers_name(self):
        """Rejecting the name must not abandon the search for a real code."""
        self.assertEqual(
            "PRAI", domain.manufacturer_code("SiO-2", "SIO2 PRAI white stoneware", "")
        )

    def test_the_sio_2_clay_series_is_a_code_despite_carrying_no_digit(self):
        """SIO-2 numbers its glazes and names its clay bodies.

        `PRGI` was read as a bare word by the rule above, so SIO-2's own
        catalogue carried no code for its clays and nothing could join a
        retailer's `PRGI` to them.
        """
        for name in ("SIO-2 PRGI stoneware 12.5kg", "SIO-2 PRAI white stoneware 0-0.2mm"):
            with self.subTest(name=name):
                self.assertEqual(
                    name.split()[1], domain.manufacturer_code("SIO-2", name, "")
                )


class ProductLineTests(unittest.TestCase):
    """A product line is a trademark, so naming it names the maker.

    `POTTER'S CHOICE 21 ARCTIC BLUE` on lescousins.fr is AMACO's PC-21 and the
    page says AMACO nowhere, so the glaze never appeared beside the same glaze
    from any other shop.
    """

    def test_a_numbered_line_gives_the_maker_and_the_code(self):
        parsed = domain.parse_title("POTTER’S CHOICE 21 ARCTIC BLUE", supplier_sku="PC_21-0_472")
        self.assertEqual("AMACO", parsed["brand"])
        self.assertEqual("line_named_in_title", parsed["brand_basis"])
        self.assertEqual("PC21", parsed["code"])

    def test_the_number_is_padded_the_same_way_the_maker_pads_it(self):
        """AMACO publishes `PC-01`; the line writes `1`. One product, one key."""
        parsed = domain.parse_title("POTTER’S CHOICE 1 SATURATION METALLIC")
        self.assertEqual("PC1", parsed["code"])

    def test_an_unnumbered_line_still_names_the_maker(self):
        """Colpaert keeps the number in its own reference, `APC70`.

        That is the shop's numbering, so no code is invented from it — but the
        row is still AMACO's.
        """
        parsed = domain.parse_title("POTTERS CHOICE COPPER RED", supplier_sku="APC70")
        self.assertEqual("AMACO", parsed["brand"])
        self.assertIsNone(parsed["code"])

    def test_a_code_in_the_title_is_read_once_the_line_names_the_maker(self):
        parsed = domain.parse_title("SC-58 501 Blues | Stroke & Coat", supplier_sku="100781")
        self.assertEqual("Mayco", parsed["brand"])
        self.assertEqual("SC58", parsed["code"])

    def test_only_a_number_touching_the_line_name_is_its_number(self):
        """`472` is the pack, further along the same title."""
        parsed = domain.parse_title("POTTER’S CHOICE COPPER RED 472 ML")
        self.assertEqual("AMACO", parsed["brand"])
        self.assertIsNone(parsed["code"])

    def test_the_maker_named_outright_still_wins(self):
        parsed = domain.parse_title("AMACO Potter's Choice PC-21 Arctic Blue")
        self.assertEqual("named_in_title", parsed["brand_basis"])

    def test_an_unnumbered_line_never_reads_the_pack_as_a_code(self):
        """Designer Liner titles read "DESIGNER LINER 37 ML BLANC".

        37 is the pack; the code is `SG402` in the shop's reference. Giving the
        line a prefix would make every colour in it the one product `SG37`.
        """
        white = domain.parse_title("DESIGNER LINER 37 ML BLANC", supplier_sku="SG402")
        black = domain.parse_title("DESIGNER LINER 37 ML NOIR", supplier_sku="SG401")
        self.assertEqual("Mayco", white["brand"])
        self.assertEqual("SG402", white["code"])
        self.assertNotEqual(white["code"], black["code"], "two colours are two products")


class NamedManufacturerTests(unittest.TestCase):
    """Makers written into titles all over the dumps and absent from the list.

    The row said who made it and nothing read it.
    """

    def test_makers_seen_in_two_or_more_shops_are_recognised(self):
        for name, expected in (
            ("Segerkegel Orton Standard Nr.03 1085°C", "Orton"),
            ("COULEUR DECOR PORCELAINE SCHJERNING N°102 VERT", "Schjerning"),
            ("COULEUR VITRIFIABLE HERAEUS 64115 BLEU – 10 G", "Heraeus"),
        ):
            with self.subTest(name=name):
                self.assertEqual(expected, domain.parse_title(name)["brand"])

    def test_a_maker_is_not_matched_inside_a_longer_word(self):
        self.assertIsNone(domain.parse_title("Norton abrasive disc")["brand"])


class CodeImpliedManufacturerTests(unittest.TestCase):
    """A code specific enough to name its maker on a page that names nobody.

    lescousins.fr sells SIO-2 clay as `PRAI - GRES REFRACTAIRE COULEUR PIERRE`
    and never writes SIO-2 anywhere, so the maker can only come from the code.
    """

    def test_a_known_code_names_its_maker(self):
        parsed = domain.parse_title(
            "PRAI – GRES REFRACTAIRE COULEUR PIERRE – CHAMOTTE IMPALPABLE 0-0.2 mm",
            supplier_sku="PRAI",
        )
        self.assertEqual("SiO-2", parsed["brand"])
        self.assertEqual("manufacturer_code", parsed["brand_basis"])
        self.assertEqual("PRAI", parsed["code"])

    def test_it_reads_the_code_from_mid_title_too(self):
        parsed = domain.parse_title("GRES BLANCO CHAMOTA FINA PRAF*E", supplier_sku="PRAF")
        self.assertEqual("SiO-2", parsed["brand"])
        self.assertEqual("PRAF", parsed["code"])

    def test_a_longer_word_that_merely_starts_with_a_code_is_not_one(self):
        """`PRAIRIE` is a colour on Les Cousins' own glazes."""
        parsed = domain.parse_title(
            "EMAIL TRANSPARENT T333B VERT PRAIRIE Poids: 25kg", supplier_sku="T333B"
        )
        self.assertIsNone(parsed["brand"])

    def test_lower_case_is_not_a_code(self):
        """Every shop quoting one writes it in capitals, and `pram` is a word."""
        self.assertIsNone(domain.manufacturer_by_code("Baby pram sponge"))

    def test_a_maker_named_on_the_page_outranks_a_code(self):
        """SIO-2 resells Colorobbia's BLS line, so the page is the better source."""
        parsed = domain.parse_title("COLOROBBIA BLS 900 Limoncello 236ml", supplier_sku="900")
        self.assertEqual("Colorobbia", parsed["brand"])
        self.assertEqual("named_in_title", parsed["brand_basis"])

    def test_a_code_outranks_the_shops_own_label(self):
        """A retailer's house brand is what the shop is, not what the product is."""
        parsed = domain.parse_title(
            "PRNI – GRES REFRACTAIRE NOIR", supplier_sku="PRNI", source_brand="Les Cousins"
        )
        self.assertEqual("SiO-2", parsed["brand"])

    def test_a_mention_names_the_maker_without_claiming_the_code(self):
        """Les Cousins sells `PRAIDEFAUT`: the same clay with a voiding defect.

        Its title mentions PRAI and its own reference does not, so it is a SIO-2
        product — but taking PRAI as *its* code makes it and the regular PRAI
        one product to `dedupe_key`, at the same price and pack, and the shop's
        two offers silently become one.
        """
        parsed = domain.parse_title(
            "GRES PRAI PRESENTANT UN DEFAUT DE VIDE – A MALAXER – DESTOCKAGE",
            supplier_sku="PRAIDEFAUT",
        )
        self.assertEqual("SiO-2", parsed["brand"])
        self.assertIsNone(parsed["code"])

    def test_a_shops_own_article_number_does_not_block_the_code(self):
        """e-cibas files the same clay under `10000085-12K`.

        That is the shop's numbering, not a statement about the product, so the
        code is still this row's — and it is the only thing that can join the
        row to the same clay in another shop.
        """
        parsed = domain.parse_title("PRGI", supplier_sku="10000085-12K")
        self.assertEqual("SiO-2", parsed["brand"])
        self.assertEqual("PRGI", parsed["code"])

    def test_the_inferred_maker_does_not_feed_back_into_code_extraction(self):
        """Naming SIO-2 from a code puts SIO-2 in the pattern search's haystack.

        Left alone, `manufacturer_code` then lifts the very code the branch
        above declined to take, straight back out of the title.
        """
        parsed = domain.parse_title(
            "GRES PRAI PRESENTANT UN DEFAUT DE VIDE", supplier_sku="PRAIDEFAUT"
        )
        self.assertNotEqual("manufacturer_pattern", parsed["code_basis"])

    def test_a_line_or_a_resold_range_is_not_a_code(self):
        for text in ("FLUMO 1 lt", "SIO-2 VIVO glaze", "COLOROBBIA BLS 900", "PA/CHF grogged white"):
            with self.subTest(text=text):
                self.assertIsNone(domain.manufacturer_by_code(text))


class ClaimTests(unittest.TestCase):
    def test_positive_food_contact_claim(self):
        claims = domain.claims("", "Lead free and dinnerware safe when fired as directed.")
        self.assertEqual("food_contact_suitability", claims[0]["type"])
        self.assertTrue(claims[0]["claim"])

    def test_negation_in_a_hyphenated_slug(self):
        """Mayco publishes safety as an icon URL; polarity comes from the name."""
        claims = domain.claims("", "Dinnerware Safe: http://example.test/not-dinnerware-safe.png")
        self.assertFalse(claims[0]["claim"])

    def test_a_specification_field_states_a_claim(self):
        """1240.design publishes "Food-safe: yes" as a spec row, not prose."""
        found = {c["type"]: c for c in domain.attribute_claims(
            {"Food-safe": "yes", "Lead free": "no", "Hue": "Black"},
        )}
        self.assertTrue(found["food_contact_suitability"]["claim"])
        self.assertFalse(found["lead_free"]["claim"])
        self.assertEqual("product_attribute", found["food_contact_suitability"]["basis"])

    def test_cookie_tables_are_not_specifications(self):
        """A cookie policy sits in the same markup as a spec table."""
        document = """
          <table><tr><th>Firing temperature</th><td>1200-1240 C</td></tr>
          <tr><th>Cookie name</th><td>Provider Purpose Expiry</td></tr>
          <tr><th>cookiesplus</th><td>Remembers cookie preferences. 1 year</td></tr></table>
        """
        self.assertEqual({"Firing temperature": "1200-1240 C"}, jsonld.specification_table(document))

    def test_claims_always_carry_their_evidence(self):
        for claim in domain.claims("", "Sans plomb, apte au contact alimentaire."):
            self.assertTrue(claim["evidence"])
            self.assertEqual("published_text", claim["basis"])


class HostLimiterTests(unittest.TestCase):
    def test_no_delay_means_no_wait(self):
        """The default is to ask again as soon as the host has answered."""
        limiter = base.HostLimiter(0.0, 8, start=2)
        self.assertEqual(0.0, limiter.spacing("example.com"))
        self.assertEqual(0.0, limiter._jittered("example.com"))

    def test_slots_space_request_starts_when_a_delay_is_asked_for(self):
        """Two slots leaving 0.8 s each is a request start every 0.4 s."""
        limiter = base.HostLimiter(0.8, 4, start=2)
        self.assertAlmostEqual(0.4, limiter.spacing("example.com"))

    def test_a_configured_delay_is_a_floor(self):
        """A slow rate the operator asked for is never divided."""
        limiter = base.HostLimiter(0.8, 4)
        limiter.set_delay("https://example.com/x", 5.0)
        self.assertAlmostEqual(5.0, limiter.spacing("example.com"))
        # The strictest request wins, whichever arrives second.
        limiter.set_delay("https://example.com/y", 2.0)
        self.assertAlmostEqual(5.0, limiter.spacing("example.com"))

    def test_failure_halves_the_slots_and_success_earns_them_back(self):
        limiter = base.HostLimiter(0.8, 8, start=8)
        limiter.record_failure("https://example.com/x", 429)
        self.assertEqual(4, limiter.slots["example.com"])
        limiter.record_failure("https://example.com/x", 503)
        self.assertEqual(2, limiter.slots["example.com"])
        for _ in range(base.HostLimiter.RECOVERY):
            limiter.record_success("https://example.com/x")
        self.assertEqual(3, limiter.slots["example.com"])
        self.assertLessEqual(base.HostLimiter.RECOVERY, 4)

    def test_slots_never_fall_below_one(self):
        limiter = base.HostLimiter(0.8, 4, start=1)
        for _ in range(5):
            limiter.record_failure("https://example.com/x", 500)
        self.assertEqual(1, limiter.slots["example.com"])

    def test_a_failing_host_earns_a_gap_that_doubles_and_is_released(self):
        limiter = base.HostLimiter(0.0, 4, start=4)
        limiter.record_failure("https://example.com/x", 429)
        self.assertAlmostEqual(base.HostLimiter.BACKOFF_START, limiter.spacing("example.com"))
        limiter.record_failure("https://example.com/x", 429)
        self.assertAlmostEqual(base.HostLimiter.BACKOFF_START * 2, limiter.spacing("example.com"))
        # Recovering every lost slot spends the gap the failures earned.
        for _ in range(base.HostLimiter.RECOVERY * 4):
            limiter.record_success("https://example.com/x")
        self.assertEqual(4, limiter.slots["example.com"])
        self.assertEqual(0.0, limiter.spacing("example.com"))

    def test_backoff_stops_doubling_at_the_ceiling(self):
        limiter = base.HostLimiter(0.0, 4)
        for _ in range(20):
            limiter.record_failure("https://example.com/x", 503)
        self.assertAlmostEqual(base.HostLimiter.BACKOFF_MAX, limiter.spacing("example.com"))

    def test_hosts_on_a_shared_edge_are_paced_together(self):
        """A 429 from one Shopify shop slows every Shopify shop in this process.

        The failure this is for: five storefronts on five unrelated domains
        refusing their *first* request, seconds after two others on the same
        edge had been throttled part-way through pagination.
        """
        limiter = base.HostLimiter(0.0, 4, start=4)
        for host in ("hot-clay.com", "www.barro.ro"):
            limiter.join_group(f"https://{host}/", "edge:shopify")

        limiter.record_failure("https://hot-clay.com/products.json", 429)

        # The shop that has not been asked anything yet already waits.
        self.assertAlmostEqual(
            base.HostLimiter.BACKOFF_START, limiter.spacing("www.barro.ro")
        )
        # Its own concurrency is untouched: the edge meters the rate, and this
        # shop has not failed at anything.
        self.assertEqual(4, limiter.slots.get("www.barro.ro", 4))
        self.assertEqual(2, limiter.slots["hot-clay.com"])

    def test_a_host_outside_the_group_is_unaffected(self):
        limiter = base.HostLimiter(0.0, 4, start=4)
        limiter.join_group("https://hot-clay.com/", "edge:shopify")
        limiter.record_failure("https://hot-clay.com/products.json", 429)
        self.assertEqual(0.0, limiter.spacing("ceradel.fr"))

    def test_a_retry_after_from_one_shop_floors_the_whole_edge(self):
        """`set_delay` names a host; on a shared edge it means all of them."""
        limiter = base.HostLimiter(0.0, 4)
        for host in ("hot-clay.com", "mudaceramica.com"):
            limiter.join_group(f"https://{host}/", "edge:shopify")
        limiter.set_delay("https://hot-clay.com/products.json", 6.0)
        self.assertAlmostEqual(6.0, limiter.spacing("mudaceramica.com"))

    def test_a_published_crawl_delay_applies_only_after_a_failure(self):
        """A healthy host is crawled at our pace; a failing one gets its own."""
        limiter = base.HostLimiter(0.0, 4, start=2)
        limiter.remember_crawl_delay("https://example.com/x", 10.0)
        self.assertEqual(0.0, limiter.spacing("example.com"))
        limiter.record_failure("https://example.com/x", 429)
        self.assertAlmostEqual(10.0, limiter.spacing("example.com"))

    def test_jitter_stays_inside_its_band_and_above_a_floor(self):
        limiter = base.HostLimiter(1.0, 1)  # one slot, so the gap is the delay
        gaps = [limiter._jittered("example.com") for _ in range(200)]
        self.assertTrue(all(0.7 <= gap <= 1.3 for gap in gaps))
        self.assertGreater(len(set(gaps)), 1)
        limiter.set_delay("https://example.com/x", 2.0)
        floored = [limiter._jittered("example.com") for _ in range(200)]
        self.assertTrue(all(gap >= 2.0 for gap in floored))


class TitleTests(unittest.TestCase):
    """The published title, split into the product, its maker and its code."""

    def test_the_raw_title_is_kept_untouched(self):
        raw = "1050 UNDERGLAZE BASE size: $/GAL"
        parsed = domain.parse_title(raw, source_is_manufacturer=True, supplier_sku="1050")
        self.assertEqual(raw, parsed["name_raw"])
        self.assertEqual("UNDERGLAZE BASE", parsed["name"])

    def test_a_manufacturers_own_shop_numbers_its_own_products(self):
        parsed = domain.parse_title(
            "1050 UNDERGLAZE BASE size: $/GAL",
            source_brand="Spectrum", source_is_manufacturer=True, supplier_sku="1050",
        )
        self.assertEqual("1050", parsed["code"])
        self.assertEqual("manufacturer_shop", parsed["code_basis"])

    def test_a_retailers_article_number_is_never_a_manufacturer_code(self):
        """The same shape on a reseller's shelf means nothing to anyone else."""
        parsed = domain.parse_title(
            "011SF2502 - GRES BIANCO 11SF0-0,2 WITGERT", supplier_sku="011SF2502",
        )
        self.assertIsNone(parsed["code"])
        self.assertEqual("Witgert", parsed["brand"])

    def test_a_manufacturer_shop_reselling_another_maker_keeps_its_number(self):
        """SiO-2's article number for a Colorobbia engobe is SiO-2's, not Colorobbia's."""
        parsed = domain.parse_title(
            "COLOROBBIA HC-0607 engobe berenjena 59ml (2oz)",
            source_brand="SiO-2", source_is_manufacturer=True, supplier_sku="303020000607",
        )
        self.assertIsNone(parsed["code"])
        self.assertEqual("Colorobbia", parsed["brand"])

    def test_the_maker_named_in_the_title_beats_the_shops_own_label(self):
        parsed = domain.parse_title(
            "Émail à effets pour grès Amaco - KI18 Artic Blush - 472ml 472",
            published_brand="Harry-Ceradel", package={"value": 472.0},
        )
        self.assertEqual("AMACO", parsed["brand"])
        self.assertEqual("named_in_title", parsed["brand_basis"])
        self.assertEqual("KI18", parsed["code"])
        self.assertEqual("Artic Blush", parsed["name"])

    def test_an_appended_size_field_is_packaging_whatever_follows_it(self):
        """One shop writes "size: $/GAL", another "size: 473 ml"."""
        parsed = domain.parse_title("MBG051 Pumpkin – Coyote size: 473 ml")
        self.assertEqual("Pumpkin", parsed["name"])
        self.assertEqual("Coyote", parsed["brand"])

    def test_the_product_is_read_after_the_code_or_before_it(self):
        after = domain.parse_title("CR-61 Speckled Yellow | AMACO")
        self.assertEqual("Speckled Yellow", after["name"])
        before = domain.parse_title("Émail liquide Terra Color pour faïence Rose FG1061")
        self.assertEqual("FG1061", before["code"])
        self.assertEqual("Émail liquide Terra Color pour faïence Rose", before["name"])

    def test_a_trailing_number_is_only_dropped_when_it_is_the_pack(self):
        """"GRES BIANCO 11" is a product; "Cobalt 472" is a product and a pack."""
        kept = domain.parse_title("GRES BIANCO 11 WITGERT")
        self.assertIn("11", kept["name"])
        dropped = domain.parse_title(
            "Émail brillant Amaco – C20 Cobalt 472", package={"value": 472.0},
        )
        self.assertEqual("Cobalt", dropped["name"])

    def test_a_shop_number_that_is_only_a_database_row_is_rejected(self):
        self.assertFalse(domain.plausible_code("303020000607"))
        self.assertFalse(domain.plausible_code("SPECTRUM", brand="Spectrum"))
        self.assertFalse(domain.plausible_code("BASE"))
        self.assertTrue(domain.plausible_code("1050"))
        self.assertTrue(domain.plausible_code("PC-20"))

    def test_an_empty_parse_never_loses_the_title(self):
        parsed = domain.parse_title("472")
        self.assertEqual("472", parsed["name"])


class ClaimsTests(unittest.TestCase):
    SENTENCE = (
        "Deze glazuur is loodvrij en voedselveilig. "
        "Not dinnerware safe when applied too thickly. "
    )

    def test_the_sentence_around_the_wording_is_the_evidence(self):
        found = {claim["type"]: claim for claim in domain.claims(self.SENTENCE)}
        self.assertIn("lead_free", found)
        self.assertEqual(
            "Deze glazuur is loodvrij en voedselveilig.", found["lead_free"]["evidence"],
        )

    def test_negation_is_read_from_the_wording(self):
        found = {claim["type"]: claim for claim in domain.claims("Not dinnerware safe.")}
        self.assertFalse(found["food_contact_suitability"]["claim"])

    def test_claims_stay_linear_in_the_length_of_the_text(self):
        """A guard on the shape of the patterns, not on the speed of the machine.

        Written as one regex ending in `[^.!?\n]*` this was quadratic and cost
        about six milliseconds per record - ninety-five per cent of all parsing
        time. Ten times the text must cost roughly ten times as much, not a
        hundred, so the ratio is what is asserted.
        """
        short = "Loodvrij glazuur. " + "beschrijving " * 40
        long = "Loodvrij glazuur. " + "beschrijving " * 400
        short_time = min(timeit.repeat(lambda: domain.claims(short), number=20, repeat=3))
        long_time = min(timeit.repeat(lambda: domain.claims(long), number=20, repeat=3))
        self.assertLess(long_time, short_time * 40)


class ResponseCacheTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def cache(self, mode="auto", max_age=None):
        return cache_module.ResponseCache(self.directory.name, mode=mode, max_age=max_age)

    def entry(self, body="<html>hi</html>", url="https://example.com/p.html"):
        return cache_module.CachedResponse(
            status=200, url=url, body=body, headers={"content-type": "text/html"},
            fetched_at=time.time(),
        )

    def test_a_stored_response_comes_back_verbatim(self):
        store = self.cache()
        key = store.key("http", "https://example.com/p.html", method="GET")
        store.write(key, self.entry())
        found = store.read(key, "https://example.com/p.html")
        self.assertEqual("<html>hi</html>", found.body)
        self.assertEqual(1, store.hits)

    def test_the_key_separates_requests_that_differ(self):
        """A page fetched as a browser is not served to the research agent."""
        store = self.cache()
        url = "https://example.com/p.html"
        self.assertNotEqual(
            store.key("http", url, agent=True), store.key("http", url, agent=False),
        )
        self.assertNotEqual(store.key("http", url), store.key("render", url))

    def test_an_entry_older_than_the_max_age_is_a_miss(self):
        store = self.cache(max_age=60)
        key = store.key("http", "https://example.com/p.html")
        stale = self.entry()
        stale.fetched_at = time.time() - 3600
        store.write(key, stale)
        self.assertIsNone(store.read(key, "https://example.com/p.html"))
        self.assertEqual(1, store.misses)

    def test_refresh_ignores_what_is_stored(self):
        store = self.cache(mode="refresh")
        key = store.key("http", "https://example.com/p.html")
        store.write(key, self.entry())
        self.assertIsNone(store.read(key, "https://example.com/p.html"))

    def test_off_stores_nothing(self):
        store = self.cache(mode="off")
        key = store.key("http", "https://example.com/p.html")
        store.write(key, self.entry())
        self.assertFalse(any(Path(self.directory.name).rglob("*.json.gz")))

    def test_a_replay_gap_is_handled_like_any_blocked_fetch(self):
        self.assertTrue(issubclass(base.NotCached, base.Blocked))


class ConditionalRefreshTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.url = "https://example.test/product"

    def _cache(self):
        store = cache_module.ResponseCache(self.directory.name, mode="auto", max_age=1)
        key = store.key("http", self.url, method="GET", params=None, body=None, agent=False)
        store.write(key, cache_module.CachedResponse(
            status=200, url=self.url, body="previous body",
            headers={"content-type": "text/plain", "etag": '"v1"'},
            fetched_at=time.time() - 3600,
        ))
        return store

    def _fetcher(self, handler, *, stale_on_error=False):
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return client, base.Fetcher(
            client, base.HostLimiter(0, 1), base.BrowserRenderer(False), "never",
            cache=self._cache(), impersonate_policy="never", stale_on_error=stale_on_error,
        )

    async def test_stale_entry_is_revalidated_and_304_reuses_its_body(self):
        seen = []

        def handler(request):
            seen.append(request.headers.get("if-none-match"))
            return httpx.Response(304, headers={"etag": '"v1"'})

        client, fetcher = self._fetcher(handler)
        async with client:
            response = await fetcher.response(self.url)
        self.assertEqual("previous body", response.text)
        self.assertEqual(['"v1"'], seen)
        self.assertEqual(1, fetcher.stats.not_modified)
        self.assertEqual(len(b"previous body"), fetcher.stats.bytes_saved_304)
        self.assertEqual("fresh", response.extensions["catalogue_cache_provenance"])

    async def test_stale_fallback_is_explicit_for_transient_errors(self):
        async def no_wait(_seconds):
            return None

        client, fetcher = self._fetcher(
            lambda request: httpx.Response(503, text="temporary"), stale_on_error=True,
        )
        with unittest.mock.patch("asyncio.sleep", no_wait):
            async with client:
                response = await fetcher.response(self.url)
        self.assertEqual(200, response.status_code)
        self.assertTrue(response.extensions["catalogue_stale_on_error"])
        self.assertEqual("stale", response.extensions["catalogue_cache_provenance"])

    async def test_stale_fallback_is_off_by_default(self):
        async def no_wait(_seconds):
            return None

        client, fetcher = self._fetcher(lambda request: httpx.Response(503, text="temporary"))
        with unittest.mock.patch("asyncio.sleep", no_wait):
            async with client:
                with self.assertRaises(httpx.HTTPStatusError):
                    await fetcher.response(self.url)

    async def test_stale_fallback_never_masks_a_deterministic_404(self):
        client, fetcher = self._fetcher(
            lambda request: httpx.Response(404, text="gone"), stale_on_error=True,
        )
        async with client:
            with self.assertRaises(httpx.HTTPStatusError):
                await fetcher.response(self.url)


class ColourTests(unittest.TestCase):
    def test_supplier_attribute_wins(self):
        self.assertEqual(
            {"name": "Noir", "basis": "product_attribute"},
            domain.colour("EMAIL GRES NOIR EG140-05B DESTOCKAGE", "Noir"),
        )

    def test_code_prefix_is_stripped(self):
        self.assertEqual("Sea Blue", domain.colour("SW-140 Sea Blue")["name"])

    def test_a_named_container_is_not_part_of_the_colour(self):
        """AMACO titles the pack: "PC-2 Saturation Gold Gallon" is one colour."""
        self.assertEqual("Saturation Gold", domain.colour("PC-2 Saturation Gold Gallon")["name"])
        self.assertEqual("Blue Rutile", domain.colour("PC-20 Blue Rutile Pint")["name"])
        self.assertEqual("Obsidian", domain.colour("C-1 Obsidian 473 ml jar")["name"])

    def test_a_long_title_is_not_a_colour(self):
        self.assertIsNone(
            domain.colour("Penguin Pottery - Underglaze for Ceramics - Black - Cone 04 to Cone 6"),
        )


class EnrichmentSelectionTests(unittest.TestCase):
    """Derived fields belong to the sources that asked for them."""

    GLAZE: ClassVar[dict[str, object]] = dict(
        source="shop", product_url="https://example.test/p",
        name="Transparent gloss glaze 1020-1060°C 500ml",
        description="Brush on two coats. Food safe.",
        price=12.5, currency="EUR", extraction_method="api_json",
    )

    def build(self, sources, **overrides):
        with record_module.RecordBuilder(sources):
            return record_module.build(**{**self.GLAZE, **overrides})

    def test_a_source_that_selected_nothing_derives_nothing(self):
        """A potter's shop publishes pots; every inference over it is invented."""
        row = self.build({"shop": {"scope": "all"}})
        for field in ("family", "form", "firing", "surface", "colour", "coats", "package_size", "unit_price"):
            with self.subTest(field=field):
                self.assertIsNone(row[field], f"{field} was derived for a source that selected no enrichment")
        self.assertIsNone(row["effects"])
        self.assertIsNone(row["application_methods"])
        self.assertIsNone(row["claims"])
        # What the shop itself published is untouched by any of this.
        self.assertEqual(12.5, row["price"])
        self.assertEqual("Transparent gloss glaze 1020-1060°C 500ml", row["name_raw"])

    def test_the_bundle_fills_the_whole_block(self):
        row = self.build({"shop": {"scope": "materials", "enrichments": ["ceramic-materials"]}})
        self.assertEqual("glaze", row["family"])
        self.assertEqual("gloss", row["surface"])
        self.assertEqual(1020, row["firing"]["min_celsius"])
        self.assertEqual(500.0, row["package_size"]["millilitres"])
        self.assertEqual(25.0, row["unit_price"]["value"])
        self.assertTrue(row["claims"])

    def test_a_module_fills_its_own_fields_and_no_others(self):
        row = self.build({"shop": {"scope": "all", "enrichments": ["firing"]}})
        self.assertEqual(1020, row["firing"]["min_celsius"])
        self.assertIsNone(row["family"])
        self.assertIsNone(row["surface"])
        self.assertIsNone(row["package_size"])

    def test_packaging_brings_the_classifier_it_reads(self):
        """"1 pt" is a volume only for a liquid, and that is classification's answer."""
        self.assertEqual(
            ("classification", "packaging"), enrichment.selected("all", ["packaging"]),
        )

    def test_a_materials_source_classifies_whatever_it_selected(self):
        """Otherwise the scope filter has nothing to read and the dump is empty."""
        self.assertIn("classification", enrichment.selected("materials", None))
        self.assertEqual((), enrichment.selected("all", None))

    def test_a_module_fills_exactly_the_fields_it_declares(self):
        """The declaration is what a reader of the record contract goes by.

        A module that quietly fills a field it does not own makes a source's
        selection a lie, and a field no module owns can never be null-filled
        for a source that selected nothing.
        """
        context = enrichment.Context(name="Transparent gloss glaze 500ml")
        owned = set()
        for name, module in enrichment.MODULES.items():
            with self.subTest(module=name):
                produced = module.run(context, dict(enrichment.EMPTY))
                self.assertEqual(set(module.fields), set(produced))
                self.assertFalse(owned & set(module.fields), "two modules own one field")
                owned |= set(module.fields)
        self.assertEqual(set(enrichment.EMPTY), owned)

    def test_an_unknown_module_is_refused_by_name(self):
        with self.assertRaises(ValueError) as caught:
            enrichment.resolve(["glazes"])
        self.assertIn("glazes", str(caught.exception))


class RecordTests(unittest.TestCase):
    #: These rows are a supplier of ceramic materials, so the source selects the
    #: bundle; without it `build` fills the derived fields with nulls, which is
    #: the whole point of the selection and is asserted separately below.
    SOURCES: ClassVar[dict[str, dict[str, object]]] = {
        "test": {
            "label": "Test",
            "scope": "materials",
            "enrichments": ["ceramic-materials"],
        }
    }

    def setUp(self):
        self._builder = record_module.RecordBuilder(self.SOURCES)
        self._builder.bind()
        self.addCleanup(self._builder.unbind)

    def build(self, **overrides):
        defaults = dict(
            source="test", product_url="https://example.test/products/glaze",
            name="Transparent gloss glaze 500ml", price=12.5, currency="EUR",
            extraction_method="api_json",
        )
        return record_module.build(**{**defaults, **overrides})

    def test_variant_rows_share_a_clean_parent(self):
        row = self.build(
            product_url="https://example.test/p?attribute=25kg", variant_id="42", variant_title="25kg",
        )
        self.assertEqual("test:https://example.test/p#42", row["external_id"])
        self.assertEqual("test:https://example.test/p", row["parent_external_id"])

    def test_identity_rows_carry_no_price(self):
        row = self.build(identity_only=True, price=0.0)
        self.assertEqual(record_module.IDENTITY_FORMAT, row["format"])
        self.assertIsNone(row["price"])
        self.assertTrue(record_module.is_valid(row))

    def test_a_priced_row_needs_a_price(self):
        self.assertFalse(record_module.is_valid(self.build(price=None)))
        self.assertTrue(record_module.is_valid(self.build(price=0.0)))

    def test_out_of_stock_is_exactly_zero(self):
        row = self.build(availability="https://schema.org/OutOfStock")
        self.assertEqual(0, row["stock_quantity"])

    def test_negative_internal_inventory_is_not_published(self):
        row = self.build(stock_quantity=-8, availability="https://schema.org/InStock")
        self.assertIsNone(row["stock_quantity"])

    def test_derived_fields_are_populated(self):
        row = self.build(name="Emaux transparent brillant 1020-1060°C 500ml")
        self.assertEqual("glaze", row["family"])
        self.assertEqual("gloss", row["surface"])
        self.assertIn("transparent", row["effects"])
        self.assertEqual(1020, row["firing"]["min_celsius"])
        self.assertEqual(500.0, row["package_size"]["millilitres"])
        self.assertEqual(25.0, row["unit_price"]["value"])

    def test_variants_are_not_deduplicated_away(self):
        rows = [
            self.build(variant_id="1", variant_title="500ml", price=12.5),
            self.build(variant_id="2", variant_title="1L", price=22.0),
        ]
        keys = {record_module.dedupe_key(row) for row in rows}
        self.assertEqual(2, len(keys))

    def test_price_fingerprint_tracks_package_changes(self):
        first = self.build(variant_title="500ml")
        second = self.build(variant_title="1L")
        self.assertNotEqual(record_module.price_fingerprint(first), record_module.price_fingerprint(second))


class PriceParsingTests(unittest.TestCase):
    def test_separators_and_symbols(self):
        for text, expected in (
            ("1 234,56 €", (1234.56, "EUR")),
            ("$1,234.56", (1234.56, "USD")),
            ("24.3 EUR", (24.3, "EUR")),
            ("5,22€", (5.22, "EUR")),
        ):
            with self.subTest(text=text):
                self.assertEqual(expected, record_module.parse_price(text))

    def test_vat_wording(self):
        self.assertEqual("exclusive", record_module.vat_status("Prix HT"))
        self.assertEqual("inclusive", record_module.vat_status("Prix TTC"))
        self.assertIsNone(record_module.vat_status("12,50 €"))


class JsonLdTests(unittest.TestCase):
    DOCUMENT = """
      <script type="application/ld+json">
      {"@graph":[{"@type":"BreadcrumbList","itemListElement":[
        {"@type":"ListItem","item":{"name":"Glazes"}}]},
       {"@type":"Product","name":"C-1 Obsidian","sku":"C-1","gtin13":"1234567890123",
        "offers":{"price":"14.26","priceCurrency":"EUR","availability":"https://schema.org/InStock"}}]}
      </script>
      <table><tr><th>Firing</th><td>1220-1260°C</td></tr></table>
    """

    def test_products_are_selected_from_a_graph(self):
        found = jsonld.products(self.DOCUMENT)
        self.assertEqual(["C-1 Obsidian"], [item["name"] for item in found])

    def test_breadcrumbs_and_specification_table(self):
        self.assertEqual(["Glazes"], jsonld.breadcrumbs(self.DOCUMENT))
        self.assertEqual({"Firing": "1220-1260°C"}, jsonld.specification_table(self.DOCUMENT))

    def test_offer_and_gtin(self):
        item = jsonld.products(self.DOCUMENT)[0]
        self.assertEqual("14.26", jsonld.offer(item)["price"])
        self.assertEqual("1234567890123", jsonld.gtin(item))


class CategoryWalkTests(unittest.IsolatedAsyncioTestCase):
    """What a category page is read for: the products, and the next page.

    Both of the shops this was written for publish bare-slug product URLs, so
    the pattern that matches a product matches the category's own path — and
    the pagination link, which is that path plus a query.
    """

    def _scraper(self, handler, **config):
        from mb_ceramics_catalogue.scrapers.pagecrawl import PageScraper

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        fetcher = base.Fetcher(
            client, base.HostLimiter(0.0, 4), base.BrowserRenderer(False), "never",
            impersonate_policy="never",
        )
        return client, PageScraper("shop", {
            "url": "https://shop.test/", "scope": "all",
            "category_urls": ["https://shop.test/glazes"],
            "product_pattern": "^/[^/]+$", **config,
        }, fetcher)

    PAGE_ONE = (
        '<a href="/blue-glaze">Blue</a><a href="/green-glaze">Green</a>'
        '<a href="/glazes?pp=2">2</a>'
    )
    PAGE_TWO = '<a href="/red-glaze">Red</a>'

    async def test_pagination_is_walked_rather_than_read_as_a_product(self):
        def handler(request):
            path = request.url.path + ("?" + str(request.url.query.decode()) if request.url.query else "")
            return httpx.Response(200, text=self.PAGE_TWO if "pp=2" in path else self.PAGE_ONE)

        client, scraper = self._scraper(handler, pagination_patterns=["[?&]pp=\\d+"])
        async with client:
            found = await scraper.discover_from_categories()
        self.assertEqual(
            ["https://shop.test/blue-glaze", "https://shop.test/green-glaze",
             "https://shop.test/red-glaze"],
            found,
            "the second page's products are missing, or its link was scraped as one",
        )

    async def test_card_links_only_reads_a_nested_card_to_its_end(self):
        """The lazy match used to stop at the first `</div>`, before the link."""
        document = (
            '<a href="/basket">Basket</a>'
            '<div class="card product-box"><div class="thumb"><img/></div>'
            '<a href="/blue-glaze">Blue</a></div>'
        )
        client, scraper = self._scraper(
            lambda request: httpx.Response(200, text=document), card_links_only=True
        )
        async with client:
            found = await scraper.discover_from_categories()
        self.assertEqual(["https://shop.test/blue-glaze"], found)

    async def test_invalid_rows_are_not_counted_as_scope_filtered(self):
        client, scraper = self._scraper(lambda request: httpx.Response(200))
        scraper.config["scope"] = "materials"

        scraper.add({"name": "Clay without a price", "price": None})
        scraper.add({"name": "Electric kiln", "price": 1000.0})

        self.assertEqual(1, scraper.result.invalid)
        self.assertEqual(1, scraper.result.filtered)
        await client.aclose()


class JsonLdLeniencyTests(unittest.TestCase):
    """Storefronts publish JSON-LD that is not valid JSON, and it still counts.

    ceramiq-pl wrote a five-line description straight into the string, which is
    a control character where JSON allows none. A strict parse dropped the whole
    block, so the shop published a complete Product on every page and was read
    as having none for months.
    """

    DOCUMENT = (
        '<script type="application/ld+json">'
        '{"@type":"Product","name":"Masa ceramiczna","sku":"5335",'
        '"description":"Masa z szamotem.\n Kurczliwosc 3,8%\n",'
        '"offers":{"@type":"Offer","price":"25.70","priceCurrency":"PLN"}}'
        "</script>"
    )

    def test_a_raw_newline_inside_a_string_does_not_lose_the_product(self):
        found = jsonld.products(self.DOCUMENT)
        self.assertEqual(["Masa ceramiczna"], [item["name"] for item in found])
        self.assertEqual("25.70", jsonld.offer(found[0])["price"])

    def test_markup_that_is_not_json_at_all_is_still_skipped(self):
        self.assertEqual([], jsonld.products(
            '<script type="application/ld+json">not json</script>'
        ))


class MicrodataPriceTests(unittest.TestCase):
    """A price rendered inside the product scope but never marked up.

    KQS.store publishes sku, name, brand and image as microdata and leaves the
    price in plain markup by the add-to-cart button. Every artequipment row came
    back priced `None` and was rejected as invalid.
    """

    def _product(self, inner: str) -> str:
        return (
            '<div itemscope itemtype="https://schema.org/Product">'
            '<h1 itemprop="name">AMACO Celadon C-26 Lagoon</h1>'
            f"{inner}</div>"
        )

    def test_the_unmarked_price_beside_the_product_is_read(self):
        document = self._product('<div class="m-price">Cena: <strong>79,00 zl z VAT</strong></div>')
        offer = microdata.products(document)[0]["offers"]
        self.assertEqual({"price": "79,00", "priceCurrency": "PLN"}, offer)

    def test_a_price_outside_the_product_scope_is_not_this_product_s(self):
        document = self._product("") + '<div class="m-price"><strong>12,00 zl</strong></div>'
        self.assertNotIn("offers", microdata.products(document)[0])

    def test_a_number_with_no_currency_is_not_a_price(self):
        document = self._product('<div class="price-and-stock">6</div>')
        self.assertNotIn("offers", microdata.products(document)[0])

    def test_an_unrelated_number_before_the_price_is_skipped(self):
        document = self._product(
            '<div class="m-price">Pack 2, price <strong>79,00 zl</strong></div>'
        )
        offer = microdata.products(document)[0]["offers"]
        self.assertEqual({"price": "79,00", "priceCurrency": "PLN"}, offer)

    def test_a_marked_up_offer_is_never_second_guessed(self):
        document = self._product(
            '<div itemprop="offers" itemscope itemtype="https://schema.org/Offer">'
            '<meta itemprop="price" content="8.50"/></div>'
            '<div class="m-price"><strong>79,00 zl</strong></div>'
        )
        self.assertEqual("8.50", microdata.products(document)[0]["offers"]["price"])


class ImpersonationLadderTests(unittest.IsolatedAsyncioTestCase):
    """The three rungs of `Fetcher.response` when a host says 403.

    The order matters and is cheapest-first: the declared research agent, then a
    browser User-Agent, and only then a browser TLS handshake, which costs a
    thread and an optional dependency.
    """

    def _fetcher(self, handler, *, impersonator=None, policy="auto"):
        transport = httpx.MockTransport(handler)
        client = httpx.AsyncClient(transport=transport, headers={"user-agent": base.USER_AGENT})
        return client, base.Fetcher(
            client,
            base.HostLimiter(0.0, 4),
            base.BrowserRenderer(False),
            "never",
            impersonate_policy=policy,
            impersonator=impersonator,
        )

    async def test_a_browser_user_agent_is_tried_before_the_handshake(self):
        seen = []

        def handler(request):
            seen.append(request.headers.get("user-agent"))
            if request.headers.get("user-agent") == base.BROWSER_USER_AGENT:
                return httpx.Response(200, text="served")
            return httpx.Response(403, text="refused")

        impersonator = _NeverCalled()
        client, fetcher = self._fetcher(handler, impersonator=impersonator)
        async with client:
            self.assertEqual("served", await fetcher.text("https://example.test/x"))
        self.assertEqual([base.USER_AGENT, base.BROWSER_USER_AGENT], seen)
        self.assertFalse(impersonator.called, "the handshake rung must not be reached")

    async def test_the_handshake_is_used_when_headers_are_not_enough(self):
        impersonator = _Serves(httpx.Response(200, text="handshake"))
        client, fetcher = self._fetcher(
            lambda request: httpx.Response(403, text="refused"), impersonator=impersonator,
        )
        async with client:
            self.assertEqual("handshake", await fetcher.text("https://example.test/x"))
        self.assertTrue(impersonator.called)

    async def test_the_site_s_refusal_survives_when_the_handshake_also_fails(self):
        """The caller must see the host's 403, not a complaint about our tooling."""
        impersonator = _Serves(httpx.Response(403, text="still refused"))
        client, fetcher = self._fetcher(
            lambda request: httpx.Response(403, text="refused"), impersonator=impersonator,
        )
        async with client:
            with self.assertRaises(httpx.HTTPStatusError) as raised:
                await fetcher.text("https://example.test/x")
        self.assertEqual(403, raised.exception.response.status_code)

    async def test_a_missing_dependency_is_not_an_error_of_its_own(self):
        impersonator = _Raises(ImportError("no curl_cffi here"))
        client, fetcher = self._fetcher(
            lambda request: httpx.Response(403, text="refused"), impersonator=impersonator,
        )
        async with client:
            with self.assertRaises(httpx.HTTPStatusError):
                await fetcher.text("https://example.test/x")

    async def test_never_skips_the_rung_entirely(self):
        impersonator = _NeverCalled()
        client, fetcher = self._fetcher(
            lambda request: httpx.Response(403, text="refused"),
            impersonator=impersonator, policy="never",
        )
        impersonator.enabled = False
        async with client:
            with self.assertRaises(httpx.HTTPStatusError):
                await fetcher.text("https://example.test/x")
        self.assertFalse(impersonator.called)


class ProxyFallbackRoutingTests(unittest.IsolatedAsyncioTestCase):
    def fetchers(self, direct_handler, proxy_handler):
        direct_client = httpx.AsyncClient(transport=httpx.MockTransport(direct_handler))
        proxy_client = httpx.AsyncClient(transport=httpx.MockTransport(proxy_handler))
        limiter = base.HostLimiter(0, 2)
        proxied = base.Fetcher(
            proxy_client, limiter, base.BrowserRenderer(False), "never",
            impersonate_policy="never",
        )
        direct = base.Fetcher(
            direct_client, limiter, base.BrowserRenderer(False), "never",
            impersonate_policy="never", proxy_fallback=proxied,
        )
        return direct_client, proxy_client, direct

    async def test_classified_403_uses_proxy_only_after_direct_ladder(self):
        direct_calls = proxy_calls = 0

        def direct_handler(request):
            nonlocal direct_calls
            direct_calls += 1
            return httpx.Response(403, text="refused")

        def proxy_handler(request):
            nonlocal proxy_calls
            proxy_calls += 1
            return httpx.Response(200, text="served")

        direct_client, proxy_client, fetcher = self.fetchers(direct_handler, proxy_handler)
        async with direct_client, proxy_client:
            self.assertEqual("served", await fetcher.text("https://shop.test/p"))
        self.assertEqual(2, direct_calls, "research and browser user agents precede proxying")
        self.assertEqual(1, proxy_calls)

    async def test_definitive_429_uses_the_proxy_fallback_immediately(self):
        direct_calls = proxy_calls = 0

        def direct_handler(request):
            nonlocal direct_calls
            direct_calls += 1
            return httpx.Response(429, text="slow down")

        def proxy_handler(request):
            nonlocal proxy_calls
            proxy_calls += 1
            return httpx.Response(200, text="served")

        direct_client, proxy_client, fetcher = self.fetchers(direct_handler, proxy_handler)
        with unittest.mock.patch("asyncio.sleep", unittest.mock.AsyncMock()):
            async with direct_client, proxy_client:
                self.assertEqual("served", await fetcher.text("https://shop.test/p"))
        self.assertEqual(1, direct_calls)
        self.assertEqual(1, proxy_calls)

    async def test_retry_after_503_is_a_rate_limit_proxy_fallback(self):
        direct_calls = proxy_calls = 0

        def direct_handler(request):
            nonlocal direct_calls
            direct_calls += 1
            return httpx.Response(503, text="edge throttle", headers={"retry-after": "1"})

        def proxy_handler(request):
            nonlocal proxy_calls
            proxy_calls += 1
            return httpx.Response(200, text="served")

        direct_client, proxy_client, fetcher = self.fetchers(direct_handler, proxy_handler)
        with unittest.mock.patch("asyncio.sleep", unittest.mock.AsyncMock()):
            async with direct_client, proxy_client:
                self.assertEqual("served", await fetcher.text("https://shop.test/p"))
        self.assertEqual(1, direct_calls)
        self.assertEqual(1, proxy_calls)

    async def test_rate_limit_opens_a_short_direct_route_circuit(self):
        direct_calls = proxy_calls = 0

        def direct_handler(request):
            nonlocal direct_calls
            direct_calls += 1
            return httpx.Response(429, text="slow down")

        def proxy_handler(request):
            nonlocal proxy_calls
            proxy_calls += 1
            return httpx.Response(200, text="served")

        direct_client, proxy_client, fetcher = self.fetchers(direct_handler, proxy_handler)
        async with direct_client, proxy_client:
            self.assertEqual("served", await fetcher.text("https://shop.test/one"))
            self.assertEqual("served", await fetcher.text("https://shop.test/two"))
        self.assertEqual(1, direct_calls)
        self.assertEqual(2, proxy_calls)

    async def test_plain_503_does_not_spend_proxy_traffic(self):
        proxy_calls = 0

        def proxy_handler(request):
            nonlocal proxy_calls
            proxy_calls += 1
            return httpx.Response(200, text="must not happen")

        direct_client, proxy_client, fetcher = self.fetchers(
            lambda request: httpx.Response(503, text="origin unavailable"), proxy_handler,
        )
        with unittest.mock.patch("asyncio.sleep", unittest.mock.AsyncMock()):
            async with direct_client, proxy_client:
                with self.assertRaises(httpx.HTTPStatusError):
                    await fetcher.text("https://shop.test/p")
        self.assertEqual(0, proxy_calls)

    async def test_exhausted_proxy_keeps_usage_in_the_source_summary(self):
        from mb_ceramics_catalogue.proxy import ProxyDenied

        class ExhaustedScraper(base.Scraper):
            async def scrape(self, limit=None):
                self.fetcher.stats.proxy_requests = 14
                raise ProxyDenied("job proxy reservation is exhausted")

        client = httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text="unused")
        ))
        fetcher = base.Fetcher(
            client, base.HostLimiter(0, 1), base.BrowserRenderer(False), "never",
            impersonate_policy="never",
        )
        fetcher.may_fetch = unittest.mock.AsyncMock(return_value=True)
        fetcher.proxy_lease = SimpleNamespace(max_bytes=25_000_000, used_bytes=26_060_082)
        scraper = ExhaustedScraper(
            "shop", {"url": "https://shop.test/", "scope": "all"}, fetcher,
        )
        async with client:
            result = await scraper.run()
        self.assertEqual(14, result.proxy_requests)
        self.assertEqual(25_000_000, result.proxy_bytes_reserved)
        self.assertEqual(26_060_082, result.proxy_bytes_estimated)
        self.assertTrue(result.truncated)
        self.assertIn("reservation is exhausted", result.errors[0]["error"])

    async def test_proxy_meter_counts_compressed_transfer_not_decoded_html(self):
        body = b"<html>" + (b"repeated catalogue markup " * 10_000) + b"</html>"
        transferred = gzip.compress(body)

        def handler(request):
            return httpx.Response(
                200,
                stream=httpx.ByteStream(transferred),
                headers={"content-encoding": "gzip", "content-type": "text/html"},
            )

        class Lease:
            used_bytes = 0
            requests = 0
            url = "http://proxy.test:8080"

            def ensure_request_allowed(self):
                return None

            def account(self, tx, rx, requests=1):
                self.used_bytes += tx + rx
                self.requests += requests

        lease = Lease()
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        fetcher = base.Fetcher(
            client, base.HostLimiter(0, 1), base.BrowserRenderer(False), "never",
            impersonate_policy="never", proxy_lease=lease,
        )
        async with client:
            self.assertEqual(body.decode(), await fetcher.text("https://shop.test/product/1"))
        self.assertEqual(1, lease.requests)
        self.assertLess(lease.used_bytes, len(body) // 10)
        self.assertGreaterEqual(lease.used_bytes, len(transferred))

    async def test_deterministic_404_never_uses_proxy(self):
        proxy_calls = 0

        def proxy_handler(request):
            nonlocal proxy_calls
            proxy_calls += 1
            return httpx.Response(200, text="must not happen")

        direct_client, proxy_client, fetcher = self.fetchers(
            lambda request: httpx.Response(404, text="gone"), proxy_handler,
        )
        async with direct_client, proxy_client:
            with self.assertRaises(httpx.HTTPStatusError):
                await fetcher.text("https://shop.test/gone")
        self.assertEqual(0, proxy_calls)

    async def test_a_200_block_page_is_a_classified_proxy_fallback(self):
        proxy_calls = 0

        def proxy_handler(request):
            nonlocal proxy_calls
            proxy_calls += 1
            return httpx.Response(200, text="real product")

        direct_client, proxy_client, fetcher = self.fetchers(
            lambda request: httpx.Response(
                200, text="<html><title>Access denied</title></html>",
                headers={"content-type": "text/html"},
            ),
            proxy_handler,
        )
        async with direct_client, proxy_client:
            self.assertEqual("real product", await fetcher.text("https://shop.test/p"))
        self.assertEqual(1, proxy_calls)


class _Stub:
    def __init__(self):
        self.called = False
        self.enabled = True

    @property
    def available(self):
        return self.enabled


class _NeverCalled(_Stub):
    async def request(self, url, **kwargs):
        self.called = True
        raise AssertionError("the handshake rung should not have been reached")


class _Serves(_Stub):
    def __init__(self, response):
        super().__init__()
        self.response = response

    async def request(self, url, **kwargs):
        self.called = True
        # The real client always attaches the request it made, and
        # `raise_for_status` needs it; a stub that omits it tests nothing real.
        self.response._request = httpx.Request(kwargs.get("method", "GET"), url)
        return self.response


class _Raises(_Stub):
    def __init__(self, error):
        super().__init__()
        self.error = error

    async def request(self, url, **kwargs):
        self.called = True
        raise self.error


class TruncationTests(unittest.IsolatedAsyncioTestCase):
    """A run that stopped early must say so, because retirement reads the flag.

    `plan_load` refuses to retire against a dump marked `truncated`. The inverse
    is the whole risk: a dump that stopped at page 8 of 14 and reports complete
    invites the loader to withdraw the six pages it never saw.
    """

    def _shopify(self, handler):
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        fetcher = base.Fetcher(
            client, base.HostLimiter(0.0, 4), base.BrowserRenderer(False), "never",
            impersonate_policy="never",
        )
        from mb_ceramics_catalogue.scrapers.shopify import ShopifyScraper

        config = {"url": "https://shop.test/", "scope": "all", "vat_status": "exclusive"}
        return client, ShopifyScraper("shop", config, fetcher)

    @staticmethod
    def _product(index):
        return {
            "handle": f"p{index}", "title": f"Glaze {index}", "vendor": "Mayco",
            "variants": [{"id": index, "price": "10.00", "available": True, "title": "Default Title"}],
        }

    async def test_a_429_partway_through_marks_the_dump_truncated(self):
        # The retry ladder waits 1s, 2s then 4s before giving up on a 429, which
        # is right in a run and seven wasted seconds in the fast suite.
        async def _no_wait(_seconds):
            return None

        def handler(request):
            if request.url.path == "/meta.json":
                return httpx.Response(200, json={"currency": "USD"})
            if request.url.params.get("page") == "1":
                return httpx.Response(200, json={"products": [self._product(i) for i in range(250)]})
            return httpx.Response(429, json={})

        client, scraper = self._shopify(handler)
        with unittest.mock.patch("asyncio.sleep", _no_wait):
            async with client:
                result = await scraper.scrape()
        self.assertTrue(result.truncated, "a refused page of pagination is not a complete catalogue")
        self.assertEqual(250, len(result.records))
        self.assertEqual(1, len(result.errors))

    async def test_a_short_last_page_is_complete_not_truncated(self):
        def handler(request):
            if request.url.path == "/meta.json":
                return httpx.Response(200, json={"currency": "USD"})
            page = request.url.params.get("page")
            products = [self._product(i) for i in range(250)] if page == "1" else [self._product(999)]
            return httpx.Response(200, json={"products": products})

        client, scraper = self._shopify(handler)
        async with client:
            result = await scraper.scrape()
        self.assertFalse(result.truncated, "reaching the end of the pages is not truncation")
        self.assertEqual(251, len(result.records))

    async def test_a_shop_with_no_readable_currency_publishes_no_price(self):
        """products.json states an amount and never says what it is in.

        The currency comes from meta.json, and when that request fails the
        amount is not a weaker fact, it is a meaningless one. Emitting it anyway
        produced rows `catalogue.load_record` refuses — and, because the refusal
        used to abort the source's whole load, one such row cost
        art-academy-direct all 4,980 of its records on 2026-08-11.
        """
        def handler(request):
            if request.url.path == "/meta.json":
                return httpx.Response(500, json={})
            page = request.url.params.get("page")
            return httpx.Response(200, json={"products": [self._product(1)] if page == "1" else []})

        client, scraper = self._shopify(handler)
        async with client:
            result = await scraper.scrape()

        # `record.is_valid` refuses a priced row with no price, so the variant
        # is dropped rather than published as a bare number — and the source
        # says why, which is what turns an empty result into a diagnosis. The
        # job then fails on `runner.barren` rather than reporting a green zero.
        self.assertEqual([], result.records)
        self.assertEqual(1, result.discovered)
        self.assertTrue(
            any("currency could not be read" in note for note in result.notes), result.notes
        )


class BrowserRoutingTests(unittest.IsolatedAsyncioTestCase):
    """A process with no browser must reroute the job, not lose the page.

    `BrowserUnavailable` is deliberately not a `Blocked`: a Blocked is the site
    refusing this page, which a source records and carries on from. This is the
    image being wrong for the job, and it is equally true of every remaining
    page, so it has to escape to the worker that can requeue it.
    """

    def _scraper(self, handler):
        from mb_ceramics_catalogue.scrapers.pagecrawl import PageScraper

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        fetcher = base.Fetcher(
            client, base.HostLimiter(0.0, 4), base.BrowserRenderer(True), "auto",
            impersonate_policy="never",
        )
        config = {"url": "https://shop.test/", "scope": "all"}
        return client, PageScraper("shop", config, fetcher)

    async def test_a_missing_browser_escapes_the_page_handler(self):
        async def no_browser(*args, **kwargs):
            raise base.BrowserUnavailable("camoufox is not installed")

        client, scraper = self._scraper(lambda request: httpx.Response(403, text="refused"))
        scraper.fetcher.browser.render = no_browser
        async with client:
            with self.assertRaises(base.BrowserUnavailable):
                await scraper.load("https://shop.test/product/1")
        self.assertEqual([], scraper.result.errors, "a routing fault is not the source's failure")

    async def test_an_ordinary_browser_error_is_still_recorded_and_survived(self):
        async def broken(*args, **kwargs):
            raise RuntimeError("the page crashed the renderer")

        client, scraper = self._scraper(lambda request: httpx.Response(403, text="refused"))
        scraper.fetcher.browser.render = broken
        async with client:
            self.assertIsNone(await scraper.load("https://shop.test/product/1"))
        self.assertEqual(1, len(scraper.result.errors))

    async def test_a_render_is_paced_by_the_host_limiter(self):
        """A rendered page is a request to someone's shop like any other.

        It was the one kind that skipped the limiter entirely: no slot, no gap,
        no backoff, no published Crawl-delay. That was survivable only because
        `BrowserRenderer` held a single lock across a whole page load, so a
        process rendered one page at a time by accident. Raising the page limit
        turned the accident into two unpaced requests at one shop.
        """
        client, scraper = self._scraper(lambda request: httpx.Response(403, text="refused"))
        limiter = scraper.fetcher.limiter
        in_flight = peak = 0

        async def render(url, *args, **kwargs):
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.01)
            in_flight -= 1
            return "<html></html>"

        scraper.fetcher.browser.render = render
        limiter.slots["shop.test"] = 1
        async with client:
            await asyncio.gather(*[
                scraper.fetcher.render(f"https://shop.test/p/{index}") for index in range(4)
            ])
        self.assertEqual(1, peak, "the host's slot count has to bound renders too")

    async def test_a_refused_render_teaches_the_limiter(self):
        """A timeout in the browser says what the host wants; it has to land."""
        async def refused(*args, **kwargs):
            raise RuntimeError("Page.goto: Timeout 45000ms exceeded")

        client, scraper = self._scraper(lambda request: httpx.Response(403, text="refused"))
        scraper.fetcher.browser.render = refused
        scraper.fetcher.limiter.slots["shop.test"] = 4
        async with client:
            with self.assertRaises(RuntimeError):
                await scraper.fetcher.render("https://shop.test/p/1")
        self.assertEqual(2, scraper.fetcher.limiter.slots["shop.test"], "slots halve on a refusal")
        self.assertGreater(scraper.fetcher.limiter.spacing("shop.test"), 0.0)


class BrowserFallbackClassificationTests(unittest.IsolatedAsyncioTestCase):
    class Fetcher:
        browser_policy = "auto"

        def __init__(self, document, rendered="<html><body>still empty</body></html>"):
            self.document = document
            self.rendered = rendered
            self.renders = 0

        async def may_fetch(self, *args, **kwargs):
            return True

        async def text(self, *args, **kwargs):
            return self.document

        async def render(self, *args, **kwargs):
            self.renders += 1
            return self.rendered

    def scraper(self, fetcher, count=1):
        from mb_ceramics_catalogue.scrapers.pagecrawl import PageScraper

        scraper = PageScraper(
            "shop", {"url": "https://shop.test", "scope": "all", "product_concurrency": 20},
            fetcher,
        )

        async def discover(limit=None):
            return [f"https://shop.test/product/{index}" for index in range(count)]

        scraper.discover = discover
        return scraper

    async def test_ordinary_unsupported_html_does_not_trigger_a_browser(self):
        fetcher = self.Fetcher("<html><body><h1>Unsupported product page</h1></body></html>")
        result = await self.scraper(fetcher).scrape()
        self.assertEqual([], result.records)
        self.assertEqual(0, fetcher.renders)

    async def test_an_explicit_javascript_shell_can_trigger_a_browser(self):
        fetcher = self.Fetcher(
            '<!doctype html><html><body><div id="app"></div><script src="app.js"></script></body></html>'
        )
        result = await self.scraper(fetcher).scrape()
        self.assertEqual(1, fetcher.renders)
        self.assertEqual(1, result.browser_zero_gain)

    async def test_ten_consecutive_zero_gain_renders_open_the_circuit_exactly(self):
        fetcher = self.Fetcher(
            '<html><body><div id="root"></div><script src="bundle.js"></script></body></html>'
        )
        result = await self.scraper(fetcher, count=25).scrape()
        self.assertEqual(10, fetcher.renders)
        self.assertEqual(10, result.browser_zero_gain)
        self.assertIn("browser_fallback_no_gain", " ".join(result.notes))


class RenderPolicyTests(unittest.IsolatedAsyncioTestCase):
    """`render: false` declines the browser rather than being merely unset.

    An unset `render` leaves the fallback available, and one page that parses to
    nothing then sends the entire source to the browser worker and restarts it
    there. For a source measured to gain nothing from rendering that is a large
    bill for no rows, so declining has to be sayable.
    """

    def _scraper(self, render):
        from mb_ceramics_catalogue.scrapers.pagecrawl import PageScraper

        client = httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(403, text="refused")))
        fetcher = base.Fetcher(
            client, base.HostLimiter(0.0, 4), base.BrowserRenderer(True), "auto",
            impersonate_policy="never",
        )
        config = {"url": "https://shop.test/", "scope": "all"}
        if render is not None:
            config["render"] = render
        return client, PageScraper("shop", config, fetcher)

    async def test_declining_never_reaches_the_browser(self):
        async def must_not_render(*args, **kwargs):
            raise AssertionError("a source that declined rendering asked for it anyway")

        client, scraper = self._scraper(False)
        scraper.fetcher.browser.render = must_not_render
        async with client:
            self.assertIsNone(await scraper.load("https://shop.test/product/1"))
            self.assertIsNone(await scraper.load("https://shop.test/product/1", render=True))
        self.assertEqual(1, len(scraper.result.errors), "the refusal is still recorded once")

    async def test_declining_browser_still_allows_an_approved_proxy_fallback(self):
        direct_client = httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                text="<html><title>Access denied</title></html>",
                headers={"content-type": "text/html"},
            )
        ))
        proxy_calls = 0

        def proxy_handler(request):
            nonlocal proxy_calls
            proxy_calls += 1
            return httpx.Response(200, text="<html>served through proxy</html>")

        proxy_client = httpx.AsyncClient(transport=httpx.MockTransport(proxy_handler))
        limiter = base.HostLimiter(0.0, 1)
        proxied = base.Fetcher(
            proxy_client, limiter, base.BrowserRenderer(False), "never",
            impersonate_policy="never",
        )
        fetcher = base.Fetcher(
            direct_client, limiter, base.BrowserRenderer(False), "auto",
            impersonate_policy="never", proxy_fallback=proxied,
        )
        from mb_ceramics_catalogue.scrapers.pagecrawl import PageScraper

        scraper = PageScraper(
            "shop", {"url": "https://shop.test/", "scope": "all", "render": False},
            fetcher,
        )

        async def must_not_render(*args, **kwargs):
            raise AssertionError("a source that declined rendering asked for it anyway")

        scraper.fetcher.browser.render = must_not_render
        async with direct_client, proxy_client:
            document = await scraper.load("https://shop.test/product/1")
        self.assertEqual("<html>served through proxy</html>", document)
        self.assertEqual(1, proxy_calls)
        self.assertEqual([], scraper.result.errors)

    async def test_leaving_it_unset_still_escalates(self):
        called = []

        async def render(*args, **kwargs):
            called.append(args)
            return "<html>rendered</html>"

        client, scraper = self._scraper(None)
        scraper.fetcher.browser.render = render
        async with client:
            self.assertEqual("<html>rendered</html>", await scraper.load("https://shop.test/p/1"))
        self.assertEqual(1, len(called))


class RateLimitHeaderTests(unittest.TestCase):
    """Pace from what the host publishes, before it has to refuse anything."""

    def setUp(self):
        self.limiter = base.HostLimiter(0.0, 8)
        self.url = "https://shop.test/products.json"

    def test_plenty_of_headroom_sets_no_gap(self):
        self.limiter.observe_headers(self.url, {"x-ratelimit-limit": "40", "x-ratelimit-remaining": "38"})
        self.assertEqual(0.0, self.limiter.spacing("shop.test"))

    def test_a_nearly_spent_budget_spreads_what_is_left_over_the_window(self):
        self.limiter.observe_headers(self.url, {
            "x-ratelimit-limit": "40", "x-ratelimit-remaining": "4", "x-ratelimit-reset": "20",
        })
        self.assertAlmostEqual(5.0, self.limiter.spacing("shop.test"))

    def test_shopify_writes_the_same_thing_as_used_over_total(self):
        self.limiter.observe_headers(self.url, {"x-shopify-shop-api-call-limit": "38/40"})
        # Two calls left of forty, and no window given, so the default minute
        # spread over them is 30s — clamped to BACKOFF_MAX, because a header is
        # a reason to slow down and never a reason to stall a run outright.
        self.assertEqual(base.HostLimiter.BACKOFF_MAX, self.limiter.spacing("shop.test"))

    def test_a_429_turns_retry_after_into_a_lasting_floor(self):
        self.limiter.observe_headers(self.url, {"x-ratelimit-limit": "40", "x-ratelimit-remaining": "0"})
        self.assertGreater(self.limiter.spacing("shop.test"), 0.0)

    def test_a_configured_delay_is_never_lowered_by_a_generous_host(self):
        self.limiter.set_delay(self.url, 2.0)
        self.limiter.observe_headers(self.url, {"x-ratelimit-limit": "40", "x-ratelimit-remaining": "40"})
        self.assertEqual(2.0, self.limiter.spacing("shop.test"))

    def test_nonsense_headers_are_ignored_rather_than_raising(self):
        self.limiter.observe_headers(self.url, {"x-ratelimit-limit": "many", "x-ratelimit-remaining": ""})
        self.limiter.observe_headers(self.url, {"x-shopify-shop-api-call-limit": "not/a/number"})
        self.assertEqual(0.0, self.limiter.spacing("shop.test"))


class BlockPageTests(unittest.TestCase):
    """A refusal served with HTTP 200 must not read as an empty page.

    theceramicshop.com answers every URL with a 2,471-byte "403 Forbidden"
    document and a 200 status. Nothing keyed on the status noticed, the crawl
    discovered nothing, and the source reported success — with `truncated`
    false, which is exactly the shape that invites retirement of a live
    catalogue.
    """

    def test_a_403_document_served_as_200_is_recognised(self):
        body = "<html><head><title>403 Forbidden</title></head><body>no</body></html>"
        self.assertIsNotNone(base.looks_like_a_block(body, "text/html"))

    def test_the_usual_interstitials_are_recognised(self):
        for title in ("Just a moment...", "Attention Required! | Cloudflare",
                      "Access denied", "Your access to this site has been limited"):
            with self.subTest(title=title):
                body = f"<html><head><title>{title}</title></head><body></body></html>"
                self.assertIsNotNone(base.looks_like_a_block(body, "text/html"))

    def test_a_real_page_about_the_subject_is_not_a_block(self):
        """The phrases are ordinary; only a title plus a small body is a refusal."""
        body = (
            "<html><head><title>Cloudflare for Potters, a book</title></head><body>"
            + "Access denied is a phrase discussed at length. " * 40
            + "</body></html>"
        )
        self.assertIsNone(base.looks_like_a_block(body, "text/html"))

    def test_a_large_document_is_never_a_block_page(self):
        body = "<html><head><title>403 Forbidden</title></head><body>" + ("x" * 40_000) + "</body></html>"
        self.assertIsNone(base.looks_like_a_block(body, "text/html"))

    def test_non_html_is_left_alone(self):
        self.assertIsNone(base.looks_like_a_block('{"title": "403 Forbidden"}', "application/json"))

    def test_a_page_with_no_title_is_left_alone(self):
        self.assertIsNone(base.looks_like_a_block("<html><body>403 Forbidden</body></html>", "text/html"))


class EnumerationInvariantTests(unittest.IsolatedAsyncioTestCase):
    """A failure while listing truncates; a failure reading one product does not.

    The point of the phase is that a scraper gets this right without knowing the
    rule exists, so the first test writes a new scraper the way someone would
    tomorrow — an overridden `discover` with a plain `self.fail` and no mention
    of truncation anywhere — and expects the safe answer regardless.
    """

    def _fetcher(self, handler):
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return client, base.Fetcher(
            client, base.HostLimiter(0.0, 4), base.BrowserRenderer(False), "never",
            impersonate_policy="never",
        )

    async def test_a_new_scraper_that_says_nothing_still_reports_truncation(self):
        from mb_ceramics_catalogue.scrapers.pagecrawl import PageScraper

        class NewShopScraper(PageScraper):
            """Written by someone who has never heard of `truncated`."""

            async def discover(self, limit=None):
                document = await self.load("https://shop.test/catalogue")
                if document is None:
                    return []
                return ["https://shop.test/product/1"]

        async def _no_wait(_seconds):
            return None

        client, fetcher = self._fetcher(lambda request: httpx.Response(500, text="boom"))
        scraper = NewShopScraper("shop", {"url": "https://shop.test/", "scope": "all"}, fetcher)
        with unittest.mock.patch("asyncio.sleep", _no_wait):
            async with client:
                result = await scraper.scrape()
        self.assertTrue(result.truncated, "a listing that could not be read is not a whole catalogue")

    async def test_one_bad_product_page_does_not_truncate(self):
        from mb_ceramics_catalogue.scrapers.pagecrawl import PageScraper

        class NewShopScraper(PageScraper):
            async def discover(self, limit=None):
                return ["https://shop.test/product/1", "https://shop.test/product/2"]

        def handler(request):
            if request.url.path.endswith("/2"):
                return httpx.Response(500, text="boom")
            return httpx.Response(200, text="<html><body>nothing parseable</body></html>")

        async def _no_wait(_seconds):
            return None

        client, fetcher = self._fetcher(handler)
        scraper = NewShopScraper("shop", {"url": "https://shop.test/", "scope": "all"}, fetcher)
        with unittest.mock.patch("asyncio.sleep", _no_wait):
            async with client:
                result = await scraper.scrape()
        self.assertTrue(result.errors, "the bad page is still recorded")
        self.assertFalse(result.truncated, "one unreadable product is not an unlisted catalogue")


class CeramicoloursPaginationTests(unittest.IsolatedAsyncioTestCase):
    """A category that repeats an earlier one must still be walked to its end.

    `products` accumulates across every category, so stopping when a page
    brings nothing new treats an overlap as the end of the listing. It cost
    ceramicolours 416 products, which the loader then retired as delisted while
    they were still on sale.
    """

    HOME = (
        '<a href="Articoli.php?Id=5101">A</a>'
        '<a href="Articoli.php?Id=5102">B</a>'
    )

    def _card(self, code):
        return f'<a href="Articolo.php?cod={code}&Name=x&Lang=IT" class="product-name">{code}</a>'

    async def test_an_overlapping_category_is_walked_past_its_first_page(self):
        from mb_ceramics_catalogue.scrapers.ceramicolours import CeramicoloursScraper

        pages = {
            ("5101", "1"): [self._card("A1"), self._card("A2")],
            ("5101", "2"): [],
            # B's first page repeats A's products entirely; its second page is
            # where its own stock lives.
            ("5102", "1"): [self._card("A1"), self._card("A2")],
            ("5102", "2"): [self._card("B1")],
            ("5102", "3"): [],
        }

        def handler(request):
            params = dict(request.url.params)
            if "Articoli.php" not in request.url.path and not params.get("Id"):
                return httpx.Response(200, text=self.HOME)
            cards = pages.get((params.get("Id"), params.get("page")), [])
            return httpx.Response(200, text="<html>" + "".join(cards) + "</html>")

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        fetcher = base.Fetcher(
            client, base.HostLimiter(0.0, 4), base.BrowserRenderer(False), "never",
            impersonate_policy="never",
        )
        config = {
            "url": "https://www.ceramicolours.it/", "scope": "all",
            "category_ids": [5101, 5102], "category_page_limit": 5,
        }
        scraper = CeramicoloursScraper("ceramicolours", config, fetcher)
        async with client:
            found = await scraper.discover_from_categories()

        self.assertTrue(
            any("B1" in url for url in found),
            "the second category was abandoned on an overlap with the first",
        )


class PerSourceRobotsTests(unittest.IsolatedAsyncioTestCase):
    """A source may be held to robots.txt even where the run ignores it.

    `robots=ignore` is a policy about the fleet. Whether one shop is crawled by
    its own rules is a fact about that shop — one that has already objected, or
    one worth staying on good terms with — so the two settings are separate and
    the stricter wins.
    """

    ROBOTS = "User-agent: *\nDisallow: /private/\n"

    def _fetcher(self):
        def handler(request):
            if request.url.path == "/robots.txt":
                return httpx.Response(200, text=self.ROBOTS)
            return httpx.Response(200, text="<html></html>")

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return client, base.Fetcher(
            client, base.HostLimiter(0.0, 4), base.BrowserRenderer(False), "never",
            impersonate_policy="never", robots_policy="ignore",
        )

    async def test_the_run_policy_ignores_disallow_by_default(self):
        client, fetcher = self._fetcher()
        async with client:
            self.assertTrue(await fetcher.may_fetch("https://shop.test/private/x"))

    async def test_a_source_may_opt_back_in(self):
        client, fetcher = self._fetcher()
        async with client:
            self.assertFalse(
                await fetcher.may_fetch("https://shop.test/private/x", obey_robots=True)
            )
            self.assertTrue(
                await fetcher.may_fetch("https://shop.test/catalogue", obey_robots=True)
            )


class RecordOrderTests(unittest.IsolatedAsyncioTestCase):
    """A dump's record order must come from the listing, not from the network.

    Product pages are fetched concurrently and were appended as each finished,
    so the order of rows in the file was a function of how fast each page
    answered. Two runs over identical data produced different files: a real
    difference to anyone diffing two dumps, and the reason the golden digest
    failed under load and passed on a quiet machine.
    """

    PRODUCT = (
        '<script type="application/ld+json">'
        '{{"@type":"Product","name":"{name}","offers":'
        '{{"price":"1.00","priceCurrency":"EUR"}}}}</script>'
    )

    async def test_rows_follow_the_order_the_pages_were_listed(self):
        from mb_ceramics_catalogue.scrapers.pagecrawl import PageScraper

        class Shop(PageScraper):
            async def discover(self, limit=None):
                return [f"https://shop.test/product/{i}" for i in range(8)]

        async def handler(request):
            if request.url.path == "/robots.txt":
                return httpx.Response(404, text="")
            index = int(request.url.path.rsplit("/", 1)[-1])
            # The last page listed answers first, so completion order is the
            # reverse of the order the pages were discovered in.
            await asyncio.sleep(0.005 * (8 - index))
            return httpx.Response(200, text=self.PRODUCT.format(name=f"P{index}"))

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        fetcher = base.Fetcher(
            client, base.HostLimiter(0.0, 8), base.BrowserRenderer(False), "never",
            impersonate_policy="never",
        )
        config = {"url": "https://shop.test/", "scope": "all", "product_concurrency": 8}
        scraper = Shop("shop", config, fetcher)
        async with client:
            result = await scraper.scrape()

        self.assertEqual(
            [f"P{i}" for i in range(8)],
            [row["name"] for row in result.records],
            "record order followed how fast each page answered",
        )


if __name__ == "__main__":
    unittest.main()
