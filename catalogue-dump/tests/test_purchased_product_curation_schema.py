from mb_ceramics_catalogue.storage import db


def test_purchased_product_curation_is_registered_after_stock_schema() -> None:
    assert db.SCHEMA_FILES[-2:] == (
        db.OFFER_STOCK_TRENDS_MIGRATION,
        db.PURCHASED_PRODUCT_CURATION_MIGRATION,
    )


def test_curation_uses_explicit_products_and_source_selectors() -> None:
    migration = (
        db.schema_directory() / db.PURCHASED_PRODUCT_CURATION_MIGRATION
    ).read_text(encoding="utf-8")

    assert "31e265bf-7d12-50f0-9b3f-a806b1594996" in migration
    assert "01080544-6570-51c8-ad41-c6358ba36252" in migration
    assert "family = 'clay_body'" in migration
    assert "source_id = any" in migration
