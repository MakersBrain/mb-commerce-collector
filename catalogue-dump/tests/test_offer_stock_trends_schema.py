from mb_ceramics_catalogue.storage import db


def test_offer_stock_trends_migration_precedes_product_curation() -> None:
    assert db.SCHEMA_FILES[-2:] == (
        db.OFFER_STOCK_TRENDS_MIGRATION,
        db.PURCHASED_PRODUCT_CURATION_MIGRATION,
    )
    assert (db.schema_directory() / db.OFFER_STOCK_TRENDS_MIGRATION).is_file()


def test_offer_stock_trends_migration_is_incremental_not_in_baseline() -> None:
    baseline = (db.schema_directory() / db.BASELINE).read_text(encoding="utf-8")
    migration = (db.schema_directory() / db.OFFER_STOCK_TRENDS_MIGRATION).read_text(
        encoding="utf-8"
    )

    assert "stock_quantity_kind" not in baseline
    assert "stock_quantity_kind" in migration
    assert "context_version" in migration
    assert "create or replace function catalogue.load_record" in migration.lower()
