from mb_ceramics_catalogue.ops.proxy_roles import PROXY_TABLES
from mb_ceramics_catalogue.storage import db


def test_secret_intent_migration_is_registered_after_provider_integrity():
    assert db.SCHEMA_FILES[-4:-2] == (
        db.PROXY_PROVIDER_INTEGRITY_MIGRATION,
        db.PROXY_PROFILE_SECRET_INTENT_MIGRATION,
    )
    assert (db.schema_directory() / db.PROXY_PROFILE_SECRET_INTENT_MIGRATION).is_file()
    assert "proxy_profile_secret_intents" in PROXY_TABLES


def test_secret_intent_schema_contains_no_credential_or_capability_columns():
    ddl = (
        db.schema_directory() / db.PROXY_PROFILE_SECRET_INTENT_MIGRATION
    ).read_text(encoding="utf-8").lower()
    for forbidden in ("username", "password", "api_key", "countries", "sticky_session"):
        assert forbidden not in ddl
