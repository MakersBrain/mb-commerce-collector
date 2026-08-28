"""Zero-record failures cannot withdraw the last good source catalogue."""

from mb_ceramics_catalogue.ops.worker import _legacy_load_plan


def test_invalid_zero_record_replacement_is_adds_only() -> None:
    summary = {
        "records": 0,
        "discovered": 4_452,
        "invalid": 8_150,
        "error_count": 0,
        "truncated": False,
        "scraper": "shopify",
    }

    whole, reason, error = _legacy_load_plan(summary, "replaced")

    assert whole is False
    assert reason == "zero-record outcome failed validation"
    assert error is not None
    assert "lacked a usable identity or price" in error


def test_exhausted_currency_failure_is_adds_only() -> None:
    summary = {
        "records": 0,
        "discovered": 4_452,
        "invalid": 8_150,
        "error_count": 1,
        "errors": [
            {
                "url": "https://shop.test/meta.json",
                "error": "429 Too Many Requests",
            }
        ],
        "truncated": True,
        "scraper": "shopify",
    }

    whole, reason, error = _legacy_load_plan(summary, "replaced")

    assert whole is False
    assert reason == "zero-record outcome failed validation"
    assert error == "429 Too Many Requests"


def test_valid_complete_replacement_can_still_retire() -> None:
    summary = {
        "records": 12,
        "discovered": 12,
        "error_count": 0,
        "truncated": False,
        "scraper": "shopify",
    }

    whole, reason, error = _legacy_load_plan(summary, "replaced")

    assert whole is True
    assert reason == ""
    assert error is None
