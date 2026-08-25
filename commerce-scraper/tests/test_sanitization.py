from copy import deepcopy
from datetime import UTC, datetime
from math import inf, nan
from typing import cast

import pytest
from pydantic import JsonValue

from mb_commerce_scraper import (
    CommerceProductSnapshot,
    CommerceVariant,
    Diagnostic,
    DiagnosticCode,
    DiagnosticSeverity,
)
from mb_commerce_scraper.models.sanitization import (
    REDACTED,
    TRUNCATED,
    JsonSanitizationLimits,
    sanitize_json_value,
)


def test_recursively_redacts_credential_keys_without_mutating_input() -> None:
    raw: dict[str, JsonValue] = {
        "Authorization": "Bearer private",
        "request_cookie_header": "session=private",
        "proxy_username": "root-proxy-user",
        "nested": {
            "access_token": "private-token",
            "client-secret": "private-secret",
            "safe": "visible",
            "tokenizer_config": "public-model-setting",
        },
        "proxy": {"username": "proxy-user", "password": "proxy-pass", "host": "proxy.test"},
    }
    original = deepcopy(raw)

    sanitized = sanitize_json_value(raw)

    assert sanitized == {
        "Authorization": REDACTED,
        "request_cookie_header": REDACTED,
        "proxy_username": REDACTED,
        "nested": {
            "access_token": REDACTED,
            "client-secret": REDACTED,
            "safe": "visible",
            "tokenizer_config": "public-model-setting",
        },
        "proxy": {"username": REDACTED, "password": REDACTED, "host": "proxy.test"},
    }
    assert raw == original
    assert sanitized is not raw
    sanitized_dict = sanitized
    assert sanitized_dict["nested"] is not raw["nested"]


def test_sanitizes_url_userinfo_query_and_oauth_fragment() -> None:
    value: dict[str, JsonValue] = {
        "url": "https://alice:hunter2@example.test/path?tag=clay&api_key=private&tag=tools#access_token=oauth&state=ok",
        "proxy_url": "socks5://proxy-user:proxy-pass@proxy.test:1080",
    }

    sanitized = sanitize_json_value(value)

    assert sanitized == {
        "url": "https://redacted@example.test/path?tag=clay&api_key=%5Bredacted%5D&tag=tools#access_token=%5Bredacted%5D&state=ok",
        "proxy_url": "socks5://redacted@proxy.test:1080",
    }


def test_caps_depth_entries_and_string_lengths() -> None:
    limits = JsonSanitizationLimits(max_depth=2, max_container_entries=2, max_string_length=16)
    value: dict[str, JsonValue] = {
        "items": [
            {"nested": "not emitted"},
            "abcdefghijklmnopqrstuvwxyz",
            "not emitted",
        ],
        "long_key_abcdefghijklmnopqrstuvwxyz": "visible",
        "not_emitted": True,
    }

    sanitized = sanitize_json_value(value, limits=limits)

    assert sanitized == {
        "items": [TRUNCATED, "abcde[truncated]", TRUNCATED],
        "long_[truncated]": "visible",
        "_truncated": 1,
    }
    sanitized_dict = sanitized
    assert len(sanitized_dict) == 3
    assert all(len(key) <= 16 for key in sanitized_dict)


def test_non_finite_numbers_become_json_null() -> None:
    assert sanitize_json_value([nan, inf, -inf, 1.25]) == [None, None, None, 1.25]


def test_total_node_budget_bounds_wide_nested_trees() -> None:
    value: JsonValue = {"rows": [{"value": index} for index in range(20)]}

    sanitized = sanitize_json_value(
        value,
        limits=JsonSanitizationLimits(max_total_nodes=6),
    )

    assert "[truncated]" in repr(sanitized)
    assert len(repr(sanitized)) < len(repr(value))


def test_default_depth_preserves_normal_graphql_edge_node_payloads() -> None:
    value: JsonValue = {
        "variant": {
            "options": {
                "edges": [
                    {
                        "node": {
                            "displayName": "Size",
                            "values": {
                                "edges": [{"node": {"label": "500 ml"}}]
                            },
                        }
                    }
                ]
            }
        }
    }

    assert sanitize_json_value(value) == value


def test_product_and_variant_extension_boundaries_are_sanitized() -> None:
    secret = "never-serialize-this"
    observed = datetime.now(UTC)
    snapshot = CommerceProductSnapshot(
        connector="test",
        source_id="shop",
        external_id="product-1",
        canonical_url="https://shop.test/product-1",
        title="Clay",
        observed_at=observed,
        variants=(
            CommerceVariant(
                external_id="variant-1",
                platform_extensions={
                    "access_token": secret,
                    "safe": "variant-value",
                },
            ),
        ),
        platform_extensions={
            "proxy": {"username": secret, "host": "proxy.test"},
            "callback_url": f"https://user:{secret}@shop.test/path?signature={secret}",
            "long": "x" * 4_000,
        },
    )

    dumped = snapshot.model_dump_json()
    assert secret not in dumped
    assert snapshot.variants[0].platform_extensions["access_token"] == REDACTED
    assert snapshot.variants[0].platform_extensions["safe"] == "variant-value"
    assert len(cast(str, snapshot.platform_extensions["long"])) <= 2_048
    assert "redacted@shop.test" in cast(
        str, snapshot.platform_extensions["callback_url"]
    )


def test_diagnostic_contract_bounds_and_redacts_retained_error_artifacts() -> None:
    secret = "diagnostic-secret-sentinel"
    diagnostic = Diagnostic(
        code=DiagnosticCode.ENTITY_FETCH_FAILED,
        severity=DiagnosticSeverity.ERROR,
        message=(
            f"backend failed Authorization: Bearer {secret} at "
            f"https://user:{secret}@shop.test/item?access_token={secret}&page=2 "
            f"details={{'password': '{secret}'}} "
            + "x" * 4_000
        ),
        retryable=True,
        affects_completeness=False,
        url=f"https://user:{secret}@shop.test/item?api_key={secret}&page=2",
        entity_id=f"token={secret}",
        metadata={
            "raw_error": {
                "password": secret,
                "request_url": f"https://shop.test/item?signature={secret}",
            },
            "stage": "detail",
        },
    )

    dumped = diagnostic.model_dump_json()
    assert secret not in dumped
    assert len(diagnostic.message) <= 2_048
    assert diagnostic.code is DiagnosticCode.ENTITY_FETCH_FAILED
    assert diagnostic.retryable
    assert diagnostic.metadata["stage"] == "detail"
    assert "[redacted]" in dumped


def test_rejects_invalid_limits() -> None:
    with pytest.raises(ValueError, match="max_depth"):
        JsonSanitizationLimits(max_depth=-1)
    with pytest.raises(ValueError, match="max_container_entries"):
        JsonSanitizationLimits(max_container_entries=0)
    with pytest.raises(ValueError, match="max_string_length"):
        JsonSanitizationLimits(max_string_length=0)
    with pytest.raises(ValueError, match="max_total_nodes"):
        JsonSanitizationLimits(max_total_nodes=0)
