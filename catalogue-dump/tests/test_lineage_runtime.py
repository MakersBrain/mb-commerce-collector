from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from mb_ceramics_catalogue.ops import outputs


def _connection() -> outputs.Connection:
    return cast(outputs.Connection, object())


async def test_create_lineage_preserves_legacy_runtime_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = uuid4()
    calls: list[tuple[str, Sequence[Any]]] = []

    async def one(
        connection: outputs.Connection, sql: str, params: Sequence[Any]
    ) -> dict[str, Any] | None:
        del connection
        calls.append((sql, params))
        return {"checkpoint_lineage": lineage}

    monkeypatch.setattr(outputs, "_one", one)

    created = await outputs.create_lineage(
        _connection(),
        uuid4(),
        source_id="shop",
        source_url="https://shop.test",
        connector="shopify",
        connector_version="1",
        connector_config_fingerprint="a" * 64,
        dataset_fingerprint="b" * 64,
        dataset_selection=[],
        lineage=lineage,
    )

    assert created == lineage
    sql, params = calls[0]
    assert "runtime_format, collection_request" in sql
    assert params[10:13] == (outputs.LineageRuntimeFormat.CATALOGUE_V1.value, None, None)


async def test_library_lineage_requires_and_persists_reconstructable_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    async def one(
        connection: outputs.Connection, sql: str, params: Sequence[Any]
    ) -> dict[str, Any] | None:
        nonlocal called
        del connection, sql, params
        called = True
        return {"checkpoint_lineage": uuid4()}

    monkeypatch.setattr(outputs, "_one", one)
    arguments: dict[str, Any] = {
        "source_id": "shop",
        "source_url": "https://shop.test",
        "connector": "shopify",
        "connector_version": "1",
        "connector_config_fingerprint": "a" * 64,
        "dataset_fingerprint": "b" * 64,
        "dataset_selection": [],
        "runtime_format": outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
    }

    with pytest.raises(ValueError, match="durable request"):
        await outputs.create_lineage(_connection(), uuid4(), **arguments)
    assert not called

    await outputs.create_lineage(
        _connection(),
        uuid4(),
        **arguments,
        collection_request={"source_id": "shop", "base_url": "https://shop.test"},
        connector_options={"page_limit": 50},
    )
    assert called


async def test_compatible_lineage_lookup_is_runtime_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: Sequence[Any] = ()

    async def one(
        connection: outputs.Connection, sql: str, params: Sequence[Any]
    ) -> dict[str, Any] | None:
        nonlocal captured
        del connection
        assert "runtime_format = %s" in sql
        captured = params
        return None

    monkeypatch.setattr(outputs, "_one", one)
    await outputs.find_compatible_lineage(
        _connection(),
        uuid4(),
        source_url="https://shop.test",
        connector="shopify",
        connector_version="1",
        connector_config_fingerprint="a" * 64,
        dataset_fingerprint="b" * 64,
        runtime_format=outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
    )

    assert captured[2] == outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1.value


async def test_runtime_configuration_is_typed_and_unknown_formats_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row: dict[str, Any] = {
        "runtime_format": "commerce-scraper-v1",
        "collection_request": {"source_id": "shop"},
        "connector_options": {"page_limit": 50},
    }

    async def one(
        connection: outputs.Connection, sql: str, params: Sequence[Any]
    ) -> dict[str, Any] | None:
        del connection, sql, params
        return row

    monkeypatch.setattr(outputs, "_one", one)
    configuration = await outputs.lineage_runtime_configuration(
        _connection(), uuid4(), uuid4()
    )
    assert configuration == outputs.LineageRuntimeConfiguration(
        runtime_format=outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
        collection_request={"source_id": "shop"},
        connector_options={"page_limit": 50},
    )

    row["runtime_format"] = "future-unknown"
    with pytest.raises(ValueError, match="future-unknown"):
        await outputs.lineage_runtime_configuration(_connection(), uuid4(), uuid4())


async def test_reject_lineage_is_conditional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results: list[dict[str, UUID] | None] = [
        {"checkpoint_lineage": uuid4()},
        None,
    ]

    async def one(
        connection: outputs.Connection, sql: str, params: Sequence[Any]
    ) -> dict[str, Any] | None:
        del connection, params
        assert "status = 'active'" in sql
        return results.pop(0)

    monkeypatch.setattr(outputs, "_one", one)
    assert await outputs.reject_lineage(_connection(), uuid4(), uuid4())
    assert not await outputs.reject_lineage(_connection(), uuid4(), uuid4())
