from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from mb_commerce_scraper.testing import (
    FakeTransport,
    FixtureLimitExceeded,
    FixtureLimits,
    load_recording,
)
from mb_commerce_scraper.transports import (
    RequestPriority,
    RequestPurpose,
    TransportFailure,
    TransportRequest,
)


def request(url: str = "https://shop.test/data") -> TransportRequest:
    return TransportRequest(
        url=url,
        purpose=RequestPurpose.DISCOVERY,
        priority=RequestPriority.DISCOVERY,
    )


@pytest.mark.asyncio
async def test_fake_transport_applies_the_configured_response_retention_limit() -> None:
    transport = FakeTransport(maximum_response_bytes=5)
    transport.add("https://shop.test/data", body=b"12345")

    response = await transport.request(request())

    assert response.content == b"12345"
    with pytest.raises(FixtureLimitExceeded, match="5-byte retention limit") as caught:
        transport.add("https://shop.test/secret", body=b"token=secret")
    assert "token=secret" not in str(caught.value)


@pytest.mark.asyncio
async def test_fake_transport_detaches_bounds_and_redacts_fixture_errors() -> None:
    original = TransportFailure(
        "authorization=very-secret-value " + "x" * 100
    )
    transport = FakeTransport(maximum_error_characters=40)
    transport.add("https://shop.test/data", error=original)

    with pytest.raises(TransportFailure) as caught:
        await transport.request(request())

    assert caught.value is not original
    assert "very-secret-value" not in str(caught.value)
    assert len(str(caught.value)) <= 40


def test_recording_loader_composes_archive_response_and_count_limits(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "recording.json"
    archive.write_text(
        json.dumps(
            [
                {"url": "https://shop.test/one", "body": "12345"},
                {"url": "https://shop.test/two", "body": "ok"},
            ]
        ),
        encoding="utf-8",
    )

    transport = load_recording(
        archive,
        limits=FixtureLimits(
            maximum_response_bytes=5,
            maximum_archive_bytes=1_000,
            maximum_responses=2,
        ),
    )
    assert transport.maximum_response_bytes == 5

    with pytest.raises(FixtureLimitExceeded, match="1-response limit"):
        load_recording(
            archive,
            limits=FixtureLimits(
                maximum_response_bytes=5,
                maximum_archive_bytes=1_000,
                maximum_responses=1,
            ),
        )
    with pytest.raises(FixtureLimitExceeded, match="10-byte archive limit"):
        load_recording(
            archive,
            limits=FixtureLimits(
                maximum_response_bytes=5,
                maximum_archive_bytes=10,
                maximum_responses=2,
            ),
        )


def test_recording_loader_rejects_oversized_body_without_exposing_it(
    tmp_path: Path,
) -> None:
    secret = "authorization=very-secret-value"
    archive = tmp_path / "recording.json"
    archive.write_text(
        json.dumps([{"url": "https://shop.test/data", "body": secret}]),
        encoding="utf-8",
    )

    with pytest.raises(FixtureLimitExceeded) as caught:
        load_recording(
            archive,
            limits=FixtureLimits(
                maximum_response_bytes=4,
                maximum_archive_bytes=1_000,
                maximum_responses=1,
            ),
        )

    assert secret not in str(caught.value)


def test_recording_loader_does_not_retain_malformed_json_in_exception_graph(
    tmp_path: Path,
) -> None:
    secret = "authorization=very-secret-value"
    archive = tmp_path / "recording.json"
    archive.write_text(f'[{secret!r}', encoding="utf-8")

    with pytest.raises(ValueError) as caught:
        load_recording(archive)

    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert secret not in str(caught.value)


@pytest.mark.parametrize(
    "payload,message",
    [
        ({"url": "https://shop.test"}, "JSON array"),
        ([{"body": "missing URL"}], "has no url"),
        ([{"url": "https://shop.test", "headers": {"x": 1}}], "string pairs"),
    ],
)
def test_recording_loader_rejects_malformed_entries(
    tmp_path: Path, payload: Any, message: str
) -> None:
    archive = tmp_path / "recording.json"
    archive.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_recording(archive)


@pytest.mark.parametrize(
    "entry",
    [
        {"url": "https://shop.test/data?access_token=secret"},
        {
            "url": "https://shop.test/data",
            "headers": {"Authorization": "Bearer secret"},
        },
    ],
)
def test_recording_loader_rejects_structural_credentials(
    tmp_path: Path, entry: dict[str, object]
) -> None:
    archive = tmp_path / "recording.json"
    archive.write_text(json.dumps([entry]), encoding="utf-8")

    with pytest.raises(ValueError, match="credential") as caught:
        load_recording(archive)

    assert "secret" not in str(caught.value)


@pytest.mark.asyncio
async def test_fake_missing_response_error_sanitizes_url() -> None:
    transport = FakeTransport()

    with pytest.raises(RuntimeError) as caught:
        await transport.request(
            request("https://shop.test/data?access_token=secret&page=2")
        )

    assert "access_token=secret" not in str(caught.value)
    assert "page=2" not in str(caught.value)
    assert "page=%5Bredacted%5D" in str(caught.value)
