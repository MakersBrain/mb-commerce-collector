from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mb_commerce_scraper.transports import sanitize_url

from .fake_transport import FakeTransport
from .limits import DEFAULT_FIXTURE_LIMITS, FixtureLimitExceeded, FixtureLimits


def load_recording(
    path: Path,
    *,
    limits: FixtureLimits = DEFAULT_FIXTURE_LIMITS,
) -> FakeTransport:
    """Load a bounded, secret-free JSON response map for deterministic replay."""
    try:
        size = path.stat().st_size
    except OSError as error:
        raise ValueError("recording is unreadable") from error
    if size > limits.maximum_archive_bytes:
        raise FixtureLimitExceeded(
            f"recording exceeds the {limits.maximum_archive_bytes}-byte archive limit"
        )
    try:
        with path.open("rb") as handle:
            encoded = handle.read(limits.maximum_archive_bytes + 1)
    except OSError as error:
        raise ValueError("recording is unreadable") from error
    if len(encoded) > limits.maximum_archive_bytes:
        raise FixtureLimitExceeded(
            f"recording exceeds the {limits.maximum_archive_bytes}-byte archive limit"
        )
    decode_failed = False
    try:
        payload: Any = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError):
        # JSONDecodeError retains the complete input in ``.doc``. Raise after
        # leaving the handler so neither cause nor implicit context can retain
        # a large or secret-bearing recording body.
        decode_failed = True
        payload = None
    if decode_failed:
        raise ValueError("recording is not valid UTF-8 JSON") from None
    if not isinstance(payload, list):
        raise ValueError("recording must be a JSON array")
    if len(payload) > limits.maximum_responses:
        raise FixtureLimitExceeded(
            f"recording exceeds the {limits.maximum_responses}-response limit"
        )

    transport = FakeTransport(
        maximum_response_bytes=limits.maximum_response_bytes,
        maximum_error_characters=limits.maximum_error_characters,
    )
    for entry_number, entry in enumerate(payload, start=1):
        if not isinstance(entry, dict):
            raise ValueError(f"recording entry {entry_number} must be an object")
        try:
            url = entry["url"]
        except KeyError as error:
            raise ValueError(f"recording entry {entry_number} has no url") from error
        if not isinstance(url, str) or not url:
            raise ValueError(f"recording entry {entry_number} url must be a string")
        if sanitize_url(url) != url:
            raise ValueError(
                f"recording entry {entry_number} url contains credentials"
            )
        body = entry.get("body", "")
        if not isinstance(body, str):
            raise ValueError(f"recording entry {entry_number} body must be a string")
        headers = entry.get("headers", {})
        if not isinstance(headers, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in headers.items()
        ):
            raise ValueError(f"recording entry {entry_number} headers must be string pairs")
        sensitive_headers = {
            "authorization",
            "cookie",
            "proxy-authorization",
            "set-cookie",
            "x-api-key",
        }
        if any(name.casefold() in sensitive_headers for name in headers):
            raise ValueError(
                f"recording entry {entry_number} contains credential headers"
            )
        status = entry.get("status", 200)
        if not isinstance(status, int) or isinstance(status, bool):
            raise ValueError(f"recording entry {entry_number} status must be an integer")
        transport.add(url, status=status, body=body, headers=headers)
    return transport
