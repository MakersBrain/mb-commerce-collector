from __future__ import annotations

import re
from collections.abc import Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import JsonValue

from mb_commerce_scraper.models.sanitization import sanitize_json_value
from mb_commerce_scraper.models.sanitization import sanitize_url as _sanitize_url

from .base import TelemetryHooks

_OMITTED = "[omitted]"
_EVENT_NAME = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*$")
_INVALID_EVENT = "telemetry.invalid_event"
_OMITTED_FIELD_KEYS = frozenset(
    {
        "body",
        "content",
        "headers",
        "jsonbody",
        "payload",
        "requestbody",
        "responsebody",
    }
)


def sanitize_url(url: str) -> str:
    """Retain URL structure while redacting all query values and fragments."""

    sanitized = _sanitize_url(url)
    if "://" not in sanitized:
        return sanitized
    try:
        parts = urlsplit(sanitized)
    except ValueError:
        return "[invalid-url]"
    query = urlencode(
        [(key, "[redacted]") for key, _value in parse_qsl(parts.query, keep_blank_values=True)],
        doseq=True,
    )
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))


def sanitize_fields(fields: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    """Omit protocol payloads, then apply the shared recursive sanitizer."""

    prepared: dict[str, JsonValue] = {
        key: _OMITTED if _normalized_key(key) in _OMITTED_FIELD_KEYS else value
        for key, value in fields.items()
    }
    sanitized = sanitize_json_value(prepared)
    if not isinstance(sanitized, dict):
        return {}
    strict = _sanitize_telemetry_urls(sanitized)
    return strict if isinstance(strict, dict) else {}


def _sanitize_telemetry_urls(value: JsonValue) -> JsonValue:
    if isinstance(value, dict):
        return {key: _sanitize_telemetry_urls(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_telemetry_urls(item) for item in value]
    if isinstance(value, str) and "://" in value:
        return sanitize_url(value)
    return value


def _normalized_key(key: str) -> str:
    return "".join(character for character in key.casefold() if character.isalnum())


class SafeTelemetry:
    """Secret-scrubbing, best-effort boundary around an application telemetry hook."""

    def __init__(self, sink: TelemetryHooks) -> None:
        self._sink = sink

    def emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        try:
            safe_event = (
                event if len(event) <= 128 and _EVENT_NAME.fullmatch(event) is not None else _INVALID_EVENT
            )
            self._sink.emit(safe_event, sanitize_fields(fields))
        except Exception:  # noqa: BLE001 -- observer failures are always non-fatal
            # Observability must never become part of collection correctness.
            return


def safe_telemetry(sink: TelemetryHooks) -> SafeTelemetry:
    if isinstance(sink, SafeTelemetry):
        return sink
    return SafeTelemetry(sink)
