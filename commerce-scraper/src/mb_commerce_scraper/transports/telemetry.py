from __future__ import annotations

from collections.abc import Mapping

from pydantic import JsonValue

from mb_commerce_scraper.models.sanitization import sanitize_json_value
from mb_commerce_scraper.models.sanitization import sanitize_url as _sanitize_url

from .base import TelemetryHooks

_OMITTED = "[omitted]"
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
    """Apply the package's shared secret-safe URL normalization policy."""

    return _sanitize_url(url)


def sanitize_fields(fields: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    """Omit protocol payloads, then apply the shared recursive sanitizer."""

    prepared: dict[str, JsonValue] = {
        key: _OMITTED if _normalized_key(key) in _OMITTED_FIELD_KEYS else value
        for key, value in fields.items()
    }
    sanitized = sanitize_json_value(prepared)
    if not isinstance(sanitized, dict):
        return {}
    return sanitized


def _normalized_key(key: str) -> str:
    return "".join(character for character in key.casefold() if character.isalnum())


class SafeTelemetry:
    """Secret-scrubbing, best-effort boundary around an application telemetry hook."""

    def __init__(self, sink: TelemetryHooks) -> None:
        self._sink = sink

    def emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        try:
            self._sink.emit(event, sanitize_fields(fields))
        except Exception:  # noqa: BLE001 -- observer failures are always non-fatal
            # Observability must never become part of collection correctness.
            return


def safe_telemetry(sink: TelemetryHooks) -> SafeTelemetry:
    if isinstance(sink, SafeTelemetry):
        return sink
    return SafeTelemetry(sink)
