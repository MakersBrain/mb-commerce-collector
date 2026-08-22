"""Secret-safe, bounded normalization for JSON-compatible contract data."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import JsonValue

REDACTED = "[redacted]"
TRUNCATED = "[truncated]"

_NON_ALPHANUMERIC = re.compile(r"[^a-z0-9]")
_EMBEDDED_URL = re.compile(r"[a-z][a-z0-9+.-]*://[^\s<>\"']+", re.IGNORECASE)
_LABELED_CREDENTIAL = re.compile(
    r"\b(authorization|proxy[-_ ]?authorization|cookie|set[-_ ]?cookie|"
    r"password|passwd|passphrase|secret|api[-_ ]?key|access[-_ ]?token|"
    r"refresh[-_ ]?token|token|signature|credential)\b"
    r"(['\"]?)(\s*[:=]\s*)"
    r"(?:'[^']*'|\"[^\"]*\"|(?:bearer|basic)\s+[^\s,;&]+|[^\s,;&]+)",
    re.IGNORECASE,
)
_AUTHORIZATION_VALUE = re.compile(
    r"\b(bearer|basic)(\s+)[a-z0-9._~+/=-]+",
    re.IGNORECASE,
)
_CREDENTIAL_MARKERS = (
    "authorization",
    "cookie",
    "password",
    "passwd",
    "passphrase",
    "secret",
    "apikey",
    "accesskey",
    "privatekey",
    "clientkey",
    "credential",
    "signature",
    "proxycredentials",
    "proxyuser",
)
_TRUNCATION_KEY = "_truncated"


@dataclass(frozen=True, slots=True)
class JsonSanitizationLimits:
    """Resource limits applied to one sanitized JSON tree."""

    # Provider payloads commonly use GraphQL ``edges -> node`` layers. Keep a
    # firm bound without truncating ordinary product/variant option trees.
    max_depth: int = 16
    max_container_entries: int = 100
    max_string_length: int = 2_048
    max_total_nodes: int = 500

    def __post_init__(self) -> None:
        if self.max_depth < 0:
            raise ValueError("max_depth must be non-negative")
        if self.max_container_entries < 1:
            raise ValueError("max_container_entries must be positive")
        if self.max_string_length < 1:
            raise ValueError("max_string_length must be positive")
        if self.max_total_nodes < 1:
            raise ValueError("max_total_nodes must be positive")


@dataclass(slots=True)
class _SanitizationState:
    remaining_nodes: int

    def consume(self) -> bool:
        if self.remaining_nodes <= 0:
            return False
        self.remaining_nodes -= 1
        return True


DEFAULT_JSON_SANITIZATION_LIMITS = JsonSanitizationLimits()


def sanitize_json_value(
    value: JsonValue,
    *,
    limits: JsonSanitizationLimits = DEFAULT_JSON_SANITIZATION_LIMITS,
) -> JsonValue:
    """Return a detached, bounded and credential-redacted JSON value."""

    return _sanitize(
        value,
        limits=limits,
        state=_SanitizationState(limits.max_total_nodes),
        depth=0,
        proxy_context=False,
    )


def sanitize_diagnostic_text(value: str, *, max_length: int = 2_048) -> str:
    """Redact common inline credentials and bound retained diagnostic text."""

    if max_length < 1:
        raise ValueError("max_length must be positive")
    sanitized = _EMBEDDED_URL.sub(
        lambda match: sanitize_url(match.group(0)),
        value,
    )
    sanitized = _LABELED_CREDENTIAL.sub(
        lambda match: (
            f"{match.group(1)}{match.group(2)}{match.group(3)}{REDACTED}"
        ),
        sanitized,
    )
    sanitized = _AUTHORIZATION_VALUE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{REDACTED}",
        sanitized,
    )
    return _bounded_string(sanitized, max_length)


def _sanitize(
    value: JsonValue,
    *,
    limits: JsonSanitizationLimits,
    state: _SanitizationState,
    depth: int,
    proxy_context: bool,
) -> JsonValue:
    if not state.consume():
        return _bounded_string(TRUNCATED, limits.max_string_length)
    if isinstance(value, dict):
        if depth >= limits.max_depth:
            return _bounded_string(TRUNCATED, limits.max_string_length)
        sanitized: dict[str, JsonValue] = {}
        items = list(value.items())
        selected = items[: limits.max_container_entries]
        for index, (key, item) in enumerate(selected):
            if state.remaining_nodes <= 0:
                sanitized[_TRUNCATION_KEY] = len(items) - index
                break
            normalized_key = _normalize_key(key)
            bounded_key = _bounded_string(key, limits.max_string_length)
            if _is_credential_key(normalized_key, proxy_context=proxy_context):
                sanitized[bounded_key] = _bounded_string(REDACTED, limits.max_string_length)
                continue
            sanitized[bounded_key] = _sanitize(
                item,
                limits=limits,
                state=state,
                depth=depth + 1,
                proxy_context=proxy_context or "proxy" in normalized_key,
            )
        if (
            len(items) > limits.max_container_entries
            and _TRUNCATION_KEY not in sanitized
        ):
            sanitized[_TRUNCATION_KEY] = len(items) - len(selected)
        return sanitized
    if isinstance(value, list):
        if depth >= limits.max_depth:
            return _bounded_string(TRUNCATED, limits.max_string_length)
        selected_items = value[: limits.max_container_entries]
        sanitized_items: list[JsonValue] = []
        for item in selected_items:
            if state.remaining_nodes <= 0:
                sanitized_items.append(TRUNCATED)
                break
            sanitized_items.append(_sanitize(
                item,
                limits=limits,
                state=state,
                depth=depth + 1,
                proxy_context=proxy_context,
            ))
        if (
            len(value) > limits.max_container_entries
            and (not sanitized_items or sanitized_items[-1] != TRUNCATED)
        ):
            sanitized_items.append(TRUNCATED)
        return sanitized_items
    if isinstance(value, str):
        return _bounded_string(sanitize_url(value), limits.max_string_length)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _normalize_key(key: str) -> str:
    return _NON_ALPHANUMERIC.sub("", key.casefold())


def _is_credential_key(key: str, *, proxy_context: bool) -> bool:
    if proxy_context and key in {"user", "username"}:
        return True
    if any(marker in key for marker in _CREDENTIAL_MARKERS):
        return True
    return (
        key == "token"
        or key.endswith("token")
        or key.startswith(("tokenheader", "tokenvalue"))
        or key in {"auth", "key", "sig"}
    )


def sanitize_url(value: str) -> str:
    """Redact URL user information and credential-bearing query/fragment values."""

    if "://" not in value:
        return value
    try:
        parts = urlsplit(value)
    except ValueError:
        return "[invalid-url]"
    if not parts.scheme or not parts.netloc:
        return value

    netloc = parts.netloc
    if "@" in netloc:
        _, _, host = netloc.rpartition("@")
        netloc = f"redacted@{host}"

    query = _sanitize_query(parts.query)
    fragment = _sanitize_query(parts.fragment) if "=" in parts.fragment else parts.fragment
    return urlunsplit((parts.scheme, netloc, parts.path, query, fragment))


def _sanitize_query(value: str) -> str:
    if not value:
        return value
    pairs = parse_qsl(value, keep_blank_values=True)
    return urlencode(
        [
            (key, REDACTED if _is_credential_key(_normalize_key(key), proxy_context=False) else item)
            for key, item in pairs
        ],
        doseq=True,
    )


def _bounded_string(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    if limit <= len(TRUNCATED):
        return TRUNCATED[:limit]
    return f"{value[: limit - len(TRUNCATED)]}{TRUNCATED}"
