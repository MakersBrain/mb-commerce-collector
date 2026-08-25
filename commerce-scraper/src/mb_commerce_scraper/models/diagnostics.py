from enum import StrEnum
from typing import Any, cast

from pydantic import Field, JsonValue, field_validator

from .commerce import ContractModel
from .sanitization import sanitize_diagnostic_text, sanitize_json_value, sanitize_url


class DiagnosticCode(StrEnum):
    RESULT_LIMIT_REACHED = "result_limit_reached"
    REQUEST_BUDGET_EXHAUSTED = "request_budget_exhausted"
    ENUMERATION_INCOMPLETE = "enumeration_incomplete"
    ENTITY_FETCH_FAILED = "entity_fetch_failed"
    PARSER_UNSUPPORTED = "parser_unsupported"
    BROWSER_REQUIRED = "browser_required"
    RATE_LIMITED = "rate_limited"
    PROXY_BUDGET_EXHAUSTED = "proxy_budget_exhausted"
    OPTIONAL_ENRICHMENT_SKIPPED = "optional_enrichment_skipped"
    SCHEMA_CHANGED = "schema_changed"
    CHECKPOINT_INVALID = "checkpoint_invalid"


class DiagnosticSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class Diagnostic(ContractModel):
    code: DiagnosticCode
    severity: DiagnosticSeverity
    message: str = Field(min_length=1, max_length=2048)
    retryable: bool
    affects_completeness: bool
    url: str | None = None
    entity_id: str | None = None
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("message", mode="before")
    @classmethod
    def sanitize_message(cls, value: Any) -> Any:
        return sanitize_diagnostic_text(value) if isinstance(value, str) else value

    @field_validator("url", mode="before")
    @classmethod
    def sanitize_diagnostic_url(cls, value: Any) -> Any:
        if not isinstance(value, str):
            return value
        return sanitize_diagnostic_text(sanitize_url(value))

    @field_validator("entity_id", mode="before")
    @classmethod
    def sanitize_entity_identifier(cls, value: Any) -> Any:
        return sanitize_diagnostic_text(value) if isinstance(value, str) else value

    @field_validator("metadata", mode="before")
    @classmethod
    def sanitize_metadata(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        sanitized = sanitize_json_value(cast(dict[str, JsonValue], value))
        return cast(dict[str, JsonValue], sanitized)


def sanitize_diagnostic(diagnostic: Diagnostic) -> Diagnostic:
    """Revalidate a diagnostic at an untrusted connector egress boundary."""

    return Diagnostic.model_validate(diagnostic.model_dump(mode="python"))


def result_limit_diagnostic(limit: int, url: str) -> Diagnostic:
    return Diagnostic(
        code=DiagnosticCode.RESULT_LIMIT_REACHED,
        severity=DiagnosticSeverity.INFO,
        message=f"caller result limit {limit} reached",
        retryable=True,
        affects_completeness=True,
        url=url,
    )
