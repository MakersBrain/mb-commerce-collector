from enum import StrEnum

from pydantic import Field, JsonValue

from .commerce import ContractModel


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


def result_limit_diagnostic(limit: int, url: str) -> Diagnostic:
    return Diagnostic(
        code=DiagnosticCode.RESULT_LIMIT_REACHED,
        severity=DiagnosticSeverity.INFO,
        message=f"caller result limit {limit} reached",
        retryable=True,
        affects_completeness=True,
        url=url,
    )

