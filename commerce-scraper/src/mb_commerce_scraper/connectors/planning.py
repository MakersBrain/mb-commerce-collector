"""Non-consuming connector work classification.

The transport owns authorization and charging for actual attempts. Connectors
use this facade only to label required and optional work consistently.
"""

from __future__ import annotations

from mb_commerce_scraper.models import (
    Diagnostic,
    DiagnosticCode,
    DiagnosticSeverity,
    SnapshotField,
)
from mb_commerce_scraper.transports import RequestPriority


class BudgetExhausted(RuntimeError):
    def __init__(self, priority: RequestPriority, url: str) -> None:
        super().__init__(f"request budget exhausted for {priority.name.lower()}")
        self.priority = priority
        self.url = url


class ConnectorBudget:
    """Classify connector work without consuming the attempt budget."""

    def require(self, priority: RequestPriority, url: str) -> None:
        del priority, url

    @staticmethod
    def required_detail_priority(
        requested_fields: frozenset[SnapshotField],
        supplied_fields: frozenset[SnapshotField],
    ) -> RequestPriority:
        return (
            RequestPriority.DATASET_REQUIRED
            if requested_fields & supplied_fields
            else RequestPriority.DETAIL
        )


def budget_diagnostic(priority: RequestPriority, url: str) -> Diagnostic:
    return Diagnostic(
        code=DiagnosticCode.REQUEST_BUDGET_EXHAUSTED,
        severity=DiagnosticSeverity.ERROR,
        message=f"request budget cannot fund required {priority.name.lower()} work",
        retryable=True,
        affects_completeness=True,
        url=url,
    )
