"""Shared retention limits for deterministic response fixtures."""

from __future__ import annotations

from dataclasses import dataclass

from mb_commerce_scraper.models.sanitization import sanitize_diagnostic_text


class FixtureLimitExceeded(ValueError):
    """A fixture would retain more data than its configured test boundary."""


@dataclass(frozen=True, slots=True)
class FixtureLimits:
    """Bounds that can mirror the production response-retention policy.

    Tests with a stricter production limit should pass the same byte value to
    ``FakeTransport`` or ``load_recording``. Archive-wide bounds are separate:
    many individually safe responses must not make loading one JSON fixture an
    unbounded allocation.
    """

    maximum_response_bytes: int = 8 * 1024 * 1024
    maximum_archive_bytes: int = 256 * 1024 * 1024
    maximum_responses: int = 10_000
    maximum_error_characters: int = 2_048

    def __post_init__(self) -> None:
        if self.maximum_response_bytes < 1:
            raise ValueError("maximum_response_bytes must be positive")
        if self.maximum_archive_bytes < 1:
            raise ValueError("maximum_archive_bytes must be positive")
        if self.maximum_responses < 1:
            raise ValueError("maximum_responses must be positive")
        if self.maximum_error_characters < 1:
            raise ValueError("maximum_error_characters must be positive")


DEFAULT_FIXTURE_LIMITS = FixtureLimits()


def retain_fixture_content(
    content: bytes,
    *,
    maximum_bytes: int,
) -> bytes:
    """Return safe content or fail without copying it or exposing its value."""

    if maximum_bytes < 1:
        raise ValueError("maximum_bytes must be positive")
    if len(content) > maximum_bytes:
        raise FixtureLimitExceeded(
            f"fixture response exceeds the {maximum_bytes}-byte retention limit"
        )
    return content


def retain_fixture_error(error: Exception, *, maximum_characters: int) -> Exception:
    """Detach a bounded, redacted error without retaining its traceback graph."""

    message = sanitize_diagnostic_text(str(error), max_length=maximum_characters)
    try:
        return type(error)(message)
    except Exception:  # noqa: BLE001 - third-party exceptions may need structured arguments
        return FixtureLimitExceeded(
            f"fixture error could not be retained safely ({type(error).__name__})"
        )
