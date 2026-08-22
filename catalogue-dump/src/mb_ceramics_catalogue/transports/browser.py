"""Backend-neutral browser lifecycle contracts.

Backends live for the worker process.  Sessions live for exactly one crawl and
own every page/storage handle they create.  Keeping those lifetimes distinct is
the isolation boundary that a shared browser process needs.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, Literal, Protocol, runtime_checkable

BrowserBackendName = Literal["camoufox", "cdp_extension_proxy"]


class Blocked(Exception):
    """A remote site's rules, response, or defences stopped a transport operation."""


# Neutral descriptive alias for new transport code, while preserving the
# long-standing public class name exposed by ``scrapers.base.Blocked``.
TransportBlocked = Blocked


class BrowserUnavailable(Exception):
    """The process cannot provide the browser session required by this job.

    This is deliberately distinct from :class:`TransportBlocked`: a refusal is
    local to one request, while an unavailable backend is a worker-placement
    failure that must escape page-level handlers and be requeued elsewhere.
    """


@dataclass(frozen=True, slots=True)
class BrowserJobContext:
    job_id: str
    logical_profile: str | None = None


@dataclass(slots=True)
class BrowserNetworkAccounting:
    """Monotonic aggregate for one lease-scoped browser backend."""

    physical_requests: int = 0
    transmitted_bytes: int = 0
    received_bytes: int = 0

    def record(
        self,
        transmitted_bytes: int,
        received_bytes: int,
        physical_requests: int,
    ) -> None:
        if min(transmitted_bytes, received_bytes, physical_requests) < 0:
            raise ValueError("browser network accounting cannot be negative")
        self.transmitted_bytes += transmitted_bytes
        self.received_bytes += received_bytes
        self.physical_requests += physical_requests

    def snapshot(self) -> tuple[int, int, int]:
        return (
            self.physical_requests,
            self.transmitted_bytes,
            self.received_bytes,
        )


@dataclass(frozen=True, slots=True)
class BrowserFetchResponse:
    """Browser-context fetch result without exposing a Playwright response."""

    status: int
    headers: Mapping[str, str]
    content: bytes
    final_url: str


@dataclass(frozen=True, slots=True)
class BrowserEvaluationResult:
    """JSON-safe browser evaluation value and its post-navigation URL."""

    value: Any
    final_url: str


@runtime_checkable
class BrowserSession(Protocol):
    async def render(
        self, url: str, wait_ms: int = 1500, wait_for: str | None = None,
    ) -> str: ...

    async def evaluate(
        self, url: str, script: str, wait_ms: int = 2000,
        wait_for: str | None = None,
    ) -> Any: ...

    async def evaluate_result(
        self, url: str, script: str, wait_ms: int = 2000,
        wait_for: str | None = None,
    ) -> BrowserEvaluationResult: ...

    async def request(
        self,
        page_url: str,
        endpoint: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        json_body: Any = None,
    ) -> BrowserFetchResponse: ...

    async def request_json(
        self, page_url: str, endpoint: str, *, method: str = "POST",
        headers: dict[str, str] | None = None, body: Any = None,
    ) -> Any: ...

    async def close(self) -> None: ...


@runtime_checkable
class BrowserBackend(Protocol):
    @property
    def backend(self) -> BrowserBackendName: ...

    def open_session(
        self, job: BrowserJobContext | None = None,
    ) -> AbstractAsyncContextManager[BrowserSession]: ...

    async def shutdown(self) -> None: ...
