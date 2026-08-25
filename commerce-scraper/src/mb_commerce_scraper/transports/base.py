from __future__ import annotations

import json
from asyncio import Lock
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from time import monotonic
from typing import Any, Literal, Protocol, runtime_checkable
from urllib.parse import urlencode, urlsplit

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

DEFAULT_MAXIMUM_RESPONSE_BYTES = 16 * 1024 * 1024


class BudgetExhausted(RuntimeError):
    """A request attempt could not be authorized by its neutral budget."""


class TransportAccounting(BaseModel):
    """Secret-free physical transport totals for one logical backend call."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    physical_requests: int = Field(default=1, ge=0, strict=True)
    transmitted_bytes: int = Field(default=0, ge=0, strict=True)
    received_bytes: int = Field(default=0, ge=0, strict=True)


class TransportFailure(RuntimeError):
    """A typed network/TLS/backend failure eligible for routing policy."""

    def __init__(
        self,
        message: str,
        *,
        accounting: TransportAccounting | None = None,
    ) -> None:
        super().__init__(message)
        self.accounting = accounting


class ResponseBodyTooLarge(RuntimeError):
    """A response exceeded the configured retained-body limit."""

    def __init__(
        self,
        *,
        maximum_bytes: int,
        received_bytes: int,
        accounting: TransportAccounting | None = None,
    ) -> None:
        super().__init__(f"response body exceeded the {maximum_bytes}-byte retention limit")
        self.maximum_bytes = maximum_bytes
        self.received_bytes = received_bytes
        self.accounting = accounting


class ResponseDecodeFailure(ValueError):
    """A response body could not be decoded without retaining parser context."""

    def __init__(self, *, line: int, column: int) -> None:
        super().__init__(f"response body is not valid JSON at line {line}, column {column}")
        self.line = line
        self.column = column


class RequestPurpose(StrEnum):
    ROBOTS = "robots"
    DISCOVERY = "discovery"
    ENTITY = "entity"
    ENRICHMENT = "enrichment"


class RequestPriority(IntEnum):
    DISCOVERY = 1
    IDENTITY = 2
    DATASET_REQUIRED = 3
    DETAIL = 4
    OPTIONAL = 5


class CachePolicy(StrEnum):
    DEFAULT = "default"
    BYPASS = "bypass"
    REFRESH = "refresh"


class BrowserHint(StrEnum):
    NEVER = "never"
    OPTIONAL = "optional"
    REQUIRED = "required"


class BrowserEvaluation(BaseModel):
    """One bounded connector-owned script evaluated in an isolated page."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    action_id: str = Field(pattern=r"^[a-z][a-z0-9_.-]{0,63}$")
    script: str = Field(min_length=1, max_length=65_536, repr=False)
    wait_for: str | None = Field(default=None, max_length=512)
    wait_milliseconds: int = Field(default=2_000, ge=0, le=30_000, strict=True)


class RotationReason(StrEnum):
    EXPLICIT = "explicit"
    BLOCKED = "blocked"
    RATE_LIMITED = "rate_limited"
    CAPTCHA = "captcha"
    TRANSPORT_FAILURE = "transport_failure"
    SESSION_EXPIRED = "session_expired"
    BUDGET_EXHAUSTED = "budget_exhausted"


class TransportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    method: str = "GET"
    url: str
    query: dict[str, str | int | float | bool] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    json_body: JsonValue | None = None
    body: bytes | None = None
    purpose: RequestPurpose
    priority: RequestPriority
    required: bool = True
    estimated_bytes: int = Field(default=0, ge=0, strict=True)
    cache: CachePolicy = CachePolicy.DEFAULT
    browser: BrowserHint = BrowserHint.NEVER
    evaluation: BrowserEvaluation | None = None
    trace_request_id: str | None = Field(default=None, min_length=1, max_length=128)
    trace_attempt: int | None = Field(default=None, ge=1, strict=True)

    @model_validator(mode="after")
    def evaluation_is_an_explicit_browser_action(self) -> TransportRequest:
        if self.evaluation is None:
            return self
        if self.browser is not BrowserHint.REQUIRED:
            raise ValueError("browser evaluation requires browser=required")
        if self.method.upper() != "GET" or any(
            (self.query, self.headers, self.json_body is not None, self.body is not None)
        ):
            raise ValueError("browser evaluation cannot be combined with HTTP request payload fields")
        if self.purpose in {RequestPurpose.ROBOTS, RequestPurpose.DISCOVERY}:
            raise ValueError("browser evaluation is only valid for entity/detail work")
        return self


def transport_trace_fields(request: TransportRequest) -> dict[str, JsonValue]:
    """Return bounded correlation fields carried across transport layers."""

    fields: dict[str, JsonValue] = {}
    if request.trace_request_id is not None:
        fields["request_id"] = request.trace_request_id
    if request.trace_attempt is not None:
        fields["attempt"] = request.trace_attempt
    return fields


def estimated_transmitted_bytes(request: TransportRequest) -> int:
    """Deterministic application-layer bytes for one physical HTTP request.

    This intentionally returns only an integer. Header, query, and body values
    are measured transiently and are never copied into accounting metadata.
    HTTP client defaults, proxy CONNECT framing, TLS, and browser subrequests
    are backend knowledge and belong in explicit ``TransportAccounting``.
    """

    parsed = urlsplit(request.url)
    existing_query = parsed.query
    supplied_query = urlencode(
        [(key, _query_accounting_value(value)) for key, value in request.query.items()]
    )
    query = "&".join(value for value in (existing_query, supplied_query) if value)
    target = parsed.path or "/"
    if query:
        target = f"{target}?{query}"

    body = request.body or b""
    if request.json_body is not None:
        body = json.dumps(
            request.json_body,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

    headers = list(request.headers.items())
    normalized = {name.casefold() for name, _ in headers}
    if "host" not in normalized:
        host = parsed.hostname or ""
        if ":" in host:
            host = f"[{host}]"
        default_port = (parsed.scheme == "https" and parsed.port in {None, 443}) or (
            parsed.scheme == "http" and parsed.port in {None, 80}
        )
        headers.append(("Host", host if default_port else f"{host}:{parsed.port}"))
    if body and "content-length" not in normalized:
        headers.append(("Content-Length", str(len(body))))
    if request.json_body is not None and "content-type" not in normalized:
        headers.append(("Content-Type", "application/json"))

    start_line = f"{request.method.upper()} {target} HTTP/1.1\r\n".encode()
    header_bytes = sum(len(name.encode("utf-8")) + len(value.encode("utf-8")) + 4 for name, value in headers)
    return len(start_line) + header_bytes + 2 + len(body)


def _query_accounting_value(value: str | int | float | bool) -> str | int | float:
    if isinstance(value, bool):
        return str(value).lower()
    return value


class RouteMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["direct", "proxy", "cache", "browser"] = "direct"
    provider: str | None = None
    endpoint_id: str | None = None
    lease_id: str | None = None


class RequestObservationPhase(StrEnum):
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"


class RequestObservation(BaseModel):
    """Typed, secret-free observation of one physical request attempt."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    phase: RequestObservationPhase
    request_id: str | None = Field(default=None, min_length=1, max_length=128)
    attempt: int = Field(ge=1, strict=True)
    source_id: str | None = Field(default=None, min_length=1, max_length=256)
    target_host: str = Field(max_length=253)
    method: str = Field(min_length=1, max_length=16)
    purpose: RequestPurpose
    status: int | None = Field(default=None, ge=100, le=599, strict=True)
    elapsed_seconds: float | None = Field(default=None, ge=0)
    route: RouteMetadata | None = None
    accounting: TransportAccounting = TransportAccounting(physical_requests=0)
    classification: str | None = Field(default=None, max_length=64)

    @property
    def outcome(self) -> str:
        if self.status in {403, 429}:
            return str(self.status)
        if self.status is not None:
            return f"{self.status // 100}xx"
        return "transport_error"

    def trace_fields(self) -> dict[str, str | int | float]:
        fields: dict[str, str | int | float] = {
            "attempt": self.attempt,
            "host": self.target_host,
            "method": self.method,
            "phase": self.phase.value,
            "purpose": self.purpose.value,
            "physical_requests": self.accounting.physical_requests,
            "transmitted_bytes": self.accounting.transmitted_bytes,
            "received_bytes": self.accounting.received_bytes,
        }
        for name, value in (
            ("request_id", self.request_id),
            ("source_id", self.source_id),
            ("status", self.status),
            ("elapsed_seconds", self.elapsed_seconds),
            ("route", self.route.kind if self.route is not None else None),
            ("provider", self.route.provider if self.route is not None else None),
            ("classification", self.classification),
        ):
            if value is not None:
                fields[name] = value
        return fields


class TransportResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    status: int = Field(ge=100, le=599)
    headers: dict[str, str] = Field(default_factory=dict)
    content: bytes
    final_url: str
    route: RouteMetadata = RouteMetadata()
    from_cache: bool = False
    elapsed_seconds: float = Field(default=0, ge=0)
    accounting: TransportAccounting | None = None

    def json_value(self) -> Any:
        location: tuple[int, int] | None = None
        try:
            return json.loads(self.content)
        except json.JSONDecodeError as error:
            location = (error.lineno, error.colno)
        # Raise after leaving the handler: JSONDecodeError retains its full input in
        # ``doc`` and must not escape through exception context or chaining.
        line, column = location
        raise ResponseDecodeFailure(line=line, column=column) from None

    def text(self, encoding: str = "utf-8") -> str:
        return self.content.decode(encoding, errors="replace")


def enforce_response_body_limit(response: TransportResponse, maximum_bytes: int) -> TransportResponse:
    received_bytes = len(response.content)
    if received_bytes > maximum_bytes:
        raise ResponseBodyTooLarge(
            maximum_bytes=maximum_bytes,
            received_bytes=received_bytes,
            accounting=response.accounting,
        )
    return response


class CommerceTransport(Protocol):
    async def request(self, request: TransportRequest) -> TransportResponse: ...
    async def rotate_identity(self, reason: RotationReason) -> None: ...


@runtime_checkable
class RequestScopedIdentityRotation(Protocol):
    """Optional rotation capability retaining the triggering request context."""

    async def rotate_identity_for_request(
        self, reason: RotationReason, request: TransportRequest
    ) -> None: ...


@runtime_checkable
class BrowserSubrequestAuthorizedTransport(Protocol):
    """Marker for transports whose browser traffic owns per-request tokens."""

    browser_subrequests_authorized: Literal[True]


def browser_subrequests_authorized(value: object) -> bool:
    return (
        isinstance(value, BrowserSubrequestAuthorizedTransport)
        and value.browser_subrequests_authorized is True
    )


class TransportCapabilityForwarder:
    """Forward optional capabilities through a transport wrapper in one place."""

    _rotation_capability_backend: CommerceTransport
    _browser_capability_backend: object | None

    def _forward_transport_capabilities(
        self,
        rotation_backend: CommerceTransport,
        *,
        browser_backend: object | None = None,
    ) -> None:
        self._rotation_capability_backend = rotation_backend
        self._browser_capability_backend = (
            rotation_backend if browser_backend is None else browser_backend
        )

    async def rotate_identity_for_request(
        self, reason: RotationReason, request: TransportRequest
    ) -> None:
        backend = self._rotation_capability_backend
        if isinstance(backend, RequestScopedIdentityRotation):
            await backend.rotate_identity_for_request(reason, request)
            return
        await backend.rotate_identity(reason)

    @property
    def browser_subrequests_authorized(self) -> bool:
        return browser_subrequests_authorized(self._browser_capability_backend)


class ResponseCache(Protocol):
    async def get(self, request: TransportRequest) -> TransportResponse | None: ...
    async def put(self, request: TransportRequest, response: TransportResponse) -> None: ...


class ResponseCacheLookup(Protocol):
    """One classified cache read retaining its request-specific write identity."""

    fresh: TransportResponse | None
    stale: TransportResponse | None

    async def put(self, response: TransportResponse) -> None: ...


@runtime_checkable
class StaleResponseCache(ResponseCache, Protocol):
    """Optional cache extension for validators and explicit stale fallback."""

    async def get_with_stale(self, request: TransportRequest) -> ResponseCacheLookup: ...


class RobotsChecker(Protocol):
    async def allowed(self, url: str) -> bool: ...


class RateLimiter(Protocol):
    async def wait(self, request: TransportRequest) -> None: ...
    async def release(self, request: TransportRequest) -> None: ...


class BudgetAuthorization(Protocol):
    """One exclusively authorized network attempt."""

    async def reconcile(self, response_bytes: int) -> None:
        """Replace the byte estimate with the actual response size."""

    async def release(self) -> None:
        """Return an authorization when dispatch did not begin."""


class RequestBudget(Protocol):
    """Attempt budget with non-consuming previews and atomic authorization."""

    def affordable(self, request: TransportRequest) -> bool: ...

    async def authorize(self, request: TransportRequest) -> BudgetAuthorization | None: ...


class TelemetryHooks(Protocol):
    def emit(self, event: str, fields: dict[str, JsonValue]) -> None: ...


@runtime_checkable
class RequestObserver(Protocol):
    def observe_request(self, observation: RequestObservation) -> None: ...


class NullTelemetry:
    def emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        del event, fields

    def observe_request(self, observation: RequestObservation) -> None:
        del observation


class MemoryRequestBudget:
    """Attempt-scoped authorization and actual-attempt accounting."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, *, maximum_requests: int | None = None, maximum_bytes: int | None = None) -> None:
        self.maximum_requests = maximum_requests
        self.maximum_bytes = maximum_bytes
        self.requests = 0
        self.bytes = 0
        self._reserved_bytes = 0
        self._next_authorization_id = 0
        self._authorizations: dict[int, int] = {}
        self._lock = Lock()

    def affordable(self, request: TransportRequest) -> bool:
        return (self.maximum_requests is None or self.requests < self.maximum_requests) and (
            self.maximum_bytes is None
            or self.bytes + self._reserved_bytes + request.estimated_bytes <= self.maximum_bytes
        )

    async def authorize(self, request: TransportRequest) -> BudgetAuthorization | None:
        async with self._lock:
            if not self.affordable(request):
                return None
            authorization_id = self._next_authorization_id
            self._next_authorization_id += 1
            self.requests += 1
            self._reserved_bytes += request.estimated_bytes
            self._authorizations[authorization_id] = request.estimated_bytes
            return _MemoryBudgetAuthorization(self, authorization_id)

    async def _reconcile(self, authorization_id: int, response_bytes: int) -> None:
        if response_bytes < 0:
            raise ValueError("response_bytes must be non-negative")
        async with self._lock:
            try:
                estimated_bytes = self._authorizations.pop(authorization_id)
            except KeyError as error:
                raise RuntimeError("budget authorization already reconciled") from error
            self._reserved_bytes -= estimated_bytes
            self.bytes += response_bytes

    async def _release(self, authorization_id: int) -> None:
        async with self._lock:
            try:
                estimated_bytes = self._authorizations.pop(authorization_id)
            except KeyError as error:
                raise RuntimeError("budget authorization already resolved") from error
            self.requests -= 1
            self._reserved_bytes -= estimated_bytes


@dataclass(slots=True)
class _MemoryBudgetAuthorization:
    budget: MemoryRequestBudget
    authorization_id: int

    async def reconcile(self, response_bytes: int) -> None:
        await self.budget._reconcile(self.authorization_id, response_bytes)

    async def release(self) -> None:
        await self.budget._release(self.authorization_id)


class Timer:
    def __init__(self) -> None:
        self._start = monotonic()

    @property
    def elapsed(self) -> float:
        return monotonic() - self._start
