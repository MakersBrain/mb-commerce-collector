from __future__ import annotations

import json
from enum import IntEnum, StrEnum
from time import monotonic
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, JsonValue


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
    estimated_bytes: int = Field(default=0, ge=0)
    cache: CachePolicy = CachePolicy.DEFAULT
    browser: BrowserHint = BrowserHint.NEVER


class RouteMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["direct", "proxy", "cache", "browser"] = "direct"
    provider: str | None = None
    endpoint_id: str | None = None
    lease_id: str | None = None


class TransportResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    status: int = Field(ge=100, le=599)
    headers: dict[str, str] = Field(default_factory=dict)
    content: bytes
    final_url: str
    route: RouteMetadata = RouteMetadata()
    from_cache: bool = False
    elapsed_seconds: float = Field(default=0, ge=0)

    def json_value(self) -> Any:
        return json.loads(self.content)

    def text(self, encoding: str = "utf-8") -> str:
        return self.content.decode(encoding, errors="replace")


class CommerceTransport(Protocol):
    async def request(self, request: TransportRequest) -> TransportResponse: ...
    async def rotate_identity(self, reason: RotationReason) -> None: ...


class ResponseCache(Protocol):
    async def get(self, request: TransportRequest) -> TransportResponse | None: ...
    async def put(self, request: TransportRequest, response: TransportResponse) -> None: ...


class RequestBudget(Protocol):
    def affordable(self, request: TransportRequest) -> bool: ...
    def charge(self, request: TransportRequest, response_bytes: int) -> None: ...


class TelemetryHooks(Protocol):
    def emit(self, event: str, fields: dict[str, JsonValue]) -> None: ...


class NullTelemetry:
    def emit(self, event: str, fields: dict[str, JsonValue]) -> None:
        del event, fields


class MemoryRequestBudget:
    """Attempt-scoped authorization and actual-attempt accounting."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, *, maximum_requests: int | None = None, maximum_bytes: int | None = None) -> None:
        self.maximum_requests = maximum_requests
        self.maximum_bytes = maximum_bytes
        self.requests = 0
        self.bytes = 0

    def affordable(self, request: TransportRequest) -> bool:
        return (
            (self.maximum_requests is None or self.requests < self.maximum_requests)
            and (self.maximum_bytes is None or self.bytes + request.estimated_bytes <= self.maximum_bytes)
        )

    def charge(self, request: TransportRequest, response_bytes: int) -> None:
        del request
        self.requests += 1
        self.bytes += response_bytes


class Timer:
    def __init__(self) -> None:
        self._start = monotonic()

    @property
    def elapsed(self) -> float:
        return monotonic() - self._start
