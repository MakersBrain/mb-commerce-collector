from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from mb_commerce_scraper.transports import RotationReason


class ProxyBudgetExhausted(RuntimeError):
    """A collection's proxy caps cannot authorize or retain an attempt."""

    def __init__(
        self,
        message: str | None = None,
        *,
        maximum_requests: int | None = None,
        maximum_bytes: int | None = None,
        used_requests: int = 0,
        used_bytes: int = 0,
    ) -> None:
        super().__init__(
            message or "proxy collection request or byte limit exhausted"
        )
        self.maximum_requests = maximum_requests
        self.maximum_bytes = maximum_bytes
        self.used_requests = used_requests
        self.used_bytes = used_bytes


class ProxyKind(StrEnum):
    RESIDENTIAL = "residential"
    MOBILE = "mobile"
    DATACENTER = "datacenter"


class ProxyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source_id: str
    target_host: str
    kind: ProxyKind = ProxyKind.RESIDENTIAL
    country: str | None = None
    region: str | None = None
    city: str | None = None
    sticky: bool = True
    session_ttl_seconds: int | None = Field(default=None, ge=1)
    maximum_requests: int | None = Field(default=None, ge=1)
    maximum_bytes: int | None = Field(default=None, ge=1)
    preferred_providers: tuple[str, ...] = ()
    excluded_providers: tuple[str, ...] = ()


class ProxyEndpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    provider: str
    endpoint_id: str
    protocol: Literal["http", "https", "socks5"]
    host: str
    port: int = Field(ge=1, le=65535)
    kind: ProxyKind
    countries: frozenset[str] = frozenset()

    @field_validator("host")
    @classmethod
    def reject_userinfo(cls, value: str) -> str:
        if "@" in value or "://" in value:
            raise ValueError("proxy host must not contain a URL or user information")
        return value


class ProxyCredentials(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    username: SecretStr
    password: SecretStr

    def __repr__(self) -> str:
        return "ProxyCredentials(username=SecretStr('**********'), password=SecretStr('**********'))"


class BrowserProxyCredentials(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    server: str
    username: SecretStr
    password: SecretStr


class ProxyOutcome(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    target_host: str
    status: int | None = None
    physical_requests: int = Field(default=1, ge=0)
    transmitted_bytes: int = Field(default=0, ge=0)
    received_bytes: int = Field(default=0, ge=0)
    classification: str


class ProxyAttemptAuthorization(Protocol):
    async def reconcile(self, outcome: ProxyOutcome) -> None: ...
    async def release(self) -> None: ...


class BrowserSubrequestOutcome(BaseModel):
    """Secret-free accounting for one continued browser network request."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    status: int | None = Field(default=None, ge=100, le=599)
    transmitted_bytes: int = Field(default=0, ge=0)
    received_bytes: int = Field(default=0, ge=0)
    classification: str


class BrowserSubrequestAuthorization(Protocol):
    """Single-use authorization obtained before a browser request continues."""

    async def reconcile(self, outcome: BrowserSubrequestOutcome) -> None: ...
    async def release(self) -> None: ...


class BrowserSubrequestAuthorizer(Protocol):
    """Authorize one physical browser request without exposing provider state."""

    async def authorize(
        self, estimated_bytes: int
    ) -> BrowserSubrequestAuthorization | None: ...


class ProxyLease(Protocol):
    lease_id: str
    provider: str
    route: ProxyEndpoint
    expires_at: datetime | None
    maximum_bytes: int | None

    def can_start(self, estimated_bytes: int = 0) -> bool:
        """Return a non-authoritative affordability preview."""
        ...
    def http_credentials(self) -> ProxyCredentials: ...
    def browser_credentials(self) -> BrowserProxyCredentials: ...


class ProxyPool(Protocol):
    async def acquire(self, request: ProxyRequest) -> ProxyLease: ...
    async def rotate(self, lease: ProxyLease, reason: RotationReason) -> ProxyLease: ...
    async def authorize(
        self, lease: ProxyLease, estimated_bytes: int
    ) -> ProxyAttemptAuthorization | None: ...
    async def report(self, lease: ProxyLease, outcome: ProxyOutcome) -> None: ...
    async def release(self, lease: ProxyLease) -> None: ...
