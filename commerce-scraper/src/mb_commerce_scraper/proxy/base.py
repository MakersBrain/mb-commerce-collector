from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from mb_commerce_scraper.transports import RotationReason


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
    transmitted_bytes: int = Field(default=0, ge=0)
    received_bytes: int = Field(default=0, ge=0)
    classification: str


class ProxyLease(Protocol):
    lease_id: str
    provider: str
    route: ProxyEndpoint
    expires_at: datetime | None
    maximum_bytes: int | None

    def can_start(self, estimated_bytes: int = 0) -> bool: ...
    def http_credentials(self) -> ProxyCredentials: ...
    def browser_credentials(self) -> BrowserProxyCredentials: ...


class ProxyPool(Protocol):
    async def acquire(self, request: ProxyRequest) -> ProxyLease: ...
    async def rotate(self, lease: ProxyLease, reason: RotationReason) -> ProxyLease: ...
    async def report(self, lease: ProxyLease, outcome: ProxyOutcome) -> None: ...
    async def release(self, lease: ProxyLease) -> None: ...
