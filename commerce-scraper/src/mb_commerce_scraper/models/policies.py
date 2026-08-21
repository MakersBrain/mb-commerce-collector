from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class RobotsPolicy(StrEnum):
    OBEY = "obey"
    IGNORE = "ignore"


class BrowserPolicy(StrEnum):
    NEVER = "never"
    ALLOW = "allow"
    REQUIRE = "require"


class ProxyMode(StrEnum):
    NEVER = "never"
    ALWAYS = "always"
    FALLBACK = "fallback"
    FAILOVER = "failover"
    ROUND_ROBIN = "round_robin"


class FetchPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    delay: float = Field(default=0.0, ge=0)
    concurrency: int = Field(default=1, ge=1)
    robots: RobotsPolicy = RobotsPolicy.OBEY
    timeout_seconds: float = Field(default=30.0, gt=0)
    browser: BrowserPolicy = BrowserPolicy.NEVER


class ProxyPolicyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: ProxyMode = ProxyMode.NEVER
    country: str | None = Field(default=None, min_length=2, max_length=2)
    provider_preferences: tuple[str, ...] = ()
    maximum_bytes: int | None = Field(default=None, ge=1)
