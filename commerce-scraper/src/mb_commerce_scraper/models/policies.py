from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    maximum_requests: int | None = Field(default=None, ge=1)
    maximum_bytes: int | None = Field(default=None, ge=1)

    @field_validator("country")
    @classmethod
    def validate_country(cls, value: str | None) -> str | None:
        if value is not None and (not value.isascii() or not value.isalpha() or not value.isupper()):
            raise ValueError("country must be an uppercase ASCII alpha-2 code")
        return value

    @field_validator("provider_preferences")
    @classmethod
    def validate_provider_preferences(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not provider or provider != provider.strip() for provider in value):
            raise ValueError("provider preferences must be non-empty trimmed names")
        if len(set(value)) != len(value):
            raise ValueError("provider preferences must be unique")
        return value
