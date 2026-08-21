from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class RoutingMode(StrEnum):
    NEVER = "never"
    ALWAYS = "always"
    FALLBACK = "fallback"
    FAILOVER = "failover"
    ROUND_ROBIN = "round_robin"


class ProxyRouting(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    mode: RoutingMode = RoutingMode.NEVER
    country: str | None = None
    provider_preferences: tuple[str, ...] = ()

    @classmethod
    def fallback(cls, *, country: str | None = None) -> ProxyRouting:
        return cls(mode=RoutingMode.FALLBACK, country=country)

