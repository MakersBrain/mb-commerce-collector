from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass
class HealthState:
    failures: int = 0
    last_success: datetime | None = None
    last_failure: datetime | None = None
    cooldown_until: datetime | None = None


class InMemoryProxyHealth:
    def __init__(self, *, initial_cooldown: float = 5.0, maximum_cooldown: float = 300.0) -> None:
        self._states: dict[tuple[str, str, str], HealthState] = {}
        self.initial_cooldown = initial_cooldown
        self.maximum_cooldown = maximum_cooldown

    def available(self, provider: str, endpoint_id: str, target_host: str) -> bool:
        state = self._states.get((provider, endpoint_id, target_host))
        return state is None or state.cooldown_until is None or state.cooldown_until <= datetime.now(UTC)

    def success(self, provider: str, endpoint_id: str, target_host: str) -> None:
        state = self._states.setdefault((provider, endpoint_id, target_host), HealthState())
        state.failures = 0
        state.last_success = datetime.now(UTC)
        state.cooldown_until = None

    def failure(self, provider: str, endpoint_id: str, target_host: str) -> None:
        now = datetime.now(UTC)
        state = self._states.setdefault((provider, endpoint_id, target_host), HealthState())
        state.failures += 1
        state.last_failure = now
        seconds = min(self.maximum_cooldown, self.initial_cooldown * 2 ** (state.failures - 1))
        state.cooldown_until = now + timedelta(seconds=seconds)

