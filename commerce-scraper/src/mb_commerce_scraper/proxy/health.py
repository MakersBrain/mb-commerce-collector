from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from math import ceil, log2
from types import MappingProxyType


class ProxyFailureReason(StrEnum):
    BLOCKED = "blocked"
    RATE_LIMITED = "rate_limited"
    CAPTCHA = "captcha"
    TRANSPORT_FAILURE = "transport_failure"


@dataclass(frozen=True, slots=True)
class ProxyHealthState:
    consecutive_failures: int = 0
    reason_failures: Mapping[ProxyFailureReason, int] = field(
        default_factory=lambda: MappingProxyType({})
    )
    last_success: datetime | None = None
    last_failure: datetime | None = None
    cooldown_until: datetime | None = None


@dataclass(slots=True)
class _MutableHealthState:
    consecutive_failures: int = 0
    reason_failures: dict[ProxyFailureReason, int] = field(default_factory=dict)
    last_success: datetime | None = None
    last_failure: datetime | None = None
    cooldown_until: datetime | None = None

    def snapshot(self) -> ProxyHealthState:
        return ProxyHealthState(
            consecutive_failures=self.consecutive_failures,
            reason_failures=MappingProxyType(dict(self.reason_failures)),
            last_success=self.last_success,
            last_failure=self.last_failure,
            cooldown_until=self.cooldown_until,
        )


_HealthKey = tuple[str, str, str]


class InMemoryProxyHealth:
    """Bounded process-local proxy health using least-recently-used eviction."""

    def __init__(
        self,
        *,
        initial_cooldown: float = 5.0,
        maximum_cooldown: float = 300.0,
        maximum_entries: int = 10_000,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if initial_cooldown <= 0:
            raise ValueError("initial_cooldown must be positive")
        if maximum_cooldown <= 0:
            raise ValueError("maximum_cooldown must be positive")
        if (
            not isinstance(maximum_entries, int)
            or isinstance(maximum_entries, bool)
            or maximum_entries < 1
        ):
            raise ValueError("maximum_entries must be a positive integer")
        self._states: OrderedDict[_HealthKey, _MutableHealthState] = OrderedDict()
        self.initial_cooldown = initial_cooldown
        self.maximum_cooldown = maximum_cooldown
        self.maximum_entries = maximum_entries
        self._clock = clock or (lambda: datetime.now(UTC))
        ratio = maximum_cooldown / initial_cooldown
        self._maximum_exponent = max(0, ceil(log2(ratio))) if ratio > 1 else 0

    @property
    def entry_count(self) -> int:
        return len(self._states)

    def snapshot(
        self, provider: str, endpoint_id: str, target_host: str
    ) -> ProxyHealthState | None:
        state = self._states.get((provider, endpoint_id, target_host))
        return None if state is None else state.snapshot()

    def available(self, provider: str, endpoint_id: str, target_host: str) -> bool:
        key = (provider, endpoint_id, target_host)
        state = self._states.get(key)
        if state is None:
            return True
        self._states.move_to_end(key)
        return state.cooldown_until is None or state.cooldown_until <= self._clock()

    def success(self, provider: str, endpoint_id: str, target_host: str) -> None:
        state = self._state((provider, endpoint_id, target_host))
        state.consecutive_failures = 0
        state.last_success = self._clock()
        state.cooldown_until = None

    def failure(
        self,
        provider: str,
        endpoint_id: str,
        target_host: str,
        reason: ProxyFailureReason = ProxyFailureReason.TRANSPORT_FAILURE,
    ) -> None:
        reason = ProxyFailureReason(reason)
        now = self._clock()
        state = self._state((provider, endpoint_id, target_host))
        state.consecutive_failures += 1
        state.reason_failures[reason] = state.reason_failures.get(reason, 0) + 1
        state.last_failure = now
        exponent = min(state.consecutive_failures - 1, self._maximum_exponent)
        seconds = min(
            self.maximum_cooldown,
            self.initial_cooldown * 2**exponent,
        )
        state.cooldown_until = now + timedelta(seconds=seconds)

    def _state(self, key: _HealthKey) -> _MutableHealthState:
        state = self._states.get(key)
        if state is not None:
            self._states.move_to_end(key)
            return state
        if len(self._states) >= self.maximum_entries:
            self._states.popitem(last=False)
        state = _MutableHealthState()
        self._states[key] = state
        return state
