from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

from .base import (
    BrowserHint,
    CachePolicy,
    CommerceTransport,
    RequestPriority,
    RequestPurpose,
    TransportFailure,
    TransportRequest,
)


class RobotsFetchFailurePolicy(StrEnum):
    """Decision to use when robots.txt cannot be obtained safely."""

    DENY = "deny"
    ALLOW = "allow"


@dataclass(frozen=True, slots=True)
class _Rules:
    parser: RobotFileParser | None = None
    forced_decision: bool | None = None

    def allowed(self, url: str, user_agent: str) -> bool:
        if self.forced_decision is not None:
            return self.forced_decision
        assert self.parser is not None
        return self.parser.can_fetch(user_agent, url)


class CachedRobotsChecker:
    """Fetch and cache one robots policy per HTTP origin.

    ``transport`` must be the raw transport below ``MiddlewareTransport``. Using
    the middleware-wrapped transport here would recursively invoke this checker.
    Ignoring robots is deliberately composed by omitting the checker rather than
    by adding a bypass flag to this obey-only component.
    """

    def __init__(
        self,
        transport: CommerceTransport,
        *,
        user_agent: str = "*",
        failure_policy: RobotsFetchFailurePolicy = RobotsFetchFailurePolicy.DENY,
        transport_failure_policy: RobotsFetchFailurePolicy | None = None,
        server_failure_policy: RobotsFetchFailurePolicy | None = None,
        maximum_origins: int = 1_000,
    ) -> None:
        if not user_agent.strip():
            raise ValueError("user_agent must not be blank")
        if maximum_origins < 1:
            raise ValueError("maximum_origins must be positive")
        self._transport = transport
        self._user_agent = user_agent
        self._transport_failure_decision = (
            transport_failure_policy or failure_policy
        ) is RobotsFetchFailurePolicy.ALLOW
        self._server_failure_decision = (
            server_failure_policy or failure_policy
        ) is RobotsFetchFailurePolicy.ALLOW
        self._maximum_origins = maximum_origins
        self._rules: dict[str, _Rules] = {}
        self._inflight: dict[str, asyncio.Task[_Rules]] = {}
        self._lock = asyncio.Lock()

    async def allowed(self, url: str) -> bool:
        origin = _safe_origin(url)
        if origin is None:
            return self._transport_failure_decision

        rules = self._rules.get(origin)
        if rules is None:
            rules = await self._rules_for(origin)
        return rules.allowed(url, self._user_agent)

    async def _rules_for(self, origin: str) -> _Rules:
        async with self._lock:
            cached = self._rules.get(origin)
            if cached is not None:
                return cached
            task = self._inflight.get(origin)
            if task is None:
                task = asyncio.create_task(self._fetch_rules(origin))
                self._inflight[origin] = task

        try:
            rules = await task
        finally:
            async with self._lock:
                self._inflight.pop(origin, None)

        async with self._lock:
            existing = self._rules.get(origin)
            if existing is not None:
                return existing
            if len(self._rules) >= self._maximum_origins:
                self._rules.pop(next(iter(self._rules)))
            self._rules[origin] = rules
        return rules

    async def _fetch_rules(self, origin: str) -> _Rules:
        request = TransportRequest(
            url=f"{origin}/robots.txt",
            purpose=RequestPurpose.ROBOTS,
            priority=RequestPriority.DISCOVERY,
            required=True,
            cache=CachePolicy.BYPASS,
            browser=BrowserHint.NEVER,
        )
        try:
            response = await self._transport.request(request)
        except TransportFailure:
            # Do not propagate backend messages: they may contain proxy credentials
            # or a secret-bearing redirect URL.
            return _Rules(forced_decision=self._transport_failure_decision)

        if 200 <= response.status < 300:
            parser = RobotFileParser()
            parser.set_url(request.url)
            parser.parse(response.text().splitlines())
            return _Rules(parser=parser)
        if response.status in {401, 403}:
            return _Rules(forced_decision=False)
        if 400 <= response.status < 500:
            return _Rules(forced_decision=True)
        return _Rules(forced_decision=self._server_failure_decision)


def _safe_origin(url: str) -> str | None:
    try:
        parsed = urlsplit(url)
        if parsed.scheme.casefold() not in {"http", "https"} or parsed.hostname is None:
            return None
        hostname = parsed.hostname.casefold()
        if ":" in hostname:
            hostname = f"[{hostname}]"
        port = parsed.port
    except ValueError:
        return None

    default_port = 80 if parsed.scheme.casefold() == "http" else 443
    netloc = hostname if port in {None, default_port} else f"{hostname}:{port}"
    return urlunsplit((parsed.scheme.casefold(), netloc, "", "", ""))
