"""Job-scoped Decodo identities and fail-closed shared budget accounting."""

from __future__ import annotations

import json
import math
import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import quote
from uuid import UUID

import httpx

from mb_ceramics_catalogue.observability import metrics

if TYPE_CHECKING:
    from psycopg import AsyncConnection

DECIMAL_MB = 1_000_000
DEFAULT_JOB_BYTES = 25 * DECIMAL_MB
_USERINFO = re.compile(r"(?P<scheme>https?://)[^/@\s]+@", re.IGNORECASE)


class ProxyDenied(RuntimeError):
    """No new paid traffic may start under the current safety state."""


@dataclass(frozen=True)
class ProxyReservationUsage:
    estimated_bytes: int
    request_count: int
    revoked: bool
    exhausted: bool


class ReservationLease(Protocol):
    reservation_id: UUID
    used_bytes: int
    requests: int


def redact_url(value: str) -> str:
    """Remove URL userinfo without changing the useful endpoint identity."""
    return _USERINFO.sub(r"\g<scheme>[REDACTED]@", value)


def _validated_provider(provider: str) -> str:
    if re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", provider) is None:
        raise ProxyDenied("proxy provider is invalid")
    return provider


@dataclass(frozen=True)
class ProxyProfile:
    name: str
    host: str
    port: int
    username: str
    password: str
    api_key: str | None = None
    username_template: str = (
        "user-{username}-country-{country}-session-{session}-sessionduration-{minutes}"
    )
    generation: int = 0

    def username_for(self, country: str | None, session: str, minutes: int) -> str:
        return self.username_template.format(
            username=self.username,
            country=(country or "any").lower(),
            session=session,
            minutes=minutes,
        )


def load_profiles(path: Path) -> dict[str, ProxyProfile]:
    """Read profiles from a mode-0600 mounted JSON secret."""
    stat = path.stat()
    if stat.st_mode & 0o077:
        raise ProxyDenied(f"proxy secret {path} must not be accessible by group or other users")
    raw = json.loads(path.read_text(encoding="utf-8"))
    profiles: dict[str, ProxyProfile] = {}
    for name, value in raw.items():
        if not isinstance(value, dict):
            raise ProxyDenied(f"proxy profile {name!r} is not an object")
        host = str(value.get("host", ""))
        if "://" in host or "@" in host:
            raise ProxyDenied(f"proxy profile {name!r} host must not be a URL")
        profiles[name] = ProxyProfile(
            name=name,
            host=host,
            port=int(value["port"]),
            username=str(value["username"]),
            password=str(value["password"]),
            api_key=str(value["api_key"]) if value.get("api_key") else None,
            username_template=str(value.get("username_template") or ProxyProfile.username_template),
            generation=int(value.get("generation", 0)),
        )
    return profiles


def load_api_key(path: Path) -> str:
    """Read only DECODO_API_KEY from a private env-style mounted secret."""
    stat = path.stat()
    if stat.st_mode & 0o077:
        raise ProxyDenied(f"proxy API secret {path} must not be accessible by group or other users")
    found: str | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        if separator and name.strip() == "DECODO_API_KEY":
            found = value.strip().strip('"').strip("'")
    if not found:
        raise ProxyDenied(f"proxy API secret {path} has no DECODO_API_KEY")
    return found


@dataclass
class ProxyLease:
    reservation_id: UUID
    job_id: UUID
    profile: ProxyProfile
    country: str | None
    session: str
    session_minutes: int
    max_bytes: int
    protocol: str = "http"
    used_bytes: int = 0
    requests: int = 0

    @classmethod
    def build(
        cls, reservation_id: UUID, job_id: UUID, profile: ProxyProfile,
        country: str | None, session_minutes: int, max_bytes: int, protocol: str = "http",
    ) -> ProxyLease:
        return cls(
            reservation_id, job_id, profile, country,
            secrets.token_hex(12), session_minutes, max_bytes, protocol,
        )

    @property
    def username(self) -> str:
        return self.profile.username_for(self.country, self.session, self.session_minutes)

    @property
    def url(self) -> str:
        scheme = "socks5h" if self.protocol == "socks5" else self.protocol
        return (
            f"{scheme}://{quote(self.username, safe='')}:{quote(self.profile.password, safe='')}"
            f"@{self.profile.host}:{self.profile.port}"
        )

    @property
    def browser_proxy(self) -> dict[str, str]:
        return {
            "server": f"{self.protocol}://{self.profile.host}:{self.profile.port}",
            "username": self.username,
            "password": self.profile.password,
        }

    def ensure_request_allowed(self) -> None:
        if self.used_bytes >= self.max_bytes:
            raise ProxyDenied("job proxy reservation is exhausted")

    def account(self, tx_bytes: int, rx_bytes: int, requests: int = 1) -> None:
        self.used_bytes += max(0, tx_bytes) + max(0, rx_bytes)
        self.requests += max(0, requests)

    def rotate_session(self) -> None:
        """Request a new provider identity without changing the reservation."""
        self.session = secrets.token_hex(12)

    @property
    def display_name(self) -> str:
        return f"decodo/{self.profile.name}/{self.country or 'any'}/{self.session[:6]}"


async def reserve(
    connection: AsyncConnection[Any], *, job_id: UUID | None = None,
    probe_id: UUID | None = None, profile: str, profile_id: UUID | None = None,
    route_id: UUID | None = None, cycle_start: datetime | None = None,
    cycle_end: datetime | None = None, requested_bytes: int = DEFAULT_JOB_BYTES,
    pilot: bool = False, secret_generation: int = 0, provider: str = "decodo",
) -> UUID:
    """Atomically reserve across job, day, pilot, and billing-cycle limits."""
    if (job_id is None) == (probe_id is None):
        raise ProxyDenied("a proxy reservation requires exactly one job or probe")
    if requested_bytes <= 0:
        raise ProxyDenied("proxy reservation bytes must be positive")
    provider = _validated_provider(provider)
    if profile_id is None or route_id is None:
        raise ProxyDenied("durable proxy reservation requires profile and route identities")
    if cycle_start is not None and cycle_start.tzinfo is None:
        raise ProxyDenied("proxy billing-cycle boundaries must include UTC offsets")
    if cycle_end is not None and cycle_end.tzinfo is None:
        raise ProxyDenied("proxy billing-cycle boundaries must include UTC offsets")
    now = datetime.now(UTC)
    async with connection.transaction():
        cycle = await connection.execute(
            """
            select * from catalogue.proxy_budget_cycles
             where provider = %(provider)s and lifecycle = 'active'
               and cycle_start <= now() and cycle_end > now()
             for update
            """,
            {"provider": provider},
        )
        rows = await cycle.fetchall()
        if len(rows) > 1:
            raise ProxyDenied(f"multiple active {provider} billing cycles exist")
        row = rows[0] if rows else None
        if row is None:
            raise ProxyDenied(f"{provider} billing cycle has not been reconciled and opened")
        if cycle_start is not None and row["cycle_start"] != cycle_start:
            raise ProxyDenied("configured billing-cycle start disagrees with the active ledger")
        if cycle_end is not None and row["cycle_end"] != cycle_end:
            raise ProxyDenied("configured billing-cycle end disagrees with the active ledger")
        cycle_start = row["cycle_start"]
        cycle_end = row["cycle_end"]
        if not (cycle_start <= now < cycle_end):
            raise ProxyDenied("active proxy billing cycle does not cover the current time")
        if row["kill_switch"] or not row["reconciliation_ok"] or row["reconciled_at"] is None:
            raise ProxyDenied(
                f"{provider} reconciliation is unsafe or the kill switch is active"
            )
        identity_cursor = await connection.execute(
            """
            select p.id
              from catalogue.proxy_profiles p
              join catalogue.proxy_routes r
                on r.id = %(route_id)s and r.profile_id = p.id
             where p.id = %(profile_id)s and p.provider = %(provider)s
               and p.logical_name = %(profile)s
               and p.secret_generation = %(generation)s
               and p.enabled and p.lifecycle = 'enabled'
               and r.enabled and r.retired_at is null
             for share of p, r
            """,
            {
                "provider": provider,
                "profile": profile,
                "profile_id": profile_id,
                "route_id": route_id,
                "generation": secret_generation,
            },
        )
        if await identity_cursor.fetchone() is None:
            raise ProxyDenied("proxy profile or route snapshot is no longer active")
        allocation_cursor = await connection.execute(
            """
            select a.allocated_bytes,
                   coalesce(sum(r.reserved_bytes) filter (where r.state in
                     ('active', 'revocation_requested')), 0) as active
              from catalogue.proxy_profile_allocations a
              left join catalogue.proxy_reservations r
                on r.profile_id = a.profile_id and r.provider = a.provider
               and r.cycle_start = a.cycle_start
             where a.provider = %(provider)s and a.cycle_start = %(start)s
               and a.profile_id = %(profile_id)s
             group by a.allocated_bytes
            """,
            {
                "provider": provider,
                "start": cycle_start,
                "profile_id": profile_id,
            },
        )
        allocation = await allocation_cursor.fetchone()
        if allocation is None or allocation["active"] + requested_bytes > allocation["allocated_bytes"]:
            raise ProxyDenied("proxy profile allocation would be exceeded")
        usage_cursor = await connection.execute(
            """
            select
              coalesce(sum(reserved_bytes) filter (
                where state in ('active', 'revocation_requested')), 0) active,
              coalesce(sum(estimated_bytes) filter (where created_at >= date_trunc('day', now())), 0) daily,
              coalesce(sum(estimated_bytes) filter (where pilot), 0) pilot_used
            from catalogue.proxy_reservations
            where provider = %(provider)s and cycle_start = %(start)s
            """,
            {"provider": provider, "start": cycle_start},
        )
        usage = await usage_cursor.fetchone()
        assert usage is not None
        accounted = max(row["provider_reported_bytes"], row["application_bytes"])
        active = usage["active"]
        if accounted + active + requested_bytes > row["operational_bytes"]:
            raise ProxyDenied(
                f"{provider} operational billing-cycle ceiling would be exceeded"
            )
        remaining_days = max(1, math.ceil((cycle_end - now).total_seconds() / 86_400))
        dynamic_daily = min(
            row["daily_bytes"], max(0, (row["operational_bytes"] - accounted - active) // remaining_days)
        )
        if usage["daily"] + active + requested_bytes > dynamic_daily:
            raise ProxyDenied(f"{provider} daily allocation would be exceeded")
        if pilot:
            # Two different denials, and one message for both sent an operator
            # looking at a budget that was 18% used: the-ceramic-shop failed
            # five nights running because the pilot had been *stopped*, not
            # because it had spent anything.
            if not row["pilot_active"]:
                raise ProxyDenied(f"{provider} pilot is not active on this billing cycle")
            if usage["pilot_used"] + active + requested_bytes > row["pilot_bytes"]:
                raise ProxyDenied(f"{provider} pilot allocation would be exceeded")
        cursor = await connection.execute(
            """
            insert into catalogue.proxy_reservations
              (job_id, probe_id, purpose, provider, profile, profile_id, route_id,
               cycle_start, reserved_bytes, pilot, secret_generation)
            values (%(job)s, %(probe)s, %(purpose)s, %(provider)s, %(profile)s,
                    %(profile_id)s, %(route_id)s, %(start)s, %(bytes)s, %(pilot)s,
                    %(generation)s)
            returning id
            """,
            {
                "job": job_id, "probe": probe_id,
                "provider": provider,
                "purpose": "job" if job_id is not None else "probe",
                "profile": profile, "profile_id": profile_id, "route_id": route_id,
                "start": cycle_start, "bytes": requested_bytes, "pilot": pilot,
                "generation": secret_generation,
            },
        )
        inserted = await cursor.fetchone()
        assert inserted is not None
        metrics.proxy_reservation("active")
        return inserted["id"]


async def close_reservation(
    connection: AsyncConnection[Any], lease: ReservationLease
) -> None:
    """Close a reservation and monotonically advance application accounting."""
    async with connection.transaction():
        identity_cursor = await connection.execute(
            """
            select provider, cycle_start
              from catalogue.proxy_reservations
             where id = %(id)s
            """,
            {"id": lease.reservation_id},
        )
        identity = await identity_cursor.fetchone()
        if identity is None:
            return
        # Every paid-traffic transition takes the cycle before an individual
        # reservation. Profile draining uses the same order, so worker cleanup
        # cannot deadlock against a concurrent control-plane revocation.
        await connection.execute(
            """
            select 1
              from catalogue.proxy_budget_cycles
             where provider = %(provider)s and cycle_start = %(start)s
             for update
            """,
            {"provider": identity["provider"], "start": identity["cycle_start"]},
        )
        cursor = await connection.execute(
            """
            update catalogue.proxy_reservations
               set estimated_bytes = greatest(estimated_bytes, %(bytes)s),
                   request_count = greatest(request_count, %(requests)s),
                   state = 'closed', closed_at = now()
             where id = %(id)s and provider = %(provider)s
               and cycle_start = %(start)s
               and state in ('active', 'revocation_requested')
            returning provider, cycle_start, estimated_bytes
            """,
            {
                "id": lease.reservation_id,
                "provider": identity["provider"],
                "start": identity["cycle_start"],
                "bytes": lease.used_bytes,
                "requests": lease.requests,
            },
        )
        row = await cursor.fetchone()
        late_settlement = False
        if row is None:
            late_cursor = await connection.execute(
                """
                update catalogue.proxy_reservations
                   set estimated_bytes = greatest(estimated_bytes, %(bytes)s),
                       request_count = greatest(request_count, %(requests)s)
                 where id = %(id)s and provider = %(provider)s
                   and cycle_start = %(start)s and state = 'cancelled'
                   and (estimated_bytes < %(bytes)s or request_count < %(requests)s)
                returning provider, cycle_start, estimated_bytes
                """,
                {
                    "id": lease.reservation_id,
                    "provider": identity["provider"],
                    "start": identity["cycle_start"],
                    "bytes": lease.used_bytes,
                    "requests": lease.requests,
                },
            )
            row = await late_cursor.fetchone()
            late_settlement = row is not None
        if row:
            if not late_settlement:
                metrics.proxy_reservation("closed")
            metrics.proxy_bytes("application", row["estimated_bytes"])
            await connection.execute(
                """
                update catalogue.proxy_budget_cycles
                   set application_bytes = greatest(
                     application_bytes,
                     (select coalesce(sum(estimated_bytes), 0)
                        from catalogue.proxy_reservations
                       where provider = %(provider)s and cycle_start = %(start)s
                         and state in ('closed', 'cancelled'))
                   ),
                   kill_switch = kill_switch or (
                     select coalesce(sum(estimated_bytes), 0) >= operational_bytes
                       from catalogue.proxy_reservations
                      where provider = %(provider)s and cycle_start = %(start)s
                        and state in ('closed', 'cancelled')
                   )
                 where provider = %(provider)s and cycle_start = %(start)s
                """,
                {"provider": row["provider"], "start": row["cycle_start"]},
            )
            await connection.execute(
                """
                insert into catalogue.proxy_reconcile_requests
                       (provider, reason, reservation_id, dedup_key)
                values (%(provider)s, %(reason)s, %(reservation)s, %(dedup)s)
                on conflict (dedup_key) do nothing
                """,
                {
                    "provider": row["provider"],
                    "reservation": lease.reservation_id,
                    "reason": (
                        "reservation_late_settled"
                        if late_settlement
                        else "reservation_closed"
                    ),
                    "dedup": (
                        f"reservation:{lease.reservation_id}:late-settlement"
                        if late_settlement
                        else f"reservation:{lease.reservation_id}"
                    ),
                },
            )


async def authorize_reservation_attempt(
    connection: AsyncConnection[Any],
    *,
    reservation_id: UUID,
    estimated_bytes: int,
    maximum_requests: int | None,
) -> UUID | None:
    """Atomically retain capacity for one physical attempt.

    The reservation row lock serializes authorizations across workers. The
    separate attempt row lets an undispatched authorization return its estimate
    without ever counting that estimate as paid usage.
    """
    if estimated_bytes < 0:
        raise ValueError("proxy attempt estimate must be non-negative")
    if maximum_requests is not None and maximum_requests < 1:
        raise ValueError("proxy request cap must be positive")
    async with connection.transaction():
        # Lock the owning cycle, profile, route, and reservation in that order.
        # Holding each outer lock while acquiring the next prevents a committed
        # control-plane transition from racing a new physical-attempt token into
        # existence.
        cycle_cursor = await connection.execute(
            """
            select c.lifecycle, c.kill_switch, c.reconciliation_ok,
                   c.reconciled_at,
                   c.cycle_start <= now() and c.cycle_end > now() as current
              from catalogue.proxy_reservations r
              join catalogue.proxy_budget_cycles c
                on c.provider = r.provider and c.cycle_start = r.cycle_start
             where r.id = %(id)s
             for update of c
            """,
            {"id": reservation_id},
        )
        cycle = await cycle_cursor.fetchone()
        if cycle is None:
            raise ProxyDenied("proxy reservation has no billing cycle")
        if (
            cycle["lifecycle"] != "active"
            or not cycle["current"]
            or cycle["kill_switch"]
            or not cycle["reconciliation_ok"]
            or cycle["reconciled_at"] is None
        ):
            raise ProxyDenied("proxy billing cycle does not authorize new paid traffic")
        profile_cursor = await connection.execute(
            """
            select p.enabled, p.lifecycle,
                   p.secret_generation = r.secret_generation as secret_current
              from catalogue.proxy_reservations r
              join catalogue.proxy_profiles p
                on p.id = r.profile_id and p.provider = r.provider
               and p.logical_name = r.profile
             where r.id = %(id)s
             for share of p
            """,
            {"id": reservation_id},
        )
        profile = await profile_cursor.fetchone()
        if (
            profile is None
            or not profile["enabled"]
            or profile["lifecycle"] != "enabled"
            or not profile["secret_current"]
        ):
            raise ProxyDenied("proxy profile does not authorize new paid traffic")
        route_cursor = await connection.execute(
            """
            select r.enabled, r.retired_at is null as current
              from catalogue.proxy_reservations x
              join catalogue.proxy_routes r
                on r.id = x.route_id and r.provider = x.provider
               and r.profile_id = x.profile_id
             where x.id = %(id)s
             for share of r
            """,
            {"id": reservation_id},
        )
        route = await route_cursor.fetchone()
        if route is None or not route["enabled"] or not route["current"]:
            raise ProxyDenied("proxy route does not authorize new paid traffic")
        cursor = await connection.execute(
            """
            select x.reserved_bytes, x.estimated_bytes, x.request_count,
                   x.revocation_requested or
                       x.state = 'revocation_requested' as revoked
              from catalogue.proxy_reservations x
              join catalogue.proxy_profiles p
                on p.id = x.profile_id and p.provider = x.provider
               and p.logical_name = x.profile
               and p.secret_generation = x.secret_generation
              join catalogue.proxy_routes r
                on r.id = x.route_id and r.provider = x.provider
               and r.profile_id = x.profile_id
             where x.id = %(id)s
               and x.state in ('active', 'revocation_requested')
               and p.enabled and p.lifecycle = 'enabled'
               and r.enabled and r.retired_at is null
             for update of x
            """,
            {"id": reservation_id},
        )
        reservation = await cursor.fetchone()
        if reservation is None:
            raise ProxyDenied("proxy reservation is not active")
        if reservation["revoked"]:
            raise ProxyDenied("proxy reservation was revoked")
        pending_cursor = await connection.execute(
            """
            select coalesce(sum(estimated_bytes), 0) as bytes, count(*) as requests
              from catalogue.proxy_attempt_authorizations
             where reservation_id = %(id)s and state = 'authorized'
            """,
            {"id": reservation_id},
        )
        pending = await pending_cursor.fetchone()
        assert pending is not None
        if (
            reservation["estimated_bytes"] + pending["bytes"] + estimated_bytes
            > reservation["reserved_bytes"]
        ) or (
            maximum_requests is not None
            and reservation["request_count"] + pending["requests"] >= maximum_requests
        ):
            return None
        inserted = await connection.execute(
            """
            insert into catalogue.proxy_attempt_authorizations
                   (reservation_id, estimated_bytes)
            values (%(reservation)s, %(bytes)s)
            returning id
            """,
            {"reservation": reservation_id, "bytes": estimated_bytes},
        )
        row = await inserted.fetchone()
        assert row is not None
        return row["id"]


async def reconcile_reservation_attempt(
    connection: AsyncConnection[Any],
    *,
    authorization_id: UUID,
    actual_bytes: int,
    physical_requests: int,
) -> ProxyReservationUsage:
    """Exactly once reconcile a dispatched attempt into durable actuals."""
    if actual_bytes < 0 or physical_requests < 0:
        raise ValueError("proxy attempt actual counters must be non-negative")
    async with connection.transaction():
        cursor = await connection.execute(
            """
            select reservation_id, state, actual_bytes, physical_requests
              from catalogue.proxy_attempt_authorizations
             where id = %(id)s
             for update
            """,
            {"id": authorization_id},
        )
        authorization = await cursor.fetchone()
        if authorization is None:
            raise ProxyDenied("proxy attempt authorization does not exist")
        if authorization["state"] == "released":
            raise ProxyDenied("proxy attempt authorization was released")
        if authorization["state"] == "authorized":
            await connection.execute(
                """
                update catalogue.proxy_attempt_authorizations
                   set state = 'reconciled', actual_bytes = %(bytes)s,
                       physical_requests = %(requests)s, resolved_at = now()
                 where id = %(id)s
                """,
                {
                    "id": authorization_id,
                    "bytes": actual_bytes,
                    "requests": physical_requests,
                },
            )
            await connection.execute(
                """
                update catalogue.proxy_reservations
                   set estimated_bytes = estimated_bytes + %(bytes)s,
                       request_count = request_count + %(requests)s
                 where id = %(reservation)s
                """,
                {
                    "reservation": authorization["reservation_id"],
                    "bytes": actual_bytes,
                    "requests": physical_requests,
                },
            )
        elif (
            authorization["actual_bytes"] != actual_bytes
            or authorization["physical_requests"] != physical_requests
        ):
            raise ProxyDenied("proxy attempt was already reconciled with different actuals")
        usage_cursor = await connection.execute(
            """
            select estimated_bytes, request_count, reserved_bytes,
                   revocation_requested or state = 'revocation_requested' as revoked
              from catalogue.proxy_reservations
             where id = %(reservation)s
            """,
            {"reservation": authorization["reservation_id"]},
        )
        usage = await usage_cursor.fetchone()
        if usage is None:
            raise ProxyDenied("proxy reservation does not exist")
        return ProxyReservationUsage(
            estimated_bytes=usage["estimated_bytes"],
            request_count=usage["request_count"],
            revoked=bool(usage["revoked"]),
            exhausted=usage["estimated_bytes"] > usage["reserved_bytes"],
        )


async def release_reservation_attempt(
    connection: AsyncConnection[Any], *, authorization_id: UUID
) -> None:
    """Release capacity for an attempt proven not to have dispatched."""
    cursor = await connection.execute(
        """
        update catalogue.proxy_attempt_authorizations
           set state = 'released', resolved_at = now()
         where id = %(id)s and state = 'authorized'
        returning id
        """,
        {"id": authorization_id},
    )
    row = await cursor.fetchone()
    if row is not None:
        return
    state_cursor = await connection.execute(
        "select state from catalogue.proxy_attempt_authorizations where id = %(id)s",
        {"id": authorization_id},
    )
    state = await state_cursor.fetchone()
    if state is None:
        raise ProxyDenied("proxy attempt authorization does not exist")
    if state["state"] != "released":
        raise ProxyDenied("a dispatched proxy attempt cannot be released")


async def reservation_revoked(connection: AsyncConnection[Any], reservation_id: UUID) -> bool:
    cursor = await connection.execute(
        """select revocation_requested or state = 'revocation_requested' as revoked
             from catalogue.proxy_reservations where id = %(id)s""",
        {"id": reservation_id},
    )
    row = await cursor.fetchone()
    return bool(row and row["revoked"])


async def reconcile(
    connection: AsyncConnection[Any], *, cycle_start: datetime,
    provider_reported_bytes: int, successful: bool, provider: str = "decodo",
) -> None:
    """Record provider usage without ever automatically lowering the ledger."""
    provider = _validated_provider(provider)
    await connection.execute(
        """
        update catalogue.proxy_budget_cycles
           set provider_reported_bytes = greatest(provider_reported_bytes, %(reported)s),
               reconciled_at = case when %(ok)s then now() else reconciled_at end,
               reconciliation_ok = %(ok)s,
               kill_switch = kill_switch or greatest(provider_reported_bytes, %(reported)s) >= operational_bytes
         where provider = %(provider)s and cycle_start = %(start)s
        """,
        {
            "provider": provider,
            "reported": max(0, provider_reported_bytes),
            "ok": successful,
            "start": cycle_start,
        },
    )


async def open_cycle(
    connection: AsyncConnection[Any], *, cycle_start: datetime, cycle_end: datetime,
    provider_reported_bytes: int,
) -> None:
    """Open the exact dashboard-confirmed cycle; never infer a calendar reset."""
    if cycle_start.tzinfo is None or cycle_end.tzinfo is None or cycle_end <= cycle_start:
        raise ProxyDenied("billing-cycle boundaries must be ordered offset-aware timestamps")
    overlap = await connection.execute(
        """select 1 from catalogue.proxy_budget_cycles
            where provider = 'decodo' and cycle_start <> %(start)s
              and tstzrange(cycle_start, cycle_end, '[)') && tstzrange(%(start)s, %(end)s, '[)')""",
        {"start": cycle_start, "end": cycle_end},
    )
    if await overlap.fetchone():
        raise ProxyDenied("Decodo billing cycle overlaps an existing ledger cycle")
    async with connection.transaction():
        await connection.execute("select pg_advisory_xact_lock(hashtext('proxy:decodo'))")
        await connection.execute(
            """update catalogue.proxy_budget_cycles
                  set lifecycle = 'closed', closed_at = coalesce(closed_at, now()),
                      closed_by = coalesce(closed_by, 'legacy-open-cycle')
                where provider = 'decodo' and lifecycle = 'active' and cycle_end <= now()"""
        )
        await connection.execute(
            """
            insert into catalogue.proxy_budget_cycles
              (provider, cycle_start, cycle_end, provider_reported_bytes,
               reconciled_at, reconciliation_ok, kill_switch, lifecycle, opened_at, opened_by)
            values ('decodo', %(start)s, %(end)s, %(reported)s, now(), true,
                    %(reported)s >= 2400000000, 'active', now(), 'legacy-open-cycle')
            on conflict (provider, cycle_start) do update
              set provider_reported_bytes = greatest(
                    catalogue.proxy_budget_cycles.provider_reported_bytes,
                    excluded.provider_reported_bytes),
                  reconciled_at = now(), reconciliation_ok = true,
                  kill_switch = catalogue.proxy_budget_cycles.kill_switch
                             or excluded.provider_reported_bytes >= catalogue.proxy_budget_cycles.operational_bytes
            """,
            {"start": cycle_start, "end": cycle_end, "reported": max(0, provider_reported_bytes)},
        )


def secret_values(profiles: dict[str, ProxyProfile]) -> set[str]:
    return {
        value
        for profile in profiles.values()
        for value in (profile.username, profile.password, profile.api_key)
        if value
    }


def scrub_secrets(value: str, secrets_to_remove: set[str]) -> str:
    cleaned = redact_url(value)
    for secret in secrets_to_remove:
        if secret:
            cleaned = cleaned.replace(secret, "[REDACTED]")
    return cleaned


async def provider_usage(
    profile: ProxyProfile,
    cycle_start: datetime,
    cycle_end: datetime,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> int:
    """Read Decodo's billing-cycle upload+download total in decimal bytes."""
    if not profile.api_key:
        raise ProxyDenied(f"proxy profile {profile.name!r} has no Decodo statistics API key")
    fmt = "%Y-%m-%d %H:%M:%S"
    async with httpx.AsyncClient(transport=transport, timeout=30) as client:
        response = await client.post(
            "https://api.decodo.com/api/v2/statistics/traffic",
            headers={"authorization": profile.api_key, "accept": "application/json"},
            json={
                "proxyType": "residential_proxies",
                "startDate": cycle_start.astimezone(UTC).strftime(fmt),
                "endDate": min(cycle_end, datetime.now(UTC)).astimezone(UTC).strftime(fmt),
                "groupBy": "day",
                "limit": 500,
                "page": 1,
                "sortBy": "grouping_key",
                "sortOrder": "asc",
            },
        )
        response.raise_for_status()
        payload = response.json()
    try:
        return max(0, int(payload["metadata"]["totals"]["total_rx_tx"]))
    except (KeyError, TypeError, ValueError) as error:
        raise ProxyDenied("Decodo statistics response omitted total_rx_tx") from error
