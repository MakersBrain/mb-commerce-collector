"""Webshare API v2 adapter.

The best structural fit of the three so far, and for one reason: its usage
endpoint reports `bandwidth_total` as an **integer count of bytes**. Decodo's
unit is undocumented and IPRoyal's is a GB float; this one needs no conversion
at all on the path that reconciles the budget, which is the path where a
rounding error becomes a permanent discrepancy.

It also has a real renewal term -- `/subscription/` carries `start_date` and
`end_date` -- so unlike IPRoyal a cycle can be proposed from it.

What it cannot do is provision a sub-user on this system's terms. A Webshare
sub-user is created with a `label`; it has no username or password field,
because its proxy credentials are issued by Webshare through the proxy list
rather than chosen by the caller. `create_subuser` therefore refuses. That is
not a gap to be worked around with a plausible-looking mapping: the caller would
believe it had set a password that does not exist. Webshare is a provider whose
traffic can be reconciled and budgeted, and whose profiles cannot be rotated,
and the registry says so with `can_provision_subusers=False`.

Sources, checked 2026-08-16:
  https://apidocs.webshare.io/
  https://apidocs.webshare.io/subuser
  https://apidocs.webshare.io/subscription
  https://apidocs.webshare.io/subscription/plan
  https://apidocs.webshare.io/proxystats/aggregate
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from .base import ProviderError, Subscription, SubUser, UsageBucket, UsageReport

DECIMAL_GB = 1_000_000_000

#: The aggregate stats endpoint refuses a window older than this.
STATS_MAX_AGE = timedelta(days=90)


class WebshareProvider:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://proxy.webshare.io/api/v2",
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 30,
    ) -> None:
        if not api_key:
            raise ProviderError("missing_api_key", "Webshare API key is not configured")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.transport = transport
        self.timeout = timeout

    async def _request(
        self, method: str, path: str, *, json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None, mutation: bool = False,
    ) -> Any:
        response: httpx.Response | None = None
        last_network_error: httpx.TimeoutException | httpx.NetworkError | None = None
        attempts = 1 if mutation else 3
        async with httpx.AsyncClient(
            transport=self.transport, timeout=self.timeout, follow_redirects=False,
        ) as client:
            for attempt in range(attempts):
                try:
                    response = await client.request(
                        method,
                        f"{self.base_url}{path}",
                        # "Token", not "Bearer": Webshare uses DRF's token scheme.
                        headers={
                            "authorization": f"Token {self.api_key}",
                            "accept": "application/json",
                        },
                        json=json,
                        params=params,
                    )
                except (httpx.TimeoutException, httpx.NetworkError) as error:
                    last_network_error = error
                    if attempt + 1 < attempts:
                        await asyncio.sleep(0.1 * (2 ** attempt))
                        continue
                    break
                if response.status_code in {429, 500, 502, 503, 504} and attempt + 1 < attempts:
                    await asyncio.sleep(0.1 * (2 ** attempt))
                    continue
                break
        if response is None:
            assert last_network_error is not None
            raise ProviderError(
                "provider_timeout" if isinstance(last_network_error, httpx.TimeoutException)
                else "provider_network",
                "Webshare did not return a conclusive response", ambiguous=mutation,
            ) from last_network_error
        if response.is_redirect:
            raise ProviderError("provider_redirect", "Webshare returned an unexpected redirect")
        if response.status_code in (401, 403):
            raise ProviderError("provider_auth", "Webshare rejected the API key")
        if response.status_code == 429:
            raise ProviderError("provider_rate_limited", "Webshare rate-limited the request")
        if response.status_code >= 500:
            raise ProviderError(
                "provider_unavailable", "Webshare is temporarily unavailable", ambiguous=mutation
            )
        if response.status_code >= 400:
            code = "provider_not_found" if response.status_code == 404 else "provider_rejected"
            try:
                payload = response.json()
                # Webshare names the failure in `detail`/`error_code`; the field
                # errors echo the request, which for a sub-user write includes
                # nothing secret today but would if the shape ever changed. Only
                # the machine-readable code is carried.
                if isinstance(payload, dict) and isinstance(payload.get("error_code"), str):
                    code = f"provider_{payload['error_code']}"
            except ValueError:
                pass
            raise ProviderError(code, "Webshare rejected the request")
        if response.status_code == 204 or not response.content:
            return {}
        if len(response.content) > 2_000_000:
            raise ProviderError("provider_response_too_large", "Webshare response exceeded 2 MB")
        try:
            return response.json()
        except ValueError as error:
            raise ProviderError("provider_invalid_json", "Webshare returned invalid JSON") from error

    @staticmethod
    def _gb_to_bytes(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(Decimal(str(value)) * DECIMAL_GB)
        except (InvalidOperation, ValueError) as error:
            raise ProviderError(
                "provider_invalid_limit", "Webshare returned an unreadable bandwidth limit"
            ) from error

    @staticmethod
    def _date(value: Any) -> datetime:
        if not isinstance(value, str):
            raise ProviderError("provider_invalid_date", "Webshare omitted a subscription date")
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    async def health(self) -> bool:
        await self._request("GET", "/subscription/")
        return True

    async def subscription(self) -> Subscription:
        """Two calls: the term comes from the subscription, the size from the plan.

        `bandwidth_limit: 0` means unlimited, and is reported as `None` rather
        than zero. Zero would read as "no traffic purchased" and a budget cycle
        built from it would refuse every lease -- the opposite of what unlimited
        means.
        """
        payload = await self._request("GET", "/subscription/")
        if not isinstance(payload, dict):
            raise ProviderError("provider_subscription_missing", "Webshare returned no subscription")
        plan_id = payload.get("plan")
        if plan_id is None:
            raise ProviderError("provider_subscription_missing", "Webshare subscription has no plan")
        plan = await self._request("GET", f"/subscription/plan/{plan_id}/")
        if not isinstance(plan, dict):
            raise ProviderError("provider_subscription_missing", "Webshare returned no plan")
        raw_limit = plan.get("bandwidth_limit")
        unlimited = raw_limit in (0, 0.0, "0")
        return Subscription(
            provider_resource_id=str(plan_id),
            service_type=str(plan.get("proxy_type") or "webshare"),
            traffic_limit_bytes=None if unlimited else self._gb_to_bytes(raw_limit),
            raw_traffic_limit=raw_limit,
            valid_from=self._date(payload.get("start_date")),
            valid_until=self._date(payload.get("end_date")),
            users_limit=plan.get("subusers_total"),
        )

    async def usage(self, start: datetime, end: datetime, *, group_by: str = "day") -> UsageReport:
        """Bytes in, bytes out -- no unit conversion on the reconciliation path.

        The endpoint aggregates over the whole window rather than bucketing, so
        the report carries a single bucket. Windows older than 90 days are
        refused by the provider; that is raised here with a code that says so
        rather than surfacing as an opaque rejection.
        """
        if group_by not in {"day", "total"}:
            raise ProviderError("invalid_group", "Webshare aggregates traffic over the window only")
        now = datetime.now(UTC)
        start = start.astimezone(UTC)
        if now - start > STATS_MAX_AGE:
            raise ProviderError(
                "provider_window_too_old", "Webshare reports at most 90 days of traffic"
            )
        payload = await self._request(
            "GET", "/stats/aggregate/",
            params={
                "timestamp__gte": start.isoformat().replace("+00:00", "Z"),
                "timestamp__lte": min(end.astimezone(UTC), now).isoformat().replace("+00:00", "Z"),
            },
        )
        if not isinstance(payload, dict):
            raise ProviderError("provider_usage_unreadable", "Webshare returned no usage")
        total = payload.get("bandwidth_total")
        if not isinstance(total, int):
            raise ProviderError(
                "provider_usage_unreadable", "Webshare returned a non-integer byte total"
            )
        if total < 0:
            raise ProviderError("provider_usage_unreadable", "Webshare returned a negative total")
        requests = payload.get("requests_total")
        requests = requests if isinstance(requests, int) and requests >= 0 else 0
        return UsageReport(
            total_bytes=total,
            requests=requests,
            buckets=[UsageBucket(
                key=start.date().isoformat(), total_bytes=total, requests=requests,
            )],
        )

    async def list_subusers(self) -> list[SubUser]:
        subusers: list[SubUser] = []
        offset = 0
        while True:
            payload = await self._request(
                "GET", "/subuser/", params={"limit": 100, "offset": offset}
            )
            rows = payload.get("results") if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                raise ProviderError("provider_subusers_missing", "Webshare returned no sub-user list")
            subusers.extend(self._subuser(row) for row in rows)
            if not payload.get("next"):
                break
            offset += len(rows)
            if not rows or offset > 10_000:
                raise ProviderError("provider_pagination_runaway", "Webshare paginated without end")
        return subusers

    @classmethod
    def _subuser(cls, row: Any) -> SubUser:
        if not isinstance(row, dict):
            raise ProviderError("provider_subuser_invalid", "Webshare returned a malformed sub-user")
        identifier = row.get("id")
        if identifier is None:
            raise ProviderError("provider_subuser_invalid", "Webshare sub-user lacked an id")
        return SubUser(
            id=str(identifier),
            # The label is the only human name a Webshare sub-user has; there is
            # no username, which is also why one cannot be chosen on creation.
            username=str(row.get("label") or identifier),
            status="unknown",
            traffic_bytes=None,
            traffic_limit_bytes=cls._gb_to_bytes(row.get("proxy_limit")),
            auto_disable=False,
        )

    async def create_subuser(
        self, *, username: str, password: str, traffic_limit_bytes: int,
        traffic_count_from: datetime,
    ) -> SubUser:
        raise ProviderError(
            "provider_provisioning_unsupported",
            "Webshare sub-users have no settable username or password; their proxy "
            "credentials are issued through the proxy list, so a profile cannot be "
            "provisioned into Webshare",
        )

    async def update_subuser(
        self, resource_id: str, *, password: str | None = None,
        traffic_limit_bytes: int | None = None, status: str | None = None,
    ) -> SubUser:
        if password is not None:
            raise ProviderError(
                "provider_password_unsupported",
                "Webshare sub-users have no password field to rotate",
            )
        if status is not None:
            raise ProviderError(
                "provider_status_unsupported", "Webshare sub-users have no status field"
            )
        if traffic_limit_bytes is None:
            raise ProviderError("provider_empty_update", "no sub-user fields were supplied")
        payload = await self._request(
            "PATCH", f"/subuser/{resource_id}/",
            json={"proxy_limit": float(Decimal(traffic_limit_bytes) / DECIMAL_GB)},
            mutation=True,
        )
        return self._subuser(payload)

    async def delete_subuser(self, resource_id: str) -> None:
        await self._request("DELETE", f"/subuser/{resource_id}/", mutation=True)
