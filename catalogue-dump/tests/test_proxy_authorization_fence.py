"""Focused unit checks for the per-attempt durable proxy identity fence."""

from contextlib import AbstractAsyncContextManager
from typing import Any
from uuid import uuid4

import pytest

from mb_ceramics_catalogue.proxy import ProxyDenied, authorize_reservation_attempt


class _Transaction(AbstractAsyncContextManager[None]):
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class _Cursor:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    async def fetchone(self) -> dict[str, Any] | None:
        return self._row


class _Connection:
    def __init__(self, profile: dict[str, Any]) -> None:
        self.profile = profile
        self.statements: list[str] = []

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def execute(self, query: str, params: object = None) -> _Cursor:
        del params
        statement = " ".join(query.split())
        self.statements.append(statement)
        if "proxy_budget_cycles c" in statement:
            return _Cursor(
                {
                    "lifecycle": "active",
                    "kill_switch": False,
                    "reconciliation_ok": True,
                    "reconciled_at": object(),
                    "current": True,
                }
            )
        if "proxy_profiles p" in statement:
            return _Cursor(self.profile)
        raise AssertionError(f"authorization continued past the profile fence: {statement}")


@pytest.mark.parametrize(
    "profile",
    (
        {"enabled": False, "lifecycle": "disabled", "secret_current": True},
        {"enabled": True, "lifecycle": "enabled", "secret_current": False},
    ),
    ids=("disabled", "secret-rotated"),
)
async def test_attempt_authorization_stops_at_an_unsafe_profile(
    profile: dict[str, Any],
) -> None:
    connection = _Connection(profile)

    with pytest.raises(ProxyDenied, match="profile does not authorize"):
        await authorize_reservation_attempt(
            connection,  # type: ignore[arg-type]
            reservation_id=uuid4(),
            estimated_bytes=1,
            maximum_requests=1,
        )

    assert len(connection.statements) == 2
    assert "for update of c" in connection.statements[0].lower()
    assert "for share of p" in connection.statements[1].lower()
