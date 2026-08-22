#!/usr/bin/env python3
"""Rotate and adopt the sole existing Decodo user as a bounded pilot profile."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from mb_ceramics_catalogue.providers.base import ProviderError
from mb_ceramics_catalogue.providers.decodo import DecodoProvider
from mb_ceramics_catalogue.proxy_secrets import (
    ProfileSecretStore,
    generate_password,
    mask_username,
    username_fingerprint,
)

CONFIRMATION = "ROTATE AND ADOPT EXISTING DECODO USER"
ALLOCATION_BYTES = 300_000_000
ROUTE_BYTES = 25_000_000
LOGICAL_NAME = "catalogue-pilot"
SOURCE_ID = "the-ceramic-shop"


def api_key(path: Path) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("DECODO_API_KEY="):
            value = line.split("=", 1)[1].strip()
            if value:
                return value
    raise RuntimeError("DECODO_API_KEY is missing")


async def audit(
    connection: psycopg.AsyncConnection[dict[str, object]],
    *,
    operation_id: UUID,
    actor: str,
    state: str,
    success: bool,
    resource_id: str | None,
    after: dict[str, object] | None = None,
    error_code: str | None = None,
) -> None:
    await connection.execute(
        """
        insert into catalogue.proxy_admin_audit
               (operation_id, actor, actor_role, request_id, idempotency_key,
                action, resource_type, resource_id, state, success, error_code,
                after_data, response_status)
        values (%(operation)s, %(actor)s, 'admin', %(request)s, %(idempotency)s,
                'profile.adopt', 'profile', %(resource)s, %(state)s, %(success)s,
                %(error)s, %(after)s, %(status)s)
        """,
        {
            "operation": operation_id,
            "actor": actor,
            "request": uuid4(),
            "idempotency": str(operation_id),
            "resource": resource_id,
            "state": state,
            "success": success,
            "error": error_code,
            "after": Jsonb(after) if after is not None else None,
            "status": 201 if success else 502,
        },
    )


async def run(options: argparse.Namespace) -> None:
    if options.confirm != CONFIRMATION:
        raise SystemExit(f"confirmation must equal {CONFIRMATION!r}")
    dsn = os.environ.get("CATALOGUE_DSN", "")
    if not dsn:
        raise SystemExit("CATALOGUE_DSN is required")

    provider = DecodoProvider(api_key(options.api_key_file), limit_unit="decimal_gb")
    subscription, users = await asyncio.gather(
        provider.subscription(), provider.list_subusers()
    )
    if subscription.traffic_limit_bytes != 3_000_000_000:
        raise SystemExit("refusing adoption: Decodo subscription is not the confirmed 3 GB plan")
    if len(users) != 1 or users[0].status != "active":
        raise SystemExit("refusing adoption: expected exactly one active Decodo user")
    user = users[0]
    operation_id = uuid4()

    connection = await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row)
    profile_id: UUID | None = None
    try:
        async with connection.transaction():
            await connection.execute("select pg_advisory_xact_lock(hashtext('proxy:decodo'))")
            existing = await connection.execute(
                "select count(*) as count from catalogue.proxy_profiles"
            )
            if int((await existing.fetchone())["count"]) != 0:  # type: ignore[index]
                raise SystemExit("refusing adoption: a local proxy profile already exists")
            cycle_cursor = await connection.execute(
                """select * from catalogue.proxy_budget_cycles
                    where provider = 'decodo' and lifecycle = 'active' for update"""
            )
            cycle = await cycle_cursor.fetchone()
            if (
                cycle is None
                or not cycle["kill_switch"]
                or cycle["pilot_active"]
                or not cycle["reconciliation_ok"]
                or int(cycle["pilot_bytes"]) < ALLOCATION_BYTES
            ):
                raise SystemExit("refusing adoption: billing cycle is not in safe pre-pilot state")
            created = await connection.execute(
                """
                insert into catalogue.proxy_profiles
                       (logical_name, display_name, provider_resource_id, lifecycle,
                        enabled, created_by, updated_by)
                values (%(name)s, 'Decodo catalogue pilot', %(resource)s, 'pending',
                        false, %(actor)s, %(actor)s)
                returning id
                """,
                {
                    "name": LOGICAL_NAME,
                    "resource": user.id,
                    "actor": options.actor,
                },
            )
            profile_id = (await created.fetchone())["id"]  # type: ignore[index]
            await connection.execute(
                """
                insert into catalogue.proxy_profile_allocations
                       (provider, cycle_start, profile_id, allocated_bytes, updated_by)
                values ('decodo', %(start)s, %(profile)s, %(bytes)s, %(actor)s)
                """,
                {
                    "start": cycle["cycle_start"],
                    "profile": profile_id,
                    "bytes": ALLOCATION_BYTES,
                    "actor": options.actor,
                },
            )

        password = generate_password()
        try:
            updated_user = await provider.update_subuser(
                user.id,
                password=password,
                traffic_limit_bytes=ALLOCATION_BYTES,
                status="active",
            )
        except ProviderError as error:
            async with connection.transaction():
                await connection.execute(
                    "delete from catalogue.proxy_profile_allocations where profile_id = %(id)s",
                    {"id": profile_id},
                )
                await connection.execute(
                    "delete from catalogue.proxy_profiles where id = %(id)s",
                    {"id": profile_id},
                )
                await audit(
                    connection,
                    operation_id=operation_id,
                    actor=options.actor,
                    state="ambiguous" if error.ambiguous else "failed",
                    success=False,
                    resource_id=str(profile_id),
                    error_code=error.code,
                )
            raise SystemExit(f"provider adoption failed safely: {error.code}") from None

        try:
            generation = ProfileSecretStore(options.profile_file).install(
                LOGICAL_NAME, username=updated_user.username, password=password
            )
        except Exception as error:
            async with connection.transaction():
                await connection.execute(
                    """update catalogue.proxy_profiles
                          set lifecycle = 'provider_changed_local_failed', enabled = false,
                              updated_at = now(), updated_by = %(actor)s where id = %(id)s""",
                    {"id": profile_id, "actor": options.actor},
                )
                await connection.execute(
                    """update catalogue.proxy_budget_cycles set kill_switch = true
                        where provider = 'decodo' and lifecycle = 'active'"""
                )
                await audit(
                    connection,
                    operation_id=operation_id,
                    actor=options.actor,
                    state="provider_changed_local_failed",
                    success=False,
                    resource_id=str(profile_id),
                    error_code="secret_install_failed",
                )
            raise SystemExit("provider changed but local secret installation failed") from error

        async with connection.transaction():
            finalized = await connection.execute(
                """
                update catalogue.proxy_profiles
                   set username_mask = %(mask)s, username_fingerprint = %(fingerprint)s,
                       provider_traffic_limit_bytes = %(limit)s, auto_disable = %(auto)s,
                       enabled = true, lifecycle = 'enabled',
                       secret_generation = %(generation)s, secret_installed_at = now(),
                       provider_observed_at = now(), updated_at = now(), updated_by = %(actor)s
                 where id = %(id)s returning id
                """,
                {
                    "mask": mask_username(updated_user.username),
                    "fingerprint": username_fingerprint(updated_user.username),
                    "limit": ALLOCATION_BYTES,
                    "auto": updated_user.auto_disable,
                    "generation": generation,
                    "actor": options.actor,
                    "id": profile_id,
                },
            )
            if await finalized.fetchone() is None:
                raise RuntimeError("pending local profile disappeared")
            route_cursor = await connection.execute(
                """
                insert into catalogue.proxy_routes
                       (provider, label, profile_id, protocol, country, session_mode,
                        session_minutes, max_bytes, pilot, enabled, created_by, updated_by)
                values ('decodo', 'The Ceramic Shop pilot', %(profile)s, 'http', 'US', 'random',
                        30, %(bytes)s, true, true, %(actor)s, %(actor)s)
                returning id
                """,
                {"profile": profile_id, "bytes": ROUTE_BYTES, "actor": options.actor},
            )
            route_id = (await route_cursor.fetchone())["id"]  # type: ignore[index]
            await connection.execute(
                """
                insert into catalogue.source_proxy_policies
                       (source_id, policy, route_id, max_bytes, pilot, evidence_state,
                        enabled_at, updated_by)
                values (%(source)s, 'fallback', %(route)s, %(bytes)s, true, 'eligible',
                        now(), %(actor)s)
                """,
                {
                    "source": SOURCE_ID,
                    "route": route_id,
                    "bytes": ROUTE_BYTES,
                    "actor": options.actor,
                },
            )
            await audit(
                connection,
                operation_id=operation_id,
                actor=options.actor,
                state="succeeded",
                success=True,
                resource_id=str(profile_id),
                after={
                    "logical_name": LOGICAL_NAME,
                    "allocated_bytes": ALLOCATION_BYTES,
                    "route_id": str(route_id),
                    "source_id": SOURCE_ID,
                    "max_bytes": ROUTE_BYTES,
                },
            )
        print(
            json.dumps(
                {
                    "profile_id": str(profile_id),
                    "route_id": str(route_id),
                    "source_id": SOURCE_ID,
                    "provider_limit_bytes": ALLOCATION_BYTES,
                    "route_limit_bytes": ROUTE_BYTES,
                },
                sort_keys=True,
            )
        )
    finally:
        await connection.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--actor", required=True)
    parser.add_argument("--confirm", required=True)
    parser.add_argument(
        "--api-key-file", type=Path, default=Path("/run/catalogue-secrets/decodo.env")
    )
    parser.add_argument(
        "--profile-file", type=Path, default=Path("/run/proxy-secrets/profiles.json")
    )
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
