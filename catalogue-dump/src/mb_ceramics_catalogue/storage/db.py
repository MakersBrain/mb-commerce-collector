"""Connecting to the catalogue database.

Thin on purpose. The interesting decisions are all in the SQL, and a layer that
hid the transaction and fencing statements would make them harder to review
rather than easier.

Two things it does insist on:

* **`row_factory=dict_row` everywhere.** Positional tuples are how a column
  added in the middle of a `select *` silently shifts every reader.
* **`autocommit=True` on the pool.** Every write path here is either a single
  statement or an explicit `async with conn.transaction()`, and an implicit
  open transaction on a pooled connection is how a worker ends up holding row
  locks it forgot about while it waits for a shop to answer.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from mb_ceramics_catalogue.observability import logging as obs

LOGGER = obs.get_logger("catalogue.db")


def dsn_from_environment(explicit: str = "") -> str:
    """The connection string, from the argument or the environment."""
    found = (
        explicit
        or os.environ.get("CATALOGUE_DSN", "")
        or os.environ.get("DATABASE_URL", "")
    )
    if not found:
        raise ValueError(
            "no database connection string; pass --dsn or set CATALOGUE_DSN"
        )
    return found


@asynccontextmanager
async def connect(dsn: str) -> AsyncIterator[psycopg.AsyncConnection[dict[str, Any]]]:
    """One connection, for a command that makes a handful of statements."""
    async with await psycopg.AsyncConnection.connect(
        dsn, row_factory=dict_row, autocommit=True
    ) as connection:
        yield connection


#: A pool whose connections are known to yield dict rows. Spelling this out is
#: what stops every `async with pool.connection()` from looking like a tuple-row
#: connection to a type checker, and then needing a cast at each of forty sites.
DictPool = AsyncConnectionPool[psycopg.AsyncConnection[dict[str, Any]]]


@asynccontextmanager
async def pool(dsn: str, *, minimum: int = 1, maximum: int = 4) -> AsyncIterator[DictPool]:
    """A pool, for a long-lived process.

    The worker needs at least two connections at once — one for the job it is
    running and one for the heartbeat that must keep heartbeating while that job
    holds its connection busy — so a pool is not an optimisation here, it is
    what stops the liveness signal from being blocked by the work it reports on.
    """
    connection_pool: DictPool = AsyncConnectionPool(
        dsn,
        connection_class=psycopg.AsyncConnection[dict[str, Any]],
        min_size=minimum,
        max_size=maximum,
        kwargs={"row_factory": dict_row, "autocommit": True},
        open=False,
    )
    await connection_pool.open(wait=True, timeout=30)
    try:
        yield connection_pool
    finally:
        await connection_pool.close()


async def fetch_all(
    connection: psycopg.AsyncConnection[dict[str, Any]], sql: str, params: Any = None
) -> list[dict[str, Any]]:
    async with connection.cursor() as cursor:
        await cursor.execute(sql, params)
        return await cursor.fetchall()


async def fetch_one(
    connection: psycopg.AsyncConnection[dict[str, Any]], sql: str, params: Any = None
) -> dict[str, Any] | None:
    async with connection.cursor() as cursor:
        await cursor.execute(sql, params)
        return await cursor.fetchone()


async def execute(
    connection: psycopg.AsyncConnection[dict[str, Any]], sql: str, params: Any = None
) -> int:
    async with connection.cursor() as cursor:
        await cursor.execute(sql, params)
        return cursor.rowcount


def schema_directory() -> Any:
    from pathlib import Path

    return Path(__file__).resolve().parent / "schema"


#: The whole schema, in one file.
#:
#: It was eleven files applied in a hand-maintained order, and the order was
#: load-bearing: `catalogue-canonical-promotion.sql` sorts first alphabetically
#: but alters a table the reference schema creates. That ordering had to be
#: repeated in `docker-compose.yml`'s initdb mounts, and the two lists drifted
#: — the compose side never gained `catalogue-ops-schema-v6.sql`, so a freshly
#: initialised stack was a version behind until a worker ran this function.
#:
#: One baseline removes the class of bug. It is a `pg_dump --schema-only` of a
#: database with all eleven applied; regenerate it the same way rather than
#: editing it by hand.
BASELINE = "catalogue-schema.sql"

SCHEMA_FILES = (BASELINE,)

#: The table added by the last file that went into the baseline. Its presence
#: is what separates a database that is already at head from one that stopped
#: somewhere in the middle of the old sequence.
HEAD_SENTINEL = "catalogue.source_health_probes"


async def apply_schema(connection: psycopg.AsyncConnection[dict[str, Any]]) -> list[str]:
    """Apply each unrecorded schema file, adopting a database already at head."""
    directory = schema_directory()
    await connection.execute("create schema if not exists catalogue")
    await connection.execute(
        """create table if not exists catalogue.schema_migrations (
             filename text primary key,
             applied_at timestamptz not null default now()
           )"""
    )
    recorded = await fetch_all(connection, "select filename from catalogue.schema_migrations")
    done = {row["filename"] for row in recorded}
    if BASELINE not in done:
        existing = await fetch_one(
            connection, "select to_regclass('catalogue.sources') is not null as present"
        )
        if existing and existing["present"]:
            # Populated by initdb, or by the eleven files this baseline
            # replaces. Either way the DDL is already there and running it
            # again would fail on the first `create table`.
            head = await fetch_one(
                connection, "select to_regclass(%s) is not null as present", (HEAD_SENTINEL,)
            )
            if not (head and head["present"]):
                raise RuntimeError(
                    f"{HEAD_SENTINEL} is missing, so this database stopped part-way through the "
                    "pre-squash migrations. Apply them with the previous release before "
                    f"upgrading to {BASELINE}, which can only create a schema, not migrate one."
                )
            await connection.execute(
                "insert into catalogue.schema_migrations(filename) values (%s)", (BASELINE,)
            )
            done.add(BASELINE)
            LOGGER.info("schema.adopted", file=BASELINE)

    applied: list[str] = []
    for name in SCHEMA_FILES:
        if name in done:
            continue
        path = directory / name
        await connection.execute(path.read_text(encoding="utf-8"))
        await connection.execute(
            "insert into catalogue.schema_migrations(filename) values (%s)", (name,)
        )
        applied.append(name)
        LOGGER.info("schema.applied", file=name)
    return applied
