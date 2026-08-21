"""Apply the catalogue schema baseline, or adopt a database that already has it."""

from __future__ import annotations

import argparse
import asyncio

from mb_ceramics_catalogue.storage import db


async def run(dsn: str) -> int:
    resolved = db.dsn_from_environment(dsn)
    async with db.pool(resolved, minimum=1, maximum=1) as pool, pool.connection() as connection:
        # A session lock spans the BEGIN/COMMIT statements inside migration
        # files; a transaction lock would be released by the first file.
        await connection.execute("select pg_advisory_lock(hashtext('catalogue-schema'))")
        try:
            applied = await db.apply_schema(connection)
        finally:
            await connection.execute("select pg_advisory_unlock(hashtext('catalogue-schema'))")
    print("applied " + ", ".join(applied))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default="")
    options = parser.parse_args()
    return asyncio.run(run(options.dsn))


if __name__ == "__main__":
    raise SystemExit(main())
