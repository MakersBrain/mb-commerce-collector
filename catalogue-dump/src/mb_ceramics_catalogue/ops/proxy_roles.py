"""Provision fixed least-privilege PostgreSQL roles for proxy operations."""

from __future__ import annotations

import argparse
import os

import psycopg
from psycopg import sql

PROXY_TABLES = (
    "proxy_budget_cycles", "proxy_profiles", "proxy_profile_allocations",
    "proxy_profile_retirements", "proxy_routes", "source_proxy_policies",
    "proxy_probes", "proxy_reservations", "proxy_attempt_authorizations",
    "proxy_provider_snapshots",
    "proxy_admin_audit", "proxy_mutation_requests", "proxy_actor_nonces",
    "proxy_reconcile_requests", "proxy_pilot_evidence",
)


def _role(connection: psycopg.Connection, name: str, *, login: bool, password: str = "") -> None:
    exists = connection.execute("select 1 from pg_roles where rolname = %s", (name,)).fetchone()
    if not exists:
        connection.execute(
            sql.SQL("create role {} {}").format(
                sql.Identifier(name), sql.SQL("login" if login else "nologin")
            )
        )
    connection.execute(
        sql.SQL("alter role {} {}").format(
            sql.Identifier(name), sql.SQL("login" if login else "nologin")
        )
    )
    if login:
        if not password:
            raise ValueError(f"password is required for {name}")
        connection.execute(
            sql.SQL("alter role {} password {}").format(
                sql.Identifier(name), sql.Literal(password)
            )
        )


def provision(
    dsn: str,
    control_password: str,
    worker_password: str,
    archive_password: str,
    service_password: str,
    dispatcher_password: str,
) -> None:
    with psycopg.connect(dsn, autocommit=True) as connection:
        _role(connection, "catalogue_proxy_owner", login=False)
        _role(connection, "catalogue_proxy_maintenance", login=False)
        _role(connection, "catalogue_control", login=True, password=control_password)
        _role(connection, "catalogue_worker", login=True, password=worker_password)
        _role(connection, "catalogue_proxy_archive", login=True, password=archive_password)
        _role(connection, "catalogue_service", login=True, password=service_password)
        _role(connection, "catalogue_dispatcher", login=True, password=dispatcher_password)

        current_row = connection.execute("select current_user").fetchone()
        assert current_row is not None
        current = current_row[0]
        connection.execute(
            sql.SQL("grant catalogue_proxy_owner to {} with admin option").format(
                sql.Identifier(current)
            )
        )
        connection.execute("grant catalogue_proxy_maintenance to catalogue_proxy_archive")
        for table in PROXY_TABLES:
            connection.execute(
                sql.SQL("alter table catalogue.{} owner to catalogue_proxy_owner").format(
                    sql.Identifier(table)
                )
            )

        # PostgreSQL executes referential-integrity checks as the owner of the
        # referencing table.  The proxy tables are reassigned to the NOLOGIN
        # owner role above, so that role still needs schema USAGE even though
        # the login roles have their own table grants.  Without it, inserting a
        # provider snapshot fails while checking its budget-cycle foreign key.
        for role in (
            "catalogue_proxy_owner", "catalogue_control", "catalogue_worker",
            "catalogue_proxy_archive", "catalogue_service", "catalogue_dispatcher",
        ):
            connection.execute(
                sql.SQL("grant connect on database {} to {}").format(
                    sql.Identifier(connection.info.dbname), sql.Identifier(role)
                )
            )
            connection.execute(
                sql.SQL("grant usage on schema catalogue to {}").format(sql.Identifier(role))
            )
        connection.execute("grant select, insert, update, delete on all tables in schema catalogue to catalogue_control")
        connection.execute("grant usage, select on all sequences in schema catalogue to catalogue_control")
        connection.execute("grant select, insert, update, delete on all tables in schema catalogue to catalogue_worker")
        connection.execute("grant usage, select on all sequences in schema catalogue to catalogue_worker")
        connection.execute("grant select on all tables in schema catalogue to catalogue_service")
        connection.execute("grant select, insert, update, delete on all tables in schema catalogue to catalogue_dispatcher")
        connection.execute("grant usage, select on all sequences in schema catalogue to catalogue_dispatcher")
        connection.execute("revoke update, delete, truncate on catalogue.proxy_admin_audit from catalogue_control")
        for table in PROXY_TABLES:
            connection.execute(
                sql.SQL("revoke all on catalogue.{} from catalogue_worker").format(
                    sql.Identifier(table)
                )
            )
        connection.execute(
            "grant select, update on catalogue.proxy_budget_cycles to catalogue_worker"
        )
        connection.execute(
            "grant select on catalogue.proxy_profiles, catalogue.proxy_profile_allocations, "
            "catalogue.proxy_routes to catalogue_worker"
        )
        connection.execute(
            "grant select, update on catalogue.source_proxy_policies to catalogue_worker"
        )
        connection.execute(
            "grant select, insert, update on catalogue.proxy_reservations to catalogue_worker"
        )
        connection.execute(
            "grant select, insert, update on catalogue.proxy_attempt_authorizations "
            "to catalogue_worker"
        )
        # INSERT ... ON CONFLICT reads the matching unique-index row even when
        # it takes DO NOTHING, so PostgreSQL also requires SELECT here.
        connection.execute(
            "grant select, insert on catalogue.proxy_reconcile_requests, "
            "catalogue.proxy_pilot_evidence to catalogue_worker"
        )
        connection.execute("grant select on catalogue.proxy_admin_audit to catalogue_proxy_archive")
        connection.execute(
            "revoke create on schema catalogue from catalogue_control, catalogue_worker, "
            "catalogue_service, catalogue_dispatcher"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.environ.get("CATALOGUE_DSN", ""))
    options = parser.parse_args()
    if not options.dsn:
        raise SystemExit("CATALOGUE_DSN is required")
    provision(
        options.dsn,
        os.environ.get("CATALOGUE_CONTROL_DB_PASSWORD", ""),
        os.environ.get("CATALOGUE_WORKER_DB_PASSWORD", ""),
        os.environ.get("CATALOGUE_PROXY_ARCHIVE_DB_PASSWORD", ""),
        os.environ.get("CATALOGUE_SERVICE_DB_PASSWORD", ""),
        os.environ.get("CATALOGUE_DISPATCHER_DB_PASSWORD", ""),
    )


if __name__ == "__main__":
    main()
