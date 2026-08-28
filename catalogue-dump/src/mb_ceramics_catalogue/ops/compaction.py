"""Bounded conversion of legacy daily duplicates into semantic intervals."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg

Connection = psycopg.Connection[dict[str, Any]]


@dataclass(frozen=True)
class CompactReport:
    raw_deleted: int = 0
    offers_deleted: int = 0


def compact_raw(connection: Connection, limit: int, *, execute: bool) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            select source_product_id,
                   catalogue.digest(convert_to((record - 'fetched_at')::text, 'UTF8'), 'sha256') semantic,
                   array_agg(
                     id order by (record_sha256 = catalogue.digest(
                       convert_to((record - 'fetched_at')::text, 'UTF8'), 'sha256'
                     )) desc, first_seen_at, id
                   ) ids,
                   min(first_seen_at) first_seen, max(last_seen_at) last_seen
              from catalogue.raw_records
             group by source_product_id,
                      catalogue.digest(convert_to((record - 'fetched_at')::text, 'UTF8'), 'sha256')
            having count(*) > 1
             order by source_product_id
             limit %s
            """,
            (limit,),
        )
        groups = cursor.fetchall()
    deleted = sum(len(row["ids"]) - 1 for row in groups)
    if not execute:
        return deleted
    for row in groups:
        keeper, *duplicates = row["ids"]
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute(
                "update catalogue.offer_observations set raw_record_id = %s where raw_record_id = any(%s)",
                (keeper, duplicates),
            )
            cursor.execute(
                "update catalogue.out_of_order_observations set raw_record_id = %s where raw_record_id = any(%s)",
                (keeper, duplicates),
            )
            cursor.execute("delete from catalogue.raw_records where id = any(%s)", (duplicates,))
            cursor.execute(
                """update catalogue.raw_records
                      set record_sha256 = %s, first_seen_at = %s, last_seen_at = %s
                    where id = %s""",
                (row["semantic"], row["first_seen"], row["last_seen"], keeper),
            )
    return deleted


def compact_offers(connection: Connection, limit: int, *, execute: bool) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            with states as (
              select id, source_product_id, observed_at, last_seen_at,
                     jsonb_build_array(
                       price, currency, price_text, vat_status, quantity, unit,
                       unit_price, unit_price_per, availability,
                       stock_quantity, stock_quantity_kind
                     ) state
                from catalogue.offer_observations
            ), previous as (
              select *, lag(state) over (
                       partition by source_product_id order by observed_at, id
                     ) previous_state
                from states
            ), islands as (
              select *, sum(case when previous_state is distinct from state then 1 else 0 end)
                           over (partition by source_product_id order by observed_at, id) island
                from previous
            )
            select source_product_id, island,
                   array_agg(id order by observed_at, id) ids,
                   min(observed_at) observed_at, max(last_seen_at) last_seen_at
              from islands
             group by source_product_id, island
            having count(*) > 1
             order by source_product_id, island
             limit %s
            """,
            (limit,),
        )
        groups = cursor.fetchall()
    deleted = sum(len(row["ids"]) - 1 for row in groups)
    if not execute:
        return deleted
    for row in groups:
        keeper, *duplicates = row["ids"]
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute("delete from catalogue.offer_observations where id = any(%s)", (duplicates,))
            cursor.execute(
                """update catalogue.offer_observations
                      set observed_at = %s, last_seen_at = %s
                    where id = %s""",
                (row["observed_at"], row["last_seen_at"], keeper),
            )
    return deleted


def compact(connection: Connection, limit: int = 100, *, execute: bool = False) -> CompactReport:
    return CompactReport(
        raw_deleted=compact_raw(connection, limit, execute=execute),
        offers_deleted=compact_offers(connection, limit, execute=execute),
    )
