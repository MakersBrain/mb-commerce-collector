"""Transactional manifests for checkpointed, multi-dataset collection output."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from mb_ceramics_catalogue.connectors.base import ConnectorCheckpoint, EntityPage
from mb_ceramics_catalogue.pipeline.outputs import ArtifactStore, BatchIdentity, StoredBatch, StoredObject

if TYPE_CHECKING:
    from mb_ceramics_catalogue.pipeline.runner import DatasetPageOutcome

Connection = psycopg.AsyncConnection[dict[str, Any]]


class LineageRuntimeFormat(StrEnum):
    CATALOGUE_V1 = "catalogue-v1"
    COMMERCE_SCRAPER_V1 = "commerce-scraper-v1"


class LineageProgressState(StrEnum):
    EMPTY = "empty"
    RESUMABLE = "resumable"
    TERMINAL_INTACT = "terminal_intact"
    TERMINAL_LIMITED = "terminal_limited"
    TERMINAL_INCOMPLETE = "terminal_incomplete"


@dataclass(frozen=True)
class LineageRuntimeConfiguration:
    runtime_format: LineageRuntimeFormat
    collection_request: dict[str, Any] | None
    connector_options: dict[str, Any] | None


@dataclass(frozen=True)
class LineageProgress:
    state: LineageProgressState
    checkpoint: ConnectorCheckpoint | None = None


@dataclass(frozen=True)
class DatasetKey:
    dataset: str
    contract_version: str
    projector_version: str


@dataclass(frozen=True)
class PageBatch:
    key: DatasetKey
    object_key: str
    sha256: str
    size: int
    records: int


@dataclass(frozen=True)
class DatasetOutcome:
    key: DatasetKey
    state: str
    records: int = 0
    error: str | None = None


class PostgresPageCommitter:
    """Bind a pipeline PageCommitter to one job/checkpoint transaction stream."""

    def __init__(
        self,
        connection: Connection,
        job_id: UUID,
        lineage: UUID,
        connector_version: str,
        datasets: dict[str, DatasetKey],
        *,
        dynamic_partitions: bool = False,
    ) -> None:
        self.connection = connection
        self.job_id = job_id
        self.lineage = lineage
        self.connector_version = connector_version
        self.datasets = datasets
        self.dynamic_partitions = dynamic_partitions

    async def commit_page(
        self,
        page: EntityPage[Any],
        batches: Sequence[StoredBatch],
        outcomes: Sequence[DatasetPageOutcome],
    ) -> None:
        converted_batches: list[PageBatch] = []
        for batch in batches:
            identity = batch.identity
            expected = (
                str(self.job_id), str(self.lineage), page.partition_key,
                page.page_id, page.sequence,
            )
            actual = (
                identity.job_id, identity.checkpoint_lineage, identity.partition_key,
                identity.page_id, identity.page_sequence,
            )
            if actual != expected:
                raise ValueError("stored batch identity does not match page commit")
            converted_batches.append(
                PageBatch(
                    DatasetKey(
                        identity.dataset, identity.contract_version, identity.projector_version
                    ),
                    batch.location,
                    batch.sha256,
                    batch.size,
                    batch.records,
                )
            )
        converted_outcomes: list[DatasetOutcome] = []
        for outcome in outcomes:
            try:
                key = self.datasets[outcome.dataset]
            except KeyError:
                raise ValueError(f"outcome for undeclared dataset {outcome.dataset!r}") from None
            converted_outcomes.append(
                DatasetOutcome(key, str(outcome.state), outcome.records, outcome.error)
            )
        await commit_page(
            self.connection,
            self.job_id,
            self.lineage,
            partition_key=page.partition_key,
            page_id=page.page_id,
            page_sequence=page.sequence,
            resume_after=page.resume_after,
            terminal=page.partition_terminal or page.terminal,
            enumeration_intact=page.enumeration_intact,
            connector_version=self.connector_version,
            batches=converted_batches,
            outcomes=converted_outcomes,
            declare_partition=self.dynamic_partitions,
        )


async def declare_dataset(connection: Connection, job_id: UUID, key: DatasetKey) -> None:
    """Declare one requested output idempotently."""
    await connection.execute(
        """insert into catalogue.job_datasets
                     (job_id, dataset, contract_version, projector_version)
              values (%s, %s, %s, %s)
              on conflict do nothing""",
        (job_id, key.dataset, key.contract_version, key.projector_version),
    )


async def prepare_dataset_for_collection(
    connection: Connection, job_id: UUID, key: DatasetKey, *, resuming: bool
) -> None:
    """Restore an interrupted output or reset it for a deliberately new lineage."""
    await declare_dataset(connection, job_id, key)
    if resuming:
        await connection.execute(
            """update catalogue.job_datasets
                  set state = case when records > 0 then 'staged' else 'pending' end,
                      complete = false, error = null, updated_at = now()
                where job_id = %s and dataset = %s and contract_version = %s
                  and projector_version = %s and state in ('cancelled', 'degraded')""",
            (job_id, key.dataset, key.contract_version, key.projector_version),
        )
    else:
        await connection.execute(
            """update catalogue.job_datasets
                  set state = 'pending', complete = false, records = 0, rejected = 0,
                      error = null, promoted_at = null, updated_at = now()
                where job_id = %s and dataset = %s and contract_version = %s
                  and projector_version = %s""",
            (job_id, key.dataset, key.contract_version, key.projector_version),
        )


async def pipeline_dataset_states(
    connection: Connection, job_id: UUID, keys: Sequence[DatasetKey]
) -> dict[str, str]:
    """Restore sticky projector failures when a process resumes a lineage."""
    result: dict[str, str] = {}
    for key in keys:
        row = await _one(
            connection,
            """select state from catalogue.job_datasets
                where job_id = %s and dataset = %s and contract_version = %s
                  and projector_version = %s""",
            (job_id, key.dataset, key.contract_version, key.projector_version),
        )
        result[key.dataset] = "failed" if row and row["state"] == "failed" else "succeeded"
    return result


async def create_lineage(
    connection: Connection,
    job_id: UUID,
    *,
    source_id: str,
    source_url: str,
    connector: str,
    connector_version: str,
    connector_configuration: dict[str, Any] | None = None,
    connector_config_fingerprint: str,
    dataset_fingerprint: str,
    dataset_selection: list[dict[str, Any]],
    runtime_format: LineageRuntimeFormat = LineageRuntimeFormat.CATALOGUE_V1,
    collection_request: dict[str, Any] | None = None,
    connector_options: dict[str, Any] | None = None,
    budget_state: dict[str, Any] | None = None,
    lineage: UUID | None = None,
    expires_at: Any = None,
) -> UUID:
    """Create the immutable compatibility identity for a checkpoint chain."""
    if runtime_format is LineageRuntimeFormat.COMMERCE_SCRAPER_V1 and (
        collection_request is None or connector_options is None
    ):
        raise ValueError(
            "commerce-scraper-v1 lineages require a durable request and connector options"
        )
    lineage = lineage or uuid4()
    inserted = await _one(
        connection,
        """insert into catalogue.job_checkpoint_lineages
                     (job_id, checkpoint_lineage, source_id, source_url, connector,
                      connector_version, connector_configuration,
                      connector_config_fingerprint, dataset_fingerprint,
                      dataset_selection, runtime_format, collection_request,
                      connector_options, budget_state, expires_at)
              values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
              on conflict (job_id, checkpoint_lineage) do nothing
              returning checkpoint_lineage""",
        (
            job_id, lineage, source_id, source_url, connector, connector_version,
            Jsonb(connector_configuration or {}), connector_config_fingerprint, dataset_fingerprint,
            Jsonb(dataset_selection), runtime_format.value,
            Jsonb(collection_request) if collection_request is not None else None,
            Jsonb(connector_options) if connector_options is not None else None,
            Jsonb(budget_state or {}), expires_at,
        ),
    )
    if inserted is None:
        existing = await _one(
            connection,
            """select source_id, source_url, connector, connector_version,
                      connector_configuration,
                      connector_config_fingerprint, dataset_fingerprint,
                      dataset_selection, runtime_format, collection_request,
                      connector_options, budget_state, expires_at
                 from catalogue.job_checkpoint_lineages
                where job_id = %s and checkpoint_lineage = %s""",
            (job_id, lineage),
        )
        identity = (
            source_id, source_url, connector, connector_version, connector_configuration or {},
            connector_config_fingerprint, dataset_fingerprint,
            dataset_selection, runtime_format.value, collection_request,
            connector_options, budget_state or {}, expires_at,
        )
        if existing is None or tuple(existing.values()) != identity:
            raise ValueError("checkpoint lineage identity changed")
    return lineage


async def find_compatible_lineage(
    connection: Connection,
    job_id: UUID,
    *,
    source_url: str,
    connector: str,
    connector_version: str,
    connector_config_fingerprint: str,
    dataset_fingerprint: str,
    runtime_format: LineageRuntimeFormat = LineageRuntimeFormat.CATALOGUE_V1,
    lock: bool = False,
) -> UUID | None:
    lock_clause = " for update" if lock else ""
    row = await _one(
        connection,
        """select checkpoint_lineage
             from catalogue.job_checkpoint_lineages
            where job_id = %s and status = 'active' and source_url = %s
              and runtime_format = %s
              and connector = %s and connector_version = %s
              and connector_config_fingerprint = %s and dataset_fingerprint = %s
              and (expires_at is null or expires_at > now())
            order by created_at desc limit 1""" + lock_clause,
        (
            job_id, source_url, runtime_format.value, connector, connector_version,
            connector_config_fingerprint, dataset_fingerprint,
        ),
    )
    return row["checkpoint_lineage"] if row else None


async def find_recoverable_library_lineage(
    connection: Connection,
    job_id: UUID,
    *,
    source_url: str,
    connector: str,
    connector_version: str,
    connector_config_fingerprint: str,
    dataset_fingerprint: str,
    lock: bool = False,
) -> UUID | None:
    """Find active collection or sealed output awaiting publication.

    Publication is intentionally after lineage sealing. If the worker dies in
    that window, the completed manifest is still the authoritative collection
    and must be compacted/published rather than fetched again.
    """
    lock_clause = " for update" if lock else ""
    row = await _one(
        connection,
        """select checkpoint_lineage
             from catalogue.job_checkpoint_lineages
            where job_id = %s and status in ('active', 'completed', 'limited')
              and source_url = %s and runtime_format = %s
              and connector = %s and connector_version = %s
              and connector_config_fingerprint = %s and dataset_fingerprint = %s
              and (expires_at is null or expires_at > now())
            order by (status = 'active') desc, created_at desc limit 1"""
        + lock_clause,
        (
            job_id,
            source_url,
            LineageRuntimeFormat.COMMERCE_SCRAPER_V1.value,
            connector,
            connector_version,
            connector_config_fingerprint,
            dataset_fingerprint,
        ),
    )
    return row["checkpoint_lineage"] if row else None


async def active_lineages_for_runtime(
    connection: Connection,
    job_id: UUID,
    runtime_format: LineageRuntimeFormat,
    *,
    lock: bool = False,
) -> tuple[UUID, ...]:
    """Return all active lineages for one runtime, optionally serializing changes.

    Locking the parent job closes the empty-result race that a ``FOR UPDATE``
    query on lineage rows cannot close: concurrent resolvers must not both see
    no active lineage and create one. Callers then lock the returned rows in a
    stable order before deciding which lineage, if any, remains active.
    """
    if lock:
        job = await _one(
            connection,
            "select id from catalogue.jobs where id = %s for update",
            (job_id,),
        )
        if job is None:
            raise ValueError("unknown checkpoint job")
    lock_clause = " for update" if lock else ""
    rows = await _all(
        connection,
        """select checkpoint_lineage
             from catalogue.job_checkpoint_lineages
            where job_id = %s and runtime_format = %s and status = 'active'
            order by created_at, checkpoint_lineage""" + lock_clause,
        (job_id, runtime_format.value),
    )
    return tuple(row["checkpoint_lineage"] for row in rows)


async def lineage_runtime_configuration(
    connection: Connection, job_id: UUID, lineage: UUID
) -> LineageRuntimeConfiguration:
    """Load the durable runtime identity without consulting mutable source config."""
    row = await _one(
        connection,
        """select runtime_format, collection_request, connector_options
             from catalogue.job_checkpoint_lineages
            where job_id = %s and checkpoint_lineage = %s""",
        (job_id, lineage),
    )
    if row is None:
        raise ValueError("unknown checkpoint lineage")
    collection_request = _optional_json_object(
        row["collection_request"], "collection_request"
    )
    connector_options = _optional_json_object(
        row["connector_options"], "connector_options"
    )
    return LineageRuntimeConfiguration(
        runtime_format=LineageRuntimeFormat(row["runtime_format"]),
        collection_request=collection_request,
        connector_options=connector_options,
    )


async def reject_lineage(
    connection: Connection, job_id: UUID, lineage: UUID
) -> bool:
    """Reject an active incompatible lineage without deleting its audit trail."""
    row = await _one(
        connection,
        """update catalogue.job_checkpoint_lineages
              set status = 'rejected', updated_at = now()
            where job_id = %s and checkpoint_lineage = %s and status = 'active'
        returning checkpoint_lineage""",
        (job_id, lineage),
    )
    return row is not None


def _optional_json_object(value: Any, field: str) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"checkpoint lineage {field} must be a JSON object")
    return dict(value)


async def commit_page(
    connection: Connection,
    job_id: UUID,
    lineage: UUID,
    *,
    partition_key: str,
    page_id: str,
    page_sequence: int,
    resume_after: Any,
    terminal: bool,
    enumeration_intact: bool,
    connector_version: str,
    batches: list[PageBatch],
    outcomes: list[DatasetOutcome] | None = None,
    declare_partition: bool = False,
) -> bool:
    """Atomically commit a page manifest and every staged projector batch.

    Returns True for a new page and False for an exact idempotent replay. The
    same identity with different manifest or batch metadata is rejected.
    """
    committed_outcomes = outcomes if outcomes is not None else [
        DatasetOutcome(batch.key, "succeeded", batch.records) for batch in batches
    ]
    async with connection.transaction():
        lineage_row = await _one(
            connection,
            """select connector_version, status
                 from catalogue.job_checkpoint_lineages
                where job_id = %s and checkpoint_lineage = %s for update""",
            (job_id, lineage),
        )
        if lineage_row is None:
            raise ValueError("unknown checkpoint lineage")
        if lineage_row["status"] != "active":
            raise ValueError(f"checkpoint lineage is {lineage_row['status']}")
        if lineage_row["connector_version"] != connector_version:
            raise ValueError("connector version does not match checkpoint lineage")
        if declare_partition:
            await connection.execute(
                """update catalogue.job_checkpoint_lineages
                      set connector_configuration = jsonb_set(
                            connector_configuration, '{partitions}',
                            coalesce(connector_configuration->'partitions', '[]'::jsonb)
                              || to_jsonb(%s::text), true),
                          updated_at = now()
                    where job_id = %s and checkpoint_lineage = %s
                      and not coalesce(connector_configuration->'partitions', '[]'::jsonb)
                              @> to_jsonb(array[%s::text])""",
                (partition_key, job_id, lineage, partition_key),
            )

        existing = await _one(
            connection,
            """select page_sequence, resume_after, terminal, enumeration_intact,
                      connector_version
                 from catalogue.job_pages
                where job_id = %s and checkpoint_lineage = %s
                  and partition_key = %s and page_id = %s""",
            (job_id, lineage, partition_key, page_id),
        )
        if existing is not None:
            await _verify_replay(
                connection, job_id, lineage, partition_key, page_id, page_sequence,
                resume_after, terminal, enumeration_intact, connector_version, batches,
                committed_outcomes, existing,
            )
            return False

        position = await _one(
            connection,
            """select max(page_sequence) as last_sequence,
                      bool_or(terminal) as already_terminal
                 from catalogue.job_pages
                where job_id = %s and checkpoint_lineage = %s and partition_key = %s""",
            (job_id, lineage, partition_key),
        )
        if position and position["already_terminal"]:
            raise ValueError("cannot append after a terminal page")
        if (
            position
            and position["last_sequence"] is not None
            and page_sequence <= int(position["last_sequence"])
        ):
            raise ValueError("page sequence must increase monotonically")

        await connection.execute(
            """insert into catalogue.job_pages
                         (job_id, checkpoint_lineage, partition_key, page_id, page_sequence,
                          resume_after, terminal, enumeration_intact, connector_version)
                  values (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                job_id, lineage, partition_key, page_id, page_sequence,
                Jsonb(resume_after) if resume_after is not None else None,
                terminal, enumeration_intact, connector_version,
            ),
        )
        for batch in batches:
            await connection.execute(
                """insert into catalogue.job_page_batches
                             (job_id, checkpoint_lineage, partition_key, page_id, page_sequence,
                              dataset, contract_version, projector_version, object_key,
                              sha256, size, records)
                      values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    job_id, lineage, partition_key, page_id, page_sequence,
                    batch.key.dataset, batch.key.contract_version, batch.key.projector_version,
                    batch.object_key, batch.sha256, batch.size, batch.records,
                ),
            )
        await _record_outcomes(
            connection, job_id, lineage, partition_key, page_id, page_sequence,
            batches, committed_outcomes,
        )
        await connection.execute(
            """update catalogue.job_checkpoint_lineages set updated_at = now()
                where job_id = %s and checkpoint_lineage = %s""",
            (job_id, lineage),
        )
    return True


async def _verify_replay(
    connection: Connection,
    job_id: UUID,
    lineage: UUID,
    partition_key: str,
    page_id: str,
    page_sequence: int,
    resume_after: Any,
    terminal: bool,
    enumeration_intact: bool,
    connector_version: str,
    batches: list[PageBatch],
    outcomes: list[DatasetOutcome],
    existing: dict[str, Any],
) -> None:
    expected_page = (
        page_sequence, resume_after, terminal, enumeration_intact, connector_version,
    )
    actual_page = (
        existing["page_sequence"], existing["resume_after"], existing["terminal"],
        existing["enumeration_intact"], existing["connector_version"],
    )
    if actual_page != expected_page:
        raise ValueError("page replay differs from committed manifest")
    rows = await _all(
        connection,
        """select dataset, contract_version, projector_version, object_key, sha256, size, records
             from catalogue.job_page_batches
            where job_id = %s and checkpoint_lineage = %s
              and partition_key = %s and page_id = %s
            order by dataset, contract_version, projector_version""",
        (job_id, lineage, partition_key, page_id),
    )
    expected_batches = sorted(
        (
            batch.key.dataset, batch.key.contract_version, batch.key.projector_version,
            batch.object_key, batch.sha256, batch.size, batch.records,
        )
        for batch in batches
    )
    actual_batches = [tuple(row.values()) for row in rows]
    if actual_batches != expected_batches:
        raise ValueError("page replay differs from committed dataset batches")
    outcome_rows = await _all(
        connection,
        """select dataset, contract_version, projector_version, state, records, error
             from catalogue.job_page_dataset_outcomes
            where job_id = %s and checkpoint_lineage = %s
              and partition_key = %s and page_id = %s
            order by dataset, contract_version, projector_version""",
        (job_id, lineage, partition_key, page_id),
    )
    expected_outcomes = sorted(
        (
            outcome.key.dataset, outcome.key.contract_version, outcome.key.projector_version,
            outcome.state, outcome.records, outcome.error,
        )
        for outcome in outcomes
    )
    if [tuple(row.values()) for row in outcome_rows] != expected_outcomes:
        raise ValueError("page replay differs from committed dataset outcomes")


async def _record_outcomes(
    connection: Connection,
    job_id: UUID,
    lineage: UUID,
    partition_key: str,
    page_id: str,
    page_sequence: int,
    batches: list[PageBatch],
    outcomes: list[DatasetOutcome],
) -> None:
    batch_records = {batch.key: batch.records for batch in batches}
    if set(batch_records) != {item.key for item in outcomes if item.state == "succeeded"}:
        raise ValueError("successful outcomes and staged batches do not match")
    for outcome in outcomes:
        if outcome.state not in {"succeeded", "failed", "skipped"}:
            raise ValueError(f"invalid dataset page outcome {outcome.state!r}")
        if outcome.state == "failed" and not outcome.error:
            raise ValueError("failed dataset outcome requires an error")
        if outcome.state == "succeeded" and batch_records[outcome.key] != outcome.records:
            raise ValueError("successful outcome record count differs from its batch")
        await connection.execute(
            """insert into catalogue.job_page_dataset_outcomes
                         (job_id, checkpoint_lineage, partition_key, page_id, page_sequence,
                          dataset, contract_version, projector_version, state, records, error)
                  values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                job_id, lineage, partition_key, page_id, page_sequence,
                outcome.key.dataset, outcome.key.contract_version,
                outcome.key.projector_version, outcome.state, outcome.records, outcome.error,
            ),
        )
        if outcome.state == "succeeded":
            await connection.execute(
                """update catalogue.job_datasets
                      set state = case when state = 'pending' then 'staged' else state end,
                          records = records + %s, updated_at = now()
                    where job_id = %s and dataset = %s and contract_version = %s
                      and projector_version = %s and state <> 'failed'""",
                (
                    outcome.records, job_id, outcome.key.dataset,
                    outcome.key.contract_version, outcome.key.projector_version,
                ),
            )
        elif outcome.state == "failed":
            await connection.execute(
                """update catalogue.job_datasets
                      set state = 'failed', complete = false,
                          error = coalesce(error, %s), updated_at = now()
                    where job_id = %s and dataset = %s and contract_version = %s
                      and projector_version = %s""",
                (
                    outcome.error, job_id, outcome.key.dataset,
                    outcome.key.contract_version, outcome.key.projector_version,
                ),
            )
        else:
            current = await _one(
                connection,
                """select state from catalogue.job_datasets
                    where job_id = %s and dataset = %s and contract_version = %s
                      and projector_version = %s""",
                (
                    job_id, outcome.key.dataset, outcome.key.contract_version,
                    outcome.key.projector_version,
                ),
            )
            if current is None or current["state"] != "failed":
                raise ValueError("a skipped projector must already be failed")


def aggregate_job_state(states: list[str], *, cancelled: bool = False) -> str:
    """Apply the documented multi-dataset terminal-state matrix."""
    if cancelled:
        return "cancelled"
    if not states:
        return "failed"
    usable = sum(state in {"succeeded", "degraded"} for state in states)
    if usable == len(states) and all(state == "succeeded" for state in states):
        return "succeeded"
    if usable:
        return "degraded"
    return "failed"


async def complete_lineage(
    connection: Connection,
    job_id: UUID,
    lineage: UUID,
    *,
    expected_partitions: Sequence[str],
    checksum: str,
) -> bool:
    """Seal an intact lineage once every expected partition is terminal."""
    async with connection.transaction():
        current = await _one(
            connection,
            """select status, checksum from catalogue.job_checkpoint_lineages
                where job_id = %s and checkpoint_lineage = %s for update""",
            (job_id, lineage),
        )
        if current is None:
            raise ValueError("unknown checkpoint lineage")
        if current["status"] == "completed":
            if current["checksum"] != checksum:
                raise ValueError("completed checkpoint checksum changed")
            return False
        if current["status"] != "active":
            raise ValueError(f"checkpoint lineage is {current['status']}")
        partitions = await _all(
            connection,
            """select partition_key, bool_or(terminal) as terminal,
                      bool_and(enumeration_intact) as enumeration_intact
                 from catalogue.job_pages
                where job_id = %s and checkpoint_lineage = %s
                group by partition_key""",
            (job_id, lineage),
        )
        if len(set(expected_partitions)) != len(expected_partitions):
            raise ValueError("checkpoint partition manifest contains duplicates")
        if {row["partition_key"] for row in partitions} != set(expected_partitions):
            raise ValueError("checkpoint partition manifest is incomplete")
        if any(not row["terminal"] or not row["enumeration_intact"] for row in partitions):
            raise ValueError("checkpoint enumeration is not intact and terminal")
        await connection.execute(
            """update catalogue.job_checkpoint_lineages
                  set status = 'completed', checksum = %s, updated_at = now()
                where job_id = %s and checkpoint_lineage = %s""",
            (checksum, job_id, lineage),
        )
    return True


async def complete_limited_lineage(
    connection: Connection,
    job_id: UUID,
    lineage: UUID,
    *,
    checksum: str,
) -> bool:
    """Seal an intentional result-limit boundary for partial publication.

    A limited lineage is terminal for this bounded collection request but not
    a complete source enumeration. Its non-null cursor remains in the audit
    manifest; publication may use the committed prefix, while catalogue load
    policy must continue to treat it as non-whole and never retire missing
    records.
    """
    async with connection.transaction():
        current = await _one(
            connection,
            """select status, checksum from catalogue.job_checkpoint_lineages
                where job_id = %s and checkpoint_lineage = %s for update""",
            (job_id, lineage),
        )
        if current is None:
            raise ValueError("unknown checkpoint lineage")
        if current["status"] == "limited":
            if current["checksum"] != checksum:
                raise ValueError("limited checkpoint checksum changed")
            return False
        if current["status"] != "active":
            raise ValueError(f"checkpoint lineage is {current['status']}")

        pages = await _all(
            connection,
            """select partition_key, page_sequence, page_id, resume_after,
                      terminal, enumeration_intact
                 from catalogue.job_pages
                where job_id = %s and checkpoint_lineage = %s""",
            (job_id, lineage),
        )
        if not pages:
            raise ValueError("limited checkpoint lineage has no committed page")
        order = await _partition_order(connection, job_id, lineage)
        latest = max(
            pages,
            key=lambda page: (
                order.get(page["partition_key"], len(order)),
                int(page["page_sequence"]),
                page["page_id"],
            ),
        )
        if (
            latest["resume_after"] is None
            or not latest["terminal"]
            or latest["enumeration_intact"]
        ):
            raise ValueError(
                "limited checkpoint lineage must end at a resumable incomplete terminal page"
            )
        await connection.execute(
            """update catalogue.job_checkpoint_lineages
                  set status = 'limited', checksum = %s, updated_at = now()
                where job_id = %s and checkpoint_lineage = %s""",
            (checksum, job_id, lineage),
        )
    return True


async def lineage_checksum(connection: Connection, job_id: UUID, lineage: UUID) -> str:
    """Hash the ordered committed manifest, including projector outcomes."""
    import hashlib
    import json

    pages = await _all(
        connection,
        """select partition_key, page_sequence, page_id, resume_after, terminal,
                  enumeration_intact, connector_version
             from catalogue.job_pages
            where job_id = %s and checkpoint_lineage = %s
            order by page_sequence, page_id""",
        (job_id, lineage),
    )
    batches = await _all(
        connection,
        """select partition_key, page_sequence, page_id, dataset, contract_version,
                  projector_version, object_key, sha256, size, records
             from catalogue.job_page_batches
            where job_id = %s and checkpoint_lineage = %s
            order by page_sequence, page_id, dataset,
                     contract_version, projector_version""",
        (job_id, lineage),
    )
    outcomes = await _all(
        connection,
        """select partition_key, page_sequence, page_id, dataset, contract_version,
                  projector_version, state, records, error
             from catalogue.job_page_dataset_outcomes
            where job_id = %s and checkpoint_lineage = %s
            order by page_sequence, page_id, dataset,
                     contract_version, projector_version""",
        (job_id, lineage),
    )
    order = await _partition_order(connection, job_id, lineage)
    pages.sort(key=lambda row: (order.get(row["partition_key"], len(order)),
                                int(row["page_sequence"]), row["page_id"]))
    batches.sort(key=lambda row: (order.get(row["partition_key"], len(order)),
                                  int(row["page_sequence"]), row["page_id"], row["dataset"],
                                  row["contract_version"], row["projector_version"]))
    outcomes.sort(key=lambda row: (order.get(row["partition_key"], len(order)),
                                   int(row["page_sequence"]), row["page_id"], row["dataset"],
                                   row["contract_version"], row["projector_version"]))
    encoded = json.dumps(
        {"pages": pages, "batches": batches, "outcomes": outcomes},
        sort_keys=True, separators=(",", ":"), default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


async def resume_checkpoint(
    connection: Connection, job_id: UUID, lineage: UUID
) -> ConnectorCheckpoint | None:
    """Return the cursor after the latest durable page, never process memory."""
    progress = await lineage_progress(connection, job_id, lineage)
    if progress.state is LineageProgressState.TERMINAL_INCOMPLETE:
        raise ValueError("incomplete terminal checkpoint requires a new lineage")
    return progress.checkpoint


async def lineage_progress(
    connection: Connection, job_id: UUID, lineage: UUID
) -> LineageProgress:
    """Classify the latest durable page without conflating empty and terminal.

    A terminal intact page means collection already finished even if the
    process crashed before lineage completion or publication. Callers must not
    turn that state into ``checkpoint=None`` and refetch from the beginning.
    """
    row = await _one(
        connection,
        """select connector, connector_version, source_id, status,
                  connector_configuration
             from catalogue.job_checkpoint_lineages
            where job_id = %s and checkpoint_lineage = %s""",
        (job_id, lineage),
    )
    if row is None:
        raise ValueError("unknown checkpoint lineage")
    if row["status"] in {"rejected", "expired"}:
        raise ValueError(f"checkpoint lineage is {row['status']}")
    pages = await _all(
        connection,
        """select partition_key, page_sequence, page_id, resume_after, terminal,
                  enumeration_intact
             from catalogue.job_pages
            where job_id = %s and checkpoint_lineage = %s""",
        (job_id, lineage),
    )
    if not pages:
        if row["status"] == "limited":
            raise ValueError("limited checkpoint lineage has no committed page")
        return LineageProgress(LineageProgressState.EMPTY)
    if row["status"] == "limited":
        return LineageProgress(LineageProgressState.TERMINAL_LIMITED)
    order = {
        partition: index
        for index, partition in enumerate(
            dict(row["connector_configuration"] or {}).get("partitions") or ()
        )
    }
    latest = max(
        pages,
        key=lambda page: (
            order.get(page["partition_key"], len(order)),
            int(page["page_sequence"]),
            page["page_id"],
        ),
    )
    if latest["resume_after"] is not None:
        return LineageProgress(
            LineageProgressState.RESUMABLE,
            ConnectorCheckpoint(
                connector=row["connector"], connector_version=row["connector_version"],
                source_id=row["source_id"], lineage=str(lineage),
                resume_after=latest["resume_after"],
            ),
        )
    if latest["terminal"]:
        if not latest["enumeration_intact"]:
            return LineageProgress(LineageProgressState.TERMINAL_INCOMPLETE)
        return LineageProgress(LineageProgressState.TERMINAL_INTACT)
    return LineageProgress(LineageProgressState.EMPTY)


async def _partition_order(
    connection: Connection, job_id: UUID, lineage: UUID
) -> dict[str, int]:
    row = await _one(
        connection,
        """select connector_configuration
             from catalogue.job_checkpoint_lineages
            where job_id = %s and checkpoint_lineage = %s""",
        (job_id, lineage),
    )
    if row is None:
        raise ValueError("unknown checkpoint lineage")
    partitions = dict(row["connector_configuration"] or {}).get("partitions") or ()
    return {str(partition): index for index, partition in enumerate(partitions)}


async def declared_partitions(
    connection: Connection, job_id: UUID, lineage: UUID
) -> tuple[str, ...]:
    order = await _partition_order(connection, job_id, lineage)
    return tuple(partition for partition, _ in sorted(order.items(), key=lambda item: item[1]))


async def reconstruct_batches(
    connection: Connection,
    job_id: UUID,
    lineage: UUID,
    key: DatasetKey,
) -> list[StoredBatch]:
    """Rebuild a dataset's committed batches in deterministic compaction order."""
    rows = await _all(
        connection,
        """select partition_key, page_id, page_sequence, object_key, sha256, size, records
             from catalogue.job_page_batches
            where job_id = %s and checkpoint_lineage = %s and dataset = %s
              and contract_version = %s and projector_version = %s
            order by page_sequence, page_id""",
        (job_id, lineage, key.dataset, key.contract_version, key.projector_version),
    )
    order = await _partition_order(connection, job_id, lineage)
    rows.sort(key=lambda row: (order.get(row["partition_key"], len(order)),
                               int(row["page_sequence"]), row["page_id"]))
    return [
        StoredBatch(
            location=row["object_key"],
            sha256=row["sha256"],
            size=int(row["size"]),
            records=int(row["records"]),
            identity=BatchIdentity(
                job_id=str(job_id),
                checkpoint_lineage=str(lineage),
                partition_key=row["partition_key"],
                page_id=row["page_id"],
                page_sequence=int(row["page_sequence"]),
                dataset=key.dataset,
                contract_version=key.contract_version,
                projector_version=key.projector_version,
            ),
        )
        for row in rows
    ]


async def register_artifact(
    connection: Connection,
    job_id: UUID,
    key: DatasetKey,
    artifact: StoredObject,
    *,
    kind: str = "ndjson.gz",
) -> UUID:
    """Register an immutable publication, accepting only an exact replay."""
    inserted = await _one(
        connection,
        """insert into catalogue.job_artifacts
                     (job_id, dataset, contract_version, projector_version, kind,
                      location, sha256, size)
              values (%s, %s, %s, %s, %s, %s, %s, %s)
              on conflict (job_id, dataset, contract_version, projector_version, kind)
              do nothing returning id""",
        (
            job_id, key.dataset, key.contract_version, key.projector_version,
            kind, artifact.location, artifact.sha256, artifact.size,
        ),
    )
    if inserted is not None:
        return inserted["id"]  # type: ignore[no-any-return]
    existing = await _one(
        connection,
        """select id, location, sha256, size from catalogue.job_artifacts
            where job_id = %s and dataset = %s and contract_version = %s
              and projector_version = %s and kind = %s""",
        (job_id, key.dataset, key.contract_version, key.projector_version, kind),
    )
    expected = (artifact.location, artifact.sha256, artifact.size)
    if existing is None or (
        existing["location"], existing["sha256"], existing["size"]
    ) != expected:
        raise ValueError("published artifact identity changed")
    return existing["id"]  # type: ignore[no-any-return]


async def publish_dataset(
    connection: Connection,
    store: ArtifactStore,
    job_id: UUID,
    lineage: UUID,
    key: DatasetKey,
    *,
    kind: str = "ndjson.gz",
) -> StoredObject:
    """Compact committed batches and register the deterministic publication."""
    lineage_row = await _one(
        connection,
        """select status from catalogue.job_checkpoint_lineages
            where job_id = %s and checkpoint_lineage = %s""",
        (job_id, lineage),
    )
    if lineage_row is None or lineage_row["status"] not in {"completed", "limited"}:
        raise ValueError("only a completed or limited checkpoint lineage can be published")
    batches = await reconstruct_batches(connection, job_id, lineage, key)
    artifact = store.publish_dataset(str(job_id), key.dataset, key.contract_version, batches)
    await register_artifact(connection, job_id, key, artifact, kind=kind)
    await connection.execute(
        """update catalogue.job_datasets
              set state = 'published', complete = true, updated_at = now()
            where job_id = %s and dataset = %s and contract_version = %s
              and projector_version = %s and state <> 'failed'""",
        (job_id, key.dataset, key.contract_version, key.projector_version),
    )
    return artifact


async def finish_dataset(
    connection: Connection,
    job_id: UUID,
    key: DatasetKey,
    *,
    state: str,
    complete: bool,
    rejected: int = 0,
    error: str | None = None,
    promoted: bool = False,
) -> None:
    if state not in {"succeeded", "degraded", "failed", "cancelled", "skipped"}:
        raise ValueError(f"invalid terminal dataset state {state!r}")
    await connection.execute(
        """update catalogue.job_datasets
              set state = %s, complete = %s, rejected = %s, error = %s,
                  promoted_at = case when %s then now() else promoted_at end,
                  updated_at = now()
            where job_id = %s and dataset = %s and contract_version = %s
              and projector_version = %s""",
        (
            state, complete, rejected, error, promoted, job_id,
            key.dataset, key.contract_version, key.projector_version,
        ),
    )


async def _one(connection: Connection, sql: str, params: Any) -> dict[str, Any] | None:
    async with connection.cursor() as cursor:
        await cursor.execute(sql, params)
        return await cursor.fetchone()


async def _all(connection: Connection, sql: str, params: Any) -> list[dict[str, Any]]:
    async with connection.cursor() as cursor:
        await cursor.execute(sql, params)
        return await cursor.fetchall()
