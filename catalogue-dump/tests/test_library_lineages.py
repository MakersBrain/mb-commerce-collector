from __future__ import annotations

from types import TracebackType
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from mb_commerce_scraper import CollectionRequest, LegacyCheckpointRestartReason

from mb_ceramics_catalogue.connectors.base import ConnectorCheckpoint as LegacyCheckpoint
from mb_ceramics_catalogue.ops import library_lineages, outputs


class RecordingTransaction:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def __aenter__(self) -> None:
        self.events.append("transaction.enter")

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del exception, traceback
        self.events.append(
            "transaction.commit" if exception_type is None else "transaction.rollback"
        )
        return False


class RecordingConnection:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def transaction(self) -> RecordingTransaction:
        return RecordingTransaction(self.events)


def _connection(events: list[str]) -> outputs.Connection:
    return cast(outputs.Connection, RecordingConnection(events))


def _spec(request: CollectionRequest) -> library_lineages.LibraryLineageSpec:
    return library_lineages.LibraryLineageSpec(
        request=request,
        connector="shopify",
        connector_version="1",
        connector_options={"page_limit": 50},
        dataset_fingerprint="d" * 64,
        dataset_selection=[
            {
                "dataset": "ceramics",
                "contract_version": "2",
                "projector_version": "1",
            }
        ],
        connector_configuration={"partitions": ["main"]},
    )


@pytest.fixture(autouse=True)
def _no_preexisting_active_lineages(monkeypatch: pytest.MonkeyPatch) -> None:
    async def active(*args: Any, **kwargs: Any) -> tuple[UUID, ...]:
        del args, kwargs
        return ()

    monkeypatch.setattr(outputs, "active_lineages_for_runtime", active)


async def test_compatible_cursor_is_decoded_before_dataset_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    job_id, lineage = uuid4(), uuid4()
    request = CollectionRequest(source_id="shop", base_url="https://shop.test")
    spec = _spec(request)

    async def find(*args: Any, **kwargs: Any) -> UUID:
        del args
        events.append("find")
        assert kwargs["lock"] is True
        return lineage

    async def configuration(*args: Any) -> outputs.LineageRuntimeConfiguration:
        del args
        events.append("configuration")
        return outputs.LineageRuntimeConfiguration(
            outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
            request.model_dump(mode="json"),
            dict(spec.connector_options),
        )

    async def progress(*args: Any) -> outputs.LineageProgress:
        del args
        events.append("cursor")
        return outputs.LineageProgress(
            outputs.LineageProgressState.RESUMABLE,
            LegacyCheckpoint(
                connector=spec.connector,
                connector_version=spec.connector_version,
                source_id=request.source_id,
                lineage=str(lineage),
                resume_after={"partition": "main", "page": 2},
            ),
        )

    async def prepare(*args: Any, **kwargs: Any) -> None:
        del args
        events.append("prepare")
        assert kwargs["resuming"] is True

    async def unexpected(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AssertionError("compatible lineage must not be replaced")

    monkeypatch.setattr(outputs, "find_recoverable_library_lineage", find)
    monkeypatch.setattr(outputs, "lineage_runtime_configuration", configuration)
    monkeypatch.setattr(outputs, "lineage_progress", progress)
    monkeypatch.setattr(outputs, "prepare_dataset_for_collection", prepare)
    monkeypatch.setattr(outputs, "reject_lineage", unexpected)
    monkeypatch.setattr(outputs, "create_lineage", unexpected)

    resolved = await library_lineages.resolve_library_lineage(
        _connection(events),
        job_id,
        spec=spec,
        datasets=[outputs.DatasetKey("ceramics", "2", "1")],
    )

    assert resolved.lineage == lineage
    assert resolved.resuming
    assert resolved.restart_reason is None
    assert resolved.checkpoint is not None
    assert resolved.checkpoint.resume_after == {"partition": "main", "page": 2}
    assert events == [
        "transaction.enter",
        "find",
        "configuration",
        "cursor",
        "prepare",
        "transaction.commit",
    ]


@pytest.mark.parametrize(
    ("configuration_case", "expected_reason"),
    (
        (
            "missing",
            LegacyCheckpointRestartReason.DURABLE_CONFIGURATION_UNAVAILABLE,
        ),
        ("invalid", LegacyCheckpointRestartReason.DURABLE_CONFIGURATION_INVALID),
        (
            "drifted",
            LegacyCheckpointRestartReason.COLLECTION_CONFIGURATION_CHANGED,
        ),
    ),
)
async def test_incompatible_durable_identity_restarts_before_prepare(
    monkeypatch: pytest.MonkeyPatch,
    configuration_case: str,
    expected_reason: LegacyCheckpointRestartReason,
) -> None:
    events: list[str] = []
    old_lineage, new_lineage = uuid4(), uuid4()
    request = CollectionRequest(source_id="shop", base_url="https://shop.test")
    spec = _spec(request)

    async def active(*args: Any, **kwargs: Any) -> tuple[UUID, ...]:
        del args, kwargs
        return (old_lineage,)

    async def find(*args: Any, **kwargs: Any) -> UUID:
        del args, kwargs
        events.append("find")
        return old_lineage

    async def configuration(*args: Any) -> outputs.LineageRuntimeConfiguration:
        del args
        events.append("configuration")
        if configuration_case == "missing":
            durable_request: dict[str, Any] | None = None
            durable_options: dict[str, Any] | None = None
        elif configuration_case == "invalid":
            durable_request = {"source_id": "shop"}
            durable_options = dict(spec.connector_options)
        else:
            durable_request = request.model_copy(
                update={"base_url": "https://old.shop.test"}
            ).model_dump(mode="json")
            durable_options = dict(spec.connector_options)
        return outputs.LineageRuntimeConfiguration(
            outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
            durable_request,
            durable_options,
        )

    async def progress(*args: Any) -> outputs.LineageProgress:
        del args
        events.append("cursor")
        return outputs.LineageProgress(
            outputs.LineageProgressState.RESUMABLE,
            LegacyCheckpoint(
                connector=spec.connector,
                connector_version=spec.connector_version,
                source_id=request.source_id,
                lineage=str(old_lineage),
                resume_after={"page": 99},
            ),
        )

    async def reject(*args: Any) -> bool:
        del args
        events.append("reject")
        return True

    async def create(*args: Any, **kwargs: Any) -> UUID:
        del args
        events.append("create")
        assert kwargs["runtime_format"] is outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1
        assert kwargs["collection_request"] == request.model_dump(mode="json")
        assert kwargs["connector_options"] == spec.connector_options
        return new_lineage

    async def prepare(*args: Any, **kwargs: Any) -> None:
        del args
        events.append("prepare")
        assert kwargs["resuming"] is False

    monkeypatch.setattr(outputs, "active_lineages_for_runtime", active)
    monkeypatch.setattr(outputs, "find_recoverable_library_lineage", find)
    monkeypatch.setattr(outputs, "lineage_runtime_configuration", configuration)
    monkeypatch.setattr(outputs, "lineage_progress", progress)
    monkeypatch.setattr(outputs, "reject_lineage", reject)
    monkeypatch.setattr(outputs, "create_lineage", create)
    monkeypatch.setattr(outputs, "prepare_dataset_for_collection", prepare)

    resolved = await library_lineages.resolve_library_lineage(
        _connection(events),
        uuid4(),
        spec=spec,
        datasets=[outputs.DatasetKey("ceramics", "2", "1")],
    )

    assert resolved == library_lineages.ResolvedLibraryLineage(
        lineage=new_lineage,
        checkpoint=None,
        resuming=False,
        restart_reason=expected_reason,
    )
    assert events == [
        "transaction.enter",
        "find",
        "configuration",
        "cursor",
        "reject",
        "create",
        "prepare",
        "transaction.commit",
    ]


async def test_prepare_failure_rolls_back_the_lineage_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    async def find(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        events.append("find")
        return None

    async def create(*args: Any, **kwargs: Any) -> UUID:
        del args, kwargs
        events.append("create")
        return uuid4()

    async def prepare(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        events.append("prepare")
        raise RuntimeError("dataset update failed")

    monkeypatch.setattr(outputs, "find_recoverable_library_lineage", find)
    monkeypatch.setattr(outputs, "create_lineage", create)
    monkeypatch.setattr(outputs, "prepare_dataset_for_collection", prepare)

    with pytest.raises(RuntimeError, match="dataset update failed"):
        await library_lineages.resolve_library_lineage(
            _connection(events),
            uuid4(),
            spec=_spec(
                CollectionRequest(source_id="shop", base_url="https://shop.test")
            ),
            datasets=[outputs.DatasetKey("ceramics", "2", "1")],
        )

    assert events == [
        "transaction.enter",
        "find",
        "create",
        "prepare",
        "transaction.rollback",
    ]


async def test_configuration_a_b_a_cannot_resurrect_stale_active_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    stale_a, fresh_b = uuid4(), uuid4()

    async def active(*args: Any, **kwargs: Any) -> tuple[UUID, ...]:
        del args
        events.append("lock-active")
        assert kwargs["lock"] is True
        return (stale_a,)

    async def find(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        events.append("find-b")
        return None

    async def reject(*args: Any, **kwargs: Any) -> bool:
        del kwargs
        events.append(f"reject:{args[2]}")
        return True

    async def create(*args: Any, **kwargs: Any) -> UUID:
        del args, kwargs
        events.append("create-b")
        return fresh_b

    async def prepare(*args: Any, **kwargs: Any) -> None:
        del args
        events.append("prepare-b")
        assert kwargs["resuming"] is False

    monkeypatch.setattr(outputs, "active_lineages_for_runtime", active)
    monkeypatch.setattr(outputs, "find_recoverable_library_lineage", find)
    monkeypatch.setattr(outputs, "reject_lineage", reject)
    monkeypatch.setattr(outputs, "create_lineage", create)
    monkeypatch.setattr(outputs, "prepare_dataset_for_collection", prepare)

    resolved = await library_lineages.resolve_library_lineage(
        _connection(events),
        uuid4(),
        spec=_spec(CollectionRequest(source_id="shop", base_url="https://shop.test")),
        datasets=[outputs.DatasetKey("ceramics", "2", "1")],
    )

    assert resolved.lineage == fresh_b
    assert not resolved.resuming
    assert events == [
        "transaction.enter",
        "lock-active",
        "find-b",
        f"reject:{stale_a}",
        "create-b",
        "prepare-b",
        "transaction.commit",
    ]


@pytest.mark.parametrize(
    ("state", "restarts"),
    (
        (outputs.LineageProgressState.TERMINAL_INTACT, False),
        (outputs.LineageProgressState.TERMINAL_LIMITED, False),
        (outputs.LineageProgressState.TERMINAL_INCOMPLETE, True),
    ),
)
async def test_terminal_progress_is_not_mistaken_for_an_empty_lineage(
    monkeypatch: pytest.MonkeyPatch,
    state: outputs.LineageProgressState,
    restarts: bool,
) -> None:
    events: list[str] = []
    old_lineage, new_lineage = uuid4(), uuid4()
    request = CollectionRequest(source_id="shop", base_url="https://shop.test")
    spec = _spec(request)

    async def active(*args: Any, **kwargs: Any) -> tuple[UUID, ...]:
        del args, kwargs
        return (
            (old_lineage,)
            if state is outputs.LineageProgressState.TERMINAL_INCOMPLETE
            else ()
        )

    async def find(*args: Any, **kwargs: Any) -> UUID:
        del args, kwargs
        return old_lineage

    async def configuration(*args: Any) -> outputs.LineageRuntimeConfiguration:
        del args
        return outputs.LineageRuntimeConfiguration(
            outputs.LineageRuntimeFormat.COMMERCE_SCRAPER_V1,
            request.model_dump(mode="json"),
            dict(spec.connector_options),
        )

    async def progress(*args: Any) -> outputs.LineageProgress:
        del args
        return outputs.LineageProgress(state)

    async def reject(*args: Any) -> bool:
        del args
        events.append("reject")
        return True

    async def create(*args: Any, **kwargs: Any) -> UUID:
        del args, kwargs
        events.append("create")
        return new_lineage

    async def prepare(*args: Any, **kwargs: Any) -> None:
        del args
        events.append(f"prepare:{kwargs['resuming']}")

    monkeypatch.setattr(outputs, "active_lineages_for_runtime", active)
    monkeypatch.setattr(outputs, "find_recoverable_library_lineage", find)
    monkeypatch.setattr(outputs, "lineage_runtime_configuration", configuration)
    monkeypatch.setattr(outputs, "lineage_progress", progress)
    monkeypatch.setattr(outputs, "reject_lineage", reject)
    monkeypatch.setattr(outputs, "create_lineage", create)
    monkeypatch.setattr(outputs, "prepare_dataset_for_collection", prepare)

    resolved = await library_lineages.resolve_library_lineage(
        _connection(events),
        uuid4(),
        spec=spec,
        datasets=[outputs.DatasetKey("ceramics", "2", "1")],
    )

    if restarts:
        assert resolved.lineage == new_lineage
        assert resolved.progress is outputs.LineageProgressState.EMPTY
        assert resolved.restart_reason is (
            LegacyCheckpointRestartReason.INCOMPLETE_TERMINAL_CHECKPOINT
        )
        assert events == ["transaction.enter", "reject", "create", "prepare:False", "transaction.commit"]
    else:
        assert resolved.lineage == old_lineage
        assert resolved.progress is state
        assert resolved.restart_reason is None
        assert events == ["transaction.enter", "prepare:True", "transaction.commit"]
