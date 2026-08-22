"""The CI replay gate cannot mistake an arbitrary cache directory for parity."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse

import pytest

from . import golden_support as support


def _complete_inputs(root: Path) -> tuple[Path, Path, Path]:
    cache = root / "cache"
    golden = root / "golden"
    manifest = root / "cache-archive.json"
    golden.mkdir()
    configured = support.sources()
    hosts: list[str] = []
    for name in support.REQUIRED_RECORDED_SOURCES:
        host = urlparse(configured[name].url).netloc
        hosts.append(host)
        host_cache = cache / host
        host_cache.mkdir(parents=True)
        (host_cache / "recorded.json.gz").write_bytes(b"recorded")
        (golden / f"{name}.json").write_text("{}", encoding="utf-8")
    manifest.write_text(
        json.dumps(
            {
                "key": "cache/test.tar",
                "sha256": "0" * 64,
                "bytes": 1,
                "files": len(hosts),
                "hosts": hosts,
            }
        ),
        encoding="utf-8",
    )
    return cache, golden, manifest


def test_unset_ci_gate_preserves_archive_free_local_runs(tmp_path: Path) -> None:
    support.require_ci_recordings(
        env={},
        cache=tmp_path / "absent-cache",
        golden=tmp_path / "absent-golden",
        manifest=tmp_path / "absent-manifest.json",
    )


def test_ci_gate_rejects_a_missing_manifest(tmp_path: Path) -> None:
    cache, golden, manifest = _complete_inputs(tmp_path)
    manifest.unlink()

    with pytest.raises(
        support.RecordedReplayPreflightError,
        match="cache archive manifest is absent or invalid",
    ):
        support.require_ci_recordings(
            env={support.CI_ARCHIVE_REQUIRED: "1"},
            cache=cache,
            golden=golden,
            manifest=manifest,
        )


def test_ci_gate_rejects_a_synthetic_unrelated_cache(tmp_path: Path) -> None:
    cache, golden, manifest = _complete_inputs(tmp_path)
    for child in list(cache.iterdir()):
        child.rename(tmp_path / child.name)
    (cache / "shop.test").mkdir(parents=True)
    (cache / "shop.test" / "synthetic.json.gz").write_bytes(b"synthetic")

    with pytest.raises(
        support.RecordedReplayPreflightError,
        match="required replay inputs are unavailable",
    ):
        support.require_ci_recordings(
            env={support.CI_ARCHIVE_REQUIRED: "1"},
            cache=cache,
            golden=golden,
            manifest=manifest,
        )


def test_ci_gate_accepts_all_required_discoverable_inputs(tmp_path: Path) -> None:
    cache, golden, manifest = _complete_inputs(tmp_path)

    support.require_ci_recordings(
        env={support.CI_ARCHIVE_REQUIRED: "1"},
        cache=cache,
        golden=golden,
        manifest=manifest,
    )
