"""The response cache travels as one immutable, verified tarball.

These never touch the network: `client()` is the single seam every transfer
goes through, so a stub standing in for it exercises the whole of push, pull
and verify, including the integrity check that is the point of the manifest.
"""

from __future__ import annotations

import json
import tarfile
from pathlib import Path

import pytest

from mb_ceramics_catalogue.ops import cache_archive


class FakeClient:
    """An S3 client backed by a directory, recording what it was asked to do."""

    def __init__(self, store: Path) -> None:
        self.store = store
        self.uploads: list[tuple[str, str]] = []

    def _path(self, key: str) -> Path:
        return self.store / key.replace("/", "_")

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.uploads.append((bucket, key))
        self._path(key).write_bytes(Path(filename).read_bytes())

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        source = self._path(key)
        if not source.exists():
            raise FileNotFoundError(key)
        Path(filename).write_bytes(source.read_bytes())

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, int]:  # boto3 spells it this way
        return {"ContentLength": self._path(Key).stat().st_size}


@pytest.fixture
def settings() -> cache_archive.Settings:
    return cache_archive.Settings(bucket="catalogue-cache", endpoint="https://r2.invalid")


@pytest.fixture
def remote(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeClient:
    store = tmp_path / "remote"
    store.mkdir()
    fake = FakeClient(store)
    monkeypatch.setattr(cache_archive, "client", lambda _settings: fake)
    return fake


def make_cache(root: Path) -> Path:
    cache = root / ".cache"
    (cache / "shop.example").mkdir(parents=True)
    (cache / "shop.example" / "a.gz").write_bytes(b"first page")
    (cache / "shop.example" / "b.gz").write_bytes(b"second page")
    (cache / "other.example").mkdir()
    (cache / "other.example" / "c.gz").write_bytes(b"third page")
    return cache


def test_push_uploads_and_records_what_it_uploaded(tmp_path, settings, remote):
    cache = make_cache(tmp_path)
    manifest = tmp_path / "cache-archive.json"

    record = cache_archive.push(settings, cache=cache, manifest=manifest)

    assert remote.uploads == [("catalogue-cache", record["key"])]
    assert record["files"] == 3
    assert record["hosts"] == ["other.example", "shop.example"]
    assert record["key"] == f"cache/{record['sha256'][:16]}.tar"
    assert json.loads(manifest.read_text()) == record


def test_push_can_publish_only_reviewed_hosts(tmp_path, settings, remote):
    cache = make_cache(tmp_path)
    manifest = tmp_path / "cache-archive.json"

    record = cache_archive.push(
        settings,
        cache=cache,
        manifest=manifest,
        hosts=("shop.example",),
    )

    assert record["files"] == 2
    assert record["hosts"] == ["shop.example"]
    archive = remote._path(record["key"])
    with tarfile.open(archive, "r") as opened:
        assert [member.name for member in opened.getmembers()] == [
            "shop.example/a.gz",
            "shop.example/b.gz",
        ]


@pytest.mark.parametrize("host", ("missing.example", "../escape", ""))
def test_push_rejects_invalid_or_absent_host_selection(
    tmp_path, settings, remote, host
):
    with pytest.raises(cache_archive.CacheArchiveError, match="cache host"):
        cache_archive.push(
            settings,
            cache=make_cache(tmp_path),
            manifest=tmp_path / "cache-archive.json",
            hosts=(host,),
        )


def test_the_archive_is_the_same_bytes_for_the_same_cache(tmp_path, settings, remote):
    one = cache_archive.push(
        settings, cache=make_cache(tmp_path / "one"), manifest=tmp_path / "one.json"
    )
    two = cache_archive.push(
        settings, cache=make_cache(tmp_path / "two"), manifest=tmp_path / "two.json"
    )
    # Two machines that recorded the same pages must not publish two objects.
    assert one["sha256"] == two["sha256"]
    assert one["key"] == two["key"]


def test_pull_restores_every_entry(tmp_path, settings, remote):
    manifest = tmp_path / "cache-archive.json"
    cache_archive.push(settings, cache=make_cache(tmp_path), manifest=manifest)

    restored = tmp_path / "restored"
    cache_archive.pull(settings, cache=restored, manifest=manifest)

    assert (restored / "shop.example" / "a.gz").read_bytes() == b"first page"
    assert (restored / "other.example" / "c.gz").read_bytes() == b"third page"
    assert sorted(path.name for path in restored.iterdir()) == ["other.example", "shop.example"]


def test_pull_refuses_to_merge_into_an_existing_cache(tmp_path, settings, remote):
    manifest = tmp_path / "cache-archive.json"
    cache = make_cache(tmp_path)
    cache_archive.push(settings, cache=cache, manifest=manifest)

    with pytest.raises(cache_archive.CacheArchiveError, match="--force"):
        cache_archive.pull(settings, cache=cache, manifest=manifest)


def test_force_replaces_rather_than_merges(tmp_path, settings, remote):
    manifest = tmp_path / "cache-archive.json"
    cache = make_cache(tmp_path)
    cache_archive.push(settings, cache=cache, manifest=manifest)
    # A stale entry from an older cache must not survive the pull: a partial
    # cache fails the golden tests rather than skipping them.
    (cache / "shop.example" / "stale.gz").write_bytes(b"gone")

    cache_archive.pull(settings, cache=cache, manifest=manifest, force=True)

    assert not (cache / "shop.example" / "stale.gz").exists()


def test_a_corrupted_download_is_refused_before_it_is_unpacked(tmp_path, settings, remote):
    manifest = tmp_path / "cache-archive.json"
    record = cache_archive.push(settings, cache=make_cache(tmp_path), manifest=manifest)
    remote._path(record["key"]).write_bytes(b"not the archive you recorded")

    restored = tmp_path / "restored"
    with pytest.raises(cache_archive.CacheArchiveError, match="does not match the manifest"):
        cache_archive.pull(settings, cache=restored, manifest=manifest)
    assert not restored.exists()


def test_push_refuses_an_absent_or_empty_cache(tmp_path, settings, remote):
    with pytest.raises(cache_archive.CacheArchiveError, match="nothing to push"):
        cache_archive.push(settings, cache=tmp_path / "absent", manifest=tmp_path / "m.json")

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(cache_archive.CacheArchiveError, match="empty"):
        cache_archive.push(settings, cache=empty, manifest=tmp_path / "m.json")


def test_pull_without_a_manifest_says_so(tmp_path, settings, remote):
    with pytest.raises(cache_archive.CacheArchiveError, match="no cache manifest"):
        cache_archive.pull(settings, cache=tmp_path / "c", manifest=tmp_path / "absent.json")


def test_verify_checks_the_object_is_still_there_and_the_right_size(tmp_path, settings, remote):
    manifest = tmp_path / "cache-archive.json"
    record = cache_archive.push(settings, cache=make_cache(tmp_path), manifest=manifest)

    assert cache_archive.verify(settings, manifest=manifest) == record

    remote._path(record["key"]).write_bytes(b"truncated")
    with pytest.raises(cache_archive.CacheArchiveError, match="manifest says"):
        cache_archive.verify(settings, manifest=manifest)


def test_settings_require_the_bucket_and_endpoint():
    with pytest.raises(cache_archive.CacheArchiveError, match="CATALOGUE_CACHE_BUCKET"):
        cache_archive.Settings.from_env({})

    loaded = cache_archive.Settings.from_env(
        {"CATALOGUE_CACHE_BUCKET": "b", "CATALOGUE_CACHE_ENDPOINT": "https://r2.invalid"}
    )
    assert loaded.bucket == "b"


def test_the_archive_carries_no_ownership_or_timestamps(tmp_path, settings, remote):
    """Reproducibility is what lets the digest in the key mean "this content"."""
    archive = tmp_path / "cache.tar"
    cache_archive.build(make_cache(tmp_path), archive)

    with tarfile.open(archive, "r") as opened:
        members = opened.getmembers()
    assert [member.name for member in members] == [
        "other.example/c.gz",
        "shop.example/a.gz",
        "shop.example/b.gz",
    ]
    assert all(member.uid == 0 and member.uname == "" and member.mtime == 0 for member in members)
