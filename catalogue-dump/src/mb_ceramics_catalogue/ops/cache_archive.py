"""Publish and retrieve the recorded response cache as a versioned tarball.

The golden tests are a pure function of `.cache` and the source config, which
makes the cache a build input rather than a working file — and a build input
has to live somewhere every machine can reach. It is 638 MB of already-gzipped
pages, so it belongs in object storage rather than in the tree: committing it
as ordinary blobs would put most of a gigabyte into every clone, and Git LFS
bills bandwidth on every fetch.

The manifest (`cache-archive.json`, checked in) names exactly one archive, so a
commit and the cache it was frozen against travel together: a change to the
cache is a reviewable one-line diff, and an older commit still pulls the
archive its golden files were written from.

Archives are immutable. The key contains the digest of the tar, so `push`
writes a new object rather than overwriting the one an older commit still
names, and `pull` verifies what it downloaded against the manifest before
unpacking a byte of it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mb_ceramics_catalogue.observability import logging as obs

LOGGER = obs.get_logger("catalogue.cache_archive")

ROOT = Path(__file__).resolve().parents[3]
CACHE = ROOT / ".cache"
MANIFEST = ROOT / "cache-archive.json"

#: Read in one megabyte at a time: the archive is larger than anything that
#: should be held in memory to be hashed.
CHUNK = 1024 * 1024


class CacheArchiveError(RuntimeError):
    """A configuration, transfer or integrity failure."""


@dataclass(frozen=True)
class Settings:
    """Everything the tool needs, all of it from the environment.

    Credentials are never accepted on argv: the Infisical agent renders them to
    an environment file, and a process argument would be visible in `ps` and in
    every shell history that ran it.
    """

    bucket: str
    endpoint: str

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        env = dict(os.environ if env is None else env)
        missing = [
            name
            for name in ("CATALOGUE_CACHE_BUCKET", "CATALOGUE_CACHE_ENDPOINT")
            if not env.get(name)
        ]
        if missing:
            raise CacheArchiveError(
                "missing required environment: "
                + ", ".join(missing)
                + ". Credentials and endpoint come from the environment only, never from argv."
            )
        return cls(bucket=env["CATALOGUE_CACHE_BUCKET"], endpoint=env["CATALOGUE_CACHE_ENDPOINT"])


def client(settings: Settings) -> Any:
    """An S3 client pointed at R2.

    boto3 is an optional dependency: a worker that only ever replays a cache it
    already has should not carry an AWS SDK, so the import failure is reported
    as the missing extra rather than as a traceback.
    """
    try:
        import boto3
    except ModuleNotFoundError as error:  # pragma: no cover - depends on the install
        raise CacheArchiveError(
            "boto3 is not installed; install the 'archive' extra: "
            "uv sync --extra archive"
        ) from error
    # R2 ignores the region but the SDK insists on one being set.
    return boto3.client("s3", endpoint_url=settings.endpoint, region_name="auto")


def digest(path: Path) -> str:
    sha = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK):
            sha.update(chunk)
    return sha.hexdigest()


def entries(cache: Path, hosts: tuple[str, ...] = ()) -> list[Path]:
    """Selected cache files in stable order; no hosts means the whole cache."""
    roots = [cache]
    if hosts:
        roots = []
        for host in sorted(set(hosts)):
            if not host or Path(host).name != host or host in {".", ".."}:
                raise CacheArchiveError(f"invalid cache host selection: {host!r}")
            root = cache / host
            if not root.is_dir():
                raise CacheArchiveError(f"selected cache host is absent: {host}")
            roots.append(root)
    return sorted(
        path
        for root in roots
        for path in root.rglob("*")
        if path.is_file()
    )


def build(
    cache: Path,
    destination: Path,
    files: list[Path] | None = None,
) -> None:
    """Tar the cache without compressing it.

    The entries are gzipped pages already; a second pass buys a percent or two
    for minutes of CPU on every push and pull. Names are sorted and ownership
    is pinned so that two runs over identical content produce identical bytes,
    which is what lets the digest in the key mean "this content" rather than
    "this afternoon".
    """
    with tarfile.open(destination, "w", format=tarfile.PAX_FORMAT) as archive:
        for path in files if files is not None else entries(cache):
            info = archive.gettarinfo(str(path), arcname=str(path.relative_to(cache)))
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = 0
            with path.open("rb") as handle:
                archive.addfile(info, handle)


def read_manifest(path: Path = MANIFEST) -> dict[str, Any]:
    if not path.exists():
        raise CacheArchiveError(
            f"no cache manifest at {path}; run `catalogue-cache-archive push` to write one"
        )
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    for field in ("key", "sha256", "bytes", "files"):
        if field not in loaded:
            raise CacheArchiveError(f"cache manifest is missing '{field}': {path}")
    return loaded


def write_manifest(record: dict[str, Any], path: Path = MANIFEST) -> None:
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def push(
    settings: Settings,
    *,
    cache: Path = CACHE,
    manifest: Path = MANIFEST,
    hosts: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Archive the local cache, upload it, and record what was uploaded."""
    if not cache.is_dir():
        raise CacheArchiveError(f"no response cache at {cache}; nothing to push")
    files = entries(cache, hosts)
    if not files:
        raise CacheArchiveError(f"response cache at {cache} is empty; nothing to push")

    with tempfile.TemporaryDirectory() as scratch:
        archive = Path(scratch) / "cache.tar"
        build(cache, archive, files)
        sha = digest(archive)
        key = f"cache/{sha[:16]}.tar"
        record = {
            "key": key,
            "sha256": sha,
            "bytes": archive.stat().st_size,
            "files": len(files),
            "hosts": sorted(
                {path.relative_to(cache).parts[0] for path in files}
            ),
        }
        LOGGER.info("cache.archive.push", key=key, bytes=record["bytes"], files=len(files))
        client(settings).upload_file(str(archive), settings.bucket, key)

    write_manifest(record, manifest)
    return record


def pull(
    settings: Settings,
    *,
    cache: Path = CACHE,
    manifest: Path = MANIFEST,
    force: bool = False,
) -> dict[str, Any]:
    """Fetch the archive the manifest names and unpack it over the cache."""
    record = read_manifest(manifest)
    if cache.exists() and not force:
        raise CacheArchiveError(
            f"{cache} already exists; pass --force to replace it. "
            "A partial cache fails the golden tests rather than skipping them, "
            "so replacing is the only safe merge."
        )

    with tempfile.TemporaryDirectory() as scratch:
        archive = Path(scratch) / "cache.tar"
        LOGGER.info("cache.archive.pull", key=record["key"])
        client(settings).download_file(settings.bucket, record["key"], str(archive))

        found = digest(archive)
        if found != record["sha256"]:
            raise CacheArchiveError(
                f"downloaded archive does not match the manifest: "
                f"expected {record['sha256']}, got {found}"
            )

        staged = Path(scratch) / "cache"
        with tarfile.open(archive, "r") as opened:
            opened.extractall(staged, filter="data")

        if cache.exists():
            shutil.rmtree(cache)
        # Same filesystem is not guaranteed: the scratch directory may be on
        # tmpfs, and rename across devices fails.
        shutil.move(str(staged), str(cache))

    return record


def verify(settings: Settings, *, manifest: Path = MANIFEST) -> dict[str, Any]:
    """Check that the object the manifest names is still there and the right size."""
    record = read_manifest(manifest)
    try:
        head = client(settings).head_object(Bucket=settings.bucket, Key=record["key"])
    except Exception as error:  # botocore raises client-specific errors
        raise CacheArchiveError(f"cannot read {record['key']}: {error}") from error
    if head["ContentLength"] != record["bytes"]:
        raise CacheArchiveError(
            f"{record['key']} is {head['ContentLength']} bytes, "
            f"manifest says {record['bytes']}"
        )
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    push_parser = commands.add_parser(
        "push", help="archive the local cache and upload it"
    )
    push_parser.add_argument(
        "--host",
        action="append",
        default=[],
        help="include only this cache host; repeat for a reviewed subset",
    )
    pull_parser = commands.add_parser("pull", help="download the archive the manifest names")
    pull_parser.add_argument(
        "--force", action="store_true", help="replace an existing local cache"
    )
    commands.add_parser("verify", help="check the manifest's object exists and matches")
    options = parser.parse_args()

    # A missing bucket or a digest mismatch is an operator error, not a defect;
    # a traceback would bury the one line that says what to do about it.
    try:
        settings = Settings.from_env()
        if options.command == "push":
            record = push(settings, hosts=tuple(options.host))
        elif options.command == "pull":
            record = pull(settings, force=options.force)
        else:
            record = verify(settings)
    except CacheArchiveError as error:
        print(f"catalogue-cache-archive: {error}", file=sys.stderr)
        return 1
    print(json.dumps(record, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
