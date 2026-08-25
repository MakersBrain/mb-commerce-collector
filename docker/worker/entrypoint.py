"""Stage bind-mounted secrets, then run the catalogue worker unprivileged."""

from __future__ import annotations

import os
import pwd
import secrets
import stat
import sys
from contextlib import suppress
from pathlib import Path

WORKER_NAME = "catalogue"
WEBSHARE_GATEWAY_SOURCE = Path("/run/secrets/webshare-gateway/webshare-gateway.json")
WEBSHARE_GATEWAY_DESTINATION = Path(
    "/run/catalogue-worker-secrets/webshare-gateway.json"
)
MAX_WEBSHARE_GATEWAY_BYTES = 1_048_576


def _read_bounded_regular(path: Path) -> bytes | None:
    """Read one non-empty regular file without following its final component."""

    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return None
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size < 1
            or metadata.st_size > MAX_WEBSHARE_GATEWAY_BYTES
        ):
            return None
        chunks: list[bytes] = []
        remaining = MAX_WEBSHARE_GATEWAY_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        contents = b"".join(chunks)
        if not contents or len(contents) > MAX_WEBSHARE_GATEWAY_BYTES:
            return None
        return contents
    except OSError:
        return None
    finally:
        os.close(descriptor)


def _remove_staged_secret(destination: Path) -> None:
    with suppress(FileNotFoundError):
        destination.unlink()


def _clear_provider_secret_environment() -> None:
    """Keep provider management credentials out of every worker identity."""

    os.environ.pop("CATALOGUE_PROXY_API_SECRET_FILE", None)
    os.environ.pop("CATALOGUE_PROXY_WEBSHARE_API_SECRET_FILE", None)
    os.environ.pop("CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE", None)


def _stage_private_copy(
    contents: bytes,
    destination: Path,
    *,
    owner_uid: int,
    owner_gid: int,
) -> None:
    directory = destination.parent
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = directory.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise RuntimeError("catalogue worker secret directory is not a regular directory")
    os.chown(directory, owner_uid, owner_gid)
    os.chmod(directory, 0o700)

    temporary = directory / f".{destination.name}.{secrets.token_hex(8)}.tmp"
    descriptor = os.open(
        temporary,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        view = memoryview(contents)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise OSError("short write while staging worker secret")
            view = view[written:]
        os.fchown(descriptor, owner_uid, owner_gid)
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
    except BaseException:
        os.close(descriptor)
        _remove_staged_secret(temporary)
        raise
    else:
        os.close(descriptor)
    try:
        os.replace(temporary, destination)
        directory_descriptor = os.open(
            directory,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        # Before replace this removes unpublished credential material. After
        # replace the temporary path no longer exists, including when the
        # directory fsync reports a durability failure.
        _remove_staged_secret(temporary)


def _configure_webshare_gateway(
    source: Path,
    destination: Path,
    *,
    owner_uid: int,
    owner_gid: int,
) -> None:
    """Stage the worker-only gateway secret without enabling paid traffic."""

    _clear_provider_secret_environment()
    contents = _read_bounded_regular(source)
    if contents is None:
        _remove_staged_secret(destination)
        return
    _stage_private_copy(
        contents,
        destination,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )
    os.environ["CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE"] = str(destination)


def _configure_rootless_webshare_gateway(
    source: Path,
    *,
    owner_uid: int,
    owner_gid: int,
) -> None:
    """Validate a keep-id mount and expose its path without privileged staging."""

    _clear_provider_secret_environment()
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(source, flags)
    except FileNotFoundError:
        return
    except OSError:
        raise RuntimeError("rootless Webshare gateway secret cannot be opened safely") from None

    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise RuntimeError("rootless Webshare gateway secret must be a regular file")
        if stat.S_IMODE(metadata.st_mode) not in {0o400, 0o600}:
            raise RuntimeError(
                "rootless Webshare gateway secret must have private owner-only mode"
            )
        if (metadata.st_uid, metadata.st_gid) != (owner_uid, owner_gid):
            raise RuntimeError("rootless Webshare gateway secret has an unexpected owner")
        if metadata.st_size == 0:
            return
        if metadata.st_size > MAX_WEBSHARE_GATEWAY_BYTES:
            raise RuntimeError("rootless Webshare gateway secret exceeds the size limit")

        remaining = MAX_WEBSHARE_GATEWAY_BYTES + 1
        total = 0
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            total += len(chunk)
            remaining -= len(chunk)
        if total == 0 or total > MAX_WEBSHARE_GATEWAY_BYTES or total != metadata.st_size:
            raise RuntimeError("rootless Webshare gateway secret changed while being read")
    except OSError:
        raise RuntimeError("rootless Webshare gateway secret cannot be read safely") from None
    finally:
        os.close(descriptor)

    os.environ["CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE"] = str(source)


def main() -> None:
    worker = pwd.getpwnam(WORKER_NAME)
    current_identity = (os.geteuid(), os.getegid())
    worker_identity = (worker.pw_uid, worker.pw_gid)
    rootful = current_identity[0] == 0
    if not rootful and current_identity != worker_identity:
        raise RuntimeError("catalogue worker entrypoint has an unexpected process identity")

    # Control owns provider credentials and atomically replaces this file.
    # Workers read the shared volume at job start, so rotations take effect
    # without restarting processes and no worker ever receives the API key.
    profiles = Path("/run/proxy-secrets/profiles.json")
    if profiles.is_file():
        os.environ["CATALOGUE_PROXY_SECRET_FILE"] = str(profiles)
    else:
        os.environ.pop("CATALOGUE_PROXY_SECRET_FILE", None)
    if rootful:
        _configure_webshare_gateway(
            WEBSHARE_GATEWAY_SOURCE,
            WEBSHARE_GATEWAY_DESTINATION,
            owner_uid=worker.pw_uid,
            owner_gid=worker.pw_gid,
        )
    else:
        _configure_rootless_webshare_gateway(
            WEBSHARE_GATEWAY_SOURCE,
            owner_uid=worker.pw_uid,
            owner_gid=worker.pw_gid,
        )

    # Docker starts this entrypoint as root so it can stage secrets and then
    # drop privileges below.  Environment variables are not updated by
    # setuid(2), however, and leaving HOME=/root makes camoufox look for its
    # downloaded browser and writable config under /root/.cache after the
    # process is already the unprivileged catalogue user.  The browser was
    # fetched into catalogue's home at image-build time, so make the runtime
    # identity internally consistent before execing the worker.
    os.environ["HOME"] = worker.pw_dir
    os.environ["USER"] = worker.pw_name
    os.environ["LOGNAME"] = worker.pw_name

    if rootful:
        os.setgroups([])
        os.setgid(worker.pw_gid)
        os.setuid(worker.pw_uid)
    os.execvp("catalogue-worker", ["catalogue-worker", *sys.argv[1:]])


if __name__ == "__main__":
    main()
