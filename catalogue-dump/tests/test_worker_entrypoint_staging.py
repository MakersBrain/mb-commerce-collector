"""Worker-only Webshare secret staging and Docker Compose placement gates."""

from __future__ import annotations

import json
import os
import pwd
import runpy
import shutil
import stat
import subprocess
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

ROOT = Path(__file__).resolve().parents[2]
ENTRYPOINT = ROOT / "docker" / "worker" / "entrypoint.py"
COMPOSE = ROOT / "docker-compose.yml"


def _entrypoint() -> tuple[Callable[..., None], int]:
    namespace = runpy.run_path(str(ENTRYPOINT), run_name="worker_entrypoint_test")
    configure = cast(Callable[..., None], namespace["_configure_webshare_gateway"])
    maximum = cast(int, namespace["MAX_WEBSHARE_GATEWAY_BYTES"])
    return configure, maximum


def _entrypoint_namespace() -> dict[str, Any]:
    return runpy.run_path(str(ENTRYPOINT), run_name="worker_entrypoint_test")


def test_worker_stages_private_gateway_copy_without_enabling_paid_traffic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure, _maximum = _entrypoint()
    source = tmp_path / "mounted" / "webshare-gateway.json"
    destination = tmp_path / "private" / "webshare-gateway.json"
    source.parent.mkdir()
    source.write_bytes(b'{"schema_version":2,"profiles":{}}')
    source.chmod(0o444)
    monkeypatch.setenv("CATALOGUE_PROXY_API_SECRET_FILE", "/management/decodo.env")
    monkeypatch.setenv(
        "CATALOGUE_PROXY_WEBSHARE_API_SECRET_FILE",
        "/management/webshare.env",
    )
    monkeypatch.setenv("CATALOGUE_PROXY_WEBSHARE_DATA_PLANE_ENABLED", "true")

    configure(
        source,
        destination,
        owner_uid=os.getuid(),
        owner_gid=os.getgid(),
    )

    metadata = destination.stat()
    assert destination.read_bytes() == source.read_bytes()
    assert stat.S_ISREG(metadata.st_mode)
    assert stat.S_IMODE(metadata.st_mode) == 0o400
    assert (metadata.st_uid, metadata.st_gid) == (os.getuid(), os.getgid())
    assert stat.S_IMODE(destination.parent.stat().st_mode) == 0o700
    assert os.environ["CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE"] == str(
        destination
    )
    assert "CATALOGUE_PROXY_API_SECRET_FILE" not in os.environ
    assert "CATALOGUE_PROXY_WEBSHARE_API_SECRET_FILE" not in os.environ
    # Staging neither sets nor clears the independent operator gate.
    assert os.environ["CATALOGUE_PROXY_WEBSHARE_DATA_PLANE_ENABLED"] == "true"


def test_invalid_sources_remove_stale_stage_and_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure, maximum = _entrypoint()
    valid_target = tmp_path / "valid.json"
    valid_target.write_text("{}", encoding="utf-8")
    sources = [
        tmp_path / "absent.json",
        tmp_path / "empty.json",
        tmp_path / "directory",
        tmp_path / "link.json",
        tmp_path / "fifo",
        tmp_path / "oversized.json",
    ]
    sources[1].touch()
    sources[2].mkdir()
    sources[3].symlink_to(valid_target)
    os.mkfifo(sources[4])
    sources[5].write_bytes(b"x" * (maximum + 1))

    for index, source in enumerate(sources):
        destination = tmp_path / f"private-{index}" / "webshare-gateway.json"
        destination.parent.mkdir()
        destination.write_text("stale", encoding="utf-8")
        monkeypatch.setenv(
            "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE",
            str(destination),
        )
        monkeypatch.setenv("CATALOGUE_PROXY_API_SECRET_FILE", "/management.env")

        configure(
            source,
            destination,
            owner_uid=os.getuid(),
            owner_gid=os.getgid(),
        )

        assert not destination.exists()
        assert "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE" not in os.environ
        assert "CATALOGUE_PROXY_API_SECRET_FILE" not in os.environ


def test_rootless_worker_uses_private_mount_directly_without_privileged_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _entrypoint_namespace()
    main = cast(Callable[[], None], namespace["main"])
    source = tmp_path / "webshare-gateway.json"
    source.write_text('{"schema_version":2,"profiles":{}}', encoding="utf-8")
    source.chmod(0o600)
    owner_uid = source.stat().st_uid or 10_001
    owner_gid = source.stat().st_gid or 10_001
    if source.stat().st_uid == 0:
        os.chown(source, owner_uid, owner_gid)
    main.__globals__["WEBSHARE_GATEWAY_SOURCE"] = source
    main.__globals__["WEBSHARE_GATEWAY_DESTINATION"] = tmp_path / "must-not-exist.json"
    monkeypatch.setattr(
        pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(
            pw_uid=owner_uid,
            pw_gid=owner_gid,
            pw_dir=str(tmp_path),
            pw_name="catalogue",
        ),
    )
    monkeypatch.setattr(os, "geteuid", lambda: owner_uid)
    monkeypatch.setattr(os, "getegid", lambda: owner_gid)

    for operation in ("chown", "setgroups", "setgid", "setuid"):
        monkeypatch.setattr(
            os,
            operation,
            lambda *_args, name=operation: pytest.fail(
                f"rootless entrypoint called privileged operation {name}"
            ),
        )

    command: list[str] = []

    class ExecCalled(RuntimeError):
        pass

    def execvp(_executable: str, arguments: list[str]) -> None:
        command.extend(arguments)
        raise ExecCalled

    monkeypatch.setattr(os, "execvp", execvp)

    with pytest.raises(ExecCalled):
        main()

    assert command[0] == "catalogue-worker"
    assert os.environ["CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE"] == str(source)
    assert source.read_text(encoding="utf-8").startswith('{"schema_version":2')
    assert stat.S_IMODE(source.stat().st_mode) == 0o600
    assert not (tmp_path / "must-not-exist.json").exists()


@pytest.mark.parametrize("mode", [0o400, 0o640, 0o644])
def test_rootless_worker_rejects_gateway_mount_with_non_private_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: int,
) -> None:
    namespace = _entrypoint_namespace()
    configure = cast(
        Callable[..., None],
        namespace["_configure_rootless_webshare_gateway"],
    )
    source = tmp_path / "webshare-gateway.json"
    source.write_text("{}", encoding="utf-8")
    source.chmod(mode)
    monkeypatch.setenv(
        "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE",
        "/stale/secret.json",
    )

    with pytest.raises(RuntimeError, match="mode 0600"):
        configure(
            source,
            owner_uid=source.stat().st_uid,
            owner_gid=source.stat().st_gid,
        )

    assert "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE" not in os.environ


def test_rootless_worker_treats_empty_private_mount_as_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _entrypoint_namespace()
    configure = cast(
        Callable[..., None],
        namespace["_configure_rootless_webshare_gateway"],
    )
    source = tmp_path / "webshare-gateway.json"
    source.touch(mode=0o600)
    monkeypatch.setenv(
        "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE",
        "/stale/secret.json",
    )

    configure(
        source,
        owner_uid=source.stat().st_uid,
        owner_gid=source.stat().st_gid,
    )

    assert "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE" not in os.environ


def test_rootless_worker_rejects_gateway_mount_with_unexpected_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _entrypoint_namespace()
    configure = cast(
        Callable[..., None],
        namespace["_configure_rootless_webshare_gateway"],
    )
    source = tmp_path / "webshare-gateway.json"
    source.write_text("{}", encoding="utf-8")
    source.chmod(0o600)
    monkeypatch.setenv(
        "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE",
        "/stale/secret.json",
    )

    with pytest.raises(RuntimeError, match="unexpected owner"):
        configure(
            source,
            owner_uid=source.stat().st_uid + 1,
            owner_gid=source.stat().st_gid,
        )

    assert "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE" not in os.environ


def test_entrypoint_rejects_unexpected_non_root_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _entrypoint_namespace()
    main = cast(Callable[[], None], namespace["main"])
    monkeypatch.setattr(
        pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(
            pw_uid=10_001,
            pw_gid=10_001,
            pw_dir=str(tmp_path),
            pw_name="catalogue",
        ),
    )
    monkeypatch.setattr(os, "geteuid", lambda: 20_001)
    monkeypatch.setattr(os, "getegid", lambda: 20_001)

    with pytest.raises(RuntimeError, match="unexpected process identity"):
        main()


def test_failed_atomic_publish_removes_temporary_secret_and_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure, _maximum = _entrypoint()
    source = tmp_path / "webshare-gateway.json"
    destination = tmp_path / "private" / "webshare-gateway.json"
    source.write_text('{"secret":"not-in-error"}', encoding="utf-8")
    monkeypatch.setenv(
        "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE",
        "/stale/webshare-gateway.json",
    )

    def fail_replace(_source: Path, _destination: Path) -> None:
        raise OSError("atomic replace unavailable")

    monkeypatch.setattr(os, "replace", fail_replace)

    with pytest.raises(OSError, match="atomic replace unavailable"):
        configure(
            source,
            destination,
            owner_uid=os.getuid(),
            owner_gid=os.getgid(),
        )

    assert not destination.exists()
    assert list(destination.parent.iterdir()) == []
    assert "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE" not in os.environ


def test_compose_mounts_default_off_gateway_secret_only_into_workers() -> None:
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker Compose is required to render the placement contract")
    environment = dict(os.environ)
    environment.update(
        {
            "CATALOGUE_CONTROL_TOKEN": "compose-test-token",
            "CATALOGUE_WEBSHARE_GATEWAY_SECRET_SOURCE": "/dev/null",
            "CATALOGUE_PROXY_WEBSHARE_DATA_PLANE_ENABLED": "false",
        }
    )
    rendered = subprocess.run(
        [
            docker,
            "compose",
            "--env-file",
            "/dev/null",
            "-f",
            str(COMPOSE),
            "config",
            "--format",
            "json",
        ],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    services = cast(dict[str, dict[str, Any]], json.loads(rendered.stdout)["services"])
    target = "/run/secrets/webshare-gateway.json"
    namespace = runpy.run_path(str(ENTRYPOINT), run_name="worker_entrypoint_test")
    assert namespace["WEBSHARE_GATEWAY_SOURCE"] == Path(target)
    for name in ("worker", "worker-browser"):
        service = services[name]
        assert service["environment"][
            "CATALOGUE_PROXY_WEBSHARE_DATA_PLANE_ENABLED"
        ] == "false"
        mounts = [volume for volume in service["volumes"] if volume["target"] == target]
        assert len(mounts) == 1
        assert mounts[0]["type"] == "bind"
        assert mounts[0]["source"] == "/dev/null"
        assert mounts[0]["read_only"] is True

    for name, service in services.items():
        if name in {"worker", "worker-browser"}:
            continue
        assert all(volume["target"] != target for volume in service.get("volumes", ()))

    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "CATALOGUE_WEBSHARE_GATEWAY_SECRET_SOURCE=/dev/null" in example
    assert "CATALOGUE_PROXY_WEBSHARE_DATA_PLANE_ENABLED=false" in example
