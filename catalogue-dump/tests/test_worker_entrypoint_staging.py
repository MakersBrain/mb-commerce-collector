"""Worker-only Webshare secret staging and Docker Compose placement gates."""

from __future__ import annotations

import json
import os
import runpy
import shutil
import stat
import subprocess
from collections.abc import Callable
from pathlib import Path
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
