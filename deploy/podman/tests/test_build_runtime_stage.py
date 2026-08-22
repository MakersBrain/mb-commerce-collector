from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("catalogue_runtime_stage", ROOT / "build_runtime_stage.py")
assert SPEC and SPEC.loader
runtime_stage = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runtime_stage)


def secret_export(root: Path) -> Path:
    for name in (
        "CATALOGUE_SERVICE_DB_PASSWORD",
        "CATALOGUE_CONTROL_DB_PASSWORD",
        "CATALOGUE_DISPATCHER_DB_PASSWORD",
        "CATALOGUE_WORKER_DB_PASSWORD",
    ):
        path = root / "catalogue/database" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("d" * 40)
    token = root / "catalogue/application/CATALOGUE_CONTROL_TOKEN"
    token.parent.mkdir(parents=True)
    token.write_text("t" * 40)
    for role in ("PUBLISH", "CONSUME", "STATS", "ADMIN"):
        path = root / "catalogue/queue" / f"NATS_{role}_PASSWORD"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(role.lower() + "x" * 40)
    return root


def test_builds_scoped_environments_and_credentials(tmp_path: Path) -> None:
    output = tmp_path / "output"
    runtime_stage.build(ROOT / "values.example.json", secret_export(tmp_path / "input"), output)
    service = (output / "config/service.env").read_text()
    worker = (output / "config/worker.env").read_text()
    assert "catalogue_service" in service
    assert "sslmode=verify-full" in service
    assert "CATALOGUE_DUMPS_DIR=/var/lib/catalogue/dumps" in worker
    assert "CATALOGUE_CONTROL_TOKEN=" not in worker
    assert "CATALOGUE_CONTROL_TOKEN=" in (output / "config/explorer.env").read_text()
    assert (output / "secrets/nats-server.conf").stat().st_mode & 0o777 == 0o400
    stats = json.loads((output / "secrets/nats-stats-credentials.json").read_text())
    assert stats["user"] == "catalogue-stats"
    webshare = output / "secrets/webshare-gateway.json"
    assert webshare.read_bytes() == b""
    assert webshare.stat().st_mode & 0o777 == 0o600
    assert "WEBSHARE" not in "".join(
        path.read_text(encoding="utf-8") for path in (output / "config").glob("*.env")
    )


def test_stages_webshare_gateway_secret_byte_exact_without_enabling_it(
    tmp_path: Path,
) -> None:
    root = secret_export(tmp_path / "input")
    source = root / runtime_stage.WEBSHARE_GATEWAY_EXPORT
    source.parent.mkdir(parents=True)
    payload = b'{"schema_version":2,"profiles":{"opaque":"issued:@secret"}}\n'
    source.write_bytes(payload)
    output = tmp_path / "output"

    runtime_stage.build(ROOT / "values.example.json", root, output)

    staged = output / "secrets/webshare-gateway.json"
    assert staged.read_bytes() == payload
    assert staged.stat().st_mode & 0o777 == 0o600
    assert b"issued:@secret" not in b"".join(
        path.read_bytes() for path in (output / "config").glob("*.env")
    )


@pytest.mark.parametrize("kind", ["symlink", "directory", "fifo", "oversized"])
def test_rejects_unsafe_webshare_gateway_secret_inputs(
    tmp_path: Path,
    kind: str,
) -> None:
    root = secret_export(tmp_path / "input")
    source = root / runtime_stage.WEBSHARE_GATEWAY_EXPORT
    source.parent.mkdir(parents=True)
    if kind == "symlink":
        target = tmp_path / "target.json"
        target.write_text("{}", encoding="utf-8")
        source.symlink_to(target)
    elif kind == "directory":
        source.mkdir()
    elif kind == "fifo":
        os.mkfifo(source)
    else:
        source.write_bytes(b"x" * (runtime_stage.MAX_PROVIDER_SECRET_BYTES + 1))

    with pytest.raises(ValueError, match=r"opened safely|regular file|size limit"):
        runtime_stage.build(ROOT / "values.example.json", root, tmp_path / "output")


def test_rejects_missing_scoped_secret(tmp_path: Path) -> None:
    root = secret_export(tmp_path / "input")
    (root / "catalogue/queue/NATS_ADMIN_PASSWORD").unlink()
    with pytest.raises(ValueError, match="NATS_ADMIN_PASSWORD"):
        runtime_stage.build(ROOT / "values.example.json", root, tmp_path / "output")
