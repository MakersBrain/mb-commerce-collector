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

WORKER_ENV = (
    "CATALOGUE_CACHE_DIR=/var/lib/catalogue/cache",
    "CATALOGUE_DUMPS_DIR=/var/lib/catalogue/dumps",
)


def test_role_table_owns_static_process_environment() -> None:
    assert runtime_stage.DB_ROLES == {
        "service": ("catalogue_service", "CATALOGUE_SERVICE_DB_PASSWORD", ()),
        "control": (
            "catalogue_control",
            "CATALOGUE_CONTROL_DB_PASSWORD",
            (
                "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE="
                "/run/secrets/webshare-gateway/webshare-gateway.json",
            ),
        ),
        "dispatcher": (
            "catalogue_dispatcher",
            "CATALOGUE_DISPATCHER_DB_PASSWORD",
            (),
        ),
        "worker": (
            "catalogue_worker",
            "CATALOGUE_WORKER_DB_PASSWORD",
            WORKER_ENV,
        ),
        "worker-browser": (
            "catalogue_worker",
            "CATALOGUE_WORKER_DB_PASSWORD",
            WORKER_ENV,
        ),
    }


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
    worker_browser = (output / "config/worker-browser.env").read_text()
    control = (output / "config/control.env").read_text()
    dispatcher = (output / "config/dispatcher.env").read_text()
    password = "d" * 40

    def database_env(role: str) -> str:
        return (
            "CATALOGUE_LOG_JSON=true\n"
            f"CATALOGUE_DSN=postgresql://{role}:{password}@10.40.3.10:5432/ateliera"
            "?sslmode=verify-full&sslrootcert=/run/database/postgres-ca.crt\n"
        )

    assert {
        process: (output / f"config/{process}.env").read_text(encoding="utf-8")
        for process in runtime_stage.DB_ROLES
    } == {
        "service": database_env("catalogue_service"),
        "control": (
            database_env("catalogue_control")
            + "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE="
            "/run/secrets/webshare-gateway/webshare-gateway.json\n"
            + f"CATALOGUE_CONTROL_TOKEN={'t' * 40}\n"
        ),
        "dispatcher": (
            database_env("catalogue_dispatcher")
            + f"CATALOGUE_CONTROL_TOKEN={'t' * 40}\n"
        ),
        "worker": database_env("catalogue_worker")
        + "".join(f"{entry}\n" for entry in WORKER_ENV),
        "worker-browser": database_env("catalogue_worker")
        + "".join(f"{entry}\n" for entry in WORKER_ENV),
    }
    assert "catalogue_service" in service
    assert "sslmode=verify-full" in service
    assert "CATALOGUE_DUMPS_DIR=/var/lib/catalogue/dumps" in worker
    assert worker_browser.splitlines()[2:4] == list(WORKER_ENV)
    assert worker.splitlines()[2:4] == list(WORKER_ENV)
    assert "CATALOGUE_CACHE_DIR=" not in service
    assert "CATALOGUE_CACHE_DIR=" not in control
    assert "CATALOGUE_CACHE_DIR=" not in dispatcher
    assert "CATALOGUE_CONTROL_TOKEN=" not in worker
    assert "CATALOGUE_CONTROL_TOKEN=" in (output / "config/explorer.env").read_text()
    assert (output / "secrets/nats-server.conf").stat().st_mode & 0o777 == 0o400
    stats = json.loads((output / "secrets/nats-stats-credentials.json").read_text())
    assert stats["user"] == "catalogue-stats"
    webshare_dir = output / "secrets/webshare-gateway"
    webshare = webshare_dir / "webshare-gateway.json"
    assert not webshare.exists()
    assert webshare_dir.stat().st_mode & 0o777 == 0o700
    assert "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE=" in (
        output / "config/control.env"
    ).read_text(encoding="utf-8")
    for process in ("service", "dispatcher", "worker", "worker-browser"):
        assert "WEBSHARE" not in (output / f"config/{process}.env").read_text(encoding="utf-8")


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

    staged = output / "secrets/webshare-gateway/webshare-gateway.json"
    assert staged.read_bytes() == payload
    assert staged.stat().st_mode & 0o777 == 0o600
    assert b"issued:@secret" not in b"".join(
        path.read_bytes() for path in (output / "config").glob("*.env")
    )


def test_rebuild_preserves_control_rotated_gateway_over_stale_bootstrap(
    tmp_path: Path,
) -> None:
    root = secret_export(tmp_path / "input")
    source = root / runtime_stage.WEBSHARE_GATEWAY_EXPORT
    source.parent.mkdir(parents=True)
    source.write_bytes(b'{"generation":1,"password":"stale-bootstrap"}')
    output = tmp_path / "output"
    runtime_stage.build(ROOT / "values.example.json", root, output)

    gateway = output / "secrets/webshare-gateway/webshare-gateway.json"
    rotated = b'{"generation":2,"password":"control-rotated"}'
    gateway.write_bytes(rotated)
    gateway.chmod(0o400)
    runtime_stage.build(ROOT / "values.example.json", root, output)

    assert gateway.read_bytes() == rotated
    assert gateway.stat().st_mode & 0o777 == 0o400


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
