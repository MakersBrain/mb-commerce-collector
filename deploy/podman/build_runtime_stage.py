#!/usr/bin/env python3
"""Build exact process environments and NATS credentials from a scoped export."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import stat
from pathlib import Path
from urllib.parse import quote

HERE = Path(__file__).resolve().parent
TOKEN = re.compile(r"^[A-Za-z0-9_-]{32,128}$")
PASSWORD = TOKEN
MAX_PROVIDER_SECRET_BYTES = 1_048_576
WEBSHARE_GATEWAY_EXPORT = "catalogue/proxy/WEBSHARE_GATEWAY_V2_JSON"
DB_ROLES = {
    "service": ("catalogue_service", "CATALOGUE_SERVICE_DB_PASSWORD"),
    "control": ("catalogue_control", "CATALOGUE_CONTROL_DB_PASSWORD"),
    "dispatcher": ("catalogue_dispatcher", "CATALOGUE_DISPATCHER_DB_PASSWORD"),
    "worker": ("catalogue_worker", "CATALOGUE_WORKER_DB_PASSWORD"),
    "worker-browser": ("catalogue_worker", "CATALOGUE_WORKER_DB_PASSWORD"),
}
NATS_ROLES = {
    "publish": "catalogue-publisher",
    "consume": "catalogue-consumer",
    "stats": "catalogue-stats",
    "admin": "catalogue-admin",
}


def _validate_existing_stage(output: Path) -> None:
    """Allow an exact prior stage so its control-owned gateway can survive."""

    if output.is_symlink() or not output.is_dir():
        raise ValueError("runtime stage must be a real directory")
    allowed_files = {
        *(Path("config") / f"{process}.env" for process in DB_ROLES),
        Path("config/explorer.env"),
        *(Path("secrets") / f"nats-{role}-credentials.json" for role in NATS_ROLES),
        Path("secrets/nats-server.conf"),
        Path("secrets/webshare-gateway/webshare-gateway.json"),
    }
    allowed_directories = {
        Path("config"),
        Path("secrets"),
        Path("secrets/webshare-gateway"),
    }
    for path in output.rglob("*"):
        relative = path.relative_to(output)
        if path.is_symlink():
            raise ValueError(f"runtime stage contains a symlink: {relative}")
        if path.is_dir():
            if relative not in allowed_directories:
                raise ValueError(f"runtime stage contains an unexpected directory: {relative}")
        elif not path.is_file() or relative not in allowed_files:
            raise ValueError(f"runtime stage contains an unexpected file: {relative}")

    gateway_directory = output / "secrets/webshare-gateway"
    if gateway_directory.exists() and stat.S_IMODE(gateway_directory.stat().st_mode) != 0o700:
        raise ValueError("existing Webshare gateway directory must have mode 0700")
    gateway = gateway_directory / "webshare-gateway.json"
    if gateway.exists():
        metadata = gateway.stat()
        if stat.S_IMODE(metadata.st_mode) not in {0o400, 0o600}:
            raise ValueError("existing Webshare gateway secret must have owner-only mode")
        if metadata.st_size > MAX_PROVIDER_SECRET_BYTES:
            raise ValueError("existing Webshare gateway secret exceeds its size limit")


def _load_nats_renderer():
    spec = importlib.util.spec_from_file_location("catalogue_nats_renderer", HERE / "render_nats_config.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _secret(root: Path, relative: str, pattern: re.Pattern[str]) -> str:
    path = root / relative
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"required secret input is missing: {relative}")
    value = path.read_text(encoding="utf-8")
    if not pattern.fullmatch(value):
        raise ValueError(f"secret input is invalid: {relative}")
    return value


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o600)


def _optional_secret_bytes(root: Path, relative: str, *, maximum: int) -> bytes:
    """Read an optional bounded regular file without following a final symlink."""
    path = root / relative
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return b""
    except OSError:
        raise ValueError(f"optional secret input cannot be opened safely: {relative}") from None

    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"optional secret input is not a regular file: {relative}")
        if metadata.st_size > maximum:
            raise ValueError(f"optional secret input exceeds its size limit: {relative}")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        content = b"".join(chunks)
        if len(content) > maximum:
            raise ValueError(f"optional secret input exceeds its size limit: {relative}")
        return content
    finally:
        os.close(descriptor)


def _write_bytes(path: Path, content: bytes) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    with os.fdopen(descriptor, "wb") as output:
        output.write(content)


def build(values_path: Path, secret_root: Path, output: Path) -> None:
    if output.exists():
        _validate_existing_stage(output)
    values = json.loads(values_path.read_text(encoding="utf-8"))
    # Reuse the release renderer's strict public-values validation.
    render_spec = importlib.util.spec_from_file_location("catalogue_render", HERE / "render.py")
    assert render_spec and render_spec.loader
    render = importlib.util.module_from_spec(render_spec)
    render_spec.loader.exec_module(render)
    render.load_values(values_path)

    config = output / "config"
    secrets = output / "secrets"
    config.mkdir(parents=True, exist_ok=True, mode=0o700)
    secrets.mkdir(parents=True, exist_ok=True, mode=0o700)
    host = values["postgres_host"]
    port = values["postgres_port"]
    database = values["postgres_database"]
    common = "CATALOGUE_LOG_JSON=true\n"
    for process, (role, password_name) in DB_ROLES.items():
        password = _secret(secret_root, f"catalogue/database/{password_name}", TOKEN)
        dsn = (
            f"postgresql://{role}:{quote(password, safe='')}@{host}:{port}/{database}"
            "?sslmode=verify-full&sslrootcert=/run/database/postgres-ca.crt"
        )
        additions = "CATALOGUE_DSN=" + dsn + "\n"
        if process == "control":
            additions += (
                "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE="
                "/run/secrets/webshare-gateway/webshare-gateway.json\n"
            )
        if process in {"worker", "worker-browser"}:
            additions += "CATALOGUE_CACHE_DIR=/var/lib/catalogue/cache\nCATALOGUE_DUMPS_DIR=/var/lib/catalogue/dumps\n"
        _write(config / f"{process}.env", common + additions)

    control_token = _secret(secret_root, "catalogue/application/CATALOGUE_CONTROL_TOKEN", TOKEN)
    for process in ("control", "dispatcher"):
        path = config / f"{process}.env"
        _write(path, path.read_text(encoding="utf-8") + f"CATALOGUE_CONTROL_TOKEN={control_token}\n")
    _write(
        config / "explorer.env",
        f"CATALOGUE_CONTROL_TOKEN={control_token}\nHOST=0.0.0.0\nPORT=3000\n",
    )

    # Control receives this dedicated directory writable for atomic generation
    # replacement; workers receive the same directory read-only. It contains no
    # provider management credential, and presence never enables paid traffic.
    webshare_gateway = secrets / "webshare-gateway"
    webshare_gateway.mkdir(mode=0o700, exist_ok=True)
    webshare_seed = _optional_secret_bytes(
        secret_root,
        WEBSHARE_GATEWAY_EXPORT,
        maximum=MAX_PROVIDER_SECRET_BYTES,
    )
    gateway_destination = webshare_gateway / "webshare-gateway.json"
    if webshare_seed and not gateway_destination.exists():
        _write_bytes(gateway_destination, webshare_seed)

    for role, user in NATS_ROLES.items():
        password = _secret(
            secret_root,
            f"catalogue/queue/NATS_{role.upper()}_PASSWORD",
            PASSWORD,
        )
        _write(
            secrets / f"nats-{role}-credentials.json",
            json.dumps({"user": user, "password": password}) + "\n",
        )
    nats_configuration = secrets / "nats-server.conf"
    nats_configuration.unlink(missing_ok=True)
    _load_nats_renderer().render(secrets, nats_configuration)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--values", type=Path, required=True)
    parser.add_argument("--secret-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build(args.values, args.secret_root, args.output)


if __name__ == "__main__":
    main()
