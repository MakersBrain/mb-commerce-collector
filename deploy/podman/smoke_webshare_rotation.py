#!/usr/bin/env python3
"""Exercise the real worker entrypoint against an atomically rotated RO store.

This is a Docker rootless-identity emulation for hosts without Podman/Quadlet.
It runs the worker image as uid/gid 10001 with all capabilities dropped and a
read-only root filesystem. It does not claim to activate a real Quadlet.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
ENTRYPOINT = ROOT / "docker/worker/entrypoint.py"
PACKAGE_SOURCE = ROOT / "catalogue-dump/src"
GATEWAY_DIRECTORY = "/run/secrets/webshare-gateway"
GATEWAY_FILE = f"{GATEWAY_DIRECTORY}/webshare-gateway.json"


def _run(arguments: list[str], *, timeout: float = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        arguments,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _rotate(image: str, volume: str, generation: int) -> None:
    expected = "None" if generation == 1 else str(generation - 1)
    code = f"""
from pathlib import Path
from pydantic import SecretStr
from mb_ceramics_catalogue.webshare_gateway_secrets import (
    WebshareGatewaySecret, WebshareGatewaySecretStore,
)
profile = WebshareGatewaySecret(
    provider="webshare", logical_name="smoke", generation={generation},
    endpoint_id="webshare-residential-backbone", protocol="http",
    host="p.webshare.io", port=80, username=SecretStr("smoke-user"),
    password=SecretStr("opaque-generation-{generation}"),
    countries=frozenset({{"FR"}}), sticky_session_ttl_seconds=3600,
)
WebshareGatewaySecretStore(Path("{GATEWAY_FILE}")).install(
    profile, expected_generation={expected}
)
"""
    _run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            "10001:10001",
            "--network",
            "none",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges:true",
            "--tmpfs",
            "/tmp:rw,noexec,nosuid,nodev",
            "--volume",
            f"{volume}:{GATEWAY_DIRECTORY}",
            "--volume",
            f"{PACKAGE_SOURCE}:/workspace/catalogue-dump/src:ro",
            "--env",
            "PYTHONPATH=/workspace/catalogue-dump/src",
            "--entrypoint",
            "python3",
            image,
            "-c",
            code,
        ]
    )


def _records(container: str) -> list[dict[str, object]]:
    logs = _run(["docker", "logs", container])
    output = logs.stdout + logs.stderr
    return [json.loads(line) for line in output.splitlines() if line.startswith("{")]


def smoke(image: str) -> dict[str, object]:
    if shutil.which("docker") is None:
        raise RuntimeError("Docker is unavailable; real Quadlet smoke requires Podman")
    _run(["docker", "image", "inspect", image])
    suffix = uuid4().hex[:12]
    volume = f"catalogue-webshare-smoke-{suffix}"
    container = f"catalogue-webshare-worker-smoke-{suffix}"
    _run(["docker", "volume", "create", volume])
    try:
        _run(
            [
                "docker",
                "run",
                "--rm",
                "--user",
                "0:0",
                "--volume",
                f"{volume}:{GATEWAY_DIRECTORY}",
                "--entrypoint",
                "sh",
                image,
                "-c",
                f"chown 10001:10001 {GATEWAY_DIRECTORY} && chmod 0700 {GATEWAY_DIRECTORY}",
            ]
        )
        _rotate(image, volume, 1)

        with tempfile.TemporaryDirectory(prefix="catalogue-webshare-smoke-") as temporary:
            Path(temporary).chmod(0o755)
            fake = Path(temporary) / "catalogue-worker"
            fake.write_text(
                """#!/usr/bin/env python3
import json, os, stat, time
path = os.environ["CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE"]
def snapshot(label):
    with open(path, encoding="utf-8") as source:
        document = json.load(source)
    record = document["profiles"]["webshare/smoke"]
    print(json.dumps({
        "label": label, "generation": record["generation"],
        "uid": os.geteuid(), "gid": os.getegid(),
        "mode": oct(stat.S_IMODE(os.stat(path).st_mode)),
        "gateway_path": path,
        "data_plane": os.environ.get("CATALOGUE_PROXY_WEBSHARE_DATA_PLANE_ENABLED"),
        "provider_api_secret": os.environ.get("CATALOGUE_PROXY_API_SECRET_FILE"),
    }), flush=True)
    return record["generation"]
initial = snapshot("initial")
deadline = time.monotonic() + 20
while time.monotonic() < deadline:
    with open(path, encoding="utf-8") as source:
        generation = json.load(source)["profiles"]["webshare/smoke"]["generation"]
    if generation != initial:
        snapshot("rotated")
        raise SystemExit(0)
    time.sleep(0.05)
raise SystemExit("atomic rotation was not visible through the read-only mount")
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            _run(
                [
                    "docker",
                    "run",
                    "--name",
                    container,
                    "--detach",
                    "--user",
                    "10001:10001",
                    "--network",
                    "none",
                    "--read-only",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges:true",
                    "--tmpfs",
                    "/tmp:rw,noexec,nosuid,nodev",
                    "--volume",
                    f"{volume}:{GATEWAY_DIRECTORY}:ro",
                    "--volume",
                    f"{ENTRYPOINT}:/smoke/entrypoint.py:ro",
                    "--volume",
                    f"{temporary}:/smoke-bin:ro",
                    "--env",
                    "PATH=/smoke-bin:/usr/local/bin:/usr/bin:/bin",
                    "--entrypoint",
                    "python3",
                    image,
                    "/smoke/entrypoint.py",
                ]
            )
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and not _records(container):
                time.sleep(0.05)
            initial = _records(container)
            if not initial or initial[0].get("generation") != 1:
                inspection = _run(
                    ["docker", "inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", container]
                ).stdout.strip()
                log_result = _run(["docker", "logs", container])
                logs = log_result.stdout + log_result.stderr
                raise RuntimeError(
                    f"worker did not read bootstrap generation 1 ({inspection}): {logs}"
                )
            _rotate(image, volume, 2)
            result = _run(["docker", "wait", container], timeout=30).stdout.strip()
            if result != "0":
                log_result = _run(["docker", "logs", container])
                logs = log_result.stdout + log_result.stderr
                raise RuntimeError(f"worker smoke exited {result}: {logs}")
            records = _records(container)

        if [record.get("generation") for record in records] != [1, 2]:
            raise RuntimeError(f"unexpected generation observations: {records}")
        for record in records:
            if record.get("uid") != 10001 or record.get("gid") != 10001:
                raise RuntimeError(f"worker identity drifted: {record}")
            if record.get("mode") != "0o400":
                raise RuntimeError(f"gateway mode is not control-private: {record}")
            if record.get("gateway_path") != GATEWAY_FILE:
                raise RuntimeError(f"entrypoint exported the wrong path: {record}")
            if record.get("data_plane") is not None:
                raise RuntimeError(f"gateway presence enabled paid traffic: {record}")
            if record.get("provider_api_secret") is not None:
                raise RuntimeError(f"worker retained a provider API credential: {record}")
        return {
            "runtime": "Docker rootless-identity emulation (not Quadlet activation)",
            "image": image,
            "generations": [1, 2],
            "uid_gid": "10001:10001",
            "mode": "0400",
            "read_only_worker_mount": True,
            "data_plane_default_off": True,
            "privileged_entrypoint_calls": False,
        }
    finally:
        subprocess.run(
            ["docker", "rm", "--force", container],
            check=False,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            ["docker", "volume", "rm", "--force", volume],
            check=False,
            capture_output=True,
            text=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="catalogue-ceramics-worker:latest")
    arguments = parser.parse_args()
    print(json.dumps(smoke(arguments.image), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
