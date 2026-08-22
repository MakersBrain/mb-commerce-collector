#!/usr/bin/env python3
"""Validate the rendered Catalogue runtime isolation contract."""

from __future__ import annotations

import argparse
from pathlib import Path

EXPECTED = {
    "catalogue.network",
    "catalogue-cache.volume",
    "catalogue-dumps.volume",
    "catalogue-nats.volume",
    "catalogue-nats.container",
    "catalogue-service.container",
    "catalogue-control.container",
    "catalogue-dispatcher.container",
    "catalogue-worker@.container",
    "catalogue-worker-browser.container",
    "catalogue-explorer.container",
    "rendered-values.json",
}
WEBSHARE_GATEWAY_MOUNT = (
    "Volume=/etc/makersbrain/catalogue-secrets/webshare-gateway.json:"
    "/run/secrets/webshare-gateway.json:ro"
)
WEBSHARE_WORKERS = {
    "catalogue-worker@.container",
    "catalogue-worker-browser.container",
}
ROOTLESS_WORKER_FIELDS = (
    "UserNS=keep-id:uid=10001,gid=10001",
    "User=10001",
    "Group=10001",
    "DropCapability=all",
)


def validate(root: Path) -> None:
    found = {path.name for path in root.iterdir() if path.is_file()}
    if found != EXPECTED:
        raise ValueError(f"rendered bundle differs from exact contract: {sorted(found ^ EXPECTED)}")
    for path in root.glob("*.container"):
        content = path.read_text(encoding="utf-8")
        if "@@" in content or "Image=localhost/" in content or ":latest" in content:
            raise ValueError(f"{path.name} contains a mutable or unresolved image")
        if "Network=catalogue.network" not in content:
            raise ValueError(f"{path.name} escaped the private Catalogue network")
        if "PublishPort=" in content:
            raise ValueError(f"{path.name} publishes a host port")
        if "WantedBy=default.target" not in content:
            raise ValueError(f"{path.name} is not a rootless user unit")
        if "AddCapability=" in content:
            raise ValueError(f"{path.name} adds a runtime capability")
        if "CATALOGUE_PROXY_WEBSHARE_DATA_PLANE_ENABLED" in content:
            raise ValueError("rendered units must not enable the Webshare data plane")
        if path.name in WEBSHARE_WORKERS:
            for field in ROOTLESS_WORKER_FIELDS:
                if content.count(field) != 1:
                    raise ValueError(f"{path.name} lacks its exact rootless identity contract")
            if content.count(WEBSHARE_GATEWAY_MOUNT) != 1:
                raise ValueError(f"{path.name} lacks its exact Webshare gateway mount")
        elif "webshare-gateway.json" in content:
            raise ValueError(f"{path.name} received the worker-only Webshare gateway secret")

    nats = (root / "catalogue-nats.container").read_text(encoding="utf-8")
    if (
        "/catalogue-secrets/nats-server.conf:/etc/nats/nats-server.conf:ro" not in nats
        or "EnvironmentFile=" in nats
    ):
        raise ValueError("NATS does not use only its scoped server configuration")

    roles = {
        "catalogue-control.container": "stats",
        "catalogue-dispatcher.container": "publish",
        "catalogue-worker@.container": "consume",
        "catalogue-worker-browser.container": "consume",
    }
    for filename, role in roles.items():
        content = (root / filename).read_text(encoding="utf-8")
        expected = f"nats-{role}-credentials.json"
        if expected not in content:
            raise ValueError(f"{filename} lacks its {role} credential")
        for forbidden in {"publish", "consume", "stats", "admin"} - {role}:
            if f"nats-{forbidden}-credentials.json" in content:
                raise ValueError(f"{filename} received the {forbidden} credential")
    all_units = "\n".join(path.read_text(encoding="utf-8") for path in root.glob("*.container"))
    if "nats-admin-credentials.json" in all_units or "podman.sock" in all_units:
        raise ValueError("runtime units received an administrative capability")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    validate(args.root)


if __name__ == "__main__":
    main()
