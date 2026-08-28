#!/usr/bin/env python3
"""Render the immutable rootless Catalogue Quadlet bundle."""

from __future__ import annotations

import argparse
import json
import re
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
IMAGE = re.compile(r"^[^\s]+@sha256:[a-f0-9]{64}$")
MAKERSBRAIN_RELEASE = re.compile(r"^control-[0-9]{4}\.[0-9]{2}\.[0-9]{2}-[a-f0-9]{16,64}$")
IMAGE_NAMES = {
    "control", "service", "worker", "worker_browser", "explorer", "nats", "database_transfer"
}
HOST = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$")
DATABASE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")
OTLP_TRACE_PROCESSES = {"worker", "worker-browser"}


def load_values(path: Path) -> dict:
    values = json.loads(path.read_text(encoding="utf-8"))
    if values.get("environment") not in {"staging", "production"}:
        raise ValueError("environment must be staging or production")
    if not MAKERSBRAIN_RELEASE.fullmatch(values.get("compatible_makersbrain_release", "")):
        raise ValueError("compatible_makersbrain_release is invalid")
    images = values.get("images", {})
    if set(images) != IMAGE_NAMES:
        raise ValueError(f"images must contain exactly: {', '.join(sorted(IMAGE_NAMES))}")
    for name, image in images.items():
        if not IMAGE.fullmatch(image):
            raise ValueError(f"image {name} is not pinned by digest")
    if not HOST.fullmatch(values.get("postgres_host", "")):
        raise ValueError("postgres_host is invalid")
    if not isinstance(values.get("postgres_port"), int) or not 1 <= values["postgres_port"] <= 65535:
        raise ValueError("postgres_port is invalid")
    if not DATABASE.fullmatch(values.get("postgres_database", "")):
        raise ValueError("postgres_database is invalid")
    workers = values.get("worker_instances")
    if (
        not isinstance(workers, list)
        or not workers
        or len(workers) > 3
        or any(not isinstance(item, int) or isinstance(item, bool) or item < 1 or item > 9 for item in workers)
        or len(workers) != len(set(workers))
    ):
        raise ValueError("worker_instances must contain one to three unique integers from 1 to 9")
    traces_enabled = values.get("otlp_traces_enabled")
    if not isinstance(traces_enabled, bool):
        raise ValueError("otlp_traces_enabled must be a boolean")
    trace_processes = values.get("otlp_trace_processes")
    if (
        not isinstance(trace_processes, list)
        or any(
            not isinstance(process, str) or process not in OTLP_TRACE_PROCESSES
            for process in trace_processes
        )
        or len(trace_processes) != len(set(trace_processes))
    ):
        raise ValueError("otlp_trace_processes must be a unique subset of worker, worker-browser")
    if traces_enabled != bool(trace_processes):
        raise ValueError(
            "otlp_traces_enabled requires a non-empty otlp_trace_processes list; "
            "disabled traces require an empty list"
        )
    return values


def render(values_path: Path, output: Path) -> None:
    values = load_values(values_path)
    if output.exists() and any(output.iterdir()):
        raise ValueError("output directory must be absent or empty")
    output.mkdir(parents=True, exist_ok=True)
    tokens = {f"{name.upper()}_IMAGE": image for name, image in values["images"].items()}
    for source in sorted(path for path in (HERE / "quadlets").iterdir() if path.is_file()):
        content = source.read_text(encoding="utf-8")
        for name, value in tokens.items():
            content = content.replace(f"@@{name}@@", value)
        if "@@" in content:
            raise ValueError(f"unresolved template value in {source.name}")
        target = output / source.name
        target.write_text(content, encoding="utf-8")
        target.chmod(0o644)
    shutil.copy2(values_path, output / "rendered-values.json")
    (output / "rendered-values.json").chmod(0o600)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--values", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    render(args.values, args.output)


if __name__ == "__main__":
    main()
