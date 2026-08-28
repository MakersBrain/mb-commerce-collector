from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("catalogue_render", ROOT / "render.py")
assert SPEC and SPEC.loader
render = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(render)


def values() -> dict:
    return json.loads((ROOT / "values.example.json").read_text(encoding="utf-8"))


def test_render_produces_exact_private_rootless_bundle(tmp_path: Path) -> None:
    output = tmp_path / "rendered"
    render.render(ROOT / "values.example.json", output)
    assert "@@" not in (output / "catalogue-control.container").read_text(encoding="utf-8")
    assert "NetworkName=catalogue" in (output / "catalogue.network").read_text(encoding="utf-8")
    assert "PublishPort=" not in "\n".join(
        path.read_text(encoding="utf-8") for path in output.glob("*.container")
    )
    mount = (
        "Volume=/etc/makersbrain/catalogue-secrets/webshare-gateway:"
        "/run/secrets/webshare-gateway:ro"
    )
    for name in ("catalogue-worker@.container", "catalogue-worker-browser.container"):
        worker = (output / name).read_text(encoding="utf-8")
        assert worker.count(mount) == 1
        assert worker.count("UserNS=keep-id:uid=10001,gid=10001") == 1
        assert worker.count("User=10001") == 1
        assert worker.count("Group=10001") == 1
        assert worker.count("DropCapability=all") == 1
        assert "AddCapability=" not in worker
    for path in output.glob("*.container"):
        content = path.read_text(encoding="utf-8")
        if path.name not in {
            "catalogue-control.container",
            "catalogue-worker@.container",
            "catalogue-worker-browser.container",
        }:
            assert "webshare-gateway.json" not in content
        assert "CATALOGUE_PROXY_WEBSHARE_DATA_PLANE_ENABLED" not in content
    control = (output / "catalogue-control.container").read_text(encoding="utf-8")
    assert control.count(
        "Volume=/etc/makersbrain/catalogue-secrets/webshare-gateway:"
        "/run/secrets/webshare-gateway"
    ) == 1
    assert "/run/secrets/webshare-gateway:ro" not in control
    assert control.count("CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE=") == 1


def test_render_rejects_mutable_image(tmp_path: Path) -> None:
    document = values()
    document["images"]["worker"] = "registry.example/catalogue/worker:latest"
    source = tmp_path / "values.json"
    source.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match="not pinned"):
        render.render(source, tmp_path / "rendered")


def test_render_rejects_oversized_worker_count(tmp_path: Path) -> None:
    document = values()
    document["worker_instances"] = [1, 2, 3, 4]
    source = tmp_path / "values.json"
    source.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match="one to three"):
        render.render(source, tmp_path / "rendered")


@pytest.mark.parametrize("flag", ["stock_trends_enabled", "explorer_trends_enabled"])
def test_render_rejects_non_boolean_trend_flags(tmp_path: Path, flag: str) -> None:
    document = values()
    document[flag] = "false"
    source = tmp_path / "values.json"
    source.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match=flag):
        render.render(source, tmp_path / "rendered")


@pytest.mark.parametrize(
    ("enabled", "processes"),
    [
        ("true", []),
        (False, ["worker"]),
        (True, []),
        (True, ["service"]),
        (True, ["worker", "worker"]),
        (True, [{}]),
    ],
)
def test_render_rejects_incoherent_trace_rollout(
    tmp_path: Path,
    enabled: object,
    processes: list[object],
) -> None:
    document = values()
    document["otlp_traces_enabled"] = enabled
    document["otlp_trace_processes"] = processes
    source = tmp_path / "values.json"
    source.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="otlp_trace"):
        render.render(source, tmp_path / "rendered")
