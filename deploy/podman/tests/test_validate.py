from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def module(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"{name}.py")
    assert spec and spec.loader
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


render = module("render")
validate = module("validate")


def test_validate_accepts_rendered_bundle(tmp_path: Path) -> None:
    render.render(ROOT / "values.example.json", tmp_path)
    validate.validate(tmp_path)


def test_validate_rejects_admin_credential_in_runtime(tmp_path: Path) -> None:
    render.render(ROOT / "values.example.json", tmp_path)
    control = tmp_path / "catalogue-control.container"
    control.write_text(
        control.read_text(encoding="utf-8")
        + "\nVolume=/x/nats-admin-credentials.json:/run/admin.json:ro\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"admin credential|administrative capability"):
        validate.validate(tmp_path)


def test_validate_rejects_webshare_gateway_secret_outside_workers(
    tmp_path: Path,
) -> None:
    render.render(ROOT / "values.example.json", tmp_path)
    control = tmp_path / "catalogue-control.container"
    control.write_text(
        control.read_text(encoding="utf-8") + "\n" + validate.WEBSHARE_GATEWAY_MOUNT + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="worker-only"):
        validate.validate(tmp_path)


def test_validate_rejects_missing_or_enabling_worker_contract(
    tmp_path: Path,
) -> None:
    render.render(ROOT / "values.example.json", tmp_path)
    worker = tmp_path / "catalogue-worker@.container"
    worker.write_text(
        worker.read_text(encoding="utf-8").replace(
            validate.WEBSHARE_GATEWAY_MOUNT,
            "Environment=CATALOGUE_PROXY_WEBSHARE_DATA_PLANE_ENABLED=true",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must not enable"):
        validate.validate(tmp_path)


def test_validate_rejects_missing_worker_webshare_mount(tmp_path: Path) -> None:
    render.render(ROOT / "values.example.json", tmp_path)
    worker = tmp_path / "catalogue-worker-browser.container"
    worker.write_text(
        worker.read_text(encoding="utf-8").replace(
            validate.WEBSHARE_GATEWAY_MOUNT + "\n",
            "",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="lacks its exact Webshare gateway mount"):
        validate.validate(tmp_path)


@pytest.mark.parametrize(
    ("target", "replacement"),
    [
        ("UserNS=keep-id:uid=10001,gid=10001\n", ""),
        ("UserNS=keep-id:uid=10001,gid=10001\n", "UserNS=host\n"),
        ("User=10001\n", ""),
        ("Group=10001\n", ""),
        (None, "AddCapability=CHOWN\n"),
    ],
)
def test_validate_rejects_weakened_worker_rootless_identity(
    tmp_path: Path,
    target: str | None,
    replacement: str,
) -> None:
    render.render(ROOT / "values.example.json", tmp_path)
    worker = tmp_path / "catalogue-worker@.container"
    content = worker.read_text(encoding="utf-8")
    if target is None:
        content += replacement
    else:
        content = content.replace(target, replacement)
    worker.write_text(content, encoding="utf-8")

    with pytest.raises(ValueError, match=r"rootless identity|runtime capability"):
        validate.validate(tmp_path)
