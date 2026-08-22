"""Build both wheels and verify catalogue composition from installed artifacts."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def _wheel(directory: Path, pattern: str) -> Path:
    wheels = tuple(directory.glob(pattern))
    if len(wheels) != 1:
        raise SystemExit(
            f"expected exactly one {pattern!r} artifact, found {len(wheels)}"
        )
    return wheels[0]


def _smoke_program() -> str:
    return """
import sys
from importlib.metadata import distribution
from pathlib import Path

from mb_commerce_scraper import CollectionRequest, SnapshotField
from mb_ceramics_catalogue.config.sources import SourcesFile, default_path
from mb_ceramics_catalogue.ops.commerce_scraper_adapter import source_definition
from mb_ceramics_catalogue.ops.commerce_scraper_runtime import (
    application_connector_registry,
    build_library_pipeline_connector,
)
from mb_ceramics_catalogue.ops.connector_adapters import (
    library_canary_route,
    runtime_plan,
)

source_root = Path(sys.argv[1]).resolve()
environment_root = Path(sys.argv[2]).resolve()
source_directories = (
    source_root / "commerce-scraper" / "src",
    source_root / "catalogue-dump" / "src",
)
assert sys.prefix != sys.base_prefix
for entry in sys.path:
    if not entry:
        continue
    resolved = Path(entry).resolve()
    assert all(
        resolved != source and not resolved.is_relative_to(source)
        for source in source_directories
    ), resolved

catalogue_package = None
for distribution_name, module_name in (
    ("mb-commerce-scraper", "mb_commerce_scraper"),
    ("mb-ceramics-catalogue", "mb_ceramics_catalogue"),
):
    installed = distribution(distribution_name)
    module = __import__(module_name)
    origin = Path(module.__file__).resolve()
    installed_root = Path(installed.locate_file("")).resolve()
    assert origin.is_relative_to(environment_root), (distribution_name, origin)
    assert installed_root.is_relative_to(environment_root), (
        distribution_name,
        installed_root,
    )
    assert not origin.is_relative_to(source_root), (distribution_name, origin)
    assert installed.version
    if module_name == "mb_ceramics_catalogue":
        catalogue_package = origin.parent

sources_path = default_path().resolve()
assert catalogue_package is not None
assert sources_path.is_relative_to(catalogue_package), sources_path
assert not sources_path.is_relative_to(source_root), sources_path
assert sources_path.name == "sources.json" and sources_path.is_file()
sources = SourcesFile.load(sources_path)
source_id = "ceradel"
config = sources[source_id]
plan = runtime_plan(config)
definition = source_definition(source_id, config, connector_plan=plan)
route = library_canary_route(plan, definition.connector)
assert route is not None
request = CollectionRequest(
    source_id=source_id,
    base_url=config.url,
    requested_fields=frozenset({SnapshotField.IDENTITY}),
    partitions=route.request_partitions,
)
connector = build_library_pipeline_connector(
    registry=application_connector_registry(),
    source=definition,
    request=request,
    checkpoint=None,
    fetcher=object(),
    cancelled=lambda: False,
    collection_id="installed-wheel-smoke",
)
assert connector.name == definition.connector == "shopify"
assert connector.platform == "shopify"
assert connector.version == "1"
print(
    f"installed catalogue composition passed: {source_id} -> "
    f"{connector.name}@{connector.version} from {sources_path}"
)
"""


def main() -> None:
    source_root = Path(__file__).resolve().parents[2]
    projects = (source_root / "commerce-scraper", source_root / "catalogue-dump")
    uv = shutil.which("uv")
    if uv is None:
        raise SystemExit("uv is required for installed composition verification")

    environment = os.environ.copy()
    for key in ("PYTHONPATH", "VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT"):
        environment.pop(key, None)

    with tempfile.TemporaryDirectory(prefix="catalogue-installed-") as directory:
        root = Path(directory)
        artifacts = root / "artifacts"
        artifacts.mkdir()
        for project in projects:
            subprocess.run(
                [
                    uv,
                    "build",
                    "--wheel",
                    "--no-sources",
                    "--out-dir",
                    str(artifacts),
                    str(project),
                ],
                cwd=root,
                env=environment,
                check=True,
            )

        scraper_wheel = _wheel(artifacts, "mb_commerce_scraper-*.whl")
        catalogue_wheel = _wheel(artifacts, "mb_ceramics_catalogue-*.whl")
        environment_root = root / "environment"
        subprocess.run(
            [
                uv,
                "venv",
                "--no-project",
                "--python",
                sys.executable,
                str(environment_root),
            ],
            cwd=root,
            env=environment,
            check=True,
        )
        environment_python = environment_root / "bin" / "python"
        subprocess.run(
            [
                uv,
                "--no-config",
                "pip",
                "install",
                "--python",
                str(environment_python),
                "--strict",
                str(scraper_wheel),
                str(catalogue_wheel),
            ],
            cwd=root,
            env=environment,
            check=True,
        )
        run_directory = root / "run"
        run_directory.mkdir()
        subprocess.run(
            [
                str(environment_python),
                "-I",
                "-c",
                _smoke_program(),
                str(source_root),
                str(environment_root),
            ],
            cwd=run_directory,
            env=environment,
            check=True,
        )


if __name__ == "__main__":
    main()
