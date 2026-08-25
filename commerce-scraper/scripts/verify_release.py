"""Verify version and artifact invariants before publishing a scraper release."""

from __future__ import annotations

import argparse
import ast
import re
import tomllib
import zipfile
from email.parser import Parser
from pathlib import Path

TAG_PREFIX = "commerce-scraper-v"
VERSION_PATTERN = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:[a-zA-Z0-9.-]+)?")


def project_version(project: Path) -> str:
    with (project / "pyproject.toml").open("rb") as handle:
        document = tomllib.load(handle)
    value = document.get("project", {}).get("version")
    if not isinstance(value, str) or VERSION_PATTERN.fullmatch(value) is None:
        raise SystemExit("pyproject.toml must declare a normalized release version")
    return value


def package_version(project: Path) -> str:
    module = project / "src" / "mb_commerce_scraper" / "__init__.py"
    tree = ast.parse(module.read_text(encoding="utf-8"), filename=str(module))
    for statement in tree.body:
        if (
            isinstance(statement, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "__version__" for target in statement.targets)
            and isinstance(statement.value, ast.Constant)
            and isinstance(statement.value.value, str)
        ):
            return statement.value.value
    raise SystemExit("mb_commerce_scraper.__version__ must be a string literal")


def artifact_version(project: Path) -> str:
    wheels = sorted((project / "dist").glob("mb_commerce_scraper-*.whl"))
    sdists = sorted((project / "dist").glob("mb_commerce_scraper-*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise SystemExit(
            "release verification requires exactly one mb-commerce-scraper wheel and sdist"
        )
    with zipfile.ZipFile(wheels[0]) as archive:
        metadata_paths = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_paths) != 1:
            raise SystemExit("release wheel must contain exactly one METADATA document")
        metadata = Parser().parsestr(archive.read(metadata_paths[0]).decode("utf-8"))
    version = metadata.get("Version")
    if not isinstance(version, str):
        raise SystemExit("release wheel metadata has no Version")
    return version


def verify_changelog(project: Path, version: str, *, tagged: bool) -> None:
    changelog = (project / "CHANGELOG.md").read_text(encoding="utf-8")
    if "## [Unreleased]" not in changelog:
        raise SystemExit("CHANGELOG.md must retain an Unreleased section")
    if tagged:
        entry = re.compile(
            rf"^## \[{re.escape(version)}\] - [0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$",
            re.MULTILINE,
        )
        if entry.search(changelog) is None:
            raise SystemExit(
                f"CHANGELOG.md needs a dated '## [{version}] - YYYY-MM-DD' release entry"
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", help=f"release tag ({TAG_PREFIX}X.Y.Z)")
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    version = project_version(project)
    versions = {
        "package": package_version(project),
        "wheel": artifact_version(project),
    }
    mismatched = {name: value for name, value in versions.items() if value != version}
    if mismatched:
        raise SystemExit(f"release versions do not match {version}: {mismatched}")
    if args.tag is not None and args.tag != f"{TAG_PREFIX}{version}":
        raise SystemExit(
            f"release tag must be {TAG_PREFIX}{version}, got {args.tag!r}"
        )
    verify_changelog(project, version, tagged=args.tag is not None)


if __name__ == "__main__":
    main()
