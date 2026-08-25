"""Install the built wheel in an isolated environment and exercise its public API."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import tomllib
import zipfile
from pathlib import Path

BASE_SMOKE_PROGRAM = r"""
import asyncio
from importlib.resources import files
from tempfile import TemporaryDirectory

from mb_commerce_scraper import SourceDefinition
from mb_commerce_scraper.connectors import ConnectorRegistry
from mb_commerce_scraper.runtime import CommerceScraper
from mb_commerce_scraper.testing import FakeTransport
from mb_commerce_scraper.transports import (
    FileResponseCache,
    RequestPriority,
    RequestPurpose,
    ResponseCache,
    TransportRequest,
    TransportResponse,
)


async def main() -> None:
    package = files("mb_commerce_scraper")
    assert package.joinpath("py.typed").is_file()
    schemas = package.joinpath("schemas")
    assert schemas.joinpath("commerce-product-snapshot.schema.json").is_file()
    assert schemas.joinpath("representative-payloads.json").is_file()
    transport = FakeTransport()
    transport.add(
        "https://shop.test/products.json",
        json_body={"products": []},
    )
    scraper = CommerceScraper(
        registry=ConnectorRegistry.with_builtins(),
        transport=transport,
    )
    source = SourceDefinition(
        id="wheel-smoke",
        label="Wheel smoke test",
        base_url="https://shop.test",
        connector="shopify",
        connector_options={"currency": "EUR"},
    )
    pages = [page async for page in scraper.collect(source)]
    assert pages[-1].terminal
    assert len(transport.requests) == 1

    # The filesystem cache is part of the dependency-free public surface. Its
    # directory persists independently of scraper lifecycle and retains exact
    # binary response bytes.
    with TemporaryDirectory() as directory:
        cache: ResponseCache = FileResponseCache(directory)
        request = TransportRequest(
            url="https://shop.test/binary",
            purpose=RequestPurpose.ENTITY,
            priority=RequestPriority.IDENTITY,
        )
        expected = TransportResponse(
            status=200,
            headers={"content-type": "application/octet-stream"},
            content=b"\x00wheel-cache\xff",
            final_url=request.url,
        )
        await cache.put(request, expected)
        retained = await cache.get(request)
        assert retained is not None
        assert retained.content == expected.content
        assert retained.status == expected.status


asyncio.run(main())
"""

HTTP_SMOKE_PROGRAM = r"""
import asyncio
from importlib.metadata import version

from mb_commerce_scraper.runtime import build_http_scraper


async def main() -> None:
    # The SOCKS module is the easy dependency to lose by accidentally changing
    # httpx[socks] to plain httpx in packaging metadata.
    version("httpx")
    version("socksio")
    async with build_http_scraper(allowed_origins=("https://shop.test",)):
        pass


asyncio.run(main())
"""

DEV_SMOKE_PROGRAM = r"""
from importlib.metadata import version

for distribution in (
    "build",
    "httpx",
    "mypy",
    "pytest",
    "pytest-asyncio",
    "ruff",
):
    version(distribution)

import mb_commerce_scraper
"""

GENERIC_EXTRA_SMOKE_PROGRAM = "import mb_commerce_scraper"


def declared_public_api(manifest: Path) -> dict[str, tuple[str, ...]]:
    """Load and validate the reviewed public import manifest."""
    with manifest.open("rb") as handle:
        document = tomllib.load(handle)
    modules = document.get("modules")
    if not isinstance(modules, dict) or not modules:
        raise SystemExit("public-api.toml must define a non-empty modules table")
    public_api: dict[str, tuple[str, ...]] = {}
    for module_name, declaration in modules.items():
        if not isinstance(module_name, str) or not isinstance(declaration, dict):
            raise SystemExit("public API module declarations must be tables")
        symbols = declaration.get("symbols")
        if not isinstance(symbols, list) or not symbols or not all(
            isinstance(symbol, str) and symbol for symbol in symbols
        ):
            raise SystemExit(f"{module_name}.symbols must be a non-empty string array")
        if len(symbols) != len(set(symbols)):
            raise SystemExit(f"{module_name}.symbols must contain no duplicates")
        public_api[module_name] = tuple(symbols)
    return public_api


def public_api_smoke_program(public_api: dict[str, tuple[str, ...]]) -> str:
    """Build a dependency-free program that inspects the installed artifact."""
    encoded = json.dumps(public_api, sort_keys=True)
    return f'''\
import importlib
import json
from importlib.metadata import PackageNotFoundError, version

expected = json.loads({encoded!r})
for module_name, expected_symbols in expected.items():
    module = importlib.import_module(module_name)
    actual_symbols = getattr(module, "__all__", None)
    assert isinstance(actual_symbols, list), f"{{module_name}} must declare __all__"
    assert actual_symbols == expected_symbols, (
        f"{{module_name}} public exports drifted: expected {{expected_symbols!r}}, "
        f"got {{actual_symbols!r}}"
    )
    for symbol in expected_symbols:
        getattr(module, symbol)

package = importlib.import_module("mb_commerce_scraper")
assert package.__version__ == version("mb-commerce-scraper")
try:
    version("httpx")
except PackageNotFoundError:
    pass
else:
    raise AssertionError("the base wheel must not install the optional HTTPX dependency")
'''


def wheel_extras(wheel: Path) -> tuple[str, ...]:
    """Read extras from the artifact under test, not the source pyproject."""
    with zipfile.ZipFile(wheel) as archive:
        metadata_names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise SystemExit(
                f"expected one METADATA file in {wheel.name}, found {len(metadata_names)}"
            )
        metadata = archive.read(metadata_names[0]).decode("utf-8")
    prefix = "Provides-Extra: "
    return tuple(
        sorted(
            line.removeprefix(prefix).strip()
            for line in metadata.splitlines()
            if line.startswith(prefix)
        )
    )


def declared_extras(pyproject: Path) -> tuple[str, ...]:
    with pyproject.open("rb") as handle:
        document = tomllib.load(handle)
    optional = document.get("project", {}).get("optional-dependencies", {})
    if not isinstance(optional, dict):
        raise SystemExit("project.optional-dependencies must be a table")
    return tuple(sorted(optional))


def verify_install(
    uv: str,
    requirement: str,
    program: str,
    *,
    directory: Path,
    environment: dict[str, str],
) -> None:
    subprocess.run(
        [
            uv,
            "run",
            "--isolated",
            "--no-project",
            "--refresh",
            "--with",
            requirement,
            "python",
            "-c",
            program,
        ],
        cwd=directory,
        env=environment,
        check=True,
    )


def main() -> None:
    project = Path(__file__).resolve().parents[1]
    wheels = sorted((project / "dist").glob("mb_commerce_scraper-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(
            f"expected exactly one built mb-commerce-scraper wheel, found {len(wheels)}"
        )
    uv = shutil.which("uv")
    if uv is None:
        raise SystemExit("uv is required for isolated wheel verification")
    environment = os.environ.copy()
    for key in ("PYTHONPATH", "VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT"):
        environment.pop(key, None)
    with tempfile.TemporaryDirectory(prefix="mb-commerce-wheel-") as directory:
        root = Path(directory)
        source_wheel = wheels[0].resolve()
        # uv keys local wheel extraction aggressively. A unique artifact path
        # prevents a rebuilt wheel with the same development version from
        # reusing files extracted from an earlier build.
        wheel = root / source_wheel.name
        shutil.copy2(source_wheel, wheel)
        extras = wheel_extras(wheel)
        expected_extras = declared_extras(project / "pyproject.toml")
        if extras != expected_extras:
            raise SystemExit(
                f"wheel extras {extras!r} do not match pyproject extras {expected_extras!r}"
            )
        base = root / "base"
        base.mkdir()
        public_api = declared_public_api(project / "public-api.toml")
        verify_install(
            uv,
            str(wheel),
            f"{BASE_SMOKE_PROGRAM}\n{public_api_smoke_program(public_api)}",
            directory=base,
            environment=environment,
        )
        smoke_programs = {
            "http": HTTP_SMOKE_PROGRAM,
            "dev": DEV_SMOKE_PROGRAM,
        }
        for extra in extras:
            extra_directory = root / f"extra-{extra}"
            extra_directory.mkdir()
            verify_install(
                uv,
                f"{wheel}[{extra}]",
                smoke_programs.get(extra, GENERIC_EXTRA_SMOKE_PROGRAM),
                directory=extra_directory,
                environment=environment,
            )


if __name__ == "__main__":
    main()
