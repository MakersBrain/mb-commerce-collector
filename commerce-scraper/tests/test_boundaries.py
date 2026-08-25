import ast
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

PACKAGE = Path(__file__).parents[1] / "src" / "mb_commerce_scraper"
PACKAGE_NAME = "mb_commerce_scraper"


def _imports(path: Path) -> Iterator[tuple[int, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    relative = path.relative_to(PACKAGE).with_suffix("")
    package = (PACKAGE_NAME, *relative.parts[:-1])
    if relative.name == "__init__":
        package = (PACKAGE_NAME, *relative.parts[:-1])
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield node.lineno, alias.name
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                if node.module:
                    yield node.lineno, node.module
                continue
            retained = max(1, len(package) - (node.level - 1))
            prefix = package[:retained]
            suffix = tuple((node.module or "").split(".")) if node.module else ()
            yield node.lineno, ".".join((*prefix, *suffix))


def _internal_layer(module: str) -> str | None:
    parts = module.split(".")
    if not parts or parts[0] != PACKAGE_NAME or len(parts) < 2:
        return None
    return parts[1]


def test_library_never_imports_catalogue_application() -> None:
    violations: list[str] = []
    for path in PACKAGE.rglob("*.py"):
        for line, module in _imports(path):
            if module.startswith("mb_ceramics_catalogue"):
                violations.append(f"{path.relative_to(PACKAGE)}:{line}: {module}")
    assert violations == []


def test_internal_layers_follow_the_documented_dependency_direction() -> None:
    forbidden = {
        "models": {"connectors", "discovery", "parsing", "proxy", "runtime", "testing", "transports"},
        "connectors": {"proxy", "runtime", "testing"},
        "discovery": {"connectors", "parsing", "proxy", "runtime", "testing"},
        "parsing": {"connectors", "discovery", "proxy", "runtime", "testing", "transports"},
        "transports": {"connectors", "discovery", "parsing", "proxy", "runtime", "testing"},
        "proxy": {"connectors", "discovery", "parsing", "runtime", "testing"},
        "runtime": {"testing"},
    }
    violations: list[str] = []
    for path in PACKAGE.rglob("*.py"):
        relative = path.relative_to(PACKAGE)
        source_layer = relative.parts[0] if len(relative.parts) > 1 else None
        if source_layer not in forbidden:
            continue
        for line, module in _imports(path):
            target_layer = _internal_layer(module)
            if target_layer in forbidden[source_layer]:
                violations.append(
                    f"{relative}:{line}: {source_layer} -> {target_layer} ({module})"
                )
            if (
                source_layer == "connectors"
                and module.startswith(f"{PACKAGE_NAME}.transports.")
                and module != f"{PACKAGE_NAME}.transports.base"
            ):
                violations.append(
                    f"{relative}:{line}: connector imports concrete transport {module}"
                )
    assert violations == []


def test_models_use_only_standard_library_pydantic_and_models() -> None:
    violations: list[str] = []
    for path in (PACKAGE / "models").rglob("*.py"):
        for line, module in _imports(path):
            root = module.split(".", 1)[0]
            if root in sys.stdlib_module_names or root == "pydantic":
                continue
            if module == PACKAGE_NAME or module.startswith(f"{PACKAGE_NAME}.models"):
                continue
            violations.append(f"{path.relative_to(PACKAGE)}:{line}: {module}")
    assert violations == []


def test_page_engine_does_not_depend_on_vendor_connectors() -> None:
    forbidden = {
        f"{PACKAGE_NAME}.connectors.generic_pages",
        f"{PACKAGE_NAME}.connectors.specialized",
    }
    violations = [
        f"{line}: {module}"
        for line, module in _imports(PACKAGE / "connectors" / "page_engine.py")
        if module in forbidden
    ]
    generic_imports = {
        module
        for _, module in _imports(PACKAGE / "connectors" / "generic_pages.py")
    }
    if f"{PACKAGE_NAME}.connectors.specialized" in generic_imports:
        violations.append("generic_pages imports the vendor connector module")
    assert violations == []


def test_core_import_does_not_load_optional_or_application_dependencies() -> None:
    output = subprocess.check_output(
        [
            sys.executable,
            "-c",
            "import sys, mb_commerce_scraper; "
            "print('httpx' in sys.modules, 'mb_ceramics_catalogue' in sys.modules)",
        ],
        text=True,
    )
    assert output.strip() == "False False"
