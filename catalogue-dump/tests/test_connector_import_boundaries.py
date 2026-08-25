from __future__ import annotations

import ast
from pathlib import Path

CONNECTORS = Path(__file__).parents[1] / "src" / "mb_ceramics_catalogue" / "connectors"
TRANSPORTS = Path(__file__).parents[1] / "src" / "mb_ceramics_catalogue" / "transports"
RUNTIME_PLAN = (
    Path(__file__).parents[1]
    / "src"
    / "mb_ceramics_catalogue"
    / "ops"
    / "connector_adapters.py"
)
CATALOGUE_PACKAGE = Path(__file__).parents[1] / "src" / "mb_ceramics_catalogue"
SUPPORTED_SCRAPER_MODULES = frozenset(
    {
        "mb_commerce_scraper",
        "mb_commerce_scraper.connectors",
        "mb_commerce_scraper.discovery",
        "mb_commerce_scraper.models",
        "mb_commerce_scraper.parsing",
        "mb_commerce_scraper.proxy",
        "mb_commerce_scraper.runtime",
        "mb_commerce_scraper.testing",
        "mb_commerce_scraper.transports",
    }
)
DEPRECATED_COMMERCE_SHIM = "mb_ceramics_catalogue.connectors.commerce"
FORBIDDEN_PREFIXES = (
    "mb_ceramics_catalogue.scrapers",
    "mb_ceramics_catalogue.datasets",
    "mb_ceramics_catalogue.ops",
    "mb_ceramics_catalogue.pipeline",
    "playwright",
    "camoufox",
)
ALLOWED_TRANSPORT_MODULES = frozenset({"mb_ceramics_catalogue.transports.browser"})
FORBIDDEN_TRANSPORT_PREFIXES = (
    "mb_ceramics_catalogue.cli",
    "mb_ceramics_catalogue.connectors",
    "mb_ceramics_catalogue.crawl",
    "mb_ceramics_catalogue.datasets",
    "mb_ceramics_catalogue.ops",
    "mb_ceramics_catalogue.scrapers",
)


def test_neutral_connectors_do_not_import_legacy_or_concrete_runtime_layers() -> None:
    violations: list[str] = []
    for path in sorted(CONNECTORS.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            modules: tuple[str, ...] = ()
            if isinstance(node, ast.Import):
                modules = tuple(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules = (node.module,)
            for module in modules:
                concrete_transport = module.startswith("mb_ceramics_catalogue.transports.") and all(
                    module != allowed and not module.startswith(allowed + ".")
                    for allowed in ALLOWED_TRANSPORT_MODULES
                )
                if concrete_transport or any(
                    module == forbidden or module.startswith(forbidden + ".")
                    for forbidden in FORBIDDEN_PREFIXES
                ):
                    violations.append(f"{path.name}:{getattr(node, 'lineno', 0)}: {module}")
    assert violations == []


def test_transports_do_not_import_concrete_higher_layers() -> None:
    violations: list[str] = []
    for path in sorted(TRANSPORTS.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            modules: tuple[str, ...] = ()
            if isinstance(node, ast.Import):
                modules = tuple(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.level == 0:
                    modules = (node.module,)
                else:
                    package = ["mb_ceramics_catalogue", "transports"]
                    keep = max(0, len(package) - (node.level - 1))
                    modules = (".".join((*package[:keep], *node.module.split("."))),)
            for module in modules:
                if any(
                    module == forbidden or module.startswith(forbidden + ".")
                    for forbidden in FORBIDDEN_TRANSPORT_PREFIXES
                ):
                    violations.append(
                        f"{path.name}:{getattr(node, 'lineno', 0)}: {module}"
                    )
    assert violations == []


def test_runtime_plan_remains_data_only() -> None:
    tree = ast.parse(RUNTIME_PLAN.read_text(encoding="utf-8"), filename=str(RUNTIME_PLAN))
    forbidden = {
        "mb_ceramics_catalogue.connectors",
        "mb_ceramics_catalogue.pipeline",
    }
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules = tuple(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            modules = (node.module,)
        else:
            modules = ()
        for module in modules:
            if any(module == prefix or module.startswith(prefix + ".") for prefix in forbidden):
                violations.append(f"{getattr(node, 'lineno', 0)}: {module}")

    plan = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "ConnectorRuntimePlan"
    )
    declared_fields = {
        node.target.id
        for node in plan.body
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
    }
    assert violations == []
    assert declared_fields.isdisjoint(
        {"build", "connector_version", "partitions", "legacy_scraper_adapter"}
    )


def test_catalogue_uses_only_supported_scraper_import_namespaces() -> None:
    violations: list[str] = []
    for path in sorted(CATALOGUE_PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = tuple(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules = (node.module,)
            else:
                modules = ()
            for module in modules:
                if module.startswith("mb_commerce_scraper") and module not in SUPPORTED_SCRAPER_MODULES:
                    violations.append(
                        f"{path.relative_to(CATALOGUE_PACKAGE)}:"
                        f"{getattr(node, 'lineno', 0)}: {module}"
                    )

    assert violations == []


def test_production_modules_do_not_import_deprecated_commerce_shim() -> None:
    violations: list[str] = []
    for path in sorted(CATALOGUE_PACKAGE.rglob("*.py")):
        relative = path.relative_to(CATALOGUE_PACKAGE)
        if relative == Path("connectors/commerce.py"):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and (
                    (node.level == 0 and node.module == DEPRECATED_COMMERCE_SHIM)
                    or (
                        relative.parts[0] == "connectors"
                        and node.level == 1
                        and node.module == "commerce"
                    )
                )
            ):
                module = node.module or DEPRECATED_COMMERCE_SHIM
                violations.append(f"{relative}:{node.lineno}: {module}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == DEPRECATED_COMMERCE_SHIM:
                        violations.append(f"{relative}:{node.lineno}: {alias.name}")

    assert violations == []
