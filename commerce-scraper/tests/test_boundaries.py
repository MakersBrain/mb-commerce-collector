import ast
import subprocess
import sys
from pathlib import Path

PACKAGE = Path(__file__).parents[1] / "src" / "mb_commerce_scraper"


def test_library_never_imports_catalogue_application() -> None:
    violations = []
    for path in PACKAGE.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("mb_ceramics_catalogue"):
                violations.append(str(path))
            if isinstance(node, ast.Import) and any(alias.name.startswith("mb_ceramics_catalogue") for alias in node.names):
                violations.append(str(path))
    assert violations == []


def test_core_import_does_not_load_optional_or_application_dependencies() -> None:
    output = subprocess.check_output([sys.executable, "-c", "import sys, mb_commerce_scraper; print('httpx' in sys.modules, 'mb_ceramics_catalogue' in sys.modules)"], text=True)
    assert output.strip() == "False False"

