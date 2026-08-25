"""Run the live Camoufox callback gate without requiring pytest in the image."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from collections.abc import Callable, Coroutine
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast


def _load_gate() -> Callable[[], Coroutine[Any, Any, None]]:
    # The browser worker deliberately excludes development-only pytest. The
    # integration module only needs its inert marker during import; all setup,
    # assertions, and cleanup remain in the same function pytest executes.
    pytest_stub = ModuleType("pytest")
    pytest_stub.__dict__["mark"] = SimpleNamespace(
        camoufox=lambda function: function
    )
    sys.modules.setdefault("pytest", pytest_stub)

    test_path = (
        Path(__file__).resolve().parents[1]
        / "tests"
        / "test_camoufox_live_callback.py"
    )
    spec = importlib.util.spec_from_file_location("live_camoufox_callback", test_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the live Camoufox callback gate")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return cast(
        Callable[[], Coroutine[Any, Any, None]],
        module.__dict__["run_live_camoufox_callback_gate"],
    )


def main() -> None:
    asyncio.run(_load_gate()())
    print("live Camoufox callback gate passed")


if __name__ == "__main__":
    main()
