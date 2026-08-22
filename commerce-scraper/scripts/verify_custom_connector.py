"""Run the clean external consumer against the wheel and example plugin."""

from __future__ import annotations

import os
import secrets
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> None:
    project = Path(__file__).resolve().parents[1]
    example = project / "examples" / "custom_connector"
    consumer = project / "examples" / "external_consumer" / "app.py"
    wheels = sorted((project / "dist").glob("mb_commerce_scraper-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected exactly one built mb-commerce-scraper wheel, found {len(wheels)}")
    uv = shutil.which("uv")
    if uv is None:
        raise SystemExit("uv is required for isolated example verification")
    environment = os.environ.copy()
    for key in ("PYTHONPATH", "VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT"):
        environment.pop(key, None)
    username = f"consumer-{secrets.token_urlsafe(12)}"
    password = secrets.token_urlsafe(32)
    environment["EXAMPLE_PROXY_USERNAME"] = username
    environment["EXAMPLE_PROXY_PASSWORD"] = password

    with tempfile.TemporaryDirectory(prefix="mb-commerce-plugin-") as directory:
        root = Path(directory)
        source_wheel = wheels[0].resolve()
        wheel = root / source_wheel.name
        shutil.copy2(source_wheel, wheel)
        application = root / "app.py"
        shutil.copy2(consumer, application)
        completed = subprocess.run(
            [
                uv,
                "run",
                "--isolated",
                "--no-project",
                "--refresh",
                "--with",
                str(wheel),
                "--with",
                str(example),
                "python",
                str(application),
            ],
            cwd=root,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        output = completed.stdout + completed.stderr
        if username in output or password in output:
            raise SystemExit("external consumer leaked proxy credentials to process output")
        if completed.returncode != 0:
            sys.stderr.write(output)
            raise SystemExit(f"external consumer failed with exit code {completed.returncode}")


if __name__ == "__main__":
    main()
