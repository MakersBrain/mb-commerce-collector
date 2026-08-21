from __future__ import annotations

import json
from pathlib import Path

from .fake_transport import FakeTransport


def load_recording(path: Path) -> FakeTransport:
    """Load a bounded, secret-free JSON response map for deterministic replay."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    transport = FakeTransport()
    for entry in payload:
        transport.add(entry["url"], status=entry.get("status", 200), body=entry.get("body", ""), headers=entry.get("headers", {}))
    return transport

