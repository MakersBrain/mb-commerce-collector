from collections.abc import AsyncIterator
from typing import Protocol


class DiscoveryStrategy(Protocol):
    """Explicitly composed, versioned product-URL discovery strategy."""

    name: str
    version: str

    def discover(self, base_url: str) -> AsyncIterator[str]: ...
