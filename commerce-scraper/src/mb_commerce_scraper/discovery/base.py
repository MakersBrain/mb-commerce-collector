from collections.abc import AsyncIterator
from typing import Protocol


class DiscoveryStrategy(Protocol):
    def discover(self, base_url: str) -> AsyncIterator[str]: ...

