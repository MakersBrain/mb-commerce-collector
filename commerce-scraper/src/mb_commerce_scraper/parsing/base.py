from __future__ import annotations

from typing import Protocol

from mb_commerce_scraper.models import CommerceProductSnapshot


class ProductParser(Protocol):
    name: str
    version: str

    def parse(self, document: str, *, url: str, source_id: str) -> tuple[CommerceProductSnapshot, ...]: ...
