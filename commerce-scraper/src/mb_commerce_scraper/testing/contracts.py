from __future__ import annotations

from collections.abc import AsyncIterator

from mb_commerce_scraper.models import CommerceProductSnapshot, EntityPage


async def assert_connector_pages(pages: AsyncIterator[EntityPage[CommerceProductSnapshot]]) -> tuple[EntityPage[CommerceProductSnapshot], ...]:
    collected = tuple([page async for page in pages])
    assert collected, "connector emitted no pages"
    assert [page.sequence for page in collected] == list(range(len(collected)))
    assert collected[-1].terminal
    for page in collected:
        for item in page.items:
            assert item.source_id and item.external_id and item.canonical_url
            dumped = item.model_dump_json()
            assert "password" not in dumped.casefold()
    return collected

