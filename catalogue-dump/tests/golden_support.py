"""Run one source against the recorded response cache, deterministically.

This module is the seam the golden tests sit on. `--cache-mode replay` never
touches the network and serves the exact bytes each shop sent, so a scraper run
this way is a pure function of the cache directory and the source's config —
which is what makes a frozen output a meaningful assertion rather than a
snapshot of one afternoon's network.

Everything that decides *how* a source is run lives in `collect()`. Phase 1
moved the orchestrator out of `dump.py` into `crawl.runner`, and rewiring this
one function was the whole of the change on the test side: every golden file
kept its meaning, and the fact that they all still pass is the evidence that the
refactor changed no output.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from mb_ceramics_catalogue.config.settings import CrawlParams
from mb_ceramics_catalogue.config.sources import SourcesFile, default_path
from mb_ceramics_catalogue.crawl.progress import Progress
from mb_ceramics_catalogue.crawl.runner import run_source
from mb_ceramics_catalogue.crawl.session import open_session
from mb_ceramics_catalogue.scrapers.record import RecordBuilder

ROOT = Path(__file__).resolve().parent.parent
GOLDEN = Path(__file__).resolve().parent / "golden"
CACHE = ROOT / ".cache"

#: `fetched_at` is the wall clock at the moment the row was built, so it is the
#: one field that legitimately differs between two identical runs.
VOLATILE = ("fetched_at",)


def sources() -> SourcesFile:
    return SourcesFile.load(default_path())


def cached_sources() -> list[str]:
    """The sources the checked-in cache can actually replay, in a stable order.

    A source whose host has no cache directory would replay to zero records and
    assert nothing, so it is left out rather than frozen as an empty golden file
    that would keep passing after the scraper broke.
    """
    if not CACHE.is_dir():
        return []
    hosts = {path.name for path in CACHE.iterdir() if path.is_dir()}
    return sorted(
        name for name, config in sources().items() if urlparse(config.url).netloc in hosts
    )


def normalise(record: dict[str, Any]) -> dict[str, Any]:
    return {**record, **dict.fromkeys(VOLATILE, "<volatile>")}


def serialise(records: list[dict[str, Any]]) -> str:
    """The NDJSON the dump would write, with volatile fields pinned.

    Keys are sorted so the comparison is about content rather than about the
    order `record.build` happens to assemble a dict in.
    """
    return "".join(
        json.dumps(normalise(record), sort_keys=True, ensure_ascii=False, default=str) + "\n"
        for record in records
    )


async def collect(
    name: str,
    limit: int | None = None,
    *,
    scraper: str | None = None,
) -> dict[str, Any]:
    """Scrape one source from the cache alone and return records plus summary.

    ``scraper`` selects an explicitly registered migration path while retaining
    every other checked-in source option.  This lets recorded parity run the
    legacy and library paths against independent readers of the same archive;
    it does not rewrite or translate the archive.
    """
    configured = sources()
    source = configured[name]
    if scraper is not None:
        source = type(source).model_validate(
            {**source.model_dump(mode="python"), "scraper": scraper}
        )
    params = CrawlParams(
        limit=limit,
        cache_mode="replay",
        # A replay must never be able to reach the network by another door: a
        # browser render does not go through the HTTP cache path, so leaving it
        # enabled would quietly turn an offline test into a live crawl.
        browser="never",
        cache_max_age_hours=0,
        dry_run=True,
        allow_empty=True,
    )
    with RecordBuilder(configured.as_scraper_configs()):
        async with (
            open_session(params, CACHE) as session,
            Progress(1) as progress,
        ):
            outcome = await run_source(name, source, session, params, progress, None)
    return outcome.as_payload()


def freeze(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Reduce a run to what is worth checking in.

    A full NDJSON per source is tens of megabytes and unreadable in a diff, so
    the digest carries the byte-equality claim and the sample carries the
    readable evidence of what changed when the digest moves.
    """
    records = payload["records"]
    summary = payload["summary"]
    body = serialise(records)
    sample = [normalise(record) for record in (records[:2] + records[-1:])]
    return {
        "source": name,
        "scraper": summary["scraper"],
        "extraction_method": summary.get("extraction_method"),
        "records": summary["records"],
        "discovered": summary["discovered"],
        "requests": summary["requests"],
        "rendered_pages": summary["rendered_pages"],
        "truncated": summary["truncated"],
        "error_count": summary["error_count"],
        "errors": summary["errors"],
        "notes": summary["notes"],
        "field_coverage": summary["field_coverage"],
        "digest": hashlib.sha256(body.encode("utf-8")).hexdigest(),
        "sample": sample,
    }


def golden_path(name: str) -> Path:
    return GOLDEN / f"{name}.json"


def write_golden(name: str, frozen: dict[str, Any]) -> None:
    GOLDEN.mkdir(parents=True, exist_ok=True)
    golden_path(name).write_text(
        json.dumps(frozen, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Freeze the replay output of each cached source.")
    parser.add_argument("--source", default="all", help="a source name, a comma-separated list, or 'all'")
    parser.add_argument("--limit", type=int, help="cap records per source, for a quick regeneration")
    options = parser.parse_args()

    names = (
        cached_sources()
        if options.source == "all"
        else [name.strip() for name in options.source.split(",") if name.strip()]
    )
    for name in names:
        payload = asyncio.run(collect(name, options.limit))
        frozen = freeze(name, payload)
        write_golden(name, frozen)
        print(f"  {name:24s} {frozen['records']:6d} records  {frozen['digest'][:16]}")


if __name__ == "__main__":
    main()
