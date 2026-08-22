from __future__ import annotations

import hashlib

from mb_commerce_scraper.models import Evidence


def observation_id(*components: str) -> str:
    return hashlib.sha256("\0".join(components).encode()).hexdigest()


def evidence_key(evidence: tuple[Evidence, ...]) -> str:
    return "\0".join(
        f"{item.method}:{item.source_url}:{item.source_field or ''}:{item.observed_at.isoformat()}"
        for item in evidence
    )
