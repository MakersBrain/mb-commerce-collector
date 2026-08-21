"""Deprecated compatibility exports for neutral commerce contracts.

Import from :mod:`mb_commerce_scraper.models` in new code. Keeping this module
for one migration window preserves existing connector and projector imports
without maintaining a second contract definition.
"""

from mb_commerce_scraper.models import (
    Availability,
    CategoryRef,
    CommerceOffer,
    CommerceProductSnapshot,
    CommerceVariant,
    ContractModel,
    DocumentRef,
    Evidence,
    MediaRef,
    Money,
    StockQuantityKind,
    StockState,
)

__all__ = [
    "Availability",
    "CategoryRef",
    "CommerceOffer",
    "CommerceProductSnapshot",
    "CommerceVariant",
    "ContractModel",
    "DocumentRef",
    "Evidence",
    "MediaRef",
    "Money",
    "StockQuantityKind",
    "StockState",
]
