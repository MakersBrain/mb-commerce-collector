from __future__ import annotations

import mb_commerce_scraper.models as library_models

from mb_ceramics_catalogue.connectors import commerce


def test_deprecated_commerce_exports_are_exact_library_model_identities() -> None:
    assert set(commerce.__all__) == {
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
    }
    for name in commerce.__all__:
        assert getattr(commerce, name) is getattr(library_models, name)
