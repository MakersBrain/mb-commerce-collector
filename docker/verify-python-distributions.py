"""Fail an image build unless both shared Python distributions are usable.

Dockerfiles invoke this script with ``python -I``.  Isolated mode keeps the
build context (notably ``/src``) off ``sys.path``, so a successful import proves
the installed wheel contents rather than an accidentally importable source
tree.
"""

from __future__ import annotations

from importlib import import_module
from importlib.metadata import distribution

DISTRIBUTIONS = (
    ("mb-commerce-scraper", "mb_commerce_scraper"),
    ("mb-ceramics-catalogue", "mb_ceramics_catalogue"),
)


def main() -> None:
    for distribution_name, module_name in DISTRIBUTIONS:
        installed = distribution(distribution_name)
        module = import_module(module_name)
        origin = getattr(module, "__file__", None)
        if origin is None:
            raise RuntimeError(f"{module_name} has no import origin")
        print(f"{distribution_name}=={installed.version}: {origin}")


if __name__ == "__main__":
    main()
