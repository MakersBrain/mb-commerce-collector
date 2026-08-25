"""Which paid-proxy providers exist, and what each one can be asked to do.

Everything that used to be the literal string ``decodo`` somewhere in the
control plane lives here instead: the database key, the advisory-lock name, the
words an operator has to type to confirm a cycle, the paid IP-check endpoint,
and whether the provider can propose a cycle at all.

THE CAPABILITY FLAGS ARE THE POINT. Two providers do not differ only in their
base URL. Decodo sells a dated subscription, so a cycle can be proposed from it;
IPRoyal sells a prepaid balance with no validity window, so it cannot, and
`propose_cycle` has to refuse rather than write a fabricated `cycle_start`.
Encoding that here means the API layer asks "can this provider do X" instead of
asking "is this Decodo", which is the difference between adding a third provider
and editing twenty-five call sites again.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from .base import ProviderError, ProxyProvider


@dataclass(frozen=True)
class ProviderSpec:
    """One provider's identity and capabilities.

    `name` is load-bearing beyond display: it is the value stored in the
    `provider` column of every budget cycle, profile, usage row and reservation,
    and it is the advisory-lock key. Changing it orphans data.
    """

    name: str
    label: str
    build: Callable[..., ProxyProvider]
    default_base_url: str

    #: Whether `subscription()` yields a usable billing window. When false,
    #: `propose_cycle` must refuse: the subscription's dates become
    #: `cycle_start`/`cycle_end`, and `(provider, cycle_start)` is the conflict
    #: key, so a synthesised window is a fabricated period under a fabricated
    #: primary key.
    proposes_cycles: bool

    #: The endpoint a paid probe streams from to learn its exit identity. `None`
    #: means no endpoint is known for this provider and probes stay refused
    #: until one is configured -- a probe spends real traffic, so guessing a URL
    #: is worse than not probing.
    probe_url: str | None = None

    #: Whether the provider exposes a per-sub-user enabled/disabled state. When
    #: false, "disable" has to be expressed some other way (removing traffic, or
    #: deleting), and `update_subuser(status=...)` will refuse.
    has_subuser_status: bool = True

    #: Whether sub-users can be provisioned with a username and password this
    #: system chooses. False for a provider whose sub-user credentials are
    #: issued by the provider rather than set on creation -- such a provider can
    #: still be reconciled and budgeted, it just cannot have profiles rotated
    #: into it, and `create_subuser` refuses rather than creating something
    #: whose password the caller believes it set.
    can_provision_subusers: bool = True

    #: Provider-supported dimensions used for durable usage reconciliation.
    #: An empty tuple means the provider cannot answer a windowed usage query
    #: and must remain fail-closed for application billing.
    reconciliation_groupings: tuple[str, ...] = ()

    #: Maximum cycle window the provider can reconcile. ``None`` means the
    #: provider has not documented a narrower limit than the subscription.
    max_reconciliation_window: timedelta | None = None

    def confirmation(self, action: str) -> str:
        """The phrase an operator types to confirm a cycle transition.

        Derived rather than hardcoded so a new provider cannot silently inherit
        another's confirmation text -- typing "OPEN DECODO CYCLE" must not open
        an IPRoyal one.
        """
        return f"{action.upper()} {self.name.upper()} CYCLE"

    @property
    def lock_key(self) -> str:
        """Provider-scoped advisory lock name.

        It used to be the constant `proxy:decodo`, which serialised every
        provider's cycle mutations against each other. Cycles are keyed by
        `(provider, cycle_start)` and never span providers, so the lock has no
        reason to either.
        """
        return f"proxy:{self.name}"


def _build_decodo(api_key: str, *, base_url: str, **options: Any) -> ProxyProvider:
    from .decodo import DecodoProvider

    return DecodoProvider(
        api_key,
        base_url=base_url,
        limit_unit=options.get("limit_unit", "unconfirmed"),
    )


def _build_webshare(api_key: str, *, base_url: str, **options: Any) -> ProxyProvider:
    from .webshare import WebshareProvider

    return WebshareProvider(api_key, base_url=base_url)


def _build_proxyscrape(api_key: str, *, base_url: str, **options: Any) -> ProxyProvider:
    from .proxyscrape import ProxyScrapeProvider

    return ProxyScrapeProvider(
        api_key,
        base_url=base_url,
        sub_account_id=options.get("sub_account_id", ""),
    )


def _build_iproyal(api_key: str, *, base_url: str, **options: Any) -> ProxyProvider:
    from .iproyal import IPRoyalProvider

    return IPRoyalProvider(
        api_key,
        base_url=base_url,
        traffic_writes=options.get("traffic_writes", "unconfirmed"),
    )


REGISTRY: dict[str, ProviderSpec] = {
    "decodo": ProviderSpec(
        name="decodo",
        label="Decodo Residential",
        build=_build_decodo,
        default_base_url="https://api.decodo.com",
        proposes_cycles=True,
        probe_url="https://ip.decodo.com/json",
        has_subuser_status=True,
        reconciliation_groupings=("day", "target"),
    ),
    "iproyal": ProviderSpec(
        name="iproyal",
        label="IPRoyal Residential",
        build=_build_iproyal,
        default_base_url="https://resi-api.iproyal.com/v1",
        # Prepaid balance, no validity window: cycles are opened by hand.
        proposes_cycles=False,
        # IPRoyal publishes no IP-check endpoint that this has verified, and a
        # probe spends paid traffic. Configure one explicitly to enable probes.
        probe_url=None,
        has_subuser_status=False,
        reconciliation_groupings=("day",),
    ),
    "webshare": ProviderSpec(
        name="webshare",
        label="Webshare",
        build=_build_webshare,
        default_base_url="https://proxy.webshare.io/api/v2",
        # A real renewal term with start and end dates.
        proposes_cycles=True,
        probe_url=None,
        has_subuser_status=False,
        # Sub-user credentials are issued by Webshare, not chosen here.
        can_provision_subusers=False,
        reconciliation_groupings=("total",),
        max_reconciliation_window=timedelta(days=90),
    ),
    "proxyscrape": ProviderSpec(
        name="proxyscrape",
        label="ProxyScrape Residential",
        build=_build_proxyscrape,
        default_base_url="https://api.proxyscrape.com",
        # Plan and allowance, but no validity window.
        proposes_cycles=False,
        probe_url=None,
        has_subuser_status=False,
        # Sub-users carry no traffic ceiling, so one could spend the whole
        # balance with only the application ledger in the way.
        can_provision_subusers=False,
    ),
}


def spec(name: str) -> ProviderSpec:
    try:
        return REGISTRY[name]
    except KeyError:
        raise ProviderError("provider_unknown", f"no such proxy provider: {name!r}") from None


def known() -> list[str]:
    return sorted(REGISTRY)
