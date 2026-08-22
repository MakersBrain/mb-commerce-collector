import pytest
from pydantic import ValidationError

from mb_commerce_scraper import FetchPolicy, ProxyMode, ProxyPolicyConfig


def test_layered_policies_are_strict_and_bounded() -> None:
    assert FetchPolicy().concurrency == 1
    with pytest.raises(ValidationError):
        FetchPolicy.model_validate({"concurrency": 0})
    with pytest.raises(ValidationError):
        ProxyPolicyConfig.model_validate({"country": "FRA"})
    with pytest.raises(ValidationError):
        ProxyPolicyConfig.model_validate({"maximum_requests": 0})
    with pytest.raises(ValidationError):
        ProxyPolicyConfig.model_validate({"provider_username_template": "secret"})


@pytest.mark.parametrize("country", ["fr", "Fr", "12", "ÉU"])
def test_proxy_policy_rejects_noncanonical_country(country: str) -> None:
    with pytest.raises(ValidationError, match="uppercase ASCII alpha-2"):
        ProxyPolicyConfig(country=country)


@pytest.mark.parametrize(
    "providers",
    [("",), (" decodo",), ("decodo ",), ("decodo", "decodo")],
)
def test_proxy_policy_rejects_ambiguous_provider_preferences(
    providers: tuple[str, ...],
) -> None:
    with pytest.raises(ValidationError, match="provider preferences"):
        ProxyPolicyConfig(provider_preferences=providers)


def test_proxy_policy_preserves_all_canonical_route_fields() -> None:
    policy = ProxyPolicyConfig(
        mode=ProxyMode.FAILOVER,
        country="FR",
        provider_preferences=("one", "two"),
        maximum_requests=7,
        maximum_bytes=8_000,
    )

    assert policy.model_dump(mode="json") == {
        "mode": "failover",
        "country": "FR",
        "provider_preferences": ["one", "two"],
        "maximum_requests": 7,
        "maximum_bytes": 8_000,
    }
