import pytest
from pydantic import ValidationError

from mb_commerce_scraper import FetchPolicy, ProxyPolicyConfig


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
