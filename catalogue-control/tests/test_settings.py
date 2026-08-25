from pathlib import Path

from catalogue_control.settings import Settings


def test_webshare_gateway_store_path_is_separate_and_default_off(
    monkeypatch,
) -> None:
    monkeypatch.delenv("CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE", raising=False)
    monkeypatch.setenv("CATALOGUE_PROXY_ENABLED", "false")
    defaults = Settings()
    assert defaults.proxy_webshare_gateway_secret_file is None

    monkeypatch.setenv(
        "CATALOGUE_PROXY_WEBSHARE_GATEWAY_SECRET_FILE",
        "/run/secrets/webshare-gateway/webshare-gateway.json",
    )
    configured = Settings()

    assert configured.proxy_webshare_gateway_secret_file == Path(
        "/run/secrets/webshare-gateway/webshare-gateway.json"
    )
    assert configured.proxy_enabled is False
