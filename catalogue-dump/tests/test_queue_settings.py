from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from mb_ceramics_catalogue.config.settings import Settings
from mb_ceramics_catalogue.ops.providers.factory import consumer, publisher
from mb_ceramics_catalogue.ops.providers.nats import NatsConsumer, NatsPublisher


def test_visibility_must_cover_the_complete_delivery_lifetime() -> None:
    with pytest.raises(ValidationError, match="complete delivery-lifetime"):
        Settings(queue_visibility_seconds=3900)
    assert Settings(queue_visibility_seconds=3901).queue_visibility_seconds == 3901


def test_cloudflare_selection_requires_every_route_and_recovery_queue() -> None:
    with pytest.raises(ValidationError, match="CATALOGUE_CF_ACCOUNT_ID"):
        Settings(queue_provider="cloudflare")


def test_webshare_data_plane_is_separately_default_off() -> None:
    defaults = Settings()

    assert defaults.proxy_webshare_data_plane_enabled is False
    assert defaults.proxy_webshare_gateway_secret_file is None

    configured = Settings(
        proxy_webshare_data_plane_enabled=True,
        proxy_webshare_gateway_secret_file=Path(
            "/run/secrets/webshare-gateway.json"
        ),
    )
    assert configured.proxy_webshare_data_plane_enabled is True
    assert configured.proxy_webshare_gateway_secret_file == Path(
        "/run/secrets/webshare-gateway.json"
    )


def test_role_scoped_nats_clients_do_not_provision(tmp_path) -> None:
    publish_token = tmp_path / "publish-token"
    consume_token = tmp_path / "consume-token"
    publish_token.write_text("publish-only", encoding="utf-8")
    consume_token.write_text("consume-only", encoding="utf-8")
    settings = SimpleNamespace(
        queue_provider="nats",
        nats_url="nats://queue:4222",
        nats_publish_token_file=publish_token,
        nats_consume_token_file=consume_token,
        nats_stream="CATALOGUE_JOBS",
    )

    scoped_publisher = publisher(settings)
    scoped_consumer = consumer(settings)
    assert isinstance(scoped_publisher, NatsPublisher)
    assert isinstance(scoped_consumer, NatsConsumer)
    assert scoped_publisher.queue.token == "publish-only"
    assert scoped_consumer.queue.token == "consume-only"


def test_nats_clients_never_provision_implicitly() -> None:
    settings = SimpleNamespace(
        queue_provider="nats",
        nats_url="nats://queue:4222",
        nats_publish_token_file=None,
        nats_consume_token_file=None,
        nats_stream="CATALOGUE_JOBS",
    )

    nats_publisher = publisher(settings)
    nats_consumer = consumer(settings)
    assert isinstance(nats_publisher, NatsPublisher)
    assert isinstance(nats_consumer, NatsConsumer)
    assert not hasattr(nats_publisher.queue, "provision_on_connect")
    assert not hasattr(nats_consumer.queue, "provision_on_connect")


def test_role_scoped_nats_user_credentials_are_loaded_without_provisioning(tmp_path) -> None:
    credentials = tmp_path / "publish.json"
    credentials.write_text(
        json.dumps({"user": "catalogue-publisher", "password": "p" * 48}),
        encoding="utf-8",
    )
    settings = SimpleNamespace(
        queue_provider="nats",
        nats_url="nats://queue:4222",
        nats_publish_token_file=None,
        nats_publish_credentials_file=credentials,
        nats_stream="CATALOGUE_JOBS",
    )

    scoped = publisher(settings)
    assert isinstance(scoped, NatsPublisher)
    assert scoped.queue.user == "catalogue-publisher"
    assert scoped.queue.password == "p" * 48
    assert scoped.queue.token == ""


def test_role_scoped_nats_credentials_reject_extra_fields(tmp_path) -> None:
    credentials = tmp_path / "publish.json"
    credentials.write_text(
        json.dumps({"user": "catalogue-publisher", "password": "p" * 48, "admin": True}),
        encoding="utf-8",
    )
    settings = SimpleNamespace(
        queue_provider="nats",
        nats_url="nats://queue:4222",
        nats_publish_token_file=None,
        nats_publish_credentials_file=credentials,
        nats_stream="CATALOGUE_JOBS",
    )

    with pytest.raises(ValueError, match="only user and password"):
        publisher(settings)
