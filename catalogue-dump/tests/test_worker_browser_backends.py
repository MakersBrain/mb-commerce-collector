from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import pytest

from mb_ceramics_catalogue.cli.worker import configure_browser_backends
from mb_ceramics_catalogue.config.settings import CrawlParams, Settings
from mb_ceramics_catalogue.connectors import BrowserBackendName
from mb_ceramics_catalogue.ops.queue import ClaimedJob
from mb_ceramics_catalogue.ops.worker import Worker
from mb_ceramics_catalogue.scrapers.base import BrowserUnavailable
from mb_ceramics_catalogue.transports.browser import BrowserJobContext
from mb_ceramics_catalogue.transports.cdp_extension_proxy import (
    CdpExtensionProxyBackend,
    CdpReadinessError,
)


class DummyBackend:
    backend: Literal["cdp_extension_proxy"] = "cdp_extension_proxy"

    def open_session(self, job: BrowserJobContext | None = None) -> Any:
        raise AssertionError("selection tests do not open a browser")

    async def shutdown(self) -> None:
        return None


def claimed(selected: BrowserBackendName | None) -> ClaimedJob:
    return ClaimedJob(
        id=uuid4(),
        run_id=uuid4(),
        source_id="source",
        host="shop.test",
        attempt=1,
        max_attempts=3,
        requires=["browser"],
        requires_any=["browser:camoufox", "browser:cdp_extension_proxy"],
        params={},
        proxy_snapshot={},
        delivery_generation=1,
        execution_token=uuid4(),
        selected_browser_backend=selected,
    )


def worker_with(backends: dict[BrowserBackendName, Any] | None = None) -> Worker:
    return Worker(
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        Settings(cdp_worker_pool="browser-cdp"),
        capabilities=["browser", "browser:cdp_extension_proxy"],
        browser_backends=backends,
    )


def write_cdp_config(directory: Path) -> Settings:
    profiles = directory / "profiles.json"
    profiles.write_text(
        json.dumps(
            {
                "clean": {
                    "endpoint": "http://127.0.0.1:9222",
                    "token_secret_ref": "cdp/endpoint",
                    "allowed_worker_pool": "browser-cdp",
                    "route": "direct",
                    "isolation": "ephemeral_profile",
                    "expected_service_version": "0.1.0",
                    "expected_profile_generation": "generation-1",
                }
            }
        ),
        encoding="utf-8",
    )
    secrets = directory / "secrets.json"
    secrets.write_text(
        json.dumps({"cdp/endpoint": "endpoint-secret", "cdp/pool_token": "pool-secret"}),
        encoding="utf-8",
    )
    secrets.chmod(0o400)
    return Settings(
        cdp_profiles_file=profiles,
        cdp_secrets_file=secrets,
        cdp_pool_endpoint="https://pool.private.example",
        cdp_pool_trusted_hostnames=frozenset({"pool.private.example"}),
        cdp_default_profile="clean",
        cdp_worker_pool="browser-cdp",
    )


def test_worker_uses_exact_selected_backend_without_platform_branching() -> None:
    backend = DummyBackend()
    worker = worker_with({BrowserBackendName.CDP_EXTENSION_PROXY: backend})
    selected, context = worker._browser_for_job(
        claimed(BrowserBackendName.CDP_EXTENSION_PROXY), CrawlParams(), False
    )
    assert selected is backend
    assert context is not None
    assert context.logical_profile is None


def test_worker_cannot_advertise_unconfigured_cdp_backend() -> None:
    with pytest.raises(ValueError, match="without a ready backend"):
        worker_with()


def test_worker_does_not_fallback_after_backend_lineage_is_selected() -> None:
    worker = worker_with({BrowserBackendName.CDP_EXTENSION_PROXY: DummyBackend()})
    worker._browser_backends.pop(BrowserBackendName.CDP_EXTENSION_PROXY)
    with pytest.raises(BrowserUnavailable, match=r"cdp_extension_proxy.*unavailable"):
        worker._browser_for_job(
            claimed(BrowserBackendName.CDP_EXTENSION_PROXY), CrawlParams(), False
        )
    assert BrowserBackendName.CAMOUFOX not in worker._browser_backends


def test_cdp_selected_job_rejects_paid_proxy_transport() -> None:
    worker = worker_with({BrowserBackendName.CDP_EXTENSION_PROXY: DummyBackend()})
    with pytest.raises(BrowserUnavailable, match="direct-route only"):
        worker._browser_for_job(
            claimed(BrowserBackendName.CDP_EXTENSION_PROXY),
            CrawlParams(),
            True,
        )


async def test_ready_cdp_backend_keeps_exact_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = write_cdp_config(tmp_path)

    async def ready(self: CdpExtensionProxyBackend) -> None:
        return None

    monkeypatch.setattr(CdpExtensionProxyBackend, "probe", ready)
    capabilities, backends = await configure_browser_backends(
        settings,
        ["browser", "browser:camoufox", "browser:cdp_extension_proxy"],
    )
    assert capabilities == ["browser", "browser:camoufox", "browser:cdp_extension_proxy"]
    assert set(backends) == {BrowserBackendName.CDP_EXTENSION_PROXY}
    await backends[BrowserBackendName.CDP_EXTENSION_PROXY].shutdown()


async def test_failed_cdp_probe_degrades_to_camoufox_without_changing_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = write_cdp_config(tmp_path)

    async def unavailable(self: CdpExtensionProxyBackend) -> None:
        raise CdpReadinessError("probe unavailable")

    monkeypatch.setattr(CdpExtensionProxyBackend, "probe", unavailable)
    capabilities, backends = await configure_browser_backends(
        settings,
        ["browser", "browser:camoufox", "browser:cdp_extension_proxy"],
    )
    assert capabilities == ["browser", "browser:camoufox"]
    assert backends == {}


async def test_failed_only_backend_removes_generic_browser_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = write_cdp_config(tmp_path)

    async def unavailable(self: CdpExtensionProxyBackend) -> None:
        raise CdpReadinessError("probe unavailable")

    monkeypatch.setattr(CdpExtensionProxyBackend, "probe", unavailable)
    capabilities, backends = await configure_browser_backends(
        settings, ["browser", "browser:cdp_extension_proxy"]
    )
    assert capabilities == []
    assert backends == {}
