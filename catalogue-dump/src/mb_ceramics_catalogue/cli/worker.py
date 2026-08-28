"""`catalogue-worker`: consume NATS JetStream jobs and run them.

    catalogue-worker                            a plain worker
    catalogue-worker --capabilities browser     one that can take browser jobs
    catalogue-worker --once                     take one job and exit, for tests

Scaled by running more of them. JetStream shares each durable route across the
eligible consumers, PostgreSQL execution tokens fence redelivery, and
`catalogue.hosts` stops three workers tripling the load on every shop.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from pathlib import Path

from mb_ceramics_catalogue import __version__
from mb_ceramics_catalogue.config.settings import Settings
from mb_ceramics_catalogue.config.sources import SourcesFile, default_path
from mb_ceramics_catalogue.connectors import BrowserBackendName
from mb_ceramics_catalogue.observability import logging as obs
from mb_ceramics_catalogue.observability import server, tracing
from mb_ceramics_catalogue.ops.worker import Worker
from mb_ceramics_catalogue.storage import db
from mb_ceramics_catalogue.transports.browser import BrowserBackend
from mb_ceramics_catalogue.transports.cdp_extension_proxy import (
    CdpExtensionProxyBackend,
    CdpReadinessError,
    FileSecretResolver,
    HttpCdpEndpointProvider,
    load_cdp_profiles,
)

LOGGER = obs.get_logger("catalogue.worker.cli")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="catalogue-worker", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--dsn", default="", help="libpq connection string; defaults to $CATALOGUE_DSN")
    parser.add_argument("--sources-file", type=Path, default=None)
    parser.add_argument(
        "--capabilities",
        default="",
        help="comma-separated capabilities this worker advertises, e.g. 'browser'. "
             "A job may only be claimed by a worker advertising everything it requires",
    )
    parser.add_argument("--cache", type=Path, default=None,
                        help="shared response cache directory; defaults to $CATALOGUE_CACHE_DIR")
    parser.add_argument("--dumps", type=Path, default=None,
                        help="where NDJSON artifacts are written, namespaced <run>/<job>/")
    parser.add_argument("--once", action="store_true",
                        help="take at most one job and exit")
    parser.add_argument("--metrics-port", type=int, default=9109, metavar="PORT",
                        help="serve /metrics and /health on this port (0 to disable)")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    parser.add_argument("--log-json", action="store_true")
    return parser


async def run(options: argparse.Namespace) -> int:
    settings = Settings()
    obs.configure(options.log_level, json=options.log_json or settings.log_json)
    tracing.configure("catalogue-worker")

    if options.dsn:
        settings.dsn = options.dsn
    settings.dsn = db.dsn_from_environment(settings.dsn)
    if options.cache is not None:
        settings.cache_dir = options.cache
    if options.dumps is not None:
        settings.dumps_dir = options.dumps

    capabilities = [item.strip() for item in options.capabilities.split(",") if item.strip()]
    if "browser" in capabilities and not any(
        item.startswith("browser:") for item in capabilities
    ):
        capabilities.append(f"browser:{BrowserBackendName.CAMOUFOX.value}")
    capabilities, browser_backends = await configure_browser_backends(settings, capabilities)
    sources = SourcesFile.load(options.sources_file or default_path())

    # Two connections at least: the heartbeat must keep beating while a job
    # holds one for the length of a crawl.
    try:
        async with db.pool(settings.dsn, minimum=2, maximum=6) as pool:
            worker = Worker(
                pool, sources, settings, capabilities=capabilities,
                browser_backends=browser_backends, once=options.once,
            )
            # `--once` is used by tests and by a one-shot backfill; binding a port
            # for a process that exits in seconds only produces address-in-use noise
            # when several run together.
            listener = None if options.once else server.serve(options.metrics_port, worker.describe)
            worker.install_signal_handlers()
            try:
                await worker.run()
            finally:
                if listener is not None:
                    listener.shutdown()
    finally:
        # Also covers a database-pool startup failure after a successful CDP
        # readiness probe. Provider shutdown is idempotent.
        for backend in browser_backends.values():
            await backend.shutdown()

    # A worker that found nothing to do has not failed. Exit status is about
    # whether the process ran correctly, not about whether the queue was busy.
    return 0


async def configure_browser_backends(
    settings: Settings, capabilities: Sequence[str],
) -> tuple[list[str], dict[BrowserBackendName, BrowserBackend]]:
    """Probe optional exact backends and degrade capability advertisement safely."""
    advertised = list(dict.fromkeys(capabilities))
    backends: dict[BrowserBackendName, BrowserBackend] = {}
    cdp_capability = f"browser:{BrowserBackendName.CDP_EXTENSION_PROXY.value}"
    if cdp_capability not in advertised:
        return advertised, backends

    backend: CdpExtensionProxyBackend | None = None
    try:
        if not settings.cdp_profiles_file or not settings.cdp_secrets_file:
            raise CdpReadinessError("CDP profile and secret files are required")
        if not settings.cdp_pool_endpoint or not settings.cdp_default_profile:
            raise CdpReadinessError("CDP pool endpoint and default profile are required")
        resolver = FileSecretResolver(settings.cdp_secrets_file)
        profiles = load_cdp_profiles(settings.cdp_profiles_file)
        profile = profiles.get(settings.cdp_default_profile)
        if profile is None:
            raise CdpReadinessError(
                f"unknown default CDP profile {settings.cdp_default_profile!r}"
            )
        if profile.allowed_worker_pool != settings.cdp_worker_pool:
            raise CdpReadinessError("default CDP profile is not allowed in this worker pool")
        pool_token = resolver.resolve(settings.cdp_pool_token_secret_ref)
        provider = HttpCdpEndpointProvider(
            settings.cdp_pool_endpoint, pool_token, profiles, resolver,
            trusted_private_hostnames=settings.cdp_pool_trusted_hostnames,
        )
        backend = CdpExtensionProxyBackend(
            profile, provider, production=settings.cdp_production
        )
        await backend.probe()
        backends[BrowserBackendName.CDP_EXTENSION_PROXY] = backend
    except (CdpReadinessError, OSError, ValueError) as error:
        if backend is not None:
            await backend.shutdown()
        advertised = [item for item in advertised if item != cdp_capability]
        if f"browser:{BrowserBackendName.CAMOUFOX.value}" not in advertised:
            advertised = [item for item in advertised if item != "browser"]
        LOGGER.warning("worker.cdp_backend_unavailable", reason=str(error))
    return advertised, backends


def main() -> int:
    options = build_parser().parse_args()
    try:
        try:
            return asyncio.run(run(options))
        except KeyboardInterrupt:  # pragma: no cover - the signal handler normally wins
            return 130
        except ValueError as error:
            print(f"catalogue-worker: {error}", file=sys.stderr)
            return 2
    finally:
        tracing.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
