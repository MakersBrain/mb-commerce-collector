"""OpenTelemetry, wired now and exporting only when something asks for it.

A run is eighty concurrent sources issuing thousands of requests, and the
question during a bad one is always "what is that source waiting on".
`scrapers/activity.py` answers it for live requests and keeps forty of them;
nothing answered it afterwards.

The span hierarchy mirrors the domain: `run` -> `job` (one per source) ->
`http.request`, the last of which
`opentelemetry-instrumentation-httpx` produces without `Fetcher` being touched.

The exporter is off unless the standard signal-specific
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (preferred) or generic
`OTEL_EXPORTER_OTLP_ENDPOINT` is set. Configuration and export are deliberately
fail-open: losing observability must never lose catalogue work.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from typing import Any

_enabled = False
_tracer: Any = None
_provider: Any = None


def enabled() -> bool:
    return _enabled


def configure(service_name: str = "catalogue", *, force: bool = False) -> bool:
    """Install a tracer provider. Returns whether spans will be exported.

    Without an endpoint a provider is still installed, so `span()` costs a
    no-op recording rather than a branch at every call site, and `trace_id()`
    still returns a real id for the job row and the logs.
    """
    global _enabled, _provider, _tracer

    if _provider is not None:
        return _enabled

    endpoint = (
        os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "").strip()
        or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    )
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
    except ImportError:  # pragma: no cover - the otel extra is optional
        return False

    from mb_ceramics_catalogue import __version__

    provider = TracerProvider(
        resource=Resource.create(
            {"service.name": service_name, "service.version": __version__}
        )
    )

    if endpoint or force:
        protocol = (
            os.environ.get("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "").strip()
            or os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf").strip()
        )
        if protocol == "http/protobuf":
            try:
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                    OTLPSpanExporter,
                )
                from opentelemetry.sdk.trace.export import BatchSpanProcessor

                # The exporter reads the signal-specific headers and timeout;
                # the processor reads the standard OTEL_BSP_* queue, batch,
                # delay and export-timeout bounds. In particular, the SDK
                # URL-decodes OTEL_EXPORTER_OTLP_TRACES_HEADERS as required by
                # the OTLP environment-variable specification.
                exporter = OTLPSpanExporter(endpoint=endpoint or None)
                provider.add_span_processor(BatchSpanProcessor(exporter))
                _enabled = True
            except Exception:  # noqa: BLE001 - tracing must never fail application startup
                _enabled = False

    trace.set_tracer_provider(provider)
    _provider = provider
    _tracer = trace.get_tracer("mb_ceramics_catalogue")

    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument()
    except Exception:  # noqa: BLE001 - instrumentation is never worth a failed run
        pass

    return _enabled


def shutdown() -> None:
    """Flush and stop the configured provider without failing application exit."""
    global _enabled, _provider, _tracer

    provider, _provider = _provider, None
    _enabled = False
    _tracer = None
    if provider is None:
        return
    with suppress(Exception):
        # BatchSpanProcessor.shutdown drains its bounded queue using the
        # configured OTEL_BSP_EXPORT_TIMEOUT. Exporter errors remain internal
        # to the SDK and this final guard preserves the application's exit.
        provider.shutdown()


@contextmanager
def span(name: str, **attributes: Any) -> Iterator[Any]:
    """A span, or nothing at all if the SDK was never installed."""
    if _tracer is None:
        yield None
        return
    with _tracer.start_as_current_span(name) as active:
        for key, value in attributes.items():
            if value is not None:
                active.set_attribute(key, value)
        yield active


def event(name: str, **attributes: Any) -> None:
    """Record something that happened inside the current span.

    This is where `Fetcher`'s own decisions go — cache hit, robots denial,
    browser fallback, backoff — which are exactly the things that are invisible
    today and exactly what one wants when a source is behaving oddly.
    """
    if _tracer is None:
        return
    from opentelemetry import trace

    current = trace.get_current_span()
    if current.is_recording():
        current.add_event(name, {k: v for k, v in attributes.items() if v is not None})


def trace_id() -> str | None:
    """The active trace as 32 lower-case hex characters, for `catalogue.jobs`.

    This is the part that is useful without a collector: a job row and every log
    line from that job carry the same id, so the correlation is in the data
    whether or not anything is currently consuming spans.
    """
    if _tracer is None:
        return None
    from opentelemetry import trace

    context = trace.get_current_span().get_span_context()
    if not context.is_valid:
        return None
    return format(context.trace_id, "032x")
