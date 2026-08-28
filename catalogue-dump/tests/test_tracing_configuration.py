from __future__ import annotations

from typing import Any, ClassVar

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http import trace_exporter
from opentelemetry.sdk.trace import export

from mb_ceramics_catalogue.observability import tracing


class FakeExporter:
    instances: ClassVar[list[FakeExporter]] = []

    def __init__(self, endpoint: str | None = None) -> None:
        self.endpoint = endpoint
        self.instances.append(self)


class FakeProcessor:
    instances: ClassVar[list[FakeProcessor]] = []

    def __init__(self, exporter: FakeExporter) -> None:
        self.exporter = exporter
        self.instances.append(self)

    def shutdown(self) -> None:
        pass


class NoopExporter(export.SpanExporter):
    def export(self, spans) -> export.SpanExportResult:
        return export.SpanExportResult.SUCCESS


def _reset() -> None:
    tracing._enabled = False
    tracing._provider = None
    tracing._tracer = None
    FakeExporter.instances.clear()
    FakeProcessor.instances.clear()


def test_signal_endpoint_enables_bounded_batch_export_with_reviewed_resource(
    monkeypatch,
) -> None:
    _reset()
    providers: list[Any] = []
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://otel.test/v1/traces")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
    monkeypatch.setenv(
        "OTEL_RESOURCE_ATTRIBUTES",
        "service.name=hostile,deployment.environment=staging",
    )
    monkeypatch.setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
    monkeypatch.setenv("OTEL_TRACES_SAMPLER_ARG", "0.01")
    monkeypatch.setattr(trace_exporter, "OTLPSpanExporter", FakeExporter)
    monkeypatch.setattr(export, "BatchSpanProcessor", FakeProcessor)
    monkeypatch.setattr(trace, "set_tracer_provider", providers.append)
    monkeypatch.setattr(trace, "get_tracer", lambda _name: object())

    assert tracing.configure("catalogue-worker") is True

    [provider] = providers
    assert FakeExporter.instances[0].endpoint == "https://otel.test/v1/traces"
    assert FakeProcessor.instances[0].exporter is FakeExporter.instances[0]
    assert provider.resource.attributes["service.name"] == "catalogue-worker"
    assert provider.resource.attributes["deployment.environment"] == "staging"
    assert provider.sampler.get_description() == "ParentBased{root:TraceIdRatioBased{0.01},remoteParentSampled:AlwaysOnSampler,remoteParentNotSampled:AlwaysOffSampler,localParentSampled:AlwaysOnSampler,localParentNotSampled:AlwaysOffSampler}"


def test_unsupported_protocol_and_exporter_failure_are_fail_open(monkeypatch) -> None:
    _reset()
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://otel.test/v1/traces")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    monkeypatch.setattr(trace, "set_tracer_provider", lambda _provider: None)
    monkeypatch.setattr(trace, "get_tracer", lambda _name: object())
    assert tracing.configure("catalogue-worker") is False

    _reset()
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
    monkeypatch.setattr(
        trace_exporter,
        "OTLPSpanExporter",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("collector unavailable")),
    )
    assert tracing.configure("catalogue-worker") is False


def test_http_exporter_url_decodes_signal_specific_access_headers(monkeypatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://otel.test/v1/traces")
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        "CF-Access-Client-Id=id%3Dpart,CF-Access-Client-Secret=secret%2Cvalue%25",
    )

    exporter = trace_exporter.OTLPSpanExporter()
    try:
        assert exporter._endpoint == "https://otel.test/v1/traces"
        assert exporter._session.headers["CF-Access-Client-Id"] == "id=part"
        assert exporter._session.headers["CF-Access-Client-Secret"] == "secret,value%"
    finally:
        exporter.shutdown()


def test_batch_processor_reads_bounded_standard_environment(monkeypatch) -> None:
    monkeypatch.setenv("OTEL_BSP_MAX_QUEUE_SIZE", "128")
    monkeypatch.setenv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "32")
    monkeypatch.setenv("OTEL_BSP_SCHEDULE_DELAY", "1000")
    monkeypatch.setenv("OTEL_BSP_EXPORT_TIMEOUT", "3000")

    processor = export.BatchSpanProcessor(NoopExporter())
    try:
        batch = processor._batch_processor
        assert batch._max_queue_size == 128
        assert batch._max_export_batch_size == 32
        assert batch._schedule_delay_millis == 1000
        assert batch._export_timeout_millis == 3000
    finally:
        processor.shutdown()


def test_shutdown_cannot_change_application_outcome() -> None:
    class BrokenProvider:
        def shutdown(self) -> None:
            raise RuntimeError("export flush failed")

    tracing._enabled = True
    tracing._provider = BrokenProvider()
    tracing._tracer = object()

    tracing.shutdown()

    assert tracing.enabled() is False
    assert tracing._provider is None
    assert tracing._tracer is None
