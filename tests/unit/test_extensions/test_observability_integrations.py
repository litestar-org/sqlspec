"""Unit tests for observability helper extensions."""

from sqlspec.observability._observer import create_event


def test_enable_tracing_sets_telemetry(monkeypatch):
    monkeypatch.setattr("sqlspec.utils.module_loader.OPENTELEMETRY_INSTALLED", True, raising=False)

    from sqlspec.extensions import otel

    config = otel.enable_tracing()
    assert config.telemetry is not None
    assert config.telemetry.enable_spans is True
    provider = config.telemetry.provider_factory() if config.telemetry.provider_factory else None
    assert provider is None  # default noop when otel not installed


def test_enable_metrics_registers_observer(monkeypatch):
    monkeypatch.setattr("sqlspec.utils.module_loader.PROMETHEUS_INSTALLED", True, raising=False)

    from sqlspec.extensions import prometheus

    config = prometheus.enable_metrics()
    assert config.statement_observers is not None
    observer = config.statement_observers[-1]

    event = create_event(
        sql="SELECT 1",
        parameters=(),
        driver="TestDriver",
        adapter="test",
        bind_key=None,
        operation="SELECT",
        execution_mode="sync",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.05,
        correlation_id=None,
    )

    observer(event)
