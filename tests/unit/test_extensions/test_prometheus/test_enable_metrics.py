"""Unit tests for the Prometheus extension helper."""

from sqlspec import create_event
from sqlspec.extensions import prometheus
from sqlspec.utils import module_loader


def _force_dependency(monkeypatch, module_name: str) -> None:
    original = module_loader.module_available

    def _fake(name: str) -> bool:
        if name == module_name:
            return True
        return original(name)

    monkeypatch.setattr(module_loader, "module_available", _fake)


def test_enable_metrics_registers_observer(monkeypatch) -> None:
    _force_dependency(monkeypatch, "prometheus_client")

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
