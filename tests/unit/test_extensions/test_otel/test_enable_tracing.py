"""Unit tests for the OpenTelemetry extension helper."""

from sqlspec.extensions import otel
from sqlspec.utils import module_loader


def _force_dependency(monkeypatch, module_name: str) -> None:
    original = module_loader.module_available

    def _fake(name: str) -> bool:
        if name == module_name:
            return True
        return original(name)

    monkeypatch.setattr(module_loader, "module_available", _fake)


def test_enable_tracing_sets_telemetry(monkeypatch) -> None:
    _force_dependency(monkeypatch, "opentelemetry")

    config = otel.enable_tracing()
    assert config.telemetry is not None
    assert config.telemetry.enable_spans is True
    provider = config.telemetry.provider_factory() if config.telemetry.provider_factory else None
    assert provider is not None
