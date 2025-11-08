"""Tests for extension_config-driven observability hooks."""

from typing import Any

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.config import NoPoolSyncConfig


class _DummySyncConfig(NoPoolSyncConfig[Any, AsyncpgDriver]):
    driver_type = AsyncpgDriver
    connection_type = object

    def create_connection(self) -> Any:
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any):  # type: ignore[override]
        raise NotImplementedError

    def provide_session(self, *args: Any, **kwargs: Any):  # type: ignore[override]
        raise NotImplementedError


def test_otel_extension_config_enables_spans(monkeypatch):
    monkeypatch.setattr("sqlspec.utils.module_loader.OPENTELEMETRY_INSTALLED", True, raising=False)

    config = _DummySyncConfig(extension_config={"otel": {"resource_attributes": {"service.name": "api"}}})

    assert config.observability_config is not None
    telemetry = config.observability_config.telemetry
    assert telemetry is not None
    assert telemetry.resource_attributes == {"service.name": "api"}


def test_prometheus_extension_registers_observer(monkeypatch):
    monkeypatch.setattr("sqlspec.utils.module_loader.PROMETHEUS_INSTALLED", True, raising=False)

    config = _DummySyncConfig(
        extension_config={"prometheus": {"namespace": "custom", "label_names": ("driver", "operation", "adapter")}}
    )

    assert config.observability_config is not None
    observers = config.observability_config.statement_observers
    assert observers is not None and observers, "expected prometheus observer to be registered"


def test_disabled_extensions_are_ignored(monkeypatch):
    monkeypatch.setattr("sqlspec.utils.module_loader.OPENTELEMETRY_INSTALLED", True, raising=False)
    config = _DummySyncConfig(extension_config={"otel": {"enabled": False}})
    assert config.observability_config is None
