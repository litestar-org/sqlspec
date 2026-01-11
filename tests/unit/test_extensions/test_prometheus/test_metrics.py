"""Unit tests for Prometheus metrics extension."""

from sqlspec import create_event
from sqlspec.utils import module_loader


def _force_dependency(monkeypatch, module_name: str) -> None:
    original = module_loader.module_available

    def _fake(name: str) -> bool:
        if name == module_name:
            return True
        return original(name)

    monkeypatch.setattr(module_loader, "module_available", _fake)


def test_default_labels_are_db_system_and_operation(monkeypatch) -> None:
    """Verify default labels changed to (db_system, operation)."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    observer = prometheus.PrometheusStatementObserver()
    assert observer._label_names == ("db_system", "operation")  # pyright: ignore[reportPrivateUsage]


def test_enable_metrics_uses_db_system_labels(monkeypatch) -> None:
    """Verify enable_metrics uses db_system as default label."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    config = prometheus.enable_metrics()
    assert config.statement_observers is not None
    observer = config.statement_observers[-1]
    assert observer._label_names == ("db_system", "operation")  # pyright: ignore[reportPrivateUsage, reportFunctionMemberAccess]


def test_db_system_label_extraction_from_event(monkeypatch) -> None:
    """Verify db_system is extracted from StatementEvent.db_system field."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    observer = prometheus.PrometheusStatementObserver()

    event = create_event(
        sql="SELECT 1",
        parameters=(),
        driver="AsyncpgDriver",
        adapter="AsyncpgConfig",
        bind_key=None,
        operation="SELECT",
        execution_mode="async",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.05,
        correlation_id=None,
        db_system="postgresql",
    )

    labels = observer._label_values(event)  # pyright: ignore[reportPrivateUsage]
    assert labels == ("postgresql", "SELECT")


def test_db_system_fallback_to_resolve(monkeypatch) -> None:
    """Verify fallback to resolve_db_system() when event.db_system is None."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    observer = prometheus.PrometheusStatementObserver()

    event = create_event(
        sql="SELECT 1",
        parameters=(),
        driver="AsyncpgDriver",
        adapter="AsyncpgConfig",
        bind_key=None,
        operation="SELECT",
        execution_mode="async",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.05,
        correlation_id=None,
        db_system=None,
    )

    labels = observer._label_values(event)  # pyright: ignore[reportPrivateUsage]
    assert labels[0] == "postgresql"
    assert labels[1] == "SELECT"


def test_custom_label_names(monkeypatch) -> None:
    """Verify custom label configurations work."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    observer = prometheus.PrometheusStatementObserver(label_names=("driver", "adapter", "operation"))
    assert observer._label_names == ("driver", "adapter", "operation")  # pyright: ignore[reportPrivateUsage]

    event = create_event(
        sql="INSERT INTO foo VALUES (1)",
        parameters=(),
        driver="SqliteDriver",
        adapter="SqliteConfig",
        bind_key=None,
        operation="INSERT",
        execution_mode="sync",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.01,
        correlation_id=None,
    )

    labels = observer._label_values(event)  # pyright: ignore[reportPrivateUsage]
    assert labels == ("SqliteDriver", "SqliteConfig", "INSERT")


def test_bind_key_label_defaults_to_default(monkeypatch) -> None:
    """Verify bind_key label defaults to 'default' when None."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    observer = prometheus.PrometheusStatementObserver(label_names=("db_system", "bind_key"))

    event = create_event(
        sql="SELECT 1",
        parameters=(),
        driver="AsyncpgDriver",
        adapter="AsyncpgConfig",
        bind_key=None,
        operation="SELECT",
        execution_mode="async",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.05,
        correlation_id=None,
        db_system="postgresql",
    )

    labels = observer._label_values(event)  # pyright: ignore[reportPrivateUsage]
    assert labels == ("postgresql", "default")


def test_bind_key_label_uses_value_when_set(monkeypatch) -> None:
    """Verify bind_key label uses actual value when provided."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    observer = prometheus.PrometheusStatementObserver(label_names=("db_system", "bind_key"))

    event = create_event(
        sql="SELECT 1",
        parameters=(),
        driver="AsyncpgDriver",
        adapter="AsyncpgConfig",
        bind_key="analytics",
        operation="SELECT",
        execution_mode="async",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.05,
        correlation_id=None,
        db_system="postgresql",
    )

    labels = observer._label_values(event)  # pyright: ignore[reportPrivateUsage]
    assert labels == ("postgresql", "analytics")


def test_operation_defaults_to_execute(monkeypatch) -> None:
    """Verify operation defaults to EXECUTE when empty string."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    observer = prometheus.PrometheusStatementObserver()

    event = create_event(
        sql="CALL my_procedure()",
        parameters=(),
        driver="OracleDriver",
        adapter="OracleConfig",
        bind_key=None,
        operation="",
        execution_mode="sync",
        is_many=False,
        is_script=False,
        rows_affected=0,
        duration_s=0.01,
        correlation_id=None,
        db_system="oracle",
    )

    labels = observer._label_values(event)  # pyright: ignore[reportPrivateUsage]
    assert labels == ("oracle", "EXECUTE")


def test_observer_records_metrics(monkeypatch) -> None:
    """Verify observer records counter, duration, and rows metrics."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    observer = prometheus.PrometheusStatementObserver()

    event = create_event(
        sql="SELECT * FROM users",
        parameters=(),
        driver="AsyncpgDriver",
        adapter="AsyncpgConfig",
        bind_key=None,
        operation="SELECT",
        execution_mode="async",
        is_many=False,
        is_script=False,
        rows_affected=5,
        duration_s=0.123,
        correlation_id=None,
        db_system="postgresql",
    )

    observer(event)


def test_observer_handles_none_rows_affected(monkeypatch) -> None:
    """Verify observer handles None rows_affected gracefully."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    observer = prometheus.PrometheusStatementObserver()

    event = create_event(
        sql="SELECT 1",
        parameters=(),
        driver="AsyncpgDriver",
        adapter="AsyncpgConfig",
        bind_key=None,
        operation="SELECT",
        execution_mode="async",
        is_many=False,
        is_script=False,
        rows_affected=None,
        duration_s=0.01,
        correlation_id=None,
        db_system="postgresql",
    )

    observer(event)


def test_observer_handles_negative_duration(monkeypatch) -> None:
    """Verify observer clamps negative duration to zero."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    observer = prometheus.PrometheusStatementObserver()

    event = create_event(
        sql="SELECT 1",
        parameters=(),
        driver="AsyncpgDriver",
        adapter="AsyncpgConfig",
        bind_key=None,
        operation="SELECT",
        execution_mode="async",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=-0.01,
        correlation_id=None,
        db_system="postgresql",
    )

    observer(event)


def test_custom_namespace_and_subsystem(monkeypatch) -> None:
    """Verify custom namespace and subsystem are applied."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    observer = prometheus.PrometheusStatementObserver(namespace="myapp", subsystem="database")

    assert observer._label_names == ("db_system", "operation")  # pyright: ignore[reportPrivateUsage]


def test_custom_duration_buckets(monkeypatch) -> None:
    """Verify custom duration buckets are applied."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    custom_buckets = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
    observer = prometheus.PrometheusStatementObserver(duration_buckets=custom_buckets)

    event = create_event(
        sql="SELECT 1",
        parameters=(),
        driver="AsyncpgDriver",
        adapter="AsyncpgConfig",
        bind_key=None,
        operation="SELECT",
        execution_mode="async",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.05,
        correlation_id=None,
        db_system="postgresql",
    )

    observer(event)


def test_enable_metrics_with_custom_registry(monkeypatch) -> None:
    """Verify enable_metrics accepts custom registry."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    config = prometheus.enable_metrics(registry=None)
    assert config.statement_observers is not None


def test_enable_metrics_merges_with_base_config(monkeypatch) -> None:
    """Verify enable_metrics merges with existing config."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus
    from sqlspec.observability import ObservabilityConfig

    base = ObservabilityConfig(print_sql=True)
    config = prometheus.enable_metrics(base_config=base)

    assert config.print_sql is True
    assert config.statement_observers is not None
    assert len(config.statement_observers) == 1
