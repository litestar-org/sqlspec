"""Unit tests for Prometheus metrics extension."""

import pytest

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
    assert isinstance(observer, prometheus.PrometheusStatementObserver)
    assert observer._label_names == ("db_system", "operation")  # pyright: ignore[reportPrivateUsage]


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


def test_observer_accumulates_per_label_set_and_exports_rows_only_when_reported(monkeypatch) -> None:
    """Repeated events accumulate on one series per label set; row series appear once rows are reported."""
    _force_dependency(monkeypatch, "prometheus_client")

    from prometheus_client import CollectorRegistry

    from sqlspec.extensions import prometheus

    registry = CollectorRegistry()
    observer = prometheus.PrometheusStatementObserver(registry=registry)

    def _observe(operation: str, rows_affected: "int | None") -> None:
        observer(
            create_event(
                sql="SELECT 1",
                parameters=(),
                driver="AsyncpgDriver",
                adapter="AsyncpgConfig",
                bind_key=None,
                operation=operation,
                execution_mode="async",
                is_many=False,
                is_script=False,
                rows_affected=rows_affected,
                duration_s=0.25,
                correlation_id=None,
                db_system="postgresql",
            )
        )

    select_labels = {"db_system": "postgresql", "operation": "SELECT"}
    insert_labels = {"db_system": "postgresql", "operation": "INSERT"}

    _observe("SELECT", None)
    _observe("SELECT", None)
    assert registry.get_sample_value("sqlspec_driver_query_total", select_labels) == 2.0
    assert registry.get_sample_value("sqlspec_driver_query_duration_seconds_sum", select_labels) == 0.5
    assert registry.get_sample_value("sqlspec_driver_query_rows_count", select_labels) is None

    _observe("SELECT", 3)
    _observe("SELECT", 4)
    _observe("INSERT", 1)
    assert registry.get_sample_value("sqlspec_driver_query_total", select_labels) == 4.0
    assert registry.get_sample_value("sqlspec_driver_query_duration_seconds_count", select_labels) == 4.0
    assert registry.get_sample_value("sqlspec_driver_query_rows_count", select_labels) == 2.0
    assert registry.get_sample_value("sqlspec_driver_query_rows_sum", select_labels) == 7.0
    assert registry.get_sample_value("sqlspec_driver_query_total", insert_labels) == 1.0
    assert registry.get_sample_value("sqlspec_driver_query_rows_sum", insert_labels) == 1.0


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


@pytest.mark.parametrize(
    "bad_label",
    [
        "sql",
        "parameters",
        "correlation_id",
        "duration_s",
        "sql_hash",
        "trace_id",
        "span_id",
        "started_at",
        "rows_affected",
        "custom_attribute",
    ],
)
def test_rejects_disallowed_and_high_cardinality_labels(monkeypatch, bad_label: str) -> None:
    """Reject labels outside the bounded low-cardinality allow-list."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    with pytest.raises(ValueError, match="High-cardinality Prometheus label names are not allowed"):
        prometheus.PrometheusStatementObserver(label_names=("db_system", bad_label))


def test_allows_all_bounded_low_cardinality_labels(monkeypatch) -> None:
    """Allow every bounded label in the explicit allow-list."""
    _force_dependency(monkeypatch, "prometheus_client")

    from sqlspec.extensions import prometheus

    allowed = (
        "driver",
        "db_system",
        "operation",
        "execution_mode",
        "is_many",
        "is_script",
        "storage_backend",
        "transaction_state",
        "prepared_statement",
        "adapter",
        "bind_key",
    )
    observer = prometheus.PrometheusStatementObserver(label_names=allowed)
    event = create_event(
        sql="SELECT 1",
        parameters=(),
        driver="SqliteDriver",
        adapter="SqliteConfig",
        bind_key="analytics",
        operation="SELECT",
        execution_mode="sync",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.01,
        correlation_id=None,
        db_system="sqlite",
        storage_backend="local",
        transaction_state="autocommit",
        prepared_statement=True,
    )

    values = observer._label_values(event)  # pyright: ignore[reportPrivateUsage]
    assert values == (
        "SqliteDriver",
        "sqlite",
        "SELECT",
        "sync",
        "False",
        "False",
        "local",
        "autocommit",
        "True",
        "SqliteConfig",
        "analytics",
    )
