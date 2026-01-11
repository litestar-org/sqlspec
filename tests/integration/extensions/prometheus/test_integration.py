"""Integration tests for Prometheus metrics with real prometheus_client library."""

import tempfile

import pytest

from sqlspec.utils.module_loader import module_available

pytestmark = [
    pytest.mark.skipif(not module_available("prometheus_client"), reason="prometheus_client not installed"),
    pytest.mark.xdist_group("prometheus"),
]


def test_prometheus_metrics_with_sqlite() -> None:
    """E2E test with real prometheus_client and SQLite."""
    from prometheus_client import CollectorRegistry

    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.prometheus import enable_metrics

    registry = CollectorRegistry()

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name}, observability_config=enable_metrics(registry=registry)
        )

        with config.provide_session() as session:
            session.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
            session.execute("INSERT INTO test_table (name) VALUES ('Alice')")
            result = session.select_one("SELECT * FROM test_table WHERE id = ?", 1)

        assert result is not None

        # CREATE TABLE is classified as "DDL" operation type
        ddl_total = registry.get_sample_value(
            "sqlspec_driver_query_total", labels={"db_system": "sqlite", "operation": "DDL"}
        )
        assert ddl_total is not None
        assert ddl_total >= 1.0

        select_total = registry.get_sample_value(
            "sqlspec_driver_query_total", labels={"db_system": "sqlite", "operation": "SELECT"}
        )
        assert select_total is not None
        assert select_total >= 1.0


def test_prometheus_metrics_with_custom_labels() -> None:
    """Test Prometheus metrics with custom label configuration."""
    from prometheus_client import CollectorRegistry

    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.prometheus import enable_metrics

    registry = CollectorRegistry()

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name},
            observability_config=enable_metrics(registry=registry, label_names=("driver", "operation", "bind_key")),
        )

        with config.provide_session() as session:
            session.execute("SELECT 1")

        # Driver class name is "SqliteDriver" (not "SqliteSyncDriver")
        query_total = registry.get_sample_value(
            "sqlspec_driver_query_total",
            labels={"driver": "SqliteDriver", "operation": "SELECT", "bind_key": "default"},
        )
        assert query_total is not None
        assert query_total >= 1.0


def test_prometheus_duration_histogram() -> None:
    """Test that duration histogram records query execution time."""
    from prometheus_client import CollectorRegistry

    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.prometheus import enable_metrics

    registry = CollectorRegistry()

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name}, observability_config=enable_metrics(registry=registry)
        )

        with config.provide_session() as session:
            session.execute("SELECT 1")

        duration_count = registry.get_sample_value(
            "sqlspec_driver_query_duration_seconds_count", labels={"db_system": "sqlite", "operation": "SELECT"}
        )
        assert duration_count is not None
        assert duration_count >= 1.0

        duration_sum = registry.get_sample_value(
            "sqlspec_driver_query_duration_seconds_sum", labels={"db_system": "sqlite", "operation": "SELECT"}
        )
        assert duration_sum is not None
        assert duration_sum >= 0.0


def test_prometheus_rows_histogram() -> None:
    """Test that rows histogram records affected rows."""
    from prometheus_client import CollectorRegistry

    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.prometheus import enable_metrics

    registry = CollectorRegistry()

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name}, observability_config=enable_metrics(registry=registry)
        )

        with config.provide_session() as session:
            session.execute("CREATE TABLE test_rows (id INTEGER PRIMARY KEY)")
            session.execute("INSERT INTO test_rows (id) VALUES (1), (2), (3)")

        rows_count = registry.get_sample_value(
            "sqlspec_driver_query_rows_count", labels={"db_system": "sqlite", "operation": "INSERT"}
        )
        assert rows_count is not None
        assert rows_count >= 1.0


def test_prometheus_custom_namespace_and_subsystem() -> None:
    """Test Prometheus metrics with custom namespace and subsystem."""
    from prometheus_client import CollectorRegistry

    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.prometheus import enable_metrics

    registry = CollectorRegistry()

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name},
            observability_config=enable_metrics(registry=registry, namespace="myapp", subsystem="database"),
        )

        with config.provide_session() as session:
            session.execute("SELECT 1")

        query_total = registry.get_sample_value(
            "myapp_database_query_total", labels={"db_system": "sqlite", "operation": "SELECT"}
        )
        assert query_total is not None
        assert query_total >= 1.0


def test_prometheus_custom_duration_buckets() -> None:
    """Test Prometheus metrics with custom duration buckets."""
    from prometheus_client import CollectorRegistry

    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.prometheus import enable_metrics

    registry = CollectorRegistry()
    custom_buckets = (0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0)

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name},
            observability_config=enable_metrics(registry=registry, duration_buckets=custom_buckets),
        )

        with config.provide_session() as session:
            session.execute("SELECT 1")

        bucket_value = registry.get_sample_value(
            "sqlspec_driver_query_duration_seconds_bucket",
            labels={"db_system": "sqlite", "operation": "SELECT", "le": "0.05"},
        )
        assert bucket_value is not None


def test_prometheus_multiple_queries() -> None:
    """Test Prometheus metrics accumulate correctly across multiple queries."""
    from prometheus_client import CollectorRegistry

    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.prometheus import enable_metrics

    registry = CollectorRegistry()

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name}, observability_config=enable_metrics(registry=registry)
        )

        with config.provide_session() as session:
            for _ in range(5):
                session.execute("SELECT 1")

        query_total = registry.get_sample_value(
            "sqlspec_driver_query_total", labels={"db_system": "sqlite", "operation": "SELECT"}
        )
        assert query_total is not None
        assert query_total >= 5.0
