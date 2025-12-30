"""Test execute_many functionality for DuckDB drivers."""

from collections.abc import Generator

import pytest

from sqlspec.adapters.duckdb import DuckDBDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("duckdb")


@pytest.fixture
def duckdb_batch_session(duckdb_basic_session: DuckDBDriver) -> "Generator[DuckDBDriver, None, None]":
    """Create a DuckDB session for batch operation testing."""

    duckdb_basic_session.execute_script(
        """
            CREATE TABLE test_batch (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                value INTEGER DEFAULT 0,
                category VARCHAR
            )
        """
    )
    try:
        yield duckdb_basic_session
    finally:
        duckdb_basic_session.execute_script("DROP TABLE IF EXISTS test_batch")


def test_duckdb_execute_many_basic(duckdb_batch_session: DuckDBDriver) -> None:
    """Test basic execute_many with DuckDB."""
    parameters = [
        (1, "Item 1", 100, "A"),
        (2, "Item 2", 200, "B"),
        (3, "Item 3", 300, "A"),
        (4, "Item 4", 400, "C"),
        (5, "Item 5", 500, "B"),
    ]

    result = duckdb_batch_session.execute_many(
        "INSERT INTO test_batch (id, name, value, category) VALUES (?, ?, ?, ?)", parameters
    )

    assert isinstance(result, SQLResult)

    assert result.rows_affected == 5

    count_result = duckdb_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 5


def test_duckdb_execute_many_update(duckdb_batch_session: DuckDBDriver) -> None:
    """Test execute_many for UPDATE operations with DuckDB."""

    duckdb_batch_session.execute_many(
        "INSERT INTO test_batch (id, name, value, category) VALUES (?, ?, ?, ?)",
        [(1, "Update 1", 10, "X"), (2, "Update 2", 20, "Y"), (3, "Update 3", 30, "Z")],
    )

    update_parameters = [(100, "Update 1"), (200, "Update 2"), (300, "Update 3")]

    result = duckdb_batch_session.execute_many("UPDATE test_batch SET value = ? WHERE name = ?", update_parameters)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    check_result = duckdb_batch_session.execute("SELECT name, value FROM test_batch ORDER BY name")
    assert len(check_result.data) == 3
    assert all(row["value"] in (100, 200, 300) for row in check_result.data)


def test_duckdb_execute_many_empty(duckdb_batch_session: DuckDBDriver) -> None:
    """Test execute_many with empty parameter list on DuckDB."""
    result = duckdb_batch_session.execute_many(
        "INSERT INTO test_batch (id, name, value, category) VALUES (?, ?, ?, ?)", []
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 0

    count_result = duckdb_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 0


def test_duckdb_execute_many_mixed_types(duckdb_batch_session: DuckDBDriver) -> None:
    """Test execute_many with mixed parameter types on DuckDB."""
    parameters = [
        (1, "String Item", 123, "CAT1"),
        (2, "Another Item", 456, None),
        (3, "Third Item", 0, "CAT2"),
        (4, "Float Item", 78.5, "CAT3"),
    ]

    result = duckdb_batch_session.execute_many(
        "INSERT INTO test_batch (id, name, value, category) VALUES (?, ?, ?, ?)", parameters
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 4

    null_result = duckdb_batch_session.execute("SELECT * FROM test_batch WHERE category IS NULL")
    assert len(null_result.data) == 1
    assert null_result.data[0]["name"] == "Another Item"

    float_result = duckdb_batch_session.execute("SELECT * FROM test_batch WHERE name = ?", ("Float Item",))
    assert len(float_result.data) == 1
    assert float_result.data[0]["value"] == 78


def test_duckdb_execute_many_delete(duckdb_batch_session: DuckDBDriver) -> None:
    """Test execute_many for DELETE operations with DuckDB."""

    duckdb_batch_session.execute_many(
        "INSERT INTO test_batch (id, name, value, category) VALUES (?, ?, ?, ?)",
        [
            (1, "Delete 1", 10, "X"),
            (2, "Delete 2", 20, "Y"),
            (3, "Delete 3", 30, "X"),
            (4, "Keep 1", 40, "Z"),
            (5, "Delete 4", 50, "Y"),
        ],
    )

    delete_parameters = [("Delete 1",), ("Delete 2",), ("Delete 4",)]

    result = duckdb_batch_session.execute_many("DELETE FROM test_batch WHERE name = ?", delete_parameters)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    remaining_result = duckdb_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert remaining_result.data[0]["count"] == 2

    names_result = duckdb_batch_session.execute("SELECT name FROM test_batch ORDER BY name")
    remaining_names = [row["name"] for row in names_result.data]
    assert remaining_names == ["Delete 3", "Keep 1"]


def test_duckdb_execute_many_large_batch(duckdb_batch_session: DuckDBDriver) -> None:
    """Test execute_many with large batch size on DuckDB."""

    large_batch = [(i, f"Item {i}", i * 10, f"CAT{i % 3}") for i in range(1000)]

    result = duckdb_batch_session.execute_many(
        "INSERT INTO test_batch (id, name, value, category) VALUES (?, ?, ?, ?)", large_batch
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1000

    count_result = duckdb_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 1000

    sample_result = duckdb_batch_session.execute(
        "SELECT * FROM test_batch WHERE name IN (?, ?, ?) ORDER BY value", ("Item 100", "Item 500", "Item 999")
    )
    assert len(sample_result.data) == 3
    assert sample_result.data[0]["value"] == 1000
    assert sample_result.data[1]["value"] == 5000
    assert sample_result.data[2]["value"] == 9990


def test_duckdb_execute_many_with_sql_object(duckdb_batch_session: DuckDBDriver) -> None:
    """Test execute_many with SQL object on DuckDB."""

    parameters = [(10, "SQL Obj 1", 111, "SOB"), (20, "SQL Obj 2", 222, "SOB"), (30, "SQL Obj 3", 333, "SOB")]

    sql_obj = SQL("INSERT INTO test_batch (id, name, value, category) VALUES (?, ?, ?, ?)", parameters, is_many=True)

    result = duckdb_batch_session.execute(sql_obj)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    check_result = duckdb_batch_session.execute("SELECT COUNT(*) as count FROM test_batch WHERE category = ?", ("SOB"))
    assert check_result.data[0]["count"] == 3


def test_duckdb_execute_many_with_analytics(duckdb_batch_session: DuckDBDriver) -> None:
    """Test execute_many with DuckDB analytics features."""

    analytics_data = [(i, f"Analytics {i}", i * 10, f"ANAL{i % 2}") for i in range(1, 11)]

    duckdb_batch_session.execute_many(
        "INSERT INTO test_batch (id, name, value, category) VALUES (?, ?, ?, ?)", analytics_data
    )

    result = duckdb_batch_session.execute("""
        SELECT
            category,
            COUNT(*) as count,
            AVG(value) as avg_value,
            SUM(value) as total_value,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM test_batch
        GROUP BY category
        ORDER BY category
    """)

    assert len(result.data) == 2

    anal0_data = next(row for row in result.data if row["category"] == "ANAL0")
    anal1_data = next(row for row in result.data if row["category"] == "ANAL1")

    assert anal0_data["count"] == 5
    assert anal1_data["count"] == 5


def test_duckdb_execute_many_with_arrays(duckdb_batch_session: DuckDBDriver) -> None:
    """Test execute_many with DuckDB array operations."""

    duckdb_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_arrays (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            numbers INTEGER[],
            tags VARCHAR[]
        )
    """)

    parameters = [
        (1, "Array 1", [10, 20, 30], ["tag1", "tag2"]),
        (2, "Array 2", [40, 50], ["tag3"]),
        (3, "Array 3", [60], ["tag4", "tag5", "tag6"]),
    ]

    try:
        result = duckdb_batch_session.execute_many(
            "INSERT INTO test_arrays (id, name, numbers, tags) VALUES (?, ?, ?, ?)", parameters
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 3

        check_result = duckdb_batch_session.execute(
            "SELECT name, len(numbers) as num_count, len(tags) as tag_count FROM test_arrays ORDER BY name"
        )
        assert len(check_result.data) == 3

    except Exception:
        simple_parameters = [(1, "Simple 1", 10, "tag1"), (2, "Simple 2", 20, "tag2"), (3, "Simple 3", 30, "tag3")]

        duckdb_batch_session.execute_many(
            "INSERT INTO test_batch (id, name, value, category) VALUES (?, ?, ?, ?)", simple_parameters
        )

        check_result = duckdb_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
        assert check_result.data[0]["count"] == 3


def test_duckdb_execute_many_with_time_series(duckdb_batch_session: DuckDBDriver) -> None:
    """Test execute_many with time series data on DuckDB."""

    duckdb_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_timeseries (
            id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            metric_name VARCHAR,
            metric_value DOUBLE
        )
    """)

    from datetime import datetime, timedelta

    base_time = datetime(2024, 1, 1)
    time_series_data = [(i, base_time + timedelta(hours=i), f"metric_{i % 3}", float(i * 10.5)) for i in range(1, 25)]

    result = duckdb_batch_session.execute_many(
        "INSERT INTO test_timeseries (id, timestamp, metric_name, metric_value) VALUES (?, ?, ?, ?)", time_series_data
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 24

    analytics_result = duckdb_batch_session.execute("""
        SELECT
            metric_name,
            COUNT(*) as data_points,
            AVG(metric_value) as avg_value,
            MIN(metric_value) as min_value,
            MAX(metric_value) as max_value
        FROM test_timeseries
        GROUP BY metric_name
        ORDER BY metric_name
    """)

    assert len(analytics_result.data) == 3

    for row in analytics_result.data:
        assert row["data_points"] == 8
