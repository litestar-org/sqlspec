"""Integration tests for aiosqlite driver implementation."""

from __future__ import annotations

import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, Literal

import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


@pytest.fixture
async def aiosqlite_session() -> AsyncGenerator[AiosqliteDriver, None]:
    """Create an aiosqlite session with test table."""
    config = AiosqliteConfig(database=":memory:")

    async with config.provide_session() as session:
        # Create test table
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        yield session
        # Cleanup is automatic with in-memory database


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_basic_crud(aiosqlite_session: AiosqliteDriver) -> None:
    """Test basic CRUD operations."""
    # INSERT
    insert_result = await aiosqlite_session.execute(
        "INSERT INTO test_table (name, value) VALUES (?, ?)", ("test_name", 42)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # SELECT
    select_result = await aiosqlite_session.execute("SELECT name, value FROM test_table WHERE name = ?", ("test_name"))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    # UPDATE
    update_result = await aiosqlite_session.execute(
        "UPDATE test_table SET value = ? WHERE name = ?", (100, "test_name")
    )
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    # Verify UPDATE
    verify_result = await aiosqlite_session.execute("SELECT value FROM test_table WHERE name = ?", ("test_name"))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    # DELETE
    delete_result = await aiosqlite_session.execute("DELETE FROM test_table WHERE name = ?", ("test_name"))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    # Verify DELETE
    empty_result = await aiosqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_value"), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_value"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_parameter_styles(aiosqlite_session: AiosqliteDriver, params: Any, style: ParamStyle) -> None:
    """Test different parameter binding styles."""
    # Insert test data
    await aiosqlite_session.execute("INSERT INTO test_table (name) VALUES (?)", ("test_value"))

    # Test parameter style
    if style == "tuple_binds":
        sql = "SELECT name FROM test_table WHERE name = ?"
    else:  # dict_binds
        sql = "SELECT name FROM test_table WHERE name = :name"

    result = await aiosqlite_session.execute(sql, params)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_value"


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_execute_many(aiosqlite_session: AiosqliteDriver) -> None:
    """Test execute_many functionality."""
    params_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = await aiosqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", params_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(params_list)

    # Verify all records were inserted
    select_result = await aiosqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(params_list)

    # Verify data integrity
    ordered_result = await aiosqlite_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(ordered_result, SQLResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_execute_script(aiosqlite_session: AiosqliteDriver) -> None:
    """Test execute_script functionality."""
    script = """
        INSERT INTO test_table (name, value) VALUES ('script_test1', 999);
        INSERT INTO test_table (name, value) VALUES ('script_test2', 888);
        UPDATE test_table SET value = 1000 WHERE name = 'script_test1';
    """

    result = await aiosqlite_session.execute_script(script)
    # Script execution now returns SQLResult object
    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"

    # Verify script effects
    select_result = await aiosqlite_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'script_test%' ORDER BY name"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2
    assert select_result.data[0]["name"] == "script_test1"
    assert select_result.data[0]["value"] == 1000
    assert select_result.data[1]["name"] == "script_test2"
    assert select_result.data[1]["value"] == 888


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_result_methods(aiosqlite_session: AiosqliteDriver) -> None:
    """Test SelectResult and ExecuteResult methods."""
    # Insert test data
    await aiosqlite_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES (?, ?)", [("result1", 10), ("result2", 20), ("result3", 30)]
    )

    # Test SelectResult methods
    result = await aiosqlite_session.execute("SELECT * FROM test_table ORDER BY name")
    assert isinstance(result, SQLResult)

    # Test get_first()
    first_row = result.get_first()
    assert first_row is not None
    assert first_row["name"] == "result1"

    # Test get_count()
    assert result.get_count() == 3

    # Test is_empty()
    assert not result.is_empty()

    # Test empty result
    empty_result = await aiosqlite_session.execute("SELECT * FROM test_table WHERE name = ?", ("nonexistent"))
    assert isinstance(empty_result, SQLResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_error_handling(aiosqlite_session: AiosqliteDriver) -> None:
    """Test error handling and exception propagation."""
    # Test invalid SQL
    with pytest.raises(Exception):  # aiosqlite.OperationalError
        await aiosqlite_session.execute("INVALID SQL STATEMENT")

    # Test constraint violation
    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("unique_test", 1))

    # Try to insert with invalid column reference
    with pytest.raises(Exception):  # aiosqlite.OperationalError
        await aiosqlite_session.execute("SELECT nonexistent_column FROM test_table")


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_data_types(aiosqlite_session: AiosqliteDriver) -> None:
    """Test SQLite data type handling with aiosqlite."""
    # Create table with various data types
    await aiosqlite_session.execute_script("""
        CREATE TABLE data_types_test (
            id INTEGER PRIMARY KEY,
            text_col TEXT,
            integer_col INTEGER,
            real_col REAL,
            blob_col BLOB,
            null_col TEXT
        )
    """)

    # Insert data with various types
    import math

    test_data = ("text_value", 42, math.pi, b"binary_data", None)

    insert_result = await aiosqlite_session.execute(
        "INSERT INTO data_types_test (text_col, integer_col, real_col, blob_col, null_col) VALUES (?, ?, ?, ?, ?)",
        test_data,
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Retrieve and verify data
    select_result = await aiosqlite_session.execute(
        "SELECT text_col, integer_col, real_col, blob_col, null_col FROM data_types_test"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["text_col"] == "text_value"
    assert row["integer_col"] == 42
    assert row["real_col"] == math.pi
    assert row["blob_col"] == b"binary_data"
    assert row["null_col"] is None

    # Clean up
    await aiosqlite_session.execute_script("DROP TABLE data_types_test")


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_transactions(aiosqlite_session: AiosqliteDriver) -> None:
    """Test transaction behavior."""
    # SQLite auto-commit mode test
    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("transaction_test", 100))

    # Verify data is committed
    result = await aiosqlite_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("transaction_test")
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 1


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_complex_queries(aiosqlite_session: AiosqliteDriver) -> None:
    """Test complex SQL queries."""
    # Insert test data
    test_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 28)]

    await aiosqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", test_data)

    # Test JOIN (self-join)
    join_result = await aiosqlite_session.execute("""
        SELECT t1.name as name1, t2.name as name2, t1.value as value1, t2.value as value2
        FROM test_table t1
        CROSS JOIN test_table t2
        WHERE t1.value < t2.value
        ORDER BY t1.name, t2.name
        LIMIT 3
    """)
    assert isinstance(join_result, SQLResult)
    assert join_result.data is not None
    assert len(join_result.data) == 3

    # Test aggregation
    agg_result = await aiosqlite_session.execute("""
        SELECT
            COUNT(*) as total_count,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM test_table
    """)
    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["total_count"] == 4
    assert agg_result.data[0]["avg_value"] == 29.5
    assert agg_result.data[0]["min_value"] == 25
    assert agg_result.data[0]["max_value"] == 35

    # Test subquery
    subquery_result = await aiosqlite_session.execute("""
        SELECT name, value
        FROM test_table
        WHERE value > (SELECT AVG(value) FROM test_table)
        ORDER BY value
    """)
    assert isinstance(subquery_result, SQLResult)
    assert subquery_result.data is not None
    assert len(subquery_result.data) == 2  # Bob and Charlie
    assert subquery_result.data[0]["name"] == "Bob"
    assert subquery_result.data[1]["name"] == "Charlie"


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_schema_operations(aiosqlite_session: AiosqliteDriver) -> None:
    """Test schema operations (DDL)."""
    # Create a new table
    create_result = await aiosqlite_session.execute_script("""
        CREATE TABLE schema_test (
            id INTEGER PRIMARY KEY,
            description TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    assert isinstance(create_result, SQLResult)
    assert create_result.operation_type == "SCRIPT"

    # Insert data into new table
    insert_result = await aiosqlite_session.execute(
        "INSERT INTO schema_test (description) VALUES (?)", ("test description")
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Verify table structure
    pragma_result = await aiosqlite_session.execute("PRAGMA table_info(schema_test)")
    assert isinstance(pragma_result, SQLResult)
    assert pragma_result.data is not None
    assert len(pragma_result.data) == 3  # id, description, created_at

    # Drop table
    drop_result = await aiosqlite_session.execute_script("DROP TABLE schema_test")
    assert isinstance(drop_result, SQLResult)
    assert drop_result.operation_type == "SCRIPT"


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_column_names_and_metadata(aiosqlite_session: AiosqliteDriver) -> None:
    """Test column names and result metadata."""
    # Insert test data
    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("metadata_test", 123))

    # Test column names
    result = await aiosqlite_session.execute(
        "SELECT id, name, value, created_at FROM test_table WHERE name = ?", ("metadata_test")
    )
    assert isinstance(result, SQLResult)
    assert result.column_names == ["id", "name", "value", "created_at"]
    assert result.data is not None
    assert len(result.data) == 1

    # Test that we can access data by column name
    row = result.data[0]
    assert row["name"] == "metadata_test"
    assert row["value"] == 123
    assert row["id"] is not None
    assert row["created_at"] is not None


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_with_schema_type(aiosqlite_session: AiosqliteDriver) -> None:
    """Test aiosqlite driver with schema type conversion."""
    from dataclasses import dataclass

    @dataclass
    class TestRecord:
        id: int | None
        name: str
        value: int

    # Insert test data
    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("schema_test", 456))

    # Query with schema type
    result = await aiosqlite_session.execute(
        "SELECT id, name, value FROM test_table WHERE name = ?", ("schema_test"), schema_type=TestRecord
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    # The data should be converted to the schema type by the ResultConverter
    assert result.column_names == ["id", "name", "value"]


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_performance_bulk_operations(aiosqlite_session: AiosqliteDriver) -> None:
    """Test performance with bulk operations."""
    # Generate bulk data
    bulk_data = [(f"bulk_user_{i}", i * 10) for i in range(100)]

    # Bulk insert
    result = await aiosqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", bulk_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 100

    # Bulk select
    select_result = await aiosqlite_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'bulk_user_%'"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    # Test pagination-like query
    page_result = await aiosqlite_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'bulk_user_%' ORDER BY value LIMIT 10 OFFSET 20"
    )
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_20"


@pytest.mark.xdist_group("aiosqlite")
async def test_aiosqlite_sqlite_specific_features(aiosqlite_session: AiosqliteDriver) -> None:
    """Test SQLite-specific features with aiosqlite."""
    # Test PRAGMA statements
    pragma_result = await aiosqlite_session.execute("PRAGMA user_version")
    assert isinstance(pragma_result, SQLResult)
    assert pragma_result.data is not None

    # Test SQLite functions
    sqlite_result = await aiosqlite_session.execute("SELECT sqlite_version() as version")
    assert isinstance(sqlite_result, SQLResult)
    assert sqlite_result.data is not None
    assert sqlite_result.data[0]["version"] is not None

    # Test JSON operations (if JSON1 extension is available)
    try:
        json_result = await aiosqlite_session.execute("SELECT json('{}') as json_test")
        assert isinstance(json_result, SQLResult)
        assert json_result.data is not None
    except Exception:
        # JSON1 extension might not be available
        pass
    non_strict_config = SQLConfig(enable_parsing=False, enable_validation=False)

    await aiosqlite_session.execute("ATTACH DATABASE ':memory:' AS temp_db", _config=non_strict_config)
    await aiosqlite_session.execute(
        "CREATE TABLE temp_db.temp_table (id INTEGER, name TEXT)", _config=non_strict_config
    )
    await aiosqlite_session.execute("INSERT INTO temp_db.temp_table VALUES (1, 'temp')", _config=non_strict_config)

    temp_result = await aiosqlite_session.execute("SELECT * FROM temp_db.temp_table")
    assert isinstance(temp_result, SQLResult)
    assert temp_result.data is not None
    assert len(temp_result.data) == 1
    assert temp_result.data[0]["name"] == "temp"

    try:
        await aiosqlite_session.execute("DETACH DATABASE temp_db", _config=non_strict_config)
    except Exception:
        # Database might be locked, which is fine for this test
        pass


@pytest.mark.asyncio
async def test_aiosqlite_fetch_arrow_table(aiosqlite_session: AiosqliteDriver) -> None:
    """Integration test: fetch_arrow_table returns ArrowResult with correct pyarrow.Table."""
    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("arrow1", 111))
    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("arrow2", 222))
    statement = SQL("SELECT name, value FROM test_table ORDER BY name")
    result = await aiosqlite_session.fetch_arrow_table(statement)
    assert isinstance(result, ArrowResult)
    assert result.num_rows == 2
    assert set(result.column_names) == {"name", "value"}
    assert result.data is not None
    table = result.data
    names = table["name"].to_pylist()
    assert "arrow1" in names and "arrow2" in names


@pytest.mark.asyncio
async def test_aiosqlite_to_parquet(aiosqlite_session: AiosqliteDriver) -> None:
    """Integration test: to_parquet writes correct data to a Parquet file."""
    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("pq1", 123))
    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("pq2", 456))
    statement = SQL("SELECT name, value FROM test_table ORDER BY name")
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "partitioned_data"
        await aiosqlite_session.export_to_storage(statement, destination_uri=str(output_path), format="parquet")
        table = pq.read_table(f"{output_path}.parquet")
        assert table.num_rows == 2
        assert set(table.column_names) == {"name", "value"}
        names = table.column("name").to_pylist()
        assert "pq1" in names and "pq2" in names
