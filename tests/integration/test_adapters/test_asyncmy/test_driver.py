"""Integration tests for asyncmy driver implementation."""

from __future__ import annotations

import tempfile
from collections.abc import AsyncGenerator
from typing import Any, Literal

import pyarrow.parquet as pq
import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


@pytest.fixture
async def asyncmy_session(mysql_service: MySQLService) -> AsyncGenerator[AsyncmyDriver, None]:
    """Create an asyncmy session with test table."""
    config = AsyncmyConfig(
        host=mysql_service.host,
        port=mysql_service.port,
        user=mysql_service.user,
        password=mysql_service.password,
        database=mysql_service.db,
    )

    async with config.provide_session() as session:
        # Create test table
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        yield session
        # Cleanup
        await session.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_basic_crud(asyncmy_session: AsyncmyDriver) -> None:
    """Test basic CRUD operations."""
    # INSERT
    insert_result = await asyncmy_session.execute(
        "INSERT INTO test_table (name, value) VALUES (%s, %s)", ("test_name", 42)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # SELECT
    select_result = await asyncmy_session.execute("SELECT name, value FROM test_table WHERE name = %s", ("test_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    # UPDATE
    update_result = await asyncmy_session.execute(
        "UPDATE test_table SET value = %s WHERE name = %s", (100, "test_name")
    )
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    # Verify UPDATE
    verify_result = await asyncmy_session.execute("SELECT value FROM test_table WHERE name = %s", ("test_name",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    # DELETE
    delete_result = await asyncmy_session.execute("DELETE FROM test_table WHERE name = %s", ("test_name",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    # Verify DELETE
    empty_result = await asyncmy_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_value",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_value"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_parameter_styles(asyncmy_session: AsyncmyDriver, params: Any, style: ParamStyle) -> None:
    """Test different parameter binding styles."""
    # Insert test data
    await asyncmy_session.execute("INSERT INTO test_table (name) VALUES (%s)", ("test_value",))

    # Test parameter style
    if style == "tuple_binds":
        sql = "SELECT name FROM test_table WHERE name = %s"
    else:  # dict_binds
        sql = "SELECT name FROM test_table WHERE name = %(name)s"

    result = await asyncmy_session.execute(sql, params)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result) == 1
    assert result.data[0]["name"] == "test_value"


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many(asyncmy_session: AsyncmyDriver) -> None:
    """Test execute_many functionality."""
    params_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = await asyncmy_session.execute_many("INSERT INTO test_table (name, value) VALUES (%s, %s)", params_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(params_list)

    # Verify all records were inserted
    select_result = await asyncmy_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(params_list)

    # Verify data integrity
    ordered_result = await asyncmy_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(ordered_result, SQLResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_script(asyncmy_session: AsyncmyDriver) -> None:
    """Test execute_script functionality."""
    script = """
        INSERT INTO test_table (name, value) VALUES ('script_test1', 999);
        INSERT INTO test_table (name, value) VALUES ('script_test2', 888);
        UPDATE test_table SET value = 1000 WHERE name = 'script_test1';
    """

    result = await asyncmy_session.execute_script(script)
    # Script execution returns a SQLResult
    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"

    # Verify script effects
    select_result = await asyncmy_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'script_test%' ORDER BY name"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2
    assert select_result.data[0]["name"] == "script_test1"
    assert select_result.data[0]["value"] == 1000
    assert select_result.data[1]["name"] == "script_test2"
    assert select_result.data[1]["value"] == 888


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_result_methods(asyncmy_session: AsyncmyDriver) -> None:
    """Test SelectResult and ExecuteResult methods."""
    # Insert test data
    await asyncmy_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES (%s, %s)", [("result1", 10), ("result2", 20), ("result3", 30)]
    )

    # Test SelectResult methods
    result = await asyncmy_session.execute("SELECT * FROM test_table ORDER BY name")
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
    empty_result = await asyncmy_session.execute("SELECT * FROM test_table WHERE name = %s", ("nonexistent",))
    assert isinstance(empty_result, SQLResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_error_handling(asyncmy_session: AsyncmyDriver) -> None:
    """Test error handling and exception propagation."""
    # Test invalid SQL
    with pytest.raises(Exception):  # asyncmy.errors.ProgrammingError
        await asyncmy_session.execute("INVALID SQL STATEMENT")

    # Test constraint violation
    await asyncmy_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("unique_test", 1))

    # Try to insert with invalid column reference
    with pytest.raises(Exception):  # asyncmy.errors.ProgrammingError
        await asyncmy_session.execute("SELECT nonexistent_column FROM test_table")


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_data_types(asyncmy_session: AsyncmyDriver) -> None:
    """Test MySQL data type handling with asyncmy."""
    # Create table with various MySQL data types
    await asyncmy_session.execute_script("""
        CREATE TABLE data_types_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            text_col TEXT,
            varchar_col VARCHAR(255),
            int_col INT,
            decimal_col DECIMAL(10,2),
            boolean_col BOOLEAN,
            date_col DATE,
            datetime_col DATETIME,
            timestamp_col TIMESTAMP,
            json_col JSON
        )
    """)

    # Insert data with various types
    await asyncmy_session.execute(
        """
        INSERT INTO data_types_test (
            text_col, varchar_col, int_col, decimal_col, boolean_col,
            date_col, datetime_col, timestamp_col, json_col
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """,
        (
            "text_value",
            "varchar_value",
            42,
            123.45,
            True,
            "2024-01-15",
            "2024-01-15 10:30:00",
            "2024-01-15 10:30:00",
            '{"key": "value"}',
        ),
    )

    # Retrieve and verify data
    select_result = await asyncmy_session.execute(
        "SELECT text_col, varchar_col, int_col, decimal_col, boolean_col, json_col FROM data_types_test"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["text_col"] == "text_value"
    assert row["varchar_col"] == "varchar_value"
    assert row["int_col"] == 42
    assert row["boolean_col"] == 1  # MySQL returns boolean as 1/0

    # Clean up
    await asyncmy_session.execute_script("DROP TABLE data_types_test")


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_transactions(asyncmy_session: AsyncmyDriver) -> None:
    """Test transaction behavior."""
    # MySQL supports explicit transactions
    await asyncmy_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("transaction_test", 100))

    # Verify data is committed
    result = await asyncmy_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name = %s", ("transaction_test",)
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 1


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_complex_queries(asyncmy_session: AsyncmyDriver) -> None:
    """Test complex SQL queries."""
    # Insert test data
    test_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 28)]

    await asyncmy_session.execute_many("INSERT INTO test_table (name, value) VALUES (%s, %s)", test_data)

    # Test JOIN (self-join)
    join_result = await asyncmy_session.execute("""
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
    agg_result = await asyncmy_session.execute("""
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
    subquery_result = await asyncmy_session.execute("""
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


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_schema_operations(asyncmy_session: AsyncmyDriver) -> None:
    """Test schema operations (DDL)."""
    # Create a new table
    await asyncmy_session.execute_script("""
        CREATE TABLE schema_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            description TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert data into new table
    insert_result = await asyncmy_session.execute(
        "INSERT INTO schema_test (description) VALUES (%s)", ("test description",)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Verify table structure
    info_result = await asyncmy_session.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'schema_test'
        ORDER BY ORDINAL_POSITION
    """)
    assert isinstance(info_result, SQLResult)
    assert info_result.data is not None
    assert len(info_result.data) == 3  # id, description, created_at

    # Drop table
    await asyncmy_session.execute_script("DROP TABLE schema_test")


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_column_names_and_metadata(asyncmy_session: AsyncmyDriver) -> None:
    """Test column names and result metadata."""
    # Insert test data
    await asyncmy_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("metadata_test", 123))

    # Test column names
    result = await asyncmy_session.execute(
        "SELECT id, name, value, created_at FROM test_table WHERE name = %s", ("metadata_test",)
    )
    assert isinstance(result, SQLResult)
    assert result.column_names == ["id", "name", "value", "created_at"]
    assert result.data is not None
    assert len(result) == 1

    # Test that we can access data by column name
    row = result.data[0]
    assert row["name"] == "metadata_test"
    assert row["value"] == 123
    assert row["id"] is not None
    assert row["created_at"] is not None


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_with_schema_type(asyncmy_session: AsyncmyDriver) -> None:
    """Test asyncmy driver with schema type conversion."""
    from dataclasses import dataclass

    @dataclass
    class TestRecord:
        id: int | None
        name: str
        value: int

    # Insert test data
    await asyncmy_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("schema_test", 456))

    # Query with schema type
    result = await asyncmy_session.execute(
        "SELECT id, name, value FROM test_table WHERE name = %s", ("schema_test",), schema_type=TestRecord
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result) == 1

    # The data should be converted to the schema type by the ResultConverter
    assert result.column_names == ["id", "name", "value"]


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_performance_bulk_operations(asyncmy_session: AsyncmyDriver) -> None:
    """Test performance with bulk operations."""
    # Generate bulk data
    bulk_data = [(f"bulk_user_{i}", i * 10) for i in range(100)]

    # Bulk insert
    result = await asyncmy_session.execute_many("INSERT INTO test_table (name, value) VALUES (%s, %s)", bulk_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 100

    # Bulk select
    select_result = await asyncmy_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'bulk_user_%'"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    # Test pagination-like query
    page_result = await asyncmy_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'bulk_user_%' ORDER BY value LIMIT 10 OFFSET 20"
    )
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_20"


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_mysql_specific_features(asyncmy_session: AsyncmyDriver) -> None:
    """Test MySQL-specific features."""
    # Test MySQL functions
    mysql_result = await asyncmy_session.execute("SELECT VERSION() as version")
    assert isinstance(mysql_result, SQLResult)
    assert mysql_result.data is not None
    assert mysql_result.data[0]["version"] is not None

    # Test MySQL SHOW statements
    show_result = await asyncmy_session.execute("SHOW TABLES")
    assert isinstance(show_result, SQLResult)
    assert show_result.data is not None

    # Test ON DUPLICATE KEY UPDATE
    await asyncmy_session.execute_script("""
        CREATE TABLE unique_test (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            counter INT DEFAULT 1
        )
    """)

    # Insert initial record
    await asyncmy_session.execute("INSERT INTO unique_test (id, name) VALUES (%s, %s)", (1, "test"))

    # Use ON DUPLICATE KEY UPDATE
    update_result = await asyncmy_session.execute(
        """
        INSERT INTO unique_test (id, name, counter) VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE counter = counter + 1
    """,
        (1, "test", 1),
    )
    assert isinstance(update_result, SQLResult)

    # Verify the update
    verify_result = await asyncmy_session.execute("SELECT counter FROM unique_test WHERE id = %s", (1,))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["counter"] == 2

    # Clean up
    await asyncmy_session.execute_script("DROP TABLE unique_test")


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_json_operations(asyncmy_session: AsyncmyDriver) -> None:
    """Test MySQL JSON operations."""
    # Create table with JSON column
    await asyncmy_session.execute_script("""
        CREATE TABLE json_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            data JSON
        )
    """)

    # Insert JSON data
    json_data = '{"name": "test", "age": 30, "tags": ["mysql", "json"]}'
    await asyncmy_session.execute("INSERT INTO json_test (data) VALUES (%s)", (json_data,))

    # Test JSON queries using MySQL JSON functions
    json_result = await asyncmy_session.execute("""
        SELECT
            JSON_EXTRACT(data, '$.name') as name,
            JSON_EXTRACT(data, '$.age') as age,
            JSON_LENGTH(JSON_EXTRACT(data, '$.tags')) as tag_count
        FROM json_test
    """)
    assert isinstance(json_result, SQLResult)
    assert json_result.data is not None
    assert json_result.data[0]["name"] == '"test"'  # JSON_EXTRACT returns quoted strings
    assert json_result.data[0]["age"] == "30"  # JSON_EXTRACT returns strings
    assert json_result.data[0]["tag_count"] == 2

    # Clean up
    await asyncmy_session.execute_script("DROP TABLE json_test")


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_mysql_charset_collation(asyncmy_session: AsyncmyDriver) -> None:
    """Test MySQL charset and collation handling."""
    # Create table with specific charset
    await asyncmy_session.execute_script("""
        CREATE TABLE charset_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            text_utf8 TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        ) ENGINE=InnoDB
    """)

    # Insert data with Unicode characters
    unicode_text = "Hello 世界 🌍 café naïve résumé"
    await asyncmy_session.execute("INSERT INTO charset_test (text_utf8) VALUES (%s)", (unicode_text,))

    # Retrieve and verify Unicode data
    select_result = await asyncmy_session.execute("SELECT text_utf8 FROM charset_test")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["text_utf8"] == unicode_text

    # Clean up
    await asyncmy_session.execute_script("DROP TABLE charset_test")


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_fetch_arrow_table(asyncmy_session: AsyncmyDriver) -> None:
    """Integration test: fetch_arrow_table returns ArrowResult with correct pyarrow.Table."""
    await asyncmy_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("arrow1", 111))
    await asyncmy_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("arrow2", 222))
    statement = SQL("SELECT name, value FROM test_table ORDER BY name")
    result = await asyncmy_session.fetch_arrow_table(statement)
    assert isinstance(result, ArrowResult)
    assert result.num_rows == 2
    assert set(result.column_names) == {"name", "value"}
    names = result.data["name"].to_pylist()
    assert "arrow1" in names and "arrow2" in names


@pytest.mark.xdist_group("mysql")
async def test_asyncmy_to_parquet(asyncmy_session: AsyncmyDriver) -> None:
    """Integration test: to_parquet writes correct data to a Parquet file."""
    await asyncmy_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("pq1", 1))
    await asyncmy_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("pq2", 2))
    statement = SQL("SELECT name, value FROM test_table ORDER BY name")
    with tempfile.NamedTemporaryFile() as tmp:
        await asyncmy_session.export_to_storage(statement, destination_uri=tmp.name, format="parquet")  # type: ignore[attr-defined]
        table = pq.read_table(tmp.name)
        assert table.num_rows == 2
        assert set(table.column_names) == {"name", "value"}
        names = table.column("name").to_pylist()
        assert "pq1" in names and "pq2" in names
