"""Integration tests for asyncpg driver implementation."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, Literal

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver, AsyncpgPoolConfig
from sqlspec.statement.result import ExecuteResult, SelectResult

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


@pytest.fixture
async def asyncpg_session(postgres_service: PostgresService) -> AsyncGenerator[AsyncpgDriver, None]:
    """Create an asyncpg session with test table."""
    config = AsyncpgConfig(
        pool_config=AsyncpgPoolConfig(
            dsn=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            min_size=1,
            max_size=5,
        ),
    )

    async with config.provide_session() as session:
        # Create test table
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        yield session
        # Cleanup
        await session.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_basic_crud(asyncpg_session: AsyncpgDriver) -> None:
    """Test basic CRUD operations."""
    # INSERT
    insert_result = await asyncpg_session.execute(
        "INSERT INTO test_table (name, value) VALUES ($1, $2)", ("test_name", 42)
    )
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    # SELECT
    select_result = await asyncpg_session.execute("SELECT name, value FROM test_table WHERE name = $1", ("test_name",))
    assert isinstance(select_result, SelectResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    # UPDATE
    update_result = await asyncpg_session.execute(
        "UPDATE test_table SET value = $1 WHERE name = $2", (100, "test_name")
    )
    assert isinstance(update_result, ExecuteResult)
    assert update_result.rows_affected == 1

    # Verify UPDATE
    verify_result = await asyncpg_session.execute("SELECT value FROM test_table WHERE name = $1", ("test_name",))
    assert isinstance(verify_result, SelectResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    # DELETE
    delete_result = await asyncpg_session.execute("DELETE FROM test_table WHERE name = $1", ("test_name",))
    assert isinstance(delete_result, ExecuteResult)
    assert delete_result.rows_affected == 1

    # Verify DELETE
    empty_result = await asyncpg_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SelectResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_value",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_value"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_parameter_styles(asyncpg_session: AsyncpgDriver, params: Any, style: ParamStyle) -> None:
    """Test different parameter binding styles."""
    # Insert test data
    await asyncpg_session.execute("INSERT INTO test_table (name) VALUES ($1)", ("test_value",))

    # Test parameter style
    if style == "tuple_binds":
        sql = "SELECT name FROM test_table WHERE name = $1"
    else:  # dict_binds
        sql = "SELECT name FROM test_table WHERE name = :name"

    result = await asyncpg_session.execute(sql, params)
    assert isinstance(result, SelectResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_value"


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_execute_many(asyncpg_session: AsyncpgDriver) -> None:
    """Test execute_many functionality."""
    params_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = await asyncpg_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", params_list)
    assert isinstance(result, ExecuteResult)
    assert result.rows_affected == len(params_list)

    # Verify all records were inserted
    select_result = await asyncpg_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SelectResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(params_list)

    # Verify data integrity
    ordered_result = await asyncpg_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(ordered_result, SelectResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_execute_script(asyncpg_session: AsyncpgDriver) -> None:
    """Test execute_script functionality."""
    script = """
        INSERT INTO test_table (name, value) VALUES ('script_test1', 999);
        INSERT INTO test_table (name, value) VALUES ('script_test2', 888);
        UPDATE test_table SET value = 1000 WHERE name = 'script_test1';
    """

    result = await asyncpg_session.execute_script(script)
    # Script execution typically returns a status string
    assert isinstance(result, str) or result is None

    # Verify script effects
    select_result = await asyncpg_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'script_test%' ORDER BY name"
    )
    assert isinstance(select_result, SelectResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2
    assert select_result.data[0]["name"] == "script_test1"
    assert select_result.data[0]["value"] == 1000
    assert select_result.data[1]["name"] == "script_test2"
    assert select_result.data[1]["value"] == 888


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_result_methods(asyncpg_session: AsyncpgDriver) -> None:
    """Test SelectResult and ExecuteResult methods."""
    # Insert test data
    await asyncpg_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES ($1, $2)", [("result1", 10), ("result2", 20), ("result3", 30)]
    )

    # Test SelectResult methods
    result = await asyncpg_session.execute("SELECT * FROM test_table ORDER BY name")
    assert isinstance(result, SelectResult)

    # Test get_first()
    first_row = result.get_first()
    assert first_row is not None
    assert first_row["name"] == "result1"

    # Test get_count()
    assert result.get_count() == 3

    # Test is_empty()
    assert not result.is_empty()

    # Test empty result
    empty_result = await asyncpg_session.execute("SELECT * FROM test_table WHERE name = $1", ("nonexistent",))
    assert isinstance(empty_result, SelectResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_error_handling(asyncpg_session: AsyncpgDriver) -> None:
    """Test error handling and exception propagation."""
    # Test invalid SQL
    with pytest.raises(Exception):  # asyncpg.PostgresSyntaxError
        await asyncpg_session.execute("INVALID SQL STATEMENT")

    # Test constraint violation
    await asyncpg_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("unique_test", 1))

    # Try to insert with invalid column reference
    with pytest.raises(Exception):  # asyncpg.UndefinedColumnError
        await asyncpg_session.execute("SELECT nonexistent_column FROM test_table")


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_data_types(asyncpg_session: AsyncpgDriver) -> None:
    """Test PostgreSQL data type handling."""
    # Create table with various PostgreSQL data types
    await asyncpg_session.execute_script("""
        CREATE TABLE data_types_test (
            id SERIAL PRIMARY KEY,
            text_col TEXT,
            integer_col INTEGER,
            numeric_col NUMERIC(10,2),
            boolean_col BOOLEAN,
            json_col JSONB,
            array_col INTEGER[],
            date_col DATE,
            timestamp_col TIMESTAMP,
            uuid_col UUID
        )
    """)

    # Insert data with various types
    await asyncpg_session.execute(
        """
        INSERT INTO data_types_test (
            text_col, integer_col, numeric_col, boolean_col, json_col,
            array_col, date_col, timestamp_col, uuid_col
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9
        )
    """,
        (
            "text_value",
            42,
            123.45,
            True,
            '{"key": "value"}',
            [1, 2, 3],
            "2024-01-15",
            "2024-01-15 10:30:00",
            "550e8400-e29b-41d4-a716-446655440000",
        ),
    )

    # Retrieve and verify data
    select_result = await asyncpg_session.execute(
        "SELECT text_col, integer_col, numeric_col, boolean_col, json_col, array_col FROM data_types_test"
    )
    assert isinstance(select_result, SelectResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["text_col"] == "text_value"
    assert row["integer_col"] == 42
    assert row["boolean_col"] is True
    assert row["array_col"] == [1, 2, 3]

    # Clean up
    await asyncpg_session.execute_script("DROP TABLE data_types_test")


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_transactions(asyncpg_session: AsyncpgDriver) -> None:
    """Test transaction behavior."""
    # PostgreSQL supports explicit transactions
    await asyncpg_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("transaction_test", 100))

    # Verify data is committed
    result = await asyncpg_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name = $1", ("transaction_test",)
    )
    assert isinstance(result, SelectResult)
    assert result.data is not None
    assert result.data[0]["count"] == 1


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_complex_queries(asyncpg_session: AsyncpgDriver) -> None:
    """Test complex SQL queries."""
    # Insert test data
    test_data = [
        ("Alice", 25),
        ("Bob", 30),
        ("Charlie", 35),
        ("Diana", 28),
    ]

    await asyncpg_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", test_data)

    # Test JOIN (self-join)
    join_result = await asyncpg_session.execute("""
        SELECT t1.name as name1, t2.name as name2, t1.value as value1, t2.value as value2
        FROM test_table t1
        CROSS JOIN test_table t2
        WHERE t1.value < t2.value
        ORDER BY t1.name, t2.name
        LIMIT 3
    """)
    assert isinstance(join_result, SelectResult)
    assert join_result.data is not None
    assert len(join_result.data) == 3

    # Test aggregation
    agg_result = await asyncpg_session.execute("""
        SELECT
            COUNT(*) as total_count,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM test_table
    """)
    assert isinstance(agg_result, SelectResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["total_count"] == 4
    assert agg_result.data[0]["avg_value"] == 29.5
    assert agg_result.data[0]["min_value"] == 25
    assert agg_result.data[0]["max_value"] == 35

    # Test subquery
    subquery_result = await asyncpg_session.execute("""
        SELECT name, value
        FROM test_table
        WHERE value > (SELECT AVG(value) FROM test_table)
        ORDER BY value
    """)
    assert isinstance(subquery_result, SelectResult)
    assert subquery_result.data is not None
    assert len(subquery_result.data) == 2  # Bob and Charlie
    assert subquery_result.data[0]["name"] == "Bob"
    assert subquery_result.data[1]["name"] == "Charlie"


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_schema_operations(asyncpg_session: AsyncpgDriver) -> None:
    """Test schema operations (DDL)."""
    # Create a new table
    await asyncpg_session.execute_script("""
        CREATE TABLE schema_test (
            id SERIAL PRIMARY KEY,
            description TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert data into new table
    insert_result = await asyncpg_session.execute(
        "INSERT INTO schema_test (description) VALUES ($1)", ("test description",)
    )
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    # Verify table structure
    info_result = await asyncpg_session.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'schema_test'
        ORDER BY ordinal_position
    """)
    assert isinstance(info_result, SelectResult)
    assert info_result.data is not None
    assert len(info_result.data) == 3  # id, description, created_at

    # Drop table
    await asyncpg_session.execute_script("DROP TABLE schema_test")


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_column_names_and_metadata(asyncpg_session: AsyncpgDriver) -> None:
    """Test column names and result metadata."""
    # Insert test data
    await asyncpg_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("metadata_test", 123))

    # Test column names
    result = await asyncpg_session.execute(
        "SELECT id, name, value, created_at FROM test_table WHERE name = $1", ("metadata_test",)
    )
    assert isinstance(result, SelectResult)
    assert result.column_names == ["id", "name", "value", "created_at"]
    assert result.data is not None
    assert len(result.data) == 1

    # Test that we can access data by column name
    row = result.data[0]
    assert row["name"] == "metadata_test"
    assert row["value"] == 123
    assert row["id"] is not None
    assert row["created_at"] is not None


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_with_schema_type(asyncpg_session: AsyncpgDriver) -> None:
    """Test asyncpg driver with schema type conversion."""
    from dataclasses import dataclass

    @dataclass
    class TestRecord:
        id: int | None
        name: str
        value: int

    # Insert test data
    await asyncpg_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("schema_test", 456))

    # Query with schema type
    result = await asyncpg_session.execute(
        "SELECT id, name, value FROM test_table WHERE name = $1", ("schema_test",), schema_type=TestRecord
    )

    assert isinstance(result, SelectResult)
    assert result.data is not None
    assert len(result.data) == 1

    # The data should be converted to the schema type by the ResultConverter
    assert result.column_names == ["id", "name", "value"]


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_performance_bulk_operations(asyncpg_session: AsyncpgDriver) -> None:
    """Test performance with bulk operations."""
    # Generate bulk data
    bulk_data = [(f"bulk_user_{i}", i * 10) for i in range(100)]

    # Bulk insert
    result = await asyncpg_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", bulk_data)
    assert isinstance(result, ExecuteResult)
    assert result.rows_affected == 100

    # Bulk select
    select_result = await asyncpg_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'bulk_user_%'"
    )
    assert isinstance(select_result, SelectResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    # Test pagination-like query
    page_result = await asyncpg_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'bulk_user_%' ORDER BY value LIMIT 10 OFFSET 20"
    )
    assert isinstance(page_result, SelectResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_20"


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_postgresql_specific_features(asyncpg_session: AsyncpgDriver) -> None:
    """Test PostgreSQL-specific features."""
    # Test RETURNING clause
    returning_result = await asyncpg_session.execute(
        "INSERT INTO test_table (name, value) VALUES ($1, $2) RETURNING id, name", ("returning_test", 999)
    )
    assert isinstance(returning_result, SelectResult)  # asyncpg returns SelectResult for RETURNING
    assert returning_result.data is not None
    assert len(returning_result.data) == 1
    assert returning_result.data[0]["name"] == "returning_test"

    # Test window functions
    await asyncpg_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES ($1, $2)", [("window1", 10), ("window2", 20), ("window3", 30)]
    )

    window_result = await asyncpg_session.execute("""
        SELECT
            name,
            value,
            ROW_NUMBER() OVER (ORDER BY value) as row_num,
            LAG(value) OVER (ORDER BY value) as prev_value
        FROM test_table
        WHERE name LIKE 'window%'
        ORDER BY value
    """)
    assert isinstance(window_result, SelectResult)
    assert window_result.data is not None
    assert len(window_result.data) == 3
    assert window_result.data[0]["row_num"] == 1
    assert window_result.data[0]["prev_value"] is None


@pytest.mark.xdist_group("postgres")
async def test_asyncpg_json_operations(asyncpg_session: AsyncpgDriver) -> None:
    """Test PostgreSQL JSON operations."""
    # Create table with JSONB column
    await asyncpg_session.execute_script("""
        CREATE TABLE json_test (
            id SERIAL PRIMARY KEY,
            data JSONB
        )
    """)

    # Insert JSON data
    json_data = '{"name": "test", "age": 30, "tags": ["postgres", "json"]}'
    await asyncpg_session.execute("INSERT INTO json_test (data) VALUES ($1)", (json_data,))

    # Test JSON queries
    json_result = await asyncpg_session.execute("SELECT data->>'name' as name, data->>'age' as age FROM json_test")
    assert isinstance(json_result, SelectResult)
    assert json_result.data is not None
    assert json_result.data[0]["name"] == "test"
    assert json_result.data[0]["age"] == "30"

    # Clean up
    await asyncpg_session.execute_script("DROP TABLE json_test")
