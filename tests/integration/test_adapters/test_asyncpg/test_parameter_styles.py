"""Test different parameter styles for AsyncPG drivers."""

import math
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.core.result import SQLResult


@pytest.fixture(scope="function")
async def asyncpg_parameters_session(postgres_service: PostgresService) -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create an AsyncPG session for parameter style testing.

    Optimized to avoid connection pool exhaustion.
    """
    config = AsyncpgConfig(
        pool_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
            "min_size": 1,  # Minimal pool size
            "max_size": 3,  # Very small pool to conserve connections
        }
    )

    try:
        async with config.provide_session() as session:
            # Create test table efficiently
            await session.execute_script("""
                DROP TABLE IF EXISTS test_parameters CASCADE;
                CREATE TABLE test_parameters (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    description TEXT
                );
                -- Insert all test data in one go
                INSERT INTO test_parameters (name, value, description) VALUES
                    ('test1', 100, 'First test'),
                    ('test2', 200, 'Second test'),
                    ('test3', 300, NULL),
                    ('alpha', 50, 'Alpha test'),
                    ('beta', 75, 'Beta test'),
                    ('gamma', 250, 'Gamma test');
            """)

            yield session
    finally:
        # Ensure pool is closed properly to avoid threading issues during test shutdown
        if config.pool_instance:
            await config.close_pool()


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
@pytest.mark.parametrize(
    "parameters,expected_count",
    [
        (("test1",), 1),  # Tuple parameter
        (["test1"], 1),  # List parameter
    ],
)
async def test_asyncpg_numeric_parameter_types(
    asyncpg_parameters_session: AsyncpgDriver, parameters: Any, expected_count: int
) -> None:
    """Test different parameter types with AsyncPG numeric style."""
    result = await asyncpg_parameters_session.execute("SELECT * FROM test_parameters WHERE name = $1", parameters)

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) == expected_count
    if expected_count > 0:
        assert result[0]["name"] == "test1"


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_numeric_parameter_style(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test PostgreSQL numeric parameter style with AsyncPG."""
    result = await asyncpg_parameters_session.execute("SELECT * FROM test_parameters WHERE name = $1", ("test1",))

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) == 1
    assert result[0]["name"] == "test1"


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_multiple_parameters_numeric(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test queries with multiple parameters using numeric style."""
    result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE value >= $1 AND value <= $2 ORDER BY value", (50, 150)
    )

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) == 3  # alpha(50), beta(75), test1(100)
    assert result[0]["value"] == 50  # First in order (alpha)
    assert result[1]["value"] == 75  # Second in order (beta)
    assert result[2]["value"] == 100  # Third in order (test1)


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_null_parameters(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test handling of NULL parameters on AsyncPG."""
    # Query for NULL values
    result = await asyncpg_parameters_session.execute("SELECT * FROM test_parameters WHERE description IS NULL")

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) == 1
    assert result[0]["name"] == "test3"
    assert result[0]["description"] is None

    # Test inserting NULL with parameters
    await asyncpg_parameters_session.execute(
        "INSERT INTO test_parameters (name, value, description) VALUES ($1, $2, $3)", ("null_param_test", 400, None)
    )

    null_result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name = $1", ("null_param_test",)
    )
    assert len(null_result) == 1
    assert null_result[0]["description"] is None


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_parameter_escaping(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameter escaping prevents SQL injection."""
    # This should safely search for a literal string with quotes
    malicious_input = "'; DROP TABLE test_parameters; --"

    result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name = $1", (malicious_input,)
    )

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) == 0  # No matches, but table should still exist

    # Verify table still exists by counting all records
    count_result = await asyncpg_parameters_session.execute("SELECT COUNT(*) as count FROM test_parameters")
    assert count_result[0]["count"] >= 3  # Our test data should still be there


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_parameter_with_like(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with LIKE operations."""
    result = await asyncpg_parameters_session.execute("SELECT * FROM test_parameters WHERE name LIKE $1", ("test%",))

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) >= 3  # test1, test2, test3

    # Test with more specific pattern
    specific_result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name LIKE $1", ("test1%",)
    )
    assert len(specific_result) == 1
    assert specific_result[0]["name"] == "test1"


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_parameter_with_any_array(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with PostgreSQL ANY and arrays."""
    # Insert additional test data with unique names
    await asyncpg_parameters_session.execute_many(
        "INSERT INTO test_parameters (name, value, description) VALUES ($1, $2, $3)",
        [("delta", 10, "Delta test"), ("epsilon", 20, "Epsilon test"), ("zeta", 30, "Zeta test")],
    )

    # Test ANY with array parameter - use names we know exist
    result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name = ANY($1) ORDER BY name", (["alpha", "beta", "test1"],)
    )

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) == 3  # Should find alpha, beta, and test1 from initial data
    assert result[0]["name"] == "alpha"
    assert result[1]["name"] == "beta"
    assert result[2]["name"] == "test1"


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_parameter_with_sql_object(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with SQL object."""
    from sqlspec.core.statement import SQL

    # Test with numeric style - parameters must be included in SQL object constructor
    sql_obj = SQL("SELECT * FROM test_parameters WHERE value > $1", [150])
    result = await asyncpg_parameters_session.execute(sql_obj)

    assert isinstance(result, SQLResult)
    assert result is not None
    assert len(result) >= 1
    assert all(row["value"] > 150 for row in result)


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_parameter_data_types(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test different parameter data types with AsyncPG."""
    # Create table for different data types
    await asyncpg_parameters_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_types (
            id SERIAL PRIMARY KEY,
            int_val INTEGER,
            real_val REAL,
            text_val TEXT,
            bool_val BOOLEAN,
            array_val INTEGER[]
        )
    """)

    # Test different data types
    test_data = [
        (42, math.pi, "hello", True, [1, 2, 3]),
        (-100, -2.5, "world", False, [4, 5, 6]),
        (0, 0.0, "", None, []),
    ]

    for data in test_data:
        await asyncpg_parameters_session.execute(
            "INSERT INTO test_types (int_val, real_val, text_val, bool_val, array_val) VALUES ($1, $2, $3, $4, $5)",
            data,
        )

    # Verify data with parameters
    result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_types WHERE int_val = $1 AND real_val = $2", (42, math.pi)
    )

    assert len(result) == 1
    assert result[0]["text_val"] == "hello"
    assert result[0]["bool_val"] is True
    assert result[0]["array_val"] == [1, 2, 3]


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_parameter_edge_cases(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test edge cases for AsyncPG parameters."""
    # Empty string parameter
    await asyncpg_parameters_session.execute(
        "INSERT INTO test_parameters (name, value, description) VALUES ($1, $2, $3)", ("", 999, "Empty name test")
    )

    empty_result = await asyncpg_parameters_session.execute("SELECT * FROM test_parameters WHERE name = $1", ("",))
    assert len(empty_result) == 1
    assert empty_result[0]["value"] == 999

    # Very long string parameter
    long_string = "x" * 1000
    await asyncpg_parameters_session.execute(
        "INSERT INTO test_parameters (name, value, description) VALUES ($1, $2, $3)", ("long_test", 1000, long_string)
    )

    long_result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE description = $1", (long_string,)
    )
    assert len(long_result) == 1
    assert len(long_result[0]["description"]) == 1000


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_parameter_with_postgresql_functions(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with PostgreSQL functions."""
    # Test with string functions
    result = await asyncpg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE LENGTH(name) > $1 AND UPPER(name) LIKE $2", (4, "TEST%")
    )

    assert isinstance(result, SQLResult)
    assert result is not None
    # Should find test1, test2, test3 (all have length > 4 and start with "test")
    assert len(result) >= 3

    # Test with math functions using PostgreSQL ::cast syntax
    math_result = await asyncpg_parameters_session.execute(
        "SELECT name, value, ROUND((value * $1::FLOAT)::NUMERIC, 2) as multiplied FROM test_parameters WHERE value >= $2",
        (1.5, 100),
    )
    assert len(math_result) >= 3
    # Now we can test the actual math results since casting should work
    for row in math_result:
        expected = round(row["value"] * 1.5, 2)
        multiplied_value = float(row["multiplied"])
        # Casting fix allows float parameters to work correctly!
        assert multiplied_value == expected


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_parameter_with_json(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with PostgreSQL JSON operations."""
    # Create table with JSONB column
    await asyncpg_parameters_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_json (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB
        );
        TRUNCATE TABLE test_json RESTART IDENTITY;
    """)

    import json

    # Test inserting JSON data with parameters
    json_data = [
        ("JSON 1", {"type": "test", "value": 100, "active": True}),
        ("JSON 2", {"type": "prod", "value": 200, "active": False}),
        ("JSON 3", {"type": "test", "value": 300, "tags": ["a", "b"]}),
    ]

    for name, metadata in json_data:
        await asyncpg_parameters_session.execute(
            "INSERT INTO test_json (name, metadata) VALUES ($1, $2)", (name, json.dumps(metadata))
        )

    # Test querying JSON with parameters (PostgreSQL ::cast syntax should now work)
    result = await asyncpg_parameters_session.execute(
        "SELECT name, metadata->>'type' as type, (metadata->>'value')::INTEGER as value FROM test_json WHERE metadata->>'type' = $1",
        ("test",),
    )

    assert len(result) == 2  # JSON 1 and JSON 3
    assert all(row["type"] == "test" for row in result)


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_parameter_with_arrays(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with PostgreSQL array operations."""
    # Create table with array columns
    await asyncpg_parameters_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_arrays (
            id SERIAL PRIMARY KEY,
            name TEXT,
            tags TEXT[],
            scores INTEGER[]
        );
        TRUNCATE TABLE test_arrays RESTART IDENTITY;
    """)

    # Test inserting array data with parameters
    array_data = [
        ("Array 1", ["tag1", "tag2"], [10, 20, 30]),
        ("Array 2", ["tag3"], [40, 50]),
        ("Array 3", ["tag4", "tag5", "tag6"], [60]),
    ]

    for name, tags, scores in array_data:
        await asyncpg_parameters_session.execute(
            "INSERT INTO test_arrays (name, tags, scores) VALUES ($1, $2, $3)", (name, tags, scores)
        )

    # Test querying arrays with parameters
    result = await asyncpg_parameters_session.execute("SELECT name FROM test_arrays WHERE $1 = ANY(tags)", ("tag2",))

    assert len(result) == 1
    assert result[0]["name"] == "Array 1"

    # Test array length with parameters
    length_result = await asyncpg_parameters_session.execute(
        "SELECT name FROM test_arrays WHERE array_length(scores, 1) > $1", (1,)
    )
    assert len(length_result) == 2  # Array 1 and Array 2


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_parameter_with_window_functions(asyncpg_parameters_session: AsyncpgDriver) -> None:
    """Test parameters with PostgreSQL window functions."""
    # Insert some test data for window functions
    await asyncpg_parameters_session.execute_many(
        "INSERT INTO test_parameters (name, value, description) VALUES ($1, $2, $3)",
        [
            ("window1", 50, "Group A"),
            ("window2", 75, "Group A"),
            ("window3", 25, "Group B"),
            ("window4", 100, "Group B"),
        ],
    )

    # Test window function with parameter
    result = await asyncpg_parameters_session.execute(
        """
        SELECT
            name,
            value,
            description,
            ROW_NUMBER() OVER (PARTITION BY description ORDER BY value) as row_num
        FROM test_parameters
        WHERE value > $1
        ORDER BY description, value
    """,
        (30,),
    )

    assert len(result) >= 4
    # Verify window function worked correctly
    group_a_rows = [row for row in result if row["description"] == "Group A"]
    assert len(group_a_rows) == 2
    assert group_a_rows[0]["row_num"] == 1  # First in partition
    assert group_a_rows[1]["row_num"] == 2  # Second in partition
