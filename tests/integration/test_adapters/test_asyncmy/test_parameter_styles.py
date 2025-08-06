"""Test parameter conversion and validation for AsyncMy driver.

This test suite validates that the SQLTransformer properly converts different
input parameter styles to the target QMARK format when necessary.
"""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver, asyncmy_statement_config
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL


@pytest.fixture
async def asyncmy_parameter_session(mysql_service: MySQLService) -> AsyncGenerator[AsyncmyDriver, None]:
    """Create an asyncmy session for parameter conversion testing."""
    config = AsyncmyConfig(
        pool_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,  # Enable autocommit for tests
        },
        statement_config=asyncmy_statement_config,
    )

    async with config.provide_session() as session:
        # Create test table
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_parameter_conversion (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT DEFAULT 0,
                description TEXT
            )
        """)

        # Clear any existing data
        await session.execute_script("TRUNCATE TABLE test_parameter_conversion")

        # Insert test data using ? placeholders (AsyncMy QMARK format)
        await session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)",
            ("test1", 100, "First test"),
        )
        await session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)",
            ("test2", 200, "Second test"),
        )
        await session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)", ("test3", 300, None)
        )

        yield session

        # Cleanup
        await session.execute_script("DROP TABLE IF EXISTS test_parameter_conversion")


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_qmark_style_no_conversion(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test that ? placeholders are used directly without conversion (target format)."""
    driver = asyncmy_parameter_session

    # Query using ? placeholders - should NOT require conversion
    result = await driver.execute("SELECT * FROM test_parameter_conversion WHERE name = ? AND value > ?", ("test1", 50))

    assert isinstance(result, SQLResult)
    assert result.rowcount == 1
    assert len(result.rows) == 1
    assert result.rows[0]["name"] == "test1"
    assert result.rows[0]["value"] == 100


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_pyformat_style_requires_conversion(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test that %s placeholders get converted to ? placeholders."""
    driver = asyncmy_parameter_session

    # Query using %s placeholders - SHOULD require conversion to ?
    # Note: This tests the SQLTransformer conversion logic
    result = await driver.execute(
        "SELECT * FROM test_parameter_conversion WHERE name = %s AND value > %s", ("test2", 150)
    )

    assert isinstance(result, SQLResult)
    assert result.rowcount == 1
    assert len(result.rows) == 1
    assert result.rows[0]["name"] == "test2"
    assert result.rows[0]["value"] == 200


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_named_pyformat_style_requires_conversion(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test that %(name)s placeholders get converted to ? placeholders."""
    driver = asyncmy_parameter_session

    # Query using %(name)s placeholders - SHOULD require conversion to ?
    result = await driver.execute(
        "SELECT * FROM test_parameter_conversion WHERE name = %(test_name)s AND value < %(max_value)s",
        {"test_name": "test3", "max_value": 350},
    )

    assert isinstance(result, SQLResult)
    assert result.rowcount == 1
    assert len(result.rows) == 1
    assert result.rows[0]["name"] == "test3"
    assert result.rows[0]["value"] == 300


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_sql_object_conversion_validation(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test parameter conversion with SQL object containing different parameter styles."""
    driver = asyncmy_parameter_session

    # Test SQL object with %s style - should convert to ?
    sql_pyformat = SQL("SELECT * FROM test_parameter_conversion WHERE value BETWEEN %s AND %s", parameters=[150, 250])
    result = await driver.execute(sql_pyformat)

    assert isinstance(result, SQLResult)
    assert result.rowcount == 1
    assert result.rows[0]["name"] == "test2"

    # Test SQL object with ? style - should use directly
    sql_qmark = SQL("SELECT * FROM test_parameter_conversion WHERE name = ? OR name = ?", parameters=["test1", "test3"])
    result2 = await driver.execute(sql_qmark)

    assert isinstance(result2, SQLResult)
    assert result2.rowcount == 2
    names = [row["name"] for row in result2.rows]
    assert "test1" in names
    assert "test3" in names


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_mixed_parameter_types_conversion(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test conversion with different parameter value types."""
    driver = asyncmy_parameter_session

    # Insert test data with different types using %s (should convert to ?)
    await driver.execute(
        "INSERT INTO test_parameter_conversion (name, value, description) VALUES (%s, %s, %s)",
        ("mixed_test", 999, "Mixed type test"),
    )

    # Query with NULL parameter using %s (should convert to ?)
    result = await driver.execute(
        "SELECT * FROM test_parameter_conversion WHERE description IS NOT NULL AND value = %s", (999,)
    )

    assert isinstance(result, SQLResult)
    assert result.rowcount == 1
    assert result.rows[0]["name"] == "mixed_test"
    assert result.rows[0]["description"] == "Mixed type test"


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_parameter_conversion(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test parameter conversion in execute_many operations."""
    driver = asyncmy_parameter_session

    # Test execute_many with %s placeholders - should convert to ?
    batch_data = [("batch1", 1000, "Batch test 1"), ("batch2", 2000, "Batch test 2"), ("batch3", 3000, "Batch test 3")]

    result = await driver.execute_many(
        "INSERT INTO test_parameter_conversion (name, value, description) VALUES (%s, %s, %s)", batch_data
    )

    assert isinstance(result, SQLResult)
    assert result.rowcount == 3

    # Verify the data was inserted correctly
    verify_result = await driver.execute(
        "SELECT COUNT(*) as count FROM test_parameter_conversion WHERE name LIKE ?", ("batch%",)
    )

    assert verify_result.rows[0]["count"] == 3


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_parameter_conversion_edge_cases(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test edge cases in parameter conversion."""
    driver = asyncmy_parameter_session

    # Empty parameter list with %s - should handle gracefully
    result = await driver.execute("SELECT COUNT(*) as total FROM test_parameter_conversion")
    assert result.rows[0]["total"] >= 3  # Our test data

    # Single parameter with %s conversion
    result2 = await driver.execute("SELECT * FROM test_parameter_conversion WHERE name = %s", ("test1",))
    assert result2.rowcount == 1
    assert result2.rows[0]["name"] == "test1"

    # Parameter with LIKE operation requiring conversion
    result3 = await driver.execute(
        "SELECT COUNT(*) as count FROM test_parameter_conversion WHERE name LIKE %s", ("test%",)
    )
    assert result3.rows[0]["count"] >= 3


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_parameter_style_consistency_validation(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test that the parameter conversion maintains consistency."""
    driver = asyncmy_parameter_session

    # Same query with different parameter styles should yield same results

    # Using ? (no conversion needed)
    result_qmark = await driver.execute(
        "SELECT name, value FROM test_parameter_conversion WHERE value >= ? ORDER BY value", (200,)
    )

    # Using %s (conversion needed)
    result_pyformat = await driver.execute(
        "SELECT name, value FROM test_parameter_conversion WHERE value >= %s ORDER BY value", (200,)
    )

    # Results should be identical
    assert result_qmark.rowcount == result_pyformat.rowcount
    assert len(result_qmark.rows) == len(result_pyformat.rows)

    for i in range(len(result_qmark.rows)):
        assert result_qmark.rows[i]["name"] == result_pyformat.rows[i]["name"]
        assert result_qmark.rows[i]["value"] == result_pyformat.rows[i]["value"]


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_complex_query_parameter_conversion(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test parameter conversion in complex queries with multiple operations."""
    driver = asyncmy_parameter_session

    # Insert additional test data
    await driver.execute_many(
        "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)",
        [("complex1", 150, "Complex test"), ("complex2", 250, "Complex test"), ("complex3", 350, "Complex test")],
    )

    # Complex query with subquery and multiple parameters using %s (should convert)
    result = await driver.execute(
        """
        SELECT name, value, description
        FROM test_parameter_conversion
        WHERE description = %s
        AND value BETWEEN %s AND %s
        AND name IN (
            SELECT name FROM test_parameter_conversion
            WHERE value > %s
        )
        ORDER BY value
        """,
        ("Complex test", 200, 300, 100),
    )

    assert isinstance(result, SQLResult)
    assert result.rowcount == 1
    assert result.rows[0]["name"] == "complex2"
    assert result.rows[0]["value"] == 250
