"""Test parameter conversion and validation for AsyncMy driver.

This test suite validates that the SQLTransformer properly converts different
input parameter styles to the target MySQL PYFORMAT style when necessary.

AsyncMy Parameter Conversion Requirements:
- Input: QMARK (?) -> Output: PYFORMAT (%s)
- Input: NAMED (%(name)s) -> Output: PYFORMAT (%s)
- Input: PYFORMAT (%s) -> Output: PYFORMAT (%s) (no conversion)

This implements MySQL's 2-phase parameter processing with CORE_ROUND_3 architecture.
"""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver, asyncmy_statement_config
from sqlspec.core.result import SQLResult
from sqlspec.core.statement import SQL


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
            "autocommit": True,
        },
        statement_config=asyncmy_statement_config,
    )

    async with config.provide_session() as session:
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_parameter_conversion (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT DEFAULT 0,
                description TEXT
            )
        """)

        await session.execute_script("TRUNCATE TABLE test_parameter_conversion")

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

        await session.execute_script("DROP TABLE IF EXISTS test_parameter_conversion")


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_qmark_to_pyformat_conversion(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test that ? placeholders get converted to %s placeholders."""
    driver = asyncmy_parameter_session

    result = await driver.execute("SELECT * FROM test_parameter_conversion WHERE name = ? AND value > ?", ("test1", 50))

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"
    assert result.data[0]["value"] == 100


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_pyformat_no_conversion_needed(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test that %s placeholders are used directly without conversion (native format)."""
    driver = asyncmy_parameter_session

    result = await driver.execute(
        "SELECT * FROM test_parameter_conversion WHERE name = %s AND value > %s", ("test2", 150)
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test2"
    assert result.data[0]["value"] == 200


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_named_to_pyformat_conversion(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test that %(name)s placeholders get converted to %s placeholders."""
    driver = asyncmy_parameter_session

    result = await driver.execute(
        "SELECT * FROM test_parameter_conversion WHERE name = %(test_name)s AND value < %(max_value)s",
        {"test_name": "test3", "max_value": 350},
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test3"
    assert result.data[0]["value"] == 300


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_sql_object_conversion_validation(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test parameter conversion with SQL object containing different parameter styles."""
    driver = asyncmy_parameter_session

    sql_pyformat = SQL("SELECT * FROM test_parameter_conversion WHERE value BETWEEN %s AND %s", 150, 250)
    result = await driver.execute(sql_pyformat)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1
    assert result.data is not None
    assert result.data[0]["name"] == "test2"

    sql_qmark = SQL("SELECT * FROM test_parameter_conversion WHERE name = ? OR name = ?", "test1", "test3")
    result2 = await driver.execute(sql_qmark)

    assert isinstance(result2, SQLResult)
    assert result2.rows_affected == 2
    assert result2.data is not None
    names = [row["name"] for row in result2.data]
    assert "test1" in names
    assert "test3" in names


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_mixed_parameter_types_conversion(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test conversion with different parameter value types."""
    driver = asyncmy_parameter_session

    await driver.execute(
        "INSERT INTO test_parameter_conversion (name, value, description) VALUES (%s, %s, %s)",
        ("mixed_test", 999, "Mixed type test"),
    )

    result = await driver.execute(
        "SELECT * FROM test_parameter_conversion WHERE description IS NOT NULL AND value = %s", (999,)
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1
    assert result.data is not None
    assert result.data[0]["name"] == "mixed_test"
    assert result.data[0]["description"] == "Mixed type test"


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_parameter_conversion(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test parameter conversion in execute_many operations."""
    driver = asyncmy_parameter_session

    batch_data = [("batch1", 1000, "Batch test 1"), ("batch2", 2000, "Batch test 2"), ("batch3", 3000, "Batch test 3")]

    result = await driver.execute_many(
        "INSERT INTO test_parameter_conversion (name, value, description) VALUES (%s, %s, %s)", batch_data
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    verify_result = await driver.execute(
        "SELECT COUNT(*) as count FROM test_parameter_conversion WHERE name LIKE ?", ("batch%",)
    )

    assert verify_result.data is not None
    assert verify_result.data[0]["count"] == 3


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_parameter_conversion_edge_cases(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test edge cases in parameter conversion."""
    driver = asyncmy_parameter_session

    result = await driver.execute("SELECT COUNT(*) as total FROM test_parameter_conversion")
    assert result.data is not None
    assert result.data[0]["total"] >= 3

    result2 = await driver.execute("SELECT * FROM test_parameter_conversion WHERE name = %s", ("test1",))
    assert result2.rows_affected == 1
    assert result2.data is not None
    assert result2.data[0]["name"] == "test1"

    result3 = await driver.execute(
        "SELECT COUNT(*) as count FROM test_parameter_conversion WHERE name LIKE %s", ("test%",)
    )
    assert result3.data is not None
    assert result3.data[0]["count"] >= 3


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_parameter_style_consistency_validation(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test that the parameter conversion maintains consistency."""
    driver = asyncmy_parameter_session

    result_qmark = await driver.execute(
        "SELECT name, value FROM test_parameter_conversion WHERE value >= ? ORDER BY value", (200,)
    )

    result_pyformat = await driver.execute(
        "SELECT name, value FROM test_parameter_conversion WHERE value >= %s ORDER BY value", (200,)
    )

    assert result_qmark.rows_affected == result_pyformat.rows_affected
    assert result_qmark.data is not None
    assert result_pyformat.data is not None
    assert len(result_qmark.data) == len(result_pyformat.data)

    for i in range(len(result_qmark.data)):
        assert result_qmark.data[i]["name"] == result_pyformat.data[i]["name"]
        assert result_qmark.data[i]["value"] == result_pyformat.data[i]["value"]


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_complex_query_parameter_conversion(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test parameter conversion in complex queries with multiple operations."""
    driver = asyncmy_parameter_session

    await driver.execute_many(
        "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)",
        [("complex1", 150, "Complex test"), ("complex2", 250, "Complex test"), ("complex3", 350, "Complex test")],
    )

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
    assert result.rows_affected == 1
    assert result.data is not None
    assert result.data[0]["name"] == "complex2"
    assert result.data[0]["value"] == 250


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_mysql_parameter_style_specifics(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test MySQL-specific parameter handling requirements."""
    driver = asyncmy_parameter_session

    result = await driver.execute("SELECT name, value FROM test_parameter_conversion ORDER BY value LIMIT ?", (2,))
    assert result.rows_affected == 2
    assert len(result.get_data()) == 2

    result2 = await driver.execute(
        """
        SELECT name FROM test_parameter_conversion WHERE value = ?
        UNION
        SELECT name FROM test_parameter_conversion WHERE value = ?
        ORDER BY name
        """,
        (100, 200),
    )
    assert result2.rows_affected == 2

    await driver.execute(
        "REPLACE INTO test_parameter_conversion (id, name, value, description) VALUES (?, ?, ?, ?)",
        (999, "replace_test", 888, "Replaced entry"),
    )

    verify_result = await driver.execute("SELECT name, value FROM test_parameter_conversion WHERE id = ?", (999,))
    assert verify_result.data is not None
    assert verify_result.data[0]["name"] == "replace_test"
    assert verify_result.data[0]["value"] == 888


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_2phase_parameter_processing(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test the 2-phase parameter processing system specific to AsyncMy/MySQL."""
    driver = asyncmy_parameter_session

    test_cases = [
        ("SELECT * FROM test_parameter_conversion WHERE name = ? AND value = ?", ("test1", 100), "test1", 100),
        ("SELECT * FROM test_parameter_conversion WHERE name = %s AND value = %s", ("test2", 200), "test2", 200),
        (
            "SELECT * FROM test_parameter_conversion WHERE name = %(n)s AND value = %(v)s",
            {"n": "test3", "v": 300},
            "test3",
            300,
        ),
    ]

    for sql_text, params, expected_name, expected_value in test_cases:
        result = await driver.execute(sql_text, params)
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["name"] == expected_name
        assert result.data[0]["value"] == expected_value

    consistent_results = []
    for sql_text, params, _, _ in test_cases:
        result = await driver.execute(sql_text.replace("name = ", "name != ").replace("AND", "OR"), params)
        consistent_results.append(len(result.get_data()))

    assert all(count == consistent_results[0] for count in consistent_results)
