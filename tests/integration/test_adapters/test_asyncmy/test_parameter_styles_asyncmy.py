"""Test parameter conversion and validation for AsyncMy driver.

This test suite validates that the SQLTransformer properly converts different
input parameter styles to the target MySQL PYFORMAT style when necessary.

AsyncMy Parameter Conversion Requirements:
- Input: QMARK (?) -> Output: PYFORMAT (%s)
- Input: NAMED (%(name)s) -> Output: PYFORMAT (%s)
- Input: PYFORMAT (%s) -> Output: PYFORMAT (%s) (no conversion)

This implements MySQL's 2-phase parameter processing.
"""

import math
from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver, default_statement_config
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("mysql")


@pytest.fixture
async def asyncmy_parameter_session(mysql_service: MySQLService) -> AsyncGenerator[AsyncmyDriver, None]:
    """Create an asyncmy session for parameter conversion testing."""
    config = AsyncmyConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
        },
        statement_config=default_statement_config,
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


async def test_asyncmy_none_parameters_pyformat(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test None values with PYFORMAT (%s) parameter style."""
    driver = asyncmy_parameter_session

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_none_values (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            value INT,
            description TEXT,
            flag BOOLEAN,
            created_at DATETIME
        )
    """)

    await driver.execute_script("TRUNCATE TABLE test_none_values")

    params = ("test_none", None, "Test with None value", None, None)
    result = await driver.execute(
        "INSERT INTO test_none_values (name, value, description, flag, created_at) VALUES (%s, %s, %s, %s, %s)", params
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    select_result = await driver.execute("SELECT * FROM test_none_values WHERE name = %s", ("test_none",))

    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    row = select_result.data[0]
    assert row["name"] == "test_none"
    assert row["value"] is None
    assert row["description"] == "Test with None value"
    assert row["flag"] is None
    assert row["created_at"] is None


async def test_asyncmy_none_parameters_qmark(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test None values with QMARK (?) parameter style."""
    driver = asyncmy_parameter_session

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_none_qmark (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            value INT,
            optional_field VARCHAR(100)
        )
    """)

    await driver.execute_script("TRUNCATE TABLE test_none_qmark")

    params = ("qmark_test", None, None)
    result = await driver.execute("INSERT INTO test_none_qmark (name, value, optional_field) VALUES (?, ?, ?)", params)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    verify_result = await driver.execute("SELECT * FROM test_none_qmark WHERE name = ?", ("qmark_test",))

    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    row = verify_result.data[0]
    assert row["name"] == "qmark_test"
    assert row["value"] is None
    assert row["optional_field"] is None


async def test_asyncmy_none_parameters_named_pyformat(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test None values with named PYFORMAT %(name)s parameter style."""
    driver = asyncmy_parameter_session

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_none_named (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            status VARCHAR(50),
            priority INT,
            metadata JSON
        )
    """)

    await driver.execute_script("TRUNCATE TABLE test_none_named")

    params = {"title": "Named test", "status": None, "priority": 5, "metadata": None}

    result = await driver.execute(
        """
        INSERT INTO test_none_named (title, status, priority, metadata)
        VALUES (%(title)s, %(status)s, %(priority)s, %(metadata)s)
    """,
        params,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    verify_result = await driver.execute(
        "SELECT * FROM test_none_named WHERE title = %(search_title)s", {"search_title": "Named test"}
    )

    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    row = verify_result.data[0]
    assert row["title"] == "Named test"
    assert row["status"] is None
    assert row["priority"] == 5
    assert row["metadata"] is None


async def test_asyncmy_all_none_parameters(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test when all parameter values are None."""
    driver = asyncmy_parameter_session

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_all_none (
            id INT AUTO_INCREMENT PRIMARY KEY,
            col1 VARCHAR(255),
            col2 INT,
            col3 BOOLEAN,
            col4 TEXT
        )
    """)

    await driver.execute_script("TRUNCATE TABLE test_all_none")

    params = (None, None, None, None)
    result = await driver.execute("INSERT INTO test_all_none (col1, col2, col3, col4) VALUES (?, ?, ?, ?)", params)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    last_id = result.last_inserted_id
    assert last_id is not None

    verify_result = await driver.execute("SELECT * FROM test_all_none WHERE id = ?", (last_id,))

    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    row = verify_result.data[0]
    assert row["col1"] is None
    assert row["col2"] is None
    assert row["col3"] is None
    assert row["col4"] is None


async def test_asyncmy_none_with_execute_many(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test None values work correctly with execute_many."""
    driver = asyncmy_parameter_session

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_none_many (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            value INT,
            category VARCHAR(100)
        )
    """)

    await driver.execute_script("TRUNCATE TABLE test_none_many")

    batch_data = [
        ("item1", 100, "A"),
        ("item2", None, "B"),  # None value
        ("item3", 300, None),  # None category
        (None, 400, "D"),  # None name
        ("item5", None, None),  # Multiple None values
    ]

    result = await driver.execute_many(
        "INSERT INTO test_none_many (name, value, category) VALUES (?, ?, ?)", batch_data
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 5

    verify_result = await driver.execute("SELECT * FROM test_none_many ORDER BY id")

    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 5

    rows = verify_result.data
    assert rows[0]["name"] == "item1" and rows[0]["value"] == 100 and rows[0]["category"] == "A"
    assert rows[1]["name"] == "item2" and rows[1]["value"] is None and rows[1]["category"] == "B"
    assert rows[2]["name"] == "item3" and rows[2]["value"] == 300 and rows[2]["category"] is None
    assert rows[3]["name"] is None and rows[3]["value"] == 400 and rows[3]["category"] == "D"
    assert rows[4]["name"] == "item5" and rows[4]["value"] is None and rows[4]["category"] is None


async def test_asyncmy_none_parameter_count_validation(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test that parameter count mismatches are properly detected with None values.

    This test verifies that None values don't cause parameter count validation to fail.
    """
    driver = asyncmy_parameter_session

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_param_validation (
            id INT AUTO_INCREMENT PRIMARY KEY,
            col1 VARCHAR(255),
            col2 INT
        )
    """)

    await driver.execute_script("TRUNCATE TABLE test_param_validation")

    # Test: Correct parameter count with None should work
    result = await driver.execute("INSERT INTO test_param_validation (col1, col2) VALUES (?, ?)", ("valid", None))
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Test: Too many parameters should raise an error (even with None)
    try:
        await driver.execute(
            "INSERT INTO test_param_validation (col1, col2) VALUES (?, ?)",  # 2 placeholders
            ("value", None, "extra"),  # 3 parameters
        )
        assert False, "Expected parameter count error"
    except Exception as e:
        error_msg = str(e).lower()
        # MySQL/AsyncMy typically reports parameter count errors
        assert any(keyword in error_msg for keyword in ["parameter", "argument", "mismatch", "count"])

    # Test: Too few parameters should raise an error
    try:
        await driver.execute(
            "INSERT INTO test_param_validation (col1, col2) VALUES (?, ?)",  # 2 placeholders
            ("only_one",),  # 1 parameter
        )
        assert False, "Expected parameter count error"
    except Exception as e:
        error_msg = str(e).lower()
        assert any(keyword in error_msg for keyword in ["parameter", "argument", "mismatch", "count"])


async def test_asyncmy_none_in_where_clauses(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test None values in WHERE clauses work correctly."""
    driver = asyncmy_parameter_session

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_none_where (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            category VARCHAR(100),
            status VARCHAR(50)
        )
    """)

    await driver.execute_script("TRUNCATE TABLE test_none_where")

    test_data = [
        ("item1", "A", "active"),
        ("item2", None, "inactive"),  # None category
        ("item3", "B", None),  # None status
        ("item4", None, None),  # Both None
    ]

    await driver.execute_many("INSERT INTO test_none_where (name, category, status) VALUES (?, ?, ?)", test_data)

    # Test WHERE with IS NULL for None values
    result = await driver.execute("SELECT * FROM test_none_where WHERE category IS NULL")

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 2  # item2 and item4

    found_names = {row["name"] for row in result.data}
    assert found_names == {"item2", "item4"}

    # Test parameterized query with None (should handle NULL comparison properly)
    result2 = await driver.execute("SELECT * FROM test_none_where WHERE status = ? OR ? IS NULL", (None, None))

    # The second condition should be TRUE since None IS NULL in SQL context
    assert isinstance(result2, SQLResult)
    assert result2.data is not None
    assert len(result2.data) == 4  # All rows because second condition is always true


async def test_asyncmy_none_complex_scenarios(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test complex scenarios with None parameters."""
    driver = asyncmy_parameter_session

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_complex_none (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            score INT,
            factor DECIMAL(10,2),
            active BOOLEAN,
            tags JSON,
            created_at TIMESTAMP,
            metadata TEXT
        )
    """)

    await driver.execute_script("TRUNCATE TABLE test_complex_none")

    # Test complex insert with mixed None and valid values
    params = {
        "name": "complex_test",
        "score": None,
        "factor": math.pi,
        "active": None,
        "tags": '["tag1", "tag2"]',
        "created_at": None,
        "metadata": None,
    }

    result = await driver.execute(
        """
        INSERT INTO test_complex_none (name, score, factor, active, tags, created_at, metadata)
        VALUES (%(name)s, %(score)s, %(factor)s, %(active)s, %(tags)s, %(created_at)s, %(metadata)s)
    """,
        params,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Verify complex insert
    verify_result = await driver.execute("SELECT * FROM test_complex_none WHERE name = ?", ("complex_test",))

    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    row = verify_result.data[0]
    assert row["name"] == "complex_test"
    assert row["score"] is None
    assert abs(float(row["factor"]) - math.pi) < 0.01  # Decimal comparison
    assert row["active"] is None
    assert row["tags"] == '["tag1", "tag2"]' or row["tags"] == ["tag1", "tag2"]  # JSON field
    assert row["created_at"] is None
    assert row["metadata"] is None


async def test_asyncmy_none_edge_cases(asyncmy_parameter_session: AsyncmyDriver) -> None:
    """Test edge cases that might reveal None parameter handling bugs."""
    driver = asyncmy_parameter_session

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_edge_cases (
            id INT AUTO_INCREMENT PRIMARY KEY,
            a VARCHAR(255),
            b VARCHAR(255),
            c VARCHAR(255),
            d INT,
            e BOOLEAN
        )
    """)

    await driver.execute_script("TRUNCATE TABLE test_edge_cases")

    # Test 1: Single None parameter
    await driver.execute("INSERT INTO test_edge_cases (a) VALUES (?)", (None,))

    # Test 2: Multiple consecutive None parameters
    await driver.execute(
        "INSERT INTO test_edge_cases (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", (None, None, None, None, None)
    )

    # Test 3: None at different positions
    test_cases = [
        (None, "middle", "end", 1, True),  # None at start
        ("start", None, "end", 2, False),  # None at middle
        ("start", "middle", None, 3, None),  # None at end
        (None, None, "end", None, True),  # Multiple None at start
        ("start", None, None, 4, None),  # Multiple None at end
    ]

    for params in test_cases:
        await driver.execute("INSERT INTO test_edge_cases (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", params)

    # Verify all rows were inserted
    count_result = await driver.execute("SELECT COUNT(*) as total FROM test_edge_cases")
    assert count_result.data is not None
    assert count_result.data[0]["total"] == 7  # 2 initial + 5 test cases
