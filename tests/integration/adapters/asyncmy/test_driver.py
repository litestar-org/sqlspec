"""Integration tests for asyncmy driver implementation.

This serves as a comprehensive test template for database drivers,
covering all core functionality including CRUD operations, parameter styles,
transaction management, and error handling.
"""

import pytest

from sqlspec import StatementStack, sql
from sqlspec.adapters.asyncmy import AsyncmyDriver

pytestmark = pytest.mark.xdist_group("mysql")


@pytest.fixture
async def asyncmy_driver(asyncmy_clean_driver: AsyncmyDriver) -> AsyncmyDriver:
    """Create and manage test table lifecycle."""

    create_sql = """
        CREATE TABLE IF NOT EXISTS test_table_asyncmy (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    await asyncmy_clean_driver.execute_script(create_sql)
    await asyncmy_clean_driver.execute_script("DELETE FROM test_table_asyncmy")

    return asyncmy_clean_driver


async def test_asyncmy_statement_stack_continue_on_error(asyncmy_driver: AsyncmyDriver) -> None:
    """Continue-on-error should still work with sequential fallback."""

    await asyncmy_driver.execute_script("DELETE FROM test_table_asyncmy")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table_asyncmy (id, name, value) VALUES (?, ?, ?)", (1, "mysql-initial", 5))
        .push_execute("INSERT INTO test_table_asyncmy (id, name, value) VALUES (?, ?, ?)", (1, "mysql-duplicate", 15))
        .push_execute("INSERT INTO test_table_asyncmy (id, name, value) VALUES (?, ?, ?)", (2, "mysql-final", 25))
    )

    results = await asyncmy_driver.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[0].rows_affected == 1
    assert results[1].error is not None
    assert results[2].rows_affected == 1

    verify = await asyncmy_driver.execute(
        "SELECT COUNT(*) AS total FROM test_table_asyncmy WHERE name LIKE ?", ("mysql-%",)
    )
    assert verify.get_data()[0]["total"] == 2


async def test_asyncmy_error_handling(asyncmy_driver: AsyncmyDriver) -> None:
    """Test error handling and exception wrapping."""
    driver = asyncmy_driver

    with pytest.raises(Exception):
        await driver.execute("INVALID SQL STATEMENT")

    await driver.execute("INSERT INTO test_table_asyncmy (id, name, value) VALUES (?, ?, ?)", (1, "user1", 100))

    with pytest.raises(Exception):
        await driver.execute("INSERT INTO test_table_asyncmy (id, name, value) VALUES (?, ?, ?)", (1, "user2", 200))


async def test_asyncmy_mysql_specific_features(asyncmy_driver: AsyncmyDriver) -> None:
    """Test MySQL-specific features and SQL constructs."""
    driver = asyncmy_driver

    await driver.execute(
        "INSERT INTO test_table_asyncmy (id, name, value) VALUES (?, ?, ?)", (1, "duplicate_test", 100)
    )

    _ = await driver.execute(
        """INSERT INTO test_table_asyncmy (id, name, value) VALUES (?, ?, ?) AS new
           ON DUPLICATE KEY UPDATE value = new.value + 50""",
        (1, "duplicate_test_updated", 200),
    )

    select_result = await driver.execute("SELECT name, value FROM test_table_asyncmy WHERE id = ?", (1,))
    assert select_result.get_data()[0]["value"] == 250


async def test_asyncmy_edge_cases(asyncmy_driver: AsyncmyDriver) -> None:
    """Test edge cases and boundary conditions."""
    driver = asyncmy_driver

    result = await driver.execute("SELECT 1 as test_col", ())
    assert len(result.get_data()) == 1
    assert result.get_data()[0]["test_col"] == 1

    result = await driver.execute("SELECT ? as param_value", (42,))
    assert result.get_data()[0]["param_value"] == 42

    data_with_nulls = [("user1", 100), ("user2", None), ("user3", 300)]

    result = await driver.execute_many("INSERT INTO test_table_asyncmy (name, value) VALUES (?, ?)", data_with_nulls)
    assert result.num_rows == 3

    select_result = await driver.execute(
        "SELECT name, value FROM test_table_asyncmy WHERE name IN (?, ?, ?) ORDER BY name", ("user1", "user2", "user3")
    )
    assert len(select_result.get_data()) == 3
    assert select_result.get_data()[1]["value"] is None


async def test_asyncmy_result_metadata(asyncmy_driver: AsyncmyDriver) -> None:
    """Test SQL result metadata and properties."""
    driver = asyncmy_driver

    insert_result = await driver.execute(
        "INSERT INTO test_table_asyncmy (name, value) VALUES (?, ?)", ("metadata_test", 500)
    )
    assert insert_result.num_rows == 1
    assert insert_result.operation_type == "INSERT"
    assert insert_result.column_names is None or len(insert_result.column_names) == 0

    select_result = await driver.execute(
        "SELECT id, name, value FROM test_table_asyncmy WHERE name = ?", ("metadata_test",)
    )
    assert select_result.num_rows == 1
    assert select_result.operation_type == "SELECT"
    assert select_result.column_names == ["id", "name", "value"]
    assert len(select_result.get_data()) == 1

    empty_result = await driver.execute("SELECT * FROM test_table_asyncmy WHERE name = ?", ("nonexistent",))
    assert empty_result.num_rows == 0
    assert empty_result.operation_type == "SELECT"
    assert len(empty_result.get_data()) == 0


async def test_asyncmy_for_update_locking(asyncmy_driver: AsyncmyDriver) -> None:
    """Test FOR UPDATE row locking with MySQL."""

    driver = asyncmy_driver

    # Insert test data
    await driver.execute("INSERT INTO test_table_asyncmy (name, value) VALUES (?, ?)", ("mysql_lock", 100))

    try:
        await driver.begin()

        # Test basic FOR UPDATE
        result = await driver.select_one(
            sql.select("id", "name", "value").from_("test_table_asyncmy").where_eq("name", "mysql_lock").for_update()
        )
        assert result is not None
        assert result["name"] == "mysql_lock"
        assert result["value"] == 100

        await driver.commit()
    except Exception:
        await driver.rollback()
        raise


async def test_asyncmy_for_update_skip_locked(asyncmy_driver: AsyncmyDriver) -> None:
    """Test FOR UPDATE SKIP LOCKED with MySQL (MySQL 8.0+ feature)."""

    driver = asyncmy_driver

    # Insert test data
    await driver.execute("INSERT INTO test_table_asyncmy (name, value) VALUES (?, ?)", ("mysql_skip", 200))

    try:
        await driver.begin()

        # Test FOR UPDATE SKIP LOCKED
        result = await driver.select_one(
            sql.select("*").from_("test_table_asyncmy").where_eq("name", "mysql_skip").for_update(skip_locked=True)
        )
        assert result is not None
        assert result["name"] == "mysql_skip"

        await driver.commit()
    except Exception:
        await driver.rollback()
        raise


async def test_asyncmy_for_share_locking(asyncmy_driver: AsyncmyDriver) -> None:
    """Test FOR SHARE row locking with MySQL."""

    driver = asyncmy_driver

    # Insert test data
    await driver.execute("INSERT INTO test_table_asyncmy (name, value) VALUES (?, ?)", ("mysql_share", 300))

    try:
        await driver.begin()

        # Test basic FOR SHARE (MySQL uses FOR SHARE syntax like PostgreSQL)
        result = await driver.select_one(
            sql.select("id", "name", "value").from_("test_table_asyncmy").where_eq("name", "mysql_share").for_share()
        )
        assert result is not None
        assert result["name"] == "mysql_share"
        assert result["value"] == 300

        await driver.commit()
    except Exception:
        await driver.rollback()
        raise
