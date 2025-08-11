"""Integration tests for SQLite driver with query mixin functionality using CORE_ROUND_3."""

from typing import Any

import pytest

from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.core.statement import SQL
from sqlspec.exceptions import NotFoundError


def test_select_one_success(sqlite_driver: SqliteDriver) -> None:
    """Test select_one returns exactly one row."""
    result: dict[str, Any] = sqlite_driver.select_one("SELECT * FROM users WHERE id = 1")
    assert result["id"] == 1
    assert result["name"] == "John Doe"
    assert result["email"] == "john@example.com"


def test_select_one_no_rows(sqlite_driver: SqliteDriver) -> None:
    """Test select_one raises when no rows found."""
    with pytest.raises(NotFoundError):
        sqlite_driver.select_one("SELECT * FROM users WHERE id = 999")


def test_select_one_multiple_rows(sqlite_driver: SqliteDriver) -> None:
    """Test select_one raises when multiple rows found."""
    with pytest.raises(ValueError, match="Expected exactly one row"):
        sqlite_driver.select_one("SELECT * FROM users WHERE age > 25")


def test_select_one_or_none_success(sqlite_driver: SqliteDriver) -> None:
    """Test select_one_or_none returns one row when found."""
    result = sqlite_driver.select_one_or_none("SELECT * FROM users WHERE email = 'jane@example.com'")
    assert result is not None
    assert result["id"] == 2
    assert result["name"] == "Jane Smith"


def test_select_one_or_none_no_rows(sqlite_driver: SqliteDriver) -> None:
    """Test select_one_or_none returns None when no rows found."""
    result = sqlite_driver.select_one_or_none("SELECT * FROM users WHERE email = 'notfound@example.com'")
    assert result is None


def test_select_one_or_none_multiple_rows(sqlite_driver: SqliteDriver) -> None:
    """Test select_one_or_none raises when multiple rows found."""
    with pytest.raises(ValueError, match="Expected at most one row"):
        sqlite_driver.select_one_or_none("SELECT * FROM users WHERE age < 35")


def test_select_value_success(sqlite_driver: SqliteDriver) -> None:
    """Test select_value returns single scalar value."""
    result = sqlite_driver.select_value("SELECT COUNT(*) FROM users")
    assert result == 5


def test_select_value_specific_column(sqlite_driver: SqliteDriver) -> None:
    """Test select_value returns specific column value."""
    result = sqlite_driver.select_value("SELECT name FROM users WHERE id = 3")
    assert result == "Bob Johnson"


def test_select_value_no_rows(sqlite_driver: SqliteDriver) -> None:
    """Test select_value raises when no rows found."""
    with pytest.raises(NotFoundError):
        sqlite_driver.select_value("SELECT name FROM users WHERE id = 999")


def test_select_value_or_none_success(sqlite_driver: SqliteDriver) -> None:
    """Test select_value_or_none returns value when found."""
    result = sqlite_driver.select_value_or_none("SELECT age FROM users WHERE name = 'Alice Brown'")
    assert result == 28


def test_select_value_or_none_no_rows(sqlite_driver: SqliteDriver) -> None:
    """Test select_value_or_none returns None when no rows."""
    result = sqlite_driver.select_value_or_none("SELECT age FROM users WHERE name = 'Unknown'")
    assert result is None


def test_select_returns_all_rows(sqlite_driver: SqliteDriver) -> None:
    """Test select returns all matching rows."""
    results: list[dict[str, Any]] = sqlite_driver.select("SELECT * FROM users ORDER BY id")
    assert len(results) == 5
    assert results[0]["name"] == "John Doe"
    assert results[4]["name"] == "Charlie Davis"


def test_select_with_filter(sqlite_driver: SqliteDriver) -> None:
    """Test select with WHERE clause."""
    results: list[dict[str, Any]] = sqlite_driver.select("SELECT * FROM users WHERE age >= 30 ORDER BY age")
    assert len(results) == 3
    assert results[0]["name"] == "John Doe"
    assert results[1]["name"] == "Charlie Davis"
    assert results[2]["name"] == "Bob Johnson"


def test_select_with_parameters(sqlite_driver: SqliteDriver) -> None:
    """Test select methods with parameterized queries."""
    # Test with named parameters using SQL object
    result: dict[str, Any] = sqlite_driver.select_one(
        SQL("SELECT * FROM users WHERE email = :email", email="bob@example.com")
    )
    assert result["name"] == "Bob Johnson"

    # Test with positional parameters using SQL object
    results: list[dict[str, Any]] = sqlite_driver.select(SQL("SELECT * FROM users WHERE age > ? ORDER BY age", 30))
    assert len(results) == 2
    assert results[0]["age"] == 32
    assert results[1]["age"] == 35


def test_complex_query_with_joins(sqlite_driver: SqliteDriver) -> None:
    """Test query methods with more complex SQL."""
    # Create a related table
    sqlite_driver.execute_script("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            total DECIMAL(10,2),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        INSERT INTO orders (user_id, total) VALUES
            (1, 100.50),
            (1, 250.00),
            (2, 75.25),
            (3, 500.00);
    """)

    # Test complex query with aggregation
    result = sqlite_driver.select_value("""
        SELECT COUNT(DISTINCT u.id)
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        WHERE o.total > 100
    """)
    assert result == 2  # Users 1 and 3 have orders > 100


def test_query_mixin_with_core_round_3_sql_object(sqlite_driver: SqliteDriver) -> None:
    """Test query mixin methods work properly with CORE_ROUND_3 SQL objects."""
    # Test select_one with SQL object containing complex parameters
    sql_obj = SQL(
        "SELECT * FROM users WHERE age BETWEEN :min_age AND :max_age ORDER BY age LIMIT 1", min_age=25, max_age=32
    )
    result: dict[str, Any] = sqlite_driver.select_one(sql_obj)
    assert result["name"] == "Jane Smith"  # Age 25, first in order
    assert result["age"] == 25


def test_query_mixin_error_handling_with_core_sql(sqlite_driver: SqliteDriver) -> None:
    """Test error handling in query mixin methods with CORE_ROUND_3 SQL objects."""
    # Test select_one with SQL that returns no rows
    sql_no_rows = SQL("SELECT * FROM users WHERE age > :max_age", max_age=100)
    with pytest.raises(NotFoundError):
        sqlite_driver.select_one(sql_no_rows)

    # Test select_one_or_none with no rows
    result = sqlite_driver.select_one_or_none(sql_no_rows)
    assert result is None

    # Test select_value with no rows
    sql_no_value = SQL("SELECT name FROM users WHERE age > :max_age", max_age=100)
    with pytest.raises(NotFoundError):
        sqlite_driver.select_value(sql_no_value)

    # Test select_value_or_none with no rows
    result = sqlite_driver.select_value_or_none(sql_no_value)
    assert result is None


def test_query_mixin_with_aggregations(sqlite_driver: SqliteDriver) -> None:
    """Test query mixin methods with aggregation queries."""
    # Test COUNT
    count_result = sqlite_driver.select_value("SELECT COUNT(*) FROM users WHERE age >= 30")
    assert count_result == 3

    # Test AVG
    avg_result = sqlite_driver.select_value("SELECT AVG(age) FROM users")
    assert avg_result == 30.0  # (30 + 25 + 35 + 28 + 32) / 5

    # Test MIN/MAX in one query returning a row
    minmax_result: dict[str, Any] = sqlite_driver.select_one(
        "SELECT MIN(age) as min_age, MAX(age) as max_age FROM users"
    )
    assert minmax_result["min_age"] == 25
    assert minmax_result["max_age"] == 35


def test_query_mixin_with_sql_functions(sqlite_driver: SqliteDriver) -> None:
    """Test query mixin methods with SQLite-specific functions."""
    # Test LENGTH function
    result = sqlite_driver.select_value("SELECT LENGTH(name) FROM users WHERE id = 1")
    assert result == 8  # "John Doe" has 8 characters

    # Test UPPER function
    result = sqlite_driver.select_value("SELECT UPPER(name) FROM users WHERE id = 2")
    assert result == "JANE SMITH"

    # Test SUBSTR function with select_one
    result_row: dict[str, Any] = sqlite_driver.select_one(
        "SELECT name, SUBSTR(name, 1, 4) as name_prefix FROM users WHERE id = 3"
    )
    assert result_row["name"] == "Bob Johnson"
    assert result_row["name_prefix"] == "Bob "


@pytest.mark.parametrize(
    "query,expected_count",
    [
        ("SELECT * FROM users WHERE age > 30", 2),
        ("SELECT * FROM users WHERE name LIKE '%o%'", 3),  # John Doe, Bob Johnson, Charlie Davis
        ("SELECT * FROM users WHERE email LIKE '%.com'", 5),  # All users
    ],
)
def test_query_mixin_parameterized_patterns(sqlite_driver: SqliteDriver, query: str, expected_count: int) -> None:
    """Test query mixin with various SQL patterns."""
    results: list[dict[str, Any]] = sqlite_driver.select(query)
    assert len(results) == expected_count
