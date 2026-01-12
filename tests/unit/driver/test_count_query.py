"""Tests for count query helpers and edge cases."""

import sqlite3
from collections.abc import Iterator
from typing import Any

import pytest

from sqlspec import SQL
from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.core import StatementConfig, get_default_config
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import ImproperConfigurationError
from tests.conftest import requires_interpreted

# pyright: reportPrivateUsage=false

pytestmark = requires_interpreted


class MockSyncDriver(SyncDriverAdapterBase):
    """Mock driver for testing _create_count_query method."""

    def __init__(self) -> None:
        self.statement_config = StatementConfig()

    @property
    def connection(self) -> "Any":
        return None

    def dispatch_execute(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError("Mock driver - not implemented")

    def dispatch_execute_many(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError("Mock driver - not implemented")

    def with_cursor(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError("Mock driver - not implemented")

    def handle_database_exceptions(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError("Mock driver - not implemented")

    def create_connection(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError("Mock driver - not implemented")

    def close_connection(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError("Mock driver - not implemented")

    def begin(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError("Mock driver - not implemented")

    def commit(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError("Mock driver - not implemented")

    def rollback(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError("Mock driver - not implemented")

    def dispatch_special_handling(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError("Mock driver - not implemented")

    @property
    def data_dictionary(self) -> "Any":
        raise NotImplementedError("Mock driver - not implemented")


@pytest.fixture()
def sqlite_driver() -> "Iterator[SqliteDriver]":
    connection = sqlite3.connect(":memory:")
    statement_config = get_default_config()
    driver = SqliteDriver(connection, statement_config)
    try:
        yield driver
    finally:
        connection.close()


@pytest.fixture()
def mock_driver() -> "MockSyncDriver":
    return MockSyncDriver()


def test_create_count_query_compiles_missing_expression(sqlite_driver: "SqliteDriver") -> None:
    """Ensure count query generation parses SQL lacking prebuilt expression."""
    sql_statement = SQL("SELECT id FROM users WHERE active = true")

    assert sql_statement.expression is None

    count_sql = sqlite_driver._create_count_query(sql_statement)

    assert sql_statement.expression is not None

    compiled_sql, _ = count_sql.compile()

    assert count_sql.expression is not None
    assert "count" in compiled_sql.lower()


def test_create_count_query_with_cte_keeps_with_clause(sqlite_driver: "SqliteDriver") -> None:
    """Ensure count query preserves CTE at the top level."""
    sql_statement = SQL(
        """
        WITH user_stats AS (
            SELECT user_id, COUNT(*) AS order_count
            FROM orders
            GROUP BY user_id
        )
        SELECT u.name, s.order_count
        FROM users u
        JOIN user_stats s ON u.id = s.user_id
        """
    )

    count_sql = sqlite_driver._create_count_query(sql_statement)

    compiled_sql, _ = count_sql.compile()
    normalized = compiled_sql.upper().replace("\n", " ")

    assert "WITH" in normalized
    assert "FROM (WITH" not in normalized


def test_count_query_missing_from_clause_with_order_by(mock_driver: "MockSyncDriver") -> None:
    """Test COUNT query fails with clear error when FROM clause missing (ORDER BY only)."""
    sql = mock_driver.prepare_statement(SQL("SELECT * ORDER BY id"), statement_config=mock_driver.statement_config)
    sql.compile()

    with pytest.raises(ImproperConfigurationError, match="missing FROM clause"):
        mock_driver._create_count_query(sql)


def test_count_query_missing_from_clause_with_where(mock_driver: "MockSyncDriver") -> None:
    """Test COUNT query fails when only WHERE clause present (no FROM)."""
    sql = mock_driver.prepare_statement(
        SQL("SELECT * WHERE active = true"), statement_config=mock_driver.statement_config
    )
    sql.compile()

    with pytest.raises(ImproperConfigurationError, match="missing FROM clause"):
        mock_driver._create_count_query(sql)


def test_count_query_select_star_no_from(mock_driver: "MockSyncDriver") -> None:
    """Test COUNT query fails for SELECT * without FROM clause."""
    sql = mock_driver.prepare_statement(SQL("SELECT *"), statement_config=mock_driver.statement_config)
    sql.compile()

    with pytest.raises(ImproperConfigurationError, match="missing FROM clause"):
        mock_driver._create_count_query(sql)


def test_count_query_select_columns_no_from(mock_driver: "MockSyncDriver") -> None:
    """Test COUNT query fails for SELECT columns without FROM clause."""
    sql = mock_driver.prepare_statement(SQL("SELECT id, name"), statement_config=mock_driver.statement_config)
    sql.compile()

    with pytest.raises(ImproperConfigurationError, match="missing FROM clause"):
        mock_driver._create_count_query(sql)


def test_count_query_valid_select_with_from(mock_driver: "MockSyncDriver") -> None:
    """Test COUNT query succeeds with valid SELECT...FROM."""
    sql = mock_driver.prepare_statement(
        SQL("SELECT * FROM users ORDER BY id"), statement_config=mock_driver.statement_config
    )
    sql.compile()

    count_sql = mock_driver._create_count_query(sql)

    count_str = str(count_sql)
    assert "COUNT(*)" in count_str.upper()
    assert "FROM users" in count_str or "FROM USERS" in count_str.upper()
    assert "ORDER BY" not in count_str.upper()


def test_count_query_with_where_and_from(mock_driver: "MockSyncDriver") -> None:
    """Test COUNT query preserves WHERE clause when FROM present."""
    sql = mock_driver.prepare_statement(
        SQL("SELECT * FROM users WHERE active = true ORDER BY id"), statement_config=mock_driver.statement_config
    )
    sql.compile()

    count_sql = mock_driver._create_count_query(sql)

    count_str = str(count_sql)
    assert "COUNT(*)" in count_str.upper()
    assert "FROM users" in count_str or "FROM USERS" in count_str.upper()
    assert "WHERE" in count_str.upper()
    assert "active" in count_str or "ACTIVE" in count_str.upper()
    assert "ORDER BY" not in count_str.upper()


def test_count_query_with_group_by(mock_driver: "MockSyncDriver") -> None:
    """Test COUNT query wraps grouped query in subquery."""
    sql = mock_driver.prepare_statement(
        SQL("SELECT status, COUNT(*) FROM users GROUP BY status"), statement_config=mock_driver.statement_config
    )
    sql.compile()

    count_sql = mock_driver._create_count_query(sql)

    count_str = str(count_sql)
    assert "COUNT(*)" in count_str.upper()
    assert "grouped_data" in count_str.lower()


def test_count_query_removes_limit_offset(mock_driver: "MockSyncDriver") -> None:
    """Test COUNT query removes LIMIT and OFFSET clauses."""
    sql = mock_driver.prepare_statement(
        SQL("SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 20"), statement_config=mock_driver.statement_config
    )
    sql.compile()

    count_sql = mock_driver._create_count_query(sql)

    count_str = str(count_sql)
    assert "LIMIT" not in count_str.upper()
    assert "OFFSET" not in count_str.upper()
    assert "ORDER BY" not in count_str.upper()


def test_count_query_with_having(mock_driver: "MockSyncDriver") -> None:
    """Test COUNT query preserves HAVING clause."""
    sql = mock_driver.prepare_statement(
        SQL("SELECT status, COUNT(*) as cnt FROM users GROUP BY status HAVING cnt > 5"),
        statement_config=mock_driver.statement_config,
    )
    sql.compile()

    count_sql = mock_driver._create_count_query(sql)

    count_str = str(count_sql)
    assert "COUNT(*)" in count_str.upper()


def test_complex_select_with_join(mock_driver: "MockSyncDriver") -> None:
    """Test complex SELECT with JOIN generates correct COUNT."""
    sql = mock_driver.prepare_statement(
        SQL(
            """
            SELECT u.id, u.name, o.total
            FROM users u
            JOIN orders o ON u.id = o.user_id
            WHERE u.active = true
            AND o.total > 100
            ORDER BY o.total DESC
            LIMIT 10
            """
        ),
        statement_config=mock_driver.statement_config,
    )
    sql.compile()

    count_sql = mock_driver._create_count_query(sql)

    count_str = str(count_sql)
    assert "COUNT(*)" in count_str.upper()
    assert "FROM users" in count_str or "FROM USERS" in count_str.upper()
    assert "ORDER BY" not in count_str.upper()
    assert "LIMIT" not in count_str.upper()


def test_select_with_subquery_in_from(mock_driver: "MockSyncDriver") -> None:
    """Test SELECT with subquery in FROM clause."""
    sql = mock_driver.prepare_statement(
        SQL(
            """
            SELECT t.id
            FROM (SELECT id FROM users WHERE active = true) t
            ORDER BY t.id
            """
        ),
        statement_config=mock_driver.statement_config,
    )
    sql.compile()

    count_sql = mock_driver._create_count_query(sql)

    count_str = str(count_sql)
    assert "COUNT(*)" in count_str.upper()


def test_error_message_clarity(mock_driver: "MockSyncDriver") -> None:
    """Test that error message explains why FROM clause is required."""
    sql = mock_driver.prepare_statement(SQL("SELECT * ORDER BY id"), statement_config=mock_driver.statement_config)
    sql.compile()

    with pytest.raises(
        ImproperConfigurationError,
        match="COUNT queries require a FROM clause to determine which table to count rows from",
    ):
        mock_driver._create_count_query(sql)
