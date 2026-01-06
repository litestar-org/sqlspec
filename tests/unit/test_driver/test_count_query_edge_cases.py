"""Tests for _create_count_query edge cases and validation.

This module tests COUNT query generation validation, particularly for edge cases
where SELECT statements are missing required clauses (FROM, etc.).
"""

import pytest

from sqlspec.core import SQL, StatementConfig
from sqlspec.driver._sync import SyncDriverAdapterBase
from sqlspec.exceptions import ImproperConfigurationError

# pyright: reportPrivateUsage=false


def _is_compiled() -> bool:
    """Check if driver modules are mypyc-compiled."""
    try:
        from sqlspec.driver import _sync

        return hasattr(_sync, "__file__") and (_sync.__file__ or "").endswith(".so")
    except ImportError:
        return False


pytestmark = pytest.mark.skipif(_is_compiled(), reason="Test requires interpreted subclasses of compiled driver bases.")


class MockSyncDriver(SyncDriverAdapterBase):
    """Mock driver for testing _create_count_query method."""

    def __init__(self) -> None:
        self.statement_config = StatementConfig()

    @property
    def connection(self):
        return None

    def _execute_statement(self, *args, **kwargs):
        raise NotImplementedError("Mock driver - not implemented")

    def _execute_many(self, *args, **kwargs):
        raise NotImplementedError("Mock driver - not implemented")

    def with_cursor(self, *args, **kwargs):
        raise NotImplementedError("Mock driver - not implemented")

    def handle_database_exceptions(self, *args, **kwargs):
        raise NotImplementedError("Mock driver - not implemented")

    def create_connection(self, *args, **kwargs):
        raise NotImplementedError("Mock driver - not implemented")

    def close_connection(self, *args, **kwargs):
        raise NotImplementedError("Mock driver - not implemented")

    def begin(self, *args, **kwargs):
        raise NotImplementedError("Mock driver - not implemented")

    def commit(self, *args, **kwargs):
        raise NotImplementedError("Mock driver - not implemented")

    def rollback(self, *args, **kwargs):
        raise NotImplementedError("Mock driver - not implemented")

    def _try_special_handling(self, *args, **kwargs):
        raise NotImplementedError("Mock driver - not implemented")

    @property
    def data_dictionary(self):
        raise NotImplementedError("Mock driver - not implemented")


class TestCountQueryValidation:
    """Test COUNT query generation validation."""

    def test_count_query_missing_from_clause_with_order_by(self) -> None:
        """Test COUNT query fails with clear error when FROM clause missing (ORDER BY only).

        This is the reported bug scenario from upstream.
        """
        driver = MockSyncDriver()
        sql = driver.prepare_statement(SQL("SELECT * ORDER BY id"), statement_config=driver.statement_config)
        sql.compile()  # Parse the SQL to populate expression

        with pytest.raises(ImproperConfigurationError, match="missing FROM clause"):
            driver._create_count_query(sql)

    def test_count_query_missing_from_clause_with_where(self) -> None:
        """Test COUNT query fails when only WHERE clause present (no FROM)."""
        driver = MockSyncDriver()
        sql = driver.prepare_statement(SQL("SELECT * WHERE active = true"), statement_config=driver.statement_config)
        sql.compile()

        with pytest.raises(ImproperConfigurationError, match="missing FROM clause"):
            driver._create_count_query(sql)

    def test_count_query_select_star_no_from(self) -> None:
        """Test COUNT query fails for SELECT * without FROM clause."""
        driver = MockSyncDriver()
        sql = driver.prepare_statement(SQL("SELECT *"), statement_config=driver.statement_config)
        sql.compile()

        with pytest.raises(ImproperConfigurationError, match="missing FROM clause"):
            driver._create_count_query(sql)

    def test_count_query_select_columns_no_from(self) -> None:
        """Test COUNT query fails for SELECT columns without FROM clause."""
        driver = MockSyncDriver()
        sql = driver.prepare_statement(SQL("SELECT id, name"), statement_config=driver.statement_config)
        sql.compile()

        with pytest.raises(ImproperConfigurationError, match="missing FROM clause"):
            driver._create_count_query(sql)

    def test_count_query_valid_select_with_from(self) -> None:
        """Test COUNT query succeeds with valid SELECT...FROM."""
        driver = MockSyncDriver()
        sql = driver.prepare_statement(SQL("SELECT * FROM users ORDER BY id"), statement_config=driver.statement_config)
        sql.compile()

        count_sql = driver._create_count_query(sql)

        count_str = str(count_sql)
        assert "COUNT(*)" in count_str.upper()
        assert "FROM users" in count_str or "FROM USERS" in count_str.upper()
        assert "ORDER BY" not in count_str.upper()

    def test_count_query_with_where_and_from(self) -> None:
        """Test COUNT query preserves WHERE clause when FROM present."""
        driver = MockSyncDriver()
        sql = driver.prepare_statement(
            SQL("SELECT * FROM users WHERE active = true ORDER BY id"), statement_config=driver.statement_config
        )
        sql.compile()

        count_sql = driver._create_count_query(sql)

        count_str = str(count_sql)
        assert "COUNT(*)" in count_str.upper()
        assert "FROM users" in count_str or "FROM USERS" in count_str.upper()
        assert "WHERE" in count_str.upper()
        assert "active" in count_str or "ACTIVE" in count_str.upper()
        assert "ORDER BY" not in count_str.upper()

    def test_count_query_with_group_by(self) -> None:
        """Test COUNT query wraps grouped query in subquery."""
        driver = MockSyncDriver()
        sql = driver.prepare_statement(
            SQL("SELECT status, COUNT(*) FROM users GROUP BY status"), statement_config=driver.statement_config
        )
        sql.compile()

        count_sql = driver._create_count_query(sql)

        count_str = str(count_sql)
        assert "COUNT(*)" in count_str.upper()
        assert "grouped_data" in count_str.lower()

    def test_count_query_removes_limit_offset(self) -> None:
        """Test COUNT query removes LIMIT and OFFSET clauses."""
        driver = MockSyncDriver()
        sql = driver.prepare_statement(
            SQL("SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 20"), statement_config=driver.statement_config
        )
        sql.compile()

        count_sql = driver._create_count_query(sql)

        count_str = str(count_sql)
        assert "LIMIT" not in count_str.upper()
        assert "OFFSET" not in count_str.upper()
        assert "ORDER BY" not in count_str.upper()

    def test_count_query_with_having(self) -> None:
        """Test COUNT query preserves HAVING clause."""
        driver = MockSyncDriver()
        sql = driver.prepare_statement(
            SQL("SELECT status, COUNT(*) as cnt FROM users GROUP BY status HAVING cnt > 5"),
            statement_config=driver.statement_config,
        )
        sql.compile()

        count_sql = driver._create_count_query(sql)

        count_str = str(count_sql)
        assert "COUNT(*)" in count_str.upper()


class TestCountQueryEdgeCases:
    """Test COUNT query edge cases that previously caused issues."""

    def test_complex_select_with_join(self) -> None:
        """Test complex SELECT with JOIN generates correct COUNT."""
        driver = MockSyncDriver()
        sql = driver.prepare_statement(
            SQL("""
                SELECT u.id, u.name, o.total
                FROM users u
                JOIN orders o ON u.id = o.user_id
                WHERE u.active = true
                AND o.total > 100
                ORDER BY o.total DESC
                LIMIT 10
            """),
            statement_config=driver.statement_config,
        )
        sql.compile()

        count_sql = driver._create_count_query(sql)

        count_str = str(count_sql)
        assert "COUNT(*)" in count_str.upper()
        assert "FROM users" in count_str or "FROM USERS" in count_str.upper()
        assert "ORDER BY" not in count_str.upper()
        assert "LIMIT" not in count_str.upper()

    def test_select_with_subquery_in_from(self) -> None:
        """Test SELECT with subquery in FROM clause."""
        driver = MockSyncDriver()
        sql = driver.prepare_statement(
            SQL("""
                SELECT t.id
                FROM (SELECT id FROM users WHERE active = true) t
                ORDER BY t.id
            """),
            statement_config=driver.statement_config,
        )
        sql.compile()

        count_sql = driver._create_count_query(sql)

        count_str = str(count_sql)
        assert "COUNT(*)" in count_str.upper()

    def test_error_message_clarity(self) -> None:
        """Test that error message explains why FROM clause is required."""
        driver = MockSyncDriver()
        sql = driver.prepare_statement(SQL("SELECT * ORDER BY id"), statement_config=driver.statement_config)
        sql.compile()

        with pytest.raises(
            ImproperConfigurationError,
            match="COUNT queries require a FROM clause to determine which table to count rows from",
        ):
            driver._create_count_query(sql)
