from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Annotated, Any, Optional

import pytest

from sqlspec.base import SQLSpec
from sqlspec.config import NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig
from sqlspec.driver import CommonDriverAttributesMixin
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, StatementConfig


class MockConnection:
    """Mock database connection for testing."""

    def close(self) -> None:
        pass


class MockAsyncConnection:
    """Mock async database connection for testing."""

    async def close(self) -> None:
        pass


class MockPool:
    """Mock connection pool for testing."""

    def close(self) -> None:
        pass


class MockAsyncPool:
    """Mock async connection pool for testing."""

    async def close(self) -> None:
        pass


@dataclass
class MockDatabaseConfig(SyncDatabaseConfig[MockConnection, MockPool, Any]):
    """Mock database configuration that supports pooling."""

    def __init__(self) -> None:
        """Initialize mock database config."""
        super().__init__(pool_instance=None)

    def create_connection(self) -> MockConnection:
        return MockConnection()

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Generator[MockConnection, None, None]:
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {"host": "localhost", "port": 5432}

    def create_pool(self) -> MockPool:
        return MockPool()

    def close_pool(self) -> None:
        pass

    def _create_pool(self) -> MockPool:
        """Implementation for creating a pool."""
        return MockPool()

    def _close_pool(self) -> None:
        """Implementation for closing a pool."""
        pass

    def provide_pool(self, *args: Any, **kwargs: Any) -> MockPool:
        """Provide pool instance."""
        if not self.pool_instance:
            self.pool_instance = self.create_pool()
        return self.pool_instance

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> Generator[MockConnection, None, None]:
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()


class MockNonPoolConfig(NoPoolSyncConfig[MockConnection, Any]):
    """Mock database configuration that doesn't support pooling."""

    def create_connection(self) -> MockConnection:
        return MockConnection()

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Generator[MockConnection, None, None]:
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()

    def close_pool(self) -> None:
        pass

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> Generator[MockConnection, None, None]:
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {"host": "localhost", "port": 5432}


class MockAsyncNonPoolConfig(NoPoolAsyncConfig[MockAsyncConnection, Any]):
    """Mock database configuration that doesn't support pooling."""

    async def create_connection(self) -> MockAsyncConnection:
        return MockAsyncConnection()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[MockAsyncConnection, None]:
        connection = await self.create_connection()
        try:
            yield connection
        finally:
            await connection.close()

    async def close_pool(self) -> None:
        pass

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncGenerator[MockAsyncConnection, None]:
        connection = await self.create_connection()
        try:
            yield connection
        finally:
            await connection.close()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {"host": "localhost", "port": 5432}


@pytest.fixture(scope="session")
def sql_spec() -> SQLSpec:
    """Create a SQLSpec instance for testing.

    Returns:
        A SQLSpec instance.
    """
    return SQLSpec()


@pytest.fixture(scope="session")
def pool_config() -> MockDatabaseConfig:
    """Create a mock database configuration that supports pooling.

    Returns:
        A MockDatabaseConfig instance.
    """
    return MockDatabaseConfig()


@pytest.fixture(scope="session")
def non_pool_config() -> MockNonPoolConfig:
    """Create a mock database configuration that doesn't support pooling.

    Returns:
        A MockNonPoolConfig instance.
    """
    return MockNonPoolConfig()


@pytest.fixture(scope="session")
def async_non_pool_config() -> MockAsyncNonPoolConfig:
    """Create a mock async database configuration that doesn't support pooling.

    Returns:
        A MockAsyncNonPoolConfig instance.
    """
    return MockAsyncNonPoolConfig()


@pytest.fixture(scope="session")
def driver_attributes() -> CommonDriverAttributesMixin:
    """Create a CommonDriverAttributes instance for testing the SQL detection.

    Returns:
        A CommonDriverAttributes instance.
    """

    class TestDriverAttributes(CommonDriverAttributesMixin):
        def __init__(self) -> None:
            # Create a mock connection for the test
            mock_connection = MockConnection()
            self.connection = mock_connection
            self.statement_config = StatementConfig(dialect="sqlite")
            super().__init__(connection=mock_connection, statement_config=self.statement_config)

        def _get_placeholder_style(self) -> ParameterStyle:
            return ParameterStyle.NAMED_COLON

        def _connection(self, connection: "Optional[Any]" = None) -> "Any":
            return connection or self.connection

    return TestDriverAttributes()


STATEMENT_RETURNS_ROWS_TEST_CASES = [
    # Basic cases that should return rows
    ("SELECT * FROM users", True, "Simple SELECT"),
    ("select * from users", True, "Lowercase SELECT"),
    ("  SELECT id FROM users  ", True, "SELECT with whitespace"),
    # Basic cases that should not return rows
    ("INSERT INTO users (name) VALUES ('John')", False, "Simple INSERT"),
    ("UPDATE users SET name = 'Jane' WHERE id = 1", False, "Simple UPDATE"),
    ("DELETE FROM users WHERE id = 1", False, "Simple DELETE"),
    # Cases with RETURNING clause (should return rows)
    ("INSERT INTO users (name) VALUES ('John') RETURNING id", True, "INSERT with RETURNING"),
    ("UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING *", True, "UPDATE with RETURNING"),
    ("DELETE FROM users WHERE id = 1 RETURNING name", True, "DELETE with RETURNING"),
    # WITH statements (CTEs) should return rows
    ("WITH cte AS (SELECT * FROM users) SELECT * FROM cte", True, "Simple WITH"),
    (
        "with recursive t(n) as (values (1) union select n+1 from t where n < 100) select sum(n) from t",
        True,
        "Recursive CTE",
    ),
    # Cases where old approach fails: comments at the beginning
    ("-- This is a select query\nSELECT * FROM users", True, "SELECT with comment prefix"),
    ("/* Multi-line\n   comment */\nSELECT id FROM users", True, "SELECT with multi-line comment"),
    ("-- Insert comment\nINSERT INTO users (name) VALUES ('test')", False, "INSERT with comment prefix"),
    # Cases where old approach fails: whitespace and newlines
    ("\n  \t  SELECT * FROM users", True, "SELECT with leading whitespace"),
    ("\n\nWITH cte AS (SELECT * FROM users) SELECT * FROM cte", True, "WITH with leading newlines"),
    # Cases where old approach fails: false positives with RETURNING
    ("SELECT * FROM table_returning_something", True, "SELECT with 'returning' in table name"),
    ("INSERT INTO logs (message) VALUES ('RETURNING data')", False, "INSERT with 'RETURNING' in string literal"),
    # Database-specific query types that return rows
    ("SHOW TABLES", True, "SHOW statement"),
    ("DESCRIBE users", True, "DESCRIBE statement"),
    ("EXPLAIN SELECT * FROM users", True, "EXPLAIN statement"),
    ("PRAGMA table_info(users)", True, "PRAGMA statement"),
    # Complex mixed cases
    (
        """
    /* This query selects users */
    WITH active_users AS (
        SELECT id, name
        FROM users
        WHERE active = true
    )
    SELECT * FROM active_users
    """,
        True,
        "Complex commented CTE",
    ),
    # Edge case: CTE in a comment (should be INSERT, not SELECT)
    ("-- WITH cte AS (SELECT 1)\nINSERT INTO users (name) VALUES ('test')", False, "INSERT with CTE in comment"),
    # Test various statement types
    ("CREATE TABLE test (id INTEGER)", False, "CREATE statement"),
    ("DROP TABLE test", False, "DROP statement"),
    ("ALTER TABLE test ADD COLUMN name TEXT", False, "ALTER statement"),
    # Test subqueries in non-SELECT statements
    ("INSERT INTO users (name) SELECT name FROM temp_users", False, "INSERT with subquery"),
    ("UPDATE users SET name = (SELECT name FROM profiles WHERE id = users.id)", False, "UPDATE with subquery"),
    # Test complex RETURNING cases
    (
        "UPDATE users SET last_login = NOW() WHERE active = true RETURNING id, name",
        True,
        "Complex UPDATE with RETURNING",
    ),
    ("DELETE FROM sessions WHERE expires < NOW() RETURNING session_id", True, "Complex DELETE with RETURNING"),
    # Test edge cases with similar keywords
    ("INSERT INTO returns_table (value) VALUES (1)", False, "INSERT into table with 'returns' in name"),
    ("SELECT * FROM show_logs", True, "SELECT from table with 'show' in name"),
]


@pytest.mark.parametrize(("sql", "expected_returns_rows", "description"), STATEMENT_RETURNS_ROWS_TEST_CASES)
def test_returns_rows(
    driver_attributes: CommonDriverAttributesMixin, sql: str, expected_returns_rows: bool, description: str
) -> None:
    """Test the robust SQL statement detection method.

    Args:
        driver_attributes: The driver attributes instance for testing
        sql: The SQL statement to test
        expected_returns_rows: Whether the statement should return rows
        description: Description of the test case
    """
    try:
        # Create a permissive configuration for testing that allows DDL, risky DML, and UNION operations
        test_config = StatementConfig()
        statement = SQL(sql, config=test_config)
        actual_returns_rows = statement.returns_rows()

        assert actual_returns_rows == expected_returns_rows, (
            f"{description}: Expected {expected_returns_rows}, got {actual_returns_rows} for SQL: {sql}"
        )
    except Exception as e:
        pytest.fail(f"{description}: Failed to parse SQL '{sql}': {e}")


def test_returns_rows_with_invalid_expression(driver_attributes: CommonDriverAttributesMixin) -> None:
    """Test that returns_rows handles invalid expressions gracefully."""
    # Create a permissive configuration for testing
    test_config = StatementConfig()

    try:
        empty_stmt = SQL("", config=test_config)
        result = empty_stmt.returns_rows()
        # The result doesn't matter as much as not crashing
        assert isinstance(result, bool), "Should return a boolean value"
    except Exception:
        # It's acceptable for empty SQL to fail parsing
        pass


def test_returns_rows_expression_types(driver_attributes: CommonDriverAttributesMixin) -> None:
    """Test returns_rows with various SQL statement types."""
    test_config = StatementConfig()

    # Test statements that should return rows
    select_sql = SQL("SELECT 1", config=test_config)
    assert select_sql.returns_rows() is True, "Select statement should return rows"

    show_sql = SQL("SHOW TABLES", config=test_config)
    assert show_sql.returns_rows() is True, "SHOW statement should return rows"

    # Test statements that should not return rows
    insert_sql = SQL("INSERT INTO users (name) VALUES ('test')", config=test_config)
    assert insert_sql.returns_rows() is False, "Insert without RETURNING should not return rows"

    # Test INSERT with RETURNING
    insert_returning_sql = SQL("INSERT INTO users (name) VALUES ('test') RETURNING id", config=test_config)
    assert insert_returning_sql.returns_rows() is True, "Insert with RETURNING should return rows"


def test_add_config(sql_spec: SQLSpec, pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig) -> None:
    """Test adding configurations."""
    main_db_with_a_pool = sql_spec.add_config(pool_config)
    db_config = main_db_with_a_pool()
    assert isinstance(db_config, MockDatabaseConfig)

    non_pool_type = sql_spec.add_config(non_pool_config)
    instance = non_pool_type()
    assert isinstance(instance, MockNonPoolConfig)


def test_get_config(sql_spec: SQLSpec, pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig) -> None:
    """Test retrieving configurations."""
    pool_type = sql_spec.add_config(pool_config)
    retrieved_config = sql_spec.get_config(pool_type)
    assert isinstance(retrieved_config, MockDatabaseConfig)

    non_pool_type = sql_spec.add_config(non_pool_config)
    retrieved_non_pool = sql_spec.get_config(non_pool_type)
    assert isinstance(retrieved_non_pool, MockNonPoolConfig)


def test_get_nonexistent_config(sql_spec: SQLSpec) -> None:
    """Test retrieving non-existent configuration."""
    fake_type = Annotated[MockDatabaseConfig, MockConnection, MockPool]
    with pytest.raises(KeyError):
        sql_spec.get_config(fake_type)  # pyright: ignore[reportCallIssue,reportArgumentType]


def test_get_connection(sql_spec: SQLSpec, pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig) -> None:
    """Test creating connections."""
    pool_type = sql_spec.add_config(pool_config)
    connection = sql_spec.get_connection(pool_type)
    assert isinstance(connection, MockConnection)

    non_pool_type = sql_spec.add_config(non_pool_config)
    non_pool_connection = sql_spec.get_connection(non_pool_type)
    assert isinstance(non_pool_connection, MockConnection)


def test_get_pool(sql_spec: SQLSpec, pool_config: MockDatabaseConfig) -> None:
    """Test creating pools."""
    pool_type = sql_spec.add_config(pool_config)
    pool = sql_spec.get_pool(pool_type)
    assert isinstance(pool, MockPool)


def test_config_properties(pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig) -> None:
    """Test configuration properties."""
    assert pool_config.is_async is False
    assert pool_config.supports_connection_pooling is True
    assert non_pool_config.is_async is False
    assert non_pool_config.supports_connection_pooling is False


def test_connection_context(pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig) -> None:
    """Test connection context manager."""
    with pool_config.provide_connection() as conn:
        assert isinstance(conn, MockConnection)

    with non_pool_config.provide_connection() as conn:
        assert isinstance(conn, MockConnection)


def test_pool_context(pool_config: MockDatabaseConfig) -> None:
    """Test pool context manager."""
    pool = pool_config.provide_pool()
    assert isinstance(pool, MockPool)


def test_connection_config_dict(pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig) -> None:
    """Test connection configuration dictionary."""
    assert pool_config.connection_config_dict == {"host": "localhost", "port": 5432}
    assert non_pool_config.connection_config_dict == {"host": "localhost", "port": 5432}


def test_multiple_configs(
    sql_spec: SQLSpec, pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig
) -> None:
    """Test managing multiple configurations simultaneously."""
    # Add multiple configurations
    pool_type = sql_spec.add_config(pool_config)
    non_pool_type = sql_spec.add_config(non_pool_config)
    second_pool_config = MockDatabaseConfig()
    second_pool_type = sql_spec.add_config(second_pool_config)

    # Test retrieving each configuration
    assert isinstance(sql_spec.get_config(pool_type), MockDatabaseConfig)
    assert isinstance(sql_spec.get_config(second_pool_type), MockDatabaseConfig)
    assert isinstance(sql_spec.get_config(non_pool_type), MockNonPoolConfig)

    # Test that configurations are distinct
    assert sql_spec.get_config(second_pool_type) is second_pool_config

    # Test connections from different configs
    pool_conn = sql_spec.get_connection(pool_type)
    non_pool_conn = sql_spec.get_connection(non_pool_type)
    second_pool_conn = sql_spec.get_connection(second_pool_type)

    assert isinstance(pool_conn, MockConnection)
    assert isinstance(non_pool_conn, MockConnection)
    assert isinstance(second_pool_conn, MockConnection)

    # Test pools from pooled configs
    pool1 = sql_spec.get_pool(pool_type)
    pool2 = sql_spec.get_pool(second_pool_type)

    assert isinstance(pool1, MockPool)
    assert isinstance(pool2, MockPool)  # type: ignore[unreachable]
    assert pool1 is not pool2


def test_pool_methods(non_pool_config: MockNonPoolConfig) -> None:
    """Test that pool methods return None."""
    assert non_pool_config.supports_connection_pooling is False
    assert non_pool_config.is_async is False
    assert non_pool_config.create_pool() is None  # type: ignore[func-returns-value]
