"""Test parameter conversion and validation for MysqlConnector drivers.

This test suite validates that the SQLTransformer properly converts different
input parameter styles to the target MySQL POSITIONAL_PYFORMAT style.

MysqlConnector Parameter Conversion Requirements:
- Input: QMARK (?) -> Output: POSITIONAL_PYFORMAT (%s)
- Input: NAMED_COLON (:name) -> Output: POSITIONAL_PYFORMAT (%s)
- Input: NAMED_PYFORMAT (%(name)s) -> Output: POSITIONAL_PYFORMAT (%s)
- Input: POSITIONAL_PYFORMAT (%s) -> Output: POSITIONAL_PYFORMAT (%s) (no conversion)

This implements MySQL's 2-phase parameter processing.
"""

from collections.abc import AsyncGenerator, Generator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.mysqlconnector import (
    MysqlConnectorAsyncConfig,
    MysqlConnectorAsyncDriver,
    MysqlConnectorSyncConfig,
    MysqlConnectorSyncDriver,
    default_statement_config,
)
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("mysql")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mysqlconnector_sync_parameter_session(
    mysql_service: MySQLService,
) -> Generator[MysqlConnectorSyncDriver, None, None]:
    """Create a MysqlConnector sync session for parameter conversion testing."""
    config = MysqlConnectorSyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "use_pure": True,
        },
        statement_config=default_statement_config,
    )

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_parameter_conversion (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT DEFAULT 0,
                description TEXT
            )
        """)

        session.execute_script("DELETE FROM test_parameter_conversion")

        session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)",
            ("test1", 100, "First test"),
        )
        session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)",
            ("test2", 200, "Second test"),
        )
        session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)", ("test3", 300, None)
        )

        yield session

        session.execute_script("DROP TABLE IF EXISTS test_parameter_conversion")


@pytest.fixture
async def mysqlconnector_async_parameter_session(
    mysql_service: MySQLService,
) -> AsyncGenerator[MysqlConnectorAsyncDriver, None]:
    """Create a MysqlConnector async session for parameter conversion testing."""
    config = MysqlConnectorAsyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "use_pure": True,
        },
        statement_config=default_statement_config,
    )

    async with config.provide_session() as session:
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_parameter_conversion_async (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT DEFAULT 0,
                description TEXT
            )
        """)

        await session.execute_script("DELETE FROM test_parameter_conversion_async")

        await session.execute(
            "INSERT INTO test_parameter_conversion_async (name, value, description) VALUES (?, ?, ?)",
            ("test1", 100, "First test"),
        )
        await session.execute(
            "INSERT INTO test_parameter_conversion_async (name, value, description) VALUES (?, ?, ?)",
            ("test2", 200, "Second test"),
        )
        await session.execute(
            "INSERT INTO test_parameter_conversion_async (name, value, description) VALUES (?, ?, ?)",
            ("test3", 300, None),
        )

        yield session

        await session.execute_script("DROP TABLE IF EXISTS test_parameter_conversion_async")


# =============================================================================
# Sync Driver Tests
# =============================================================================


class TestSyncQmarkConversion:
    """Test QMARK (?) to POSITIONAL_PYFORMAT (%s) conversion for sync driver."""

    def test_qmark_single_parameter(self, mysqlconnector_sync_parameter_session: MysqlConnectorSyncDriver) -> None:
        """Test single ? placeholder gets converted to %s."""
        result = mysqlconnector_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = ?", ("test1",)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"

    def test_qmark_multiple_parameters(self, mysqlconnector_sync_parameter_session: MysqlConnectorSyncDriver) -> None:
        """Test multiple ? placeholders get converted to %s."""
        result = mysqlconnector_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = ? AND value > ?", ("test1", 50)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"
        assert result.data[0]["value"] == 100


class TestSyncNamedColonConversion:
    """Test NAMED_COLON (:name) to POSITIONAL_PYFORMAT (%s) conversion for sync driver."""

    def test_named_colon_single_parameter(
        self, mysqlconnector_sync_parameter_session: MysqlConnectorSyncDriver
    ) -> None:
        """Test single :name placeholder gets converted."""
        result = mysqlconnector_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = :name", {"name": "test1"}
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"

    def test_named_colon_multiple_parameters(
        self, mysqlconnector_sync_parameter_session: MysqlConnectorSyncDriver
    ) -> None:
        """Test multiple :name placeholders get converted."""
        result = mysqlconnector_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = :name AND value > :min_val",
            {"name": "test2", "min_val": 100},
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test2"


class TestSyncNamedPyformatConversion:
    """Test NAMED_PYFORMAT (%(name)s) to POSITIONAL_PYFORMAT (%s) conversion for sync driver."""

    def test_named_pyformat_parameters(self, mysqlconnector_sync_parameter_session: MysqlConnectorSyncDriver) -> None:
        """Test %(name)s placeholders get converted."""
        result = mysqlconnector_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = %(test_name)s AND value < %(max_val)s",
            {"test_name": "test3", "max_val": 350},
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test3"


class TestSyncPositionalPyformatNative:
    """Test POSITIONAL_PYFORMAT (%s) works natively for sync driver."""

    def test_pyformat_parameters(self, mysqlconnector_sync_parameter_session: MysqlConnectorSyncDriver) -> None:
        """Test %s placeholders work directly."""
        result = mysqlconnector_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = %s AND value > %s", ("test2", 150)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test2"


class TestSyncSQLObject:
    """Test parameter conversion with SQL objects for sync driver."""

    def test_sql_object_with_qmark(self, mysqlconnector_sync_parameter_session: MysqlConnectorSyncDriver) -> None:
        """Test SQL object with ? placeholders."""
        sql_qmark = SQL("SELECT * FROM test_parameter_conversion WHERE name = ? OR name = ?", "test1", "test3")
        result = mysqlconnector_sync_parameter_session.execute(sql_qmark)

        assert isinstance(result, SQLResult)
        assert len(result.data) == 2


class TestSyncExecuteMany:
    """Test parameter conversion with execute_many for sync driver."""

    def test_execute_many_with_qmark(self, mysqlconnector_sync_parameter_session: MysqlConnectorSyncDriver) -> None:
        """Test execute_many with ? placeholders."""
        data = [("batch1", 1001, "Batch 1"), ("batch2", 1002, "Batch 2"), ("batch3", 1003, "Batch 3")]

        result = mysqlconnector_sync_parameter_session.execute_many(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)", data
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 3


class TestSyncEdgeCases:
    """Test edge cases for sync driver."""

    def test_boolean_parameters(self, mysqlconnector_sync_parameter_session: MysqlConnectorSyncDriver) -> None:
        """Test boolean parameters are converted to integers for MySQL."""
        mysqlconnector_sync_parameter_session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_bools (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                active TINYINT(1)
            )
        """)

        mysqlconnector_sync_parameter_session.execute(
            "INSERT INTO test_bools (name, active) VALUES (?, ?)", ("bool_test", True)
        )

        result = mysqlconnector_sync_parameter_session.execute("SELECT * FROM test_bools WHERE active = ?", (True,))

        assert len(result.data) == 1
        assert result.data[0]["name"] == "bool_test"

        mysqlconnector_sync_parameter_session.execute_script("DROP TABLE IF EXISTS test_bools")

    def test_sql_injection_prevention(self, mysqlconnector_sync_parameter_session: MysqlConnectorSyncDriver) -> None:
        """Test that parameter escaping prevents SQL injection."""
        malicious_input = "'; DROP TABLE test_parameter_conversion; --"

        result = mysqlconnector_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = ?", (malicious_input,)
        )

        assert len(result.data) == 0

        # Verify table still exists
        count_result = mysqlconnector_sync_parameter_session.execute(
            "SELECT COUNT(*) as count FROM test_parameter_conversion"
        )
        assert count_result.data[0]["count"] >= 3


# =============================================================================
# Async Driver Tests
# =============================================================================


class TestAsyncQmarkConversion:
    """Test QMARK (?) to POSITIONAL_PYFORMAT (%s) conversion for async driver."""

    @pytest.mark.asyncio
    async def test_qmark_single_parameter(
        self, mysqlconnector_async_parameter_session: MysqlConnectorAsyncDriver
    ) -> None:
        """Test single ? placeholder gets converted to %s."""
        result = await mysqlconnector_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = ?", ("test1",)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"

    @pytest.mark.asyncio
    async def test_qmark_multiple_parameters(
        self, mysqlconnector_async_parameter_session: MysqlConnectorAsyncDriver
    ) -> None:
        """Test multiple ? placeholders get converted to %s."""
        result = await mysqlconnector_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = ? AND value > ?", ("test1", 50)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"
        assert result.data[0]["value"] == 100


class TestAsyncNamedColonConversion:
    """Test NAMED_COLON (:name) to POSITIONAL_PYFORMAT (%s) conversion for async driver."""

    @pytest.mark.asyncio
    async def test_named_colon_single_parameter(
        self, mysqlconnector_async_parameter_session: MysqlConnectorAsyncDriver
    ) -> None:
        """Test single :name placeholder gets converted."""
        result = await mysqlconnector_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = :name", {"name": "test1"}
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"

    @pytest.mark.asyncio
    async def test_named_colon_multiple_parameters(
        self, mysqlconnector_async_parameter_session: MysqlConnectorAsyncDriver
    ) -> None:
        """Test multiple :name placeholders get converted."""
        result = await mysqlconnector_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = :name AND value > :min_val",
            {"name": "test2", "min_val": 100},
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test2"


class TestAsyncNamedPyformatConversion:
    """Test NAMED_PYFORMAT (%(name)s) to POSITIONAL_PYFORMAT (%s) conversion for async driver."""

    @pytest.mark.asyncio
    async def test_named_pyformat_parameters(
        self, mysqlconnector_async_parameter_session: MysqlConnectorAsyncDriver
    ) -> None:
        """Test %(name)s placeholders get converted."""
        result = await mysqlconnector_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = %(test_name)s AND value < %(max_val)s",
            {"test_name": "test3", "max_val": 350},
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test3"


class TestAsyncPositionalPyformatNative:
    """Test POSITIONAL_PYFORMAT (%s) works natively for async driver."""

    @pytest.mark.asyncio
    async def test_pyformat_parameters(self, mysqlconnector_async_parameter_session: MysqlConnectorAsyncDriver) -> None:
        """Test %s placeholders work directly."""
        result = await mysqlconnector_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = %s AND value > %s", ("test2", 150)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test2"


class TestAsyncSQLObject:
    """Test parameter conversion with SQL objects for async driver."""

    @pytest.mark.asyncio
    async def test_sql_object_with_qmark(
        self, mysqlconnector_async_parameter_session: MysqlConnectorAsyncDriver
    ) -> None:
        """Test SQL object with ? placeholders."""
        sql_qmark = SQL("SELECT * FROM test_parameter_conversion_async WHERE name = ? OR name = ?", "test1", "test3")
        result = await mysqlconnector_async_parameter_session.execute(sql_qmark)

        assert isinstance(result, SQLResult)
        assert len(result.data) == 2


class TestAsyncExecuteMany:
    """Test parameter conversion with execute_many for async driver."""

    @pytest.mark.asyncio
    async def test_execute_many_with_qmark(
        self, mysqlconnector_async_parameter_session: MysqlConnectorAsyncDriver
    ) -> None:
        """Test execute_many with ? placeholders."""
        data = [("batch1", 1001, "Batch 1"), ("batch2", 1002, "Batch 2"), ("batch3", 1003, "Batch 3")]

        result = await mysqlconnector_async_parameter_session.execute_many(
            "INSERT INTO test_parameter_conversion_async (name, value, description) VALUES (?, ?, ?)", data
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 3


class TestAsyncEdgeCases:
    """Test edge cases for async driver."""

    @pytest.mark.asyncio
    async def test_boolean_parameters(self, mysqlconnector_async_parameter_session: MysqlConnectorAsyncDriver) -> None:
        """Test boolean parameters are converted to integers for MySQL."""
        await mysqlconnector_async_parameter_session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_bools_async (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                active TINYINT(1)
            )
        """)

        await mysqlconnector_async_parameter_session.execute(
            "INSERT INTO test_bools_async (name, active) VALUES (?, ?)", ("bool_test", True)
        )

        result = await mysqlconnector_async_parameter_session.execute(
            "SELECT * FROM test_bools_async WHERE active = ?", (True,)
        )

        assert len(result.data) == 1
        assert result.data[0]["name"] == "bool_test"

        await mysqlconnector_async_parameter_session.execute_script("DROP TABLE IF EXISTS test_bools_async")

    @pytest.mark.asyncio
    async def test_sql_injection_prevention(
        self, mysqlconnector_async_parameter_session: MysqlConnectorAsyncDriver
    ) -> None:
        """Test that parameter escaping prevents SQL injection."""
        malicious_input = "'; DROP TABLE test_parameter_conversion_async; --"

        result = await mysqlconnector_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = ?", (malicious_input,)
        )

        assert len(result.data) == 0

        # Verify table still exists
        count_result = await mysqlconnector_async_parameter_session.execute(
            "SELECT COUNT(*) as count FROM test_parameter_conversion_async"
        )
        assert count_result.data[0]["count"] >= 3
