"""Test parameter conversion and validation for CockroachDB psycopg drivers.

This test suite validates that the SQLTransformer properly converts different
input parameter styles to the target PostgreSQL NUMERIC style ($1, $2, etc.).

CockroachDB psycopg Parameter Conversion Requirements:
- Input: QMARK (?) -> Output: NUMERIC ($1, $2, ...)
- Input: NAMED_COLON (:name) -> Output: NUMERIC ($1, $2, ...)
- Input: NAMED_PYFORMAT (%(name)s) -> Output: NUMERIC ($1, $2, ...)
- Input: NUMERIC ($1) -> Output: NUMERIC ($1) (no conversion)

CockroachDB uses PostgreSQL-compatible syntax for parameters.
"""

from collections.abc import AsyncGenerator, Generator

import pytest
from pytest_databases.docker.cockroachdb import CockroachDBService

from sqlspec.adapters.cockroach_psycopg import (
    CockroachPsycopgAsyncConfig,
    CockroachPsycopgAsyncDriver,
    CockroachPsycopgSyncConfig,
    CockroachPsycopgSyncDriver,
)
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("cockroachdb")


def _conninfo(service: CockroachDBService) -> str:
    return f"host={service.host} port={service.port} user=root dbname={service.database} sslmode=disable"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def cockroach_psycopg_sync_parameter_session(
    cockroachdb_service: CockroachDBService,
) -> Generator[CockroachPsycopgSyncDriver, None, None]:
    """Create a CockroachDB psycopg sync session for parameter conversion testing."""
    config = CockroachPsycopgSyncConfig(connection_config={"conninfo": _conninfo(cockroachdb_service)})

    with config.provide_session() as session:
        session.execute_script("""
            DROP TABLE IF EXISTS test_parameter_conversion CASCADE;
            CREATE TABLE test_parameter_conversion (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INT DEFAULT 0,
                description TEXT
            );
            INSERT INTO test_parameter_conversion (name, value, description) VALUES
                ('test1', 100, 'First test'),
                ('test2', 200, 'Second test'),
                ('test3', 300, NULL);
        """)

        yield session

        session.execute_script("DROP TABLE IF EXISTS test_parameter_conversion")
        config.close_pool()


@pytest.fixture
async def cockroach_psycopg_async_parameter_session(
    cockroachdb_service: CockroachDBService,
) -> AsyncGenerator[CockroachPsycopgAsyncDriver, None]:
    """Create a CockroachDB psycopg async session for parameter conversion testing."""
    config = CockroachPsycopgAsyncConfig(connection_config={"conninfo": _conninfo(cockroachdb_service)})

    async with config.provide_session() as session:
        await session.execute_script("""
            DROP TABLE IF EXISTS test_parameter_conversion_async CASCADE;
            CREATE TABLE test_parameter_conversion_async (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INT DEFAULT 0,
                description TEXT
            );
            INSERT INTO test_parameter_conversion_async (name, value, description) VALUES
                ('test1', 100, 'First test'),
                ('test2', 200, 'Second test'),
                ('test3', 300, NULL);
        """)

        yield session

        await session.execute_script("DROP TABLE IF EXISTS test_parameter_conversion_async")
        await config.close_pool()


# =============================================================================
# Sync Driver Tests
# =============================================================================


class TestSyncNumericParameterStyle:
    """Test NUMERIC ($1, $2) parameter style (native) for sync driver."""

    def test_numeric_single_parameter(
        self, cockroach_psycopg_sync_parameter_session: CockroachPsycopgSyncDriver
    ) -> None:
        """Test single $1 placeholder works natively."""
        result = cockroach_psycopg_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = $1", ("test1",)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.get_data()[0]["name"] == "test1"

    def test_numeric_multiple_parameters(
        self, cockroach_psycopg_sync_parameter_session: CockroachPsycopgSyncDriver
    ) -> None:
        """Test multiple $n placeholders work natively."""
        result = cockroach_psycopg_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE value >= $1 AND value <= $2 ORDER BY value", (100, 200)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 2
        assert result.get_data()[0]["value"] == 100
        assert result.get_data()[1]["value"] == 200


class TestSyncQmarkConversion:
    """Test QMARK (?) to NUMERIC ($1) conversion for sync driver."""

    def test_qmark_single_parameter(self, cockroach_psycopg_sync_parameter_session: CockroachPsycopgSyncDriver) -> None:
        """Test single ? placeholder gets converted to $1."""
        result = cockroach_psycopg_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = ?", ("test1",)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.get_data()[0]["name"] == "test1"

    def test_qmark_multiple_parameters(
        self, cockroach_psycopg_sync_parameter_session: CockroachPsycopgSyncDriver
    ) -> None:
        """Test multiple ? placeholders get converted."""
        result = cockroach_psycopg_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = ? AND value > ?", ("test2", 100)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.get_data()[0]["name"] == "test2"


class TestSyncNamedColonConversion:
    """Test NAMED_COLON (:name) to NUMERIC ($1) conversion for sync driver."""

    def test_named_colon_single_parameter(
        self, cockroach_psycopg_sync_parameter_session: CockroachPsycopgSyncDriver
    ) -> None:
        """Test single :name placeholder gets converted."""
        result = cockroach_psycopg_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = :name", {"name": "test1"}
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.get_data()[0]["name"] == "test1"

    def test_named_colon_multiple_parameters(
        self, cockroach_psycopg_sync_parameter_session: CockroachPsycopgSyncDriver
    ) -> None:
        """Test multiple :name placeholders get converted."""
        result = cockroach_psycopg_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = :name AND value > :min_val",
            {"name": "test2", "min_val": 100},
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.get_data()[0]["name"] == "test2"


class TestSyncNamedPyformatConversion:
    """Test NAMED_PYFORMAT (%(name)s) to NUMERIC ($1) conversion for sync driver."""

    def test_named_pyformat_parameters(
        self, cockroach_psycopg_sync_parameter_session: CockroachPsycopgSyncDriver
    ) -> None:
        """Test %(name)s placeholders get converted."""
        result = cockroach_psycopg_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = %(test_name)s AND value < %(max_val)s",
            {"test_name": "test3", "max_val": 350},
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.get_data()[0]["name"] == "test3"


class TestSyncSQLObject:
    """Test parameter conversion with SQL objects for sync driver."""

    def test_sql_object_with_qmark(self, cockroach_psycopg_sync_parameter_session: CockroachPsycopgSyncDriver) -> None:
        """Test SQL object with ? placeholders."""
        sql_qmark = SQL("SELECT * FROM test_parameter_conversion WHERE name = ? OR name = ?", "test1", "test3")
        result = cockroach_psycopg_sync_parameter_session.execute(sql_qmark)

        assert isinstance(result, SQLResult)
        assert len(result.data) == 2


class TestSyncExecuteMany:
    """Test parameter conversion with execute_many for sync driver."""

    def test_execute_many_with_numeric(
        self, cockroach_psycopg_sync_parameter_session: CockroachPsycopgSyncDriver
    ) -> None:
        """Test execute_many with $n placeholders."""
        data = [("batch1", 1001, "Batch 1"), ("batch2", 1002, "Batch 2"), ("batch3", 1003, "Batch 3")]

        result = cockroach_psycopg_sync_parameter_session.execute_many(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES ($1, $2, $3)", data
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 3


class TestSyncEdgeCases:
    """Test edge cases for sync driver."""

    def test_sql_injection_prevention(
        self, cockroach_psycopg_sync_parameter_session: CockroachPsycopgSyncDriver
    ) -> None:
        """Test that parameter escaping prevents SQL injection."""
        malicious_input = "'; DROP TABLE test_parameter_conversion; --"

        result = cockroach_psycopg_sync_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = $1", (malicious_input,)
        )

        assert len(result.data) == 0

        # Verify table still exists
        count_result = cockroach_psycopg_sync_parameter_session.execute(
            "SELECT COUNT(*) as count FROM test_parameter_conversion"
        )
        assert count_result.get_data()[0]["count"] >= 3


# =============================================================================
# Async Driver Tests
# =============================================================================


class TestAsyncNumericParameterStyle:
    """Test NUMERIC ($1, $2) parameter style (native) for async driver."""

    @pytest.mark.asyncio
    async def test_numeric_single_parameter(
        self, cockroach_psycopg_async_parameter_session: CockroachPsycopgAsyncDriver
    ) -> None:
        """Test single $1 placeholder works natively."""
        result = await cockroach_psycopg_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = $1", ("test1",)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.get_data()[0]["name"] == "test1"

    @pytest.mark.asyncio
    async def test_numeric_multiple_parameters(
        self, cockroach_psycopg_async_parameter_session: CockroachPsycopgAsyncDriver
    ) -> None:
        """Test multiple $n placeholders work natively."""
        result = await cockroach_psycopg_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE value >= $1 AND value <= $2 ORDER BY value", (100, 200)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 2
        assert result.get_data()[0]["value"] == 100
        assert result.get_data()[1]["value"] == 200


class TestAsyncQmarkConversion:
    """Test QMARK (?) to NUMERIC ($1) conversion for async driver."""

    @pytest.mark.asyncio
    async def test_qmark_single_parameter(
        self, cockroach_psycopg_async_parameter_session: CockroachPsycopgAsyncDriver
    ) -> None:
        """Test single ? placeholder gets converted to $1."""
        result = await cockroach_psycopg_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = ?", ("test1",)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.get_data()[0]["name"] == "test1"

    @pytest.mark.asyncio
    async def test_qmark_multiple_parameters(
        self, cockroach_psycopg_async_parameter_session: CockroachPsycopgAsyncDriver
    ) -> None:
        """Test multiple ? placeholders get converted."""
        result = await cockroach_psycopg_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = ? AND value > ?", ("test2", 100)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.get_data()[0]["name"] == "test2"


class TestAsyncNamedColonConversion:
    """Test NAMED_COLON (:name) to NUMERIC ($1) conversion for async driver."""

    @pytest.mark.asyncio
    async def test_named_colon_single_parameter(
        self, cockroach_psycopg_async_parameter_session: CockroachPsycopgAsyncDriver
    ) -> None:
        """Test single :name placeholder gets converted."""
        result = await cockroach_psycopg_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = :name", {"name": "test1"}
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.get_data()[0]["name"] == "test1"

    @pytest.mark.asyncio
    async def test_named_colon_multiple_parameters(
        self, cockroach_psycopg_async_parameter_session: CockroachPsycopgAsyncDriver
    ) -> None:
        """Test multiple :name placeholders get converted."""
        result = await cockroach_psycopg_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = :name AND value > :min_val",
            {"name": "test2", "min_val": 100},
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.get_data()[0]["name"] == "test2"


class TestAsyncNamedPyformatConversion:
    """Test NAMED_PYFORMAT (%(name)s) to NUMERIC ($1) conversion for async driver."""

    @pytest.mark.asyncio
    async def test_named_pyformat_parameters(
        self, cockroach_psycopg_async_parameter_session: CockroachPsycopgAsyncDriver
    ) -> None:
        """Test %(name)s placeholders get converted."""
        result = await cockroach_psycopg_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = %(test_name)s AND value < %(max_val)s",
            {"test_name": "test3", "max_val": 350},
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.get_data()[0]["name"] == "test3"


class TestAsyncSQLObject:
    """Test parameter conversion with SQL objects for async driver."""

    @pytest.mark.asyncio
    async def test_sql_object_with_qmark(
        self, cockroach_psycopg_async_parameter_session: CockroachPsycopgAsyncDriver
    ) -> None:
        """Test SQL object with ? placeholders."""
        sql_qmark = SQL("SELECT * FROM test_parameter_conversion_async WHERE name = ? OR name = ?", "test1", "test3")
        result = await cockroach_psycopg_async_parameter_session.execute(sql_qmark)

        assert isinstance(result, SQLResult)
        assert len(result.data) == 2


class TestAsyncExecuteMany:
    """Test parameter conversion with execute_many for async driver."""

    @pytest.mark.asyncio
    async def test_execute_many_with_numeric(
        self, cockroach_psycopg_async_parameter_session: CockroachPsycopgAsyncDriver
    ) -> None:
        """Test execute_many with $n placeholders."""
        data = [("batch1", 1001, "Batch 1"), ("batch2", 1002, "Batch 2"), ("batch3", 1003, "Batch 3")]

        result = await cockroach_psycopg_async_parameter_session.execute_many(
            "INSERT INTO test_parameter_conversion_async (name, value, description) VALUES ($1, $2, $3)", data
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 3


class TestAsyncEdgeCases:
    """Test edge cases for async driver."""

    @pytest.mark.asyncio
    async def test_sql_injection_prevention(
        self, cockroach_psycopg_async_parameter_session: CockroachPsycopgAsyncDriver
    ) -> None:
        """Test that parameter escaping prevents SQL injection."""
        malicious_input = "'; DROP TABLE test_parameter_conversion_async; --"

        result = await cockroach_psycopg_async_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion_async WHERE name = $1", (malicious_input,)
        )

        assert len(result.data) == 0

        # Verify table still exists
        count_result = await cockroach_psycopg_async_parameter_session.execute(
            "SELECT COUNT(*) as count FROM test_parameter_conversion_async"
        )
        assert count_result.get_data()[0]["count"] >= 3
