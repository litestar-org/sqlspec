"""Test parameter conversion and validation for CockroachDB asyncpg driver.

This test suite validates that the SQLTransformer properly converts different
input parameter styles to the target PostgreSQL NUMERIC style ($1, $2, etc.).

CockroachDB asyncpg Parameter Conversion Requirements:
- Input: QMARK (?) -> Output: NUMERIC ($1, $2, ...)
- Input: NAMED_COLON (:name) -> Output: NUMERIC ($1, $2, ...)
- Input: NAMED_PYFORMAT (%(name)s) -> Output: NUMERIC ($1, $2, ...)
- Input: NUMERIC ($1) -> Output: NUMERIC ($1) (no conversion)

CockroachDB uses PostgreSQL-compatible syntax for parameters.
"""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.cockroachdb import CockroachDBService

from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig, CockroachAsyncpgDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("cockroachdb")


@pytest.fixture
async def cockroach_asyncpg_parameter_session(
    cockroachdb_service: CockroachDBService,
) -> AsyncGenerator[CockroachAsyncpgDriver, None]:
    """Create a CockroachDB asyncpg session for parameter conversion testing."""
    config = CockroachAsyncpgConfig(
        connection_config={
            "host": cockroachdb_service.host,
            "port": cockroachdb_service.port,
            "user": "root",
            "password": "",
            "database": cockroachdb_service.database,
            "ssl": None,
        }
    )

    async with config.provide_session() as session:
        await session.execute_script("""
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

        await session.execute_script("DROP TABLE IF EXISTS test_parameter_conversion")
        await config.close_pool()


class TestNumericParameterStyle:
    """Test NUMERIC ($1, $2) parameter style (native for CockroachDB)."""

    @pytest.mark.asyncio
    async def test_numeric_single_parameter(self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver) -> None:
        """Test single $1 placeholder works natively."""
        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = $1", ("test1",)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"

    @pytest.mark.asyncio
    async def test_numeric_multiple_parameters(
        self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver
    ) -> None:
        """Test multiple $n placeholders work natively."""
        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE value >= $1 AND value <= $2 ORDER BY value", (100, 200)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 2
        assert result.data[0]["value"] == 100
        assert result.data[1]["value"] == 200


class TestQmarkConversion:
    """Test QMARK (?) to NUMERIC ($1) conversion."""

    @pytest.mark.asyncio
    async def test_qmark_single_parameter(self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver) -> None:
        """Test single ? placeholder gets converted to $1."""
        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = ?", ("test1",)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"

    @pytest.mark.asyncio
    async def test_qmark_multiple_parameters(self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver) -> None:
        """Test multiple ? placeholders get converted to $1, $2, etc."""
        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = ? AND value > ?", ("test2", 100)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test2"

    @pytest.mark.asyncio
    async def test_qmark_in_insert(self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver) -> None:
        """Test ? placeholders in INSERT statements."""
        await cockroach_asyncpg_parameter_session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)",
            ("qmark_insert", 500, "Inserted via QMARK"),
        )

        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = ?", ("qmark_insert",)
        )
        assert len(result.data) == 1
        assert result.data[0]["value"] == 500


class TestNamedColonConversion:
    """Test NAMED_COLON (:name) to NUMERIC ($1) conversion."""

    @pytest.mark.asyncio
    async def test_named_colon_single_parameter(
        self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver
    ) -> None:
        """Test single :name placeholder gets converted."""
        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = :name", {"name": "test1"}
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"

    @pytest.mark.asyncio
    async def test_named_colon_multiple_parameters(
        self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver
    ) -> None:
        """Test multiple :name placeholders get converted."""
        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = :name AND value > :min_val",
            {"name": "test2", "min_val": 100},
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test2"

    @pytest.mark.asyncio
    async def test_named_colon_repeated_parameter(
        self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver
    ) -> None:
        """Test same :name used multiple times."""
        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = :val OR description LIKE :val", {"val": "test1"}
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1


class TestNamedPyformatConversion:
    """Test NAMED_PYFORMAT (%(name)s) to NUMERIC ($1) conversion."""

    @pytest.mark.asyncio
    async def test_named_pyformat_single_parameter(
        self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver
    ) -> None:
        """Test single %(name)s placeholder gets converted."""
        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = %(name)s", {"name": "test1"}
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"

    @pytest.mark.asyncio
    async def test_named_pyformat_multiple_parameters(
        self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver
    ) -> None:
        """Test multiple %(name)s placeholders get converted."""
        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = %(test_name)s AND value < %(max_val)s",
            {"test_name": "test3", "max_val": 350},
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test3"


class TestSQLObjectConversion:
    """Test parameter conversion with SQL objects."""

    @pytest.mark.asyncio
    async def test_sql_object_with_numeric(self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver) -> None:
        """Test SQL object with $n placeholders."""
        sql_numeric = SQL("SELECT * FROM test_parameter_conversion WHERE value BETWEEN $1 AND $2", 150, 250)
        result = await cockroach_asyncpg_parameter_session.execute(sql_numeric)

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test2"

    @pytest.mark.asyncio
    async def test_sql_object_with_qmark(self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver) -> None:
        """Test SQL object with ? placeholders."""
        sql_qmark = SQL("SELECT * FROM test_parameter_conversion WHERE name = ? OR name = ?", "test1", "test3")
        result = await cockroach_asyncpg_parameter_session.execute(sql_qmark)

        assert isinstance(result, SQLResult)
        assert len(result.data) == 2
        names = [row["name"] for row in result.data]
        assert "test1" in names
        assert "test3" in names


class TestExecuteMany:
    """Test parameter conversion with execute_many."""

    @pytest.mark.asyncio
    async def test_execute_many_with_numeric(self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver) -> None:
        """Test execute_many with $n placeholders."""
        data = [("batch1", 1001, "Batch 1"), ("batch2", 1002, "Batch 2"), ("batch3", 1003, "Batch 3")]

        result = await cockroach_asyncpg_parameter_session.execute_many(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES ($1, $2, $3)", data
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 3

    @pytest.mark.asyncio
    async def test_execute_many_with_qmark(self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver) -> None:
        """Test execute_many with ? placeholders."""
        data = [("qbatch1", 2001, "QBatch 1"), ("qbatch2", 2002, "QBatch 2")]

        result = await cockroach_asyncpg_parameter_session.execute_many(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)", data
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 2


class TestEdgeCases:
    """Test edge cases in parameter conversion."""

    @pytest.mark.asyncio
    async def test_null_parameters(self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver) -> None:
        """Test NULL parameter handling."""
        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE description IS NULL"
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test3"

    @pytest.mark.asyncio
    async def test_sql_injection_prevention(self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver) -> None:
        """Test that parameter escaping prevents SQL injection."""
        malicious_input = "'; DROP TABLE test_parameter_conversion; --"

        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = $1", (malicious_input,)
        )

        assert len(result.data) == 0

        # Verify table still exists
        count_result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT COUNT(*) as count FROM test_parameter_conversion"
        )
        assert count_result.data[0]["count"] >= 3

    @pytest.mark.asyncio
    async def test_special_characters_in_parameters(
        self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver
    ) -> None:
        """Test special characters in parameter values."""
        special_value = 'O\'Reilly & Sons "Test" <script>'
        await cockroach_asyncpg_parameter_session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES ($1, $2, $3)",
            ("special", 999, special_value),
        )

        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = $1", ("special",)
        )
        assert len(result.data) == 1
        assert result.data[0]["description"] == special_value

    @pytest.mark.asyncio
    async def test_like_with_wildcards(self, cockroach_asyncpg_parameter_session: CockroachAsyncpgDriver) -> None:
        """Test LIKE queries with wildcard parameters."""
        result = await cockroach_asyncpg_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name LIKE $1", ("test%",)
        )

        assert len(result.data) >= 3
        for row in result.data:
            assert row["name"].startswith("test")
