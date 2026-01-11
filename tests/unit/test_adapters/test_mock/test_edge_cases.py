"""Unit tests for edge cases and error handling in mock adapter."""

import pytest

from sqlspec.adapters.mock import MockAsyncConfig, MockSyncConfig
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)


def test_empty_initial_sql_list() -> None:
    """Test config with empty initial_sql list."""
    config = MockSyncConfig(initial_sql=[])

    with config.provide_session() as session:
        result = session.select_value("SELECT 42")
        assert result == 42


def test_unicode_data() -> None:
    """Test handling of Unicode data."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE unicode_test (text TEXT)")
        session.execute("INSERT INTO unicode_test VALUES (?)", "Hello 世界 🌍")

        result = session.select_value("SELECT text FROM unicode_test")
        assert result == "Hello 世界 🌍"


def test_large_text_data() -> None:
    """Test handling of large text values."""
    config = MockSyncConfig()
    large_text = "x" * 10000

    with config.provide_session() as session:
        session.execute("CREATE TABLE large_text (data TEXT)")
        session.execute("INSERT INTO large_text VALUES (?)", large_text)

        result = session.select_value("SELECT data FROM large_text")
        assert len(result) == 10000


def test_null_parameter_values() -> None:
    """Test handling of None parameter values."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE nullable (id INTEGER, value TEXT)")
        session.execute("INSERT INTO nullable VALUES (?, ?)", 1, None)

        result = session.select_one("SELECT * FROM nullable WHERE id = ?", 1)
        assert result is not None
        assert result["value"] is None


def test_empty_result_set() -> None:
    """Test handling of empty result sets."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE empty_test (id INTEGER)")

        result = session.select("SELECT * FROM empty_test")
        assert result == []

        value = session.select_value_or_none("SELECT id FROM empty_test")
        assert value is None


def test_multiple_parameter_types() -> None:
    """Test handling of multiple parameter types in single query."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE mixed_types (a INTEGER, b REAL, c TEXT, d BLOB)")
        session.execute("INSERT INTO mixed_types VALUES (?, ?, ?, ?)", 42, 3.14, "text", b"bytes")

        result = session.select_one("SELECT * FROM mixed_types")
        assert result is not None
        assert result["a"] == 42
        assert abs(result["b"] - 3.14) < 0.01
        assert result["c"] == "text"


def test_transaction_without_begin() -> None:
    """Test commit/rollback without explicit begin."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE auto_tx (id INTEGER)")
        session.execute("INSERT INTO auto_tx VALUES (1)")
        session.commit()

        result = session.select("SELECT * FROM auto_tx")
        assert len(result) == 1


def test_nested_transaction_detection() -> None:
    """Test that connection properly detects transaction state."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        assert not session._connection_in_transaction()  # type: ignore[attr-defined]

        session.begin()
        assert session._connection_in_transaction()  # type: ignore[attr-defined]

        session.commit()
        assert not session._connection_in_transaction()  # type: ignore[attr-defined]


def test_exception_unique_violation() -> None:
    """Test unique constraint violation error mapping."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE unique_test (id INTEGER UNIQUE)")
        session.execute("INSERT INTO unique_test VALUES (1)")

        with pytest.raises(UniqueViolationError):
            session.execute("INSERT INTO unique_test VALUES (1)")


def test_exception_not_null_violation() -> None:
    """Test not null constraint violation error mapping."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE not_null_test (id INTEGER NOT NULL)")

        with pytest.raises(NotNullViolationError):
            session.execute("INSERT INTO not_null_test VALUES (NULL)")


def test_exception_check_violation() -> None:
    """Test check constraint violation error mapping."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE check_test (age INTEGER CHECK(age >= 0))")

        with pytest.raises(CheckViolationError):
            session.execute("INSERT INTO check_test VALUES (-5)")


def test_exception_foreign_key_violation() -> None:
    """Test foreign key constraint violation error mapping."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("PRAGMA foreign_keys = ON")
        session.execute("CREATE TABLE fk_parent (id INTEGER PRIMARY KEY)")
        session.execute(
            "CREATE TABLE fk_child (id INTEGER, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES fk_parent(id))"
        )

        with pytest.raises(ForeignKeyViolationError):
            session.execute("INSERT INTO fk_child VALUES (1, 999)")


def test_exception_syntax_error() -> None:
    """Test SQL syntax error mapping."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        with pytest.raises(SQLParsingError):
            session.execute("INVALID SQL STATEMENT")


def test_execute_many_empty_list_raises() -> None:
    """Test execute_many with empty list raises error."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE batch_test (id INTEGER)")

        with pytest.raises(ValueError, match="execute_many requires parameters"):
            session.execute_many("INSERT INTO batch_test VALUES (?)", [])


def test_execute_many_none_raises() -> None:
    """Test execute_many with None raises error."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE batch_test2 (id INTEGER)")

        with pytest.raises(SQLSpecError, match="Parameter count mismatch"):
            session.execute_many("INSERT INTO batch_test2 VALUES (?)", None)  # type: ignore[arg-type]


def test_execute_many_single_row() -> None:
    """Test execute_many with single row."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE single_batch (id INTEGER)")
        session.execute_many("INSERT INTO single_batch VALUES (?)", [(42,)])

        result = session.select("SELECT * FROM single_batch")
        assert len(result) == 1
        assert result[0]["id"] == 42


def test_connection_context_manager() -> None:
    """Test connection context manager cleanup."""
    config = MockSyncConfig()

    with config.provide_connection() as conn:
        assert conn is not None
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1


def test_driver_transpilation_sqlite_native() -> None:
    """Test that SQLite dialect skips transpilation."""
    config = MockSyncConfig(target_dialect="sqlite")

    with config.provide_session() as session:
        session.execute("CREATE TABLE native (id INTEGER)")
        session.execute("INSERT INTO native VALUES (?)", 1)

        result = session.select_value("SELECT id FROM native WHERE id = ?", 1)
        assert result == 1


def test_postgres_dialect_with_complex_query() -> None:
    """Test Postgres dialect with subquery and aggregation."""
    config = MockSyncConfig(target_dialect="postgres")

    with config.provide_session() as session:
        session.execute("CREATE TABLE sales (id INTEGER, amount REAL, region TEXT)")
        session.execute("INSERT INTO sales VALUES ($1, $2, $3)", 1, 100.0, "North")
        session.execute("INSERT INTO sales VALUES ($1, $2, $3)", 2, 200.0, "South")
        session.execute("INSERT INTO sales VALUES ($1, $2, $3)", 3, 150.0, "North")

        result = session.select_one(
            """
            SELECT region, SUM(amount) as total
            FROM sales
            WHERE region = $1
            GROUP BY region
            """,
            "North",
        )
        assert result is not None
        assert result["total"] == 250.0


def test_mysql_dialect_with_case_statement() -> None:
    """Test MySQL dialect with CASE statement."""
    config = MockSyncConfig(target_dialect="mysql")

    with config.provide_session() as session:
        session.execute("CREATE TABLE status_test (id INTEGER, value INTEGER)")
        session.execute("INSERT INTO status_test VALUES (%s, %s)", 1, 10)
        session.execute("INSERT INTO status_test VALUES (%s, %s)", 2, 50)
        session.execute("INSERT INTO status_test VALUES (%s, %s)", 3, 100)

        results = session.select(
            """
            SELECT id,
                   CASE
                       WHEN value < 25 THEN 'low'
                       WHEN value < 75 THEN 'medium'
                       ELSE 'high'
                   END as category
            FROM status_test
            ORDER BY id
            """
        )
        assert len(results) == 3
        assert results[0]["category"] == "low"
        assert results[1]["category"] == "medium"
        assert results[2]["category"] == "high"


@pytest.mark.anyio
async def test_async_empty_result_set() -> None:
    """Test async handling of empty result sets."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        await session.execute("CREATE TABLE async_empty (id INTEGER)")

        result = await session.select("SELECT * FROM async_empty")
        assert result == []


@pytest.mark.anyio
async def test_async_null_parameters() -> None:
    """Test async handling of None parameter values."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        await session.execute("CREATE TABLE async_nullable (id INTEGER, value TEXT)")
        await session.execute("INSERT INTO async_nullable VALUES (?, ?)", 1, None)

        result = await session.select_one("SELECT * FROM async_nullable WHERE id = ?", 1)
        assert result is not None
        assert result["value"] is None


@pytest.mark.anyio
async def test_async_transaction_state() -> None:
    """Test async transaction state detection."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        assert not session._connection_in_transaction()  # type: ignore[attr-defined]

        await session.begin()
        assert session._connection_in_transaction()  # type: ignore[attr-defined]

        await session.commit()
        assert not session._connection_in_transaction()  # type: ignore[attr-defined]


@pytest.mark.anyio
async def test_async_exception_unique_violation() -> None:
    """Test async unique constraint violation error mapping."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        await session.execute("CREATE TABLE async_unique (id INTEGER UNIQUE)")
        await session.execute("INSERT INTO async_unique VALUES (1)")

        with pytest.raises(UniqueViolationError):
            await session.execute("INSERT INTO async_unique VALUES (1)")


@pytest.mark.anyio
async def test_async_exception_syntax_error() -> None:
    """Test async SQL syntax error mapping."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        with pytest.raises(SQLParsingError):
            await session.execute("INVALID ASYNC SQL")


@pytest.mark.anyio
async def test_async_connection_context_manager() -> None:
    """Test async connection context manager cleanup."""
    config = MockAsyncConfig()

    async with config.provide_connection() as conn:
        assert conn is not None
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1


@pytest.mark.anyio
async def test_async_execute_many() -> None:
    """Test async execute_many operation."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        await session.execute("CREATE TABLE async_batch (id INTEGER, name TEXT)")
        await session.execute_many("INSERT INTO async_batch VALUES (?, ?)", [(1, "a"), (2, "b"), (3, "c")])

        result = await session.select("SELECT * FROM async_batch ORDER BY id")
        assert len(result) == 3
        assert result[0]["name"] == "a"
        assert result[2]["name"] == "c"


@pytest.mark.anyio
async def test_async_unicode_data() -> None:
    """Test async handling of Unicode data."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        await session.execute("CREATE TABLE async_unicode (text TEXT)")
        await session.execute("INSERT INTO async_unicode VALUES (?)", "Hello 世界 🌍")

        result = await session.select_value("SELECT text FROM async_unicode")
        assert result == "Hello 世界 🌍"
