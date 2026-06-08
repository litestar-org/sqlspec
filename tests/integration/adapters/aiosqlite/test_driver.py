"""Integration tests for aiosqlite driver implementation.

Shared CRUD-lifecycle and transaction-visibility behavior lives in
``tests/integration/adapters/contracts/test_driver_contract.py``. This file keeps
aiosqlite-specific cases: native data-type round trips, statement-stack fallback,
SQL-object integration, FOR UPDATE/SHARE SQL generation, and async-only features.
"""

import math

import pytest

from sqlspec import SQLResult, StatementStack, sql
from sqlspec.adapters.aiosqlite import AiosqliteDriver
from sqlspec.core import StatementConfig

pytestmark = pytest.mark.xdist_group("sqlite")


async def test_aiosqlite_data_types(aiosqlite_session: AiosqliteDriver) -> None:
    """Test SQLite data type handling with aiosqlite."""

    await aiosqlite_session.execute_script("""
        CREATE TABLE aiosqlite_data_types_test (
            id INTEGER PRIMARY KEY,
            text_col TEXT,
            integer_col INTEGER,
            real_col REAL,
            blob_col BLOB,
            null_col TEXT
        )
    """)

    test_data = ("text_value", 42, math.pi, b"binary_data", None)

    insert_result = await aiosqlite_session.execute(
        "INSERT INTO aiosqlite_data_types_test (text_col, integer_col, real_col, blob_col, null_col) VALUES (?, ?, ?, ?, ?)",
        test_data,
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = await aiosqlite_session.execute(
        "SELECT text_col, integer_col, real_col, blob_col, null_col FROM aiosqlite_data_types_test"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.get_data()[0]
    assert row["text_col"] == "text_value"
    assert row["integer_col"] == 42
    assert row["real_col"] == math.pi
    assert row["blob_col"] == b"binary_data"
    assert row["null_col"] is None

    await aiosqlite_session.execute_script("DROP TABLE aiosqlite_data_types_test")


async def test_aiosqlite_statement_stack_continue_on_error(aiosqlite_session: AiosqliteDriver) -> None:
    """Sequential execution should continue when continue_on_error is enabled."""

    await aiosqlite_session.execute("DELETE FROM test_table")
    await aiosqlite_session.commit()

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "aiosqlite-initial", 5))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "aiosqlite-duplicate", 15))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (2, "aiosqlite-final", 25))
    )

    results = await aiosqlite_session.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[0].rows_affected == 1
    assert results[1].error is not None
    assert results[2].rows_affected == 1

    verify = await aiosqlite_session.execute("SELECT COUNT(*) AS total FROM test_table")
    assert verify.data is not None
    assert verify.get_data()[0]["total"] == 2


async def test_aiosqlite_schema_operations(aiosqlite_session: AiosqliteDriver) -> None:
    """Test schema operations (DDL)."""

    create_result = await aiosqlite_session.execute_script("""
        CREATE TABLE schema_test (
            id INTEGER PRIMARY KEY,
            description TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    assert isinstance(create_result, SQLResult)
    assert create_result.operation_type == "SCRIPT"

    insert_result = await aiosqlite_session.execute(
        "INSERT INTO schema_test (description) VALUES (?)", ("test description",)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    pragma_result = await aiosqlite_session.execute("PRAGMA table_info(schema_test)")
    assert isinstance(pragma_result, SQLResult)
    assert pragma_result.data is not None
    assert len(pragma_result.data) == 3

    drop_result = await aiosqlite_session.execute_script("DROP TABLE schema_test")
    assert isinstance(drop_result, SQLResult)
    assert drop_result.operation_type == "SCRIPT"


async def test_aiosqlite_sqlite_specific_features(aiosqlite_session: AiosqliteDriver) -> None:
    """Test SQLite-specific features with aiosqlite."""

    pragma_result = await aiosqlite_session.execute("PRAGMA user_version")
    assert isinstance(pragma_result, SQLResult)
    assert pragma_result.data is not None

    sqlite_result = await aiosqlite_session.execute("SELECT sqlite_version() as version")
    assert isinstance(sqlite_result, SQLResult)
    assert sqlite_result.data is not None
    assert sqlite_result.get_data()[0]["version"] is not None

    try:
        json_result = await aiosqlite_session.execute("SELECT json('{}') as json_test")
        assert isinstance(json_result, SQLResult)
        assert json_result.data is not None
    except Exception:
        pass

    non_strict_config = StatementConfig(enable_parsing=False, enable_validation=False)

    await aiosqlite_session.execute("ATTACH DATABASE ':memory:' AS temp_db", statement_config=non_strict_config)
    await aiosqlite_session.execute(
        "CREATE TABLE temp_db.temp_table (id INTEGER, name TEXT)", statement_config=non_strict_config
    )
    await aiosqlite_session.execute(
        "INSERT INTO temp_db.temp_table VALUES (1, 'temp')", statement_config=non_strict_config
    )

    temp_result = await aiosqlite_session.execute("SELECT * FROM temp_db.temp_table")
    assert isinstance(temp_result, SQLResult)
    assert temp_result.data is not None
    assert len(temp_result.data) == 1
    assert temp_result.get_data()[0]["name"] == "temp"

    try:
        await aiosqlite_session.execute("DETACH DATABASE temp_db", statement_config=non_strict_config)
    except Exception:
        pass


async def test_aiosqlite_for_update_generates_sql(aiosqlite_session: AiosqliteDriver) -> None:
    """Test that FOR UPDATE generates SQL for aiosqlite (though SQLite doesn't support row-level locking)."""

    # Create test table
    await aiosqlite_session.execute_script("""
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER
        );
    """)

    # Insert test data
    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("aiosqlite_test", 100))

    # Should generate SQL even though SQLite doesn't support the functionality
    query = sql.select("*").from_("test_table").where_eq("name", "aiosqlite_test").for_update()
    stmt = query.build()
    # SQLite doesn't support FOR UPDATE, so SQLGlot strips it out (expected behavior)
    assert "FOR UPDATE" not in stmt.sql
    assert "SELECT" in stmt.sql  # But the rest of the query works

    # Should execute without error (SQLite just ignores the FOR UPDATE)
    result = await aiosqlite_session.execute(query)
    assert result is not None
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "aiosqlite_test"


async def test_aiosqlite_for_share_generates_sql_but_may_not_work(aiosqlite_session: AiosqliteDriver) -> None:
    """Test that FOR SHARE generates SQL for aiosqlite but note it doesn't provide row-level locking."""

    # Create test table
    await aiosqlite_session.execute_script("""
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER
        );
    """)

    # Insert test data
    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("aiosqlite_share", 200))

    # Should generate SQL even though SQLite doesn't support the functionality
    query = sql.select("*").from_("test_table").where_eq("name", "aiosqlite_share").for_share()
    stmt = query.build()
    # SQLite doesn't support FOR SHARE, so SQLGlot strips it out (expected behavior)
    assert "FOR SHARE" not in stmt.sql
    assert "SELECT" in stmt.sql  # But the rest of the query works

    # Should execute without error (SQLite just ignores the FOR SHARE)
    result = await aiosqlite_session.execute(query)
    assert result is not None
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "aiosqlite_share"


async def test_aiosqlite_for_update_skip_locked_generates_sql(aiosqlite_session: AiosqliteDriver) -> None:
    """Test that FOR UPDATE SKIP LOCKED generates SQL for aiosqlite."""

    # Create test table
    await aiosqlite_session.execute_script("""
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER
        );
    """)

    # Insert test data
    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("aiosqlite_skip", 300))

    # Should generate SQL even though SQLite doesn't support the functionality
    query = sql.select("*").from_("test_table").where_eq("name", "aiosqlite_skip").for_update(skip_locked=True)
    stmt = query.build()
    # The exact SQL generated may vary based on dialect support
    assert stmt.sql is not None

    # Should execute (SQLite will ignore unsupported clauses)
    result = await aiosqlite_session.execute(query)
    assert result is not None
    assert len(result.data) == 1
