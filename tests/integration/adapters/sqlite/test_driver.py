"""Integration tests for SQLite driver implementation.

Shared CRUD-lifecycle and transaction-visibility behavior lives in
``tests/integration/adapters/contracts/test_driver_contract.py``. This file keeps
SQLite-specific cases: native data-type round trips, PRAGMA schema introspection,
statement-stack fallback, FOR UPDATE/SHARE SQL generation, and connection hooks.
"""

import math

import pytest

from sqlspec import SQLResult, StatementStack, sql
from sqlspec.adapters.sqlite import SqliteDriver
from tests.conftest import requires_interpreted

pytestmark = pytest.mark.xdist_group("sqlite")


@pytest.mark.parametrize("rowid", [-7, 0])
def test_sqlite_insert_preserves_integer_lastrowid(sqlite_session: SqliteDriver, rowid: int) -> None:
    sqlite_session.execute_script("CREATE TABLE sync_lastrowid_values (id INTEGER PRIMARY KEY, value TEXT)")

    result = sqlite_session.execute("INSERT INTO sync_lastrowid_values (id, value) VALUES (?, ?)", (rowid, "value"))

    assert result.last_inserted_id == rowid


def test_sqlite_update_and_delete_ignore_sticky_lastrowid(sqlite_session: SqliteDriver) -> None:
    sqlite_session.execute_script("CREATE TABLE sync_lastrowid_sticky (id INTEGER PRIMARY KEY, value TEXT)")
    inserted = sqlite_session.execute("INSERT INTO sync_lastrowid_sticky (value) VALUES (?)", ("before",))

    updated = sqlite_session.execute("UPDATE sync_lastrowid_sticky SET value = ? WHERE id = ?", ("after", 1))
    deleted = sqlite_session.execute("DELETE FROM sync_lastrowid_sticky WHERE id = ?", (1,))

    assert isinstance(inserted.last_inserted_id, int)
    assert updated.last_inserted_id is None
    assert deleted.last_inserted_id is None


def test_sqlite_repeated_insert_cache_hit_preserves_lastrowid(
    sqlite_session: SqliteDriver, monkeypatch: pytest.MonkeyPatch
) -> None:
    sqlite_session.execute_script(
        "CREATE TABLE sync_lastrowid_cached (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)"
    )
    statement = "INSERT INTO sync_lastrowid_cached (value) VALUES (?)"
    first = sqlite_session.execute(statement, ("first",))

    def fail_dispatch(*_: object) -> object:
        pytest.fail("repeated INSERT should use the cached fast path")

    monkeypatch.setattr(SqliteDriver, "dispatch_execute", fail_dispatch)
    second = sqlite_session.execute(statement, ("second",))

    assert isinstance(first.last_inserted_id, int)
    assert isinstance(second.last_inserted_id, int)
    assert second.last_inserted_id != first.last_inserted_id


def test_sqlite_insert_returning_preserves_rows_and_cached_lastrowid(
    sqlite_session: SqliteDriver, monkeypatch: pytest.MonkeyPatch
) -> None:
    sqlite_session.execute_script(
        "CREATE TABLE sync_lastrowid_returning (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)"
    )
    statement = "INSERT INTO sync_lastrowid_returning (value) VALUES (?) RETURNING id, value"
    first = sqlite_session.execute(statement, ("first",))

    def fail_dispatch(*_: object) -> object:
        pytest.fail("repeated INSERT RETURNING should use the cached fast path")

    monkeypatch.setattr(SqliteDriver, "dispatch_execute", fail_dispatch)
    second = sqlite_session.execute(statement, ("second",))

    assert first.get_data() == [{"id": 1, "value": "first"}]
    assert first.last_inserted_id == 1
    assert second.get_data() == [{"id": 2, "value": "second"}]
    assert second.last_inserted_id == 2


def test_sqlite_without_rowid_insert_never_reuses_stale_lastrowid(
    sqlite_session: SqliteDriver, monkeypatch: pytest.MonkeyPatch
) -> None:
    sqlite_session.execute_script("""
        CREATE TABLE sync_rowid_source (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT);
        CREATE TABLE sync_without_rowid (id TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID;
    """)
    prior = sqlite_session.execute("INSERT INTO sync_rowid_source (value) VALUES (?)", ("prior",))
    statement = "INSERT INTO sync_without_rowid (id, value) VALUES (?, ?)"
    first = sqlite_session.execute(statement, ("first", "value"))

    def fail_dispatch(*_: object) -> object:
        pytest.fail("repeated WITHOUT ROWID INSERT should use the cached fast path")

    monkeypatch.setattr(SqliteDriver, "dispatch_execute", fail_dispatch)
    second = sqlite_session.execute(statement, ("second", "value"))

    assert isinstance(prior.last_inserted_id, int)
    assert first.last_inserted_id is None
    assert second.last_inserted_id is None


def test_sqlite_repeated_cached_insert_reuses_rowid_eligibility_lookup(
    sqlite_session: SqliteDriver, monkeypatch: pytest.MonkeyPatch
) -> None:
    from sqlspec.adapters.sqlite import core as sqlite_core

    sqlite_session.execute_script("CREATE TABLE sync_rowid_lookup (id INTEGER PRIMARY KEY, value TEXT)")
    lookup_count = 0
    original_lookup = sqlite_core._target_supports_rowid

    def counted_lookup(connection: object, target: tuple[str | None, str]) -> bool:
        nonlocal lookup_count
        lookup_count += 1
        return original_lookup(connection, target)

    monkeypatch.setattr(sqlite_core, "_target_supports_rowid", counted_lookup)
    statement = "INSERT INTO sync_rowid_lookup (value) VALUES (?)"
    first = sqlite_session.execute(statement, ("first",))
    second = sqlite_session.execute(statement, ("second",))

    assert isinstance(first.last_inserted_id, int)
    assert isinstance(second.last_inserted_id, int)
    assert lookup_count == 1


def test_sqlite_schema_change_invalidates_rowid_eligibility_cache(sqlite_session: SqliteDriver) -> None:
    sqlite_session.execute("CREATE TABLE sync_rowid_replaced (id TEXT PRIMARY KEY, value TEXT)")
    statement = "INSERT INTO sync_rowid_replaced (id, value) VALUES (?, ?)"
    first = sqlite_session.execute(statement, ("first", "value"))

    sqlite_session.execute("DROP TABLE sync_rowid_replaced")
    sqlite_session.execute("CREATE TABLE sync_rowid_replaced (id TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID")
    second = sqlite_session.execute(statement, ("second", "value"))

    assert isinstance(first.last_inserted_id, int)
    assert second.last_inserted_id is None


def test_sqlite_data_types(sqlite_session: SqliteDriver) -> None:
    """Test SQLite data type handling."""

    sqlite_session.execute_script("""
        CREATE TABLE test_sqlite_data_types (
            id INTEGER PRIMARY KEY,
            text_col TEXT,
            integer_col INTEGER,
            real_col REAL,
            blob_col BLOB,
            null_col TEXT
        )
    """)

    test_data = ("text_value", 42, math.pi, b"binary_data", None)

    insert_result = sqlite_session.execute(
        "INSERT INTO test_sqlite_data_types (text_col, integer_col, real_col, blob_col, null_col) VALUES (?, ?, ?, ?, ?)",
        test_data,
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = sqlite_session.execute(
        "SELECT text_col, integer_col, real_col, blob_col, null_col FROM test_sqlite_data_types"
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


@requires_interpreted
def test_sqlite_statement_stack_continue_on_error(sqlite_session: SqliteDriver) -> None:
    """Sequential fallback should honor continue-on-error mode."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "sqlite-initial", 5))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "sqlite-duplicate", 15))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (2, "sqlite-final", 25))
    )

    results = sqlite_session.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[0].rows_affected == 1
    assert results[1].error is not None
    assert results[2].rows_affected == 1

    verify = sqlite_session.execute("SELECT COUNT(*) AS total FROM test_table")
    assert verify.data is not None
    assert verify.get_data()[0]["total"] == 2


def test_sqlite_schema_operations(sqlite_session: SqliteDriver) -> None:
    """Test schema operations (DDL)."""

    create_result = sqlite_session.execute_script("""
        CREATE TABLE schema_test (
            id INTEGER PRIMARY KEY,
            description TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    assert isinstance(create_result, SQLResult)
    assert create_result.operation_type == "SCRIPT"

    insert_result = sqlite_session.execute("INSERT INTO schema_test (description) VALUES (?)", ("test_description",))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    pragma_result = sqlite_session.execute("PRAGMA table_info(schema_test)")
    assert isinstance(pragma_result, SQLResult)
    assert pragma_result.data is not None
    assert len(pragma_result.get_data()) == 3

    drop_result = sqlite_session.execute_script("DROP TABLE schema_test")
    assert isinstance(drop_result, SQLResult)
    assert drop_result.operation_type == "SCRIPT"


def test_asset_maintenance_alert_complex_query(sqlite_session: SqliteDriver) -> None:
    """Test complex CTE query with INSERT, ON CONFLICT, RETURNING, and LEFT JOIN.

    This tests the specific asset_maintenance_alert query pattern with:
    - WITH clause (CTE)
    - INSERT INTO with SELECT subquery
    - ON CONFLICT ON CONSTRAINT with DO NOTHING
    - RETURNING clause
    - LEFT JOIN with to_jsonb function
    - Named parameters (:date_start, :date_end)
    """

    sqlite_session.execute_script("""
        CREATE TABLE alert_definition (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE asset_maintenance (
            id INTEGER PRIMARY KEY,
            responsible_id INTEGER NOT NULL,
            planned_date_start DATE,
            cancelled BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        );

        CREATE TABLE alert_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            asset_maintenance_id INTEGER NOT NULL,
            alert_definition_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT unique_alert UNIQUE (user_id, asset_maintenance_id, alert_definition_id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (asset_maintenance_id) REFERENCES asset_maintenance(id),
            FOREIGN KEY (alert_definition_id) REFERENCES alert_definition(id)
        );
    """)

    sqlite_session.execute("INSERT INTO alert_definition (id, name) VALUES (?, ?)", (1, "maintenances_today"))

    sqlite_session.execute_many(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
        [
            (1, "John Doe", "john@example.com"),
            (2, "Jane Smith", "jane@example.com"),
            (3, "Bob Wilson", "bob@example.com"),
        ],
    )

    sqlite_session.execute_many(
        "INSERT INTO asset_maintenance (id, responsible_id, planned_date_start, cancelled) VALUES (?, ?, ?, ?)",
        [
            (1, 1, "2024-01-15", False),
            (2, 2, "2024-01-16", False),
            (3, 3, "2024-01-17", False),
            (4, 1, "2024-01-18", True),
            (5, 2, "2024-01-10", False),
            (6, 3, "2024-01-20", False),
        ],
    )

    insert_result = sqlite_session.execute(
        """
        INSERT INTO alert_users (user_id, asset_maintenance_id, alert_definition_id)
        SELECT responsible_id, id, (SELECT id FROM alert_definition WHERE name = 'maintenances_today')
        FROM asset_maintenance
        WHERE planned_date_start IS NOT NULL
        AND planned_date_start BETWEEN :date_start AND :date_end
        AND cancelled = 0
        ON CONFLICT(user_id, asset_maintenance_id, alert_definition_id) DO NOTHING
    """,
        {"date_start": "2024-01-15", "date_end": "2024-01-17"},
    )

    sqlite_session.connection.commit()

    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 3

    select_result = sqlite_session.execute("""
        SELECT
            au.*,
            u.id as user_id_from_join,
            u.name as user_name,
            u.email as user_email
        FROM alert_users au
        LEFT JOIN users u ON u.id = au.user_id
        WHERE au.created_at >= datetime('now', '-1 minute')
        ORDER BY au.id
    """)

    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 3

    for row in select_result.get_data():
        assert row["user_id"] in [1, 2, 3]
        assert row["asset_maintenance_id"] in [1, 2, 3]
        assert row["alert_definition_id"] == 1
        assert row["user_name"] in ["John Doe", "Jane Smith", "Bob Wilson"]
        assert "@example.com" in row["user_email"]

    insert_result2 = sqlite_session.execute(
        """
        INSERT INTO alert_users (user_id, asset_maintenance_id, alert_definition_id)
        SELECT responsible_id, id, (SELECT id FROM alert_definition WHERE name = 'maintenances_today')
        FROM asset_maintenance
        WHERE planned_date_start IS NOT NULL
        AND planned_date_start BETWEEN :date_start AND :date_end
        AND cancelled = 0
        ON CONFLICT(user_id, asset_maintenance_id, alert_definition_id) DO NOTHING
    """,
        {"date_start": "2024-01-15", "date_end": "2024-01-17"},
    )

    assert insert_result2.rows_affected == 0

    count_result = sqlite_session.execute("SELECT COUNT(*) as count FROM alert_users")
    assert count_result.data is not None
    assert count_result.get_data()[0]["count"] == 3


def test_sqlite_for_update_generates_sql_but_may_not_work(sqlite_session: SqliteDriver) -> None:
    """Test that FOR UPDATE generates SQL for SQLite but note it doesn't provide row-level locking."""

    # Insert test data
    sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("sqlite_test", 100))

    # SQLite will generate FOR UPDATE SQL but it doesn't provide row-level locking like PostgreSQL/MySQL
    # The SQL should be generated without errors, but SQLite ignores the FOR UPDATE clause
    query = sql.select("*").from_("test_table").where_eq("name", "sqlite_test").for_update()

    # Should generate SQL without throwing an error
    stmt = query.build()
    # SQLite doesn't support FOR UPDATE, so SQLGlot strips it out (expected behavior)
    assert "FOR UPDATE" not in stmt.sql
    assert "SELECT" in stmt.sql  # But the rest of the query works

    # Should execute without error (SQLite just ignores the FOR UPDATE)
    result = sqlite_session.execute(query)
    assert result is not None
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "sqlite_test"


def test_sqlite_for_share_generates_sql_but_may_not_work(sqlite_session: SqliteDriver) -> None:
    """Test that FOR SHARE generates SQL for SQLite but note it doesn't provide row-level locking."""

    # Insert test data
    sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("sqlite_share", 200))

    # SQLite will generate FOR SHARE SQL but it doesn't provide row-level locking
    query = sql.select("*").from_("test_table").where_eq("name", "sqlite_share").for_share()

    # Should generate SQL without throwing an error
    stmt = query.build()
    # SQLite doesn't support FOR SHARE, so SQLGlot strips it out (expected behavior)
    assert "FOR SHARE" not in stmt.sql
    assert "SELECT" in stmt.sql  # But the rest of the query works

    # Should execute without error (SQLite just ignores the FOR SHARE)
    result = sqlite_session.execute(query)
    assert result is not None
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "sqlite_share"


def test_sqlite_for_update_skip_locked_generates_sql(sqlite_session: SqliteDriver) -> None:
    """Test that FOR UPDATE SKIP LOCKED generates SQL for SQLite."""

    # Insert test data
    sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("sqlite_skip", 300))

    # Should generate SQL even though SQLite doesn't support the functionality
    query = sql.select("*").from_("test_table").where_eq("name", "sqlite_skip").for_update(skip_locked=True)

    stmt = query.build()
    # The exact SQL generated may vary based on dialect support
    assert stmt.sql is not None

    # Should execute (SQLite will ignore unsupported clauses)
    result = sqlite_session.execute(query)
    assert result is not None
    assert len(result.data) == 1
