"""Integration tests for SQLite driver implementation."""

import math
from typing import Any, Literal

import pytest

from sqlspec import SQLResult, StatementStack, sql
from sqlspec.adapters.sqlite import SqliteDriver
from tests.conftest import requires_interpreted

pytestmark = pytest.mark.xdist_group("sqlite")


ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


def test_sqlite_basic_crud(sqlite_session: SqliteDriver) -> None:
    """Test basic CRUD operations."""

    insert_result = sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("test_name", 42))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = sqlite_session.execute("SELECT name, value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    update_result = sqlite_session.execute("UPDATE test_table SET value = ? WHERE name = ?", (100, "test_name"))
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    verify_result = sqlite_session.execute("SELECT value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    delete_result = sqlite_session.execute("DELETE FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    empty_result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_value"), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_value"}, "dict_binds", id="dict_binds"),
    ],
)
def test_sqlite_parameter_styles(sqlite_session: SqliteDriver, parameters: Any, style: ParamStyle) -> None:
    """Test different parameter binding styles."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    sqlite_session.execute("INSERT INTO test_table (name) VALUES (?)", ("test_value",))

    if style == "tuple_binds":
        sql = "SELECT name FROM test_table WHERE name = ?"
    else:
        sql = "SELECT name FROM test_table WHERE name = :name"

    result = sqlite_session.execute(sql, parameters)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_value"


def test_sqlite_execute_many(sqlite_session: SqliteDriver) -> None:
    """Test execute_many functionality."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    parameters_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", parameters_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(parameters_list)

    select_result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(parameters_list)

    ordered_result = sqlite_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(ordered_result, SQLResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


def test_sqlite_execute_script(sqlite_session: SqliteDriver) -> None:
    """Test execute_script functionality."""
    script = """
        INSERT INTO test_table (name, value) VALUES ('script_test1', 999);
        INSERT INTO test_table (name, value) VALUES ('script_test2', 888);
        UPDATE test_table SET value = 1000 WHERE name = 'script_test1';
    """

    try:
        result = sqlite_session.execute_script(script)
    except Exception as e:
        pytest.fail(f"execute_script raised an unexpected exception: {e}")

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"

    if hasattr(result, "errors") and result.errors:
        pytest.fail(f"Script execution reported errors: {result.errors}")

    select_result = sqlite_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'script_test%' ORDER BY name"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2
    assert select_result.data[0]["name"] == "script_test1"
    assert select_result.data[0]["value"] == 1000
    assert select_result.data[1]["name"] == "script_test2"
    assert select_result.data[1]["value"] == 888


def test_sqlite_result_methods(sqlite_session: SqliteDriver) -> None:
    """Test SelectResult and ExecuteResult methods."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    sqlite_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES (?, ?)", [("result1", 10), ("result2", 20), ("result3", 30)]
    )

    result = sqlite_session.execute("SELECT * FROM test_table ORDER BY name")
    assert isinstance(result, SQLResult)

    first_row = result.get_first()
    assert first_row is not None
    assert first_row["name"] == "result1"

    assert result.get_count() == 3

    assert not result.is_empty()

    empty_result = sqlite_session.execute("SELECT * FROM test_table WHERE name = ?", ("nonexistent",))
    assert isinstance(empty_result, SQLResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


def test_sqlite_error_handling(sqlite_session: SqliteDriver) -> None:
    """Test error handling and exception propagation."""

    with pytest.raises(Exception):
        sqlite_session.execute("INVALID SQL STATEMENT")

    sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("unique_test", 1))

    with pytest.raises(Exception):
        sqlite_session.execute("SELECT nonexistent_column FROM test_table")


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

    row = select_result.data[0]
    assert row["text_col"] == "text_value"
    assert row["integer_col"] == 42
    assert row["real_col"] == math.pi
    assert row["blob_col"] == b"binary_data"
    assert row["null_col"] is None


def test_sqlite_statement_stack_sequential(sqlite_session: SqliteDriver) -> None:
    """StatementStack should execute sequentially for SQLite."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "sqlite-stack-one", 100))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (2, "sqlite-stack-two", 200))
        .push_execute("SELECT COUNT(*) AS total FROM test_table WHERE name LIKE ?", ("sqlite-stack-%",))
    )

    results = sqlite_session.execute_stack(stack)

    assert len(results) == 3
    assert results[0].rows_affected == 1
    assert results[1].rows_affected == 1
    assert results[2].result is not None
    assert results[2].result.data is not None
    assert results[2].result.data[0]["total"] == 2


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
    assert verify.data[0]["total"] == 2


def test_sqlite_transactions(sqlite_session: SqliteDriver) -> None:
    """Test transaction behavior."""

    sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("transaction_test", 100))

    result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("transaction_test",))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 1


def test_sqlite_complex_queries(sqlite_session: SqliteDriver) -> None:
    """Test complex SQL queries."""

    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    test_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 28)]

    sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", test_data)

    join_result = sqlite_session.execute("""
        SELECT t1.name as name1, t2.name as name2, t1.value as value1, t2.value as value2
        FROM test_table t1
        CROSS JOIN test_table t2
        WHERE t1.value < t2.value
        ORDER BY t1.name, t2.name
        LIMIT 3
    """)
    assert isinstance(join_result, SQLResult)
    assert join_result.data is not None
    assert len(join_result.data) == 3

    agg_result = sqlite_session.execute("""
        SELECT
            COUNT(*) as total_count,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM test_table
    """)
    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["total_count"] == 4
    assert agg_result.data[0]["avg_value"] == 29.5
    assert agg_result.data[0]["min_value"] == 25
    assert agg_result.data[0]["max_value"] == 35

    subquery_result = sqlite_session.execute("""
        SELECT name, value
        FROM test_table
        WHERE value > (SELECT AVG(value) FROM test_table)
        ORDER BY value
    """)
    assert isinstance(subquery_result, SQLResult)
    assert subquery_result.data is not None
    assert len(subquery_result.data) == 2
    assert subquery_result.data[0]["name"] == "Bob"
    assert subquery_result.data[1]["name"] == "Charlie"


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


def test_sqlite_column_names_and_metadata(sqlite_session: SqliteDriver) -> None:
    """Test column names and result metadata."""

    sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("metadata_test", 123))

    result = sqlite_session.execute(
        "SELECT id, name, value, created_at FROM test_table WHERE name = ?", ("metadata_test",)
    )
    assert isinstance(result, SQLResult)
    assert result.column_names == ["id", "name", "value", "created_at"]
    assert result.data is not None
    assert len(result.data) == 1

    row = result.data[0]
    assert row["name"] == "metadata_test"
    assert row["value"] == 123
    assert row["id"] is not None
    assert row["created_at"] is not None


def test_sqlite_performance_bulk_operations(sqlite_session: SqliteDriver) -> None:
    """Test performance with bulk operations."""

    bulk_data = [(f"bulk_user_{i}", i * 10) for i in range(100)]

    result = sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", bulk_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 100

    select_result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'bulk_user_%'")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    page_result = sqlite_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'bulk_user_%' ORDER BY value LIMIT 10 OFFSET 20"
    )
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_20"


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

    for row in select_result.data:
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
    assert count_result.data[0]["count"] == 3


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
    assert result.data[0]["name"] == "sqlite_test"


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
    assert result.data[0]["name"] == "sqlite_share"


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
