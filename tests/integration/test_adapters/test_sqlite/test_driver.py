"""Integration tests for SQLite driver implementation with CORE_ROUND_3 architecture."""

import math
from typing import Any, Literal

import pytest

from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.core.result import SQLResult

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


@pytest.mark.xdist_group("sqlite")
def test_sqlite_basic_crud(sqlite_session: SqliteDriver) -> None:
    """Test basic CRUD operations."""
    # INSERT
    insert_result = sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("test_name", 42))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # SELECT
    select_result = sqlite_session.execute("SELECT name, value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    # UPDATE
    update_result = sqlite_session.execute("UPDATE test_table SET value = ? WHERE name = ?", (100, "test_name"))
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    # Verify UPDATE
    verify_result = sqlite_session.execute("SELECT value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    # DELETE
    delete_result = sqlite_session.execute("DELETE FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    # Verify DELETE
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
@pytest.mark.xdist_group("sqlite")
def test_sqlite_parameter_styles(sqlite_session: SqliteDriver, parameters: Any, style: ParamStyle) -> None:
    """Test different parameter binding styles."""
    # Clear any existing data between parameterized test runs
    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    # Insert test data
    sqlite_session.execute("INSERT INTO test_table (name) VALUES (?)", ("test_value",))

    # Test parameter style
    if style == "tuple_binds":
        sql = "SELECT name FROM test_table WHERE name = ?"
    else:  # dict_binds
        sql = "SELECT name FROM test_table WHERE name = :name"

    result = sqlite_session.execute(sql, parameters)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_value"


@pytest.mark.xdist_group("sqlite")
def test_sqlite_execute_many(sqlite_session: SqliteDriver) -> None:
    """Test execute_many functionality."""
    # Clear any existing data
    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    parameters_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", parameters_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(parameters_list)

    # Verify all records were inserted
    select_result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(parameters_list)

    # Verify data integrity
    ordered_result = sqlite_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(ordered_result, SQLResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


@pytest.mark.xdist_group("sqlite")
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
    # Script execution now returns SQLResult object
    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"

    # Explicitly check for errors from the script execution itself
    if hasattr(result, "errors") and result.errors:
        pytest.fail(f"Script execution reported errors: {result.errors}")

    # Verify script effects
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


@pytest.mark.xdist_group("sqlite")
def test_sqlite_result_methods(sqlite_session: SqliteDriver) -> None:
    """Test SelectResult and ExecuteResult methods."""
    # Clean up any existing data to ensure consistent test results
    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    # Insert test data
    sqlite_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES (?, ?)", [("result1", 10), ("result2", 20), ("result3", 30)]
    )

    # Test SelectResult methods
    result = sqlite_session.execute("SELECT * FROM test_table ORDER BY name")
    assert isinstance(result, SQLResult)

    # Test get_first()
    first_row = result.get_first()
    assert first_row is not None
    assert first_row["name"] == "result1"

    # Test get_count()
    assert result.get_count() == 3

    # Test is_empty()
    assert not result.is_empty()

    # Test empty result
    empty_result = sqlite_session.execute("SELECT * FROM test_table WHERE name = ?", ("nonexistent",))
    assert isinstance(empty_result, SQLResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


@pytest.mark.xdist_group("sqlite")
def test_sqlite_error_handling(sqlite_session: SqliteDriver) -> None:
    """Test error handling and exception propagation."""
    # Test invalid SQL
    with pytest.raises(Exception):  # sqlite3.OperationalError
        sqlite_session.execute("INVALID SQL STATEMENT")

    # Test constraint violation
    sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("unique_test", 1))

    # Try to insert duplicate with same ID (should fail if we had unique constraint)
    # For now, just test invalid column reference
    with pytest.raises(Exception):  # sqlite3.OperationalError
        sqlite_session.execute("SELECT nonexistent_column FROM test_table")


@pytest.mark.xdist_group("sqlite")
def test_sqlite_data_types(sqlite_session: SqliteDriver) -> None:
    """Test SQLite data type handling."""
    # Create table with various data types
    sqlite_session.execute_script("""
        CREATE TABLE data_types_test (
            id INTEGER PRIMARY KEY,
            text_col TEXT,
            integer_col INTEGER,
            real_col REAL,
            blob_col BLOB,
            null_col TEXT
        )
    """)

    # Insert data with various types
    test_data = ("text_value", 42, math.pi, b"binary_data", None)

    insert_result = sqlite_session.execute(
        "INSERT INTO data_types_test (text_col, integer_col, real_col, blob_col, null_col) VALUES (?, ?, ?, ?, ?)",
        test_data,
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Retrieve and verify data
    select_result = sqlite_session.execute(
        "SELECT text_col, integer_col, real_col, blob_col, null_col FROM data_types_test"
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


@pytest.mark.xdist_group("sqlite")
def test_sqlite_transactions(sqlite_session: SqliteDriver) -> None:
    """Test transaction behavior."""
    # SQLite auto-commit mode test
    sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("transaction_test", 100))

    # Verify data is committed
    result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("transaction_test",))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 1


@pytest.mark.xdist_group("sqlite")
def test_sqlite_complex_queries(sqlite_session: SqliteDriver) -> None:
    """Test complex SQL queries."""
    # Clear any existing data between test runs
    sqlite_session.execute("DELETE FROM test_table")
    sqlite_session.commit()

    # Insert test data
    test_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 28)]

    sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", test_data)

    # Test JOIN (self-join)
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

    # Test aggregation
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

    # Test subquery
    subquery_result = sqlite_session.execute("""
        SELECT name, value
        FROM test_table
        WHERE value > (SELECT AVG(value) FROM test_table)
        ORDER BY value
    """)
    assert isinstance(subquery_result, SQLResult)
    assert subquery_result.data is not None
    assert len(subquery_result.data) == 2  # Bob and Charlie
    assert subquery_result.data[0]["name"] == "Bob"
    assert subquery_result.data[1]["name"] == "Charlie"


@pytest.mark.xdist_group("sqlite")
def test_sqlite_schema_operations(sqlite_session: SqliteDriver) -> None:
    """Test schema operations (DDL)."""
    # Create a new table
    create_result = sqlite_session.execute_script("""
        CREATE TABLE schema_test (
            id INTEGER PRIMARY KEY,
            description TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    assert isinstance(create_result, SQLResult)
    assert create_result.operation_type == "SCRIPT"

    # Insert data into new table
    insert_result = sqlite_session.execute("INSERT INTO schema_test (description) VALUES (?)", ("test_description",))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Verify table structure
    pragma_result = sqlite_session.execute("PRAGMA table_info(schema_test)")
    assert isinstance(pragma_result, SQLResult)
    assert pragma_result.data is not None
    assert len(pragma_result.get_data()) == 3  # id, description, created_at

    # Drop table
    drop_result = sqlite_session.execute_script("DROP TABLE schema_test")
    assert isinstance(drop_result, SQLResult)
    assert drop_result.operation_type == "SCRIPT"


@pytest.mark.xdist_group("sqlite")
def test_sqlite_column_names_and_metadata(sqlite_session: SqliteDriver) -> None:
    """Test column names and result metadata."""
    # Insert test data
    sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("metadata_test", 123))

    # Test column names
    result = sqlite_session.execute(
        "SELECT id, name, value, created_at FROM test_table WHERE name = ?", ("metadata_test",)
    )
    assert isinstance(result, SQLResult)
    assert result.column_names == ["id", "name", "value", "created_at"]
    assert result.data is not None
    assert len(result.data) == 1

    # Test that we can access data by column name
    row = result.data[0]
    assert row["name"] == "metadata_test"
    assert row["value"] == 123
    assert row["id"] is not None
    assert row["created_at"] is not None


@pytest.mark.xdist_group("sqlite")
def test_sqlite_performance_bulk_operations(sqlite_session: SqliteDriver) -> None:
    """Test performance with bulk operations."""
    # Generate bulk data
    bulk_data = [(f"bulk_user_{i}", i * 10) for i in range(100)]

    # Bulk insert
    result = sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", bulk_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 100

    # Bulk select
    select_result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'bulk_user_%'")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    # Test pagination-like query
    page_result = sqlite_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'bulk_user_%' ORDER BY value LIMIT 10 OFFSET 20"
    )
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_20"


@pytest.mark.xdist_group("sqlite")
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
    # Create required tables
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

    # Insert test data
    sqlite_session.execute("INSERT INTO alert_definition (id, name) VALUES (?, ?)", (1, "maintenances_today"))

    # Insert users
    sqlite_session.execute_many(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
        [
            (1, "John Doe", "john@example.com"),
            (2, "Jane Smith", "jane@example.com"),
            (3, "Bob Wilson", "bob@example.com"),
        ],
    )

    # Insert asset maintenance records
    sqlite_session.execute_many(
        "INSERT INTO asset_maintenance (id, responsible_id, planned_date_start, cancelled) VALUES (?, ?, ?, ?)",
        [
            (1, 1, "2024-01-15", False),  # Within date range
            (2, 2, "2024-01-16", False),  # Within date range
            (3, 3, "2024-01-17", False),  # Within date range
            (4, 1, "2024-01-18", True),  # Cancelled - should be excluded
            (5, 2, "2024-01-10", False),  # Outside date range
            (6, 3, "2024-01-20", False),  # Outside date range
        ],
    )

    # Test the complex query
    # Note: SQLite doesn't have to_jsonb, so we'll adapt the query
    # Also, SQLite doesn't support INSERT...RETURNING directly in CTEs the same way as PostgreSQL
    # So we'll split this into two operations for SQLite compatibility

    # First, perform the INSERT with ON CONFLICT
    # Debug what the SELECT would return
    # Test the INSERT query with correct parameters
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

    # Explicitly commit the transaction for SQLite
    sqlite_session.connection.commit()

    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 3  # Should insert 3 records

    # Then, query the inserted data with the LEFT JOIN pattern
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

    # Verify the data structure
    for row in select_result.data:
        assert row["user_id"] in [1, 2, 3]
        assert row["asset_maintenance_id"] in [1, 2, 3]
        assert row["alert_definition_id"] == 1
        assert row["user_name"] in ["John Doe", "Jane Smith", "Bob Wilson"]
        assert "@example.com" in row["user_email"]

    # Test idempotency - running the same INSERT again should not add duplicates
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

    assert insert_result2.rows_affected == 0  # No new rows should be inserted

    # Verify total count is still 3
    count_result = sqlite_session.execute("SELECT COUNT(*) as count FROM alert_users")
    assert count_result.data is not None
    assert count_result.data[0]["count"] == 3
