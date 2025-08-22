"""Integration tests for aiosqlite driver implementation."""

from __future__ import annotations

import math
from typing import Any, Literal

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteDriver
from sqlspec.core.result import SQLResult
from sqlspec.core.statement import SQL

pytestmark = pytest.mark.xdist_group("sqlite")
ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


async def test_aiosqlite_basic_crud(aiosqlite_session: AiosqliteDriver) -> None:
    """Test basic CRUD operations."""

    insert_result = await aiosqlite_session.execute(
        "INSERT INTO test_table (name, value) VALUES (?, ?)", ("test_name", 42)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = await aiosqlite_session.execute("SELECT name, value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    update_result = await aiosqlite_session.execute(
        "UPDATE test_table SET value = ? WHERE name = ?", (100, "test_name")
    )
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    verify_result = await aiosqlite_session.execute("SELECT value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    delete_result = await aiosqlite_session.execute("DELETE FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    empty_result = await aiosqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_value",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_value"}, "dict_binds", id="dict_binds"),
    ],
)
async def test_aiosqlite_parameter_styles(
    aiosqlite_session: AiosqliteDriver, parameters: Any, style: ParamStyle
) -> None:
    """Test different parameter binding styles."""

    await aiosqlite_session.execute("DELETE FROM test_table")
    await aiosqlite_session.commit()

    await aiosqlite_session.execute("INSERT INTO test_table (name) VALUES (?)", ("test_value",))

    if style == "tuple_binds":
        sql = "SELECT name FROM test_table WHERE name = ?"
    else:
        sql = "SELECT name FROM test_table WHERE name = :name"

    result = await aiosqlite_session.execute(sql, parameters)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_value"


async def test_aiosqlite_execute_many(aiosqlite_session: AiosqliteDriver) -> None:
    """Test execute_many functionality."""

    await aiosqlite_session.execute("DELETE FROM test_table")
    await aiosqlite_session.commit()

    parameters_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = await aiosqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", parameters_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(parameters_list)

    select_result = await aiosqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(parameters_list)

    ordered_result = await aiosqlite_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(ordered_result, SQLResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


async def test_aiosqlite_execute_script(aiosqlite_session: AiosqliteDriver) -> None:
    """Test execute_script functionality."""
    script = """
        INSERT INTO test_table (name, value) VALUES ('script_test1', 999);
        INSERT INTO test_table (name, value) VALUES ('script_test2', 888);
        UPDATE test_table SET value = 1000 WHERE name = 'script_test1';
    """

    result = await aiosqlite_session.execute_script(script)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"

    select_result = await aiosqlite_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'script_test%' ORDER BY name"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2
    assert select_result.data[0]["name"] == "script_test1"
    assert select_result.data[0]["value"] == 1000
    assert select_result.data[1]["name"] == "script_test2"
    assert select_result.data[1]["value"] == 888


async def test_aiosqlite_result_methods(aiosqlite_session: AiosqliteDriver) -> None:
    """Test SelectResult and ExecuteResult methods."""

    await aiosqlite_session.execute("DELETE FROM test_table")
    await aiosqlite_session.commit()

    await aiosqlite_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES (?, ?)", [("result1", 10), ("result2", 20), ("result3", 30)]
    )

    result = await aiosqlite_session.execute("SELECT * FROM test_table ORDER BY name")
    assert isinstance(result, SQLResult)

    first_row = result.get_first()
    assert first_row is not None
    assert first_row["name"] == "result1"

    assert result.get_count() == 3

    assert not result.is_empty()

    empty_result = await aiosqlite_session.execute("SELECT * FROM test_table WHERE name = ?", ("nonexistent",))
    assert isinstance(empty_result, SQLResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


async def test_aiosqlite_error_handling(aiosqlite_session: AiosqliteDriver) -> None:
    """Test error handling and exception propagation."""

    with pytest.raises(Exception):
        await aiosqlite_session.execute("INVALID SQL STATEMENT")

    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("unique_test", 1))

    with pytest.raises(Exception):
        await aiosqlite_session.execute("SELECT nonexistent_column FROM test_table")


async def test_aiosqlite_data_types(aiosqlite_session: AiosqliteDriver) -> None:
    """Test SQLite data type handling with aiosqlite."""

    await aiosqlite_session.execute_script("""
        CREATE TABLE data_types_test_unique (
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
        "INSERT INTO data_types_test_unique (text_col, integer_col, real_col, blob_col, null_col) VALUES (?, ?, ?, ?, ?)",
        test_data,
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = await aiosqlite_session.execute(
        "SELECT text_col, integer_col, real_col, blob_col, null_col FROM data_types_test_unique"
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

    await aiosqlite_session.execute_script("DROP TABLE data_types_test_unique")


async def test_aiosqlite_transactions(aiosqlite_session: AiosqliteDriver) -> None:
    """Test transaction behavior."""

    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("transaction_test", 100))

    result = await aiosqlite_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("transaction_test",)
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 1


async def test_aiosqlite_complex_queries(aiosqlite_session: AiosqliteDriver) -> None:
    """Test complex SQL queries."""

    await aiosqlite_session.execute("DELETE FROM test_table")
    await aiosqlite_session.commit()

    test_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 28)]

    await aiosqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", test_data)

    join_result = await aiosqlite_session.execute("""
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

    agg_result = await aiosqlite_session.execute("""
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

    subquery_result = await aiosqlite_session.execute("""
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


async def test_aiosqlite_column_names_and_metadata(aiosqlite_session: AiosqliteDriver) -> None:
    """Test column names and result metadata."""

    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("metadata_test", 123))

    result = await aiosqlite_session.execute(
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


async def test_aiosqlite_performance_bulk_operations(aiosqlite_session: AiosqliteDriver) -> None:
    """Test performance with bulk operations."""

    bulk_data = [(f"bulk_user_{i}", i * 10) for i in range(100)]

    result = await aiosqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", bulk_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 100

    select_result = await aiosqlite_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'bulk_user_%'"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    page_result = await aiosqlite_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'bulk_user_%' ORDER BY value LIMIT 10 OFFSET 20"
    )
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_20"


async def test_aiosqlite_sqlite_specific_features(aiosqlite_session: AiosqliteDriver) -> None:
    """Test SQLite-specific features with aiosqlite."""

    pragma_result = await aiosqlite_session.execute("PRAGMA user_version")
    assert isinstance(pragma_result, SQLResult)
    assert pragma_result.data is not None

    sqlite_result = await aiosqlite_session.execute("SELECT sqlite_version() as version")
    assert isinstance(sqlite_result, SQLResult)
    assert sqlite_result.data is not None
    assert sqlite_result.data[0]["version"] is not None

    try:
        json_result = await aiosqlite_session.execute("SELECT json('{}') as json_test")
        assert isinstance(json_result, SQLResult)
        assert json_result.data is not None
    except Exception:
        pass

    from sqlspec.core.statement import StatementConfig

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
    assert temp_result.data[0]["name"] == "temp"

    try:
        await aiosqlite_session.execute("DETACH DATABASE temp_db", statement_config=non_strict_config)
    except Exception:
        pass


async def test_aiosqlite_sql_object_integration(aiosqlite_session: AiosqliteDriver) -> None:
    """Test integration with SQL object."""

    sql_obj = SQL("SELECT name, value FROM test_table WHERE value > ?")

    await aiosqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("sql_test", 50))

    result = await aiosqlite_session.execute(sql_obj, (25,))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "sql_test"
    assert result.data[0]["value"] == 50


async def test_aiosqlite_core_result_features(aiosqlite_session: AiosqliteDriver) -> None:
    """Test SQLResult features."""

    test_data = [("core1", 10), ("core2", 20), ("core3", 30)]
    await aiosqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", test_data)

    result = await aiosqlite_session.execute("SELECT * FROM test_table WHERE name LIKE 'core%' ORDER BY name")
    assert isinstance(result, SQLResult)

    assert result.get_count() == 3
    assert not result.is_empty()

    first = result.get_first()
    assert first is not None
    assert first["name"] == "core1"

    assert "name" in result.column_names
    assert "value" in result.column_names

    assert len(result.data) == 3
    assert all(row["name"].startswith("core") for row in result.data)
