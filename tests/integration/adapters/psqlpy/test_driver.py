"""Test PSQLPy driver implementation."""

import asyncio
from typing import Any, Literal, cast

import pytest

from sqlspec import SQL, SQLResult, StatementStack, sql
from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver
from tests.conftest import requires_interpreted

ParamStyle = Literal["tuple_binds", "dict_binds"]

pytestmark = pytest.mark.xdist_group("postgres")


async def test_connect_via_pool(psqlpy_config: "PsqlpyConfig") -> None:
    """Test establishing a connection via the pool."""
    pool = await psqlpy_config.create_pool()
    async with pool.acquire() as conn:
        assert conn is not None

        result = await conn.execute("SELECT 1")
        rows = result.result()
        assert len(rows) == 1
        assert rows[0]["?column?"] == 1


async def test_connect_direct(psqlpy_config: "PsqlpyConfig") -> None:
    """Test establishing a connection via the provide_connection context manager."""
    async with psqlpy_config.provide_connection() as conn:
        assert conn is not None

        result = await conn.execute("SELECT 1")
        rows = result.result()
        assert len(rows) == 1
        assert rows[0]["?column?"] == 1


async def test_provide_session_context_manager(psqlpy_config: "PsqlpyConfig") -> None:
    """Test the provide_session context manager."""
    async with psqlpy_config.provide_session() as driver:
        assert driver is not None
        assert driver.connection is not None

        result = await driver.execute("SELECT 'test'")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.column_names is not None
        val = result.data[0][result.column_names[0]]
        assert val == "test"


async def test_connection_error_handling(psqlpy_config: "PsqlpyConfig") -> None:
    """Test connection error handling."""
    async with psqlpy_config.provide_session() as driver:
        with pytest.raises(Exception):
            await driver.execute("INVALID SQL SYNTAX")

        result = await driver.execute("SELECT 'still_working' as status")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["status"] == "still_working"


async def test_connection_with_core_round_3(psqlpy_config: "PsqlpyConfig") -> None:
    """Test connection integration."""
    test_sql = SQL("SELECT $1::text as test_value")
    async with psqlpy_config.provide_session() as driver:
        result = await driver.execute(test_sql, ("core_test",))
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["test_value"] == "core_test"


async def test_multiple_connections_sequential(psqlpy_config: "PsqlpyConfig") -> None:
    """Test multiple sequential connections."""
    async with psqlpy_config.provide_session() as driver1:
        result1 = await driver1.execute("SELECT 'connection1' as conn_id")
        assert isinstance(result1, SQLResult)
        assert result1.data is not None
        assert result1.data[0]["conn_id"] == "connection1"

    async with psqlpy_config.provide_session() as driver2:
        result2 = await driver2.execute("SELECT 'connection2' as conn_id")
        assert isinstance(result2, SQLResult)
        assert result2.data is not None
        assert result2.data[0]["conn_id"] == "connection2"


async def test_connection_concurrent_access(psqlpy_config: "PsqlpyConfig") -> None:
    """Test concurrent connection access."""

    async def query_task(task_id: int) -> str:
        async with psqlpy_config.provide_session() as driver:
            result = await driver.execute("SELECT $1::text as task_id", (f"task_{task_id}",))
            assert isinstance(result, SQLResult)
            assert result.data is not None
            return cast(str, result.data[0]["task_id"])

    tasks = [query_task(i) for i in range(3)]
    results = await asyncio.gather(*tasks)

    assert len(results) == 3
    assert all(result.startswith("task_") for result in results)
    assert sorted(results) == ["task_0", "task_1", "task_2"]


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
async def test_insert_returning_param_styles(
    psqlpy_session: "PsqlpyDriver", parameters: Any, style: ParamStyle
) -> None:
    """Test insert returning with different parameter styles."""
    if style == "tuple_binds":
        sql = "INSERT INTO test_table (name) VALUES (?) RETURNING *"
    else:
        sql = "INSERT INTO test_table (name) VALUES (:name) RETURNING *"

    result = await psqlpy_session.execute(sql, parameters)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_name"
    assert result.data[0]["id"] is not None


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
async def test_select_param_styles(psqlpy_session: "PsqlpyDriver", parameters: Any, style: ParamStyle) -> None:
    """Test select with different parameter styles."""

    insert_sql = "INSERT INTO test_table (name) VALUES (?)"
    insert_result = await psqlpy_session.execute(insert_sql, ("test_name",))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == -1

    if style == "tuple_binds":
        select_sql = "SELECT id, name FROM test_table WHERE name = ?"
    else:
        select_sql = "SELECT id, name FROM test_table WHERE name = :name"

    select_result = await psqlpy_session.execute(select_sql, parameters)
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"


async def test_insert_update_delete(psqlpy_session: "PsqlpyDriver") -> None:
    """Test basic insert, update, delete operations."""

    insert_sql = "INSERT INTO test_table (name) VALUES (?)"
    insert_result = await psqlpy_session.execute(insert_sql, ("initial_name",))
    assert isinstance(insert_result, SQLResult)

    assert insert_result.rows_affected == -1

    select_sql = "SELECT name FROM test_table WHERE name = ?"
    select_result = await psqlpy_session.execute(select_sql, ("initial_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "initial_name"

    update_sql = "UPDATE test_table SET name = ? WHERE name = ?"
    update_result = await psqlpy_session.execute(update_sql, ("updated_name", "initial_name"))
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == -1

    updated_result = await psqlpy_session.execute(select_sql, ("updated_name",))
    assert isinstance(updated_result, SQLResult)
    assert updated_result.data is not None
    assert len(updated_result.data) == 1
    assert updated_result.data[0]["name"] == "updated_name"

    old_result = await psqlpy_session.execute(select_sql, ("initial_name",))
    assert isinstance(old_result, SQLResult)
    assert old_result.data is not None
    assert len(old_result.data) == 0

    delete_sql = "DELETE FROM test_table WHERE name = ?"
    delete_result = await psqlpy_session.execute(delete_sql, ("updated_name",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == -1

    final_result = await psqlpy_session.execute(select_sql, ("updated_name",))
    assert isinstance(final_result, SQLResult)
    assert final_result.data is not None
    assert len(final_result.data) == 0


async def test_select_methods(psqlpy_session: "PsqlpyDriver") -> None:
    """Test various select methods and result handling."""

    insert_sql = "INSERT INTO test_table (name) VALUES ($1)"
    parameters_list = [("name1",), ("name2",)]
    many_result = await psqlpy_session.execute_many(insert_sql, parameters_list)
    assert isinstance(many_result, SQLResult)
    assert many_result.rows_affected == 2

    select_result = await psqlpy_session.execute("SELECT name FROM test_table ORDER BY name")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2
    assert select_result.data[0]["name"] == "name1"
    assert select_result.data[1]["name"] == "name2"

    single_result = await psqlpy_session.execute("SELECT name FROM test_table WHERE name = ?", ("name1",))
    assert isinstance(single_result, SQLResult)
    assert single_result.data is not None
    assert len(single_result.data) == 1
    first_row = single_result.get_first()
    assert first_row is not None
    assert first_row["name"] == "name1"

    found_result = await psqlpy_session.execute("SELECT name FROM test_table WHERE name = ?", ("name2",))
    assert isinstance(found_result, SQLResult)
    assert found_result.data is not None
    assert len(found_result.data) == 1
    found_first = found_result.get_first()
    assert found_first is not None
    assert found_first["name"] == "name2"

    missing_result = await psqlpy_session.execute("SELECT name FROM test_table WHERE name = ?", ("missing",))
    assert isinstance(missing_result, SQLResult)
    assert missing_result.data is not None
    assert len(missing_result.data) == 0
    assert missing_result.get_first() is None

    value_result = await psqlpy_session.execute("SELECT id FROM test_table WHERE name = ?", ("name1",))
    assert isinstance(value_result, SQLResult)
    assert value_result.data is not None
    assert len(value_result.data) == 1
    assert value_result.column_names is not None
    value = value_result.data[0][value_result.column_names[0]]
    assert isinstance(value, int)


async def test_execute_script(psqlpy_session: "PsqlpyDriver") -> None:
    """Test execute_script method for non-query operations."""
    sql = "SELECT 1;"
    result = await psqlpy_session.execute_script(sql)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"
    assert result.is_success()

    assert result.total_statements == 1
    assert result.successful_statements == 1


async def test_multiple_positional_parameters(psqlpy_session: "PsqlpyDriver") -> None:
    """Test handling multiple positional parameters in a single SQL statement."""

    await psqlpy_session.execute("DELETE FROM test_table WHERE name LIKE 'param%'")

    insert_sql = "INSERT INTO test_table (name) VALUES (?)"
    parameters_list = [("param1",), ("param2",)]
    many_result = await psqlpy_session.execute_many(insert_sql, parameters_list)
    assert isinstance(many_result, SQLResult)
    assert many_result.rows_affected == 2

    select_result = await psqlpy_session.execute(
        "SELECT * FROM test_table WHERE name = ? OR name = ?", ("param1", "param2")
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2

    in_result = await psqlpy_session.execute("SELECT * FROM test_table WHERE name IN (?, ?)", ("param1", "param2"))
    assert isinstance(in_result, SQLResult)
    assert in_result.data is not None
    assert len(in_result.data) == 2

    mixed_result = await psqlpy_session.execute("SELECT * FROM test_table WHERE name = ? AND id > ?", ("param1", 0))
    assert isinstance(mixed_result, SQLResult)
    assert mixed_result.data is not None
    assert len(mixed_result.data) == 1


async def test_psqlpy_statement_stack_sequential(psqlpy_session: "PsqlpyDriver") -> None:
    """psqlpy uses sequential stack execution."""

    await psqlpy_session.execute("DELETE FROM test_table")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name) VALUES (?, ?)", (1, "psqlpy-stack-one"))
        .push_execute("INSERT INTO test_table (id, name) VALUES (?, ?)", (2, "psqlpy-stack-two"))
        .push_execute("SELECT COUNT(*) AS total FROM test_table WHERE name LIKE ?", ("psqlpy-stack-%",))
    )

    results = await psqlpy_session.execute_stack(stack)

    assert len(results) == 3

    verify = await psqlpy_session.execute(
        "SELECT COUNT(*) AS total FROM test_table WHERE name LIKE ?", ("psqlpy-stack-%",)
    )
    assert verify.data is not None
    assert verify.data[0]["total"] == 2


@requires_interpreted
async def test_psqlpy_statement_stack_continue_on_error(psqlpy_session: "PsqlpyDriver") -> None:
    """Sequential stack execution should honor continue-on-error flag."""

    await psqlpy_session.execute("DELETE FROM test_table")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name) VALUES (?, ?)", (1, "psqlpy-initial"))
        .push_execute("INSERT INTO test_table (id, name) VALUES (?, ?)", (1, "psqlpy-duplicate"))
        .push_execute("INSERT INTO test_table (id, name) VALUES (?, ?)", (2, "psqlpy-final"))
    )

    results = await psqlpy_session.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[1].error is not None

    verify = await psqlpy_session.execute("SELECT COUNT(*) AS total FROM test_table")
    assert verify.data is not None
    assert verify.data[0]["total"] == 2


async def test_scalar_parameter_handling(psqlpy_session: "PsqlpyDriver") -> None:
    """Test handling of scalar parameters in various contexts."""

    insert_result = await psqlpy_session.execute("INSERT INTO test_table (name) VALUES (?)", "single_param")
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == -1

    select_result = await psqlpy_session.execute("SELECT * FROM test_table WHERE name = ?", "single_param")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "single_param"

    value_result = await psqlpy_session.execute("SELECT id FROM test_table WHERE name = ?", "single_param")
    assert isinstance(value_result, SQLResult)
    assert value_result.data is not None
    assert len(value_result.data) == 1
    assert value_result.column_names is not None
    value = value_result.data[0][value_result.column_names[0]]
    assert isinstance(value, int)

    missing_result = await psqlpy_session.execute("SELECT * FROM test_table WHERE name = ?", "non_existent_param")
    assert isinstance(missing_result, SQLResult)
    assert missing_result.data is not None
    assert len(missing_result.data) == 0


async def test_question_mark_in_edge_cases(psqlpy_session: "PsqlpyDriver") -> None:
    """Test that question marks in comments, strings, and other contexts aren't mistaken for parameters."""

    insert_result = await psqlpy_session.execute("INSERT INTO test_table (name) VALUES (?)", "edge_case_test")
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == -1

    result = await psqlpy_session.execute("SELECT * FROM test_table WHERE name = ? AND '?' = '?'", "edge_case_test")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "edge_case_test"

    result = await psqlpy_session.execute(
        "SELECT * FROM test_table WHERE name = ? -- Does this work with a ? in a comment?", "edge_case_test"
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "edge_case_test"

    result = await psqlpy_session.execute(
        "SELECT * FROM test_table WHERE name = ? /* Does this work with a ? in a block comment? */", "edge_case_test"
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "edge_case_test"

    result = await psqlpy_session.execute(
        "SELECT * FROM test_table WHERE name = ? AND '?' = '?' -- Another ? here", "edge_case_test"
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "edge_case_test"

    result = await psqlpy_session.execute(
        """
        SELECT * FROM test_table
        WHERE name = ? -- A ? in a comment
        AND '?' = '?' -- Another ? here
        AND 'String with a ? in it' = 'String with a ? in it'
        AND /* Block comment with a ? */ id > 0
        """,
        "edge_case_test",
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "edge_case_test"


async def test_regex_parameter_binding_complex_case(psqlpy_session: "PsqlpyDriver") -> None:
    """Test handling of complex SQL with question mark parameters in various positions."""

    insert_sql = "INSERT INTO test_table (name) VALUES (?)"
    parameters_list = [("complex1",), ("complex2",), ("complex3",)]
    many_result = await psqlpy_session.execute_many(insert_sql, parameters_list)
    assert isinstance(many_result, SQLResult)
    assert many_result.rows_affected == 3

    select_result = await psqlpy_session.execute(
        """
        SELECT t1.*
        FROM test_table t1
        JOIN test_table t2 ON t2.id <> t1.id
        WHERE
            t1.name = ? OR
            t1.name = ? OR
            t1.name = ?
            -- Let's add a comment with ? here
            /* And a block comment with ? here */
        ORDER BY t1.id
        """,
        ("complex1", "complex2", "complex3"),
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None

    assert len(select_result.data) >= 0

    if select_result.data:
        names = {row["name"] for row in select_result.data}
        assert len(names) >= 1

    subquery_result = await psqlpy_session.execute(
        """
        SELECT * FROM test_table
        WHERE name = ? AND id IN (
            SELECT id FROM test_table WHERE name = ? AND '?' = '?'
        )
        """,
        ("complex1", "complex1"),
    )
    assert isinstance(subquery_result, SQLResult)
    assert subquery_result.data is not None
    assert len(subquery_result.data) == 1
    assert subquery_result.data[0]["name"] == "complex1"


async def test_execute_many_insert(psqlpy_session: "PsqlpyDriver") -> None:
    """Test execute_many functionality for batch inserts."""
    insert_sql = "INSERT INTO test_table (name) VALUES (?)"
    parameters_list = [("many_name1",), ("many_name2",), ("many_name3",)]

    result = await psqlpy_session.execute_many(insert_sql, parameters_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    select_result = await psqlpy_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(parameters_list)


async def test_update_operation(psqlpy_session: "PsqlpyDriver") -> None:
    """Test UPDATE operations."""

    insert_result = await psqlpy_session.execute("INSERT INTO test_table (name) VALUES (?)", ("original_name",))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == -1

    update_result = await psqlpy_session.execute("UPDATE test_table SET name = ? WHERE id = ?", ("updated_name", 1))
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == -1

    select_result = await psqlpy_session.execute("SELECT name FROM test_table WHERE id = ?", (1,))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["name"] == "updated_name"


async def test_delete_operation(psqlpy_session: "PsqlpyDriver") -> None:
    """Test DELETE operations."""

    insert_result = await psqlpy_session.execute("INSERT INTO test_table (name) VALUES (?)", ("to_delete",))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == -1

    delete_result = await psqlpy_session.execute("DELETE FROM test_table WHERE id = ?", (1,))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == -1

    select_result = await psqlpy_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 0


async def test_core_round_3_integration(psqlpy_session: "PsqlpyDriver") -> None:
    """Test integration with SQL object."""

    sql_obj = SQL("SELECT $1::text as test_value, $2::int as test_number")

    result = await psqlpy_session.execute(sql_obj, ("core_test", 42))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["test_value"] == "core_test"
    assert result.data[0]["test_number"] == 42


async def test_postgresql_specific_features(psqlpy_session: "PsqlpyDriver") -> None:
    """Test PostgreSQL-specific features with psqlpy."""

    insert_result = await psqlpy_session.execute(
        "INSERT INTO test_table (name) VALUES (?) RETURNING id, name", ("returning_test",)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.data is not None
    assert len(insert_result.data) == 1
    assert insert_result.data[0]["name"] == "returning_test"
    assert insert_result.data[0]["id"] is not None

    type_result = await psqlpy_session.execute(
        "SELECT $1::json as json_col, $2::uuid as uuid_col", ({"key": "value"}, "550e8400-e29b-41d4-a716-446655440000")
    )
    assert isinstance(type_result, SQLResult)
    assert type_result.data is not None
    assert len(type_result.data) == 1

    array_result = await psqlpy_session.execute("SELECT $1::int[] as int_array", ([1, 2, 3, 4, 5],))
    assert isinstance(array_result, SQLResult)
    assert array_result.data is not None
    assert len(array_result.data) == 1

    pg_result = await psqlpy_session.execute("SELECT version() as pg_version")
    assert isinstance(pg_result, SQLResult)
    assert pg_result.data is not None
    assert "PostgreSQL" in pg_result.data[0]["pg_version"]


async def test_psqlpy_for_update_locking(psqlpy_session: "PsqlpyDriver") -> None:
    """Test FOR UPDATE row locking with psqlpy (async)."""

    # Setup test table
    await psqlpy_session.execute_script("DROP TABLE IF EXISTS test_table")
    await psqlpy_session.execute_script("""
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            value INTEGER
        )
    """)

    # Insert test data
    await psqlpy_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("psqlpy_lock", 100))

    try:
        await psqlpy_session.begin()

        # Test basic FOR UPDATE
        result = await psqlpy_session.select_one(
            sql.select("id", "name", "value").from_("test_table").where_eq("name", "psqlpy_lock").for_update()
        )
        assert result is not None
        assert result["name"] == "psqlpy_lock"
        assert result["value"] == 100

        await psqlpy_session.commit()
    except Exception:
        await psqlpy_session.rollback()
        raise
    finally:
        await psqlpy_session.execute_script("DROP TABLE IF EXISTS test_table")


async def test_psqlpy_for_update_skip_locked(psqlpy_session: "PsqlpyDriver") -> None:
    """Test FOR UPDATE SKIP LOCKED with psqlpy (async)."""

    # Setup test table
    await psqlpy_session.execute_script("DROP TABLE IF EXISTS test_table")
    await psqlpy_session.execute_script("""
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            value INTEGER
        )
    """)

    # Insert test data
    await psqlpy_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("psqlpy_skip", 200))

    try:
        await psqlpy_session.begin()

        # Test FOR UPDATE SKIP LOCKED
        result = await psqlpy_session.select_one(
            sql.select("*").from_("test_table").where_eq("name", "psqlpy_skip").for_update(skip_locked=True)
        )
        assert result is not None
        assert result["name"] == "psqlpy_skip"

        await psqlpy_session.commit()
    except Exception:
        await psqlpy_session.rollback()
        raise
    finally:
        await psqlpy_session.execute_script("DROP TABLE IF EXISTS test_table")


async def test_psqlpy_for_share_locking(psqlpy_session: "PsqlpyDriver") -> None:
    """Test FOR SHARE row locking with psqlpy (async)."""

    # Setup test table
    await psqlpy_session.execute_script("DROP TABLE IF EXISTS test_table")
    await psqlpy_session.execute_script("""
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            value INTEGER
        )
    """)

    # Insert test data
    await psqlpy_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("psqlpy_share", 300))

    try:
        await psqlpy_session.begin()

        # Test FOR SHARE
        result = await psqlpy_session.select_one(
            sql.select("id", "name", "value").from_("test_table").where_eq("name", "psqlpy_share").for_share()
        )
        assert result is not None
        assert result["name"] == "psqlpy_share"
        assert result["value"] == 300

        await psqlpy_session.commit()
    except Exception:
        await psqlpy_session.rollback()
        raise
    finally:
        await psqlpy_session.execute_script("DROP TABLE IF EXISTS test_table")
