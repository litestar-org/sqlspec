"""Test PSQLPy driver implementation."""

import pytest

from sqlspec import SQLResult, StatementStack
from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver

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
        val = result.get_data()[0][result.column_names[0]]
        assert val == "test"


async def test_psqlpy_statement_stack_continue_on_error(psqlpy_session: "PsqlpyDriver") -> None:
    """Sequential stack execution should honor continue-on-error flag."""

    await psqlpy_session.execute("DELETE FROM test_table_psqlpy")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table_psqlpy (id, name) VALUES (?, ?)", (1, "psqlpy-initial"))
        .push_execute("INSERT INTO test_table_psqlpy (id, name) VALUES (?, ?)", (1, "psqlpy-duplicate"))
        .push_execute("INSERT INTO test_table_psqlpy (id, name) VALUES (?, ?)", (2, "psqlpy-final"))
    )

    results = await psqlpy_session.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[1].error is not None

    verify = await psqlpy_session.execute("SELECT COUNT(*) AS total FROM test_table_psqlpy")
    assert verify.data is not None
    assert verify.get_data()[0]["total"] == 2


async def test_scalar_parameter_handling(psqlpy_session: "PsqlpyDriver") -> None:
    """Test handling of scalar parameters in various contexts."""

    insert_result = await psqlpy_session.execute("INSERT INTO test_table_psqlpy (name) VALUES (?)", "single_param")
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = await psqlpy_session.execute("SELECT * FROM test_table_psqlpy WHERE name = ?", "single_param")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.get_data()[0]["name"] == "single_param"

    value_result = await psqlpy_session.execute("SELECT id FROM test_table_psqlpy WHERE name = ?", "single_param")
    assert isinstance(value_result, SQLResult)
    assert value_result.data is not None
    assert len(value_result.data) == 1
    assert value_result.column_names is not None
    value = value_result.get_data()[0][value_result.column_names[0]]
    assert isinstance(value, int)

    missing_result = await psqlpy_session.execute(
        "SELECT * FROM test_table_psqlpy WHERE name = ?", "non_existent_param"
    )
    assert isinstance(missing_result, SQLResult)
    assert missing_result.data is not None
    assert len(missing_result.data) == 0


async def test_question_mark_in_edge_cases(psqlpy_session: "PsqlpyDriver") -> None:
    """Test that question marks in comments, strings, and other contexts aren't mistaken for parameters."""

    insert_result = await psqlpy_session.execute("INSERT INTO test_table_psqlpy (name) VALUES (?)", "edge_case_test")
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    result = await psqlpy_session.execute(
        "SELECT * FROM test_table_psqlpy WHERE name = ? AND '?' = '?'", "edge_case_test"
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "edge_case_test"

    result = await psqlpy_session.execute(
        "SELECT * FROM test_table_psqlpy WHERE name = ? -- Does this work with a ? in a comment?", "edge_case_test"
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "edge_case_test"

    result = await psqlpy_session.execute(
        "SELECT * FROM test_table_psqlpy WHERE name = ? /* Does this work with a ? in a block comment? */",
        "edge_case_test",
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "edge_case_test"

    result = await psqlpy_session.execute(
        "SELECT * FROM test_table_psqlpy WHERE name = ? AND '?' = '?' -- Another ? here", "edge_case_test"
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "edge_case_test"

    result = await psqlpy_session.execute(
        """
        SELECT * FROM test_table_psqlpy
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
    assert result.get_data()[0]["name"] == "edge_case_test"


async def test_regex_parameter_binding_complex_case(psqlpy_session: "PsqlpyDriver") -> None:
    """Test handling of complex SQL with question mark parameters in various positions."""

    insert_sql = "INSERT INTO test_table_psqlpy (name) VALUES (?)"
    parameters_list = [("complex1",), ("complex2",), ("complex3",)]
    many_result = await psqlpy_session.execute_many(insert_sql, parameters_list)
    assert isinstance(many_result, SQLResult)
    assert many_result.rows_affected == 3

    select_result = await psqlpy_session.execute(
        """
        SELECT t1.*
        FROM test_table_psqlpy t1
        JOIN test_table_psqlpy t2 ON t2.id <> t1.id
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
        names = {row["name"] for row in select_result.get_data()}
        assert len(names) >= 1

    subquery_result = await psqlpy_session.execute(
        """
        SELECT * FROM test_table_psqlpy
        WHERE name = ? AND id IN (
            SELECT id FROM test_table_psqlpy WHERE name = ? AND '?' = '?'
        )
        """,
        ("complex1", "complex1"),
    )
    assert isinstance(subquery_result, SQLResult)
    assert subquery_result.data is not None
    assert len(subquery_result.data) == 1
    assert subquery_result.get_data()[0]["name"] == "complex1"


async def test_postgresql_specific_features(psqlpy_session: "PsqlpyDriver") -> None:
    """Test PostgreSQL-specific features with psqlpy."""

    insert_result = await psqlpy_session.execute(
        "INSERT INTO test_table_psqlpy (name) VALUES (?) RETURNING id, name", ("returning_test",)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.data is not None
    assert len(insert_result.data) == 1
    assert insert_result.get_data()[0]["name"] == "returning_test"
    assert insert_result.get_data()[0]["id"] is not None

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
    assert "PostgreSQL" in pg_result.get_data()[0]["pg_version"]


@pytest.mark.integration
async def test_extensions_not_enabled_on_standard_postgres(psqlpy_config: "PsqlpyConfig") -> None:
    """Verify pgvector and paradedb extensions are not detected on standard postgres.

    Standard PostgreSQL does not have the 'vector' or 'pg_search' extensions installed,
    so the driver should detect this and keep the default 'postgres' dialect.
    """
    async with psqlpy_config.provide_session() as session:
        await session.execute("SELECT 1")

    assert psqlpy_config._pgvector_available is False  # pyright: ignore[reportPrivateUsage]
    assert psqlpy_config._paradedb_available is False  # pyright: ignore[reportPrivateUsage]
    assert psqlpy_config.statement_config.dialect == "postgres"
