"""Test PSQLPy driver implementation."""

from typing import Any

import pytest

from sqlspec import SQLResult, StatementStack
from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver
from sqlspec.exceptions import SQLSpecError

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


async def test_psqlpy_cached_dml_and_errors(psqlpy_session: "PsqlpyDriver") -> None:
    """Native psqlpy cache hits preserve counts and SQLSpec exception translation."""
    from psqlpy.exceptions import Error

    for name in ("cached-first", "cached-second"):
        result = await psqlpy_session.execute("INSERT INTO test_table_psqlpy (name) VALUES (?)", name)
        assert result.rows_affected == 1
    query = "SELECT 10 / ?::integer AS value"
    assert (await psqlpy_session.execute(query, 2)).get_data() == [{"value": 5}]
    with pytest.raises(SQLSpecError) as error:
        await psqlpy_session.execute(query, 0)
    assert isinstance(error.value.__cause__, Error)


async def test_psqlpy_provide_connection_body_exception_propagates_cleanly(psqlpy_config: "PsqlpyConfig") -> None:
    """A body error inside provide_connection propagates once and leaves the pool working."""
    with pytest.raises(ValueError, match="raised in body"):
        async with psqlpy_config.provide_connection():
            raise ValueError("raised in body")

    async with psqlpy_config.provide_connection() as conn:
        result = await conn.execute("SELECT 1")
        assert len(result.result()) == 1


async def test_psqlpy_raw_begin_rolls_back_on_exception_exit(psqlpy_config: "PsqlpyConfig") -> None:
    """Raw BEGIN + INSERT followed by exception rolls back and leaves the pooled connection clean."""
    bounded_config = PsqlpyConfig(connection_config={**psqlpy_config.connection_config, "max_db_pool_size": 2})
    pool = await bounded_config.create_pool()
    try:
        async with pool.acquire():
            async with bounded_config.provide_connection() as setup_conn:
                await setup_conn.execute("CREATE TABLE IF NOT EXISTS psqlpy_raw_begin_test (name TEXT NOT NULL)")
                await setup_conn.execute("DELETE FROM psqlpy_raw_begin_test")

            dirty_conn_pid: int | None = None
            with pytest.raises(RuntimeError, match="abort raw connection begin"):
                async with bounded_config.provide_connection() as conn:
                    dirty_conn_pid = (await conn.execute("SELECT pg_backend_pid() AS pid")).result()[0]["pid"]
                    await conn.execute("BEGIN")
                    await conn.execute("INSERT INTO psqlpy_raw_begin_test (name) VALUES ('dirty_connection')")
                    raise RuntimeError("abort raw connection begin")

            async with bounded_config.provide_connection() as verify_conn:
                verify_pid = (await verify_conn.execute("SELECT pg_backend_pid() AS pid")).result()[0]["pid"]
                assert verify_pid == dirty_conn_pid
                rows = (await verify_conn.execute("SELECT name FROM psqlpy_raw_begin_test")).result()
                assert rows == []
                tx_status = (await verify_conn.execute("SELECT txid_current_if_assigned() AS txid")).result()
                assert tx_status[0]["txid"] is None

            dirty_session_pid: int | None = None
            with pytest.raises(RuntimeError, match="abort raw session begin"):
                async with bounded_config.provide_session() as session:
                    pid_res = await session.execute("SELECT pg_backend_pid() AS pid")
                    dirty_session_pid = pid_res.get_data()[0]["pid"]
                    await session.execute("BEGIN")
                    await session.execute("INSERT INTO psqlpy_raw_begin_test (name) VALUES ('dirty_session')")
                    raise RuntimeError("abort raw session begin")

            async with bounded_config.provide_session() as verify_session:
                verify_session_pid = (await verify_session.execute("SELECT pg_backend_pid() AS pid")).get_data()[0][
                    "pid"
                ]
                assert verify_session_pid == dirty_session_pid
                result = await verify_session.execute("SELECT name FROM psqlpy_raw_begin_test")
                assert result.get_data() == []
                tx_check = await verify_session.execute("SELECT txid_current_if_assigned() AS txid")
                assert tx_check.get_data()[0]["txid"] is None
    finally:
        await bounded_config.close_pool()


async def test_tenant_rls_native_pool_and_disposal(postgres_service: Any) -> None:
    """Sequential psqlpy leases restore identity and dispose an unverified backend."""
    from uuid import uuid4

    from sqlspec.adapters.psqlpy import PsqlpyRLSStrategy
    from sqlspec.exceptions import TenantCleanupError, TenantContextError
    from sqlspec.utils.tenant import TenantContext
    from sqlspec.utils.text import quote_identifier

    credentials = {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "username": postgres_service.user,
        "password": postgres_service.password,
        "db_name": postgres_service.database,
        "max_db_pool_size": 2,
    }
    admin_config = PsqlpyConfig(connection_config=credentials)
    admin = await admin_config.create_connection()
    role = "sqlspec_tenant_" + uuid4().hex[:12]
    table = "sqlspec_tenant_rows_" + uuid4().hex[:12]
    role_sql = await admin.fetch_val(
        "SELECT format('CREATE ROLE %I LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD %L', $1::text, $2::text)",
        [role, "sqlspec-tenant-test"],
    )
    await admin.execute(role_sql)
    quoted_role, quoted_table = quote_identifier(role), quote_identifier(table)
    config = PsqlpyConfig(
        connection_config={**credentials, "username": role, "password": "sqlspec-tenant-test"},
        driver_features={"tenant_strategy": PsqlpyRLSStrategy(variable_keys={"region": "app.region"})},
    )
    try:
        await admin.execute(f"CREATE TABLE {quoted_table} (tenant_id text NOT NULL, value integer NOT NULL)")
        await admin.execute(f"ALTER TABLE {quoted_table} ENABLE ROW LEVEL SECURITY")
        await admin.execute(f"ALTER TABLE {quoted_table} FORCE ROW LEVEL SECURITY")
        await admin.execute(
            f"CREATE POLICY tenant_policy ON {quoted_table} USING (tenant_id = current_setting('app.current_tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true))"
        )
        await admin.execute(f"GRANT SELECT, INSERT ON {quoted_table} TO {quoted_role}")
        first_pid = 0
        for tenant, value in [("a", 1), ("b", 2)]:
            with TenantContext.scope(tenant, variables={"region": "eu"} if tenant == "a" else None):
                async with config.provide_session() as session:
                    pid = await session.select_value("SELECT pg_backend_pid()")
                    assert pid == first_pid if first_pid else pid > 0
                    first_pid = pid
                    await session.execute(f"INSERT INTO {quoted_table} VALUES ($1, $2)", (tenant, value))
                    assert await session.select_value(f"SELECT value FROM {quoted_table}") == value
                    if tenant == "b":
                        assert await session.select_value("SELECT current_setting('app.region', true)") in {None, ""}
        with TenantContext.scope("a"):
            async with config.provide_session() as session:
                with pytest.raises(SQLSpecError):
                    await session.execute(f"INSERT INTO {quoted_table} VALUES ($1, $2)", ("b", 3))
                await session.begin()
                await session.commit()
                assert await session.select_value("SELECT current_setting('app.current_tenant_id', true)") == "a"
        with pytest.raises(TenantContextError):
            async with config.provide_session():
                pass
        with TenantContext.scope("a"):
            async with config.provide_connection() as connection:
                await connection.execute("BEGIN")
                await connection.execute(f"INSERT INTO {quoted_table} VALUES ($1, $2)", ["a", 99])
        with TenantContext.scope("b"):
            async with config.provide_connection() as connection:
                await connection.execute("BEGIN")
                await connection.execute("ROLLBACK")
                assert await connection.fetch_val("SELECT current_setting('app.current_tenant_id', true)") == "b"
                assert await connection.fetch_val("SELECT pg_backend_pid()") == first_pid
        with TenantContext.scope("a"):
            async with config.provide_session() as session:
                assert await session.select_value(f"SELECT count(*) FROM {quoted_table}") == 1
        strategy = config.driver_features["tenant_strategy"]

        class FailingRestore(PsqlpyRLSStrategy):
            __slots__ = ()

            async def checkin(self, connection: Any, state: Any) -> None:
                raise TenantCleanupError("injected restore failure")

        config.driver_features["tenant_strategy"] = FailingRestore()
        with TenantContext.scope("a"), pytest.raises(TenantCleanupError):
            async with config.provide_connection():
                pass
        config.driver_features["tenant_strategy"] = strategy
        with TenantContext.scope("b"):
            async with config.provide_session() as session:
                assert await session.select_value("SELECT pg_backend_pid()") != first_pid
                assert await session.select_value(f"SELECT value FROM {quoted_table}") == 2
    finally:
        await config.close_pool()
        await admin.execute(f"DROP TABLE IF EXISTS {quoted_table}")
        await admin.execute(f"DROP ROLE {quoted_role}")
        admin.close()
        await admin_config.close_pool()


async def test_tenant_partial_connection_hook_failure_discards_transactional_state(postgres_service: Any) -> None:
    """A failed setup hook cannot leave identity hidden behind a transaction."""
    from sqlspec.adapters.psqlpy import PsqlpyConnection
    from sqlspec.utils.tenant import TenantContext

    failure = RuntimeError("partial connection initialization")
    failed_pid: int | None = None

    async def partial_hook(connection: PsqlpyConnection) -> None:
        nonlocal failed_pid
        if failed_pid is not None:
            return
        failed_pid = await connection.fetch_val("SELECT pg_backend_pid()")
        await connection.execute("SELECT set_config($1, $2, false)", ["app.current_tenant_id", "a"])
        await connection.execute("BEGIN")
        raise failure

    config = PsqlpyConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "username": postgres_service.user,
            "password": postgres_service.password,
            "db_name": postgres_service.database,
            "max_db_pool_size": 2,
        },
        driver_features={"tenant_strategy": "rls", "on_connection_create": partial_hook},
    )
    try:
        with TenantContext.scope("a"), pytest.raises(RuntimeError) as caught:
            async with config.provide_connection():
                pass
        assert caught.value is failure
        with TenantContext.scope("b"):
            async with config.provide_connection() as connection:
                assert await connection.fetch_val("SELECT current_setting('app.current_tenant_id', true)") == "b"
                await connection.execute("ROLLBACK")
                assert await connection.fetch_val("SELECT current_setting('app.current_tenant_id', true)") == "b"
                assert await connection.fetch_val("SELECT pg_backend_pid()") != failed_pid
    finally:
        await config.close_pool()
