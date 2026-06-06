"""Test OracleDB async driver implementation."""

from typing import Any, Literal

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import sql
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleAsyncDriver, OraclePoolParams
from sqlspec.core import SQLResult
from sqlspec.exceptions import SQLSpecError

pytestmark = [pytest.mark.xdist_group("oracle")]

ParamStyle = Literal["positional_binds", "dict_binds"]


async def test_async_connection(oracle_23ai_service: "OracleService") -> None:
    """Test async connection components for OracleDB."""
    base_config = OraclePoolParams(
        host=oracle_23ai_service.host,
        port=oracle_23ai_service.port,
        service_name=oracle_23ai_service.service_name,
        user=oracle_23ai_service.user,
        password=oracle_23ai_service.password,
    )
    async_config = OracleAsyncConfig(connection_config=base_config)
    pool = await async_config.create_pool()
    assert pool is not None
    try:
        async with pool.acquire() as conn:
            assert conn is not None
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1 FROM dual")
                result = await cur.fetchone()
                assert result == (1,)
    finally:
        await pool.close()

    pooled_config = OraclePoolParams(**base_config)
    pooled_config["min"] = 1
    pooled_config["max"] = 5
    another_config = OracleAsyncConfig(connection_config=pooled_config)
    pool = await another_config.create_pool()
    assert pool is not None
    try:
        async with pool.acquire() as conn:
            assert conn is not None
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1 FROM dual")
                result = await cur.fetchone()
                assert result == (1,)
    finally:
        await pool.close()


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_name",), "positional_binds", id="positional_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
async def test_async_select(oracle_async_session: "OracleAsyncDriver", parameters: Any, style: ParamStyle) -> None:
    """Test async select functionality with Oracle parameter styles."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql = """
    CREATE TABLE test_table_oracledb_async (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    await oracle_async_session.execute_script(sql)

    if style == "positional_binds":
        insert_sql = "INSERT INTO test_table_oracledb_async (id, name) VALUES (1, :1)"
        select_sql = "SELECT name FROM test_table_oracledb_async WHERE name = :1"
    else:
        insert_sql = "INSERT INTO test_table_oracledb_async (id, name) VALUES (1, :name)"
        select_sql = "SELECT name FROM test_table_oracledb_async WHERE name = :name"

    insert_result = await oracle_async_session.execute(insert_sql, parameters)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = await oracle_async_session.execute(select_sql, parameters)
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.get_data()[0]["name"] == "test_name"

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_name",), "positional_binds", id="positional_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
async def test_async_select_value(
    oracle_async_session: "OracleAsyncDriver", parameters: Any, style: ParamStyle
) -> None:
    """Test async select value functionality with Oracle parameter styles."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql = """
    CREATE TABLE test_table_oracledb_async (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    await oracle_async_session.execute_script(sql)

    if style == "positional_binds":
        insert_sql = "INSERT INTO test_table_oracledb_async (id, name) VALUES (1, :1)"
    else:
        insert_sql = "INSERT INTO test_table_oracledb_async (id, name) VALUES (1, :name)"

    insert_result = await oracle_async_session.execute(insert_sql, parameters)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_sql = "SELECT 'test_value' FROM dual"
    value_result = await oracle_async_session.execute(select_sql)
    assert isinstance(value_result, SQLResult)
    assert value_result.data is not None
    assert len(value_result.data) == 1

    value = value_result.get_data()[0][value_result.column_names[0]]
    assert value == "test_value"

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_execute_many_insert(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test execute_many functionality for batch inserts."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_many_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql_create = """
    CREATE TABLE test_many_table_oracledb_async (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    await oracle_async_session.execute_script(sql_create)

    insert_sql = "INSERT INTO test_many_table_oracledb_async (id, name) VALUES (:1, :2)"
    parameters_list = [(1, "name1"), (2, "name2"), (3, "name3")]

    result = await oracle_async_session.execute_many(insert_sql, parameters_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(parameters_list)

    select_sql = "SELECT COUNT(*) as count FROM test_many_table_oracledb_async"
    count_result = await oracle_async_session.execute(select_sql)
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.get_data()[0]["count"] == len(parameters_list)

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_many_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_execute_script(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test execute_script functionality for multi-statement scripts."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_script_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    script = """
    CREATE TABLE test_script_table_oracledb_async (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    );
    INSERT INTO test_script_table_oracledb_async (id, name) VALUES (1, 'script_name1');
    INSERT INTO test_script_table_oracledb_async (id, name) VALUES (2, 'script_name2');
    """

    result = await oracle_async_session.execute_script(script)
    assert isinstance(result, SQLResult)

    select_result = await oracle_async_session.execute("SELECT COUNT(*) as count FROM test_script_table_oracledb_async")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.get_data()[0]["count"] == 2

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_script_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_update_operation(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test UPDATE operations."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql = """
    CREATE TABLE test_table_oracledb_async (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    await oracle_async_session.execute_script(sql)

    insert_result = await oracle_async_session.execute(
        "INSERT INTO test_table_oracledb_async (id, name) VALUES (1, :1)", ("original_name",)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    update_result = await oracle_async_session.execute(
        "UPDATE test_table_oracledb_async SET name = :1 WHERE name = :2", ("updated_name", "original_name")
    )
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    select_result = await oracle_async_session.execute(
        "SELECT name FROM test_table_oracledb_async WHERE name = :1", ("updated_name",)
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.get_data()[0]["name"] == "updated_name"

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_delete_operation(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test DELETE operations."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql = """
    CREATE TABLE test_table_oracledb_async (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    await oracle_async_session.execute_script(sql)

    insert_result = await oracle_async_session.execute(
        "INSERT INTO test_table_oracledb_async (id, name) VALUES (1, :1)", ("to_delete",)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    delete_result = await oracle_async_session.execute(
        "DELETE FROM test_table_oracledb_async WHERE name = :1", ("to_delete",)
    )
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    select_result = await oracle_async_session.execute("SELECT COUNT(*) as count FROM test_table_oracledb_async")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.get_data()[0]["count"] == 0

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_oracle_for_update_locking(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test FOR UPDATE row locking with Oracle."""

    # Setup test table
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("""
        CREATE TABLE test_table_oracledb_async (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    await oracle_async_session.execute(
        "INSERT INTO test_table_oracledb_async (id, name, value) VALUES (1, :1, :2)", ("oracle_lock", 100)
    )

    try:
        await oracle_async_session.begin()

        # Test basic FOR UPDATE
        result = await oracle_async_session.select_one(
            sql
            .select("id", "name", "value")
            .from_("test_table_oracledb_async")
            .where_eq("name", "oracle_lock")
            .for_update()
        )
        assert result is not None
        assert result["name"] == "oracle_lock"
        assert result["value"] == 100

        await oracle_async_session.commit()
    except Exception:
        await oracle_async_session.rollback()
        raise
    finally:
        await oracle_async_session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )


async def test_oracle_for_update_nowait(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test FOR UPDATE NOWAIT with Oracle."""

    # Setup test table
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("""
        CREATE TABLE test_table_oracledb_async (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    await oracle_async_session.execute(
        "INSERT INTO test_table_oracledb_async (id, name, value) VALUES (1, :1, :2)", ("oracle_nowait", 200)
    )

    try:
        await oracle_async_session.begin()

        # Test FOR UPDATE NOWAIT
        result = await oracle_async_session.select_one(
            sql.select("*").from_("test_table_oracledb_async").where_eq("name", "oracle_nowait").for_update(nowait=True)
        )
        assert result is not None
        assert result["name"] == "oracle_nowait"

        await oracle_async_session.commit()
    except Exception:
        await oracle_async_session.rollback()
        raise
    finally:
        await oracle_async_session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )


async def test_oracle_for_share_locking_unsupported(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test that FOR SHARE is not supported in Oracle and raises expected error."""

    # Setup test table
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("""
        CREATE TABLE test_table_oracledb_async (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    await oracle_async_session.execute(
        "INSERT INTO test_table_oracledb_async (id, name, value) VALUES (1, :1, :2)", ("oracle_share", 300)
    )

    try:
        await oracle_async_session.begin()

        # Test FOR SHARE - Oracle doesn't support this syntax, should raise ORA-02000
        # Note: Oracle only supports FOR UPDATE for row-level locking
        with pytest.raises(SQLSpecError, match=r"ORA-02000.*missing COMPRESS or UPDATE keyword"):
            await oracle_async_session.select_one(
                sql
                .select("id", "name", "value")
                .from_("test_table_oracledb_async")
                .where_eq("name", "oracle_share")
                .for_share()
            )

        await oracle_async_session.rollback()
    except Exception:
        await oracle_async_session.rollback()
        raise
    finally:
        await oracle_async_session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )


async def test_oracle_async_on_connection_create_hook(oracle_23ai_service: "OracleService") -> None:
    """Test on_connection_create callback is invoked for each connection."""
    hook_call_count = 0

    async def connection_hook(conn: Any, tag: str) -> None:
        nonlocal hook_call_count
        hook_call_count += 1

    config = OracleAsyncConfig(
        connection_config=OraclePoolParams(
            host=oracle_23ai_service.host,
            port=oracle_23ai_service.port,
            service_name=oracle_23ai_service.service_name,
            user=oracle_23ai_service.user,
            password=oracle_23ai_service.password,
            min=1,
            max=2,
        ),
        driver_features={"on_connection_create": connection_hook},
    )

    try:
        async with config.provide_session() as session:
            await session.execute("SELECT 1 FROM DUAL")
        assert hook_call_count >= 1, "Hook should be called at least once"
    finally:
        if config.connection_instance:
            await config.close_pool()
