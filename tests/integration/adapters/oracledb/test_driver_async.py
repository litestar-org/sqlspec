"""Test OracleDB async driver implementation."""

from typing import Any, Literal

import msgspec
import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import sql
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleAsyncDriver
from sqlspec.core import SQLResult
from sqlspec.exceptions import SQLSpecError

pytestmark = [pytest.mark.xdist_group("oracle"), pytest.mark.asyncio(loop_scope="function")]

ParamStyle = Literal["positional_binds", "dict_binds"]


async def test_async_connection(oracle_23ai_service: "OracleService") -> None:
    """Test async connection components for OracleDB."""
    base_config: dict[str, object] = {
        "host": oracle_23ai_service.host,
        "port": oracle_23ai_service.port,
        "service_name": oracle_23ai_service.service_name,
        "user": oracle_23ai_service.user,
        "password": oracle_23ai_service.password,
    }
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

    pooled_config = dict(base_config)
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
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql = """
    CREATE TABLE test_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    await oracle_async_session.execute_script(sql)

    if style == "positional_binds":
        insert_sql = "INSERT INTO test_table (id, name) VALUES (1, :1)"
        select_sql = "SELECT name FROM test_table WHERE name = :1"
    else:
        insert_sql = "INSERT INTO test_table (id, name) VALUES (1, :name)"
        select_sql = "SELECT name FROM test_table WHERE name = :name"

    insert_result = await oracle_async_session.execute(insert_sql, parameters)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = await oracle_async_session.execute(select_sql, parameters)
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
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
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql = """
    CREATE TABLE test_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    await oracle_async_session.execute_script(sql)

    if style == "positional_binds":
        insert_sql = "INSERT INTO test_table (id, name) VALUES (1, :1)"
    else:
        insert_sql = "INSERT INTO test_table (id, name) VALUES (1, :name)"

    insert_result = await oracle_async_session.execute(insert_sql, parameters)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_sql = "SELECT 'test_value' FROM dual"
    value_result = await oracle_async_session.execute(select_sql)
    assert isinstance(value_result, SQLResult)
    assert value_result.data is not None
    assert len(value_result.data) == 1

    value = value_result.data[0][value_result.column_names[0]]
    assert value == "test_value"

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_insert_with_sequence(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test Oracle's sequences and NEXTVAL/CURRVAL functionality."""

    await oracle_async_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP SEQUENCE test_seq';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -2289 THEN RAISE; END IF;
        END;
        """)
    await oracle_async_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_table';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN RAISE; END IF;
        END;
        """)

    await oracle_async_session.execute_script("""
        CREATE SEQUENCE test_seq START WITH 1 INCREMENT BY 1;
        CREATE TABLE test_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50)
        )
    """)

    await oracle_async_session.execute(
        "INSERT INTO test_table (id, name) VALUES (test_seq.NEXTVAL, :1)", ("test_name",)
    )

    result = await oracle_async_session.execute("SELECT test_seq.CURRVAL as last_id FROM dual")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    last_id = result.data[0]["last_id"]

    verify_result = await oracle_async_session.execute("SELECT id, name FROM test_table WHERE id = :1", (last_id,))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    assert verify_result.data[0]["name"] == "test_name"
    assert verify_result.data[0]["id"] == last_id

    await oracle_async_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_table';
            EXECUTE IMMEDIATE 'DROP SEQUENCE test_seq';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 AND SQLCODE != -2289 THEN RAISE; END IF;
        END;
    """)


async def test_async_execute_many_insert(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test execute_many functionality for batch inserts."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_many_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql_create = """
    CREATE TABLE test_many_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    await oracle_async_session.execute_script(sql_create)

    insert_sql = "INSERT INTO test_many_table (id, name) VALUES (:1, :2)"
    parameters_list = [(1, "name1"), (2, "name2"), (3, "name3")]

    result = await oracle_async_session.execute_many(insert_sql, parameters_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(parameters_list)

    select_sql = "SELECT COUNT(*) as count FROM test_many_table"
    count_result = await oracle_async_session.execute(select_sql)
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["count"] == len(parameters_list)

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_many_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_execute_script(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test execute_script functionality for multi-statement scripts."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_script_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    script = """
    CREATE TABLE test_script_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    );
    INSERT INTO test_script_table (id, name) VALUES (1, 'script_name1');
    INSERT INTO test_script_table (id, name) VALUES (2, 'script_name2');
    """

    result = await oracle_async_session.execute_script(script)
    assert isinstance(result, SQLResult)

    select_result = await oracle_async_session.execute("SELECT COUNT(*) as count FROM test_script_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 2

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_script_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_update_operation(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test UPDATE operations."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql = """
    CREATE TABLE test_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    await oracle_async_session.execute_script(sql)

    insert_result = await oracle_async_session.execute(
        "INSERT INTO test_table (id, name) VALUES (1, :1)", ("original_name",)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    update_result = await oracle_async_session.execute(
        "UPDATE test_table SET name = :1 WHERE name = :2", ("updated_name", "original_name")
    )
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    select_result = await oracle_async_session.execute("SELECT name FROM test_table WHERE name = :1", ("updated_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["name"] == "updated_name"

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_delete_operation(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test DELETE operations."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql = """
    CREATE TABLE test_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    await oracle_async_session.execute_script(sql)

    insert_result = await oracle_async_session.execute(
        "INSERT INTO test_table (id, name) VALUES (1, :1)", ("to_delete",)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    delete_result = await oracle_async_session.execute("DELETE FROM test_table WHERE name = :1", ("to_delete",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    select_result = await oracle_async_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 0

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_oracle_for_update_locking(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test FOR UPDATE row locking with Oracle."""

    # Setup test table
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("""
        CREATE TABLE test_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    await oracle_async_session.execute(
        "INSERT INTO test_table (id, name, value) VALUES (1, :1, :2)", ("oracle_lock", 100)
    )

    try:
        await oracle_async_session.begin()

        # Test basic FOR UPDATE
        result = await oracle_async_session.select_one(
            sql.select("id", "name", "value").from_("test_table").where_eq("name", "oracle_lock").for_update()
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
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )


async def test_oracle_for_update_nowait(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test FOR UPDATE NOWAIT with Oracle."""

    # Setup test table
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("""
        CREATE TABLE test_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    await oracle_async_session.execute(
        "INSERT INTO test_table (id, name, value) VALUES (1, :1, :2)", ("oracle_nowait", 200)
    )

    try:
        await oracle_async_session.begin()

        # Test FOR UPDATE NOWAIT
        result = await oracle_async_session.select_one(
            sql.select("*").from_("test_table").where_eq("name", "oracle_nowait").for_update(nowait=True)
        )
        assert result is not None
        assert result["name"] == "oracle_nowait"

        await oracle_async_session.commit()
    except Exception:
        await oracle_async_session.rollback()
        raise
    finally:
        await oracle_async_session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )


async def test_oracle_for_share_locking_unsupported(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test that FOR SHARE is not supported in Oracle and raises expected error."""

    # Setup test table
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("""
        CREATE TABLE test_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    await oracle_async_session.execute(
        "INSERT INTO test_table (id, name, value) VALUES (1, :1, :2)", ("oracle_share", 300)
    )

    try:
        await oracle_async_session.begin()

        # Test FOR SHARE - Oracle doesn't support this syntax, should raise ORA-02000
        # Note: Oracle only supports FOR UPDATE for row-level locking
        with pytest.raises(SQLSpecError, match=r"ORA-02000.*missing COMPRESS or UPDATE keyword"):
            await oracle_async_session.select_one(
                sql.select("id", "name", "value").from_("test_table").where_eq("name", "oracle_share").for_share()
            )

        await oracle_async_session.rollback()
    except Exception:
        await oracle_async_session.rollback()
        raise
    finally:
        await oracle_async_session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )


async def test_async_lowercase_columns_default(oracle_async_session: "OracleAsyncDriver") -> None:
    """Ensure implicit Oracle column names hydrate to lowercase when feature enabled."""
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_case_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("""
        CREATE TABLE test_case_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50)
        )
    """)

    await oracle_async_session.execute("INSERT INTO test_case_table (id, name) VALUES (:1, :2)", (1, "widget"))

    class Product(msgspec.Struct):
        id: int
        name: str

    result = await oracle_async_session.execute("SELECT id, name FROM test_case_table")
    row_dict = result.get_first()
    assert row_dict is not None
    assert "id" in row_dict
    assert "ID" not in row_dict
    assert row_dict["id"] == 1

    hydrated = result.get_first(schema_type=Product)
    assert hydrated is not None
    assert hydrated.id == 1
    assert hydrated.name == "widget"

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_case_table'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


async def test_async_uppercase_columns_when_disabled(oracle_async_config: OracleAsyncConfig) -> None:
    """Ensure disabling lowercase feature preserves uppercase columns."""
    custom_config = OracleAsyncConfig(
        connection_config=dict(oracle_async_config.connection_config),
        driver_features={"enable_lowercase_column_names": False},
    )

    async with custom_config.provide_session() as session:
        await session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_case_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        await session.execute_script("""
            CREATE TABLE test_case_table (
                id NUMBER PRIMARY KEY,
                name VARCHAR2(50)
            )
        """)

        await session.execute("INSERT INTO test_case_table (id, name) VALUES (:1, :2)", (1, "widget"))

        class Product(msgspec.Struct):
            id: int
            name: str

        result = await session.execute("SELECT id, name FROM test_case_table")
        row = result.get_first()
        assert row is not None
        assert "ID" in row
        assert "id" not in row
        with pytest.raises(msgspec.ValidationError):
            result.get_first(schema_type=Product)

        await session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_case_table'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )
