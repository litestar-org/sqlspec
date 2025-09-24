"""Test OracleDB sync driver implementation."""

from typing import Any, Literal

import pytest

from sqlspec.adapters.oracledb import OracleSyncDriver
from sqlspec.core.result import SQLResult

pytestmark = pytest.mark.xdist_group("oracle")

ParamStyle = Literal["positional_binds", "dict_binds"]


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_name",), "positional_binds", id="positional_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_sync_select(oracle_sync_session: OracleSyncDriver, parameters: Any, style: ParamStyle) -> None:
    """Test synchronous select functionality with Oracle parameter styles."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql = """
    CREATE TABLE test_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    oracle_sync_session.execute_script(sql)

    if style == "positional_binds":
        insert_sql = "INSERT INTO test_table (id, name) VALUES (1, :1)"
        select_sql = "SELECT name FROM test_table WHERE name = :1"
    else:
        insert_sql = "INSERT INTO test_table (id, name) VALUES (1, :name)"
        select_sql = "SELECT name FROM test_table WHERE name = :name"

    insert_result = oracle_sync_session.execute(insert_sql, parameters)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = oracle_sync_session.execute(select_sql, parameters)
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["NAME"] == "test_name"

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_name",), "positional_binds", id="positional_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_sync_select_value(oracle_sync_session: OracleSyncDriver, parameters: Any, style: ParamStyle) -> None:
    """Test synchronous select_value functionality with Oracle parameter styles."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql = """
    CREATE TABLE test_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    oracle_sync_session.execute_script(sql)

    if style == "positional_binds":
        insert_sql = "INSERT INTO test_table (id, name) VALUES (1, :1)"
    else:
        insert_sql = "INSERT INTO test_table (id, name) VALUES (1, :name)"

    insert_result = oracle_sync_session.execute(insert_sql, parameters)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_sql = "SELECT 'test_value' FROM dual"
    value_result = oracle_sync_session.execute(select_sql)
    assert isinstance(value_result, SQLResult)
    assert value_result.data is not None
    assert len(value_result.data) == 1

    value = value_result.data[0][value_result.column_names[0]]
    assert value == "test_value"

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_insert_with_sequence(oracle_sync_session: OracleSyncDriver) -> None:
    """Test Oracle's sequences and NEXTVAL/CURRVAL functionality."""

    oracle_sync_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP SEQUENCE test_seq';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -2289 THEN RAISE; END IF;
        END;
        """)
    oracle_sync_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_table';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN RAISE; END IF;
        END;
        """)

    oracle_sync_session.execute_script("""
        CREATE SEQUENCE test_seq START WITH 1 INCREMENT BY 1;
        CREATE TABLE test_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50)
        )
    """)

    oracle_sync_session.execute("INSERT INTO test_table (id, name) VALUES (test_seq.NEXTVAL, :1)", ("test_name",))

    result = oracle_sync_session.execute("SELECT test_seq.CURRVAL as last_id FROM dual")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    last_id = result.data[0]["LAST_ID"]

    verify_result = oracle_sync_session.execute("SELECT id, name FROM test_table WHERE id = :1", (last_id,))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    assert verify_result.data[0]["NAME"] == "test_name"
    assert verify_result.data[0]["ID"] == last_id

    oracle_sync_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_table';
            EXECUTE IMMEDIATE 'DROP SEQUENCE test_seq';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 AND SQLCODE != -2289 THEN RAISE; END IF;
        END;
    """)


def test_sync_execute_many_insert(oracle_sync_session: OracleSyncDriver) -> None:
    """Test execute_many functionality for batch inserts."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_many_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql_create = """
    CREATE TABLE test_many_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    oracle_sync_session.execute_script(sql_create)

    insert_sql = "INSERT INTO test_many_table (id, name) VALUES (:1, :2)"
    parameters_list = [(1, "name1"), (2, "name2"), (3, "name3")]

    result = oracle_sync_session.execute_many(insert_sql, parameters_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(parameters_list)

    select_sql = "SELECT COUNT(*) as count FROM test_many_table"
    count_result = oracle_sync_session.execute(select_sql)
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["COUNT"] == len(parameters_list)

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_many_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_execute_script(oracle_sync_session: OracleSyncDriver) -> None:
    """Test execute_script functionality for multi-statement scripts."""

    oracle_sync_session.execute_script(
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

    result = oracle_sync_session.execute_script(script)
    assert isinstance(result, SQLResult)

    select_result = oracle_sync_session.execute("SELECT COUNT(*) as count FROM test_script_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["COUNT"] == 2

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_script_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_update_operation(oracle_sync_session: OracleSyncDriver) -> None:
    """Test UPDATE operations."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql = """
    CREATE TABLE test_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    oracle_sync_session.execute_script(sql)

    insert_result = oracle_sync_session.execute("INSERT INTO test_table (id, name) VALUES (1, :1)", ("original_name",))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    update_result = oracle_sync_session.execute(
        "UPDATE test_table SET name = :1 WHERE name = :2", ("updated_name", "original_name")
    )
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    select_result = oracle_sync_session.execute("SELECT name FROM test_table WHERE name = :1", ("updated_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["NAME"] == "updated_name"

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_delete_operation(oracle_sync_session: OracleSyncDriver) -> None:
    """Test DELETE operations."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    sql = """
    CREATE TABLE test_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    oracle_sync_session.execute_script(sql)

    insert_result = oracle_sync_session.execute("INSERT INTO test_table (id, name) VALUES (1, :1)", ("to_delete",))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    delete_result = oracle_sync_session.execute("DELETE FROM test_table WHERE name = :1", ("to_delete",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    select_result = oracle_sync_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["COUNT"] == 0

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_oracle_sync_for_update_locking(oracle_sync_session: OracleSyncDriver) -> None:
    """Test FOR UPDATE row locking with Oracle (sync)."""
    from sqlspec import sql

    # Setup test table
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    oracle_sync_session.execute_script("""
        CREATE TABLE test_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    oracle_sync_session.execute(
        "INSERT INTO test_table (id, name, value) VALUES (1, :1, :2)", ("oracle_sync_lock", 100)
    )

    try:
        oracle_sync_session.begin()

        # Test basic FOR UPDATE
        result = oracle_sync_session.select_one(
            sql.select("id", "name", "value").from_("test_table").where_eq("name", "oracle_sync_lock").for_update()
        )
        assert result is not None
        assert result["NAME"] == "oracle_sync_lock"
        assert result["VALUE"] == 100

        oracle_sync_session.commit()
    except Exception:
        oracle_sync_session.rollback()
        raise
    finally:
        oracle_sync_session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )


def test_oracle_sync_for_update_nowait(oracle_sync_session: OracleSyncDriver) -> None:
    """Test FOR UPDATE NOWAIT with Oracle (sync)."""
    from sqlspec import sql

    # Setup test table
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    oracle_sync_session.execute_script("""
        CREATE TABLE test_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    oracle_sync_session.execute(
        "INSERT INTO test_table (id, name, value) VALUES (1, :1, :2)", ("oracle_sync_nowait", 200)
    )

    try:
        oracle_sync_session.begin()

        # Test FOR UPDATE NOWAIT
        result = oracle_sync_session.select_one(
            sql.select("*").from_("test_table").where_eq("name", "oracle_sync_nowait").for_update(nowait=True)
        )
        assert result is not None
        assert result["NAME"] == "oracle_sync_nowait"

        oracle_sync_session.commit()
    except Exception:
        oracle_sync_session.rollback()
        raise
    finally:
        oracle_sync_session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )


def test_oracle_sync_for_share_locking(oracle_sync_session: OracleSyncDriver) -> None:
    """Test FOR SHARE row locking with Oracle (sync)."""
    from sqlspec import sql

    # Setup test table
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    oracle_sync_session.execute_script("""
        CREATE TABLE test_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    oracle_sync_session.execute(
        "INSERT INTO test_table (id, name, value) VALUES (1, :1, :2)", ("oracle_sync_share", 300)
    )

    try:
        oracle_sync_session.begin()

        # Test FOR SHARE (Oracle uses FOR UPDATE for shared locks too)
        result = oracle_sync_session.select_one(
            sql.select("id", "name", "value").from_("test_table").where_eq("name", "oracle_sync_share").for_share()
        )
        assert result is not None
        assert result["NAME"] == "oracle_sync_share"
        assert result["VALUE"] == 300

        oracle_sync_session.commit()
    except Exception:
        oracle_sync_session.rollback()
        raise
    finally:
        oracle_sync_session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )
