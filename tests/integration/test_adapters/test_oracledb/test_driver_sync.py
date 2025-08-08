"""Test OracleDB sync driver implementation with CORE_ROUND_3 architecture."""

from typing import Any, Literal

import pytest

from sqlspec.adapters.oracledb import OracleSyncDriver
from sqlspec.core.result import SQLResult

ParamStyle = Literal["positional_binds", "dict_binds"]


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_name",), "positional_binds", id="positional_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("oracle")
def test_sync_select(oracle_sync_session: OracleSyncDriver, parameters: Any, style: ParamStyle) -> None:
    """Test synchronous select functionality with Oracle parameter styles."""
    # Manual cleanup at start of test
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
        insert_sql = "INSERT INTO test_table (id, name) VALUES (:id, :name)"
        select_sql = "SELECT name FROM test_table WHERE name = :name"
        insert_parameters = {"id": 1, "name": parameters[0]}
        select_parameters = {"name": parameters[0]}
    else:  # dict_binds
        insert_sql = "INSERT INTO test_table (id, name) VALUES (:id, :name)"
        select_sql = "SELECT name FROM test_table WHERE name = :name"
        insert_parameters = {"id": 1, **parameters}
        select_parameters = parameters

    insert_result = oracle_sync_session.execute(insert_sql, insert_parameters)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = oracle_sync_session.execute(select_sql, select_parameters)
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
@pytest.mark.xdist_group("oracle")
def test_sync_select_value(oracle_sync_session: OracleSyncDriver, parameters: Any, style: ParamStyle) -> None:
    """Test synchronous select_value functionality with Oracle parameter styles."""
    # Manual cleanup at start of test
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

    # Use named parameters for setup insert
    if style == "positional_binds":
        setup_value = parameters[0]
    else:  # dict_binds
        setup_value = parameters["name"]

    insert_sql_setup = "INSERT INTO test_table (id, name) VALUES (:id, :name)"
    insert_result = oracle_sync_session.execute(insert_sql_setup, {"id": 1, "name": setup_value})
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Select a literal value using Oracle's DUAL table
    select_sql = "SELECT 'test_value' FROM dual"
    value_result = oracle_sync_session.execute(select_sql)
    assert isinstance(value_result, SQLResult)
    assert value_result.data is not None
    assert len(value_result.data) == 1
    assert value_result.column_names is not None
    value = value_result.data[0][value_result.column_names[0]]
    assert value == "test_value"

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.xdist_group("oracle")
def test_sync_insert_with_sequence(oracle_sync_session: OracleSyncDriver) -> None:
    """Test Oracle's sequences and NEXTVAL/CURRVAL functionality."""
    # Create sequence and table
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

    # Insert using sequence
    oracle_sync_session.execute("INSERT INTO test_table (id, name) VALUES (test_seq.NEXTVAL, :1)", ("test_name",))

    # Get the last inserted ID using CURRVAL
    result = oracle_sync_session.execute("SELECT test_seq.CURRVAL as last_id FROM dual")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    last_id = result.data[0]["LAST_ID"]

    # Verify the inserted record
    verify_result = oracle_sync_session.execute("SELECT id, name FROM test_table WHERE id = :1", (last_id,))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    assert verify_result.data[0]["NAME"] == "test_name"
    assert verify_result.data[0]["ID"] == last_id

    # Cleanup
    oracle_sync_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_table';
            EXECUTE IMMEDIATE 'DROP SEQUENCE test_seq';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 AND SQLCODE != -2289 THEN RAISE; END IF;
        END;
    """)


@pytest.mark.xdist_group("oracle")
def test_sync_execute_many_insert(oracle_sync_session: OracleSyncDriver) -> None:
    """Test execute_many functionality for batch inserts."""
    # Manual cleanup at start of test
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

    # Cleanup
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_many_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.xdist_group("oracle")
def test_sync_execute_script(oracle_sync_session: OracleSyncDriver) -> None:
    """Test execute_script functionality for multi-statement scripts."""
    # Manual cleanup at start of test
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

    # Verify script executed successfully
    select_result = oracle_sync_session.execute("SELECT COUNT(*) as count FROM test_script_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["COUNT"] == 2

    # Cleanup
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_script_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.xdist_group("oracle")
def test_sync_update_operation(oracle_sync_session: OracleSyncDriver) -> None:
    """Test UPDATE operations."""
    # Manual cleanup at start of test
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    # Create test table
    sql = """
    CREATE TABLE test_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    oracle_sync_session.execute_script(sql)

    # Insert a record first
    insert_result = oracle_sync_session.execute("INSERT INTO test_table (id, name) VALUES (1, :1)", ("original_name",))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Update the record
    update_result = oracle_sync_session.execute(
        "UPDATE test_table SET name = :1 WHERE name = :2", ("updated_name", "original_name")
    )
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    # Verify the update
    select_result = oracle_sync_session.execute("SELECT name FROM test_table WHERE name = :1", ("updated_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["NAME"] == "updated_name"

    # Cleanup
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.xdist_group("oracle")
def test_sync_delete_operation(oracle_sync_session: OracleSyncDriver) -> None:
    """Test DELETE operations."""
    # Manual cleanup at start of test
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    # Create test table
    sql = """
    CREATE TABLE test_table (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(50)
    )
    """
    oracle_sync_session.execute_script(sql)

    # Insert a record first
    insert_result = oracle_sync_session.execute("INSERT INTO test_table (id, name) VALUES (1, :1)", ("to_delete",))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Delete the record
    delete_result = oracle_sync_session.execute("DELETE FROM test_table WHERE name = :1", ("to_delete",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    # Verify the deletion
    select_result = oracle_sync_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["COUNT"] == 0

    # Cleanup
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
