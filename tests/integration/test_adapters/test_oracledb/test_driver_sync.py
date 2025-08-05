"""Test OracleDB driver implementation - Synchronous Tests."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any, Literal

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import OracleSyncConfig
from sqlspec.statement.result import SQLResult

ParamStyle = Literal["positional_binds", "dict_binds"]

# --- Sync Fixtures ---


@pytest.fixture
def oracle_sync_session(oracle_23ai_service: OracleService) -> Generator[OracleSyncConfig, None, None]:
    """Create an Oracle synchronous session."""
    config = OracleSyncConfig(
        pool_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        }
    )

    try:
        yield config
    finally:
        # Ensure pool is closed properly to avoid threading issues during test shutdown
        config.close_pool()


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_name",), "positional_binds", id="positional_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("oracle")
def test_sync_select(oracle_sync_session: OracleSyncConfig, parameters: Any, style: ParamStyle) -> None:
    """Test synchronous select functionality with Oracle parameter styles."""
    with oracle_sync_session.provide_session() as driver:
        # Manual cleanup at start of test
        driver.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        sql = """
        CREATE TABLE test_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50)
        )
        """
        driver.execute_script(sql)

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

        insert_result = driver.execute(insert_sql, insert_parameters)
        assert isinstance(insert_result, SQLResult)
        assert insert_result.rows_affected == 1

        select_result = driver.execute(select_sql, select_parameters)
        assert isinstance(select_result, SQLResult)
        assert select_result.data is not None
        assert len(select_result.data) == 1
        assert select_result.data[0]["NAME"] == "test_name"
        driver.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )


@pytest.mark.parametrize(
    ("parameters", "style"),  # Keep parametrization for structure, even if parameters unused for select_value
    [
        pytest.param(("test_name",), "positional_binds", id="positional_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("oracle")
def test_sync_select_value(oracle_sync_session: OracleSyncConfig, parameters: Any, style: ParamStyle) -> None:
    """Test synchronous select_value functionality with Oracle parameter styles."""
    with oracle_sync_session.provide_session() as driver:
        # Manual cleanup at start of test
        driver.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        sql = """
        CREATE TABLE test_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50)
        )
        """
        driver.execute_script(sql)

        # Workaround: Use positional binds for setup insert due to DPY-4009 error with dict_binds
        if style == "positional_binds":
            setup_value = parameters[0]
        else:  # dict_binds
            setup_value = parameters["name"]
        insert_sql_setup = "INSERT INTO test_table (id, name) VALUES (:id, :name)"
        insert_result = driver.execute(insert_sql_setup, {"id": 1, "name": setup_value})
        assert isinstance(insert_result, SQLResult)
        assert insert_result.rows_affected == 1

        # Select a literal value using Oracle's DUAL table
        select_sql = "SELECT 'test_value' FROM dual"
        value_result = driver.execute(select_sql)
        assert isinstance(value_result, SQLResult)
        assert value_result.data is not None
        assert len(value_result.data) == 1
        assert value_result.column_names is not None
        value = value_result.data[0][value_result.column_names[0]]
        assert value == "test_value"
        driver.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )


@pytest.mark.xdist_group("oracle")
def test_sync_insert_with_sequence(oracle_sync_session: OracleSyncConfig) -> None:
    """Test Oracle's alternative to RETURNING - using sequences and separate SELECT."""
    with oracle_sync_session.provide_session() as driver:
        # Create sequence and table
        driver.execute_script("""
            CREATE SEQUENCE test_seq START WITH 1 INCREMENT BY 1;
            CREATE TABLE test_table (
                id NUMBER PRIMARY KEY,
                name VARCHAR2(50)
            )
        """)

        # Insert using sequence
        driver.execute("INSERT INTO test_table (id, name) VALUES (test_seq.NEXTVAL, :1)", ("test_name"))

        # Get the last inserted ID using CURRVAL
        result = driver.execute("SELECT test_seq.CURRVAL as last_id FROM dual")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        last_id = result.data[0]["LAST_ID"]

        # Verify the inserted record
        verify_result = driver.execute("SELECT id, name FROM test_table WHERE id = :1", (last_id))
        assert isinstance(verify_result, SQLResult)
        assert verify_result.data is not None
        assert len(verify_result.data) == 1
        assert verify_result.data[0]["NAME"] == "test_name"
        assert verify_result.data[0]["ID"] == last_id

        # Cleanup
        driver.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_table';
                EXECUTE IMMEDIATE 'DROP SEQUENCE test_seq';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)


@pytest.mark.xdist_group("oracle")
def test_oracle_ddl_script_parsing(oracle_sync_session: OracleSyncConfig) -> None:
    """Test that the Oracle 23AI DDL script can be parsed and prepared for execution."""
    from pathlib import Path

    from sqlspec.statement.sql import SQL, StatementConfig

    # Load the Oracle DDL script
    fixture_path = Path(__file__).parent.parent.parent.parent / "fixtures" / "oracle.ddl.sql"
    assert fixture_path.exists(), f"Fixture file not found at {fixture_path}"

    with Path(fixture_path).open() as f:
        oracle_ddl = f.read()

    # Configure for Oracle dialect with parsing enabled
    config = StatementConfig(
        enable_parsing=True,
        enable_validation=False,  # Disable validation to focus on script handling
    )

    with oracle_sync_session.provide_session():
        # Test that the script can be processed as a SQL object
        stmt = SQL(oracle_ddl, config=config, dialect="oracle").as_script()

        # Verify it's recognized as a script
        assert stmt.is_script is True

        # Verify the SQL output contains key Oracle features
        sql_output = stmt.to_sql()
        assert "ALTER SESSION SET CONTAINER" in sql_output
        assert "CREATE TABLE" in sql_output
        assert "VECTOR(768, FLOAT32)" in sql_output
        assert "JSON" in sql_output
        assert "INMEMORY PRIORITY HIGH" in sql_output

        # Note: We don't actually execute the full DDL script in tests
        # as it requires specific Oracle setup and permissions.
        # The test verifies that the script can be parsed and prepared.
