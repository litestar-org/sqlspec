"""Test Oracle-specific features."""

import operator
from pathlib import Path

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core import SQL, SQLResult, StatementConfig


def _lower_keys(row: dict[str, object]) -> dict[str, object]:
    return {key.lower(): value for key, value in row.items()}


pytestmark = pytest.mark.xdist_group("oracle")


def test_sync_plsql_block_execution(oracle_sync_session: OracleSyncDriver) -> None:
    """Test PL/SQL block execution with variables and control structures."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_plsql_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_plsql_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            calculated_value NUMBER
        )
    """)

    plsql_block = """
    DECLARE
        v_base_value NUMBER := 10;
        v_multiplier NUMBER := 3;
        v_result NUMBER;
        v_name VARCHAR2(50) := 'plsql_test';
    BEGIN
        -- Calculate a value
        v_result := v_base_value * v_multiplier;

        -- Conditional logic
        IF v_result > 25 THEN
            v_result := v_result + 100;
        END IF;

        -- Insert the calculated result
        INSERT INTO test_plsql_table (id, name, calculated_value)
        VALUES (1, v_name, v_result);

        -- Loop to insert additional records
        FOR i IN 2..4 LOOP
            INSERT INTO test_plsql_table (id, name, calculated_value)
            VALUES (i, v_name || '_' || i, v_result + i);
        END LOOP;

        COMMIT;
    END;
    """

    result = oracle_sync_session.execute_script(plsql_block)
    assert isinstance(result, SQLResult)

    select_result = oracle_sync_session.execute("SELECT id, name, calculated_value FROM test_plsql_table ORDER BY id")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 4

    rows = [_lower_keys(row) for row in select_result.data]
    first_row = rows[0]
    assert first_row["name"] == "plsql_test"
    assert first_row["calculated_value"] == 130

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_plsql_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_plsql_procedure_execution(oracle_async_session: OracleAsyncDriver) -> None:
    """Test creation and execution of PL/SQL stored procedures."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_proc_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE test_procedure'; EXCEPTION WHEN OTHERS THEN IF SQLCODE NOT IN (-942, -4043) THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_proc_table (
            id NUMBER PRIMARY KEY,
            input_value NUMBER,
            output_value NUMBER
        )
    """)

    procedure_sql = """
    CREATE OR REPLACE PROCEDURE test_procedure(
        p_input IN NUMBER,
        p_output OUT NUMBER
    ) AS
    BEGIN
        -- Simple calculation
        p_output := p_input * 2 + 10;

        -- Insert a record
        INSERT INTO test_proc_table (id, input_value, output_value)
        VALUES (p_input, p_input, p_output);

        COMMIT;
    END test_procedure;
    """

    await oracle_async_session.execute_script(procedure_sql)

    call_procedure = """
    DECLARE
        v_output NUMBER;
    BEGIN
        test_procedure(5, v_output);
        test_procedure(10, v_output);
    END;
    """

    result = await oracle_async_session.execute_script(call_procedure)
    assert isinstance(result, SQLResult)

    select_result = await oracle_async_session.execute(
        "SELECT id, input_value, output_value FROM test_proc_table ORDER BY id"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2

    rows = [_lower_keys(row) for row in select_result.data]
    first_row = rows[0]
    assert first_row["input_value"] == 5
    assert first_row["output_value"] == 20

    second_row = rows[1]
    assert second_row["input_value"] == 10
    assert second_row["output_value"] == 30

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE test_procedure'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_proc_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_oracle_data_types(oracle_sync_session: OracleSyncDriver) -> None:
    """Test Oracle-specific data types (NUMBER, VARCHAR2, CLOB, DATE)."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_datatypes_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_datatypes_table (
            id NUMBER(10) PRIMARY KEY,
            name VARCHAR2(100),
            description CLOB,
            price NUMBER(10, 2),
            created_date DATE,
            is_active NUMBER(1) CHECK (is_active IN (0, 1))
        )
    """)

    insert_sql = """
        INSERT INTO test_datatypes_table
        (id, name, description, price, created_date, is_active)
        VALUES (:1, :2, :3, :4, SYSDATE, :5)
    """

    description_text = "This is a long description that would be stored as CLOB data type in Oracle. " * 10

    result = oracle_sync_session.execute(insert_sql, (1, "Test Product", description_text, 99.99, 1))
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    select_result = oracle_sync_session.execute(
        "SELECT id, name, description, price, is_active FROM test_datatypes_table WHERE id = :1", (1,)
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = _lower_keys(select_result.data[0])
    assert row["id"] == 1
    assert row["name"] == "Test Product"

    assert row["price"] == 99.99
    assert row["is_active"] == 1

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_datatypes_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_oracle_analytic_functions(oracle_async_session: OracleAsyncDriver) -> None:
    """Test Oracle's analytic/window functions."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_analytics_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_analytics_table (
            id NUMBER PRIMARY KEY,
            department VARCHAR2(50),
            employee_name VARCHAR2(100),
            salary NUMBER
        )
    """)

    await oracle_async_session.execute_script("""
        INSERT ALL
            INTO test_analytics_table VALUES (1, 'SALES', 'John Doe', 50000)
            INTO test_analytics_table VALUES (2, 'SALES', 'Jane Smith', 55000)
            INTO test_analytics_table VALUES (3, 'SALES', 'Bob Johnson', 48000)
            INTO test_analytics_table VALUES (4, 'IT', 'Alice Brown', 60000)
            INTO test_analytics_table VALUES (5, 'IT', 'Charlie Wilson', 65000)
            INTO test_analytics_table VALUES (6, 'IT', 'Diana Lee', 58000)
        SELECT * FROM dual
    """)

    analytic_sql = """
        SELECT
            employee_name,
            department,
            salary,
            ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
            RANK() OVER (ORDER BY salary DESC) as overall_rank,
            SUM(salary) OVER (PARTITION BY department) as dept_total_salary,
            AVG(salary) OVER () as company_avg_salary
        FROM test_analytics_table
        ORDER BY department, salary DESC
    """

    result = await oracle_async_session.execute(analytic_sql)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 6

    rows = [_lower_keys(row) for row in result.data]
    it_employees = [row for row in rows if row["department"] == "IT"]
    assert len(it_employees) == 3

    it_sorted = sorted(it_employees, key=operator.itemgetter("salary"), reverse=True)
    assert it_sorted[0]["dept_rank"] == 1
    assert it_sorted[1]["dept_rank"] == 2
    assert it_sorted[2]["dept_rank"] == 3

    for emp in it_employees:
        assert emp["dept_total_salary"] == 183000

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_analytics_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_oracle_ddl_script_parsing(oracle_sync_session: OracleSyncDriver) -> None:
    """Test that Oracle DDL script can be parsed and prepared for execution."""

    _ = Path(__file__).parent.parent.parent.parent / "fixtures" / "oracle.ddl.sql"

    sample_oracle_ddl = """
    -- Oracle DDL Script Test
    ALTER SESSION SET CONTAINER = PDB1;

    CREATE TABLE test_vector_table (
        id NUMBER PRIMARY KEY,
        description VARCHAR2(1000),
        embedding VECTOR(768, FLOAT32),
        metadata JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    INMEMORY PRIORITY HIGH;

    CREATE SEQUENCE test_seq START WITH 1 INCREMENT BY 1;

    CREATE INDEX idx_vector_search ON test_vector_table (embedding)
    PARAMETERS ('type=IVF, neighbor_part=8');
    """

    config = StatementConfig(enable_parsing=True, enable_validation=False)

    stmt = SQL(sample_oracle_ddl, config=config, dialect="oracle").as_script()

    assert stmt.is_script is True

    sql_output = stmt.sql
    assert "ALTER SESSION SET CONTAINER" in sql_output
    assert "CREATE TABLE" in sql_output
    assert "VECTOR(768, FLOAT32)" in sql_output
    assert "JSON" in sql_output
    assert "INMEMORY PRIORITY HIGH" in sql_output
    assert "CREATE SEQUENCE" in sql_output


async def test_async_oracle_exception_handling(oracle_async_session: OracleAsyncDriver) -> None:
    """Test Oracle-specific exception handling in PL/SQL."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_exception_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script(
        "CREATE TABLE test_exception_table (id NUMBER PRIMARY KEY, name VARCHAR2(50))"
    )

    exception_handling_block = """
    DECLARE
        v_count NUMBER;
        duplicate_key EXCEPTION;
        PRAGMA EXCEPTION_INIT(duplicate_key, -1);
    BEGIN
        -- Insert first record
        INSERT INTO test_exception_table VALUES (1, 'First Record');

        -- Try to insert duplicate - should raise exception
        BEGIN
            INSERT INTO test_exception_table VALUES (1, 'Duplicate Record');
        EXCEPTION
            WHEN duplicate_key THEN
                -- Handle the duplicate key error
                INSERT INTO test_exception_table VALUES (2, 'Exception Handled');
        END;

        -- This should succeed
        INSERT INTO test_exception_table VALUES (3, 'Final Record');

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            -- Catch any other exceptions
            ROLLBACK;
            RAISE;
    END;
    """

    result = await oracle_async_session.execute_script(exception_handling_block)
    assert isinstance(result, SQLResult)

    select_result = await oracle_async_session.execute("SELECT id, name FROM test_exception_table ORDER BY id")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 3

    names = [row["name"] for row in (_lower_keys(row) for row in select_result.data)]
    assert "First Record" in names
    assert "Exception Handled" in names
    assert "Final Record" in names
    assert "Duplicate Record" not in names

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_exception_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
