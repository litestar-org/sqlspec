"""Test Oracle-specific features with CORE_ROUND_3 architecture."""

import operator
from pathlib import Path

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core.result import SQLResult
from sqlspec.core.statement import SQL, StatementConfig

# Note: Only apply asyncio mark to actual async tests, not all tests in the file


@pytest.mark.xdist_group("oracle")
def test_sync_plsql_block_execution(oracle_sync_session: OracleSyncDriver) -> None:
    """Test PL/SQL block execution with variables and control structures."""
    # Cleanup first
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_plsql_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    # Create test table
    oracle_sync_session.execute_script("""
        CREATE TABLE test_plsql_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            calculated_value NUMBER
        )
    """)

    # Execute a PL/SQL block with variables and control logic
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

    # Verify the PL/SQL block executed correctly
    select_result = oracle_sync_session.execute("SELECT id, name, calculated_value FROM test_plsql_table ORDER BY id")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 4  # Records 1, 2, 3, 4 - 1 initial + 3 from loop (2..4)

    # Check the calculated values
    first_row = select_result.data[0]
    assert first_row["NAME"] == "plsql_test"
    assert first_row["CALCULATED_VALUE"] == 130  # 10 * 3 = 30, + 100 = 130

    # Cleanup
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_plsql_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.xdist_group("oracle")
async def test_async_plsql_procedure_execution(oracle_async_session: OracleAsyncDriver) -> None:
    """Test creation and execution of PL/SQL stored procedures."""
    # Cleanup first
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_proc_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE test_procedure'; EXCEPTION WHEN OTHERS THEN IF SQLCODE NOT IN (-942, -4043) THEN RAISE; END IF; END;"
    )

    # Create test table
    await oracle_async_session.execute_script("""
        CREATE TABLE test_proc_table (
            id NUMBER PRIMARY KEY,
            input_value NUMBER,
            output_value NUMBER
        )
    """)

    # Create a PL/SQL procedure
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

    # Call the procedure using PL/SQL block
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

    # Verify the procedure executed correctly
    select_result = await oracle_async_session.execute(
        "SELECT id, input_value, output_value FROM test_proc_table ORDER BY id"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2

    # Check calculated values
    first_row = select_result.data[0]
    assert first_row["INPUT_VALUE"] == 5
    assert first_row["OUTPUT_VALUE"] == 20  # 5 * 2 + 10

    second_row = select_result.data[1]
    assert second_row["INPUT_VALUE"] == 10
    assert second_row["OUTPUT_VALUE"] == 30  # 10 * 2 + 10

    # Cleanup
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE test_procedure'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_proc_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.xdist_group("oracle")
def test_sync_oracle_data_types(oracle_sync_session: OracleSyncDriver) -> None:
    """Test Oracle-specific data types (NUMBER, VARCHAR2, CLOB, DATE)."""
    # Cleanup first
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_datatypes_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    # Create table with Oracle-specific data types
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

    # Insert data with various Oracle data types
    insert_sql = """
        INSERT INTO test_datatypes_table
        (id, name, description, price, created_date, is_active)
        VALUES (:1, :2, :3, :4, SYSDATE, :5)
    """

    description_text = "This is a long description that would be stored as CLOB data type in Oracle. " * 10

    result = oracle_sync_session.execute(insert_sql, (1, "Test Product", description_text, 99.99, 1))
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Query the data back
    select_result = oracle_sync_session.execute(
        "SELECT id, name, description, price, is_active FROM test_datatypes_table WHERE id = :1", (1,)
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["ID"] == 1
    assert row["NAME"] == "Test Product"
    # CLOB field - Oracle returns this as a LOB object, read it
    description_value = row["DESCRIPTION"].read() if hasattr(row["DESCRIPTION"], "read") else str(row["DESCRIPTION"])
    assert len(description_value) > 100  # CLOB field
    assert row["PRICE"] == 99.99
    assert row["IS_ACTIVE"] == 1

    # Cleanup
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_datatypes_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.xdist_group("oracle")
async def test_async_oracle_analytic_functions(oracle_async_session: OracleAsyncDriver) -> None:
    """Test Oracle's analytic/window functions."""
    # Cleanup first
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_analytics_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    # Create test table
    await oracle_async_session.execute_script("""
        CREATE TABLE test_analytics_table (
            id NUMBER PRIMARY KEY,
            department VARCHAR2(50),
            employee_name VARCHAR2(100),
            salary NUMBER
        )
    """)

    # Insert test data
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

    # Test analytic functions (ROW_NUMBER, RANK, SUM OVER)
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

    # Verify analytic results for IT department
    it_employees = [row for row in result.data if row["DEPARTMENT"] == "IT"]
    assert len(it_employees) == 3

    # Check that ROW_NUMBER within IT department is correct
    it_sorted = sorted(it_employees, key=operator.itemgetter("SALARY"), reverse=True)
    assert it_sorted[0]["DEPT_RANK"] == 1  # Highest paid in IT
    assert it_sorted[1]["DEPT_RANK"] == 2
    assert it_sorted[2]["DEPT_RANK"] == 3

    # Check department total salary
    for emp in it_employees:
        assert emp["DEPT_TOTAL_SALARY"] == 183000  # 60000 + 65000 + 58000

    # Cleanup
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_analytics_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.xdist_group("oracle")
def test_oracle_ddl_script_parsing(oracle_sync_session: OracleSyncDriver) -> None:
    """Test that Oracle DDL script can be parsed and prepared for execution."""
    # Load the Oracle DDL script if it exists
    _ = Path(__file__).parent.parent.parent.parent / "fixtures" / "oracle.ddl.sql"

    # If fixture doesn't exist, create a sample Oracle DDL script
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

    # Configure for Oracle dialect with parsing enabled
    config = StatementConfig(
        enable_parsing=True,
        enable_validation=False,  # Disable validation to focus on script handling
    )

    # Test that the script can be processed as a SQL object
    stmt = SQL(sample_oracle_ddl, config=config, dialect="oracle").as_script()

    # Verify it's recognized as a script
    assert stmt.is_script is True

    # Verify the SQL output contains key Oracle features
    sql_output = stmt.sql
    assert "ALTER SESSION SET CONTAINER" in sql_output
    assert "CREATE TABLE" in sql_output
    assert "VECTOR(768, FLOAT32)" in sql_output
    assert "JSON" in sql_output
    assert "INMEMORY PRIORITY HIGH" in sql_output
    assert "CREATE SEQUENCE" in sql_output

    # Note: We don't actually execute the full DDL script in tests
    # as it requires specific Oracle setup and permissions.
    # The test verifies that the script can be parsed and prepared.


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.xdist_group("oracle")
async def test_async_oracle_exception_handling(oracle_async_session: OracleAsyncDriver) -> None:
    """Test Oracle-specific exception handling in PL/SQL."""
    # Cleanup first - ensure table doesn't exist
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_exception_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    # Create the table first (Oracle PL/SQL compiler needs to see the table at compile time)
    await oracle_async_session.execute_script(
        "CREATE TABLE test_exception_table (id NUMBER PRIMARY KEY, name VARCHAR2(50))"
    )

    # Test exception handling in PL/SQL blocks
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

    # Verify exception was handled properly
    select_result = await oracle_async_session.execute("SELECT id, name FROM test_exception_table ORDER BY id")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 3

    # Check that we have the expected records
    names = [row["NAME"] for row in select_result.data]
    assert "First Record" in names
    assert "Exception Handled" in names  # This proves exception was caught and handled
    assert "Final Record" in names
    assert "Duplicate Record" not in names  # This should not exist

    # Cleanup
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_exception_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
