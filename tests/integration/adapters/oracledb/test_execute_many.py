"""Test Oracle execute_many functionality.

Only Oracle-specific batch semantics remain here (NEXTVAL sequence batching and
mid-batch constraint-violation partial-commit). Generic insert/update/named-dict
batching is covered by the shared execute_many and parameter-style contracts.
"""

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core import SQLResult

pytestmark = pytest.mark.xdist_group("oracle")


async def test_async_execute_many_with_sequences(oracle_async_session: OracleAsyncDriver) -> None:
    """Test execute_many with Oracle sequences for auto-incrementing IDs."""

    await oracle_async_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP SEQUENCE batch_seq_oracledb_async';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -2289 THEN RAISE; END IF;
        END;
        """)
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_sequence_batch_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE SEQUENCE batch_seq_oracledb_async START WITH 1 INCREMENT BY 1;
        CREATE TABLE test_sequence_batch_oracledb_async (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            department VARCHAR2(50),
            hire_date DATE DEFAULT SYSDATE
        )
    """)

    insert_sql = "INSERT INTO test_sequence_batch_oracledb_async (id, name, department) VALUES (batch_seq_oracledb_async.NEXTVAL, :1, :2)"

    employee_data = [
        ("Alice Johnson", "ENGINEERING"),
        ("Bob Smith", "SALES"),
        ("Carol Williams", "MARKETING"),
        ("David Brown", "ENGINEERING"),
        ("Eve Davis", "HR"),
    ]

    result = await oracle_async_session.execute_many(insert_sql, employee_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(employee_data)

    select_result = await oracle_async_session.execute(
        "SELECT id, name, department FROM test_sequence_batch_oracledb_async ORDER BY id"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == len(employee_data)

    for i, row in enumerate(select_result.get_data()):
        assert row["id"] == i + 1
        assert row["name"] == employee_data[i][0]
        assert row["department"] == employee_data[i][1]

    sequence_result = await oracle_async_session.execute(
        "SELECT batch_seq_oracledb_async.CURRVAL as current_value FROM dual"
    )
    assert isinstance(sequence_result, SQLResult)
    assert sequence_result.data is not None
    assert sequence_result.get_data()[0]["current_value"] == len(employee_data)

    dept_result = await oracle_async_session.execute("""
        SELECT department, COUNT(*) as employee_count
        FROM test_sequence_batch_oracledb_async
        GROUP BY department
        ORDER BY department
    """)
    assert isinstance(dept_result, SQLResult)
    assert dept_result.data is not None

    engineering_count = next(
        row["employee_count"] for row in dept_result.get_data() if row["department"] == "ENGINEERING"
    )
    assert engineering_count == 2

    await oracle_async_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_sequence_batch_oracledb_async';
            EXECUTE IMMEDIATE 'DROP SEQUENCE batch_seq_oracledb_async';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 AND SQLCODE != -2289 THEN RAISE; END IF;
        END;
    """)


def test_sync_execute_many_error_handling(oracle_sync_session: OracleSyncDriver) -> None:
    """Test execute_many error handling with constraint violations."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_error_handling'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_error_handling (
            id NUMBER PRIMARY KEY,
            email VARCHAR2(100) UNIQUE NOT NULL,
            name VARCHAR2(100)
        )
    """)

    valid_data = [(1, "user1@example.com", "User 1"), (2, "user2@example.com", "User 2")]

    insert_sql = "INSERT INTO test_error_handling (id, email, name) VALUES (:1, :2, :3)"
    result = oracle_sync_session.execute_many(insert_sql, valid_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(valid_data)

    duplicate_data = [
        (3, "user3@example.com", "User 3"),
        (4, "user1@example.com", "Duplicate User"),
        (5, "user5@example.com", "User 5"),
    ]

    with pytest.raises(Exception):
        oracle_sync_session.execute_many(insert_sql, duplicate_data)

    count_result = oracle_sync_session.execute("SELECT COUNT(*) as total_count FROM test_error_handling")
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.get_data()[0]["total_count"] == len(valid_data) + 1

    new_valid_data = [(6, "user6@example.com", "User 6"), (7, "user7@example.com", "User 7")]

    result = oracle_sync_session.execute_many(insert_sql, new_valid_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(new_valid_data)

    final_count_result = oracle_sync_session.execute("SELECT COUNT(*) as total_count FROM test_error_handling")
    assert isinstance(final_count_result, SQLResult)
    assert final_count_result.data is not None
    expected_total = len(valid_data) + 1 + len(new_valid_data)
    assert final_count_result.get_data()[0]["total_count"] == expected_total

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_error_handling'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_execute_many_batch_errors_surface_metadata(oracle_sync_session: OracleSyncDriver) -> None:
    """oracle_batch_errors collects per-row failures instead of raising."""
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE batch_err_sync'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    oracle_sync_session.execute_script("CREATE TABLE batch_err_sync (id NUMBER PRIMARY KEY, name VARCHAR2(50))")
    oracle_sync_session.execute("INSERT INTO batch_err_sync (id, name) VALUES (:1, :2)", (2, "existing"))
    oracle_sync_session.commit()

    config = oracle_sync_session.statement_config.replace(execution_args={"oracle_batch_errors": True})
    rows = [(1, "a"), (2, "dup"), (3, "c")]
    result = oracle_sync_session.execute_many(
        "INSERT INTO batch_err_sync (id, name) VALUES (:1, :2)", rows, statement_config=config
    )

    assert isinstance(result, SQLResult)
    assert result.metadata["oracle_batch_errors"]
    assert result.metadata["oracle_batch_errors"][0]["offset"] == 1

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE batch_err_sync'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_execute_many_array_dml_row_counts(oracle_sync_session: OracleSyncDriver) -> None:
    """oracle_array_dml_row_counts surfaces per-statement counts and sums rows_affected."""
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE dml_counts_sync'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    oracle_sync_session.execute_script("CREATE TABLE dml_counts_sync (id NUMBER PRIMARY KEY, grp NUMBER)")
    oracle_sync_session.execute_many(
        "INSERT INTO dml_counts_sync (id, grp) VALUES (:1, :2)", [(1, 1), (2, 1), (3, 2)]
    )
    oracle_sync_session.commit()

    config = oracle_sync_session.statement_config.replace(execution_args={"oracle_array_dml_row_counts": True})
    result = oracle_sync_session.execute_many(
        "UPDATE dml_counts_sync SET grp = grp + 10 WHERE grp = :1", [(1,), (2,)], statement_config=config
    )

    assert isinstance(result, SQLResult)
    counts = result.metadata["oracle_dml_row_counts"]
    assert counts == [2, 1]
    assert result.rows_affected == sum(counts)

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE dml_counts_sync'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_execute_many_batch_errors_surface_metadata(oracle_async_session: OracleAsyncDriver) -> None:
    """Async oracle_batch_errors collects per-row failures instead of raising."""
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE batch_err_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("CREATE TABLE batch_err_async (id NUMBER PRIMARY KEY, name VARCHAR2(50))")
    await oracle_async_session.execute("INSERT INTO batch_err_async (id, name) VALUES (:1, :2)", (2, "existing"))
    await oracle_async_session.commit()

    config = oracle_async_session.statement_config.replace(execution_args={"oracle_batch_errors": True})
    rows = [(1, "a"), (2, "dup"), (3, "c")]
    result = await oracle_async_session.execute_many(
        "INSERT INTO batch_err_async (id, name) VALUES (:1, :2)", rows, statement_config=config
    )

    assert isinstance(result, SQLResult)
    assert result.metadata["oracle_batch_errors"]
    assert result.metadata["oracle_batch_errors"][0]["offset"] == 1

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE batch_err_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
