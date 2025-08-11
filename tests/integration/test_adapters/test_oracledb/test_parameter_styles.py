"""Test Oracle parameter style conversion with CORE_ROUND_3 architecture."""

from typing import Any, Union

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core.result import SQLResult

# Oracle uses named parameters with colon prefix (:param)
OracleParamData = Union[tuple[Any, ...], list[Any], dict[str, Any]]


@pytest.mark.parametrize(
    ("sql", "params", "expected_rows"),
    [
        # Test named parameters with dictionary
        ("SELECT :name as result FROM dual", {"name": "oracle_test"}, [{"RESULT": "oracle_test"}]),
        # Test positional parameters with tuple
        ("SELECT :1 as result FROM dual", ("oracle_positional",), [{"RESULT": "oracle_positional"}]),
        # Test multiple named parameters
        (
            "SELECT :first_name || ' ' || :last_name as full_name FROM dual",
            {"first_name": "John", "last_name": "Doe"},
            [{"FULL_NAME": "John Doe"}],
        ),
        # Test numeric parameters
        ("SELECT :num1 + :num2 as sum FROM dual", {"num1": 10, "num2": 20}, [{"SUM": 30}]),
    ],
)
@pytest.mark.xdist_group("oracle")
def test_sync_oracle_parameter_styles(
    oracle_sync_session: OracleSyncDriver, sql: str, params: OracleParamData, expected_rows: list[dict[str, Any]]
) -> None:
    """Test Oracle named parameter style conversion in sync driver."""
    result = oracle_sync_session.execute(sql, params)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == len(expected_rows)

    for i, expected_row in enumerate(expected_rows):
        actual_row = result.data[i]
        for key, expected_value in expected_row.items():
            assert actual_row[key] == expected_value


@pytest.mark.parametrize(
    ("sql", "params", "expected_rows"),
    [
        # Test named parameters with dictionary
        ("SELECT :name as result FROM dual", {"name": "oracle_async_test"}, [{"RESULT": "oracle_async_test"}]),
        # Test positional parameters with tuple
        ("SELECT :1 as result FROM dual", ("oracle_async_positional",), [{"RESULT": "oracle_async_positional"}]),
        # Test multiple named parameters
        (
            "SELECT :city || ', ' || :state as location FROM dual",
            {"city": "San Francisco", "state": "CA"},
            [{"LOCATION": "San Francisco, CA"}],
        ),
        # Test boolean-like parameters (Oracle uses 1/0)
        (
            "SELECT CASE WHEN :is_active = 1 THEN 'Active' ELSE 'Inactive' END as status FROM dual",
            {"is_active": 1},
            [{"STATUS": "Active"}],
        ),
    ],
)
@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.xdist_group("oracle")
async def test_async_oracle_parameter_styles(
    oracle_async_session: OracleAsyncDriver, sql: str, params: OracleParamData, expected_rows: list[dict[str, Any]]
) -> None:
    """Test Oracle named parameter style conversion in async driver."""
    result = await oracle_async_session.execute(sql, params)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == len(expected_rows)

    for i, expected_row in enumerate(expected_rows):
        actual_row = result.data[i]
        for key, expected_value in expected_row.items():
            assert actual_row[key] == expected_value


@pytest.mark.xdist_group("oracle")
def test_sync_oracle_insert_with_named_params(oracle_sync_session: OracleSyncDriver) -> None:
    """Test INSERT operations using Oracle named parameters."""
    # Setup test table
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_params_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            age NUMBER,
            city VARCHAR2(50)
        )
    """)

    # Test named parameter INSERT
    insert_sql = "INSERT INTO test_params_table (id, name, age, city) VALUES (:id, :name, :age, :city)"
    params = {"id": 1, "name": "Alice Johnson", "age": 30, "city": "Oracle City"}

    result = oracle_sync_session.execute(insert_sql, params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Verify the insertion with named parameters
    select_sql = "SELECT name, age, city FROM test_params_table WHERE id = :id"
    select_result = oracle_sync_session.execute(select_sql, {"id": 1})
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["NAME"] == "Alice Johnson"
    assert row["AGE"] == 30
    assert row["CITY"] == "Oracle City"

    # Cleanup
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.xdist_group("oracle")
async def test_async_oracle_update_with_mixed_params(oracle_async_session: OracleAsyncDriver) -> None:
    """Test UPDATE operations using mixed parameter styles."""
    # Setup test table
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_mixed_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_mixed_params_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            status VARCHAR2(20),
            last_updated DATE
        )
    """)

    # Insert initial data
    await oracle_async_session.execute(
        "INSERT INTO test_mixed_params_table (id, name, status, last_updated) VALUES (:1, :2, :3, SYSDATE)",
        (1, "Test User", "PENDING"),
    )

    # Test UPDATE with named parameters
    update_sql = """
        UPDATE test_mixed_params_table
        SET name = :new_name, status = :new_status, last_updated = SYSDATE
        WHERE id = :target_id
    """

    update_params = {"new_name": "Updated User", "new_status": "ACTIVE", "target_id": 1}

    result = await oracle_async_session.execute(update_sql, update_params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Verify the update
    select_result = await oracle_async_session.execute(
        "SELECT name, status FROM test_mixed_params_table WHERE id = :1", (1,)
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["NAME"] == "Updated User"
    assert row["STATUS"] == "ACTIVE"

    # Cleanup
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_mixed_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.xdist_group("oracle")
def test_sync_oracle_in_clause_with_params(oracle_sync_session: OracleSyncDriver) -> None:
    """Test IN clause with Oracle parameter binding."""
    # Setup test table
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_in_clause_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_in_clause_table (
            id NUMBER PRIMARY KEY,
            category VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    test_data = [(1, "TYPE_A", 100), (2, "TYPE_B", 200), (3, "TYPE_C", 300), (4, "TYPE_A", 150), (5, "TYPE_B", 250)]

    for data in test_data:
        oracle_sync_session.execute("INSERT INTO test_in_clause_table (id, category, value) VALUES (:1, :2, :3)", data)

    # Test IN clause with named parameters
    # Note: Oracle requires separate parameters for each IN value
    select_sql = """
        SELECT id, category, value
        FROM test_in_clause_table
        WHERE category IN (:cat1, :cat2)
        ORDER BY id
    """

    result = oracle_sync_session.execute(select_sql, {"cat1": "TYPE_A", "cat2": "TYPE_B"})
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 4  # 2 TYPE_A + 2 TYPE_B records

    # Verify correct records returned
    categories = [row["CATEGORY"] for row in result.data]
    assert all(cat in ["TYPE_A", "TYPE_B"] for cat in categories)
    assert "TYPE_C" not in categories

    # Cleanup
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_in_clause_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.xdist_group("oracle")
async def test_async_oracle_null_parameter_handling(oracle_async_session: OracleAsyncDriver) -> None:
    """Test handling of NULL parameters in Oracle."""
    # Setup test table
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_null_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_null_params_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            optional_field VARCHAR2(100)
        )
    """)

    # Test INSERT with NULL parameter
    insert_sql = "INSERT INTO test_null_params_table (id, name, optional_field) VALUES (:id, :name, :optional_field)"

    # Insert record with NULL optional_field
    result = await oracle_async_session.execute(insert_sql, {"id": 1, "name": "Test User", "optional_field": None})
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Insert record with non-NULL optional_field
    result = await oracle_async_session.execute(
        insert_sql, {"id": 2, "name": "Another User", "optional_field": "Not Null"}
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Test SELECT with NULL comparison
    select_null_sql = "SELECT id, name FROM test_null_params_table WHERE optional_field IS NULL"
    null_result = await oracle_async_session.execute(select_null_sql)
    assert isinstance(null_result, SQLResult)
    assert null_result.data is not None
    assert len(null_result.data) == 1
    assert null_result.data[0]["ID"] == 1

    # Test SELECT with NOT NULL comparison
    select_not_null_sql = "SELECT id, name, optional_field FROM test_null_params_table WHERE optional_field IS NOT NULL"
    not_null_result = await oracle_async_session.execute(select_not_null_sql)
    assert isinstance(not_null_result, SQLResult)
    assert not_null_result.data is not None
    assert len(not_null_result.data) == 1
    assert not_null_result.data[0]["ID"] == 2
    assert not_null_result.data[0]["OPTIONAL_FIELD"] == "Not Null"

    # Cleanup
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_null_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.xdist_group("oracle")
def test_sync_oracle_date_parameter_handling(oracle_sync_session: OracleSyncDriver) -> None:
    """Test Oracle DATE parameter handling and formatting."""
    # Setup test table
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_date_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_date_params_table (
            id NUMBER PRIMARY KEY,
            event_name VARCHAR2(100),
            event_date DATE,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Test DATE parameter using Oracle's TO_DATE function
    insert_sql = """
        INSERT INTO test_date_params_table (id, event_name, event_date)
        VALUES (:id, :event_name, TO_DATE(:date_str, 'YYYY-MM-DD'))
    """

    result = oracle_sync_session.execute(
        insert_sql, {"id": 1, "event_name": "Oracle Conference", "date_str": "2024-06-15"}
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Test date comparison with parameters
    select_sql = """
        SELECT id, event_name,
               TO_CHAR(event_date, 'YYYY-MM-DD') as formatted_date
        FROM test_date_params_table
        WHERE event_date = TO_DATE(:target_date, 'YYYY-MM-DD')
    """

    select_result = oracle_sync_session.execute(select_sql, {"target_date": "2024-06-15"})
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["EVENT_NAME"] == "Oracle Conference"
    assert row["FORMATTED_DATE"] == "2024-06-15"

    # Test date range query with parameters
    range_sql = """
        SELECT COUNT(*) as event_count
        FROM test_date_params_table
        WHERE event_date BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD')
                             AND TO_DATE(:end_date, 'YYYY-MM-DD')
    """

    range_result = oracle_sync_session.execute(range_sql, {"start_date": "2024-01-01", "end_date": "2024-12-31"})
    assert isinstance(range_result, SQLResult)
    assert range_result.data is not None
    assert range_result.data[0]["EVENT_COUNT"] == 1

    # Cleanup
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_date_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
