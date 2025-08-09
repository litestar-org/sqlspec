"""Test Oracle execute_many functionality with CORE_ROUND_3 architecture."""

from typing import Any, Union

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core.result import SQLResult

# Note: Only apply asyncio mark to actual async tests, not all tests in the file

BatchParameters = Union[list[tuple[Any, ...]], list[dict[str, Any]], list[list[Any]]]


@pytest.mark.xdist_group("oracle")
def test_sync_execute_many_insert_batch(oracle_sync_session: OracleSyncDriver) -> None:
    """Test execute_many with batch INSERT operations using positional parameters."""
    # Setup test table
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_batch_insert'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_batch_insert (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            category VARCHAR2(50),
            value NUMBER
        )
    """)

    # Test batch insert with positional parameters
    insert_sql = "INSERT INTO test_batch_insert (id, name, category, value) VALUES (:1, :2, :3, :4)"

    batch_data = [
        (1, "Item 1", "TYPE_A", 100),
        (2, "Item 2", "TYPE_B", 200),
        (3, "Item 3", "TYPE_A", 150),
        (4, "Item 4", "TYPE_C", 300),
        (5, "Item 5", "TYPE_B", 250),
    ]

    result = oracle_sync_session.execute_many(insert_sql, batch_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(batch_data)

    # Verify all records were inserted
    count_result = oracle_sync_session.execute("SELECT COUNT(*) as total_count FROM test_batch_insert")
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["TOTAL_COUNT"] == len(batch_data)

    # Verify data integrity
    select_result = oracle_sync_session.execute("SELECT id, name, category, value FROM test_batch_insert ORDER BY id")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == len(batch_data)

    # Check first and last records
    first_record = select_result.data[0]
    assert first_record["ID"] == 1
    assert first_record["NAME"] == "Item 1"
    assert first_record["CATEGORY"] == "TYPE_A"
    assert first_record["VALUE"] == 100

    last_record = select_result.data[-1]
    assert last_record["ID"] == 5
    assert last_record["NAME"] == "Item 5"
    assert last_record["CATEGORY"] == "TYPE_B"
    assert last_record["VALUE"] == 250

    # Cleanup
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_batch_insert'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.xdist_group("oracle")
async def test_async_execute_many_update_batch(oracle_async_session: OracleAsyncDriver) -> None:
    """Test execute_many with batch UPDATE operations."""
    # Setup test table
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_batch_update'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_batch_update (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            status VARCHAR2(20),
            score NUMBER DEFAULT 0
        )
    """)

    # Insert initial data
    initial_data = [
        (1, "User 1", "PENDING", 0),
        (2, "User 2", "PENDING", 0),
        (3, "User 3", "PENDING", 0),
        (4, "User 4", "PENDING", 0),
    ]

    insert_sql = "INSERT INTO test_batch_update (id, name, status, score) VALUES (:1, :2, :3, :4)"
    await oracle_async_session.execute_many(insert_sql, initial_data)

    # Test batch update with positional parameters
    update_sql = "UPDATE test_batch_update SET status = :1, score = :2 WHERE id = :3"

    update_data = [("ACTIVE", 85, 1), ("ACTIVE", 92, 2), ("INACTIVE", 78, 3), ("ACTIVE", 95, 4)]

    result = await oracle_async_session.execute_many(update_sql, update_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(update_data)

    # Verify updates were applied correctly
    select_result = await oracle_async_session.execute(
        "SELECT id, name, status, score FROM test_batch_update ORDER BY id"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == len(initial_data)

    # Check updated values
    for i, row in enumerate(select_result.data):
        expected_status, expected_score, expected_id = update_data[i]
        assert row["ID"] == expected_id
        assert row["STATUS"] == expected_status
        assert row["SCORE"] == expected_score

    # Test aggregate query on updated data
    active_count_result = await oracle_async_session.execute(
        "SELECT COUNT(*) as active_count FROM test_batch_update WHERE status = 'ACTIVE'"
    )
    assert isinstance(active_count_result, SQLResult)
    assert active_count_result.data is not None
    assert active_count_result.data[0]["ACTIVE_COUNT"] == 3

    # Cleanup
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_batch_update'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.xdist_group("oracle")
def test_sync_execute_many_with_named_parameters(oracle_sync_session: OracleSyncDriver) -> None:
    """Test execute_many with named parameters using dictionary format."""
    # Setup test table
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_named_batch'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_named_batch (
            id NUMBER PRIMARY KEY,
            product_name VARCHAR2(100),
            category_id NUMBER,
            price NUMBER(10,2),
            in_stock NUMBER(1) CHECK (in_stock IN (0, 1))
        )
    """)

    # Test batch insert with named parameters
    insert_sql = """
        INSERT INTO test_named_batch (id, product_name, category_id, price, in_stock)
        VALUES (:id, :product_name, :category_id, :price, :in_stock)
    """

    batch_data = [
        {"id": 1, "product_name": "Oracle Database", "category_id": 1, "price": 999.99, "in_stock": 1},
        {"id": 2, "product_name": "Oracle Cloud", "category_id": 2, "price": 1299.99, "in_stock": 1},
        {"id": 3, "product_name": "Oracle Analytics", "category_id": 1, "price": 799.99, "in_stock": 0},
        {"id": 4, "product_name": "Oracle Security", "category_id": 3, "price": 1499.99, "in_stock": 1},
    ]

    result = oracle_sync_session.execute_many(insert_sql, batch_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(batch_data)

    # Verify records were inserted correctly
    select_result = oracle_sync_session.execute(
        "SELECT id, product_name, category_id, price, in_stock FROM test_named_batch ORDER BY id"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == len(batch_data)

    # Check data accuracy
    for i, row in enumerate(select_result.data):
        expected = batch_data[i]
        assert row["ID"] == expected["id"]
        assert row["PRODUCT_NAME"] == expected["product_name"]
        assert row["CATEGORY_ID"] == expected["category_id"]
        assert row["PRICE"] == expected["price"]
        assert row["IN_STOCK"] == expected["in_stock"]

    # Test aggregation on batch data
    category_result = oracle_sync_session.execute("""
        SELECT category_id, COUNT(*) as product_count, AVG(price) as avg_price
        FROM test_named_batch
        WHERE in_stock = 1
        GROUP BY category_id
        ORDER BY category_id
    """)
    assert isinstance(category_result, SQLResult)
    assert category_result.data is not None
    assert len(category_result.data) >= 1

    # Cleanup
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_named_batch'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.xdist_group("oracle")
async def test_async_execute_many_with_sequences(oracle_async_session: OracleAsyncDriver) -> None:
    """Test execute_many with Oracle sequences for auto-incrementing IDs."""
    # Clean up any existing sequence and table
    await oracle_async_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP SEQUENCE batch_seq';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -2289 THEN RAISE; END IF;
        END;
        """)
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_sequence_batch'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    # Create sequence and table
    await oracle_async_session.execute_script("""
        CREATE SEQUENCE batch_seq START WITH 1 INCREMENT BY 1;
        CREATE TABLE test_sequence_batch (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            department VARCHAR2(50),
            hire_date DATE DEFAULT SYSDATE
        )
    """)

    # Test batch insert using sequence for ID generation
    insert_sql = "INSERT INTO test_sequence_batch (id, name, department) VALUES (batch_seq.NEXTVAL, :1, :2)"

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

    # Verify records were inserted with sequential IDs
    select_result = await oracle_async_session.execute(
        "SELECT id, name, department FROM test_sequence_batch ORDER BY id"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == len(employee_data)

    # Check that IDs are sequential starting from 1
    for i, row in enumerate(select_result.data):
        assert row["ID"] == i + 1
        assert row["NAME"] == employee_data[i][0]
        assert row["DEPARTMENT"] == employee_data[i][1]

    # Check sequence current value
    sequence_result = await oracle_async_session.execute("SELECT batch_seq.CURRVAL as current_value FROM dual")
    assert isinstance(sequence_result, SQLResult)
    assert sequence_result.data is not None
    assert sequence_result.data[0]["CURRENT_VALUE"] == len(employee_data)

    # Test department aggregation
    dept_result = await oracle_async_session.execute("""
        SELECT department, COUNT(*) as employee_count
        FROM test_sequence_batch
        GROUP BY department
        ORDER BY department
    """)
    assert isinstance(dept_result, SQLResult)
    assert dept_result.data is not None

    # Find ENGINEERING department count
    engineering_count = next(row["EMPLOYEE_COUNT"] for row in dept_result.data if row["DEPARTMENT"] == "ENGINEERING")
    assert engineering_count == 2

    # Cleanup
    await oracle_async_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_sequence_batch';
            EXECUTE IMMEDIATE 'DROP SEQUENCE batch_seq';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 AND SQLCODE != -2289 THEN RAISE; END IF;
        END;
    """)


@pytest.mark.xdist_group("oracle")
def test_sync_execute_many_error_handling(oracle_sync_session: OracleSyncDriver) -> None:
    """Test execute_many error handling with constraint violations."""
    # Setup test table with unique constraint
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

    # Insert valid initial data
    valid_data = [(1, "user1@example.com", "User 1"), (2, "user2@example.com", "User 2")]

    insert_sql = "INSERT INTO test_error_handling (id, email, name) VALUES (:1, :2, :3)"
    result = oracle_sync_session.execute_many(insert_sql, valid_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(valid_data)

    # Attempt batch insert with duplicate email (should fail)
    duplicate_data = [
        (3, "user3@example.com", "User 3"),
        (4, "user1@example.com", "Duplicate User"),  # This will cause constraint violation
        (5, "user5@example.com", "User 5"),
    ]

    # This should raise an exception due to unique constraint violation
    with pytest.raises(Exception):  # Oracle will raise an ORA-00001 error
        oracle_sync_session.execute_many(insert_sql, duplicate_data)

    # Verify that the failed batch partially inserted valid records before hitting constraint violation
    # Oracle's executemany processes sequentially, so the first valid record (id=3) should be inserted
    count_result = oracle_sync_session.execute("SELECT COUNT(*) as total_count FROM test_error_handling")
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert (
        count_result.data[0]["TOTAL_COUNT"] == len(valid_data) + 1
    )  # Original data + first valid record from failed batch

    # Test successful batch after error
    new_valid_data = [(6, "user6@example.com", "User 6"), (7, "user7@example.com", "User 7")]

    result = oracle_sync_session.execute_many(insert_sql, new_valid_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(new_valid_data)

    # Final count should be original + partial failed batch + new valid records
    final_count_result = oracle_sync_session.execute("SELECT COUNT(*) as total_count FROM test_error_handling")
    assert isinstance(final_count_result, SQLResult)
    assert final_count_result.data is not None
    expected_total = len(valid_data) + 1 + len(new_valid_data)  # +1 for first record from failed batch
    assert final_count_result.data[0]["TOTAL_COUNT"] == expected_total

    # Cleanup
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_error_handling'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
