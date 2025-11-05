"""Test Oracle execute_many functionality."""

from typing import Any

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core import SQLResult

BatchParameters = list[tuple[Any, ...]] | list[dict[str, Any]] | list[list[Any]]
pytestmark = pytest.mark.xdist_group("oracle")


def test_sync_execute_many_insert_batch(oracle_sync_session: OracleSyncDriver) -> None:
    """Test execute_many with batch INSERT operations using positional parameters."""

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

    count_result = oracle_sync_session.execute("SELECT COUNT(*) as total_count FROM test_batch_insert")
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["total_count"] == len(batch_data)

    select_result = oracle_sync_session.execute("SELECT id, name, category, value FROM test_batch_insert ORDER BY id")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == len(batch_data)

    first_record = select_result.data[0]
    assert first_record["id"] == 1
    assert first_record["name"] == "Item 1"
    assert first_record["category"] == "TYPE_A"
    assert first_record["value"] == 100

    last_record = select_result.data[-1]
    assert last_record["id"] == 5
    assert last_record["name"] == "Item 5"
    assert last_record["category"] == "TYPE_B"
    assert last_record["value"] == 250

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_batch_insert'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_execute_many_update_batch(oracle_async_session: OracleAsyncDriver) -> None:
    """Test execute_many with batch UPDATE operations."""

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

    initial_data = [
        (1, "User 1", "PENDING", 0),
        (2, "User 2", "PENDING", 0),
        (3, "User 3", "PENDING", 0),
        (4, "User 4", "PENDING", 0),
    ]

    insert_sql = "INSERT INTO test_batch_update (id, name, status, score) VALUES (:1, :2, :3, :4)"
    await oracle_async_session.execute_many(insert_sql, initial_data)

    update_sql = "UPDATE test_batch_update SET status = :1, score = :2 WHERE id = :3"

    update_data = [("ACTIVE", 85, 1), ("ACTIVE", 92, 2), ("INACTIVE", 78, 3), ("ACTIVE", 95, 4)]

    result = await oracle_async_session.execute_many(update_sql, update_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(update_data)

    select_result = await oracle_async_session.execute(
        "SELECT id, name, status, score FROM test_batch_update ORDER BY id"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == len(initial_data)

    for i, row in enumerate(select_result.data):
        expected_status, expected_score, expected_id = update_data[i]
        assert row["id"] == expected_id
        assert row["status"] == expected_status
        assert row["score"] == expected_score

    active_count_result = await oracle_async_session.execute(
        "SELECT COUNT(*) as active_count FROM test_batch_update WHERE status = 'ACTIVE'"
    )
    assert isinstance(active_count_result, SQLResult)
    assert active_count_result.data is not None
    assert active_count_result.data[0]["active_count"] == 3

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_batch_update'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_execute_many_with_named_parameters(oracle_sync_session: OracleSyncDriver) -> None:
    """Test execute_many with named parameters using dictionary format."""

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

    select_result = oracle_sync_session.execute(
        "SELECT id, product_name, category_id, price, in_stock FROM test_named_batch ORDER BY id"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == len(batch_data)

    for i, row in enumerate(select_result.data):
        expected = batch_data[i]
        assert row["id"] == expected["id"]
        assert row["product_name"] == expected["product_name"]
        assert row["category_id"] == expected["category_id"]
        assert row["price"] == expected["price"]
        assert row["in_stock"] == expected["in_stock"]

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

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_named_batch'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_execute_many_with_sequences(oracle_async_session: OracleAsyncDriver) -> None:
    """Test execute_many with Oracle sequences for auto-incrementing IDs."""

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

    await oracle_async_session.execute_script("""
        CREATE SEQUENCE batch_seq START WITH 1 INCREMENT BY 1;
        CREATE TABLE test_sequence_batch (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            department VARCHAR2(50),
            hire_date DATE DEFAULT SYSDATE
        )
    """)

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

    select_result = await oracle_async_session.execute(
        "SELECT id, name, department FROM test_sequence_batch ORDER BY id"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == len(employee_data)

    for i, row in enumerate(select_result.data):
        assert row["id"] == i + 1
        assert row["name"] == employee_data[i][0]
        assert row["department"] == employee_data[i][1]

    sequence_result = await oracle_async_session.execute("SELECT batch_seq.CURRVAL as current_value FROM dual")
    assert isinstance(sequence_result, SQLResult)
    assert sequence_result.data is not None
    assert sequence_result.data[0]["current_value"] == len(employee_data)

    dept_result = await oracle_async_session.execute("""
        SELECT department, COUNT(*) as employee_count
        FROM test_sequence_batch
        GROUP BY department
        ORDER BY department
    """)
    assert isinstance(dept_result, SQLResult)
    assert dept_result.data is not None

    engineering_count = next(row["employee_count"] for row in dept_result.data if row["department"] == "ENGINEERING")
    assert engineering_count == 2

    await oracle_async_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_sequence_batch';
            EXECUTE IMMEDIATE 'DROP SEQUENCE batch_seq';
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
    assert count_result.data[0]["total_count"] == len(valid_data) + 1

    new_valid_data = [(6, "user6@example.com", "User 6"), (7, "user7@example.com", "User 7")]

    result = oracle_sync_session.execute_many(insert_sql, new_valid_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(new_valid_data)

    final_count_result = oracle_sync_session.execute("SELECT COUNT(*) as total_count FROM test_error_handling")
    assert isinstance(final_count_result, SQLResult)
    assert final_count_result.data is not None
    expected_total = len(valid_data) + 1 + len(new_valid_data)
    assert final_count_result.data[0]["total_count"] == expected_total

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_error_handling'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
