"""Integration tests for MERGE statement on Oracle."""

from collections.abc import AsyncGenerator, Generator

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import sql
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleAsyncDriver, OracleSyncConfig, OracleSyncDriver
from sqlspec.core import SQLResult

pytestmark = pytest.mark.xdist_group("oracle")


@pytest.fixture
async def oracle_merge_async_session(oracle_23ai_service: OracleService) -> AsyncGenerator[OracleAsyncDriver, None]:
    """Create Oracle async session with test table for MERGE tests."""
    config = OracleAsyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
            "min": 1,
            "max": 5,
        }
    )

    try:
        async with config.provide_session() as session:
            await session.execute_script("""
                CREATE TABLE products (
                    id NUMBER PRIMARY KEY,
                    name VARCHAR2(100),
                    price NUMBER(10, 2),
                    stock NUMBER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await session.execute_script("""
                CREATE TABLE staging_products (
                    id NUMBER PRIMARY KEY,
                    name VARCHAR2(100),
                    price NUMBER(10, 2),
                    stock NUMBER
                )
            """)

            await session.execute(
                "INSERT INTO products (id, name, price, stock) VALUES (:id, :name, :price, :stock)",
                {"id": 1, "name": "Existing Product", "price": 19.99, "stock": 10},
            )

            yield session

            await session.execute_script("""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE products';
                    EXECUTE IMMEDIATE 'DROP TABLE staging_products';
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL;
                END;
            """)
    finally:
        await config.close_pool()


@pytest.fixture
def oracle_merge_sync_session(oracle_23ai_service: OracleService) -> Generator[OracleSyncDriver, None, None]:
    """Create Oracle sync session with test table for MERGE tests."""
    config = OracleSyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        }
    )

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE products (
                    id NUMBER PRIMARY KEY,
                    name VARCHAR2(100),
                    price NUMBER(10, 2),
                    stock NUMBER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            session.execute_script("""
                CREATE TABLE staging_products (
                    id NUMBER PRIMARY KEY,
                    name VARCHAR2(100),
                    price NUMBER(10, 2),
                    stock NUMBER
                )
            """)

            session.execute(
                "INSERT INTO products (id, name, price, stock) VALUES (:id, :name, :price, :stock)",
                {"id": 1, "name": "Existing Product", "price": 19.99, "stock": 10},
            )

            yield session

            session.execute_script("""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE products';
                    EXECUTE IMMEDIATE 'DROP TABLE staging_products';
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL;
                END;
            """)
    finally:
        config.close_pool()


async def test_oracle_merge_basic_update_existing(oracle_merge_async_session: OracleAsyncDriver) -> None:
    """Test MERGE updates existing row."""
    merge_query = (
        sql
        .merge()
        .into("products", alias="t")
        .using({"id": 1, "name": "Updated Product", "price": 24.99}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
    )

    result = await oracle_merge_async_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await oracle_merge_async_session.execute(
        "SELECT id, name, price FROM products WHERE id = :id", {"id": 1}
    )
    assert len(verify_result) == 1
    assert verify_result[0]["name"] == "Updated Product"
    assert float(verify_result[0]["price"]) == 24.99


async def test_oracle_merge_basic_insert_new(oracle_merge_async_session: OracleAsyncDriver) -> None:
    """Test MERGE inserts new row."""
    merge_query = (
        sql
        .merge()
        .into("products", alias="t")
        .using({"id": 2, "name": "New Product", "price": 39.99, "stock": 5}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
        .when_not_matched_then_insert(id=2, name="New Product", price=39.99, stock=5)
    )

    result = await oracle_merge_async_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await oracle_merge_async_session.execute(
        "SELECT id, name, price, stock FROM products WHERE id = :id", {"id": 2}
    )
    assert len(verify_result) == 1
    assert verify_result[0]["name"] == "New Product"
    assert float(verify_result[0]["price"]) == 39.99
    assert verify_result[0]["stock"] == 5


async def test_oracle_merge_update_and_insert(oracle_merge_async_session: OracleAsyncDriver) -> None:
    """Test MERGE handles both update and insert in one operation."""
    merge_query1 = (
        sql
        .merge()
        .into("products", alias="t")
        .using({"id": 1, "name": "Updated Existing", "price": 29.99}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
        .when_not_matched_then_insert(id=1, name="Updated Existing", price=29.99)
    )

    await oracle_merge_async_session.execute(merge_query1)

    merge_query2 = (
        sql
        .merge()
        .into("products", alias="t")
        .using({"id": 3, "name": "Brand New", "price": 49.99, "stock": 15}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
        .when_not_matched_then_insert(id=3, name="Brand New", price=49.99, stock=15)
    )

    await oracle_merge_async_session.execute(merge_query2)

    count_result = await oracle_merge_async_session.execute("SELECT COUNT(*) as cnt FROM products")
    assert count_result[0]["cnt"] == 2

    existing_result = await oracle_merge_async_session.execute("SELECT name FROM products WHERE id = :id", {"id": 1})
    assert existing_result[0]["name"] == "Updated Existing"

    new_result = await oracle_merge_async_session.execute("SELECT name FROM products WHERE id = :id", {"id": 3})
    assert new_result[0]["name"] == "Brand New"


async def test_oracle_merge_with_null_values(oracle_merge_async_session: OracleAsyncDriver) -> None:
    """Test MERGE handles NULL values correctly."""
    merge_query = (
        sql
        .merge()
        .into("products", alias="t")
        .using({"id": 1, "name": "Updated", "price": 19.99, "stock": None}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", stock="src.stock")
    )

    result = await oracle_merge_async_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await oracle_merge_async_session.execute("SELECT stock FROM products WHERE id = :id", {"id": 1})
    assert verify_result[0]["stock"] is None


async def test_oracle_merge_with_column_expressions(oracle_merge_async_session: OracleAsyncDriver) -> None:
    """Test MERGE with column expressions (not just values)."""
    merge_query = (
        sql
        .merge()
        .into("products", alias="t")
        .using({"id": 1, "additional": 5}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(stock="t.stock + src.additional", updated_at="CURRENT_TIMESTAMP")
    )

    result = await oracle_merge_async_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await oracle_merge_async_session.execute("SELECT stock FROM products WHERE id = :id", {"id": 1})
    assert verify_result[0]["stock"] == 15


async def test_oracle_merge_when_matched_with_condition(oracle_merge_async_session: OracleAsyncDriver) -> None:
    """Test MERGE WHEN MATCHED with WHERE condition."""
    merge_query = (
        sql
        .merge()
        .into("products", alias="t")
        .using({"id": 1, "new_price": 5.00}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update({"price": "src.new_price"}, condition="src.new_price < t.price")
    )

    result = await oracle_merge_async_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await oracle_merge_async_session.execute("SELECT price FROM products WHERE id = :id", {"id": 1})
    assert float(verify_result[0]["price"]) == 5.00


async def test_oracle_merge_from_table_source(oracle_merge_async_session: OracleAsyncDriver) -> None:
    """Test MERGE using table as source instead of dict."""
    await oracle_merge_async_session.execute(
        "INSERT INTO staging_products (id, name, price, stock) VALUES (:id, :name, :price, :stock)",
        {"id": 1, "name": "Staged Update", "price": 99.99, "stock": 100},
    )

    await oracle_merge_async_session.execute(
        "INSERT INTO staging_products (id, name, price, stock) VALUES (:id, :name, :price, :stock)",
        {"id": 4, "name": "Staged New", "price": 149.99, "stock": 50},
    )

    merge_query = (
        sql
        .merge()
        .into("products", alias="t")
        .using("staging_products", alias="s")
        .on("t.id = s.id")
        .when_matched_then_update(name="s.name", price="s.price", stock="s.stock")
        .when_not_matched_then_insert(columns=["id", "name", "price", "stock"])
    )

    result = await oracle_merge_async_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    count_result = await oracle_merge_async_session.execute("SELECT COUNT(*) as cnt FROM products")
    assert count_result[0]["cnt"] == 2


@pytest.mark.skip(
    reason="Oracle does not support standalone WHEN MATCHED THEN DELETE with conditions - requires UPDATE SET clause first"
)
async def test_oracle_merge_when_matched_delete(oracle_merge_async_session: OracleAsyncDriver) -> None:
    """Test MERGE WHEN MATCHED THEN DELETE.

    Note: Oracle requires DELETE to be combined with UPDATE:
        WHEN MATCHED THEN UPDATE SET col = col DELETE WHERE condition

    Standalone DELETE with AND condition is not supported:
        WHEN MATCHED AND condition THEN DELETE  ← Not valid in Oracle

    This test is skipped as sqlglot doesn't support Oracle's UPDATE+DELETE syntax yet.
    """
    merge_query = (
        sql
        .merge()
        .into("products", alias="t")
        .using({"id": 1, "discontinued": 1}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_delete(condition="src.discontinued = 1")
    )

    result = await oracle_merge_async_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    count_result = await oracle_merge_async_session.execute(
        "SELECT COUNT(*) as cnt FROM products WHERE id = :id", {"id": 1}
    )
    assert count_result[0]["cnt"] == 0


def test_oracle_merge_sync_basic(oracle_merge_sync_session: OracleSyncDriver) -> None:
    """Test MERGE with sync driver."""
    merge_query = (
        sql
        .merge()
        .into("products", alias="t")
        .using({"id": 1, "name": "Sync Updated", "price": 15.99}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
    )

    result = oracle_merge_sync_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = oracle_merge_sync_session.execute("SELECT name, price FROM products WHERE id = :id", {"id": 1})
    assert len(verify_result) == 1
    assert verify_result[0]["name"] == "Sync Updated"
    assert float(verify_result[0]["price"]) == 15.99


def test_oracle_merge_sync_insert(oracle_merge_sync_session: OracleSyncDriver) -> None:
    """Test MERGE INSERT with sync driver."""
    merge_query = (
        sql
        .merge()
        .into("products", alias="t")
        .using({"id": 5, "name": "Sync New", "price": 59.99, "stock": 25}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id=5, name="Sync New", price=59.99, stock=25)
    )

    result = oracle_merge_sync_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = oracle_merge_sync_session.execute("SELECT name, stock FROM products WHERE id = :id", {"id": 5})
    assert len(verify_result) == 1
    assert verify_result[0]["name"] == "Sync New"
    assert verify_result[0]["stock"] == 25
