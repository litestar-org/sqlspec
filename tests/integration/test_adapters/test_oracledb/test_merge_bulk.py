"""Integration tests for MERGE bulk operations with OracleDB.

Tests bulk upsert functionality using JSON_TABLE strategy:
- Handles 1000 bind variable limit
- JSON serialization for bulk data
- NULL value handling in bulk
- Performance with different row counts
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from decimal import Decimal

import pytest

from sqlspec import sql
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver
from sqlspec.core.result import SQLResult

pytestmark = [pytest.mark.oracle, pytest.mark.integration]


@pytest.fixture
async def oracle_bulk_session(oracle_async_session: OracleAsyncDriver) -> AsyncGenerator[OracleAsyncDriver, None]:
    """Create test tables for bulk MERGE tests."""
    await oracle_async_session.execute("""
        CREATE TABLE products (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100) NOT NULL,
            price NUMBER(10, 2),
            stock NUMBER DEFAULT 0,
            category VARCHAR2(50)
        )
    """)

    yield oracle_async_session

    await oracle_async_session.execute("DROP TABLE products PURGE")


async def test_oracle_merge_bulk_10_rows(oracle_bulk_session: OracleAsyncDriver) -> None:
    """Test MERGE with 10 rows using JSON_TABLE strategy."""
    bulk_data = [
        {"id": i, "name": f"Product {i}", "price": Decimal(f"{10 + i}.99"), "stock": i * 10, "category": "electronics"}
        for i in range(1, 11)
    ]

    merge_query = (
        sql.merge(dialect="oracle")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await oracle_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await oracle_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["COUNT"] == 10

    verify_product = await oracle_bulk_session.execute("SELECT * FROM products WHERE id = :1", [5])
    assert verify_product[0]["NAME"] == "Product 5"
    assert float(verify_product[0]["PRICE"]) == 15.99
    assert verify_product[0]["STOCK"] == 50
    assert verify_product[0]["CATEGORY"] == "electronics"


async def test_oracle_merge_bulk_100_rows(oracle_bulk_session: OracleAsyncDriver) -> None:
    """Test MERGE with 100 rows."""
    bulk_data = [
        {
            "id": i,
            "name": f"Product {i}",
            "price": Decimal(f"{100 + i}.50"),
            "stock": i * 5,
            "category": "bulk" if i % 2 == 0 else "regular",
        }
        for i in range(1, 101)
    ]

    merge_query = (
        sql.merge(dialect="oracle")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await oracle_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await oracle_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["COUNT"] == 100

    verify_bulk = await oracle_bulk_session.execute(
        "SELECT COUNT(*) as count FROM products WHERE category = :1", ["bulk"]
    )
    assert verify_bulk[0]["COUNT"] == 50


async def test_oracle_merge_bulk_500_rows(oracle_bulk_session: OracleAsyncDriver) -> None:
    """Test MERGE with 500 rows - validates JSON_TABLE handles parameter limits."""
    bulk_data = [
        {"id": i, "name": f"Product {i}", "price": Decimal(f"{500 + i}.00"), "stock": i, "category": f"cat_{i % 10}"}
        for i in range(1, 501)
    ]

    merge_query = (
        sql.merge(dialect="oracle")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await oracle_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await oracle_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["COUNT"] == 500


async def test_oracle_merge_bulk_1000_rows(oracle_bulk_session: OracleAsyncDriver) -> None:
    """Test MERGE with 1000 rows."""
    bulk_data = [
        {
            "id": i,
            "name": f"Product {i}",
            "price": Decimal(f"{1000 + i}.00"),
            "stock": i % 100,
            "category": f"cat_{i % 20}",
        }
        for i in range(1, 1001)
    ]

    merge_query = (
        sql.merge(dialect="oracle")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await oracle_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await oracle_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["COUNT"] == 1000


async def test_oracle_merge_bulk_with_nulls(oracle_bulk_session: OracleAsyncDriver) -> None:
    """Test MERGE bulk operations with NULL values."""
    bulk_data = [
        {"id": 1, "name": "Product 1", "price": Decimal("10.99"), "stock": 5, "category": "electronics"},
        {"id": 2, "name": "Product 2", "price": None, "stock": 10, "category": None},
        {"id": 3, "name": "Product 3", "price": Decimal("30.99"), "stock": None, "category": "books"},
        {"id": 4, "name": "Product 4", "price": None, "stock": None, "category": None},
    ]

    merge_query = (
        sql.merge(dialect="oracle")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await oracle_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await oracle_bulk_session.execute("SELECT * FROM products WHERE id = :1", [2])
    assert verify_result[0]["PRICE"] is None
    assert verify_result[0]["CATEGORY"] is None

    verify_result = await oracle_bulk_session.execute("SELECT * FROM products WHERE id = :1", [3])
    assert verify_result[0]["STOCK"] is None

    verify_result = await oracle_bulk_session.execute("SELECT * FROM products WHERE id = :1", [4])
    assert verify_result[0]["PRICE"] is None
    assert verify_result[0]["STOCK"] is None
    assert verify_result[0]["CATEGORY"] is None


async def test_oracle_merge_bulk_update_existing(oracle_bulk_session: OracleAsyncDriver) -> None:
    """Test bulk MERGE updates existing rows."""
    await oracle_bulk_session.execute(
        "INSERT INTO products (id, name, price, stock, category) VALUES (:1, :2, :3, :4, :5)",
        [1, "Old Product 1", Decimal("5.00"), 100, "old"],
    )
    await oracle_bulk_session.execute(
        "INSERT INTO products (id, name, price, stock, category) VALUES (:1, :2, :3, :4, :5)",
        [2, "Old Product 2", Decimal("10.00"), 200, "old"],
    )

    bulk_data = [
        {"id": 1, "name": "Updated Product 1", "price": Decimal("15.00"), "stock": 50, "category": "new"},
        {"id": 2, "name": "Updated Product 2", "price": Decimal("25.00"), "stock": 75, "category": "new"},
    ]

    merge_query = (
        sql.merge(dialect="oracle")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await oracle_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await oracle_bulk_session.execute("SELECT * FROM products WHERE id = :1", [1])
    assert verify_result[0]["NAME"] == "Updated Product 1"
    assert float(verify_result[0]["PRICE"]) == 15.00
    assert verify_result[0]["STOCK"] == 50
    assert verify_result[0]["CATEGORY"] == "new"


async def test_oracle_merge_bulk_mixed_operations(oracle_bulk_session: OracleAsyncDriver) -> None:
    """Test bulk MERGE with mixed insert and update operations."""
    await oracle_bulk_session.execute(
        "INSERT INTO products (id, name, price, stock, category) VALUES (:1, :2, :3, :4, :5)",
        [1, "Existing Product", Decimal("20.00"), 50, "existing"],
    )

    bulk_data = [
        {"id": 1, "name": "Updated Existing", "price": Decimal("25.00"), "stock": 60, "category": "updated"},
        {"id": 2, "name": "New Product 2", "price": Decimal("30.00"), "stock": 10, "category": "new"},
        {"id": 3, "name": "New Product 3", "price": Decimal("35.00"), "stock": 20, "category": "new"},
    ]

    merge_query = (
        sql.merge(dialect="oracle")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await oracle_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_count = await oracle_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_count[0]["COUNT"] == 3

    verify_updated = await oracle_bulk_session.execute("SELECT * FROM products WHERE id = :1", [1])
    assert verify_updated[0]["NAME"] == "Updated Existing"
    assert verify_updated[0]["CATEGORY"] == "updated"

    verify_new = await oracle_bulk_session.execute(
        "SELECT COUNT(*) as count FROM products WHERE category = :1", ["new"]
    )
    assert verify_new[0]["COUNT"] == 2


@pytest.mark.skip(reason="Oracle Cloud Free Tier may timeout on very large datasets")
async def test_oracle_merge_bulk_5000_rows(oracle_bulk_session: OracleAsyncDriver) -> None:
    """Test MERGE with 5000 rows - stress test for JSON_TABLE strategy."""
    bulk_data = [
        {
            "id": i,
            "name": f"Product {i}",
            "price": Decimal(f"{5000 + i}.00"),
            "stock": i % 200,
            "category": f"cat_{i % 50}",
        }
        for i in range(1, 5001)
    ]

    merge_query = (
        sql.merge(dialect="oracle")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await oracle_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await oracle_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["COUNT"] == 5000
