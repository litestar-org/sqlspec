"""Integration tests for MERGE bulk operations with Psqlpy.

Tests bulk upsert functionality with varying dataset sizes to validate:
- JSON-based bulk strategies
- Parameter limit handling
- NULL value handling in bulk
- Performance with different row counts
"""

from collections.abc import AsyncGenerator

import pytest

from sqlspec import sql
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.core import SQLResult

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.psqlpy, pytest.mark.integration]


@pytest.fixture
async def psqlpy_bulk_session(psqlpy_session: PsqlpyDriver) -> AsyncGenerator[PsqlpyDriver, None]:
    """Create test tables for bulk MERGE tests."""
    await psqlpy_session.execute("DROP TABLE IF EXISTS products CASCADE")
    await psqlpy_session.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price NUMERIC(10, 2),
            stock INTEGER DEFAULT 0,
            category TEXT
        )
    """)

    yield psqlpy_session

    await psqlpy_session.execute("DROP TABLE IF EXISTS products CASCADE")


async def test_psqlpy_merge_bulk_10_rows(psqlpy_bulk_session: PsqlpyDriver) -> None:
    """Test MERGE with 10 rows using jsonb_to_recordset strategy."""
    bulk_data = [
        {"id": i, "name": f"Product {i}", "price": float(f"{10 + i}.99"), "stock": i * 10, "category": "electronics"}
        for i in range(1, 11)
    ]

    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await psqlpy_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await psqlpy_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["count"] == 10

    verify_product = await psqlpy_bulk_session.execute("SELECT * FROM products WHERE id = $1", [5])
    assert verify_product[0]["name"] == "Product 5"
    assert float(verify_product[0]["price"]) == 15.99
    assert verify_product[0]["stock"] == 50
    assert verify_product[0]["category"] == "electronics"


async def test_psqlpy_merge_bulk_100_rows(psqlpy_bulk_session: PsqlpyDriver) -> None:
    """Test MERGE with 100 rows."""
    bulk_data = [
        {
            "id": i,
            "name": f"Product {i}",
            "price": float(f"{100 + i}.50"),
            "stock": i * 5,
            "category": "bulk" if i % 2 == 0 else "regular",
        }
        for i in range(1, 101)
    ]

    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await psqlpy_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await psqlpy_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["count"] == 100

    verify_bulk = await psqlpy_bulk_session.execute(
        "SELECT COUNT(*) as count FROM products WHERE category = $1", ["bulk"]
    )
    assert verify_bulk[0]["count"] == 50


async def test_psqlpy_merge_bulk_500_rows(psqlpy_bulk_session: PsqlpyDriver) -> None:
    """Test MERGE with 500 rows - should trigger JSON strategy."""
    bulk_data = [
        {"id": i, "name": f"Product {i}", "price": float(f"{500 + i}.00"), "stock": i, "category": f"cat_{i % 10}"}
        for i in range(1, 501)
    ]

    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await psqlpy_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await psqlpy_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["count"] == 500


async def test_psqlpy_merge_bulk_1000_rows(psqlpy_bulk_session: PsqlpyDriver) -> None:
    """Test MERGE with 1000 rows."""
    bulk_data = [
        {
            "id": i,
            "name": f"Product {i}",
            "price": float(f"{1000 + i}.00"),
            "stock": i % 100,
            "category": f"cat_{i % 20}",
        }
        for i in range(1, 1001)
    ]

    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await psqlpy_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await psqlpy_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["count"] == 1000


async def test_psqlpy_merge_bulk_with_nulls(psqlpy_bulk_session: PsqlpyDriver) -> None:
    """Test MERGE bulk operations with NULL values."""
    bulk_data = [
        {"id": 1, "name": "Product 1", "price": 10.99, "stock": 5, "category": "electronics"},
        {"id": 2, "name": "Product 2", "price": None, "stock": 10, "category": None},
        {"id": 3, "name": "Product 3", "price": 30.99, "stock": None, "category": "books"},
        {"id": 4, "name": "Product 4", "price": None, "stock": None, "category": None},
    ]

    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await psqlpy_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await psqlpy_bulk_session.execute("SELECT * FROM products WHERE id = $1", [2])
    assert verify_result[0]["price"] is None
    assert verify_result[0]["category"] is None

    verify_result = await psqlpy_bulk_session.execute("SELECT * FROM products WHERE id = $1", [3])
    assert verify_result[0]["stock"] is None

    verify_result = await psqlpy_bulk_session.execute("SELECT * FROM products WHERE id = $1", [4])
    assert verify_result[0]["price"] is None
    assert verify_result[0]["stock"] is None
    assert verify_result[0]["category"] is None


async def test_psqlpy_merge_bulk_update_existing(psqlpy_bulk_session: PsqlpyDriver) -> None:
    """Test bulk MERGE updates existing rows."""
    await psqlpy_bulk_session.execute(
        "INSERT INTO products (id, name, price, stock, category) VALUES ($1, $2, $3, $4, $5)",
        [1, "Old Product 1", 5.00, 100, "old"],
    )
    await psqlpy_bulk_session.execute(
        "INSERT INTO products (id, name, price, stock, category) VALUES ($1, $2, $3, $4, $5)",
        [2, "Old Product 2", 10.00, 200, "old"],
    )

    bulk_data = [
        {"id": 1, "name": "Updated Product 1", "price": 15.00, "stock": 50, "category": "new"},
        {"id": 2, "name": "Updated Product 2", "price": 25.00, "stock": 75, "category": "new"},
    ]

    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await psqlpy_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await psqlpy_bulk_session.execute("SELECT * FROM products WHERE id = $1", [1])
    assert verify_result[0]["name"] == "Updated Product 1"
    assert float(verify_result[0]["price"]) == 15.00
    assert verify_result[0]["stock"] == 50
    assert verify_result[0]["category"] == "new"


async def test_psqlpy_merge_bulk_mixed_operations(psqlpy_bulk_session: PsqlpyDriver) -> None:
    """Test bulk MERGE with mixed insert and update operations."""
    await psqlpy_bulk_session.execute(
        "INSERT INTO products (id, name, price, stock, category) VALUES ($1, $2, $3, $4, $5)",
        [1, "Existing Product", 20.00, 50, "existing"],
    )

    bulk_data = [
        {"id": 1, "name": "Updated Existing", "price": 25.00, "stock": 60, "category": "updated"},
        {"id": 2, "name": "New Product 2", "price": 30.00, "stock": 10, "category": "new"},
        {"id": 3, "name": "New Product 3", "price": 35.00, "stock": 20, "category": "new"},
    ]

    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = await psqlpy_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_count = await psqlpy_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_count[0]["count"] == 3

    verify_updated = await psqlpy_bulk_session.execute("SELECT * FROM products WHERE id = $1", [1])
    assert verify_updated[0]["name"] == "Updated Existing"
    assert verify_updated[0]["category"] == "updated"

    verify_new = await psqlpy_bulk_session.execute(
        "SELECT COUNT(*) as count FROM products WHERE category = $1", ["new"]
    )
    assert verify_new[0]["count"] == 2
