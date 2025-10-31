"""Integration tests for MERGE bulk operations with Psycopg.

Tests bulk upsert functionality with varying dataset sizes to validate:
- JSON-based bulk strategies
- Parameter limit handling
- NULL value handling in bulk
- Performance with different row counts
"""

from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal

import pytest

from sqlspec import sql
from sqlspec.adapters.psycopg.driver import PsycopgSyncDriver
from sqlspec.core.result import SQLResult

pytestmark = [pytest.mark.psycopg, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
def psycopg_bulk_session(psycopg_sync_config) -> Generator[PsycopgSyncDriver, None, None]:
    """Create test tables for bulk MERGE tests."""
    with psycopg_sync_config.provide_session() as session:
        session.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price NUMERIC(10, 2),
                stock INTEGER DEFAULT 0,
                category TEXT
            )
        """)
        session.commit()

        yield session

        try:
            session.execute("DROP TABLE IF EXISTS products CASCADE")
            session.commit()
        except Exception:
            pass


def test_psycopg_merge_bulk_10_rows(psycopg_bulk_session: PsycopgSyncDriver) -> None:
    """Test MERGE with 10 rows using jsonb_to_recordset strategy."""
    bulk_data = [
        {"id": i, "name": f"Product {i}", "price": Decimal(f"{10 + i}.99"), "stock": i * 10, "category": "electronics"}
        for i in range(1, 11)
    ]

    merge_query = (
        sql.merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = psycopg_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = psycopg_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["count"] == 10

    verify_product = psycopg_bulk_session.execute("SELECT * FROM products WHERE id = %s", [5])
    assert verify_product[0]["name"] == "Product 5"
    assert float(verify_product[0]["price"]) == 15.99
    assert verify_product[0]["stock"] == 50
    assert verify_product[0]["category"] == "electronics"


def test_psycopg_merge_bulk_100_rows(psycopg_bulk_session: PsycopgSyncDriver) -> None:
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
        sql.merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = psycopg_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = psycopg_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["count"] == 100

    verify_bulk = psycopg_bulk_session.execute("SELECT COUNT(*) as count FROM products WHERE category = %s", ["bulk"])
    assert verify_bulk[0]["count"] == 50


def test_psycopg_merge_bulk_500_rows(psycopg_bulk_session: PsycopgSyncDriver) -> None:
    """Test MERGE with 500 rows - should trigger JSON strategy."""
    bulk_data = [
        {"id": i, "name": f"Product {i}", "price": Decimal(f"{500 + i}.00"), "stock": i, "category": f"cat_{i % 10}"}
        for i in range(1, 501)
    ]

    merge_query = (
        sql.merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = psycopg_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = psycopg_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["count"] == 500


def test_psycopg_merge_bulk_1000_rows(psycopg_bulk_session: PsycopgSyncDriver) -> None:
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
        sql.merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = psycopg_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = psycopg_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_result[0]["count"] == 1000


def test_psycopg_merge_bulk_with_nulls(psycopg_bulk_session: PsycopgSyncDriver) -> None:
    """Test MERGE bulk operations with NULL values."""
    bulk_data = [
        {"id": 1, "name": "Product 1", "price": Decimal("10.99"), "stock": 5, "category": "electronics"},
        {"id": 2, "name": "Product 2", "price": None, "stock": 10, "category": None},
        {"id": 3, "name": "Product 3", "price": Decimal("30.99"), "stock": None, "category": "books"},
        {"id": 4, "name": "Product 4", "price": None, "stock": None, "category": None},
    ]

    merge_query = (
        sql.merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = psycopg_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = psycopg_bulk_session.execute("SELECT * FROM products WHERE id = %s", [2])
    assert verify_result[0]["price"] is None
    assert verify_result[0]["category"] is None

    verify_result = psycopg_bulk_session.execute("SELECT * FROM products WHERE id = %s", [3])
    assert verify_result[0]["stock"] is None

    verify_result = psycopg_bulk_session.execute("SELECT * FROM products WHERE id = %s", [4])
    assert verify_result[0]["price"] is None
    assert verify_result[0]["stock"] is None
    assert verify_result[0]["category"] is None


def test_psycopg_merge_bulk_update_existing(psycopg_bulk_session: PsycopgSyncDriver) -> None:
    """Test bulk MERGE updates existing rows."""
    psycopg_bulk_session.execute(
        "INSERT INTO products (id, name, price, stock, category) VALUES (%s, %s, %s, %s, %s)",
        [1, "Old Product 1", Decimal("5.00"), 100, "old"],
    )
    psycopg_bulk_session.execute(
        "INSERT INTO products (id, name, price, stock, category) VALUES (%s, %s, %s, %s, %s)",
        [2, "Old Product 2", Decimal("10.00"), 200, "old"],
    )

    bulk_data = [
        {"id": 1, "name": "Updated Product 1", "price": Decimal("15.00"), "stock": 50, "category": "new"},
        {"id": 2, "name": "Updated Product 2", "price": Decimal("25.00"), "stock": 75, "category": "new"},
    ]

    merge_query = (
        sql.merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = psycopg_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = psycopg_bulk_session.execute("SELECT * FROM products WHERE id = %s", [1])
    assert verify_result[0]["name"] == "Updated Product 1"
    assert float(verify_result[0]["price"]) == 15.00
    assert verify_result[0]["stock"] == 50
    assert verify_result[0]["category"] == "new"


def test_psycopg_merge_bulk_mixed_operations(psycopg_bulk_session: PsycopgSyncDriver) -> None:
    """Test bulk MERGE with mixed insert and update operations."""
    psycopg_bulk_session.execute(
        "INSERT INTO products (id, name, price, stock, category) VALUES (%s, %s, %s, %s, %s)",
        [1, "Existing Product", Decimal("20.00"), 50, "existing"],
    )

    bulk_data = [
        {"id": 1, "name": "Updated Existing", "price": Decimal("25.00"), "stock": 60, "category": "updated"},
        {"id": 2, "name": "New Product 2", "price": Decimal("30.00"), "stock": 10, "category": "new"},
        {"id": 3, "name": "New Product 3", "price": Decimal("35.00"), "stock": 20, "category": "new"},
    ]

    merge_query = (
        sql.merge(dialect="postgres")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )

    result = psycopg_bulk_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_count = psycopg_bulk_session.execute("SELECT COUNT(*) as count FROM products")
    assert verify_count[0]["count"] == 3

    verify_updated = psycopg_bulk_session.execute("SELECT * FROM products WHERE id = %s", [1])
    assert verify_updated[0]["name"] == "Updated Existing"
    assert verify_updated[0]["category"] == "updated"

    verify_new = psycopg_bulk_session.execute("SELECT COUNT(*) as count FROM products WHERE category = %s", ["new"])
    assert verify_new[0]["count"] == 2
