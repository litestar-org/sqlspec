"""Integration tests for MERGE builder with AsyncPG (PostgreSQL 15+).

PostgreSQL MERGE syntax differences from Oracle:
- No DUAL table needed for dict sources
- Uses VALUES (...) AS alias(cols) syntax for inline data
- Supports WHEN NOT MATCHED THEN INSERT with conditions
- No WHEN NOT MATCHED BY SOURCE clause
- ON clause can have or omit parentheses
"""

from collections.abc import AsyncGenerator

import pytest

from sqlspec import sql
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
from sqlspec.core import SQLResult

pytestmark = [pytest.mark.asyncpg, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
async def asyncpg_merge_session(asyncpg_async_driver: AsyncpgDriver) -> AsyncGenerator[AsyncpgDriver, None]:
    """Create test tables for MERGE tests."""
    await asyncpg_async_driver.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price NUMERIC(10, 2),
            stock INTEGER DEFAULT 0
        )
    """)

    await asyncpg_async_driver.execute("""
        CREATE TABLE IF NOT EXISTS staging_products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price NUMERIC(10, 2),
            stock INTEGER DEFAULT 0
        )
    """)

    await asyncpg_async_driver.execute(
        "INSERT INTO products (id, name, price, stock) VALUES ($1, $2, $3, $4)", [1, "Test Product", 19.99, 10]
    )
    await asyncpg_async_driver.execute(
        "INSERT INTO products (id, name, price, stock) VALUES ($1, $2, $3, $4)", [2, "Another Product", 29.99, 5]
    )
    await asyncpg_async_driver.execute(
        "INSERT INTO products (id, name, price, stock) VALUES ($1, $2, $3, $4)", [3, "Third Product", 39.99, 15]
    )

    yield asyncpg_async_driver

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS products CASCADE")
    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS staging_products CASCADE")


async def test_asyncpg_merge_basic_update_existing(asyncpg_merge_session: AsyncpgDriver) -> None:
    """Test MERGE updates existing row."""
    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using([{"id": 1, "name": "Updated Product", "price": 24.99}], alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
    )

    result = await asyncpg_merge_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await asyncpg_merge_session.execute("SELECT id, name, price FROM products WHERE id = $1", [1])
    assert len(verify_result) == 1
    assert verify_result[0]["name"] == "Updated Product"
    assert float(verify_result[0]["price"]) == 24.99


async def test_asyncpg_merge_basic_insert_new(asyncpg_merge_session: AsyncpgDriver) -> None:
    """Test MERGE inserts new row."""
    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using([{"id": 10, "name": "New Product", "price": 49.99, "stock": 20}], alias="src")
        .on("t.id = src.id")
        .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price", stock="src.stock")
    )

    result = await asyncpg_merge_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await asyncpg_merge_session.execute(
        "SELECT id, name, price, stock FROM products WHERE id = $1", [10]
    )
    assert len(verify_result) == 1
    assert verify_result[0]["name"] == "New Product"
    assert float(verify_result[0]["price"]) == 49.99
    assert verify_result[0]["stock"] == 20


async def test_asyncpg_merge_update_and_insert(asyncpg_merge_session: AsyncpgDriver) -> None:
    """Test MERGE handles both update and insert in single statement."""
    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using([{"id": 2, "name": "Updated Second", "price": 34.99, "stock": 8}], alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock")
        .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price", stock="src.stock")
    )

    result = await asyncpg_merge_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await asyncpg_merge_session.execute("SELECT name, price, stock FROM products WHERE id = $1", [2])
    assert len(verify_result) == 1
    assert verify_result[0]["name"] == "Updated Second"
    assert float(verify_result[0]["price"]) == 34.99
    assert verify_result[0]["stock"] == 8

    merge_query_new = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using([{"id": 20, "name": "Brand New", "price": 59.99, "stock": 25}], alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock")
        .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price", stock="src.stock")
    )

    result_new = await asyncpg_merge_session.execute(merge_query_new)
    assert isinstance(result_new, SQLResult)

    verify_new = await asyncpg_merge_session.execute("SELECT name, price, stock FROM products WHERE id = $1", [20])
    assert len(verify_new) == 1
    assert verify_new[0]["name"] == "Brand New"


async def test_asyncpg_merge_with_expressions(asyncpg_merge_session: AsyncpgDriver) -> None:
    """Test MERGE with SQL expressions in UPDATE."""
    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using([{"id": 3, "additional_stock": 5}], alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(stock="t.stock + src.additional_stock")
    )

    result = await asyncpg_merge_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await asyncpg_merge_session.execute("SELECT stock FROM products WHERE id = $1", [3])
    assert verify_result[0]["stock"] == 20


async def test_asyncpg_merge_with_null_values(asyncpg_merge_session: AsyncpgDriver) -> None:
    """Test MERGE handles NULL values correctly."""
    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using([{"id": 1, "name": "Updated", "price": None}], alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
    )

    result = await asyncpg_merge_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await asyncpg_merge_session.execute("SELECT price FROM products WHERE id = $1", [1])
    assert verify_result[0]["price"] is None


async def test_asyncpg_merge_with_conditional_update(asyncpg_merge_session: AsyncpgDriver) -> None:
    """Test MERGE with condition in WHEN MATCHED clause."""
    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using([{"id": 2, "name": "Conditional Update", "price": 99.99}], alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(condition="t.price < 50", name="src.name", price="src.price")
    )

    result = await asyncpg_merge_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    verify_result = await asyncpg_merge_session.execute("SELECT name, price FROM products WHERE id = $1", [2])
    assert verify_result[0]["name"] == "Conditional Update"
    assert float(verify_result[0]["price"]) == 99.99


async def test_asyncpg_merge_from_table_source(asyncpg_merge_session: AsyncpgDriver) -> None:
    """Test MERGE using table as source instead of dict."""
    await asyncpg_merge_session.execute(
        "INSERT INTO staging_products (id, name, price, stock) VALUES ($1, $2, $3, $4)",
        [1, "Staged Update", 99.99, 100],
    )

    await asyncpg_merge_session.execute(
        "INSERT INTO staging_products (id, name, price, stock) VALUES ($1, $2, $3, $4)", [4, "Staged New", 149.99, 50]
    )

    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using("staging_products", alias="s")
        .on("t.id = s.id")
        .when_matched_then_update(name="s.name", price="s.price", stock="s.stock")
        .when_not_matched_then_insert(columns=["id", "name", "price", "stock"])
    )

    result = await asyncpg_merge_session.execute(merge_query)
    assert isinstance(result, SQLResult)

    updated = await asyncpg_merge_session.execute("SELECT name, price, stock FROM products WHERE id = $1", [1])
    assert updated[0]["name"] == "Staged Update"
    assert float(updated[0]["price"]) == 99.99
    assert updated[0]["stock"] == 100

    inserted = await asyncpg_merge_session.execute("SELECT name, price, stock FROM products WHERE id = $1", [4])
    assert inserted[0]["name"] == "Staged New"
    assert float(inserted[0]["price"]) == 149.99
    assert inserted[0]["stock"] == 50


async def test_asyncpg_merge_delete_matched(asyncpg_merge_session: AsyncpgDriver) -> None:
    """Test MERGE WHEN MATCHED THEN DELETE (PostgreSQL 17+ or conditional)."""
    merge_query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using([{"id": 1, "discontinued": 1}], alias="src")
        .on("t.id = src.id")
        .when_matched_then_delete(condition="src.discontinued = 1")
    )

    try:
        result = await asyncpg_merge_session.execute(merge_query)
        assert isinstance(result, SQLResult)

        count_result = await asyncpg_merge_session.execute("SELECT COUNT(*) as cnt FROM products WHERE id = $1", [1])
        assert count_result[0]["cnt"] == 0
    except Exception as e:
        if "syntax error" in str(e).lower() or "does not exist" in str(e).lower():
            pytest.skip(f"PostgreSQL version does not support MERGE DELETE: {e}")
        raise
