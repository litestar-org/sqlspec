"""Unit tests for without_indexes context manager and drop_indexes bulk load orchestration."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.driver._storage_helpers import build_create_index_sql, build_drop_index_sql, filter_droppable_table_indexes


def test_filter_droppable_table_indexes_preserves_primary_and_constraints() -> None:
    """Primary and constraint-backing indexes are never dropped."""
    indexes: list[dict[str, Any]] = [
        {"index_name": "pk_users", "table_name": "users", "columns": ["id"], "unique": True, "is_primary": True},
        {
            "index_name": "uq_users_email",
            "table_name": "users",
            "columns": ["email"],
            "unique": True,
            "is_constraint_backed": True,
        },
        {
            "index_name": "idx_users_created",
            "table_name": "users",
            "columns": ["created_at"],
            "unique": False,
            "is_primary": False,
        },
    ]
    droppable = filter_droppable_table_indexes(indexes)
    assert [idx["index_name"] for idx in droppable] == ["idx_users_created"]


@pytest.mark.parametrize(
    ("dialect", "concurrent", "expected_drop", "expected_create"),
    [
        ("postgres", True, "DROP INDEX IF EXISTS", "CREATE INDEX CONCURRENTLY"),
        ("postgres", False, "DROP INDEX IF EXISTS", "CREATE INDEX"),
        ("sqlite", False, "DROP INDEX IF EXISTS", "CREATE INDEX"),
        ("mysql", False, " ON ", "CREATE INDEX"),
        ("mssql", False, " ON ", "CREATE INDEX"),
        ("oracle", False, "DROP INDEX", "CREATE INDEX"),
    ],
)
def test_build_drop_and_create_index_sql_across_dialects(
    dialect: str, concurrent: bool, expected_drop: str, expected_create: str
) -> None:
    """Index drop and recreate SQL builders generate dialect-appropriate DDL."""
    meta: dict[str, Any] = {
        "index_name": "idx_orders_customer",
        "table_name": "orders",
        "columns": ["customer_id", "created_at"],
        "unique": False,
        "is_primary": False,
    }
    drop_sql = build_drop_index_sql(dialect, meta, "orders")
    create_sql = build_create_index_sql(dialect, meta, "orders", concurrent=concurrent)
    assert drop_sql is not None and expected_drop in drop_sql
    assert create_sql is not None and expected_create in create_sql


def test_sync_without_indexes_drops_secondary_indexes_and_restores_on_error(tmp_path: Path) -> None:
    """Sync without_indexes drops only secondary non-unique indexes and rebuilds them even on failure."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "indexes_sync.db")})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, sku TEXT UNIQUE, customer_id INTEGER)")
        driver.execute("CREATE INDEX idx_orders_customer ON orders (customer_id)")

        initial_indexes = {idx["index_name"] for idx in driver.data_dictionary.get_indexes(driver, "orders")}
        assert "idx_orders_customer" in initial_indexes

        with pytest.raises(RuntimeError, match="load boom"):
            with driver.without_indexes("orders", concurrent=False):
                mid_indexes = {idx["index_name"] for idx in driver.data_dictionary.get_indexes(driver, "orders")}
                assert "idx_orders_customer" not in mid_indexes
                raise RuntimeError("load boom")

        restored_indexes = {idx["index_name"] for idx in driver.data_dictionary.get_indexes(driver, "orders")}
        assert "idx_orders_customer" in restored_indexes

        table = pa.Table.from_pydict({"id": [1, 2], "sku": ["A1", "B2"], "customer_id": [10, 20]})
        driver.load_from_arrow("orders", table, drop_indexes=True, concurrent_indexes=False)
        final_indexes = {idx["index_name"] for idx in driver.data_dictionary.get_indexes(driver, "orders")}
        assert "idx_orders_customer" in final_indexes


@pytest.mark.anyio
async def test_async_without_indexes_and_load_from_arrow_drop_indexes(tmp_path: Path) -> None:
    """Async without_indexes and load_from_arrow(drop_indexes=True) drop and recreate secondary indexes."""
    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "indexes_async.db")})
    try:
        async with config.provide_session() as driver:
            await driver.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)")
            await driver.execute("CREATE INDEX idx_async_orders_cust ON orders (customer_id)")

            async with driver.without_indexes("orders", concurrent=False):
                mid_indexes = {idx["index_name"] for idx in await driver.data_dictionary.get_indexes(driver, "orders")}
                assert "idx_async_orders_cust" not in mid_indexes

            restored_indexes = {idx["index_name"] for idx in await driver.data_dictionary.get_indexes(driver, "orders")}
            assert "idx_async_orders_cust" in restored_indexes

            table = pa.Table.from_pydict({"id": [1, 2], "customer_id": [100, 200]})
            await driver.load_from_arrow("orders", table, drop_indexes=True, concurrent_indexes=False)
            final_indexes = {idx["index_name"] for idx in await driver.data_dictionary.get_indexes(driver, "orders")}
            assert "idx_async_orders_cust" in final_indexes
    finally:
        await config.close_pool()
