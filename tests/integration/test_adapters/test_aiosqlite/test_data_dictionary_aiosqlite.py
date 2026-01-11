"""Integration tests for AioSQLite data dictionary."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver

pytestmark = pytest.mark.xdist_group("aiosqlite")


@pytest.mark.aiosqlite
async def test_aiosqlite_data_dictionary_topology_and_fks(aiosqlite_session: "AiosqliteDriver") -> None:
    """Test topological sort and FK metadata."""
    aiosqlite_driver = aiosqlite_session
    import uuid

    unique_suffix = uuid.uuid4().hex[:8]
    users_table = f"dd_users_{unique_suffix}"
    orders_table = f"dd_orders_{unique_suffix}"
    items_table = f"dd_items_{unique_suffix}"

    await aiosqlite_driver.execute_script(f"""
        CREATE TABLE {users_table} (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50)
        );
        CREATE TABLE {orders_table} (
            id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES {users_table}(id),
            amount INTEGER
        );
        CREATE TABLE {items_table} (
            id INTEGER PRIMARY KEY,
            order_id INTEGER REFERENCES {orders_table}(id),
            name VARCHAR(50)
        );
    """)

    try:
        # Test 1: Topological Sort
        sorted_tables = await aiosqlite_driver.data_dictionary.get_tables(aiosqlite_driver)
        table_names = [table.get("table_name") for table in sorted_tables if table.get("table_name")]
        test_tables = [name for name in table_names if name in (users_table, orders_table, items_table)]
        assert len(test_tables) == 3

        idx_users = test_tables.index(users_table)
        idx_orders = test_tables.index(orders_table)
        idx_items = test_tables.index(items_table)

        assert idx_users < idx_orders
        assert idx_orders < idx_items

        # Test 2: Foreign Keys
        fks = await aiosqlite_driver.data_dictionary.get_foreign_keys(aiosqlite_driver, table=orders_table)
        assert len(fks) >= 1
        my_fk = next((fk for fk in fks if fk.referenced_table == users_table), None)
        assert my_fk is not None
        assert my_fk.column_name == "user_id"

        # Test 3: Indexes
        await aiosqlite_driver.execute(f"CREATE INDEX idx_{unique_suffix} ON {users_table}(name)")
        indexes = await aiosqlite_driver.data_dictionary.get_indexes(aiosqlite_driver, table=users_table)
        assert len(indexes) >= 1
        assert any(idx["index_name"] == f"idx_{unique_suffix}" for idx in indexes)
        all_indexes = await aiosqlite_driver.data_dictionary.get_indexes(aiosqlite_driver)
        assert any(idx["index_name"] == f"idx_{unique_suffix}" for idx in all_indexes)

    finally:
        await aiosqlite_driver.execute_script(f"""
            DROP TABLE IF EXISTS {items_table};
            DROP TABLE IF EXISTS {orders_table};
            DROP TABLE IF EXISTS {users_table};
        """)
