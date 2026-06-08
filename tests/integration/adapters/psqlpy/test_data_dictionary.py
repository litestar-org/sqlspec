"""Integration tests for psqlpy PostgreSQL data dictionary (regression for #361)."""

import uuid
from typing import TYPE_CHECKING

import pytest

from sqlspec.data_dictionary import VersionInfo

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy import PsqlpyDriver

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.mark.psqlpy
async def test_psqlpy_data_dictionary_version_detection(psqlpy_driver: "PsqlpyDriver") -> None:
    """Test PostgreSQL version detection via psqlpy."""
    version = await psqlpy_driver.data_dictionary.get_version(psqlpy_driver)
    assert version is not None
    assert isinstance(version, VersionInfo)
    assert version.major >= 9


@pytest.mark.psqlpy
async def test_psqlpy_data_dictionary_get_columns(psqlpy_driver: "PsqlpyDriver") -> None:
    """Exercise get_columns(table=...) end-to-end (regression for #361)."""
    table_name = f"dd_cols_{uuid.uuid4().hex[:8]}"
    await psqlpy_driver.execute_script(f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            label TEXT NOT NULL,
            status VARCHAR(20) DEFAULT 'pending',
            note TEXT
        );
    """)
    try:
        cols = await psqlpy_driver.data_dictionary.get_columns(psqlpy_driver, table=table_name, schema="public")
        by_name = {c["column_name"]: c for c in cols}
        assert set(by_name) >= {"id", "label", "status", "note"}

        assert by_name["label"]["is_nullable"] == "NO"
        assert by_name["note"]["is_nullable"] == "YES"

        status_default = by_name["status"].get("column_default")
        assert status_default is not None
        assert "pending" in status_default
    finally:
        await psqlpy_driver.execute_script(f"DROP TABLE IF EXISTS {table_name} CASCADE;")


@pytest.mark.psqlpy
async def test_psqlpy_data_dictionary_get_columns_by_schema(psqlpy_driver: "PsqlpyDriver") -> None:
    """Exercise get_columns(schema=...) without a table filter (regression for #361)."""
    cols = await psqlpy_driver.data_dictionary.get_columns(psqlpy_driver, schema="pg_catalog")
    assert len(cols) > 0


@pytest.mark.psqlpy
async def test_psqlpy_data_dictionary_topology_and_fks(psqlpy_driver: "PsqlpyDriver") -> None:
    """Test topological sort and FK metadata via psqlpy."""
    unique_suffix = uuid.uuid4().hex[:8]
    users_table = f"dd_users_{unique_suffix}"
    orders_table = f"dd_orders_{unique_suffix}"
    items_table = f"dd_items_{unique_suffix}"

    await psqlpy_driver.execute_script(f"""
        CREATE TABLE {users_table} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        CREATE TABLE {orders_table} (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES {users_table}(id),
            amount INTEGER
        );
        CREATE TABLE {items_table} (
            id SERIAL PRIMARY KEY,
            order_id INTEGER REFERENCES {orders_table}(id),
            name VARCHAR(50)
        );
    """)
    try:
        sorted_tables = await psqlpy_driver.data_dictionary.get_tables(psqlpy_driver)
        table_names = [table.get("table_name") for table in sorted_tables if table.get("table_name")]
        test_tables = [name for name in table_names if name in (users_table, orders_table, items_table)]
        assert len(test_tables) == 3

        idx_users = test_tables.index(users_table)
        idx_orders = test_tables.index(orders_table)
        idx_items = test_tables.index(items_table)
        assert idx_users < idx_orders
        assert idx_orders < idx_items

        fks = await psqlpy_driver.data_dictionary.get_foreign_keys(psqlpy_driver, table=orders_table)
        assert len(fks) >= 1
        my_fk = next((fk for fk in fks if fk.referenced_table == users_table), None)
        assert my_fk is not None
        assert my_fk.column_name == "user_id"

        indexes = await psqlpy_driver.data_dictionary.get_indexes(psqlpy_driver, table=users_table)
        assert len(indexes) >= 1
    finally:
        await psqlpy_driver.execute_script(f"""
            DROP TABLE IF EXISTS {items_table} CASCADE;
            DROP TABLE IF EXISTS {orders_table} CASCADE;
            DROP TABLE IF EXISTS {users_table} CASCADE;
        """)
