"""SQLite and DuckDB data-dictionary replacement metadata tests."""

import sqlite3
from typing import cast
from unittest.mock import AsyncMock, Mock

import pytest

from sqlspec.adapters.aiosqlite.data_dictionary import AiosqliteDataDictionary
from sqlspec.adapters.duckdb import DuckDBDriver
from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.adapters.sqlite.data_dictionary import SqliteDataDictionary
from sqlspec.data_dictionary import DataDictionaryLoader, DependencyMetadata, MetadataSupport
from tests.conftest import requires_interpreted

pytestmark = requires_interpreted


def test_direct_sqlite_duckdb_domain_query_packs_cover_c7_domains() -> None:
    """C7 query packs should use direct dialect/domain paths without v2 namespaces."""
    loader = DataDictionaryLoader()
    expected_queries = (
        ("sqlite", "schemas", "database_list"),
        ("sqlite", "objects", "by_schema"),
        ("sqlite", "tables", "by_schema"),
        ("sqlite", "columns", "by_table"),
        ("sqlite", "constraints", "foreign_keys_by_table"),
        ("sqlite", "indexes", "by_schema"),
        ("sqlite", "views", "by_schema"),
        ("sqlite", "triggers", "by_schema"),
        ("sqlite", "native_sql", "by_object"),
        ("sqlite", "functions", "list"),
        ("sqlite", "modules", "list"),
        ("sqlite", "compile_options", "list"),
        ("sqlite", "system", "integrity_check"),
        ("duckdb", "schemas", "list"),
        ("duckdb", "databases", "list"),
        ("duckdb", "objects", "by_schema"),
        ("duckdb", "columns", "by_schema"),
        ("duckdb", "constraints", "by_schema"),
        ("duckdb", "indexes", "by_schema"),
        ("duckdb", "views", "by_schema"),
        ("duckdb", "sequences", "by_schema"),
        ("duckdb", "functions", "by_schema"),
        ("duckdb", "dependencies", "by_schema"),
        ("duckdb", "extensions", "list"),
        ("duckdb", "system", "settings"),
    )

    for dialect, domain, name in expected_queries:
        query = loader.get_domain_query(dialect, domain, name)
        assert query.is_supported, f"{dialect}/{domain}/{name} should be present"
        assert query.query_text is not None

    assert loader.get_domain_query("sqlite", "comments", "by_schema").capability.support == MetadataSupport.UNSUPPORTED
    assert (
        loader.get_domain_query("sqlite", "privileges", "by_schema").capability.support == MetadataSupport.UNSUPPORTED
    )
    assert loader.get_domain_query("sqlite", "routines", "by_schema").capability.support == MetadataSupport.UNSUPPORTED
    assert loader.get_domain_query("duckdb", "triggers", "by_schema").capability.support == MetadataSupport.UNSUPPORTED
    assert (
        loader.get_domain_query("duckdb", "privileges", "by_schema").capability.support == MetadataSupport.UNSUPPORTED
    )


def test_sqlite_table_xinfo_includes_generated_columns() -> None:
    """SQLite column introspection should use table_xinfo, not table_info."""
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    driver = SqliteDriver(connection)
    try:
        driver.execute_script("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                price INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                total INTEGER GENERATED ALWAYS AS (price * quantity) VIRTUAL
            );
        """)

        columns = driver.data_dictionary.get_columns(driver, table="products")

        columns_by_name = {column["column_name"]: column for column in columns}
        assert {"id", "price", "quantity", "total"} <= set(columns_by_name)
        assert columns_by_name["total"]["extra"] == "generated"
    finally:
        connection.close()


def test_sqlite_schema_wide_index_lookup_is_batched() -> None:
    """SQLite schema-wide index lookup should not recurse table-by-table."""
    driver = Mock()
    driver.select.return_value = [
        {
            "index_name": "idx_users_email",
            "table_name": "users",
            "columns": ["email"],
            "is_primary": False,
            "is_unique": 1,
        }
    ]

    indexes = SqliteDataDictionary().get_indexes(driver)

    assert indexes == driver.select.return_value
    assert driver.select.call_count == 1
    query_text = driver.select.call_args.args[0]
    assert "pragma_index_xinfo" in query_text.lower()


def test_sqlite_indexes_include_expression_partial_metadata() -> None:
    """SQLite index introspection should preserve expression and partial-index metadata."""
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    driver = SqliteDriver(connection)
    try:
        driver.execute_script("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                email TEXT
            );
            CREATE INDEX idx_users_email_expr
                ON users((lower(email)) COLLATE NOCASE DESC)
                WHERE email IS NOT NULL;
        """)

        indexes = driver.data_dictionary.get_indexes(driver, table="users")

        index = next(index for index in indexes if index["index_name"] == "idx_users_email_expr")
        index_details = cast("dict[str, object]", index)
        assert index["columns"] == ["<expression>"]
        assert index_details["is_partial"] == 1
        assert "WHERE email IS NOT NULL" in str(index_details["native_sql"])
    finally:
        connection.close()


async def test_aiosqlite_schema_wide_index_lookup_is_batched() -> None:
    """aiosqlite should match SQLite schema-wide index batching."""
    expected_indexes = [
        {
            "index_name": "idx_users_email",
            "table_name": "users",
            "columns": ["email"],
            "is_primary": False,
            "is_unique": 1,
        }
    ]
    driver = Mock()
    driver.select = AsyncMock(return_value=expected_indexes)

    indexes = await AiosqliteDataDictionary().get_indexes(driver)

    assert indexes == expected_indexes
    assert driver.select.await_count == 1
    query_text = driver.select.call_args.args[0]
    assert "pragma_index_xinfo" in query_text.lower()


def test_duckdb_explicit_indexes_not_empty() -> None:
    """DuckDB should expose explicit indexes through duckdb_indexes()."""
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect(":memory:")
    driver = DuckDBDriver(connection)
    try:
        driver.execute_script("""
            CREATE SCHEMA app;
            CREATE TABLE app.events (
                id INTEGER PRIMARY KEY,
                tenant_id INTEGER NOT NULL,
                payload VARCHAR
            );
            CREATE INDEX idx_events_tenant ON app.events(tenant_id);
        """)

        indexes = driver.data_dictionary.get_indexes(driver, table="events", schema="app")

        assert any(index["index_name"] == "idx_events_tenant" for index in indexes)
        index = next(index for index in indexes if index["index_name"] == "idx_events_tenant")
        assert index["table_name"] == "events"
        assert index["columns"] == ["tenant_id"]
        assert index["is_primary"] is False
    finally:
        connection.close()


def test_duckdb_dependencies_from_duckdb_dependencies() -> None:
    """DuckDB dependency metadata should be backed by duckdb_dependencies()."""
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect(":memory:")
    driver = DuckDBDriver(connection)
    try:
        driver.execute_script("""
            CREATE SCHEMA app;
            CREATE TABLE app.parent (id INTEGER PRIMARY KEY);
            CREATE TABLE app.child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES app.parent(id));
            CREATE INDEX idx_child_parent ON app.child(parent_id);
        """)

        result = driver.data_dictionary.get_dependencies(driver, schema="app")

        assert result.capability.support == MetadataSupport.SUPPORTED
        assert result.items
        assert any(isinstance(item, DependencyMetadata) and "deptype" in item.attributes for item in result.items)
    finally:
        connection.close()
