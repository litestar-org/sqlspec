"""SQLite and DuckDB data-dictionary metadata tests."""

import ast
import sqlite3
from importlib import import_module
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

import sqlspec.data_dictionary.dialects.duckdb as duckdb_dialect_pkg
import sqlspec.data_dictionary.dialects.sqlite as sqlite_dialect_pkg
from sqlspec.adapters.aiosqlite.data_dictionary import AiosqliteDataDictionary
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.adapters.duckdb.data_dictionary import DuckDBDataDictionary
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.adapters.sqlite.data_dictionary import SqliteDataDictionary
from sqlspec.data_dictionary import DataDictionaryLoader, DependencyMetadata, MetadataSupport, VersionInfo
from sqlspec.data_dictionary.dialects.duckdb import DuckDBSyncDataDictionary
from sqlspec.data_dictionary.dialects.sqlite import SqliteAsyncDataDictionary, SqliteSyncDataDictionary
from sqlspec.exceptions import SQLFileNotFoundError
from tests.conftest import requires_interpreted

pytestmark = requires_interpreted


def test_direct_sqlite_duckdb_domain_query_packs_cover_c7_domains() -> None:
    """C7 query packs should use direct dialect/domain paths without v2 namespaces."""
    loader = DataDictionaryLoader()
    expected_queries = (
        ("sqlite", "schemas", "list"),
        ("sqlite", "objects", "by_schema"),
        ("sqlite", "tables", "by_schema"),
        ("sqlite", "columns", "by_table"),
        ("sqlite", "foreign_keys", "by_table"),
        ("sqlite", "indexes", "by_schema"),
        ("sqlite", "views", "by_schema"),
        ("sqlite", "triggers", "by_schema"),
        ("sqlite", "ddl", "by_object"),
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
        assert columns_by_name["total"]["extra"] == "virtual generated"
        assert columns_by_name["total"]["generation_expression"] == "price * quantity"
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


def test_sqlite_columns_report_primary_key_columns() -> None:
    """SQLite column metadata flags primary-key columns with their key position."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute(
            'CREATE TABLE "Orders" ("tenant" TEXT, "orderId" INTEGER, note TEXT, PRIMARY KEY ("tenant", "orderId"))'
        )

        columns = driver.data_dictionary.get_columns(driver, table="Orders")

    assert [(column["column_name"], column["is_primary"]) for column in columns] == [
        ("tenant", 1),
        ("orderId", 2),
        ("note", 0),
    ]
    config.close_pool()


def test_duckdb_columns_report_primary_key_columns(tmp_path: Path) -> None:
    """DuckDB column metadata flags primary-key columns for exact-case table names."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "metadata.duckdb")})
    with config.provide_session() as driver:
        driver.execute(
            'CREATE TABLE "Orders" ("tenant" VARCHAR, "orderId" INTEGER, note VARCHAR, PRIMARY KEY ("tenant", "orderId"))'
        )

        by_table = driver.data_dictionary.get_columns(driver, table="Orders")
        by_schema = driver.data_dictionary.get_columns(driver)

    assert [(column["column_name"], bool(column["is_primary"])) for column in by_table] == [
        ("tenant", True),
        ("orderId", True),
        ("note", False),
    ]
    assert {
        column["column_name"]: bool(column["is_primary"]) for column in by_schema if column["table_name"] == "Orders"
    } == {"tenant": True, "orderId": True, "note": False}
    config.close_pool()


def test_sqlite_columns_report_generated_columns() -> None:
    """SQLite column metadata marks virtual and stored generated columns in ``extra``."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute(
            "CREATE TABLE gen (id INTEGER PRIMARY KEY, v INTEGER GENERATED ALWAYS AS (id * 2) VIRTUAL, "
            "s INTEGER GENERATED ALWAYS AS (id * 3) STORED)"
        )

        columns = driver.data_dictionary.get_columns(driver, table="gen")

    assert [(column["column_name"], column["extra"], column.get("generation_expression")) for column in columns] == [
        ("id", None, None),
        ("v", "virtual generated", "id * 2"),
        ("s", "stored generated", "id * 3"),
    ]
    config.close_pool()


def test_duckdb_columns_report_generated_columns(tmp_path: Path) -> None:
    """DuckDB column metadata flags generated columns, including quoted and mixed-case names."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "generated.duckdb")})
    with config.provide_session() as driver:
        driver.execute(
            'CREATE TABLE "Gen" (id INTEGER PRIMARY KEY, "userName" VARCHAR, '
            "doubled INTEGER GENERATED ALWAYS AS (id * 2) VIRTUAL, "
            '"Two Words" DECIMAL(10, 2) AS (id + 1), "select" INTEGER AS (id), '
            "note VARCHAR DEFAULT 'doubled INTEGER GENERATED ALWAYS AS(')"
        )

        by_table = driver.data_dictionary.get_columns(driver, table="Gen")
        by_schema = driver.data_dictionary.get_columns(driver)

    expected = {"id": False, "userName": False, "doubled": True, "Two Words": True, "select": True, "note": False}
    assert {column["column_name"]: bool(column["is_generated"]) for column in by_table} == expected
    assert {
        column["column_name"]: bool(column["is_generated"]) for column in by_schema if column["table_name"] == "Gen"
    } == expected
    config.close_pool()


def test_duckdb_columns_ignore_generated_text_in_string_literals(tmp_path: Path) -> None:
    """DuckDB generated-column detection ignores DDL text inside string literals and quoted identifiers."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "literals.duckdb")})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE t1(id INTEGER, g VARCHAR DEFAULT ', h INTEGER GENERATED ALWAYS AS(', h INTEGER)")
        driver.execute(
            'CREATE TABLE t2("it\'s" VARCHAR DEFAULT \'x\', "a""b\nc" INTEGER AS (1), '
            "e VARCHAR DEFAULT 'it''s, z INTEGER GENERATED ALWAYS AS(', z INTEGER, "
            'w VARCHAR DEFAULT \'"\', "(""q"" INTEGER GENERATED ALWAYS AS(" INTEGER, '
            '"v, q INTEGER GENERATED ALWAYS AS(" INTEGER, q INTEGER, "MixedCase" INTEGER GENERATED ALWAYS AS (2), u AS (3))'
        )

        driver.execute("CREATE TYPE mood AS ENUM ('x', 'y')")
        driver.execute(
            "CREATE TABLE t3(id INTEGER, e ENUM('a', 'b') GENERATED ALWAYS AS ('a'), "
            "m mood GENERATED ALWAYS AS ('x'), s STRUCT(\"a b\" INTEGER) GENERATED ALWAYS AS ({'a b': 1}), "
            "\"E x\" ENUM('p', 'q') AS ('p'), d ENUM('a', ', z INTEGER GENERATED ALWAYS AS('), z INTEGER)"
        )

        by_table = {table: driver.data_dictionary.get_columns(driver, table=table) for table in ("t1", "t2", "t3")}
        by_schema = driver.data_dictionary.get_columns(driver)

    expected = {
        "t1": {"id": False, "g": False, "h": False},
        "t2": {
            "it's": False,
            'a"b\nc': True,
            "e": False,
            "z": False,
            "w": False,
            '("q" INTEGER GENERATED ALWAYS AS(': False,
            "v, q INTEGER GENERATED ALWAYS AS(": False,
            "q": False,
            "MixedCase": True,
            "u": True,
        },
        "t3": {"id": False, "e": True, "m": True, "s": True, "E x": True, "d": False, "z": False},
    }
    for table, columns in by_table.items():
        assert {column["column_name"]: bool(column["is_generated"]) for column in columns} == expected[table]
    assert {
        (column["table_name"], column["column_name"]): bool(column["is_generated"])
        for column in by_schema
        if column["table_name"] in expected
    } == {(table, name): flag for table, flags in expected.items() for name, flag in flags.items()}
    config.close_pool()


def test_sqlite_and_duckdb_self_referential_fk_tables_and_constraints() -> None:
    """Self-referential foreign keys do not cause infinite recursion in get_tables."""
    loader = DataDictionaryLoader()
    assert loader.has_domain_queries("sqlite", "constraints") is True
    assert loader.has_domain_queries("sqlite", "comments") is False

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    driver = SqliteDriver(connection)
    try:
        driver.execute_script("""
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES categories(id)
            );
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                category_id INTEGER REFERENCES categories(id)
            );
        """)
        tables = driver.data_dictionary.get_tables(driver)
        assert [t["table_name"] for t in tables] == ["categories", "items"]
    finally:
        connection.close()

    duckdb = pytest.importorskip("duckdb")
    duck_conn = duckdb.connect(":memory:")
    duck_driver = DuckDBDriver(duck_conn)
    try:
        duck_driver.execute_script("""
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES categories(id)
            );
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                category_id INTEGER REFERENCES categories(id)
            );
        """)
        duck_tables = duck_driver.data_dictionary.get_tables(duck_driver)
        assert [t["table_name"] for t in duck_tables] == ["categories", "items"]
    finally:
        duck_conn.close()


def test_sqlite_constraints_include_rowid_alias_primary_key() -> None:
    """SQLite constraints query reports INTEGER PRIMARY KEY columns that have no autoindex."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
            constraints = driver.data_dictionary.get_constraints(driver, table="items")
    finally:
        config.close_pool()

    constraint_items = cast("tuple[dict[str, object], ...]", constraints.items)
    types = {item["constraint_type"]: item["columns"] for item in constraint_items}
    assert types["PRIMARY KEY"] == ["id"]
    assert types["UNIQUE"] == ["code"]


def test_sqlite_get_columns_and_indexes_with_none_schema() -> None:
    """Introspection with schema=None should coalesce to main."""
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    driver = SqliteDriver(connection)
    try:
        driver.execute_script("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            );
            CREATE INDEX idx_users_name ON users(name);
        """)

        cols = driver.data_dictionary.get_columns(driver, table="users", schema=None)
        assert len(cols) == 3
        assert all(c["schema_name"] == "main" for c in cols)

        indexes = driver.data_dictionary.get_indexes(driver, table="users", schema=None)
        assert len(indexes) == 1
        assert indexes[0]["schema_name"] == "main"
        assert indexes[0]["index_name"] == "idx_users_name"
    finally:
        connection.close()


def test_sqlite_data_dictionary_missing_query_raises_sql_file_not_found() -> None:
    """Missing query in SQLite data dictionary raises SQLFileNotFoundError."""
    dd = SqliteDataDictionary()
    with pytest.raises(SQLFileNotFoundError, match="Missing SQLite data-dictionary query"):
        dd._get_domain_query_text("nonexistent_domain", "nonexistent_query")


def test_sqlite_generated_column_memoization(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify generated column expressions are parsed once per table."""
    from sqlspec.data_dictionary.dialects.sqlite import dictionary as dd_module

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    driver = SqliteDriver(connection)
    try:
        driver.execute_script("""
            CREATE TABLE gen_test (
                id INTEGER PRIMARY KEY,
                a INT,
                b INT,
                c INT GENERATED ALWAYS AS (a + b) VIRTUAL,
                d INT GENERATED ALWAYS AS (a * b) STORED
            );
        """)

        call_count = 0
        original_fn = dd_module.sqlite_generated_expressions

        def counting_parser(sql: str | None) -> dict[str, str]:
            nonlocal call_count
            call_count += 1
            return original_fn(sql)

        monkeypatch.setattr(dd_module, "sqlite_generated_expressions", counting_parser)

        cols = driver.data_dictionary.get_columns(driver, table="gen_test")
        assert call_count == 1
        gen_cols = {c["column_name"]: c["generation_expression"] for c in cols if c.get("is_generated")}
        assert gen_cols == {"c": "a + b", "d": "a * b"}
    finally:
        connection.close()


def test_duckdb_metadata_capabilities_profile() -> None:
    """DuckDBDataDictionary reports supported capabilities across metadata domains."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    driver = DuckDBDriver(config.create_connection())
    try:
        profile = driver.data_dictionary.get_metadata_capabilities(driver)
        assert profile.dialect == "duckdb"
        assert profile.get("tables").support == MetadataSupport.SUPPORTED
        assert profile.get("columns").support == MetadataSupport.SUPPORTED
        assert profile.get("schemas").support == MetadataSupport.SUPPORTED
        assert profile.get("objects").support == MetadataSupport.SUPPORTED
        assert profile.get("routines").support == MetadataSupport.SUPPORTED
        assert profile.get("system").support == MetadataSupport.SUPPORTED
        assert profile.get("databases").support == MetadataSupport.SUPPORTED
        assert profile.get("extensions").support == MetadataSupport.SUPPORTED
        assert profile.get("privileges").support == MetadataSupport.UNSUPPORTED
    finally:
        driver.connection.close()


@pytest.mark.parametrize(("table", "expected_names"), [("target", ["target"]), ("missing", [])])
def test_duckdb_table_details_filters_requested_table(table: str, expected_names: list[str]) -> None:
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    driver = DuckDBDriver(config.create_connection())
    try:
        driver.execute_script("""
            CREATE SCHEMA app;
            CREATE TABLE app.target (id INTEGER);
            CREATE TABLE app.other (id INTEGER);
            CREATE TABLE target (id INTEGER);
        """)

        details = driver.data_dictionary.get_table_details(driver, table=table, schema="app")

        assert {row["table_name"] for row in driver.data_dictionary.get_tables(driver, schema="app")} == {
            "target",
            "other",
        }
        assert details.capability.support == MetadataSupport.SUPPORTED
        assert [cast("dict[str, object]", item)["table_name"] for item in details.items] == expected_names
    finally:
        driver.connection.close()


def test_duckdb_unwired_sql_domains() -> None:
    """DuckDBDataDictionary wires schemas, objects, routines, databases, extensions, system, and table_details."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    driver = DuckDBDriver(config.create_connection())
    try:
        driver.execute("CREATE SCHEMA custom_schema")
        driver.execute("CREATE TABLE custom_schema.test_unwired (id INT PRIMARY KEY, name VARCHAR)")

        schemas = driver.data_dictionary.get_schemas(driver)
        assert schemas.domain == "schemas"
        assert len(schemas.items) > 0

        objects = driver.data_dictionary.get_objects(driver, schema="custom_schema")
        assert objects.domain == "objects"
        assert any(cast("dict[str, object]", obj).get("object_name") == "test_unwired" for obj in objects.items)

        routines = driver.data_dictionary.get_routines(driver, schema="custom_schema")
        assert routines.domain == "routines"

        databases = driver.data_dictionary.get_databases(driver)
        assert databases.domain == "databases"
        assert len(databases.items) > 0

        extensions = driver.data_dictionary.get_extensions(driver)
        assert extensions.domain == "extensions"
        assert len(extensions.items) > 0

        table_details = driver.data_dictionary.get_table_details(driver, table="test_unwired", schema="custom_schema")
        assert table_details.domain == "tables"
        assert len(table_details.items) == 1
        assert cast("dict[str, object]", table_details.items[0])["table_name"] == "test_unwired"

        system_settings = driver.data_dictionary.get_system_metadata(driver, "settings", include_system=True)
        assert system_settings.request.domain == "settings"
        assert len(system_settings.rows) > 0
    finally:
        driver.connection.close()


def test_references_identifier() -> None:
    """DuckDBDataDictionary._references_identifier matches whole identifiers inherited from base."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    driver = DuckDBDriver(config.create_connection())
    try:
        dd = driver.data_dictionary
        assert dd._references_identifier("SELECT * FROM my_table", "my_table") is True
        assert dd._references_identifier("SELECT * FROM my_table_extra", "my_table") is False
    finally:
        driver.connection.close()


@pytest.mark.parametrize(
    ("reported_version", "expected_version"), [("9.99.7", VersionInfo(9, 99, 7)), ("8.88.1", VersionInfo(8, 88, 1))]
)
def test_sqlite_sync_base_get_version_queries_driver_and_caches(
    reported_version: str, expected_version: VersionInfo
) -> None:
    """SqliteSyncDataDictionary queries the remote driver version instead of reading local sqlite3."""
    assert reported_version != sqlite3.sqlite_version
    driver = Mock()
    driver.select_value_or_none.return_value = reported_version
    dictionary = SqliteSyncDataDictionary()

    first = dictionary.get_version(driver)
    second = dictionary.get_version(driver)

    assert first == expected_version
    assert second == expected_version
    assert driver.select_value_or_none.call_count == 1


@pytest.mark.parametrize(
    ("reported_version", "expected_version"), [("9.99.7", VersionInfo(9, 99, 7)), ("8.88.1", VersionInfo(8, 88, 1))]
)
async def test_sqlite_async_base_get_version_queries_driver_and_caches(
    reported_version: str, expected_version: VersionInfo
) -> None:
    """SqliteAsyncDataDictionary queries the remote driver version instead of local sqlite3.sqlite_version."""
    assert reported_version != sqlite3.sqlite_version
    driver = Mock()
    driver.select_value_or_none = AsyncMock(return_value=reported_version)
    dictionary = SqliteAsyncDataDictionary()

    first = await dictionary.get_version(driver)
    second = await dictionary.get_version(driver)

    assert first == expected_version
    assert second == expected_version
    assert driver.select_value_or_none.await_count == 1


async def test_sqlite_base_version_cache_isolated_per_driver() -> None:
    """Version caching isolates distinct sync and async driver instances by driver id."""
    sync_driver_a = Mock()
    sync_driver_a.select_value_or_none.return_value = "3.35.0"
    sync_driver_b = Mock()
    sync_driver_b.select_value_or_none.return_value = "3.45.0"
    sync_dd = SqliteSyncDataDictionary()

    assert sync_dd.get_version(sync_driver_a) == VersionInfo(3, 35, 0)
    assert sync_dd.get_version(sync_driver_b) == VersionInfo(3, 45, 0)
    assert sync_driver_a.select_value_or_none.call_count == 1
    assert sync_driver_b.select_value_or_none.call_count == 1

    async_driver_a = Mock()
    async_driver_a.select_value_or_none = AsyncMock(return_value="3.35.0")
    async_driver_b = Mock()
    async_driver_b.select_value_or_none = AsyncMock(return_value="3.45.0")
    async_dd = SqliteAsyncDataDictionary()

    assert await async_dd.get_version(async_driver_a) == VersionInfo(3, 35, 0)
    assert await async_dd.get_version(async_driver_b) == VersionInfo(3, 45, 0)
    assert async_driver_a.select_value_or_none.await_count == 1
    assert async_driver_b.select_value_or_none.await_count == 1


@pytest.mark.parametrize("raw_version", [None, "", "not-a-version"])
def test_sqlite_sync_base_missing_or_invalid_version_cached(raw_version: str | None) -> None:
    """Missing or unparseable sync version returns None and avoids re-querying on second call."""
    driver = Mock()
    driver.select_value_or_none.return_value = raw_version
    dictionary = SqliteSyncDataDictionary()

    assert dictionary.get_version(driver) is None
    assert dictionary.get_version(driver) is None
    assert driver.select_value_or_none.call_count == 1


@pytest.mark.parametrize("raw_version", [None, "", "not-a-version"])
async def test_sqlite_async_base_missing_or_invalid_version_cached(raw_version: str | None) -> None:
    """Missing or unparseable async version returns None and avoids re-querying on second call."""
    driver = Mock()
    driver.select_value_or_none = AsyncMock(return_value=raw_version)
    dictionary = SqliteAsyncDataDictionary()

    assert await dictionary.get_version(driver) is None
    assert await dictionary.get_version(driver) is None
    assert driver.select_value_or_none.await_count == 1


@pytest.mark.parametrize(
    ("raw_version", "type_category", "expected_type"),
    [("3.20.0", "json", "TEXT"), ("3.45.0", "json", "JSON"), ("3.45.0", "boolean", "INTEGER")],
)
def test_sqlite_sync_base_optimal_type(raw_version: str, type_category: str, expected_type: str) -> None:
    """SqliteSyncDataDictionary resolves JSON type by queried version and delegates other categories."""
    driver = Mock()
    driver.select_value_or_none.return_value = raw_version
    dictionary = SqliteSyncDataDictionary()

    assert dictionary.get_optimal_type(driver, type_category) == expected_type


@pytest.mark.parametrize(
    ("raw_version", "type_category", "expected_type"),
    [("3.20.0", "json", "TEXT"), ("3.45.0", "json", "JSON"), ("3.45.0", "boolean", "INTEGER")],
)
async def test_sqlite_async_base_optimal_type(raw_version: str, type_category: str, expected_type: str) -> None:
    """SqliteAsyncDataDictionary resolves JSON type by queried version and delegates other categories."""
    driver = Mock()
    driver.select_value_or_none = AsyncMock(return_value=raw_version)
    dictionary = SqliteAsyncDataDictionary()

    assert await dictionary.get_optimal_type(driver, type_category) == expected_type


@pytest.mark.parametrize(
    ("schema", "expected"),
    [(None, sqlite_dialect_pkg.SQLITE_CONFIG.default_schema), ("temp", "temp"), ('"aux"', "aux"), (" aux ", "aux")],
)
def test_sqlite_base_resolve_schema(schema: str | None, expected: str | None) -> None:
    """Both SQLite dictionary base classes normalize schema names and default None to dialect config."""
    assert SqliteSyncDataDictionary().resolve_schema(schema) == expected
    assert SqliteAsyncDataDictionary().resolve_schema(schema) == expected


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_sqlite_base_feature_flags_and_available_features(mode: str) -> None:
    """Both SQLite base classes expose available features and resolve version-gated flags."""
    if mode == "sync":
        driver = Mock()
        driver.select_value_or_none.return_value = "3.45.0"
        dictionary = SqliteSyncDataDictionary()
        assert "supports_returning" in dictionary.list_available_features()
        assert dictionary.get_feature_flag(driver, "supports_returning") is True
    else:
        driver = Mock()
        driver.select_value_or_none = AsyncMock(return_value="3.20.0")
        async_dictionary = SqliteAsyncDataDictionary()
        assert "supports_returning" in async_dictionary.list_available_features()
        assert await async_dictionary.get_feature_flag(driver, "supports_returning") is False


@pytest.mark.parametrize(
    ("raw_rows", "expected_columns"),
    [
        (
            [
                {
                    "table_name": "users",
                    "index_name": "idx_users_expr",
                    "column_name": None,
                    "column_id": -2,
                    "is_key_column": 1,
                    "is_unique": 0,
                    "is_primary": 0,
                    "is_partial": 1,
                    "index_sql": "CREATE INDEX idx_users_expr ON users((lower(email))) WHERE email IS NOT NULL",
                },
                {
                    "table_name": "users",
                    "index_name": "idx_users_expr",
                    "column_name": "id",
                    "column_id": 0,
                    "is_key_column": 0,
                    "is_unique": 0,
                    "is_primary": 0,
                    "is_partial": 1,
                },
                {
                    "table_name": "users",
                    "index_name": "idx_users_expr",
                    "column_name": "tenant_id",
                    "column_id": 1,
                    "is_key_column": 1,
                    "is_unique": 0,
                    "is_primary": 0,
                    "is_partial": 1,
                },
            ],
            ["<expression>", "tenant_id"],
        ),
        (
            [
                {
                    "table_name": "users",
                    "index_name": "idx_users_prebuilt",
                    "columns": ["email"],
                    "is_primary": False,
                    "is_unique": 1,
                }
            ],
            ["email"],
        ),
    ],
)
async def test_sqlite_base_expression_index_grouping(
    raw_rows: list[dict[str, object]], expected_columns: list[str]
) -> None:
    """Sync and async SQLite bases group index_xinfo rows, emit <expression>, and skip non-key columns."""
    sync_driver = Mock()
    sync_driver.select.return_value = [dict(row) for row in raw_rows]
    sync_indexes = SqliteSyncDataDictionary().get_indexes(sync_driver, table="users")
    assert len(sync_indexes) == 1
    assert sync_indexes[0]["columns"] == expected_columns

    async_driver = Mock()
    async_driver.select = AsyncMock(return_value=[dict(row) for row in raw_rows])
    async_indexes = await SqliteAsyncDataDictionary().get_indexes(async_driver, table="users")
    assert len(async_indexes) == 1
    assert async_indexes[0]["columns"] == expected_columns


@pytest.mark.parametrize("method_name", ["get_sequences", "get_types"])
async def test_sqlite_base_empty_sequences_and_types_make_zero_driver_calls(method_name: str) -> None:
    """get_sequences and get_types return empty MetadataResult with zero driver calls."""
    sync_driver = Mock()
    sync_result = getattr(SqliteSyncDataDictionary(), method_name)(sync_driver)
    assert sync_result.items == ()
    assert sync_driver.method_calls == []

    async_driver = Mock()
    async_result = await getattr(SqliteAsyncDataDictionary(), method_name)(async_driver)
    assert async_result.items == ()
    assert async_driver.method_calls == []


async def test_sqlite_base_generated_column_expressions_populated() -> None:
    """Sync and async SQLite base get_columns attach parsed generated expressions from table DDL."""
    column_rows: list[dict[str, str | None]] = [
        {"table_name": "orders", "column_name": "price", "extra": None},
        {"table_name": "orders", "column_name": "total", "extra": "virtual generated"},
    ]
    ddl_rows: list[dict[str, str]] = [
        {
            "object_name": "orders",
            "object_type": "table",
            "ddl": "CREATE TABLE orders (price INTEGER, total INTEGER GENERATED ALWAYS AS (price * 2) VIRTUAL)",
        }
    ]

    sync_driver = Mock()
    sync_driver.select.side_effect = [[r.copy() for r in column_rows], ddl_rows]
    sync_cols = SqliteSyncDataDictionary().get_columns(sync_driver, table="orders")
    assert sync_cols[1]["generation_expression"] == "price * 2"

    async_driver = Mock()
    async_driver.select = AsyncMock(side_effect=[[r.copy() for r in column_rows], ddl_rows])
    async_cols = await SqliteAsyncDataDictionary().get_columns(async_driver, table="orders")
    assert async_cols[1]["generation_expression"] == "price * 2"


@pytest.mark.parametrize("dictionary_cls", [SqliteSyncDataDictionary, SqliteAsyncDataDictionary])
def test_sqlite_base_missing_domain_query_raises(
    dictionary_cls: type[SqliteSyncDataDictionary] | type[SqliteAsyncDataDictionary],
) -> None:
    """Both SQLite base classes raise SQLFileNotFoundError when a domain query is missing."""
    dictionary = dictionary_cls()
    with pytest.raises(SQLFileNotFoundError, match="Missing SQLite data-dictionary query: nope/missing"):
        dictionary._get_domain_query_text("nope", "missing")


def test_sqlite_sync_base_end_to_end_live_connection() -> None:
    """SqliteSyncDataDictionary executes core reflection methods against a live SQLite connection."""
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    driver = SqliteDriver(connection)
    dictionary = SqliteSyncDataDictionary()
    try:
        driver.execute_script("""
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE,
                qty INTEGER CHECK (qty >= 0)
            );
            CREATE VIEW active_items AS SELECT id, code FROM items;
        """)

        version = dictionary.get_version(driver)
        assert version is not None
        assert [t["table_name"] for t in dictionary.get_tables(driver)] == ["items"]
        constraints = dictionary.get_constraints(driver, table="items")
        constraint_types = {cast("dict[str, object]", item)["constraint_type"] for item in constraints.items}
        assert {"PRIMARY KEY", "UNIQUE", "CHECK"} <= constraint_types
        views = dictionary.get_views(driver)
        assert len(views.items) == 1
        table_ddl = dictionary._get_native_object_ddl(
            driver, "items", schema=None, object_type="table", include_dependencies=False, redact=False
        )
        assert table_ddl is not None
        assert table_ddl.ddl is not None
        assert "CREATE TABLE items" in table_ddl.ddl
        schema_ddl = dictionary._get_native_schema_ddl(driver, None, include_dependencies=False, redact=False)
        assert schema_ddl is not None
        assert len(schema_ddl) >= 2
    finally:
        connection.close()


async def test_sqlite_async_base_reflection_methods_and_no_shared_version_cache() -> None:
    """SqliteAsyncDataDictionary delegates table/fk/constraint/view/ddl queries without class-level version cache."""
    assert not hasattr(SqliteAsyncDataDictionary, "_shared_version_info")
    dictionary = SqliteAsyncDataDictionary()

    driver = Mock()
    driver.select = AsyncMock(return_value=[{"table_name": "items", "schema_name": "main"}])
    tables = await dictionary.get_tables(driver)
    assert tables == [{"table_name": "items", "schema_name": "main"}]

    fk_driver = Mock()
    fk_driver.select = AsyncMock(
        return_value=[
            {"table_name": "child", "column_name": "parent_id", "referenced_table": "parent", "referenced_column": "id"}
        ]
    )
    fks = await dictionary.get_foreign_keys(fk_driver, table="child")
    assert len(fks) == 1
    assert fks[0].referenced_table == "parent"

    view_driver = Mock()
    view_driver.select = AsyncMock(return_value=[{"view_name": "active_items", "schema_name": "main"}])
    views = await dictionary.get_views(view_driver)
    assert len(views.items) == 1

    constraint_driver = Mock()
    constraint_driver.select = AsyncMock(
        side_effect=[
            [],
            [
                {
                    "object_name": "items",
                    "object_type": "table",
                    "ddl": "CREATE TABLE items (qty INTEGER CHECK (qty >= 0))",
                }
            ],
        ]
    )
    constraints = await dictionary.get_constraints(constraint_driver, table="items")
    assert len(constraints.items) == 1

    ddl_driver = Mock()
    ddl_driver.select = AsyncMock(
        return_value=[{"object_name": "items", "object_type": "table", "ddl": "CREATE TABLE items (id INTEGER)"}]
    )
    obj_ddl = await dictionary._get_native_object_ddl(
        ddl_driver, "items", schema=None, object_type="table", include_dependencies=False, redact=False
    )
    assert obj_ddl is not None
    assert obj_ddl.ddl == "CREATE TABLE items (id INTEGER);"
    schema_ddl = await dictionary._get_native_schema_ddl(ddl_driver, None, include_dependencies=False, redact=False)
    assert schema_ddl is not None
    assert len(schema_ddl) == 1


def test_sqlite_dictionary_module_has_no_sqlite3_or_adapter_imports() -> None:
    """sqlspec/data_dictionary/dialects/sqlite/dictionary.py must be adapter- and sqlite3-free."""
    dictionary_path = Path("sqlspec/data_dictionary/dialects/sqlite/dictionary.py")
    tree = ast.parse(dictionary_path.read_text(encoding="utf-8"))
    imported_modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported_modules.append(node.module)

    assert all(name != "sqlite3" and not name.startswith("sqlite3.") for name in imported_modules)
    assert all(not name.startswith("sqlspec.adapters") for name in imported_modules)
    assert "sqlspec.data_dictionary.dialects.sqlite" not in imported_modules


@pytest.mark.parametrize("export_name", ["SqliteSyncDataDictionary", "SqliteAsyncDataDictionary"])
def test_sqlite_dialect_package_lazy_exports(export_name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """PEP 562 lazy package exports expose both classes with identity match and AttributeError on unknown names."""
    dictionary_module = import_module("sqlspec.data_dictionary.dialects.sqlite.dictionary")
    assert export_name in dir(sqlite_dialect_pkg)
    assert export_name in sqlite_dialect_pkg.__all__

    monkeypatch.delitem(sqlite_dialect_pkg.__dict__, export_name, raising=False)
    resolved = getattr(sqlite_dialect_pkg, export_name)
    assert resolved is getattr(dictionary_module, export_name)
    assert sqlite_dialect_pkg.__dict__[export_name] is resolved

    with pytest.raises(AttributeError, match="has no attribute 'MissingSqliteExport'"):
        getattr(sqlite_dialect_pkg, "MissingSqliteExport")


@pytest.mark.parametrize(
    ("dictionary_cls", "expected_adapter"),
    [(DuckDBSyncDataDictionary, "DuckDBSyncDataDictionary"), (DuckDBDataDictionary, "DuckDBDataDictionary")],
)
def test_duckdb_sync_base_capabilities_and_adapter_name(
    dictionary_cls: type[DuckDBSyncDataDictionary], expected_adapter: str
) -> None:
    """Direct base and thin adapter subclass report duckdb dialect and distinct adapter names."""
    profile = dictionary_cls().get_metadata_capabilities(cast("Any", None))

    assert profile.dialect == "duckdb"
    assert profile.adapter == expected_adapter
    assert profile.get("tables").support == MetadataSupport.SUPPORTED
    assert profile.get("privileges").support == MetadataSupport.UNSUPPORTED


def test_duckdb_sync_base_end_to_end_live_connection() -> None:
    """DuckDBSyncDataDictionary executes core reflection methods against a live DuckDB connection."""
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect(":memory:")
    driver = DuckDBDriver(connection)
    dictionary = DuckDBSyncDataDictionary()
    try:
        driver.execute_script("""
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                code VARCHAR UNIQUE,
                qty INTEGER CHECK (qty >= 0)
            );
            CREATE VIEW active_items AS SELECT id, code FROM items;
        """)

        version = dictionary.get_version(driver)
        assert version is not None
        assert [t["table_name"] for t in dictionary.get_tables(driver)] == ["items"]
        assert [c["column_name"] for c in dictionary.get_columns(driver, table="items")] == ["id", "code", "qty"]
        constraints = dictionary.get_constraints(driver, table="items")
        constraint_types = {cast("dict[str, object]", item)["constraint_type"] for item in constraints.items}
        assert {"PRIMARY KEY", "UNIQUE", "CHECK"} <= constraint_types
        views = dictionary.get_views(driver)
        assert len(views.items) == 1
        table_ddl = dictionary._get_native_object_ddl(
            driver, "items", schema="main", object_type="table", include_dependencies=False, redact=False
        )
        assert table_ddl is not None
        assert table_ddl.ddl is not None
    finally:
        connection.close()


def test_duckdb_dictionary_module_has_no_driver_or_adapter_imports() -> None:
    """sqlspec/data_dictionary/dialects/duckdb/dictionary.py must be adapter- and duckdb-driver-free."""
    dictionary_path = Path("sqlspec/data_dictionary/dialects/duckdb/dictionary.py")
    tree = ast.parse(dictionary_path.read_text(encoding="utf-8"))
    imported_modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported_modules.append(node.module)

    assert all(name != "duckdb" and not name.startswith("duckdb.") for name in imported_modules)
    assert all(not name.startswith("sqlspec.adapters") for name in imported_modules)
    assert "sqlspec.data_dictionary.dialects.duckdb" not in imported_modules


def test_duckdb_dialect_package_lazy_exports(monkeypatch: pytest.MonkeyPatch) -> None:
    """PEP 562 lazy package exports expose DuckDBSyncDataDictionary and keep config exports eager."""
    assert duckdb_dialect_pkg.__all__ == ("DUCKDB_CONFIG", "DUCKDB_VERSION_PATTERN", "DuckDBSyncDataDictionary")
    assert "DUCKDB_CONFIG" in duckdb_dialect_pkg.__dict__
    assert "DUCKDB_VERSION_PATTERN" in duckdb_dialect_pkg.__dict__
    assert "DuckDBSyncDataDictionary" in dir(duckdb_dialect_pkg)

    dictionary_module = import_module("sqlspec.data_dictionary.dialects.duckdb.dictionary")
    monkeypatch.delitem(duckdb_dialect_pkg.__dict__, "DuckDBSyncDataDictionary", raising=False)
    resolved = getattr(duckdb_dialect_pkg, "DuckDBSyncDataDictionary")
    assert resolved is dictionary_module.DuckDBSyncDataDictionary
    assert duckdb_dialect_pkg.__dict__["DuckDBSyncDataDictionary"] is resolved

    with pytest.raises(AttributeError, match="has no attribute 'MissingDuckDBExport'"):
        getattr(duckdb_dialect_pkg, "MissingDuckDBExport")
