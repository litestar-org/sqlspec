"""Unit tests for native catalog DDL extraction across SQLite, DuckDB, CockroachDB, and BigQuery."""

from typing import Any, cast

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.data_dictionary import AiosqliteDataDictionary
from sqlspec.adapters.bigquery.data_dictionary import BigQueryDataDictionary
from sqlspec.adapters.cockroach_asyncpg.data_dictionary import CockroachAsyncpgDataDictionary
from sqlspec.adapters.cockroach_psycopg.data_dictionary import (
    CockroachPsycopgAsyncDataDictionary,
    CockroachPsycopgSyncDataDictionary,
)
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.duckdb.data_dictionary import DuckDBDataDictionary
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.data_dictionary import SqliteDataDictionary
from sqlspec.data_dictionary import (
    DataDictionaryLoader,
    DDLResult,
    ForeignKeyMetadata,
    MetadataFidelity,
    MetadataSource,
    MetadataSupport,
)


def test_native_ddl_query_packs_expose_by_object_and_by_schema() -> None:
    """SQLite, DuckDB, CockroachDB, and BigQuery expose both by_object and by_schema DDL queries."""
    loader = DataDictionaryLoader()

    for dialect in ("sqlite", "duckdb", "cockroachdb", "bigquery"):
        by_object = loader.get_domain_query_text(dialect, "ddl", "by_object")
        by_schema = loader.get_domain_query_text(dialect, "ddl", "by_schema")
        assert by_object is not None, f"Missing {dialect} ddl/by_object"
        assert by_schema is not None, f"Missing {dialect} ddl/by_schema"

    duckdb_by_schema = loader.get_domain_query_text("duckdb", "ddl", "by_schema") or ""
    assert "duckdb_tables()" in duckdb_by_schema
    assert "duckdb_views()" in duckdb_by_schema
    assert "duckdb_indexes()" in duckdb_by_schema
    assert "duckdb_sequences()" in duckdb_by_schema
    assert "duckdb_types()" in duckdb_by_schema

    crdb_by_schema = loader.get_domain_query_text("cockroachdb", "ddl", "by_schema") or ""
    assert "crdb_internal.tables" in crdb_by_schema
    assert "create_statement" in crdb_by_schema
    assert "pg_get_indexdef" in crdb_by_schema

    sqlite_by_schema = loader.get_domain_query_text("sqlite", "ddl", "by_schema") or ""
    assert "sqlite_schema" in sqlite_by_schema
    assert "sqlite_%" in sqlite_by_schema

    bigquery_by_schema = loader.get_domain_query_text("bigquery", "ddl", "by_schema") or ""
    assert "INFORMATION_SCHEMA.TABLES" in bigquery_by_schema
    assert "INFORMATION_SCHEMA.ROUTINES" in bigquery_by_schema


def test_sqlite_native_ddl_and_schema_ddl_topological_order() -> None:
    """SqliteDataDictionary extracts native DDL and orders schema DDL topologically."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    dictionary = SqliteDataDictionary()

    try:
        with config.provide_session() as session:
            session.execute("PRAGMA foreign_keys = ON")
            session.execute("CREATE TABLE ddl_migrations (version_num TEXT PRIMARY KEY)")
            session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)")
            session.execute(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id))"
            )
            session.execute("CREATE INDEX idx_orders_user ON orders (user_id)")
            session.execute(
                "CREATE VIEW v_user_orders AS SELECT u.email, o.id AS order_id FROM users u JOIN orders o ON o.user_id = u.id"
            )
            session.execute("CREATE TRIGGER trg_orders_touch AFTER INSERT ON orders BEGIN SELECT 1; END")

            table_ddl = dictionary.get_ddl(session, "users", object_type="table")
            assert isinstance(table_ddl, DDLResult)
            assert table_ddl.status == MetadataSupport.SUPPORTED
            assert table_ddl.fidelity == MetadataFidelity.NATIVE
            assert table_ddl.source == MetadataSource.CATALOG
            assert table_ddl.ddl is not None
            assert "CREATE TABLE users" in table_ddl.ddl

            alias_ddl = dictionary.get_object_ddl(session, "orders", object_type="table")
            assert alias_ddl.ddl is not None
            assert "REFERENCES users(id)" in alias_ddl.ddl

            view_ddl = dictionary.get_ddl(session, "v_user_orders", object_type="view")
            assert view_ddl.ddl is not None
            assert "CREATE VIEW v_user_orders" in view_ddl.ddl

            index_ddl = dictionary.get_ddl(session, "idx_orders_user", object_type="index")
            assert index_ddl.ddl is not None
            assert "CREATE INDEX idx_orders_user" in index_ddl.ddl

            trigger_ddl = dictionary.get_ddl(session, "trg_orders_touch", object_type="trigger")
            assert trigger_ddl.ddl is not None
            assert "CREATE TRIGGER trg_orders_touch" in trigger_ddl.ddl

            missing_ddl = dictionary.get_ddl(session, "nonexistent_table", object_type="table")
            assert missing_ddl.status == MetadataSupport.UNSUPPORTED
            assert missing_ddl.ddl is None

            schema_result = dictionary.get_schema_ddl(session)
            assert schema_result.domain == "ddl"
            assert schema_result.capability.fidelity == MetadataFidelity.NATIVE
            names = [item.identity.name for item in schema_result.items if isinstance(item, DDLResult)]
            assert "ddl_migrations" not in names
            assert names.index("users") < names.index("orders")
            assert names.index("orders") < names.index("idx_orders_user")
            assert names.index("orders") < names.index("v_user_orders")
            assert names.index("orders") < names.index("trg_orders_touch")

            with_migrations = dictionary.get_schema_ddl(session, exclude_tables=())
            all_names = [item.identity.name for item in with_migrations.items if isinstance(item, DDLResult)]
            assert "ddl_migrations" in all_names

            drop_result = dictionary.get_schema_ddl(session, direction="drop")
            drop_names = [item.identity.name for item in drop_result.items if isinstance(item, DDLResult)]
            assert drop_names.index("orders") < drop_names.index("users")
            assert drop_names.index("idx_orders_user") < drop_names.index("orders")
    finally:
        config.close_pool()


@pytest.mark.anyio
async def test_aiosqlite_native_ddl_and_schema_ddl() -> None:
    """AiosqliteDataDictionary extracts native DDL and orders schema DDL asynchronously."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    dictionary = AiosqliteDataDictionary()

    try:
        async with config.provide_session() as session:
            await session.execute("PRAGMA foreign_keys = ON")
            await session.execute("CREATE TABLE ddl_migrations (version_num TEXT PRIMARY KEY)")
            await session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)")
            await session.execute(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id))"
            )
            await session.execute("CREATE INDEX idx_orders_user ON orders (user_id)")

            user_ddl = await dictionary.get_ddl(session, "users", object_type="table")
            assert user_ddl.fidelity == MetadataFidelity.NATIVE
            assert user_ddl.ddl is not None
            assert "CREATE TABLE users" in user_ddl.ddl

            order_ddl = await dictionary.get_object_ddl(session, "orders", object_type="table")
            assert order_ddl.ddl is not None
            assert "CREATE TABLE orders" in order_ddl.ddl

            schema_result = await dictionary.get_schema_ddl(session)
            names = [item.identity.name for item in schema_result.items if isinstance(item, DDLResult)]
            assert "ddl_migrations" not in names
            assert names.index("users") < names.index("orders")
            assert names.index("orders") < names.index("idx_orders_user")
    finally:
        await config.close_pool()


def test_duckdb_native_ddl_and_schema_ddl_topological_order() -> None:
    """DuckDBDataDictionary extracts native DDL for types, sequences, tables, views, and indexes."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    dictionary = DuckDBDataDictionary()

    try:
        with config.provide_session() as session:
            session.execute("CREATE TABLE ddl_migrations (version_num VARCHAR PRIMARY KEY)")
            session.execute("CREATE TYPE order_status AS ENUM ('pending', 'paid')")
            session.execute("CREATE SEQUENCE seq_order_id START 1")
            session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR NOT NULL)")
            session.execute(
                "CREATE TABLE orders ("
                "id INTEGER DEFAULT nextval('seq_order_id') PRIMARY KEY, "
                "user_id INTEGER REFERENCES users(id), "
                "status order_status"
                ")"
            )
            session.execute("CREATE INDEX idx_orders_user ON orders (user_id)")
            session.execute("CREATE VIEW v_orders AS SELECT id, user_id, status FROM orders")

            table_ddl = dictionary.get_ddl(session, "orders", object_type="table")
            assert table_ddl.status == MetadataSupport.SUPPORTED
            assert table_ddl.fidelity == MetadataFidelity.NATIVE
            assert table_ddl.source == MetadataSource.CATALOG
            assert table_ddl.ddl is not None
            assert "CREATE TABLE orders" in table_ddl.ddl

            seq_ddl = dictionary.get_object_ddl(session, "seq_order_id", object_type="sequence")
            assert seq_ddl.ddl is not None
            assert "CREATE SEQUENCE" in seq_ddl.ddl

            type_ddl = dictionary.get_ddl(session, "order_status", object_type="type")
            assert type_ddl.ddl is not None
            assert "CREATE TYPE order_status" in type_ddl.ddl

            view_ddl = dictionary.get_ddl(session, "v_orders", object_type="view")
            assert view_ddl.ddl is not None
            assert "CREATE VIEW v_orders" in view_ddl.ddl

            index_ddl = dictionary.get_ddl(session, "idx_orders_user", object_type="index")
            assert index_ddl.ddl is not None
            assert "CREATE INDEX idx_orders_user" in index_ddl.ddl

            schema_result = dictionary.get_schema_ddl(session)
            assert schema_result.domain == "ddl"
            names = [item.identity.name for item in schema_result.items if isinstance(item, DDLResult)]
            assert "ddl_migrations" not in names
            assert names.index("order_status") < names.index("orders")
            assert names.index("seq_order_id") < names.index("orders")
            assert names.index("users") < names.index("orders")
            assert names.index("orders") < names.index("idx_orders_user")
            assert names.index("orders") < names.index("v_orders")

            drop_result = dictionary.get_schema_ddl(session, direction="drop")
            drop_names = [item.identity.name for item in drop_result.items if isinstance(item, DDLResult)]
            assert drop_names.index("orders") < drop_names.index("users")
            assert drop_names.index("orders") < drop_names.index("seq_order_id")
            assert drop_names.index("idx_orders_user") < drop_names.index("orders")
    finally:
        config.close_pool()


class _FakeCockroachSyncDriver:
    """Fake CockroachDB sync driver for unit testing DDL extraction."""

    def __init__(self, rows_by_call: list[list[Any]]) -> None:
        self.rows_by_call = list(rows_by_call)
        self.calls: list[tuple[Any, dict[str, Any]]] = []

    def select(self, query: Any, **kwargs: Any) -> list[Any]:
        self.calls.append((query, kwargs))
        if not self.rows_by_call:
            return []
        return self.rows_by_call.pop(0)


class _FakeCockroachAsyncDriver:
    """Fake CockroachDB async driver for unit testing DDL extraction."""

    def __init__(self, rows_by_call: list[list[Any]]) -> None:
        self.rows_by_call = list(rows_by_call)
        self.calls: list[tuple[Any, dict[str, Any]]] = []

    async def select(self, query: Any, **kwargs: Any) -> list[Any]:
        self.calls.append((query, kwargs))
        if not self.rows_by_call:
            return []
        return self.rows_by_call.pop(0)


def test_cockroach_sync_native_ddl_and_schema_ddl() -> None:
    """CockroachPsycopgSyncDataDictionary returns native DDL when crdb_internal.tables provides create_statement."""
    dictionary = CockroachPsycopgSyncDataDictionary()
    driver = _FakeCockroachSyncDriver([
        [
            {
                "schema_name": "public",
                "object_name": "users",
                "object_type": "table",
                "ddl": "CREATE TABLE public.users (id INT8 PRIMARY KEY)",
                "fidelity": "native",
            }
        ],
        [
            {
                "schema_name": "public",
                "object_name": "ddl_migrations",
                "object_type": "table",
                "ddl": "CREATE TABLE public.ddl_migrations (version_num STRING PRIMARY KEY)",
                "fidelity": "native",
            },
            {
                "schema_name": "public",
                "object_name": "orders",
                "object_type": "table",
                "ddl": "CREATE TABLE public.orders (id INT8 PRIMARY KEY, user_id INT8 REFERENCES public.users(id))",
                "fidelity": "native",
            },
            {
                "schema_name": "public",
                "object_name": "users",
                "object_type": "table",
                "ddl": "CREATE TABLE public.users (id INT8 PRIMARY KEY)",
                "fidelity": "native",
            },
            {
                "schema_name": "public",
                "object_name": "idx_orders_user",
                "object_type": "index",
                "table_name": "orders",
                "ddl": "CREATE INDEX idx_orders_user ON public.orders (user_id)",
                "fidelity": "native",
            },
        ],
        [
            ForeignKeyMetadata(
                table_name="orders",
                column_name="user_id",
                referenced_table="users",
                referenced_column="id",
                constraint_name="fk_orders_users",
                schema="public",
                referenced_schema="public",
            )
        ],
    ])

    single = dictionary.get_object_ddl(cast("Any", driver), "users", schema="public", object_type="table")
    assert single.status == MetadataSupport.SUPPORTED
    assert single.fidelity == MetadataFidelity.NATIVE
    assert single.ddl == "CREATE TABLE public.users (id INT8 PRIMARY KEY)"

    schema_ddl = dictionary.get_schema_ddl(cast("Any", driver), schema="public")
    names = [item.identity.name for item in schema_ddl.items if isinstance(item, DDLResult)]
    assert "ddl_migrations" not in names
    assert names == ["users", "orders", "idx_orders_user"]


@pytest.mark.anyio
async def test_cockroach_async_native_ddl_and_schema_ddl() -> None:
    """CockroachAsyncpgDataDictionary and CockroachPsycopgAsyncDataDictionary extract native DDL."""
    for dictionary in (CockroachAsyncpgDataDictionary(), CockroachPsycopgAsyncDataDictionary()):
        driver = _FakeCockroachAsyncDriver([
            [
                {
                    "schema_name": "public",
                    "object_name": "users",
                    "object_type": "table",
                    "ddl": "CREATE TABLE public.users (id INT8 PRIMARY KEY)",
                    "fidelity": "native",
                }
            ],
            [
                {
                    "schema_name": "public",
                    "object_name": "orders",
                    "object_type": "table",
                    "ddl": "CREATE TABLE public.orders (id INT8 PRIMARY KEY, user_id INT8 REFERENCES public.users(id))",
                    "fidelity": "native",
                },
                {
                    "schema_name": "public",
                    "object_name": "users",
                    "object_type": "table",
                    "ddl": "CREATE TABLE public.users (id INT8 PRIMARY KEY)",
                    "fidelity": "native",
                },
            ],
            [
                ForeignKeyMetadata(
                    table_name="orders",
                    column_name="user_id",
                    referenced_table="users",
                    referenced_column="id",
                    constraint_name="fk_orders_users",
                    schema="public",
                    referenced_schema="public",
                )
            ],
        ])

        single = await dictionary.get_ddl(cast("Any", driver), "users", schema="public", object_type="table")
        assert single.fidelity == MetadataFidelity.NATIVE
        assert single.ddl == "CREATE TABLE public.users (id INT8 PRIMARY KEY)"

        schema_ddl = await dictionary.get_schema_ddl(cast("Any", driver), schema="public")
        names = [item.identity.name for item in schema_ddl.items if isinstance(item, DDLResult)]
        assert names == ["users", "orders"]


class _FakeBigQueryDriver:
    """Fake BigQuery driver for unit testing native DDL extraction."""

    def __init__(self, rows_by_call: list[list[Any]]) -> None:
        self.rows_by_call = list(rows_by_call)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def select(self, query: str, **kwargs: Any) -> list[Any]:
        self.calls.append((query, kwargs))
        if not self.rows_by_call:
            return []
        return self.rows_by_call.pop(0)


def test_bigquery_native_ddl_and_schema_ddl() -> None:
    """BigQueryDataDictionary extracts native DDL from INFORMATION_SCHEMA.TABLES and ROUTINES."""
    dictionary = BigQueryDataDictionary()
    driver = _FakeBigQueryDriver([
        [
            {
                "table_catalog": "proj",
                "table_schema": "analytics",
                "table_name": "users",
                "table_type": "BASE TABLE",
                "object_name": "users",
                "object_type": "table",
                "ddl": "CREATE TABLE `proj.analytics.users` (id INT64 NOT NULL)",
                "fidelity": "native",
            }
        ],
        [
            {
                "table_catalog": "proj",
                "table_schema": "analytics",
                "object_name": "ddl_migrations",
                "object_type": "table",
                "ddl": "CREATE TABLE `proj.analytics.ddl_migrations` (version_num STRING)",
                "fidelity": "native",
            },
            {
                "table_catalog": "proj",
                "table_schema": "analytics",
                "object_name": "orders",
                "object_type": "table",
                "ddl": "CREATE TABLE `proj.analytics.orders` (id INT64, user_id INT64)",
                "fidelity": "native",
            },
            {
                "table_catalog": "proj",
                "table_schema": "analytics",
                "object_name": "users",
                "object_type": "table",
                "ddl": "CREATE TABLE `proj.analytics.users` (id INT64)",
                "fidelity": "native",
            },
            {
                "table_catalog": "proj",
                "table_schema": "analytics",
                "object_name": "v_orders",
                "object_type": "view",
                "ddl": "CREATE VIEW `proj.analytics.v_orders` AS SELECT * FROM `proj.analytics.orders`",
                "fidelity": "native",
            },
        ],
        [
            ForeignKeyMetadata(
                table_name="orders",
                column_name="user_id",
                referenced_table="users",
                referenced_column="id",
                constraint_name="fk_orders_users",
                schema="analytics",
                referenced_schema="analytics",
            )
        ],
    ])

    single = dictionary.get_ddl(cast("Any", driver), "users", schema="proj.analytics", object_type="table")
    assert single.status == MetadataSupport.SUPPORTED
    assert single.fidelity == MetadataFidelity.NATIVE
    assert single.source == MetadataSource.INFORMATION_SCHEMA
    assert single.ddl == "CREATE TABLE `proj.analytics.users` (id INT64 NOT NULL)"
    assert "`proj.analytics.INFORMATION_SCHEMA.TABLES`" in driver.calls[0][0]

    schema_ddl = dictionary.get_schema_ddl(cast("Any", driver), schema="proj.analytics")
    names = [item.identity.name for item in schema_ddl.items if isinstance(item, DDLResult)]
    assert "ddl_migrations" not in names
    assert names == ["users", "orders", "v_orders"]
