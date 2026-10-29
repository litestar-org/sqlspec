"""Native catalog DDL extraction tests: SQLite/DuckDB by execution, CockroachDB/BigQuery by full statements."""

import sqlite3
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.bigquery.data_dictionary import BigQueryDataDictionary
from sqlspec.adapters.cockroach_asyncpg.data_dictionary import CockroachAsyncpgDataDictionary
from sqlspec.adapters.cockroach_psycopg.data_dictionary import (
    CockroachPsycopgAsyncDataDictionary,
    CockroachPsycopgSyncDataDictionary,
)
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.data_dictionary import SqliteDataDictionary
from sqlspec.data_dictionary import (
    DataDictionaryLoader,
    DDLResult,
    MetadataFidelity,
    MetadataResult,
    MetadataSource,
    MetadataSupport,
    get_dialect_config,
)

NATIVE_SQLITE_SCHEMA = (
    "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)",
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id))",
    "CREATE INDEX idx_orders_user ON orders (user_id)",
    "CREATE VIEW v_user_orders AS SELECT u.email, o.id AS order_id FROM users u JOIN orders o ON o.user_id = u.id",
    "CREATE TRIGGER trg_orders_touch AFTER INSERT ON orders BEGIN SELECT 1; END",
)


def _statements(result: MetadataResult) -> list[str]:
    items = [item for item in result.items if isinstance(item, DDLResult)]
    assert all(item.status == MetadataSupport.SUPPORTED and item.ddl for item in items)
    return [str(item.ddl) for item in items]


def test_native_ddl_query_packs_expose_by_object_and_by_schema() -> None:
    """SQLite, DuckDB, CockroachDB, and BigQuery expose both by_object and by_schema DDL queries."""
    loader = DataDictionaryLoader()

    for dialect in ("sqlite", "duckdb", "bigquery"):
        assert loader.get_domain_query_text(dialect, "ddl", "by_object") is not None, dialect
        assert loader.get_domain_query_text(dialect, "ddl", "by_schema") is not None, dialect

    duckdb_by_schema = loader.get_domain_query_text("duckdb", "ddl", "by_schema") or ""
    for source in ("duckdb_tables()", "duckdb_views()", "duckdb_indexes()", "duckdb_sequences()", "duckdb_types()"):
        assert source in duckdb_by_schema

    crdb_stable = loader.get_domain_query_text("cockroachdb", "ddl", "by_object") or ""
    assert "crdb_internal" not in crdb_stable
    crdb_by_schema = loader.get_domain_query_text("cockroachdb", "crdb_internal", "ddl_by_schema") or ""
    assert "crdb_internal.tables" in crdb_by_schema
    assert "pg_get_indexdef" in crdb_by_schema
    gated = loader.get_domain_query(
        "cockroachdb", "crdb_internal", "ddl_by_schema", required_features=("supports_crdb_internal_metadata",)
    )
    assert gated.is_supported is False


def test_sqlite_native_object_ddl_returns_stored_statements() -> None:
    """SQLite object DDL returns the stored sqlite_schema statement for every object kind."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    dictionary = SqliteDataDictionary()
    try:
        with config.provide_session() as session:
            for statement in NATIVE_SQLITE_SCHEMA:
                session.execute_script(statement)

            table_ddl = dictionary.get_ddl(session, "users")
            assert table_ddl.status == MetadataSupport.SUPPORTED
            assert table_ddl.fidelity == MetadataFidelity.NATIVE
            assert table_ddl.source == MetadataSource.CATALOG
            assert table_ddl.ddl == f"{NATIVE_SQLITE_SCHEMA[0]};"
            assert dictionary.get_object_ddl(session, "orders").ddl == f"{NATIVE_SQLITE_SCHEMA[1]};"
            assert (
                dictionary.get_ddl(session, "idx_orders_user", object_type="index").ddl == f"{NATIVE_SQLITE_SCHEMA[2]};"
            )
            assert dictionary.get_ddl(session, "v_user_orders", object_type="view").ddl == f"{NATIVE_SQLITE_SCHEMA[3]};"
            assert dictionary.get_ddl(session, "trg_orders_touch", object_type="trigger").ddl == (
                f"{NATIVE_SQLITE_SCHEMA[4]};"
            )

            missing = dictionary.get_ddl(session, "nonexistent_table")
            assert missing.status == MetadataSupport.UNSUPPORTED
            assert missing.ddl is None
    finally:
        config.close_pool()


@pytest.mark.anyio
async def test_aiosqlite_native_object_ddl_returns_stored_statements() -> None:
    """Async SQLite object DDL returns the stored sqlite_schema statement."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    try:
        async with config.provide_session() as session:
            for statement in NATIVE_SQLITE_SCHEMA[:3]:
                await session.execute_script(statement)
            dictionary = session.data_dictionary
            users = await dictionary.get_ddl(session, "users")
            assert users.fidelity == MetadataFidelity.NATIVE
            assert users.ddl == f"{NATIVE_SQLITE_SCHEMA[0]};"
            orders = await dictionary.get_object_ddl(session, "orders")
            assert orders.ddl == f"{NATIVE_SQLITE_SCHEMA[1]};"
    finally:
        await config.close_pool()


def test_duckdb_native_object_ddl_reports_tables_with_foreign_keys_as_generated() -> None:
    """DuckDB table DDL with foreign keys is synthesized because the catalog SQL drops self references."""
    config = DuckDBConfig(connection_config={"database": ":memory:shared_native_object"})
    try:
        with config.provide_session() as session:
            session.execute_script(
                "CREATE TYPE order_status AS ENUM ('pending', 'paid');"
                "CREATE SEQUENCE seq_order_id START 1;"
                "CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR NOT NULL);"
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id));"
            )
            dictionary = session.data_dictionary

            users = dictionary.get_ddl(session, "users")
            assert users.fidelity == MetadataFidelity.NATIVE
            assert users.ddl == "CREATE TABLE users(id INTEGER PRIMARY KEY, email VARCHAR NOT NULL);"

            orders = dictionary.get_ddl(session, "orders")
            assert orders.status == MetadataSupport.SUPPORTED
            assert orders.fidelity == MetadataFidelity.GENERATED
            assert any("self-referencing foreign keys" in warning for warning in orders.warnings)

            assert dictionary.get_object_ddl(session, "seq_order_id", object_type="sequence").ddl == (
                'CREATE SEQUENCE "seq_order_id" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 '
                "START WITH 1 NO CYCLE;"
            )
            assert dictionary.get_ddl(session, "order_status", object_type="type").ddl == (
                "CREATE TYPE \"order_status\" AS ENUM ('pending', 'paid');"
            )
    finally:
        config.close_pool()


def test_get_ddl_override_calling_super_does_not_recurse() -> None:
    """A subclass overriding get_ddl and delegating to super() resolves DDL without recursion."""

    class AuditingSqliteDataDictionary(SqliteDataDictionary):
        calls: list[str] = []

        def get_ddl(self, driver: Any, object_name: str, schema: "str | None" = None, **kwargs: Any) -> DDLResult:
            self.calls.append(object_name)
            return super().get_ddl(driver, object_name, schema, **kwargs)

    config = SqliteConfig(connection_config={"database": ":memory:"})
    dictionary = AuditingSqliteDataDictionary()
    try:
        with config.provide_session() as session:
            session.execute_script(NATIVE_SQLITE_SCHEMA[0])
            ddl = dictionary.get_ddl(session, "users")
            object_ddl = dictionary.get_object_ddl(session, "users")
    finally:
        config.close_pool()

    assert ddl.ddl == f"{NATIVE_SQLITE_SCHEMA[0]};"
    assert object_ddl.ddl == ddl.ddl
    assert dictionary.calls == ["users"]


def test_native_create_view_is_transpiled_to_target_dialect() -> None:
    """Catalog CREATE VIEW text is parsed in the source dialect and rendered in the target dialect."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as session:
            session.execute_script(
                "CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT);"
                "CREATE VIEW v_accounts AS SELECT id, IFNULL(email, 'n/a') AS email FROM accounts;"
            )
            dictionary = session.data_dictionary
            view = dictionary.get_ddl(session, "v_accounts", object_type="view", target_dialect="postgres")
            schema = dictionary.get_schema_ddl(session, target_dialect="postgres", include_domains=("views",))
    finally:
        config.close_pool()

    expected = "CREATE VIEW v_accounts AS SELECT id, COALESCE(email, 'n/a') AS email FROM accounts;"
    assert view.identity.dialect == "postgres"
    assert view.ddl == expected
    assert _statements(schema) == [expected]


def test_sqlite_and_duckdb_honor_prefer_native_target_dialect_and_domain_filters() -> None:
    """SQLite and DuckDB honor prefer_native=False, target_dialect, and domain filters."""
    sqlite_cfg = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with sqlite_cfg.provide_session() as session:
            session.execute_script(
                "CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT NOT NULL);"
                "CREATE VIEW v_accounts AS SELECT id, email FROM accounts;"
            )
            dictionary = session.data_dictionary

            synthesized = dictionary.get_ddl(session, "accounts", prefer_native=False)
            assert synthesized.fidelity == MetadataFidelity.GENERATED
            assert (
                synthesized.ddl == 'CREATE TABLE "accounts" (\n  "id" INTEGER PRIMARY KEY,\n  "email" TEXT NOT NULL\n);'
            )

            transpiled = dictionary.get_object_ddl(session, "accounts", target_dialect="postgres")
            assert transpiled.identity.dialect == "postgres"
            assert transpiled.ddl == 'CREATE TABLE "accounts" (\n  "id" INT PRIMARY KEY,\n  "email" TEXT NOT NULL\n);'

            tables_only = dictionary.get_schema_ddl(session, include_domains=("tables",))
            assert _statements(tables_only) == ["CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT NOT NULL);"]
    finally:
        sqlite_cfg.close_pool()

    duck_cfg = DuckDBConfig(connection_config={"database": ":memory:shared_prefer_native"})
    try:
        with duck_cfg.provide_session() as session:
            session.execute_script(
                "CREATE TABLE metrics (id INTEGER PRIMARY KEY, label VARCHAR NOT NULL);"
                "CREATE VIEW v_metrics AS SELECT id, label FROM metrics;"
            )
            transpiled_schema = session.data_dictionary.get_schema_ddl(
                session, target_dialect="postgres", exclude_domains=("views",)
            )
    finally:
        duck_cfg.close_pool()

    items = [item for item in transpiled_schema.items if isinstance(item, DDLResult)]
    assert [item.identity.name for item in items] == ["metrics"]
    assert items[0].identity.dialect == "postgres"


class _FakeSequencedSyncDriver:
    """Sync driver stub returning queued result sets in call order."""

    def __init__(self, rows_by_call: list[list[Any]]) -> None:
        self.rows_by_call = list(rows_by_call)
        self.calls: list[tuple[Any, dict[str, Any]]] = []

    def select(self, query: Any, **kwargs: Any) -> list[Any]:
        self.calls.append((query, kwargs))
        return self.rows_by_call.pop(0) if self.rows_by_call else []


class _FakeSequencedAsyncDriver:
    """Async driver stub returning queued result sets in call order."""

    def __init__(self, rows_by_call: list[list[Any]]) -> None:
        self._sync = _FakeSequencedSyncDriver(rows_by_call)

    async def select(self, query: Any, **kwargs: Any) -> list[Any]:
        return self._sync.select(query, **kwargs)


def _cockroach_schema_rows() -> list[dict[str, Any]]:
    return [
        {
            "schema_name": "public",
            "object_name": "ddl_migrations",
            "object_type": "table",
            "ddl": "CREATE TABLE public.ddl_migrations (version_num STRING PRIMARY KEY)",
            "fidelity": "native",
        },
        {
            "schema_name": "public",
            "object_name": "Orders",
            "object_type": "table",
            "ddl": 'CREATE TABLE public."Orders" (id INT8 PRIMARY KEY, user_id INT8 REFERENCES public.users(id))',
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
            "table_name": "Orders",
            "ddl": 'CREATE INDEX idx_orders_user ON public."Orders" (user_id)',
            "fidelity": "native",
        },
    ]


def _cockroach_fk_rows() -> list[dict[str, Any]]:
    return [
        {
            "table_name": "Orders",
            "column_name": "user_id",
            "referenced_table": "users",
            "referenced_column": "id",
            "constraint_name": "fk_orders_users",
            "table_schema": "public",
            "referenced_schema": "public",
            "on_delete": "CASCADE",
            "on_update": "NO ACTION",
        }
    ]


@pytest.fixture
def crdb_internal_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enable the crdb_internal metadata feature gate for native CockroachDB DDL."""
    monkeypatch.setitem(get_dialect_config("cockroachdb").feature_flags, "supports_crdb_internal_metadata", True)


def test_cockroach_default_ddl_uses_stable_catalog_and_synthesizes_tables() -> None:
    """Without crdb_internal metadata, CockroachDB schema DDL is synthesized from stable catalog surfaces."""
    dictionary = CockroachPsycopgSyncDataDictionary()
    driver = _FakeSequencedSyncDriver([
        [{"schema_name": "public", "object_name": "users", "object_type": "table", "ddl": None, "fidelity": "lossy"}]
    ])
    result = dictionary.get_object_ddl(cast("Any", driver), "users", schema="public")
    assert "crdb_internal" not in str(driver.calls[0][0])
    assert result.status == MetadataSupport.UNSUPPORTED


@pytest.mark.usefixtures("crdb_internal_enabled")
def test_cockroach_sync_native_ddl_and_schema_ddl() -> None:
    """CockroachDB native schema DDL is ordered by foreign keys and dropped with quoted exact names."""
    dictionary = CockroachPsycopgSyncDataDictionary()
    single_driver = _FakeSequencedSyncDriver([
        [
            {
                "schema_name": "public",
                "object_name": "users",
                "object_type": "table",
                "ddl": "CREATE TABLE public.users (id INT8 PRIMARY KEY)",
                "fidelity": "native",
            }
        ]
    ])
    single = dictionary.get_object_ddl(cast("Any", single_driver), "users", schema="public")
    assert single.fidelity == MetadataFidelity.NATIVE
    assert single.ddl == "CREATE TABLE public.users (id INT8 PRIMARY KEY)"

    create_driver = _FakeSequencedSyncDriver([_cockroach_schema_rows(), _cockroach_fk_rows()])
    create = dictionary.get_schema_ddl(cast("Any", create_driver), schema="public", exclude_tables=("ddl_migrations",))
    assert _statements(create) == [
        "CREATE TABLE public.users (id INT8 PRIMARY KEY)",
        'CREATE TABLE public."Orders" (id INT8 PRIMARY KEY, user_id INT8 REFERENCES public.users(id))',
        'CREATE INDEX idx_orders_user ON public."Orders" (user_id)',
    ]

    drop_driver = _FakeSequencedSyncDriver([_cockroach_schema_rows(), _cockroach_fk_rows()])
    drop = dictionary.get_schema_ddl(
        cast("Any", drop_driver), schema="public", direction="drop", exclude_tables=('"public"."ddl_migrations"',)
    )
    assert _statements(drop) == ['DROP TABLE IF EXISTS "public"."Orders";', 'DROP TABLE IF EXISTS "public"."users";']


@pytest.mark.anyio
@pytest.mark.usefixtures("crdb_internal_enabled")
async def test_cockroach_async_native_ddl_and_schema_ddl() -> None:
    """Async CockroachDB dictionaries share the native schema DDL ordering and DROP generation."""
    for dictionary in (CockroachAsyncpgDataDictionary(), CockroachPsycopgAsyncDataDictionary()):
        create_driver = _FakeSequencedAsyncDriver([_cockroach_schema_rows(), _cockroach_fk_rows()])
        create = await dictionary.get_schema_ddl(cast("Any", create_driver), schema="public")
        assert [item.identity.name for item in create.items if isinstance(item, DDLResult)] == [
            "ddl_migrations",
            "users",
            "Orders",
            "idx_orders_user",
        ]

        drop_driver = _FakeSequencedAsyncDriver([_cockroach_schema_rows(), _cockroach_fk_rows()])
        drop = await dictionary.get_schema_ddl(
            cast("Any", drop_driver), schema="public", direction="drop", exclude_tables=("DDL_MIGRATIONS",)
        )
        assert _statements(drop) == [
            'DROP TABLE IF EXISTS "public"."Orders";',
            'DROP TABLE IF EXISTS "public"."users";',
        ]


def _bigquery_schema_rows() -> list[dict[str, Any]]:
    return [
        {
            "table_catalog": "proj",
            "table_schema": "analytics",
            "object_name": "ddl_migrations",
            "object_type": "table",
            "ddl": "CREATE TABLE `proj.analytics.ddl_migrations` (version_num STRING)",
        },
        {
            "table_catalog": "proj",
            "table_schema": "analytics",
            "object_name": "orders",
            "object_type": "table",
            "ddl": "CREATE TABLE `proj.analytics.orders` (id INT64, user_id INT64)",
        },
        {
            "table_catalog": "proj",
            "table_schema": "analytics",
            "object_name": "users",
            "object_type": "table",
            "ddl": "CREATE TABLE `proj.analytics.users` (id INT64)",
        },
        {
            "table_catalog": "proj",
            "table_schema": "analytics",
            "object_name": "v_orders",
            "object_type": "view",
            "ddl": "CREATE VIEW `proj.analytics.v_orders` AS SELECT * FROM `proj.analytics.orders`",
        },
    ]


def _bigquery_fk_rows() -> list[dict[str, Any]]:
    return [
        {
            "table_name": "orders",
            "column_name": "user_id",
            "referenced_table": "users",
            "referenced_column": "id",
            "constraint_name": "fk_orders_users",
            "table_schema": "analytics",
            "referenced_schema": "analytics",
        }
    ]


def test_bigquery_native_ddl_and_schema_ddl() -> None:
    """BigQuery native schema DDL orders views after tables and drops objects with quoted project paths."""
    dictionary = BigQueryDataDictionary()
    single_driver = _FakeSequencedSyncDriver([
        [
            {
                "table_catalog": "proj",
                "table_schema": "analytics",
                "object_name": "users",
                "object_type": "table",
                "ddl": "CREATE TABLE `proj.analytics.users` (id INT64 NOT NULL)",
            }
        ]
    ])
    single = dictionary.get_ddl(cast("Any", single_driver), "users", schema="proj.analytics")
    assert single.fidelity == MetadataFidelity.NATIVE
    assert single.source == MetadataSource.INFORMATION_SCHEMA
    assert single.ddl == "CREATE TABLE `proj.analytics.users` (id INT64 NOT NULL)"
    assert "`proj.analytics.INFORMATION_SCHEMA.TABLES`" in single_driver.calls[0][0]

    create_driver = _FakeSequencedSyncDriver([_bigquery_schema_rows(), _bigquery_fk_rows()])
    create = dictionary.get_schema_ddl(
        cast("Any", create_driver), schema="proj.analytics", exclude_tables=("analytics.ddl_migrations",)
    )
    assert _statements(create) == [
        "CREATE TABLE `proj.analytics.users` (id INT64)",
        "CREATE TABLE `proj.analytics.orders` (id INT64, user_id INT64)",
        "CREATE VIEW `proj.analytics.v_orders` AS SELECT * FROM `proj.analytics.orders`",
    ]

    drop_driver = _FakeSequencedSyncDriver([_bigquery_schema_rows(), _bigquery_fk_rows()])
    drop = dictionary.get_schema_ddl(
        cast("Any", drop_driver), schema="proj.analytics", direction="drop", exclude_tables=("`ddl_migrations`",)
    )
    assert _statements(drop) == [
        "DROP VIEW IF EXISTS `proj`.`analytics`.`v_orders`;",
        "DROP TABLE IF EXISTS `proj`.`analytics`.`orders`;",
        "DROP TABLE IF EXISTS `proj`.`analytics`.`users`;",
    ]


SQLITE_SCHEMA = """
CREATE TABLE "Parent Table" (
    "Id" INTEGER PRIMARY KEY,
    "select" TEXT NOT NULL,
    "Code" TEXT,
    UNIQUE ("Code")
);
CREATE TABLE "Child" (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER REFERENCES "Parent Table"("Id") ON DELETE CASCADE ON UPDATE SET NULL,
    manager_id INTEGER REFERENCES "Child"(id),
    email TEXT UNIQUE
);
CREATE INDEX "Idx Child Email" ON "Child" (email, parent_id);
CREATE VIEW "Child View" AS SELECT id, email FROM "Child";
CREATE TABLE ddl_migrations (version_num TEXT PRIMARY KEY);
"""

DUCKDB_SCHEMA = """
CREATE TYPE "My Mood" AS ENUM ('happy', 'it''s');
CREATE SEQUENCE "Seq One" START 5;
CREATE TABLE "Parent Table" (
    "Id" INTEGER PRIMARY KEY,
    "select" VARCHAR NOT NULL,
    "Code" VARCHAR,
    feeling "My Mood",
    UNIQUE ("Code"),
    CHECK ("Id" > 0)
);
CREATE TABLE "Child" (
    id INTEGER PRIMARY KEY DEFAULT nextval('"Seq One"'),
    parent_id INTEGER REFERENCES "Parent Table"("Id"),
    manager_id INTEGER REFERENCES "Child"(id),
    email VARCHAR UNIQUE
);
CREATE INDEX "Idx Child Email" ON "Child" (email, parent_id);
CREATE VIEW "Child View" AS SELECT id, email FROM "Child";
CREATE TABLE ddl_migrations (version_num VARCHAR PRIMARY KEY);
"""

EXCLUDED_VERSION_TABLE = ('"DDL_Migrations"',)


def _sqlite_snapshot(path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(path)
    try:
        objects = connection.execute(
            "SELECT type, name, tbl_name FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name <> 'ddl_migrations' ORDER BY type, name"
        ).fetchall()
        tables = [name for kind, name, _ in objects if kind == "table"]
        details: dict[str, Any] = {}
        for table in tables:
            indexes = []
            for _, index_name, unique, origin, partial in connection.execute(
                'SELECT seq, name, "unique", origin, partial FROM pragma_index_list(?)', (table,)
            ).fetchall():
                columns = [
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM pragma_index_info(?) ORDER BY seqno", (index_name,)
                    ).fetchall()
                ]
                indexes.append((index_name if origin == "c" else None, unique, origin, partial, tuple(columns)))
            details[table] = {
                "columns": connection.execute(
                    'SELECT name, type, "notnull", dflt_value, pk, hidden FROM pragma_table_xinfo(?) ORDER BY cid',
                    (table,),
                ).fetchall(),
                "foreign_keys": sorted(
                    connection.execute(
                        'SELECT "table", "from", "to", on_update, on_delete FROM pragma_foreign_key_list(?)', (table,)
                    ).fetchall()
                ),
                "indexes": sorted(indexes, key=repr),
            }
        return {"objects": objects, "tables": details}
    finally:
        connection.close()


def _sqlite_user_objects(path: Path) -> list[tuple[str, str]]:
    connection = sqlite3.connect(path)
    try:
        return connection.execute(
            "SELECT type, name FROM sqlite_schema WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name"
        ).fetchall()
    finally:
        connection.close()


def _sqlite_apply(path: Path, statements: list[str]) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        for statement in statements:
            connection.executescript(statement)
        connection.commit()
    finally:
        connection.close()


def _duckdb_snapshot(path: Path) -> dict[str, Any]:
    connection = duckdb.connect(str(path))
    try:
        return {
            "tables": connection.execute(
                "SELECT table_name FROM duckdb_tables() WHERE NOT internal AND table_name <> 'ddl_migrations' "
                "ORDER BY 1"
            ).fetchall(),
            "columns": connection.execute(
                "SELECT table_name, column_name, data_type, is_nullable, column_default FROM duckdb_columns() "
                "WHERE NOT internal AND table_name <> 'ddl_migrations' ORDER BY table_name, column_index"
            ).fetchall(),
            "constraints": connection.execute(
                "SELECT table_name, constraint_type, constraint_column_names, referenced_table, "
                "referenced_column_names, lower(expression) FROM duckdb_constraints() "
                "WHERE table_name <> 'ddl_migrations' ORDER BY ALL"
            ).fetchall(),
            "indexes": connection.execute(
                "SELECT index_name, table_name, is_unique, expressions FROM duckdb_indexes() ORDER BY 1"
            ).fetchall(),
            "views": connection.execute(
                "SELECT view_name FROM duckdb_views() WHERE NOT internal ORDER BY 1"
            ).fetchall(),
            "sequences": connection.execute(
                "SELECT sequence_name, start_value, increment_by, min_value, max_value, cycle "
                "FROM duckdb_sequences() ORDER BY 1"
            ).fetchall(),
            "types": connection.execute(
                "SELECT type_name, labels FROM duckdb_types() WHERE NOT internal ORDER BY 1"
            ).fetchall(),
        }
    finally:
        connection.close()


def _duckdb_apply(path: Path, statements: list[str]) -> None:
    connection = duckdb.connect(str(path))
    try:
        for statement in statements:
            connection.execute(statement)
    finally:
        connection.close()


def _duckdb_user_objects(path: Path) -> list[tuple[str, str]]:
    connection = duckdb.connect(str(path))
    try:
        return connection.execute(
            "SELECT 'table', table_name FROM duckdb_tables() WHERE NOT internal "
            "UNION ALL SELECT 'view', view_name FROM duckdb_views() WHERE NOT internal "
            "UNION ALL SELECT 'sequence', sequence_name FROM duckdb_sequences() "
            "UNION ALL SELECT 'type', type_name FROM duckdb_types() WHERE NOT internal ORDER BY ALL"
        ).fetchall()
    finally:
        connection.close()


def _create_sqlite_source(tmp_path: Path) -> Path:
    source = tmp_path / "source.sqlite"
    connection = sqlite3.connect(source)
    try:
        connection.executescript(SQLITE_SCHEMA)
    finally:
        connection.close()
    return source


def _create_duckdb_source(tmp_path: Path) -> Path:
    source = tmp_path / "source.duckdb"
    _duckdb_apply(source, [DUCKDB_SCHEMA])
    return source


@pytest.mark.parametrize("prefer_native", [True, False], ids=["native", "synthesized"])
def test_sqlite_schema_ddl_replays_and_drops_cleanly(tmp_path: Path, prefer_native: bool) -> None:
    """SQLite create DDL rebuilds an identical schema and drop DDL removes every user object."""
    source = _create_sqlite_source(tmp_path)
    config = SqliteConfig(connection_config={"database": str(source)})
    try:
        with config.provide_session() as session:
            dictionary = session.data_dictionary
            create = dictionary.get_schema_ddl(
                session, prefer_native=prefer_native, exclude_tables=EXCLUDED_VERSION_TABLE
            )
            drop = dictionary.get_schema_ddl(
                session, prefer_native=prefer_native, direction="drop", exclude_tables=("main.ddl_migrations",)
            )
    finally:
        config.close_pool()

    expected_fidelity = MetadataFidelity.NATIVE if prefer_native else MetadataFidelity.GENERATED
    assert create.capability.fidelity == expected_fidelity
    names = [item.identity.name for item in create.items if isinstance(item, DDLResult)]
    assert "ddl_migrations" not in names
    drop_statements = _statements(drop)
    assert drop_statements == [
        'DROP VIEW IF EXISTS "Child View";',
        'DROP TABLE IF EXISTS "Child";',
        'DROP TABLE IF EXISTS "Parent Table";',
    ]

    replay = tmp_path / "replay.sqlite"
    _sqlite_apply(replay, _statements(create))
    assert _sqlite_snapshot(replay) == _sqlite_snapshot(source)

    _sqlite_apply(replay, drop_statements)
    assert _sqlite_user_objects(replay) == []


@pytest.mark.anyio
@pytest.mark.parametrize("prefer_native", [True, False], ids=["native", "synthesized"])
async def test_aiosqlite_schema_ddl_replays_and_drops_cleanly(tmp_path: Path, prefer_native: bool) -> None:
    """Async SQLite create DDL rebuilds an identical schema and drop DDL removes every user object."""
    source = _create_sqlite_source(tmp_path)
    config = AiosqliteConfig(connection_config={"database": str(source)})
    try:
        async with config.provide_session() as session:
            dictionary = session.data_dictionary
            create = await dictionary.get_schema_ddl(
                session, prefer_native=prefer_native, exclude_tables=EXCLUDED_VERSION_TABLE
            )
            drop = await dictionary.get_schema_ddl(
                session, prefer_native=prefer_native, direction="drop", exclude_tables=EXCLUDED_VERSION_TABLE
            )
    finally:
        await config.close_pool()

    replay = tmp_path / "replay.sqlite"
    _sqlite_apply(replay, _statements(create))
    assert _sqlite_snapshot(replay) == _sqlite_snapshot(source)
    _sqlite_apply(replay, _statements(drop))
    assert _sqlite_user_objects(replay) == []


@pytest.mark.parametrize("prefer_native", [True, False], ids=["native", "synthesized"])
def test_duckdb_schema_ddl_replays_and_drops_cleanly(tmp_path: Path, prefer_native: bool) -> None:
    """DuckDB create DDL rebuilds types, sequences, tables, indexes, and views; drop DDL removes them all."""
    source = _create_duckdb_source(tmp_path)
    config = DuckDBConfig(connection_config={"database": str(source)})
    try:
        with config.provide_session() as session:
            dictionary = session.data_dictionary
            create = dictionary.get_schema_ddl(
                session, prefer_native=prefer_native, exclude_tables=EXCLUDED_VERSION_TABLE
            )
            drop = dictionary.get_schema_ddl(
                session, prefer_native=prefer_native, direction="drop", exclude_tables=EXCLUDED_VERSION_TABLE
            )
    finally:
        config.close_pool()

    create_items = [item for item in create.items if isinstance(item, DDLResult)]
    assert "ddl_migrations" not in [item.identity.name for item in create_items]
    assert create.capability.fidelity == MetadataFidelity.GENERATED
    if prefer_native:
        child = next(item for item in create_items if item.identity.name == "Child")
        assert child.fidelity == MetadataFidelity.GENERATED
        assert any("self-referencing foreign keys" in warning for warning in child.warnings)
        assert any("self-referencing foreign keys" in warning for warning in create.warnings)

    replay = tmp_path / "replay.duckdb"
    _duckdb_apply(replay, _statements(create))
    expected = _duckdb_snapshot(source)
    if not prefer_native:
        expected["types"] = []
    assert _duckdb_snapshot(replay) == expected

    _duckdb_apply(replay, _statements(drop))
    assert _duckdb_user_objects(replay) == []


def test_duckdb_native_drop_statements_use_object_kinds(tmp_path: Path) -> None:
    """DuckDB drop DDL issues DROP TYPE / SEQUENCE / VIEW / TABLE with quoted exact names in dependency order."""
    source = _create_duckdb_source(tmp_path)
    config = DuckDBConfig(connection_config={"database": str(source)})
    try:
        with config.provide_session() as session:
            drop = session.data_dictionary.get_schema_ddl(session, direction="drop", exclude_tables=("ddl_migrations",))
    finally:
        config.close_pool()

    assert _statements(drop) == [
        'DROP VIEW IF EXISTS "main"."Child View";',
        'DROP TABLE IF EXISTS "main"."Child";',
        'DROP TABLE IF EXISTS "main"."Parent Table";',
        'DROP SEQUENCE IF EXISTS "main"."Seq One";',
        'DROP TYPE IF EXISTS "main"."My Mood";',
    ]


def test_sqlite_synthesized_ddl_keeps_checks_generated_columns_and_autoincrement(tmp_path: Path) -> None:
    """Synthesized SQLite DDL reproduces CHECK constraints, generated columns, and AUTOINCREMENT."""
    source = tmp_path / "ledger.sqlite"
    _sqlite_apply(
        source,
        [
            (
                'CREATE TABLE "Ledger" ('
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "amount INTEGER NOT NULL CHECK (amount >= 0), "
                '"Doubled" INTEGER GENERATED ALWAYS AS (amount * 2) STORED, '
                "CONSTRAINT ck_small CHECK (amount < 1000))"
            )
        ],
    )
    config = SqliteConfig(connection_config={"database": str(source)})
    try:
        with config.provide_session() as session:
            create = session.data_dictionary.get_schema_ddl(session, prefer_native=False)
    finally:
        config.close_pool()

    items = [item for item in create.items if isinstance(item, DDLResult)]
    assert [item.fidelity for item in items] == [MetadataFidelity.GENERATED]
    assert create.warnings == ()

    replay = tmp_path / "ledger_replay.sqlite"
    _sqlite_apply(replay, _statements(create))
    assert _sqlite_snapshot(replay) == _sqlite_snapshot(source)

    connection = sqlite3.connect(replay)
    try:
        connection.execute('INSERT INTO "Ledger" (amount) VALUES (5)')
        assert connection.execute('SELECT "Doubled" FROM "Ledger"').fetchone() == (10,)
        assert connection.execute("SELECT name FROM sqlite_sequence").fetchall() == [("Ledger",)]
        for invalid in (-1, 1000):
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
                connection.execute('INSERT INTO "Ledger" (amount) VALUES (?)', (invalid,))
    finally:
        connection.close()


def test_sqlite_foreign_key_metadata_reports_actions_without_pragma_ids(tmp_path: Path) -> None:
    """SQLite foreign keys expose ON DELETE/UPDATE actions; only composite keys get a grouping name."""
    source = tmp_path / "fk.sqlite"
    parent_ddl = "CREATE TABLE parent (a INTEGER, b INTEGER, PRIMARY KEY (a, b))"
    _sqlite_apply(
        source,
        [
            parent_ddl,
            (
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pa INTEGER, pb INTEGER, "
                "self_id INTEGER REFERENCES child(id) ON DELETE SET NULL, "
                "FOREIGN KEY (pa, pb) REFERENCES parent(a, b) ON UPDATE CASCADE)"
            ),
        ],
    )
    config = SqliteConfig(connection_config={"database": str(source)})
    try:
        with config.provide_session() as session:
            foreign_keys = session.data_dictionary.get_foreign_keys(session, table="child")
            child_ddl = session.data_dictionary.get_ddl(session, "child", prefer_native=False)
    finally:
        config.close_pool()

    assert sorted(
        (fk.column_name, fk.referenced_table, fk.referenced_column, fk.constraint_name, fk.on_delete, fk.on_update)
        for fk in foreign_keys
    ) == [
        ("pa", "parent", "a", "child_fk_0", "NO ACTION", "CASCADE"),
        ("pb", "parent", "b", "child_fk_0", "NO ACTION", "CASCADE"),
        ("self_id", "child", "id", None, "SET NULL", "NO ACTION"),
    ]
    assert child_ddl.ddl is not None
    assert 'CONSTRAINT "0"' not in child_ddl.ddl
    assert 'CONSTRAINT "1"' not in child_ddl.ddl

    replay = tmp_path / "fk_replay.sqlite"
    _sqlite_apply(replay, [parent_ddl, child_ddl.ddl])
    assert _sqlite_snapshot(replay)["tables"]["child"] == _sqlite_snapshot(source)["tables"]["child"]
