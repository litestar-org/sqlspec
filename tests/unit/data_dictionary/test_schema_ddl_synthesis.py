"""Builder-backed table DDL synthesis, schema DDL ordering, exact catalog names, and profiling SQL."""

import sqlite3
from pathlib import Path
from typing import Any, cast

import pytest

from sqlspec.adapters.adbc.data_dictionary import AdbcDataDictionary
from sqlspec.adapters.asyncpg.data_dictionary import AsyncpgDataDictionary
from sqlspec.adapters.db2.data_dictionary import Db2AsyncDataDictionary, Db2SyncDataDictionary
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.psqlpy.data_dictionary import PsqlpyDataDictionary
from sqlspec.adapters.psycopg.data_dictionary import PsycopgAsyncDataDictionary, PsycopgSyncDataDictionary
from sqlspec.adapters.pymysql.data_dictionary import PyMysqlDataDictionary
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.data_dictionary import (
    DDLResult,
    ForeignKeyMetadata,
    MetadataFidelity,
    MetadataResult,
    MetadataSource,
    MetadataSupport,
    get_data_dictionary_loader,
)

_ROUTED_DOMAINS = (
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
    "sequences",
    "types",
    "ddl",
    "dependencies",
    "foreign_keys",
)
_ROUTED_QUERIES = ("by_schema", "by_table", "by_object", "all_by_schema")


def _column(schema: str, table: str, name: str, data_type: str, position: int, **extra: Any) -> dict[str, Any]:
    return {
        "schema_name": schema,
        "table_name": table,
        "column_name": name,
        "data_type": data_type,
        "is_nullable": "NO",
        "ordinal_position": position,
        **extra,
    }


def _fk_row(schema: str, table: str, column: str, ref_table: str, ref_column: str, name: str) -> dict[str, Any]:
    return {
        "table_name": table,
        "column_name": column,
        "referenced_table_name": ref_table,
        "referenced_column_name": ref_column,
        "constraint_name": name,
        "table_schema": schema,
        "referenced_table_schema": schema,
        "on_delete": "CASCADE",
        "on_update": "NO ACTION",
    }


def _empty_schema(**overrides: Any) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "tables": [],
        "columns": {},
        "constraints": {},
        "foreign_keys": [],
        "indexes": {},
        "views": [],
        "dependencies": [],
        "sequences": [],
        "types": [],
    }
    schema.update(overrides)
    return schema


POSTGRES_PUBLIC: dict[str, Any] = {
    "tables": ["order_items", "orders", "customers", "ddl_migrations"],
    "columns": {
        "customers": [
            _column("public", "customers", "id", "BIGINT", 1, is_primary=True),
            _column("public", "customers", "email", "VARCHAR", 2, max_length=255),
        ],
        "orders": [
            _column("public", "orders", "id", "BIGINT", 1, is_primary=True),
            _column("public", "orders", "customer_id", "BIGINT", 2),
            _column("public", "orders", "order_code", "VARCHAR", 3, max_length=64),
            _column("public", "orders", "total_cents", "INTEGER", 4, column_default="0"),
            _column("public", "orders", "status", "order_status", 5, column_default="'new'::order_status"),
        ],
        "order_items": [
            _column("public", "order_items", "id", "BIGINT", 1, is_primary=True),
            _column("public", "order_items", "order_id", "BIGINT", 2),
        ],
        "ddl_migrations": [
            _column("public", "ddl_migrations", "version_num", "VARCHAR", 1, max_length=64, is_primary=True)
        ],
    },
    "constraints": {
        "orders": [
            {"constraint_name": "pk_orders", "constraint_type": "PRIMARY KEY", "columns": ["id"]},
            {"constraint_name": "uq_orders_code", "constraint_type": "UNIQUE", "columns": ["order_code"]},
            {
                "constraint_name": "chk_orders_total",
                "constraint_type": "CHECK",
                "definition": "CHECK (total_cents >= 0)",
            },
        ]
    },
    "foreign_keys": [
        _fk_row("public", "orders", "customer_id", "customers", "id", "fk_orders_customer"),
        _fk_row("public", "order_items", "order_id", "orders", "id", "fk_order_items_order"),
    ],
    "indexes": {
        "orders": [
            {
                "schema_name": "public",
                "table_name": "orders",
                "index_name": "idx_orders_customer",
                "columns": ["customer_id"],
                "is_unique": False,
                "is_primary": False,
            }
        ]
    },
    "views": [{"schema_name": "public", "view_name": "active_orders", "definition": " SELECT orders.id FROM orders;"}],
    "dependencies": [
        {
            "schema_name": "public",
            "object_name": "active_orders",
            "object_type": "v",
            "referenced_schema": "public",
            "referenced_object": "orders",
            "referenced_object_type": "r",
        }
    ],
    "types": [
        {"schema_name": "public", "type_name": "order_status", "logical_type": "ENUM", "labels": ["new", "it's paid"]}
    ],
    "sequences": [
        {
            "schema_name": "public",
            "sequence_name": "Order Code Seq",
            "start_value": 100,
            "minimum_value": 1,
            "maximum_value": 9223372036854775807,
            "increment": 5,
            "cycles": True,
            "is_identity_owned": False,
            "is_extension_owned": False,
        },
        {
            "schema_name": "public",
            "sequence_name": "orders_id_seq",
            "start_value": 1,
            "minimum_value": 1,
            "maximum_value": 9223372036854775807,
            "increment": 1,
            "cycles": False,
            "is_identity_owned": True,
            "is_extension_owned": False,
        },
    ],
}

POSTGRES_CATALOG: dict[str, dict[str, Any]] = {
    "public": POSTGRES_PUBLIC,
    "Sales": _empty_schema(
        tables=["UserAccounts"],
        columns={
            "UserAccounts": [
                _column("Sales", "UserAccounts", "Id", "BIGINT", 1, is_primary=True),
                _column("Sales", "UserAccounts", "select", "TEXT", 2),
            ]
        },
    ),
}


class FakeCatalogSyncDriver:
    """Sync driver stub routing data-dictionary queries by their exact query-pack text."""

    def __init__(self, dialect: str = "postgres", catalog: "dict[str, dict[str, Any]] | None" = None) -> None:
        self.dialect = dialect
        self.catalog = POSTGRES_CATALOG if catalog is None else catalog
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        loader = get_data_dictionary_loader()
        self.routes: dict[str, tuple[str, str]] = {}
        for domain in _ROUTED_DOMAINS:
            for query_name in _ROUTED_QUERIES:
                text = loader.get_domain_query_text(dialect, domain, query_name)
                if text:
                    self.routes[text.strip()] = (domain, query_name)

    def select(self, statement: Any, **kwargs: Any) -> list[Any]:
        sql_text = str(getattr(statement, "raw_sql", statement)).strip()
        domain, query_name = self.routes[sql_text]
        self.calls.append((domain, query_name, kwargs))
        schema = self.catalog.get(str(kwargs.get("schema_name")), _empty_schema())
        table_name = kwargs.get("table_name")
        if domain == "tables":
            return [{"schema_name": kwargs.get("schema_name"), "table_name": name} for name in schema["tables"]]
        if domain in {"columns", "constraints", "indexes"}:
            if table_name is None:
                return [row for rows in schema[domain].values() for row in rows]
            return list(schema[domain].get(table_name, ()))
        if domain == "foreign_keys":
            return [row for row in schema["foreign_keys"] if table_name is None or row["table_name"] == table_name]
        if domain in {"views", "dependencies", "sequences", "types"}:
            return list(schema.get(domain, ()))
        if kwargs.get("object_type") == "view":
            return [
                {
                    "schema_name": kwargs.get("schema_name"),
                    "object_name": kwargs.get("object_name"),
                    "ddl": "CREATE VIEW public.active_orders AS SELECT orders.id FROM orders",
                    "fidelity": "native",
                }
            ]
        return [{"schema_name": kwargs.get("schema_name"), "object_name": kwargs.get("object_name"), "ddl": None}]


class FakeCatalogAsyncDriver:
    """Async driver stub delegating to FakeCatalogSyncDriver."""

    def __init__(self, dialect: str = "postgres", catalog: "dict[str, dict[str, Any]] | None" = None) -> None:
        self.sync = FakeCatalogSyncDriver(dialect=dialect, catalog=catalog)

    async def select(self, statement: Any, **kwargs: Any) -> list[Any]:
        return self.sync.select(statement, **kwargs)


EXPECTED_ORDERS_DDL = (
    'CREATE TABLE "public"."orders" (\n'
    '  "id" BIGINT NOT NULL,\n'
    '  "customer_id" BIGINT NOT NULL,\n'
    '  "order_code" VARCHAR(64) NOT NULL,\n'
    '  "total_cents" INT NOT NULL DEFAULT 0,\n'
    '  "status" order_status NOT NULL DEFAULT CAST(\'new\' AS "order_status"),\n'
    '  CONSTRAINT "pk_orders" PRIMARY KEY ("id"),\n'
    '  CONSTRAINT "uq_orders_code" UNIQUE ("order_code"),\n'
    '  CONSTRAINT "chk_orders_total" CHECK ("total_cents" >= 0),\n'
    '  CONSTRAINT "fk_orders_customer" FOREIGN KEY ("customer_id") REFERENCES "public"."customers"("id") '
    "ON DELETE CASCADE\n"
    ");\n\n"
    'CREATE INDEX "idx_orders_customer" ON "public"."orders"("customer_id");'
)


DB2_CUSTOMERS_DDL = "CREATE TABLE app.customers (\n  id BIGINT NOT NULL PRIMARY KEY\n);"


def _statements(result: MetadataResult) -> list[str]:
    items = [item for item in result.items if isinstance(item, DDLResult)]
    assert all(item.status == MetadataSupport.SUPPORTED and item.ddl for item in items)
    return [str(item.ddl) for item in items]


def test_postgres_sync_get_ddl_synthesizes_table_and_preserves_native_view() -> None:
    """PostgreSQL synthesizes table DDL when the catalog has none and keeps native view DDL."""
    driver = FakeCatalogSyncDriver()
    dictionary = PsycopgSyncDataDictionary()

    table_ddl = dictionary.get_ddl(cast("Any", driver), "orders", schema="public")
    assert table_ddl.status == MetadataSupport.SUPPORTED
    assert table_ddl.fidelity == MetadataFidelity.GENERATED
    assert table_ddl.source == MetadataSource.GENERATED
    assert table_ddl.ddl == EXPECTED_ORDERS_DDL

    view_ddl = dictionary.get_ddl(cast("Any", driver), "active_orders", schema="public", object_type="view")
    assert view_ddl.fidelity == MetadataFidelity.NATIVE
    assert view_ddl.ddl == "CREATE VIEW public.active_orders AS SELECT orders.id FROM orders"


@pytest.mark.anyio
@pytest.mark.parametrize("dictionary", [AsyncpgDataDictionary(), PsycopgAsyncDataDictionary(), PsqlpyDataDictionary()])
async def test_postgres_async_get_ddl_synthesizes_table_ddl(dictionary: Any) -> None:
    """Async PostgreSQL dictionaries synthesize the same table DDL as the sync dictionary."""
    table_ddl = await dictionary.get_ddl(cast("Any", FakeCatalogAsyncDriver()), "orders", schema="public")
    assert table_ddl.fidelity == MetadataFidelity.GENERATED
    assert table_ddl.ddl == EXPECTED_ORDERS_DDL


def test_postgres_foreign_keys_carry_referential_actions() -> None:
    """PostgreSQL foreign key metadata exposes ON DELETE / ON UPDATE actions from the catalog."""
    foreign_keys = PsycopgSyncDataDictionary().get_foreign_keys(
        cast("Any", FakeCatalogSyncDriver()), table="orders", schema="public"
    )
    assert foreign_keys == [
        ForeignKeyMetadata(
            table_name="orders",
            column_name="customer_id",
            referenced_table="customers",
            referenced_column="id",
            constraint_name="fk_orders_customer",
            schema="public",
            referenced_schema="public",
            on_delete="CASCADE",
            on_update="NO ACTION",
        )
    ]
    pg_text = get_data_dictionary_loader().get_domain_query_text("postgres", "foreign_keys", "by_schema") or ""
    assert "con.confdeltype" in pg_text
    assert "con.confupdtype" in pg_text


def _db2_column(table: str, name: str, position: int, *, primary: bool = False) -> dict[str, Any]:
    return {
        "schema_name": "APP",
        "table_name": table,
        "column_name": name,
        "data_type": "BIGINT",
        "is_nullable": 0,
        "ordinal_position": position,
        "is_primary": 1 if primary else 0,
    }


def _db2_fk(table: str, column: str, ref_table: str, name: str) -> dict[str, Any]:
    return {
        "schema_name": "APP",
        "table_name": table,
        "constraint_name": name,
        "column_name": column,
        "referenced_schema": "APP",
        "referenced_table": ref_table,
        "referenced_column": "ID",
    }


DB2_CATALOG: dict[str, dict[str, Any]] = {
    "APP": _empty_schema(
        tables=["ORDER_ITEMS", "ORDERS", "CUSTOMERS"],
        columns={
            "CUSTOMERS": [_db2_column("CUSTOMERS", "ID", 1, primary=True)],
            "ORDERS": [_db2_column("ORDERS", "ID", 1, primary=True), _db2_column("ORDERS", "CUSTOMER_ID", 2)],
            "ORDER_ITEMS": [
                _db2_column("ORDER_ITEMS", "ID", 1, primary=True),
                _db2_column("ORDER_ITEMS", "ORDER_ID", 2),
            ],
        },
        foreign_keys=[
            _db2_fk("ORDERS", "CUSTOMER_ID", "CUSTOMERS", "FK_ORDERS_CUSTOMER"),
            _db2_fk("ORDER_ITEMS", "ORDER_ID", "ORDERS", "FK_ORDER_ITEMS_ORDER"),
        ],
    )
}


def test_db2_and_adbc_synthesize_table_ddl() -> None:
    """Db2 and ADBC synthesize table DDL from catalog metadata when no native DDL source exists."""
    db2_result = Db2SyncDataDictionary().get_ddl(
        cast("Any", FakeCatalogSyncDriver(dialect="db2", catalog=DB2_CATALOG)), "customers", schema="app"
    )
    assert db2_result.fidelity == MetadataFidelity.GENERATED
    assert db2_result.identity.name == "CUSTOMERS"
    assert db2_result.ddl == DB2_CUSTOMERS_DDL

    adbc_driver = FakeCatalogSyncDriver(dialect="postgres")
    setattr(adbc_driver, "connection", object())
    adbc_result = AdbcDataDictionary().get_ddl(cast("Any", adbc_driver), "orders", schema="public", prefer_native=False)
    assert adbc_result.fidelity == MetadataFidelity.GENERATED
    assert adbc_result.ddl == (
        'CREATE TABLE "public"."orders" (\n'
        '  "id" BIGINT NOT NULL PRIMARY KEY,\n'
        '  "customer_id" BIGINT NOT NULL,\n'
        '  "order_code" VARCHAR(64) NOT NULL,\n'
        '  "total_cents" INT NOT NULL DEFAULT 0,\n'
        '  "status" order_status NOT NULL DEFAULT CAST(\'new\' AS "order_status"),\n'
        '  CONSTRAINT "fk_orders_customer" FOREIGN KEY ("customer_id") REFERENCES "public"."customers"("id") '
        "ON DELETE CASCADE\n"
        ");\n\n"
        'CREATE INDEX "idx_orders_customer" ON "public"."orders"("customer_id");'
    )


def test_sync_get_schema_ddl_orders_create_and_drop_with_sequences_and_views() -> None:
    """Synthesized schema DDL collects sequences, tables, and views and emits complete DROP statements."""
    dictionary = PsycopgSyncDataDictionary()

    create = dictionary.get_schema_ddl(
        cast("Any", FakeCatalogSyncDriver()), schema="public", prefer_native=False, exclude_tables=("ddl_migrations",)
    )
    names = [item.identity.name for item in create.items if isinstance(item, DDLResult)]
    assert names == ["order_status", "Order Code Seq", "customers", "orders", "order_items", "active_orders"]
    statements = _statements(create)
    assert statements[0] == "CREATE TYPE \"public\".\"order_status\" AS ENUM ('new', 'it''s paid');"
    assert statements[1] == (
        'CREATE SEQUENCE "public"."Order Code Seq" START WITH 100 INCREMENT BY 5 MINVALUE 1 '
        "MAXVALUE 9223372036854775807 CYCLE;"
    )
    assert statements[3] == EXPECTED_ORDERS_DDL
    assert statements[5] == 'CREATE VIEW "public"."active_orders" AS SELECT orders.id FROM orders;'
    orders = next(item for item in create.items if isinstance(item, DDLResult) and item.identity.name == "orders")
    assert {edge.to_object.name for edge in orders.dependencies} == {"order_status", "customers"}

    drop_driver = FakeCatalogSyncDriver()
    drop = dictionary.get_schema_ddl(
        cast("Any", drop_driver),
        schema="public",
        direction="drop",
        prefer_native=False,
        exclude_tables=('"public"."DDL_MIGRATIONS"',),
    )
    assert _statements(drop) == [
        'DROP VIEW IF EXISTS "public"."active_orders";',
        'DROP TABLE IF EXISTS "public"."order_items";',
        'DROP TABLE IF EXISTS "public"."orders";',
        'DROP TABLE IF EXISTS "public"."customers";',
        'DROP SEQUENCE IF EXISTS "public"."Order Code Seq";',
        'DROP TYPE IF EXISTS "public"."order_status";',
    ]
    assert not [domain for domain, _, kwargs in drop_driver.calls if domain == "columns" and kwargs["table_name"]]

    with_migrations = dictionary.get_schema_ddl(cast("Any", FakeCatalogSyncDriver()), schema="public")
    assert "ddl_migrations" in [item.identity.name for item in with_migrations.items if isinstance(item, DDLResult)]


def test_postgres_mixed_case_identifiers_keep_exact_catalog_names() -> None:
    """Catalog names that PostgreSQL would fold are looked up quoted and emitted with exact quoting."""
    dictionary = PsycopgSyncDataDictionary()

    create = dictionary.get_schema_ddl(cast("Any", FakeCatalogSyncDriver()), schema='"Sales"', prefer_native=False)
    assert _statements(create) == [
        'CREATE TABLE "Sales"."UserAccounts" (\n  "Id" BIGINT NOT NULL PRIMARY KEY,\n  "select" TEXT NOT NULL\n);'
    ]

    drop = dictionary.get_schema_ddl(cast("Any", FakeCatalogSyncDriver()), schema='"Sales"', direction="drop")
    assert _statements(drop) == ['DROP TABLE IF EXISTS "Sales"."UserAccounts";']

    driver = FakeCatalogSyncDriver()
    single = dictionary.get_object_ddl(cast("Any", driver), '"UserAccounts"', schema='"Sales"')
    assert single.identity.name == "UserAccounts"
    assert single.identity.schema == "Sales"
    column_lookups = [
        (kwargs["schema_name"], kwargs["table_name"]) for domain, _, kwargs in driver.calls if domain == "columns"
    ]
    assert column_lookups == [("Sales", "UserAccounts")]


@pytest.mark.anyio
@pytest.mark.parametrize("dictionary", [AsyncpgDataDictionary(), PsqlpyDataDictionary()])
async def test_async_get_schema_ddl_topological_ordering_and_cross_dialect(dictionary: Any) -> None:
    """Async schema DDL orders tables by foreign keys and renders the requested target dialect."""
    tables_only = {**POSTGRES_PUBLIC, "views": [], "dependencies": [], "sequences": [], "types": []}
    driver = FakeCatalogAsyncDriver(catalog={"public": tables_only})

    create = await dictionary.get_schema_ddl(
        cast("Any", driver), schema="public", target_dialect="duckdb", exclude_tables=("ddl_migrations",)
    )
    items = [item for item in create.items if isinstance(item, DDLResult)]
    assert [item.identity.name for item in items] == ["customers", "orders", "order_items"]
    assert all(item.identity.dialect == "duckdb" for item in items)

    drop = await dictionary.get_schema_ddl(
        cast("Any", driver), schema="public", direction="drop", exclude_tables=("ddl_migrations",)
    )
    assert _statements(drop) == [
        'DROP TABLE IF EXISTS "public"."order_items";',
        'DROP TABLE IF EXISTS "public"."orders";',
        'DROP TABLE IF EXISTS "public"."customers";',
    ]


@pytest.mark.anyio
async def test_db2_async_schema_ddl_orders_tables_and_drops_them() -> None:
    """Db2 async schema DDL synthesizes tables in foreign-key order and drops them in reverse."""
    dictionary = Db2AsyncDataDictionary()
    driver = FakeCatalogAsyncDriver(dialect="db2", catalog=DB2_CATALOG)

    create = await dictionary.get_schema_ddl(cast("Any", driver), schema="APP", exclude_domains=("sequences",))
    assert [item.identity.name for item in create.items if isinstance(item, DDLResult)] == [
        "CUSTOMERS",
        "ORDERS",
        "ORDER_ITEMS",
    ]
    assert _statements(create)[0] == DB2_CUSTOMERS_DDL

    drop = await dictionary.get_schema_ddl(cast("Any", driver), schema="APP", direction="drop")
    assert _statements(drop) == [
        'DROP TABLE IF EXISTS "APP"."ORDER_ITEMS";',
        'DROP TABLE IF EXISTS "APP"."ORDERS";',
        'DROP TABLE IF EXISTS "APP"."CUSTOMERS";',
    ]


def test_schema_ddl_reports_domains_the_dictionary_cannot_collect() -> None:
    """Domains without data-dictionary support are reported as warnings instead of silently omitted."""
    result = Db2SyncDataDictionary().get_schema_ddl(
        cast("Any", FakeCatalogSyncDriver(dialect="db2", catalog={"APP": _empty_schema()})),
        schema="APP",
        prefer_native=False,
    )
    assert result.items == ()
    assert result.warnings == (
        "db2 data dictionary does not expose types; types were omitted from schema DDL",
        "db2 data dictionary does not expose sequences; sequences were omitted from schema DDL",
    )


def test_table_profile_sql_quotes_exact_identifiers() -> None:
    """Profiling and sampling SQL quote reserved and mixed-case identifiers."""
    dictionary = PsycopgSyncDataDictionary()
    profile_sql = dictionary._build_table_profile_sql("UserAccounts", [{"column_name": "select"}], schema="Public")
    sample_sql = dictionary._build_table_sample_sql("UserAccounts", schema="Public", sample_size=5)
    assert profile_sql.raw_sql == (
        'SELECT COUNT(*) AS __row_count, COUNT("select") AS __non_null_0, COUNT(DISTINCT "select") AS __distinct_0, '
        'MAX(LENGTH(CAST("select" AS VARCHAR))) AS __max_len_0 FROM "Public"."UserAccounts"'
    )
    assert sample_sql.raw_sql == 'SELECT * FROM "Public"."UserAccounts" LIMIT 5'


def test_table_profile_executes_with_reserved_and_mixed_case_identifiers() -> None:
    """SQLite profiling runs against tables and columns that require quoting."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as session:
            session.execute_script(
                'CREATE TABLE "User Accounts" ("select" TEXT, "Order" INTEGER);'
                "INSERT INTO \"User Accounts\" VALUES ('a', 1), ('b', 2), ('b', NULL);"
            )
            profiles = session.data_dictionary.get_table_profile(session, '"User Accounts"')
    finally:
        config.close_pool()

    by_name = {profile.column_name: profile for profile in profiles}
    assert by_name["select"].row_count == 3
    assert by_name["select"].distinct_count == 2
    assert by_name["Order"].null_count == 1


def test_table_profile_propagates_query_errors() -> None:
    """Profiling surfaces database errors instead of returning empty statistics."""

    class DeniedDriver(FakeCatalogSyncDriver):
        def select(self, statement: Any, **kwargs: Any) -> list[Any]:
            sql_text = str(getattr(statement, "raw_sql", statement)).strip()
            if sql_text.startswith("SELECT COUNT(*)"):
                msg = "permission denied for table orders"
                raise PermissionError(msg)
            return super().select(statement, **kwargs)

    with pytest.raises(PermissionError, match="permission denied"):
        PsycopgSyncDataDictionary().get_table_profile(cast("Any", DeniedDriver()), "orders", schema="public")


class _StubMariaDBDriver:
    """Stub driver exposing engine_family for MariaDB/MySQL resolution checks."""

    def __init__(self, engine_family: str) -> None:
        self.engine_family = engine_family
        self.dialect = "mysql"


def test_mysql_data_dictionary_resolve_ddl_dialect_returns_mariadb_when_engine_family_is_mariadb() -> None:
    """MySQL data dictionaries return 'mariadb' from resolve_ddl_dialect when engine_family is mariadb."""
    dictionary = PyMysqlDataDictionary()
    assert dictionary.resolve_ddl_dialect(_StubMariaDBDriver("mariadb")) == "mariadb"
    assert dictionary.resolve_ddl_dialect(_StubMariaDBDriver("mysql")) == "mysql"


def test_duckdb_native_ddl_restricts_create_type_to_enum_and_executes(tmp_path: Path) -> None:
    """DuckDB native DDL queries emit CREATE TYPE only for ENUM types with non-null SQL and replay cleanly."""
    with DuckDBConfig(connection_config={"database": str(tmp_path / "source.duckdb")}).provide_session() as source:
        source.execute_script(
            "CREATE TYPE status_enum AS ENUM ('pending', 'paid'); "
            "CREATE TYPE custom_int AS INTEGER; "
            "CREATE TABLE invoices (id INTEGER PRIMARY KEY, status status_enum);"
        )
        schema_ddl = source.data_dictionary.get_schema_ddl(source, prefer_native=True)
        alias_ddl = source.data_dictionary.get_ddl(source, "custom_int", object_type="type")

    assert alias_ddl.status == MetadataSupport.UNSUPPORTED
    type_items = [
        item for item in schema_ddl.items if isinstance(item, DDLResult) and item.identity.object_type == "type"
    ]
    assert [item.identity.name for item in type_items] == ["status_enum"]
    assert type_items[0].ddl == "CREATE TYPE \"status_enum\" AS ENUM ('pending', 'paid');"

    with DuckDBConfig(connection_config={"database": str(tmp_path / "replay.duckdb")}).provide_session() as replay:
        for item in schema_ddl.items:
            if isinstance(item, DDLResult) and item.ddl:
                replay.execute_script(item.ddl)
        replay.execute_script("INSERT INTO invoices VALUES (1, 'paid')")
        assert replay.select_value("SELECT status FROM invoices WHERE id = 1") == "paid"


def test_ddl_and_sequence_queries_use_upfront_ctes_and_left_joins() -> None:
    """DuckDB, Postgres, and MSSQL DDL/sequence catalog queries use upfront CTEs with LEFT JOINs."""
    loader = get_data_dictionary_loader()
    duck_by_object = loader.get_domain_query_text("duckdb", "ddl", "by_object") or ""
    duck_by_schema = loader.get_domain_query_text("duckdb", "ddl", "by_schema") or ""
    pg_ddl = loader.get_domain_query_text("postgres", "ddl", "by_object") or ""
    pg_sequences = loader.get_domain_query_text("postgres", "sequences", "by_schema") or ""
    mssql_indexes = loader.get_domain_query_text("mssql", "ddl", "index_inputs_by_table") or ""
    mssql_tables = loader.get_domain_query_text("mssql", "ddl", "table_inputs_by_table") or ""

    for query_text in (duck_by_object, duck_by_schema, pg_ddl, pg_sequences, mssql_indexes, mssql_tables):
        assert "WITH " in query_text
        assert "LEFT JOIN " in query_text
        assert "EXISTS (" not in query_text


def test_sqlite_schema_ddl_synthesis_replays_multi_column_fks_in_memory() -> None:
    """SQLite synthesized schema DDL groups composite foreign keys with synthetic names and executes in memory."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as session:
            session.execute_script(
                "CREATE TABLE accounts (tenant_id INTEGER, account_id INTEGER, PRIMARY KEY (tenant_id, account_id));"
                "CREATE TABLE entries ("
                "  id INTEGER PRIMARY KEY,"
                "  tenant_id INTEGER NOT NULL,"
                "  account_id INTEGER NOT NULL,"
                "  amount NUMERIC(12, 2) NOT NULL,"
                "  FOREIGN KEY (tenant_id, account_id) REFERENCES accounts(tenant_id, account_id) ON DELETE CASCADE"
                ");"
            )
            schema_ddl = session.data_dictionary.get_schema_ddl(session, prefer_native=False)
    finally:
        config.close_pool()

    statements = _statements(schema_ddl)
    assert len(statements) == 2
    assert "entries_fk_0" in statements[1]
    assert 'FOREIGN KEY ("tenant_id", "account_id") REFERENCES "accounts"("tenant_id", "account_id")' in statements[1]

    conn = sqlite3.connect(":memory:")
    for stmt in statements:
        conn.executescript(stmt)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("INSERT INTO accounts VALUES (1, 42)")
    conn.execute("INSERT INTO entries VALUES (1, 1, 42, 99.95)")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO entries VALUES (2, 1, 404, 10.00)")
