"""Unit tests for shared builder-backed table DDL synthesis and topological schema DDL."""

from typing import Any, cast

import pytest

from sqlspec.adapters.adbc.data_dictionary import AdbcDataDictionary
from sqlspec.adapters.asyncpg.data_dictionary import AsyncpgDataDictionary
from sqlspec.adapters.db2.data_dictionary import Db2AsyncDataDictionary, Db2SyncDataDictionary
from sqlspec.adapters.psqlpy.data_dictionary import PsqlpyDataDictionary
from sqlspec.adapters.psycopg.data_dictionary import PsycopgAsyncDataDictionary, PsycopgSyncDataDictionary
from sqlspec.data_dictionary import DDLResult, ForeignKeyMetadata, MetadataFidelity, MetadataSource, MetadataSupport
from sqlspec.data_dictionary.dialects.mssql.config import build_mssql_table_ddl_result


def _sample_orders_columns(schema: str = "public") -> list[dict[str, Any]]:
    return [
        {
            "schema_name": schema,
            "table_name": "orders",
            "column_name": "id",
            "data_type": "BIGINT",
            "is_nullable": "NO",
            "ordinal_position": 1,
            "is_primary": True,
        },
        {
            "schema_name": schema,
            "table_name": "orders",
            "column_name": "customer_id",
            "data_type": "BIGINT",
            "is_nullable": "NO",
            "ordinal_position": 2,
        },
        {
            "schema_name": schema,
            "table_name": "orders",
            "column_name": "order_code",
            "data_type": "VARCHAR",
            "max_length": 64,
            "is_nullable": "NO",
            "ordinal_position": 3,
        },
        {
            "schema_name": schema,
            "table_name": "orders",
            "column_name": "total_cents",
            "data_type": "INTEGER",
            "is_nullable": "NO",
            "column_default": "0",
            "ordinal_position": 4,
        },
    ]


def _sample_orders_constraints(schema: str = "public") -> list[dict[str, Any]]:
    return [
        {
            "schema_name": schema,
            "table_name": "orders",
            "constraint_name": "pk_orders",
            "constraint_type": "PRIMARY KEY",
            "columns": ["id"],
        },
        {
            "schema_name": schema,
            "table_name": "orders",
            "constraint_name": "uq_orders_code",
            "constraint_type": "UNIQUE",
            "columns": ["order_code"],
        },
        {
            "schema_name": schema,
            "table_name": "orders",
            "constraint_name": "chk_orders_total",
            "constraint_type": "CHECK",
            "definition": "CHECK (total_cents >= 0)",
        },
    ]


def _sample_orders_fks(schema: str = "public") -> list[ForeignKeyMetadata]:
    return [
        ForeignKeyMetadata(
            table_name="orders",
            column_name="customer_id",
            referenced_table="customers",
            referenced_column="id",
            constraint_name="fk_orders_customer",
            schema=schema,
            referenced_schema=schema,
        )
    ]


def _sample_orders_indexes(schema: str = "public") -> list[dict[str, Any]]:
    return [
        {
            "schema_name": schema,
            "table_name": "orders",
            "index_name": "idx_orders_customer",
            "columns": ["customer_id"],
            "is_unique": False,
            "is_primary": False,
        }
    ]


class FakeCatalogSyncDriver:
    """Sync driver stub serving multi-table schema metadata for synthesis and topological sorting."""

    def __init__(self, dialect: str = "postgres") -> None:
        self.dialect = dialect

    def select(self, statement: Any, **kwargs: Any) -> list[Any]:
        sql_text = getattr(statement, "raw_sql", str(statement))
        schema = str(kwargs.get("schema_name") or "public")
        table_name = kwargs.get("table_name")
        object_name = kwargs.get("object_name")
        object_type = kwargs.get("object_type")

        if object_type == "view" or (object_name == "active_orders" and "pg_get_viewdef" in sql_text):
            return [
                {
                    "schema_name": schema,
                    "object_name": object_name or "active_orders",
                    "ddl": f"CREATE VIEW {schema}.active_orders AS SELECT id FROM {schema}.orders",
                    "fidelity": "native",
                }
            ]

        if "pg_get_viewdef" in sql_text and object_type == "table":
            return [{"schema_name": schema, "object_name": object_name, "ddl": None, "fidelity": "unsupported"}]

        if (
            "information_schema.columns" in sql_text
            or "a.attname::text AS column_name" in sql_text
            or "FROM SYSCAT.COLUMNS" in sql_text
        ):
            t_lower = str(table_name or "").lower()
            if t_lower == "orders":
                return _sample_orders_columns(schema)
            if t_lower == "customers":
                return [
                    {
                        "schema_name": schema,
                        "table_name": "customers",
                        "column_name": "id",
                        "data_type": "BIGINT",
                        "is_nullable": "NO",
                        "ordinal_position": 1,
                        "is_primary": True,
                    },
                    {
                        "schema_name": schema,
                        "table_name": "customers",
                        "column_name": "email",
                        "data_type": "VARCHAR",
                        "max_length": 255,
                        "is_nullable": "NO",
                        "ordinal_position": 2,
                    },
                ]
            if t_lower == "order_items":
                return [
                    {
                        "schema_name": schema,
                        "table_name": "order_items",
                        "column_name": "id",
                        "data_type": "BIGINT",
                        "is_nullable": "NO",
                        "ordinal_position": 1,
                        "is_primary": True,
                    },
                    {
                        "schema_name": schema,
                        "table_name": "order_items",
                        "column_name": "order_id",
                        "data_type": "BIGINT",
                        "is_nullable": "NO",
                        "ordinal_position": 2,
                    },
                ]
            if t_lower == "ddl_migrations":
                return [
                    {
                        "schema_name": schema,
                        "table_name": "ddl_migrations",
                        "column_name": "version_num",
                        "data_type": "VARCHAR",
                        "max_length": 64,
                        "is_nullable": "NO",
                        "ordinal_position": 1,
                        "is_primary": True,
                    }
                ]
            return []

        if "pg_get_constraintdef" in sql_text or "FROM SYSCAT.TABCONST" in sql_text:
            if table_name is not None and str(table_name).lower() == "orders":
                return _sample_orders_constraints(schema)
            return []

        if (
            "referenced_table_name" in sql_text
            or "FROM SYSCAT.REFERENCES" in sql_text
            or ("FOREIGN KEY" in sql_text and "pg_get_constraintdef" not in sql_text)
        ):
            all_fks = [
                ForeignKeyMetadata(
                    table_name="orders",
                    column_name="customer_id",
                    referenced_table="customers",
                    referenced_column="id",
                    constraint_name="fk_orders_customer",
                    schema=schema,
                    referenced_schema=schema,
                ),
                ForeignKeyMetadata(
                    table_name="order_items",
                    column_name="order_id",
                    referenced_table="orders",
                    referenced_column="id",
                    constraint_name="fk_order_items_order",
                    schema=schema,
                    referenced_schema=schema,
                ),
            ]
            if "SYSCAT.REFERENCES" in sql_text:
                return [
                    {
                        "schema_name": fk.schema,
                        "table_name": fk.table_name,
                        "constraint_name": fk.constraint_name,
                        "column_name": fk.column_name,
                        "referenced_schema": fk.referenced_schema,
                        "referenced_table": fk.referenced_table,
                        "referenced_column": fk.referenced_column,
                    }
                    for fk in all_fks
                    if table_name is None or fk.table_name.lower() == str(table_name).lower()
                ]
            if table_name is not None:
                return [fk for fk in all_fks if fk.table_name.lower() == str(table_name).lower()]
            return all_fks

        if "pg_index" in sql_text or "FROM SYSCAT.INDEXES" in sql_text:
            if table_name is not None and str(table_name).lower() == "orders":
                if "SYSCAT.INDEXES" in sql_text:
                    return [
                        {
                            "schema_name": schema,
                            "table_name": "orders",
                            "index_name": "idx_orders_customer",
                            "column_name": "customer_id",
                            "is_unique": 0,
                            "is_primary": 0,
                        }
                    ]
                return _sample_orders_indexes(schema)
            return []

        if "pg_views" in sql_text or "SYSCAT.VIEWS" in sql_text:
            return []

        if "pg_depend" in sql_text:
            return []

        return [
            {"schema_name": schema, "table_name": "order_items"},
            {"schema_name": schema, "table_name": "orders"},
            {"schema_name": schema, "table_name": "customers"},
            {"schema_name": schema, "table_name": "ddl_migrations"},
        ]


class FakeCatalogAsyncDriver:
    """Async driver stub delegating to FakeCatalogSyncDriver."""

    def __init__(self, dialect: str = "postgres") -> None:
        self._sync = FakeCatalogSyncDriver(dialect=dialect)

    async def select(self, statement: Any, **kwargs: Any) -> list[Any]:
        return self._sync.select(statement, **kwargs)


def test_postgres_sync_get_ddl_synthesizes_table_and_preserves_native_view() -> None:
    """PsycopgSyncDataDictionary should synthesize table DDL and preserve native view DDL."""
    driver = FakeCatalogSyncDriver()
    dictionary = PsycopgSyncDataDictionary()

    table_ddl = dictionary.get_ddl(cast("Any", driver), "orders", schema="public", object_type="table")
    assert isinstance(table_ddl, DDLResult)
    assert table_ddl.status == MetadataSupport.SUPPORTED
    assert table_ddl.fidelity == MetadataFidelity.GENERATED
    assert table_ddl.source == MetadataSource.GENERATED
    assert table_ddl.ddl is not None
    assert 'CREATE TABLE "public"."orders"' in table_ddl.ddl
    assert 'CONSTRAINT "pk_orders" PRIMARY KEY ("id")' in table_ddl.ddl
    assert 'CONSTRAINT "uq_orders_code" UNIQUE ("order_code")' in table_ddl.ddl
    assert 'CONSTRAINT "chk_orders_total" CHECK ("total_cents" >= 0)' in table_ddl.ddl
    assert 'CONSTRAINT "fk_orders_customer" FOREIGN KEY ("customer_id") REFERENCES "customers"("id")' in table_ddl.ddl
    assert 'CREATE INDEX "idx_orders_customer" ON "public"."orders"' in table_ddl.ddl

    view_ddl = dictionary.get_ddl(cast("Any", driver), "active_orders", schema="public", object_type="view")
    assert view_ddl.status == MetadataSupport.SUPPORTED
    assert view_ddl.fidelity == MetadataFidelity.NATIVE
    assert view_ddl.ddl is not None
    assert "CREATE VIEW public.active_orders" in view_ddl.ddl


@pytest.mark.parametrize("dictionary", [AsyncpgDataDictionary(), PsycopgAsyncDataDictionary(), PsqlpyDataDictionary()])
async def test_postgres_async_get_ddl_synthesizes_table_ddl(dictionary: Any) -> None:
    """Async PostgreSQL data dictionaries should synthesize table DDL when catalog ddl is None."""
    driver = FakeCatalogAsyncDriver()

    table_ddl = await dictionary.get_ddl(cast("Any", driver), "orders", schema="public", object_type="table")
    assert table_ddl.status == MetadataSupport.SUPPORTED
    assert table_ddl.fidelity == MetadataFidelity.GENERATED
    assert table_ddl.source == MetadataSource.GENERATED
    assert table_ddl.ddl is not None
    assert 'CREATE TABLE "public"."orders"' in table_ddl.ddl
    assert 'CONSTRAINT "fk_orders_customer" FOREIGN KEY ("customer_id") REFERENCES "customers"("id")' in table_ddl.ddl
    assert 'CREATE INDEX "idx_orders_customer" ON "public"."orders"' in table_ddl.ddl


def test_db2_and_adbc_synthesize_table_ddl() -> None:
    """Db2SyncDataDictionary and AdbcDataDictionary should synthesize table DDL via shared builder helpers."""
    db2_driver = FakeCatalogSyncDriver(dialect="db2")
    db2_dict = Db2SyncDataDictionary()

    db2_result = db2_dict.get_ddl(cast("Any", db2_driver), "orders", schema="APP", object_type="table")
    assert db2_result.status == MetadataSupport.SUPPORTED
    assert db2_result.fidelity == MetadataFidelity.GENERATED
    assert db2_result.ddl is not None
    assert "CREATE TABLE" in db2_result.ddl
    assert "orders" in db2_result.ddl.lower()
    assert "idx_orders_customer" in db2_result.ddl.lower()

    adbc_dict = AdbcDataDictionary()
    adbc_driver = FakeCatalogSyncDriver(dialect="postgres")
    setattr(adbc_driver, "connection", object())
    adbc_result = adbc_dict.get_ddl(
        cast("Any", adbc_driver), "orders", schema="public", object_type="table", prefer_native=False
    )
    assert adbc_result.status == MetadataSupport.SUPPORTED
    assert adbc_result.fidelity == MetadataFidelity.GENERATED
    assert adbc_result.ddl is not None
    assert "CREATE TABLE" in adbc_result.ddl


def test_sync_get_schema_ddl_topological_ordering_and_exclude_tables() -> None:
    """Sync get_schema_ddl should exclude ddl_migrations and order tables topologically for create and drop."""
    driver = FakeCatalogSyncDriver()
    dictionary = PsycopgSyncDataDictionary()

    create_schema = dictionary.get_schema_ddl(cast("Any", driver), schema="public", direction="create")
    assert create_schema.capability.support == MetadataSupport.SUPPORTED
    create_items = [cast("DDLResult", item) for item in create_schema.items]
    create_names = [item.identity.name for item in create_items]
    assert "ddl_migrations" not in create_names
    assert create_names == ["customers", "orders", "order_items"]
    assert all(item.ddl is not None and "CREATE TABLE" in item.ddl for item in create_items)

    drop_schema = dictionary.get_schema_ddl(cast("Any", driver), schema="public", direction="drop")
    drop_items = [cast("DDLResult", item) for item in drop_schema.items]
    drop_names = [item.identity.name for item in drop_items]
    assert "ddl_migrations" not in drop_names
    assert drop_names == ["order_items", "orders", "customers"]
    assert all(item.ddl is not None and "DROP TABLE" in item.ddl for item in drop_items)

    with_migrations = dictionary.get_schema_ddl(cast("Any", driver), schema="public", exclude_tables=())
    all_names = [cast("DDLResult", item).identity.name for item in with_migrations.items]
    assert "ddl_migrations" in all_names


@pytest.mark.parametrize("dictionary", [AsyncpgDataDictionary(), Db2AsyncDataDictionary()])
async def test_async_get_schema_ddl_topological_ordering_and_cross_dialect(dictionary: Any) -> None:
    """Async get_schema_ddl should sort tables topologically and support target_dialect cross-compilation."""
    driver = FakeCatalogAsyncDriver()

    create_schema = await dictionary.get_schema_ddl(
        cast("Any", driver), schema="public", direction="create", target_dialect="duckdb"
    )
    assert create_schema.capability.support == MetadataSupport.SUPPORTED
    create_items = [cast("DDLResult", item) for item in create_schema.items]
    create_names = [item.identity.name.lower() for item in create_items]
    assert "ddl_migrations" not in create_names
    assert create_names == ["customers", "orders", "order_items"]
    assert all(item.identity.dialect == "duckdb" for item in create_items)

    drop_schema = await dictionary.get_schema_ddl(cast("Any", driver), schema="public", direction="drop")
    drop_names = [cast("DDLResult", item).identity.name.lower() for item in drop_schema.items]
    assert drop_names == ["order_items", "orders", "customers"]


def test_build_mssql_table_ddl_result_with_constraints_and_foreign_keys() -> None:
    """build_mssql_table_ddl_result should delegate to sqlspec.builder with constraints and foreign keys."""
    column_rows = [
        {
            "schema_name": "dbo",
            "table_name": "orders",
            "column_name": "id",
            "data_type": "int",
            "max_length": 4,
            "numeric_precision": 10,
            "numeric_scale": 0,
            "is_nullable": False,
            "ordinal_position": 1,
            "is_identity": True,
            "identity_seed": 1,
            "identity_increment": 1,
            "is_computed": False,
            "computed_definition": None,
            "column_default": None,
            "primary_key_name": "pk_orders",
            "primary_key_ordinal": 1,
        },
        {
            "schema_name": "dbo",
            "table_name": "orders",
            "column_name": "customer_id",
            "data_type": "int",
            "is_nullable": False,
            "ordinal_position": 2,
        },
        {
            "schema_name": "dbo",
            "table_name": "orders",
            "column_name": "total",
            "data_type": "decimal",
            "numeric_precision": 10,
            "numeric_scale": 2,
            "is_nullable": False,
            "ordinal_position": 3,
        },
    ]
    constraint_rows = [
        {"constraint_name": "chk_orders_total", "constraint_type": "CHECK_CONSTRAINT", "definition": "([total]>=(0))"}
    ]
    foreign_key_rows = [
        ForeignKeyMetadata(
            table_name="orders",
            column_name="customer_id",
            referenced_table="customers",
            referenced_column="id",
            constraint_name="fk_orders_customer",
            schema="dbo",
            referenced_schema="dbo",
        )
    ]
    index_rows = [
        {"index_name": "idx_orders_customer", "columns": "customer_id", "is_unique": False, "type_desc": "NONCLUSTERED"}
    ]

    result = build_mssql_table_ddl_result(
        "dbo", "orders", column_rows, index_rows, constraint_rows=constraint_rows, foreign_key_rows=foreign_key_rows
    )

    assert result.status == MetadataSupport.SUPPORTED
    assert result.fidelity == MetadataFidelity.GENERATED
    assert result.source == MetadataSource.GENERATED
    assert result.ddl is not None
    assert "CREATE TABLE [dbo].[orders]" in result.ddl
    assert "[id] int IDENTITY(1,1) NOT NULL" in result.ddl
    assert "CONSTRAINT [pk_orders] PRIMARY KEY ([id])" in result.ddl
    assert "CONSTRAINT [chk_orders_total] CHECK" in result.ddl
    assert "CONSTRAINT [fk_orders_customer] FOREIGN KEY ([customer_id]) REFERENCES [customers]([id])" in result.ddl
    assert "CREATE INDEX [idx_orders_customer] ON [dbo].[orders]([customer_id])" in result.ddl
