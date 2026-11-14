"""Unit tests for without_indexes context manager and drop_indexes bulk load orchestration."""

import asyncio
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.data_dictionary._index_ddl import (
    AsyncWithoutIndexesContext,
    SyncWithoutIndexesContext,
    _split_schema_and_table,
    build_create_index_sql,
    build_drop_index_sql,
    filter_droppable_table_indexes,
)


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


@pytest.mark.parametrize("dialect", ["sybase", "sqlanywhere"])
@pytest.mark.parametrize(
    ("table", "schema", "qualified"),
    [
        ("orders", None, '"orders"'),
        ("orders", "audit", '"audit"."orders"'),
        ("audit.orders", None, '"audit"."orders"'),
        ("[audit].[order.items]", None, '"audit"."order.items"'),
        ('"audit"."order.items"', None, '"audit"."order.items"'),
    ],
)
def test_sap_indexes_quote_owner_table_and_index_separately(
    dialect: str, table: str, schema: str | None, qualified: str
) -> None:
    metadata = {"index_name": 'idx"value', "columns": ["value"]}
    assert build_drop_index_sql(dialect, metadata, table, schema) == f'DROP INDEX {qualified}."idx""value"'
    assert build_create_index_sql(dialect, metadata, table, schema=schema) == (
        f'CREATE INDEX "idx""value" ON {qualified} ("value")'
    )


def test_index_ddl_edge_cases_and_constraint_matching() -> None:
    """Index helpers skip indexless dialects, constraint-matched names, and empty definitions."""
    indexes: list[dict[str, Any]] = [
        {"index_name": ""},
        {"index_name": "sqlite_autoindex_users_1", "columns": ["id"]},
        {"index_name": "pk_users", "columns": ["id"], "constraint_type": "PRIMARY KEY"},
        {"index_name": "uq_email_constraint", "columns": ["email"]},
        {"index_name": "idx_users_status", "columns": "status, updated_at"},
    ]
    constraints = [{"constraint_name": "uq_email_constraint"}]
    droppable = filter_droppable_table_indexes(indexes, constraints)
    assert [row["index_name"] for row in droppable] == ["idx_users_status"]

    assert build_drop_index_sql("bigquery", {"index_name": "idx_x"}, "users") is None
    assert build_drop_index_sql("snowflake", {"index_name": "idx_x"}, "users") is None
    assert build_drop_index_sql("postgres", {"index_name": ""}, "users") is None
    assert build_drop_index_sql("spanner", {"index_name": "idx_x"}, "users") == "DROP INDEX `idx_x`"
    assert build_create_index_sql("bigquery", {"index_name": "idx_x", "columns": ["a"]}, "users") is None
    assert build_create_index_sql("snowflake", {"index_name": "idx_x", "columns": ["a"]}, "users") is None
    assert build_create_index_sql("postgres", {"index_name": "idx_x", "columns": []}, "users") is None
    unique_concurrent = build_create_index_sql(
        "postgres", {"index_name": "idx_u", "columns": "email, tenant_id", "is_unique": True}, "users", concurrent=True
    )
    assert unique_concurrent == 'CREATE UNIQUE INDEX CONCURRENTLY "idx_u" ON "users" ("email", "tenant_id")'


@pytest.mark.anyio
async def test_without_indexes_ddl_override_transaction_and_rebuild_error() -> None:
    """Sync and async contexts honor get_ddl overrides, strip CONCURRENTLY inside transactions, and surface rebuild errors."""
    executed_sync: list[str] = []
    in_tx = False
    fail_rebuild = False

    class _SyncDD:
        def get_indexes(self, _driver: Any, *, table: str, schema: str | None) -> tuple[dict[str, Any], ...]:
            assert (schema, table) == ("public", "orders")
            return ({"index_name": "idx_custom", "columns": ["customer_id"]},)

        def get_constraints(self, _driver: Any, *, table: str, schema: str | None) -> list[dict[str, Any]]:
            return []

        def get_ddl(self, _driver: Any, name: str, *, schema: str | None, object_type: str) -> list[dict[str, str]]:
            assert (name, schema, object_type) == ("idx_custom", "public", "INDEX")
            return [{"ddl": "CREATE INDEX CONCURRENTLY idx_custom ON public.orders (customer_id)"}]

    class _SyncDriver:
        dialect = "postgres"
        data_dictionary = _SyncDD()

        def is_in_transaction(self) -> bool:
            return in_tx

        def execute(self, sql: str) -> None:
            if fail_rebuild and sql.startswith("CREATE "):
                raise RuntimeError("rebuild failed")
            executed_sync.append(sql)

    driver = _SyncDriver()
    with SyncWithoutIndexesContext(driver, "public.orders", concurrent=True):
        in_tx = True
    assert executed_sync == [
        'DROP INDEX IF EXISTS "public"."idx_custom"',
        "CREATE INDEX idx_custom ON public.orders (customer_id)",
    ]

    in_tx = False
    fail_rebuild = True
    with pytest.raises(RuntimeError, match="rebuild failed"):
        with SyncWithoutIndexesContext(driver, "orders", schema="public"):
            pass

    executed_async: list[str] = []

    class _AsyncDD:
        async def get_indexes(self, _driver: Any, *, table: str, schema: str | None) -> tuple[dict[str, Any], ...]:
            return ({"index_name": "idx_async_custom", "columns": ["customer_id"]},)

        async def get_constraints(self, _driver: Any, *, table: str, schema: str | None) -> list[dict[str, Any]]:
            return []

        async def get_ddl(self, _driver: Any, name: str, *, schema: str | None, object_type: str) -> str:
            return "CREATE INDEX CONCURRENTLY idx_async_custom ON public.orders (customer_id)"

    class _AsyncDriver:
        dialect = "postgres"
        data_dictionary = _AsyncDD()

        def is_in_transaction(self) -> bool:
            return True

        async def execute(self, sql: str) -> None:
            executed_async.append(sql)

    async with AsyncWithoutIndexesContext(_AsyncDriver(), "public.orders", concurrent=True):
        pass
    assert executed_async == [
        'DROP INDEX IF EXISTS "public"."idx_async_custom"',
        "CREATE INDEX idx_async_custom ON public.orders (customer_id)",
    ]


def test_teradata_without_indexes_keeps_primary_index_and_addresses_unnamed_index_by_columns() -> None:
    """Teradata primary indexes stay in place and unnamed secondary indexes are dropped and rebuilt by columns."""
    from sqlspec.adapters.teradata.data_dictionary import build_index_metadata

    rows = [
        {"schema_name": "mart", "table_name": "orders", "index_name": "PI_orders_1", "column_name": "id"}
        | {"is_unique": 0, "is_primary": 1, "catalog_name_generated": 1},
        {"schema_name": "mart", "table_name": "orders", "index_name": "IDX_orders_4", "column_name": "region"}
        | {"is_unique": 0, "is_primary": 0, "catalog_name_generated": 1},
        {"schema_name": "mart", "table_name": "orders", "index_name": "IDX_orders_4", "column_name": "status"}
        | {"is_unique": 0, "is_primary": 0, "catalog_name_generated": 1},
        {"schema_name": "mart", "table_name": "orders", "index_name": "idx_orders_customer", "column_name": "cust"}
        | {"is_unique": 1, "is_primary": 0, "catalog_name_generated": 0},
    ]
    executed: list[str] = []

    class _DD:
        def get_indexes(self, _driver: Any, *, table: str, schema: str | None) -> list[Any]:
            return list(build_index_metadata(rows))

        def get_constraints(self, _driver: Any, *, table: str, schema: str | None) -> list[dict[str, Any]]:
            return []

        def get_ddl(self, _driver: Any, name: str, *, schema: str | None, object_type: str) -> str:
            raise RuntimeError("no stored text")

    class _Driver:
        dialect = "teradata"
        data_dictionary = _DD()

        def is_in_transaction(self) -> bool:
            return False

        def execute(self, sql: str) -> None:
            executed.append(sql)

    with SyncWithoutIndexesContext(_Driver(), "mart.orders"):
        pass
    assert executed == [
        'DROP INDEX ("region", "status") ON "mart"."orders"',
        'DROP INDEX "idx_orders_customer" ON "mart"."orders"',
        'CREATE INDEX ("region", "status") ON "mart"."orders"',
        'CREATE UNIQUE INDEX "idx_orders_customer" ("cust") ON "mart"."orders"',
    ]


@pytest.mark.anyio
async def test_without_indexes_restores_dropped_indexes_when_a_later_drop_fails() -> None:
    """A failed drop recreates indexes already dropped by the same context before the error propagates."""
    indexes = ({"index_name": "idx_a", "columns": ["a"]}, {"index_name": "idx_b", "columns": ["b"]})
    executed_sync: list[str] = []
    executed_async: list[str] = []

    class _SyncDD:
        def get_indexes(self, _driver: Any, *, table: str, schema: str | None) -> tuple[dict[str, Any], ...]:
            return indexes

        def get_constraints(self, _driver: Any, *, table: str, schema: str | None) -> list[dict[str, Any]]:
            return []

    class _SyncDriver:
        dialect = "sqlite"
        data_dictionary = _SyncDD()

        def execute(self, sql: str) -> None:
            if "idx_b" in sql and sql.startswith("DROP"):
                raise RuntimeError("drop failed")
            executed_sync.append(sql)

    with pytest.raises(RuntimeError, match="drop failed"):
        with SyncWithoutIndexesContext(_SyncDriver(), "orders"):
            pass
    assert len(executed_sync) == 2
    assert executed_sync[0].startswith("DROP INDEX")
    assert executed_sync[1].startswith("CREATE INDEX") and "idx_a" in executed_sync[1]

    class _AsyncDD:
        async def get_indexes(self, _driver: Any, *, table: str, schema: str | None) -> tuple[dict[str, Any], ...]:
            return indexes

        async def get_constraints(self, _driver: Any, *, table: str, schema: str | None) -> list[dict[str, Any]]:
            return []

    class _AsyncDriver:
        dialect = "sqlite"
        data_dictionary = _AsyncDD()

        async def execute(self, sql: str) -> None:
            if "idx_b" in sql and sql.startswith("DROP"):
                raise RuntimeError("drop failed")
            executed_async.append(sql)

    with pytest.raises(RuntimeError, match="drop failed"):
        async with AsyncWithoutIndexesContext(_AsyncDriver(), "orders"):
            pass
    assert executed_async == executed_sync

    class _SnowflakeNoopDriver:
        dialect = "snowflake"

    with SyncWithoutIndexesContext(_SnowflakeNoopDriver(), "ANALYTICS.ORDERS"):
        pass
    async with AsyncWithoutIndexesContext(_SnowflakeNoopDriver(), "ANALYTICS.ORDERS"):
        pass


@pytest.mark.parametrize(
    ("dialect", "index_name", "table", "schema", "columns", "expected_drop", "expected_create"),
    [
        (
            "postgres",
            'idx"cust',
            'ord"ers',
            'pub"lic',
            ['col"a DESC', "col_b ASC"],
            'DROP INDEX IF EXISTS "pub""lic"."idx""cust"',
            'CREATE INDEX "idx""cust" ON "pub""lic"."ord""ers" ("col""a" DESC, "col_b" ASC)',
        ),
        (
            "mssql",
            "idx]cust",
            "ord]ers",
            "dbo",
            ["col]a DESC", "col_b"],
            "DROP INDEX [idx]]cust] ON [dbo].[ord]]ers]",
            "CREATE INDEX [idx]]cust] ON [dbo].[ord]]ers] ([col]]a] DESC, [col_b])",
        ),
        (
            "mysql",
            "idx`cust",
            "ord`ers",
            "app",
            ["col`a ASC"],
            "DROP INDEX `idx``cust` ON `app`.`ord``ers`",
            "CREATE INDEX `idx``cust` ON `app`.`ord``ers` (`col``a` ASC)",
        ),
        (
            "spanner",
            "idx`cust",
            "orders",
            None,
            ["customer_id DESC"],
            "DROP INDEX `idx``cust`",
            "CREATE INDEX `idx``cust` ON `orders` (`customer_id` DESC)",
        ),
        (
            "oracle",
            'idx"cust',
            "orders",
            "APP",
            ["customer_id DESC"],
            'DROP INDEX "APP"."idx""cust"',
            'CREATE INDEX "idx""cust" ON "APP"."orders" ("customer_id" DESC)',
        ),
    ],
)
def test_index_ddl_quotes_delimiters_and_preserves_sort_direction(
    dialect: str,
    index_name: str,
    table: str,
    schema: str | None,
    columns: list[str],
    expected_drop: str,
    expected_create: str,
) -> None:
    """Index DDL builders quote embedded delimiters and preserve ASC/DESC modifiers per dialect."""
    meta = {"index_name": index_name, "columns": columns}
    assert build_drop_index_sql(dialect, meta, table, schema=schema) == expected_drop
    assert build_create_index_sql(dialect, meta, table, schema=schema) == expected_create


@pytest.mark.parametrize(
    ("table", "explicit_schema", "expected_pair"),
    [
        ('"my.schema"."my.orders"', None, ("my.schema", "my.orders")),
        ("[audit].[order.items]", None, ("audit", "order.items")),
        ("`my.db`.`my.table`", None, ("my.db", "my.table")),
        ("catalog.schema.orders", None, ("catalog.schema", "orders")),
        ('"order.items"', "audit", ("audit", "order.items")),
        ("orders", '"My.Schema"', ("My.Schema", "orders")),
    ],
)
def test_without_indexes_parses_quoted_dotted_schema_and_table_and_handles_async_tx_and_cancellation(
    table: str, explicit_schema: str | None, expected_pair: tuple[str, str]
) -> None:
    """Quoted dotted and multi-segment schema/table identifiers split cleanly across lookup and DDL helpers."""
    assert _split_schema_and_table(table, explicit_schema) == expected_pair
    assert (
        build_drop_index_sql("postgres", {"index_name": "idx_cust"}, '"my.schema"."my.orders"')
        == 'DROP INDEX IF EXISTS "my.schema"."idx_cust"'
    )
    assert (
        build_drop_index_sql("postgres", {"index_name": "idx_cust"}, "catalog.schema.orders")
        == 'DROP INDEX IF EXISTS "catalog"."schema"."idx_cust"'
    )
    assert (
        build_drop_index_sql("postgres", {"index_name": "idx_cust"}, "orders", schema="catalog.schema")
        == 'DROP INDEX IF EXISTS "catalog"."schema"."idx_cust"'
    )


@pytest.mark.parametrize(
    ("table", "explicit_schema", "expected_lookup", "expected_drop", "expected_create"),
    [
        (
            "catalog.schema.orders",
            None,
            ("catalog.schema", "orders"),
            'DROP INDEX IF EXISTS "catalog"."schema"."idx_a"',
            'CREATE INDEX "idx_a" ON "catalog"."schema"."orders" ("a")',
        ),
        (
            "orders",
            "catalog.schema",
            ("catalog.schema", "orders"),
            'DROP INDEX IF EXISTS "catalog"."schema"."idx_a"',
            'CREATE INDEX "idx_a" ON "catalog"."schema"."orders" ("a")',
        ),
    ],
)
def test_sync_without_indexes_preserves_multi_segment_schema_in_ddl(
    table: str, explicit_schema: str | None, expected_lookup: tuple[str, str], expected_drop: str, expected_create: str
) -> None:
    """Multi-segment unquoted schemas quote per segment in WithoutIndexesContext DDL."""
    executed: list[str] = []

    class _DD:
        def get_indexes(self, _driver: Any, *, table: str, schema: str | None) -> tuple[dict[str, Any], ...]:
            assert (schema, table) == expected_lookup
            return ({"index_name": "idx_a", "columns": ["a"]},)

        def get_constraints(self, _driver: Any, *, table: str, schema: str | None) -> list[dict[str, Any]]:
            return []

    class _SyncDriver:
        dialect = "postgres"
        data_dictionary = _DD()

        def execute(self, sql: str) -> None:
            executed.append(sql)

    with SyncWithoutIndexesContext(_SyncDriver(), table, schema=explicit_schema, concurrent=False):
        pass

    assert executed == [expected_drop, expected_create]


@pytest.mark.anyio
async def test_async_without_indexes_uses_transaction_open_and_restores_on_cancellation() -> None:
    """AsyncWithoutIndexesContext checks _transaction_open and restores already-dropped indexes on CancelledError."""
    executed: list[str] = []

    class _DD:
        async def get_indexes(self, _driver: Any, *, table: str, schema: str | None) -> tuple[dict[str, Any], ...]:
            assert (schema, table) == ("my.schema", "my.orders")
            return ({"index_name": "idx_a", "columns": ["a"]}, {"index_name": "idx_b", "columns": ["b"]})

        async def get_constraints(self, _driver: Any, *, table: str, schema: str | None) -> list[dict[str, Any]]:
            return []

    class _AsyncDriver:
        dialect = "postgres"
        data_dictionary = _DD()

        async def _transaction_open(self) -> bool:
            return True

        def _connection_in_transaction(self) -> bool:
            raise AssertionError("sync _connection_in_transaction should not run when _transaction_open exists")

        async def execute(self, sql: str) -> None:
            executed.append(sql)
            if "DROP INDEX" in sql and "idx_b" in sql:
                raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        async with AsyncWithoutIndexesContext(_AsyncDriver(), '"my.schema"."my.orders"', concurrent=True):
            pass

    assert executed == [
        'DROP INDEX IF EXISTS "my.schema"."idx_a"',
        'DROP INDEX IF EXISTS "my.schema"."idx_b"',
        'CREATE INDEX "idx_a" ON "my.schema"."my.orders" ("a")',
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("table", "meta_schema", "expected_lookup", "expected_drop", "expected_create"),
    [
        (
            '"My.Schema"."My.Orders"',
            "My.Schema",
            ("My.Schema", "My.Orders"),
            'DROP INDEX IF EXISTS "My.Schema"."idx_cust"',
            'CREATE INDEX "idx_cust" ON "My.Schema"."My.Orders" ("cust")',
        ),
        (
            "catalog.schema.orders",
            "schema",
            ("catalog.schema", "orders"),
            'DROP INDEX IF EXISTS "catalog"."schema"."idx_cust"',
            'CREATE INDEX "idx_cust" ON "catalog"."schema"."orders" ("cust")',
        ),
    ],
)
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_without_indexes_ignores_catalog_schema_name_when_table_is_already_qualified(
    is_async: bool,
    table: str,
    meta_schema: str,
    expected_lookup: tuple[str, str],
    expected_drop: str,
    expected_create: str,
) -> None:
    """Catalog schema_name metadata does not override or re-split qualified table identifiers."""
    executed: list[str] = []

    if is_async:

        class _AsyncDD:
            async def get_indexes(self, _driver: Any, *, table: str, schema: str | None) -> tuple[dict[str, Any], ...]:
                assert (schema, table) == expected_lookup
                return ({"index_name": "idx_cust", "columns": ["cust"], "schema_name": meta_schema},)

            async def get_constraints(self, _driver: Any, *, table: str, schema: str | None) -> list[dict[str, Any]]:
                return []

        class _AsyncDriver:
            dialect = "postgres"
            data_dictionary = _AsyncDD()

            async def execute(self, sql: str) -> None:
                executed.append(sql)

        async with AsyncWithoutIndexesContext(_AsyncDriver(), table, concurrent=False):
            pass
    else:

        class _SyncDD:
            def get_indexes(self, _driver: Any, *, table: str, schema: str | None) -> tuple[dict[str, Any], ...]:
                assert (schema, table) == expected_lookup
                return ({"index_name": "idx_cust", "columns": ["cust"], "schema_name": meta_schema},)

            def get_constraints(self, _driver: Any, *, table: str, schema: str | None) -> list[dict[str, Any]]:
                return []

        class _SyncDriver:
            dialect = "postgres"
            data_dictionary = _SyncDD()

            def execute(self, sql: str) -> None:
                executed.append(sql)

        with SyncWithoutIndexesContext(_SyncDriver(), table, concurrent=False):
            pass

    assert executed == [expected_drop, expected_create]


@pytest.mark.parametrize(
    ("dialect", "expected_drop", "expected_create"),
    [
        (
            "postgres",
            'DROP INDEX IF EXISTS "My.Schema"."idx_cust"',
            'CREATE INDEX "idx_cust" ON "My.Schema"."orders" ("cust")',
        ),
        ("oracle", 'DROP INDEX "My.Schema"."idx_cust"', 'CREATE INDEX "idx_cust" ON "My.Schema"."orders" ("cust")'),
        (
            "mssql",
            "DROP INDEX [idx_cust] ON [My.Schema].[orders]",
            "CREATE INDEX [idx_cust] ON [My.Schema].[orders] ([cust])",
        ),
    ],
)
def test_index_ddl_quotes_dotted_catalog_schema_name_as_single_identifier(
    dialect: str, expected_drop: str, expected_create: str
) -> None:
    """Unqualified tables keep dotted index_meta['schema_name'] values intact as one schema identifier."""
    meta = {"index_name": "idx_cust", "columns": ["cust"], "schema_name": "My.Schema"}
    assert build_drop_index_sql(dialect, meta, "orders") == expected_drop
    assert build_create_index_sql(dialect, meta, "orders") == expected_create
