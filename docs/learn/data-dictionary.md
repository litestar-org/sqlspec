# Data Dictionary

Every SQLSpec driver exposes `session.data_dictionary`—a dialect-aware catalog introspection and DDL synthesis interface returning normalized typed metadata (`TableMetadata`, `ColumnMetadata`, `IndexMetadata`, `ForeignKeyMetadata`, `ColumnProfileMetadata`, `DDLResult`, `VersionInfo`) across SQLite, PostgreSQL, CockroachDB, MySQL, MariaDB, Oracle, SQL Server, Db2, DuckDB, BigQuery, and Spanner.

## Introspecting Tables, Columns, Indexes, and Foreign Keys

Pass the active `session` (sync) or `await session.data_dictionary.*(session, ...)` (async) to inspect live schema catalogs without writing engine-specific `information_schema`, `pg_catalog`, `ALL_TAB_COLUMNS`, or `SYSCAT` queries:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

spec = SQLSpec()
config = spec.add_config(
    SqliteConfig(connection_config={"database": ":memory:"})
)

with spec.provide_session(config) as session:
    session.execute(
        "CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)"
    )
    session.execute(
        """
        CREATE TABLE members (
            id INTEGER PRIMARY KEY,
            team_id INTEGER REFERENCES teams(id),
            email TEXT NOT NULL
        )
        """
    )
    session.execute("CREATE INDEX idx_members_team_id ON members (team_id)")

    dd = session.data_dictionary
    tables = dd.get_tables(session)
    columns = dd.get_columns(session, table="members")
    indexes = dd.get_indexes(session, table="members")
    foreign_keys = dd.get_foreign_keys(session, table="members")
```

=== "Sync Driver"

    ```python
    tables = session.data_dictionary.get_tables(session, schema="public")
    columns = session.data_dictionary.get_columns(
        session, table="members", schema="public"
    )
    version = session.data_dictionary.get_version(session)
    ```

=== "Async Driver"

    ```python
    tables = await session.data_dictionary.get_tables(session, schema="public")
    columns = await session.data_dictionary.get_columns(
        session, table="members", schema="public"
    )
    version = await session.data_dictionary.get_version(session)
    ```

## Core Introspection and DDL Methods

| Method | Returned Metadata | Description |
| --- | --- | --- |
| `get_tables(driver, schema=None)` | `list[TableMetadata]` | Lists user tables in topological/catalog order (including self-referential root tables). |
| `get_columns(driver, table=None, schema=None)` | `list[ColumnMetadata]` | Returns column names, types, nullability, defaults, and identity/generated expressions. |
| `get_indexes(driver, table=None, schema=None)` | `list[IndexMetadata]` | Returns index names, indexed column lists, and uniqueness flags. |
| `get_foreign_keys(driver, table=None, schema=None)` | `list[ForeignKeyMetadata]` | Returns referencing/referenced schemas, tables, columns, and `on_delete` / `on_update` actions. |
| `get_ddl(driver, object_name, ...)` | `DDLResult` | Extracts native engine DDL when available or synthesizes dialect-aware AST DDL (`target_dialect` optional). |
| `get_schema_ddl(driver, schema=None, ...)` | `MetadataResult` | Emits topologically ordered `CREATE` or reverse-topological `DROP` `DDLResult` items across a schema. |
| `get_table_profile(driver, table, ...)` | `list[ColumnProfileMetadata]` | Samples live rows to compute null counts, cardinality, max string lengths, and narrower SQL types. |
| `infer_table_ddl_from_data(driver, table, ...)` | `DDLResult` | Generates a typed `CREATE TABLE` statement from profiled column values (`target_table` / `target_dialect`). |
| `get_version(driver)` | `VersionInfo \| None` | Parses and caches the connected database server's `major`, `minor`, and `patch` version. |
| `get_optimal_type(driver, type_category)` | `str` | Resolves the best DDL type for categories such as `"json"`, `"uuid"`, `"binary"`, `"text"`, or `"timestamp"`. |

### Metadata Record Fields

| Metadata Type | Key Fields |
| --- | --- |
| `TableMetadata` | `table_name`, `schema_name`, `table_type`, `dependency_level` |
| `ColumnMetadata` | `column_name`, `data_type`, `is_nullable`, `column_default`, `identity_generation`, `is_generated`, `generation_expression` |
| `IndexMetadata` | `index_name`, `table_name`, `column_names`, `is_unique`, `is_primary` |
| `ForeignKeyMetadata` | `constraint_name`, `table_name`, `column_name`, `referenced_table`, `referenced_column`, `on_delete`, `on_update` |
| `ColumnProfileMetadata` | `column_name`, `declared_type`, `inferred_type`, `row_count`, `null_count`, `distinct_count`, `nullable`, `max_char_length`, `is_unique_candidate` |
| `DDLResult` | `identity`, `status`, `fidelity`, `source`, `ddl`, `dependencies`, `warnings` |

## DDL Extraction, Cross-Dialect Synthesis, and Table Profiling

`get_ddl` and `get_schema_ddl` prefer native catalog DDL where supported (SQLite `sqlite_master`, DuckDB `duckdb_tables()`, CockroachDB `SHOW CREATE`, BigQuery `INFORMATION_SCHEMA.TABLES`) and synthesize dialect-valid AST DDL from catalog metadata otherwise—with optional cross-dialect transpilation via `target_dialect`:

```{ .python title="docs/examples/patterns/schema_ddl_and_inference.py" hl_lines="27 28 29 30 32 33 34" }
--8<-- "docs/examples/patterns/schema_ddl_and_inference.py"
```

When migrating loosely typed staging tables (`TEXT` / `VARCHAR` landing zones) or raw JSON/Arrow datasets, `get_table_profile` and `infer_table_ddl_from_data` inspect live sample values to recommend tighter types (`BIGINT`, `NUMERIC(p, s)`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `UUID`) and `NOT NULL` constraints.

## Dependency Ordering and Portable DDL Helpers

When exporting schemas, seeding fixtures, or truncating tables in foreign-key order, combine `get_foreign_keys` with `dependency_edges_from_foreign_keys`, `sort_dependencies`, and `sort_ddl_results` from `sqlspec.data_dictionary`:

```python
from sqlspec.data_dictionary import (
    dependency_edges_from_foreign_keys,
    sort_dependencies,
)

edges = dependency_edges_from_foreign_keys(foreign_keys)
sorted_plan = sort_dependencies(edges)
```

`get_optimal_type` adapts automatically to server capabilities—for example, selecting native `JSON` on Oracle 21c+/26ai vs `CLOB` on older releases, `JSONB` on PostgreSQL, or `STRUCT`/`JSON` on analytical engines—making multi-adapter extension stores (`ADK`, `Events`, `Litestar`) portable without hardcoded version branches.

## Identifier Quoting vs Catalog Lookups

Catalog views store unquoted identifier names (`schema_versions`, not `"schema_versions"`). Pass bare table and schema names (`table="members"`, `schema="public"`) to `data_dictionary` methods; synthesized DDL quotes reserved words and mixed-case catalog names automatically for the target dialect.

## Recap

- Access `session.data_dictionary` to inspect live schemas, extract or transpile topological DDL, and profile staging tables across all database adapters.
- Use `get_version` and `get_optimal_type` when generating cross-database DDL for extensions or multi-engine libraries.
- Pair `get_schema_ddl(direction="create" | "drop")` or `sort_dependencies` to determine safe parent-before-child execution order.
