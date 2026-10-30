# Data Dictionary

Every SQLSpec driver exposes `session.data_dictionary`—a dialect-aware catalog introspection interface that returns normalized typed metadata (`TableMetadata`, `ColumnMetadata`, `IndexMetadata`, `ForeignKeyMetadata`, `VersionInfo`) across SQLite, PostgreSQL, CockroachDB, MySQL, MariaDB, Oracle, SQL Server, Db2, DuckDB, BigQuery, and Spanner.

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

## Core Introspection Methods

| Method | Returned Metadata | Description |
| --- | --- | --- |
| `get_tables(driver, schema=None)` | `list[TableMetadata]` | Lists user tables in topological/catalog order (including self-referential root tables). |
| `get_columns(driver, table=None, schema=None)` | `list[ColumnMetadata]` | Returns column names, data types, nullability, defaults, and identity/generated flags. |
| `get_indexes(driver, table=None, schema=None)` | `list[IndexMetadata]` | Returns index names, indexed column lists, and uniqueness flags. |
| `get_foreign_keys(driver, table=None, schema=None)` | `list[ForeignKeyMetadata]` | Returns referencing and referenced schemas, tables, and correlated composite columns. |
| `get_version(driver)` | `VersionInfo \| None` | Parses and caches the connected database server's `major`, `minor`, and `patch` version. |
| `get_optimal_type(driver, type_category)` | `str` | Resolves the best DDL type for categories such as `"json"`, `"uuid"`, `"binary"`, `"text"`, or `"timestamp"` on the connected server version. |

### Metadata Record Fields

| Metadata Type | Key Fields |
| --- | --- |
| `TableMetadata` | `table_name`, `schema_name`, `table_type` |
| `ColumnMetadata` | `name`, `type`, `nullable`, `default`, `is_identity`, `is_generated` |
| `IndexMetadata` | `name`, `table_name`, `columns`, `unique` |
| `ForeignKeyMetadata` | `name`, `table_name`, `column_name`, `foreign_table_name`, `foreign_column_name`, `foreign_schema` |
| `VersionInfo` | `major`, `minor`, `patch` |

## Dependency Ordering and Portable DDL Helpers

When exporting schemas, seeding fixtures, or truncating tables in foreign-key order, combine `get_foreign_keys` with `dependency_edges_from_foreign_keys` and `sort_dependencies` from `sqlspec.data_dictionary`:

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

Catalog views store unquoted identifier names (`schema_versions`, not `"schema_versions"`). Pass bare table and schema names (`table="members"`, `schema="public"`) to `data_dictionary` methods even when your DDL statements quote identifiers.

## Recap

- Access `session.data_dictionary` to inspect live schemas portably across all database adapters.
- Use `get_version` and `get_optimal_type` when generating cross-database DDL for extensions or multi-engine libraries.
- Pair `dependency_edges_from_foreign_keys` and `sort_dependencies` to determine safe parent-before-child load order.
