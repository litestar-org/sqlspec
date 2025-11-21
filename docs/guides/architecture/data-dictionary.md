# Data Dictionary & Introspection

SQLSpec provides a unified Data Dictionary API to introspect database schemas across all supported adapters. This allows you to retrieve table metadata, columns, indexes, and foreign keys in a consistent format, regardless of the underlying database engine.

## Core Concepts

The `DataDictionary` is accessed via the `driver.data_dictionary` property. It provides methods to query the database catalog.

### Introspection Capabilities

- **Tables**: List tables in a schema.
- **Columns**: Get column details (name, type, nullable, default).
- **Indexes**: Get index definitions (columns, uniqueness).
- **Foreign Keys**: Get foreign key constraints and relationships.
- **Topological Sorting**: Get tables sorted by dependency order (useful for cleanups or migrations).

## Usage

### Basic Introspection

```python
async with config.provide_session() as session:
    # Get all tables in the default schema
    tables = await session.data_dictionary.get_tables(session)
    print(f"Tables: {tables}")

    # Get columns for a specific table
    columns = await session.data_dictionary.get_columns(session, "users")
    for col in columns:
        print(f"{col['column_name']}: {col['data_type']}")
```

### Topological Sort (Dependency Ordering)

One of the most powerful features is `get_tables_in_topological_order`. This returns table names sorted such that parent tables appear before child tables (tables with foreign keys to parents).

This is essential for:

- **Data Loading**: Insert into parents first.
- **Cleanup**: Delete in reverse order to avoid foreign key violations.

```python
async with config.provide_session() as session:
    # Get tables sorted parent -> child
    sorted_tables = await session.data_dictionary.get_tables_in_topological_order(session)
    
    print("Insertion Order:", sorted_tables)
    print("Deletion Order:", list(reversed(sorted_tables)))
```

**Implementation Details**:

- **Postgres / SQLite / MySQL 8+**: Uses efficient Recursive CTEs in SQL.
- **Oracle**: Uses `CONNECT BY` queries.
- **Others (BigQuery, MySQL 5.7)**: Falls back to a Python-based topological sort using `graphlib`.

### Metadata Types

SQLSpec uses typed dataclasses for metadata results where possible.

```python
from sqlspec.driver import ForeignKeyMetadata

async with config.provide_session() as session:
    fks: list[ForeignKeyMetadata] = await session.data_dictionary.get_foreign_keys(session, "orders")
    
    for fk in fks:
        print(f"FK: {fk.column_name} -> {fk.referenced_table}.{fk.referenced_column}")
```

## Adapter Support Matrix

| Feature | Postgres | SQLite | Oracle | MySQL | DuckDB | BigQuery |
|---------|----------|--------|--------|-------|--------|----------|
| Tables | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Columns | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Indexes | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| Foreign Keys | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Topological Sort | ✅ (CTE) | ✅ (CTE) | ✅ (Connect By) | ✅ (CTE/Python) | ✅ (CTE) | ✅ (Python) |

## API Reference

### Data Dictionary Protocol

The base interface shared by all adapters.

```python
class DataDictionaryBase:
    async def get_tables(self, driver, schema=None) -> list[str]: ...
    
    async def get_columns(self, driver, table, schema=None) -> list[dict]: ...
    
    async def get_indexes(self, driver, table, schema=None) -> list[dict]: ...
    
    async def get_foreign_keys(self, driver, table=None, schema=None) -> list[ForeignKeyMetadata]: ...
    
    async def get_tables_in_topological_order(self, driver, schema=None) -> list[str]: ...
```
