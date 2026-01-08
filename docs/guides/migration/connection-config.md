# Migration Guide: pool_config → connection_config

**Version:** 0.33.0
**Type:** Breaking Change
**Impact:** All SQLSpec configurations

## Summary

SQLSpec 0.33.0 standardizes configuration parameter naming across all database adapters:

- `pool_config` → `connection_config` (configuration dictionary)
- `pool_instance` → `connection_instance` (pre-created pool/connection instance)

This change affects **all 11 database adapters** and provides a consistent, intuitive API.
Legacy `pool_config` and `pool_instance` arguments are now rejected; pass the standardized names instead.

## Why This Change?

### Previous Inconsistency

Before this change, SQLSpec had inconsistent parameter naming:

- **9 pooled adapters** used `pool_config` and `pool_instance`
- **2 non-pooled adapters** (BigQuery, ADBC) used `connection_config` and `pool_instance`

This created confusion:
- `pool_instance` didn't make semantic sense for non-pooled adapters
- New users had to learn which adapters used which parameter names
- Documentation had to explain the split

### New Consistency

After this change, **all adapters** use the same parameter names:

```python
# Every adapter now follows the same pattern
config = AdapterConfig(
    connection_config={...},      # Settings for connection/pool
    connection_instance=instance  # Pre-created pool or connection
)
```

**Benefits:**
- Single pattern to learn and remember
- Works semantically for both pooled and non-pooled adapters
- Reduced cognitive load when switching between adapters
- Clearer documentation

## Migration Steps

### 1. Search and Replace

The simplest migration approach is a global search and replace:

**For Python files:**

```bash
# Replace pool_config → connection_config
find . -type f -name "*.py" -exec sed -i 's/pool_config=/connection_config=/g' {} +

# Replace pool_instance → connection_instance
find . -type f -name "*.py" -exec sed -i 's/pool_instance=/connection_instance=/g' {} +
```

**For IDE users:**

1. Global search for `pool_config=`
2. Replace all with `connection_config=`
3. Global search for `pool_instance=`
4. Replace all with `connection_instance=`

### 2. Before and After Examples

#### Pooled Adapters (PostgreSQL, MySQL, etc.)

**Before:**

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    pool_config={
        "dsn": "postgresql://localhost/mydb",
        "min_size": 5,
        "max_size": 20,
    }
)

# Or with pre-created pool
config = AsyncpgConfig(
    pool_instance=my_existing_pool
)
```

**After:**

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    connection_config={
        "dsn": "postgresql://localhost/mydb",
        "min_size": 5,
        "max_size": 20,
    }
)

# Or with pre-created pool
config = AsyncpgConfig(
    connection_instance=my_existing_pool
)
```

#### SQLite with Custom Pool

**Before:**

```python
from sqlspec.adapters.sqlite import SqliteConfig

config = SqliteConfig(
    pool_config={
        "database": "mydb.db",
        "check_same_thread": False,
        "pool_min_size": 5,
        "pool_max_size": 10,
    }
)
```

**After:**

```python
from sqlspec.adapters.sqlite import SqliteConfig

config = SqliteConfig(
    connection_config={
        "database": "mydb.db",
        "check_same_thread": False,
        "pool_min_size": 5,
        "pool_max_size": 10,
    }
)
```

#### Non-Pooled Adapters (BigQuery, ADBC)

**Before:**

```python
from sqlspec.adapters.bigquery import BigQueryConfig

# Already used connection_config, but pool_instance was misleading
config = BigQueryConfig(
    connection_config={
        "project": "my-project",
        "dataset_id": "my-dataset",
    },
    pool_instance=my_client  # Misleading name!
)
```

**After:**

```python
from sqlspec.adapters.bigquery import BigQueryConfig

# Now semantically correct
config = BigQueryConfig(
    connection_config={
        "project": "my-project",
        "dataset_id": "my-dataset",
    },
    connection_instance=my_client  # Clear!
)
```

#### Framework Extensions

**Before:**

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/db"},
    extension_config={
        "litestar": {"commit_mode": "autocommit"}
    }
)
```

**After:**

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://localhost/db"},
    extension_config={
        "litestar": {"commit_mode": "autocommit"}
    }
)
```

### 3. Verification

After migration, verify your changes:

```bash
# Run linting to catch any issues
make lint

# Run type checking
make mypy

# Run your test suite
pytest
```

## Breaking Changes

### What Changed

1. **Parameter names** in all adapter config classes:
   - `pool_config` parameter → `connection_config`
   - `pool_instance` parameter → `connection_instance`

2. **Attribute names** in config instances:
   - `config.pool_config` attribute → `config.connection_config`
   - `config.pool_instance` attribute → `config.connection_instance`

3. **Custom pool constructors**:
   - `SqliteConnectionPool(**pool_config)` → `SqliteConnectionPool(**connection_config)`
   - `AiosqliteConnectionPool(**pool_config)` → `AiosqliteConnectionPool(**connection_config)`
   - `DuckDBConnectionPool(**pool_config)` → `DuckDBConnectionPool(**connection_config)`

### What Didn't Change

1. **Connection configuration keys** remain the same:
   ```python
   # These keys are unchanged
   connection_config={
       "dsn": "...",        # Same key
       "min_size": 5,       # Same key
       "max_size": 20,      # Same key
   }
   ```

2. **Driver implementation** unchanged - no behavior changes

3. **Extension configuration** unchanged - same `extension_config` parameter

4. **Statement configuration** unchanged - same `statement_config` parameter

## Affected Components

### All Adapters (11 total)

1. AsyncPG (`asyncpg`)
2. Psycopg (`psycopg`)
3. Asyncmy (`asyncmy`)
4. Psqlpy (`psqlpy`)
5. OracleDB (`oracledb`)
6. SQLite (`sqlite`)
7. AioSQLite (`aiosqlite`)
8. DuckDB (`duckdb`)
9. BigQuery (`bigquery`)
10. ADBC (`adbc`)
11. Spanner (`spanner`)

### Framework Extensions

- Litestar plugin
- Starlette extension
- FastAPI extension
- Flask extension

### Custom Pools

- `SqliteConnectionPool`
- `AiosqliteConnectionPool`
- `DuckDBConnectionPool`

## Timeline

- **Introduced:** v0.33.0
- **Deprecation Period:** None (clean break)
- **Removal of Old Names:** Immediate in v0.33.0

## Rationale

This breaking change was chosen over a deprecation period because:

1. **Simple migration** - Automated search and replace
2. **Early in lifecycle** - SQLSpec is pre-1.0, breaking changes expected
3. **Clear improvement** - Eliminates confusion, improves consistency
4. **Low effort** - Mechanical change with high value

## Support

If you encounter issues during migration:

1. Check this guide for examples
2. Search for remaining `pool_config` or `pool_instance` references:
   ```bash
   grep -r "pool_config\|pool_instance" your_project/
   ```
3. File an issue on GitHub if you find edge cases not covered here

## See Also

- [Configuration Guide](../usage/configuration.rst)
- [CHANGELOG](../../changelog.rst)
- [GitHub Release Notes](https://github.com/litestar-org/sqlspec/releases)
