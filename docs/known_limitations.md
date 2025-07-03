# Known Limitations

This document tracks known limitations in SQLSpec and its adapters that are due to upstream dependencies or driver constraints.

## ADBC PostgreSQL Adapter

### Null Parameter Handling

**Status**: Known limitation in upstream ADBC PostgreSQL driver
**Tracking**: <https://github.com/apache/arrow-adbc/issues/81>
**Impact**: Cannot use NULL values as bind parameters in parameterized queries

#### Description

The ADBC PostgreSQL driver has incomplete support for null values in bind parameters. This affects:

- INSERT statements with NULL parameter values
- UPDATE statements setting columns to NULL via parameters
- Any parameterized query where a parameter value is None/NULL

#### Example of Affected Code

```python
# This will fail with ADBC PostgreSQL driver
cursor.execute("INSERT INTO table (col1, col2) VALUES ($1, $2)", (None, "value"))

# This will also fail
cursor.execute("UPDATE table SET col1 = $1 WHERE id = $2", (None, 123))
```

#### Workaround

Until the upstream driver is fixed, you can:

1. Use literal NULL in SQL instead of parameters:

   ```python
   cursor.execute("INSERT INTO table (col1, col2) VALUES (NULL, $1)", ("value",))
   ```

2. Use dynamic SQL construction (less secure, not recommended):

   ```python
   sql = f"INSERT INTO table (col1, col2) VALUES ({'NULL' if val1 is None else '$1'}, $2)"
   ```

3. Use a different PostgreSQL adapter (psycopg, asyncpg) if null parameter support is critical

### Execute Many (Batch Operations)

**Status**: Known limitation in upstream ADBC PostgreSQL driver
**Impact**: Cannot use `executemany()` for batch operations with parameters

#### Description

The ADBC PostgreSQL driver fails with the error "Can't map Arrow type 'na' to Postgres type" when using `executemany()`. This appears to be an issue with how the driver converts parameter batches through Arrow format for PostgreSQL.

#### Example of Affected Code

```python
# This will fail with ADBC PostgreSQL driver
params_list = [("name1", 1), ("name2", 2), ("name3", 3)]
cursor.executemany("INSERT INTO table (name, value) VALUES ($1, $2)", params_list)
```

#### Workaround

Until the upstream driver is fixed, you can:

1. Use individual execute() calls in a loop (less efficient):

   ```python
   for params in params_list:
       cursor.execute("INSERT INTO table (name, value) VALUES ($1, $2)", params)
   ```

2. Use a different PostgreSQL adapter (psycopg, asyncpg) for batch operations

#### Test Status

The following tests are marked as `xfail` due to these limitations:

- `tests/integration/test_adapters/test_adbc/test_postgres_driver.py::test_null_parameters` (null parameter issue)
- `tests/integration/test_adapters/test_adbc/test_postgres_driver.py::test_execute_many` (executemany issue)
- `tests/integration/test_adapters/test_adbc/test_data_types.py::test_postgresql_null_values` (null parameter issue)
- `tests/integration/test_adapters/test_adbc/test_parameter_styles.py::test_postgresql_null_parameters` (null parameter issue)

These tests should be re-enabled once the upstream issues are resolved.
