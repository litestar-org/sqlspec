# SQLSpec Integration Test Failure Analysis

## Summary of Failure Patterns

### 1. **Parquet Export Issues - Temporary File Extension**

- **Pattern**: When exporting to Parquet format, temporary files are created without `.parquet` extension
- **Root Cause**: In `_export_via_backend()`, temp files are created with just `.{format}` suffix
- **Affected Tests**:
    - `test_psycopg_to_parquet`
    - Various export_to_storage tests
- **Fix**: Ensure temp files have proper extensions for format detection

### 2. **ADBC PostgreSQL Transaction Errors**

- **Pattern**: `current transaction is aborted, commands ignored until end of transaction block`
- **Root Cause**: After an error (like invalid SQL), PostgreSQL requires rollback before new commands
- **Affected Tests**:
    - `test_adbc_postgresql_error_handling`
    - `test_postgresql_parameter_types`
- **Fix**: Add transaction rollback after errors in ADBC driver

### 3. **Parameter Style Validation Issues**

- **Pattern**: Parameter style mismatches between what's detected and what's supported
- **Root Cause**: Config's parameter style settings not properly propagated to drivers
- **Affected Tests**: Multiple parameter style tests across adapters
- **Fix**: Ensure parameter style configuration is properly injected in all drivers

### 4. **Arrow Table Fetching - Missing Method**

- **Pattern**: `AttributeError: 'Cursor' object has no attribute 'fetch_arrow_table'`
- **Root Cause**: Psycopg driver tries to call non-existent method on cursor
- **Affected Tests**:
    - `test_psycopg_to_parquet`
    - Arrow-related tests in Psycopg
- **Fix**: Use fallback implementation in Psycopg driver

### 5. **JSON Export Type Issues**

- **Pattern**: `TypeError: '>' not supported between instances of 'str' and 'int'`
- **Root Cause**: JSON export converts numeric values to strings
- **Affected Tests**: `test_export_to_storage_json_format`
- **Fix**: Preserve data types in JSON export or use proper serialization

### 6. **Storage Backend Import/Export**

- **Pattern**: Various failures in storage operations
- **Root Cause**: Missing or incorrect storage backend integration
- **Affected Tests**: Multiple storage integration tests
- **Fix**: Review storage backend operations and ensure proper file handling

### 7. **Dialect Propagation Issues**

- **Pattern**: Dialect not properly propagated through execution chain
- **Root Cause**: Dialect information lost during SQL object creation
- **Affected Tests**: All dialect propagation tests
- **Fix**: Ensure dialect is passed through all SQL operations

## Proposed Fixes

### Fix 1: Parquet Export Temporary File Extension

```python
# In _export_via_backend() around line 513:
# Change from:
suffix = f".{format}"
# To:
suffix = f".{format}" if format != "parquet" else ".parquet"
```

### Fix 2: ADBC PostgreSQL Transaction Handling

```python
# In ADBC driver's _execute() method:
try:
    cursor.execute(sql, parameters or [])
except Exception as e:
    # Rollback transaction on error for PostgreSQL
    if self.dialect == "postgres":
        try:
            conn.rollback()
        except Exception:
            pass
    raise
```

### Fix 3: Psycopg Arrow Table Fetching

```python
# In Psycopg driver, remove custom _fetch_arrow_table and use fallback:
# Remove the method that tries to call cursor.fetch_arrow_table()
# The base class fallback will handle it properly
```

### Fix 4: JSON Export Type Preservation

```python
# In _write_json() method:
# Ensure proper type conversion when creating JSON rows
# Use custom JSON encoder that preserves numeric types
```

### Fix 5: Parameter Style Configuration

```python
# Ensure all drivers properly implement provide_session with parameter style injection
# Similar to what SqliteConfig does
```

## Testing Strategy

1. **Run focused tests first**: Test each fix against its specific failing test
2. **Verify no regressions**: Run full test suite after each fix
3. **Check related tests**: Ensure fixes don't break similar functionality
4. **Integration validation**: Run full integration suite to catch edge cases

## Priority Order

1. **High Priority**: Parquet export (affects multiple tests)
2. **High Priority**: ADBC transaction handling (blocks other tests)
3. **Medium Priority**: Arrow table fetching (feature-specific)
4. **Medium Priority**: Parameter style configuration
5. **Low Priority**: JSON type handling (cosmetic issue)
