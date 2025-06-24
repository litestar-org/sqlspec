# Integration Test Failure Analysis

Date: 2024-12-23

## Executive Summary

Analyzed 100+ integration test failures across multiple database adapters. Most failures fall into 4 main categories:

1. **Parquet Export** - Missing .parquet extension (40% of failures)
2. **Transaction Management** - Psycopg isolation and ADBC rollback issues (35% of failures)  
3. **Parameter Style** - Configuration and conversion issues (20% of failures)
4. **Type Handling** - JSON/Arrow type preservation (5% of failures)

## Root Cause Analysis

### 1. Parquet File Extension Issue ⚠️ CRITICAL

**Problem**: `export_to_storage` creates temp files without .parquet extension
**Impact**: All parquet export tests fail with "Parquet magic bytes not found"
**Root Cause**: `_infer_format()` relies on file extension, temp files have no extension
**Solution**: Add logic to append extension based on format parameter

### 2. Psycopg Transaction Isolation 🔴 BLOCKING

**Problem**: INSERT operations report success but SELECT returns empty
**Evidence**:

- `rows_affected=1` but subsequent SELECT finds no rows
- Tests pass individually but fail in batch
**Root Cause**: Connection pool returning different connections OR transactions not committed
**Solution**: Enforce autocommit in tests or ensure same connection throughout test

### 3. ADBC PostgreSQL Transaction Errors 🟡 HIGH

**Problem**: "current transaction is aborted, commands ignored until end of transaction block"
**Root Cause**: Error in transaction leaves connection in bad state
**Solution**: Add automatic rollback on PostgreSQL errors in ADBC driver

### 4. Parameter Style Validation 🟢 MEDIUM

**Problem**: Various adapters fail parameter style checks
**Examples**:

- Oracle missing 'numeric' in supported styles (oracle doesn't support numeric.  it's only positional_colon and named_colon)
- AsyncPG parameter conversion issues  
- ADBC parameter binding errors
**Solution**: Ensure parameter styles properly configured and validated

## Detailed Fixes

### Fix #1: Parquet Extension (sqlspec/driver/mixins/_storage.py)

```python
def export_to_storage(self, statement, destination_uri, format="parquet", **kwargs):
    # Add extension if missing
    if format and not destination_uri.endswith(f".{format}"):
        destination_uri = f"{destination_uri}.{format}"
```

### Fix #2: ADBC Transaction Handling (sqlspec/adapters/adbc/driver.py)

```python
except Exception:
    # Rollback on PostgreSQL errors
    if self.dialect == "postgres" and hasattr(conn, "rollback"):
        with contextlib.suppress(Exception):
            conn.rollback()
    raise
```

### Fix #3: Psycopg Test Configuration

- Add `autocommit=True` to all test fixtures
- Or ensure explicit `commit()` after INSERTs
- Consider using single connection for entire test

### Fix #4: Oracle Config (sqlspec/adapters/oracledb/config.py)

```python
supported_parameter_styles: ClassVar[tuple[str, ...]] = ("named_colon", "positional_colon")
```

## Test Categories Affected

### Critical Path (Must Fix)

- All parquet export tests (~40 tests)
- Psycopg CRUD operations (~25 tests)
- ADBC PostgreSQL tests (~20 tests)

### Secondary Issues

- Parameter style validation (~15 tests)
- JSON type handling (~5 tests)
- Dialect propagation (~5 tests)

## Implementation Priority

1. **Immediate**: Parquet extension fix (affects most tests)
2. **High**: Transaction management fixes
3. **Medium**: Parameter style configurations
4. **Low**: Type preservation improvements

## Validation Strategy

After implementing fixes:

1. Run affected test suites individually
2. Verify no regression in unit tests
3. Run full integration test suite
4. Check for any new failures introduced

## Recommendations

1. **Short-term**: Apply tactical fixes listed above
2. **Medium-term**:
   - Add integration test fixtures for proper transaction isolation
   - Improve error messages for parameter style mismatches
3. **Long-term**:
   - Consider connection lifecycle logging
   - Add debug mode for transaction boundaries
   - Implement test helpers for database state verification
