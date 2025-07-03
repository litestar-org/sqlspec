# Phase 3 Integration Plan

## Current Status

### Completed Features (Phase 2)

1. **Consolidated Utilities**
   - `sqlspec/driver/connection.py` - Connection management utilities
   - `sqlspec/driver/parameters.py` - Parameter processing utilities
   - `sqlspec/protocols.py` - Runtime-checkable protocols
   - `sqlspec/driver/protocols.py` - Connection/driver capability protocols
   - `sqlspec/storage/capabilities.py` - Storage backend capabilities

2. **Type Safety Improvements**
   - Created type guards using protocols
   - Replaced hasattr() with isinstance(protocol) in some places
   - Updated connection.py and pipeline.py to use type guards

### Critical Issue: Test Failures

**Problem**: The ParameterizeLiterals transformer is extracting string literals from SQL statements, but when tests pass raw SQL without parameters, the driver receives SQL with placeholders but no values.

**Example**:

```sql
-- Original SQL from test
SELECT name FROM table WHERE name LIKE 'test%'

-- After ParameterizeLiterals processing
SELECT name FROM table WHERE name LIKE ?
-- But no parameters are provided!
```

**Root Cause**: The default SQLConfig has `enable_transformations=True`, which always applies ParameterizeLiterals unless:

1. The SQL is marked as a script (`as_script()`)
2. The SQL already had placeholders (`input_sql_had_placeholders=True`)
3. Transformations are explicitly disabled

## Integration Plan

### Phase 3A: Fix Parameter Extraction Issues (URGENT)

**Option 1: Disable transformations for tests**

- Tests that pass raw SQL without parameters should use `SQL(query, config=SQLConfig(enable_transformations=False))`
- Or mark as script: `SQL(query).as_script()`

**Option 2: Update tests to provide parameters**

- Change tests to use parameterized queries properly
- Example: `session.execute(SQL("SELECT * WHERE name LIKE ?", "test%"))`

**Option 3: Smart detection in SQL class**

- If no parameters are provided and SQL contains literals, disable transformations
- Risk: Could break intentional parameterization

**Recommended**: Option 2 - Update tests to use proper parameterization

### Phase 3B: Database Adapter Migration

**Goal**: Update all 9 database adapters to use consolidated utilities

**Pilot Adapter**: SQLite

1. Update `execute` and `execute_many` to use:
   - `managed_connection_sync` from `connection.py`
   - `managed_transaction_sync` for transaction handling
   - Parameter utilities from `parameters.py`

2. Pattern for adapter refactoring:

```python
from sqlspec.driver.connection import managed_connection_sync, managed_transaction_sync
from sqlspec.driver.parameters import normalize_parameter_sequence, convert_parameters_to_positional

def execute(self, sql: SQL, connection: Optional[ConnectionT] = None) -> SQLResult:
    with managed_connection_sync(self.config, connection) as conn:
        with managed_transaction_sync(conn, auto_commit=True) as txn_conn:
            # Normalize parameters
            params = normalize_parameter_sequence(sql.parameters)
            # Execute
            cursor = txn_conn.cursor()
            cursor.execute(sql.sql, params)
            # Build result
```

3. Roll out to remaining adapters:
   - AsyncPG, Psycopg, AsyncMy, DuckDB
   - BigQuery, OracleDB, ADBC, AIOSQLite

### Phase 3C: Storage Backend Capabilities (DETAILED IMPLEMENTATION)

**Goal**: Enhance capability detection and optimize storage operations

#### Current State Analysis

**ObStoreBackend** (✅ Well-implemented):

- Already implements `HasStorageCapabilities` with excellent native capabilities
- Properly sets cloud-native features: `supports_multipart_upload=True`, `supports_arrow=True`, etc.
- **Recommended by default** for sync operations due to superior performance (~9x faster)
- Native async support for all operations
- No major changes needed - just optimization opportunities

**FSSpecBackend** (❌ Needs Enhancement):

- Implements `HasStorageCapabilities` but uses generic capabilities
- Doesn't detect underlying filesystem type (S3, GCS, Azure, local, etc.)
- Sets `is_cloud_native=False` even when wrapping cloud storage
- **Fallback only** - async support is sporadic, uses sync ops wrapped with `async_()` utility
- Misses protocol-specific optimizations

#### Specific Changes Required

##### 1. Enhanced FSSpec Capability Detection

```python
# Current generic implementation in FSSpecBackend.__init__
capabilities: ClassVar[StorageCapabilities] = StorageCapabilities(
    supports_arrow=PYARROW_INSTALLED,
    is_cloud_native=False,  # ❌ Wrong for S3/GCS/Azure
)

# Proposed dynamic capability detection
def _detect_capabilities(self) -> StorageCapabilities:
    """Detect capabilities based on underlying filesystem."""
    protocol = self.protocol.lower()
    
    if protocol in ('s3', 's3a', 's3n'):
        return StorageCapabilities.s3_compatible()
    elif protocol in ('gcs', 'gs'):
        return StorageCapabilities.gcs()
    elif protocol in ('abfs', 'az', 'azure'):
        return StorageCapabilities.azure_blob()
    elif protocol in ('file', 'local'):
        return StorageCapabilities.local_filesystem()
    elif protocol == 'http':
        return StorageCapabilities(
            supports_read=True, supports_write=False,
            is_remote=True, is_cloud_native=False
        )
    else:
        # Generic fsspec capabilities
        return StorageCapabilities(
            supports_arrow=PYARROW_INSTALLED,
            is_remote=True, is_cloud_native=False
        )
```

##### 2. Instance-Level Capability Assignment

```python
# Update FSSpecBackend.__init__ to set capabilities dynamically
def __init__(self, fs: Union[str, AbstractFileSystem], base_path: str = "") -> None:
    # ... existing initialization ...
    
    # Override class-level capabilities with instance-specific ones
    self._instance_capabilities = self._detect_capabilities()

@property
def capabilities(self) -> StorageCapabilities:
    """Return instance-specific capabilities."""
    return getattr(self, '_instance_capabilities', self.__class__.capabilities)
```

##### 3. Protocol-Specific Feature Detection

```python
# Enhanced capability detection for specific features
def _detect_advanced_features(self) -> dict[str, bool]:
    """Detect advanced features from underlying filesystem."""
    features = {}
    
    # Check for S3 Select support
    if hasattr(self.fs, 'select_object_content'):
        features['supports_s3_select'] = True
    
    # Check for multipart upload
    if hasattr(self.fs, 'start_multipart') or 'multipart' in str(type(self.fs)):
        features['supports_multipart_upload'] = True
    
    # Note: FSSpec async support is sporadic - ObStore recommended for async operations
    # FSSpec will use sync operations wrapped with async_ utility
    
    return features
```

#### Code Migration Examples

##### 4. Replace isinstance() Checks in Driver Mixins

Current code in `sqlspec/driver/mixins/_storage.py`:

```python
# ❌ Before - Hard-coded type checking
def export_to_storage(self, query: SQL, storage_uri: str) -> None:
    backend = get_storage_backend(storage_uri)
    
    if isinstance(backend, ObStoreBackend):
        # Use native Arrow export
        table = self.fetch_arrow_table(query)
        backend.write_arrow(path, table)
    elif isinstance(backend, FSSpecBackend):
        # Use CSV export
        result = self.execute(query)
        backend.write_text(path, result.to_csv())
```

```python
# ✅ After - Capability-based optimization
def export_to_storage(self, query: SQL, storage_uri: str) -> None:
    backend = get_storage_backend(storage_uri)
    
    if backend.has_capability('supports_arrow') and backend.has_capability('has_low_latency'):
        # Use efficient Arrow export for high-performance backends
        table = self.fetch_arrow_table(query)
        backend.write_arrow(path, table)
    elif backend.has_capability('supports_streaming'):
        # Use streaming for large results
        for batch in self.stream_arrow_batches(query):
            backend.write_arrow_batch(path, batch, append=True)
    else:
        # Fallback to CSV
        result = self.execute(query)
        backend.write_text(path, result.to_csv())
```

##### 5. Storage Operation Optimization

```python
# New capability-based bulk operations
class SyncStorageMixin:
    def bulk_export(self, queries: list[SQL], storage_uri: str) -> None:
        backend = get_storage_backend(storage_uri)
        
        if backend.has_capability('supports_batch_operations'):
            # Use native batch operations
            tables = [self.fetch_arrow_table(q) for q in queries]
            backend.write_arrow_batch(tables)
        elif backend.has_capability('supports_multipart_upload'):
            # Use multipart upload for large exports
            with backend.multipart_writer(storage_uri) as writer:
                for query in queries:
                    table = self.fetch_arrow_table(query)
                    writer.write_part(table)
        else:
            # Sequential fallback
            for i, query in enumerate(queries):
                self.export_to_storage(query, f"{storage_uri}/part_{i}.parquet")
```

#### Migration Locations

**Files requiring isinstance() → capability migration:**

1. `sqlspec/driver/mixins/_storage.py` - Storage operation optimizations
2. `sqlspec/storage/registry.py` - Backend selection logic  
3. `sqlspec/adapters/*/driver.py` - Adapter-specific storage integration
4. `tests/integration/test_storage/` - Test backend selection

**Example migration in storage registry:**

```python
# Before
def select_optimal_backend(uri: str) -> type[ObjectStoreBase]:
    if uri.startswith('s3://'):
        return ObStoreBackend if OBSTORE_INSTALLED else FSSpecBackend
    return FSSpecBackend

# After  
def select_optimal_backend(uri: str, required_capabilities: set[str]) -> type[ObjectStoreBase]:
    backends = [ObStoreBackend, FSSpecBackend]
    
    for backend_cls in backends:
        if not backend_cls.is_available():
            continue
        
        # Check if backend supports required capabilities
        backend_caps = backend_cls.capabilities
        if all(backend_caps.has_capability(cap) for cap in required_capabilities):
            return backend_cls
    
    raise StorageOperationFailedError(f"No backend supports: {required_capabilities}")
```

#### Benefits of Enhanced Implementation

1. **FSSpec Protocol Awareness**: Correctly detect S3/GCS/Azure capabilities
2. **Optimized Operations**: Choose best strategy based on backend features  
3. **Future-Proof**: Easy to add new backends/capabilities
4. **Performance**: Avoid inefficient operations on incompatible backends
5. **Type Safety**: Replace isinstance() with capability queries

#### Implementation Steps

1. **Week 1**: Enhance FSSpec capability detection
2. **Week 2**: Migrate storage mixin optimizations
3. **Week 3**: Update storage registry selection logic
4. **Week 4**: Replace all isinstance() checks in adapters
5. **Week 5**: Add comprehensive capability tests

### Phase 3D: Complete Protocol Adoption

1. Update all function signatures to use protocol types
2. Replace remaining hasattr() patterns
3. Ensure consistent type safety

## Migration Timeline

1. **Week 1**: Fix test failures (Phase 3A)
2. **Week 2**: Pilot SQLite adapter migration
3. **Week 3-4**: Roll out to all adapters
4. **Week 5**: Implement storage capabilities
5. **Week 6**: Complete protocol adoption

## Benefits

- **DRY**: No duplicate connection/parameter code
- **Type Safety**: Better static analysis with protocols
- **Performance**: Capability-based optimizations
- **Maintainability**: Centralized utilities
- **Extensibility**: New adapters are simpler

## Risks

- Test suite disruption during migration
- Potential performance regression if not careful
- Breaking changes for custom adapters

## Success Criteria

1. All tests passing
2. All adapters using consolidated utilities
3. Zero hasattr() usage (replaced with protocols)
4. Storage operations optimized via capabilities
5. Improved type coverage metrics
