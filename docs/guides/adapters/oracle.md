---
orphan: true
---

# OracleDB Adapter Guide

This guide provides specific instructions and best practices for working with the `oracledb` adapter in `sqlspec`.

## Key Information

- **Driver:** `oracledb`
- **Parameter Style:** `named` (e.g., `:name`)

## Thick vs. Thin Client

The `oracledb` driver supports two modes:

- **Thin Mode:** Default, no Oracle Client libraries needed. This is the preferred mode for ease of use and deployment.
- **Thick Mode:** Requires Oracle Instant Client libraries to be installed. It may offer better performance in some scenarios and is required for certain advanced features.

**Implementation Guidance:**

- When developing, prioritize compatibility with **Thin Mode**.
- If a feature requires Thick Mode, it must be explicitly documented and tested.
- Use `Context7` to get the latest `oracledb` documentation on how to initialize the client in each mode.

## Autonomous Database Connectivity

Connecting to Oracle Autonomous Database (e.g., on OCI or Google Cloud) requires a wallet.

**Workflow:**

1. **Securely Obtain Wallet:** The wallet files (`tnsnames.ora`, `sqlnet.ora`, `ewallet.p12`, etc.) must be available in a secure location.
2. **Configuration:** The `oracledb` connection parameters need to be configured to use the wallet. This typically involves setting the `config_dir` and `dsn` parameters in the connection configuration.
3. **Research:** Use `google_web_search` for "python oracledb connect to autonomous database" to get the most up-to-date connection string examples and best practices.

## Oracle Database In-Memory

Oracle Database In-Memory is a paid feature (licensed separately from Enterprise Edition) that provides significant performance improvements for read-heavy workloads.

### What is Oracle In-Memory?

The Oracle Database In-Memory Column Store enables tables, partitions, or materialized views to be populated into memory in a columnar format, optimized for analytic queries while maintaining row format for transactional operations.

**Key Benefits:**

- **Faster Queries:** 10x-100x performance improvement for analytic and reporting queries
- **Transparent:** No application changes needed - same SQL works on both row and column formats
- **Dual Format:** Data maintained in both row (for OLTP) and column (for analytics) formats

### Litestar Session Store In-Memory Support

The Oracle session stores (`OracleAsyncStore` and `OracleSyncStore`) support optional In-Memory Column Store via the `use_in_memory` parameter:

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig
from sqlspec.adapters.oracledb.litestar import OracleAsyncStore

config = OracleAsyncConfig(pool_config={"dsn": "oracle://..."})

# Standard table (default)
store = OracleAsyncStore(config)

# In-Memory enabled table (requires license)
store_inmem = OracleAsyncStore(config, use_in_memory=True)
await store_inmem.create_table()
```

**When to Use In-Memory for Sessions:**

- ✅ High-volume session lookups (>1000 req/sec)
- ✅ Large session tables (>1M rows)
- ✅ Complex session queries with analytics
- ❌ Small deployments (<100 concurrent users)
- ❌ Budget-constrained projects (paid feature)
- ❌ Primarily write-heavy workloads

### Requirements

1. **Oracle Database Version:** 12.1.0.2 or higher (12c+)
2. **License:** Oracle Database In-Memory option license required
3. **Configuration:** `INMEMORY_SIZE` parameter must be set > 0

### Detection and Validation

Check if In-Memory is available:

```sql
-- Check INMEMORY_SIZE parameter
SELECT NAME, VALUE FROM V$PARAMETER WHERE NAME = 'inmemory_size';

-- If VALUE is 0, In-Memory is disabled
-- If VALUE > 0, In-Memory is available
```

### Error Handling

If `use_in_memory=True` but In-Memory is not available/licensed, table creation will fail with:

- **ORA-00439:** Feature not enabled
- **ORA-62142:** INMEMORY clause specified but INMEMORY_SIZE is 0

**Recommendation:** Use `use_in_memory=False` (default) unless you have confirmed licensing and configuration.

## Column Name Normalization

Oracle returns unquoted identifiers in uppercase (for example `ID`, `PRODUCT_NAME`). When those rows feed into schema libraries that expect snake_case fields, the uppercase keys can trigger validation errors. SQLSpec resolves this automatically through the `enable_lowercase_column_names` driver feature, which is **enabled by default**.

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig

config = OracleAsyncConfig(
    pool_config={"dsn": "oracle://..."},
    driver_features={"enable_lowercase_column_names": True},
)
```

### How normalization works

- Identifiers matching Oracle's implicit uppercase pattern (`^(?!\d)(?:[A-Z0-9_]+)$`) are lowercased.
- Quoted or user-defined aliases (mixed case, symbols, or names beginning with digits) retain their original casing.
- Disabling the feature restores Oracle's native uppercase behaviour:

```python
config = OracleAsyncConfig(
    pool_config={"dsn": "oracle://..."},
    driver_features={"enable_lowercase_column_names": False},
)
```

### When to opt out

- You rely on two columns that differ only by case (for example `ID` and `Id`).
- You intentionally alias everything in uppercase and want to preserve that style.
- You prefer to manage casing entirely in SQL using quoted identifiers.

In those scenarios set `enable_lowercase_column_names=False`. Otherwise, keep the default for seamless msgspec/pydantic hydration without extra SQL aliases.

## NumPy Vector Support (Oracle 23ai+)

Oracle Database 23ai introduces the `VECTOR` data type for AI/ML embeddings and similarity search. SQLSpec provides seamless NumPy integration for automatic conversion between NumPy arrays and Oracle VECTOR columns.

### Overview

The VECTOR data type stores high-dimensional vectors efficiently and supports:

- **Vector similarity search** (cosine, euclidean, dot product)
- **Multiple precision levels** (FLOAT32, FLOAT64, INT8, BINARY)
- **Flexible dimensions** (e.g., 768 for BERT, 1536 for OpenAI embeddings)
- **Optimized storage and indexing** for large-scale vector operations

### Configuration

Enable NumPy vector support via `driver_features`:

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig

config = OracleAsyncConfig(
    pool_config={
        "dsn": "oracle://host:port/service_name",
        "user": "username",
        "password": "password",
    },
    driver_features={
        "enable_numpy_vectors": True  # Enable automatic NumPy conversion
    }
)
```

### Supported Data Types

| NumPy dtype | Oracle VECTOR Type | Array Code | Use Case |
|-------------|-------------------|------------|----------|
| `float32` | `VECTOR(*, FLOAT32)` | `f` | General embeddings (default) |
| `float64` | `VECTOR(*, FLOAT64)` | `d` | High-precision embeddings |
| `int8` | `VECTOR(*, INT8)` | `b` | Quantized embeddings |
| `uint8` | `VECTOR(*, BINARY)` | `B` | Binary/hash vectors |

### Basic Usage

#### Create Table with VECTOR Column

```python
async with config.provide_session() as session:
    await session.execute("""
        CREATE TABLE embeddings (
            id NUMBER PRIMARY KEY,
            text VARCHAR2(4000),
            embedding VECTOR(768, FLOAT32)
        )
    """)
```

#### Insert NumPy Array (Automatic Conversion)

```python
import numpy as np

# Create embedding vector
vector = np.random.rand(768).astype(np.float32)

await session.execute(
    "INSERT INTO embeddings VALUES (:1, :2, :3)",
    (1, "sample text", vector)
)
```

#### Retrieve as NumPy Array (Automatic Conversion)

```python
result = await session.select_one(
    "SELECT * FROM embeddings WHERE id = :1",
    (1,)
)

embedding = result["EMBEDDING"]
assert isinstance(embedding, np.ndarray)
assert embedding.dtype == np.float32
assert embedding.shape == (768,)
```

### Vector Similarity Search

```python
# Find top 5 most similar embeddings using cosine similarity
query_vector = np.random.rand(768).astype(np.float32)

results = await session.select_all("""
    SELECT id, text,
           VECTOR_DISTANCE(embedding, :1, COSINE) as distance
    FROM embeddings
    ORDER BY distance
    FETCH FIRST 5 ROWS ONLY
""", (query_vector,))

for row in results:
    print(f"{row['TEXT']}: distance={row['DISTANCE']}")
```

### Manual Conversion API

For advanced use cases where automatic conversion is disabled, use the type converter:

```python
from sqlspec.adapters.oracledb.type_converter import OracleTypeConverter

converter = OracleTypeConverter()

# NumPy → Oracle VECTOR
oracle_array = converter.convert_numpy_to_vector(numpy_array)

# Oracle VECTOR → NumPy
numpy_array = converter.convert_vector_to_numpy(oracle_array)
```

### Multiple Vector Types in One Table

```python
await session.execute("""
    CREATE TABLE ml_models (
        model_id NUMBER PRIMARY KEY,
        weights_f32 VECTOR(1024, FLOAT32),
        weights_f64 VECTOR(1024, FLOAT64),
        quantized_i8 VECTOR(1024, INT8),
        binary_hash VECTOR(256, BINARY)
    )
""")

# Each column automatically converts to/from appropriate NumPy dtype
await session.execute(
    "INSERT INTO ml_models VALUES (:1, :2, :3, :4, :5)",
    (
        1,
        np.random.rand(1024).astype(np.float32),
        np.random.rand(1024).astype(np.float64),
        np.random.randint(-128, 127, 1024, dtype=np.int8),
        np.random.randint(0, 256, 256, dtype=np.uint8),
    )
)
```

### Requirements

1. **Oracle Database 23ai or higher** (VECTOR type introduced in 23ai)
2. **NumPy installed** (`pip install numpy`) - optional but required for automatic conversion
3. **`enable_numpy_vectors=True`** in driver_features (opt-in design)

### Limitations

- **Sparse vectors** require Oracle 23.7+ (future support planned)
- **Dimension limits** depend on Oracle version and configuration
- **Automatic conversion** only active when feature enabled
- **Graceful fallback** to `array.array` if NumPy not installed

### Performance Considerations

- **Array copying**: Uses `copy=True` for safety (prevents data corruption)
- **Handler overhead**: <1ms per connection (negligible in practice)
- **Large vectors**: Efficient for embeddings up to 10K+ dimensions
- **Batch operations**: No performance penalty for bulk inserts

### Error Handling

**Unsupported dtype:**

```python
vector = np.array([1.0, 2.0], dtype=np.float16)  # Not supported
# Raises: TypeError: Unsupported NumPy dtype for Oracle VECTOR: float16
```

**Dimension mismatch:**

```python
vector = np.random.rand(512).astype(np.float32)
# Trying to insert into VECTOR(768, FLOAT32) column
# Raises: ORA-51813: Vector dimension count must match
```

**NumPy not installed:**

```python
# With enable_numpy_vectors=True but NumPy not installed
# Falls back to array.array (Python stdlib)
```

### Troubleshooting

- **`ImportError: NumPy is not installed`**: Install NumPy: `pip install numpy`
- **`TypeError: Unsupported NumPy dtype`**: Use float32, float64, int8, or uint8 only
- **`ORA-51813: Vector dimension count must match`**: Ensure array length matches column definition
- **No automatic conversion**: Verify `enable_numpy_vectors=True` in driver_features
- **`ORA-00904: "VECTOR": invalid identifier`**: Requires Oracle Database 23ai or higher

## Common Issues & Troubleshooting

- **`ORA-12154: TNS:could not resolve the connect identifier specified`**: This usually means the `tnsnames.ora` file is not found or the DSN is incorrect. Ensure the wallet path is correct.
- **`ORA-28759: failure to open file`**: Wallet file permission issue. Ensure the application has read access to the wallet files.
- **`ORA-00439: feature not enabled: Oracle Database In-Memory`**: In-Memory option not licensed. Set `use_in_memory=False` or acquire license.
- **`ORA-62142: INMEMORY attribute cannot be specified`**: `INMEMORY_SIZE` parameter is 0. Either increase it or set `use_in_memory=False`.
- **Performance:** If experiencing performance issues, investigate whether Thick Mode is required for your use case.
