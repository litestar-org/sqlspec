---
orphan: true
---

# OracleDB Adapter Guide

This guide provides specific instructions and best practices for working with the `oracledb` adapter in `sqlspec`.

## Key Information

- **Driver:** `oracledb`
- **Parameter Style:** `named` (e.g., `:name`)

## Parameter Profile

- **Registry Key:** `"oracledb"`
- **JSON Strategy:** `helper` (shared JSON serializer applied through the profile)
- **Extras:** None (uses defaults with native list expansion disabled)

## Query Stack Support

`StatementStack` executions automatically use python-oracledb's native pipeline APIs when the adapter detects a compatible runtime (Oracle Database 23ai+ and python-oracledb ≥ 2.4.0). The pipeline path batches every operation in a stack into a single round-trip while preserving the regular `StackResult.result` semantics, so downstream helpers like `get_data()` or `rows_affected` continue to work without code changes.

### Requirements

- Oracle Database 23ai or newer (`SELECT version FROM v$instance`)
- python-oracledb 2.4.0 or newer (thin **or** thick mode)
- Stacks that only contain `push_execute`/`push_execute_many` operations. `push_execute_arrow` and `push_execute_script` fall back to sequential execution automatically.

### Telemetry and Overrides

- Every stack execution emits `StackExecutionMetrics` counters (e.g., `stack.execute.invocations`, `stack.execute.path.native`, `stack.execute.partial_errors`) and a `sqlspec.stack.execute` tracing span whenever `ObservabilityRuntime` is enabled. These metrics include tags for the adapter, fail-fast vs. continue-on-error mode, native vs. sequential path, and the forced-disable flag so operators can chart adoption and error rates.
- When the pipeline is disabled because of driver/database version constraints, the adapter logs `stack.native_pipeline.skip` at `DEBUG` with reason codes such as `driver_version`, `database_version`, or `driver_api_missing` to make diagnosis straightforward.
- `driver_features={"stack_native_disabled": True}` forces sequential execution if you need to bypass the pipeline temporarily.

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

The Oracle session stores (`OracleAsyncStore` and `OracleSyncStore`) support optional In-Memory Column Store via the `in_memory` parameter:

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig
from sqlspec.adapters.oracledb.litestar import OracleAsyncStore

config = OracleAsyncConfig(connection_config={"dsn": "oracle://..."})

# Standard table (default)
store = OracleAsyncStore(config)

# In-Memory enabled table (requires license)
store_inmem = OracleAsyncStore(config, in_memory=True)
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

If `in_memory=True` but In-Memory is not available/licensed, table creation will fail with:

- **ORA-00439:** Feature not enabled
- **ORA-62142:** INMEMORY clause specified but INMEMORY_SIZE is 0

**Recommendation:** Use `in_memory=False` (default) unless you have confirmed licensing and configuration.

## CLOB/BLOB Handling

Oracle's `oracledb` driver returns LOB (Large Object) handles for CLOB and BLOB columns, which must be read before use. SQLSpec **automatically reads CLOB columns into strings** to provide seamless integration with typed schemas like msgspec and Pydantic.

### Automatic CLOB Hydration

CLOB values are automatically read and converted to Python strings:

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig
import msgspec

class Article(msgspec.Struct):
    id: int
    title: str
    content: str  # CLOB column automatically becomes string

config = OracleAsyncConfig(connection_config={"dsn": "oracle://..."})

async with config.provide_session() as session:
    # Insert large text content
    large_text = "x" * 5000  # >4KB content
    await session.execute(
        "INSERT INTO articles (id, title, content) VALUES (:1, :2, :3)",
        (1, "My Article", large_text)
    )

    # Query returns string, not LOB handle
    result = await session.execute(
        "SELECT id, title, content FROM articles WHERE id = :1",
        (1,)
    )

    # Works seamlessly with msgspec
    article = result.get_first(schema_type=Article)
    assert isinstance(article.content, str)
    assert len(article.content) == 5000
```

### JSON Detection in CLOBs

When CLOB content contains JSON, it is automatically detected and parsed into Python dictionaries:

```python
import json

class Document(msgspec.Struct):
    id: int
    metadata: dict  # JSON stored in CLOB

# Store JSON in CLOB
metadata = {"key": "value", "nested": {"data": "example"}}
await session.execute(
    "INSERT INTO documents (id, metadata) VALUES (:1, :2)",
    (1, json.dumps(metadata))
)

# Retrieved as parsed dict, not string
result = await session.execute(
    "SELECT id, metadata FROM documents WHERE id = :1",
    (1,)
)
doc = result.get_first(schema_type=Document)
assert isinstance(doc.metadata, dict)
assert doc.metadata["key"] == "value"
```

### BLOB Handling (Binary Data)

BLOB columns remain as bytes and are not converted to strings:

```python
class FileRecord(msgspec.Struct):
    id: int
    data: bytes  # BLOB column remains bytes

binary_data = b"\x00\x01\x02\x03" * 2000
await session.execute(
    "INSERT INTO files (id, data) VALUES (:1, :2)",
    (1, binary_data)
)

result = await session.execute(
    "SELECT id, data FROM files WHERE id = :1",
    (1,)
)
file_record = result.get_first(schema_type=FileRecord)
assert isinstance(file_record.data, bytes)
```

### Before and After

**Before (manual workaround required):**

```python
# Had to use DBMS_LOB.SUBSTR, truncating to 4000 chars
result = await session.execute(
    "SELECT id, DBMS_LOB.SUBSTR(content, 4000) as content FROM articles"
)
```

**After (automatic, no truncation):**

```python
# CLOB automatically read to full string
result = await session.execute(
    "SELECT id, content FROM articles"
)
```

### Performance Considerations

- **Memory usage:** Large CLOBs (>100MB) are fully materialized into memory. For multi-GB CLOBs, consider using database-side processing or pagination.
- **Sync vs Async:** Both sync and async drivers perform automatic CLOB hydration with equivalent performance.
- **Multiple CLOBs:** All CLOB columns in a result row are hydrated automatically.

## Column Name Normalization

Oracle returns unquoted identifiers in uppercase (for example `ID`, `PRODUCT_NAME`). When those rows feed into schema libraries that expect snake_case fields, the uppercase keys can trigger validation errors. SQLSpec resolves this automatically through the `enable_lowercase_column_names` driver feature, which is **enabled by default**.

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig

config = OracleAsyncConfig(
    connection_config={"dsn": "oracle://..."},
    driver_features={"enable_lowercase_column_names": True},
)
```

### How normalization works

- Identifiers matching Oracle's implicit uppercase pattern (`^(?!\d)(?:[A-Z0-9_]+)$`) are lowercased.
- Quoted or user-defined aliases (mixed case, symbols, or names beginning with digits) retain their original casing.
- Disabling the feature restores Oracle's native uppercase behaviour:

```python
config = OracleAsyncConfig(
    connection_config={"dsn": "oracle://..."},
    driver_features={"enable_lowercase_column_names": False},
)
```

### When to opt out

- You rely on two columns that differ only by case (for example `ID` and `Id`).
- You intentionally alias everything in uppercase and want to preserve that style.
- You prefer to manage casing entirely in SQL using quoted identifiers.

In those scenarios set `enable_lowercase_column_names=False`. Otherwise, keep the default for seamless msgspec/pydantic hydration without extra SQL aliases.

## UUID Binary Storage

Oracle databases commonly use `RAW(16)` columns for UUID storage to achieve 50% space savings compared to `VARCHAR2(36)`. SQLSpec provides automatic conversion between Python UUID objects and Oracle `RAW(16)` binary format, eliminating the need for manual `.bytes` conversion.

### Overview

This feature enables seamless UUID handling with optimal storage efficiency:

- **Zero configuration required** - enabled by default (uses Python stdlib uuid)
- **50% storage savings** - 16 bytes (RAW) vs 36 bytes (VARCHAR2)
- **Type-safe** - Python UUID objects in code, binary storage in database
- **Automatic bidirectional conversion** - insert UUID objects, retrieve UUID objects
- **Graceful fallback** - non-UUID binary data remains as bytes

### Basic Usage

```python
import uuid
from sqlspec.adapters.oracledb import OracleAsyncConfig

config = OracleAsyncConfig(connection_config={"dsn": "oracle://..."})

async with config.provide_session() as session:
    # Create table with RAW(16) for UUID storage
    await session.execute("""
        CREATE TABLE users (
            id NUMBER PRIMARY KEY,
            user_id RAW(16) NOT NULL,
            email VARCHAR2(255)
        )
    """)

    # Insert UUID object directly (automatic conversion)
    user_id = uuid.uuid4()
    await session.execute(
        "INSERT INTO users (id, user_id, email) VALUES (:1, :2, :3)",
        (1, user_id, "user@example.com")
    )

    # Retrieve as UUID object (automatic conversion)
    result = await session.select_one(
        "SELECT user_id, email FROM users WHERE id = :1",
        (1,)
    )
    assert isinstance(result["user_id"], uuid.UUID)
    assert result["user_id"] == user_id
```

### Storage Comparison

| Storage Type | Size | Format | Index Size | Notes |
|--------------|------|--------|------------|-------|
| `VARCHAR2(36)` | 36 bytes | `'550e8400-e29b-41d4-a716-446655440000'` | Large | String storage |
| `RAW(16)` | 16 bytes | Binary (16 bytes) | Small | Binary storage (50% savings) |

### Configuration

UUID binary conversion is enabled by default (no configuration required):

```python
config = OracleAsyncConfig(
    connection_config={"dsn": "oracle://..."},
    driver_features={
        "enable_uuid_binary": True  # Default: True (stdlib, always available)
    }
)
```

To disable automatic conversion:

```python
config = OracleAsyncConfig(
    connection_config={"dsn": "oracle://..."},
    driver_features={
        "enable_uuid_binary": False  # Revert to manual .bytes conversion
    }
)
```

### NULL Handling

NULL values are handled correctly in both directions:

```python
# Insert NULL
await session.execute(
    "INSERT INTO users (id, user_id, email) VALUES (:1, :2, :3)",
    (2, None, "null@example.com")
)

# Retrieve NULL
result = await session.select_one(
    "SELECT user_id FROM users WHERE id = :1",
    (2,)
)
assert result["user_id"] is None
```

### UUID Variants

All RFC 4122 UUID variants are supported:

```python
import uuid

# UUID v1 (timestamp-based)
uuid1 = uuid.uuid1()
await session.execute(
    "INSERT INTO users (id, user_id) VALUES (:1, :2)",
    (1, uuid1)
)

# UUID v4 (random)
uuid4 = uuid.uuid4()
await session.execute(
    "INSERT INTO users (id, user_id) VALUES (:1, :2)",
    (2, uuid4)
)

# UUID v5 (namespace + name)
uuid5 = uuid.uuid5(uuid.NAMESPACE_DNS, "example.com")
await session.execute(
    "INSERT INTO users (id, user_id) VALUES (:1, :2)",
    (3, uuid5)
)
```

### Bulk Operations

Bulk inserts with UUID parameters work seamlessly:

```python
user_data = [(i, uuid.uuid4(), f"user{i}@example.com") for i in range(1, 101)]

await session.executemany(
    "INSERT INTO users (id, user_id, email) VALUES (:1, :2, :3)",
    user_data
)
```

### Edge Cases

#### Non-UUID Binary Data in RAW(16)

If a `RAW(16)` column contains non-UUID binary data, the handler gracefully falls back to bytes:

```python
import os

# Insert random bytes (not a valid UUID)
random_bytes = os.urandom(16)
await session.execute(
    "INSERT INTO users (id, user_id) VALUES (:1, :2)",
    (999, random_bytes)
)

# Retrieved as bytes (not UUID)
result = await session.select_one(
    "SELECT user_id FROM users WHERE id = :1",
    (999,)
)
assert isinstance(result["user_id"], bytes)
assert result["user_id"] == random_bytes
```

#### RAW Columns with Other Sizes

Only `RAW(16)` columns are converted to UUID. Other sizes remain as bytes:

```python
await session.execute("""
    CREATE TABLE binary_data (
        id NUMBER PRIMARY KEY,
        uuid_col RAW(16),    -- Converted to UUID
        hash_col RAW(32),    -- Remains bytes
        small_col RAW(4)     -- Remains bytes
    )
""")

await session.execute(
    "INSERT INTO binary_data VALUES (:1, :2, :3, :4)",
    (1, uuid.uuid4(), os.urandom(32), os.urandom(4))
)

result = await session.select_one("SELECT * FROM binary_data WHERE id = :1", (1,))
assert isinstance(result["uuid_col"], uuid.UUID)  # Converted
assert isinstance(result["hash_col"], bytes)      # Not converted
assert isinstance(result["small_col"], bytes)     # Not converted
```

#### VARCHAR2 UUID Columns

String-based UUID columns are not automatically converted:

```python
await session.execute("""
    CREATE TABLE legacy_users (
        id NUMBER PRIMARY KEY,
        user_id VARCHAR2(36)  -- String UUID, not converted
    )
""")

# String UUIDs require manual str() conversion
user_id = uuid.uuid4()
await session.execute(
    "INSERT INTO legacy_users (id, user_id) VALUES (:1, :2)",
    (1, str(user_id))  # Manual conversion required
)

result = await session.select_one("SELECT user_id FROM legacy_users WHERE id = :1", (1,))
assert isinstance(result["user_id"], str)
assert uuid.UUID(result["user_id"]) == user_id
```

### Migration from VARCHAR2 to RAW(16)

To migrate existing string-based UUID columns to binary format:

```sql
-- Step 1: Add new RAW(16) column
ALTER TABLE users ADD user_id_binary RAW(16);

-- Step 2: Convert existing data
UPDATE users
SET user_id_binary = HEXTORAW(REPLACE(user_id, '-', ''))
WHERE user_id IS NOT NULL;

-- Step 3: Drop old column and rename
ALTER TABLE users DROP COLUMN user_id;
ALTER TABLE users RENAME COLUMN user_id_binary TO user_id;
```

After migration, Python code automatically works with UUID objects (no code changes needed).

### Handler Chaining with NumPy Vectors

UUID handlers coexist with other type handlers (e.g., NumPy vectors) through handler chaining:

```python
config = OracleAsyncConfig(
    connection_config={"dsn": "oracle://..."},
    driver_features={
        "enable_numpy_vectors": True,  # NumPy vector support
        "enable_uuid_binary": True      # UUID binary support
    }
)

# Both features work together
await session.execute(
    "INSERT INTO ml_data (id, model_id, embedding) VALUES (:1, :2, :3)",
    (1, uuid.uuid4(), np.random.rand(768).astype(np.float32))
)
```

Handler registration order:
1. NumPy handlers registered first (if enabled)
2. UUID handlers registered second, chaining to NumPy handlers

This ensures both types of conversions work without conflicts.

### Performance

- **Conversion overhead**: <1% vs manual `UUID.bytes` conversion
- **Storage savings**: 50% (16 bytes vs 36 bytes)
- **Index efficiency**: Smaller indexes, faster lookups
- **Network efficiency**: 50% fewer bytes transferred

### Before and After

**Before (manual conversion required):**

```python
user_id = uuid.uuid4()

# Insert - manual .bytes conversion
await session.execute(
    "INSERT INTO users (id, user_id) VALUES (:1, :2)",
    (1, user_id.bytes)
)

# Retrieve - manual UUID() construction
result = await session.select_one("SELECT user_id FROM users WHERE id = :1", (1,))
user_id_retrieved = uuid.UUID(bytes=result["user_id"])
```

**After (automatic conversion):**

```python
user_id = uuid.uuid4()

# Insert - UUID object works directly
await session.execute(
    "INSERT INTO users (id, user_id) VALUES (:1, :2)",
    (1, user_id)
)

# Retrieve - returns UUID object
result = await session.select_one("SELECT user_id FROM users WHERE id = :1", (1,))
assert isinstance(result["user_id"], uuid.UUID)
assert result["user_id"] == user_id
```

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
    connection_config={
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

## MERGE Operations

Oracle supports MERGE operations with high-performance bulk upserts using `JSON_TABLE()` for efficient multi-row operations.

### Single Row Upsert

```python
from sqlspec import sql

async with config.provide_session() as session:
    query = (
        sql.merge_
        .into("products", alias="t")
        .using({"id": 1, "name": "Widget", "price": 19.99}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
        .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
    )

    result = await session.execute(query)
    print(f"Rows affected: {result.rows_affected}")
```

### Bulk Upsert with JSON_TABLE (High Performance)

For 100+ rows, Oracle automatically uses `JSON_TABLE()` for optimal performance:

```python
from decimal import Decimal

products = [
    {"id": 1, "name": "Widget", "price": Decimal("19.99")},
    {"id": 2, "name": "Gadget", "price": Decimal("29.99")},
    # ... up to 1000+ rows
]

query = (
    sql.merge_
    .into("products", alias="t")
    .using(products, alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)

result = await session.execute(query)
print(f"Upserted {result.rows_affected} rows")
```

Generated SQL uses Oracle's efficient JSON_TABLE strategy:

```sql
MERGE INTO products t
USING (
  SELECT * FROM JSON_TABLE(
    :data,
    '$[*]' COLUMNS(
      id NUMBER PATH '$.id',
      name VARCHAR2(4000) PATH '$.name',
      price NUMBER PATH '$.price'
    )
  )
) src
ON (t.id = src.id)
WHEN MATCHED THEN UPDATE SET t.name = src.name, t.price = src.price
WHEN NOT MATCHED THEN INSERT (id, name, price) VALUES (src.id, src.name, src.price)
```

### Oracle Bind Variable Limit Handling

Oracle has a 1000 bind variable limit per statement. SQLSpec automatically handles this by:

1. Using multi-row VALUES for small datasets (<100 rows)
2. Switching to JSON_TABLE for larger datasets (100+ rows)
3. Avoiding bind variable limits entirely with JSON-based approach

This means you can safely upsert 1000+ rows without hitting Oracle's limit:

```python
large_dataset = [{"id": i, "name": f"Item {i}", "price": i * 10} for i in range(1, 2001)]

query = sql.merge_.into("products", alias="t").using(large_dataset, alias="src").on("t.id = src.id")
# JSON_TABLE automatically used - no bind variable limit issues
result = await session.execute(query)
print(f"Upserted {result.rows_affected} rows")  # Works with 2000 rows!
```

### NULL Value Handling in Bulk Operations

Oracle MERGE correctly handles NULL values in bulk data:

```python
products_with_nulls = [
    {"id": 1, "name": "Widget", "price": None},
    {"id": 2, "name": None, "price": Decimal("29.99")},
]

query = (
    sql.merge_
    .into("products", alias="t")
    .using(products_with_nulls, alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)

result = await session.execute(query)
```

### Unified Upsert API

Use `sql.upsert()` for database-agnostic upsert operations:

```python
upsert_query = (
    sql.upsert("products", dialect="oracle")
    .using([{"id": 1, "name": "Widget", "price": 19.99}], alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)
```

For comprehensive examples and migration guides, see:

- [MERGE Statement Builder Guide](/guides/builder/merge.md)
- [Unified Upsert API Guide](/guides/upsert.md)

## Event Channels

- Set `driver_features["events_backend"] = "oracle_aq"` to enable native
  Advanced Queuing support. Event publishing uses `connection.queue()` and
  inherits the AQ options surfaced via `extension_config["events"]`
  (`aq_queue`, `aq_wait_seconds`, `aq_visibility`).
- AQ requires DBA-provisioned queues plus enqueue/dequeue privileges. When the
  driver detects missing privileges it logs a warning and falls back to the
  durable queue backend automatically.
- The queue fallback uses the same hints as other adapters, so you can reuse
  `extension_config["events"]["lease_seconds"]`, `poll_interval`, etc., when
  AQ cannot be enabled.

## Common Issues & Troubleshooting

- **`ORA-12154: TNS:could not resolve the connect identifier specified`**: This usually means the `tnsnames.ora` file is not found or the DSN is incorrect. Ensure the wallet path is correct.
- **`ORA-28759: failure to open file`**: Wallet file permission issue. Ensure the application has read access to the wallet files.
- **`ORA-00439: feature not enabled: Oracle Database In-Memory`**: In-Memory option not licensed. Set `in_memory=False` or acquire license.
- **`ORA-62142: INMEMORY attribute cannot be specified`**: `INMEMORY_SIZE` parameter is 0. Either increase it or set `in_memory=False`.
- **Performance:** If experiencing performance issues, investigate whether Thick Mode is required for your use case.
