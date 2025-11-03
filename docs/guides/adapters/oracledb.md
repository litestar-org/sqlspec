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

config = OracleAsyncConfig(pool_config={"dsn": "oracle://..."})

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

config = OracleAsyncConfig(pool_config={"dsn": "oracle://..."})

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

Oracle returns unquoted identifiers in uppercase (e.g., `ID`, `PRODUCT_NAME`). When these rows are used to hydrate typed schemas (like msgspec or Pydantic) that expect lowercase, snake_case fields, the mismatch can cause validation errors.

To prevent this, SQLSpec provides the `enable_lowercase_column_names` driver feature, which is **enabled by default**. This feature automatically normalizes implicitly uppercased column names to lowercase, ensuring seamless schema hydration without needing to alias every column in your SQL queries.

### How It Works

- **Normalization**: Identifiers matching Oracle's implicit uppercase pattern (e.g., `PRODUCT_ID`, `CUSTOMER_NAME`) are converted to lowercase (`product_id`, `customer_name`).
- **Preservation**: Quoted or user-defined aliases that are not implicitly uppercased (e.g., `"ProductId"`, `customerName`) retain their original casing.

### When to Disable It

You only need to configure this feature if you want to **disable** it. Set `enable_lowercase_column_names=False` in the following scenarios:

- You rely on two columns that differ only by case (e.g., `ID` and `Id`).
- You intentionally alias all columns in uppercase and want to preserve that style.
- You prefer to manage all column casing manually in SQL using quoted identifiers.

To disable the feature:

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig

config = OracleAsyncConfig(
    pool_config={"dsn": "oracle://..."},
    driver_features={"enable_lowercase_column_names": False},
)
```

For most applications, the default behavior provides the best experience.

## NumPy Vector Support (Oracle 23ai+)

Oracle Database 23ai introduces the `VECTOR` data type for AI/ML embeddings and similarity search. SQLSpec provides seamless NumPy integration for automatic conversion between NumPy arrays and Oracle VECTOR columns.

### Overview

The VECTOR data type stores high-dimensional vectors efficiently and supports:

- **Vector similarity search** (cosine, euclidean, dot product)
- **Multiple precision levels** (FLOAT32, FLOAT64, INT8, BINARY)
- **Flexible dimensions** (e.g., 768 for BERT, 1536 for OpenAI embeddings)
- **Optimized storage and indexing** for large-scale vector operations

### Configuration

NumPy vector support is **auto-enabled** when `sqlspec` detects that the `numpy` package is installed. You only need to explicitly configure this feature if you want to disable it when NumPy is present.

To override the default behavior, use `driver_features`:

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig

# Explicitly disable NumPy integration
config = OracleAsyncConfig(
    pool_config={
        "dsn": "oracle://host:port/service_name",
        "user": "username",
        "password": "password",
    },
    driver_features={
        "enable_numpy_vectors": False  # Override auto-detection
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

## Common Issues & Troubleshooting

- **`ORA-12154: TNS:could not resolve the connect identifier specified`**: This usually means the `tnsnames.ora` file is not found or the DSN is incorrect. Ensure the wallet path is correct.
- **`ORA-28759: failure to open file`**: Wallet file permission issue. Ensure the application has read access to the wallet files.
- **`ORA-00439: feature not enabled: Oracle Database In-Memory`**: In-Memory option not licensed. Set `in_memory=False` or acquire license.
- **`ORA-62142: INMEMORY attribute cannot be specified`**: `INMEMORY_SIZE` parameter is 0. Either increase it or set `in_memory=False`.
- **Performance:** If experiencing performance issues, investigate whether Thick Mode is required for your use case.
