# Apache Arrow Integration

SQLSpec provides seamless integration with Apache Arrow for high-performance data interchange. The `select_to_arrow()` method enables efficient data retrieval in Arrow format across all database adapters.

## Overview

Apache Arrow is a columnar memory format designed for high-performance analytics and data interchange. SQLSpec's Arrow integration provides:

- **Zero-copy performance**: Native Arrow support for ADBC and DuckDB adapters (5-10x faster)
- **Universal compatibility**: All 10 database adapters support Arrow output
- **Framework integration**: Seamless conversion to pandas, Polars, and other data science tools
- **Flexible formats**: Support for both Arrow Table and RecordBatch formats

## Architecture

### Two-Path Strategy

SQLSpec implements two distinct paths for Arrow result generation:

#### 1. Native Arrow Path (ADBC, DuckDB, BigQuery)

Adapters with native Arrow support use zero-copy data transfer:

```python
# ADBC example
cursor.execute(sql, parameters)
arrow_table = cursor.fetch_arrow_table()  # Zero-copy!
```

**Benefits**:
- No data copying - direct memory transfer
- 5-10x faster than dict conversion for large datasets
- Preserves native database type metadata
- Optimal for high-throughput analytics workloads

**Supported adapters**:
- **ADBC**: Uses `cursor.fetch_arrow_table()` for all ADBC-compatible databases
- **DuckDB**: Uses `result.arrow()` for native columnar format
- **BigQuery**: Uses Storage Read API when available (requires `google-cloud-bigquery-storage`)

#### 2. Conversion Path (7 Other Adapters)

Adapters without native Arrow support convert dict results to Arrow:

```python
# Execute query to get dict results
result = await session.execute(sql, parameters)

# Convert list[dict] to Arrow Table
arrow_table = convert_dict_to_arrow(result.data, return_format="table")
```

**Process**:
1. Execute query using standard driver path
2. Fetch results as list of dictionaries
3. Convert to columnar format using PyArrow
4. Wrap in ArrowResult for consistent API

**Supported adapters**:
- asyncpg, psycopg, psqlpy (PostgreSQL)
- oracledb (Oracle)
- sqlite, aiosqlite (SQLite)
- asyncmy (MySQL)

### Type Mapping

SQLSpec automatically maps database types to Arrow types:

| Database Type | Python Type | Arrow Type | Notes |
|--------------|-------------|------------|-------|
| INTEGER, BIGINT | int | int64 | Standardized to 64-bit |
| REAL, DOUBLE | float | float64 | Double precision |
| TEXT, VARCHAR | str | utf8 | UTF-8 encoded strings |
| BLOB, BYTEA | bytes | binary | Binary data |
| BOOLEAN | bool | bool | True/False values |
| DATE | datetime.date | date32 | Days since epoch |
| TIME | datetime.time | time64[us] | Microsecond precision |
| TIMESTAMP | datetime.datetime | timestamp[us] | Microsecond precision |
| NUMERIC, DECIMAL | Decimal | decimal128 | High-precision decimals |
| JSON, JSONB | dict/list | utf8 | JSON as text |
| ARRAY | list | list\<T\> | PostgreSQL arrays |
| UUID | uuid.UUID | utf8 | Converted to string |
| NULL | None | nullable | Preserves NULL semantics |

### ArrowResult Class

The `ArrowResult` class wraps Arrow Table or RecordBatch with convenience methods:

```python
class ArrowResult:
    """Wrapper for Arrow results with framework integrations."""

    data: ArrowTable | ArrowRecordBatch
    rows_affected: int

    def to_pandas(self) -> pd.DataFrame:
        """Convert to pandas DataFrame."""

    def to_polars(self) -> pl.DataFrame:
        """Convert to Polars DataFrame."""

    def to_dict(self) -> list[dict[str, Any]]:
        """Convert to list of dictionaries."""

    def __len__(self) -> int:
        """Return number of rows."""

    def __iter__(self):
        """Iterate over rows as dictionaries."""
```

## Performance Characteristics

### Native Path (ADBC, DuckDB)

**Benchmarks** (10K rows, 10 columns):
- Native Arrow: ~5ms
- Dict conversion: ~25ms
- **Speedup**: 5x

**Memory usage**:
- Zero-copy: Same as Arrow Table size
- No intermediate dict allocation

**Best for**:
- Large result sets (>10K rows)
- Analytics workloads
- Data science pipelines
- ETL processes

### Conversion Path (Other Adapters)

**Overhead** (vs standard `execute()`):
- Small datasets (&lt;1K rows): ~20% overhead
- Medium datasets (1K-100K rows): ~15% overhead
- Large datasets (&gt;100K rows): ~10% overhead

**Memory usage**:
- Peak: ~1.5-2x dict representation
- Columnar format more efficient for large datasets

**Best for**:
- Medium to large result sets
- When Arrow format is required
- Framework interoperability

## Usage Patterns

### Basic Query Execution

```python
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

sql = SQLSpec()
config = AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/mydb"})
sql.add_config(config)

async with sql.provide_session() as session:
    result = await session.select_to_arrow(
        "SELECT * FROM users WHERE age > :age",
        {"age": 18}
    )

    print(f"Rows: {len(result)}")
    print(f"Columns: {result.data.column_names}")
```

### Return Format Options

**Table format** (default) - Best for most use cases:

```python
result = await session.select_to_arrow(
    "SELECT * FROM products",
    return_format="table"
)
# Returns: ArrowResult with pyarrow.Table
```

**Batch format** - For streaming or chunked processing:

```python
result = await session.select_to_arrow(
    "SELECT * FROM logs",
    return_format="batch"
)
# Returns: ArrowResult with pyarrow.RecordBatch
```

### Framework Integration

**pandas integration**:

```python
result = await session.select_to_arrow("SELECT * FROM sales")
df = result.to_pandas()  # Zero-copy if native path

# Standard pandas operations
print(df.describe())
df.to_csv("sales.csv")
```

**Polars integration**:

```python
result = await session.select_to_arrow("SELECT * FROM events")
pl_df = result.to_polars()

# Polars operations
print(pl_df.schema)
pl_df.write_parquet("events.parquet")
```

**Dict conversion** (for compatibility):

```python
result = await session.select_to_arrow("SELECT * FROM items")
rows = result.to_dict()  # Same format as execute()

for row in rows:
    print(row["id"], row["name"])
```

### Native-Only Mode

Force native Arrow path or raise error:

```python
try:
    result = await session.select_to_arrow(
        "SELECT * FROM data",
        native_only=True
    )
except RuntimeError:
    print("Native Arrow not supported for this adapter")
```

**Use cases**:
- Ensure zero-copy performance
- Validate adapter capabilities
- Performance-critical code paths

### Custom Arrow Schema

Provide explicit schema for conversion path:

```python
import pyarrow as pa

schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.utf8()),
    pa.field("score", pa.float64()),
])

result = await session.select_to_arrow(
    "SELECT id, name, score FROM results",
    arrow_schema=schema
)
```

## Adapter-Specific Behaviors

### ADBC (Native)

**Implementation**: Uses `cursor.fetch_arrow_table()`

**Features**:
- Zero-copy data transfer
- Native type preservation
- Optimal for all ADBC-supported databases (PostgreSQL, SQLite, Snowflake, etc.)

**Example**:

```python
from sqlspec.adapters.adbc import AdbcConfig

config = AdbcConfig(
    driver="adbc_driver_postgresql",
    pool_config={"uri": "postgresql://localhost/db"}
)

async with sql.provide_session(config) as session:
    result = await session.select_to_arrow("SELECT * FROM large_table")
    # Uses native ADBC Arrow fetch - very fast!
```

### DuckDB (Native)

**Implementation**: Uses `result.arrow()` when available

**Features**:
- Native columnar format
- Efficient for analytical queries
- In-memory analytics optimized

**Example**:

```python
from sqlspec.adapters.duckdb import DuckDBConfig

config = DuckDBConfig(database=":memory:")

with sql.provide_session(config) as session:
    result = session.select_to_arrow(
        "SELECT * FROM read_parquet('data.parquet')"
    )
    # Native DuckDB Arrow format
```

### BigQuery (Conditional Native)

**Implementation**: Uses Storage Read API when `google-cloud-bigquery-storage` installed

**Features**:
- Fast parallel reads for large tables
- Automatic fallback to conversion path if Storage API unavailable
- Row-level access controls respected

**Example**:

```python
from sqlspec.adapters.bigquery import BigQueryConfig

config = BigQueryConfig(
    pool_config={"project": "my-project"},
    driver_features={"enable_storage_api": True}  # Auto-detected
)

async with sql.provide_session(config) as session:
    result = await session.select_to_arrow(
        "SELECT * FROM `dataset.large_table`"
    )
    # Uses Storage Read API if available
```

### PostgreSQL Adapters (Conversion)

**Adapters**: asyncpg, psycopg, psqlpy

**Special handling**:
- PostgreSQL arrays → Arrow list type
- JSONB → Arrow utf8 (JSON as text)
- UUID → Arrow utf8
- Custom types preserved when possible

**Example**:

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/db"})

async with sql.provide_session(config) as session:
    result = await session.select_to_arrow(
        "SELECT id, tags, metadata FROM articles"
    )
    # PostgreSQL arrays and JSONB converted to Arrow
```

### Oracle (Conversion)

**Special handling**:
- CLOB → Arrow utf8 (automatically read)
- BLOB → Arrow binary
- VECTOR (23ai) → Arrow list<float64> (with NumPy installed)
- NUMBER → Arrow decimal128

**Example**:

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig

config = OracleAsyncConfig(pool_config={"dsn": "oracle://localhost/FREE"})

async with sql.provide_session(config) as session:
    result = await session.select_to_arrow(
        "SELECT id, document, embedding FROM docs"
    )
    # CLOBs automatically hydrated to strings
```

### MySQL (Conversion)

**Special handling**:
- JSON columns → Arrow utf8
- DECIMAL → Arrow decimal128
- Binary → Arrow binary

**Example**:

```python
from sqlspec.adapters.asyncmy import AsyncmyConfig

config = AsyncmyConfig(pool_config={"dsn": "mysql://localhost/db"})

async with sql.provide_session(config) as session:
    result = await session.select_to_arrow(
        "SELECT id, config, price FROM products"
    )
    # JSON and DECIMAL types converted
```

### SQLite (Conversion)

**Type handling**:
- Simple type system (INTEGER, REAL, TEXT, BLOB)
- All types mapped to Arrow equivalents

**Example**:

```python
from sqlspec.adapters.sqlite import SqliteConfig

config = SqliteConfig(database="app.db")

with sql.provide_session(config) as session:
    result = session.select_to_arrow(
        "SELECT * FROM users"
    )
    # Efficient conversion from SQLite types
```

## Best Practices

### When to Use Arrow Format

**Good use cases**:
- Exporting to pandas, Polars, or other data science tools
- Columnar analytics workloads
- Data interchange between systems
- Large result sets requiring efficient memory usage
- Integration with Apache ecosystem (Parquet, Spark, etc.)

**Not recommended**:
- Small result sets (&lt;100 rows) - use standard `execute()`
- Single-row queries - use `select_one()`
- When dict format is required downstream

### Performance Optimization

**For native adapters** (ADBC, DuckDB):

```python
# Always use Arrow when available - it's faster!
result = await session.select_to_arrow("SELECT * FROM big_table")
```

**For conversion adapters**:

```python
# Use Arrow for medium to large datasets
if expected_rows > 1000:
    result = await session.select_to_arrow(sql)
else:
    result = await session.execute(sql)
```

### Memory Management

**Large datasets**:

```python
# Process in chunks for very large results
async for batch in session.select_to_arrow_stream("SELECT * FROM huge_table"):
    # Process batch
    df = batch.to_pandas()
    process(df)
    # Memory freed after each batch
```

Note: Streaming support is planned for future release.

**Current approach** for large datasets:

```python
# Add LIMIT for memory safety
result = await session.select_to_arrow(
    "SELECT * FROM huge_table LIMIT 1000000"
)
```

### Type Safety

**Use schema validation** when needed:

```python
import pyarrow as pa

# Define expected schema
expected_schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.utf8()),
    pa.field("created_at", pa.timestamp('us')),
])

result = await session.select_to_arrow("SELECT * FROM users")

# Validate schema matches
assert result.data.schema.equals(expected_schema)
```

## Error Handling

### Missing PyArrow

```python
from sqlspec.exceptions import MissingDependencyError

try:
    result = await session.select_to_arrow("SELECT * FROM data")
except MissingDependencyError as e:
    print("PyArrow not installed: pip install sqlspec[arrow]")
```

### Native Path Failures

```python
# Native-only mode raises error if not supported
try:
    result = await session.select_to_arrow(
        "SELECT * FROM data",
        native_only=True
    )
except RuntimeError:
    # Fallback to conversion path
    result = await session.select_to_arrow("SELECT * FROM data")
```

### Type Conversion Errors

```python
from sqlspec.exceptions import ConversionError

try:
    result = await session.select_to_arrow("SELECT custom_type FROM data")
except ConversionError as e:
    print(f"Failed to convert to Arrow: {e}")
    # Use standard execute() instead
    result = await session.execute("SELECT custom_type FROM data")
```

## Troubleshooting

### Slow Conversion Performance

**Problem**: Conversion path is slow for large datasets

**Solutions**:
1. Use native Arrow adapter (ADBC) if possible
2. Add WHERE clause to reduce result set
3. Use pagination with LIMIT/OFFSET
4. Consider streaming API (future enhancement)

### Memory Issues

**Problem**: Out of memory with large result sets

**Solutions**:
1. Reduce result set size with WHERE clause
2. Process in smaller batches using LIMIT/OFFSET
3. Increase available memory
4. Use native Arrow path for better efficiency

### Type Mapping Issues

**Problem**: Database type not mapping correctly to Arrow

**Solutions**:
1. Provide explicit `arrow_schema` parameter
2. Cast database type in SQL: `CAST(column AS TEXT)`
3. Use standard `execute()` and convert manually
4. Report issue with database type details

### Missing Dependencies

**Problem**: `MissingDependencyError` for optional packages

**Solutions**:

```bash
# Install Arrow support
pip install sqlspec[arrow]

# Install pandas support
pip install pandas

# Install Polars support
pip install polars

# Install all data science tools
pip install sqlspec[arrow,pandas,polars]
```

## Migration Guide

### From execute() to select_to_arrow()

**Before** (dict results):

```python
result = await session.execute("SELECT * FROM users")
df = pd.DataFrame(result.data)
```

**After** (Arrow results):

```python
result = await session.select_to_arrow("SELECT * FROM users")
df = result.to_pandas()  # Zero-copy if native path
```

### Converting SQLResult to DataFrames

Both `SQLResult` and `ArrowResult` support convenient conversion methods for data science workflows:

```python
# Standard execute returns SQLResult
result = await session.execute("SELECT * FROM users")

# Convert to pandas DataFrame
df = result.to_pandas()

# Convert to Polars DataFrame
pl_df = result.to_polars()

# Convert to Arrow Table (for SQLResult only)
arrow_table = result.to_arrow()
```

**Key differences**:
- `SQLResult.to_arrow()` performs dict→Arrow conversion
- `ArrowResult` data is already in Arrow format
- Both provide consistent API for pandas and Polars conversion

### From DataFrame-Centric to Arrow-First

**Before**:

```python
# Query → dict → pandas → processing
result = await session.execute("SELECT * FROM sales")
df = pd.DataFrame(result.data)
df_filtered = df[df["amount"] > 100]
```

**After**:

```python
# Query → Arrow → pandas (when needed)
result = await session.select_to_arrow("SELECT * FROM sales WHERE amount > :min", {"min": 100})
df = result.to_pandas()  # Only convert if pandas needed
```

## Future Enhancements

Planned improvements for Arrow integration:

1. **Streaming Arrow Results**: Process large datasets in chunks without loading into memory
2. **Schema Caching**: Cache Arrow schema for repeated queries to reduce conversion overhead
3. **Custom Type Converters**: User-defined Python → Arrow type converters for domain-specific types
4. **Write Support**: `insert_from_arrow()` for bulk insert from Arrow tables
5. **DuckDB Zero-Copy Integration**: Direct SQLSpec ↔ DuckDB data transfer via Arrow

## See Also

- [ADBC Adapter Guide](../adapters/adbc.md) - Native Arrow support
- [DuckDB Adapter Guide](../adapters/duckdb.md) - Native Arrow support
- [PostgreSQL Adapters](../adapters/postgres.md) - Conversion path with arrays
- [Performance Guide](../performance/arrow-performance.md) - Benchmarks and optimization
- [Apache Arrow Documentation](https://arrow.apache.org/docs/python/) - Official Arrow docs
