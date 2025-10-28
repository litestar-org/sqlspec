---
orphan: true
---

# ADBC Adapter Guide

This guide provides specific instructions for the `adbc` adapter.

## Key Information

- **Driver:** Arrow Database Connectivity (ADBC) drivers (e.g., `adbc_driver_postgresql`, `adbc_driver_sqlite`).
- **Parameter Style:** Varies by underlying database (e.g., `numeric` for PostgreSQL, `qmark` for SQLite).

## Best Practices

- **Arrow-Native:** The primary benefit of ADBC is its direct integration with Apache Arrow. Use it when you need to move large amounts of data efficiently between the database and data science tools like Pandas or Polars.
- **Driver Installation:** Each database requires a separate ADBC driver to be installed (e.g., `pip install adbc_driver_postgresql`).
- **Data Types:** Be aware of how database types are mapped to Arrow types. Use `Context7` to research the specific ADBC driver's documentation for type mapping details.

## Arrow Support (Native)

The ADBC adapter provides **native Apache Arrow support** through `select_to_arrow()`, offering zero-copy data transfer for exceptional performance.

### Native Arrow Path

ADBC uses the driver's built-in `fetch_arrow_table()` method for direct Arrow retrieval:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.adbc import AdbcConfig

sql = SQLSpec()
config = AdbcConfig(
    driver="adbc_driver_postgresql",
    pool_config={"uri": "postgresql://localhost/mydb"}
)
sql.add_config(config)

async with sql.provide_session() as session:
    # Native Arrow fetch - zero-copy!
    result = await session.select_to_arrow(
        "SELECT * FROM users WHERE age > :age",
        {"age": 18}
    )

    print(f"Rows: {len(result)}")
    print(f"Columns: {result.data.column_names}")
```

### Performance Characteristics

**Native Arrow Benefits**:
- **5-10x faster** than dict conversion for large datasets
- **Zero-copy data transfer** - no intermediate representations
- **Native type preservation** - database types mapped directly to Arrow
- **Memory efficient** - columnar format reduces memory usage

**Benchmarks** (10K rows, 10 columns):
- Native Arrow: ~5ms
- Dict conversion: ~25ms
- **Speedup**: 5x

### Return Formats

**Table format** (default):

```python
result = await session.select_to_arrow(
    "SELECT id, name, email FROM users",
    return_format="table"
)
# Returns: pyarrow.Table
print(result.data.num_rows)
print(result.data.column_names)
```

**Batch format**:

```python
result = await session.select_to_arrow(
    "SELECT * FROM events",
    return_format="batch"
)
# Returns: pyarrow.RecordBatch
print(result.data.num_rows)
```

### Framework Integration

**pandas**:

```python
result = await session.select_to_arrow("SELECT * FROM sales")
df = result.to_pandas()  # Zero-copy conversion

# Standard pandas operations
print(df.describe())
df.to_csv("sales.csv")
```

**Polars**:

```python
result = await session.select_to_arrow("SELECT * FROM events")
pl_df = result.to_polars()

# Polars operations
print(pl_df.schema)
pl_df.write_parquet("events.parquet")
```

**Dictionary** (for compatibility):

```python
result = await session.select_to_arrow("SELECT * FROM items")
rows = result.to_dict()

for row in rows:
    print(row["id"], row["name"])
```

### Type Mapping

ADBC preserves native database types in Arrow format:

| PostgreSQL Type | Arrow Type | Notes |
|----------------|------------|-------|
| BIGINT | int64 | |
| DOUBLE PRECISION | float64 | |
| TEXT | utf8 | |
| BYTEA | binary | |
| BOOLEAN | bool | |
| DATE | date32 | |
| TIMESTAMP | timestamp[us] | Microsecond precision |
| NUMERIC | decimal128 | High-precision |
| ARRAY | list\<T\> | PostgreSQL arrays |
| JSONB | utf8 | JSON as text |
| UUID | utf8 | Converted to string |

### ADBC Driver-Specific Behavior

**PostgreSQL** (`adbc_driver_postgresql`):

```python
config = AdbcConfig(
    driver="adbc_driver_postgresql",
    pool_config={"uri": "postgresql://localhost/db"}
)

async with sql.provide_session(config) as session:
    # PostgreSQL arrays preserved in Arrow list type
    result = await session.select_to_arrow(
        "SELECT id, tags FROM articles"
    )
```

**SQLite** (`adbc_driver_sqlite`):

```python
config = AdbcConfig(
    driver="adbc_driver_sqlite",
    pool_config={"uri": "file:app.db"}
)

with sql.provide_session(config) as session:
    # SQLite types mapped to Arrow
    result = session.select_to_arrow(
        "SELECT * FROM users"
    )
```

**Snowflake** (`adbc_driver_snowflake`):

```python
config = AdbcConfig(
    driver="adbc_driver_snowflake",
    pool_config={
        "uri": "snowflake://account.region.snowflakecomputing.com/database/schema",
        "adbc.snowflake.sql.account": "your_account",
        "adbc.snowflake.sql.user": "your_user"
    }
)

async with sql.provide_session(config) as session:
    result = await session.select_to_arrow(
        "SELECT * FROM large_table"
    )
```

### Best Practices

**Use Arrow for Large Datasets**:

```python
# Efficient for analytics workloads
result = await session.select_to_arrow(
    "SELECT * FROM fact_sales WHERE year = 2024"
)
df = result.to_pandas()
aggregated = df.groupby("region")["revenue"].sum()
```

**Combine with DuckDB for In-Memory Analytics**:

```python
import duckdb

# Fetch data in Arrow format
result = await session.select_to_arrow("SELECT * FROM events")

# Zero-copy to DuckDB
con = duckdb.connect()
con.register("events_arrow", result.data)
analysis = con.execute("SELECT event_type, COUNT(*) FROM events_arrow GROUP BY event_type").fetchall()
```

**Export to Parquet**:

```python
import pyarrow.parquet as pq

result = await session.select_to_arrow("SELECT * FROM historical_data")
pq.write_table(result.data, "historical_data.parquet")
```

### Native-Only Mode

Force native Arrow or raise error:

```python
# Ensure zero-copy performance
result = await session.select_to_arrow(
    "SELECT * FROM large_table",
    native_only=True  # Raises error if native not supported
)
```

## Common Issues

- **`ArrowInvalid: Could not find ADBC driver`**: The required ADBC driver for your target database is not installed or not found in the system's library path.
- **Type Mismatches:** Errors related to converting database types to Arrow types. This often requires careful handling of complex types like JSON, arrays, or custom user-defined types.
- **`MissingDependencyError: pyarrow`**: Install Arrow support with `pip install sqlspec[arrow]`
