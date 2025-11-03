---
orphan: true
---

# DuckDB Adapter Guide

This guide provides specific instructions for the `duckdb` adapter.

## Key Information

-   **Driver:** `duckdb`
-   **Parameter Style:** `qmark` (e.g., `?`)

## Parameter Profile

-   **Registry Key:** `"duckdb"`
-   **JSON Strategy:** `helper` (shared serializer covers dict/list/tuple)
-   **Extras:** None (profile preserves existing `allow_mixed_parameter_styles=False`)

## Best Practices

-   **In-Memory vs. File:** DuckDB can run entirely in-memory (`:memory:`) or with a file-based database. In-memory is great for fast, temporary analytics. File-based is for persistence.
-   **Extensions:** DuckDB has a rich ecosystem of extensions (e.g., for reading Parquet files, JSON, etc.). These can be loaded via the `sqlspec` configuration.
-   **Vectorized Execution:** DuckDB is extremely fast for analytical queries due to its vectorized execution engine. Write queries that operate on columns rather than row-by-row.

## Driver Features

The `duckdb` adapter configuration is managed through a `TypedDict` for clarity and type safety, allowing you to configure extensions, secrets, and type handling behavior.

### DuckDBDriverFeatures TypedDict

```python
class DuckDBDriverFeatures(TypedDict):
    """TypedDict for DuckDB driver features configuration.

    Attributes:
        extensions: List of extensions to install/load on connection creation.
        secrets: List of secrets to create for AI/API integrations.
        on_connection_create: Callback executed when connection is created.
        json_serializer: Custom JSON serializer for dict/list parameter conversion.
            Defaults to sqlspec.utils.serializers.to_json if not provided.
        enable_uuid_conversion: Enable automatic UUID string conversion.
            When True (default), UUID strings are automatically converted to UUID objects.
            When False, UUID strings are treated as regular strings.
    """

    extensions: NotRequired[Sequence[DuckDBExtensionConfig]]
    secrets: NotRequired[Sequence[DuckDBSecretConfig]]
    on_connection_create: NotRequired["Callable[[DuckDBConnection], DuckDBConnection | None]"]
    json_serializer: NotRequired["Callable[[Any], str]"]
    enable_uuid_conversion: NotRequired[bool]
```

### Configuration and Defaults

-   **UUID Conversion (`enable_uuid_conversion`)**: This feature is **enabled by default** (`True`). It automatically converts UUID strings from your query results into Python `UUID` objects.
-   **JSON Serializer (`json_serializer`)**: Defaults to a standard JSON serializer. You can provide a custom function for performance or special encoding needs.

### Example: Configuring Extensions and Secrets

You can automatically install and load DuckDB extensions and configure secrets for services like OpenAI.

```python
from sqlspec.adapters.duckdb import DuckDBConfig

config = DuckDBConfig(
    pool_config={"database": ":memory:"},
    driver_features={
        "extensions": [
            {"name": "httpfs"},
            {"name": "json"},
            {"name": "openai"},
        ],
        "secrets": [
            {
                "secret_type": "openai",
                "name": "my_openai_secret",
                "value": {"api_key": "sk-..."},
            }
        ],
    },
)
```

### Example: Custom Type Handling

You can override the default JSON serializer or disable the automatic UUID conversion.

```python
import msgspec
from sqlspec.adapters.duckdb import DuckDBConfig

# A faster JSON serializer from an external library
def custom_json_serializer(obj: Any) -> str:
    return msgspec.json.encode(obj).decode("utf-8")

config = DuckDBConfig(
    pool_config={"database": "analytics.db"},
    driver_features={
        "json_serializer": custom_json_serializer,
        "enable_uuid_conversion": False,  # Disable auto-conversion
    },
)
```

## Arrow Support (Native)

The DuckDB adapter provides **native Apache Arrow support** through `select_to_arrow()`, leveraging DuckDB's columnar format for optimal performance.

### Native Arrow Path

DuckDB uses its native `result.arrow()` method for direct Arrow retrieval:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig

sql = SQLSpec()
config = DuckDBConfig(database=":memory:")
sql.add_config(config)

with sql.provide_session() as session:
    # Native Arrow fetch - columnar format!
    result = session.select_to_arrow(
        "SELECT * FROM read_parquet('data.parquet') WHERE amount > ?",
        (100,)
    )

    print(f"Rows: {len(result)}")
    print(f"Columns: {result.data.column_names}")
```

### Performance Characteristics

**Native Arrow Benefits**:
- **Columnar-native format** - DuckDB already uses columnar storage
- **Zero-copy data transfer** - direct Arrow output
- **Optimal for analytics** - Perfect for OLAP workloads
- **Parquet integration** - Seamless Arrow ↔ Parquet conversion

**Best for**:
- Analytical queries on large datasets
- Reading from Parquet, CSV, or JSON files
- In-memory data transformations
- Integration with data science tools

### Return Formats

**Table format** (default):

```python
result = session.select_to_arrow(
    "SELECT * FROM users WHERE age > ?",
    (18,),
    return_format="table"
)
# Returns: pyarrow.Table
```

**Batch format**:

```python
result = session.select_to_arrow(
    "SELECT * FROM events",
    return_format="batch"
)
# Returns: pyarrow.RecordBatch
```

### Framework Integration

**pandas**:

```python
# Read Parquet, convert to pandas
result = session.select_to_arrow(
    "SELECT * FROM read_parquet('sales.parquet')"
)
df = result.to_pandas()
print(df.describe())
```

**Polars**:

```python
# DuckDB to Polars via Arrow
result = session.select_to_arrow(
    "SELECT region, SUM(revenue) as total FROM sales GROUP BY region"
)
pl_df = result.to_polars()
print(pl_df)
```

### DuckDB-Specific Patterns

**Read Parquet with Arrow**:

```python
# Efficient Parquet → Arrow → pandas pipeline
result = session.select_to_arrow(
    """
    SELECT *
    FROM read_parquet('events/*.parquet')
    WHERE event_date >= CAST('2024-01-01' AS DATE)
    """
)
df = result.to_pandas()
```

**Join with In-Memory Arrow Table**:

```python
import pyarrow as pa

# Create Arrow table in memory
arrow_data = pa.table({
    "id": [1, 2, 3],
    "category": ["A", "B", "C"]
})

# Register Arrow table in DuckDB
with sql.provide_session() as session:
    con = session.connection  # Access raw DuckDB connection
    con.register("categories", arrow_data)

    # Query combining Parquet and Arrow table
    result = session.select_to_arrow(
        """
        SELECT e.*, c.category
        FROM read_parquet('events.parquet') e
        JOIN categories c ON e.cat_id = c.id
        """
    )
```

**Aggregate and Export to Arrow**:

```python
# DuckDB aggregation → Arrow → Parquet
result = session.select_to_arrow(
    """
    SELECT
        date_trunc('day', timestamp) as day,
        user_id,
        COUNT(*) as event_count
    FROM events
    GROUP BY 1, 2
    """
)

# Write directly to Parquet
import pyarrow.parquet as pq
pq.write_table(result.data, "daily_user_events.parquet")
```

### Type Mapping

DuckDB types map cleanly to Arrow:

| DuckDB Type | Arrow Type | Notes |
|-------------|------------|-------|
| INTEGER | int64 | |
| DOUBLE | float64 | |
| VARCHAR | utf8 | |
| BLOB | binary | |
| BOOLEAN | bool | |
| DATE | date32 | |
| TIME | time64[us] | |
| TIMESTAMP | timestamp[us] | |
| DECIMAL | decimal128 | |
| LIST | list\<T\> | Nested lists supported |
| STRUCT | struct | Nested structures |
| UUID | utf8 | Converted to string |

### Best Practices

**Use DuckDB for Complex Analytics**:

```python
# Complex analytical query with Arrow output
result = session.select_to_arrow(
    """
    WITH monthly_revenue AS (
        SELECT
            date_trunc('month', sale_date) as month,
            region,
            SUM(amount) as revenue
        FROM sales
        GROUP BY 1, 2
    )
    SELECT
        month,
        region,
        revenue,
        revenue / SUM(revenue) OVER (PARTITION BY month) as pct_of_total
    FROM monthly_revenue
    ORDER BY month, revenue DESC
    """
)
df = result.to_pandas()
```

**Combine Multiple Data Sources**:

```python
# Query across Parquet, CSV, and in-memory tables
result = session.select_to_arrow(
    """
    SELECT
        p.product_id,
        p.name,
        s.total_sales,
        c.category
    FROM read_parquet('products.parquet') p
    JOIN read_csv('sales.csv') s ON p.product_id = s.product_id
    JOIN categories c ON p.cat_id = c.id
    WHERE s.sale_date >= '2024-01-01'
    """
)
```

**Export Window Function Results**:

```python
# Window functions with Arrow output
result = session.select_to_arrow(
    """
    SELECT
        user_id,
        event_time,
        event_type,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time) as event_seq,
        LAG(event_type) OVER (PARTITION BY user_id ORDER BY event_time) as prev_event
    FROM events
    """
)
pl_df = result.to_polars()  # Process with Polars
```

### Integration with PyArrow Datasets

```python
import pyarrow.dataset as ds

# Read large Parquet dataset
result = session.select_to_arrow(
    """
    SELECT year, month, SUM(revenue) as total
    FROM read_parquet('s3://bucket/data/**/*.parquet', hive_partitioning=true)
    WHERE year >= 2023
    GROUP BY 1, 2
    """
)

# Create new dataset from results
ds.write_dataset(
    result.data,
    "output/",
    format="parquet",
    partitioning=["year", "month"]
)
```

## Common Issues

-   **`duckdb.IOException`**: Usually occurs when there are issues reading a file (e.g., a Parquet or CSV file). Check file paths and permissions.
-   **Memory Management:** While fast, DuckDB can be memory-intensive. For large datasets, monitor memory usage and consider using a file-based database to allow for out-of-core processing.
-   **`MissingDependencyError: pyarrow`**: Install Arrow support with `pip install sqlspec[arrow]`
