# DuckDB Adapter Skill

**Adapter:** DuckDB (Columnar Analytics, In-Process OLAP)
**Category:** Database Adapter
**Status:** Active

## Description

Expert guidance for using SQLSpec's DuckDB adapter. DuckDB is an in-process columnar OLAP database optimized for analytical workloads with zero-copy Arrow integration, native Parquet support, and direct file querying capabilities.

DuckDB excels at analytics over structured data (CSV, Parquet, JSON), supports extensions for AI/ML integrations (vector similarity search, spatial data, HTTP/S3 access), and provides SQL-based data transformation pipelines without requiring external database servers.

## When to Use DuckDB

- **Analytics workloads** - Aggregate queries, window functions, complex joins
- **Data engineering pipelines** - Transform CSV/Parquet files with SQL
- **Embedded analytics** - No server setup, runs in-process
- **Direct file querying** - Query Parquet/CSV files without loading
- **Arrow integration** - Zero-copy data exchange with pandas/polars
- **AI/ML workflows** - Vector similarity search with vss extension
- **Prototyping** - Fast iteration with in-memory shared databases
- **ETL workflows** - SQL-based transformations with native Parquet I/O

## Configuration

```python
from sqlspec.adapters.duckdb import (
    DuckDBConfig,
    DuckDBDriverFeatures,
    DuckDBExtensionConfig,
    DuckDBSecretConfig,
)

config = DuckDBConfig(
    pool_config={
        # Database path (defaults to ":memory:shared_db")
        "database": ":memory:shared_db",  # Shared in-memory DB
        # OR: "analytics.duckdb",  # Persistent file
        # OR: ":memory:",  # Private in-memory (auto-converted to shared)

        # Connection settings
        "read_only": False,
        "threads": 4,
        "memory_limit": "1GB",
        "temp_directory": "/tmp/duckdb",
        "max_temp_directory_size": "10GB",

        # Extension settings (can also use driver_features.extension_flags)
        "autoload_known_extensions": True,
        "autoinstall_known_extensions": True,
        "allow_community_extensions": True,
        "allow_unsigned_extensions": False,
        "extension_directory": ".duckdb_extensions",
        "custom_extension_repository": "https://extensions.duckdb.org",

        # Secret and access settings
        "allow_persistent_secrets": True,
        "enable_external_access": True,
        "secret_directory": ".duckdb_secrets",

        # Performance settings
        "enable_object_cache": True,
        "parquet_metadata_cache": "enabled",
        "enable_external_file_cache": True,
        "checkpoint_threshold": "16MB",

        # Logging and debugging
        "enable_progress_bar": False,
        "progress_bar_time": 2.0,
        "enable_logging": True,
        "log_query_path": "duckdb_queries.log",
        "logging_level": "INFO",

        # Query behavior
        "preserve_insertion_order": True,
        "default_null_order": "NULLS LAST",
        "default_order": "ASC",
        "ieee_floating_point_ops": True,
        "binary_as_string": False,
        "arrow_large_buffer_size": True,
        "errors_as_json": False,

        # Pool settings (per-thread connections)
        "pool_min_size": 1,
        "pool_max_size": 4,
        "pool_timeout": 30.0,
        "pool_recycle_seconds": 86400,  # 24 hours

        # Advanced config dictionary
        "config": {
            "default_order": "ASC",
            "enable_progress_bar": False,
        },
    },
    driver_features=DuckDBDriverFeatures(
        # Extension management
        extensions=[
            DuckDBExtensionConfig(
                name="parquet",
                force_install=False,
            ),
            DuckDBExtensionConfig(
                name="httpfs",
                repository="core",
            ),
            DuckDBExtensionConfig(
                name="vss",  # Vector similarity search
                repository="community",
            ),
        ],

        # Secrets for AI/API integrations
        secrets=[
            DuckDBSecretConfig(
                secret_type="openai",
                name="my_openai_key",
                value={"api_key": "sk-..."},
                scope="LOCAL",
            ),
            DuckDBSecretConfig(
                secret_type="aws",
                name="s3_credentials",
                value={
                    "access_key_id": "AKIA...",
                    "secret_access_key": "...",
                    "region": "us-east-1",
                },
            ),
        ],

        # Connection-level extension flags (SET statements)
        extension_flags={
            "allow_community_extensions": True,
            "allow_unsigned_extensions": False,
            "enable_external_access": True,
        },

        # Custom JSON serializer (defaults to to_json)
        json_serializer=custom_json_encoder,

        # UUID conversion (default: True)
        enable_uuid_conversion=True,

        # Connection creation hook
        on_connection_create=lambda conn: conn.execute("SET threads TO 4"),
    ),
)
```

## Parameter Style

**Positional**: `?` (SQLite-style positional parameters)

```python
# Single parameter
result = session.execute(
    "SELECT * FROM users WHERE id = ?",
    user_id
)

# Multiple parameters
result = session.execute(
    "SELECT * FROM users WHERE status = ? AND age > ?",
    "active", 18
)

# Named parameters NOT supported - use positional
result = session.execute(
    "SELECT * FROM users WHERE email = ? AND created_at > ?",
    "alice@example.com", "2024-01-01"
)
```

## Extension Management

### Core Extensions

```python
config = DuckDBConfig(
    driver_features={
        "extensions": [
            {"name": "parquet"},     # Native Parquet I/O
            {"name": "httpfs"},      # HTTP/S3 file access
            {"name": "json"},        # JSON file querying
            {"name": "excel"},       # Excel file reading
            {"name": "arrow"},       # Arrow format support
            {"name": "spatial"},     # GIS/spatial operations
            {"name": "icu"},         # Internationalization
            {"name": "fts"},         # Full-text search
        ]
    }
)

# Extensions auto-install and load on connection creation
session.execute("SELECT * FROM read_parquet('data.parquet')")
session.execute("SELECT * FROM read_json_auto('data.json')")
session.execute("SELECT * FROM read_csv_auto('s3://bucket/data.csv')")
```

### Community Extensions

```python
config = DuckDBConfig(
    pool_config={
        "allow_community_extensions": True,
    },
    driver_features={
        "extensions": [
            # Vector similarity search
            {"name": "vss", "repository": "community"},
        ]
    }
)

# Vector similarity with vss extension
session.execute("""
    CREATE TABLE embeddings (
        id INTEGER,
        vector FLOAT[768]
    )
""")

session.execute("""
    SELECT id, array_distance(vector, ?) as distance
    FROM embeddings
    ORDER BY distance
    LIMIT 10
""", query_embedding)
```

## AI/ML Integration with Secrets

### OpenAI Integration

```python
config = DuckDBConfig(
    pool_config={
        "allow_persistent_secrets": True,
        "enable_external_access": True,
    },
    driver_features={
        "secrets": [
            {
                "secret_type": "openai",
                "name": "my_openai_key",
                "value": {"api_key": os.getenv("OPENAI_API_KEY")},
                "scope": "PERSISTENT",
            }
        ]
    }
)

# Generate embeddings with OpenAI
session.execute("""
    CREATE TABLE documents AS
    SELECT
        id,
        text,
        embedding(text, 'openai/text-embedding-3-small') as vector
    FROM raw_documents
""")
```

### AWS S3 Access

```python
config = DuckDBConfig(
    driver_features={
        "extensions": [{"name": "httpfs"}],
        "secrets": [
            {
                "secret_type": "aws",
                "name": "s3_creds",
                "value": {
                    "access_key_id": "AKIA...",
                    "secret_access_key": "...",
                    "region": "us-east-1",
                },
            }
        ]
    }
)

# Query S3 files directly
result = session.execute("""
    SELECT *
    FROM read_parquet('s3://my-bucket/data/*.parquet')
    WHERE date >= '2024-01-01'
""").all()
```

## Direct File Querying

### Parquet Files

```python
# Query Parquet files without loading
result = session.execute("""
    SELECT product_id, SUM(revenue) as total
    FROM read_parquet('sales/*.parquet')
    WHERE date >= '2024-01-01'
    GROUP BY product_id
    ORDER BY total DESC
    LIMIT 10
""").all()

# Filter pushdown to Parquet
result = session.execute("""
    SELECT *
    FROM read_parquet('data.parquet', hive_partitioning=true)
    WHERE year = 2024 AND month = 1
""").all()
```

### CSV Files

```python
# Auto-detect schema
result = session.execute("""
    SELECT *
    FROM read_csv_auto('users.csv')
    WHERE age > 18
""").all()

# Manual schema
result = session.execute("""
    SELECT *
    FROM read_csv('users.csv',
        columns={'id': 'INTEGER', 'name': 'VARCHAR', 'age': 'INTEGER'},
        header=true,
        delim=','
    )
""").all()
```

### JSON Files

```python
# Auto-detect JSON structure
result = session.execute("""
    SELECT user.name, event.type, event.timestamp
    FROM read_json_auto('events.json')
    WHERE event.type = 'purchase'
""").all()
```

## Arrow Integration

### Zero-Copy Export

```python
# Export to Arrow table (zero-copy)
result = session.execute("SELECT * FROM large_table")
arrow_table = result.to_arrow()

# Use with pandas (zero-copy when possible)
df = arrow_table.to_pandas(use_threads=True)

# Use with polars (zero-copy)
import polars as pl
polars_df = pl.from_arrow(arrow_table)
```

### Native Arrow Import

```python
import pyarrow as pa

# Create Arrow table
data = pa.table({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
})

# Import directly (zero-copy)
session.execute("CREATE TABLE users AS SELECT * FROM data")

# Query imported data
result = session.execute("SELECT * FROM users WHERE id > 1").all()
```

## Performance Features

### Columnar Processing

```python
# DuckDB optimized for analytics
result = session.execute("""
    SELECT
        date_trunc('month', order_date) as month,
        product_category,
        SUM(revenue) as total_revenue,
        AVG(revenue) as avg_revenue,
        COUNT(*) as order_count
    FROM orders
    WHERE order_date >= '2024-01-01'
    GROUP BY month, product_category
    ORDER BY month, total_revenue DESC
""").all()
```

### Parallel Query Execution

```python
config = DuckDBConfig(
    pool_config={
        "threads": 8,  # Use 8 threads for query execution
    }
)

# Automatic parallelization for large queries
result = session.execute("""
    SELECT *
    FROM read_parquet('large_dataset/*.parquet')
    WHERE condition = true
""").all()
```

### Result Caching

```python
config = DuckDBConfig(
    pool_config={
        "enable_object_cache": True,
        "parquet_metadata_cache": "enabled",
    }
)

# Metadata cached for repeated queries
for i in range(10):
    result = session.execute("""
        SELECT * FROM read_parquet('data.parquet')
        WHERE id = ?
    """, i).all()
```

## Connection Pooling

### Per-Thread Connections

```python
# DuckDB uses thread-local connections
config = DuckDBConfig(
    pool_config={
        "database": ":memory:shared_db",  # Shared across threads
        "pool_min_size": 1,
        "pool_max_size": 4,
    }
)

# Each thread gets its own connection to shared database
import concurrent.futures

def run_query(user_id):
    with config.provide_session() as session:
        return session.execute(
            "SELECT * FROM users WHERE id = ?", user_id
        ).all()

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(run_query, range(100)))
```

## Best Practices

1. **Use shared memory databases** - Default `:memory:shared_db` for proper concurrency
2. **Enable auto-install extensions** - Set `autoinstall_known_extensions=True`
3. **Use Arrow for large results** - Call `result.to_arrow()` for zero-copy export
4. **Query files directly** - Use `read_parquet()` instead of loading into tables
5. **Leverage filter pushdown** - DuckDB optimizes Parquet filters automatically
6. **Set thread count** - Match `threads` to available CPU cores
7. **Use secrets for APIs** - Safer than hardcoding credentials
8. **Enable object cache** - Improves repeated query performance
9. **Use positional parameters** - Named parameters not supported
10. **Batch file queries** - Use glob patterns (`*.parquet`) for multiple files

## Common Issues

### "Extension not found"

Install extension explicitly:
```python
config = DuckDBConfig(
    pool_config={
        "autoinstall_known_extensions": True,
    },
    driver_features={
        "extensions": [
            {"name": "httpfs", "force_install": True}
        ]
    }
)
```

Or enable community extensions:
```python
config = DuckDBConfig(
    pool_config={
        "allow_community_extensions": True,
    }
)
```

### "Cannot open database file"

Ensure directory exists for file-based databases:
```python
import os
os.makedirs("data", exist_ok=True)

config = DuckDBConfig(
    pool_config={"database": "data/analytics.duckdb"}
)
```

### "Memory limit exceeded"

Increase memory limit:
```python
config = DuckDBConfig(
    pool_config={
        "memory_limit": "4GB",
        "temp_directory": "/tmp/duckdb",
        "max_temp_directory_size": "20GB",
    }
)
```

### "S3 access denied"

Configure AWS credentials:
```python
config = DuckDBConfig(
    driver_features={
        "extensions": [{"name": "httpfs"}],
        "secrets": [{
            "secret_type": "aws",
            "name": "s3",
            "value": {
                "access_key_id": "...",
                "secret_access_key": "...",
                "region": "us-east-1",
            }
        }]
    }
)
```

### "Extension load failed"

Check extension flags:
```python
config = DuckDBConfig(
    driver_features={
        "extension_flags": {
            "allow_community_extensions": True,
            "enable_external_access": True,
        }
    }
)
```

## Performance Benchmarks

Compared to other embedded analytics databases:

- **DuckDB**: Fastest for analytics (baseline)
- **SQLite**: 10-100x slower for aggregations
- **Pandas**: 2-5x slower for large datasets
- **Polars**: Comparable performance (different API)

For OLAP workloads, DuckDB provides:
- Columnar storage (10-100x faster than row-oriented)
- Parallel execution (scales with CPU cores)
- Native Parquet support (no parsing overhead)
- Zero-copy Arrow integration (minimal memory overhead)

Best used for analytics; use SQLite/PostgreSQL for OLTP workloads.
