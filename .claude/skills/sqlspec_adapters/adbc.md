# ADBC Adapter Skill

**Adapter:** Arrow Database Connectivity (Multi-Driver)
**Category:** Database Adapter
**Status:** Active

## Description

Expert guidance for using SQLSpec's ADBC (Arrow Database Connectivity) adapter. ADBC provides a unified Arrow-native interface for multiple database systems with zero-copy data transfers and consistent API across backends.

ADBC is unique because it:
- Supports multiple database backends through a single adapter interface
- Uses Apache Arrow for efficient zero-copy data transfers
- Auto-detects the appropriate driver based on URI or driver_name
- Provides native Arrow/Parquet import/export capabilities
- Operates synchronously (no async support)

## When to Use ADBC

- **Arrow-native workloads** - Direct Arrow Table/RecordBatch operations
- **Multi-database support** - Single codebase for multiple backends
- **Zero-copy performance** - Efficient memory usage for large datasets
- **Data pipeline integration** - Arrow ecosystem compatibility (PyArrow, Pandas, Polars)
- **Parquet workflows** - Native Parquet import/export without intermediate conversions
- **Cross-database analytics** - Consistent interface across PostgreSQL, DuckDB, BigQuery, etc.

**NOT suitable for:**
- Async/await applications (ADBC is sync-only)
- Connection pooling (uses NoPoolSyncConfig)
- Transactional DDL operations (not supported)

## Supported Drivers

ADBC supports six primary database backends. Each requires its own driver package:

| Database | Driver Name | Package | Parameter Style | Auto-Detect URI |
|----------|-------------|---------|-----------------|-----------------|
| PostgreSQL | `postgresql`, `postgres`, `pg` | `adbc-driver-postgresql` | `$1, $2` (numeric) | `postgresql://...` |
| SQLite | `sqlite`, `sqlite3` | `adbc-driver-sqlite` | `?` or `:name` (qmark/named_colon) | `sqlite://...` |
| DuckDB | `duckdb` | `adbc-driver-duckdb` | `?` or `$1` (qmark/numeric) | `duckdb://...` |
| BigQuery | `bigquery`, `bq` | `adbc-driver-bigquery` | `@param` (named_at) | `bigquery://...` |
| Snowflake | `snowflake`, `sf` | `adbc-driver-snowflake` | `?` or `$1` (qmark/numeric) | `snowflake://...` |
| FlightSQL | `flightsql`, `grpc` | `adbc-driver-flightsql` | `?` (qmark) | `grpc://...` |

### Driver Installation

Install the base ADBC package plus the driver(s) you need:

```bash
# Base ADBC (required)
pip install adbc-driver-manager

# PostgreSQL
pip install adbc-driver-postgresql

# SQLite (often included with Python)
pip install adbc-driver-sqlite

# DuckDB
pip install adbc-driver-duckdb

# BigQuery
pip install adbc-driver-bigquery

# Snowflake
pip install adbc-driver-snowflake

# FlightSQL (gRPC)
pip install adbc-driver-flightsql
```

## Configuration

### Import Pattern

```python
from sqlspec.adapters.adbc import AdbcConfig, AdbcDriverFeatures
```

### PostgreSQL Configuration

```python
# Using URI (auto-detects driver)
config = AdbcConfig(
    connection_config={
        "uri": "postgresql://user:pass@localhost:5432/mydb",
    }
)

# Using explicit driver_name
config = AdbcConfig(
    connection_config={
        "driver_name": "postgresql",
        "uri": "postgresql://user:pass@localhost:5432/mydb",
    }
)

# Using individual parameters
config = AdbcConfig(
    connection_config={
        "driver_name": "postgres",
        "host": "localhost",
        "port": 5432,
        "user": "myuser",
        "password": "mypass",
        "database": "mydb",
    }
)

# With SSL
config = AdbcConfig(
    connection_config={
        "uri": "postgresql://user:pass@localhost:5432/mydb",
        "ssl_mode": "require",
        "ssl_cert": "/path/to/client-cert.pem",
        "ssl_key": "/path/to/client-key.pem",
        "ssl_ca": "/path/to/ca-cert.pem",
    }
)
```

### SQLite Configuration

```python
# File-based database
config = AdbcConfig(
    connection_config={
        "driver_name": "sqlite",
        "uri": "sqlite:///path/to/database.db",
    }
)

# Auto-detection from URI
config = AdbcConfig(
    connection_config={
        "uri": "sqlite:///path/to/database.db",
    }
)

# In-memory database (use absolute path for temp file)
import tempfile

config = AdbcConfig(
    connection_config={
        "driver_name": "sqlite",
        "uri": f"sqlite:///{tempfile.mkdtemp()}/temp.db",
    }
)
```

### DuckDB Configuration

```python
# File-based database
config = AdbcConfig(
    connection_config={
        "driver_name": "duckdb",
        "uri": "duckdb:///path/to/database.duckdb",
    }
)

# Auto-detection from URI
config = AdbcConfig(
    connection_config={
        "uri": "duckdb:///data/analytics.duckdb",
    }
)

# In-memory database
config = AdbcConfig(
    connection_config={
        "driver_name": "duckdb",
        "path": ":memory:",
    }
)
```

### BigQuery Configuration

```python
# Using project and dataset
config = AdbcConfig(
    connection_config={
        "driver_name": "bigquery",
        "project_id": "my-gcp-project",
        "dataset_id": "my_dataset",
    }
)

# With authentication token
config = AdbcConfig(
    connection_config={
        "driver_name": "bq",
        "project_id": "my-gcp-project",
        "dataset_id": "my_dataset",
        "token": "ya29.c.Ku...",  # OAuth2 token
    }
)

# With db_kwargs for additional options
config = AdbcConfig(
    connection_config={
        "driver_name": "bigquery",
        "db_kwargs": {
            "project_id": "my-gcp-project",
            "dataset_id": "my_dataset",
            "location": "US",
        }
    }
)
```

### Snowflake Configuration

```python
# Standard configuration
config = AdbcConfig(
    connection_config={
        "driver_name": "snowflake",
        "account": "mycompany",
        "warehouse": "COMPUTE_WH",
        "database": "MY_DATABASE",
        "schema": "PUBLIC",
        "username": "myuser",
        "password": "mypass",
    }
)

# With role and additional options
config = AdbcConfig(
    connection_config={
        "driver_name": "sf",
        "account": "mycompany.us-east-1",
        "warehouse": "ANALYTICS_WH",
        "database": "PRODUCTION",
        "schema": "ANALYTICS",
        "role": "ANALYST",
        "username": "myuser",
        "password": "mypass",
        "autocommit": False,
        "query_timeout": 300.0,
    }
)
```

### FlightSQL Configuration

```python
# Basic gRPC connection
config = AdbcConfig(
    connection_config={
        "driver_name": "flightsql",
        "uri": "grpc://localhost:8815",
    }
)

# With authentication
config = AdbcConfig(
    connection_config={
        "driver_name": "grpc",
        "uri": "grpc://arrow-server.example.com:443",
        "authorization_header": "Bearer eyJhbGc...",
    }
)

# With gRPC options
config = AdbcConfig(
    connection_config={
        "driver_name": "flightsql",
        "uri": "grpc://localhost:8815",
        "grpc_options": {
            "grpc.max_receive_message_length": 1024 * 1024 * 100,  # 100MB
            "grpc.keepalive_time_ms": 30000,
        }
    }
)
```

## Parameter Style

ADBC's parameter style **varies by driver**. SQLSpec handles conversion automatically based on the detected driver:

| Driver | Style | Placeholder | Example |
|--------|-------|-------------|---------|
| PostgreSQL | numeric | `$1, $2, $3` | `SELECT * FROM users WHERE id = $1 AND status = $2` |
| SQLite | qmark or named_colon | `?` or `:name` | `SELECT * FROM users WHERE id = ? AND status = ?` |
| DuckDB | qmark or numeric | `?` or `$1` | `SELECT * FROM users WHERE id = $1` |
| BigQuery | named_at | `@param` | `SELECT * FROM users WHERE id = @user_id` |
| Snowflake | qmark or numeric | `?` or `$1` | `SELECT * FROM users WHERE id = ?` |
| FlightSQL | qmark | `?` | `SELECT * FROM users WHERE id = ?` |

### PostgreSQL Examples

```python
config = AdbcConfig(connection_config={"uri": "postgresql://localhost/db"})

with config.provide_session() as session:
    # Single parameter
    result = session.execute("SELECT * FROM users WHERE id = $1", 123)

    # Multiple parameters
    result = session.execute(
        "SELECT * FROM users WHERE status = $1 AND age > $2",
        "active", 18
    )

    # Named parameters (SQLSpec converts to numeric)
    result = session.execute(
        "SELECT * FROM users WHERE email = :email",
        {"email": "user@example.com"}
    )
```

### BigQuery Examples

```python
config = AdbcConfig(
    connection_config={
        "driver_name": "bigquery",
        "project_id": "my-project",
        "dataset_id": "analytics",
    }
)

with config.provide_session() as session:
    # Named parameters with @ syntax
    result = session.execute(
        "SELECT * FROM users WHERE status = @status AND created > @date",
        {"status": "active", "date": "2024-01-01"}
    )
```

### SQLite Examples

```python
config = AdbcConfig(connection_config={"uri": "sqlite:///data.db"})

with config.provide_session() as session:
    # Positional parameters
    result = session.execute(
        "SELECT * FROM users WHERE id = ? AND status = ?",
        123, "active"
    )

    # Named parameters
    result = session.execute(
        "SELECT * FROM users WHERE email = :email",
        {"email": "user@example.com"}
    )
```

## Arrow Integration

ADBC provides **native Arrow support** with zero-copy data transfers. This is the primary advantage of using ADBC.

### Native Arrow Fetch

```python
import pyarrow as pa

config = AdbcConfig(connection_config={"uri": "postgresql://localhost/db"})

with config.provide_session() as session:
    # Fetch as Arrow Table (zero-copy)
    result = session.execute("SELECT * FROM large_dataset")
    arrow_table: pa.Table = result.arrow()

    # Fetch as Arrow RecordBatchReader (streaming)
    result = session.execute("SELECT * FROM huge_dataset")
    reader: pa.RecordBatchReader = result.arrow_reader()

    for batch in reader:
        process_batch(batch)  # Process in chunks
```

### Convert to Pandas/Polars

```python
# Direct to Pandas (zero-copy via Arrow)
result = session.execute("SELECT * FROM users")
df = result.to_pandas()

# Direct to Polars (zero-copy via Arrow)
result = session.execute("SELECT * FROM users")
pl_df = result.to_polars()
```

### Arrow Extension Types

Enable Arrow extension type preservation:

```python
config = AdbcConfig(
    connection_config={"uri": "postgresql://localhost/db"},
    driver_features={
        "arrow_extension_types": True,  # Default: True
    }
)

with config.provide_session() as session:
    # Extension types like UUIDs, decimals preserved
    result = session.execute("SELECT id, balance FROM accounts")
    arrow_table = result.arrow()

    # Check preserved types
    print(arrow_table.schema)
```

## Driver-Specific Features

### PostgreSQL-Specific

```python
config = AdbcConfig(
    connection_config={
        "driver_name": "postgresql",
        "uri": "postgresql://localhost/db",
        "isolation_level": "SERIALIZABLE",
        "autocommit": False,
    },
    driver_features={
        "enable_cast_detection": True,  # Detect ::JSONB casts
        "json_serializer": custom_json_encoder,
    }
)

with config.provide_session() as session:
    # JSONB handling with cast detection
    session.execute(
        "INSERT INTO docs (data) VALUES ($1::JSONB)",
        {"key": "value"}
    )

    # Transaction control
    session.begin()
    session.execute("UPDATE accounts SET balance = balance - $1 WHERE id = $2", 100, 1)
    session.execute("UPDATE accounts SET balance = balance + $1 WHERE id = $2", 100, 2)
    session.commit()
```

### SQLite-Specific

```python
config = AdbcConfig(
    connection_config={
        "driver_name": "sqlite",
        "uri": "sqlite:///app.db",
        "autocommit": True,  # Enable autocommit mode
    }
)

with config.provide_session() as session:
    # PRAGMA statements
    session.execute("PRAGMA journal_mode = WAL")
    session.execute("PRAGMA synchronous = NORMAL")

    # Attach additional databases
    session.execute("ATTACH DATABASE 'other.db' AS other")
    session.execute("SELECT * FROM other.users")
```

### DuckDB-Specific

```python
config = AdbcConfig(
    connection_config={
        "driver_name": "duckdb",
        "uri": "duckdb:///analytics.duckdb",
    }
)

with config.provide_session() as session:
    # Direct Parquet queries
    result = session.execute(
        "SELECT * FROM 's3://bucket/data/*.parquet' WHERE date > $1",
        "2024-01-01"
    )

    # Create views from remote data
    session.execute("""
        CREATE VIEW sales AS
        SELECT * FROM read_parquet('s3://data/sales/*.parquet')
    """)

    # Native Arrow export (zero-copy)
    result = session.execute("SELECT * FROM sales")
    arrow_table = result.arrow()
```

### Snowflake-Specific

```python
config = AdbcConfig(
    connection_config={
        "driver_name": "snowflake",
        "account": "mycompany",
        "warehouse": "COMPUTE_WH",
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "username": "user",
        "password": "pass",
        "role": "ANALYST",
        "query_timeout": 600.0,  # 10 minutes
    }
)

with config.provide_session() as session:
    # Use warehouse
    session.execute("USE WAREHOUSE LARGE_WH")

    # Query with parameters
    result = session.execute(
        "SELECT * FROM sales WHERE region = ? AND date > ?",
        "US", "2024-01-01"
    )

    # Result caching
    result = session.execute("SELECT /*+ RESULT_CACHE */ * FROM dim_customers")
```

### BigQuery-Specific

```python
config = AdbcConfig(
    connection_config={
        "driver_name": "bigquery",
        "project_id": "my-project",
        "dataset_id": "analytics",
        "db_kwargs": {
            "location": "US",
        }
    }
)

with config.provide_session() as session:
    # Standard SQL with named parameters
    result = session.execute("""
        SELECT
            user_id,
            COUNT(*) as event_count
        FROM events
        WHERE event_date >= @start_date
        GROUP BY user_id
    """, {"start_date": "2024-01-01"})

    # Cross-dataset queries
    result = session.execute("""
        SELECT * FROM `other-project.other_dataset.table`
        WHERE id = @id
    """, {"id": 123})
```

### FlightSQL-Specific

```python
config = AdbcConfig(
    connection_config={
        "driver_name": "flightsql",
        "uri": "grpc://arrow-server:8815",
        "authorization_header": "Bearer token123",
        "grpc_options": {
            "grpc.max_receive_message_length": 1024 * 1024 * 100,
            "grpc.keepalive_time_ms": 30000,
        },
        "connection_timeout": 30.0,
    }
)

with config.provide_session() as session:
    # Query remote Arrow Flight endpoint
    result = session.execute("SELECT * FROM remote_table WHERE id = ?", 123)

    # Stream large results
    reader = result.arrow_reader()
    for batch in reader:
        process_arrow_batch(batch)
```

## Performance Features

### Zero-Copy Data Transfers

ADBC's primary performance advantage is zero-copy transfers:

```python
config = AdbcConfig(connection_config={"uri": "postgresql://localhost/db"})

with config.provide_session() as session:
    # Traditional approach (multiple copies)
    result = session.execute("SELECT * FROM large_table")
    rows = result.all()  # Copy 1: DB -> Python dicts
    df = pd.DataFrame(rows)  # Copy 2: Dicts -> Pandas

    # Arrow approach (zero-copy)
    result = session.execute("SELECT * FROM large_table")
    arrow_table = result.arrow()  # Zero-copy: DB -> Arrow
    df = arrow_table.to_pandas(zero_copy_only=True)  # Zero-copy: Arrow -> Pandas
```

### Batch Processing

Configure batch size for optimal memory usage:

```python
config = AdbcConfig(
    connection_config={
        "uri": "postgresql://localhost/db",
        "batch_size": 10000,  # Process 10k rows per batch
    }
)

with config.provide_session() as session:
    result = session.execute("SELECT * FROM huge_table")
    reader = result.arrow_reader()

    # Process in batches
    for batch in reader:
        # Each batch is ~10k rows (Arrow RecordBatch)
        process_batch(batch)
```

### Parquet Import/Export

Native Parquet support for efficient storage:

```python
from sqlspec.storage import ParquetStorage

config = AdbcConfig(connection_config={"uri": "duckdb:///data.duckdb"})

# Export to Parquet (native, zero-copy)
storage = ParquetStorage(
    uri="s3://bucket/data/export.parquet",
    partition_strategy="fixed",
    partition_size=1000000,  # 1M rows per file
)

with config.provide_session() as session:
    # Export large table to partitioned Parquet
    session.export_to_storage(
        storage=storage,
        query="SELECT * FROM large_dataset WHERE date > $1",
        query_params=("2024-01-01",)
    )

    # Import from Parquet (native)
    session.import_from_storage(
        storage=storage,
        table_name="imported_data",
        if_exists="replace"
    )
```

### Strict Type Coercion

Control type conversion behavior:

```python
config = AdbcConfig(
    connection_config={"uri": "postgresql://localhost/db"},
    driver_features={
        "strict_type_coercion": True,  # Fail on invalid conversions
    }
)

with config.provide_session() as session:
    # This will raise an error if types don't match exactly
    session.execute("INSERT INTO users (id, name) VALUES ($1, $2)", "invalid", 123)
```

## Driver Features Configuration

```python
from sqlspec.adapters.adbc import AdbcDriverFeatures
from sqlspec.utils.serializers import to_json

config = AdbcConfig(
    connection_config={"uri": "postgresql://localhost/db"},
    driver_features=AdbcDriverFeatures(
        # JSON serialization function
        json_serializer=to_json,  # Default

        # Cast-aware parameter processing (PostgreSQL JSONB)
        enable_cast_detection=True,  # Default: True

        # Strict type coercion rules
        strict_type_coercion=False,  # Default: False

        # Preserve Arrow extension type metadata
        arrow_extension_types=True,  # Default: True
    )
)
```

## Best Practices

1. **Choose the right driver** - Use native drivers (asyncpg, psycopg) if you don't need Arrow
2. **Leverage Arrow ecosystem** - Use `.arrow()`, `.to_pandas()`, `.to_polars()` for zero-copy
3. **Stream large results** - Use `.arrow_reader()` for datasets larger than memory
4. **Set batch_size appropriately** - Balance memory usage and performance (10k-100k rows)
5. **Use Parquet for exports** - Native support avoids intermediate conversions
6. **Enable cast_detection** - For PostgreSQL JSONB and complex types
7. **Configure timeouts** - Set query_timeout and connection_timeout for long-running queries
8. **Understand parameter styles** - Each driver uses different placeholders
9. **Avoid connection pooling** - ADBC uses NoPoolSyncConfig (create new connections)
10. **Test DDL transactions** - ADBC doesn't support transactional DDL (supports_transactional_ddl=False)

## Common Issues

### 1. "Driver not found" or "Import error"

**Problem**: ADBC driver package not installed.

**Solution**: Install the specific driver package:

```bash
pip install adbc-driver-postgresql  # For PostgreSQL
pip install adbc-driver-duckdb      # For DuckDB
pip install adbc-driver-bigquery    # For BigQuery
```

Verify installation:

```python
import adbc_driver_postgresql.dbapi
print("PostgreSQL driver installed")
```

### 2. "Parameter style mismatch"

**Problem**: Using wrong placeholder syntax for the driver.

**Solution**: Check parameter style table and use correct syntax:

```python
# PostgreSQL - use $1, $2
session.execute("SELECT * FROM users WHERE id = $1", user_id)

# BigQuery - use @param
session.execute("SELECT * FROM users WHERE id = @user_id", {"user_id": 123})

# SQLite/DuckDB - use ?
session.execute("SELECT * FROM users WHERE id = ?", user_id)
```

### 3. "Memory error on large dataset"

**Problem**: Fetching entire result set into memory.

**Solution**: Use streaming with arrow_reader():

```python
# Bad - loads everything into memory
result = session.execute("SELECT * FROM huge_table")
all_data = result.all()  # OOM!

# Good - stream in batches
result = session.execute("SELECT * FROM huge_table")
reader = result.arrow_reader()
for batch in reader:
    process_batch(batch)  # Process incrementally
```

### 4. "Transactional DDL failed"

**Problem**: Attempting DDL within a transaction.

**Solution**: ADBC doesn't support transactional DDL. Run DDL outside transactions:

```python
# Don't do this
session.begin()
session.execute("CREATE TABLE new_table (id INT)")  # May fail
session.commit()

# Do this instead
session.execute("CREATE TABLE new_table (id INT)")  # Outside transaction
```

### 5. "URI auto-detection not working"

**Problem**: ADBC not detecting driver from URI.

**Solution**: Explicitly specify driver_name:

```python
# Auto-detection might fail for non-standard URIs
config = AdbcConfig(
    connection_config={
        "uri": "postgresql://localhost/db",
        "driver_name": "postgresql",  # Explicit
    }
)
```

## Important Notes

### No Transactional DDL

ADBC sets `supports_transactional_ddl = False`. This means:

- CREATE/DROP/ALTER statements cannot be rolled back
- Schema changes are immediately committed
- Avoid mixing DDL and DML in transactions

```python
# This works but DDL is not transactional
with config.provide_session() as session:
    session.execute("CREATE TABLE logs (id INT)")  # Committed immediately
    session.begin()
    session.execute("INSERT INTO logs VALUES (1)")  # Can rollback
    session.rollback()  # Only INSERT rolled back, CREATE persists
```

### No Connection Pooling

ADBC uses `NoPoolSyncConfig`:

- Each session creates a new connection
- No connection pool management
- Suitable for batch jobs, not high-concurrency web apps

```python
# Each session creates new connection
with config.provide_session() as session1:
    session1.execute("SELECT 1")

with config.provide_session() as session2:
    session2.execute("SELECT 1")  # New connection
```

### Synchronous Only

ADBC is sync-only (no async support):

```python
# This is correct (sync)
with config.provide_session() as session:
    result = session.execute("SELECT * FROM users")

# This won't work (no async)
async with config.provide_session() as session:  # Error!
    result = await session.execute("SELECT * FROM users")
```

Use asyncpg, asyncmy, or psqlpy for async workloads.

### Arrow Native Advantages

ADBC excels when working with Arrow ecosystem:

```python
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl

with config.provide_session() as session:
    # Zero-copy to Arrow
    arrow_table = session.execute("SELECT * FROM data").arrow()

    # Write to Parquet (zero-copy)
    pq.write_table(arrow_table, "output.parquet")

    # Convert to Polars (zero-copy)
    df = pl.from_arrow(arrow_table)

    # All operations avoid unnecessary data copies
```
