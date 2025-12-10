# SQLSpec Configuration Patterns

Complete guide to configuring SQLSpec across all supported database adapters.

## Configuration Structure

All SQLSpec configurations follow a consistent four-tier model:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.{adapter} import {Adapter}Config

spec = SQLSpec()
db = spec.add_config(
    {Adapter}Config(
        connection_config={...},           # Tier 1: Connection parameters
        statement_config={...},      # Tier 2: SQL processing (optional)
        extension_config={...},      # Tier 3: Framework integration (optional)
        driver_features={...},       # Tier 4: Adapter-specific features (optional)
        migration_config={...},      # Migrations (optional)
    )
)
```

## Tier 1: connection_config (Connection Parameters)

Adapter-specific connection settings. Each adapter has different parameters.

### PostgreSQL Adapters

**AsyncPG (Async, High Performance):**
```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    connection_config={
        "dsn": "postgresql://user:pass@localhost:5432/dbname",
        # OR individual parameters:
        "host": "localhost",
        "port": 5432,
        "user": "myuser",
        "password": "mypass",
        "database": "mydb",
        # Pool settings:
        "min_size": 10,
        "max_size": 20,
        "max_queries": 50000,
        "max_inactive_connection_lifetime": 300.0,
        "timeout": 60.0,
    }
)
```

**Psycopg (Sync/Async, Feature-Rich):**
```python
from sqlspec.adapters.psycopg import PsycopgConfig, PsycopgAsyncConfig

# Sync
config = PsycopgConfig(
    connection_config={
        "conninfo": "postgresql://localhost/db",
        "min_size": 4,
        "max_size": 10,
    }
)

# Async
config = PsycopgAsyncConfig(
    connection_config={
        "conninfo": "postgresql://localhost/db",
        "min_size": 4,
        "max_size": 10,
    }
)
```

**Psqlpy (Rust-Based, High Performance):**
```python
from sqlspec.adapters.psqlpy import PsqlpyConfig

config = PsqlpyConfig(
    connection_config={
        "dsn": "postgresql://localhost/db",
        "max_db_pool_size": 20,
    }
)
```

### SQLite Adapters

**SQLite (Sync):**
```python
from sqlspec.adapters.sqlite import SqliteConfig

config = SqliteConfig(
    connection_config={
        "database": "/path/to/database.db",  # or ":memory:"
        "timeout": 5.0,
        "check_same_thread": False,  # For multi-threaded use
    }
)
```

**AioSQLite (Async):**
```python
from sqlspec.adapters.aiosqlite import AiosqliteConfig

config = AiosqliteConfig(
    connection_config={
        "database": "/path/to/database.db",
        "timeout": 5.0,
    }
)
```

### DuckDB

```python
from sqlspec.adapters.duckdb import DuckDBConfig

config = DuckDBConfig(
    connection_config={
        "database": "/path/to/database.duckdb",  # or ":memory:"
        "read_only": False,
        "config": {
            "memory_limit": "8GB",
            "threads": 4,
        }
    }
)
```

### Oracle

```python
from sqlspec.adapters.oracledb import OracleConfig, OracleAsyncConfig

# Sync
config = OracleConfig(
    connection_config={
        "user": "myuser",
        "password": "mypass",
        "dsn": "localhost:1521/ORCLPDB1",
        "min": 2,
        "max": 10,
        "increment": 1,
    }
)

# Async
config = OracleAsyncConfig(
    connection_config={
        "user": "myuser",
        "password": "mypass",
        "dsn": "localhost:1521/ORCLPDB1",
        "min": 2,
        "max": 10,
    }
)
```

### MySQL/MariaDB

**Asyncmy (Async):**
```python
from sqlspec.adapters.asyncmy import AsyncmyConfig

config = AsyncmyConfig(
    connection_config={
        "host": "localhost",
        "port": 3306,
        "user": "myuser",
        "password": "mypass",
        "db": "mydb",
        "minsize": 1,
        "maxsize": 10,
    }
)
```

### BigQuery

```python
from sqlspec.adapters.bigquery import BigQueryConfig

config = BigQueryConfig(
    connection_config={
        "project": "my-gcp-project",
        "dataset": "my_dataset",
        "credentials": "/path/to/service-account.json",  # or None for ADC
    }
)
```

### ADBC (Apache Arrow Database Connectivity)

```python
from sqlspec.adapters.adbc import ADBCConfig

config = ADBCConfig(
    connection_config={
        "driver": "adbc_driver_postgresql",
        "uri": "postgresql://localhost/db",
        # Driver-specific options
        "db_kwargs": {
            "username": "myuser",
            "password": "mypass",
        }
    }
)
```

## Tier 2: statement_config (SQL Processing)

Controls SQL statement parsing, validation, and transformation:

```python
from sqlspec.core import StatementConfig

config = AsyncpgConfig(
    connection_config={...},
    statement_config=StatementConfig(
        enable_validation=True,        # Validate SQL syntax
        enable_transformations=True,   # Apply SQL transformations
        enable_security_checks=True,   # Check for SQL injection
        max_query_size=1_000_000,     # Max query size in bytes
    )
)
```

**When to customize:**
- Disable validation for trusted, performance-critical queries
- Increase max_query_size for complex queries
- Disable transformations for specific SQL dialects

## Tier 3: extension_config (Framework Integration)

Framework-specific settings keyed by framework name.

### Litestar

```python
config = AsyncpgConfig(
    connection_config={...},
    extension_config={
        "litestar": {
            "connection_key": "postgres_connection",
            "pool_key": "postgres_pool",
            "session_key": "db_session",  # Default, used for DI
            "commit_mode": "autocommit",  # or "manual", "autocommit_include_redirect"
            "extra_commit_statuses": {201, 202},  # Additional status codes to commit on
            "extra_rollback_statuses": {422},     # Additional status codes to rollback on
            "enable_correlation_middleware": True,  # Request tracking
            "disable_di": False,  # Set True to disable built-in DI
        }
    }
)
```

### Starlette/FastAPI

```python
config = AsyncpgConfig(
    connection_config={...},
    extension_config={
        "starlette": {  # Same key for FastAPI
            "connection_key": "postgres_connection",
            "pool_key": "postgres_pool",
            "session_key": "db_session",
            "commit_mode": "autocommit",
            "extra_commit_statuses": None,
            "extra_rollback_statuses": None,
            "disable_di": False,
        }
    }
)
```

### Flask

```python
config = SqliteConfig(
    connection_config={...},
    extension_config={
        "flask": {
            "connection_key": "db_connection",
            "session_key": "db_session",
            "commit_mode": "manual",  # Flask typically uses manual
            "disable_di": False,
        }
    }
)
```

**Commit Modes:**
- `manual`: No automatic transaction management
- `autocommit`: Commit on 2xx responses, rollback on 4xx/5xx
- `autocommit_include_redirect`: Commit on 2xx and 3xx responses

## Tier 4: driver_features (Adapter-Specific Features)

Optional features that require additional dependencies or control adapter behavior.

### AsyncPG driver_features

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriverFeatures

config = AsyncpgConfig(
    connection_config={...},
    driver_features=AsyncpgDriverFeatures(
        enable_pgvector=True,  # Auto-detected if pgvector installed
        enable_json_codecs=True,  # Register JSON codecs
        json_serializer=custom_json_encoder,  # Custom JSON encoder
        json_deserializer=custom_json_decoder,  # Custom JSON decoder
        enable_cloud_sql=False,  # Google Cloud SQL connector
        cloud_sql_instance="project:region:instance",
        cloud_sql_enable_iam_auth=False,
        cloud_sql_ip_type="PRIVATE",
        enable_alloydb=False,  # Google AlloyDB connector
        alloydb_instance_uri="projects/.../instances/...",
        alloydb_enable_iam_auth=False,
    )
)
```

### Psycopg driver_features

```python
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgDriverFeatures

config = PsycopgAsyncConfig(
    connection_config={...},
    driver_features=PsycopgDriverFeatures(
        enable_pgvector=True,
        enable_json_codecs=True,
        json_serializer=custom_encoder,
    )
)
```

### DuckDB driver_features

```python
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriverFeatures

config = DuckDBConfig(
    connection_config={...},
    driver_features=DuckDBDriverFeatures(
        enable_uuid_conversion=True,  # Convert UUID strings to UUID objects
        json_serializer=orjson.dumps,  # Use orjson for performance
        extensions=["httpfs", "parquet"],  # Load DuckDB extensions
        secrets={  # Register secrets for extensions
            "s3": {"key": "...", "secret": "..."}
        }
    )
)
```

### Oracle driver_features

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleDriverFeatures

config = OracleAsyncConfig(
    connection_config={...},
    driver_features=OracleDriverFeatures(
        enable_numpy_vectors=True,  # NumPy array ↔ Oracle VECTOR conversion
        enable_uuid_binary=True,  # UUID ↔ RAW(16) conversion
    )
)
```

### SQLite/AioSQLite driver_features

```python
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriverFeatures

config = AiosqliteConfig(
    connection_config={...},
    driver_features=AiosqliteDriverFeatures(
        enable_json_detection=True,  # Detect and parse JSON strings
        json_serializer=json.dumps,
    )
)
```

## Multi-Database Configuration

Configure multiple databases with unique keys:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.duckdb import DuckDBConfig

spec = SQLSpec()

# Primary PostgreSQL database
primary = spec.add_config(
    AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/main"},
        extension_config={
            "litestar": {"session_key": "primary_db"}
        }
    )
)

# Analytics DuckDB database
analytics = spec.add_config(
    DuckDBConfig(
        connection_config={"database": "analytics.duckdb"},
        extension_config={
            "litestar": {"session_key": "analytics_db"}
        }
    )
)

# Use in handlers via dependency injection
@get("/report")
async def report(primary_db: AsyncpgDriver, analytics_db: DuckDBDriver):
    users = await primary_db.select_value("SELECT COUNT(*) FROM users")
    events = await analytics_db.select_value("SELECT COUNT(*) FROM events")
    return {"users": users, "events": events}
```

**Key Requirements:**
1. Unique `session_key` for each database
2. Framework extension validates key uniqueness
3. Access via type annotation or custom key

## Migration Configuration

Configure migration behavior:

```python
config = AsyncpgConfig(
    connection_config={...},
    migration_config={
        "script_location": "migrations",  # Directory for migration files
        "version_table": "sqlspec_version",  # Version tracking table
        "include_extensions": ["litestar"],  # Include framework migrations
        "template_directory": "templates/migrations",  # Custom templates
    }
)
```

## Configuration Best Practices

### 1. Store Config Keys

```python
# GOOD
db = spec.add_config(AsyncpgConfig(...))
with spec.provide_session(db) as session:
    pass

# BAD
spec.add_config(AsyncpgConfig(...))  # Lost reference!
```

### 2. Use TypedDict for driver_features

```python
# GOOD - IDE autocomplete, type checking
from sqlspec.adapters.asyncpg import AsyncpgDriverFeatures

config = AsyncpgConfig(
    driver_features=AsyncpgDriverFeatures(
        enable_pgvector=True
    )
)

# ACCEPTABLE - Plain dict
config = AsyncpgConfig(
    driver_features={"enable_pgvector": True}
)
```

### 3. Auto-Detect Optional Features

Most adapters auto-detect optional features:

```python
# pgvector auto-enabled if package installed
config = AsyncpgConfig(connection_config={...})
# driver_features["enable_pgvector"] auto-set based on import

# Explicit override
config = AsyncpgConfig(
    connection_config={...},
    driver_features={"enable_pgvector": False}  # Force disable
)
```

### 4. Use Unique Keys for Multi-Database

```python
# Each database needs unique session_key
primary = spec.add_config(AsyncpgConfig(
    extension_config={"litestar": {"session_key": "primary"}}
))
cache = spec.add_config(SqliteConfig(
    extension_config={"litestar": {"session_key": "cache"}}
))
```

### 5. Environment-Specific Configuration

```python
import os

config = AsyncpgConfig(
    connection_config={
        "dsn": os.getenv("DATABASE_URL"),
        "min_size": int(os.getenv("DB_POOL_MIN", "10")),
        "max_size": int(os.getenv("DB_POOL_MAX", "20")),
    },
    statement_config=StatementConfig(
        enable_validation=os.getenv("ENV") != "production",
    )
)
```

## Common Configuration Errors

### Error: "Invalid connection_config parameter"

**Cause:** Using wrong parameter name for adapter

```python
# WRONG - using asyncpg params for psycopg
config = PsycopgConfig(
    connection_config={"dsn": "...", "min_size": 10}
)

# CORRECT
config = PsycopgConfig(
    connection_config={"conninfo": "...", "min_size": 10}
)
```

### Error: "Duplicate state keys found"

**Cause:** Multiple configs using same session_key

```python
# WRONG
spec.add_config(AsyncpgConfig(
    extension_config={"litestar": {"session_key": "db"}}
))
spec.add_config(DuckDBConfig(
    extension_config={"litestar": {"session_key": "db"}}  # Duplicate!
))

# CORRECT
spec.add_config(AsyncpgConfig(
    extension_config={"litestar": {"session_key": "postgres"}}
))
spec.add_config(DuckDBConfig(
    extension_config={"litestar": {"session_key": "duckdb"}}
))
```

### Error: "Config not found in registry"

**Cause:** Not storing config key

```python
# WRONG
spec.add_config(AsyncpgConfig(...))
with spec.provide_session(AsyncpgConfig(...)) as session:  # Different instance!
    pass

# CORRECT
db = spec.add_config(AsyncpgConfig(...))
with spec.provide_session(db) as session:
    pass
```

## Adapter Selection Guide

| Use Case | Recommended Adapter | Reason |
|----------|-------------------|--------|
| PostgreSQL async | `asyncpg` | Fastest, most mature |
| PostgreSQL sync | `psycopg` | Feature-rich, widely used |
| PostgreSQL extreme perf | `psqlpy` | Rust-based, highest throughput |
| Embedded database | `sqlite` or `duckdb` | No server required |
| Analytics queries | `duckdb` | Columnar, OLAP-optimized |
| Oracle enterprise | `oracledb` | Official Oracle driver |
| MySQL/MariaDB | `asyncmy` | Async, good performance |
| Cloud data warehouse | `bigquery` | Native GCP integration |
| Multi-database | `adbc` | Standardized interface |
| Arrow ecosystem | `adbc` or `duckdb` | Native Arrow support |
