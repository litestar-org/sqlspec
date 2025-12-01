# OracleDB Adapter Skill

**Adapter:** Oracle Database (Sync & Async, Enterprise)
**Category:** Database Adapter
**Status:** Active

## Description

Expert guidance for using SQLSpec's OracleDB adapter for Oracle Database. The python-oracledb driver is Oracle's official Python driver, providing both synchronous and asynchronous connectivity to Oracle Database with support for Oracle Cloud Autonomous Database, enterprise connection pooling, and Oracle 23ai's modern features like VECTOR columns for AI/ML workloads.

This adapter supports dual sync/async patterns, making it suitable for both traditional web applications and modern async frameworks. It includes specialized type handlers for NumPy vectors (Oracle 23ai), UUID binary storage optimization, and automatic lowercase normalization for Oracle's uppercase identifier defaults.

## When to Use OracleDB

- **Oracle Database deployments** (on-premises or cloud)
- **Oracle Cloud Autonomous Database** (with wallet authentication)
- **Enterprise applications** (requiring Oracle-specific features)
- **Oracle 23ai AI/ML workloads** (with VECTOR data type support)
- **Dual sync/async requirements** (flexible deployment patterns)
- **High-performance connection pooling** (enterprise-grade pool management)
- **UUID optimization needs** (binary storage for 55% space savings)

## Configuration

### Synchronous Configuration

```python
from sqlspec.adapters.oracledb import OracleSyncConfig, OracleDriverFeatures

config = OracleSyncConfig(
    pool_config={
        # Basic connection:
        "dsn": "localhost:1521/XEPDB1",
        # OR individual parameters:
        "host": "localhost",
        "port": 1521,
        "service_name": "XEPDB1",
        "user": "myuser",
        "password": "mypass",

        # Pool settings:
        "min": 4,
        "max": 16,
        "increment": 1,
        "getmode": oracledb.POOL_GETMODE_WAIT,
        "timeout": 30,
        "wait_timeout": 1000,
        "max_lifetime_session": 3600,
        "ping_interval": 60,

        # Advanced:
        "threaded": True,
        "homogeneous": True,
        "soda_metadata_cache": False,
    },
    driver_features=OracleDriverFeatures(
        enable_numpy_vectors=True,  # Auto-detected if NumPy installed
        enable_lowercase_column_names=True,  # Default: True
        enable_uuid_binary=True,  # Default: True
    )
)
```

### Asynchronous Configuration

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleDriverFeatures

config = OracleAsyncConfig(
    pool_config={
        "dsn": "localhost:1521/XEPDB1",
        "user": "myuser",
        "password": "mypass",
        "min": 4,
        "max": 16,
    },
    driver_features={
        "enable_numpy_vectors": True,
        "enable_lowercase_column_names": True,
        "enable_uuid_binary": True,
    }
)

# Use with async context manager
async with config.provide_session() as session:
    result = await session.execute("SELECT * FROM users")
```

### Oracle Cloud Autonomous Database (Wallet)

```python
config = OracleSyncConfig(
    pool_config={
        "user": "ADMIN",
        "password": "MyCloudPassword123",
        "dsn": "mydb_high",  # TNS alias from tnsnames.ora
        "config_dir": "/path/to/wallet",  # Wallet directory
        "wallet_location": "/path/to/wallet",
        "wallet_password": "WalletPassword123",
    }
)
```

### Connection with SID (Legacy)

```python
config = OracleSyncConfig(
    pool_config={
        "host": "localhost",
        "port": 1521,
        "sid": "XE",  # Use SID instead of service_name
        "user": "myuser",
        "password": "mypass",
    }
)
```

## Parameter Style

**Named**: `:name`, `:param`, etc.

```python
# Single parameter
result = await session.execute(
    "SELECT * FROM users WHERE id = :id",
    {"id": user_id}
)

# Multiple parameters
result = await session.execute(
    "SELECT * FROM users WHERE status = :status AND age > :min_age",
    {"status": "active", "min_age": 18}
)

# Repeated parameters (same value used multiple times)
result = await session.execute(
    "SELECT * FROM orders WHERE user_id = :uid OR assigned_to = :uid",
    {"uid": user_id}
)
```

## Special Features

### NumPy Vector Support (Oracle 23ai)

Automatic bidirectional conversion between NumPy arrays and Oracle VECTOR columns:

```python
import numpy as np

# Auto-enabled if NumPy installed
config = OracleSyncConfig(
    pool_config={...},
    driver_features={"enable_numpy_vectors": True}  # Auto-detected
)

# Insert NumPy array as VECTOR
embedding = np.random.rand(1536).astype(np.float32)

with config.provide_session() as session:
    session.execute(
        "INSERT INTO embeddings (id, vector) VALUES (:id, :vec)",
        {"id": 1, "vec": embedding}
    )

    # Query returns NumPy array automatically
    result = session.execute(
        "SELECT vector FROM embeddings WHERE id = :id",
        {"id": 1}
    ).one()

    vector = result["vector"]  # NumPy ndarray
    assert isinstance(vector, np.ndarray)
    assert vector.dtype == np.float32
```

**Supported dtypes**: float32, float64, int8, uint8

**Requirements**: NumPy installed, Oracle Database 23ai+, VECTOR column type

### UUID Binary Storage Optimization

Automatic conversion between Python UUIDs and RAW(16) binary format:

```python
import uuid

config = OracleSyncConfig(
    pool_config={...},
    driver_features={"enable_uuid_binary": True}  # Default: True
)

# Create table with RAW(16) column
with config.provide_session() as session:
    session.execute("""
        CREATE TABLE users (
            id RAW(16) PRIMARY KEY,
            email VARCHAR2(255)
        )
    """)

    # Insert UUID (automatically converted to 16 bytes)
    user_id = uuid.uuid4()
    session.execute(
        "INSERT INTO users (id, email) VALUES (:id, :email)",
        {"id": user_id, "email": "alice@example.com"}
    )

    # Query returns UUID object automatically
    result = session.execute(
        "SELECT id FROM users WHERE email = :email",
        {"email": "alice@example.com"}
    ).one()

    retrieved_id = result["id"]  # uuid.UUID
    assert isinstance(retrieved_id, uuid.UUID)
    assert retrieved_id == user_id
```

**Benefits**:
- 16 bytes vs 36 bytes (55% space savings)
- Type-safe UUID objects in Python
- Faster comparisons (binary vs string)
- Index efficiency (smaller keys)

**Only applies to RAW(16) columns** - other RAW sizes remain unchanged.

### Lowercase Column Name Normalization

Oracle defaults unquoted identifiers to uppercase. SQLSpec normalizes to lowercase for Python compatibility:

```python
config = OracleSyncConfig(
    pool_config={...},
    driver_features={"enable_lowercase_column_names": True}  # Default: True
)

with config.provide_session() as session:
    # Oracle stores as FIRST_NAME, LAST_NAME (uppercase)
    session.execute("""
        CREATE TABLE users (
            first_name VARCHAR2(100),
            last_name VARCHAR2(100)
        )
    """)

    result = session.execute("SELECT * FROM users").one()

    # Access with lowercase (normalized)
    first = result["first_name"]  # Works!
    last = result["last_name"]    # Works!

    # Original uppercase still works
    first = result["FIRST_NAME"]  # Also works
```

**Preserves case-sensitive aliases**:
```python
# Quoted alias preserved as-is
result = session.execute(
    'SELECT user_id AS "userId" FROM users'
).one()

user_id = result["userId"]  # Exact case preserved
```

## Performance Features

### Native Arrow Import/Export

Direct Arrow integration for high-performance data transfer:

```python
import pyarrow as pa

# Export to Arrow
result = session.execute("SELECT * FROM large_table").to_arrow()
arrow_table: pa.Table = result  # Zero-copy when possible

# Import from Arrow
session.load_arrow(arrow_table, "target_table")
```

### Native Parquet Import/Export

Built-in Parquet support without intermediate formats:

```python
# Export to Parquet
session.execute("SELECT * FROM users").to_parquet("/tmp/users.parquet")

# Import from Parquet
session.load_parquet("/tmp/users.parquet", "users_import")
```

### Enterprise Connection Pooling

Oracle's connection pool provides production-grade resource management:

```python
config = OracleSyncConfig(
    pool_config={
        "dsn": "localhost:1521/XEPDB1",
        "user": "myuser",
        "password": "mypass",

        # Pool sizing
        "min": 4,                    # Keep 4 connections warm
        "max": 16,                   # Allow up to 16 total
        "increment": 1,              # Grow by 1 when needed

        # Timeout & lifecycle
        "timeout": 30,               # Pool acquisition timeout (seconds)
        "wait_timeout": 1000,        # Wait for connection (milliseconds)
        "max_lifetime_session": 3600,  # Recycle after 1 hour
        "ping_interval": 60,         # Health check every 60 seconds

        # Behavior
        "getmode": oracledb.POOL_GETMODE_WAIT,  # Wait vs fail fast
        "threaded": True,            # Thread safety
        "homogeneous": True,         # Same credentials for all
    }
)
```

### Session Callbacks for Custom Initialization

```python
def init_session(connection, tag):
    """Called for each new connection from pool."""
    cursor = connection.cursor()
    cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
    cursor.execute("ALTER SESSION SET TIME_ZONE = 'UTC'")
    cursor.close()

config = OracleSyncConfig(
    pool_config={
        "dsn": "localhost:1521/XEPDB1",
        "user": "myuser",
        "password": "mypass",
        "session_callback": init_session,
    }
)
```

## Oracle-Specific Features

### RETURNING Clause

```python
# Get inserted ID or computed values
result = session.execute("""
    INSERT INTO users (name, email, created_at)
    VALUES (:name, :email, SYSTIMESTAMP)
    RETURNING id, created_at INTO :new_id, :new_ts
""", {
    "name": "Alice",
    "email": "alice@example.com",
    "new_id": session.connection.cursor().var(int),
    "new_ts": session.connection.cursor().var(str)
})

new_id = result["new_id"]
created_at = result["new_ts"]
```

### PL/SQL Stored Procedures

```python
# Call stored procedure
cursor = session.connection.cursor()
result = cursor.var(str)

cursor.callproc("get_user_status", [user_id, result])
status = result.getvalue()
```

### Batch Operations (executemany)

```python
# Efficient bulk insert
users = [
    {"name": "Alice", "email": "alice@example.com"},
    {"name": "Bob", "email": "bob@example.com"},
    {"name": "Carol", "email": "carol@example.com"},
]

session.execute_many(
    "INSERT INTO users (name, email) VALUES (:name, :email)",
    users
)
```

## Best Practices

1. **Use connection pooling** - Essential for production (min=4, max=16 is a good start)
2. **Enable UUID binary storage** - 55% space savings over VARCHAR2(36)
3. **Use lowercase normalization** - Better Python/schema library compatibility
4. **Set ping_interval** - Detect stale connections (60 seconds recommended)
5. **Configure session callbacks** - Initialize NLS settings, time zones consistently
6. **Use wallet for Cloud** - Secure credential management for Autonomous Database
7. **Leverage native Arrow/Parquet** - 10-100x faster for large datasets
8. **Set max_lifetime_session** - Prevent connection leaks (3600 seconds recommended)
9. **Use RETURNING clause** - Avoid extra round-trips for generated values
10. **Optimize pool sizing** - Monitor connection usage, adjust min/max accordingly

## Common Issues

### "ORA-12154: TNS:could not resolve the connect identifier"

**Problem**: Oracle cannot find the service name or TNS alias.

**Solution**:
```python
# Use full DSN string instead of TNS alias
config = OracleSyncConfig(
    pool_config={
        "dsn": "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=XEPDB1)))"
    }
)

# OR set TNS_ADMIN environment variable
import os
os.environ["TNS_ADMIN"] = "/path/to/tnsnames_dir"
```

### "ORA-01017: invalid username/password"

**Problem**: Authentication failure or wallet not found.

**Solution for Cloud Wallet**:
```python
config = OracleSyncConfig(
    pool_config={
        "user": "ADMIN",
        "password": "CloudPassword123",
        "dsn": "mydb_high",  # Must match tnsnames.ora alias
        "config_dir": "/absolute/path/to/wallet",  # Use absolute path
        "wallet_location": "/absolute/path/to/wallet",
        "wallet_password": "WalletPassword123",
    }
)
```

### "Pool is exhausted" or "ORA-24418: Cannot open further sessions"

**Problem**: All pool connections in use, or database session limit reached.

**Solution**:
```python
# Increase pool size
config = OracleSyncConfig(
    pool_config={
        "max": 32,  # Increase from 16
        "wait_timeout": 5000,  # Wait longer (5 seconds)
    }
)

# OR check database session limit
# SQL> SELECT value FROM v$parameter WHERE name = 'sessions';
# Increase if needed: ALTER SYSTEM SET sessions=500 SCOPE=SPFILE;
```

### NumPy vectors not converting

**Problem**: VECTOR columns return as strings instead of NumPy arrays.

**Solution**:
```python
# Ensure NumPy installed
pip install numpy

# Ensure feature enabled (should auto-detect)
config = OracleSyncConfig(
    driver_features={"enable_numpy_vectors": True}
)

# Verify Oracle 23ai with VECTOR support
# SQL> SELECT * FROM v$version;  -- Should be 23ai or higher
```

### Case sensitivity issues with column names

**Problem**: Lowercase column access fails or returns None.

**Solution**:
```python
# Enable lowercase normalization (default: True)
config = OracleSyncConfig(
    driver_features={"enable_lowercase_column_names": True}
)

# For case-sensitive columns, quote them in DDL
session.execute('''
    CREATE TABLE users (
        "userId" NUMBER PRIMARY KEY,  -- Quoted = case-sensitive
        email VARCHAR2(255)           -- Unquoted = uppercase
    )
''')

result = session.execute("SELECT * FROM users").one()
user_id = result["userId"]  # Exact case
email = result["email"]     # Lowercase normalized
```

## Important Notes

### ⚠️ No Transactional DDL

Oracle Database does **NOT** support transactional DDL. This means:
- DDL statements (CREATE, ALTER, DROP) are NOT automatically rolled back on error
- Each DDL statement commits immediately and cannot be undone
- Plan DDL operations carefully and consider backups before schema changes
- Use explicit transaction boundaries only for DML (INSERT, UPDATE, DELETE)

**Example of non-transactional behavior**:
```python
with config.provide_session() as session:
    try:
        await session.begin()

        # This commits immediately - NOT rolled back!
        await session.execute("CREATE TABLE temp_table (id NUMBER)")

        # Subsequent error won't undo the CREATE TABLE
        await session.execute("INSERT INTO nonexistent VALUES (1)")

        await session.commit()  # Never reached
    except Exception:
        await session.rollback()  # Rollback won't affect CREATE TABLE
        # temp_table still exists in database!
```

### Wallet Security

- Never commit wallet files to version control
- Use environment variables for wallet passwords
- Rotate wallet credentials regularly
- Use separate wallets for dev/staging/prod

### Performance Tuning

- Use `EXPLAIN PLAN` to analyze query performance
- Create indexes on frequently queried columns
- Consider partitioning for large tables (100M+ rows)
- Use bind variables (`:name` style) to prevent SQL injection and improve parsing cache hits

### Connection Lifecycle

- Connections are pooled - don't create/close manually
- Use context managers (`provide_session()`) for automatic cleanup
- Set `ping_interval` to detect broken connections
- Monitor pool health with Oracle's connection statistics

## Performance Benchmarks

Compared to other Oracle drivers:

- **python-oracledb (thin)**: Baseline, pure Python, no Oracle Client required
- **python-oracledb (thick)**: ~20-30% faster, requires Oracle Client libraries
- **cx_Oracle**: Legacy (replaced by python-oracledb)

**NumPy vector operations**:
- NumPy ↔ VECTOR conversion: ~5-10x faster than string parsing
- Binary UUID storage: 55% space savings, ~2x faster index lookups

**Connection pooling**:
- Pool overhead: <1ms per acquisition
- Session callback overhead: ~2-5ms per new connection

For most applications, python-oracledb (thin mode) provides excellent performance without requiring Oracle Client installation. Use thick mode only when needing maximum throughput or Oracle Client-specific features.
