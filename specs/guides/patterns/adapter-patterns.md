# Cross-Adapter Patterns

Patterns that apply across multiple database adapters in SQLSpec. These patterns ensure consistency, maintainability, and feature parity across all supported databases.

## Table of Contents

1. [Configuration Pattern](#configuration-pattern)
2. [Type Handler Pattern](#type-handler-pattern)
3. [Exception Handling Pattern](#exception-handling-pattern)
4. [Connection Lifecycle Pattern](#connection-lifecycle-pattern)
5. [driver_features Pattern](#driver_features-pattern)
6. [Parameter Style Pattern](#parameter-style-pattern)
7. [Arrow Integration Pattern](#arrow-integration-pattern)

---

## Configuration Pattern

### Context

**When to use this pattern**:
- Creating a new database adapter
- Adding configuration options to existing adapter
- Supporting connection pooling
- Multi-database applications (bind_key)

**When NOT to use this pattern**:
- Runtime-only configuration (use driver_features instead)
- Temporary query-level overrides (use statement_config)

### Problem

Database adapters need consistent configuration interfaces across different database libraries, each with unique connection parameters, pool settings, and feature flags.

**Symptoms**:
- Inconsistent config APIs across adapters
- Difficulty switching between databases
- Hard to discover available configuration options
- Type checking doesn't catch config errors

**Root cause**:
Each database library has its own configuration style, parameter names, and defaults.

### Solution

Use TypedDict for strongly-typed configuration with three-tier structure:
1. **ConnectionConfig**: Basic connection parameters
2. **PoolConfig**: Connection pool settings (inherits ConnectionConfig)
3. **DatabaseConfig**: SQLSpec wrapper with connection_config, driver_features, bind_key

**Key principles**:
1. Use TypedDict with NotRequired fields for optional parameters
2. Inherit PoolConfig from ConnectionConfig to DRY
3. Provide explicit connection_config parameter (dict or TypedDict)
4. Support connection_instance for pre-configured pools
5. Include bind_key for multi-database support
6. Use extension_config for framework-specific settings

### Code Example

#### Minimal Example

```python
from typing import TypedDict
from typing_extensions import NotRequired

class SimpleConnectionConfig(TypedDict):
    """Basic connection parameters."""
    dsn: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]

class SimplePoolConfig(SimpleConnectionConfig):
    """Pool parameters, inheriting connection parameters."""
    min_size: NotRequired[int]
    max_size: NotRequired[int]
```

#### Full Example (AsyncPG)

```python
"""AsyncPG database configuration with direct field-based configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict
from typing_extensions import NotRequired

from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig


class AsyncpgConnectionConfig(TypedDict):
    """TypedDict for AsyncPG connection parameters."""

    dsn: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    ssl: NotRequired[Any]
    connect_timeout: NotRequired[float]
    command_timeout: NotRequired[float]
    statement_cache_size: NotRequired[int]


class AsyncpgPoolConfig(AsyncpgConnectionConfig):
    """TypedDict for AsyncPG pool parameters, inheriting connection parameters."""

    min_size: NotRequired[int]
    max_size: NotRequired[int]
    max_queries: NotRequired[int]
    max_inactive_connection_lifetime: NotRequired[float]
    setup: NotRequired[Callable[[Connection], Awaitable[None]]]
    init: NotRequired[Callable[[Connection], Awaitable[None]]]


class AsyncpgConfig(AsyncDatabaseConfig[AsyncpgConnection, Pool, AsyncpgDriver]):
    """Configuration for AsyncPG database connections."""

    driver_type: ClassVar[type[AsyncpgDriver]] = AsyncpgDriver
    connection_type: ClassVar[type[AsyncpgConnection]] = type(AsyncpgConnection)
    supports_transactional_ddl: ClassVar[bool] = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True

    def __init__(
        self,
        *,
        connection_config: "AsyncpgPoolConfig | dict[str, Any] | None" = None,
        connection_instance: "Pool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "AsyncpgDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
    ) -> None:
        """Initialize AsyncPG configuration.

        Args:
            connection_config: Pool configuration (TypedDict or dict)
            connection_instance: Existing pool to use
            migration_config: Migration settings
            statement_config: Statement processing overrides
            driver_features: Feature flags (TypedDict or dict)
            bind_key: Unique identifier for multi-database
            extension_config: Framework-specific settings
        """
        # Process driver_features with defaults
        features_dict: dict[str, Any] = dict(driver_features) if driver_features else {}
        features_dict.setdefault("enable_json_codecs", True)
        features_dict.setdefault("enable_pgvector", PGVECTOR_INSTALLED)

        super().__init__(
            connection_config=dict(connection_config) if connection_config else {},
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
        )
```

#### Anti-Pattern Example

```python
# BAD - Using **kwargs without type hints
class BadConfig:
    def __init__(self, **kwargs):
        self.config = kwargs  # No type safety!

# BAD - Mixing connection and pool params without structure
class BadConfig2:
    def __init__(self, host, port, min_pool, max_pool):
        # Hard to extend, no optional params
        pass

# BAD - Using separate parameters instead of TypedDict
class BadConfig3:
    def __init__(
        self,
        dsn=None,
        host=None,
        port=None,
        # ... 30 more parameters
    ):
        # Parameter explosion!
        pass

# GOOD - TypedDict with inheritance
class GoodConnectionConfig(TypedDict):
    dsn: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]

class GoodPoolConfig(GoodConnectionConfig):
    min_size: NotRequired[int]
    max_size: NotRequired[int]
```

### Variations

#### Variation 1: Sync Adapter

For synchronous adapters (psycopg, oracledb sync):

```python
from sqlspec.config import SyncDatabaseConfig

class OracleSyncConfig(SyncDatabaseConfig[OracleSyncConnection, OracleSyncConnectionPool, OracleSyncDriver]):
    """Synchronous Oracle configuration."""

    def _create_pool(self) -> "OracleSyncConnectionPool":
        """Create sync connection pool."""
        return oracledb.create_pool(**self.connection_config)

    @contextlib.contextmanager
    def provide_connection(self) -> "Generator[OracleSyncConnection, None, None]":
        """Provide sync connection."""
        if self.connection_instance is None:
            self.connection_instance = self.create_pool()
        conn = self.connection_instance.acquire()
        try:
            yield conn
        finally:
            self.connection_instance.release(conn)
```

#### Variation 2: External Connector (Cloud SQL, AlloyDB)

For Google Cloud connectors that require custom connection factory:

```python
def _setup_cloud_sql_connector(self, config: "dict[str, Any]") -> None:
    """Setup Cloud SQL connector and modify pool config."""
    from google.cloud.sql.connector import Connector

    self._cloud_sql_connector = Connector()

    async def get_conn() -> "AsyncpgConnection":
        conn: AsyncpgConnection = await self._cloud_sql_connector.connect_async(
            instance_connection_string=self.driver_features["cloud_sql_instance"],
            driver="asyncpg",
            enable_iam_auth=self.driver_features.get("cloud_sql_enable_iam_auth", False),
        )
        return conn

    # Remove standard connection params, use factory instead
    for key in ("dsn", "host", "port", "user", "password"):
        config.pop(key, None)

    config["connect"] = get_conn
```

### Related Patterns

- **driver_features Pattern** - Feature flag management
- **Connection Lifecycle Pattern** - Pool creation and cleanup
- **Framework Extension Pattern** (integration-patterns.md) - extension_config usage

### SQLSpec Files

**Core implementation**:
- `/home/cody/code/litestar/sqlspec/sqlspec/config.py` - Base DatabaseConfig classes
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/config.py` - AsyncPG implementation
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/oracledb/config.py` - Oracle implementation

**Tests**:
- `/home/cody/code/litestar/sqlspec/tests/unit/test_adapters/test_asyncpg/test_config.py`

### References

- **Documentation**: `docs/guides/adapters/`
- **External**: [PEP 589 TypedDict](https://peps.python.org/pep-0589/)

---

## Type Handler Pattern

### Context

**When to use this pattern**:
- Adding support for optional database-specific types (vectors, arrays, JSON)
- Automatic conversion between Python and database types
- Feature requires external package (pgvector, NumPy)
- Type support varies by database version

**When NOT to use this pattern**:
- Standard types already supported by driver
- Simple type coercion (use ParameterStyleConfig instead)
- Runtime parameter validation (use driver logic)

### Problem

Databases support specialized types (PostgreSQL vectors, Oracle VECTOR, MySQL JSON) that require bidirectional conversion between Python objects and database representations. These types often require optional dependencies that may not be installed.

**Symptoms**:
- Users can't insert Python objects into specialized columns
- Query results return raw database types instead of Python objects
- Import errors when optional package not installed
- Configuration breaks when feature unavailable

**Root cause**:
Database drivers provide extension points (input/output handlers) but each has different APIs and registration mechanisms.

### Solution

Create adapter-specific handler modules with graceful degradation when optional packages unavailable. Use driver_features to control registration.

**Key principles**:
1. Separate handler registration into dedicated `_*_handlers.py` modules
2. Check package availability at module level
3. Return early with DEBUG log if package not installed
4. Register handlers in connection init callback (setup, init, session_callback)
5. Use driver_features flag to control registration

**Implementation steps**:
1. Create `_type_handlers.py` module in adapter directory
2. Define `register_*_handlers(connection)` function
3. Check package availability, return early if missing
4. Register input/output handlers using driver API
5. Add driver_features flag in config `__init__`
6. Call registration in connection init callback

### Code Example

#### Minimal Example

```python
import logging
from typing import TYPE_CHECKING

from sqlspec.typing import OPTIONAL_PACKAGE_INSTALLED

if TYPE_CHECKING:
    from connection_type import Connection

logger = logging.getLogger(__name__)


def register_optional_handlers(connection: "Connection") -> None:
    """Register optional type handlers.

    Args:
        connection: Database connection instance.
    """
    if not OPTIONAL_PACKAGE_INSTALLED:
        logger.debug("Optional package not installed - skipping handlers")
        return

    # Register handlers using driver API
    connection.register_type("custom_type", python_converter)
    logger.debug("Registered optional type handlers")
```

#### Full Example (PostgreSQL pgvector)

```python
"""AsyncPG type handlers for pgvector support."""

import logging
from typing import TYPE_CHECKING

from sqlspec.typing import PGVECTOR_INSTALLED

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg._types import AsyncpgConnection

__all__ = ("register_pgvector_support",)

logger = logging.getLogger(__name__)


def _is_missing_vector_error(error: Exception) -> bool:
    """Check if error indicates vector extension not installed."""
    message = str(error).lower()
    return 'type "vector" does not exist' in message or "unknown type" in message


async def register_pgvector_support(connection: "AsyncpgConnection") -> None:
    """Register pgvector extension support on asyncpg connection.

    Enables automatic conversion between Python vector types and PostgreSQL
    VECTOR columns when the pgvector library is installed. Gracefully skips
    if pgvector is not available.

    Args:
        connection: AsyncPG connection instance.
    """
    if not PGVECTOR_INSTALLED:
        logger.debug("pgvector not installed - skipping vector type support")
        return

    try:
        import pgvector.asyncpg

        await pgvector.asyncpg.register_vector(connection)
        logger.debug("Registered pgvector support on asyncpg connection")
    except (ValueError, TypeError) as exc:
        # Vector extension not installed in database
        if _is_missing_vector_error(exc):
            logger.debug("Skipping pgvector registration because extension is unavailable")
            return
        logger.exception("Failed to register pgvector support")
    except Exception:
        logger.exception("Failed to register pgvector support")
```

#### Full Example (Oracle NumPy vectors)

```python
"""Oracle NumPy vector type handlers for VECTOR data type support."""

import array
import logging
from typing import TYPE_CHECKING, Any

from sqlspec.typing import NUMPY_INSTALLED

if TYPE_CHECKING:
    from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor

__all__ = ("register_numpy_handlers",)

logger = logging.getLogger(__name__)

DTYPE_TO_ARRAY_CODE: dict[str, str] = {
    "float64": "d",
    "float32": "f",
    "uint8": "B",
    "int8": "b"
}


def numpy_converter_in(value: Any) -> "array.array[Any]":
    """Convert NumPy array to Oracle array for VECTOR insertion.

    Args:
        value: NumPy ndarray to convert.

    Returns:
        Python array.array compatible with Oracle VECTOR type.

    Raises:
        ImportError: If NumPy is not installed.
        TypeError: If NumPy dtype is not supported for Oracle VECTOR.
    """
    if not NUMPY_INSTALLED:
        msg = "NumPy is not installed - cannot convert vectors"
        raise ImportError(msg)

    dtype_name = value.dtype.name
    array_code = DTYPE_TO_ARRAY_CODE.get(dtype_name)

    if not array_code:
        supported = ", ".join(DTYPE_TO_ARRAY_CODE.keys())
        msg = f"Unsupported NumPy dtype for Oracle VECTOR: {dtype_name}. Supported: {supported}"
        raise TypeError(msg)

    return array.array(array_code, value)


def numpy_converter_out(value: "array.array[Any]") -> Any:
    """Convert Oracle array to NumPy array for VECTOR retrieval.

    Args:
        value: Oracle array.array from VECTOR column.

    Returns:
        NumPy ndarray with appropriate dtype, or original value if NumPy not installed.
    """
    if not NUMPY_INSTALLED:
        return value

    import numpy as np

    return np.array(value, copy=True, dtype=value.typecode)


def _input_type_handler(cursor: "Cursor | AsyncCursor", value: Any, arraysize: int) -> Any:
    """Oracle input type handler for NumPy arrays.

    Args:
        cursor: Oracle cursor (sync or async).
        value: Value being inserted.
        arraysize: Array size for the cursor variable.

    Returns:
        Cursor variable with NumPy converter if value is ndarray, None otherwise.
    """
    if not NUMPY_INSTALLED:
        return None

    import numpy as np
    import oracledb

    if isinstance(value, np.ndarray):
        return cursor.var(
            oracledb.DB_TYPE_VECTOR,
            arraysize=arraysize,
            inconverter=numpy_converter_in
        )
    return None


def _output_type_handler(cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
    """Oracle output type handler for VECTOR columns.

    Args:
        cursor: Oracle cursor (sync or async).
        metadata: Column metadata from Oracle.

    Returns:
        Cursor variable with NumPy converter if column is VECTOR, None otherwise.
    """
    if not NUMPY_INSTALLED:
        return None

    import oracledb

    if metadata.type_code is oracledb.DB_TYPE_VECTOR:
        return cursor.var(
            metadata.type_code,
            arraysize=cursor.arraysize,
            outconverter=numpy_converter_out
        )
    return None


def register_numpy_handlers(connection: "Connection | AsyncConnection") -> None:
    """Register NumPy type handlers on Oracle connection.

    Enables automatic conversion between NumPy arrays and Oracle VECTOR types.
    Works for both sync and async connections.

    Args:
        connection: Oracle connection (sync or async).
    """
    if not NUMPY_INSTALLED:
        logger.debug("NumPy not installed - skipping vector type handlers")
        return

    connection.inputtypehandler = _input_type_handler
    connection.outputtypehandler = _output_type_handler
    logger.debug("Registered NumPy vector type handlers on Oracle connection")
```

#### Integration in Config

```python
class OracleSyncConfig(SyncDatabaseConfig):
    def __init__(self, *, driver_features=None, ...):
        # Auto-detect NumPy availability
        processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
        processed_driver_features.setdefault("enable_numpy_vectors", NUMPY_INSTALLED)

        super().__init__(driver_features=processed_driver_features, ...)

    def _create_pool(self) -> "OracleSyncConnectionPool":
        """Create pool with session callback."""
        config = dict(self.connection_config)

        # Register session callback if any handlers enabled
        if self.driver_features.get("enable_numpy_vectors", False):
            config["session_callback"] = self._init_connection

        return oracledb.create_pool(**config)

    def _init_connection(self, connection: "OracleSyncConnection", tag: str) -> None:
        """Initialize connection with type handlers.

        Args:
            connection: Oracle connection to initialize.
            tag: Connection tag (unused).
        """
        if self.driver_features.get("enable_numpy_vectors", False):
            register_numpy_handlers(connection)
```

#### Anti-Pattern Example

```python
# BAD - Hard import fails if package not installed
import pgvector  # ImportError if not installed!

# BAD - No graceful degradation
def register_handlers(connection):
    pgvector.asyncpg.register_vector(connection)
    # Breaks if pgvector not installed or extension not in database

# BAD - Silent failure without logging
def register_handlers(connection):
    try:
        import pgvector
        pgvector.asyncpg.register_vector(connection)
    except Exception:
        pass  # User has no idea why vectors don't work

# GOOD - Graceful degradation with logging
def register_handlers(connection):
    if not PGVECTOR_INSTALLED:
        logger.debug("pgvector not installed - skipping handlers")
        return

    try:
        import pgvector.asyncpg
        await pgvector.asyncpg.register_vector(connection)
        logger.debug("Registered pgvector support")
    except Exception:
        logger.exception("Failed to register pgvector support")
```

### Variations

#### Variation 1: JSON Codecs (AsyncPG)

For universal types that every connection needs:

```python
async def register_json_codecs(
    connection: "AsyncpgConnection",
    encoder: "Callable[[Any], str]",
    decoder: "Callable[[str], Any]",
) -> None:
    """Register JSON type codecs on asyncpg connection.

    Args:
        connection: AsyncPG connection instance.
        encoder: Function to serialize Python objects to JSON strings.
        decoder: Function to deserialize JSON strings to Python objects.
    """
    try:
        await connection.set_type_codec("json", encoder=encoder, decoder=decoder, schema="pg_catalog")
        await connection.set_type_codec("jsonb", encoder=encoder, decoder=decoder, schema="pg_catalog")
        logger.debug("Registered JSON type codecs on asyncpg connection")
    except Exception:
        logger.exception("Failed to register JSON type codecs")
```

#### Variation 2: UUID Binary Conversion (Oracle)

For standard library types with custom encoding:

```python
import uuid

def _uuid_input_converter(value: uuid.UUID) -> bytes:
    """Convert UUID to RAW(16) binary format."""
    return value.bytes

def _uuid_output_converter(value: bytes) -> uuid.UUID:
    """Convert RAW(16) binary to UUID."""
    return uuid.UUID(bytes=value)

def _input_type_handler(cursor, value, arraysize):
    if isinstance(value, uuid.UUID):
        return cursor.var(oracledb.DB_TYPE_RAW, arraysize=arraysize, inconverter=_uuid_input_converter)
    return None

def _output_type_handler(cursor, metadata):
    if metadata.type_code is oracledb.DB_TYPE_RAW and metadata.size == 16:
        return cursor.var(metadata.type_code, arraysize=cursor.arraysize, outconverter=_uuid_output_converter)
    return None
```

### Related Patterns

- **driver_features Pattern** - Control registration with feature flags
- **Configuration Pattern** - Integration point for type handlers
- **Exception Handling Pattern** - Wrap registration errors

### SQLSpec Files

**Core implementation**:
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/_type_handlers.py` - JSON/pgvector
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/oracledb/_numpy_handlers.py` - NumPy vectors
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/oracledb/_uuid_handlers.py` - UUID binary

**Tests**:
- `/home/cody/code/litestar/sqlspec/tests/unit/test_adapters/test_asyncpg/test_type_handlers.py`

### References

- **Documentation**: `docs/guides/adapters/postgres.md#pgvector-support`
- **External**: [pgvector-python docs](https://github.com/pgvector/pgvector-python)

---

## Exception Handling Pattern

### Context

**When to use this pattern**:
- Adapter layer query execution
- Wrapping database-specific exceptions
- Providing consistent error interface
- Optional exception suppression

**When NOT to use this pattern**:
- Application logic errors (ValueError, TypeError)
- Configuration validation (use ImproperConfigurationError)
- Already wrapped SQLSpec exceptions

### Problem

Each database library raises different exception types for similar errors (connection failures, integrity violations, query errors). Applications need a consistent exception interface across all adapters.

**Symptoms**:
- Different exception types per adapter
- Hard to catch database errors generically
- Loss of error context when wrapping
- No way to suppress expected errors

**Root cause**:
Database libraries have their own exception hierarchies that don't interoperate.

### Solution

Use `wrap_exceptions` context manager to translate database exceptions to SQLSpec exception hierarchy. Preserve original exception as `__cause__`.

**Key principles**:
1. Wrap all database operations in context manager
2. Let SQLSpec exceptions pass through unwrapped
3. Preserve original exception with `raise ... from exc`
4. Support optional suppression for expected errors
5. Use specific SQLSpec exceptions when possible

### Code Example

#### Minimal Example

```python
from sqlspec.exceptions import wrap_exceptions

async def execute(self, sql: str) -> None:
    """Execute SQL statement."""
    with wrap_exceptions():
        await self._connection.execute(sql)
```

#### Full Example

```python
from sqlspec.exceptions import (
    wrap_exceptions,
    IntegrityError,
    UniqueViolationError,
    ForeignKeyViolationError,
)

async def execute(self, sql: str, params: dict | None = None) -> list[dict]:
    """Execute SQL and return results.

    Args:
        sql: SQL statement to execute.
        params: Optional query parameters.

    Returns:
        Query results as list of dicts.

    Raises:
        IntegrityError: If constraint violation occurs.
        RepositoryError: For other database errors.
    """
    with wrap_exceptions():
        if params:
            result = await self._connection.fetch(sql, *params.values())
        else:
            result = await self._connection.fetch(sql)
        return [dict(row) for row in result]


# With specific exception mapping
async def insert(self, table: str, data: dict) -> int:
    """Insert row and return ID.

    Args:
        table: Table name.
        data: Column values.

    Returns:
        Inserted row ID.

    Raises:
        UniqueViolationError: If unique constraint violated.
        ForeignKeyViolationError: If foreign key constraint violated.
        IntegrityError: For other integrity errors.
    """
    try:
        with wrap_exceptions():
            # Database-specific insert logic
            result = await self._connection.fetchval(
                f"INSERT INTO {table} (...) VALUES (...) RETURNING id",
                *data.values()
            )
            return result
    except IntegrityError as exc:
        # Map database-specific error codes to SQLSpec exceptions
        original = exc.__cause__
        if hasattr(original, "pgcode"):
            if original.pgcode == "23505":  # unique_violation
                raise UniqueViolationError(str(exc)) from original
            if original.pgcode == "23503":  # foreign_key_violation
                raise ForeignKeyViolationError(str(exc)) from original
        raise
```

#### With Suppression

```python
from sqlspec.exceptions import wrap_exceptions, NotFoundError

async def delete_if_exists(self, id: int) -> bool:
    """Delete row if exists.

    Args:
        id: Row ID.

    Returns:
        True if deleted, False if not found.
    """
    # Suppress NotFoundError if row doesn't exist
    with wrap_exceptions(suppress=NotFoundError):
        await self._connection.execute("DELETE FROM users WHERE id = $1", id)
        return True

    # If suppressed, we reach here
    return False
```

#### Anti-Pattern Example

```python
# BAD - Catching generic Exception
async def execute(self, sql: str):
    try:
        return await self._connection.execute(sql)
    except Exception as e:
        raise RepositoryError(str(e))  # Lost original exception!

# BAD - No wrapping at all
async def execute(self, sql: str):
    return await self._connection.execute(sql)
    # Database-specific exception leaks to application

# BAD - Wrapping SQLSpec exceptions
async def execute(self, sql: str):
    try:
        with wrap_exceptions():
            if not sql:
                raise ImproperConfigurationError("SQL required")
            return await self._connection.execute(sql)
    except ImproperConfigurationError:
        # Don't re-wrap SQLSpec exceptions!
        raise

# GOOD - Clean wrapping
async def execute(self, sql: str):
    if not sql:
        raise ImproperConfigurationError("SQL required")

    with wrap_exceptions():
        return await self._connection.execute(sql)
```

### Variations

#### Variation 1: Conditional Wrapping

For operations that should raise specific exceptions:

```python
async def fetch_one(self, sql: str) -> dict:
    """Fetch exactly one row.

    Raises:
        NotFoundError: If no rows found.
        MultipleResultsFoundError: If multiple rows found.
    """
    with wrap_exceptions(wrap_exceptions=False):  # Let exceptions pass through
        result = await self._connection.fetch(sql)

        if len(result) == 0:
            raise NotFoundError("No rows found")
        if len(result) > 1:
            raise MultipleResultsFoundError(f"Expected 1 row, got {len(result)}")

        return dict(result[0])
```

#### Variation 2: Multiple Exception Types

Suppressing multiple exception types:

```python
with wrap_exceptions(suppress=(NotFoundError, MultipleResultsFoundError)):
    result = await self._connection.fetch(sql)
    return result if result else None
```

### Related Patterns

- **Configuration Pattern** - Validation errors use ImproperConfigurationError
- **Type Handler Pattern** - Handler registration wrapped

### SQLSpec Files

**Core implementation**:
- `/home/cody/code/litestar/sqlspec/sqlspec/exceptions.py` - Exception hierarchy and wrap_exceptions

**Examples in adapters**:
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/driver.py`
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/oracledb/driver.py`

### References

- **Documentation**: `docs/guides/development/error-handling.md`

---

## Connection Lifecycle Pattern

### Context

**When to use this pattern**:
- Managing database connection pools
- Providing connections to drivers
- Ensuring proper cleanup
- Supporting both sync and async

**When NOT to use this pattern**:
- Single-use connections (use pool instead)
- Long-lived connections (connection pools better)
- Testing (use fixtures with explicit cleanup)

### Problem

Database connections are expensive resources requiring proper lifecycle management: creation, acquisition, release, and cleanup. Connections must be returned to pool even when errors occur.

**Symptoms**:
- Connection leaks
- Pool exhaustion
- Connections not released on error
- Resource warnings on shutdown

**Root cause**:
Manual resource management is error-prone, especially with exceptions.

### Solution

Use context managers for automatic resource cleanup with try/finally patterns. Lazy pool creation on first use.

**Key principles**:
1. Lazy pool creation (create on first use, not config init)
2. Context managers for connections (provide_connection)
3. Context managers for sessions (provide_session)
4. Always release in finally block
5. Cleanup pools on config close

**Implementation steps**:
1. Implement `_create_pool()` (private, actual creation)
2. Implement `create_pool()` (public, sets connection_instance)
3. Implement `provide_connection()` context manager
4. Implement `provide_session()` context manager (wraps connection)
5. Implement `_close_pool()` for cleanup
6. Set connection_instance to None after close

### Code Example

#### Minimal Example (Async)

```python
from contextlib import asynccontextmanager

class MinimalAsyncConfig:
    def __init__(self):
        self.connection_instance = None

    async def _create_pool(self):
        """Create the actual pool."""
        return await library.create_pool(**self.connection_config)

    async def create_pool(self):
        """Public pool creation."""
        if self.connection_instance is None:
            self.connection_instance = await self._create_pool()
        return self.connection_instance

    @asynccontextmanager
    async def provide_connection(self):
        """Provide connection with automatic cleanup."""
        if self.connection_instance is None:
            self.connection_instance = await self._create_pool()

        connection = None
        try:
            connection = await self.connection_instance.acquire()
            yield connection
        finally:
            if connection is not None:
                await self.connection_instance.release(connection)
```

#### Full Example (AsyncPG)

```python
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator

if TYPE_CHECKING:
    from asyncpg import Pool
    from sqlspec.adapters.asyncpg._types import AsyncpgConnection


class AsyncpgConfig(AsyncDatabaseConfig):
    def __init__(self, *, connection_config=None, connection_instance=None, ...):
        super().__init__(
            connection_config=dict(connection_config) if connection_config else {},
            connection_instance=connection_instance,
            ...
        )

    async def _create_pool(self) -> "Pool":
        """Create the actual async connection pool.

        Returns:
            AsyncPG connection pool instance.
        """
        config = self._get_connection_config_dict()
        config.setdefault("init", self._init_connection)
        return await asyncpg_create_pool(**config)

    async def create_pool(self) -> "Pool":
        """Create and store pool instance.

        Returns:
            AsyncPG connection pool.
        """
        if self.connection_instance is None:
            self.connection_instance = await self._create_pool()
        return self.connection_instance

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.connection_instance:
            await self.connection_instance.close()
            self.connection_instance = None

    async def close_pool(self) -> None:
        """Public close method."""
        await self._close_pool()

    @asynccontextmanager
    async def provide_connection(
        self, *args: Any, **kwargs: Any
    ) -> "AsyncGenerator[AsyncpgConnection, None]":
        """Provide an async connection context manager.

        Automatically acquires connection from pool and releases on exit.
        Creates pool if it doesn't exist.

        Args:
            *args: Positional arguments (unused).
            **kwargs: Keyword arguments (unused).

        Yields:
            AsyncPG connection instance.
        """
        if self.connection_instance is None:
            self.connection_instance = await self._create_pool()

        connection = None
        try:
            connection = await self.connection_instance.acquire()
            yield connection
        finally:
            if connection is not None:
                await self.connection_instance.release(connection)

    @asynccontextmanager
    async def provide_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any
    ) -> "AsyncGenerator[AsyncpgDriver, None]":
        """Provide an async driver session context manager.

        Creates driver with connection, provides statement config override.

        Args:
            *args: Positional arguments (unused).
            statement_config: Optional statement config override.
            **kwargs: Keyword arguments (unused).

        Yields:
            AsyncpgDriver instance with active connection.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            final_statement_config = statement_config or self.statement_config
            driver = self.driver_type(
                connection=connection,
                statement_config=final_statement_config,
                driver_features=self.driver_features,
            )
            yield self._prepare_driver(driver)
```

#### Full Example (Oracle Sync)

```python
import contextlib
from typing import Generator

class OracleSyncConfig(SyncDatabaseConfig):
    def _create_pool(self) -> "OracleSyncConnectionPool":
        """Create sync connection pool."""
        config = dict(self.connection_config)

        # Add session callback if handlers enabled
        if self.driver_features.get("enable_numpy_vectors", False):
            config["session_callback"] = self._init_connection

        return oracledb.create_pool(**config)

    def create_pool(self) -> "OracleSyncConnectionPool":
        """Public pool creation."""
        if self.connection_instance is None:
            self.connection_instance = self._create_pool()
        return self.connection_instance

    def _close_pool(self) -> None:
        """Close sync pool."""
        if self.connection_instance:
            self.connection_instance.close()
            self.connection_instance = None

    @contextlib.contextmanager
    def provide_connection(self) -> "Generator[OracleSyncConnection, None, None]":
        """Provide sync connection context manager.

        Yields:
            Oracle Connection instance.
        """
        if self.connection_instance is None:
            self.connection_instance = self._create_pool()

        conn = None
        try:
            conn = self.connection_instance.acquire()
            yield conn
        finally:
            if conn is not None:
                self.connection_instance.release(conn)

    @contextlib.contextmanager
    def provide_session(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any
    ) -> "Generator[OracleSyncDriver, None, None]":
        """Provide sync driver session.

        Yields:
            OracleSyncDriver with active connection.
        """
        with self.provide_connection() as conn:
            driver = self.driver_type(
                connection=conn,
                statement_config=statement_config or self.statement_config,
                driver_features=self.driver_features,
            )
            yield self._prepare_driver(driver)
```

#### Anti-Pattern Example

```python
# BAD - No finally block
async def provide_connection(self):
    if not self.connection_instance:
        self.connection_instance = await self._create_pool()
    connection = await self.connection_instance.acquire()
    yield connection
    await self.connection_instance.release(connection)  # Skipped if exception!

# BAD - Creating pool in __init__
class BadConfig:
    def __init__(self, connection_config):
        # Pool created even if never used!
        self.connection_instance = asyncio.run(create_pool(**connection_config))

# BAD - Manual connection management
connection = await config.connection_instance.acquire()
try:
    result = await connection.fetch(sql)
finally:
    await config.connection_instance.release(connection)
# Verbose and error-prone

# GOOD - Context manager
async with config.provide_connection() as connection:
    result = await connection.fetch(sql)
# Automatic cleanup
```

### Variations

#### Variation 1: Provide Pool

For frameworks that need direct pool access (Litestar):

```python
async def provide_pool(self) -> "Pool":
    """Provide pool instance for framework injection.

    Returns:
        Connection pool instance.
    """
    if not self.connection_instance:
        self.connection_instance = await self.create_pool()
    return self.connection_instance
```

#### Variation 2: External Connector Cleanup

For Google Cloud connectors requiring cleanup:

```python
async def _close_pool(self) -> None:
    """Close pool and cleanup connectors."""
    if self.connection_instance:
        await self.connection_instance.close()
        self.connection_instance = None

    # Cleanup Cloud SQL connector
    if self._cloud_sql_connector is not None:
        await self._cloud_sql_connector.close_async()
        self._cloud_sql_connector = None

    # Cleanup AlloyDB connector
    if self._alloydb_connector is not None:
        await self._alloydb_connector.close()
        self._alloydb_connector = None
```

### Related Patterns

- **Configuration Pattern** - Pool creation integration
- **Type Handler Pattern** - Connection initialization
- **Framework Extension Pattern** - provide_pool for DI

### SQLSpec Files

**Core implementation**:
- `/home/cody/code/litestar/sqlspec/sqlspec/config.py` - Base provide_connection/provide_session
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/config.py` - AsyncPG pools
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/oracledb/config.py` - Oracle pools

**Tests**:
- `/home/cody/code/litestar/sqlspec/tests/unit/test_adapters/test_asyncpg/test_config.py`

### References

- **Documentation**: `docs/guides/adapters/connection-pools.md`

---

## driver_features Pattern

### Context

**When to use this pattern**:
- Adding optional adapter features
- Auto-detecting package availability
- Providing feature toggles
- Supporting callback hooks

**When NOT to use this pattern**:
- Required features (put in config directly)
- Pool configuration (use connection_config)
- Statement-level overrides (use statement_config)

### Problem

Adapters support optional features (JSON codecs, vector types, Cloud SQL) that depend on external packages or database extensions. Configuration needs feature toggles that auto-detect availability and gracefully degrade.

**Symptoms**:
- Config breaks when optional package not installed
- No way to disable features
- Hard to discover available features
- Inconsistent feature flag naming

**Root cause**:
Feature flags mixed with configuration parameters without clear structure.

### Solution

Use `driver_features` TypedDict with `NotRequired` fields. Prefix boolean flags with `enable_`. Auto-detect package availability in config `__init__`.

**Key principles**:
1. Separate TypedDict for driver_features
2. All boolean flags prefixed with `enable_`
3. Auto-detect in __init__ with setdefault
4. Default to True if package installed
5. Allow explicit override (True/False)
6. Document each flag in TypedDict docstring

**Implementation steps**:
1. Create `{Adapter}DriverFeatures` TypedDict
2. Add `enable_*` fields with NotRequired
3. Document each field in class docstring
4. In __init__, convert to dict and apply setdefaults
5. Pass to super().__init__()

### Code Example

#### Minimal Example

```python
from typing import TypedDict
from typing_extensions import NotRequired

from sqlspec.typing import OPTIONAL_PACKAGE_INSTALLED


class MinimalDriverFeatures(TypedDict):
    """Driver feature flags.

    enable_optional_feature: Enable optional feature support.
        Requires optional-package (pip install optional-package).
        Defaults to True when package is installed.
    """

    enable_optional_feature: NotRequired[bool]


class MinimalConfig:
    def __init__(self, *, driver_features=None):
        features_dict = dict(driver_features) if driver_features else {}

        # Auto-detect availability
        features_dict.setdefault("enable_optional_feature", OPTIONAL_PACKAGE_INSTALLED)

        self.driver_features = features_dict
```

#### Full Example (AsyncPG)

```python
from typing import TYPE_CHECKING, Any, Callable, TypedDict
from typing_extensions import NotRequired

from sqlspec.typing import (
    PGVECTOR_INSTALLED,
    CLOUD_SQL_CONNECTOR_INSTALLED,
    ALLOYDB_CONNECTOR_INSTALLED,
)

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg._types import AsyncpgConnection


class AsyncpgDriverFeatures(TypedDict):
    """AsyncPG driver feature flags.

    json_serializer: Custom JSON serializer function for PostgreSQL JSON/JSONB types.
        Defaults to sqlspec.utils.serializers.to_json.
        Use for performance optimization (e.g., orjson) or custom encoding behavior.
        Applied when enable_json_codecs is True.
    json_deserializer: Custom JSON deserializer function for PostgreSQL JSON/JSONB types.
        Defaults to sqlspec.utils.serializers.from_json.
        Use for performance optimization (e.g., orjson) or custom decoding behavior.
        Applied when enable_json_codecs is True.
    enable_json_codecs: Enable automatic JSON/JSONB codec registration on connections.
        Defaults to True for seamless Python dict/list to PostgreSQL JSON/JSONB conversion.
        Set to False to disable automatic codec registration (manual handling required).
    enable_pgvector: Enable pgvector extension support for vector similarity search.
        Requires pgvector-python package (pip install pgvector) and PostgreSQL with pgvector extension.
        Defaults to True when pgvector-python is installed.
        Provides automatic conversion between Python objects and PostgreSQL vector types.
    enable_cloud_sql: Enable Google Cloud SQL connector integration.
        Requires cloud-sql-python-connector package.
        Defaults to False (explicit opt-in required).
        Mutually exclusive with enable_alloydb.
    cloud_sql_instance: Cloud SQL instance connection name.
        Format: "project:region:instance"
        Required when enable_cloud_sql is True.
    cloud_sql_enable_iam_auth: Enable IAM database authentication.
        Defaults to False for passwordless authentication.
    enable_alloydb: Enable Google AlloyDB connector integration.
        Requires cloud-alloydb-python-connector package.
        Defaults to False (explicit opt-in required).
        Mutually exclusive with enable_cloud_sql.
    alloydb_instance_uri: AlloyDB instance URI.
        Format: "projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE"
        Required when enable_alloydb is True.
    """

    json_serializer: NotRequired[Callable[[Any], str]]
    json_deserializer: NotRequired[Callable[[str], Any]]
    enable_json_codecs: NotRequired[bool]
    enable_pgvector: NotRequired[bool]
    enable_cloud_sql: NotRequired[bool]
    cloud_sql_instance: NotRequired[str]
    cloud_sql_enable_iam_auth: NotRequired[bool]
    enable_alloydb: NotRequired[bool]
    alloydb_instance_uri: NotRequired[str]


class AsyncpgConfig(AsyncDatabaseConfig):
    def __init__(
        self,
        *,
        driver_features: "AsyncpgDriverFeatures | dict[str, Any] | None" = None,
        ...
    ):
        """Initialize AsyncPG configuration."""
        features_dict: dict[str, Any] = dict(driver_features) if driver_features else {}

        # Set defaults with auto-detection
        serializer = features_dict.setdefault("json_serializer", to_json)
        deserializer = features_dict.setdefault("json_deserializer", from_json)
        features_dict.setdefault("enable_json_codecs", True)
        features_dict.setdefault("enable_pgvector", PGVECTOR_INSTALLED)
        features_dict.setdefault("enable_cloud_sql", False)  # Explicit opt-in
        features_dict.setdefault("enable_alloydb", False)  # Explicit opt-in

        super().__init__(
            driver_features=features_dict,
            ...
        )

        self._validate_connector_config()
```

#### Full Example (Oracle)

```python
class OracleDriverFeatures(TypedDict):
    """Oracle driver feature flags.

    enable_numpy_vectors: Enable automatic NumPy array ↔ Oracle VECTOR conversion.
        Requires NumPy and Oracle Database 23ai or higher with VECTOR data type support.
        Defaults to True when NumPy is installed.
        Supports float32, float64, int8, and uint8 dtypes.
    enable_lowercase_column_names: Normalize implicit Oracle uppercase column names to lowercase.
        Targets unquoted Oracle identifiers that default to uppercase.
        Defaults to True for compatibility with schema libraries expecting snake_case fields.
    enable_uuid_binary: Enable automatic UUID ↔ RAW(16) binary conversion.
        When True (default), Python UUID objects are automatically converted to/from
        RAW(16) binary format for optimal storage efficiency (16 bytes vs 36 bytes).
        Defaults to True for improved type safety and storage efficiency.
    """

    enable_numpy_vectors: NotRequired[bool]
    enable_lowercase_column_names: NotRequired[bool]
    enable_uuid_binary: NotRequired[bool]


class OracleSyncConfig(SyncDatabaseConfig):
    def __init__(
        self,
        *,
        driver_features: "OracleDriverFeatures | dict[str, Any] | None" = None,
        ...
    ):
        """Initialize Oracle synchronous configuration."""
        processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}

        # Auto-detect with sensible defaults
        processed_driver_features.setdefault("enable_numpy_vectors", NUMPY_INSTALLED)
        processed_driver_features.setdefault("enable_lowercase_column_names", True)
        processed_driver_features.setdefault("enable_uuid_binary", True)

        super().__init__(
            driver_features=processed_driver_features,
            ...
        )
```

#### Usage Example

```python
# Auto-detect (recommended)
config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://..."},
    # driver_features automatically enables pgvector if installed
)

# Explicit disable
config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://..."},
    driver_features={"enable_pgvector": False}  # Disable even if installed
)

# Explicit enable (fails if not installed)
config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://..."},
    driver_features={"enable_pgvector": True}  # Will fail if package missing
)

# Custom serializer
config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://..."},
    driver_features={
        "json_serializer": orjson.dumps,
        "json_deserializer": orjson.loads,
    }
)

# Cloud SQL connector
config = AsyncpgConfig(
    connection_config={"user": "myuser", "database": "mydb"},
    driver_features={
        "enable_cloud_sql": True,
        "cloud_sql_instance": "project:region:instance",
        "cloud_sql_enable_iam_auth": False,
    }
)
```

#### Anti-Pattern Example

```python
# BAD - Feature flags mixed with pool config
class BadConfig:
    def __init__(self, connection_config=None, enable_pgvector=True):
        # Mixed concerns!
        pass

# BAD - No auto-detection
class BadConfig:
    def __init__(self, driver_features=None):
        # User must know if package installed
        self.driver_features = driver_features or {}

# BAD - Inconsistent naming
class BadDriverFeatures(TypedDict):
    pgvector: NotRequired[bool]  # Not prefixed
    cloud_sql_enabled: NotRequired[bool]  # Inconsistent suffix
    enable_numpy: NotRequired[bool]  # Missing context

# GOOD - Consistent, auto-detected
class GoodDriverFeatures(TypedDict):
    enable_pgvector: NotRequired[bool]
    enable_cloud_sql: NotRequired[bool]
    enable_numpy_vectors: NotRequired[bool]

class GoodConfig:
    def __init__(self, driver_features=None):
        features_dict = dict(driver_features) if driver_features else {}
        features_dict.setdefault("enable_pgvector", PGVECTOR_INSTALLED)
        features_dict.setdefault("enable_cloud_sql", False)
        features_dict.setdefault("enable_numpy_vectors", NUMPY_INSTALLED)
        self.driver_features = features_dict
```

### Variations

#### Variation 1: Callback Hooks

For connection lifecycle hooks:

```python
class CallbackDriverFeatures(TypedDict):
    """Driver features with callback support."""

    on_connection_create: NotRequired[Callable[[Connection], None]]
    on_connection_close: NotRequired[Callable[[Connection], None]]
    on_query_execute: NotRequired[Callable[[str], None]]


# In config __init__
features_dict.setdefault("on_connection_create", None)
features_dict.setdefault("on_connection_close", None)

# In connection lifecycle
if hook := self.driver_features.get("on_connection_create"):
    hook(connection)
```

#### Variation 2: Validation

For features requiring configuration validation:

```python
def _validate_connector_config(self) -> None:
    """Validate Google Cloud connector configuration."""
    enable_cloud_sql = self.driver_features.get("enable_cloud_sql", False)
    enable_alloydb = self.driver_features.get("enable_alloydb", False)

    if enable_cloud_sql and enable_alloydb:
        msg = "Cannot enable both Cloud SQL and AlloyDB connectors simultaneously."
        raise ImproperConfigurationError(msg)

    if enable_cloud_sql:
        if not CLOUD_SQL_CONNECTOR_INSTALLED:
            msg = "cloud-sql-python-connector package not installed."
            raise ImproperConfigurationError(msg)

        instance = self.driver_features.get("cloud_sql_instance")
        if not instance:
            msg = "cloud_sql_instance required when enable_cloud_sql is True."
            raise ImproperConfigurationError(msg)
```

### Related Patterns

- **Configuration Pattern** - driver_features integration point
- **Type Handler Pattern** - Controlled by enable_* flags
- **Connection Lifecycle Pattern** - Callback hooks

### SQLSpec Files

**Core implementation**:
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/config.py` - AsyncpgDriverFeatures
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/oracledb/config.py` - OracleDriverFeatures
- `/home/cody/code/litestar/sqlspec/sqlspec/typing.py` - Package detection flags

**Tests**:
- `/home/cody/code/litestar/sqlspec/tests/unit/test_adapters/test_asyncpg/test_driver_features.py`

### References

- **Documentation**: `docs/guides/adapters/driver-features.md`

---

## Parameter Style Pattern

### Context

**When to use this pattern**:
- Creating new adapter for database with different parameter style
- Converting between parameter styles (?, :name, $1, %s)
- Supporting multiple parameter formats
- Static script compilation (BigQuery)

**When NOT to use this pattern**:
- Simple value substitution (use driver directly)
- Pre-formatted SQL (already in correct style)

### Problem

Different database drivers expect parameters in different formats:
- SQLite, DuckDB: `?` (qmark)
- PostgreSQL (asyncpg): `$1, $2` (numeric)
- PostgreSQL (psycopg): `%s` (format) or `:name` (named)
- Oracle: `:name` (named)
- MySQL: `%s` (format)

**Symptoms**:
- Same SQL doesn't work across databases
- Manual parameter conversion in application code
- Parameter mismatch errors at runtime
- Hard to switch database backends

**Root cause**:
No standard parameter style across database drivers (PEP 249 defines 5 styles).

### Solution

Use `DriverParameterProfile` to declare adapter's supported styles. SQLSpec automatically converts parameters during statement processing.

**Key principles**:
1. Each adapter registers DriverParameterProfile
2. Profile declares supported styles and defaults
3. Statement pipeline converts to target style
4. Support both named (:name) and positional (?, $1, %s) styles
5. Handle special cases (list expansion, static compilation)

### Code Example

#### Minimal Example

```python
from sqlspec.core.parameters import DriverParameterProfile, register_driver_profile

# Register adapter's parameter profile
asyncpg_profile = DriverParameterProfile(
    default_style="numeric",  # Preferred style
    supported_styles={"numeric", "named"},  # What adapter accepts
    default_execution_style="numeric",  # What driver expects at runtime
)

register_driver_profile("asyncpg", asyncpg_profile)
```

#### Full Example (AsyncPG)

```python
from sqlspec.core.parameters import DriverParameterProfile, register_driver_profile

# AsyncPG supports $1, $2 (numeric) style
asyncpg_profile = DriverParameterProfile(
    default_style="numeric",
    supported_styles={"numeric", "named"},
    default_execution_style="numeric",
    supported_execution_styles={"numeric"},
    has_native_list_expansion=True,  # Supports ANY($1::int[])
    needs_static_script_compilation=False,
    allow_mixed_parameter_styles=False,
    preserve_parameter_format=False,
    preserve_original_params_for_many=False,
    default_output_transformer=None,
    default_ast_transformer=None,
    custom_type_coercions={},
    json_serializer_strategy="driver",  # Driver handles JSON serialization
)

register_driver_profile("asyncpg", asyncpg_profile)
```

#### Full Example (Oracle)

```python
# Oracle uses :name (named) style
oracledb_profile = DriverParameterProfile(
    default_style="named",
    supported_styles={"named"},
    default_execution_style="named",
    supported_execution_styles={"named"},
    has_native_list_expansion=False,  # Must expand lists manually
    needs_static_script_compilation=False,
    allow_mixed_parameter_styles=False,
    preserve_parameter_format=True,  # Keep :name format
    preserve_original_params_for_many=True,
    custom_type_coercions={},
    json_serializer_strategy="helper",  # Use helper for JSON
)

register_driver_profile("oracledb", oracledb_profile)
```

#### Full Example (BigQuery - Static Compilation)

```python
# BigQuery requires pre-compiled queries with static parameters
bigquery_profile = DriverParameterProfile(
    default_style="named",
    supported_styles={"named"},
    default_execution_style="named",
    supported_execution_styles={},  # No runtime parameters!
    has_native_list_expansion=False,
    needs_static_script_compilation=True,  # Compile params into SQL
    allow_mixed_parameter_styles=False,
    preserve_parameter_format=False,
    preserve_original_params_for_many=False,
    custom_type_coercions={},
    json_serializer_strategy="helper",
)

register_driver_profile("bigquery", bigquery_profile)
```

#### Conversion Examples

```python
# Input SQL with named parameters
sql = "SELECT * FROM users WHERE id = :id AND status = :status"
params = {"id": 123, "status": "active"}

# AsyncPG (numeric style)
# Output: "SELECT * FROM users WHERE id = $1 AND status = $2"
# Params: [123, "active"]

# Oracle (named style)
# Output: "SELECT * FROM users WHERE id = :id AND status = :status"
# Params: {"id": 123, "status": "active"}

# SQLite (qmark style)
# Output: "SELECT * FROM users WHERE id = ? AND status = ?"
# Params: [123, "active"]

# BigQuery (static compilation)
# Output: "SELECT * FROM users WHERE id = 123 AND status = 'active'"
# Params: None
```

#### Anti-Pattern Example

```python
# BAD - Manual parameter conversion in application
def execute_asyncpg(sql, params):
    # User converts manually
    converted_sql = sql.replace(":id", "$1").replace(":status", "$2")
    param_list = [params["id"], params["status"]]
    return connection.execute(converted_sql, *param_list)

# BAD - Different SQL for each database
asyncpg_sql = "SELECT * FROM users WHERE id = $1"
oracle_sql = "SELECT * FROM users WHERE id = :id"
sqlite_sql = "SELECT * FROM users WHERE id = ?"

# GOOD - SQLSpec handles conversion
sql = "SELECT * FROM users WHERE id = :id"
params = {"id": 123}
result = await driver.execute(sql, params)  # Automatic conversion
```

### Variations

#### Variation 1: List Expansion

For databases without native list support:

```python
# Input
sql = "SELECT * FROM users WHERE id IN :ids"
params = {"ids": [1, 2, 3]}

# Without native list expansion (Oracle, SQLite)
# Output: "SELECT * FROM users WHERE id IN (:ids_0, :ids_1, :ids_2)"
# Params: {"ids_0": 1, "ids_1": 2, "ids_2": 3}

# With native list expansion (PostgreSQL)
# Output: "SELECT * FROM users WHERE id = ANY($1)"
# Params: [[1, 2, 3]]
```

#### Variation 2: Custom Type Coercion

For database-specific type handling:

```python
bigquery_profile = DriverParameterProfile(
    custom_type_coercions={
        "datetime": lambda dt: dt.isoformat(),
        "Decimal": lambda d: float(d),
    },
    ...
)
```

### Related Patterns

- **Configuration Pattern** - statement_config integration
- **Custom Expression Pattern** - Custom SQL generation

### SQLSpec Files

**Core implementation**:
- `/home/cody/code/litestar/sqlspec/sqlspec/core/parameters/_registry.py` - Profile registry
- `/home/cody/code/litestar/sqlspec/sqlspec/core/parameters/_types.py` - DriverParameterProfile
- `/home/cody/code/litestar/sqlspec/sqlspec/core/parameters/_processor.py` - Conversion logic

**Adapter profiles**:
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/driver.py`
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/oracledb/driver.py`
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/bigquery/driver.py`

**Tests**:
- `/home/cody/code/litestar/sqlspec/tests/unit/test_core/test_parameters/`

### References

- **Documentation**: `docs/guides/adapters/parameter-styles.md`
- **External**: [PEP 249 Parameter Styles](https://peps.python.org/pep-0249/#paramstyle)

---

## Arrow Integration Pattern

### Context

**When to use this pattern**:
- Bulk data import/export
- High-performance data transfer
- Integration with data science tools (Pandas, Polars)
- Large result sets
- ETL pipelines

**When NOT to use this pattern**:
- Small result sets (< 1000 rows)
- Row-by-row processing
- Databases without Arrow support
- Memory-constrained environments

### Problem

Transferring large datasets between databases and Python applications is slow when using row-by-row processing. Each row requires Python object allocation, type conversion, and memory copying.

**Symptoms**:
- Slow bulk inserts
- High memory usage with large results
- CPU spent on type conversion
- Inefficient data pipeline

**Root cause**:
Row-oriented processing requires repeated type conversion and memory allocation.

### Solution

Use Apache Arrow for zero-copy columnar data transfer. Arrow provides language-agnostic in-memory format with efficient serialization.

**Key principles**:
1. Use `fetch_arrow()` for columnar result retrieval
2. Use `load_from_arrow()` for bulk inserts
3. Convert to/from Pandas/Polars when needed
4. Leverage native database Arrow support when available
5. Set supports_native_arrow_* flags in adapter config

**Implementation steps**:
1. Set adapter ClassVars (supports_native_arrow_export/import)
2. Implement fetch_arrow() using driver's native support or fallback
3. Implement load_from_arrow() using COPY or bulk insert
4. Add conversion helpers (to_pandas, to_polars, from_pandas, from_polars)

### Code Example

#### Minimal Example

```python
import pyarrow as pa

async def fetch_arrow(self, sql: str) -> pa.Table:
    """Fetch results as Arrow table."""
    # Native support
    if hasattr(self._connection, "fetch_arrow"):
        return await self._connection.fetch_arrow(sql)

    # Fallback: fetch rows and convert
    rows = await self._connection.fetch(sql)
    return pa.Table.from_pylist([dict(row) for row in rows])


async def load_from_arrow(self, table_name: str, arrow_table: pa.Table) -> int:
    """Load Arrow table into database."""
    # Convert to format driver accepts
    records = arrow_table.to_pylist()

    # Bulk insert
    columns = ", ".join(arrow_table.column_names)
    placeholders = ", ".join([f"${i+1}" for i in range(len(arrow_table.column_names))])
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    await self._connection.executemany(sql, records)
    return len(records)
```

#### Full Example (DuckDB - Native Support)

```python
import pyarrow as pa

class DuckDBConfig(SyncDatabaseConfig):
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True


class DuckDBDriver(SyncDriver):
    def fetch_arrow(self, sql: str, params: dict | None = None) -> pa.Table:
        """Fetch results as Arrow table using DuckDB's native support.

        Args:
            sql: SQL query.
            params: Optional query parameters.

        Returns:
            PyArrow Table with query results.
        """
        with wrap_exceptions():
            if params:
                result = self._connection.execute(sql, params).arrow()
            else:
                result = self._connection.execute(sql).arrow()
            return result

    def load_from_arrow(self, table_name: str, arrow_table: pa.Table) -> int:
        """Load Arrow table using DuckDB's native Arrow import.

        Args:
            table_name: Target table name.
            arrow_table: PyArrow table to load.

        Returns:
            Number of rows inserted.
        """
        with wrap_exceptions():
            # DuckDB can directly insert from Arrow
            self._connection.execute(
                f"INSERT INTO {table_name} SELECT * FROM arrow_table"
            )
            return len(arrow_table)
```

#### Full Example (PostgreSQL - COPY Protocol)

```python
import pyarrow as pa
from io import BytesIO

class AsyncpgDriver(AsyncDriver):
    async def fetch_arrow(self, sql: str, params: dict | None = None) -> pa.Table:
        """Fetch results as Arrow table.

        PostgreSQL doesn't have native Arrow support, so we fetch rows
        and convert to Arrow format.

        Args:
            sql: SQL query.
            params: Optional query parameters.

        Returns:
            PyArrow Table with query results.
        """
        with wrap_exceptions():
            if params:
                rows = await self._connection.fetch(sql, *params.values())
            else:
                rows = await self._connection.fetch(sql)

            # Convert asyncpg Records to Arrow
            if not rows:
                return pa.Table.from_pylist([])

            data = [dict(row) for row in rows]
            return pa.Table.from_pylist(data)

    async def load_from_arrow(self, table_name: str, arrow_table: pa.Table) -> int:
        """Load Arrow table using PostgreSQL COPY protocol.

        Args:
            table_name: Target table name.
            arrow_table: PyArrow table to load.

        Returns:
            Number of rows inserted.
        """
        with wrap_exceptions():
            # Convert Arrow to CSV in memory
            import pyarrow.csv as csv

            buffer = BytesIO()
            csv.write_csv(arrow_table, buffer)
            buffer.seek(0)

            # Use COPY for fast bulk insert
            columns = ", ".join(arrow_table.column_names)
            await self._connection.copy_to_table(
                table_name,
                source=buffer,
                columns=list(arrow_table.column_names),
                format="csv",
                header=True,
            )

            return len(arrow_table)
```

#### Pandas/Polars Integration

```python
import pandas as pd
import polars as pl

# Fetch as Pandas DataFrame
async def fetch_pandas(self, sql: str) -> pd.DataFrame:
    """Fetch results as Pandas DataFrame."""
    arrow_table = await self.fetch_arrow(sql)
    return arrow_table.to_pandas()

# Fetch as Polars DataFrame
async def fetch_polars(self, sql: str) -> pl.DataFrame:
    """Fetch results as Polars DataFrame."""
    arrow_table = await self.fetch_arrow(sql)
    return pl.from_arrow(arrow_table)

# Load from Pandas
async def load_from_pandas(self, table_name: str, df: pd.DataFrame) -> int:
    """Load Pandas DataFrame into database."""
    arrow_table = pa.Table.from_pandas(df)
    return await self.load_from_arrow(table_name, arrow_table)

# Load from Polars
async def load_from_polars(self, table_name: str, df: pl.DataFrame) -> int:
    """Load Polars DataFrame into database."""
    arrow_table = df.to_arrow()
    return await self.load_from_arrow(table_name, arrow_table)
```

#### Usage Example

```python
# Fetch large result set as Arrow
arrow_table = await driver.fetch_arrow("SELECT * FROM large_table")

# Convert to Pandas for analysis
df = arrow_table.to_pandas()
df_filtered = df[df["status"] == "active"]

# Convert back to Arrow
filtered_arrow = pa.Table.from_pandas(df_filtered)

# Load into another table
await driver.load_from_arrow("filtered_users", filtered_arrow)

# Direct Pandas integration
df = await driver.fetch_pandas("SELECT * FROM users")
await driver.load_from_pandas("users_copy", df)
```

#### Anti-Pattern Example

```python
# BAD - Row-by-row insert
rows = await driver.fetch("SELECT * FROM source_table")
for row in rows:
    await driver.execute("INSERT INTO target_table VALUES (:id, :name)", dict(row))
# Thousands of round trips!

# BAD - Convert to dict unnecessarily
arrow_table = await driver.fetch_arrow("SELECT * FROM users")
data = arrow_table.to_pydict()
df = pd.DataFrame(data)
# Extra conversion step

# GOOD - Direct Arrow to Pandas
arrow_table = await driver.fetch_arrow("SELECT * FROM users")
df = arrow_table.to_pandas()

# GOOD - Bulk insert with Arrow
await driver.load_from_arrow("target_table", arrow_table)
```

### Variations

#### Variation 1: Parquet Export/Import

For file-based data exchange:

```python
async def export_to_parquet(self, sql: str, file_path: str) -> None:
    """Export query results to Parquet file."""
    import pyarrow.parquet as pq

    arrow_table = await self.fetch_arrow(sql)
    pq.write_table(arrow_table, file_path)


async def load_from_parquet(self, table_name: str, file_path: str) -> int:
    """Load Parquet file into database."""
    import pyarrow.parquet as pq

    arrow_table = pq.read_table(file_path)
    return await self.load_from_arrow(table_name, arrow_table)
```

#### Variation 2: Streaming Large Results

For memory-efficient processing:

```python
async def fetch_arrow_batches(self, sql: str, batch_size: int = 10000):
    """Fetch results as Arrow batches.

    Yields:
        Arrow RecordBatch objects.
    """
    cursor = await self._connection.cursor(sql)

    while True:
        rows = await cursor.fetchmany(batch_size)
        if not rows:
            break

        batch_data = [dict(row) for row in rows]
        yield pa.RecordBatch.from_pylist(batch_data)
```

### Related Patterns

- **Configuration Pattern** - supports_native_arrow_* flags
- **Performance Patterns** - Zero-copy transfers
- **Storage Backend Pattern** - Parquet file handling

### SQLSpec Files

**Core implementation**:
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/duckdb/driver.py` - Native Arrow support
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/driver.py` - Arrow fallback
- `/home/cody/code/litestar/sqlspec/sqlspec/adapters/bigquery/driver.py` - BigQuery Arrow

**Tests**:
- `/home/cody/code/litestar/sqlspec/tests/integration/test_adapters/test_duckdb/test_arrow.py`

### References

- **Documentation**: `docs/guides/architecture/arrow-integration.md`
- **External**: [Apache Arrow Python](https://arrow.apache.org/docs/python/)

---

## Summary

These cross-adapter patterns provide a consistent foundation for all SQLSpec database adapters:

1. **Configuration Pattern**: Strongly-typed TypedDict-based configuration
2. **Type Handler Pattern**: Graceful degradation for optional types
3. **Exception Handling Pattern**: Consistent error interface with wrap_exceptions
4. **Connection Lifecycle Pattern**: Context managers for automatic cleanup
5. **driver_features Pattern**: Auto-detected feature flags with enable_* prefix
6. **Parameter Style Pattern**: Automatic parameter style conversion
7. **Arrow Integration Pattern**: Zero-copy bulk data transfer

When implementing a new adapter or adding features to existing adapters, follow these patterns to ensure consistency, maintainability, and feature parity across the SQLSpec ecosystem.
