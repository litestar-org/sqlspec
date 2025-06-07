# Database Adapter Implementation Guide

## Introduction

This guide provides comprehensive instructions for implementing new database adapters in SQLSpec. Every adapter follows a standardized pattern that ensures consistency, type safety, and automatic instrumentation while allowing for database-specific optimizations.

## Adapter Structure

Each database adapter consists of three core files:

```
sqlspec/adapters/<database_name>/
├── __init__.py      # Public exports
├── config.py        # Configuration classes
└── driver.py        # Driver implementation
```

## Configuration Implementation

### Base Configuration Classes

SQLSpec provides four base configuration classes that adapters must inherit from:

```python
from typing import ClassVar
from sqlspec.config import (
    NoPoolSyncConfig,      # Sync without pooling
    NoPoolAsyncConfig,     # Async without pooling
    SyncDatabaseConfig,    # Sync with pooling
    AsyncDatabaseConfig    # Async with pooling
)
```

### Configuration Example

```python
# sqlspec/adapters/mydb/config.py
from typing import ClassVar, Optional, Any
from dataclasses import dataclass, field
from sqlspec.config import SyncDatabaseConfig
from sqlspec.adapters.mydb.driver import MyDBConnection, MyDBDriver

@dataclass
class MyDBConfig(SyncDatabaseConfig[MyDBConnection, MyDBDriver]):
    """Configuration for MyDB database connections."""

    # Database-specific configuration
    host: str = "localhost"
    port: int = 5432
    database: str = "mydb"
    user: Optional[str] = None
    password: Optional[str] = None

    # Parameter style configuration (MANDATORY)
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark", "named_colon")
    preferred_parameter_style: ClassVar[str] = "qmark"

    # Connection pool configuration (inherited)
    # pool_size: int = 10
    # max_overflow: int = 20
    # pool_timeout: float = 30.0

    def get_connection_kwargs(self) -> dict[str, Any]:
        """Convert config to connection parameters."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }
```

### Parameter Style Declaration

Every adapter MUST declare its supported parameter styles:

```python
# Common parameter style patterns
supported_parameter_styles: ClassVar[tuple[str, ...]] = (
    "qmark",              # ? (SQLite, DuckDB)
    "numeric",            # $1, $2 (PostgreSQL)
    "named_colon",        # :name (Oracle, SQLite)
    "named_at",           # @name (BigQuery)
    "pyformat_positional", # %s (MySQL, Psycopg)
    "pyformat_named",     # %(name)s (Psycopg)
)
```

## Driver Implementation

### Protocol Requirements

Every driver must implement the driver protocol with these mandatory methods:

```python
from typing import Optional, Any, Sequence
from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.driver.mixins import (
    SQLTranslatorMixin,
    SyncStorageMixin,    # Unified storage operations
    ToSchemaMixin,       # Schema conversion
)
from sqlspec.statement.sql import SQL
from sqlspec.statement.parameters import SQLParameterType, ParameterStyle

class MyDBDriver(
    SyncDriverAdapterProtocol[MyDBConnection, RowT],
    SQLTranslatorMixin,      # SQL dialect translation
    SyncStorageMixin,        # All storage operations (Arrow, Export, Copy)
    ToSchemaMixin,           # Pydantic/msgspec conversion
):
    """MyDB driver implementation."""

    dialect: str = "mydb"
    __supports_arrow__: ClassVar[bool] = True  # Enable Arrow support
```

### Mandatory Execution Methods

The four-method execution structure is MANDATORY and must be implemented in this exact order:

```python
def _execute_statement(
    self,
    statement: "SQL",
    connection: "Optional[MyDBConnection]" = None,
    **kwargs: "Any"
) -> "Any":
    """Main dispatch method - relies on SQL object state."""
    # Use statement properties to determine execution mode
    if statement.is_script:
        return self._execute_script(
            statement.to_sql(placeholder_style=ParameterStyle.STATIC),
            connection or self.connection,
            **kwargs
        )
    elif statement.is_many:
        return self._execute_many(
            statement.to_sql(placeholder_style=self.config.preferred_parameter_style),
            statement.get_parameters(style=self.config.preferred_parameter_style),
            connection or self.connection,
            **kwargs
        )
    else:
        # Single statement execution
        sql = statement.to_sql(placeholder_style=self.config.preferred_parameter_style)
        params = statement.get_parameters(style=self.config.preferred_parameter_style)

        return self._execute(
            sql,
            params,
            connection or self.connection,
            **kwargs
        )

def _execute(
    self,
    sql: "str",
    parameters: "SQLParameterType",
    connection: "MyDBConnection",
    **kwargs: "Any"
) -> "Any":
    """Execute single statement with parameters."""
    cursor = connection.cursor()
    try:
        # Parameters are already in the correct format from SQL object
        cursor.execute(sql, parameters)

        # Handle different result types
        if self._is_select_query(sql):
            return cursor.fetchall()
        else:
            return cursor.rowcount
    finally:
        cursor.close()

def _execute_many(
    self,
    sql: "str",
    parameters: "Sequence[SQLParameterType]",
    connection: "MyDBConnection",
    **kwargs: "Any"
) -> "Any":
    """Execute statement multiple times with different parameters."""
    cursor = connection.cursor()
    try:
        # Parameters are already in the correct format
        cursor.executemany(sql, parameters)
        return cursor.rowcount
    finally:
        cursor.close()

def _execute_script(
    self,
    sql: "str",
    connection: "MyDBConnection",
    **kwargs: "Any"
) -> "Any":
    """Execute multi-statement script."""
    cursor = connection.cursor()
    try:
        # Scripts use STATIC style (no parameters)
        cursor.executescript(sql)
        return -1  # Scripts typically don't return row counts
    finally:
        cursor.close()
```

### Parameter Style Configuration

The SQL object handles parameter conversion based on the adapter's configuration:

```python
# Example for PostgreSQL adapter that uses numeric style ($1, $2)
class AsyncpgDriver(AsyncDriverAdapterProtocol):
    async def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[AsyncpgConnection] = None,
        **kwargs: Any,
    ) -> Any:
        if statement.is_script:
            # Scripts use STATIC style (no placeholders)
            return await self._execute_script(
                statement.to_sql(placeholder_style=ParameterStyle.STATIC),
                connection=connection,
                **kwargs,
            )
        if statement.is_many:
            # Use adapter's preferred style
            return await self._execute_many(
                statement.to_sql(placeholder_style=self.config.preferred_parameter_style),
                statement.get_parameters(style=self.config.preferred_parameter_style),
                connection=connection,
                **kwargs,
            )

        # Single execution with adapter's preferred style
        sql = statement.to_sql(placeholder_style=self.config.preferred_parameter_style)
        params = statement.get_parameters(style=self.config.preferred_parameter_style)

        return await self._execute(
            sql,
            params,
            connection=connection,
            **kwargs,
        )
```

### Result Wrapping

Different statement types return different result types:

```python
def _wrap_select_result(self, result: Any, **kwargs: Any) -> SelectResult[RowT]:
    """Wrap SELECT query results."""
    from sqlspec.statement.result import SelectResult

    # Convert database-specific result to list of rows
    if hasattr(result, 'fetchall'):
        rows = result.fetchall()
    else:
        rows = list(result)

    return SelectResult(
        rows=rows,
        row_count=len(rows),
        schema_type=kwargs.get('schema_type'),
        deserializer=kwargs.get('deserializer', self._deserialize_row)
    )

def _wrap_execute_result(self, result: Any, **kwargs: Any) -> ExecuteResult:
    """Wrap DML/DDL results."""
    from sqlspec.statement.result import ExecuteResult

    # Extract row count
    if isinstance(result, int):
        row_count = result
    elif hasattr(result, 'rowcount'):
        row_count = result.rowcount
    else:
        row_count = -1

    return ExecuteResult(row_count=row_count)
```

### Arrow Support Implementation

For Arrow support, implement the `_fetch_arrow_table` method:

```python
def _fetch_arrow_table(
    self,
    sql: "str",
    parameters: "SQLParameterType",
    connection: "MyDBConnection",
    batch_size: "Optional[int]" = None,
    **kwargs: "Any"
) -> "ArrowResult":
    """Execute query and return Arrow table."""
    import pyarrow as pa

    # Execute query
    cursor = connection.cursor()
    cursor.execute(sql, parameters)  # Parameters already in correct format

    # Get column info
    columns = [desc[0] for desc in cursor.description]

    # Fetch data in batches if specified
    if batch_size:
        batches = []
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            batch = pa.record_batch(
                {col: [row[i] for row in rows] for i, col in enumerate(columns)}
            )
            batches.append(batch)
        table = pa.Table.from_batches(batches)
    else:
        # Fetch all at once
        rows = cursor.fetchall()
        table = pa.table(
            {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        )

    cursor.close()
    return ArrowResult(table=table)
```

## Connection Management

### Basic Connection Creation

```python
def create_connection(self, **kwargs: Any) -> MyDBConnection:
    """Create a new database connection."""
    import mydb  # Database-specific import

    conn_kwargs = self.config.get_connection_kwargs()
    conn_kwargs.update(kwargs)

    connection = mydb.connect(**conn_kwargs)

    # Set connection properties
    if hasattr(connection, 'autocommit'):
        connection.autocommit = self.config.autocommit

    return connection
```

### Connection Context Manager

```python
from contextlib import contextmanager

@contextmanager
def provide_connection(self, **kwargs: Any) -> Generator[MyDBConnection, None, None]:
    """Provide a managed connection."""
    connection = None
    try:
        if self.config.use_pool:
            connection = self._pool.get_connection()
        else:
            connection = self.create_connection(**kwargs)

        yield connection

        # Commit if no exception
        if not connection.autocommit:
            connection.commit()

    except Exception:
        # Rollback on error
        if connection and not connection.autocommit:
            connection.rollback()
        raise
    finally:
        # Return to pool or close
        if connection:
            if self.config.use_pool:
                self._pool.return_connection(connection)
            else:
                connection.close()
```

## Testing Your Adapter

### Unit Tests Structure

Create comprehensive unit tests with proper instrumentation:

```python
# tests/unit/test_adapters/test_mydb/test_driver.py
import pytest
from unittest.mock import MagicMock, patch
from sqlspec.adapters.mydb import MyDBDriver, MyDBConfig
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQL

class TestMyDBDriver:
    """Test MyDB driver implementation."""

    @pytest.fixture
    def mock_connection(self):
        """Create mock connection with context manager support."""
        conn = MagicMock()
        # CRITICAL: Use MagicMock for context managers, not Mock
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=None)
        return conn

    @pytest.fixture
    def driver(self, mock_connection):
        """Create driver with mocked connection and instrumentation."""
        config = MyDBConfig()
        instrumentation = InstrumentationConfig(
            log_queries=True,
            log_parameters=False,  # Security: off by default
            enable_correlation_ids=True,
        )
        driver = MyDBDriver(
            connection=mock_connection,
            config=config,
            instrumentation_config=instrumentation,
        )
        return driver

    def test_execute_with_instrumentation(self, driver, mock_connection):
        """Test execution includes proper instrumentation."""
        # Arrange
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        sql = SQL("INSERT INTO users (name) VALUES (?)", ("John",))

        # Act
        with patch('sqlspec.utils.telemetry.instrument_operation') as mock_instrument:
            result = driver.execute(sql)

        # Assert - instrumentation was called
        mock_instrument.assert_called_once()
        call_args = mock_instrument.call_args
        assert call_args[0][1] == "mydb_execute"  # operation name
        assert call_args[0][2] == "database"      # operation type

    def test_storage_mixin_methods(self, driver):
        """Test unified storage mixin provides all methods."""
        # Verify all storage operations are available
        storage_methods = [
            'fetch_arrow_table',
            'export_to_storage',
            'import_from_storage',
            'copy_from',
            'copy_to',
            'to_parquet',
            'to_csv',
            'to_json',
        ]
        for method in storage_methods:
            assert hasattr(driver, method), f"Missing {method} from SyncStorageMixin"
```

### Integration Tests

Create integration tests that use real database connections:

```python
# tests/integration/test_adapters/test_mydb/test_connection.py
import pytest
from sqlspec.adapters.mydb import MyDBConfig
from sqlspec import SQLSpec

@pytest.mark.mydb
class TestMyDBConnection:
    """Integration tests for MyDB connections."""

    @pytest.fixture
    def sqlspec(self, mydb_url):
        """Create SQLSpec with MyDB config."""
        spec = SQLSpec()
        config = MyDBConfig(url=mydb_url)
        spec.register_config(config, name="test")
        return spec

    def test_basic_connection(self, sqlspec):
        """Test basic connection and query."""
        with sqlspec.get_session("test") as session:
            # Create test table
            session.execute("""
                CREATE TABLE IF NOT EXISTS test_users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
            """)

            # Insert data
            result = session.execute(
                "INSERT INTO test_users (name) VALUES (?)",
                ("Test User",)
            )
            assert result.row_count == 1

            # Select data
            result = session.execute("SELECT * FROM test_users")
            rows = result.all()
            assert len(rows) == 1
            assert rows[0]["name"] == "Test User"
```

## Best Practices

### 1. Error Handling

Always use the `wrap_exceptions` context manager:

```python
from sqlspec.exceptions import wrap_exceptions

def _execute(self, sql, parameters, connection, **kwargs):
    with wrap_exceptions(self._error_mapping):
        # Database operations
```

### 2. Parameter Style Consistency

Always use the SQL object's parameter conversion capabilities:

```python
# CORRECT - Let SQL object handle parameter conversion
sql = statement.to_sql(placeholder_style=self.config.preferred_parameter_style)
params = statement.get_parameters(style=self.config.preferred_parameter_style)

# WRONG - Don't manually convert parameters
if isinstance(params, dict):
    params = [params[f"param_{i}"] for i in range(len(params))]
```

### 3. Transaction Management

Never implicitly manage transactions:

```python
# WRONG - Don't do this
def _execute(self, sql, params, connection, **kwargs):
    result = cursor.execute(sql, params)
    connection.commit()  # NO! User controls transactions

# CORRECT
def _execute(self, sql, params, connection, **kwargs):
    result = cursor.execute(sql, params)
    # Let user decide when to commit
```

### 4. Result Type Detection

Use SQL analysis to determine result types:

```python
def _should_return_select_result(self, sql: str) -> bool:
    """Determine if query returns SELECT-like results."""
    sql_upper = sql.strip().upper()
    return (
        sql_upper.startswith("SELECT") or
        sql_upper.startswith("WITH") or
        "RETURNING" in sql_upper
    )
```

## Common Pitfalls and Solutions

### 1. Empty Dictionary Parameters

The SQL object handles this, but be aware:

```python
# SQL object will convert empty dict to None if needed
params = statement.get_parameters(style=self.config.preferred_parameter_style)
# params will be None or appropriate empty value for your DB
```

### 2. Parameter Style Mismatch

Always use consistent parameter styles:

```python
# Ensure SQL and parameters use same style
placeholder_style = self.config.preferred_parameter_style
sql = statement.to_sql(placeholder_style=placeholder_style)
params = statement.get_parameters(style=placeholder_style)
```

### 3. Missing Mixin Methods

Always inherit the unified storage mixin:

```python
class MyDBDriver(
    SyncDriverAdapterProtocol[Connection, RowT],
    SQLTranslatorMixin,
    SyncStorageMixin,     # Provides ALL storage operations!
    ToSchemaMixin,        # Schema conversion
):
    """The SyncStorageMixin provides:
    - fetch_arrow_table() - Arrow table export
    - export_to_storage() - Export to any format/location
    - import_from_storage() - Import from any format/location
    - copy_from() - High-performance bulk import
    - copy_to() - High-performance bulk export
    - to_parquet(), to_csv(), to_json() - Format-specific exports
    """
```

## Async Adapter Considerations

For async adapters, the pattern is similar but uses async/await:

```python
async def _execute_statement(
    self,
    statement: SQL,
    connection: Optional[AsyncConnection] = None,
    **kwargs: Any
) -> Any:
    """Async execution with parameter conversion."""
    if statement.is_script:
        return await self._execute_script(
            statement.to_sql(placeholder_style=ParameterStyle.STATIC),
            connection=connection,
            **kwargs
        )

    # Use configured parameter style
    sql = statement.to_sql(placeholder_style=self.config.preferred_parameter_style)
    params = statement.get_parameters(style=self.config.preferred_parameter_style)

    if statement.is_many:
        return await self._execute_many(sql, params, connection=connection, **kwargs)
    else:
        return await self._execute(sql, params, connection=connection, **kwargs)
```

## Summary

Implementing a SQLSpec adapter requires:

1. **Configuration class** with parameter style declaration
2. **Driver class** implementing the four mandatory execution methods
3. **Use SQL object's parameter conversion** - don't convert manually
4. **Result wrapping** for type safety
5. **Comprehensive tests** including mocked unit tests and integration tests

The SQL object handles all parameter conversion based on your adapter's declared parameter styles. Focus on implementing the execution methods and let SQLSpec handle the parameter complexity.

---

[← Driver Architecture](./05-driver-architecture.md) | [Connection Management →](./07-connection-management.md)
