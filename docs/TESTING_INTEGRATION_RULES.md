# SQLSpec Integration & Unit Testing Comprehensive Rules

## Overview

This document outlines the complete testing strategy for updating SQLSpec's integration tests and creating missing unit tests for adapter drivers. Based on the current state analysis and the massive refactoring that has occurred, we need to:

1. **Update Integration Tests**: Replace outdated integration tests with comprehensive new tests following our testing guidelines
2. **Create Missing Unit Tests**: Add unit tests for all adapter drivers that are currently missing
3. **Ensure Consistency**: Follow established patterns and maintain consistency across all adapters

## Current State Analysis

### What We Have

- ✅ Comprehensive unit tests for base driver protocols (`tests/unit/test_driver.py`)
- ✅ Some existing integration tests (but outdated and incomplete)
- ✅ **NEW**: Complete SQLite adapter tests (unit + integration) - **MODEL IMPLEMENTATION**
- ✅ **NEW**: Complete asyncpg adapter unit tests - **MODEL IMPLEMENTATION**

### What We Need

- ❌ Unit tests for individual adapter drivers (config + driver classes)
- ❌ Comprehensive integration tests using pytest-databases fixtures
- ❌ Consistent testing patterns across all adapters

## Part I: Unit Testing Strategy

### Unit Test Structure for Each Adapter

Each adapter needs two unit test files following the established patterns:

#### 1. Configuration Unit Tests (`test_config.py`)

**File Location**: `tests/unit/test_adapters/test_{adapter_name}/test_config.py`

**Test Coverage**:

- Configuration creation and validation
- Connection parameter handling
- Context managers (provide_connection, provide_session)
- Error handling and cleanup
- Driver and connection type properties
- Adapter-specific configuration options

**Example Pattern** (based on SQLite implementation):

```python
"""Unit tests for {Adapter} configuration."""

import pytest
from unittest.mock import Mock, patch, AsyncMock  # AsyncMock for async adapters
from sqlspec.adapters.{adapter} import {Adapter}Config, {Adapter}ConnectionConfig, {Adapter}Driver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig

def test_{adapter}_connection_config_creation():
    """Test {adapter} connection config creation with valid parameters."""
    # Test basic and full parameter configurations

def test_{adapter}_config_initialization():
    """Test {adapter} config initialization."""
    # Test with default and custom parameters

@patch('sqlspec.adapters.{adapter}.config.{connection_method}')
def test_{adapter}_config_connection_creation(mock_connect):
    """Test {adapter} config connection creation (mocked)."""
    # Test connection creation with mocked dependencies

# ... additional tests for context managers, error handling, etc.
```

#### 2. Driver Unit Tests (`test_driver.py`)

**File Location**: `tests/unit/test_adapters/test_{adapter_name}/test_driver.py`

**Test Coverage**:

- Driver initialization and properties
- Execute implementation methods
- Result wrapping (SelectResult, ExecuteResult)
- Parameter processing and placeholder styles
- Error handling and instrumentation
- Connection management

**Example Pattern** (based on SQLite implementation):

```python
"""Unit tests for {Adapter} driver."""

import pytest
from unittest.mock import Mock, AsyncMock  # AsyncMock for async adapters
from sqlspec.adapters.{adapter} import {Adapter}Driver, {Adapter}Connection
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.statement.result import SelectResult, ExecuteResult
from sqlspec.config import InstrumentationConfig

@pytest.fixture
def mock_{adapter}_connection():
    """Create a mock {adapter} connection."""
    # Create properly mocked connection with all required methods

@pytest.fixture
def {adapter}_driver(mock_{adapter}_connection):
    """Create a {adapter} driver with mocked connection."""
    # Return configured driver instance

def test_{adapter}_driver_initialization(mock_{adapter}_connection):
    """Test {adapter} driver initialization."""
    # Test driver setup and property validation

def test_{adapter}_driver_execute_impl_select({adapter}_driver, mock_{adapter}_connection):
    """Test {adapter} driver _execute_impl for SELECT statements."""
    # Test SELECT query execution

# ... additional tests for different SQL operations, result wrapping, etc.
```

### Unit Test Requirements

1. **Type Annotations**: All test functions must have proper type annotations
2. **Mocking Strategy**: Use `Mock` for sync adapters, `AsyncMock` for async adapters
3. **Fixtures**: Create reusable fixtures for connections and drivers
4. **Error Testing**: Test both success and error scenarios
5. **Coverage**: Test all public methods and properties

## Part II: Integration Testing Strategy

### Integration Test Structure Using pytest-databases

Integration tests use real database connections via pytest-databases fixtures and follow comprehensive testing patterns.

#### 1. Connection Management Tests (`test_connection.py`)

**File Location**: `tests/integration/test_adapters/test_{adapter_name}/test_connection.py`

**Test Coverage**:

- Basic connection functionality
- Configuration options (timeouts, SSL, etc.)
- Connection pooling (for supported adapters)
- Error handling with real connections

**Example Pattern**:

```python
"""Integration tests for {Adapter} connection management."""

import pytest
from sqlspec.adapters.{adapter} import {Adapter}Config, {Adapter}ConnectionConfig
from sqlspec.statement.result import SelectResult, ExecuteResult

@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_basic_connection({adapter}_database):
    """Test basic {adapter} connection functionality."""
    # Test direct connection and session management

@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_connection_configuration({adapter}_database):
    """Test {adapter} connection with various configuration options."""
    # Test different configuration scenarios
```

#### 2. Comprehensive Driver Integration Tests (`test_driver.py`)

**File Location**: `tests/integration/test_adapters/test_{adapter_name}/test_driver.py`

**Test Coverage**:

- CRUD operations (Create, Read, Update, Delete)
- Parameter binding styles (tuple, dict, named)
- Bulk operations (execute_many)
- Script execution (where supported)
- Result handling and metadata
- Error handling with real databases
- Data type handling
- Complex queries (JOINs, aggregations, subqueries)
- Schema operations (DDL)
- Performance testing with bulk data

**Example Pattern** (based on comprehensive SQLite implementation):

```python
"""Integration tests for {Adapter} driver implementation."""

import pytest
from typing import Any, Literal, Generator  # AsyncGenerator for async
from sqlspec.adapters.{adapter} import {Adapter}Config, {Adapter}ConnectionConfig, {Adapter}Driver
from sqlspec.statement.result import SelectResult, ExecuteResult

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]

@pytest.fixture
def {adapter}_session({adapter}_database) -> Generator[{Adapter}Driver, None, None]:
    """Create a {adapter} session with test table."""
    # Setup test database and tables, yield session

@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_basic_crud({adapter}_session):
    """Test basic CRUD operations."""
    # Comprehensive INSERT, SELECT, UPDATE, DELETE testing

@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_value",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_value"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_parameter_styles({adapter}_session, params: Any, style: ParamStyle):
    """Test different parameter binding styles."""
    # Test various parameter binding approaches

@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_execute_many({adapter}_session):
    """Test execute_many functionality."""
    # Test bulk insert operations

@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_result_methods({adapter}_session):
    """Test SelectResult and ExecuteResult methods."""
    # Test get_first(), get_count(), is_empty(), etc.

@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_error_handling({adapter}_session):
    """Test error handling and exception propagation."""
    # Test invalid SQL and constraint violations

@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_data_types({adapter}_session):
    """Test {adapter} data type handling."""
    # Test various data types specific to the database

@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_complex_queries({adapter}_session):
    """Test complex SQL queries."""
    # Test JOINs, aggregations, subqueries

@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_schema_operations({adapter}_session):
    """Test schema operations (DDL)."""
    # Test CREATE, ALTER, DROP operations

@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_column_names_and_metadata({adapter}_session):
    """Test column names and result metadata."""
    # Test result metadata and column access

@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_with_schema_type({adapter}_session):
    """Test {adapter} driver with schema type conversion."""
    # Test dataclass/schema type conversion

@pytest.mark.xdist_group("{adapter}")
def test_{adapter}_performance_bulk_operations({adapter}_session):
    """Test performance with bulk operations."""
    # Test with larger datasets (100+ records)
```

### Integration Test Requirements

1. **pytest-databases Integration**: Use appropriate database fixtures
2. **xdist_group Markers**: Ensure proper test isolation with `@pytest.mark.xdist_group("{adapter}")`
3. **Real Database Testing**: Tests must work with actual database connections
4. **Comprehensive Coverage**: Test all major database operations
5. **Error Scenarios**: Test both success and failure cases
6. **Performance Testing**: Include bulk operation tests
7. **Data Type Testing**: Test database-specific data types
8. **Cleanup**: Proper test isolation and cleanup

### pytest-databases Configuration

Each adapter needs appropriate pytest-databases configuration:

#### Synchronous Adapters (SQLite, psycopg, etc.)

```python
@pytest.fixture
def sqlite_database():
    """SQLite database fixture using pytest-databases."""
    # Uses in-memory database or temporary file

@pytest.fixture
def postgresql_database():
    """PostgreSQL database fixture using pytest-databases."""
    # Uses pytest-databases postgresql fixture
```

#### Asynchronous Adapters (asyncpg, asyncmy, etc.)

```python
@pytest.fixture
async def asyncpg_database():
    """AsyncPG database fixture using pytest-databases."""
    # Uses pytest-databases async postgresql fixture

@pytest.fixture
async def asyncmy_database():
    """AsyncMy database fixture using pytest-databases."""
    # Uses pytest-databases async mysql fixture
```

## Part III: Adapter-Specific Considerations

### Database-Specific Testing Patterns

#### SQLite

- ✅ **COMPLETED** - Model implementation
- In-memory databases (`:memory:`)
- File-based databases with cleanup
- SQLite-specific features (PRAGMA statements)
- Transaction isolation levels

#### PostgreSQL (asyncpg, psycopg)

- ✅ **asyncpg unit tests COMPLETED**
- Connection pooling
- SSL configuration
- Server settings
- PostgreSQL-specific data types (JSON, arrays, etc.)
- Async/await patterns (asyncpg)

#### MySQL (asyncmy, mysql-connector)

- Connection pooling
- SSL configuration
- MySQL-specific data types
- Async/await patterns (asyncmy)

#### BigQuery

- Service account authentication
- Project/dataset configuration
- BigQuery-specific SQL dialect
- Large result set handling

#### DuckDB

- In-memory and file-based databases
- DuckDB-specific features
- Arrow integration testing

#### Oracle

- Connection string formats
- Oracle-specific data types
- Transaction handling

#### ADBC Adapters

- Arrow result format testing
- ADBC-specific connection patterns
- Multiple ADBC driver testing

## Part IV: Implementation Checklist

### For Each Adapter

#### Unit Tests

- [ ] Create `tests/unit/test_adapters/test_{adapter}/` directory
- [ ] Create `__init__.py` with proper docstring and `__all__`
- [ ] Implement `test_config.py` following SQLite pattern
- [ ] Implement `test_driver.py` following SQLite pattern
- [ ] Ensure all tests have proper type annotations
- [ ] Verify mocking strategy (Mock vs AsyncMock)
- [ ] Test all configuration options
- [ ] Test all driver methods and properties

#### Integration Tests

- [ ] Create `tests/integration/test_adapters/test_{adapter}/` directory
- [ ] Implement `test_connection.py` following SQLite pattern
- [ ] Implement `test_driver.py` following comprehensive SQLite pattern
- [ ] Configure pytest-databases fixtures
- [ ] Add xdist_group markers
- [ ] Test CRUD operations
- [ ] Test parameter binding styles
- [ ] Test bulk operations
- [ ] Test error handling
- [ ] Test data types
- [ ] Test complex queries
- [ ] Test schema operations
- [ ] Test result metadata
- [ ] Test schema type conversion
- [ ] Test performance scenarios

#### Validation

- [ ] Run unit tests: `uv run pytest tests/unit/test_adapters/test_{adapter}/`
- [ ] Run integration tests: `uv run pytest tests/integration/test_adapters/test_{adapter}/`
- [ ] Verify test coverage
- [ ] Check linting and type checking
- [ ] Ensure consistent patterns with SQLite/asyncpg models

## Part V: Key Patterns and Best Practices

### Established Patterns from SQLite/asyncpg Models

1. **Consistent File Structure**:

   ```
   tests/unit/test_adapters/test_{adapter}/
   ├── __init__.py
   ├── test_config.py
   └── test_driver.py

   tests/integration/test_adapters/test_{adapter}/
   ├── test_connection.py
   └── test_driver.py
   ```

2. **Type Safety**: All functions have proper type annotations including fixtures

3. **Comprehensive Mocking**: Proper use of Mock/AsyncMock with spec parameters

4. **Fixture Patterns**: Reusable fixtures for connections, drivers, and database sessions

5. **Parameterized Testing**: Use `@pytest.mark.parametrize` for testing multiple scenarios

6. **Error Testing**: Always test both success and failure scenarios

7. **Real Database Integration**: Integration tests use actual database connections

8. **Performance Testing**: Include bulk operation tests with meaningful data sizes

9. **Metadata Testing**: Verify column names, result types, and data access patterns

10. **Schema Type Testing**: Test conversion to dataclasses/schema types

### Testing Anti-Patterns to Avoid

1. **Don't** test implementation details, focus on public interfaces
2. **Don't** use real databases in unit tests (use mocks)
3. **Don't** skip error scenarios
4. **Don't** forget async/await patterns for async adapters
5. **Don't** ignore database-specific features
6. **Don't** forget proper cleanup in integration tests
7. **Don't** use hardcoded values without clear purpose

## Part VI: Completion Strategy

### Phase 1: Complete Remaining Unit Tests

1. **asyncpg driver unit tests** (config already done)
2. **psycopg** (both config and driver)
3. **aiosqlite** (both config and driver)
4. **asyncmy** (both config and driver)
5. **bigquery** (both config and driver)
6. **duckdb** (both config and driver)
7. **oracledb** (both config and driver)
8. **psqlpy** (both config and driver)
9. **adbc variants** (both config and driver)

### Phase 2: Complete Integration Tests

1. **asyncpg integration tests** (following SQLite comprehensive pattern)
2. **All remaining adapters** (following established patterns)

### Phase 3: Validation and Documentation

1. **Run full test suite** for each adapter
2. **Verify test coverage** meets requirements
3. **Update documentation** as needed
4. **Create adapter-specific testing notes** for unique features

This comprehensive approach ensures consistent, thorough testing across all SQLSpec adapters while leveraging the proven patterns established in the SQLite and asyncpg implementations.
