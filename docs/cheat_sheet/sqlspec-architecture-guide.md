# SQLSpec Architecture Guide

*A comprehensive guide for understanding and implementing SQLSpec components*

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Flow](#data-flow)
3. [Core Components](#core-components)
4. [Driver Implementation](#driver-implementation)
5. [Parameter Handling](#parameter-handling)
6. [Testing & Development](#testing--development)

---

## Architecture Overview

SQLSpec provides a unified interface for SQL execution across multiple database drivers with a focus on type safety, parameter handling, and consistent behavior.

### Key Design Principles

1. **Single Source of Truth**: The `SQL` object holds all state for a given statement.
2. **Immutability**: All operations on a `SQL` object return new instances.
3. **Type Safety**: Parameters carry type information through the processing pipeline.
4. **Separation of Concerns**: Clear boundaries between statement representation, parameter processing, and driver execution.
5. **Composition over Inheritance**: Use of mixins is minimized in favor of a clear protocol-based driver architecture.

### Component Hierarchy

```mermaid
graph TD
    A[User API: SQL(), sql.select()] --> B{SQL Object};
    B --> C{Driver.execute()};
    C --> D{SQL Processing Pipeline};
    D --> E{Driver-Specific Execution};
    E --> F[SQLResult];
    F --> G[User Code];
```

**Note**: The architecture uses a single-pass pipeline system with `SQLTransformContext` and `compose_pipeline`, delivering significant performance improvements, further enhanced by a multi-tier caching system.

---

## Data Flow

For a detailed breakdown of the data flow, please see the [SQLSpec Data Flow Guide](./sqlspec-data-flow-guide.md).

---

## Core Components

### `SQL` Class (`sqlspec.statement.sql.SQL`)

The central abstraction for SQL statements.

**Key Responsibilities:**

- Parse SQL strings into an AST (via `sqlglot`).
- Manage parameters (positional and named) and filters.
- Compile to a target parameter style for a specific driver.

**Key Methods:**

```python
# Public API
sql.compile(placeholder_style="qmark") # -> (str, params)
sql.where(condition) # -> SQL
sql.limit(n) # -> SQL
sql.as_many(params) # -> SQL
sql.as_script() # -> SQL

# Internal
sql._ensure_processed() # -> None
sql._build_final_state() # -> (expression, params)
```

### `StatementConfig` (`sqlspec.statement.sql.StatementConfig`)

Configuration for SQL processing behavior.

```python
StatementConfig(
    enable_parsing=True,           # Use SQLGlot parsing
    enable_validation=True,        # Run security validators
    enable_transformations=True,   # Apply transformers
    enable_caching=True,          # Cache processed results
    dialect="postgres",           # Target SQL dialect
)
```

### `TypedParameter` (`sqlspec.parameters.types.TypedParameter`)

Carries type information through the pipeline, ensuring that data types are handled correctly by each database driver.

---

## Driver Implementation

### Base Requirements

Every driver must:

1. Inherit from `SyncDriverAdapterBase` or `AsyncDriverAdapterBase`.
2. Implement the required abstract methods.
3. Define its `dialect` and `parameter_config`.

### Required Methods

```python
class MyDriver(SyncDriverAdapterBase):
    dialect: DialectType = "mydialect"
    parameter_config: DriverParameterConfig = DriverParameterConfig(...)

    def with_cursor(self, connection: Any) -> Any:
        # ...

    def _perform_execute(self, cursor: Any, statement: "SQL") -> None:
        # ...

    def _extract_select_data(self, cursor: Any) -> tuple[list[dict[str, Any]], list[str], int]:
        # ...

    def _extract_execute_rowcount(self, cursor: Any) -> int:
        # ...

    # begin(), commit(), rollback() are also required
```

### Implementation Pattern

The `_dispatch_execution` method in the base class handles the overall flow. Your driver's main responsibility is to implement `_perform_execute` to send the compiled SQL and parameters to the database.

```python
def _perform_execute(self, cursor: Any, statement: "SQL") -> None:
    # 1. Compile the SQL to the driver's expected parameter style
    sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

    # 2. Execute the query using the DB-API cursor
    if statement.is_many:
        cursor.executemany(sql, params)
    else:
        cursor.execute(sql, params)
```

---

## Parameter Handling

### `ParameterProcessor` (`sqlspec.parameters.core.ParameterProcessor`)

This class is the heart of `sqlspec`'s parameter handling. It is used internally by `SQL.compile()` to:

- Convert between parameter styles (e.g., `qmark` to `pyformat`).
- Apply driver-specific type coercions.
- Expand lists for `IN` clauses if the driver doesn't support it natively.

### `DriverParameterConfig` (`sqlspec.parameters.config.DriverParameterConfig`)

Each driver defines a `DriverParameterConfig` to declare its parameter handling requirements:

```python
self.parameter_config = DriverParameterConfig(
    supported_parameter_styles=[ParameterStyle.QMARK],
    default_parameter_style=ParameterStyle.QMARK,
    type_coercion_map={
        bool: int,
        datetime.datetime: lambda v: v.isoformat(),
    },
    has_native_list_expansion=False,
)
```

This declarative approach centralizes the parameter processing logic, making drivers simpler and more consistent.

---

## Testing & Development

### Directory Structure

```
sqlspec/
├── .tmp/           # Debug scripts and outputs
├── .todos/         # Requirements and status docs
├── tests/
│   ├── unit/       # Fast, isolated tests
│   └── integration/# Full adapter tests
```

### Testing Commands

```bash
# Run all tests
make test

# Run specific adapter tests
uv run pytest tests/integration/test_adapter_adbc.py -xvs

# Type checking
make type-check

# Linting
make lint
```

### Adding a New Adapter

1. Create adapter module: `sqlspec/adapters/mydb/`
2. Implement the `config.py` and `driver.py` files.
3. Add integration tests for the new adapter.
4. Document any special cases or configurations.
