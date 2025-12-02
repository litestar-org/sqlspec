---
orphan: true
---

# SQLSpec Architecture Guide

*A comprehensive guide for understanding and implementing SQLSpec components*

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Flow](#data-flow)
3. [Core Components](#core-components)
4. [Driver Implementation](#driver-implementation)
5. [Parameter Handling](#parameter-handling)
6. [Testing & Development](#testing--development)
7. [Observability Runtime](#observability-runtime)

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

**Note**: The architecture uses a single-pass pipeline system with `SQLTransformContext` and `compose_pipeline`, delivering significant performance improvements, further enhanced by a comprehensive multi-tier caching system providing 12x+ performance improvements.

---

## Data Flow

For a detailed breakdown of the data flow, please see the [SQLSpec Data Flow Guide](./data-flow.md).

---

## Core Components

### `SQL` Class (`sqlspec.core.statement.SQL`)

The central abstraction for SQL statements.

**Key Responsibilities:**

- Parse SQL strings into an AST (via `sqlglot`).
- Manage parameters (positional and named) and filters.
- Compile to a target parameter style for a specific driver.

**Key Methods:**

```python
# Public API
sql.compile(placeholder_style="qmark") # -> (str, parameters)
sql.where(condition) # -> SQL
sql.limit(n) # -> SQL
sql.as_script() # -> SQL

# Internal
sql._ensure_processed() # -> None
sql._build_final_state() # -> (expression, parameters)
```

### `StatementConfig` (`sqlspec.core.statement.StatementConfig`)

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

### `TypedParameter` (`sqlspec.core.parameters.TypedParameter`)

Carries type information through the pipeline, ensuring that data types are handled correctly by each database driver.

### Enhanced Caching Architecture (NEW)

SQLSpec implements a comprehensive multi-tier caching system. While individual statements can toggle caching via `StatementConfig.enable_caching`, the underlying cache configuration is managed globally.

```python
# Global cache configuration (via SQLSpec.configure_cache)
# - sql_cache_size: Size of the SQL statement cache
# - fragment_cache_size: Size of the AST fragment cache
# - optimized_cache_size: Size of the optimized expression cache

# Cache integration in StatementConfig
StatementConfig(
    enable_caching=True,             # Master caching switch for this statement
    # ... other configuration
)
```

**Performance Benefits:**

- **SQL Cache**: Avoids recompilation of identical queries
- **Optimized Cache**: Reuses AST optimization results
- **Builder Cache**: Accelerates QueryBuilder state serialization
- **File Cache**: 12x+ speedup with checksum validation
- **Analysis Cache**: Caches pipeline step results for reuse

**StatementConfig-Aware Caching:**

- All cache keys include StatementConfig hash to prevent cross-contamination
- Different configurations maintain separate cache entries
- Automatic cache invalidation on configuration changes

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

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        # ...

    def begin(self) -> None:
        # ...

    def commit(self) -> None:
        # ...

    def rollback(self) -> None:
        # ...

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
        # ...

    def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        # ...

    def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        # ...

    def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        # ...
```

### Implementation Pattern (CURRENT)

The `dispatch_statement_execution` method in the base class handles the overall flow using a template method pattern. Modern drivers implement specific execution methods:

```python
# Current template method pattern
def dispatch_statement_execution(self, statement: "SQL", connection: "Any") -> "SQLResult":
    """Central execution dispatcher using the Template Method Pattern."""
    with self.handle_database_exceptions(), self.with_cursor(connection) as cursor:
        special_result = self._try_special_handling(cursor, statement)
        if special_result is not None:
            return special_result

        if statement.is_script:
            execution_result = self._execute_script(cursor, statement)
        elif statement.is_many:
            execution_result = self._execute_many(cursor, statement)
        else:
            execution_result = self._execute_statement(cursor, statement)

        return self.build_statement_result(statement, execution_result)

# Drivers implement these specific methods:
def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
    """Hook for database-specific operations (COPY, bulk ops, etc.)"""

def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
    """Execute single statement"""

def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
    """Execute with parameter batches"""

def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
    """Execute multi-statement script"""
```

---

## Parameter Handling

### Enhanced Parameter Processing (CURRENT)

The current parameter processing system uses `ParameterStyleConfig` integrated with the pipeline architecture:

```python
# Current parameter configuration
from sqlspec.core import ParameterStyle, ParameterStyleConfig

parameter_config = ParameterStyleConfig(
    default_parameter_style=ParameterStyle.QMARK,
    supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
    type_coercion_map={
        bool: int,
        datetime.datetime: lambda v: v.isoformat(),
        Decimal: str,
        dict: to_json,
        list: to_json,
    },
    has_native_list_expansion=False,
    needs_static_script_compilation=True,  # New flag for script handling
)

# Integration with StatementConfig
statement_config = StatementConfig(
    dialect="postgres",
    parameter_config=parameter_config,
    enable_caching=True,  # Cache-aware parameter processing
)
```

**Key Enhancements:**

- **StatementConfig Integration**: Parameter processing respects overall configuration
- **Pipeline Awareness**: Works with SQLTransformContext for consistency
- **Enhanced Caching**: Parameter processing results are cached with StatementConfig keys
- **Script Compilation**: New `needs_static_script_compilation` flag for script handling
- **Type Preservation**: Enhanced TypedParameter support through pipeline

**Current Processing Flow:**

1. SQL object processes parameters through pipeline (parameterize_literals_step)
2. Parameters are compiled with StatementConfig-aware caching
3. Driver receives pre-processed parameters via prepare_driver_parameters()
4. Type coercion and style conversion applied consistently
5. Results cached for identical StatementConfig + SQL combinations

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

### Testing Commands (CURRENT)

```bash
# Run all tests
make test

# Run specific adapter tests (current naming)
uv run pytest tests/integration/test_adapters/test_adbc/test_driver.py -xvs

# Run integration tests for specific database
uv run pytest tests/integration/test_adapters/test_psycopg/ -xvs

# Type checking
make type-check  # Runs both mypy and pyright

# Linting and formatting
make lint        # All linting checks
make fix         # Auto-fix formatting issues

# MyPyC compilation testing
HATCH_BUILD_HOOKS_ENABLE=1 uv sync --all-extras --dev  # Install with compilation
make install     # Standard development installation
```

### Adding a New Adapter

1. Create adapter module: `sqlspec/adapters/mydb/`
2. Implement the `config.py` and `driver.py` files.
3. Add integration tests for the new adapter.
4. Document any special cases or configurations.

## Observability Runtime

The observability subsystem (lifecycle dispatcher, statement observers, span manager, diagnostics) now sits alongside the driver architecture. Refer to the dedicated [Observability Runtime guide](./observability.md) for:

- configuration sources (`ObservabilityConfig`, adapter overrides, and `driver_features` compatibility),
- the full list of lifecycle events emitted by SQLSpec,
- guidance on statement observers, redaction, and OpenTelemetry spans,
- the Phase 4/5 roadmap for spans + diagnostics.
