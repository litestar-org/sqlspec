# Creating Database Adapters

This guide explains how to build and contribute a database adapter for SQLSpec. Adapters connect SQLSpec's core statement compilation, parameter conversion, and result-mapping pipeline to a concrete synchronous or asynchronous database driver.

## Sync vs. Async Base Classes

SQLSpec provides parallel base hierarchies in `sqlspec.config` and `sqlspec.driver`:

| Component | Synchronous Base | Asynchronous Base |
| --- | --- | --- |
| **Database Config** | `SyncDatabaseConfig` (or `NoPoolSyncConfig`) | `AsyncDatabaseConfig` (or `NoPoolAsyncConfig`) |
| **Driver Adapter** | `SyncDriverAdapterBase` | `AsyncDriverAdapterBase` |
| **Data Dictionary** | `SyncDataDictionaryBase` | `AsyncDataDictionaryBase` |
| **Exception Handler** | `BaseSyncExceptionHandler` | `BaseAsyncExceptionHandler` |

Both modes expose the same high-level query methods (`execute`, `select`, `select_one`, `select_one_or_none`, `select_value`, `select_to_arrow`, `select_stream`, `execute_many`), with asynchronous methods defined as awaitable coroutines and async context managers.

## Standard Adapter Module Layout

Every adapter lives under `sqlspec/adapters/<name>/` and follows a consistent file structure:

```text
sqlspec/adapters/<name>/
├── __init__.py           # Re-exports public Config, Driver, ConnectionParams, DriverFeatures
├── _typing.py            # Connection, Cursor, Pool, and SessionContext type aliases
├── config.py             # Typed Config class, ConnectionParams, PoolParams, DriverFeatures
├── core.py               # default_statement_config, apply_driver_features, exception mapping
├── driver.py             # Driver adapter and exception handler implementation
├── pool.py               # Connection pool implementation or native pool wrapper
├── data_dictionary.py    # Dialect catalog introspection (tables, columns, indexes, FKs)
├── type_converter.py     # Optional engine-specific type coercion and bind/output handlers
├── litestar/             # Optional Litestar session Store implementation
├── adk/                  # Optional Google ADK session, memory, and artifact stores
└── events/               # Optional Events queue store and adapter EventsConfig
```

### Module Responsibilities

- **`config.py`**: Subclasses `SyncDatabaseConfig` or `AsyncDatabaseConfig`, declares `<Name>ConnectionParams`, `<Name>PoolParams`, and `<Name>DriverFeatures` `TypedDict` schemas (with inline PEP 257 field docstrings for every key), and implements `create_connection`, `provide_connection`, `provide_session`, and pool lifecycle methods.
- **`core.py`**: Defines `default_statement_config` (SQLGlot dialect, parameter style, and parameter coercions), `apply_driver_features(statement_config, driver_features)`, and `create_mapped_exception(error)` translating native driver exceptions into `sqlspec.exceptions.SQLSpecError` subclasses (`UniqueViolationError`, `ForeignKeyViolationError`, `NotNullViolationError`, `CheckViolationError`, `DeadlockError`, `DatabaseConnectionError`, `OperationalError`, `DataError`, `SQLParsingError`).
- **`driver.py`**: Subclasses `SyncDriverAdapterBase` or `AsyncDriverAdapterBase`, implementing `dispatch_execute`, `dispatch_execute_many`, `dispatch_execute_script`, `begin`, `commit`, `rollback`, `with_cursor`, `handle_database_exceptions`, `_connection_in_transaction`, and `data_dictionary`.
- **`data_dictionary.py`**: Subclasses `SyncDataDictionaryBase` or `AsyncDataDictionaryBase`, binding the dialect's SQL query pack from `sqlspec/data_dictionary/dialects/<dialect>/` to introspect tables, columns, indexes, foreign keys, and server versions.

## The `apply_driver_features` Contract

Each adapter's `core.py` normalizes user-supplied `driver_features` and applies custom JSON serializers or type converters to the adapter's `StatementConfig`:

```python
from collections.abc import Callable, Mapping
from typing import Any, TypedDict
from typing_extensions import NotRequired

from sqlspec.core import StatementConfig


class ExampleDriverFeatures(TypedDict):
    """Feature flags controlling Example adapter type handling."""

    enable_uuid_conversion: NotRequired[bool]
    """Convert Python uuid.UUID objects to driver-compatible strings or bytes."""
    json_serializer: NotRequired[Callable[[Any], str]]
    """Callable used to serialize Python mappings and sequences into JSON."""
    json_deserializer: NotRequired[Callable[[str], Any]]
    """Callable used to deserialize JSON columns into Python objects."""


def apply_driver_features(
    statement_config: StatementConfig, driver_features: Mapping[str, Any] | None
) -> tuple[StatementConfig, dict[str, Any]]:
    """Normalize driver features and return an updated StatementConfig."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    features.setdefault("enable_uuid_conversion", True)
    return statement_config, features
```

## Driver Adapter Skeleton

Below is a minimal synchronous driver skeleton demonstrating the required `SyncDriverAdapterBase` methods:

```{ .python title="docs/examples/contributing/new_adapter.py" hl_lines="21-72" }
--8<-- "docs/examples/contributing/new_adapter.py"
```

## Testing and Quality Gates

1. **Unit Tests (`tests/unit/adapters/test_<adapter>/`)**: Verify configuration normalization, `apply_driver_features`, statement compilation, exception mapping, and type conversion without live database containers.
2. **Shared Family Contract Tests (`tests/integration/adapters/<family>/test_shared.py`)**: Run multi-driver behavioral contracts across adapters in the same database family.
3. **Driver Integration Tests (`tests/integration/adapters/<family>/<driver>/`)**: Verify connection pooling, transactions, savepoints, `execute_many`, `select_to_arrow`, and extension stores against `pytest-databases` container fixtures.

Run all repository quality gates before submitting a pull request:

```bash
make lint
make type-check
make test
make docs
```
