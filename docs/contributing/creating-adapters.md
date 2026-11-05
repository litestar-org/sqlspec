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
├── __init__.py        # Re-exports public Config, Driver, features
├── _typing.py         # Connection, Cursor, Pool, and SessionContext
├── config.py          # Typed Config, ConnectionParams, DriverFeatures
├── core.py            # Statement config, features, exception mapping
├── driver.py          # Driver adapter and exception handler
├── pool.py            # Connection pool or native pool wrapper
├── data_dictionary.py # Dialect catalog introspection
├── type_converter.py  # Optional engine-specific type coercion
├── litestar/          # Optional Litestar session Store
├── adk/               # Optional Google ADK stores
└── events/            # Optional Events queue store
```

### Module Responsibilities

- **`config.py`**: Subclasses `SyncDatabaseConfig` or `AsyncDatabaseConfig`, declares `<Name>ConnectionParams`, `<Name>PoolParams`, and `<Name>DriverFeatures` `TypedDict` schemas (with inline PEP 257 field docstrings for every key), and implements `create_connection`, `provide_connection`, `provide_session`, and pool lifecycle methods.
- **`core.py`**: Defines `default_statement_config` (SQLGlot dialect, parameter style, and parameter coercions), `apply_driver_features(statement_config, driver_features)`, and `create_mapped_exception(error)` translating native driver exceptions into `sqlspec.exceptions.SQLSpecError` subclasses (`UniqueViolationError`, `ForeignKeyViolationError`, `NotNullViolationError`, `CheckViolationError`, `DeadlockError`, `DatabaseConnectionError`, `OperationalError`, `DataError`, `SQLParsingError`).
- **`driver.py`**: Subclasses `SyncDriverAdapterBase` or `AsyncDriverAdapterBase`, implementing `dispatch_execute`, `dispatch_execute_many`, `dispatch_execute_script`, `begin`, `commit`, `rollback`, `with_cursor`, `handle_database_exceptions`, `_connection_in_transaction`, and `data_dictionary`.
- **`data_dictionary.py`**: Subclasses `SyncDataDictionaryBase` or `AsyncDataDictionaryBase`, binding the dialect's SQL query pack from `sqlspec/data_dictionary/dialects/<dialect>/` to introspect tables, columns, indexes, foreign keys, and server versions.

## Import Contract

Every driver is optional: users install only the extras for the adapters they use, and any one adapter (or all of them) works without the other drivers installed. Each adapter follows the same import rules:

- **Own driver, imported eagerly.** `import sqlspec.adapters.<name>` imports that adapter's driver once, at the top of `_typing.py`, as aliased re-exports (`from asyncpg import Connection as AsyncpgRawConnection`). A missing driver raises the driver's own `ModuleNotFoundError`. Do not wrap the adapter's own driver in `import_optional`, `import_optional_attr`, `importlib.import_module`, `try`/`except ImportError` fallbacks, `None` placeholders, stub classes, or a module `__getattr__`.
- **No other drivers.** An adapter never imports another adapter's driver or package. The CockroachDB adapters wrapping `asyncpg` and `psycopg` are the only allowed cross-adapter imports.
- **One source of driver names.** `config.py`, `core.py`, `driver.py`, `pool.py`, and the extension stores import driver names from `_typing`, not from the driver package.
- **One `TYPE_CHECKING` block.** `_typing.py` uses a single `if TYPE_CHECKING: ... else: ...` block, only for subscripted generics (`Pool[Record]`), `Protocol` types for compiled driver classes, sqlspec imports that would be circular, and secondary optional types.
- **Secondary dependencies through the shared shim.** When code cannot avoid referencing an optional dependency at import time (a runtime-evaluated annotation, an `isinstance` target, or an availability check), import the type or flag from `sqlspec.typing` (for example `ArrowTable`, `NumpyArray`, `PYARROW_INSTALLED`, `PGVECTOR_INSTALLED`). `sqlspec/_typing.py` resolves these lazily and supplies protocol fallbacks when the package is absent. If the shim lacks a type or flag you need, add it there instead of writing a local fallback. When the module itself is needed (`pyarrow`, `pandas`, `polars`, `numpy`, `pgvector`, `rich`, cloud connectors, storage backends, observability packages), load it inside the function that uses it (`ensure_*` when required, `import_optional` when there is a fallback), never at module import.

Register a new adapter's driver module(s) in `ADAPTER_DRIVERS` in `tests/unit/test_import_time.py`. The import-contract tests fail for unregistered adapters and check that the adapter raises the driver's own error when the driver is missing, imports with every other driver blocked, and loads no secondary optional module at import. If the driver package itself imports an optional module, list it in `DRIVER_OWNED_OPTIONAL_IMPORTS` with the reason. For development, add the driver to the `extras` dependency group in `pyproject.toml`; dependency groups are not published to PyPI.

## The apply_driver_features Contract

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

## Extension Integration & Third-Party Adapter Contract

SQLSpec extensions (`events`, `adk`, `litestar`) and their associated migration loaders resolve the underlying database adapter name and extension store classes through `sqlspec.utils.adapters.resolve_adapter_name` and `sqlspec.utils.adapters.resolve_extension_store_class`.

### Adapter Name Resolution

`resolve_adapter_name(config_or_store)` determines the adapter identifier using a two-stage hierarchy:

1. **Explicit `adapter_name` ClassVar**: If the configuration or store class explicitly defines an `adapter_name: ClassVar[str | None]` attribute containing a non-empty string, that value is returned immediately.
2. **Module Hierarchy Fallback (MRO)**: The resolver inspects `target_cls.__mro__`. The first class whose `__module__` begins with `"sqlspec.adapters."` provides the adapter name from the third segment (`cls.__module__.split(".")[2]`).

If an instance wraps an inner configuration (accessible via `_config`), resolution recursively inspects the wrapped object.

### Extension Store Class Resolution

When instantiating stores or running extension migrations, `resolve_extension_store_class` resolves the concrete store implementation:

1. **Built-in Adapters (Auto-discovery)**: For adapters located inside `sqlspec.adapters.<name>`, stores are automatically discovered by inspecting `sqlspec.adapters.<name>.<extension>.store` (or `sqlspec.adapters.<name>.<extension>`). The loader matches the configuration class name with the appropriate store suffix (for example, `SpannerAsyncConfig` maps to `SpannerAsyncEventQueueStore` for `events`, or `SpannerAsyncStore` for `litestar`), falling back to sync or async store variants.
2. **Subclasses of Built-in Configs**: Application-level subclasses of built-in configurations (such as subclassing `SpannerSyncConfig` or `SqliteConfig`) automatically inherit adapter resolution and store discovery from their base class in `sqlspec.adapters` via MRO. Subclasses can optionally override `adapter_name` or any explicit store ClassVar.
3. **Third-Party Adapters & Custom Stores**: Third-party adapter packages or custom configuration classes defined outside `sqlspec.adapters` declare explicit store class attributes on their configuration class:
   - `events_store_class: ClassVar[type[BaseEventQueueStore] | None]`: Custom event queue store subclassing `sqlspec.extensions.events.BaseEventQueueStore`.
   - `adk_store_class: ClassVar[type[BaseSyncADKStore] | type[BaseAsyncADKStore] | None]`: Custom ADK session store subclassing `BaseSyncADKStore` or `BaseAsyncADKStore`.
   - `adk_memory_store_class: ClassVar[type[BaseSyncADKMemoryStore] | type[BaseAsyncADKMemoryStore] | None]`: Custom ADK memory store subclassing `BaseSyncADKMemoryStore` or `BaseAsyncADKMemoryStore`.
   - `litestar_store_class: ClassVar[type[BaseSQLSpecStore] | None]`: Custom Litestar session store subclassing `sqlspec.extensions.litestar.BaseSQLSpecStore`.

Below is an example of a third-party configuration registering custom extension stores:

```python
from typing import ClassVar

from sqlspec.config import AsyncDatabaseConfig
from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.extensions.litestar import BaseSQLSpecStore


class CustomEventQueueStore(BaseEventQueueStore):
    """Custom event queue store implementation."""

    adapter_name: ClassVar[str | None] = "custom_driver"


class CustomLitestarStore(BaseSQLSpecStore):
    """Custom Litestar session store implementation."""

    adapter_name: ClassVar[str | None] = "custom_driver"


class CustomAsyncConfig(AsyncDatabaseConfig):
    """Third-party database configuration."""

    adapter_name: ClassVar[str | None] = "custom_driver"
    events_store_class: ClassVar[type[BaseEventQueueStore] | None] = (
        CustomEventQueueStore
    )
    litestar_store_class: ClassVar[type[BaseSQLSpecStore] | None] = (
        CustomLitestarStore
    )
```

## Driver Adapter Skeleton

Below is a minimal synchronous driver skeleton demonstrating the required `SyncDriverAdapterBase` methods:

```{ .python title="docs/examples/contributing/new_adapter.py" }
--8<-- "docs/examples/contributing/new_adapter.py:example"
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
