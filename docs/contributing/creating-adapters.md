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

Module responsibilities:

- **`config.py`**: Subclasses `SyncDatabaseConfig` or `AsyncDatabaseConfig`, declares `<Name>ConnectionParams`, `<Name>PoolParams`, and `<Name>DriverFeatures` `TypedDict` schemas (with inline PEP 257 field docstrings for every key), and implements `create_connection`, `provide_connection`, `provide_session`, and pool lifecycle methods.
- **`core.py`**: Defines `default_statement_config` (SQLGlot dialect, parameter style, and parameter coercions), `apply_driver_features(statement_config, driver_features)`, and `create_mapped_exception(error)` translating native driver exceptions into `sqlspec.exceptions.SQLSpecError` subclasses (`UniqueViolationError`, `ForeignKeyViolationError`, `NotNullViolationError`, `CheckViolationError`, `DeadlockError`, `DatabaseConnectionError`, `OperationalError`, `DataError`, `SQLParsingError`).
- **`driver.py`**: Subclasses `SyncDriverAdapterBase` or `AsyncDriverAdapterBase`, implementing `dispatch_execute`, `dispatch_execute_many`, `dispatch_execute_script`, `begin`, `commit`, `rollback`, `with_cursor`, `handle_database_exceptions`, `_connection_in_transaction`, and `data_dictionary`.
- **`data_dictionary.py`**: Subclasses `SyncDataDictionaryBase` or `AsyncDataDictionaryBase`, binding the dialect's SQL query pack from `sqlspec/data_dictionary/dialects/<dialect>/` to introspect tables, columns, indexes, foreign keys, and server versions.

## CI-Enforced Adapter Invariants

Every SQLSpec database adapter is verified by automated repository gates that enforce compilation boundaries, import isolation, schema documentation, and runtime parity across all 22 adapters:

| Invariant | Enforcing Gate or Test | Requirement |
| --- | --- | --- |
| **MyPyC Compilation Boundary** | `python3 tools/scripts/mypyc_inventory.py --check` | `<adapter>/core.py` is MyPyC-compiled (`200` compiled modules across `22` adapters); `<adapter>/driver.py` and `<adapter>/config.py` remain interpreted. |
| **Self-Contained Core Module** | AST parity suites (`mysql_four_way`, `test_mssql_store_parity.py`) | Duplicate adapter-family helpers directly inside `<adapter>/core.py`; never create cross-adapter shared helper modules (`mysql_common.py`, etc.). |
| **Adapter Import Contract** | `tests/unit/test_import_time.py` (`ADAPTER_DRIVERS`) | Import only the adapter's own primary driver eagerly in `_typing.py`; never import optional secondary packages at module import time. |
| **ClassVar and MRO Store Resolution** | `sqlspec/utils/adapters.py` (`resolve_adapter_name`, `resolve_extension_store_class`) | Resolve adapter names and extension stores via `adapter_name` and `*_store_class` `ClassVar` attributes or MRO, never by slicing `__module__.split(".")[2]`. |
| **Inline Field Docstrings and Feature Key Parity** | `python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts sqlspec` & `test_driver_feature_keys_read_by_adapter_are_declared` | Every `TypedDict` and `msgspec.Struct` field must have an inline PEP 257 triple-quoted docstring, and every key read from `driver_features` must be declared on `<Name>DriverFeatures`. |
| **Type Coercion Capabilities and Parameter Profile** | `sqlspec/core/capabilities.py` & `sqlspec/core/parameters/` | Declare `type_coercion_capabilities: ClassVar[TypeCoercionCapabilities]` and register a `DriverParameterProfile` via `register_driver_profile` / `get_driver_profile`. |

### MyPyC Compilation Boundary

SQLSpec compiles `200` performance-critical modules across its core pipeline and all `22` adapters into native C extensions using MyPyC (`python3 tools/scripts/mypyc_inventory.py --check`):

- **Compiled (`<adapter>/core.py`, `<adapter>/pool.py`, `<adapter>/type_converter.py`)**: Keep hot-path parameter normalization, row collection, statement configuration, and exception mapping in `core.py` with strict static typing and MyPyC-compatible constructs.
- **Interpreted (`<adapter>/driver.py`, `<adapter>/config.py`, `<adapter>/_typing.py`, `<adapter>/data_dictionary.py`, extension stores)**: Leave `driver.py` and `config.py` on the `MYPYC_INTERPRETED_BOUNDARY_MODULES` allowlist so user and framework code can subclass configurations, inspect generic type arguments, and attach dynamic hooks without C ABI metaclass conflicts.

### Self-Contained Adapter Core Module

Each of the 22 adapters is completely self-contained. Adapter families that share wire conventions or SQL dialects (such as `asyncmy`, `aiomysql`, `pymysql`, and `mysqlconnector`, or `mssql_python` and `pymssql`) duplicate family-level helper functions directly inside each adapter's own MyPyC-compiled `<adapter>/core.py`. Never introduce cross-adapter shared helper modules (such as `sqlspec/adapters/mysql_common.py`). AST parity test suites (`mysql_four_way`, `test_mssql_store_parity.py`) verify structural equivalence across sibling adapters.

### Adapter Import Contract

Every driver is optional: users install only the extras for the adapters they use, and any one adapter (or all of them) works without the other drivers installed. Each adapter follows the same import rules:

- **Own driver, imported eagerly.** `import sqlspec.adapters.<name>` imports that adapter's driver once, at the top of `_typing.py`, as aliased re-exports (`from asyncpg import Connection as AsyncpgRawConnection`). A missing driver raises the driver's own `ModuleNotFoundError`. Do not wrap the adapter's own driver in `import_optional`, `import_optional_attr`, `importlib.import_module`, `try`/`except ImportError` fallbacks, `None` placeholders, stub classes, or a module `__getattr__`.
- **No other drivers.** An adapter never imports another adapter's driver or package. The CockroachDB adapters wrapping `asyncpg` and `psycopg` are the only allowed cross-adapter imports.
- **One source of driver names.** `config.py`, `core.py`, `driver.py`, `pool.py`, and the extension stores import driver names from `_typing`, not from the driver package.
- **One `TYPE_CHECKING` block.** `_typing.py` uses a single `if TYPE_CHECKING: ... else: ...` block, only for subscripted generics (`Pool[Record]`), `Protocol` types for compiled driver classes, sqlspec imports that would be circular, and secondary optional types.
- **Secondary dependencies through the shared shim.** When code cannot avoid referencing an optional dependency at import time (a runtime-evaluated annotation, an `isinstance` target, or an availability check), import the type or flag from `sqlspec.typing` (for example `ArrowTable`, `NumpyArray`, `PYARROW_INSTALLED`, `PGVECTOR_INSTALLED`). `sqlspec/_typing.py` resolves these lazily and supplies protocol fallbacks when the package is absent. If the shim lacks a type or flag you need, add it there instead of writing a local fallback. When the module itself is needed (`pyarrow`, `pandas`, `polars`, `numpy`, `pgvector`, `rich`, cloud connectors, storage backends, observability packages), load it inside the function that uses it (`ensure_*` when required, `import_optional` when there is a fallback), never at module import.

Register a new adapter's driver module(s) in `ADAPTER_DRIVERS` in `tests/unit/test_import_time.py`. The import-contract tests fail for unregistered adapters and check that the adapter raises the driver's own error when the driver is missing, imports with every other driver blocked, and loads no secondary optional module at import. If the driver package itself imports an optional module, list it in `DRIVER_OWNED_OPTIONAL_IMPORTS` with the reason. For development, add the driver to the `extras` dependency group in `pyproject.toml`; dependency groups are not published to PyPI.

### Inline Field Docstrings and Feature Key Parity

All public `TypedDict` classes (`<Name>ConnectionParams`, `<Name>PoolParams`, `<Name>DriverFeatures`) and `msgspec.Struct` classes must include PEP 257 inline triple-quoted docstrings immediately below every field declaration. In addition, every key accessed from `driver_features` inside `core.py`, `config.py`, or `driver.py` must be declared on `<Name>DriverFeatures` (verified by `test_driver_feature_keys_read_by_adapter_are_declared` and `python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts sqlspec`).

### Type Coercion Capabilities and Driver Parameter Profile Registration

Each adapter declares its wire-level binding behavior via `TypeCoercionCapabilities` (`sqlspec/core/capabilities.py`) and registers a `DriverParameterProfile` (`sqlspec/core/parameters/`) specifying supported `ParameterStyle` formats and custom Python-to-driver type coercions:

```python
from typing import ClassVar

from sqlspec.core import (
    DriverParameterProfile,
    ParameterStyle,
    TypeCoercionCapabilities,
    get_driver_profile,
    register_driver_profile,
)

EXAMPLE_PROFILE = DriverParameterProfile(
    name="Example",
    default_style=ParameterStyle.QMARK,
    supported_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
    default_execution_style=ParameterStyle.QMARK,
    supported_execution_styles={ParameterStyle.QMARK},
    has_native_list_expansion=False,
    preserve_parameter_format=True,
    needs_static_script_compilation=False,
    allow_mixed_parameter_styles=False,
    preserve_original_params_for_many=False,
    json_serializer_strategy="helper",
    default_dialect="sqlite",
)
register_driver_profile("example", EXAMPLE_PROFILE, allow_override=True)
resolved_profile = get_driver_profile("example")


class ExampleCapabilitiesMixin:
    """Declare wire-level parameter binding capabilities."""

    type_coercion_capabilities: ClassVar[TypeCoercionCapabilities] = (
        TypeCoercionCapabilities(
            datetime_binding="iso_text",
            timestamp_precision="microsecond",
            json_columns_decoded=False,
            uuid_binding="text",
        )
    )
```

## Driver Features Normalization Contract

Each adapter's `core.py` normalizes user-supplied `driver_features` and applies custom JSON serializers or type converters to the adapter's `StatementConfig`:

```python
from collections.abc import Callable, Mapping
from typing import Any, NotRequired, TypedDict

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

`resolve_adapter_name(config_or_store)` determines the adapter identifier in this order:

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

#### Declaring ADK Store Options

ADK configuration validation accepts `ADKConfig` keys, the `*ADKConfig` keys of a built-in adapter, and the `extension_config_options` of `adk_store_class` and `adk_memory_store_class`. Options from both stores are accepted on every ADK configuration path, so declare each key on either store. Any other key raises `ImproperConfigurationError`.

```python
from typing import ClassVar

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk.store import SqliteADKMemoryStore, SqliteADKStore


class WarehouseADKStore(SqliteADKStore):
    extension_config_options: ClassVar[frozenset[str]] = frozenset({"warehouse_partition"})


class WarehouseConfig(SqliteConfig):
    adapter_name: ClassVar[str | None] = "warehouse"
    adk_store_class = WarehouseADKStore
    adk_memory_store_class = SqliteADKMemoryStore


config = WarehouseConfig(
    connection_config={"database": ":memory:"},
    extension_config={"adk": {"session_table": "agent_session", "warehouse_partition": "daily"}},
)
```

An `adapter_name` outside `sqlspec.adapters` has no `*ADKConfig` keys. Its stores declare every key outside `ADKConfig`, including base memory settings such as `vector_dimensions`. To extend a parent store's options, write `Parent.extension_config_options | frozenset({"extra"})`, because a subclass value replaces the inherited one.

An `adapter_name` outside `sqlspec.adapters` with no explicit stores skips option validation because no store declares options. Enabled ADK features raise `SQLSpecError` when the store class cannot be imported.

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
python3 tools/scripts/mypyc_inventory.py --check
uv run --no-sync pytest tests/unit -q
make docs
```
