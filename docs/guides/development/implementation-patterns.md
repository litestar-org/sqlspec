# Implementation Patterns

This guide documents the key implementation patterns used throughout SQLSpec. Reference these patterns when implementing new adapters, features, or framework extensions.

## Protocol Abstract Methods Pattern

When adding methods that need to support both sync and async configurations:

### Step 1: Define Abstract Method in Protocol

```python
from abc import abstractmethod
from typing import Awaitable, ClassVar

class DatabaseConfigProtocol(Protocol):
    is_async: ClassVar[bool]

    @abstractmethod
    def migrate_up(
        self,
        revision: str = "head",
        allow_missing: bool = False,
        auto_sync: bool = True,
        dry_run: bool = False,
    ) -> "Awaitable[None] | None":
        """Apply database migrations up to specified revision."""
        raise NotImplementedError
```

### Step 2: Implement in Sync Base Class

```python
class NoPoolSyncConfig(DatabaseConfigProtocol):
    is_async: ClassVar[bool] = False

    def migrate_up(
        self,
        revision: str = "head",
        allow_missing: bool = False,
        auto_sync: bool = True,
        dry_run: bool = False,
    ) -> None:
        commands = self._ensure_migration_commands()
        commands.upgrade(revision, allow_missing, auto_sync, dry_run)
```

### Step 3: Implement in Async Base Class

```python
class NoPoolAsyncConfig(DatabaseConfigProtocol):
    is_async: ClassVar[bool] = True

    async def migrate_up(
        self,
        revision: str = "head",
        allow_missing: bool = False,
        auto_sync: bool = True,
        dry_run: bool = False,
    ) -> None:
        commands = cast("AsyncMigrationCommands", self._ensure_migration_commands())
        await commands.upgrade(revision, allow_missing, auto_sync, dry_run)
```

**Key Principles:**
- Protocol defines interface with union return type (`Awaitable[T] | T`)
- Sync base classes implement without `async def` or `await`
- Async base classes implement with `async def` and `await`
- All 4 base classes inherit working methods automatically

## driver_features Pattern

### TypedDict Definition (MANDATORY)

Every adapter must define a TypedDict for its `driver_features`:

```python
class AdapterDriverFeatures(TypedDict):
    """Adapter driver feature flags.

    enable_feature_name: Short one-line description.
        Requirements: List prerequisites (packages, database versions).
        Defaults to X when Y is installed/True for stdlib features.
    """
    enable_feature_name: NotRequired[bool]
    json_serializer: NotRequired[Callable[[Any], str]]
```

### Naming Conventions

- **Boolean flags**: MUST use `enable_` prefix (e.g., `enable_numpy_vectors`, `enable_json_codecs`)
- **Function parameters**: Descriptive names without prefix (e.g., `json_serializer`, `session_callback`)
- **Complex config**: Plural nouns for lists (e.g., `extensions`, `secrets`)

### Auto-Detection Pattern

```python
from sqlspec.typing import NUMPY_INSTALLED, PGVECTOR_INSTALLED

class AdapterConfig(AsyncDatabaseConfig):
    def __init__(
        self,
        *,
        driver_features: "AdapterDriverFeatures | dict[str, Any] | None" = None,
        **kwargs: Any,
    ) -> None:
        processed_features = dict(driver_features) if driver_features else {}

        # Auto-detect optional feature if not explicitly configured
        if "enable_feature" not in processed_features:
            processed_features["enable_feature"] = OPTIONAL_PACKAGE_INSTALLED

        super().__init__(driver_features=processed_features, **kwargs)
```

### Default Value Guidelines

**Default to `True` when:**
- Dependency is in stdlib (uuid, json)
- Feature improves Python type handling
- No performance cost when unused
- Feature is backward-compatible

**Default to auto-detected when:**
- Feature requires optional dependency (NumPy, pgvector)

**Default to `False` when:**
- Feature has performance implications
- Feature changes database behavior in non-obvious ways
- Feature is experimental

## Type Handler Pattern

### Module Structure

```python
"""Feature-specific type handlers for database adapter.

Provides automatic conversion for [feature] via connection type handlers.
Requires [optional dependency].
"""

import logging
from typing import TYPE_CHECKING, Any

from sqlspec._typing import OPTIONAL_PACKAGE_INSTALLED

if TYPE_CHECKING:
    from driver import Connection

__all__ = (
    "_input_type_handler",
    "_output_type_handler",
    "converter_in",
    "converter_out",
    "register_handlers",
)

logger = logging.getLogger(__name__)


def converter_in(value: Any) -> Any:
    """Convert Python type to database type."""
    if not OPTIONAL_PACKAGE_INSTALLED:
        msg = "Optional package not installed"
        raise ImportError(msg)
    return converted_value


def converter_out(value: Any) -> Any:
    """Convert database type to Python type."""
    if not OPTIONAL_PACKAGE_INSTALLED:
        return value
    return converted_value


def register_handlers(connection: "Connection") -> None:
    """Register type handlers on database connection."""
    if not OPTIONAL_PACKAGE_INSTALLED:
        logger.debug("Optional package not installed - skipping type handlers")
        return

    connection.inputtypehandler = _input_type_handler
    connection.outputtypehandler = _output_type_handler
    logger.debug("Registered type handlers for [feature]")
```

### Handler Chaining (Multiple Type Handlers)

When multiple type handlers need to coexist:

```python
def register_handlers(connection: "Connection") -> None:
    """Register type handlers with chaining support."""
    existing_input = getattr(connection, "inputtypehandler", None)
    existing_output = getattr(connection, "outputtypehandler", None)

    def combined_input_handler(cursor: "Cursor", value: Any, arraysize: int) -> Any:
        result = _input_type_handler(cursor, value, arraysize)
        if result is not None:
            return result
        if existing_input is not None:
            return existing_input(cursor, value, arraysize)
        return None

    def combined_output_handler(cursor: "Cursor", metadata: Any) -> Any:
        result = _output_type_handler(cursor, metadata)
        if result is not None:
            return result
        if existing_output is not None:
            return existing_output(cursor, metadata)
        return None

    connection.inputtypehandler = combined_input_handler
    connection.outputtypehandler = combined_output_handler
```

### Config Integration

```python
async def _create_pool(self) -> Pool:
    config = dict(self.pool_config)

    if self.driver_features.get("enable_feature", False):
        config["session_callback"] = self._init_connection

    return await create_pool(**config)

async def _init_connection(self, connection: "Connection") -> None:
    if self.driver_features.get("enable_feature", False):
        from ._feature_handlers import register_handlers
        register_handlers(connection)
```

## Framework Extension Pattern

### Middleware-Based (Starlette/FastAPI)

```python
class SQLSpecPlugin:
    def __init__(self, sqlspec: SQLSpec, app: "App | None" = None) -> None:
        self._sqlspec = sqlspec
        self._config_states: "list[_ConfigState]" = []

        for cfg in self._sqlspec.configs.values():
            settings = self._extract_framework_settings(cfg)
            state = self._create_config_state(cfg, settings)
            self._config_states.append(state)

        if app is not None:
            self.init_app(app)

    def init_app(self, app: "App") -> None:
        self._validate_unique_keys()

        # Wrap existing lifespan
        original_lifespan = app.router.lifespan_context

        @asynccontextmanager
        async def combined_lifespan(app: "App") -> "AsyncGenerator[None, None]":
            async with self.lifespan(app):
                async with original_lifespan(app):
                    yield

        app.router.lifespan_context = combined_lifespan

        # Add middleware for each config
        for config_state in self._config_states:
            self._add_middleware(app, config_state)

    @asynccontextmanager
    async def lifespan(self, app: "App") -> "AsyncGenerator[None, None]":
        # Create pools on startup
        for config_state in self._config_states:
            if config_state.config.supports_connection_pooling:
                pool = await config_state.config.create_pool()
                setattr(app.state, config_state.pool_key, pool)
        try:
            yield
        finally:
            # Close pools on shutdown
            for config_state in self._config_states:
                if config_state.config.supports_connection_pooling:
                    close_result = config_state.config.close_pool()
                    if close_result is not None:
                        await close_result
```

### Hook-Based (Flask)

```python
def init_app(self, app: "Flask") -> None:
    app.before_request(self._before_request_handler)
    app.after_request(self._after_request_handler)
    app.teardown_appcontext(self._teardown_appcontext_handler)

def _before_request_handler(self) -> None:
    from flask import current_app, g

    for config_state in self._config_states:
        if config_state.config.supports_connection_pooling:
            pool = current_app.extensions["sqlspec"]["pools"][config_state.session_key]
            conn_ctx = config_state.config.provide_connection(pool)
            setattr(g, config_state.connection_key, connection)

def _after_request_handler(self, response: "Response") -> "Response":
    from flask import g

    for config_state in self._config_states:
        if config_state.commit_mode == "manual":
            continue
        # Commit or rollback based on status code

    return response  # MUST return response unchanged
```

### Configuration via extension_config

```python
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/mydb"},
    extension_config={
        "starlette": {
            "commit_mode": "autocommit",
            "session_key": "db"
        }
    }
)
```

### disable_di Pattern

For users integrating their own DI solution:

```python
class StarletteConfig(TypedDict):
    disable_di: NotRequired[bool]
    """Disable built-in dependency injection. Default: False."""

# In init_app
for config_state in self._config_states:
    if not config_state.disable_di:
        self._add_middleware(app, config_state)
```

## LOB (Large Object) Hydration Pattern

For databases returning handle objects (Oracle CLOBs, etc.):

```python
def _coerce_sync_row_values(row: "tuple[Any, ...]") -> "list[Any]":
    """Coerce LOB handles to concrete values."""
    coerced_values: list[Any] = []
    for value in row:
        if hasattr(value, "read"):  # Duck-typing for LOB detection
            try:
                processed_value = value.read()
            except Exception:
                coerced_values.append(value)
                continue
            if isinstance(processed_value, str):
                processed_value = _type_converter.convert_if_detected(processed_value)
            coerced_values.append(processed_value)
        else:
            coerced_values.append(value)
    return coerced_values
```

## Apache Arrow Integration

### Native Arrow Path (Preferred)

```python
async def select_to_arrow(
    self,
    statement: "Statement | QueryBuilder",
    /,
    *parameters: "StatementParameters",
    return_format: str = "table",
    **kwargs: Any,
) -> "Any":
    ensure_pyarrow()

    sql_statement = self._prepare_statement(statement, parameters)

    async with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
        await cursor.execute(str(sql_statement), sql_statement.parameters or ())

        # Native Arrow fetch - zero-copy!
        arrow_table = await cursor.fetch_arrow_table()

        if return_format == "batch":
            batches = arrow_table.to_batches()
            arrow_data = batches[0] if batches else pa.RecordBatch.from_pydict({})
        else:
            arrow_data = arrow_table

        return create_arrow_result(arrow_data, rows_affected=arrow_table.num_rows)
```

### Result Wrapping

Always use `create_arrow_result()` for consistent result wrapping:

```python
from sqlspec.core import create_arrow_result

result = create_arrow_result(arrow_table, rows_affected=arrow_table.num_rows)
```

## Google Cloud Connector Pattern

### Connection Factory Pattern

```python
def _setup_cloud_sql_connector(self, config: dict[str, Any]) -> None:
    from google.cloud.sql.connector import Connector

    self._cloud_sql_connector = Connector()

    user = config.get("user")
    database = config.get("database")

    async def get_conn() -> AsyncpgConnection:
        return await self._cloud_sql_connector.connect_async(
            instance_connection_string=self.driver_features["cloud_sql_instance"],
            driver="asyncpg",
            enable_iam_auth=self.driver_features.get("cloud_sql_enable_iam_auth", False),
            ip_type=self.driver_features.get("cloud_sql_ip_type", "PRIVATE"),
            user=user,
            db=database,
        )

    # Remove standard connection params, use factory
    for key in ("dsn", "host", "port", "user", "password", "database"):
        config.pop(key, None)

    config["connect"] = get_conn
```

### Cleanup

```python
async def _close_pool(self) -> None:
    if self.pool_instance:
        await self.pool_instance.close()

    if self._cloud_sql_connector is not None:
        await self._cloud_sql_connector.close_async()
        self._cloud_sql_connector = None
```

## Query Stack Implementation

### Immutable Builder

```python
# StatementStack stores operations as tuples
# Every mutating helper returns a NEW instance
stack = StatementStack()
stack = stack.push_select("SELECT * FROM users")
stack = stack.push_execute("UPDATE users SET active = true")
```

### Adapter Override Pattern

```python
async def execute_stack(
    self,
    stack: "StatementStack",
    continue_on_error: bool = False,
) -> "list[StackResult]":
    # Check for native pipeline support
    if not self._has_native_pipeline():
        return await super().execute_stack(stack, continue_on_error)

    # Use native pipeline
    with StackExecutionObserver(self, stack, continue_on_error, native_pipeline=True):
        return await self._execute_native_pipeline(stack, continue_on_error)
```

### Error Handling

```python
# Wrap driver exceptions in StackExecutionError
raise StackExecutionError(
    operation_index=idx,
    sql=describe_stack_statement(operation),
    adapter=self.adapter_name,
    mode="fail-fast" if not continue_on_error else "continue-on-error",
)

# Continue-on-error flows
results.append(StackResult.from_error(error))
```

## Portal Pattern for Sync Frameworks

Enable async adapters in sync WSGI frameworks:

```python
from sqlspec.utils.portal import get_global_portal

# In sync context
portal = get_global_portal()
result = portal.call(some_async_function, arg1, arg2)
```

The `await_()` function automatically uses the portal:

```python
from sqlspec.utils.sync_tools import await_

sync_add = await_(async_add)
result = sync_add(5, 3)  # Returns 8, using portal internally
```
