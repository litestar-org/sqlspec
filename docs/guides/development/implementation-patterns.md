# Implementation Patterns

This guide documents the key implementation patterns used throughout SQLSpec. Reference these patterns when implementing new adapters, features, or framework extensions.

<a id="protocol-abstract-methods-pattern"></a>

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

<a id="driver-features-pattern"></a>

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

<a id="type-handler-pattern"></a>

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
    config = dict(self.connection_config)

    if self.driver_features.get("enable_feature", False):
        config["session_callback"] = self._init_connection

    return await create_pool(**config)

async def _init_connection(self, connection: "Connection") -> None:
    if self.driver_features.get("enable_feature", False):
        from ._feature_handlers import register_handlers
        register_handlers(connection)
```

<a id="framework-extension-pattern"></a>

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
    connection_config={"dsn": "postgresql://localhost/mydb"},
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
    if self.connection_instance:
        await self.connection_instance.close()

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

<a id="explain-builder-pattern"></a>

## EXPLAIN Builder Pattern

The EXPLAIN builder demonstrates dialect-aware SQL generation for a non-standard statement type.

### Dialect Dispatch Pattern

```python
POSTGRES_DIALECTS = frozenset({"postgres", "postgresql", "redshift"})
MYSQL_DIALECTS = frozenset({"mysql", "mariadb"})

def build_explain_sql(
    statement_sql: str,
    options: "ExplainOptions",
    dialect: "DialectType | None" = None,
) -> str:
    """Build dialect-specific EXPLAIN SQL."""
    dialect_name = _normalize_dialect_name(dialect)

    if dialect_name in POSTGRES_DIALECTS:
        return _build_postgres_explain(statement_sql, options)
    if dialect_name in MYSQL_DIALECTS:
        return _build_mysql_explain(statement_sql, options)
    # ... more dialects

    return _build_generic_explain(statement_sql, options)
```

### Options Class with Immutable Copy

```python
@mypyc_attr(allow_interpreted_subclasses=False)
class ExplainOptions:
    """Mypyc-compatible options class with immutable copy pattern."""

    __slots__ = ("analyze", "verbose", "format", ...)

    def __init__(self, analyze: bool = False, ...) -> None:
        self.analyze = analyze
        # ... set all slots

    def copy(self, analyze: "bool | None" = None, ...) -> "ExplainOptions":
        """Create a copy with optional modifications."""
        return ExplainOptions(
            analyze=analyze if analyze is not None else self.analyze,
            # ... copy all fields with overrides
        )
```

### Mixin for QueryBuilder Integration

```python
class ExplainMixin:
    """Add .explain() to QueryBuilder subclasses."""

    __slots__ = ()

    dialect: "DialectType | None"

    def explain(
        self,
        analyze: bool = False,
        verbose: bool = False,
        format: "ExplainFormat | str | None" = None,
    ) -> "Explain":
        """Create an EXPLAIN builder for this query."""
        options = ExplainOptions(
            analyze=analyze,
            verbose=verbose,
            format=ExplainFormat(format.lower()) if isinstance(format, str) else format,
        )
        return Explain(self, dialect=self.dialect, options=options)
```

### Statement Resolution Pattern

```python
def _resolve_statement_sql(
    self, statement: "str | exp.Expression | SQL | SQLBuilderProtocol"
) -> str:
    """Resolve different statement types to SQL string."""
    if isinstance(statement, str):
        return statement

    if isinstance(statement, SQL):
        self._parameters.update(statement.named_parameters)
        return statement.raw_sql

    if is_expression(statement):
        return statement.sql(dialect=self._dialect)

    if has_parameter_builder(statement):
        safe_query = statement.build(dialect=self._dialect)
        if safe_query.parameters:
            self._parameters.update(safe_query.parameters)
        return str(safe_query.sql)

    if has_expression_and_sql(statement):
        return statement.sql

    msg = f"Cannot resolve statement to SQL: {type(statement).__name__}"
    raise SQLBuilderError(msg)
```

**Key principles:**

- Use frozenset for dialect groupings (hashable, immutable)
- Normalize dialect names to lowercase for consistent matching
- Preserve parameters from underlying statements
- Use type guards instead of `isinstance()` for protocol checks

<a id="dynamic-optional-dependency-pattern"></a>

## Dynamic Optional Dependency Detection

SQLSpec uses a runtime detection pattern for optional dependencies that works correctly with mypyc compilation. This pattern prevents constant-folding of availability checks at compile time.

### The Problem

Module-level boolean constants like `PACKAGE_INSTALLED = module_available("package")` get frozen during mypyc compilation. If the optional package is missing during compilation but installed later, compiled code still sees `False` forever.

### The Solution

Use `dependency_flag()` from `sqlspec.utils.dependencies`:

```python
from sqlspec.utils.dependencies import dependency_flag, module_available

# CORRECT - Lazy evaluation via OptionalDependencyFlag
FSSPEC_INSTALLED = dependency_flag("fsspec")
OBSTORE_INSTALLED = dependency_flag("obstore")

# These evaluate at runtime, not compile time
if FSSPEC_INSTALLED:
    # This code path remains available even in compiled modules
    from sqlspec.storage.backends.fsspec import FSSpecBackend
```

### The API

```python
from sqlspec.utils.dependencies import (
    dependency_flag,        # Returns OptionalDependencyFlag (bool-like)
    module_available,       # Returns bool, cached per session
    reset_dependency_cache, # Clear cache for testing
)

# OptionalDependencyFlag is boolean-like
flag = dependency_flag("numpy")
if flag:  # Evaluates module_available("numpy") at runtime
    import numpy as np
```

### Using in ensure_* Functions

```python
from sqlspec.utils.dependencies import module_available
from sqlspec.exceptions import MissingDependencyError

def _require_dependency(
    module_name: str, *, package_name: str | None = None, install_package: str | None = None
) -> None:
    """Raise MissingDependencyError when an optional dependency is absent."""
    if module_available(module_name):
        return

    package = package_name or module_name
    install = install_package or package
    raise MissingDependencyError(package=package, install_package=install)

def ensure_numpy() -> None:
    """Ensure NumPy is available for array operations."""
    _require_dependency("numpy")
```

### Testing Dynamic Detection

Use `reset_dependency_cache()` when tests manipulate `sys.path`:

```python
import sys
from pathlib import Path
from sqlspec.utils import dependencies

def test_dependency_detection_after_install(tmp_path, monkeypatch):
    """Ensure detection reflects runtime environment changes."""
    module_name = "my_test_package"

    # Initially not available
    dependencies.reset_dependency_cache(module_name)
    assert dependencies.module_available(module_name) is False

    # Create package
    pkg_path = tmp_path / module_name
    pkg_path.mkdir()
    (pkg_path / "__init__.py").write_text("", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))

    # Now available after cache reset
    dependencies.reset_dependency_cache(module_name)
    assert dependencies.module_available(module_name) is True
```

**Key principles:**

- Never use module-level boolean constants for optional dependencies in mypyc-compiled code
- Use `dependency_flag()` for boolean-like guards that evaluate at runtime
- Use `module_available()` inside functions for on-demand checks
- Call `reset_dependency_cache()` in tests that modify `sys.path`
- See `docs/guides/performance/mypyc.md` for the full anti-pattern documentation

<a id="eager-compilation-pattern"></a>

## Eager Compilation Pattern

When returning SQL objects that will be used with downstream operations requiring a parsed expression (like pagination with `select_with_total()`), compile the SQL eagerly to ensure predictable fail-fast behavior.

### The Problem

Lazy compilation can cause confusing errors when SQL objects are passed to methods that require a parsed expression:

```python
# Lazy pattern - errors surface late at usage time
sql = SQL(raw_sql, dialect=dialect)
return sql  # expression is None until compile() called

# Later, in select_with_total():
# "Cannot create COUNT query from empty SQL expression"
```

### The Solution

Compile SQL immediately after construction and before returning:

```python
def get_sql(self, name: str) -> "SQL":
    """Get a SQL object by statement name.

    Returns:
        SQL object ready for execution (pre-compiled).

    Raises:
        SQLFileNotFoundError: If statement name not found.
        SQLFileParseError: If SQL cannot be compiled.
    """
    # ... lookup logic ...

    sql = SQL(parsed_statement.sql, dialect=sqlglot_dialect)
    try:
        sql.compile()
    except Exception as exc:
        raise SQLFileParseError(name=name, path="<statement>", original_error=exc) from exc
    return sql
```

### Benefits

1. **Fail-fast**: Invalid SQL errors surface immediately at load time, not at query time
2. **Predictable**: All returned SQL objects have `expression` populated
3. **Compatible**: Works seamlessly with `select_with_total()`, pagination, and other AST-dependent features
4. **Cached**: The `compile()` result is cached in the SQL object, so subsequent calls are free

### When to Use

Use eager compilation when:

- Returning SQL objects from loaders or factories
- Building SQL objects that will be used with pagination
- Creating SQL objects that may be passed to methods requiring `expression`

**Key principle:** If downstream code might need `sql.expression`, compile eagerly at construction time rather than lazily at usage time.

<a id="protocol-capability-property-pattern"></a>

## Protocol Capability Property Pattern

When adding optional functionality to a protocol that may not be supported by all implementations, use a capability property to enable runtime capability checking.

### The Problem

Not all implementations of a protocol support every operation. Calling unsupported methods should raise `NotImplementedError`, but callers need a way to check capability before calling.

### The Solution

Add a `supports_X` property to the protocol with a default implementation returning `False`. Implementations that support the feature override to return `True`.

```python
@runtime_checkable
class ObjectStoreProtocol(Protocol):
    """Protocol for object storage operations."""

    @property
    def supports_signing(self) -> bool:
        """Whether this backend supports URL signing.

        Returns:
            True if the backend supports generating signed URLs, False otherwise.
        """
        return False

    @overload
    def sign_sync(self, paths: str, expires_in: int = 3600, for_upload: bool = False) -> str: ...

    @overload
    def sign_sync(self, paths: list[str], expires_in: int = 3600, for_upload: bool = False) -> list[str]: ...

    def sign_sync(
        self, paths: "str | list[str]", expires_in: int = 3600, for_upload: bool = False
    ) -> "str | list[str]":
        """Generate signed URL(s) for object(s).

        Raises:
            NotImplementedError: If the backend does not support URL signing.
        """
        msg = "URL signing not supported by this backend"
        raise NotImplementedError(msg)
```

### Implementation Pattern

```python
class ObStoreBackend:
    """Backend with signing support for cloud protocols."""

    @property
    def supports_signing(self) -> bool:
        """Only S3, GCS, and Azure support signing."""
        signable_protocols = {"s3", "gs", "gcs", "az", "azure"}
        return self.protocol in signable_protocols

    def sign_sync(
        self, paths: "str | list[str]", expires_in: int = 3600, for_upload: bool = False
    ) -> "str | list[str]":
        if not self.supports_signing:
            msg = f"URL signing is not supported for protocol '{self.protocol}'."
            raise NotImplementedError(msg)
        # Actual implementation...


class LocalStore:
    """Backend without signing support."""

    @property
    def supports_signing(self) -> bool:
        """Local storage never supports signing."""
        return False

    def sign_sync(
        self, paths: "str | list[str]", expires_in: int = 3600, for_upload: bool = False
    ) -> "str | list[str]":
        msg = "Local file storage does not support URL signing."
        raise NotImplementedError(msg)
```

### Usage Pattern

```python
def get_signed_url_if_supported(backend: ObjectStoreProtocol, path: str) -> str | None:
    """Get signed URL if backend supports it, otherwise return None."""
    if backend.supports_signing:
        return backend.sign_sync(path)
    return None
```

### Benefits

1. **Type-safe**: No `hasattr()` checks needed - property is always present
2. **Explicit**: Capability is documented in the protocol
3. **Testable**: Property can be mocked in tests
4. **Extensible**: New implementations just override the property

### When to Use

Use this pattern when:

- Adding optional functionality to an existing protocol
- Some implementations can support a feature, others cannot
- Callers need to check capability before calling

**Reference implementation:** `sqlspec/protocols.py` (`ObjectStoreProtocol.supports_signing`)
