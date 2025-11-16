# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SQLSpec is a type-safe SQL query mapper for Python - NOT an ORM. It provides flexible connectivity with consistent interfaces across multiple database systems. Write raw SQL, use the builder API, or load SQL from files. All statements pass through a sqlglot-powered AST pipeline for validation and dialect conversion.

## Common Development Commands

### Building and Installation

```bash
make install                    # Install with dev dependencies
uv sync --all-extras --dev      # Alternative
make build                      # Build package
HATCH_BUILD_HOOKS_ENABLE=1 uv build --extra mypyc  # Build with mypyc compilation
```

### Testing

```bash
make test                                          # Run full test suite
uv run pytest -n 2 --dist=loadgroup tests          # Alternative with parallelism
uv run pytest tests/path/to/test_file.py           # Single file
uv run pytest tests/path/to/test_file.py::test_fn  # Single test
uv run pytest tests/integration/test_adapters/test_<adapter>/ -v  # Adapter tests
```

### Linting and Type Checking

```bash
make lint                       # Run all linting checks
make fix                        # Auto-fix issues
make mypy                       # Run mypy (uses dmypy)
make pyright                    # Run pyright
uv run pre-commit run --all-files  # Pre-commit hooks
```

### Development Infrastructure

- **Start development databases**: `make infra-up` or `./tools/local-infra.sh up`
- **Stop development databases**: `make infra-down` or `./tools/local-infra.sh down`
- **Start specific database**: `make infra-postgres`, `make infra-oracle`, or `make infra-mysql`

## High-Level Architecture

SQLSpec is a type-safe SQL query mapper designed for minimal abstraction between Python and SQL. It is NOT an ORM but rather a flexible connectivity layer that provides consistent interfaces across multiple database systems.

### Core Components

1. **SQLSpec Base (`sqlspec/base.py`)**: The main registry and configuration manager. Handles database configuration registration, connection pooling lifecycle, and provides context managers for sessions.

2. **Adapters (`sqlspec/adapters/`)**: Database-specific implementations. Each adapter consists of:
   - `config.py`: Configuration classes specific to the database
   - `driver.py`: Driver implementation (sync/async) that executes queries
   - `_types.py`: Type definitions specific to the adapter or other uncompilable mypyc objects
   - Supported adapters: `adbc`, `aiosqlite`, `asyncmy`, `asyncpg`, `bigquery`, `duckdb`, `oracledb`, `psqlpy`, `psycopg`, `sqlite`

3. **Driver System (`sqlspec/driver/`)**: Base classes and mixins for all database drivers:
   - `_async.py`: Async driver base class with transaction support
   - `_sync.py`: Sync driver base class with transaction support
   - `_common.py`: Shared functionality and result handling
   - `mixins/`: Additional capabilities like result processing and SQL translation

4. **Core Query Processing (`sqlspec/core/`)**:
   - `statement.py`: SQL statement wrapper with metadata
   - `parameters.py`: Parameter style conversion (e.g., `?` to `$1` for Postgres)
   - `result.py`: Result set handling with type mapping support
   - `cache.py`: Statement caching for performance
   - `compiler.py`: SQL compilation and validation using sqlglot

5. **SQL Builder (`sqlspec/builder/`)**: Experimental fluent API for building SQL queries programmatically. Uses method chaining and mixins for different SQL operations (SELECT, INSERT, UPDATE, DELETE, etc.).

6. **SQL Factory (`sqlspec/_sql.py`)**: SQL Factory that combines raw SQL parsing with the SQL builder components.

7. **Storage (`sqlspec/storage/`)**: Unified interface for data import/export operations with backends for fsspec and obstore.

8. **Extensions (`sqlspec/extensions/`)**: Framework integrations:
   - `litestar/`: Litestar web framework integration with dependency injection
   - `aiosql/`: Integration with aiosql for SQL file loading

9. **Loader (`sqlspec/loader.py`)**: SQL file loading system that parses `.sql` files and creates callable query objects with type hints.

10. **Database Migrations (`sqlspec/migrations/`)**: A set of tools and CLI commands to enable database migrations generations.  Offers SQL and Python templates and up/down methods to apply.  It also uses the builder API to create a version tracking table to track applied revisions in the database.

### Key Design Patterns

- **Protocol-Based Design**: Uses Python protocols (`sqlspec/protocols.py`) for runtime type checking instead of inheritance
    - ALL protocols in `sqlspec.protocols.py`
    - ALL type guards in `sqlspec.utils.type_guards.py`
- **Configuration-Driver Separation**: Each adapter has a config class (connection details) and driver class (execution logic)
- **Context Manager Pattern**: All database sessions use context managers for proper resource cleanup
- **Parameter Style Abstraction**: Automatically converts between different parameter styles (?, :name, $1, %s)
- **Type Safety**: Supports mapping results to Pydantic, msgspec, attrs, and other typed models
- **Single-Pass Processing**: Parse once → transform once → validate once - SQL object is single source of truth
- **Abstract Methods with Concrete Implementations**: Protocol defines abstract methods, base classes provide concrete sync/async implementations

### Query Stack Implementation Guidelines

- **Builder Discipline**
    - `StatementStack` and `StackOperation` are immutable (`__slots__`, tuple storage). Every push helper returns a new stack; never mutate `_operations` in place.
    - Validate inputs at push time (non-empty SQL, execute_many payloads, reject nested stacks) so drivers can assume well-formed operations.
- **Adapter Responsibilities**
    - Add a single capability gate per adapter (e.g., Oracle pipeline version check, `psycopg.capabilities.has_pipeline()`), return `super().execute_stack()` immediately when unsupported.
    - Preserve `StackResult.result` by building SQL/Arrow results via `create_sql_result()` / `create_arrow_result()` instead of copying row data.
    - Honor manual toggles via `driver_features={"stack_native_disabled": True}` and document the behavior in the adapter guide.
- **Telemetry + Tracing**
    - Always wrap adapter overrides with `StackExecutionObserver(self, stack, continue_on_error, native_pipeline=bool)`.
    - Do **not** emit duplicate metrics; the observer already increments `stack.execute.*`, logs `stack.execute.start/complete/failed`, and publishes the `sqlspec.stack.execute` span.
- **Error Handling**
    - Wrap driver exceptions in `StackExecutionError` with `operation_index`, summarized SQL (`describe_stack_statement()`), adapter name, and execution mode.
    - Continue-on-error stacks append `StackResult.from_error()` and keep executing. Fail-fast stacks roll back (if they started the transaction) before re-raising the wrapped error.
- **Testing Expectations**
    - Add integration tests under `tests/integration/test_adapters/<adapter>/test_driver.py::test_*statement_stack*` that cover native path, sequential fallback, and continue-on-error.
    - Guard base behavior (empty stacks, large stacks, transaction boundaries) via `tests/integration/test_stack_edge_cases.py`.

### Driver Parameter Profile Registry

- All adapter parameter defaults live in `DriverParameterProfile` entries inside `sqlspec/core/parameters.py`.
- Use lowercase adapter keys (e.g., `"asyncpg"`, `"duckdb"`) and populate every required field: default style, supported styles, execution style, native list expansion flags, JSON strategy, and optional extras.
- JSON behaviour is controlled through `json_serializer_strategy`:
    - `"helper"`: call `ParameterStyleConfig.with_json_serializers()` (dict/list/tuple auto-encode)
    - `"driver"`: defer to driver codecs while surfacing serializer references for later registration
    - `"none"`: skip JSON helpers entirely (reserve for adapters that must not touch JSON)
- Extras should encapsulate adapter-specific tweaks (e.g., `type_coercion_overrides`, `json_tuple_strategy`). Document new extras inline and keep them immutable.
- Always build `StatementConfig` via `build_statement_config_from_profile()` and pass adapter-specific overrides through the helper instead of instantiating configs manually in drivers.
- When introducing a new adapter, add its profile, update relevant guides, and extend unit coverage so each JSON strategy path is exercised.
- Record the canonical adapter key, JSON strategy, and extras in the corresponding adapter guide so contributors can verify behaviour without reading the registry source.

### Protocol Abstract Methods Pattern

When adding methods that need to support both sync and async configurations, use this pattern:

**Step 1: Define abstract method in protocol**

```python
from abc import abstractmethod
from typing import Awaitable

class DatabaseConfigProtocol(Protocol):
    is_async: ClassVar[bool]  # Set by base classes

    @abstractmethod
    def migrate_up(
        self, revision: str = "head", allow_missing: bool = False, auto_sync: bool = True, dry_run: bool = False
    ) -> "Awaitable[None] | None":
        """Apply database migrations up to specified revision.

        Args:
            revision: Target revision or "head" for latest.
            allow_missing: Allow out-of-order migrations.
            auto_sync: Auto-reconcile renamed migrations.
            dry_run: Show what would be done without applying.
        """
        raise NotImplementedError
```

**Step 2: Implement in sync base class (no async/await)**

```python
class NoPoolSyncConfig(DatabaseConfigProtocol):
    is_async: ClassVar[bool] = False

    def migrate_up(
        self, revision: str = "head", allow_missing: bool = False, auto_sync: bool = True, dry_run: bool = False
    ) -> None:
        """Apply database migrations up to specified revision."""
        commands = self._ensure_migration_commands()
        commands.upgrade(revision, allow_missing, auto_sync, dry_run)
```

**Step 3: Implement in async base class (with async/await)**

```python
class NoPoolAsyncConfig(DatabaseConfigProtocol):
    is_async: ClassVar[bool] = True

    async def migrate_up(
        self, revision: str = "head", allow_missing: bool = False, auto_sync: bool = True, dry_run: bool = False
    ) -> None:
        """Apply database migrations up to specified revision."""
        commands = cast("AsyncMigrationCommands", self._ensure_migration_commands())
        await commands.upgrade(revision, allow_missing, auto_sync, dry_run)
```

**Key principles:**

- Protocol defines the interface with union return type (`Awaitable[T] | T`)
- Sync base classes implement without `async def` or `await`
- Async base classes implement with `async def` and `await`
- Each base class has concrete implementation - no need for child classes to override
- Use `cast()` to narrow types when delegating to command objects
- All 4 base classes (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig) implement the same way

**Benefits:**

- Single source of truth (protocol) for API contract
- Each base class provides complete implementation
- Child adapter classes (AsyncpgConfig, SqliteConfig, etc.) inherit working methods automatically
- Type checkers understand sync vs async based on `is_async` class variable
- No code duplication across adapters

**When to use:**

- Adding convenience methods that delegate to external command objects
- Methods that need identical behavior across all adapters
- Operations that differ only in sync vs async execution
- Any protocol method where behavior is determined by sync/async mode

**Anti-patterns to avoid:**

- Don't use runtime `if self.is_async:` checks in a single implementation
- Don't make protocol methods concrete (always use `@abstractmethod`)
- Don't duplicate logic across the 4 base classes
- Don't forget to update all 4 base classes when adding new methods

### Database Connection Flow

```python
import tempfile

def test_starlette_autocommit_mode() -> None:
    """Test autocommit mode automatically commits on success."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            pool_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "autocommit"}}
        )
        sql.add_config(config)
        db_ext = SQLSpecPlugin(sql, app)

        # Test logic here - each test gets isolated database
```

**Why this works**:

- Each test creates a unique temporary file
- No database state shared between tests
- Tests can run in parallel safely with `pytest -n 2 --dist=loadgroup`
- Files automatically deleted on test completion

**When to use**:

- Framework extension tests (Starlette, FastAPI, Flask, etc.)
- Any test using connection pooling with SQLite
- Integration tests that run in parallel

**Alternatives NOT recommended**:

- `CREATE TABLE IF NOT EXISTS` - Masks test isolation issues
- Disabling pooling - Tests don't reflect production configuration
- Running tests serially - Slows down CI significantly

### CLI Config Loader Isolation Pattern

- When exercising CLI migration commands, generate a unique module namespace for each test (for example `cli_test_config_<uuid>`).
- Place temporary config modules inside `tmp_path` and register them via `sys.modules` within the test, then delete them during teardown to prevent bleed-through.
- Always patch `Path.cwd()` or provide explicit path arguments so helper functions resolve the test-local module rather than cached global fixtures.
- Add regression tests ensuring the helper cleaning logic runs even if CLI commands raise exceptions to avoid polluting later suites.

### Performance Optimizations

- **Mypyc Compilation**: Core modules can be compiled with mypyc for performance
- **Statement Caching**: Parsed SQL statements are cached to avoid re-parsing
- **Connection Pooling**: Built-in support for connection pooling in async drivers
- **Arrow Integration**: Direct export to Arrow format for efficient data handling

## **MANDATORY** Code Quality Standards (TOP PRIORITY)

### Type Annotations

- **PROHIBITED**: `from __future__ import annotations`
- **REQUIRED**: Stringified type hints for non-builtins: `def foo(config: "SQLConfig"):`
- **REQUIRED**: PEP 604 syntax: `T | None`, not `Optional[T]`

### Import Standards

- All imports at module level (no nested imports except for circular import prevention)
- Absolute imports only
- Order: stdlib → third-party → first-party
- Use `if TYPE_CHECKING:` for type-only imports

### Code Style

- Maximum 75 lines per function (preferred 30-50)
- Early returns and guard clauses over nested conditionals
- Type guards instead of `hasattr()` checks
- No inline comments - use docstrings
- Google-style docstrings with Args, Returns, Raises sections

### Testing

- **MANDATORY**: Function-based tests only (`def test_something():`)
- **PROHIBITED**: Class-based tests (`class TestSomething:`)
- Use `pytest-databases` for containerized database tests
- Use `tempfile.NamedTemporaryFile` for SQLite pooling tests (not `:memory:`)

### Mypyc Compatibility

For classes in `sqlspec/core/` and `sqlspec/driver/`:

- Use `__slots__` for data-holding classes
- Implement explicit `__init__`, `__repr__`, `__eq__`, `__hash__`
- Avoid dataclasses in performance-critical paths

## Detailed Guides

For detailed implementation patterns, consult these guides:

| Topic | Guide Location |
|-------|----------------|
| Architecture | `docs/guides/architecture/architecture.md` |
| Data Flow | `docs/guides/architecture/data-flow.md` |
| Arrow Integration | `docs/guides/architecture/arrow-integration.md` |
| Query Stack Patterns | `docs/guides/architecture/patterns.md` |
| Testing | `docs/guides/testing/testing.md` |
| Mypyc Optimization | `docs/guides/performance/mypyc.md` |
| SQLglot Best Practices | `docs/guides/performance/sqlglot.md` |
| Parameter Profiles | `docs/guides/adapters/parameter-profile-registry.md` |
| Adapter Guides | `docs/guides/adapters/{adapter}.md` |
| Framework Extensions | `docs/guides/extensions/{framework}.md` |
| Quick Reference | `docs/guides/quick-reference/quick-reference.md` |
| Code Standards | `docs/guides/development/code-standards.md` |
| Implementation Patterns | `docs/guides/development/implementation-patterns.md` |

## Agent Workflow

This project uses a multi-agent workflow. Agent definitions are in `.claude/agents/`:

| Agent | File | Purpose |
|-------|------|---------|
| PRD | `.claude/agents/prd.md` | Requirements planning |
| Expert | `.claude/agents/expert.md` | Implementation |
| Testing | `.claude/agents/testing.md` | Test creation |
| Docs & Vision | `.claude/agents/docs-vision.md` | Documentation, QA, knowledge capture |

### Workspace Structure

```
specs/
├── active/{feature}/    # Active work (gitignored)
│   ├── prd.md          # Requirements
│   ├── tasks.md        # Checklist
│   ├── recovery.md     # Resume guide
│   └── research/       # Findings
├── archive/            # Completed work
└── template-spec/      # Template structure
```

### Slash Commands

- `/prd` - Create product requirements document
- `/implement` - Full implementation workflow (auto-invokes `/test` and `/review`)
- `/test` - Create comprehensive tests
- `/review` - Documentation, quality gate, and archival

## PR Description Standards

Pull requests must be concise (30-40 lines max). Required sections:

1. **Summary** (2-3 sentences)
2. **The Problem** (2-4 lines)
3. **The Solution** (2-4 lines)
4. **Key Features** (3-5 bullets)

Prohibited: test coverage tables, file change lists, quality metrics, commit breakdowns.

## Key Patterns Quick Reference

### driver_features Pattern

```python
class AdapterDriverFeatures(TypedDict):
    """Feature flags with enable_ prefix for booleans."""
    enable_feature: NotRequired[bool]
    json_serializer: NotRequired[Callable[[Any], str]]

# Auto-detect in config __init__
if "enable_feature" not in driver_features:
    driver_features["enable_feature"] = OPTIONAL_PACKAGE_INSTALLED
```

### Type Handler Pattern

```python
# In adapter's _feature_handlers.py
def register_handlers(connection: "Connection") -> None:
    if not OPTIONAL_PACKAGE_INSTALLED:
        logger.debug("Package not installed - skipping handlers")
        return
    connection.inputtypehandler = _input_type_handler
    connection.outputtypehandler = _output_type_handler
```

### Binary Data Encoding Pattern (Spanner)

For databases requiring specific binary data encoding (e.g., Spanner's base64 requirement):

```python
# In adapter's _type_handlers.py
import base64

def bytes_to_database(value: bytes | None) -> bytes | None:
    """Convert Python bytes to database-required format.

    Spanner Python client requires base64-encoded bytes when
    param_types.BYTES is specified.
    """
    if value is None:
        return None
    return base64.b64encode(value)

def database_to_bytes(value: Any) -> bytes | None:
    """Convert database BYTES result back to Python bytes.

    Handles both raw bytes and base64-encoded bytes.
    """
    if value is None:
        return None
    if isinstance(value, bytes | str):
        return base64.b64decode(value)
    return None
```

**Use this pattern when**:

- Database client library requires specific encoding for binary data
- Need transparent conversion between Python bytes and database format
- Want to centralize encoding/decoding logic for reuse

**Key principles**:

- Centralize conversion functions in `_type_handlers.py`
- Handle None/NULL values explicitly
- Support both raw and encoded formats on read (graceful handling)
- Use in `coerce_params_for_database()` and type converter

**Example**: Spanner requires base64-encoded bytes for `param_types.BYTES` parameters, but you work with raw Python bytes in application code.

### Instance-Based Config Registry Pattern

SQLSpec uses config instances as handles for database connections. The registry keys by `id(config)` instead of `type(config)` to support multiple databases of the same adapter type.

```python
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

manager = SQLSpec()

# Config instance IS the handle - add_config returns same instance
main_db = manager.add_config(AsyncpgConfig(connection_config={"dsn": "postgresql://main/..."}))
analytics_db = manager.add_config(AsyncpgConfig(connection_config={"dsn": "postgresql://analytics/..."}))

# Type checker knows: AsyncpgConfig → AsyncContextManager[AsyncpgDriver]
async with manager.provide_session(main_db) as driver:
    await driver.execute("SELECT 1")

# Different connection pool! Works correctly now.
async with manager.provide_session(analytics_db) as driver:
    await driver.execute("SELECT 1")
```

**Use this pattern when**:

- Managing multiple databases of the same adapter type (e.g., main + analytics PostgreSQL)
- Integrating with DI frameworks (config instance is the dependency)
- Need type-safe session handles without `# type: ignore`

**Key principles**:

- Registry uses `id(config)` as key (not `type(config)`)
- Multiple configs of same adapter type are stored separately
- `add_config` returns the same instance passed in
- All methods require registered config instances
- Unregistered configs raise `ValueError`

**DI framework integration**:

```python
# Just pass the config instance - it's already correctly typed
def get_main_db() -> AsyncpgConfig:
    return main_db

# DI provider knows this is async from the config type
async def provide_db_session(db: AsyncpgConfig, manager: SQLSpec):
    async with manager.provide_session(db) as driver:
        yield driver
```

**Reference implementation**: `sqlspec/base.py` (lines 58, 128-151, 435-581)

### Framework Extension Pattern

All extensions use `extension_config` in database config:

```python
config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://..."},
    extension_config={
        "starlette": {"commit_mode": "autocommit", "session_key": "db"}
    }
)
```

### Custom SQLGlot Expression Pattern

For dialect-specific SQL generation (e.g., vector distance functions):

```python
# In sqlspec/builder/_custom_expressions.py
from sqlglot import exp
from typing import Any

class CustomExpression(exp.Expression):
    """Custom expression with dialect-aware SQL generation."""
    arg_types = {"this": True, "expression": True, "metric": False}

    def sql(self, dialect: "Any | None" = None, **opts: Any) -> str:
        """Override sql() method for dialect-specific generation."""
        dialect_name = str(dialect).lower() if dialect else "generic"

        left_sql = self.left.sql(dialect=dialect, **opts)
        right_sql = self.right.sql(dialect=dialect, **opts)

        if dialect_name == "postgres":
            return self._sql_postgres(left_sql, right_sql)
        if dialect_name == "mysql":
            return self._sql_mysql(left_sql, right_sql)
        return self._sql_generic(left_sql, right_sql)

# Register with SQLGlot generator system
def _register_with_sqlglot() -> None:
    from sqlglot.dialects.postgres import Postgres
    from sqlglot.generator import Generator

    def custom_sql_base(generator: "Generator", expression: "CustomExpression") -> str:
        return expression._sql_generic(generator.sql(expression.left), generator.sql(expression.right))

    Generator.TRANSFORMS[CustomExpression] = custom_sql_base
    Postgres.Generator.TRANSFORMS[CustomExpression] = custom_sql_postgres

_register_with_sqlglot()
```

**Use this pattern when**:

- Database syntax varies significantly across dialects
- Standard SQLGlot expressions don't match any database's native syntax
- Need operator syntax (e.g., `<->`) vs function calls (e.g., `DISTANCE()`)

**Key principles**:

- Override `.sql()` method for dialect detection
- Register with SQLGlot's TRANSFORMS for nested expression support
- Store metadata (like metric) as `exp.Identifier` in `arg_types` for runtime access
- Provide generic fallback for unsupported dialects

**Example**: `VectorDistance` in `sqlspec/builder/_vector_expressions.py` generates:

- PostgreSQL: `embedding <-> '[0.1,0.2]'` (operator)
- MySQL: `DISTANCE(embedding, STRING_TO_VECTOR('[0.1,0.2]'), 'EUCLIDEAN')` (function)
- Oracle: `VECTOR_DISTANCE(embedding, TO_VECTOR('[0.1,0.2]'), EUCLIDEAN)` (function)

### Custom SQLglot Dialect Pattern

For databases with unique SQL syntax not supported by existing sqlglot dialects:

```python
# In sqlspec/adapters/{adapter}/dialect/_dialect.py
from sqlglot import exp
from sqlglot.dialects.bigquery import BigQuery
from sqlglot.tokens import TokenType

class CustomDialect(BigQuery):
    """Inherit from closest matching dialect."""

    class Tokenizer(BigQuery.Tokenizer):
        """Add custom keywords."""
        KEYWORDS = {
            **BigQuery.Tokenizer.KEYWORDS,
            "INTERLEAVE": TokenType.INTERLEAVE,
        }

    class Parser(BigQuery.Parser):
        """Override parser for custom syntax."""
        def _parse_table_parts(self, schema=False, is_db_reference=False, wildcard=False):
            table = super()._parse_table_parts(schema=schema, is_db_reference=is_db_reference, wildcard=wildcard)

            # Parse custom clause
            if self._match_text_seq("INTERLEAVE", "IN", "PARENT"):
                parent = self._parse_table(schema=True, is_db_reference=True)
                table.set("interleave_parent", parent)

            return table

    class Generator(BigQuery.Generator):
        """Override generator for custom SQL output."""
        def table_sql(self, expression, sep=" "):
            sql = super().table_sql(expression, sep=sep)

            # Generate custom clause
            parent = expression.args.get("interleave_parent")
            if parent:
                sql = f"{sql}\nINTERLEAVE IN PARENT {self.sql(parent)}"

            return sql

# Register dialect in adapter __init__.py
from sqlglot.dialects.dialect import Dialect
Dialect.classes["custom"] = CustomDialect
```

**Create custom dialect when**:

- Database has unique DDL/DML syntax not in existing dialects
- Need to parse and validate database-specific keywords
- Need to generate database-specific SQL from AST
- An existing dialect provides 80%+ compatibility to inherit from

**Do NOT create custom dialect if**:

- Only parameter style differences (use parameter profiles)
- Only type conversion differences (use type converters)
- Only connection management differences (use config/driver)

**Key principles**:

- **Inherit from closest dialect**: Spanner inherits BigQuery (both GoogleSQL)
- **Minimal overrides**: Only override methods that need customization
- **Store metadata in AST**: Use `expression.set(key, value)` for custom data
- **Handle missing tokens**: Check `getattr(TokenType, "KEYWORD", None)` before using
- **Test thoroughly**: Unit tests for parsing/generation, integration tests with real DB

**Reference implementation**: `sqlspec/adapters/spanner/dialect/` (GoogleSQL and PostgreSQL modes)

**Documentation**: See `/docs/guides/architecture/custom-sqlglot-dialects.md` for full guide

### Configuration Parameter Standardization Pattern

For API consistency across all adapters (pooled and non-pooled):

```python
# ALL configs accept these parameters for consistency:
class AdapterConfig(AsyncDatabaseConfig):  # or SyncDatabaseConfig
    def __init__(
        self,
        *,
        connection_config: dict[str, Any] | None = None,  # Settings dict
        connection_instance: PoolT | None = None,         # Pre-created pool/connection
        ...
    ) -> None:
        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            ...
        )
```

**Key principles:**

- `connection_config` holds ALL connection and pool configuration (unified dict)
- `connection_instance` accepts pre-created pools or connections (for dependency injection)
- Works semantically for both pooled (AsyncPG) and non-pooled adapters (BigQuery, ADBC)
- Non-pooled adapters accept `connection_instance` for API consistency (even if always None)
- NoPoolSyncConfig and NoPoolAsyncConfig accept `connection_instance: Any = None` for flexibility

**Why this pattern:**

- Consistent API eliminates cognitive load when switching adapters
- Clear separation: config dict vs pre-created instance
- Supports dependency injection scenarios
- Better than adapter-specific parameter names

**Migration from old names:**

- v0.33.0+: `pool_config` → `connection_config`, `pool_instance` → `connection_instance`

### Parameter Deprecation Pattern

For backwards-compatible parameter renames in configuration classes:

```python
def __init__(
    self,
    *,
    new_param: dict[str, Any] | None = None,
    **kwargs: Any,  # Capture old parameter names
) -> None:
    from sqlspec.utils.deprecation import warn_deprecation

    if "old_param" in kwargs:
        warn_deprecation(
            version="0.33.0",
            deprecated_name="old_param",
            kind="parameter",
            removal_in="0.34.0",
            alternative="new_param",
            info="Parameter renamed for consistency across pooled and non-pooled adapters",
        )
        if new_param is None:
            new_param = kwargs.pop("old_param")
        else:
            kwargs.pop("old_param")  # Discard if new param provided

    # Continue with initialization using new_param
```

**Use this pattern when:**

- Renaming configuration parameters for consistency
- Need backwards compatibility during migration period
- Want clear deprecation warnings for users

**Key principles:**

- Use `**kwargs` to capture old parameter names without changing signature
- Import `warn_deprecation` inside function to avoid circular imports
- New parameter takes precedence when both old and new provided
- Use `kwargs.pop()` to remove handled parameters and avoid `**kwargs` passing issues
- Provide clear migration path (version, alternative, removal timeline)
- Set removal timeline (typically next minor or major version)

**Reference implementation:** `sqlspec/config.py` (lines 920-1517, all 4 base config classes)

### Error Handling

- Custom exceptions inherit from `SQLSpecError` in `sqlspec/exceptions.py`
- Use `wrap_exceptions` context manager in adapter layer
- Two-tier pattern: graceful skip (DEBUG) for expected conditions, hard errors for malformed input

### Click Environment Variable Pattern

When adding CLI options that should support environment variables:

**Use Click's native `envvar` parameter instead of custom parsing:**

```python
# Good - Click handles env var automatically
@click.option(
    "--config",
    help="Dotted path to SQLSpec config(s) (env: SQLSPEC_CONFIG)",
    required=False,
    type=str,
    envvar="SQLSPEC_CONFIG",  # Click handles precedence: CLI flag > env var
)
def command(config: str | None):
    pass

# Bad - Custom env var parsing
import os

@click.option("--config", required=False, type=str)
def command(config: str | None):
    if config is None:
        config = os.getenv("SQLSPEC_CONFIG")  # Don't do this!
```

**Benefits:**

- Click automatically handles precedence (CLI flag always overrides env var)
- Help text automatically shows env var name
- Support for multiple fallback env vars via `envvar=["VAR1", "VAR2"]`
- Less code, fewer bugs

**For project file discovery (pyproject.toml, etc.):**

- Use custom logic as fallback after Click env var handling
- Walk filesystem from cwd to find config files
- Return `None` if not found to trigger helpful error message

**Multi-config support:**

- Split comma-separated values from CLI flag, env var, or pyproject.toml
- Resolve each config path independently
- Flatten results if callables return lists
- Deduplicate by `bind_key` (later configs override earlier ones with same key)

**Reference implementation:** `sqlspec/cli.py` (lines 26-110), `sqlspec/utils/config_discovery.py`

### CLI Sync/Async Dispatch Pattern

When implementing CLI commands that support both sync and async database adapters:

**Problem:** Sync adapters (SQLite, DuckDB, BigQuery, ADBC, Spanner sync, Psycopg sync, Oracle sync) fail with `await_ cannot be called from within an async task` if wrapped in an event loop.

**Solution:** Partition configs and execute sync/async batches separately:

```python
def _execute_for_config(
    config: "AsyncDatabaseConfig[Any, Any, Any] | SyncDatabaseConfig[Any, Any, Any]",
    sync_fn: "Callable[[], Any]",
    async_fn: "Callable[[], Any]",
) -> Any:
    """Execute with appropriate sync/async handling."""
    from sqlspec.utils.sync_tools import run_

    if config.is_async:
        return run_(async_fn)()
    return sync_fn()

def _partition_configs_by_async(
    configs: "list[tuple[str, Any]]",
) -> "tuple[list[tuple[str, Any]], list[tuple[str, Any]]]":
    """Partition configs into sync and async groups."""
    sync_configs = [(name, cfg) for name, cfg in configs if not cfg.is_async]
    async_configs = [(name, cfg) for name, cfg in configs if cfg.is_async]
    return sync_configs, async_configs
```

**Usage pattern for single-config operations:**

```python
def _operation_for_config(config: Any) -> None:
    migration_commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = (
        create_migration_commands(config=config)
    )

    def sync_operation() -> None:
        migration_commands.operation(<args>)

    async def async_operation() -> None:
        await cast("AsyncMigrationCommands[Any]", migration_commands).operation(<args>)

    _execute_for_config(config, sync_operation, async_operation)
```

**Usage pattern for multi-config operations:**

```python
# Partition first
sync_configs, async_configs = _partition_configs_by_async(configs_to_process)

# Process sync configs directly (no event loop)
for config_name, config in sync_configs:
    _operation_for_config(config)

# Process async configs via single run_() call
if async_configs:
    async def _run_async_configs() -> None:
        for config_name, config in async_configs:
            migration_commands: AsyncMigrationCommands[Any] = cast(
                "AsyncMigrationCommands[Any]", create_migration_commands(config=config)
            )
            await migration_commands.operation(<args>)

    run_(_run_async_configs)()
```

**Key principles:**

- Sync configs must execute outside event loop (direct function calls)
- Async configs must execute inside event loop (via `run_()`)
- Multi-config operations should batch sync and async separately for efficiency
- Use `cast()` in async contexts for type safety with migration commands

**Reference implementation:** `sqlspec/cli.py` (lines 218-255, 311-724)

## Collaboration Guidelines

- Challenge suboptimal requests constructively
- Ask clarifying questions for ambiguous requirements
- Propose better alternatives with clear reasoning
- Consider edge cases, performance, and maintainability
