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

```bash
make infra-up                   # Start all dev databases (Docker)
make infra-down                 # Stop databases
make infra-postgres             # Start only PostgreSQL
make infra-oracle               # Start only Oracle
make infra-mysql                # Start only MySQL
```

### Documentation

```bash
make docs                       # Build Sphinx documentation
```

## High-Level Architecture

### Core Components

| Component | Location | Purpose |
|-----------|----------|---------|
| SQLSpec Base | `sqlspec/base.py` | Registry, config management, session context managers |
| Adapters | `sqlspec/adapters/` | Database-specific implementations (asyncpg, psycopg, duckdb, etc.) |
| Driver System | `sqlspec/driver/` | Base classes for sync/async drivers with transaction support |
| Core Processing | `sqlspec/core/` | Statement parsing, parameter conversion, result handling, caching |
| SQL Builder | `sqlspec/builder/` | Fluent API for programmatic query construction |
| Storage | `sqlspec/storage/` | Data import/export with fsspec and obstore backends |
| Extensions | `sqlspec/extensions/` | Framework integrations (Litestar, Starlette, FastAPI, Flask) |
| Migrations | `sqlspec/migrations/` | Database migration tools and CLI |

### Adapters Structure

Each adapter in `sqlspec/adapters/` follows this structure:
- `config.py` - Configuration classes with pool settings
- `driver.py` - Query execution implementation (sync/async)
- `_types.py` - Adapter-specific type definitions
- Optional: `_*_handlers.py` - Type handlers for optional features (numpy, pgvector)

Supported adapters: adbc, aiosqlite, asyncmy, asyncpg, bigquery, duckdb, oracledb, psqlpy, psycopg, spanner, sqlite

### Key Design Patterns

- **Protocol-Based Design**: All protocols in `sqlspec/protocols.py`, type guards in `sqlspec/utils/type_guards.py`
- **Configuration-Driver Separation**: Config holds connection details, Driver executes queries
- **Context Manager Pattern**: All sessions use context managers for resource cleanup
- **Parameter Style Abstraction**: Automatic conversion between ?, :name, $1, %s styles

### Database Connection Flow

```python
# 1. Create configuration
config = AsyncpgConfig(pool_config={"dsn": "postgresql://..."})

# 2. Register with SQLSpec
sql = SQLSpec()
sql.add_config(config)

# 3. Get session via context manager
async with sql.provide_session(config) as session:
    result = await session.execute("SELECT * FROM users")
```

## Code Quality Standards (MANDATORY)

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

### Framework Extension Pattern

All extensions use `extension_config` in database config:

```python
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://..."},
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

### Error Handling

- Custom exceptions inherit from `SQLSpecError` in `sqlspec/exceptions.py`
- Use `wrap_exceptions` context manager in adapter layer
- Two-tier pattern: graceful skip (DEBUG) for expected conditions, hard errors for malformed input

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
