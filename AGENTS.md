# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Reference

This file contains **MANDATORY rules** and **unique patterns**. For detailed implementations, see:

| Category | Location |
|----------|----------|
| Implementation Patterns | `docs/guides/development/implementation-patterns.md` |
| Code Standards | `docs/guides/development/code-standards.md` |
| SQLGlot Dialects | `docs/guides/architecture/custom-sqlglot-dialects.md` |
| Events Extension | `docs/guides/events/database-event-channels.md` |

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

### Adapter Transaction Detection Pattern

Each adapter MUST override `_connection_in_transaction()` with direct attribute access instead of using the base class fallback which relies on `getattr()` chains.

```python
# In each adapter's driver.py
class MyAdapterDriver(SyncDriverBase):
    def _connection_in_transaction(self) -> bool:
        # AsyncPG: uses is_in_transaction() method
        return self.connection.is_in_transaction()

        # SQLite/DuckDB: uses in_transaction property
        return self.connection.in_transaction

        # Psycopg: uses status attribute
        return self.connection.status != psycopg.pq.TransactionStatus.IDLE

        # BigQuery: No transaction support
        return False
```

**Why this matters:**

- The base class uses `getattr()` chains which are slow and prevent mypyc optimization
- Each adapter knows exactly which attribute to check
- Direct attribute access is 10-50x faster in hot paths

**Reference implementations:** All adapters in `sqlspec/adapters/*/driver.py` have this override.

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

→ See `docs/guides/development/implementation-patterns.md#protocol-abstract-methods-pattern`

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

### Type Guards Pattern

Use guards from `sqlspec.utils.type_guards` instead of `hasattr()`:
`is_readable`, `has_array_interface`, `has_cursor_metadata`, `has_expression_and_sql`, `has_expression_and_parameters`, `is_statement_filter`

→ See `docs/guides/development/code-standards.md#type-guards`

### Testing

- **MANDATORY**: Function-based tests only (`def test_something():`)
- **PROHIBITED**: Class-based tests (`class TestSomething:`)
- Use `pytest-databases` for containerized database tests
- Use `tempfile.NamedTemporaryFile` for SQLite pooling tests (not `:memory:`)

### Mypyc Compatibility

Use `__slots__`, explicit `__init__/__repr__/__eq__/__hash__`, avoid dataclasses in `sqlspec/core/` and `sqlspec/driver/`.

→ See `docs/guides/development/code-standards.md#mypyc-compatible-class-pattern`

## Detailed Guides

For detailed implementation patterns, consult these guides:

| Topic | Guide Location |
|-------|----------------|
| Architecture | `docs/guides/architecture/architecture.md` |
| Data Flow | `docs/guides/architecture/data-flow.md` |
| Arrow Integration | `docs/guides/architecture/arrow-integration.md` |
| Query Stack Patterns | `docs/guides/architecture/patterns.md` |
| EXPLAIN Plans | `docs/guides/builder/explain.md` |
| MERGE Statements | `docs/guides/builder/merge.md` |
| Testing | `docs/guides/testing/testing.md` |
| Mypyc Optimization | `docs/guides/performance/mypyc.md` |
| SQLglot Best Practices | `docs/guides/performance/sqlglot.md` |
| Parameter Profiles | `docs/guides/adapters/parameter-profile-registry.md` |
| Adapter Guides | `docs/guides/adapters/{adapter}.md` |
| Framework Extensions | `docs/guides/extensions/{framework}.md` |
| Database Events | `docs/guides/events/database-event-channels.md` |
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

| Pattern | Reference |
|---------|-----------|
| driver_features | `docs/guides/development/implementation-patterns.md#driver-features-pattern` |
| Type Handler | `docs/guides/development/implementation-patterns.md#type-handler-pattern` |
| Framework Extension | `docs/guides/development/implementation-patterns.md#framework-extension-pattern` |
| EXPLAIN Builder | `docs/guides/development/implementation-patterns.md#explain-builder-pattern` |
| Dynamic Optional Deps | `docs/guides/development/implementation-patterns.md#dynamic-optional-dependency-pattern` |
| Eager Compilation | `docs/guides/development/implementation-patterns.md#eager-compilation-pattern` |
| Protocol Capability | `docs/guides/development/implementation-patterns.md#protocol-capability-property-pattern` |
| Custom SQLGlot Dialect | `docs/guides/architecture/custom-sqlglot-dialects.md#custom-sqlglot-dialect` |
| Events Extension | `docs/guides/events/database-event-channels.md#events-architecture` |
| Binary Data Encoding | `sqlspec/adapters/spanner/_type_handlers.py` |
| Instance-Based Config | `sqlspec/base.py` |
| Config Param Standard | `sqlspec/config.py` base classes |
| Parameter Deprecation | `sqlspec/utils/deprecation.py` |
| CLI Patterns | `sqlspec/cli.py` |

### Error Handling

- Inherit from `SQLSpecError` in `sqlspec/exceptions.py`
- Use `wrap_exceptions` context manager
- Two-tier: graceful skip (DEBUG) for expected, hard errors for malformed

## Collaboration Guidelines

- Challenge suboptimal requests constructively
- Ask clarifying questions for ambiguous requirements
- Propose better alternatives with clear reasoning
- Consider edge cases, performance, and maintainability
