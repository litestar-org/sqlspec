# AGENTS.md

This file provides guidance to Gemini, Claude Code, Codex, and other agents when working with code in this repository.

## Collaboration Guidelines

- **Challenge and question**: Don't immediately agree or proceed with requests that seem suboptimal, unclear, or potentially problematic
- **Push back constructively**: If a proposed approach has issues, suggest better alternatives with clear reasoning
- **Think critically**: Consider edge cases, performance implications, maintainability, and best practices before implementing
- **Seek clarification**: Ask follow-up questions when requirements are ambiguous or could be interpreted multiple ways
- **Propose improvements**: Suggest better patterns, more robust solutions, or cleaner implementations when appropriate
- **Be a thoughtful collaborator**: Act as a good teammate who helps improve the overall quality and direction of the project

## Pull Request Guidelines

### PR Description Standards (MANDATORY)

Pull request descriptions MUST be concise, factual, and human-readable. Avoid excessive detail that should live in documentation or commit messages.

**Maximum length**: ~30-40 lines for typical features
**Tone**: Direct, clear, professional - no marketing language or excessive enthusiasm

**Required sections**:

1. **Summary** (2-3 sentences): What does this do and why?
2. **The Problem** (2-4 lines): What issue does this solve?
3. **The Solution** (2-4 lines): How does it solve it?
4. **Key Features** (3-5 bullet points): Most important capabilities
5. **Example** (optional): Brief code example if it clarifies usage
6. **Link to docs** (if comprehensive guide exists)

**PROHIBITED content**:

- Extensive test coverage tables (this belongs in CI reports)
- Detailed file change lists (GitHub shows this automatically)
- Quality metrics and linting results (CI handles this)
- Commit-by-commit breakdown (git history shows this)
- Implementation details (belongs in code comments/docs)
- Excessive formatting (tables, sections, subsections)
- Marketing language or hype

**Example of GOOD PR description**:

```markdown
## Summary

Adds hybrid versioning for migrations: timestamps in development (no conflicts),
sequential in production (deterministic ordering). Includes an automated
`sqlspec fix` command to convert between formats.

Closes #116

## The Problem

- Sequential migrations (0001, 0002): merge conflicts when multiple devs create migrations
- Timestamp migrations (20251011120000): no conflicts, but ordering depends on creation time

## The Solution

Use timestamps during development, convert to sequential before merging:

    $ sqlspec create-migration -m "add users"
    Created: 20251011120000_add_users.sql

    $ sqlspec fix --yes
    ✓ Converted to 0003_add_users.sql

## Key Features

- Automated conversion via `sqlspec fix` command
- Updates database tracking to prevent errors
- Idempotent - safe to re-run after pulling changes
- Stable checksums through conversions

See [docs/guides/migrations/hybrid-versioning.md](docs/guides/migrations/hybrid-versioning.md)
for full documentation.
```

**Example of BAD PR description**:

```markdown
## Summary
[800+ lines of excessive detail including test counts, file changes,
quality metrics, implementation details, commit lists, etc.]
```

**CI Integration examples** - Keep to 5-10 lines maximum:

```yaml
# GitHub Actions example
- run: sqlspec fix --yes
- run: git add migrations/ && git commit && git push
```

**When to include more detail**:

- Breaking changes warrant a "Breaking Changes" section
- Complex architectural changes may need a "Design Decisions" section
- Security fixes may need a "Security Impact" section

Keep it focused: the PR description should help reviewers understand WHAT and WHY quickly.
Implementation details belong in code, commits, and documentation.

## Common Development Commands

### Building and Installation

- **Install project with development dependencies**: `make install` or `uv sync --all-extras --dev`
- **Install with mypyc compilation**: `make install-compiled` or `HATCH_BUILD_HOOKS_ENABLE=1 uv pip install -e . --extra mypyc`
- **Build package**: `make build` or `uv build`
- **Build with mypyc compilation**: `make build-performance` or `HATCH_BUILD_HOOKS_ENABLE=1 uv build --extra mypyc`

### Testing

- **Run tests**: `make test` or `uv run pytest -n 2 --dist=loadgroup tests`
- **Run single test file**: `uv run pytest tests/path/to/test_file.py`
- **Run single test**: `uv run pytest tests/path/to/test_file.py::test_function_name`
- **Run tests with coverage**: `make coverage` or `uv run pytest --cov -n 2 --dist=loadgroup`
- **Run integration tests for specific database**: `uv run pytest tests/integration/test_adapters/test_<adapter>/ -v`

### Linting and Type Checking

- **Run all linting checks**: `make lint`
- **Run pre-commit hooks**: `make pre-commit` or `uv run pre-commit run --all-files`
- **Auto-fix code issues**: `make fix` or `uv run ruff check --fix --unsafe-fixes`
- **Run mypy**: `make mypy` or `uv run dmypy run`
- **Run pyright**: `make pyright` or `uv run pyright`

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

1. Create configuration instance (e.g., `SqliteConfig(database=":memory:")`)
2. Register with SQLSpec: `sql.add_config(config)`
3. Get session via context manager: `with sql.provide_session(config) as session:`
4. Execute queries through session: `session.execute()`, `session.select_one()`, etc.
5. Results automatically mapped to specified types

### Testing Strategy

- **Unit Tests** (`tests/unit/`): Test individual components in isolation
- **Integration Tests** (`tests/integration/`): Test actual database connections
- Tests use `pytest-databases` for containerized database instances
- Marker system for database-specific tests: `@pytest.mark.postgres`, `@pytest.mark.duckdb`, etc.
- **MANDATORY**: Use function-based pytest tests, NOT class-based tests
- **PROHIBITED**: Class-based test organization (TestSomething classes)

### Test Isolation Patterns for Pooled Connections

When writing integration tests for framework extensions or pooled database connections, ensure proper test isolation to prevent parallel test execution failures.

**Problem**: Using `:memory:` databases with connection pooling causes test failures when pytest-xdist runs tests in parallel. The shared in-memory database persists tables across tests, causing "table already exists" errors.

**Root Cause**: AioSQLite config auto-converts `:memory:` to `file::memory:?cache=shared` for pooling support, which creates a single shared database instance across all connections in the pool.

**Solution**: Use unique temporary database files per test instead of `:memory:`:

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

### Type Annotation Standards (STRICT ENFORCEMENT)

- **PROHIBITED**: `from __future__ import annotations`
- **MANDATORY**: Stringified type hints for non-builtin types: `"SQLConfig"`
- **MANDATORY**: `T | None` and `A | B` for Python 3.10+ (PEP 604 pipe syntax)
- **Built-in generics**: Stringified: `"list[str]"`, `"dict[str, int]"`
- **`__all__` definition**: Use tuples: `__all__ = ("MyClass", "my_function")`
- **MANDATORY**: never leave inline comments in the code. Comments must be in a docstring if they are important enough to save
- **MANDATORY**: Only use nested imports when it's required to prevent import errors

### Import Standards (STRICT ENFORCEMENT)

- NO nested imports unless preventing circular imports
- ALL imports at module level
- Absolute imports only - no relative imports
- Organization: standard library → third-party → first-party
- Third-party nested ONLY for optional dependencies

```python
# BAD - Unnecessary nested import
def process_data(self):
    from sqlspec.protocols import DataProtocol  # NO!

# GOOD - All imports at top
from sqlspec.protocols import DataProtocol

def process_data(self): ...

# ACCEPTABLE - Only for circular import prevention
if TYPE_CHECKING:
    from sqlspec.statement.sql import SQL
```

### Clean Code Principles (MANDATORY)

**Code Clarity**:

- Write self-documenting code - no comments needed
- Extract complex conditions to well-named variables/methods
- Early returns over nested if blocks
- Guard clauses for edge cases at function start

**Variable and Function Naming**:

- Descriptive names explaining purpose, not type
- No abbreviations unless widely understood
- Boolean variables as questions: `is_valid`, `has_data`
- Functions as verbs describing action

**Function Length**:

- Maximum 75 lines per function (including docstring)
- Preferred 30-50 lines for most functions
- Split longer functions into smaller helpers

**Anti-Patterns to Avoid (PROHIBITED)**:

```python
# BAD - Defensive programming
if hasattr(obj, 'method') and obj.method:
    result = obj.method()

# GOOD - Type guard based
from sqlspec.utils.type_guards import supports_where
if supports_where(obj):
    result = obj.where("condition")
```

### Performance Patterns (MANDATORY)

**PERF401 - List Operations**:

```python
# BAD
result = []
for item in items:
    if condition(item):
        result.append(transform(item))

# GOOD
result = [transform(item) for item in items if condition(item)]
```

**PLR2004 - Magic Value Rule**:

```python
# BAD
if len(parts) != 2:
    raise ValueError("Invalid format")

# GOOD
URI_PARTS_MIN_COUNT = 2
if len(parts) != URI_PARTS_MIN_COUNT:
    raise ValueError("Invalid format")
```

**TRY301 - Abstract Raises**:

```python
# BAD
def process(self, data):
    if not data:
        msg = "Data is required"
        raise ValueError(msg)

# GOOD
def process(self, data):
    if not data:
        self._raise_data_required()

def _raise_data_required(self):
    msg = "Data is required"
    raise ValueError(msg)
```

### Error Handling Standards

- Custom exceptions in `sqlspec.exceptions.py` inherit from `SQLSpecError`
- Use `wrap_exceptions` context manager in adapter layer
- Let exceptions propagate - avoid needless catch-re-raise
- Abstract raise statements to inner functions in try blocks
- Remove unnecessary try/catch blocks that will be caught higher in the execution

#### Two-Tier Error Handling Pattern

When processing user input that may be incomplete or malformed, use a two-tier approach:

**Tier 1: Graceful Skip (Expected Incomplete Input)**

- Condition: Input lacks required markers but is otherwise valid
- Action: Return empty result (empty dict, None, etc.)
- Log level: DEBUG
- Example: SQL file without `-- name:` markers

**Tier 2: Hard Error (Malformed Input)**

- Condition: Input has required markers but is malformed
- Action: Raise specific exception with clear message
- Log level: ERROR (via exception handler)
- Example: Duplicate statement names, empty names

**Implementation Pattern:**

## Recent edit: Configuration examples (2025-11-04)

- Updated docs/examples/usage examples: consolidated example filenames to
  docs/examples/usage/test_configuration_*.py and ensured the documentation
  references match the renamed examples.
- Added an explicit :lines: range and a :dedent: directive to the
  literalinclude for test_configuration_23.py so Sphinx renders the snippet
  with correct indentation.
- Built the Sphinx documentation (make docs) and verified HTML output was
  generated successfully. Two minor warnings were reported (dedent and a
  missing stylesheet copy) but they did not prevent the build.
- Updated project TODOs to reflect completed steps.

This summary documents the small documentation and example maintenance
performed on the configuration usage guide and can be expanded into a
longer changelog entry if desired.

```python
def parse_user_input(content: str, source: str) -> "dict[str, Result]":
    """Parse user input with two-tier error handling.

    Files without required markers are gracefully skipped by returning
    an empty dictionary. The caller is responsible for handling empty results.

    Args:
        content: Raw input content to parse.
        source: Source identifier for error reporting.

    Returns:
        Dictionary of parsed results. Empty dict if no required markers found.

    Raises:
        ParseError: If required markers are present but malformed.
    """
    # Check for required markers
    markers = list(MARKER_PATTERN.finditer(content))
    if not markers:
        return {}  # Tier 1: Graceful skip

    results = {}
    for marker in markers:
        # Parse marker
        if malformed_marker(marker):
            raise ParseError(source, "Malformed marker")  # Tier 2: Hard error

        # Process and add result
        results[marker.name] = process(marker)

    return results


def caller_function(file_path: str) -> None:
    """Caller handles empty results from graceful skip."""
    results = parse_user_input(content, str(file_path))

    if not results:
        logger.debug(
            "Skipping file without required markers: %s",
            str(file_path),
            extra={"file_path": str(file_path), "correlation_id": get_correlation_id()},
        )
        return  # Early return - don't process empty results

    # Process non-empty results
    for name, result in results.items():
        store_result(name, result)
```

**Key Benefits:**

1. **Clear Intent**: Distinguishes "no markers" from "bad markers"
2. **User-Friendly**: Doesn't break batch processing for missing markers
3. **Debuggable**: DEBUG logs show what was skipped
4. **Fail-Fast**: Malformed input still raises clear errors

**When to Use:**

- File/directory loading with optional markers
- Batch processing where some inputs may lack required data
- Migration systems processing mixed file types
- Configuration loading with optional sections

**When NOT to Use:**

- Required input validation (use strict validation)
- Single-file processing where empty result is an error
- API endpoints expecting specific data format

**Real-World Example:**

See `sqlspec/loader.py:_parse_sql_content()` for the reference implementation in SQL file loading.

### Logging Standards

- Use `logging` module, NEVER `print()`
- NO f-strings in log messages - use lazy formatting
- Provide meaningful context in all log messages

#### DEBUG Level for Expected Skip Behavior

Use DEBUG level (not INFO or WARNING) when gracefully skipping expected conditions:

```python
# GOOD - DEBUG for expected skip
if not statements:
    logger.debug(
        "Skipping SQL file without named statements: %s",
        path_str,
        extra={
            "file_path": path_str,
            "correlation_id": CorrelationContext.get(),
        },
    )
    return

# BAD - INFO suggests important information
logger.info("Skipping file %s", path_str)  # Too high level

# BAD - WARNING suggests potential problem
logger.warning("No statements found in %s", path_str)  # Misleading
```

**DEBUG vs INFO vs WARNING Guidelines:**

- **DEBUG**: Expected behavior that aids troubleshooting
    - Files gracefully skipped during batch processing
    - Optional features not enabled (dependencies missing)
    - Cache hits/misses
    - Internal state transitions

- **INFO**: Significant events during normal operation
    - Connection pool created
    - Migration applied successfully
    - Background task started

- **WARNING**: Unexpected but recoverable conditions
    - Retrying after transient failure
    - Falling back to alternative implementation
    - Configuration using deprecated options

**Context Requirements:**

Always include `extra` dict with:

- Primary identifier (file_path, query_name, etc.)
- Correlation ID via `CorrelationContext.get()`
- Additional relevant context (size, duration, etc.)

### Documentation Standards

**Docstrings (Google Style - MANDATORY)**:

- All public modules, classes, functions need docstrings
- Include `Args:`, `Returns:`, `Yields`, `Raises:` sections with types
- Don't document return if `None`
- Sphinx-compatible format
- Focus on WHY not WHAT

**Documenting Graceful Degradation:**

When functions implement graceful skip behavior, document it clearly in the docstring:

```python
def parse_content(content: str, source: str) -> "dict[str, Result]":
    """Parse content and extract structured data.

    Files without required markers are gracefully skipped by returning
    an empty dictionary. The caller is responsible for handling empty results
    appropriately.

    Args:
        content: Raw content to parse.
        source: Source identifier for error reporting.

    Returns:
        Dictionary mapping names to results.
        Empty dict if no required markers found in the content.

    Raises:
        ParseError: If required markers are present but malformed
                   (duplicate names, empty names, invalid content).
    """
```

**Key elements:**

1. **First paragraph after summary**: Explain graceful skip behavior
2. **Caller responsibility**: Note that caller must handle empty results
3. **Returns section**: Explicitly document empty dict case
4. **Raises section**: Only document hard errors, not graceful skips

**Example from codebase:**

See `sqlspec/loader.py:_parse_sql_content()` for reference implementation.

**Project Documentation**:

- Update `docs/` for new features and API changes
- Build locally: `make docs` before submission
- Use reStructuredText (.rst) and Markdown (.md via MyST)

## Type Handler Pattern

### When to Use Type Handlers vs Type Converters

**Type Converters** (`type_converter.py`):

- Use for post-query data transformation (output conversion)
- Use for pre-query parameter transformation (input conversion)
- Examples: JSON detection, datetime formatting, LOB processing
- Located in adapter's `type_converter.py` module

**Type Handlers** (`_type_handlers.py` or `_<feature>_handlers.py`):

- Use for database driver-level type registration
- Use for optional features requiring external dependencies
- Examples: pgvector support, NumPy array conversion
- Located in adapter's `_<feature>_handlers.py` module (e.g., `_numpy_handlers.py`)

### Structure of Type Handler Modules

Type handler modules should follow this pattern:

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
    """Convert Python type to database type.

    Args:
        value: Python value to convert.

    Returns:
        Database-compatible value.

    Raises:
        ImportError: If optional dependency not installed.
        TypeError: If value type not supported.
    """
    if not OPTIONAL_PACKAGE_INSTALLED:
        msg = "Optional package not installed"
        raise ImportError(msg)
    # Conversion logic here
    return converted_value


def converter_out(value: Any) -> Any:
    """Convert database type to Python type.

    Args:
        value: Database value to convert.

    Returns:
        Python value, or original if package not installed.
    """
    if not OPTIONAL_PACKAGE_INSTALLED:
        return value
    # Conversion logic here
    return converted_value


def _input_type_handler(cursor: "Connection", value: Any, arraysize: int) -> Any:
    """Database input type handler.

    Args:
        cursor: Database cursor.
        value: Value being inserted.
        arraysize: Array size for cursor variable.

    Returns:
        Cursor variable with converter, or None.
    """
    if not OPTIONAL_PACKAGE_INSTALLED:
        return None
    # Type detection and registration logic
    return cursor_var


def _output_type_handler(cursor: "Connection", metadata: Any) -> Any:
    """Database output type handler.

    Args:
        cursor: Database cursor.
        metadata: Column metadata.

    Returns:
        Cursor variable with converter, or None.
    """
    if not OPTIONAL_PACKAGE_INSTALLED:
        return None
    # Type detection and registration logic
    return cursor_var


def register_handlers(connection: "Connection") -> None:
    """Register type handlers on database connection.

    Enables automatic conversion for [feature].

    Args:
        connection: Database connection.
    """
    if not OPTIONAL_PACKAGE_INSTALLED:
        logger.debug("Optional package not installed - skipping type handlers")
        return

    connection.inputtypehandler = _input_type_handler
    connection.outputtypehandler = _output_type_handler
    logger.debug("Registered type handlers for [feature]")
```

### Handler Chaining Pattern (Multiple Type Handlers)

When multiple type handlers need to coexist (e.g., NumPy vectors + UUID binary), use handler chaining to avoid conflicts. Oracle's python-oracledb allows only ONE inputtypehandler and ONE outputtypehandler per connection.

**Problem**: Directly assigning a new handler overwrites any existing handler.

**Solution**: Check for existing handlers and chain them together:

```python
def register_handlers(connection: "Connection") -> None:
    """Register type handlers with chaining support.

    Chains to existing type handlers to avoid conflicts with other features.

    Args:
        connection: Database connection.
    """
    existing_input = getattr(connection, "inputtypehandler", None)
    existing_output = getattr(connection, "outputtypehandler", None)

    def combined_input_handler(cursor: "Cursor", value: Any, arraysize: int) -> Any:
        # Try new handler first
        result = _input_type_handler(cursor, value, arraysize)
        if result is not None:
            return result
        # Chain to existing handler
        if existing_input is not None:
            return existing_input(cursor, value, arraysize)
        return None

    def combined_output_handler(cursor: "Cursor", metadata: Any) -> Any:
        # Try new handler first
        result = _output_type_handler(cursor, metadata)
        if result is not None:
            return result
        # Chain to existing handler
        if existing_output is not None:
            return existing_output(cursor, metadata)
        return None

    connection.inputtypehandler = combined_input_handler
    connection.outputtypehandler = combined_output_handler
    logger.debug("Registered type handlers with chaining support")
```

**Registration Order Matters**:

```python
async def _init_connection(self, connection):
    """Initialize connection with multiple type handlers."""
    # Register handlers in order of priority
    if self.driver_features.get("enable_numpy_vectors", False):
        from ._numpy_handlers import register_handlers
        register_handlers(connection)  # First handler

    if self.driver_features.get("enable_uuid_binary", False):
        from ._uuid_handlers import register_handlers
        register_handlers(connection)  # Chains to NumPy handler
```

**Key Principles**:

1. **Use getattr() to check for existing handlers** - This is acceptable duck-typing (not defensive programming)
2. **Chain handlers in combined functions** - New handler checks first, then delegates to existing
3. **Return None if no match** - Signals to continue to next handler or default behavior
4. **Order matters** - Last registered handler gets first chance to process
5. **Log chaining** - Include "with chaining support" in debug message

**Example Usage**:

```python
# Both features work together via chaining
config = OracleAsyncConfig(
    pool_config={"dsn": "oracle://..."},
    driver_features={
        "enable_numpy_vectors": True,  # NumPy vectors
        "enable_uuid_binary": True      # UUID binary (chains to NumPy)
    }
)

# Insert both types in same transaction
await session.execute(
    "INSERT INTO ml_data (id, model_id, embedding) VALUES (:1, :2, :3)",
    (1, uuid.uuid4(), np.random.rand(768).astype(np.float32))
)
```

### Oracle Metadata Tuple Unpacking Pattern

Oracle's cursor.description returns a 7-element tuple for each column. Always unpack explicitly to access internal_size:

```python
def _output_type_handler(cursor: "Cursor", metadata: Any) -> Any:
    """Oracle output type handler.

    Args:
        cursor: Oracle cursor.
        metadata: Column metadata tuple (name, type_code, display_size,
                  internal_size, precision, scale, null_ok).
    """
    import oracledb

    # Unpack tuple explicitly - metadata[3] is internal_size
    _name, type_code, _display_size, internal_size, _precision, _scale, _null_ok = metadata

    if type_code is oracledb.DB_TYPE_RAW and internal_size == 16:
        return cursor.var(type_code, arraysize=cursor.arraysize, outconverter=converter_out)
    return None
```

**Why explicit unpacking**:

- **Correctness**: Oracle metadata is a tuple, not an object with attributes
- **No .size attribute**: Attempting `metadata.size` raises AttributeError
- **Clear intent**: Unpacking documents the 7-element structure
- **Prevents errors**: Catches unexpected metadata format changes

**Common mistake**:

```python
# WRONG - metadata has no .size attribute
if type_code is oracledb.DB_TYPE_RAW and metadata.size == 16:
    ...
```

**Correct approach**:

```python
# RIGHT - unpack tuple to access internal_size
_name, type_code, _display_size, internal_size, _precision, _scale, _null_ok = metadata
if type_code is oracledb.DB_TYPE_RAW and internal_size == 16:
    ...
```

### Configuring driver_features with Auto-Detection

In adapter's `config.py`, implement auto-detection:

```python
from sqlspec._typing import OPTIONAL_PACKAGE_INSTALLED

class DatabaseConfig(AsyncDatabaseConfig):
    def __init__(
        self,
        *,
        driver_features: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        # Auto-detect optional features if not explicitly configured
        if driver_features is None:
            driver_features = {}
        if "enable_feature" not in driver_features:
            driver_features["enable_feature"] = OPTIONAL_PACKAGE_INSTALLED

        super().__init__(driver_features=driver_features, **kwargs)

    async def _create_pool(self):
        """Create pool with optional session callback."""
        config = dict(self.pool_config)

        if self.driver_features.get("enable_feature", False):
            config["session_callback"] = self._init_connection

        return await create_pool(**config)

    async def _init_connection(self, connection):
        """Initialize connection with optional type handlers."""
        if self.driver_features.get("enable_feature", False):
            from ._feature_handlers import register_handlers
            register_handlers(connection)
```

### Pattern for Graceful Optional Dependency Handling

**In `_typing.py`** - Define constants:

```python
try:
    import optional_package
    OPTIONAL_PACKAGE_INSTALLED = True
except ImportError:
    OPTIONAL_PACKAGE_INSTALLED = False
```

**In type handler module** - Check before use:

```python
from sqlspec._typing import OPTIONAL_PACKAGE_INSTALLED

def converter(value):
    if not OPTIONAL_PACKAGE_INSTALLED:
        return value  # Graceful degradation
    import optional_package
    return optional_package.convert(value)
```

**In config** - Auto-enable when available:

```python
if "enable_feature" not in driver_features:
    driver_features["enable_feature"] = OPTIONAL_PACKAGE_INSTALLED
```

### Error Handling in Type Handlers

When implementing type handlers that register optional database extensions, distinguish between expected and unexpected failures:

**Expected Failures** (graceful degradation) → **DEBUG level**:

- Database extension not enabled (e.g., `CREATE EXTENSION vector` not run)
- Optional Python package not installed
- Database version doesn't support the feature

**Unexpected Failures** (need investigation) → **WARNING or ERROR level**:

- Network errors during registration
- Permission issues
- Invalid configuration
- Unknown exceptions

**Pattern for Extension Registration**:

```python
async def register_optional_extension(connection):
    """Register optional database extension support.

    Gracefully handles missing extensions with DEBUG logging.
    """
    if not OPTIONAL_PACKAGE_INSTALLED:
        logger.debug("Optional package not installed - skipping extension support")
        return

    try:
        import optional_package
        await optional_package.register(connection)
        logger.debug("Registered optional extension support")
    except SpecificExpectedError as error:
        message = str(error).lower()
        if "extension not found" in message or "type not found" in message:
            logger.debug("Skipping extension registration - extension not enabled in database")
            return
        logger.warning("Unexpected error during extension registration: %s", error)
    except Exception:
        logger.exception("Failed to register optional extension")
```

**Real-World Example - PostgreSQL pgvector**:

Different PostgreSQL drivers raise different error messages for the same condition (pgvector extension not enabled):

```python
# AsyncPG - raises ValueError("unknown type: public.vector")
async def register_pgvector_support(connection):
    if not PGVECTOR_INSTALLED:
        logger.debug("pgvector not installed - skipping vector type support")
        return

    try:
        import pgvector.asyncpg
        await pgvector.asyncpg.register_vector(connection)
        logger.debug("Registered pgvector support on asyncpg connection")
    except ValueError as exc:
        message = str(exc).lower()
        if "unknown type" in message and "vector" in message:
            logger.debug("Skipping pgvector registration - extension not enabled in database")
            return
        logger.warning("Unexpected error during pgvector registration: %s", exc)
    except Exception:
        logger.exception("Failed to register pgvector support")

# Psycopg - raises ValueError("vector type not found in the database")
def register_pgvector_sync(connection):
    if not PGVECTOR_INSTALLED:
        logger.debug("pgvector not installed - skipping vector type handlers")
        return

    try:
        import pgvector.psycopg
        pgvector.psycopg.register_vector(connection)
        logger.debug("Registered pgvector type handlers on psycopg sync connection")
    except ValueError as error:
        message = str(error).lower()
        if "vector type not found" in message:
            logger.debug("Skipping pgvector registration - extension not enabled in database")
            return
        logger.warning("Unexpected error during pgvector registration: %s", error)
    except Exception:
        logger.exception("Failed to register pgvector for psycopg sync")
```

**Key Principles**:

1. Check for **specific error messages** to identify expected vs unexpected failures
2. Use **DEBUG** for expected graceful degradation (extension not available)
3. Use **WARNING** for unexpected issues during optional feature setup
4. Use **ERROR/exception** for critical failures
5. Provide **clear, actionable log messages**
6. Never break application flow on expected failures
7. Match error message checks to **actual driver behavior** (different drivers raise different messages)

### Examples from Existing Adapters

**Oracle NumPy VECTOR Support** (`oracledb/_numpy_handlers.py`):

- Converts NumPy arrays ↔ Oracle VECTOR types
- Auto-enabled when numpy installed
- Controlled via `driver_features["enable_numpy_vectors"]`
- Supports float32, float64, int8, uint8 dtypes

**PostgreSQL pgvector Support** (`asyncpg/config.py`, `psycopg/config.py`):

- Registers pgvector extension support
- Auto-enabled when pgvector installed
- Always-on (no driver_features toggle needed)
- Handles graceful fallback if registration fails

### Testing Requirements for Type Handlers

**Unit Tests** - Test handler logic in isolation:

- Test converters with mock values
- Test graceful degradation when package not installed
- Test error conditions (unsupported types, etc.)

**Integration Tests** - Test with real database:

- Test round-trip conversion (insert → retrieve)
- Test with actual optional package installed
- Test behavior when package not installed
- Mark tests with `@pytest.mark.skipif(not INSTALLED, reason="...")`

Example:

```python
import pytest
from sqlspec._typing import NUMPY_INSTALLED

@pytest.mark.skipif(not NUMPY_INSTALLED, reason="NumPy not installed")
async def test_numpy_vector_roundtrip(oracle_session):
    import numpy as np

    vector = np.random.rand(768).astype(np.float32)
    await oracle_session.execute(
        "INSERT INTO embeddings VALUES (:1, :2)",
        (1, vector)
    )
    result = await oracle_session.select_one(
        "SELECT * FROM embeddings WHERE id = :1",
        (1,)
    )
    assert isinstance(result["embedding"], np.ndarray)
    assert np.allclose(result["embedding"], vector)
```

### Important Notes

- Always use absolute imports within the codebase
- Follow existing parameter style patterns when adding new adapters
- Use type hints extensively - the library is designed for type safety
- Test against actual databases using the docker infrastructure
- The SQL builder API is experimental and will change significantly

## LOB (Large Object) Hydration Pattern

### Overview

Some database drivers (Oracle, PostgreSQL with large objects) return handle objects for large data types that must be explicitly read before use. SQLSpec provides automatic hydration to ensure typed schemas receive concrete Python values.

### When to Use LOB Hydration

Use LOB hydration helpers when:

- Database driver returns handle objects (LOB, AsyncLOB) instead of concrete values
- Typed schemas (msgspec, Pydantic) expect concrete types (str, bytes, dict)
- Users would otherwise need manual workarounds (`DBMS_LOB.SUBSTR`)

### Implementation Pattern

**Step 1: Create Hydration Helpers**

Add helpers in the adapter's `driver.py` to read LOB handles:

```python
def _coerce_sync_row_values(row: "tuple[Any, ...]") -> "list[Any]":
    """Coerce LOB handles to concrete values for synchronous execution.

    Processes each value in the row, reading LOB objects and applying
    type detection for JSON values stored in CLOBs.

    Args:
        row: Tuple of column values from database fetch.

    Returns:
        List of coerced values with LOBs read to strings/bytes.
    """
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


async def _coerce_async_row_values(row: "tuple[Any, ...]") -> "list[Any]":
    """Coerce LOB handles to concrete values for asynchronous execution.

    Processes each value in the row, reading LOB objects asynchronously
    and applying type detection for JSON values stored in CLOBs.

    Args:
        row: Tuple of column values from database fetch.

    Returns:
        List of coerced values with LOBs read to strings/bytes.
    """
    coerced_values: list[Any] = []
    for value in row:
        if hasattr(value, "read"):
            try:
                processed_value = await _type_converter.process_lob(value)
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

**Step 2: Integrate into Execution Path**

Call hydration helpers before dict construction in `_execute_statement`:

```python
# Sync driver
async for row in cursor:
    coerced = _coerce_sync_row_values(row)
    rows.append(dict(zip(columns, coerced)))

# Async driver
async for row in cursor:
    coerced = await _coerce_async_row_values(row)
    rows.append(dict(zip(columns, coerced)))
```

### Key Design Principles

**Duck-Typing for LOB Detection**:

- Use `hasattr(value, "read")` to detect LOB handles
- This is appropriate duck-typing, NOT defensive programming
- Avoids importing driver-specific types

**Error Handling**:

- Catch exceptions during LOB reading
- Fall back to original value on error
- Prevents breaking queries with unexpected handle types

**Type Detection After Reading**:

- Apply `convert_if_detected()` to string results
- Enables JSON detection for JSON-in-CLOB scenarios
- Preserves binary data (bytes) without conversion

**Separation of Concerns**:

- Hydration happens at result-fetching layer
- Type conversion handled by existing type converter
- Schema conversion remains unchanged

### Testing Requirements

**Integration Tests** - Test with real database and typed schemas:

```python
import msgspec

class Article(msgspec.Struct):
    id: int
    content: str  # CLOB column

async def test_clob_msgspec_hydration(session):
    large_text = "x" * 5000  # >4KB to ensure CLOB
    await session.execute(
        "INSERT INTO articles (id, content) VALUES (:1, :2)",
        (1, large_text)
    )

    result = await session.execute(
        "SELECT id, content FROM articles WHERE id = :1",
        (1,)
    )

    article = result.get_first(schema_type=Article)
    assert isinstance(article.content, str)
    assert article.content == large_text
```

**Test Coverage Areas**:

1. Basic CLOB/text LOB hydration to string
2. BLOB/binary LOB hydration to bytes
3. JSON detection in CLOB content
4. Mixed CLOB and regular columns
5. Multiple LOB columns in one row
6. NULL/empty LOB handling
7. Both sync and async drivers

### Performance Considerations

**Memory Usage**:

- LOBs are fully materialized into memory
- Document limitations for very large LOBs (>100MB)
- Consider pagination for multi-GB LOBs

**Sync vs Async**:

- Sync uses `.read()` directly
- Async uses `await` for LOB reading
- Both approaches have equivalent performance

### Examples from Existing Adapters

**Oracle CLOB Hydration** (`oracledb/driver.py`):

- Automatically reads CLOB handles to strings
- Preserves BLOB as bytes
- Enables JSON detection for JSON-in-CLOB
- No configuration required - always enabled
- Eliminates need for `DBMS_LOB.SUBSTR` workaround

### Documentation Requirements

When implementing LOB hydration:

1. **Update adapter guide** - Document new behavior and before/after comparison
2. **Add examples** - Show typed schema usage without manual workarounds
3. **Note performance** - Mention memory considerations for large LOBs
4. **Show JSON detection** - Demonstrate automatic JSON parsing in LOBs

Example documentation structure:

```markdown
## CLOB/BLOB Handling

### Automatic CLOB Hydration

CLOB values are automatically read and converted to Python strings:

[Example with msgspec]

### JSON Detection in CLOBs

[Example showing JSON parsing]

### BLOB Handling (Binary Data)

BLOB columns remain as bytes:

[Example with bytes]

### Before and After

**Before (manual workaround):**
[SQL with DBMS_LOB.SUBSTR]

**After (automatic):**
[Clean SQL without workarounds]

### Performance Considerations
- Memory usage notes
- When to use pagination
```

## Apache Arrow Integration Pattern

### Overview

SQLSpec implements Apache Arrow support through a dual-path architecture: native Arrow for high-performance adapters (ADBC, DuckDB, BigQuery) and conversion-based Arrow for all other adapters. This pattern enables universal Arrow compatibility while optimizing for zero-copy performance where available.

### When to Implement Arrow Support

**Implement select_to_arrow() when**:

- Adapter supports high-throughput analytical queries
- Users need integration with pandas, Polars, or data science tools
- Data interchange with Arrow ecosystem (Parquet, Spark, etc.) is common
- Large result sets are typical for the adapter's use cases

**Use native Arrow path when**:

- Database driver provides direct Arrow output (e.g., ADBC `fetch_arrow_table()`)
- Zero-copy data transfer available
- Performance is critical for large datasets

**Use conversion path when**:

- Database driver returns dict/row results
- Native Arrow support not available
- Conversion overhead acceptable for use case

### Implementation Pattern

#### Native Arrow Path (Preferred)

Override `select_to_arrow()` in adapter's driver class:

```python
from sqlspec.core import create_arrow_result
from sqlspec.utils.module_loader import ensure_pyarrow

class NativeArrowDriver(AsyncDriverAdapterBase):
    """Driver with native Arrow support."""

    async def select_to_arrow(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        return_format: str = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any = None,
        **kwargs: Any,
    ) -> "Any":
        """Execute query using native Arrow support."""
        ensure_pyarrow()  # Validate PyArrow installed
        import pyarrow as pa

        sql_statement = self._prepare_statement(statement, parameters, statement_config)

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

**Key principles**:

- Use `ensure_pyarrow()` for dependency validation
- Validate `native_only` flag if adapter doesn't support native path
- Preserve Arrow schema metadata from database
- Support both "table" and "batch" return formats
- Return `ArrowResult` via `create_arrow_result()` helper

#### Conversion Arrow Path (Fallback)

Base driver classes provide default implementation via dict conversion:

```python
# Implemented in _async.py and _sync.py
async def select_to_arrow(self, statement, /, *parameters, **kwargs):
    """Base implementation using dict → Arrow conversion."""
    ensure_pyarrow()

    # Execute using standard path
    result = await self.execute(statement, *parameters, **kwargs)

    # Convert to Arrow
    from sqlspec.utils.arrow_helpers import convert_dict_to_arrow
    arrow_data = convert_dict_to_arrow(
        result.data,
        return_format=kwargs.get("return_format", "table")
    )

    return create_arrow_result(arrow_data, rows_affected=len(result.data))
```

**When to use**:

- Adapter has no native Arrow support
- Conversion overhead acceptable (&lt;20% for most cases)
- Provides Arrow compatibility for all adapters

### Type Mapping Best Practices

**Standard type mappings**:

```python
# PostgreSQL → Arrow
BIGINT → int64
DOUBLE PRECISION → float64
TEXT → utf8
BYTEA → binary
BOOLEAN → bool
TIMESTAMP → timestamp[us]
ARRAY → list<T>
JSONB → utf8 (JSON as text)
UUID → utf8 (converted to string)
```

**Complex type handling**:

- Arrays: Preserve as Arrow list types when possible
- JSON: Convert to utf8 (text) for portability
- UUIDs: Convert to strings for cross-platform compatibility
- Decimals: Use decimal128 for precision preservation
- Binary: Use binary or large_binary for LOBs

### ArrowResult Helper Pattern

Use `create_arrow_result()` for consistent result wrapping:

```python
from sqlspec.core import create_arrow_result

# Create ArrowResult from Arrow Table
result = create_arrow_result(arrow_table, rows_affected=arrow_table.num_rows)

# Create ArrowResult from RecordBatch
result = create_arrow_result(record_batch, rows_affected=record_batch.num_rows)
```

**Benefits**:

- Consistent API across all adapters
- Automatic to_pandas(), to_polars(), to_dict() support
- Iteration and length operations
- Metadata handling

### Testing Requirements

**Unit tests** for Arrow helpers:

- Test `convert_dict_to_arrow()` with various data types
- Test empty result handling
- Test NULL value preservation
- Test schema inference

**Integration tests** per adapter:

- Test native Arrow path (if supported)
- Test table and batch return formats
- Test pandas/Polars conversion
- Test large datasets (>10K rows)
- Test adapter-specific types
- Test parameter binding
- Test empty results

**Performance benchmarks** (for native paths):

- Measure native vs conversion speedup
- Validate zero-copy behavior
- Benchmark memory usage

### Example Implementations

**ADBC** (native, zero-copy):

```python
def select_to_arrow(self, statement, /, *parameters, **kwargs):
    """ADBC native Arrow - gold standard."""
    ensure_pyarrow()

    sql_statement = self._prepare_statement(statement, parameters)

    with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
        cursor.execute(str(sql_statement), sql_statement.parameters or ())
        arrow_table = cursor.fetch_arrow_table()  # Native fetch!

        if kwargs.get("return_format") == "batch":
            batches = arrow_table.to_batches()
            return create_arrow_result(batches[0] if batches else empty_batch)

        return create_arrow_result(arrow_table)
```

**DuckDB** (native, columnar):

```python
def select_to_arrow(self, statement, /, *parameters, **kwargs):
    """DuckDB native columnar Arrow."""
    ensure_pyarrow()

    sql_statement = self._prepare_statement(statement, parameters)

    with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
        cursor.execute(str(sql_statement), sql_statement.parameters or ())
        arrow_table = cursor.arrow()  # DuckDB's native method

        if kwargs.get("return_format") == "batch":
            batches = arrow_table.to_batches()
            return create_arrow_result(batches[0] if batches else empty_batch)

        return create_arrow_result(arrow_table)
```

**PostgreSQL adapters** (conversion, arrays preserved):

```python
# Base implementation in _async.py handles conversion
# PostgreSQL arrays automatically convert to Arrow list types
# No override needed unless optimizing specific types
```

### Documentation Requirements

When implementing Arrow support:

1. **Adapter guide** (`docs/guides/adapters/{adapter}.md`):
   - Add "Arrow Support" section
   - Specify native vs conversion path
   - Document type mapping table
   - Provide usage examples with pandas/Polars
   - Note performance characteristics

2. **Architecture guide** (`docs/guides/architecture/arrow-integration.md`):
   - Document overall Arrow strategy
   - Explain dual-path architecture
   - Provide performance benchmarks
   - List all supported adapters

3. **Examples** (`docs/examples/`):
   - Basic Arrow usage example
   - pandas integration example
   - Polars integration example
   - Export to Parquet example

### Common Pitfalls

**Avoid**:

- Returning raw Arrow objects instead of ArrowResult
- Missing `ensure_pyarrow()` dependency check
- Not supporting both "table" and "batch" return formats
- Ignoring `native_only` flag when adapter has no native support
- Breaking existing `execute()` behavior

**Do**:

- Use `create_arrow_result()` for consistent wrapping
- Support all standard type mappings
- Test with large datasets
- Document performance characteristics
- Preserve metadata when possible

### Performance Guidelines

**Native path targets**:

- Overhead &lt;5% vs direct driver Arrow fetch
- Zero-copy data transfer
- 5-10x faster than dict conversion for datasets >10K rows

**Conversion path targets**:

- Overhead &lt;20% vs standard `execute()` for datasets &lt;1K rows
- Overhead &lt;15% for datasets 1K-100K rows
- Overhead &lt;10% for datasets >100K rows (columnar efficiency)

**Memory targets**:

- Peak memory &lt;2x dict representation
- Arrow columnar format more efficient for large datasets

## driver_features Pattern

### Overview

The `driver_features` parameter provides a standardized way to configure adapter-specific features that:

1. **Require optional dependencies** (NumPy, pgvector, etc.)
2. **Control type conversion behavior** (UUID conversion, JSON serialization)
3. **Enable database-specific capabilities** (extensions, secrets, custom codecs)

Use `driver_features` when the feature:

- Depends on an optional external package
- Controls runtime type conversion behavior
- Enables database-specific functionality not part of standard SQL

**Do NOT use `driver_features` for**:

- Core connection parameters (use `pool_config` instead)
- Standard pool settings (min_size, max_size, etc.)
- Statement parsing configuration (use `statement_config` instead)

### TypedDict Requirements (MANDATORY)

Every adapter MUST define a TypedDict for its `driver_features`:

```python
class AdapterDriverFeatures(TypedDict):
    """Adapter driver feature flags.

    feature_name: Description of what this feature does.
        Requirements: List any dependencies or database versions.
        Defaults to X when Y condition is met.
        Behavior when enabled/disabled.
    """

    feature_name: NotRequired[bool]
    custom_param: NotRequired[Callable[[Any], str]]
```

**Why TypedDict is mandatory**:

- Provides IDE autocomplete and type checking
- Documents available features inline
- Prevents typos in feature names
- Makes API discoverable

### Naming Conventions (STRICT ENFORCEMENT)

**Boolean Feature Flags**:

- MUST use `enable_` prefix for boolean toggles
- Examples: `enable_numpy_vectors`, `enable_json_codecs`, `enable_pgvector`, `enable_custom_adapters`

**Function/Callable Parameters**:

- Use descriptive names without prefix
- Examples: `json_serializer`, `json_deserializer`, `session_callback`, `on_connection_create`

**Complex Configuration**:

- Use plural nouns for lists
- Examples: `extensions`, `secrets`

### Auto-Detection Pattern (RECOMMENDED)

For optional dependencies, auto-enable features when the dependency is available:

```python
from sqlspec.typing import NUMPY_INSTALLED, PGVECTOR_INSTALLED

class AdapterDriverFeatures(TypedDict):
    """Adapter driver feature flags."""

    enable_feature: NotRequired[bool]


class AdapterConfig(AsyncDatabaseConfig):
    def __init__(
        self,
        *,
        driver_features: "AdapterDriverFeatures | dict[str, Any] | None" = None,
        **kwargs: Any,
    ) -> None:
        # Process driver_features with auto-detection
        processed_features = dict(driver_features) if driver_features else {}

        # Auto-detect optional feature if not explicitly configured
        if "enable_feature" not in processed_features:
            processed_features["enable_feature"] = OPTIONAL_PACKAGE_INSTALLED

        super().__init__(driver_features=processed_features, **kwargs)
```

**Why auto-detection**:

- Best user experience - features "just work" when dependencies installed
- Explicit opt-out available (set to `False` to disable)
- No surprises - feature availability matches dependency installation

### Default Value Guidelines

**Default to `True` when**:

- The dependency is in the standard library (uuid, json)
- The feature improves Python type handling (UUID conversion, JSON detection)
- No performance cost when feature is unused
- Feature is backward-compatible

**Default to auto-detected when**:

- Feature requires optional dependency (NumPy, pgvector)
- Feature is widely desired but not universally available

**Default to `False` when**:

- Feature has performance implications
- Feature changes database behavior in non-obvious ways
- Feature is experimental or unstable

### Implementation Examples

#### Gold Standard: Oracle NumPy VECTOR Support

**Auto-detection with type handlers**:

```python
from sqlspec.typing import NUMPY_INSTALLED

class OracleDriverFeatures(TypedDict):
    """Oracle driver feature flags.

    enable_numpy_vectors: Enable automatic NumPy array ↔ Oracle VECTOR conversion.
        Requires NumPy and Oracle Database 23ai or higher with VECTOR data type support.
        Defaults to True when NumPy is installed.
        Provides automatic bidirectional conversion between NumPy ndarrays and Oracle VECTOR columns.
        Supports float32, float64, int8, and uint8 dtypes.
    """

    enable_numpy_vectors: NotRequired[bool]


class OracleAsyncConfig(AsyncDatabaseConfig):
    def __init__(self, *, driver_features=None, **kwargs):
        processed_features = dict(driver_features) if driver_features else {}
        if "enable_numpy_vectors" not in processed_features:
            processed_features["enable_numpy_vectors"] = NUMPY_INSTALLED

        super().__init__(driver_features=processed_features, **kwargs)

    async def _create_pool(self):
        config = dict(self.pool_config)

        if self.driver_features.get("enable_numpy_vectors", False):
            config["session_callback"] = self._init_connection

        return await oracledb.create_pool_async(**config)

    async def _init_connection(self, connection):
        if self.driver_features.get("enable_numpy_vectors", False):
            from ._numpy_handlers import register_handlers
            register_handlers(connection)
```

**Why this is gold standard**:

- TypedDict with comprehensive documentation
- Auto-detection using `NUMPY_INSTALLED`
- Consistent `enable_` prefix
- Graceful degradation in type handlers
- Clear opt-out path (set to `False`)

#### Multiple Features: AsyncPG (JSON + pgvector)

```python
from sqlspec.typing import PGVECTOR_INSTALLED

class AsyncpgDriverFeatures(TypedDict):
    """AsyncPG driver feature flags."""

    json_serializer: NotRequired[Callable[[Any], str]]
    json_deserializer: NotRequired[Callable[[str], Any]]
    enable_json_codecs: NotRequired[bool]
    enable_pgvector: NotRequired[bool]


class AsyncpgConfig(AsyncDatabaseConfig):
    def __init__(self, *, driver_features=None, **kwargs):
        processed_features = dict(driver_features) if driver_features else {}

        # Auto-detect pgvector
        if "enable_pgvector" not in processed_features:
            processed_features["enable_pgvector"] = PGVECTOR_INSTALLED

        # Default JSON codecs to enabled
        if "enable_json_codecs" not in processed_features:
            processed_features["enable_json_codecs"] = True

        # Default serializers
        if "json_serializer" not in processed_features:
            processed_features["json_serializer"] = to_json
        if "json_deserializer" not in processed_features:
            processed_features["json_deserializer"] = from_json

        super().__init__(driver_features=processed_features, **kwargs)
```

**Key points**:

- Handles both optional dependencies (pgvector) and stdlib features (JSON)
- Multiple related features grouped logically
- Provides sensible defaults for all features

#### Appropriate Hardcoded Defaults: DuckDB UUID Conversion

```python
class DuckDBDriverFeatures(TypedDict):
    """DuckDB driver feature flags.

    enable_uuid_conversion: Enable automatic UUID string conversion.
        When True (default), UUID strings are automatically converted to UUID objects.
        When False, UUID strings are treated as regular strings.
        No external dependencies - uses Python stdlib uuid module.
    """

    enable_uuid_conversion: NotRequired[bool]
    json_serializer: NotRequired[Callable[[Any], str]]


class DuckDBConfig(SyncDatabaseConfig):
    def __init__(self, *, driver_features=None, **kwargs):
        processed_features = dict(driver_features) if driver_features else {}

        # Default to True - uuid is stdlib, always available
        if "enable_uuid_conversion" not in processed_features:
            processed_features["enable_uuid_conversion"] = True

        super().__init__(driver_features=processed_features, **kwargs)
```

**Why hardcoded `True` is appropriate**:

- Feature uses standard library (uuid) - always available
- Improves Python type handling with zero cost
- No dependency to detect
- Backward-compatible behavior

### Anti-Patterns (PROHIBITED)

#### Anti-Pattern 1: Missing TypedDict

```python
# BAD - No TypedDict definition
class AdapterConfig(AsyncDatabaseConfig):
    def __init__(self, *, driver_features=None, **kwargs):
        super().__init__(driver_features=driver_features, **kwargs)
```

**Why this is bad**:

- No IDE autocomplete
- Typos go undetected
- Features are undiscoverable
- No inline documentation

#### Anti-Pattern 2: Defaulting Optional Features to False Without Reason

```python
# BAD - Before Asyncmy fix
class AsyncmyDriverFeatures(TypedDict):
    json_serializer: NotRequired[Callable[[Any], str]]
    json_deserializer: NotRequired[Callable[[str], Any]]


class AsyncmyConfig(AsyncDatabaseConfig):
    def __init__(self, *, driver_features=None, **kwargs):
        # No defaults provided at all!
        super().__init__(driver_features=driver_features or {}, **kwargs)
```

**Why this is bad**:

- Forces users to explicitly configure basic features
- Poor user experience
- No guidance on what values to use

**Fixed version**:

```python
# GOOD - After fix
class AsyncmyConfig(AsyncDatabaseConfig):
    def __init__(self, *, driver_features=None, **kwargs):
        processed_features = dict(driver_features) if driver_features else {}

        if "json_serializer" not in processed_features:
            processed_features["json_serializer"] = to_json
        if "json_deserializer" not in processed_features:
            processed_features["json_deserializer"] = from_json

        super().__init__(driver_features=processed_features, **kwargs)
```

#### Anti-Pattern 3: Inconsistent Naming

```python
# BAD - Inconsistent prefixes
class BadDriverFeatures(TypedDict):
    numpy_vectors: NotRequired[bool]  # Missing enable_ prefix
    use_pgvector: NotRequired[bool]   # Wrong prefix (use_)
    json_on: NotRequired[bool]        # Wrong prefix (_on)
```

**Fixed version**:

```python
# GOOD - Consistent enable_ prefix
class GoodDriverFeatures(TypedDict):
    enable_numpy_vectors: NotRequired[bool]
    enable_pgvector: NotRequired[bool]
    enable_json_codecs: NotRequired[bool]
```

### Compliance Table

### Change log: configuration examples

- Renamed documentation example references to use docs/examples/usage/test_configuration_*.py
- Added explicit :lines: ranges and :dedent: directive for the literalinclude at the top of docs/usage/configuration.rst
- Rebuilt documentation to verify the changes (make docs). Build completed with 2 warnings about dedent and a missing stylesheet; output HTML written to docs/_build/html

### Compliance Table

Current state of all adapters (as of type-cleanup branch):

| Adapter    | TypedDict | Auto-Detect | enable_ Prefix | Defaults | Grade      | Notes                                    |
|------------|-----------|-------------|----------------|----------|------------|------------------------------------------|
| Oracle     | ✅        | ✅          | ✅             | ✅       | Gold       | NumPy vectors + UUID binary w/ chaining  |
| AsyncPG    | ✅        | ✅          | ✅             | ✅       | Excellent  | Comprehensive TypedDict docs added       |
| Psycopg    | ✅        | ✅          | ✅             | ✅       | Excellent  | Comprehensive TypedDict docs added       |
| Psqlpy     | ✅        | ✅          | ✅             | ✅       | Excellent  | Simple but correct                       |
| DuckDB     | ✅        | N/A         | ✅             | ✅       | Excellent  | Stdlib features, comprehensive docs      |
| BigQuery   | ✅        | N/A         | ✅             | ✅       | Good       | Simple config, well documented           |
| ADBC       | ✅        | N/A         | ✅             | ✅       | Excellent  | Comprehensive TypedDict documentation    |
| SQLite     | ✅        | N/A         | ✅             | ✅       | Excellent  | Provides sensible defaults               |
| AioSQLite  | ✅        | N/A         | ✅             | ✅       | Excellent  | Matches SQLite patterns                  |
| Asyncmy    | ✅        | N/A         | N/A            | ✅       | Excellent  | Provides defaults (no bool flags)        |

**Grading criteria**:

- **Gold**: Perfect adherence to all patterns, serves as reference
- **Excellent**: Follows all patterns, well documented
- **Good**: Follows patterns appropriately for adapter's needs

## Google Cloud Connector Pattern

### Overview

Google Cloud SQL and AlloyDB connectors provide automatic IAM authentication, SSL management, and IP routing for PostgreSQL databases hosted on Google Cloud Platform. SQLSpec integrates these connectors through the AsyncPG adapter using a connection factory pattern.

### When to Use This Pattern

Use Google Cloud connectors when:

- Connecting to Cloud SQL for PostgreSQL instances
- Connecting to AlloyDB for PostgreSQL clusters
- Need automatic IAM authentication
- Want managed SSL/TLS connections
- Require private IP or PSC connectivity

### Implementation Pattern

#### Step 1: Add Optional Dependencies

Add connector packages as optional dependency groups in pyproject.toml:

```toml
[project.optional-dependencies]
cloud-sql = ["cloud-sql-python-connector[asyncpg]"]
alloydb = ["cloud-alloydb-python-connector[asyncpg]"]
```

#### Step 2: Add Detection Constants

In sqlspec/_typing.py:

```python
try:
    import google.cloud.sql.connector
    CLOUD_SQL_CONNECTOR_INSTALLED = True
except ImportError:
    CLOUD_SQL_CONNECTOR_INSTALLED = False

try:
    import google.cloud.alloydb.connector
    ALLOYDB_CONNECTOR_INSTALLED = True
except ImportError:
    ALLOYDB_CONNECTOR_INSTALLED = False
```

Re-export in sqlspec/typing.py and add to **all**.

#### Step 3: Update Driver Features TypedDict

Document all connector options with comprehensive descriptions:

```python
class AsyncpgDriverFeatures(TypedDict):
    """AsyncPG driver feature flags."""

    enable_cloud_sql: NotRequired[bool]
    """Enable Google Cloud SQL connector integration.
    Requires cloud-sql-python-connector package.
    Defaults to True when package is installed.
    Auto-configures IAM authentication, SSL, and IP routing.
    Mutually exclusive with enable_alloydb.
    """

    cloud_sql_instance: NotRequired[str]
    """Cloud SQL instance connection name.
    Format: "project:region:instance"
    Required when enable_cloud_sql is True.
    """

    cloud_sql_enable_iam_auth: NotRequired[bool]
    """Enable IAM database authentication.
    Defaults to False for passwordless authentication.
    When False, requires user/password in pool_config.
    """

    cloud_sql_ip_type: NotRequired[str]
    """IP address type for connection.
    Options: "PUBLIC", "PRIVATE", "PSC"
    Defaults to "PRIVATE".
    """

    enable_alloydb: NotRequired[bool]
    """Enable Google AlloyDB connector integration.
    Requires cloud-alloydb-python-connector package.
    Defaults to True when package is installed.
    Auto-configures IAM authentication and private networking.
    Mutually exclusive with enable_cloud_sql.
    """

    alloydb_instance_uri: NotRequired[str]
    """AlloyDB instance URI.
    Format: "projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE"
    Required when enable_alloydb is True.
    """

    alloydb_enable_iam_auth: NotRequired[bool]
    """Enable IAM database authentication.
    Defaults to False for passwordless authentication.
    """

    alloydb_ip_type: NotRequired[str]
    """IP address type for connection.
    Options: "PUBLIC", "PRIVATE", "PSC"
    Defaults to "PRIVATE".
    """
```

#### Step 4: Add Auto-Detection to Config Init

```python
class AsyncpgConfig(AsyncDatabaseConfig):
    def __init__(self, *, driver_features=None, **kwargs):
        features_dict = dict(driver_features) if driver_features else {}

        features_dict.setdefault("enable_cloud_sql", CLOUD_SQL_CONNECTOR_INSTALLED)
        features_dict.setdefault("enable_alloydb", ALLOYDB_CONNECTOR_INSTALLED)

        super().__init__(driver_features=features_dict, **kwargs)

        self._cloud_sql_connector = None
        self._alloydb_connector = None

        self._validate_connector_config()
```

#### Step 5: Add Configuration Validation

```python
def _validate_connector_config(self) -> None:
    """Validate Google Cloud connector configuration."""
    enable_cloud_sql = self.driver_features.get("enable_cloud_sql", False)
    enable_alloydb = self.driver_features.get("enable_alloydb", False)

    if enable_cloud_sql and enable_alloydb:
        msg = "Cannot enable both Cloud SQL and AlloyDB connectors simultaneously. Use separate configs for each database."
        raise ImproperConfigurationError(msg)

    if enable_cloud_sql:
        if not CLOUD_SQL_CONNECTOR_INSTALLED:
            msg = "cloud-sql-python-connector package not installed. Install with: pip install cloud-sql-python-connector"
            raise ImproperConfigurationError(msg)

        instance = self.driver_features.get("cloud_sql_instance")
        if not instance:
            msg = "cloud_sql_instance required when enable_cloud_sql is True. Format: 'project:region:instance'"
            raise ImproperConfigurationError(msg)

        cloud_sql_instance_parts_expected = 2
        if instance.count(":") != cloud_sql_instance_parts_expected:
            msg = f"Invalid Cloud SQL instance format: {instance}. Expected format: 'project:region:instance'"
            raise ImproperConfigurationError(msg)

    elif enable_alloydb:
        if not ALLOYDB_CONNECTOR_INSTALLED:
            msg = "cloud-alloydb-python-connector package not installed. Install with: pip install cloud-alloydb-python-connector"
            raise ImproperConfigurationError(msg)

        instance_uri = self.driver_features.get("alloydb_instance_uri")
        if not instance_uri:
            msg = "alloydb_instance_uri required when enable_alloydb is True. Format: 'projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE'"
            raise ImproperConfigurationError(msg)

        if not instance_uri.startswith("projects/"):
            msg = f"Invalid AlloyDB instance URI format: {instance_uri}. Expected format: 'projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE'"
            raise ImproperConfigurationError(msg)
```

#### Step 6: Implement Connection Factory Pattern

Extract connector setup into private helper methods:

```python
def _setup_cloud_sql_connector(self, config: dict[str, Any]) -> None:
    """Setup Cloud SQL connector and configure pool for connection factory pattern."""
    from google.cloud.sql.connector import Connector

    self._cloud_sql_connector = Connector()

    user = config.get("user")
    password = config.get("password")
    database = config.get("database")

    async def get_conn() -> AsyncpgConnection:
        conn_kwargs = {
            "instance_connection_string": self.driver_features["cloud_sql_instance"],
            "driver": "asyncpg",
            "enable_iam_auth": self.driver_features.get("cloud_sql_enable_iam_auth", False),
            "ip_type": self.driver_features.get("cloud_sql_ip_type", "PRIVATE"),
        }

        if user:
            conn_kwargs["user"] = user
        if password:
            conn_kwargs["password"] = password
        if database:
            conn_kwargs["db"] = database

        return await self._cloud_sql_connector.connect_async(**conn_kwargs)

    for key in ("dsn", "host", "port", "user", "password", "database"):
        config.pop(key, None)

    config["connect"] = get_conn


def _setup_alloydb_connector(self, config: dict[str, Any]) -> None:
    """Setup AlloyDB connector and configure pool for connection factory pattern."""
    from google.cloud.alloydb.connector import AsyncConnector

    self._alloydb_connector = AsyncConnector()

    user = config.get("user")
    password = config.get("password")
    database = config.get("database")

    async def get_conn() -> AsyncpgConnection:
        conn_kwargs = {
            "instance_uri": self.driver_features["alloydb_instance_uri"],
            "driver": "asyncpg",
            "enable_iam_auth": self.driver_features.get("alloydb_enable_iam_auth", False),
            "ip_type": self.driver_features.get("alloydb_ip_type", "PRIVATE"),
        }

        if user:
            conn_kwargs["user"] = user
        if password:
            conn_kwargs["password"] = password
        if database:
            conn_kwargs["db"] = database

        return await self._alloydb_connector.connect(**conn_kwargs)

    for key in ("dsn", "host", "port", "user", "password", "database"):
        config.pop(key, None)

    config["connect"] = get_conn
```

#### Step 7: Use in Pool Creation

```python
async def _create_pool(self) -> Pool[Record]:
    config = self._get_pool_config_dict()

    if self.driver_features.get("enable_cloud_sql", False):
        self._setup_cloud_sql_connector(config)
    elif self.driver_features.get("enable_alloydb", False):
        self._setup_alloydb_connector(config)

    if "init" not in config:
        config["init"] = self._init_connection

    return await asyncpg_create_pool(**config)
```

#### Step 8: Cleanup Connectors

```python
async def _close_pool(self) -> None:
    if self.pool_instance:
        await self.pool_instance.close()

    if self._cloud_sql_connector is not None:
        await self._cloud_sql_connector.close_async()
        self._cloud_sql_connector = None

    if self._alloydb_connector is not None:
        await self._alloydb_connector.close()
        self._alloydb_connector = None
```

### Key Design Principles

1. **Auto-Detection**: Default to package installation status
2. **Mutual Exclusion**: Cannot enable both connectors simultaneously
3. **Connection Factory Pattern**: Use driver's `connect` parameter
4. **Clean Helper Methods**: Extract setup logic for maintainability
5. **Proper Lifecycle**: Initialize in create_pool, cleanup in close_pool
6. **Clear Validation**: Validate instance names, package installation, config
7. **Comprehensive TypedDict**: Document all options inline

### Testing Requirements

- Unit tests with mocked connectors
- Integration tests with real instances (conditional)
- Test auto-detection with both packages installed/not installed
- Test mutual exclusion validation
- Test connection factory pattern integration
- Test lifecycle (initialization and cleanup)
- Test all IP types and auth modes

### Driver Compatibility

| Driver | Cloud SQL | AlloyDB | Notes |
|--------|-----------|---------|-------|
| AsyncPG | ✅ Full | ✅ Full | Connection factory pattern via `connect` param |
| Psycopg | ⚠️ Research | ⚠️ Research | Not officially documented, needs prototype |
| Psqlpy | ❌ No | ❌ No | Internal Rust driver, architecturally incompatible |
| ADBC | ❌ No | ❌ No | URI-only interface, no factory pattern support |

### Examples from Existing Implementations

See sqlspec/adapters/asyncpg/config.py for the reference implementation.

### Documentation Requirements

When implementing cloud connector support:

1. **Update adapter guide** - Add cloud integration section with examples
2. **Create cloud connector guide** - Comprehensive configuration reference
3. **Document limitations** - Clearly state unsupported drivers
4. **Provide troubleshooting** - Common errors and solutions
5. **Include migration guide** - From direct DSN to connector pattern

### Testing Requirements

When implementing `driver_features`, you MUST test:

1. **Default behavior** - Feature enabled/disabled by default
2. **Explicit override** - User can set to `True`/`False`
3. **Graceful degradation** - Works when optional dependency missing
4. **Type safety** - TypedDict provides proper IDE support

**Example test structure**:

```python
import pytest
from sqlspec.typing import NUMPY_INSTALLED

def test_default_feature_enabled(config):
    """Test feature is enabled by default when dependency available."""
    if NUMPY_INSTALLED:
        assert config.driver_features["enable_numpy_vectors"] is True
    else:
        assert config.driver_features["enable_numpy_vectors"] is False


def test_explicit_override(config_class):
    """Test user can explicitly disable feature."""
    config = config_class(
        pool_config={"dsn": "test"},
        driver_features={"enable_numpy_vectors": False}
    )
    assert config.driver_features["enable_numpy_vectors"] is False


@pytest.mark.skipif(not NUMPY_INSTALLED, reason="NumPy not installed")
def test_feature_roundtrip(session):
    """Test feature works end-to-end with dependency."""
    # Test actual functionality
    pass
```

### Documentation Requirements

When adding a new `driver_features` option:

1. **Document in TypedDict docstring** - Full description inline
2. **Update adapter docs** - Add example in `docs/reference/adapters.rst`
3. **Update CHANGELOG** - Note the new feature
4. **Add example** - Show real-world usage

**Example TypedDict documentation**:

```python
class AdapterDriverFeatures(TypedDict):
    """Adapter driver feature flags.

    enable_feature_name: Short one-line description.
        Requirements: List prerequisites (packages, database versions).
        Defaults to X when Y is installed/True for stdlib features.
        Behavior when enabled: What happens when True.
        Behavior when disabled: What happens when False.
        Use case: When you would enable/disable this.
    """

    enable_feature_name: NotRequired[bool]
```

### Cross-References

- **Type Handler Pattern** (above): Implementation details for type handlers used with `driver_features`
- **Optional Dependency Handling**: See `sqlspec.typing` for detection constants
- **Testing Standards**: See Testing Strategy section for general testing requirements

## Flask Extension Pattern (Hook-Based)

### Overview

The Flask extension uses a **hook-based lifecycle** pattern instead of middleware, since Flask doesn't have native ASGI middleware like Starlette/FastAPI.

### Hook-Based Lifecycle Pattern

Flask uses three hooks for request lifecycle management:

```python
def init_app(self, app: "Flask") -> None:
    """Initialize Flask application with SQLSpec."""
    # Register hooks for request lifecycle
    app.before_request(self._before_request_handler)
    app.after_request(self._after_request_handler)
    app.teardown_appcontext(self._teardown_appcontext_handler)

def _before_request_handler(self) -> None:
    """Acquire connection before request. Store in Flask g object."""
    from flask import current_app, g

    for config_state in self._config_states:
        if config_state.config.supports_connection_pooling:
            pool = current_app.extensions["sqlspec"]["pools"][config_state.session_key]
            conn_ctx = config_state.config.provide_connection(pool)
            # Acquire connection (via portal if async)
            setattr(g, config_state.connection_key, connection)

def _after_request_handler(self, response: "Response") -> "Response":
    """Handle transaction after request based on response status."""
    from flask import g

    for config_state in self._config_states:
        if config_state.commit_mode == "manual":
            continue
        # Commit or rollback based on status code

    return response  # MUST return response unchanged

def _teardown_appcontext_handler(self, _exc: "Exception | None" = None) -> None:
    """Clean up connections when request context ends."""
    from flask import g
    # Close connections and cleanup g object
```

**Key differences from Starlette/FastAPI**:

- **No middleware**: Use `before_request`, `after_request`, `teardown_appcontext` hooks
- **Flask g object**: Store connections/sessions on `g` (request-scoped global)
- **Must return response**: `after_request` hook must return the response unchanged
- **Nested imports required**: Import `flask.g` and `flask.current_app` inside hooks to access request context

### DEFAULT_SESSION_KEY Standardization

All framework extensions MUST use the same default session key for consistency:

```python
# MANDATORY: All framework extensions use "db_session"
DEFAULT_SESSION_KEY = "db_session"
```

**Why "db_session"**:

- Used by Litestar as dependency injection key name
- Changing would break Litestar's DI system
- Consistency across frameworks reduces cognitive overhead
- More descriptive than alternatives like "default"

**Where it's used**:

- `sqlspec/extensions/flask/extension.py:22`
- `sqlspec/extensions/starlette/extension.py:24`
- `sqlspec/extensions/litestar/plugin.py:50`

**Pattern for multi-database setups**:

```python
# Primary database uses default key
primary_config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/main"},
    extension_config={
        "starlette": {"session_key": "db_session", "commit_mode": "autocommit"}
    }
)

# Secondary database uses custom key
analytics_config = SqliteConfig(
    pool_config={"database": "analytics.db"},
    extension_config={
        "starlette": {"session_key": "analytics", "commit_mode": "manual"}
    }
)

# Access by key
db = plugin.get_session()              # Uses "db_session" (default)
analytics = plugin.get_session("analytics")  # Uses custom key
```

### Portal Pattern for Sync Frameworks

Enable async adapters (asyncpg, asyncmy, aiosqlite) in sync WSGI frameworks:

```python
class PortalProvider:
    """Manages background thread with event loop for async operations."""

    def __init__(self) -> None:
        self._request_queue: queue.Queue = queue.Queue()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready_event: threading.Event = threading.Event()

    def start(self) -> None:
        """Start daemon thread with event loop."""
        self._thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self._thread.start()
        self._ready_event.wait()  # Block until loop ready

    def call(self, func, *args, **kwargs):
        """Execute async function from sync context."""
        future = asyncio.run_coroutine_threadsafe(
            self._async_caller(func, args, kwargs),
            self._loop
        )
        result, exception = local_result_queue.get()  # Block until done
        if exception:
            raise exception
        return result
```

**Portal usage in extension**:

```python
# Auto-detect async configs and create portal
if self._has_async_configs:
    self._portal = PortalProvider()
    self._portal.start()

# Use portal for async operations
if config_state.is_async:
    pool = self._portal.portal.call(config_state.config.create_pool)
else:
    pool = config_state.config.create_pool()  # Direct sync call
```

**Performance**: ~1-2ms overhead per operation. **Recommended**: Use sync adapters for Flask.

### Flask Pool and Portal Cleanup Pattern

Flask requires explicit resource cleanup using Python's `atexit` module:

```python
import atexit

class SQLSpecPlugin:
    def init_app(self, app: "Flask") -> None:
        """Initialize Flask application with SQLSpec."""
        # Create pools
        pools: dict[str, Any] = {}
        for config_state in self._config_states:
            if config_state.config.supports_connection_pooling:
                if config_state.is_async:
                    pool = self._portal.portal.call(config_state.config.create_pool)
                else:
                    pool = config_state.config.create_pool()
                pools[config_state.session_key] = pool

        # Register cleanup hook
        self._register_shutdown_hook()

    def _register_shutdown_hook(self) -> None:
        """Register shutdown hook for pool and portal cleanup."""
        if self._cleanup_registered:
            return

        atexit.register(self.shutdown)
        self._cleanup_registered = True

    def shutdown(self) -> None:
        """Dispose connection pools and stop async portal."""
        if self._shutdown_complete:
            return

        self._shutdown_complete = True

        # Close all pools
        for config_state in self._config_states:
            if config_state.config.supports_connection_pooling:
                try:
                    if config_state.is_async:
                        self._portal.portal.call(config_state.config.close_pool)
                    else:
                        config_state.config.close_pool()
                except Exception:
                    logger.exception("Error closing pool during shutdown")

        # Stop portal
        if self._portal is not None:
            try:
                self._portal.stop()
            except Exception:
                logger.exception("Error stopping portal during shutdown")
            finally:
                self._portal = None
```

**Key requirements**:

- **Idempotent**: Use `_shutdown_complete` flag to prevent double cleanup
- **Single registration**: Use `_cleanup_registered` flag to avoid multiple atexit hooks
- **Error handling**: Log exceptions but don't fail on cleanup errors
- **Portal cleanup**: Always stop portal for async configs to prevent thread leaks
- **Sync and async**: Handle both sync pool cleanup and async pool cleanup via portal

**Why atexit**:

- Flask (WSGI) lacks native lifecycle context managers
- atexit ensures cleanup on interpreter shutdown
- Works with development servers, production servers, and tests
- Standard pattern for library resource cleanup

**Testing pattern**:

```python
def test_flask_pool_cleanup():
    """Verify pools are cleaned up on shutdown."""
    plugin = SQLSpecPlugin(sqlspec, app)

    # Verify atexit hook registered
    assert plugin._cleanup_registered

    # Trigger shutdown
    plugin.shutdown()

    # Verify cleanup complete
    assert plugin._shutdown_complete
    assert plugin._portal is None
```

### HTTP Status Code Constants

Avoid magic values by defining module-level constants:

```python
# In _state.py
HTTP_SUCCESS_MIN = 200
HTTP_SUCCESS_MAX = 300
HTTP_REDIRECT_MAX = 400

@dataclass
class FlaskConfigState:
    def should_commit(self, status_code: int) -> bool:
        if self.commit_mode == "autocommit":
            return HTTP_SUCCESS_MIN <= status_code < HTTP_SUCCESS_MAX
```

**Why**: Satisfies Ruff PLR2004, self-documenting, easy to update.

### Flask g Object Storage

Store connection state on Flask's `g` object for request-scoped access:

```python
# Store with dynamic keys
setattr(g, config_state.connection_key, connection)
setattr(g, f"{config_state.connection_key}_ctx", conn_ctx)

# Retrieve
connection = getattr(g, config_state.connection_key)

# Clean up - always check hasattr first
if hasattr(g, config_state.connection_key):
    delattr(g, config_state.connection_key)
```

### Portal in Utils (Implemented)

**Location**: `sqlspec/utils/portal.py` (moved from Flask extension for broader reusability)

Portal now available for any sync framework or utility needing async-to-sync bridging:

```python
from sqlspec.utils.portal import Portal, PortalProvider, PortalManager, get_global_portal

# Option 1: Create dedicated portal (Flask extension pattern)
portal_provider = PortalProvider()
portal_provider.start()
result = portal_provider.portal.call(some_async_function, arg1, arg2)

# Option 2: Use global singleton portal (sync_tools pattern)
portal = get_global_portal()
result = portal.call(some_async_function, arg1, arg2)

# Option 3: Direct PortalManager access
manager = PortalManager()
portal = manager.get_or_create_portal()
result = portal.call(some_async_function, arg1, arg2)
```

**Benefits**:

- Available to Django, Bottle, or any sync framework
- Single import location for all portal functionality
- Thread-safe singleton for automatic portal management

### Portal + sync_tools Integration (Implemented)

The `await_()` function now automatically uses the global portal when no event loop exists:

```python
from sqlspec.utils.sync_tools import await_

async def async_add(a: int, b: int) -> int:
    await asyncio.sleep(0.01)
    return a + b

# Automatically uses portal if no loop exists (default behavior)
sync_add = await_(async_add)
result = sync_add(5, 3)  # Returns 8, using portal internally

# To disable portal and raise errors instead, use raise_sync_error=True
sync_add_strict = await_(async_add, raise_sync_error=True)
```

**How it works**:

```python
def await_(async_function, raise_sync_error=False):  # Portal enabled by default
    @functools.wraps(async_function)
    def wrapper(*args, **kwargs):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if raise_sync_error:
                raise RuntimeError("Cannot run async function")
            # Automatically use global portal (default behavior)
            from sqlspec.utils.portal import get_global_portal
            portal = get_global_portal()
            return portal.call(async_function, *args, **kwargs)
        # ... rest of implementation
```

**Benefits**:

- **Transparent async support by default** - no parameter needed
- No manual portal management required
- Global portal automatically created and reused
- Works across all sync frameworks (Flask, Django, Bottle, etc.)
- Opt-in to strict error mode with `raise_sync_error=True` if needed

**PortalManager Implementation**:

```python
class PortalManager(metaclass=SingletonMeta):
    """Singleton manager for global portal instance."""

    def __init__(self) -> None:
        self._provider: PortalProvider | None = None
        self._lock = threading.Lock()

    def get_or_create_portal(self) -> Portal:
        """Get or create the global portal instance.

        Lazily creates and starts the portal provider on first access.
        Thread-safe via locking.
        """
        if self._provider is None:
            with self._lock:
                if self._provider is None:
                    self._provider = PortalProvider()
                    self._provider.start()
        return self._provider.portal
```

**Key Features**:

- Uses `SingletonMeta` for thread-safe singleton pattern
- Double-checked locking for performance
- Lazy initialization - portal only created when needed
- Global portal shared across all `await_()` calls

## Framework Extension Pattern (Middleware-Based)

### Overview

SQLSpec provides framework extensions for Litestar, Starlette, and FastAPI that handle connection pooling, request-scoped sessions, and automatic transaction management through middleware.

### When to Use Framework Extensions

Use framework extensions when:

- Building web applications that need database integration
- Need automatic transaction handling based on HTTP response status
- Want request-scoped session management
- Require connection pooling with lifecycle management
- Support multiple databases in a single application

**Key benefits**:

- **Automatic lifecycle management**: Pools created on startup, closed on shutdown
- **Request-scoped sessions**: Sessions cached per request for consistency
- **Transaction automation**: Commit/rollback based on response status
- **Multi-database support**: Multiple configs with unique state keys
- **Minimal boilerplate**: Plugin pattern vs manual context managers

### Implementation Pattern

**Core Components**:

1. **Plugin/Extension Class**: Main integration point (`SQLSpecPlugin` for Starlette/FastAPI, `SQLSpecPlugin` for Litestar)
2. **Middleware**: Transaction handling (Manual, Autocommit, AutocommitIncludeRedirect)
3. **Configuration State**: Dataclass holding config, keys, and commit settings
4. **Session Helpers**: Utilities for session retrieval and caching

**Structure for New Framework Extensions**:

```python
# sqlspec/extensions/<framework>/
├── __init__.py           # Public API exports
├── extension.py          # Main plugin class
├── middleware.py         # Transaction middleware classes
├── _state.py            # Configuration state dataclass
└── _utils.py            # Session management helpers
```

### Example Implementation (Starlette/FastAPI Pattern)

**Step 1: Configuration State Dataclass**

```python
from dataclasses import dataclass
from typing import Any


@dataclass
class _ConfigState:
    """Internal state for framework configuration.

    Holds database config, state keys, and transaction settings.
    """

    config: Any
    connection_key: str
    pool_key: str
    session_key: str
    commit_mode: str
    extra_commit_statuses: "set[int] | None"
    extra_rollback_statuses: "set[int] | None"
```

**Step 2: Middleware Classes**

```python
class SQLSpecManualMiddleware(BaseHTTPMiddleware):
    """Manual mode - no automatic transactions."""

    def __init__(self, app: Any, config_state: "_ConfigState") -> None:
        super().__init__(app)
        self.config_state = config_state

    async def dispatch(self, request: "Request", call_next: Any) -> Any:
        config = self.config_state.config
        connection_key = self.config_state.connection_key

        if config.supports_connection_pooling:
            pool = getattr(request.app.state, self.config_state.pool_key)
            async with config.provide_connection(pool) as connection:
                setattr(request.state, connection_key, connection)
                try:
                    return await call_next(request)
                finally:
                    delattr(request.state, connection_key)
        else:
            connection = await config.create_connection()
            setattr(request.state, connection_key, connection)
            try:
                return await call_next(request)
            finally:
                await connection.close()


class SQLSpecAutocommitMiddleware(BaseHTTPMiddleware):
    """Autocommit mode - commit on 2xx, rollback otherwise."""

    async def dispatch(self, request: "Request", call_next: Any) -> Any:
        # Acquire connection
        # Call handler
        # Commit on success status, rollback on error status
        # Release connection
        pass
```

**Step 3: Session Management Helpers**

```python
def get_or_create_session(request: "Request", config_state: "_ConfigState") -> Any:
    """Get or create cached session for request.

    Sessions are cached per request using a unique cache key to ensure
    the same session is reused throughout the request lifecycle.
    """
    cache_key = f"_sqlspec_session_{config_state.session_key}"

    cached_session = getattr(request.state, cache_key, None)
    if cached_session is not None:
        return cached_session

    connection = getattr(request.state, config_state.connection_key)
    session = config_state.config.driver_type(
        connection=connection,
        statement_config=config_state.config.statement_config,
    )

    setattr(request.state, cache_key, session)
    return session
```

**Step 4: Main Plugin Class**

```python
class SQLSpecPlugin:
    """Framework extension for database integration."""

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
        """Initialize application with SQLSpec.

        Validates configuration, wraps lifespan, and adds middleware.
        """
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
        """Manage connection pool lifecycle."""
        for config_state in self._config_states:
            if config_state.config.supports_connection_pooling:
                pool = await config_state.config.create_pool()
                setattr(app.state, config_state.pool_key, pool)

        try:
            yield
        finally:
            for config_state in self._config_states:
                if config_state.config.supports_connection_pooling:
                    close_result = config_state.config.close_pool()
                    if close_result is not None:
                        await close_result

    def get_session(self, request: "Request", key: "str | None" = None) -> Any:
        """Get or create database session for request."""
        if key is None:
            config_state = self._config_states[0]
        else:
            config_state = self._get_config_state_by_key(key)

        return get_or_create_session(request, config_state)
```

### Inheritance Pattern for Related Frameworks

**FastAPI extends Starlette** - Reuse base functionality, add framework-specific helpers:

```python
from sqlspec.extensions.starlette.extension import SQLSpecPlugin as _StarlettePlugin


class SQLSpecPlugin(_StarlettePlugin):
    """FastAPI extension - inherits Starlette + adds dependency injection."""

    def session_dependency(self, key: "str | None" = None) -> "Callable[[Request], Any]":
        """Create dependency factory for session injection.

        Returns callable for use with FastAPI's Depends().
        """

        def dependency(request: Request) -> Any:
            return self.get_session(request, key)

        return dependency

    def connection_dependency(self, key: "str | None" = None) -> "Callable[[Request], Any]":
        """Create dependency factory for connection injection."""

        def dependency(request: Request) -> Any:
            return self.get_connection(request, key)

        return dependency
```

**Key inheritance principles**:

- Base framework provides core middleware and lifecycle management
- Child framework adds framework-specific helpers (dependency injection, etc.)
- Both use same configuration key (`"starlette"` for both Starlette and FastAPI)
- Shared configuration via TypedDict in `sqlspec/config.py`

### Configuration via TypedDict

**Define configuration in `sqlspec/config.py`**:

```python
class StarletteConfig(TypedDict):
    """Configuration options for Starlette and FastAPI extensions.

    All fields are optional with sensible defaults. Use in extension_config["starlette"]:

    Example:
        config = AsyncpgConfig(
            pool_config={"dsn": "postgresql://localhost/mydb"},
            extension_config={
                "starlette": {
                    "commit_mode": "autocommit",
                    "session_key": "db"
                }
            }
        )
    """

    connection_key: NotRequired[str]
    pool_key: NotRequired[str]
    session_key: NotRequired[str]
    commit_mode: NotRequired[Literal["manual", "autocommit", "autocommit_include_redirect"]]
    extra_commit_statuses: NotRequired[set[int]]
    extra_rollback_statuses: NotRequired[set[int]]
```

### Disabling Built-in Dependency Injection (disable_di Pattern)

**When to Use**: When users want to integrate SQLSpec with their own dependency injection solution (e.g., Dishka, dependency-injector) and need full control over database lifecycle management.

**Pattern**: Add a `disable_di` boolean flag to framework extension configuration that conditionally skips the built-in DI setup.

**Implementation Steps**:

1. **Add to TypedDict in `sqlspec/config.py`**:

```python
class StarletteConfig(TypedDict):
    # ... existing fields ...

    disable_di: NotRequired[bool]
    """Disable built-in dependency injection. Default: False.
    When True, the Starlette/FastAPI extension will not add middleware for managing
    database connections and sessions. Users are responsible for managing the
    database lifecycle manually via their own DI solution.
    """
```

2. **Add to Configuration State Dataclass**:

```python
@dataclass
class SQLSpecConfigState:
    config: "DatabaseConfigProtocol[Any, Any, Any]"
    connection_key: str
    pool_key: str
    session_key: str
    commit_mode: CommitMode
    extra_commit_statuses: "set[int] | None"
    extra_rollback_statuses: "set[int] | None"
    disable_di: bool  # Add this field
```

3. **Extract from Config and Default to False**:

```python
def _extract_starlette_settings(self, config):
    starlette_config = config.extension_config.get("starlette", {})
    return {
        # ... existing keys ...
        "disable_di": starlette_config.get("disable_di", False),  # Default False
    }
```

4. **Conditionally Skip DI Setup**:

**Middleware-based (Starlette/FastAPI)**:

```python
def init_app(self, app):
    # ... lifespan setup ...

    for config_state in self._config_states:
        if not config_state.disable_di:  # Only add if DI enabled
            self._add_middleware(app, config_state)
```

**Provider-based (Litestar)**:

```python
def on_app_init(self, app_config):
    for state in self._plugin_configs:
        # ... signature namespace ...

        if not state.disable_di:  # Only register if DI enabled
            app_config.before_send.append(state.before_send_handler)
            app_config.lifespan.append(state.lifespan_handler)
            app_config.dependencies.update({
                state.connection_key: Provide(state.connection_provider),
                state.pool_key: Provide(state.pool_provider),
                state.session_key: Provide(state.session_provider),
            })
```

**Hook-based (Flask)**:

```python
def init_app(self, app):
    # ... pool setup ...

    # Only register hooks if at least one config has DI enabled
    if any(not state.disable_di for state in self._config_states):
        app.before_request(self._before_request_handler)
        app.after_request(self._after_request_handler)
        app.teardown_appcontext(self._teardown_appcontext_handler)

def _before_request_handler(self):
    for config_state in self._config_states:
        if config_state.disable_di:  # Skip if DI disabled
            continue
        # ... connection setup ...
```

**Testing Requirements**:

1. **Test with `disable_di=True`**: Verify DI mechanisms are not active
2. **Test default behavior**: Verify `disable_di=False` preserves existing functionality
3. **Integration tests**: Demonstrate manual DI setup works correctly

**Example Usage**:

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.base import SQLSpec
from sqlspec.extensions.starlette import SQLSpecPlugin

sql = SQLSpec()
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/db"},
    extension_config={"starlette": {"disable_di": True}}  # Disable built-in DI
)
sql.add_config(config)
plugin = SQLSpecPlugin(sql)

# User is now responsible for manual lifecycle management
async def my_route(request):
    pool = await config.create_pool()
    async with config.provide_connection(pool) as connection:
        session = config.driver_type(connection=connection, statement_config=config.statement_config)
        result = await session.execute("SELECT 1")
        await config.close_pool()
        return result
```

**Key Principles**:

- **Backward Compatible**: Default `False` preserves existing behavior
- **Consistent Naming**: Use `disable_di` across all frameworks
- **Clear Documentation**: Warn users they are responsible for lifecycle management
- **Complete Control**: When disabled, extension does zero automatic DI

### Multi-Database Support

**Key validation ensures unique state keys**:

```python
def _validate_unique_keys(self) -> None:
    """Validate that all state keys are unique across configs."""
    all_keys: "set[str]" = set()

    for state in self._config_states:
        keys = {state.connection_key, state.pool_key, state.session_key}
        duplicates = all_keys & keys

        if duplicates:
            msg = f"Duplicate state keys found: {duplicates}"
            raise ImproperConfigurationError(msg)

        all_keys.update(keys)
```

**Access multiple databases by key**:

```python
# Configuration
pg_config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/main"},
    extension_config={"starlette": {"session_key": "pg_db"}}
)
mysql_config = AsyncmyConfig(
    pool_config={"dsn": "mysql://localhost/analytics"},
    extension_config={"starlette": {"session_key": "mysql_db"}}
)

# Usage
pg_db = db_ext.get_session(request, key="pg_db")
mysql_db = db_ext.get_session(request, key="mysql_db")
```

### Testing Requirements

**Unit Tests** - Test components in isolation:

- Configuration state creation
- Unique key validation
- Session caching logic
- Middleware transaction logic

**Integration Tests** - Test with real framework apps:

- Basic query execution
- Manual commit mode
- Autocommit mode
- Autocommit with redirects
- Multi-database scenarios
- Session caching per request
- Pool lifecycle management

**Example integration test**:

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
        db_ext = SQLSpecPlugin(sql)

        async def create_table(request):
            session = db_ext.get_session(request)
            await session.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            await session.execute("INSERT INTO test (name) VALUES (:name)", {"name": "Bob"})
            return JSONResponse({"created": True})

        app = Starlette(routes=[Route("/create", create_table)])
        db_ext.init_app(app)

        with TestClient(app) as client:
            response = client.get("/create")
            assert response.status_code == 200
```

**Note**: Use `tempfile.NamedTemporaryFile` for test isolation when testing with connection pooling. See "Test Isolation Patterns for Pooled Connections" section for details.

### Documentation Requirements

When implementing framework extensions:

1. **User Guide** - Create `docs/guides/extensions/<framework>.md`:
   - Installation instructions
   - Quick start example
   - Configuration options
   - Commit modes
   - Multi-database setup
   - Testing patterns
   - Best practices

2. **API Reference** - Document plugin class and methods

3. **Examples** - Provide working code examples

4. **Migration Guide** - Show before/after for manual pattern → plugin pattern

### Examples from Existing Extensions

**Litestar Plugin** (`sqlspec/extensions/litestar/plugin.py`):

- Comprehensive plugin with handlers for connection, session, pool
- Correlation middleware for request tracking
- Store integration for serialization
- Signature namespace for type handling

**Starlette Plugin** (`sqlspec/extensions/starlette/extension.py`):

- Middleware-based transaction handling
- Lifespan context manager for pool lifecycle
- Session caching per request
- Multi-database support with unique keys

**FastAPI Plugin** (`sqlspec/extensions/fastapi/extension.py`):

- Inherits from Starlette plugin
- Adds `session_dependency()` and `connection_dependency()` for FastAPI `Depends()`
- Reuses all Starlette middleware and lifecycle logic

### Key Patterns

1. **Middleware for transactions**: Use framework's middleware system for automatic commit/rollback
2. **Lifespan for pools**: Manage pool creation/destruction in framework lifecycle
3. **Request state for connections**: Store connections in request-scoped state
4. **Session caching**: Cache sessions per request using unique cache keys
5. **Configuration via extension_config**: Use `extension_config["framework"]` pattern
6. **TypedDict for type safety**: Define config schemas in `sqlspec/config.py`
7. **Inheritance for related frameworks**: Child framework extends parent, adds specific helpers

## Agent Workflow Coordination

### Automated Multi-Agent Workflow

SQLSpec uses a coordinated multi-agent system where the Expert agent orchestrates the complete development lifecycle:

```
User runs: /implement {feature-name}

**IMPORTANT**: `/implement` automatically runs `/test` and `/review` - no manual steps needed!

┌─────────────────────────────────────────────────────────────┐
│                      EXPERT AGENT                            │
│                                                              │
│  1. Read Plan & Research (from specs/active/{feature}/)    │
│  2. Implement Feature (following AGENTS.md standards)      │
│  3. Self-Test & Verify                                      │
│  4. ──► Auto-Invoke Testing Agent (subagent)               │
│         │                                                    │
│         ├─► Create unit tests                              │
│         ├─► Create integration tests (all adapters)        │
│         ├─► Test edge cases                                │
│         └─► Verify coverage & all tests pass               │
│  5. ──► Auto-Invoke Docs & Vision Agent (subagent)         │
│         │                                                    │
│         ├─► Phase 1: Update documentation                  │
│         ├─► Phase 2: Quality gate validation               │
│         ├─► Phase 3: Knowledge capture (AGENTS.md+guides)  │
│         ├─► Phase 4: Re-validate after updates             │
│         ├─► Phase 5: Clean tmp/ and archive                │
│         └─► Generate completion report                      │
│  6. Return Complete Summary                                 │
└─────────────────────────────────────────────────────────────┘

Result: Feature implemented, tested, documented, archived - all automatically!
```

### Codex-Oriented Workflow Usage

Codex can execute the same lifecycle without invoking Claude slash commands. Use the prompts below to engage Codex directly while keeping the workflow artifacts identical.

- **General Rule**: Tell Codex which phase to emulate (`prd`, `implement`, `test`, `review`) and point to the active workspace root (`specs/active/{slug}/` preferred). Codex will create the folder if it does not exist.
- **Codex `/prd` Equivalent**: Ask "Codex: create PRD for {feature}" and provide any context. Codex must (1) research via docs/guides/ as outlined in `.claude/agents/prd.md`, (2) write or update `prd.md`, `tasks.md`, `research/plan.md`, `recovery.md`, and (3) ensure `tmp/` exists. Planning output follows the same structure the PRD agent would create.
- **Codex `/implement` Equivalent**: Ask Codex to "execute implementation phase for {workspace}". Codex then reads the workspace, consults guides, writes code under `sqlspec/`, updates tasks, and runs local checks exactly as described in `.claude/agents/expert.md`. When the plan calls for sub-agents, Codex continues by emulating the Testing and Docs & Vision phases in order.
- **Codex `/test` Equivalent**: Request "Codex: perform testing phase for {workspace}". Codex creates or updates pytest suites, ensures coverage thresholds, and records progress in `tasks.md`, mirroring `.claude/agents/testing.md`.
- **Codex `/review` Equivalent**: Request "Codex: run docs, quality gate, and cleanup for {workspace}". Codex completes the five Docs & Vision phases—documentation, quality gate, knowledge capture (including AGENTS.md and guides updates), re-validation, and workspace archival.
- **Knowledge Capture Reminder**: Whenever Codex finishes implementation or review work, it must update this AGENTS.md and any relevant guides with new patterns so Claude and other assistants inherit the learnings.

### Gemini CLI Workflow Usage

Gemini CLI can execute the same lifecycle. Direct Gemini to the desired phase and reference the active workspace so it mirrors the PRD, Expert, Testing, and Docs & Vision agents.

- **General Rule**: Specify the phase (`prd`, `implement`, `test`, `review`) and point Gemini to `specs/active/{slug}/` (fallback `requirements/{slug}/`). Gemini should create the workspace if it is missing and follow `.claude/agents/{agent}.md` for detailed steps.
- **Gemini `/prd` Equivalent**: Prompt "Gemini: create PRD for {feature} using AGENTS.md." Gemini reads the guides, writes `prd.md`, `tasks.md`, `research/plan.md`, `recovery.md`, and ensures a `tmp/` directory exists.
- **Gemini `/implement` Equivalent**: Prompt "Gemini: run implementation phase for {workspace}." Gemini reads the workspace artifacts, implements code per `.claude/agents/expert.md`, and continues by emulating the Testing and Docs & Vision workflows in sequence.
- **Gemini `/test` Equivalent**: Prompt "Gemini: execute testing phase for {workspace}." Gemini creates or updates pytest suites, verifies coverage targets, and records progress in `tasks.md` exactly like the Testing agent.
- **Gemini `/review` Equivalent**: Prompt "Gemini: perform docs, quality gate, and cleanup for {workspace}." Gemini completes all five Docs & Vision phases, including knowledge capture updates to AGENTS.md and guides, followed by archival.
- **Prompt Templates**: If using Gemini CLI prompt files, include a directive to consult the relevant agent guide plus this section so each invocation stays aligned.

### Claude Workflow Usage

Claude already maps to these phases through the slash commands defined in `.claude/commands/`. Use the commands or free-form prompts—the agent guides remain the source of truth.

- **Default Flow**: `/prd`, `/implement`, `/test`, `/review` trigger the PRD, Expert, Testing, and Docs & Vision workflows automatically.
- **Manual Prompts**: When not using slash commands, instruct Claude which phase to run and provide the workspace path so it follows the same sequence.
- **Knowledge Capture Expectation**: Claude must update AGENTS.md and guides during the Docs & Vision phase before archiving, regardless of invocation style.

**Key Workflow Principles**:

- **Single Command**: `/implement` handles entire lifecycle
- **No Manual Steps**: Testing, docs, and archival automatic
- **Knowledge Preservation**: Patterns captured in AGENTS.md and guides
- **Quality Assurance**: Multi-phase validation before completion
- **Session Resumability**: All work tracked in specs/active/{feature}/

### Knowledge Capture Process

After every feature implementation, the Docs & Vision agent **must** extract and preserve new patterns:

#### Step 1: Analyze Implementation

Review what was built for reusable patterns:

- **New Patterns**: Novel approaches to common problems
- **Best Practices**: Techniques that worked particularly well
- **Conventions**: Naming, structure, or organization patterns
- **Type Handling**: New type conversion or validation approaches
- **Testing Patterns**: Effective test strategies
- **Performance Techniques**: Optimization discoveries

#### Step 2: Update AGENTS.md

Add patterns to relevant sections in this file:

- **Code Quality Standards** - New coding patterns
- **Testing Strategy** - New test approaches
- **Performance Optimizations** - New optimization techniques
- **Database Adapter Implementation** - Adapter-specific patterns
- **driver_features Pattern** - New feature configurations

Example addition:

```python
# In Docs & Vision agent
Edit(
    file_path="AGENTS.md",
    old_string="### Compliance Table",
    new_string="""### New Pattern: Session Callbacks for Type Handlers

When implementing optional type handlers:

```python
class AdapterConfig(AsyncDatabaseConfig):
    async def _create_pool(self):
        config = dict(self.pool_config)
        if self.driver_features.get("enable_feature", False):
            config["session_callback"] = self._init_connection
        return await create_pool(**config)

    async def _init_connection(self, connection):
        if self.driver_features.get("enable_feature", False):
            from ._feature_handlers import register_handlers
            register_handlers(connection)
```

### Compliance Table"""

)

```

#### Step 3: Update docs/guides/

Enhance relevant guides with new patterns:

- `docs/guides/adapters/{adapter}.md` - Adapter-specific patterns
- `docs/guides/testing/testing.md` - Testing patterns
- `docs/guides/performance/` - Performance techniques
- `docs/guides/architecture/` - Architectural patterns

#### Step 4: Validate Examples

Ensure all new patterns have working code examples that execute successfully.

## Re-validation Protocol

After updating AGENTS.md or guides, the Docs & Vision agent **must** re-validate to ensure consistency:

### Step 1: Re-run Tests

```bash
uv run pytest -n 2 --dist=loadgroup
```

**All tests must still pass** after documentation updates.

### Step 2: Rebuild Documentation

```bash
make docs
```

**Documentation must build without new errors** after updates.

### Step 3: Verify Pattern Consistency

Manually check that:

- New patterns align with existing standards
- No contradictory advice introduced
- Examples follow project conventions
- Terminology is consistent

### Step 4: Check for Breaking Changes

Verify no unintended breaking changes in documentation updates.

### Step 5: Block if Re-validation Fails

**DO NOT archive the spec** if re-validation fails:

- Fix issues introduced by documentation updates
- Re-run re-validation
- Only proceed to archival when all checks pass

## Workspace Management

### Folder Structure

```
specs/
├── active/                     # Active work (gitignored)
│   ├── {feature-name}/
│   │   ├── prd.md             # Product Requirements Document
│   │   ├── tasks.md           # Phase-by-phase checklist
│   │   ├── recovery.md        # Session resume guide
│   │   ├── research/          # Research findings
│   │   └── tmp/               # Temporary files (cleaned by Docs & Vision)
│   └── .gitkeep
├── archive/                    # Completed work (gitignored)
│   └── {completed-feature}/
├── template-spec/              # Template structure (committed)
│   ├── prd.md
│   ├── tasks.md
│   ├── recovery.md
│   ├── README.md
│   ├── research/.gitkeep
│   └── tmp/.gitkeep
└── README.md
```

### Lifecycle

1. **Planning**: PRD agent creates `specs/active/{feature}/`
2. **Implementation**: Expert implements, auto-invokes Testing and Docs & Vision
3. **Testing**: Testing agent creates comprehensive test suite (automatic)
4. **Documentation**: Docs & Vision updates docs (automatic)
5. **Knowledge Capture**: Docs & Vision extracts patterns (automatic)
6. **Re-validation**: Docs & Vision verifies consistency (automatic)
7. **Archive**: Docs & Vision moves to `specs/archive/{feature}/` (automatic)

### Session Resumability

Any agent can resume work by reading:

1. `specs/active/{feature}/recovery.md` - Current status and next steps
2. `specs/active/{feature}/tasks.md` - What's complete
3. `specs/active/{feature}/prd.md` - Full requirements
4. `specs/active/{feature}/research/` - Findings and analysis
