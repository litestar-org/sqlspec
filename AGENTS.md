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

### Logging Standards

- Use `logging` module, NEVER `print()`
- NO f-strings in log messages - use lazy formatting
- Provide meaningful context in all log messages

### Documentation Standards

**Docstrings (Google Style - MANDATORY)**:

- All public modules, classes, functions need docstrings
- Include `Args:`, `Returns:`, `Yields`, `Raises:` sections with types
- Don't document return if `None`
- Sphinx-compatible format
- Focus on WHY not WHAT

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

Current state of all adapters (as of type-cleanup branch):

| Adapter    | TypedDict | Auto-Detect | enable_ Prefix | Defaults | Grade      | Notes                                    |
|------------|-----------|-------------|----------------|----------|------------|------------------------------------------|
| Oracle     | ✅        | ✅          | ✅             | ✅       | Gold       | Perfect implementation, reference model  |
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

## Agent Workflow Coordination

### Automated Multi-Agent Workflow

SQLSpec uses a coordinated multi-agent system where the Expert agent orchestrates the complete development lifecycle:

```
User runs: /implement {feature-name}

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

- **General Rule**: Tell Codex which phase to emulate (`plan`, `implement`, `test`, `review`) and point to the active workspace root (`specs/active/{slug}/` preferred). Codex will create the folder if it does not exist.
- **Codex `/plan` Equivalent**: Ask “Codex: run planning for {feature}” and provide any context. Codex must (1) research via docs/guides/ as outlined in `.claude/agents/planner.md`, (2) write or update `prd.md`, `tasks.md`, `research/plan.md`, `recovery.md`, and (3) ensure `tmp/` exists. Planning output follows the same structure the Planner agent would create.
- **Codex `/implement` Equivalent**: Ask Codex to “execute implementation phase for {workspace}”. Codex then reads the workspace, consults guides, writes code under `sqlspec/`, updates tasks, and runs local checks exactly as described in `.claude/agents/expert.md`. When the plan calls for sub-agents, Codex continues by emulating the Testing and Docs & Vision phases in order.
- **Codex `/test` Equivalent**: Request “Codex: perform testing phase for {workspace}”. Codex creates or updates pytest suites, ensures coverage thresholds, and records progress in `tasks.md`, mirroring `.claude/agents/testing.md`.
- **Codex `/review` Equivalent**: Request “Codex: run docs, quality gate, and cleanup for {workspace}”. Codex completes the five Docs & Vision phases—documentation, quality gate, knowledge capture (including AGENTS.md and guides updates), re-validation, and workspace archival.
- **Knowledge Capture Reminder**: Whenever Codex finishes implementation or review work, it must update this AGENTS.md and any relevant guides with new patterns so Claude and other assistants inherit the learnings.

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

1. **Planning**: Planner creates `specs/active/{feature}/`
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
