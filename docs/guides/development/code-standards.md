# Code Quality Standards

This guide defines the mandatory code quality standards for SQLSpec development. These standards ensure consistency, maintainability, and mypyc compatibility across the codebase.

## Type Annotation Standards

### Prohibited Patterns

```python
# PROHIBITED - Never use future annotations
from __future__ import annotations
```

### Required Patterns

```python
# REQUIRED - Stringified type hints for non-builtin types
def process_config(config: "SQLConfig") -> "SessionResult":
    ...

# REQUIRED - PEP 604 pipe syntax for unions
def get_value(key: str) -> str | None:
    ...

# REQUIRED - Stringified built-in generics
def get_items() -> "list[str]":
    ...

def get_mapping() -> "dict[str, int]":
    ...

# REQUIRED - Tuple for __all__ definitions
__all__ = ("MyClass", "my_function", "CONSTANT")
```

## Import Standards

### Organization

Imports must be organized in this order with blank lines between groups:
1. Standard library
2. Third-party packages
3. First-party (sqlspec)

```python
import logging
from typing import TYPE_CHECKING, Any

from sqlglot import exp

from sqlspec.core.result import SQLResult
from sqlspec.protocols import SupportsWhere
```

### Nested Import Rules

- **ALL imports at module level** by default
- **ONLY nest imports when preventing circular imports**
- Third-party packages may be nested for **optional dependencies only**

```python
# BAD - Unnecessary nested import
def process_data(self):
    from sqlspec.protocols import DataProtocol  # NO!
    ...

# GOOD - All imports at top
from sqlspec.protocols import DataProtocol

def process_data(self):
    ...

# ACCEPTABLE - Only for circular import prevention
if TYPE_CHECKING:
    from sqlspec.statement.sql import SQL

# ACCEPTABLE - Optional dependency
def use_numpy_feature():
    try:
        import numpy as np
    except ImportError:
        raise ImportError("numpy required for this feature")
```

## Clean Code Principles

### Code Clarity

- Write self-documenting code - minimize comments
- Extract complex conditions to well-named variables/methods
- Use early returns over nested if blocks
- Place guard clauses at function start

### Naming Conventions

- **Variables/Functions**: Descriptive names explaining purpose, not type
- **No abbreviations** unless widely understood (e.g., `db`, `sql`, `config`)
- **Boolean variables** as questions: `is_valid`, `has_data`, `can_execute`
- **Functions** as verbs: `process_query()`, `validate_config()`, `execute_batch()`

### Function Length

- **Maximum**: 75 lines per function (including docstring)
- **Preferred**: 30-50 lines for most functions
- Split longer functions into smaller helpers

### Anti-Patterns to Avoid

```python
# BAD - Defensive programming with hasattr
if hasattr(obj, 'method') and obj.method:
    result = obj.method()

# GOOD - Use type guards
from sqlspec.utils.type_guards import supports_where

if supports_where(obj):
    result = obj.where("condition")
```

## Performance Patterns

### PERF401 - List Comprehensions

```python
# BAD - Manual list building
result = []
for item in items:
    if condition(item):
        result.append(transform(item))

# GOOD - List comprehension
result = [transform(item) for item in items if condition(item)]
```

### PLR2004 - Magic Value Constants

```python
# BAD - Magic numbers
if len(parts) != 2:
    raise ValueError("Invalid format")

# GOOD - Named constants
URI_PARTS_MIN_COUNT = 2
if len(parts) != URI_PARTS_MIN_COUNT:
    raise ValueError("Invalid format")
```

### TRY301 - Abstract Raise Statements

```python
# BAD - Raise in function body
def process(self, data):
    if not data:
        msg = "Data is required"
        raise ValueError(msg)

# GOOD - Abstract to helper
def process(self, data):
    if not data:
        self._raise_data_required()

def _raise_data_required(self):
    msg = "Data is required"
    raise ValueError(msg)
```

## Error Handling Standards

### Custom Exceptions

All custom exceptions in `sqlspec/exceptions.py` must inherit from `SQLSpecError`:

```python
from sqlspec.exceptions import SQLSpecError

class AdapterError(SQLSpecError):
    """Error specific to database adapter operations."""
```

### Exception Handling Pattern

```python
from sqlspec.exceptions import wrap_exceptions

async def execute(self, sql: str) -> None:
    with wrap_exceptions():
        await self._connection.execute(sql)
```

### Two-Tier Error Handling

When processing user input that may be incomplete or malformed:

**Tier 1: Graceful Skip (Expected Incomplete Input)**
- Condition: Input lacks required markers but is otherwise valid
- Action: Return empty result (empty dict, None, etc.)
- Log level: DEBUG

**Tier 2: Hard Error (Malformed Input)**
- Condition: Input has required markers but is malformed
- Action: Raise specific exception with clear message
- Log level: ERROR (via exception handler)

```python
def parse_user_input(content: str, source: str) -> "dict[str, Result]":
    """Parse user input with two-tier error handling.

    Args:
        content: Raw input content to parse.
        source: Source identifier for error reporting.

    Returns:
        Dictionary of parsed results. Empty dict if no required markers found.

    Raises:
        ParseError: If required markers are present but malformed.
    """
    markers = list(MARKER_PATTERN.finditer(content))
    if not markers:
        return {}  # Tier 1: Graceful skip

    results = {}
    for marker in markers:
        if malformed_marker(marker):
            raise ParseError(source, "Malformed marker")  # Tier 2: Hard error
        results[marker.name] = process(marker)

    return results
```

## Logging Standards

### Basic Rules

- Use `logging` module, **NEVER `print()`**
- NO f-strings in log messages - use lazy formatting
- Provide meaningful context in all log messages

### Log Level Guidelines

**DEBUG**: Expected behavior that aids troubleshooting
- Files gracefully skipped during batch processing
- Optional features not enabled (dependencies missing)
- Cache hits/misses
- Internal state transitions

**INFO**: Significant events during normal operation
- Connection pool created
- Migration applied successfully
- Background task started

**WARNING**: Unexpected but recoverable conditions
- Retrying after transient failure
- Falling back to alternative implementation
- Configuration using deprecated options

### Context Requirements

Always include `extra` dict with:
- Primary identifier (file_path, query_name, etc.)
- Correlation ID via `CorrelationContext.get()`
- Additional relevant context (size, duration, etc.)

```python
logger.debug(
    "Skipping SQL file without named statements: %s",
    path_str,
    extra={
        "file_path": path_str,
        "correlation_id": CorrelationContext.get(),
    },
)
```

## Documentation Standards

### Docstrings (Google Style)

- All public modules, classes, functions need docstrings
- Include `Args:`, `Returns:`, `Yields:`, `Raises:` sections with types
- Don't document return if `None`
- Focus on WHY not WHAT

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

### Project Documentation

- Update `docs/` for new features and API changes
- Build locally: `make docs` before submission
- Use reStructuredText (.rst) and Markdown (.md via MyST)

## Mypyc-Compatible Class Pattern

For data-holding classes in `sqlspec/core/` and `sqlspec/driver/`:

```python
class MyMetadata:
    __slots__ = ("field1", "field2", "optional_field")

    def __init__(self, field1: str, field2: int, optional_field: str | None = None) -> None:
        self.field1 = field1
        self.field2 = field2
        self.optional_field = optional_field

    def __repr__(self) -> str:
        return f"MyMetadata(field1={self.field1!r}, field2={self.field2!r}, optional_field={self.optional_field!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MyMetadata):
            return NotImplemented
        return (
            self.field1 == other.field1
            and self.field2 == other.field2
            and self.optional_field == other.optional_field
        )

    def __hash__(self) -> int:
        return hash((self.field1, self.field2, self.optional_field))
```

**Key Principles:**
- `__slots__` reduces memory and speeds up attribute access
- Explicit `__init__`, `__repr__`, `__eq__`, `__hash__` for full control
- Avoid `@dataclass` decorators in mypyc-compiled modules

## Testing Standards

### Function-Based Tests Only

```python
# GOOD - Function-based test
def test_config_validation():
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://..."})
    assert config.is_async is True

# BAD - Class-based test (PROHIBITED)
class TestConfig:
    def test_validation(self):
        ...
```

### Test Isolation for Pooled Connections

Use unique temporary database files per test instead of `:memory:`:

```python
import tempfile

def test_starlette_autocommit_mode() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "autocommit"}}
        )
        # Test logic - each test gets isolated database
```

### CLI Config Loader Isolation

- Generate unique module namespace for each test: `cli_test_config_<uuid>`
- Place temporary config modules inside `tmp_path`
- Register via `sys.modules` within test, delete during teardown
- Patch `Path.cwd()` or provide explicit path arguments
