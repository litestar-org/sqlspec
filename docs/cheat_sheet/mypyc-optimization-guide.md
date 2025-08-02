# MyPyC Optimization Guide: Do's and Don'ts

**A comprehensive guide for writing high-performance, mypyc-compilable Python code**

## Table of Contents

1. [Core Principles](#core-principles)
2. [Type Annotations](#type-annotations)
3. [Class Design](#class-design)
4. [Native Class Control](#native-class-control)
5. [Generic Types](#generic-types)
6. [Performance Patterns](#performance-patterns)
7. [Memory Management](#memory-management)
8. [Function Design](#function-design)
9. [Compilation Units](#compilation-units)
10. [Common Pitfalls](#common-pitfalls)
11. [Performance Optimization Tips](#performance-optimization-tips)
12. [SQLSpec-Specific Guidelines](#sqlspec-specific-guidelines)
13. [Quick Reference](#quick-reference)

---

## Core Principles

### ✅ DO: Write Type-First Code

```python
# ✅ GOOD: Explicit types enable optimization
def calculate_sum(numbers: list[int]) -> int:
    total: int = 0
    for num in numbers:
        total += num
    return total

# ❌ BAD: Untyped code falls back to slow Python
def calculate_sum(numbers):
    total = 0
    for num in numbers:
        total += num
    return total
```

### ✅ DO: Use Primitive Types When Possible

```python
# ✅ GOOD: Primitive types are unboxed and fast
def process_flags(active: bool, count: int) -> int:
    if active:
        return count * 2
    return count

# ❌ BAD: Any type prevents optimization
def process_flags(active: Any, count: Any) -> Any:
    if active:
        return count * 2
    return count
```

---

## Type Annotations

### 1. Complete Type Coverage

```python
# ✅ DO: Annotate all parameters and return types
def parse_data(raw: str, delimiter: str = ",") -> list[str]:
    return raw.split(delimiter)

# ❌ DON'T: Leave types incomplete
def parse_data(raw: str, delimiter=","):  # Missing return type
    return raw.split(delimiter)
```

### 2. Use Specific Types

```python
# ✅ DO: Be as specific as possible
from typing import Final, Literal

OperationType = Literal["SELECT", "INSERT", "UPDATE", "DELETE"]

class QueryConfig:
    MAX_RETRIES: Final[int] = 3
    TIMEOUT: Final[float] = 30.0

# ❌ DON'T: Use overly broad types
class QueryConfig:
    MAX_RETRIES = 3  # Type not clear
    TIMEOUT: Any = 30.0  # Too broad
```

### 3. Native Integer Types for Performance

```python
# ✅ DO: Use native integer types for performance-critical code
from mypy_extensions import i64, i32

def fast_sum(values: list[i64]) -> i64:
    total: i64 = 0
    for val in values:
        total += val
    return total

# ❌ DON'T: Mix native types carelessly
def bad_sum(x: i64, y: i32) -> i64:
    return x + y  # Error: incompatible types
```

---

## Class Design

### 1. Simple, Non-Generic Classes

```python
# ✅ DO: Create simple, concrete classes
class SQLResult:
    """Non-generic result class optimized for mypyc."""

    def __init__(self, data: list[dict[str, Any]]) -> None:
        self.data = data
        self.row_count = len(data)

    def first(self) -> Optional[dict[str, Any]]:
        return self.data[0] if self.data else None

# ❌ DON'T: Use generics unless absolutely necessary
class SQLResult(Generic[T]):  # Generic prevents mypyc compilation
    def __init__(self, data: list[T]) -> None:
        self.data = data
```

### 2. Use `__slots__` for Memory Efficiency

```python
# ✅ DO: Define __slots__ for native classes
class Connection:
    __slots__ = ("host", "port", "database", "_connected")

    def __init__(self, host: str, port: int, database: str) -> None:
        self.host = host
        self.port = port
        self.database = database
        self._connected = False

# ❌ DON'T: Use dynamic attributes
class Connection:
    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)  # Dynamic attributes prevent optimization
```

### 3. Traits for Multiple Inheritance

```python
# ✅ DO: Use traits for shared behavior
from mypy_extensions import trait

@trait
class Queryable:
    def execute(self, sql: str) -> None: ...

class PostgresDriver(BaseDriver, Queryable):  # Trait comes after base class
    def execute(self, sql: str) -> None:
        # Implementation
        pass

# ❌ DON'T: Put traits before non-trait base classes
class PostgresDriver(Queryable, BaseDriver):  # Wrong order!
    pass
```

---

## Native Class Control

### 1. Allowing Interpreted Subclasses

```python
# ✅ DO: Use @mypyc_attr when you need flexibility
from mypy_extensions import mypyc_attr

@mypyc_attr(allow_interpreted_subclasses=True)
class BasePlugin:
    """Base class that can be subclassed by non-compiled code."""
    def process(self, data: dict[str, Any]) -> dict[str, Any]:
        return data

# This allows users to extend your class without compilation
class UserPlugin(BasePlugin):  # Works even if not compiled
    def process(self, data: dict[str, Any]) -> dict[str, Any]:
        # Custom processing
        return {**data, "processed": True}

# ❌ DON'T: Prevent extension when it's needed
class BasePlugin:  # Native class by default
    def process(self, data: dict[str, Any]) -> dict[str, Any]:
        return data

# This will fail at runtime if UserPlugin is not compiled
class UserPlugin(BasePlugin):  # Error!
    pass
```

### 2. Controlling Class Compilation

```python
# ✅ DO: Exclude classes that can't be compiled
# In mypyc config or command line:
# --exclude-class MyModule.ProblematicClass

# Or use interpreted base when needed
@mypyc_attr(allow_interpreted_subclasses=True)
class FlexibleBase:
    """Use this for classes that need dynamic features."""
    pass

# ✅ DO: Create separate base classes for compiled/interpreted code
class NativeBase:
    """Fast compiled base for internal use."""
    __slots__ = ("_data",)

    def __init__(self, data: list[int]) -> None:
        self._data = data

@mypyc_attr(allow_interpreted_subclasses=True)
class ExtensibleBase:
    """Flexible base for user extensions."""
    def __init__(self, data: list[int]) -> None:
        self._data = data
```

---

## Generic Types

### 1. Avoid Generics in Hot Paths

```python
# ✅ DO: Use concrete types or type unions
ResultData = Union[list[dict[str, Any]], dict[str, Any]]

class QueryResult:
    def __init__(self, data: ResultData) -> None:
        self.data = data

# ❌ DON'T: Use generics for core classes
class QueryResult(Generic[T]):
    def __init__(self, data: T) -> None:
        self.data = data
```

### 2. Type Casting for Native Performance

```python
# ✅ DO: Use cast() to maintain type safety with native types
from typing import cast

class PostgresDriver:
    def execute(self, query: str) -> SQLResult:
        # Native psycopg returns psycopg.Row objects
        native_rows = cursor.fetchall()
        # Cast for type checker, no runtime conversion
        return SQLResult(data=cast("list[dict[str, Any]]", native_rows))

# ❌ DON'T: Convert data unnecessarily
def execute(self, query: str) -> SQLResult:
    native_rows = cursor.fetchall()
    # Expensive conversion at runtime
    return SQLResult(data=[dict(row) for row in native_rows])
```

---

## Performance Patterns

### 1. Early Binding with Final

```python
# ✅ DO: Use Final for constants
from typing import Final

MAX_CONNECTIONS: Final[int] = 100
DEFAULT_TIMEOUT: Final[float] = 30.0

def check_limit(count: int) -> bool:
    # Compiler can inline MAX_CONNECTIONS
    return count < MAX_CONNECTIONS

# ❌ DON'T: Use mutable module-level variables
MAX_CONNECTIONS = 100  # Late binding, slower access

def check_limit(count: int) -> bool:
    return count < MAX_CONNECTIONS  # Requires runtime lookup
```

### 2. Optimize Loops

```python
# ✅ DO: Use typed, efficient loops
def process_records(records: list[dict[str, Any]]) -> int:
    count: int = 0
    for record in records:  # Direct iteration
        if record.get("active"):
            count += 1
    return count

# ❌ DON'T: Use unnecessary comprehensions or functional constructs
def process_records(records: list[dict[str, Any]]) -> int:
    # Creates intermediate list
    return len([r for r in records if r.get("active")])
```

### 3. Minimize Allocations

```python
# ✅ DO: Reuse objects when possible
class QueryBuilder:
    def __init__(self) -> None:
        self._parts: list[str] = []

    def add_clause(self, clause: str) -> None:
        self._parts.append(clause)  # Modify in place

    def build(self) -> str:
        return " ".join(self._parts)

# ❌ DON'T: Create unnecessary intermediate objects
class QueryBuilder:
    def __init__(self) -> None:
        self._query = ""

    def add_clause(self, clause: str) -> None:
        self._query = self._query + " " + clause  # Creates new strings
```

---

## Memory Management

### 1. Value vs Reference Types

```python
# ✅ DO: Understand boxing/unboxing
def efficient_sum() -> int:
    # Small integers use value representation (unboxed)
    total: int = 0
    for i in range(100):
        total += i  # Fast unboxed arithmetic
    return total

# Lists always contain boxed values
numbers: list[int] = [1, 2, 3]  # Integers are boxed in list
x: int = numbers[0]  # Automatically unboxed on access
```

### 2. Garbage Collection Tuning

```python
# ✅ DO: Adjust GC for batch operations
import gc

def process_large_dataset(data: list[dict[str, Any]]) -> None:
    # Reduce GC overhead for performance-critical sections
    gc.set_threshold(150000)

    try:
        # Process data...
        pass
    finally:
        # Restore default
        gc.set_threshold(700)
```

---

## Function Design

### 1. Type Narrowing

```python
# ✅ DO: Help the compiler with type narrowing
def process_value(val: Union[int, str]) -> str:
    if isinstance(val, int):
        # Compiler knows val is int here
        return str(val * 2)
    else:
        # Compiler knows val is str here
        return val.upper()

# ❌ DON'T: Use hasattr/getattr patterns
def process_value(val: Any) -> Any:
    if hasattr(val, "upper"):  # Prevents optimization
        return val.upper()
    return str(val)
```

### 2. Avoid Dynamic Features

```python
# ✅ DO: Use static method calls
class DataProcessor:
    def process(self, data: list[int]) -> int:
        return sum(data)

processor = DataProcessor()
result = processor.process([1, 2, 3])  # Direct call

# ❌ DON'T: Use dynamic dispatch
method_name = "process"
method = getattr(processor, method_name)  # Dynamic lookup
result = method([1, 2, 3])  # Slow call
```

---

## Compilation Units

### 1. Organizing Compiled Modules with Hatch-MyPyC

```toml
# ✅ DO: Configure compilation in pyproject.toml using hatch-mypyc

# pyproject.toml
[build-system]
requires = ["hatchling", "hatch-mypyc"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel.hooks.mypyc]
dependencies = ["hatch-mypyc"]
enable-by-default = false  # Opt-in compilation

# Exclude files that shouldn't be compiled
exclude = [
    "tests/**",              # Test files
    "sqlspec/__main__.py",   # Entry points (can't run directly when compiled)
    "sqlspec/cli.py",        # CLI modules (not performance critical)
    "sqlspec/**/__init__.py" # Init files (usually just imports)
]

# Include only hot-path modules for compilation
include = [
    # Core SQL processing (profiled bottlenecks)
    "sqlspec/statement/sql.py",
    "sqlspec/statement/parameters.py",
    "sqlspec/statement/pipeline.py",

    # Query builders
    "sqlspec/statement/builder/_base.py",
    "sqlspec/statement/builder/_select.py",
    "sqlspec/statement/builder/_insert.py",

    # Driver core
    "sqlspec/driver/parameters.py",

    # Utilities used in hot paths
    "sqlspec/utils/type_guards.py",
    "sqlspec/utils/statement_hashing.py",
]

# MyPy compilation arguments
mypy-args = [
    "--ignore-missing-imports",
    "--allow-untyped-defs",
    "--no-implicit-reexport",
    "--follow-imports=skip",
]

# Compilation options
[tool.hatch.build.targets.wheel.hooks.mypyc.options]
opt_level = "3"    # Maximum optimization
multi_file = true  # Cross-module optimization
debug_level = "0"  # No debug info in production
```

### Package Structure Example

```python
# ✅ DO: Organize for selective compilation

# sqlspec/
# ├── __init__.py          # NOT compiled (just imports)
# ├── statement/
# │   ├── __init__.py      # NOT compiled
# │   ├── sql.py           # COMPILED (hot path) 
# │   └── result.py        # COMPILED (used everywhere)
# ├── adapters/
# │   ├── __init__.py      # NOT compiled
# │   ├── base.py          # Mixed (@mypyc_attr for extensibility)
# │   └── sqlite/
# │       └── driver.py    # NOT compiled (uses native sqlite3)
# └── utils/
#     ├── __init__.py      # NOT compiled
#     └── type_guards.py   # COMPILED (used in hot paths)
```

### 2. Building and Testing Compiled Modules

```bash
# ✅ DO: Use SQLSpec's Makefile commands for compilation

# Install with mypyc compilation (UPDATED COMMANDS)
$ HATCH_BUILD_HOOKS_ENABLE=1 uv sync --all-extras --dev
# This will:
# - Enable hatch-mypyc build hook
# - Install in editable mode with compilation
# - Compile all hot-path modules to .so files
# - Install all development dependencies

# Alternative: Force reinstall with compilation
$ HATCH_BUILD_HOOKS_ENABLE=1 uv pip install -e . --force-reinstall

# Regular installation (for development)
$ make install

# Build compiled wheel for distribution
$ HATCH_BUILD_HOOKS_ENABLE=1 uv build --wheel

# Test installation and compilation verification
$ make install
$ make test

# ✅ DO: Verify compilation after installation
$ python -c "import sqlspec.statement.sql; print(sqlspec.statement.sql.__file__)"
# Should show: .../sqlspec/statement/sql.cpython-312-x86_64-linux-gnu.so

# Count compiled modules
$ find sqlspec -name "*.so" | wc -l
# Should match the number of modules in your include list
```

### 3. Profiling to Identify Hot Paths

```python
# ✅ DO: Profile before deciding what to compile
import cProfile
import pstats
from sqlspec import create_engine

# Profile your application
profiler = cProfile.Profile()
profiler.enable()

# Run typical workload
engine = create_engine("sqlite:///:memory:")
for _ in range(1000):
    engine.execute("SELECT * FROM users WHERE id = ?", [1])

profiler.disable()

# Analyze results
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(20)  # Top 20 functions

# Look for:
# - Functions called frequently (high ncalls)
# - Functions with high cumulative time
# - Functions in YOUR code (not libraries)
# These are candidates for the 'include' list
```

### 4. Development Workflow

```python
# ✅ DO: Use separate compilation for development
# config.py - Not compiled, can be changed without recompilation
from typing import Final

DEBUG: Final[bool] = False
MAX_RETRIES: Final[int] = 3

# core.py - Compiled, imports config
from .config import DEBUG, MAX_RETRIES

def process() -> None:
    if DEBUG:  # Checked at runtime
        print("Debug mode")
    # ... rest of processing
```

### 5. SQLSpec's MyPyC Configuration

```toml
# ✅ CURRENT: SQLSpec's actual configuration from pyproject.toml
[tool.hatch.build.targets.wheel.hooks.mypyc]
dependencies = ["hatch-mypyc"]
enable-by-default = false
exclude = [
    "tests/**",                          # Test files
    "sqlspec/__main__.py",               # Entry point (can't run directly when compiled)
    "sqlspec/cli.py",                    # CLI module (not performance critical)
    "sqlspec/typing.py",                 # Type aliases
    "sqlspec/_typing.py",                # Type aliases
    "sqlspec/adapters/*/config.py",      # Configuration classes
    "sqlspec/adapters/*/_types.py",      # Types classes Often not found during mypy checks
    "sqlspec/config.py",                 # Main config
    "sqlspec/**/__init__.py",            # Init files (usually just imports)
]
include = [
 "sqlspec/statement/**/*.py", # All statement-related modules
  "sqlspec/loader.py",

  # === DRIVER CORE ===
  "sqlspec/driver/*.py",
  "sqlspec/driver/mixins/*.py",
  "sqlspec/parameters/**/*.py",

  # === STORAGE LAYER ===
  "sqlspec/storage/registry.py",
  "sqlspec/storage/capabilities.py",
  "sqlspec/storage/backends/obstore.py",
  "sqlspec/storage/backends/fsspec.py",

  # === CORE ADAPTERS ===
  "sqlspec/adapters/*/*.py", # All adapters

  # === UTILITY MODULES ===
  "sqlspec/utils/statement_hashing.py",
  "sqlspec/utils/text.py",
  "sqlspec/utils/sync_tools.py",
  "sqlspec/utils/type_guards.py",
  "sqlspec/utils/fixtures.py",
]
mypy-args = [
  "--ignore-missing-imports",
  "--allow-untyped-defs",
  "--allow-untyped-globals",
  "--no-implicit-reexport",
  "--no-warn-redundant-casts",
  "--no-warn-unused-ignores",
  "--follow-imports=skip",
]
require-runtime-dependencies = true
require-runtime-features = ["performance"]  # sqlglot[rs], msgspec

# Compilation options for maximum performance
[tool.hatch.build.targets.wheel.hooks.mypyc.options]
opt_level = "3"    # Maximum optimization (0-3)
multi_file = true  # Enable cross-module optimization
debug_level = "0"  # No debug info in production (0-2)
```

---

## Common Pitfalls

### 1. Type Erasure Awareness

```python
# ⚠️ CAUTION: Container types are erased at runtime
from typing import Any

def risky_operation(items: list[Any]) -> None:
    typed_list: list[int] = items  # No runtime check!
    # This will fail at runtime if items contains non-integers
    for item in typed_list:
        print(item + 1)  # Runtime error if item is not int

# ✅ BETTER: Validate at boundaries
def safe_operation(items: list[Any]) -> None:
    typed_list: list[int] = []
    for item in items:
        if isinstance(item, int):
            typed_list.append(item)
    # Now safe to use
```

### 2. Import Patterns

```python
# ✅ DO: Import from compiled modules
from sqlspec.statement.result import SQLResult  # Compiled module

# ❌ DON'T: Use circular imports or dynamic imports
def get_result_class():
    from sqlspec.statement.result import SQLResult  # Late import
    return SQLResult
```

### 3. Exception Handling

```python
# ✅ DO: Type exception handling properly
def safe_divide(a: int, b: int) -> Optional[float]:
    try:
        return float(a) / float(b)
    except ZeroDivisionError:
        return None

# ❌ DON'T: Use bare except or overly broad catches
def unsafe_divide(a: int, b: int) -> Any:
    try:
        return a / b
    except:  # Too broad, prevents optimization
        return None
```

---

## SQLSpec-Specific Guidelines

### 1. Parameter Handling

```python
# ✅ DO: Use TypedParameter for type preservation
from sqlspec.parameters.types import TypedParameter

class TypedParameter:
    __slots__ = ("name", "value", "type")

    def __init__(self, name: str, value: Any, type_: type) -> None:
        self.name = name
        self.value = value
        self.type = type_

# ❌ DON'T: Lose type information
params = {"name": "value"}  # Type information lost
```

### 2. File Caching Optimization

```python
# ✅ DO: Use regular class with __slots__ for optimal MyPyC performance
class CachedSQLFile:
    """Cached SQL file with parsed queries for efficient reloading.
    
    CRITICAL: Uses regular class with __slots__ instead of @dataclass
    for MyPyC compilation compatibility and optimal performance.
    This pattern provides 22x speedup after compilation.
    """

    __slots__ = ("parsed_queries", "query_names", "sql_file")

    def __init__(self, sql_file: SQLFile, parsed_queries: dict[str, str]) -> None:
        """Initialize cached SQL file."""
        self.sql_file = sql_file
        self.parsed_queries = parsed_queries
        self.query_names = list(parsed_queries.keys())

# ❌ DON'T: Use @dataclass for performance-critical cached structures
@dataclass
class CachedSQLFile:  # This prevents MyPyC optimization
    sql_file: SQLFile
    parsed_queries: dict[str, str]
    query_names: list[str] = field(init=False)
```

### 3. Single-Pass Processing

```python
# ✅ DO: Process once, validate once
def process_query(sql: str) -> SQLResult:
    # Parse once
    ast = parse_sql(sql)
    # Transform once
    optimized = optimize_ast(ast)
    # Execute once
    return execute_ast(optimized)

# ❌ DON'T: Multiple passes over data
def process_query(sql: str) -> SQLResult:
    # Parse for validation
    ast = parse_sql(sql)
    validate_ast(ast)
    # Parse again for optimization
    ast = parse_sql(sql)  # Redundant!
    optimized = optimize_ast(ast)
    return execute_ast(optimized)
```

### 4. Adapter Pattern

```python
# ✅ DO: Inherit from typed mixins
from sqlspec.driver.mixins import SyncStorageMixin

class SQLiteDriver(SyncStorageMixin["sqlite3.Connection", "sqlite3.Row"]):
    def _execute(self, statement: SQL, connection: "sqlite3.Connection") -> SQLResult:
        cursor = connection.execute(statement.sql)
        # Use cast for type safety without conversion
        return SQLResult(
            data=cast("list[dict[str, Any]]", cursor.fetchall()),
            statement=statement
        )
```

---

## Performance Optimization Tips

### 1. Hot Path Optimization

```python
# ✅ DO: Profile and focus on hot paths
import cProfile
import pstats

# Profile to find bottlenecks
profiler = cProfile.Profile()
profiler.enable()
# ... your code ...
profiler.disable()
stats = pstats.Stats(profiler).sort_stats('cumulative')
stats.print_stats(10)  # Top 10 functions

# Then optimize the hot paths with:
# - Type annotations
# - Native operations
# - Reduced allocations
```

### 2. Batch Operations

```python
# ✅ DO: Process in batches to amortize overhead
def process_batch(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Pre-allocate result list
    results: list[dict[str, Any]] = [{}] * len(items)

    for i, item in enumerate(items):
        # Process in-place when possible
        results[i] = {"id": item["id"], "processed": True}

    return results

# ❌ DON'T: Process one at a time with function call overhead
def process_single(item: dict[str, Any]) -> dict[str, Any]:
    return {"id": item["id"], "processed": True}

# Calling this in a loop has more overhead
results = [process_single(item) for item in items]
```

### 3. String Operations

```python
# ✅ DO: Use string methods efficiently
def format_output(values: list[str]) -> str:
    # Single join is efficient
    return ", ".join(values)

# ❌ DON'T: Concatenate in loops
def format_output_slow(values: list[str]) -> str:
    result = ""
    for i, value in enumerate(values):
        if i > 0:
            result += ", "  # Creates new string each time
        result += value
    return result
```

### 4. Attribute Access Optimization

```python
# ✅ DO: Cache attribute lookups in tight loops
def process_records(records: list[Record]) -> int:
    total = 0
    # Cache method lookup outside loop
    get_value = Record.get_value

    for record in records:
        total += get_value(record)

    return total

# ❌ DON'T: Repeated attribute lookups
def process_records_slow(records: list[Record]) -> int:
    total = 0
    for record in records:
        # Attribute lookup on each iteration
        total += record.get_value()
    return total
```

### 5. Fast Exit Paths

```python
# ✅ DO: Check common cases first
def validate_data(data: Any) -> bool:
    # Fast path for common case
    if data is None:
        return False

    # More expensive checks later
    if not isinstance(data, dict):
        return False

    # Most expensive validation last
    return all(
        isinstance(k, str) and isinstance(v, (str, int, float))
        for k, v in data.items()
    )
```

---

## MyPyC Behavioral Differences

### 1. Runtime Type Checking

```python
# ⚠️ DIFFERENCE: MyPyC enforces types at runtime
def strict_function(x: int) -> int:
    return x * 2

# In regular Python: works (returns "aa")
# In compiled code: TypeError!
strict_function("a")

# ✅ DO: Add runtime validation at boundaries
def api_function(x: Any) -> int:
    if not isinstance(x, int):
        raise TypeError(f"Expected int, got {type(x)}")
    return strict_function(x)
```

### 2. Early Binding Behavior

```python
# ⚠️ DIFFERENCE: Module-level values use late binding
# Regular module-level variable
CONFIG_VALUE = "initial"

def get_config() -> str:
    return CONFIG_VALUE  # Late binding

# But Final values use early binding
from typing import Final
CONFIG_FINAL: Final = "initial"

def get_config_fast() -> str:
    return CONFIG_FINAL  # Early binding, faster

# Class attributes also use early binding
class Config:
    VALUE = "initial"

    @classmethod
    def get(cls) -> str:
        return cls.VALUE  # Early binding
```

### 3. Native Class Restrictions

```python
# ⚠️ DIFFERENCE: Native classes have restrictions

# Native classes cannot:
# - Have metaclasses (except type)
# - Have __del__ methods
# - Multiple inheritance from native classes
# - Be used with most decorators that create wrapper classes

# ✅ DO: Design around limitations
class NativeClass:
    """Optimized for compilation."""
    __slots__ = ("_data",)

    def __init__(self, data: list[int]) -> None:
        self._data = data

    # Use context managers instead of __del__
    def __enter__(self) -> "NativeClass":
        return self

    def __exit__(self, *args: Any) -> None:
        # Cleanup code here
        self._data.clear()
```

### 4. Exception Handling Costs

```python
# ⚠️ DIFFERENCE: try/except has higher overhead in compiled code

# ✅ DO: Check before exception
def safe_divide(a: float, b: float) -> Optional[float]:
    if b == 0:
        return None
    return a / b

# ❌ DON'T: Rely on exception for control flow
def unsafe_divide(a: float, b: float) -> Optional[float]:
    try:
        return a / b
    except ZeroDivisionError:
        return None  # More expensive in compiled code
```

---

## Quick Reference

### Essential Rules

1. **Always use type annotations** - Every parameter, return value, and variable
2. **Avoid Generic[T] inheritance** - Use concrete types or Union types
3. **Use Final for constants** - Enables compile-time optimization
4. **Prefer cast() over conversion** - Maintain native performance
5. **Define `__slots__`** - Better memory usage and access speed
6. **Type narrow with isinstance** - Help the compiler optimize
7. **Avoid dynamic features** - No getattr, setattr, **kwargs abuse
8. **Use native int types** - i64/i32 for performance-critical paths

### Type Annotation Patterns

```python
# String annotations for forward references
def process(self) -> "SQLResult":
    pass

# Union instead of Optional
result: Union[dict[str, Any], None]  # Not Optional[dict[str, Any]]

# Literal types for enums
OpType = Literal["SELECT", "INSERT", "UPDATE", "DELETE"]

# Cast for type assertions
data = cast("list[dict[str, Any]]", native_result)

# Final for constants
BUFFER_SIZE: Final[int] = 8192
```

### Performance Checklist

- [ ] All functions have complete type annotations
- [ ] No Generic[T] inheritance in core classes
- [ ] Constants marked with Final
- [ ] **slots** defined for frequently-instantiated classes
- [ ] Type narrowing with isinstance instead of hasattr
- [ ] Native operations used (no unnecessary conversions)
- [ ] cast() used for type assertions without runtime cost
- [ ] Loops use direct iteration, not comprehensions for side effects

---

## Compilation Verification

### Check Compilation Success

```python
# Run mypy first
$ mypy sqlspec --strict

# Compile with mypyc
$ mypyc sqlspec/module.py

# Test compiled module
$ python -c "import module; print(module.__file__)"
# Should show .so file, not .py
```

### Benchmark Template

```python
import time
from typing import Final

ITERATIONS: Final[int] = 1000000

def benchmark_compiled() -> None:
    start = time.time()
    # Your optimized code here
    elapsed = time.time() - start
    print(f"Compiled: {elapsed:.4f}s")

if __name__ == "__main__":
    benchmark_compiled()
```

---

## Contributing to SQLSpec with MyPyC in Mind

When adding new code to SQLSpec:

1. **Write types first** - Design with types, don't add them later
2. **Avoid premature generalization** - Concrete types compile better
3. **Profile before optimizing** - Focus on hot paths
4. **Document type decisions** - Explain cast() usage and type choices
5. **Test both interpreted and compiled** - Ensure compatibility
6. **Benchmark critical paths** - Verify optimization benefits

Remember: MyPyC's power comes from static typing and predictable patterns. When in doubt, choose the more explicit, statically-typed approach.
