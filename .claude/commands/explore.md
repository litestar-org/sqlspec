---
description: Explore codebase to understand patterns, architecture, or answer questions
allowed-tools: Read, Glob, Grep, Bash, Task, mcp__zen__analyze, mcp__context7__resolve-library-id, mcp__context7__get-library-docs
---

# Explore Codebase

Explore the sqlspec codebase for: **$ARGUMENTS**

## Rules

- READ-ONLY operations only
- NO modifications to any files
- Focus on understanding, not changing

## Exploration Strategy

### Step 1: Understand the Question

Categorize the exploration into one of these types:

| Question Type | Focus | Example |
|---------------|-------|---------|
| **Architecture** | How components connect | "How does parameter conversion work?" |
| **Pattern** | How similar features are implemented | "How do adapters handle transactions?" |
| **Location** | Where specific code lives | "Where is JSON serialization implemented?" |
| **Usage** | How to use a feature | "How do I configure connection pooling?" |
| **Performance** | Optimization patterns | "How is SQLglot caching implemented?" |
| **Integration** | Third-party library usage | "How does asyncpg integration work?" |

### Step 2: Search Strategy

Use the right search tool for each task:

**A. Find files by pattern (use Glob):**

```python
# Find all adapter configs
Glob(pattern="**/adapters/*/config.py")

# Find vector-related files
Glob(pattern="**/*vector*.py")

# Find test files for specific adapter
Glob(pattern="tests/integration/test_adapters/test_asyncpg/**/*.py")

# Find all builder components
Glob(pattern="sqlspec/builder/*.py")
```

**B. Search code content (use Grep):**

```python
# Find all uses of a class
Grep(
    pattern="AsyncpgConfig",
    output_mode="files_with_matches",
    type="py"
)

# Find function definitions
Grep(
    pattern="def provide_session",
    output_mode="content",
    type="py",
    head_limit=20
)

# Find pattern with context
Grep(
    pattern="wrap_exceptions",
    output_mode="content",
    type="py",
    A=2,  # 2 lines after
    B=2   # 2 lines before
)

# Case-insensitive search
Grep(
    pattern="transaction",
    i=True,
    output_mode="files_with_matches",
    glob="sqlspec/driver/*.py"
)

# Find TODO comments
Grep(
    pattern="# TODO",
    output_mode="content",
    type="py",
    head_limit=50
)

# Search in specific directory
Grep(
    pattern="class.*Config",
    path="sqlspec/adapters/",
    output_mode="content",
    type="py"
)
```

**C. Deep architectural analysis (use zen.analyze):**

```python
# Analyze architecture of a component
mcp__zen__analyze(
    step="Analyze adapter pattern across all database implementations",
    step_number=1,
    total_steps=3,
    analysis_type="architecture",
    findings="Examining config.py, driver.py structure across adapters",
    files_checked=[
        "/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/config.py",
        "/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/driver.py"
    ],
    confidence="medium",
    next_step_required=True
)

# Analyze performance patterns
mcp__zen__analyze(
    step="Analyze SQLglot usage patterns for optimization opportunities",
    step_number=1,
    total_steps=2,
    analysis_type="performance",
    findings="Checking parse caching, statement reuse patterns",
    files_checked=[
        "/home/cody/code/litestar/sqlspec/sqlspec/core/statement.py",
        "/home/cody/code/litestar/sqlspec/sqlspec/core/cache.py"
    ],
    confidence="high",
    next_step_required=False,
    use_assistant_model=True
)
```

**D. Library documentation (use Context7):**

```python
# Get library documentation
mcp__context7__resolve-library-id(libraryName="asyncpg")
# Returns: /MagicStack/asyncpg

mcp__context7__get-library-docs(
    context7CompatibleLibraryID="/MagicStack/asyncpg",
    topic="connection pooling",
    mode="code"
)

# For conceptual understanding
mcp__context7__get-library-docs(
    context7CompatibleLibraryID="/MagicStack/asyncpg",
    topic="architecture",
    mode="info"
)
```

### Step 3: Deep Dive

Once you've located relevant files, read them systematically:

**A. Read core files in logical order:**

```python
# For architecture questions, start with base
Read("sqlspec/base.py")
Read("sqlspec/protocols.py")
Read("sqlspec/driver/base.py")

# For adapter questions, follow structure
Read("sqlspec/adapters/asyncpg/config.py")
Read("sqlspec/adapters/asyncpg/driver.py")
Read("sqlspec/adapters/asyncpg/_types.py")

# For builder questions
Read("sqlspec/builder/builder.py")
Read("sqlspec/builder/_expressions.py")

# For storage questions
Read("sqlspec/storage/importer.py")
Read("sqlspec/storage/exporter.py")
```

**B. Gather context from related files:**

```python
# After finding target file, read dependencies
# Example: Understanding parameter conversion
Read("sqlspec/core/parameters.py")  # Main implementation
Read("sqlspec/protocols.py")         # Protocol definitions
Read("docs/guides/adapters/parameter-profile-registry.md")  # Documentation
Read("tests/unit/test_core/test_parameters.py")  # Test examples
```

**C. Read documentation guides:**

```python
# Architecture understanding
Read("docs/guides/architecture/architecture.md")
Read("docs/guides/architecture/data-flow.md")
Read("docs/guides/architecture/patterns.md")

# Adapter-specific patterns
Read("docs/guides/adapters/postgres.md")
Read("docs/guides/adapters/parameter-profile-registry.md")

# Performance patterns
Read("docs/guides/performance/mypyc.md")
Read("docs/guides/performance/sqlglot.md")

# Quick reference for common patterns
Read("docs/guides/quick-reference/quick-reference.md")
```

**D. Check tests for usage examples:**

```python
# Integration tests show real-world usage
Read("tests/integration/test_adapters/test_asyncpg/test_driver.py")

# Unit tests show API contracts
Read("tests/unit/test_core/test_statement.py")

# Fixture files show setup patterns
Read("tests/conftest.py")
```

### Step 4: Synthesize Findings

Structure your report with:

**A. Executive Summary (2-3 sentences)**

Clear, direct answer to the original question.

**B. Key Files (with line references)**

```
/home/cody/code/litestar/sqlspec/sqlspec/core/parameters.py:45-67
  - convert_parameters() function handles conversion
  - Uses ParameterProfile for dialect-specific styles

/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/driver.py:123-145
  - AsyncpgDriver uses numbered parameters ($1, $2)
  - Calls convert_parameters() before execution
```

**C. Pattern Summary**

Describe how the pattern works across the codebase:

```markdown
## Pattern: Parameter Style Conversion

All adapters follow this pattern:

1. Define ParameterProfile in adapter config
2. Driver calls convert_parameters() before execution
3. Parameter converter uses dialect rules from profile
4. Converted SQL and params passed to database client

Example adapters:
- asyncpg: numbered ($1, $2)
- psycopg: numbered (%s, %s)
- oracledb: named (:name)
- sqlite: positional (?)
```

**D. Code Examples**

Show minimal working examples:

```python
# Example: How to configure custom parameter profile
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://..."},
    parameter_profile=ParameterProfile(
        style="numbered",
        prefix="$"
    )
)
```

**E. Related Patterns**

Link to related concepts:

```markdown
## Related Patterns

- Type conversion: sqlspec/adapters/{adapter}/type_converter.py
- Error handling: sqlspec/exceptions.py with wrap_exceptions
- Connection pooling: sqlspec/driver/base.py context managers
```

**F. Documentation References**

Point to relevant docs:

```markdown
## Documentation

- Architecture: docs/guides/architecture/data-flow.md
- Parameter Profiles: docs/guides/adapters/parameter-profile-registry.md
- Adapter Guide: docs/guides/adapters/postgres.md
```

## Tool Selection Matrix

| Question Type | Primary Tool | Secondary Tools | Example |
|---------------|-------------|-----------------|---------|
| "Where is X defined?" | Grep (files_with_matches) | Glob | "Where is AsyncpgConfig?" |
| "How does X work?" | Read → zen.analyze | Grep, Context7 | "How does caching work?" |
| "Show me examples of X" | Grep (content, -A/-B) | Read tests | "Show vector query examples" |
| "What files handle X?" | Glob | Grep | "What files handle migrations?" |
| "How is X implemented across adapters?" | Glob → Read multiple | zen.analyze | "How do adapters handle JSON?" |
| "What are best practices for X?" | Read docs/guides/ | WebSearch, Context7 | "Best practices for pooling?" |
| "How do I use library X?" | Context7 | Read adapter code | "How to use asyncpg pools?" |
| "What's the architecture of X?" | zen.analyze | Read, Grep | "Architecture of storage layer?" |

## Workflow Examples

### Example 1: "How does transaction handling work?"

```python
# Step 1: Search for transaction-related code
Grep(pattern="transaction", output_mode="files_with_matches", type="py")

# Step 2: Read base driver
Read("sqlspec/driver/base.py")

# Step 3: Read adapter implementation
Read("sqlspec/adapters/asyncpg/driver.py")

# Step 4: Read tests for examples
Read("tests/integration/test_adapters/test_asyncpg/test_transactions.py")

# Step 5: Analyze pattern
mcp__zen__analyze(
    step="Analyze transaction pattern across async and sync drivers",
    analysis_type="architecture",
    ...
)
```

### Example 2: "Where is JSON serialization implemented?"

```python
# Step 1: Search for JSON-related files
Glob(pattern="**/*json*.py")

# Step 2: Search for json_serializer in code
Grep(pattern="json_serializer", output_mode="content", type="py", head_limit=30)

# Step 3: Read driver_features pattern docs
Read("CLAUDE.md")  # Contains driver_features pattern

# Step 4: Read adapter implementations
Read("sqlspec/adapters/oracledb/config.py")
Read("sqlspec/adapters/oracledb/_json_handlers.py")
```

### Example 3: "How do I configure connection pooling for asyncpg?"

```python
# Step 1: Get library docs
mcp__context7__resolve-library-id(libraryName="asyncpg")
mcp__context7__get-library-docs(
    context7CompatibleLibraryID="/MagicStack/asyncpg",
    topic="connection pooling"
)

# Step 2: Read adapter config
Read("sqlspec/adapters/asyncpg/config.py")

# Step 3: Read usage guide
Read("docs/guides/adapters/postgres.md")

# Step 4: Read test examples
Read("tests/integration/test_adapters/test_asyncpg/test_config.py")
```

## Success Criteria

Exploration is complete when you can provide:

✅ **Direct Answer** - Clear answer to the original question
✅ **File Locations** - Absolute paths with line references
✅ **Code Examples** - Minimal working examples
✅ **Pattern Description** - How it works across codebase
✅ **Documentation Links** - Relevant guides and docs
✅ **No Modifications** - Read-only exploration only

## Example Report Format

```markdown
## Answer: How Parameter Conversion Works

### Summary
SQLSpec automatically converts parameter styles between dialects using
ParameterProfile definitions and the convert_parameters() function.

### Key Files

/home/cody/code/litestar/sqlspec/sqlspec/core/parameters.py:34-89
  - convert_parameters() main conversion logic
  - Handles :named, $1, ?, %s styles

/home/cody/code/litestar/sqlspec/sqlspec/adapters/asyncpg/config.py:45-52
  - AsyncpgConfig defines numbered parameter profile
  - Uses "$" prefix for $1, $2, etc.

### Pattern

1. Each adapter defines ParameterProfile in config
2. Driver calls convert_parameters() before execution
3. Converter transforms SQL and params dict to target style
4. Database client receives native parameter format

### Example Usage

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(connection_config={"dsn": "postgresql://..."})

# Input: "SELECT * FROM users WHERE id = :id"
# Output: "SELECT * FROM users WHERE id = $1"
```

### Documentation

- Parameter Profiles: docs/guides/adapters/parameter-profile-registry.md
- Data Flow: docs/guides/architecture/data-flow.md
```
