# Pattern Library

This directory contains reusable implementation patterns extracted from completed features in SQLSpec. Consult this library before implementing new features to maintain consistency and avoid reinventing solutions.

## How Patterns Are Captured

The pattern library follows a systematic capture-and-refine workflow:

### 1. During Implementation (Expert Agent)

New patterns are documented in `workspace/tmp/new-patterns.md`:

```markdown
# New Patterns from Feature X

## Pattern: Driver Feature Auto-Detection

Used in: asyncpg Cloud SQL connector integration

**Problem**: Optional dependencies shouldn't break config initialization

**Solution**: Auto-detect package availability in __init__, set enable_* flags

**Code**:
```python
features_dict.setdefault("enable_cloud_sql", CLOUD_SQL_CONNECTOR_INSTALLED)
```

**Related**: driver_features pattern, graceful degradation pattern
```

### 2. During Knowledge Capture (Docs-Vision Agent Phase 3)

The Docs-Vision agent:
1. Reviews `workspace/tmp/new-patterns.md`
2. Determines if patterns are project-wide or adapter-specific
3. Extracts to appropriate files in `specs/guides/patterns/`
4. Updates existing pattern documents with new examples
5. Links patterns to relevant guides in `docs/guides/`

### 3. During PRD Planning (PRD Agent)

The PRD agent:
1. Consults pattern library first
2. Identifies relevant patterns for the feature
3. References patterns in research findings
4. Ensures consistency with established patterns

**Result**: Patterns flow from implementation → knowledge base → future implementations, creating a learning system.

## Pattern Categories

### Adapter Patterns (`adapter-patterns.md`)

Cross-adapter implementation patterns that apply to all database adapters:

- **Configuration Pattern**: pool_config TypedDict, driver_features, bind_key
- **Type Handler Pattern**: Input/output type handlers, graceful degradation
- **Exception Handling Pattern**: wrap_exceptions, SQLSpec exception hierarchy
- **Connection Lifecycle Pattern**: provide_connection, provide_session, pool management
- **driver_features Pattern**: Auto-detection, enable_* prefix, TypedDict with NotRequired
- **Parameter Style Pattern**: ParameterProfile, style conversion
- **Arrow Integration Pattern**: fetch_arrow, load_from_arrow, zero-copy transfers

**When to use**: Implementing new adapters, modifying existing adapters, adding adapter features

### Architecture Patterns (`architecture-patterns.md`)

High-level structural patterns:

- **Protocol-Based Design**: Protocols + type guards instead of inheritance
- **Configuration-Driver Separation**: Config holds settings, Driver executes queries
- **Context Manager Lifecycle**: Automatic resource cleanup
- **Statement Pipeline**: SQL → Parse → Transform → Compile → Execute
- **Lazy Pool Creation**: Pool created on first use, not on config instantiation

**When to use**: Major architectural decisions, new subsystems, refactoring core components

### Testing Patterns (`testing-patterns.md`)

Patterns for comprehensive test coverage:

- **Function-Based Tests**: `def test_*():` not `class Test*:`
- **Database Container Pattern**: pytest-databases for real database tests
- **Fixture Hierarchies**: Scoped fixtures (session, module, function)
- **Parameterized Adapter Tests**: Test all adapters with same logic
- **Mock vs Real Database**: When to use each approach
- **Named Temporary Files**: SQLite pooling tests with tempfile.NamedTemporaryFile

**When to use**: Writing tests, debugging test failures, improving test coverage

### Performance Patterns (`performance-patterns.md`)

Optimization techniques proven effective in SQLSpec:

- **Statement Caching**: LRU cache with TTL
- **Parse Once, Transform Once**: Avoid re-parsing in loops
- **Mypyc Compilation**: Performance-critical modules, __slots__, explicit methods
- **Zero-Copy Transfers**: Arrow/Parquet for bulk data
- **Batch Operations**: execute_many, load_from_arrow
- **Connection Pooling**: Reuse connections, configure pool size

**When to use**: Performance optimization, identifying bottlenecks, scaling improvements

### Integration Patterns (`integration-patterns.md`)

Framework and tool integration patterns:

- **Framework Extension Pattern**: extension_config in database config
- **Dependency Injection**: Litestar plugin registration
- **Middleware Integration**: Starlette/FastAPI middleware
- **CLI Tool Pattern**: Click commands, configuration discovery
- **Storage Backend Pattern**: fsspec/obstore abstractions
- **Migration System Pattern**: Version tracking, timestamp vs sequential

**When to use**: Adding framework support, CLI tools, storage backends, migrations

### Custom Expression Patterns (`custom-expression-patterns.md`)

SQLglot custom expression patterns for dialect-specific SQL:

- **Dialect-Specific Generation**: Override `.sql()` for custom syntax
- **Generator Registration**: Register with SQLGlot TRANSFORMS
- **Metric/Flag Storage**: Use exp.Identifier for runtime-accessible metadata
- **Generic Fallback**: Provide default SQL for unknown dialects

**When to use**: Database syntax varies across dialects, standard SQLGlot expressions insufficient

## Pattern Template

Each pattern follows this structure for consistency and completeness:

```markdown
# Pattern: [Descriptive Name]

## Context

**When to use this pattern**:
- Scenario 1
- Scenario 2
- Scenario 3

**When NOT to use this pattern**:
- Anti-pattern scenario 1
- Anti-pattern scenario 2

## Problem

[Clear description of the problem this pattern solves]

**Symptoms**:
- Symptom 1
- Symptom 2

**Root cause**:
[Why this problem exists]

## Solution

[High-level description of the solution]

**Key principles**:
1. Principle 1
2. Principle 2
3. Principle 3

**Implementation steps**:
1. Step 1
2. Step 2
3. Step 3

## Code Example

### Minimal Example

```python
# Simplest possible working example
```

### Full Example

```python
# Complete real-world example from SQLSpec codebase
```

### Anti-Pattern Example

```python
# BAD - Common mistake to avoid
```

```python
# GOOD - Correct implementation
```

## Variations

### Variation 1: [Name]

[When to use this variation]

```python
# Code example
```

### Variation 2: [Name]

[When to use this variation]

```python
# Code example
```

## Related Patterns

- **[Pattern Name]** (`file.md#section`) - Relationship description
- **[Pattern Name]** (`file.md#section`) - Relationship description

## SQLSpec Files

**Core implementation**:
- `/home/cody/code/litestar/sqlspec/path/to/file.py` - Description

**Examples**:
- `/home/cody/code/litestar/sqlspec/path/to/example.py` - Description

**Tests**:
- `/home/cody/code/litestar/sqlspec/tests/path/to/test.py` - Description

## References

- **Documentation**: [Link to docs/guides/]
- **External**: [Link to library docs, blog posts, etc.]
- **Discussion**: [Link to PR, issue, design doc]

## History

- **Introduced**: [Version/PR] - [Brief description]
- **Modified**: [Version/PR] - [What changed and why]
```

## Using Patterns

### For Expert Agent (Implementation)

1. **Before implementing** a feature, search the pattern library:
   ```bash
   grep -r "connection pool" specs/guides/patterns/
   ```

2. **During implementation**, document new patterns in `workspace/tmp/new-patterns.md`

3. **Reference existing patterns** in code comments:
   ```python
   # Follows driver_features auto-detection pattern (see adapter-patterns.md)
   features_dict.setdefault("enable_pgvector", PGVECTOR_INSTALLED)
   ```

### For PRD Agent (Planning)

1. **Research phase**: Consult pattern library for relevant patterns
2. **Plan phase**: Reference patterns in PRD requirements
3. **Research findings**: Link to pattern files for context

**Example PRD reference**:
```markdown
## Implementation Approach

This feature will follow the **Type Handler Pattern** (adapter-patterns.md#type-handler-pattern):
- Graceful degradation when optional package not installed
- DEBUG log when skipping handlers
- Register in config's _init_connection callback
```

### For Testing Agent (Test Design)

1. **Test structure**: Follow testing patterns for organization
2. **Coverage**: Ensure tests cover all pattern variations
3. **Examples**: Use pattern examples as test inspiration

### For Docs-Vision Agent (Knowledge Capture)

1. **Review** `workspace/tmp/new-patterns.md`
2. **Extract** patterns to appropriate category files
3. **Update** existing patterns with new examples or variations
4. **Link** patterns to relevant docs/guides/

## Pattern Discovery

### By Category

```bash
# Adapter patterns
cat specs/guides/patterns/adapter-patterns.md

# Testing patterns
cat specs/guides/patterns/testing-patterns.md

# All patterns
ls specs/guides/patterns/*.md
```

### By Keyword

```bash
# Find patterns about caching
grep -r "cache" specs/guides/patterns/

# Find patterns about connection management
grep -r "connection" specs/guides/patterns/

# Find patterns about type handlers
grep -r "type handler" specs/guides/patterns/
```

### By File Reference

```bash
# Find patterns used in asyncpg adapter
grep -r "asyncpg" specs/guides/patterns/

# Find patterns related to Oracle
grep -r "oracle" specs/guides/patterns/
```

## Contributing New Patterns

### During Feature Implementation

Add to `workspace/tmp/new-patterns.md`:

```markdown
## Pattern: [Name]

**Context**: [Where/when pattern was used]

**Problem**: [What problem it solved]

**Solution**: [High-level approach]

**Code Example**:
```python
# Minimal working example
```

**Files**:
- path/to/implementation.py
- path/to/test.py

**Related**:
- Existing pattern name
```

### Pattern Quality Checklist

Before adding a pattern, ensure it meets these criteria:

- [ ] **Reusable**: Applies to multiple features/adapters/situations
- [ ] **Proven**: Already implemented and working in SQLSpec
- [ ] **Documented**: Clear problem, solution, and code example
- [ ] **Tested**: Has working tests demonstrating the pattern
- [ ] **Linked**: Connected to related patterns and documentation

### Pattern vs One-Off Solution

**Extract as pattern** when:
- Used in 2+ places in the codebase
- Solves a recurring problem
- Other developers will face the same problem
- Best practices worth codifying

**Keep as one-off** when:
- Feature-specific implementation detail
- Unlikely to be reused elsewhere
- Too simple to need documentation
- Already covered by existing pattern

## Pattern Evolution

Patterns evolve as the codebase matures:

### Adding Variations

When a new use case emerges:
1. Document as "Variation" in existing pattern
2. Show code example of variation
3. Explain when to use each variation

### Deprecating Patterns

When a pattern becomes obsolete:
1. Mark pattern as **DEPRECATED** in heading
2. Explain why it's deprecated
3. Link to replacement pattern
4. Keep for historical reference

### Merging Patterns

When patterns overlap:
1. Identify common elements
2. Create unified pattern
3. Note merged patterns in history
4. Update all references

## Pattern Index

Quick reference to all available patterns:

| Pattern | Category | File | Key Use Case |
|---------|----------|------|--------------|
| Configuration Pattern | Adapter | adapter-patterns.md | Adapter config setup |
| Type Handler Pattern | Adapter | adapter-patterns.md | Optional type conversions |
| driver_features Pattern | Adapter | adapter-patterns.md | Feature auto-detection |
| Exception Handling | Adapter | adapter-patterns.md | Error wrapping |
| Connection Lifecycle | Adapter | adapter-patterns.md | Pool management |
| Parameter Style | Adapter | adapter-patterns.md | Style conversion |
| Arrow Integration | Adapter | adapter-patterns.md | Bulk data transfer |
| Protocol-Based Design | Architecture | architecture-patterns.md | Type system |
| Configuration-Driver Separation | Architecture | architecture-patterns.md | Separation of concerns |
| Context Manager Lifecycle | Architecture | architecture-patterns.md | Resource cleanup |
| Statement Pipeline | Architecture | architecture-patterns.md | SQL processing |
| Function-Based Tests | Testing | testing-patterns.md | Test structure |
| Database Container | Testing | testing-patterns.md | Integration tests |
| Parameterized Adapter Tests | Testing | testing-patterns.md | Multi-adapter coverage |
| Statement Caching | Performance | performance-patterns.md | Avoid re-parsing |
| Mypyc Compilation | Performance | performance-patterns.md | Speed optimization |
| Zero-Copy Transfers | Performance | performance-patterns.md | Bulk operations |
| Framework Extension | Integration | integration-patterns.md | Framework support |
| Storage Backend | Integration | integration-patterns.md | Data import/export |
| Migration System | Integration | integration-patterns.md | Schema evolution |
| Custom Expression | Custom Expression | custom-expression-patterns.md | Dialect-specific SQL |

## Example: Finding the Right Pattern

### Scenario: "I need to add support for a new optional type in PostgreSQL"

**Step 1: Identify category**
- Adapter-specific feature → Adapter Patterns

**Step 2: Search keywords**
```bash
grep -i "optional" specs/guides/patterns/adapter-patterns.md
grep -i "type" specs/guides/patterns/adapter-patterns.md
```

**Step 3: Review results**
- **Type Handler Pattern** - Matches! Shows how to:
  - Register input/output handlers
  - Gracefully degrade when package not installed
  - Use driver_features for feature flags

**Step 4: Check examples**
- asyncpg pgvector support
- Oracle NumPy vector support
- Both show complete implementations

**Step 5: Implement following pattern**
```python
# In config.py
features_dict.setdefault("enable_mytype", MYTYPE_INSTALLED)

# In _mytype_handlers.py
def register_mytype_handlers(connection: "AsyncpgConnection") -> None:
    if not MYTYPE_INSTALLED:
        logger.debug("mytype not installed - skipping handlers")
        return
    # Register handlers...
```

## Summary

The pattern library is a living knowledge base that:

1. **Captures** proven solutions from completed work
2. **Guides** new implementations for consistency
3. **Evolves** as the codebase matures
4. **Accelerates** development by avoiding reinvention

**Key principle**: If you're solving a problem, check if the pattern library already has the answer. If you're solving a *new* problem, document the pattern so others can benefit.
