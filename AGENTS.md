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

Supported adapters: adbc, aiosqlite, asyncmy, asyncpg, bigquery, duckdb, oracledb, psqlpy, psycopg, sqlite

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

### Error Handling

- Custom exceptions inherit from `SQLSpecError` in `sqlspec/exceptions.py`
- Use `wrap_exceptions` context manager in adapter layer
- Two-tier pattern: graceful skip (DEBUG) for expected conditions, hard errors for malformed input

## Collaboration Guidelines

- Challenge suboptimal requests constructively
- Ask clarifying questions for ambiguous requirements
- Propose better alternatives with clear reasoning
- Consider edge cases, performance, and maintainability
