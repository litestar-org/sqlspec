# Claude Skills for SQLSpec

Comprehensive skills library for proper SQLSpec usage, ensuring consistent best practices across all database adapters, frameworks, and use cases.

## Overview

This skills library was created by analyzing:

- **SQLSpec documentation** - Official usage guides
- **SQLSpec source code** - Implementation patterns and standards
- **AGENTS.md** - Project-specific conventions
- **Example projects** - Real-world usage patterns

## Skills Structure

### 1. Main SQLSpec Usage Skill

**Location:** [sqlspec-usage/skill.md](sqlspec-usage/skill.md)

**Purpose:** Primary skill for all SQLSpec-related queries. Provides guidance on:

- Configuration (all adapters)
- Query execution patterns
- Framework integration (Litestar, FastAPI, Starlette, Flask)
- Migration management
- Testing best practices
- Performance optimization
- Multi-database setups

**Activation:** Automatically activates when questions involve SQLSpec configuration, database setup, query execution, or framework integration.

### 2. Pattern Guides

**Location:** `sqlspec-usage/patterns/`

Detailed reference guides for specific SQLSpec usage patterns:

| Guide | Purpose | Key Topics |
|-------|---------|-----------|
| [configuration.md](sqlspec-usage/patterns/configuration.md) | Configuration across all adapters | pool_config, driver_features, extension_config, multi-database |
| [queries.md](sqlspec-usage/patterns/queries.md) | Query execution patterns | Parameter binding, result handling, transactions, type mapping |
| [frameworks.md](sqlspec-usage/patterns/frameworks.md) | Framework integration | Litestar, FastAPI, Starlette, Flask patterns |
| [migrations.md](sqlspec-usage/patterns/migrations.md) | Database migrations | CLI commands, hybrid versioning, programmatic control |
| [testing.md](sqlspec-usage/patterns/testing.md) | Testing best practices | Test isolation, pytest-databases, parallel execution |
| [performance.md](sqlspec-usage/patterns/performance.md) | Performance optimization | Pooling, caching, batch ops, Arrow integration |
| [troubleshooting.md](sqlspec-usage/patterns/troubleshooting.md) | Common issues & solutions | Installation, config, query, transaction issues |

### 3. Working Examples

**Location:** `sqlspec-usage/examples/`

Production-ready code examples:

| Example | Description |
|---------|-------------|
| [litestar-integration.py](sqlspec-usage/examples/litestar-integration.py) | Complete Litestar + SQLSpec app with multi-database, DI, error handling |
| [fastapi-integration.py](sqlspec-usage/examples/fastapi-integration.py) | FastAPI integration with dependency injection and type-safe responses |
| [multi-database.py](sqlspec-usage/examples/multi-database.py) | Multi-database configuration and cross-database queries |
| [migration-workflow.sh](sqlspec-usage/examples/migration-workflow.sh) | Complete migration workflow with hybrid versioning |
| [testing-patterns.py](sqlspec-usage/examples/testing-patterns.py) | Pytest testing patterns with isolation and parallel execution |

### 4. Adapter-Specific Skills

**Location:** `sqlspec-adapters/`

Detailed guidance for individual database adapters:

| Adapter | File | Status |
|---------|------|--------|
| AsyncPG (PostgreSQL async) | [asyncpg.md](sqlspec_adapters/asyncpg.md) | ✅ Complete |
| Psycopg (PostgreSQL sync/async) | [psycopg.md](sqlspec_adapters/psycopg.md) | ✅ Complete |
| Psqlpy (PostgreSQL Rust-based) | [psqlpy.md](sqlspec_adapters/psqlpy.md) | ✅ Complete |
| SQLite (sync) | [sqlite.md](sqlspec_adapters/sqlite.md) | ✅ Complete |
| AioSQLite (async) | [aiosqlite.md](sqlspec_adapters/aiosqlite.md) | ✅ Complete |
| DuckDB (analytics) | [duckdb.md](sqlspec_adapters/duckdb.md) | ✅ Complete |
| Oracle | [oracledb.md](sqlspec_adapters/oracledb.md) | ✅ Complete |
| Asyncmy (MySQL async) | [asyncmy.md](sqlspec_adapters/asyncmy.md) | ✅ Complete |
| BigQuery | [bigquery.md](sqlspec_adapters/bigquery.md) | ✅ Complete |
| Spanner | [spanner.md](sqlspec_adapters/spanner.md) | ✅ Complete |
| ADBC (Arrow-native) | [adbc.md](sqlspec_adapters/adbc.md) | ✅ Complete |

**Note:** Template adapters follow the AsyncPG structure and can be quickly expanded when needed.

## Usage in Agent Workflow

### Expert Agent

The Expert agent references these skills during implementation:

```python
# Main skill for overall guidance
Read(".claude/skills/sqlspec-usage/skill.md")

# Specific pattern guides as needed
Read(".claude/skills/sqlspec-usage/patterns/configuration.md")
Read(".claude/skills/sqlspec-usage/patterns/queries.md")

# Adapter-specific guidance
Read(f".claude/skills/sqlspec-adapters/{adapter}.md")

# Working examples for reference
Read(".claude/skills/sqlspec-usage/examples/litestar-integration.py")
```

### Testing Agent

The Testing agent uses testing-specific skills:

```python
Read(".claude/skills/sqlspec-usage/patterns/testing.md")
Read(".claude/skills/sqlspec-usage/examples/testing-patterns.py")
```

## Bootstrap Integration

The `.claude/bootstrap.md` includes automatic skill creation for SQLSpec projects:

**Phase 4.6:** Detects SQLSpec projects and auto-generates:

1. Main SQLSpec skill
2. All pattern guides
3. Working examples
4. Adapter-specific skills for detected adapters
5. Updates agent files to reference skills

## Anti-Pattern Detection

All skills include sections on anti-patterns to avoid:

- Configuration mistakes (missing pool_config, duplicate session keys)
- Session management errors (no context managers, mixing sync/async)
- Query execution issues (SQL injection, wrong parameter style)
- Framework integration problems (duplicate keys, missing middleware)
- Testing pitfalls (:memory: with pooling, class-based tests)

## Best Practices Enforcement

Skills enforce SQLSpec best practices:

1. **Always use context managers** for session management
2. **Store config keys** returned from `add_config()`
3. **Use parameter binding** (never string concatenation)
4. **Enable connection pooling** in production
5. **Use unique session_key values** for multi-database setups
6. **Close pools on shutdown** with `close_all_pools()`
7. **Use typed schema mapping** for type safety
8. **Prefer framework plugins** over manual setup
9. **Use temp files for SQLite pooling tests**, not `:memory:`
10. **Define TypedDict for driver_features** in all adapters

## Maintenance

### Adding New Patterns

When new SQLSpec patterns are discovered:

1. Add to relevant pattern guide in `patterns/`
2. Update main skill.md if it's a major pattern
3. Add working example if applicable
4. Update AGENTS.md if it affects project standards

### Expanding Adapter Skills

To expand a template adapter skill:

1. Copy `asyncpg.md` as template
2. Fill in adapter-specific:
   - Configuration parameters
   - Parameter binding style
   - Adapter-specific features
   - Performance characteristics
   - Common issues
3. Update `sqlspec-adapters/README.md`
4. Add to main skill.md adapter comparison table

### Knowledge Capture Process

After significant SQLSpec work:

1. **Analyze** what was built for reusable patterns
2. **Update** relevant pattern guides
3. **Add examples** if they demonstrate new techniques
4. **Update** adapter skills if adapter-specific features were used
5. **Update** AGENTS.md if it affects project conventions

## Real-World Pattern Sources

Skills incorporate patterns from:

- **oracle-vertexai-demo** - Oracle-specific usage, embeddings
- **postgres-vertexai-demo** - PostgreSQL with vector search
- **sqlstack** - Multi-framework integration
- **accelerator** - Production deployment patterns

(Note: Deep analysis of these projects is pending and will enhance skills further)

## Skill Activation

Skills activate automatically when:

- User asks about SQLSpec configuration
- Database connection issues arise
- Query execution questions occur
- Framework integration is discussed
- Migration management is needed
- Testing patterns are requested
- Performance optimization is sought
- Troubleshooting is required

No manual invocation needed - skills provide context-aware guidance.

## Contributing

To contribute new patterns or improvements:

1. Test pattern in real code
2. Document in appropriate guide
3. Add working example if useful
4. Update this README if structure changes
5. Submit PR with clear benefit explanation

## Version

**Version:** 1.0.0
**Created:** November 15, 2025
**Last Updated:** November 15, 2025

## License

Same as SQLSpec project license.
