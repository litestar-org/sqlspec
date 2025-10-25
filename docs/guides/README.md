---
orphan: true
---

# SQLSpec Development Guides

Comprehensive guides for developing with and contributing to SQLSpec.

## Adapters

Database adapter-specific guides covering patterns, best practices, and implementation details:

- [**ADBC**](adapters/adbc.md) - Arrow Database Connectivity
- [**AioSQLite**](adapters/aiosqlite.md) - Async SQLite
- [**AsyncMy**](adapters/asyncmy.md) - Async MySQL/MariaDB
- [**BigQuery**](adapters/bigquery.md) - Google Cloud BigQuery
- [**DuckDB**](adapters/duckdb.md) - OLAP analytical database
- [**MySQL**](adapters/mysql.md) - MySQL patterns
- [**Oracle**](adapters/oracle.md) - Oracle Database (comprehensive guide)
- [**PostgreSQL**](adapters/postgres.md) - PostgreSQL patterns (asyncpg, psycopg)
- [**Psqlpy**](adapters/psqlpy.md) - Rust-based async PostgreSQL
- [**SQLite**](adapters/sqlite.md) - Embedded SQLite

## Performance

Optimization guides for SQLSpec:

- [**SQLglot Guide**](performance/sqlglot.md) - SQL parsing, transformation, and optimization with SQLglot
- [**MyPyC Guide**](performance/mypyc.md) - Compilation strategies for high-performance Python code

## Migrations

Database migration strategies and workflows:

- [**Hybrid Versioning**](migrations/hybrid-versioning.md) - Combine timestamp and sequential versioning for optimal workflows

## Testing

Testing strategies and patterns:

- [**Testing Guide**](testing/testing.md) - Pytest strategies, integration testing, database fixtures

## Architecture

Core architecture and design patterns:

- [**Architecture Guide**](architecture/architecture.md) - SQLSpec architecture overview
- [**Data Flow Guide**](architecture/data-flow.md) - How data flows through SQLSpec

## Extensions

- [**aiosql Extension**](extensions/aiosql.md) - Bridge existing aiosql SQL files into SQLSpec
- [**Litestar Extension**](extensions/litestar.md) - Plugin usage, dependency injection, sessions, CLI commands
- [**Google ADK Extension**](extensions/google-adk.md) - Persist Google ADK sessions/events with SQLSpec stores
- [**Starlette Integration**](extensions/starlette.md) - Lifespan management and middleware patterns
- [**FastAPI Integration**](extensions/fastapi.md) - Dependency injection and transaction handling
- [**Flask Integration**](extensions/flask.md) - Request-scoped sessions for synchronous drivers
- [**Sanic Integration**](extensions/sanic.md) - Listeners, middleware, and Sanic-Ext dependencies

## Quick Reference

- [**Quick Reference**](quick-reference/quick-reference.md) - Common patterns and code snippets
- [**SQLglot LLM Playbook**](quick-reference/sqlglot-llm-playbook.md) - Fast-start guide for agents leveraging SQLglot in SQLSpec
- [**Litestar LLM Playbook**](quick-reference/litestar-llm-playbook.md) - Guidelines for wiring SQLSpec into Litestar apps quickly
- [**MyPyC LLM Playbook**](quick-reference/mypyc-llm-playbook.md) - Compilation workflow tips for high-performance modules

## Writing Documentation

Style guide and best practices for writing SQLSpec documentation:

- [**Documentation Style Guide**](writing/documentation-style-guide.md) - Voice, tone, terminology, and writing standards

## Usage

These guides are referenced by AI agents in and are the **canonical source of truth** for SQLSpec development patterns. When working with AI coding assistants:

1. **Agents read these guides first** before making implementation decisions
2. **Guides are verified and updated** to reflect current best practices
3. **Cross-AI compatible** - works with Claude, Gemini, Codex, and other AI assistants

## Contributing

When adding new patterns or updating guides:

1. Update the relevant guide in this directory
2. Run verification to ensure accuracy
3. Update this README if adding new guide categories
4. Commit guides with descriptive messages

## Guide Organization

```
docs/guides/
├── README.md                    # This file
├── adapters/                    # Database adapter guides
│   ├── adbc.md
│   ├── oracle.md
│   ├── postgres.md
│   └── ...
├── migrations/                  # Migration workflows
│   └── hybrid-versioning.md
├── performance/                 # Performance optimization
│   ├── sqlglot.md
│   └── mypyc.md
├── testing/                     # Testing strategies
│   └── testing.md
├── architecture/                # Architecture guides
│   ├── architecture.md
│   └── data-flow.md
├── extensions/                  # Extension integration guides
│   └── aiosql.md
├── quick-reference/             # Quick reference
│   ├── quick-reference.md
│   ├── sqlglot-llm-playbook.md
│   ├── litestar-llm-playbook.md
│   └── mypyc-llm-playbook.md
└── writing/                     # Documentation writing
    └── documentation-style-guide.md
```

## See Also

- [AGENTS.md](../../AGENTS.md) - Code quality standards and collaboration guidelines
- [.claude/agents/](../../.claude/agents/) - AI agent definitions that reference these guides
