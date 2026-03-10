# SQLSpec

[![PyPI](https://img.shields.io/pypi/v/sqlspec)](https://pypi.org/project/sqlspec/)
[![Python](https://img.shields.io/pypi/pyversions/sqlspec)](https://pypi.org/project/sqlspec/)
[![License](https://img.shields.io/pypi/l/sqlspec)](https://github.com/litestar-org/sqlspec/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-sqlspec.dev-blue)](https://sqlspec.dev/)

SQLSpec is a SQL execution layer for Python. You write the SQL -- as strings, through a builder API, or loaded from files -- and SQLSpec handles connections, parameter binding, SQL injection validation, dialect translation, and mapping results back to typed Python objects. It uses [sqlglot](https://github.com/tobymao/sqlglot) under the hood to parse, validate, and optimize your queries before they hit the database.

It works with PostgreSQL (asyncpg, psycopg, psqlpy), SQLite (sqlite3, aiosqlite), DuckDB, MySQL (asyncmy, mysql-connector, pymysql), Oracle (oracledb), CockroachDB, BigQuery, Spanner, and anything ADBC-compatible. Sync or async, same API.

## Status

SQLSpec is under active development. The public API may still change. Check the [docs](https://sqlspec.dev/) and changelog for updates.

## Quick Start

```bash
pip install sqlspec
```

```python
from pydantic import BaseModel
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

class Greeting(BaseModel):
    message: str

spec = SQLSpec()
db = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

with spec.provide_session(db) as session:
    greeting = session.select_one(
        "SELECT 'Hello, SQLSpec!' AS message",
        schema_type=Greeting,
    )
    print(greeting.message)  # Output: Hello, SQLSpec!
```

Write SQL, define a schema, get typed objects back. Connection pooling, parameter binding, and result mapping are handled for you.

## What It Does

**Connects to databases** with pooled connections that work the same way whether you're writing sync or async code. Adapters are included for psycopg, asyncpg, psqlpy, sqlite3, aiosqlite, DuckDB, asyncmy, mysql-connector, pymysql, oracledb, BigQuery, and ADBC drivers.

**Runs your SQL** with automatic parameter binding and dialect translation. You can also build queries programmatically with the builder API, load them from `.sql` files, or batch operations with statement stacks.

**Maps results to types** -- Pydantic, msgspec, attrs, or plain dataclasses. Need columnar data instead? Export to Arrow tables for zero-copy handoff to pandas, Polars, or other analytical tools.

**Plugs into frameworks** you already use. There's a Litestar plugin with full DI support, Starlette/FastAPI middleware, and a Flask extension.

**Handles production concerns** like OpenTelemetry and Prometheus instrumentation, database event channels (LISTEN/NOTIFY, Oracle AQ, and a portable fallback), structured logging with correlation IDs, and a migration CLI for schema versioning.

## Documentation

- [Getting Started](https://sqlspec.dev/getting_started/) -- installation, adapter selection, first steps
- [Usage Guides](https://sqlspec.dev/usage/) -- adapters, configuration, SQL file loader, and more
- [Examples Gallery](https://sqlspec.dev/examples/) -- working code for common patterns
- [API Reference](https://sqlspec.dev/reference/) -- full API docs
- [CLI Reference](https://sqlspec.dev/usage/cli.html) -- migration and management commands

## Playground

Want to try it without installing anything? The [interactive playground](https://sqlspec.dev/playground) runs SQLSpec in your browser with a sandboxed Python runtime.

## Reference Applications

- **[PostgreSQL + Vertex AI Demo](https://github.com/cofin/postgres-vertexai-demo)** -- Vector search with pgvector and real-time chat using Litestar and Google ADK. Shows connection pooling, migrations, type-safe result mapping, vector embeddings, and response caching.
- **[Oracle + Vertex AI Demo](https://github.com/cofin/oracledb-vertexai-demo)** -- Oracle 23ai vector search with semantic similarity using HNSW indexes. Demonstrates NumPy array conversion, large object handling, and real-time performance metrics.

## Contributing

Contributions are welcome -- whether that's bug reports, new adapter ideas, or pull requests. Take a look at the [contributor guide](https://sqlspec.dev/contributing/) to get started.

## License

MIT
