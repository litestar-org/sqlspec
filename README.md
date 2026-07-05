# SQLSpec

[![PyPI](https://img.shields.io/pypi/v/sqlspec)](https://pypi.org/project/sqlspec/)
[![Python](https://img.shields.io/pypi/pyversions/sqlspec)](https://pypi.org/project/sqlspec/)
[![License](https://img.shields.io/pypi/l/sqlspec)](https://github.com/litestar-org/sqlspec/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-sqlspec.dev-blue)](https://sqlspec.dev/)

SQLSpec is a SQL execution layer for Python. You write the SQL -- as strings, through a builder API, or loaded from files -- and SQLSpec handles connections, parameter binding, SQL injection prevention, dialect translation, and mapping results back to typed Python objects. It uses [sqlglot](https://github.com/tobymao/sqlglot) under the hood to parse, validate, and optimize your queries before they hit the database.

It works with PostgreSQL (asyncpg, psycopg, psqlpy), SQLite (sqlite3, aiosqlite), DuckDB, MySQL (asyncmy, aiomysql, mysql-connector, pymysql), SQL Server (mssql-python, pymssql, arrow-odbc), Oracle (oracledb), CockroachDB, BigQuery, Spanner, and supported ADBC backends including Snowflake, Flight SQL, and GizmoSQL. Sync or async, same API. It also includes a built-in storage layer, Arrow export through native paths or conversion fallbacks, storage-bridge bulk ingest for adapters with native ingest support, and integrations for Litestar, FastAPI, Flask, Sanic, and Starlette.

## Quick Start

```bash
pip install sqlspec
```

```python
from dataclasses import dataclass

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

@dataclass
class Greeting:
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

Write SQL, define a schema, get typed objects back. Or use the query builder -- they're interchangeable:

```python
from sqlspec import sql

# Builder API -- same driver, same result mapping
users = session.select(
    sql.select("id", "name", "email")
       .from_("users")
       .where("active = :active")
       .order_by("name")
       .limit(10),
    active=True,
    schema_type=User,
)
```

## Features

- **Session lifecycle** -- sync and async sessions with pooling where the adapter supports it
- **Parameter binding and dialect translation** -- powered by sqlglot, with a fluent query builder and `.sql` file loader
- **Result mapping** -- map rows to Pydantic, msgspec, attrs, or dataclass models, or export to Arrow tables for pandas and Polars
- **Storage layer** -- read and write Arrow tables to local files, fsspec, or object stores
- **Framework integrations** -- Litestar plugin with DI, Starlette/FastAPI/Sanic middleware, Flask extension
- **Google ADK** -- SQLSpec-backed session, event, memory, and artifact services
- **Observability** -- OpenTelemetry and Prometheus instrumentation, structured logging with correlation IDs
- **Event channels** -- LISTEN/NOTIFY, Oracle AQ/TxEventQ, and durable table-backed queues with polling fallback
- **Migrations** -- native schema migration CLI backed by SQLSpec's SQL file loader

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
