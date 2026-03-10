# SQLSpec

[![PyPI](https://img.shields.io/pypi/v/sqlspec)](https://pypi.org/project/sqlspec/)
[![Python](https://img.shields.io/pypi/pyversions/sqlspec)](https://pypi.org/project/sqlspec/)
[![License](https://img.shields.io/pypi/l/sqlspec)](https://github.com/litestar-org/sqlspec/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-sqlspec.dev-blue)](https://sqlspec.dev/)

SQLSpec is a SQL execution layer for Python. You write the SQL -- as strings, through a builder API, or loaded from files -- and SQLSpec handles connections, parameter binding, dialect translation, and mapping results back to typed Python objects. It uses [sqlglot](https://github.com/tobymao/sqlglot) under the hood to parse, validate, and optimize your queries before they hit the database.

It works with PostgreSQL (asyncpg, psycopg, psqlpy), SQLite (sqlite3, aiosqlite), DuckDB, MySQL (asyncmy, mysql-connector, pymysql), Oracle (oracledb), CockroachDB, BigQuery, Spanner, and anything ADBC-compatible. Sync or async, same API.

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

**Handles production concerns** like OpenTelemetry and Prometheus instrumentation, database event channels, structured logging with correlation IDs, and a migration CLI for schema versioning.

## Documentation

Full docs, usage guides, and an interactive playground are available at [sqlspec.dev](https://sqlspec.dev/).

## Contributing

Contributions are welcome. See the [contributor guide](https://sqlspec.dev/contributing/) to get started.

## License

MIT
