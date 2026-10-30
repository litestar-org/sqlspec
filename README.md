<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/banner-inline-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="docs/assets/banner-inline-light.svg">
  <img alt="SQLSpec" src="docs/assets/banner-inline-light.svg" width="520">
</picture>

<p><strong>SQL-first database execution for Python across 20 sync and async adapters.</strong></p>

[![PyPI](https://img.shields.io/pypi/v/sqlspec)](https://pypi.org/project/sqlspec/)
[![Python](https://img.shields.io/pypi/pyversions/sqlspec)](https://pypi.org/project/sqlspec/)
[![License](https://img.shields.io/pypi/l/sqlspec)](https://github.com/litestar-org/sqlspec/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-sqlspec.dev-1d4ed8)](https://sqlspec.dev/)

[Documentation](https://sqlspec.dev/) · [Quickstart](https://sqlspec.dev/learn/quickstart/) · [Databases](https://sqlspec.dev/databases/) · [Integrations](https://sqlspec.dev/integrations/) · [Playground](https://sqlspec.dev/playground/) · [API Reference](https://sqlspec.dev/reference/)

</div>

SQLSpec provides unified synchronous and asynchronous session APIs, SQLGlot-backed parameter compilation, statement caching, schema migrations, Apache Arrow streaming, and typed row mapping without an ORM. Write plain SQL strings, load named statements from `.sql` files, or compose dialect-portable queries programmatically while keeping full control over your SQL and underlying driver.

## Install

```bash
uv add sqlspec
```

Install optional driver, schema validation, or framework extras as needed:

```bash
uv add "sqlspec[asyncpg,msgspec,litestar]"
```

## Quickstart

```python
from dataclasses import dataclass

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig


@dataclass(frozen=True, slots=True)
class User:
    id: int
    name: str


spec = SQLSpec()
config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

with spec.provide_session(config) as session:
    session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    session.execute("INSERT INTO users (name) VALUES (:name)", name="Ada")
    user = session.select_one("SELECT id, name FROM users WHERE name = :name", name="Ada", schema_type=User)
```

## Capabilities

| Area | Overview |
| --- | --- |
| **Unified Execution** | Consistent `execute`, `select`, `select_one`, `select_one_or_none`, `select_value`, `execute_many`, `select_stream`, and transaction APIs across sync and `asyncio` drivers. |
| **20 Database Adapters** | PostgreSQL (`asyncpg`, `psycopg`, `psqlpy`), CockroachDB, SQLite (`sqlite3`, `aiosqlite`), DuckDB, MySQL & MariaDB, Oracle, SQL Server, Db2, BigQuery, Cloud Spanner, ADBC, and Arrow ODBC. |
| **SQL Files & Builder** | Load named queries from `.sql` files via `SQLFileLoader` or build statements with `sql.select`, `sql.insert`, `sql.update`, `sql.delete`, and `sql.merge` with cached AST compilation. |
| **Typed Rows & Arrow** | Project rows into `msgspec.Struct`, `Pydantic`, `dataclass`, `TypedDict`, or `attrs` models, or stream `pyarrow`, `polars`, and `pandas` datasets across local, `fsspec`, and `obstore` backends. |
| **Integrations & Ops** | First-party plugins and stores for Litestar, Google ADK, FastAPI, Starlette, Flask, Sanic, Dishka, and Events, plus OpenTelemetry spans, Prometheus metrics, and CLI migrations. |

## Documentation

- **[Learn & Quickstart](https://sqlspec.dev/learn/quickstart/)** — progressive tutorials, query patterns, migrations, and production recipes.
- **[Database Adapters](https://sqlspec.dev/databases/)** — capability matrix, driver installation extras, pool configuration, and dialect notes.
- **[Framework & AI Integrations](https://sqlspec.dev/integrations/)** — Litestar, Google ADK, Events, FastAPI, Starlette, Flask, Sanic, and Dishka guides.
- **[Interactive Playground](https://sqlspec.dev/playground/)** — zero-install browser SQLite environment powered by Pyodide WebAssembly.
- **[API Reference](https://sqlspec.dev/reference/)** — class, configuration, driver, builder, and CLI reference documentation.

## Reference Applications

- **[postgres-vertexai-demo](https://github.com/cofin/postgres-vertexai-demo)** — PostgreSQL `pgvector` semantic search and real-time chat with Litestar and Google ADK.
- **[oracledb-vertexai-demo](https://github.com/cofin/oracledb-vertexai-demo)** — Oracle vector search with HNSW indexes, Litestar, and Google ADK.

## Contributing & License

Contributions are welcome; see the [Contributing Guide](https://sqlspec.dev/contributing/) for local setup and adapter guidelines. Released under the [MIT License](LICENSE).
