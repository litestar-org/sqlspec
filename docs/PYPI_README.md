# SQLSpec

**Type-safe SQL query mapper with minimal abstraction between Python and SQL.**

SQLSpec keeps you close to the SQL you already write while providing typed
results, automatic parameter handling, and a unified driver interface across
popular databases (PostgreSQL, SQLite, DuckDB, MySQL, Oracle, BigQuery, and
more). It is not an ORM. Think of it as a connectivity and query mapping layer
that favors raw SQL, observability, and predictable behavior.

## Status

SQLSpec is currently in active development. The public API may change at
any time and production use is not yet recommended. Follow the
[docs](https://sqlspec.dev/) and changelog for updates.

## Highlights

- **SQL first**: Validate and execute the SQL you write, with helpers for
  statement stacks, SQL file loading, and dialect-aware compilation.
- **SQL AST pipeline**: Every statement is processed by the `sqlglot` library for validation, dialect tuning, and caching before it ever hits the driver.
- **Unified connectivity**: One session API for sync and async drivers across
  a growing list of adapters (psycopg, asyncpg, aiosqlite, DuckDB, BigQuery,
  Oracle, asyncmy, ADBC, and more).
- **Typed results**: Map rows directly into Pydantic, Msgspec, attrs, or
  dataclasses for predictable data structures.
- **Statement stack + builder**: Compose multi-statement workloads, stream
  them through the stack observer pipeline, and rely on Arrow export support
  across every driver when you need columnar results.
- **SQL file loading**: Ship named queries alongside your code and load them
  aiosql-style with observability, caching, and parameter validation baked in.
- **Framework integrations**: Litestar plugin with automatic dependency
  injection plus extension points for FastAPI, Starlette, and others.
- **Observability ready**: Built-in instrumentation hooks for OpenTelemetry
  and Prometheus, plus structured logging guidance.

## Quick Start

### Install

```bash
pip install "sqlspec[sqlite]"
```

### Run your first query

```python
from pydantic import BaseModel

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig


class Greeting(BaseModel):
    message: str


sql = SQLSpec()
sqlite_db = sql.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

with sql.provide_session(sqlite_db) as session:
    greeting = session.select_one(
        "SELECT 'Hello, SQLSpec!' AS message",
        schema_type=Greeting,
    )
    print(greeting.message)
```

Explore the [Getting Started guide](https://sqlspec.dev/getting_started/)
for installation variants, driver selection, and typed result mapping.

## Documentation

- [Getting Started](https://sqlspec.dev/getting_started/)
- [Usage Guides](https://sqlspec.dev/usage/)
- [Examples Gallery](https://sqlspec.dev/examples/)
- [API Reference](https://sqlspec.dev/reference/)
- [CLI Reference](https://sqlspec.dev/usage/cli.html)

## Ecosystem Snapshot

- **Adapters**: PostgreSQL (psycopg, asyncpg, psqlpy), SQLite (sqlite3,
  aiosqlite), DuckDB (native + ADBC), MySQL (asyncmy), Oracle (oracledb),
  BigQuery, Snowflake, and additional ADBC targets.
- **Extensions**: Litestar integration, SQL file loader, storage backends,
  telemetry observers, and experimental SQL builder.
- **Tooling**: Migration CLI, stack execution observers, driver parameter
  profiles, and Arrow-friendly storage helpers.

See the [usage docs](https://sqlspec.dev/usage/) for the latest adapter matrix,
configuration patterns, and feature deep dives—including the
[SQL file loader guide](https://sqlspec.dev/usage/loader.html).

## Contributing

Contributions, issue reports, and adapter ideas are welcome. Review the
[contributor guide](https://sqlspec.dev/contributing/) and follow the project
coding standards before opening a pull request.

## License

SQLSpec is distributed under the MIT License.
