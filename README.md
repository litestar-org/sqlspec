# SQLSpec

**Type-safe SQL execution layer for Python.**

SQLSpec handles database connectivity and result mapping so you can focus on SQL. Write raw queries when you need precision, use the builder API when you need composability, or load SQL from files when you need organization. Every statement passes through a [sqlglot](https://github.com/tobymao/sqlglot)-powered AST pipeline for validation, dialect conversion, and optimization before execution. Export results as Python objects, Arrow tables, Polars or pandas DataFrames.

It's not an ORM. It's the connectivity and processing layer between your application and your database that provides the right abstraction for each situation without dictating how you write SQL.

## Status

SQLSpec is currently in active development. The public API may change. Follow the [docs](https://sqlspec.dev/) and changelog for updates.

## What You Get

**Connection Management**

- Connection pooling with configurable size, timeout, and lifecycle hooks
- Sync and async support with a unified API surface
- Adapters for PostgreSQL (psycopg, asyncpg, psqlpy), SQLite (sqlite3, aiosqlite), DuckDB, MySQL (asyncmy), Oracle, BigQuery, and ADBC-compatible databases

**Query Execution**

- Raw SQL strings with automatic parameter binding and dialect translation
- SQL AST parsing via sqlglot for validation, optimization, and dialect conversion
- Builder API for programmatic query construction without string concatenation
- SQL file loading to keep queries organized alongside your code (aiosql-style)
- Statement stacks for batching multiple operations with transaction control

**Result Handling**

- Type-safe result mapping to Pydantic, msgspec, attrs, or dataclasses
- Apache Arrow export for zero-copy integration with pandas, Polars, and analytical tools
- Result iteration, single-row fetch, or bulk retrieval based on your use case

**Framework Integration**

- Litestar plugin with dependency injection for connections, sessions, and pools
- Starlette/FastAPI middleware for automatic transaction management
- Flask extension with sync/async portal support

**Production Features**

- SQL validation and caching via sqlglot AST parsing
- OpenTelemetry and Prometheus instrumentation hooks
- Structured logging with correlation ID support
- Migration CLI for schema versioning

## Quick Start

### Install

```bash
pip install "sqlspec"
```

### Run your first query

```python
from pydantic import BaseModel
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

class Greeting(BaseModel):
    message: str

spec = SQLSpec()
db = sql.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

with spec.provide_session(db) as session:
    greeting = session.select_one(
        "SELECT 'Hello, SQLSpec!' AS message",
        schema_type=Greeting,
    )
    print(greeting.message)  # Output: Hello, SQLSpec!
```

That's it. Write SQL, define a schema, get typed objects back. Connection pooling, parameter binding, and result mapping are handled automatically.

See the [Getting Started guide](https://sqlspec.dev/getting_started/) for installation variants, adapter selection, and advanced result mapping options.

## Documentation

- [Getting Started](https://sqlspec.dev/getting_started/)
- [Usage Guides](https://sqlspec.dev/usage/)
- [Examples Gallery](https://sqlspec.dev/examples/)
- [API Reference](https://sqlspec.dev/reference/)
- [CLI Reference](https://sqlspec.dev/usage/cli.html)

## Reference Applications

- **[PostgreSQL + Vertex AI Demo](https://github.com/cofin/postgres-vertexai-demo)** - Vector search with pgvector and real-time chat using Litestar and Google ADK. Shows connection pooling, migrations, type-safe result mapping, vector embeddings, and response caching.
- **[Oracle + Vertex AI Demo](https://github.com/cofin/oracledb-vertexai-demo)** - Oracle 23ai vector search with semantic similarity using HNSW indexes. Demonstrates NumPy array conversion, large object (CLOB) handling, and real-time performance metrics.

See the [usage docs](https://sqlspec.dev/usage/) for detailed guides on adapters, configuration patterns, and features like the [SQL file loader](https://sqlspec.dev/usage/loader.html).

## Built With

- **[sqlglot](https://github.com/tobymao/sqlglot)** - SQL parser, transpiler, and optimizer powering SQLSpec's AST pipeline

## Contributing

Contributions, issue reports, and adapter ideas are welcome. Review the
[contributor guide](https://sqlspec.dev/contributing/) and follow the project
coding standards before opening a pull request.

## License

SQLSpec is distributed under the MIT License.
