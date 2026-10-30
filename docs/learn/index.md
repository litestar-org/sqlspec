# Learn SQLSpec

This curriculum walks through SQLSpec from your first in-memory SQLite query to production connection management, Arrow analytics, observability, and multi-database recipes.

Every core guide runs out of the box against Python's built-in `sqlite3` module (or `aiosqlite` for `asyncio`) so you can copy and run examples locally without starting external database containers. When you are ready to connect to PostgreSQL, Oracle, DuckDB, Spanner, BigQuery, MySQL, SQL Server, or Db2, pair these guides with the adapter setup pages under [Databases](/databases/).

## Execution Architecture

```mermaid
flowchart LR
    App["Application / Service"] --> Spec["SQLSpec Registry"]
    Spec --> Config["Adapter Config + Pool"]
    Config --> Session["Driver Session"]
    Session --> Compiler["SQL + Parameter Compiler"]
    Compiler --> Engine["Database Wire Driver"]
```

SQLSpec keeps each layer explicit:

1. **Registry and Config**: `SQLSpec` and typed `*Config` classes own connection pools, statement caches, named `.sql` loaders, and observability hooks.
2. **Driver Session**: Short-lived `session` context managers expose consistent `select*`, `execute*`, `select_stream`, and `select_to_arrow` methods across all 20 adapters.
3. **Statement Compiler**: Parameter styles (`:name`, `?`, `$1`, `@name`), `StatementFilter` predicates, and fluent `sql.*` builders compile once per SQL shape and reuse cached plans across sessions.

## How to Read This Guide

- **Run Every Snippet Locally**: Examples in `Learn` import `SqliteConfig` (sync) or `AiosqliteConfig` (async) and execute as standalone modules (`uv run python -m docs.examples...`).
- **Sync and Async Parity**: Every async driver mirrors the synchronous session surface with `async with` and `await`. Tabbed `Sync` / `Async` examples appear wherever lifecycle syntax differs.
- **Swap Engines Without Rewriting Queries**: Moving from SQLite in local tests to `AsyncpgConfig`, `OracleAsyncConfig`, `DuckDBConfig`, or `SpannerSyncConfig` in production changes only the config object and any engine-specific SQL features.

## Getting Started

Start here to install `sqlspec`, run your first parameterized query, and wire database configurations cleanly.

| Guide | Focus |
| --- | --- |
| [Installation](installation.md) | Install `sqlspec` with `uv` or `pip` and select optional extras by database family, web framework, serializer, or storage backend. |
| [Quickstart](quickstart.md) | Execute synchronous and asynchronous SQLite queries and decode rows into `msgspec.Struct` models in under five minutes. |
| [Configuration](configuration.md) | Register single or multiple databases on `SQLSpec`, choose pooled vs direct configs, customize `StatementConfig`, and attach lifecycle hooks. |

## Core Querying and Transactions

Master day-to-day query execution, parameter safety, typed row hydration, and explicit transaction boundaries.

| Guide | Focus |
| --- | --- |
| [Querying](querying.md) | Choose between `execute`, `select`, `select_one`, `select_one_or_none`, `select_value`, `execute_many`, `StatementStack`, and `select_stream`. |
| [Parameters](parameters.md) | Bind named and positional parameters safely across dialects and avoid mixed-parameter pitfalls when composing filters. |
| [Schema Mapping](schema-mapping.md) | Project result rows into `msgspec.Struct`, `TypedDict`, `pydantic.BaseModel`, `dataclasses`, and `attrs` classes with `schema_type`. |
| [Transactions](transactions.md) | Manage explicit `begin`, `commit`, `rollback`, nested savepoints, and framework request commit modes. |

## SQL Files, Query Builder, and Filtering

Organize larger query suites without giving up SQL readability or composable runtime predicates.

| Guide | Focus |
| --- | --- |
| [SQL Files](sql-files.md) | Load `-- name:` SQL blocks from disk or cloud storage, validate `-- param:` headers, and append dynamic `WHERE` clauses. |
| [Query Builder](query-builder.md) | Construct parameterized `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, upsert, join, and `RETURNING` statements with `sql`. |
| [Filtering and Pagination](filtering-and-pagination.md) | Apply composable `StatementFilter` objects (`LimitOffsetFilter`, `BeforeAfterFilter`, `SearchFilter`, `OrderByFilter`) and keyset cursors. |

## Operations, Analytics, and Quality

Operate schemas, move columnar data, instrument production queries, and keep test suites fast.

| Guide | Focus |
| --- | --- |
| [Migrations](migrations.md) | Author sequential `.sql` and `.py` migrations, configure `pyproject.toml`, and isolate version-tracking schemas. |
| [CLI](cli.md) | Inspect configs, scaffold migrations, apply or squash revisions, and run ADK memory maintenance from the `sqlspec` CLI. |
| [Arrow and Storage](arrow-and-storage.md) | Stream query results to Apache Arrow, Polars, or Pandas via `select_to_arrow` and run Parquet/JSONL pipelines with `StorageRegistry`. |
| [Data Dictionary](data-dictionary.md) | Introspect tables, columns, indexes, foreign keys, server versions, and dialect-optimal column types across engines. |
| [Observability](observability.md) | Configure `StatementObserver` callbacks, OpenTelemetry spans, Prometheus metrics, wire-level `sqlcommenter`, and log redaction. |
| [Testing](testing.md) | Combine in-memory SQLite unit tests, `pytest-databases` container fixtures, and JSONL table fixtures. |
| [Performance](performance.md) | Understand per-config statement caching, `SlotPooledStatement`, `msgspec` hydration, `sqlspec[mypyc]` wheels, and Arrow batch sizing. |

## Production Recipes

Combine SQLSpec primitives into reusable application patterns in [Production Recipes](recipes/index.md):

- [Service Layer](recipes/service-layer.md) — encapsulate short-lived reads and atomic write transactions with `SQLSpecSyncService` and `SQLSpecAsyncService`.
- [Multi-Tenancy](recipes/multi-tenancy.md) — isolate tenants by `bind_key`, schema search paths, or row-level predicates.
- [Health Checks](recipes/health-checks.md) — build lightweight readiness and liveness probes across pooled sync and async adapters.
- [ETL and Bulk Ingest](recipes/etl-and-bulk-ingest.md) — stream chunks, Arrow batches, and batched writes between operational and analytical stores.
