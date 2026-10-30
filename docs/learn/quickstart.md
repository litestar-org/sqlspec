# Quickstart

Run your first parameterized query with zero external setup using Python's built-in SQLite driver:

```python
--8 < --"docs/examples/quickstart/first_query.py:example"
```

Execute the script with `uv`:

```bash
uv run python -m docs.examples.quickstart.first_query
```

Expected output:

```text
{'id': 1, 'name': 'Ada', 'points': 42}
```

## Recap

Key steps in `first_query.py`:

- **`SQLSpec()`**: Creates the central registry that manages database configurations, connection pools, and named SQL files.
- **`spec.add_config(...)`**: Registers a `SqliteConfig` pointing at an in-memory SQLite database and returns a typed handle.
- **`with spec.provide_session(config) as session:`**: Acquires a pooled connection and yields a `SqliteDriver` session that cleans up automatically on block exit.
- **`session.execute(..., name="Ada", points=42)`**: Compiles `:name` and `:points` placeholders for SQLite and binds keyword arguments safely without string interpolation.
- **`select_value` and `select_one`**: `select_value` extracts a single scalar column (`int`), while `select_one` returns the single matching row as a `dict[str, Any]` (raising `NotFoundError` if zero rows match).

## Positional vs Named Parameters

SQLSpec accepts both named keyword parameters (`:name`) and positional placeholders (`?` or `$1`) on any adapter, translating them to the underlying driver's wire syntax automatically:

```python
--8 < --"docs/examples/quickstart/basic_connection.py:example"
```

Use positional parameters for quick single-column statements and named parameters whenever a query binds multiple fields or composes `StatementFilter` objects.

## Asynchronous Sessions

Switching from synchronous `sqlite3` to `asyncio` uses the exact same session methods with `await` and `async with`. Install `sqlspec[aiosqlite]` and swap `SqliteConfig` for `AiosqliteConfig`:

```python
--8 < --"docs/examples/quickstart/async_connection.py:example"
```

Always call `await config.close_pool()` during application shutdown (or let a framework plugin manage lifespan cleanup automatically).

## Typed Row Mapping

By default, `select` and `select_one` return `dict[str, Any]` rows. Pass `schema_type=` to decode rows directly into `msgspec.Struct`, `TypedDict`, `pydantic.BaseModel`, `dataclass`, or `attrs` instances (`uv add "sqlspec[msgspec]"`):

```python
--8 < --"docs/examples/quickstart/typed_mapping.py:example"
```

Defining `msgspec.Struct(..., frozen=True, gc=False)` gives you immutable attribute access, static type checking, and lower allocation overhead than plain dictionaries.

## Moving from `:memory:` to a Local File

Replace `":memory:"` with a file path to persist SQLite state across process restarts while keeping the rest of your query code identical:

```python
from pathlib import Path

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

spec = SQLSpec()
config = spec.add_config(SqliteConfig(connection_config={"database": str(Path("app.db"))}))
```

When you are ready to point the same queries at PostgreSQL, Oracle, DuckDB, Spanner, or MySQL, swap `SqliteConfig` for the corresponding adapter configuration without rewriting your `session.select(...)` or `session.execute(...)` calls.

## Next Steps

| Goal | Recommended Guide |
| --- | --- |
| Register multiple pools, `bind_key` names, or custom statement settings | [Configuration](configuration.md) |
| Choose between `select`, `select_one_or_none`, `execute_many`, `StatementStack`, and `select_stream` | [Querying](querying.md) |
| Move multi-line SQL out of Python strings into `-- name:` `.sql` files | [SQL Files](sql-files.md) |
| Manage schema revisions with the `sqlspec` CLI | [Migrations](migrations.md) |
| Connect to PostgreSQL, Oracle, DuckDB, Spanner, BigQuery, MySQL, SQL Server, or Db2 | [Databases](/databases/) |
| Wire request-scoped sessions into Litestar, FastAPI, Google ADK, or Dishka | [Integrations](/integrations/) |
