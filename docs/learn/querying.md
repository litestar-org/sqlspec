# Querying

Every SQLSpec driver exposes a uniform set of query methods on `session` (sync) and `await session` (async) so you can express row cardinality intent directly at the call site without unpacking raw DB-API cursors.

```mermaid
flowchart LR
    Input["SQL String / SQL(...) / Loader / Builder"] --> Compile["Compile & Statement Cache"]
    Compile --> Execute["Adapter Wire Execution"]
    Execute --> Result["SQLResult / ArrowResult"]
    Result --> Output["dict / msgspec / Pydantic / Arrow"]
```

## Unified Query Authoring and Execution Mental Model

Every execution method (`session.select*`, `session.fetch*`, `session.execute*`) accepts any of SQLSpec's four query authoring surfaces alongside positional `*filters: StatementFilter` and keyword parameters:

| Authoring Surface | Entry Point | Runtime Modifiers & Filters | Best For |
| --- | --- | --- | --- |
| Raw SQL string | `"SELECT id, name FROM users WHERE active = :active"` | Accepts `*filters: StatementFilter` and `**params` directly at execution time | Quick inline queries, health probes, and scripts |
| Immutable `SQL` object | `SQL("SELECT id, name FROM users", active=1).where(...)` | Supports `.where(...)`, `.where_eq(...)`, `.order_by(...)`, `.paginate(...)`, and `*filters` | Reusable SQL templates that append dynamic predicates |
| Named `.sql` file (`SQLFileLoader`) | `spec.get_sql("list_users")` | Returns a `SQL` object with `-- param:` validation, slots, `.where(...)`, and `*filters` | Multi-line reporting SQL, CTEs, and SQL-first repositories |
| Fluent `QueryBuilder` (`sql.*`) | `sql.select("id", "name").from_("users").where_eq("active", 1)` | Chains `.where*`, `.join`, `.order_by`, `.limit`, dialect upserts/merges, and `*filters` | Programmatic query construction and cross-dialect DML/DDL |

## Choosing the Right Execution Method

SQLSpec provides both `select*` methods and `asyncpg`-style `fetch*` aliases with identical behavior and static typing:

| Method | Fetch Alias | Return Value | Behavior on Empty Result |
| --- | --- | --- | --- |
| `execute(sql, ...)` | — | `SQLResult` | Returns metadata (`rows_affected`) and row helpers (`.all()`, `.one()`, `.one_or_none()`) |
| `select(sql, ...)` | `fetch(sql, ...)` | `list[dict[str, Any]]` or `list[SchemaT]` | Returns `[]` |
| `select_one(sql, ...)` | `fetch_one(sql, ...)` | `dict[str, Any]` or `SchemaT` | Raises `NotFoundError` (and raises if multiple rows match) |
| `select_one_or_none(sql, ...)` | `fetch_one_or_none(sql, ...)` | `dict[str, Any] or SchemaT or None` | Returns `None` (raises if multiple rows match) |
| `select_value(sql, ..., value_type=...)` | `fetch_value(sql, ...)` | Scalar value (`Any` or `ValueT`) | Raises `NotFoundError` |
| `select_value_or_none(sql, ...)` | `fetch_value_or_none(sql, ...)` | Scalar value or `None` | Returns `None` |
| `select_with_total(sql, ...)` | `fetch_with_total(sql, ...)` | `tuple[list[...], int]` | Returns `([], 0)` alongside the total unpaginated count |
| `select_stream(sql, ...)` | `fetch_stream(sql, ...)` | `SyncRowStream` or `AsyncRowStream` | Yields rows in bounded `chunk_size` batches |
| `select_to_arrow(sql, ...)` | `fetch_to_arrow(sql, ...)` | `ArrowResult` | Returns an empty Arrow `Table` or `RecordBatch` |
| `execute_many(sql, seq_of_params)` | — | `SQLResult` | Executes batched DML across a sequence of parameter tuples or dicts |
| `execute_script(sql)` | — | `SQLResult` | Runs multi-statement DDL or seed scripts |

Note that there is no `select_value_list` method on driver sessions; to extract a list of values from a single column, call `select(...)` and project the column key from each returned row.

## Typed Projection with Schema Types

Pass `schema_type=` to any `select*` or `fetch*` method (or `SQLResult.all(schema_type=...)`) to hydrate rows into typed models (covered in depth in [Schema Mapping](schema-mapping.md)):

| Target `schema_type` | Install Extra | Runtime & Typing Behavior |
| --- | --- | --- |
| `None` *(default)* | *(built-in)* | Returns `dict[str, Any]` rows |
| `msgspec.Struct` | `sqlspec[msgspec]` | Fastest C-extension struct decoding with static attribute types |
| `typing.TypedDict` | *(built-in)* | Static dict key/value typing with zero runtime conversion overhead |
| `pydantic.BaseModel` | `sqlspec[pydantic]` | Pydantic validation and model construction |
| `dataclasses.dataclass` | *(built-in)* | Standard-library dataclass hydration |
| `attrs` (`@define`) | `sqlspec[attrs]` | `attrs` class instantiation |

## Inspecting SQLResult Metadata

While `select*` and `fetch*` helpers return rows or scalars directly, `session.execute(...)` returns a `SQLResult` carrying affected row counts, column names, and convenience accessors for `INSERT ... RETURNING` or `UPDATE` statements:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

spec = SQLSpec()
config = spec.add_config(
    SqliteConfig(connection_config={"database": ":memory:"})
)

with spec.provide_session(config) as session:
    session.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)"
    )
    insert_result = session.execute(
        "INSERT INTO items (name, active) VALUES (:name, :active)",
        name="Keyboard",
        active=1,
    )
    assert insert_result.rows_affected == 1

    returning_result = session.execute(
        """
        UPDATE items
        SET active = :active
        WHERE name = :name
        RETURNING id, name
        """,
        active=0,
        name="Keyboard",
    )
    updated_row = returning_result.one()
    assert updated_row == {"id": 1, "name": "Keyboard"}
```

## Batched Writes, Scalars, and Window-Count Pagination

Use `execute_many` for multi-row inserts or updates, `select_value` / `select_value_or_none` for scalar aggregates or lookups, and `select_with_total` (or `fetch_with_total`) when pairing a paginated query with a total row count. Pass `count_with_window=True` to compute the total count inside a single query via `COUNT(*) OVER()` instead of executing a separate `SELECT COUNT(*)` round-trip:

```{ .python title="docs/examples/querying/batch_operations.py" }
--8<-- "docs/examples/querying/batch_operations.py:example"
```

Use `count_with_window=True` when the current page typically returns at least one row and avoiding a second database round-trip matters; leave the default `count_with_window=False` when paginating past the end of the result set still needs an exact non-zero total.

## Multi-Step Execution with StatementStack

When you want to group ordered DDL, batched DML, and `SELECT` operations into a single logical sequence—taking advantage of native driver pipelining where supported (such as PostgreSQL `asyncpg` or Oracle 23ai/26ai)—compose a `StatementStack` and pass it to `session.execute_stack(stack)`:

```{ .python title="docs/examples/querying/statement_stack.py" }
--8<-- "docs/examples/querying/statement_stack.py:example"
```

## Bounded Cursor Streaming

Materializing millions of rows with `select()` buffers the entire result set in memory. Use `session.select_stream(..., chunk_size=...)` or `session.fetch_stream(...)` to iterate over rows in bounded chunks:

```{ .python title="docs/examples/querying/streaming_results.py" }
--8<-- "docs/examples/querying/streaming_results.py:example"
```

=== "Sync"

    ```python
    query = "SELECT id, payload FROM events ORDER BY id"
    with session.select_stream(query, chunk_size=500) as stream:
        for row in stream:
            process_row(row)
    ```

=== "Async"

    ```python
    query = "SELECT id, payload FROM events ORDER BY id"
    async with session.select_stream(query, chunk_size=500) as stream:
        async for row in stream:
            await process_row(row)
    ```

For columnar analytics or Parquet export over large result sets, prefer `session.select_to_arrow(...)` (covered in [Arrow and Storage](arrow-and-storage.md)).

## Recap

- Prefer `select*` (or `fetch*` aliases) for concise, self-documenting reads, and `execute` when you need `SQLResult.rows_affected` on `INSERT`, `UPDATE`, or `DELETE` statements.
- Pass `count_with_window=True` to `select_with_total` to fold total counting into a single `COUNT(*) OVER()` query.
- Switch to `select_stream` (row-oriented) or `select_to_arrow` (columnar) whenever result sets exceed comfortable per-request memory bounds.
