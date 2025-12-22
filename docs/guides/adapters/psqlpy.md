---
orphan: true
---

# psqlpy Adapter Guide

This guide provides specific instructions for the `psqlpy` adapter for PostgreSQL.

## Key Information

-   **Driver:** `psqlpy`
-   **Parameter Style:** `numeric` (e.g., `$1, $2`)
-   **Type System:** Rust-level type conversion (not Python-level)

## Parameter Profile

-   **Registry Key:** `"psqlpy"`
-   **JSON Strategy:** `helper` (shared JSON serializer applied before Rust-side codecs)
-   **Extras:** Decimal writes coerce through `_decimal_to_float` to match Rust numeric expectations

## Architecture

Psqlpy handles type conversion differently than other PostgreSQL drivers:

### Python-Level Type Handlers (asyncpg, psycopg, oracledb)
- **asyncpg**: `connection.set_type_codec()` - register Python codecs
- **psycopg**: `register_vector()` - Python-level type adapters
- **oracledb**: `inputtypehandler`/`outputtypehandler` - Python callback functions

### Rust-Level Type Handlers (psqlpy)
- All type marshalling happens in **Rust layer**
- No Python-level type handler registration API exposed
- Custom types handled via:
  - **Input**: `PyCustomType(bytes)` wrapper
  - **Output**: `custom_decoders={"column": decoder_fn}` in `result()` method

## Type Handling

### Standard Types
Psqlpy provides explicit type wrappers in `psqlpy.extra_types`:

```python
from psqlpy.extra_types import (
    SmallInt, Integer, BigInt,
    Float32, Float64,
    PyText, PyVarChar,
    Point, Box, Path, Line, LineSegment, Circle,
    PyMacAddr6, PyMacAddr8,
    TextArray, VarCharArray,  # Array types
    PyJSONB,  # For lists in JSONB fields
)

await pool.execute(
    "INSERT INTO data (small_num, big_num, description) VALUES ($1, $2, $3)",
    [SmallInt(10), BigInt(1000000), PyText("Long text")]
)
```

### Custom Types (Unsupported Types)

For types not natively supported, use `PyCustomType` for input and custom decoders for output:

```python
from psqlpy.extra_types import PyCustomType

# Input: Send as bytes
await pool.execute(
    "INSERT INTO custom_table (custom_col) VALUES ($1)",
    [PyCustomType(b"SomeDataInBytes")]
)

# Output: Use custom decoder
def custom_decoder(bytes_from_psql: bytes | None) -> Any:
    return bytes_from_psql.decode() if bytes_from_psql else None

result = await pool.execute("SELECT custom_col FROM custom_table")
parsed_result = result.result(custom_decoders={"custom_col": custom_decoder})
```

### JSON/JSONB Handling

```python
from psqlpy.extra_types import PyJSONB

# Dictionaries can be used directly
dict_data = {"key": "value", "nested": {"data": 123}}
await pool.execute(
    "INSERT INTO users (info) VALUES ($1)",
    [dict_data]
)

# Lists MUST be wrapped in PyJSONB
list_data = [{"item": 1}, {"item": 2}]
await pool.execute(
    "INSERT INTO users (info) VALUES ($1)",
    [PyJSONB(list_data)]
)
```

### Array Types

```python
from psqlpy.extra_types import TextArray, VarCharArray

# Empty arrays need explicit type
await pool.execute(
    "INSERT INTO arr_table (tags) VALUES ($1)",
    [VarCharArray([])]  # NOT just []
)

# Non-empty arrays with explicit type
await pool.execute(
    "SELECT * FROM users WHERE name = ANY($1)",
    [TextArray(["Alice", "Bob", "Charlie"])]
)
```

## pgvector Support

### Current Limitation

**Psqlpy does NOT have native pgvector support** because:

1. No Python-level type handler registration API
2. pgvector-python library doesn't provide a psqlpy adapter
3. All type handling is Rust-level (no extension points)

### Workaround for Vector Types

You can still work with pgvector columns using string casting:

```python
# Sending vectors (PostgreSQL will cast string to vector)
await pool.execute(
    "INSERT INTO embeddings (id, vector) VALUES ($1, $2::vector)",
    [1, '[1.0, 2.0, 3.0]']  # Send as string with explicit cast
)

# Receiving vectors (with custom decoder)
def vector_decoder(bytes_data: bytes | None) -> list[float] | None:
    if not bytes_data:
        return None
    # PostgreSQL vector format: "[1,2,3]"
    text = bytes_data.decode('utf-8')
    return [float(x) for x in text.strip('[]').split(',')]

result = await pool.execute("SELECT vector FROM embeddings WHERE id = $1", [1])
embeddings = result.result(custom_decoders={"vector": vector_decoder})
```

### Why No `_type_handlers.py` for Psqlpy

Unlike asyncpg and psycopg adapters in SQLSpec, **psqlpy does NOT have a `_type_handlers.py` file** because:

1. **No Extension Points**: Psqlpy doesn't expose Python-level type handler registration
2. **Rust Architecture**: Type handling is Rust's responsibility, not Python's
3. **No pgvector Integration**: pgvector-python doesn't support psqlpy
4. **Alternative Exists**: Users can handle custom types via `PyCustomType` and `custom_decoders`

For comparison:
- **asyncpg**: Has `_type_handlers.py` with `register_json_codecs()` and `register_pgvector_support()`
- **psycopg**: Has `_type_handlers.py` with `register_pgvector_sync()` and `register_pgvector_async()`
- **oracledb**: Has `_numpy_handlers.py` with `register_numpy_handlers()`
- **psqlpy**: **No type handlers file** - all handled in Rust

## Query Stack Support

`psqlpy` does **not** expose a pipeline or batch API beyond the standard execute/execute_many entry points, so SQLSpec intentionally keeps the base sequential stack implementation:

- `execute_stack()` simply iterates operations using the shared transaction semantics from the driver base.
- Telemetry/logging still fire for observability, so stack executions remain traceable even without a performance boost.

If you need reduced round-trips on PostgreSQL, prefer the `asyncpg` or `psycopg` adapters, both of which provide native stack overrides.

## MERGE Operations (PostgreSQL 15+)

Psqlpy supports MERGE operations for bulk upserts using PostgreSQL's native MERGE statement with `jsonb_to_recordset()`.

### Single Row Upsert

```python
from sqlspec import sql

async with config.provide_session() as session:
    query = (
        sql.merge_
        .into("products", alias="t")
        .using({"id": 1, "name": "Widget", "price": 19.99}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
        .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
    )

    result = await session.execute(query)
    print(f"Rows affected: {result.rows_affected}")
```

### Bulk Upsert (High Performance)

For 100+ rows, Psqlpy automatically uses `jsonb_to_recordset()` for optimal performance:

```python
from decimal import Decimal

products = [
    {"id": 1, "name": "Widget", "price": Decimal("19.99")},
    {"id": 2, "name": "Gadget", "price": Decimal("29.99")},
    # ... up to 1000+ rows
]

query = (
    sql.merge_
    .into("products", alias="t")
    .using(products, alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)

result = await session.execute(query)
print(f"Upserted {result.rows_affected} rows")
```

Generated SQL uses PostgreSQL's efficient JSON-based strategy:

```sql
MERGE INTO products AS t
USING (
  SELECT * FROM jsonb_to_recordset($1) AS src(id INTEGER, name TEXT, price NUMERIC)
) AS src
ON t.id = src.id
WHEN MATCHED THEN UPDATE SET name = src.name, price = src.price
WHEN NOT MATCHED THEN INSERT (id, name, price) VALUES (src.id, src.name, src.price)
```

### Unified Upsert API

Use `sql.upsert()` for database-agnostic upsert operations:

```python
upsert_query = (
    sql.upsert("products", dialect="postgres")
    .using([{"id": 1, "name": "Widget", "price": 19.99}], alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)
```

For comprehensive examples and migration guides, see:

- [MERGE Statement Builder Guide](/guides/builder/merge.md)
- [Unified Upsert API Guide](/guides/upsert.md)

## Event Channels

- Psqlpy defaults to native LISTEN/NOTIFY support (`backend="listen_notify"`).
  Call `spec.event_channel(config)` to obtain a channel—no migrations required.
- Native listeners use the `Listener` API and a dedicated connection so the
  shared pool remains available for normal queries.
- For durability and retries, set `extension_config={"events": {"backend": "listen_notify_durable"}}`
  and include the `events` extension migrations.
- Force the durable queue fallback (for deterministic testing or multi-tenant
  workloads) by setting `extension_config={"events": {"backend": "table_queue"}}`
  and including the `events` migrations.

## Best Practices

-   **Rust-Based:** `psqlpy` is a modern, Rust-based driver for PostgreSQL, which can offer significant performance advantages.
-   **Async Only:** This is an asynchronous driver. Use it in `asyncio` applications.
-   **Explicit Type Wrappers:** Use explicit type wrappers from `psqlpy.extra_types` when Python's native types don't map correctly.
-   **Empty Arrays:** Always use typed array wrappers (e.g., `VarCharArray([])`) for empty arrays.
-   **LIMIT/OFFSET:** Use `BigInt(value)` for LIMIT and OFFSET parameters (PostgreSQL expects BIGINT).
-   **Custom Types:** For unsupported types, use `PyCustomType` for input and `custom_decoders` for output.

## Common Issues

-   **Compilation:** As a Rust-based library, `psqlpy` may require a Rust toolchain to be installed on the system for certain versions or platforms if a binary wheel is not available.
-   **Feature Parity:** As a newer driver, it may not have 100% feature parity with more established drivers like `asyncpg` or `psycopg`. Verify that it supports all the PostgreSQL features your application requires.
-   **Wrong Binary Data Error:** When using `WHERE = ANY()` with arrays, wrap the list in appropriate type (e.g., `TextArray(["a", "b"])`).
-   **Type Mismatch Errors:** If you get type errors, check if you need to use explicit type wrappers from `psqlpy.extra_types`.
-   **pgvector:** Native pgvector support not available - use string casting workaround shown above.
