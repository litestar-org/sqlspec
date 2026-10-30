# PostgreSQL — asyncpg

`sqlspec.adapters.asyncpg` wraps MagicStack's Cython `asyncpg` driver, delivering low-latency binary-protocol execution, managed `asyncio` connection pooling, Cloud SQL and AlloyDB IAM connectors, `LISTEN/NOTIFY` events, and `pgvector` / ParadeDB ADK stores.

## Install and When to Choose

```bash
uv add "sqlspec[asyncpg]"
```

Choose `asyncpg` for pure-`asyncio` APIs and background workers that prioritize binary-protocol throughput, per-statement query timeouts, and automatic custom type codecs. If the same codebase also requires synchronous DB-API connections, see [`psycopg`](psycopg.md).

## Minimal Connection and Pool Setup

```{ .python title="docs/examples/drivers/asyncpg_connection.py" }
--8<-- "docs/examples/drivers/asyncpg_connection.py:example"
```

Open request or task sessions with `async with spec.provide_session(config) as session:` and close the pool on shutdown via `await config.close_pool()`:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

spec = SQLSpec()
config = spec.add_config(
    AsyncpgConfig(
        connection_config={
            "dsn": "postgresql://app:secret@localhost:5432/prod",
            "min_size": 2,
            "max_size": 20,
            "command_timeout": 30.0,
        },
        driver_features={"pooler_mode": "session", "enable_pgvector": True},
    )
)
```

## Key Driver Features and Pool Tuning

| Setting | Location | Description |
| --- | --- | --- |
| `dsn` / `host`, `port`, `user`, `password`, `database` | `connection_config` | Target PostgreSQL DSN or discrete connection endpoints. |
| `min_size` / `max_size` | `connection_config` | Minimum warm connections and maximum concurrent pool size. |
| `max_queries` / `max_inactive_connection_lifetime` | `connection_config` | Connection rotation thresholds preventing backend memory growth. |
| `command_timeout` | `connection_config` | Default per-command timeout in seconds (`timeout=` per query overrides). |
| `pooler_mode` | `driver_features` | `"session"` (default), `"transaction"`, or `"statement"`; disables protocol statement caches on transaction/statement poolers. |
| `direct_dsn` | `driver_features` | Direct PostgreSQL DSN used by `derive_direct_config()` for DDL migrations and `ListenerHub`. |
| `enable_cloud_sql` / `cloud_sql_instance` | `driver_features` | Connects through `cloud-sql-python-connector` (`enable_cloud_sql=True`, `cloud_sql_enable_iam_auth=True`). |
| `enable_alloydb` / `alloydb_instance_uri` | `driver_features` | Connects through `google-cloud-alloydb-connector` with optional `enable_alloydb_columnar=True`. |
| `enable_json_codecs` / `type_codecs` | `driver_features` | Registers binary `json` / `jsonb` serializers and custom PostgreSQL OID codecs on every connection. |

See [AsyncPG API Reference](/reference/adapters/asyncpg/) for the complete `AsyncpgConnectionConfig`, `AsyncpgPoolConfig`, and `AsyncpgDriverFeatures` TypedDict tables.

## Type Mapping and Dialect Notes

- **Untyped Parameters Inside Polymorphic Functions**: Because `asyncpg` prepares statements over PostgreSQL's binary protocol, untyped parameters inside functions like `jsonb_build_object(:k, :v)` require an explicit SQL cast such as `CAST(:k AS text)` so PostgreSQL can resolve the parameter OID.
- **Binding JSON String Primitives**: When `enable_json_codecs=True` (default), `asyncpg`'s JSON encoder treats Python `dict`, `list`, `int`, and `bool` values by serializing them with `json_serializer`, while treating bare Python `str` values as pre-serialized JSON text. If you store a bare JSON string literal (rather than an object or array) into a `jsonb` column, pass `to_json("value")` so it is sent over the wire as valid quoted JSON (`'"value"'`).
- **`StatementStack` Execution**: `asyncpg`'s Cython protocol forbids concurrent overlapping operations on a single connection; `session.execute_stack(stack)` executes steps sequentially against cached prepared statements and records `sequential_prepared` in observer telemetry.
- **Binary `COPY` and Arrow Ingest**: `load_from_arrow` streams Arrow batches into PostgreSQL via binary `COPY ... FROM STDIN` for high-throughput bulk loads.
- **Row-Locking with CTEs**: When combining `FOR UPDATE SKIP LOCKED` with a parameter CTE (`FROM jobs, ctx`), qualify the target table explicitly (`FOR UPDATE OF jobs SKIP LOCKED`) because PostgreSQL cannot row-lock a CTE alias.
