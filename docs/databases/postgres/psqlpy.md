# PostgreSQL — psqlpy

`sqlspec.adapters.psqlpy` integrates `psqlpy`—a Rust/PyO3 `asyncio` PostgreSQL driver built on Tokio—combining native Rust connection pooling, true wire-level transaction pipelining (`StatementStack`), and built-in `PgVector` parameter support.

## Install and When to Choose

```bash
uv add "sqlspec[psqlpy]"
```

Choose `psqlpy` for latency-sensitive `asyncio` workloads that benefit from Tokio's multi-threaded Rust socket pool, single-round-trip wire pipelining on `execute_stack`, and `pgvector` bindings without requiring an extra Python `pgvector` client dependency.

## Minimal Connection and Pool Setup

```python
from sqlspec import SQLSpec
from sqlspec.adapters.psqlpy import PsqlpyConfig

spec = SQLSpec()
config = spec.add_config(
    PsqlpyConfig(
        connection_config={"dsn": "postgresql://app:secret@localhost:5432/prod", "max_db_pool_size": 20},
        driver_features={"pooler_mode": "session", "enable_pgvector": True},
    )
)
```

Acquire sessions via `async with spec.provide_session(config) as session:` and release the Rust pool on shutdown with `await config.close_pool()`.

## Key Driver Features and Pool Tuning

| Setting | Location | Description |
| --- | --- | --- |
| `dsn` / `host`, `port`, `username`, `password`, `db_name` | `connection_config` | PostgreSQL connection URI or discrete Rust pool endpoint settings. |
| `max_db_pool_size` | `connection_config` | Maximum concurrent connections managed by the Tokio `ConnectionPool`. |
| `connect_timeout_sec` / `connect_timeout_nanosec` | `connection_config` | Socket establishment timeout bounds. |
| `keepalive` / `tcp_user_timeout_sec` | `connection_config` | TCP keepalive probes for detecting dropped firewall or load-balancer sockets. |
| `pooler_mode` | `driver_features` | `"session"`, `"transaction"`, or `"statement"`; passes `prepared=False` automatically in transaction and statement pooler modes. |
| `direct_dsn` | `driver_features` | Unpooled PostgreSQL DSN used by `derive_direct_config()` for `ListenerHub` (`LISTEN/NOTIFY`) and DDL migrations. |
| `enable_alloydb_columnar` | `driver_features` | Enables `google_columnar_engine.enabled = 'on'` on Google Cloud AlloyDB. |
| `enable_cast_detection` | `driver_features` | Detects explicit `::vector` / `CAST(... AS vector)` expressions to coerce Python lists into `psqlpy.extra_types.PgVector`. |
| `enable_pgvector` / `enable_paradedb` / `enable_pg_textsearch` | `driver_features` | Enables vector similarity, ParadeDB BM25, and `pg_textsearch` probing in ADK memory stores. |

See [Psqlpy API Reference](/reference/adapters/psqlpy/) for complete `PsqlpyConnectionParams`, `PsqlpyPoolParams`, and `PsqlpyDriverFeatures` TypedDict definitions.

## Type Mapping and Dialect Notes

- **Wire-Level `StatementStack` Pipelining**: Unlike drivers that serialize `execute_stack` step-by-step, `PsqlpyDriver.execute_stack` dispatches eligible multi-statement stacks over `connection.transaction().pipeline(...)` in a single network batch while preserving `rows_affected` counts on non-`RETURNING` DML statements.
- **Wire `sqlcommenter` Cache Stability**: DML row-count wrappers are computed before dynamic wire-level `sqlcommenter` trace comments are appended, keeping internal LRU cache entries stable across traced HTTP requests.
- **Native `PgVector` Coercion**: Python `list[float]` sequences bound to `VECTOR` columns convert directly to `psqlpy.extra_types.PgVector`, and `PsqlpyADKMemoryStore` decodes text, UTF-8 bytes, and binary `vector_send` frames out of the box.
- **Binary `BYTEA` Payloads**: `psqlpy`'s Rust PyO3 boundary expects `bytes` or `list[int]` sequences for `BYTEA` parameters; SQLSpec's type converter and Litestar/ADK stores handle `bytes` coercion transparently.
- **Rust Extension Object Semantics**: `psqlpy.Connection` is a slotted Rust PyO3 type without a Python `__dict__`; avoid attaching ad-hoc attributes to `session.connection` inside custom `on_connection_create` hooks.
