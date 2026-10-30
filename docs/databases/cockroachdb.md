# CockroachDB

SQLSpec provides two dedicated CockroachDB adapters—`sqlspec.adapters.cockroach_asyncpg` (`CockroachAsyncpgConfig`) and `sqlspec.adapters.cockroach_psycopg` (`CockroachPsycopgSyncConfig`, `CockroachPsycopgAsyncConfig`)—layering distributed `40001` transaction retry policies, bounded exponential jitter, and `AS OF SYSTEM TIME` follower reads onto PostgreSQL wire drivers.

## Install and When to Choose

```bash
uv add "sqlspec[cockroachdb]"
```

- Choose **`CockroachAsyncpgConfig`** for pure-`asyncio` services prioritizing binary-protocol performance.
- Choose **`CockroachPsycopgSyncConfig`** or **`CockroachPsycopgAsyncConfig`** when sharing sync/async pools or libpq TLS/GSSAPI certificate options.

## Minimal Connection and Pool Setup

```{ .python title="docs/examples/drivers/cockroach_psycopg_connection.py" }
--8<-- "docs/examples/drivers/cockroach_psycopg_connection.py:example"
```

=== "Async (`CockroachAsyncpgConfig`)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig

    spec = SQLSpec()
    config = spec.add_config(
        CockroachAsyncpgConfig(
            connection_config={
                "dsn": (
                    "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
                ),
                "max_size": 20,
            },
            driver_features={
                "enable_auto_retry": True,
                "max_retries": 5,
                "enable_follower_reads": False,
            },
        )
    )
    ```

=== "Async (`CockroachPsycopgAsyncConfig`)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        CockroachPsycopgAsyncConfig(
            connection_config={
                "conninfo": (
                    "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
                )
            },
            driver_features={"enable_auto_retry": True, "max_retries": 5},
        )
    )
    ```

## Key Driver Features and Pool Tuning

| `driver_features` Key | Type | Description |
| --- | --- | --- |
| `enable_auto_retry` | `bool` | Automatically retries `SQLSTATE 40001` (`SerializationConflictError`) failures on idempotent/retryable execution blocks. |
| `max_retries` | `int` | Maximum retry attempts before surfacing `SerializationConflictError` to the caller. |
| `retry_delay_base_ms` / `retry_delay_max_ms` | `float` | Exponential backoff base and cap in milliseconds (randomized with `secrets.SystemRandom()` jitter). |
| `enable_retry_logging` | `bool` | Emits structured debug logs when `40001` serialization retries trigger. |
| `enable_follower_reads` | `bool` | Enables bounded-staleness reads from nearest replica nodes for read-only queries. |
| `default_staleness` | `str` | Follower-read staleness expression (such as `"follower_read_timestamp()"` or `"-10s"`). |
| `enable_native_storage` | `bool` | Uses CockroachDB native CSV/nodelocal/cloud storage import/export paths where configured. |
| `enable_pgvector` | `bool` | Enables CockroachDB `VECTOR` column codecs and similarity search in ADK stores (`cockroach_asyncpg`). |

See [Cockroach AsyncPG Reference](/reference/adapters/cockroach-asyncpg/) and [Cockroach Psycopg Reference](/reference/adapters/cockroach-psycopg/) for full configuration and retry TypedDict tables.

## Type Mapping and Dialect Notes

- **Serializable Isolation by Default**: CockroachDB runs every transaction at `SERIALIZABLE` isolation. Multi-statement read-modify-write transactions under contention may receive `SQLSTATE 40001`, which SQLSpec maps to `SerializationConflictError` and retries when `enable_auto_retry=True` or inside retry helpers.
- **Follower Read Transaction Scope**: When `enable_follower_reads=True`, the driver enters an explicit read-only transaction before applying `SET TRANSACTION AS OF SYSTEM TIME ...` and commits it cleanly once the read finishes so pooled connections never leak stale-read state.
- **Primary Keys and `UUID` / ` unique_rowid()`**: Prefer `UUID DEFAULT gen_random_uuid()` over monotonic `SERIAL` sequences on high-write tables to prevent single-range hotspotting across CockroachDB nodes.
- **Events Queue**: CockroachDB does not implement PostgreSQL `LISTEN/NOTIFY`; `EventChannel` uses SQLSpec's transactional table-backed queue store (`CockroachAsyncpgEventQueueStore` / `CockroachPsycopg*EventQueueStore`) automatically.
