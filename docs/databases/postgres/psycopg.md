# PostgreSQL — psycopg

`sqlspec.adapters.psycopg` wraps Psycopg 3 (`psycopg[binary,pool]`), providing symmetric **synchronous** (`PsycopgSyncConfig`) and **asynchronous** (`PsycopgAsyncConfig`) connection pools, `NullConnectionPool` support for serverless runtimes, streaming `COPY`, and full libpq compatibility.

## Install and When to Choose

```bash
uv add "sqlspec[psycopg]"
```

Choose `psycopg` when your service mixes synchronous CLI/worker processes with `asyncio` HTTP servers, relies on standard `libpq` environment variables and `.pgpass` / GSSAPI / Kerberos authentication, or runs on scale-to-zero serverless platforms where `null_pool=True` avoids idle connections.

## Minimal Connection and Pool Setup

=== "Async (`PsycopgAsyncConfig`)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        PsycopgAsyncConfig(
            connection_config={
                "conninfo": "postgresql://app:secret@localhost:5432/prod",
                "min_size": 2,
                "max_size": 20,
            }
        )
    )
    ```

=== "Sync (`PsycopgSyncConfig`)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.psycopg import PsycopgSyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        PsycopgSyncConfig(
            connection_config={
                "conninfo": "postgresql://app:secret@localhost:5432/prod",
                "min_size": 2,
                "max_size": 10,
            }
        )
    )
    ```

## Key Driver Features and Pool Tuning

| Setting | Location | Description |
| --- | --- | --- |
| `conninfo` | `connection_config` | Standard PostgreSQL URI or space-separated libpq keyword/value string. |
| `min_size` / `max_size` | `connection_config` | Pool bounds passed to `psycopg_pool.ConnectionPool` or `AsyncConnectionPool`. |
| `timeout` / `max_waiting` | `connection_config` | Pool checkout timeout (seconds) and maximum queued callers before raising pool timeout errors. |
| `max_lifetime` / `max_idle` | `connection_config` | Connection recycle and idle eviction intervals in seconds. |
| `null_pool` | `driver_features` | Instantiates `NullConnectionPool` / `AsyncNullConnectionPool` (zero idle connections while still enforcing `max_size` concurrency). |
| `pooler_mode` | `driver_features` | `"session"`, `"transaction"`, or `"statement"`; sets `prepare_threshold=None` automatically in transaction/statement modes. |
| `direct_dsn` | `driver_features` | Direct PostgreSQL URI used by `derive_direct_config()` for migrations and `LISTEN/NOTIFY`. |
| `enable_alloydb` / `enable_alloydb_columnar` | `driver_features` | Connects via AlloyDB connector and enables the AlloyDB columnar engine while honoring `prepare_threshold`. |
| `enable_pgvector` / `enable_paradedb` | `driver_features` | Activates vector (`pgvector`) and BM25 (`ParadeDB`) codecs and ADK hybrid search. |

See [Psycopg API Reference](/reference/adapters/psycopg/) for full `PsycopgConnectionParams`, `PsycopgPoolParams`, and `PsycopgDriverFeatures` TypedDict specifications.

## Type Mapping and Dialect Notes

- **Placeholder Compilation**: Named `:param` and positional `?` placeholders compile to Psycopg's `%s` / `%(param)s` styles automatically while escaping literal `%` characters in SQL strings.
- **Built-In `executemany` Pipelining**: Psycopg 3's `cursor.executemany` enters pipeline mode internally, so `session.execute_many(...)` batches multi-row writes over a single network pipeline without extra configuration.
- **Bounded `COPY` Streaming**: `load_from_arrow` and `COPY` operations stream data in bounded memory chunks rather than materializing entire tables into Python strings.
- **Vector Decoding in ADK Stores**: `PsycopgSyncADKMemoryStore` and `PsycopgAsyncADKMemoryStore` decode text `"[0.1,0.2]"`, UTF-8 `bytes`, `pgvector` binary wire frames, and Python float sequences uniformly via `decode_postgres_vector`.
