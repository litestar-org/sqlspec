# PostgreSQL — psqlpy

`sqlspec.adapters.psqlpy` integrates `psqlpy`, a Rust/PyO3 `asyncio` PostgreSQL driver built on Tokio, combining native Rust connection pooling, binary `COPY` ingestion, and built-in `PgVector` parameter support.

## Overview & Capabilities

Choose `PsqlpyConfig` for latency-sensitive `asyncio` workloads that benefit from Tokio's multi-threaded Rust socket pool and `pgvector` bindings without requiring an extra Python `pgvector` client dependency.

| Capability | Details |
| --- | --- |
| **Adapter Config** | `PsqlpyConfig` (`sqlspec.adapters.psqlpy`) |
| **Execution Mode** | Asynchronous (`asyncio` backed by Rust Tokio) |
| **Pool Architecture** | `psqlpy.ConnectionPool` (`max_db_pool_size`, TCP keepalive, recycling modes) |
| **Authentication** | Static credentials or dynamic IAM tokens via `auth_token_provider` in `connection_config` |
| **Extensions** | Built-in `psqlpy.extra_types.PgVector`, ParadeDB `pg_search` BM25, `pg_textsearch`, `LISTEN/NOTIFY` |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[psqlpy]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[psqlpy]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[psqlpy]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[psqlpy]"
    ```

```python
from sqlspec import SQLSpec
from sqlspec.adapters.psqlpy import PsqlpyConfig

spec = SQLSpec()
config = spec.add_config(
    PsqlpyConfig(
        connection_config={
            "dsn": "postgresql://app:secret@localhost:5432/prod",
            "max_db_pool_size": 20,
        },
        driver_features={"pooler_mode": "session", "enable_pgvector": True},
    )
)
```

Acquire sessions via `async with spec.provide_session(config) as session:` and release the Rust pool on shutdown with `await config.close_pool()`.

## Connection & Pool Configuration

### Connection and Pool Parameters

Pass `PsqlpyConnectionParams` and `PsqlpyPoolParams` keys inside `connection_config={...}` on `PsqlpyConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.psqlpy.config.PsqlpyConnectionParams,sqlspec.adapters.psqlpy.config.PsqlpyPoolParams -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `dsn` | `str` | - | PostgreSQL connection URI (`postgresql://user:pass@host:port/db`). |
| `username` | `str` | - | PostgreSQL role name used for authentication. |
| `password` | `str` | - | Password for `username`. |
| `auth_token_provider` | `Callable[[], str] or Callable[[], Awaitable[str]]` | - | Callable or async callable returning a fresh password or IAM token when creating connections. |
| `db_name` | `str` | - | Target PostgreSQL database name. |
| `host` | `str` | - | Database server hostname or IP address. |
| `port` | `int` | `5432` | Database server TCP port (defaults to `5432`). |
| `connect_timeout_sec` | `int` | - | Whole-second component of the TCP connection establishment timeout. |
| `connect_timeout_nanosec` | `int` | - | Sub-second nanosecond component of the connection establishment timeout. |
| `tcp_user_timeout_sec` | `int` | - | Whole-second TCP unacknowledged transmission timeout. |
| `tcp_user_timeout_nanosec` | `int` | - | Sub-second nanosecond component of the TCP user timeout. |
| `keepalives` | `bool` | - | Enable TCP keepalive probes on client sockets. |
| `keepalives_idle_sec` | `int` | - | Idle duration in seconds before sending the first TCP keepalive probe. |
| `keepalives_idle_nanosec` | `int` | - | Nanosecond component of the TCP keepalive idle interval. |
| `keepalives_interval_sec` | `int` | - | Interval in seconds between consecutive unacknowledged TCP keepalive probes. |
| `keepalives_interval_nanosec` | `int` | - | Nanosecond component of the TCP keepalive probe interval. |
| `keepalives_retries` | `int` | - | Maximum unacknowledged TCP keepalive probes before dropping the connection. |
| `ssl_mode` | `str` | - | TLS negotiation policy (`disable`, `prefer`, `require`, `verify-ca`, or `verify-full`). |
| `ca_file` | `str` | - | Path to a PEM root CA bundle used to verify server certificates. |
| `target_session_attrs` | `str` | - | Required server session state for multi-host routing (such as `read-write`). |
| `options` | `str` | - | Command-line startup GUC options sent to the PostgreSQL backend. |
| `application_name` | `str` | - | Application identifier reported in `pg_stat_activity`. |
| `client_encoding` | `str` | `UTF8` | Client character set encoding (defaults to `UTF8`). |
| `gssencmode` | `str` | - | GSSAPI transport encryption mode (`disable`, `prefer`, or `require`). |
| `sslnegotiation` | `str` | - | PostgreSQL 17+ TLS handshake mode (`postgres` or `direct`). |
| `sslcompression` | `str` | - | TLS compression setting forwarded to `libpq`/Rust TLS configuration. |
| `sslcert` | `str` | - | Path to the client TLS certificate file. |
| `sslkey` | `str` | - | Path to the client TLS private key file. |
| `sslpassword` | `str` | - | Passphrase unlocking an encrypted `sslkey`. |
| `sslrootcert` | `str` | - | Path to the trusted root certificate authority file. |
| `sslcrl` | `str` | - | Path to the certificate revocation list file. |
| `require_auth` | `str` | - | Comma-separated list of acceptable server authentication methods. |
| `channel_binding` | `str` | - | SCRAM channel binding requirement (`disable`, `prefer`, or `require`). |
| `krbsrvname` | `str` | - | Kerberos service principal name for GSSAPI authentication. |
| `gsslib` | `str` | - | GSSAPI library implementation selector. |
| `gssdelegation` | `str` | - | Whether to delegate GSSAPI credentials to the PostgreSQL server. |
| `service` | `str` | - | Service profile name defined in `pg_service.conf`. |
| `load_balance_hosts` | `str` | - | Host ordering policy across multi-host configurations (`disable` or `random`). |
| `hosts` | `list[str]` | - | Ordered list of PostgreSQL hostnames for multi-host failover or load balancing. |
| `ports` | `list[int]` | - | Port numbers corresponding to `hosts`. |
| `conn_recycling_method` | `str` | - | Pool connection reset strategy (such as `fast` or `verified`). |
| `max_db_pool_size` | `int` | - | Maximum number of concurrent connections held by `psqlpy.ConnectionPool`. |
| `configure` | `Callable[..., Any]` | - | Optional pool connection configuration callback. |
| `extra` | `dict[str, Any]` | - | Additional keyword options passed directly to `psqlpy.ConnectionPool`. |
<!-- GENERATED:END config-table:sqlspec.adapters.psqlpy.config.PsqlpyConnectionParams,sqlspec.adapters.psqlpy.config.PsqlpyPoolParams -->

### Driver Features

Pass `PsqlpyDriverFeatures` keys inside `driver_features={...}` on `PsqlpyConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.psqlpy.config.PsqlpyDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `tenant_strategy` | `AsyncTenantStrategy or TenantStrategyShorthand or None` | - | Opt-in tenant strategy, or the `"rls"` shorthand, applied to every session and connection lease. |
| `enable_cast_detection` | `bool` | `True` | Use SQL type casts in statements to coerce JSON, UUID, and temporal bind values (defaults to `True`). |
| `enable_pgvector` | `bool` | - | Detect the PostgreSQL `vector` extension and promote the dialect to `pgvector`. |
| `enable_paradedb` | `bool` | `True` | Detect `pg_search` and promote the dialect to `paradedb` (defaults to `True`). |
| `enable_pg_textsearch` | `bool` | `True` | Detect `pg_textsearch` and promote the dialect for BM25 `<@>` ranking (defaults to `True`). |
| `enable_alloydb_columnar` | `bool` | - | Enable `google_columnar_engine.enable_columnar_scan=on` at session startup or per transaction. |
| `pooler_mode` | `Literal['session', 'transaction', 'statement', 'auto'] or None` | - | External pooler compatibility mode; executes queries with `prepared=False` when not `session`. |
| `direct_dsn` | `str or Mapping[str, Any] or None` | - | Direct PostgreSQL DSN or connection mapping bypassing the pooler for migrations and `LISTEN/NOTIFY`. |
| `json_serializer` | `Callable[[Any], str]` | `to_json` | Custom JSON serializer applied to statement parameter conversion (defaults to `to_json`). |
| `json_deserializer` | `Callable[[str], Any]` | `from_json` | Custom JSON deserializer retained alongside `json_serializer` (defaults to `from_json`). |
| `on_connection_create` | `Callable[[PsqlpyConnection], Awaitable[None]]` | - | Async callback invoked once per physical `psqlpy` connection acquired from the pool. |
| `enable_events` | `bool` | `True` | Enable SQLSpec event channel pub/sub support (defaults to `True` when `events` is configured). |
| `enable_pgcopy_binary` | `bool` | - | Enable binary wire format for PostgreSQL COPY operations. |
| `events_backend` | `Literal['notify', 'notify_queue', 'poll_queue']` | - | Event transport backend (`notify` for LISTEN/NOTIFY, `notify_queue`, or `poll_queue`). |
<!-- GENERATED:END config-table:sqlspec.adapters.psqlpy.config.PsqlpyDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `JSON` / `JSONB` | `dict` / `list` | Serialized and deserialized via `json_serializer` and `json_deserializer`. |
| `UUID` | `uuid.UUID` / `str` | Native Rust PyO3 UUID conversion. |
| `VECTOR` (`pgvector`) | `list[float]` / `psqlpy.extra_types.PgVector` | Coerced automatically when `enable_pgvector=True` or via `::vector` cast detection (`enable_cast_detection=True`). |
| `ARRAY` | `list[T]` | Native PostgreSQL array binding. |
| `TSRANGE` / `TSTZRANGE` / `DATERANGE` | `tuple` / `str` | PostgreSQL range literals and composite values. |
| `BYTEA` | `bytes` / `list[int]` | Coerced transparently by SQLSpec's parameter pipeline for Rust PyO3 `BYTEA` binding. |
| `NUMERIC` / `DECIMAL` | `decimal.Decimal` | Exact arbitrary-precision decimal values. |

- **Pooler Mode and Direct DSN**: Setting `pooler_mode` to `"transaction"` or `"statement"` passes `prepared=False` automatically so statements execute safely through PgBouncer or Supavisor, while `direct_dsn` provides an unpooled connection target for `config.derive_direct_config()` migrations and `LISTEN/NOTIFY` listeners.
- **StatementStack Execution**: `psqlpy` has no native pipeline override, so `session.execute_stack(stack)` uses the shared sequential execution path and runs each step in order.
- **Bulk COPY and Arrow Ingest**: `session.load_from_arrow(...)` streams Arrow batches into PostgreSQL via `binary_copy_to_table`, avoiding intermediate row-by-row INSERT overhead.
- **Rust Extension Object Semantics**: `psqlpy.Connection` is a slotted Rust PyO3 type without a Python `__dict__`; avoid attaching ad-hoc attributes to `session.connection` inside custom `on_connection_create` hooks.

## See Also

- [PostgreSQL and AlloyDB Overview](index.md)
- [Psqlpy API Reference](/reference/adapters/psqlpy/)
- [PostgreSQL Dialect Reference](/reference/data-dictionary/dialects/postgres/)
