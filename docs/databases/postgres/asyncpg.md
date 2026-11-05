# PostgreSQL — asyncpg

`sqlspec.adapters.asyncpg` wraps MagicStack's compiled `asyncpg` driver, delivering low-latency binary-protocol execution, managed `asyncio` connection pooling, Cloud SQL and AlloyDB IAM connectors, `LISTEN/NOTIFY` events, and `pgvector` / ParadeDB ADK stores.

## Overview & Capabilities

Choose `AsyncpgConfig` for pure-`asyncio` APIs and background workers that prioritize binary-protocol throughput, per-statement query timeouts, and automatic custom type codecs. If the same codebase also requires synchronous DB-API connections, see [`psycopg`](psycopg.md).

| Capability | Details |
| --- | --- |
| **Adapter Config** | `AsyncpgConfig` (`sqlspec.adapters.asyncpg`) |
| **Execution Mode** | Asynchronous (`asyncio`) |
| **Pool Architecture** | `asyncpg.Pool` (`min_size`, `max_size`, `max_queries`, `max_inactive_connection_lifetime`) |
| **Cloud Connectors** | Google Cloud SQL (`enable_cloud_sql`), AlloyDB (`enable_alloydb`), IAM (`auth_token_provider`) |
| **Extensions** | `pgvector`, ParadeDB `pg_search` BM25, `pg_textsearch`, `LISTEN/NOTIFY` event channels |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[asyncpg]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[asyncpg]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[asyncpg]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[asyncpg]"
    ```

```{ .python title="docs/examples/drivers/asyncpg_connection.py" }
--8<-- "docs/examples/drivers/asyncpg_connection.py:example"
```

Open request or task sessions with `async with spec.provide_session(config) as session:` and close the pool on shutdown via `await config.close_pool()`.

## Connection & Pool Configuration

### Connection and Pool Parameters

Pass `AsyncpgConnectionConfig` and `AsyncpgPoolConfig` keys inside `connection_config={...}` on `AsyncpgConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.asyncpg.config.AsyncpgConnectionConfig,sqlspec.adapters.asyncpg.config.AsyncpgPoolConfig -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `dsn` | `str` | - | PostgreSQL connection URI (`postgresql://user:pass@host:port/dbname`). |
| `host` | `str` | - | Database server hostname or Unix domain socket directory. |
| `port` | `int` | `5432` | Database server TCP port (defaults to `5432`). |
| `user` | `str` | - | PostgreSQL role name used for authentication. |
| `password` | `str` | - | Password or dynamic token for `user`. |
| `auth_token_provider` | `Callable[[], str] or Callable[[], Awaitable[str]]` | - | Callable or async callable returning a fresh password or IAM token for each new connection. |
| `database` | `str` | - | Target PostgreSQL database name. |
| `ssl` | `Any` | - | TLS mode string, boolean flag, or `ssl.SSLContext` instance. |
| `passfile` | `str` | - | Path to a `pgpass` password file. |
| `service` | `str` | - | Service section name in `pg_service.conf`. |
| `servicefile` | `str` | - | Path to an alternate `pg_service.conf` file. |
| `direct_tls` | `bool` | - | Initiate TLS negotiation directly without PostgreSQL `SSLRequest` startup. |
| `timeout` | `float` | - | Connection establishment timeout in seconds (alias supported by `asyncpg.connect`). |
| `connect_timeout` | `float` | - | Pool connection establishment timeout in seconds. |
| `command_timeout` | `float` | - | Default per-command execution timeout in seconds. |
| `statement_cache_size` | `int` | - | Maximum number of server-side prepared statements cached per connection (`0` disables caching). |
| `max_cached_statement_lifetime` | `int` | - | Maximum age in seconds before cached prepared statements are evicted. |
| `max_cacheable_statement_size` | `int` | - | Maximum SQL byte length eligible for prepared statement caching. |
| `server_settings` | `dict[str, str]` | - | Startup PostgreSQL run-time GUC settings (such as `application_name` or `search_path`). |
| `target_session_attrs` | `AsyncpgTargetSessionAttrs` | - | Required server session role when connecting to multi-host targets (such as `read-write`). |
| `krbsrvname` | `str` | - | Kerberos service principal name used for GSSAPI authentication. |
| `gsslib` | `AsyncpgGSSLib` | - | GSSAPI library backend (`gssapi` or `sspi` on Windows). |
| `min_size` | `int` | - | Minimum number of connections maintained in the pool. |
| `max_size` | `int` | - | Maximum number of concurrent connections allowed in the pool. |
| `max_queries` | `int` | - | Number of executed queries after which a pooled connection is closed and replaced. |
| `max_inactive_connection_lifetime` | `float` | - | Idle duration in seconds before unused connections above `min_size` are closed. |
| `connect` | `Callable[..., Awaitable[AsyncpgConnection]]` | - | Custom async callable invoked to establish raw `asyncpg` connections. |
| `setup` | `Callable[[AsyncpgConnection], Awaitable[None]]` | - | Async callback invoked before each connection acquisition from the pool. |
| `init` | `Callable[[AsyncpgConnection], Awaitable[None]]` | - | Async callback invoked once when a new physical connection is created. |
| `reset` | `Callable[[AsyncpgConnection], Awaitable[None]]` | - | Async callback invoked when a connection is released back to the pool. |
| `loop` | `AbstractEventLoop` | - | Explicit `asyncio` event loop passed to `asyncpg.create_pool`. |
| `connection_class` | `type[AsyncpgConnection]` | - | Custom `asyncpg.Connection` subclass instantiated by the pool. |
| `record_class` | `type[Record]` | - | Custom `asyncpg.Record` subclass returned by raw queries. |
| `extra` | `dict[str, Any]` | - | Additional keyword arguments forwarded directly to `asyncpg.create_pool`. |
<!-- GENERATED:END config-table:sqlspec.adapters.asyncpg.config.AsyncpgConnectionConfig,sqlspec.adapters.asyncpg.config.AsyncpgPoolConfig -->

### Driver Features

Pass `AsyncpgDriverFeatures` keys inside `driver_features={...}` on `AsyncpgConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.asyncpg.config.AsyncpgDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `json_serializer` | `Callable[[Any], str]` | `to_json` | Custom JSON serializer registered on `json`/`jsonb` codecs (defaults to `to_json`). |
| `json_deserializer` | `Callable[[str], Any]` | `from_json` | Custom JSON deserializer registered on `json`/`jsonb` codecs (defaults to `from_json`). |
| `enable_json_codecs` | `bool` | `True` | Automatically register `json` and `jsonb` codecs on new connections (defaults to `True`). |
| `enable_pgvector` | `bool` | - | Register `pgvector` binary codecs and promote the dialect when `pgvector` is installed. |
| `enable_paradedb` | `bool` | `True` | Detect `pg_search` and promote the SQL dialect to `paradedb` (defaults to `True`). |
| `enable_pg_textsearch` | `bool` | `True` | Detect `pg_textsearch` and promote the SQL dialect for BM25 `<@>` ranking (defaults to `True`). |
| `enable_cloud_sql` | `bool` | - | Route connections through `cloud-sql-python-connector` (mutually exclusive with `enable_alloydb`). |
| `cloud_sql_instance` | `str` | - | Cloud SQL instance connection string in `project:region:instance` format. |
| `cloud_sql_enable_iam_auth` | `bool` | - | Use IAM database authentication with the Cloud SQL Python Connector. |
| `cloud_sql_ip_type` | `str` | `PRIVATE` | Cloud SQL network path (`PUBLIC`, `PRIVATE`, or `PSC`; defaults to `PRIVATE`). |
| `enable_alloydb` | `bool` | - | Route connections through `google-cloud-alloydb-connector` (mutually exclusive with `enable_cloud_sql`). |
| `alloydb_instance_uri` | `str` | - | AlloyDB instance resource URI (`projects/.../locations/.../clusters/.../instances/...`). |
| `enable_alloydb_iam_auth` | `bool` | - | Use IAM database authentication with the AlloyDB Python Connector. |
| `alloydb_ip_type` | `str` | `PRIVATE` | AlloyDB network path (`PUBLIC`, `PRIVATE`, or `PSC`; defaults to `PRIVATE`). |
| `enable_alloydb_columnar` | `bool` | - | Enable `google_columnar_engine.enable_columnar_scan=on` at session startup or per transaction. |
| `enable_pgcopy_binary` | `bool` | - | Enable binary COPY format for Arrow table ingestion. |
| `enable_events` | `bool` | `True` | Enable SQLSpec event channel pub/sub support (defaults to `True` when `events` is configured). |
| `events_backend` | `Literal['notify', 'notify_queue', 'poll_queue']` | - | Event transport backend (`notify` for LISTEN/NOTIFY, `notify_queue`, or `poll_queue`). |
| `on_connection_create` | `Callable[[AsyncpgConnection], Awaitable[None]]` | - | Async hook invoked with each raw `asyncpg` connection after internal codec setup. |
| `pooler_mode` | `Literal['session', 'transaction', 'statement', 'auto'] or None` | - | External pooler compatibility mode; sets `statement_cache_size=0` in `transaction` or `statement` mode. |
| `direct_dsn` | `str or Mapping[str, Any] or None` | - | Direct PostgreSQL DSN or connection mapping bypassing the pooler for migrations and `LISTEN/NOTIFY`. |
| `type_codecs` | `list[dict[str, Any]]` | - | Custom `asyncpg` type codec specifications registered on connection initialization. |
<!-- GENERATED:END config-table:sqlspec.adapters.asyncpg.config.AsyncpgDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `JSON` / `JSONB` | `dict` / `list` | Registered via binary codecs when `enable_json_codecs=True` (default) using `json_serializer` and `json_deserializer`. |
| `UUID` | `uuid.UUID` / `str` | Native binary UUID codec; coerces string UUID parameters automatically. |
| `VECTOR` (`pgvector`) | `list[float]` / `numpy.ndarray` | Registered when `enable_pgvector=True` and `pgvector` is installed. |
| `ARRAY` | `list[T]` | Native binary array encoding and decoding across nested dimensions. |
| `TSRANGE` / `TSTZRANGE` / `DATERANGE` | `asyncpg.Range` | Native PostgreSQL range types with inclusive/exclusive bound inspection. |
| `BYTEA` | `bytes` / `bytearray` | Binary wire transport without hex-encoding overhead. |
| `NUMERIC` / `DECIMAL` | `decimal.Decimal` | Exact arbitrary-precision decimal representation. |

- **Pooler Mode and Direct DSN**: Set `driver_features={"pooler_mode": "transaction", "direct_dsn": "..."}` when running behind PgBouncer or Supavisor in transaction or statement mode; SQLSpec disables `statement_cache_size` automatically and uses `config.derive_direct_config()` for DDL migrations and `LISTEN/NOTIFY`.
- **Untyped Parameters Inside Polymorphic Functions**: Because `asyncpg` prepares statements over PostgreSQL's binary protocol, untyped parameters inside functions like `jsonb_build_object(:k, :v)` require an explicit SQL cast such as `CAST(:k AS text)` so PostgreSQL can resolve the parameter OID.
- **Binding JSON String Primitives**: When `enable_json_codecs=True`, Python `dict`, `list`, `int`, and `bool` values serialize via `json_serializer`, while bare `str` values are treated as pre-serialized JSON text. Pass `to_json("value")` when inserting a scalar JSON string literal into a `jsonb` column.
- **Binary COPY and Arrow Ingest**: `session.load_from_arrow(...)` streams Arrow batches into PostgreSQL via binary `COPY ... FROM STDIN` for high-throughput bulk loads, and `LISTEN/NOTIFY` channels integrate directly with `EventChannel`.
- **Sequential StatementStack Execution**: `asyncpg`'s binary protocol forbids overlapping operations on a single connection, so `session.execute_stack(stack)` runs each step in order against native prepared statements instead of pipelining them.

## See Also

- [PostgreSQL and AlloyDB Overview](index.md)
- [AsyncPG API Reference](/reference/adapters/asyncpg/)
- [PostgreSQL Dialect Reference](/reference/data-dictionary/dialects/postgres/)
