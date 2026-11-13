# Teradata Vantage

Use `TeradataConfig` to connect synchronously to Teradata Vantage through `teradatasql`. The adapter uses `?` parameter markers, a bounded connection pool, and SQLSpec's Teradata SQL dialect. SQLSpec registers that dialect with sqlglot as `vantage` and maps `teradata` to it in its own builders and statement configs, so sqlglot's built-in `teradata` dialect stays unchanged for other libraries.

## Install and connect

Install the driver extra separately. Review the Teradata driver license before installing it; the default development extras do not include this driver.

```bash
pip install 'sqlspec[teradata]'
```

Keep credentials in your environment or secret manager:

```python
--8<-- "docs/examples/teradata/quickstart.py:example"
```

Construction does not connect. After setting your environment credentials, open a session and close the pool when finished:

```python
try:
    with config.provide_session() as session:
        rows = session.select(
            "SELECT order_id FROM orders WHERE customer_id = ?", (42,)
        )
finally:
    config.close_pool()
```

`teradatasql` is loaded when a connection is opened. Importing the adapter and constructing a configuration do not require an installed driver or open a network connection. `build_connection_config` defaults the character set to UTF-8 and sets `encryptdata` to `"true"` unless overridden in `connection_config`. Pass driver authentication and TLS options in `connection_config` using the names accepted by `teradatasql`, such as `encryptdata`, `logmech`, `logdata`, and `sslmode`.

## Transactions and pooling

Teradata supports `TERA` (default) and `ANSI` session transaction modes; select `tmode` in `connection_config` before opening connections. Use explicit `begin()`, `commit()`, and `rollback()` for multi-statement units of work. Transaction-abort behavior differs by session mode: in default `TERA` mode, server-side uniqueness or constraint errors abort and roll back the entire active `autocommit=False` transaction, whereas `ANSI` mode rolls back only the failing statement. The driver commits DDL and session query-band requests before subsequent statements to avoid Teradata error 3932; these operations also commit an enclosing unit of work, so keep them outside application transactions. Savepoints are unavailable.

A session owns its checked-out connection until the context exits. Size pools conservatively against your database's TASM session limits, and close the pool during application shutdown. Connections must not cross process boundaries.

## SQL, Arrow, and catalogs

The dialect renders a limit as `TOP` and offset pagination with `QUALIFY ROW_NUMBER()`. Supply a stable `ORDER BY` for deterministic paging. `BOOLEAN` values use `BYTEINT`; large text and binary values use `CLOB` and `BLOB`. Merge sources cast parameters to concrete Teradata types.

`select_to_arrow()` converts bounded DB-API chunks to Arrow; `native_only=True` is refused because this is a conversion path. `select_stream()` yields row dictionaries or mapped objects. FastExport and FastLoad are opt-in driver capabilities; check server permissions and restrictions before enabling them. Storage upserts use a staging table and separate update and insert requests.

### Bulk loads and transactions

Teradata requires a DDL statement to be the last statement of a transaction (error 3932) in both TERA and ANSI transaction modes. Staged loads (`stage_table` or `conflict_keys`), `drop_indexes=True`, and `checkpoint_table` all run DDL, so they need a session with no open transaction. When the session is inside `begin()` or `transaction()`, or runs in manual mode (`autocommit=False`), these operations raise `TransactionError` before they change anything, rather than committing work the caller still owns. Commit or roll back first, or run the load from an autocommit session. Plain `load_from_arrow()` and `load_from_storage()` inserts without these options run inside the caller's transaction as usual.

The data dictionary queries modern Unicode `DBC.*V` catalog views with `LOCKING ROW FOR ACCESS`. Catalog access depends on the connected user's privileges. See the [catalog reference](../reference/data-dictionary/dialects/teradata.md) and [adapter API](../reference/adapters/teradata.md).

## Extensions and ADBC

The adapter supplies Litestar session, event queue, and Google ADK session and memory stores. Configure extensions through `extension_config`; their schema reconcilers create missing tables and validate existing schemas.

For native Arrow access through Columnar `dbc` or Flight SQL, see [ADBC Teradata connections](adbc.md#teradata). Native `teradatasql` and ADBC use separate drivers and installation paths.

## Read Workload Pools and Fallback Nodes

`TeradataConfig(replicas=...)` selects a COP/reader host and a separate workload account. Use server roles and TASM rules to bound reads. Another pool does not by itself stop writes or copy a dataset. Native read-only failures map to `ReadOnlyTransactionError`.

Derived readers keep native aliases, hooks, query bands, and pool bounds. Each session keeps its own lease and the same cleanup rules. See [Read Replicas](../learn/recipes/read-replicas.md) for named groups and [Replica Operations](../learn/recipes/read-replicas-operations.md) for failure policy.

## Configuration Reference

### Connection and Pool Parameters

<!-- GENERATED:START config-table:sqlspec.adapters.teradata.config.TeradataConnectionParams,sqlspec.adapters.teradata.config.TeradataPoolParams -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `host` | `str` | - | Teradata server hostname. |
| `user` | `str` | - | Native authentication user. |
| `username` | `str` | - | Friendly authentication user alias. |
| `password` | `str` | - | Authentication password. |
| `database` | `str` | - | Default database. |
| `dbs_port` | `str` | - | Native server port. |
| `port` | `int or str` | - | Friendly server port alias. |
| `autocommit` | `bool` | - | Initial transaction autocommit state. |
| `logmech` | `TeradataLogMech` | - | Authentication mechanism. |
| `tmode` | `TeradataTransactionMode` | - | Transaction mode. |
| `sslmode` | `TeradataSSLMode` | - | TLS verification mode. |
| `encryptdata` | `bool or str` | - | Enable encrypted data transport. |
| `account` | `str` | - | Teradata workload management account string. |
| `logdata` | `str` | - | Mechanism-specific authentication payload. |
| `charset` | `str` | - | Session character set. |
| `sslca` | `str` | - | Path to CA certificate bundle file. |
| `sslcapath` | `str` | - | Path to CA certificate directory. |
| `lob_support` | `bool or str` | - | Enable LOB streaming support. |
| `connect_timeout` | `int or float or str` | - | Connection establishment timeout. |
| `request_timeout` | `int or float or str` | - | Statement request timeout. |
| `sJson` | `str` | - | Raw teradatasql JSON connection payload. |
| `cop` | `bool or str` | - | Enable COP discovery. |
| `coplast` | `bool or str` | - | Retry last COP entry. |
| `https_port` | `int or str` | - | HTTPS port for TLS/OAuth connections. |
| `extra` | `dict[str, Any]` | - | Additional vendor options. |
| `pool_min_size` | `int` | - | Minimum number of connections maintained in the pool. |
| `pool_max_size` | `int` | - | Maximum number of connections allowed in the pool. |
| `pool_timeout` | `float` | - | Seconds to wait for a connection before timing out. |
| `pool_recycle_seconds` | `int` | - | Maximum age in seconds before a connection is recycled. |
| `health_check_interval` | `float` | - | Interval in seconds between background health checks. |
<!-- GENERATED:END config-table:sqlspec.adapters.teradata.config.TeradataConnectionParams,sqlspec.adapters.teradata.config.TeradataPoolParams -->

### Driver Features

<!-- GENERATED:START config-table:sqlspec.adapters.teradata.config.TeradataDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `json_serializer` | `Callable[[Any], str]` | - | Custom callable for serializing objects to JSON strings. |
| `json_deserializer` | `Callable[[str], Any]` | - | Custom callable for deserializing JSON strings. |
| `on_connection_create` | `Callable[[TeradataConnection], None]` | - | Callback invoked immediately when a new connection is established. |
| `arraysize` | `int` | - | DB-API row fetch chunk size. |
| `enable_lowercase_column_names` | `bool` | - | Fold implicit uppercase column names. |
| `enable_fastexport` | `bool` | - | Attempt native FastExport for reads. |
| `enable_fastload` | `bool` | - | Attempt native FastLoad for inserts. |
| `query_band` | `Mapping[str, str] or str` | - | Session query-band metadata. |
| `enable_events` | `bool` | - | Enable database event listening. |
| `events_backend` | `Literal['poll_queue']` | - | Backend mechanism used for dispatching events. |
<!-- GENERATED:END config-table:sqlspec.adapters.teradata.config.TeradataDriverFeatures -->

## Live verification

Set `TERADATA_HOST`, `TERADATA_USER`, and `TERADATA_PASSWORD`, then run:

```bash
UV_NO_SYNC=1 uv run --no-sync pytest tests/integration/adapters/teradata -q
```

Optional variables include `TERADATA_PORT`, `TERADATA_DATABASE`, `TERADATA_LOGMECH`, `TERADATA_LOGDATA`, `TERADATA_SSLMODE`, and `TERADATA_ENCRYPTDATA`. The live suite skips when no host is configured.

For a licensed, runnable local database image, the test suite can start a custom service through `pytest-databases`:

```bash
TERADATA_TEST_IMAGE=your-licensed-teradata-image \
TERADATA_USER=dbc TERADATA_PASSWORD=your-test-password \
UV_NO_SYNC=1 uv run --no-sync pytest tests/integration/adapters/teradata -q
```

The service publishes container port `1025` on a dynamic host port and waits for a successful driver probe. Set `TERADATA_STARTUP_TIMEOUT` to adjust the default 300-second startup allowance. Override the session-scoped `teradata_service_environment` fixture for image-specific initialization settings. An explicit `TERADATA_HOST` takes precedence over the local image. Supply your own licensed image; [Teradata Vantage Express](https://downloads.teradata.com/download/database/teradata-express/vmware) is distributed as a virtual machine, so this fixture does not assume a public Docker image exists.
