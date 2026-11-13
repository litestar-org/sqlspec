# Oracle Database

`sqlspec.adapters.oracledb` provides synchronous and async Oracle sessions through `python-oracledb`. Use Thin mode for Oracle Database 12c through 26ai without client libraries, or Thick mode for deployments that need Oracle Instant Client. Both configurations support pools, wallet authentication, session context, JSON, vectors, and bulk loading. The native features below need `python-oracledb` 26.1 and server support.

## Overview & Capabilities

`python-oracledb` runs in pure-Python **Thin mode** by default with zero Oracle Instant Client dependencies. Enable **Thick mode** via `connection_config={"thick_mode": True, "lib_dir": "..."}` only when connecting to legacy encryption suites, Kerberos/Radius OS authentication, or Continuous Query Notification features requiring Oracle Client libraries.

| Adapter Config | Driver Extra | Execution | Pool Architecture | Best For |
| --- | --- | --- | --- | --- |
| `OracleAsyncConfig` | `sqlspec[oracledb]` | Async (`asyncio`) | `oracledb.AsyncConnectionPool` (`min`, `max`, `increment`, DRCP `cclass` / `purity`) | High-concurrency `asyncio` HTTP APIs, ADK memory stores, and zero-copy Arrow streaming in Thin mode. |
| `OracleSyncConfig` | `sqlspec[oracledb]` | Sync | `oracledb.ConnectionPool` (`min`, `max`, `increment`, DRCP `cclass` / `purity`) | Synchronous services, CLI migrations, Direct Path Loads, and Thick-mode client deployments. |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[oracledb]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[oracledb]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[oracledb]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[oracledb]"
    ```

=== "Async (OracleAsyncConfig)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.oracledb import (
        OracleAsyncConfig,
        OracleSessionContextConfig,
    )

    spec = SQLSpec()
    config = spec.add_config(
        OracleAsyncConfig(
            connection_config={
                "user": "app",
                "password": "secret",
                "dsn": "localhost:1521/FREEPDB1",
                "min": 2,
                "max": 20,
            },
            driver_features={
                "enable_lowercase_column_names": True,
                "session_context_hook": OracleSessionContextConfig(
                    module="orders-api",
                    action="http-request",
                ),
            },
        )
    )
    ```

=== "Sync (OracleSyncConfig)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.oracledb import OracleSyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        OracleSyncConfig(
            connection_config={
                "user": "app",
                "password": "secret",
                "dsn": "localhost:1521/FREEPDB1",
                "min": 2,
                "max": 10,
                "thick_mode": False,
            }
        )
    )
    ```

### Autonomous Database Wallets (wallet_zip) and RAC Topologies

Pass an Autonomous Database wallet `.zip` archive (`str`, `Path`, or raw `bytes` payload) directly via `wallet_zip` inside `connection_config`, or declare multi-endpoint Oracle RAC and Data Guard failover descriptors with `OracleTopologyConfig` and `OracleEndpoint`:

```python
from pathlib import Path
from sqlspec.adapters.oracledb import (
    OracleAsyncConfig,
    OracleEndpoint,
    OracleTopologyConfig,
)

adb_config = OracleAsyncConfig(
    connection_config={
        "user": "admin",
        "password": "secret",
        "tns_alias": "mydb_high",
        "wallet_zip": Path("/etc/oracle/Wallet_mydb.zip"),
        "wallet_password": "wallet_secret",
    }
)

rac_config = OracleAsyncConfig(
    connection_config={
        "user": "app",
        "password": "secret",
        "topology": OracleTopologyConfig(
            service_name="prod_svc.example.internal",
            endpoints=(
                OracleEndpoint(host="rac-scan-primary.internal", port=1521),
                OracleEndpoint(host="rac-scan-standby.internal", port=1521),
            ),
            retry_count=3,
            retry_delay=2,
        ),
    }
)
```

## Connection & Pool Configuration

### Connection and Pool Parameters

Pass `OracleConnectionParams` and `OraclePoolParams` keys (including `thick_mode`, `lib_dir`, `wallet_zip`, and `topology`) inside `connection_config={...}` on `OracleSyncConfig` or `OracleAsyncConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.oracledb.config.OracleConnectionParams,sqlspec.adapters.oracledb.config.OraclePoolParams -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `dsn` | `str` | - | Easy Connect string, TNS alias, or full connect descriptor. |
| `pool_alias` | `str` | - | Named Thick-mode pool alias registered in the driver cache. |
| `user` | `str` | - | Database username for authentication. |
| `proxy_user` | `str` | - | Proxy target username appended inside `[proxy_user]` credentials. |
| `password` | `str` | - | Database password for the authenticating user. |
| `newpassword` | `str` | - | Replacement password applied atomically during connection establishment. |
| `wallet_password` | `str` | - | Passphrase decrypting an encrypted PEM `ewallet.pem` or PKCS#12 `ewallet.p12` wallet. |
| `access_token` | `OracleAccessToken` | - | OAuth 2.0 or IAM bearer token string, `(token, private_key)` tuple, or refresh callable. |
| `host` | `str` | - | Oracle listener hostname or IP address when building a connect descriptor. |
| `port` | `int` | `1521` | Oracle listener TCP/TCPS port (defaults to `1521`). |
| `protocol` | `OracleProtocol` | - | Transport protocol (`'tcp'` or `'tcps'` for TLS/mTLS). |
| `https_proxy` | `str` | - | HTTPS proxy hostname used when tunneling TCPS connections. |
| `https_proxy_port` | `int` | - | HTTPS proxy TCP port used with `https_proxy`. |
| `service_name` | `str` | - | Oracle database service name registered with the listener. |
| `instance_name` | `str` | - | Specific RAC instance name targeted within the service. |
| `sid` | `str` | - | Legacy Oracle System Identifier used when `service_name` is unavailable. |
| `server_type` | `OracleServerType` | - | Server handler mode (`'dedicated'`, `'shared'`, or `'pooled'` for DRCP). |
| `cclass` | `str` | - | Database Resident Connection Pooling (DRCP) connection class name. |
| `purity` | `Purity` | - | DRCP session purity (`PURITY_NEW` or `PURITY_SELF`). |
| `expire_time` | `int` | - | Dead-connection detection probe interval in minutes. |
| `externalauth` | `bool` | - | Authenticate via OS credentials or an Oracle external password store wallet. |
| `mode` | `AuthMode` | - | Administrative privilege mode such as `AUTH_MODE_SYSDBA` or `AUTH_MODE_SYSOPER`. |
| `wallet_location` | `str` | - | Directory path containing `ewallet.pem` or `ewallet.p12` for mTLS. |
| `config_dir` | `str` | - | Directory path containing `tnsnames.ora` and `sqlnet.ora` configuration files. |
| `tcp_connect_timeout` | `float` | - | Maximum seconds to wait when establishing the initial TCP socket. |
| `retry_count` | `int` | - | Number of connection-level retry attempts across listener endpoints. |
| `retry_delay` | `int` | - | Delay in seconds between connection retry attempts. |
| `ssl_server_dn_match` | `bool` | - | Verify that the server certificate distinguished name matches the target service. |
| `ssl_server_cert_dn` | `str` | - | Expected distinguished name on the Oracle server TLS certificate. |
| `events` | `bool` | - | Enable Thick-mode Continuous Query Notification and FAN HA event delivery. |
| `disable_oob` | `bool` | - | Disable TCP out-of-band break messages on networks that drop urgent packets. |
| `stmtcachesize` | `int` | - | Per-connection cursor statement cache size. |
| `edition` | `str` | - | Edition-Based Redefinition (EBR) edition name activated on connect. |
| `tag` | `str` | - | Pool session state tag requested when acquiring a connection. |
| `matchanytag` | `bool` | - | Allow acquiring a pooled connection with a different tag when an exact match is unavailable. |
| `appcontext` | `Sequence[OracleAppContext] or Mapping[str, Mapping[str, str]]` | - | Initial `(namespace, name, value)` application context entries set on connect. |
| `sharding_key` | `Sequence[Any]` | - | Primary shard key forwarded as native `shardingkey`. |
| `super_sharding_key` | `Sequence[Any]` | - | Super-shard key forwarded as native `supershardingkey`. |
| `transaction_priority` | `OracleTransactionPriority` | - | Priority applied during connection checkout on supported drivers. |
| `shardingkey` | `list[Any]` | - | Primary sharding key values routing the connection to an Oracle shard. |
| `supershardingkey` | `list[Any]` | - | Composite super-sharding key values for multi-tier sharded topologies. |
| `debug_jdwp` | `str` | - | JDWP `host:port` endpoint attached for PL/SQL and Java stored-procedure debugging. |
| `connection_id_prefix` | `str` | - | Prefix prepended to driver-generated end-to-end diagnostic connection identifiers. |
| `ssl_context` | `Any` | - | Custom `ssl.SSLContext` overriding default Thin-mode TLS verification. |
| `sdu` | `int` | - | Oracle Net Session Data Unit buffer size in bytes. |
| `pool_boundary` | `OraclePoolBoundary` | - | Implicit DRCP/PRCP release boundary (`'statement'` or `'transaction'`). |
| `use_tcp_fast_open` | `bool` | - | Enable TCP Fast Open (TFO) for Oracle Autonomous Database connections. |
| `ssl_version` | `TLSVersion` | - | Minimum or pinned `ssl.TLSVersion` negotiated for TCPS connections. |
| `program` | `str` | - | Client program name reported in `V$SESSION.PROGRAM`. |
| `machine` | `str` | - | Client host name reported in `V$SESSION.MACHINE`. |
| `terminal` | `str` | - | Client terminal identifier reported in `V$SESSION.TERMINAL`. |
| `osuser` | `str` | - | Operating system username reported in `V$SESSION.OSUSER`. |
| `driver_name` | `str` | - | Client driver label reported in `V$SESSION_CONNECT_INFO.CLIENT_DRIVER`. |
| `use_sni` | `bool` | - | Send TLS Server Name Indication (SNI) to bypass double-handshake listener negotiation. |
| `thick_mode_dsn_passthrough` | `bool` | - | Forward the raw DSN string unchanged to Oracle Client libraries in Thick mode. |
| `extra_auth_params` | `dict[str, Any]` | - | Additional provider parameters passed to cloud token authentication plugins. |
| `pool_name` | `str` | - | Explicit server-side DRCP/PRCP pool name when multiple pools exist. |
| `on_connect_callback` | `Callable[..., Any]` | - | Driver-level callback invoked by `python-oracledb` immediately on physical connect. |
| `handle` | `int` | - | Raw OCI service context pointer wrapped in Thick mode. |
| `extra` | `dict[str, Any]` | - | Additional keyword arguments merged into `oracledb.connect` or `create_pool`. |
| `topology` | `OracleTopologyConfig or dict[str, Any]` | - | Multi-endpoint RAC/Data Guard topology descriptor compiled into a TNS `DESCRIPTION`. |
| `wallet_zip` | `str or Path or bytes` | - | Autonomous Database wallet ZIP path, directory, or raw bytes extracted into a secure temporary directory. |
| `tns_alias` | `str` | - | Alias resolved from `tnsnames.ora` inside `config_dir` or an extracted `wallet_zip`. |
| `thick_mode` | `bool` | - | Initialize Oracle Client Thick mode via `oracledb.init_oracle_client()` before creating the pool. |
| `lib_dir` | `str` | - | Filesystem path to Oracle Instant Client libraries implying Thick mode. |
| `pool_class` | `type[Any]` | - | Custom `ConnectionPool` subclass passed to `python-oracledb`. |
| `params` | `oracledb.PoolParams` | - | Pre-constructed `oracledb.PoolParams` instance merged with keyword settings. |
| `min` | `int` | - | Minimum number of sessions maintained in the pool. |
| `max` | `int` | - | Maximum number of concurrent sessions permitted in the pool. |
| `increment` | `int` | - | Number of new sessions opened at once when all pooled sessions are busy. |
| `connectiontype` | `type[Any]` | - | Custom `oracledb.Connection` subclass instantiated for pooled sessions. |
| `getmode` | `PoolGetMode` | - | Behavior when the pool is at capacity (`POOL_GETMODE_WAIT`, `NOWAIT`, `FORCEGET`, or `TIMEDWAIT`). |
| `homogeneous` | `bool` | `True` | Use identical database credentials across all pooled sessions (defaults to `True`). |
| `timeout` | `int` | - | Idle seconds after which surplus sessions above `min` are closed. |
| `wait_timeout` | `int` | - | Maximum milliseconds to wait for an available session under `POOL_GETMODE_TIMEDWAIT`. |
| `max_lifetime_session` | `int` | - | Maximum lifetime in seconds before a pooled session is retired. |
| `session_callback` | `Callable[..., Any] or str` | - | Callback `(connection, requested_tag)` invoked when a pooled session is acquired. |
| `max_sessions_per_shard` | `int` | - | Maximum pooled sessions allocated to any single Oracle shard. |
| `soda_metadata_cache` | `bool` | - | Enable Thick-mode SODA collection metadata caching across pooled sessions. |
| `ping_interval` | `int` | - | Idle seconds elapsed before a pooled session is health-checked on checkout. |
| `ping_timeout` | `int` | - | Maximum milliseconds allowed for a checkout health-check ping before replacing the session. |
<!-- GENERATED:END config-table:sqlspec.adapters.oracledb.config.OracleConnectionParams,sqlspec.adapters.oracledb.config.OraclePoolParams -->

### Driver Features

Pass `OracleDriverFeatures` keys inside `driver_features={...}` on `OracleSyncConfig` or `OracleAsyncConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.oracledb.config.OracleDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `plugins` | `Sequence[OraclePluginName or str]` | - | Optional cloud configuration or token plugins imported lazily. |
| `params_hooks` | `Sequence[Callable[[Any], None]]` | - | Native parameter hooks registered idempotently before connecting. |
| `enable_auto_state_tagging` | `bool` | `True` | Synthesize and reuse Thick pool session state tags (defaults to True). |
| `enable_numpy_vectors` | `bool` | `True` | Register bidirectional NumPy `ndarray` handlers for Oracle 23ai+ `VECTOR` columns (defaults to `True` when NumPy is installed). |
| `enable_lowercase_column_names` | `bool` | `True` | Normalize unquoted uppercase Oracle column names to lowercase while preserving quoted aliases (defaults to `True`). |
| `enable_uuid_binary` | `bool` | - | Convert Python `UUID` values to and from `RAW(16)` columns. |
| `vector_return_format` | `OracleVectorReturnFormat` | - | Decoded type for dense `VECTOR` columns (`'numpy'`, `'list'`, or `'array'`). |
| `oracle_varchar2_byte_limit` | `int` | `4000` | UTF-8 byte threshold above which `str` parameters bind as `DB_TYPE_CLOB` (defaults to `4000`). |
| `oracle_raw_byte_limit` | `int` | `2000` | Byte threshold above which `bytes` parameters bind as `DB_TYPE_BLOB` (defaults to `2000`). |
| `arraysize` | `int` | - | Per-cursor `arraysize` row fetch buffer override. |
| `prefetchrows` | `int` | - | Per-cursor `prefetchrows` initial round-trip prefetch count override. |
| `fetch_lobs` | `bool` | - | Return LOB columns as locator objects when `True` or inline `str`/`bytes` when `False`. |
| `fetch_decimals` | `bool` | - | Return `NUMBER` columns as `decimal.Decimal` values when `True`. |
| `on_connection_create` | `Callable[..., Any]` | - | Hook `(connection, tag)` invoked after built-in JSON, VECTOR, and UUID handlers are registered. |
| `enable_events` | `bool` | - | Enable SQLSpec event channels (separate from `connection_config['events']` FAN/CQN notifications). |
| `events_backend` | `OracleEventsBackend` | - | Event queue backend (`'poll_queue'` default, `'aq'`, or `'txeventq'`). |
| `enable_direct_path_load` | `bool` | `True` | Route `load_from_arrow` through Thin-mode `Connection.direct_path_load` with `executemany` fallback (defaults to `True`). |
| `tenant_strategy` | `TenantStrategy or OracleTenantStrategyShorthand or None` | - | Opt-in tenant strategy, or the `"identifier"` shorthand, applied to every session and connection lease. |
| `session_context_hook` | `OracleSessionContextConfig or dict[str, Any]` | - | Declarative `DBMS_SESSION` / `CLIENT_IDENTIFIER` context applied on checkout and cleared on release. |
| `sharding_key_variable` | `str` | - | Tenant variable carrying the primary shard key. |
| `super_sharding_key_variable` | `str` | - | Tenant variable carrying the super-shard key. |
| `shard_resolver` | `OracleShardResolver` | - | Resolve primary and super-shard keys from the captured tenant payload. |
| `use_tenant_id_as_sharding_key` | `bool` | - | Route directly by captured tenant identity in Thick mode. |
| `enable_otel_trace_context` | `bool` | - | Piggyback W3C trace headers on supported Oracle connections. |
| `enable_session_tracing` | `bool` | - | Piggyback active correlation and trace identifiers onto `module`, `action`, `client_identifier`, and `econtext_id` in zero extra RTTs. |
| `dbms_cloud_credential` | `str or None` | - | Default `DBMS_CLOUD` credential name used for server-side `DBMS_CLOUD.COPY_DATA` object-store loads. |
<!-- GENERATED:END config-table:sqlspec.adapters.oracledb.config.OracleDriverFeatures -->

## Oracle 26ai Session Workflows

Use `python-oracledb>=26.1` for these APIs. Each feature also needs server support and the right client mode. A flag cannot enable a feature that the server or client lacks.

#### Centralized configuration

Point `connection_config["dsn"]` at a native config provider URI. `config-file://` reads a local configuration; `file://` normalizes to that scheme. Cloud schemes are `config-ociobject://`, `config-azure://`, `config-awss3://`, `config-awssecretsmanager://`, `config-gcpstorage://`, and `config-gcpsecretmanager://`.

```python
from sqlspec.adapters.oracledb import OracleAsyncConfig

config = OracleAsyncConfig(
    connection_config={"dsn": "config-file:///etc/oracle/connection.json"}
)
config.refresh_config_cache()
```

Cloud modules load when needed. Install the provider's SDK too. A missing SDK raises `MissingDependencyError` with an install hint. `driver_features["plugins"]` also accepts the native configuration plugins and `oci_tokens` or `azure_tokens`. `params_hooks` registers each hook once. Use `register_oracle_params_hook`, `unregister_oracle_params_hook`, and `register_oracle_protocol` for app setup and teardown. Cache refresh can take a different `dsn` and `remove_permanently=True`. Thick mode defaults `thick_mode_dsn_passthrough=False` for config URIs, so the provider can resolve them.

#### Acquire options, sharding, and state tags

Use `OracleAcquireOptions` to select a shard or request a pool tag for one checkout:

```python
from sqlspec.adapters.oracledb import OracleAcquireOptions, OracleSyncConfig

config = OracleSyncConfig(
    connection_config={"dsn": "db.example.internal/app", "user": "app", "password": "secret"}
)
with config.provide_session(acquire_options=OracleAcquireOptions(sharding_key=["acme"])) as session:
    tenant_name = session.select_value("SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') FROM DUAL")
```

The providers also accept `sharding_key` and `super_sharding_key` directly. Direct client routing needs Thick mode and a sharded database. Thin mode omits native shard-key arguments; use the configured routing endpoint. Tenant configs can derive keys from `sharding_key_variable`, `super_sharding_key_variable`, a `shard_resolver`, or `use_tenant_id_as_sharding_key=True`. Select one primary shard source. The resolver receives the captured tenant payload before acquisition.

Thick pools reuse session state through generated tags (`enable_auto_state_tagging=True`). Explicit tags take precedence. Thin pools never read or release a socket's Thick-only tag. Use `pool_boundary="transaction"` with tenant or stateful session hooks. Statement-boundary DRCP release would lose their state. Put an edition in `OracleSessionContextConfig` when also using `cclass`. Connect-time `edition` and `cclass` are rejected together.

#### Session context and end-user security

`OracleSessionContextConfig(contexts={"CLIENTCONTEXT": {"TENANT_ID": "acme"}})` sets native client context. The state travels with the next request. `current_schema`, `clientinfo`, and `dbop` use the same path. Edition, NLS, and other context changes can still need SQL. Reset runs before pool reuse. A failed reset drops the socket.

Use `OracleEndUserSecurityConfig(database_access_token=..., end_user_identity=..., data_roles=..., attributes=...)` in the session hook for Deep Data Security. This needs a supported Oracle 26ai Thin TCPS server. Tokens may be callables resolved at checkout. Set up the server policy and valid credentials first. A tenant ID alone does not enforce row access. The [tenant operations recipe](../learn/recipes/multi-tenancy-operations.md#oracle-client-context-and-security) covers tenant strategies and cleanup.

#### Suspend and resume transactions

`sessionless_transaction()` yields a transaction ID. On clean exit it suspends the work on the server and releases the pool socket. A second checkout can resume that ID with `resume=True` and commit. Errors roll back by default. The sync and async drivers also expose explicit `begin_sessionless_transaction`, `resume_sessionless_transaction`, and `suspend_sessionless_transaction` methods; await all three on the async driver.

Run the [sessionless example](https://github.com/litestar-org/sqlspec/blob/main/docs/examples/databases/oracledb/sessionless_transactions.py) with `ORACLE_DSN`, `ORACLE_USER`, and `ORACLE_PASSWORD`. It creates a temporary ledger table, writes on the first checkout, and resumes and commits on the second. It requires Oracle 23ai/26ai sessionless support and DDL rights for its example table. Keep the ID in app state and resume before the server timeout.

??? example "Complete sessionless example"

    ```python
    --8<-- "docs/examples/databases/oracledb/sessionless_transactions.py"
    ```

`session.set_transaction_priority("HIGH")` sets the native priority field without a query. This call is sync on both drivers. `OracleTransactionPriority` accepts `"DEFAULT"`, `"LOW"`, `"MEDIUM"`, or `"HIGH"`. Priority support and policy depend on the server. `execute(..., suspend_on_success=True)` suspends after one statement. It needs an active sessionless transaction and does not commit.

#### Async pipelines and trace propagation

`execute_stack(..., continue_on_error=False)` tells the native Thin pipeline to stop at the first error. Later operations do not run. `continue_on_error=True` collects each result. Native pipelines need a supported SDK and Oracle 23ai/26ai server. Set `driver_features={"stack_native_disabled": True}` to run stacks one statement at a time. LOB values are read before the pipeline result is returned.

With spans enabled, SQLSpec checks the socket's `supports_opentelemetry_tracing` flag. Supported Oracle 26ai sockets receive W3C headers through `set_opentelemetry_trace_context` on the next wire request. `enable_otel_trace_context=False` disables this propagation. See [observability](../learn/observability.md#request-correlation-and-wire-level-sqlcommenter) for trace extraction and SQLCommenter behavior.

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `NUMBER` | `int` / `float` / `decimal.Decimal` | Exact scale-aware conversion to `int` or `Decimal`. |
| `VARCHAR2` / `NVARCHAR2` | `str` | Bound strings exceeding `oracle_varchar2_byte_limit` (`4000` bytes by default, or `32767` with `MAX_STRING_SIZE=EXTENDED`) promote automatically to `CLOB` parameters to avoid `ORA-01461`. |
| `CLOB` / `NCLOB` / `BLOB` | `str` / `bytes` | Automatically streamed and materialized without leaking `oracledb.LOB` descriptors outside the session. |
| `JSON` (OSON) | `dict` / `list` | Native binary OSON (`DB_TYPE_JSON`) on Oracle 21c+ and 26ai; transparent `CLOB`/`BLOB` fallback on 19c. |
| `RAW(16)` | `uuid.UUID` / `bytes` | Bi-directional `uuid.UUID` <-> `RAW(16)` coercion when `enable_uuid_binary=True` (default). |
| `VECTOR` (Oracle 26ai) | `numpy.ndarray` / `array.array` / `list[float]` | Zero-copy `float32`, `float64`, `int8`, and `binary` vector conversion controlled by `enable_numpy_vectors` and `vector_return_format`. |

- **Session Context on Checkout (`session_context_hook`)**: Pass an `OracleSessionContextConfig` (or an equivalent dict) as `session_context_hook`. Each field accepts a static value or a zero-argument callable resolved per checkout. `module`, `action`, `client_identifier`, and `ecid` are set as connection attributes that `python-oracledb` piggybacks onto the next round trip, while `edition`, `time_zone`, `nls_parameters`, and non-`CLIENTCONTEXT` contexts run in a checkout PL/SQL block. The context is cleared on release when `reset_on_release=True` (default).
- **Direct Path Load and Cloud Storage Copy**: When `enable_direct_path_load=True`, `session.load_from_arrow(...)` uses Oracle's Direct Path Load API (bypassing buffer cache when constraints/triggers allow) and falls back cleanly to batched array DML. For object-store Parquet/CSV/JSON files, set `dbms_cloud_credential` to run server-side `DBMS_CLOUD.COPY_DATA`.
- **Oracle 26ai VECTOR HNSW and Hybrid RRF**: `sqlspec.builder` exposes `build_oracle_hnsw_subquery` and `build_oracle_hybrid_rrf_sql` (used by `OracleSyncADKMemoryStore` and `OracleAsyncADKMemoryStore`) to isolate `VECTOR_DISTANCE(..., COSINE)` inside an inline view so the Oracle Cost-Based Optimizer uses HNSW/IVF vector indexes alongside Oracle Text `CONTAINS` scores in a single-round-trip Reciprocal Rank Fusion query.
- **Oracle AQ Events**: `EventChannel` supports both transactional table queues and native Oracle Advanced Queuing (`DBMS_AQ`) via `driver_features={"events_backend": "aq"}`.

## Standby and Read-Service Pools

Use `replicas={"service_name": "app_ro"}` for an Active Data Guard reader service or a supported True Cache read service. A reader override inherits credentials, tenant setup, and checkout hygiene. Direct reads use `provide_read_session()`; writes use the primary pool.

`OracleTopologyConfig(address_lists=..., connect_timeout=...)` can describe several site address lists, and `with_service_name()` changes the service without losing that topology. `OracleSessionContextConfig` supports `standby_max_data_delay`, `sync_with_primary`, and `adg_redirect_dml`. These controls need the corresponding Oracle server features and privileges. They do not turn an ordinary database into a standby or enable True Cache.

`retry_on_standby_lag=True` can choose primary after a standby-lag failure during checkout, before the session is yielded. It never replays caller SQL. Native read-only errors such as `ORA-16000`, `ORA-01456`, `ORA-16397`, and `ORA-00372` map to `ReadOnlyTransactionError`. Standby/unavailable errors (`ORA-03172`, `ORA-16456`, `ORA-12757`, `ORA-01033`, `DPY-4005`, `DPY-4033`) map to `ReplicaUnavailableError`; `ORA-03173` maps to `ImproperConfigurationError`. Validate ADG, True Cache, role, and licensing on your own deployment. See [Replica Operations](../learn/recipes/read-replicas-operations.md).

## See Also

- [OracleDB API Reference](/reference/adapters/oracledb/)
- [Oracle Database Dialect Reference](/reference/data-dictionary/dialects/oracle/)
