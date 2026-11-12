# Oracle Database

`sqlspec.adapters.oracledb` wraps `python-oracledb` (`>= 3.4`) across Oracle Database 12c through 26ai in Thin mode (older releases require Thick mode), supporting synchronous (`OracleSyncConfig`) and `asyncio` (`OracleAsyncConfig`) pools, zero-client Thin mode, Instant Client Thick mode, mTLS/PKCS#12 wallets (`wallet_zip`), multi-endpoint RAC/Data Guard topologies (`OracleTopologyConfig`), zero-RTT session context piggybacking (`OracleSessionContextConfig`), Oracle 26ai `VECTOR` and Hybrid RRF search, Direct Path Loads, and server-side `DBMS_CLOUD.COPY_DATA`.

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
| `appcontext` | `list[OracleAppContext]` | - | Initial `(namespace, name, value)` application context entries set on connect. |
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
| `session_callback` | `Callable[..., Any]` | - | Callback `(connection, requested_tag)` invoked when a pooled session is acquired. |
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
| `tenant_strategy` | `TenantStrategy or TenantStrategyShorthand or None` | - | Opt-in tenant strategy, or the `"identifier"` shorthand, applied to every session and connection lease. |
| `session_context_hook` | `OracleSessionContextConfig or dict[str, Any]` | - | Declarative `DBMS_SESSION` / `CLIENT_IDENTIFIER` context applied on checkout and cleared on release. |
| `enable_session_tracing` | `bool` | - | Piggyback active correlation and trace identifiers onto `module`, `action`, `client_identifier`, and `econtext_id` in zero extra RTTs. |
| `dbms_cloud_credential` | `str or None` | - | Default `DBMS_CLOUD` credential name used for server-side `DBMS_CLOUD.COPY_DATA` object-store loads. |
<!-- GENERATED:END config-table:sqlspec.adapters.oracledb.config.OracleDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `NUMBER` | `int` / `float` / `decimal.Decimal` | Exact scale-aware conversion to `int` or `Decimal`. |
| `VARCHAR2` / `NVARCHAR2` | `str` | Bound strings exceeding `oracle_varchar2_byte_limit` (`4000` bytes by default, or `32767` with `MAX_STRING_SIZE=EXTENDED`) promote automatically to `CLOB` parameters to avoid `ORA-01461`. |
| `CLOB` / `NCLOB` / `BLOB` | `str` / `bytes` | Automatically streamed and materialized without leaking `oracledb.LOB` descriptors outside the session. |
| `JSON` (OSON) | `dict` / `list` | Native binary OSON (`DB_TYPE_JSON`) on Oracle 21c+ and 26ai; transparent `CLOB`/`BLOB` fallback on 19c. |
| `RAW(16)` | `uuid.UUID` / `bytes` | Bi-directional `uuid.UUID` <-> `RAW(16)` coercion when `enable_uuid_binary=True` (default). |
| `VECTOR` (Oracle 26ai) | `numpy.ndarray` / `array.array` / `list[float]` | Zero-copy `float32`, `float64`, `int8`, and `binary` vector conversion controlled by `enable_numpy_vectors` and `vector_return_format`. |

- **Session Context on Checkout (`session_context_hook`)**: Pass an `OracleSessionContextConfig` (or an equivalent dict) as `session_context_hook`. Each field accepts a static value or a zero-argument callable resolved per checkout. `module`, `action`, `client_identifier`, and `ecid` are set as connection attributes that `python-oracledb` piggybacks onto the next round trip, while `edition`, `time_zone`, `nls_parameters`, and `contexts` run in a single checkout PL/SQL block. The context is cleared on release when `reset_on_release=True` (default).
- **Direct Path Load and Cloud Storage Copy**: When `enable_direct_path_load=True`, `session.load_from_arrow(...)` uses Oracle's Direct Path Load API (bypassing buffer cache when constraints/triggers allow) and falls back cleanly to batched array DML. For object-store Parquet/CSV/JSON files, set `dbms_cloud_credential` to run server-side `DBMS_CLOUD.COPY_DATA`.
- **Oracle 26ai VECTOR HNSW and Hybrid RRF**: `sqlspec.builder` exposes `build_oracle_hnsw_subquery` and `build_oracle_hybrid_rrf_sql` (used by `OracleSyncADKMemoryStore` and `OracleAsyncADKMemoryStore`) to isolate `VECTOR_DISTANCE(..., COSINE)` inside an inline view so the Oracle Cost-Based Optimizer uses HNSW/IVF vector indexes alongside Oracle Text `CONTAINS` scores in a single-round-trip Reciprocal Rank Fusion query.
- **Oracle AQ Events**: `EventChannel` supports both transactional table queues and native Oracle Advanced Queuing (`DBMS_AQ`) via `driver_features={"events_backend": "aq"}`.

## See Also

- [OracleDB API Reference](/reference/adapters/oracledb/)
- [Oracle Database Dialect Reference](/reference/data-dictionary/dialects/oracle/)
