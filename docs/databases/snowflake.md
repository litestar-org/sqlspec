# Snowflake

Use `SnowflakeConfig` to run SQL, stream Arrow results, and load warehouse tables through `snowflake-connector-python`. The adapter is synchronous and owns a bounded connection pool. Install `sqlspec[snowflake]` to include the connector and PyArrow.

## Installation and Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[snowflake]"
    ```
=== "pip"

    ```bash
    pip install "sqlspec[snowflake]"
    ```
=== "pdm"

    ```bash
    pdm add "sqlspec[snowflake]"
    ```
=== "poetry"

    ```bash
    poetry add "sqlspec[snowflake]"
    ```

Configure a connector profile named `prod` in `~/.snowflake/connections.toml`, then run:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.snowflake import SnowflakeConfig

spec = SQLSpec()
config = spec.add_config(
    SnowflakeConfig(connection_config={"connection_name": "prod"})
)
try:
    with config.provide_session() as session:
        print(session.select_value("SELECT :value::INTEGER", {"value": 42}))
finally:
    config.close_pool()
```

Output: `42`. Session contexts release their connection; close the pool when the application stops. Commit DML explicitly or use `session.transaction()`. `execute_many()` follows the connection's transaction settings; wrap it in `session.transaction()` when a batch must be atomic. DDL commits independently, and savepoint methods raise `NotImplementedError`.

## Choosing a Snowflake Adapter

| Choose | When you need |
| --- | --- |
| `SnowflakeConfig` with `sqlspec[snowflake]` | Python connector authentication, TOML profiles, query job IDs, native script execution, stage uploads, or Snowflake extension stores. |
| `AdbcConfig` with `sqlspec[adbc]` and `adbc-driver-snowflake` | The ADBC connection API and Arrow interfaces shared with other analytical engines. See [ADBC](adbc.md). |

## Authentication and Session Settings

Read credentials from your environment or secret manager.
=== "Workload identity"

    ```python
    from sqlspec.adapters.snowflake import SnowflakeConfig
    config = SnowflakeConfig(connection_config={
        "account": "org-account", "user": "warehouse_service",
        "auth_type": "workload_identity", "workload_identity_provider": "AWS",
    })
    ```
    Providers are `AWS`, `GCP`, `AZURE`, and `OIDC`. Azure accepts `workload_identity_entra_resource`;
    identity chains use `workload_identity_impersonation_path`. OIDC can use `token` or `token_file_path`.
=== "Access token"

    ```python
    import os
    from sqlspec.adapters.snowflake import SnowflakeConfig
    config = SnowflakeConfig(connection_config={
        "account": "org-account", "user": "warehouse_service", "auth_type": "pat",
        "auth_token_provider": lambda: os.environ["SNOWFLAKE_TOKEN"],
        "warehouse": "ANALYTICS", "database": "REPORTING", "schema": "PUBLIC",
    })
    ```
    `pat` selects `PROGRAMMATIC_ACCESS_TOKEN`. The callback runs for each new physical connection;
    existing pooled connections retain their authenticated session. Use `token` for a fixed credential.
=== "Key pair"

    ```python
    from pathlib import Path
    from sqlspec.adapters.snowflake import SnowflakeConfig
    config = SnowflakeConfig(connection_config={
        "account": "org-account", "user": "warehouse_service", "auth_type": "jwt",
        "private_key": Path("service-key.p8"),
    })
    ```
    `private_key` accepts DER bytes, PEM text, a path, or a private key object.
    Use `private_key_passphrase` for encrypted keys, or `private_key_file` with `private_key_file_pwd`.
=== "Profile and query tags"

    ```python
    from sqlspec.adapters.snowflake import SnowflakeConfig
    config = SnowflakeConfig(connection_config={
        "connection_name": "prod", "timezone": "UTC", "query_tag": "sales-report",
        "session_parameters": {"QUOTED_IDENTIFIERS_IGNORE_CASE": False},
    })
    ```
    Session options are sent when connecting; explicit `session_parameters` entries take precedence.
    `enable_query_tag_correlation` adds the current SQLSpec correlation ID to statement tags.

For local Embucket testing, set `host="localhost"`, `port=3000`, and `protocol="http"` with local credentials. Embucket covers local protocol tests; cloud authentication and stage uploads require Snowflake.

## Connection and Pool Configuration

Pass connection and pool keys in `connection_config`, and feature flags in `driver_features`. The pool defaults to zero initial connections and five maximum connections. Acquisition waits up to 30 seconds, and a pool inherited through a process fork must be recreated.

<!-- GENERATED:START config-table:sqlspec.adapters.snowflake.config.SnowflakeConnectionParams,sqlspec.adapters.snowflake.config.SnowflakePoolParams -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `account` | `str` | - | Snowflake account identifier, including organization when needed. |
| `user` | `str` | - | Snowflake login user name. |
| `username` | `str` | - | Alias for `user` when the canonical key is absent. |
| `password` | `str` | - | Password for password-based authentication. |
| `host` | `str` | - | Explicit Snowflake endpoint host name. |
| `port` | `int` | - | Endpoint port, normally `443`. |
| `protocol` | `str` | - | Endpoint transport protocol. |
| `warehouse` | `str` | - | Warehouse selected for the connection. |
| `database` | `str` | - | Database selected for the connection. |
| `schema` | `str` | - | Schema selected for the connection. |
| `role` | `str` | - | Role selected for the connection. |
| `auth_type` | `str` | - | Authentication shorthand such as `jwt`, `pat`, `workload_identity`, or `oauth`. |
| `authenticator` | `str` | - | Connector authenticator name or HTTPS Okta endpoint. |
| `token` | `str` | - | Access token for token-based authentication. |
| `token_file_path` | `str or Path` | - | Path containing a workload identity token. |
| `auth_token_provider` | `Callable[[], str]` | - | Callback producing a fresh credential for each physical connection. |
| `private_key` | `Any` | - | PKCS#8 DER bytes, PEM text, key path, or a private key object. |
| `private_key_file` | `str or Path` | - | Path to an optionally encrypted private key file. |
| `private_key_passphrase` | `str or bytes` | - | Passphrase decrypting an in-memory PEM key or private key file. |
| `private_key_file_pwd` | `str or bytes` | - | Connector passphrase for an encrypted private key file. |
| `workload_identity_provider` | `str` | - | Cloud identity provider used for workload identity authentication. |
| `workload_identity_entra_resource` | `str` | - | Microsoft Entra resource requested during Azure token exchange. |
| `workload_identity_impersonation_path` | `str or Sequence[str]` | - | Ordered identity impersonation chain. |
| `connection_name` | `str` | - | Named profile loaded from the connector connections file. |
| `connections_file_path` | `str or Path` | - | Custom path to the connector TOML connections file. |
| `paramstyle` | `Literal['qmark', 'numeric', 'pyformat', 'format']` | `qmark` | Connector parameter binding mode; defaults to `qmark`. |
| `autocommit` | `bool` | - | Whether Snowflake commits statements automatically. |
| `timezone` | `str` | - | Initial session time zone, unless supplied in `session_parameters`. |
| `query_tag` | `str` | - | Initial `QUERY_TAG`, unless supplied in `session_parameters`. |
| `session_parameters` | `Mapping[str, Any]` | - | Session settings applied together when connecting. |
| `login_timeout` | `int or float` | - | Maximum login duration in seconds. |
| `network_timeout` | `int or float` | - | Maximum network retry duration in seconds. |
| `socket_timeout` | `int or float` | - | Socket timeout in seconds. |
| `application` | `str` | - | Application identifier recorded by Snowflake. |
| `client_session_keep_alive` | `bool` | - | Keep the session alive while the connection is idle. |
| `ocsp_fail_open` | `bool` | - | Allow connections when certificate revocation checks are unavailable. |
| `insecure_mode` | `bool` | - | Disable connector certificate revocation checks. |
| `validate_default_parameters` | `bool` | - | Validate database, schema, and warehouse names when connecting. |
| `extra` | `Mapping[str, Any]` | - | Additional connector options, with explicit modeled options taking precedence. |
| `pool_min_size` | `int` | `0` | Number of physical sessions opened when constructing the pool; defaults to `0`. |
| `pool_max_size` | `int` | `5` | Maximum number of simultaneously owned sessions; defaults to `5`. |
| `pool_timeout` | `float` | `30` | Maximum seconds waiting for an available pool slot; defaults to `30`. |
| `pool_recycle_seconds` | `int` | - | Maximum connection lifetime in seconds; `0` disables recycling. |
| `health_check_interval` | `float` | `60` | Idle seconds between connection health checks; defaults to `60`. |
<!-- GENERATED:END config-table:sqlspec.adapters.snowflake.config.SnowflakeConnectionParams,sqlspec.adapters.snowflake.config.SnowflakePoolParams -->

### Driver Features

<!-- GENERATED:START config-table:sqlspec.adapters.snowflake.config.SnowflakeDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `json_serializer` | `Callable[[Any], str]` | - | Serialize structured parameters to JSON text. |
| `json_deserializer` | `Callable[[str], Any]` | - | Decode Snowflake semi-structured result values. |
| `on_connection_create` | `Callable[[SnowflakeConnection], None]` | - | Callback invoked for each newly opened physical connection. |
| `enable_lowercase_column_names` | `bool` | - | Lowercase implicitly uppercase result names while preserving mixed-case aliases. |
| `enable_stage_parquet_load` | `bool` | - | Enable temporary-stage Parquet uploads for large Arrow loads. |
| `stage_parquet_threshold_rows` | `int` | `1000` | Minimum row count selecting stage-based loads; defaults to `1000`. |
| `enable_json_deserialization` | `bool` | `True` | Decode VARIANT, OBJECT, and ARRAY result values with `json_deserializer`; defaults to `True`. |
| `enable_query_tag_correlation` | `bool` | - | Attach statement correlation information to Snowflake query tags. |
| `enable_events` | `bool` | - | Enable SQLSpec table-backed event channels. |
| `events_backend` | `Literal['poll_queue']` | - | Event backend used for Snowflake table queues (`'poll_queue'`). |
| `tenant_strategy` | `SyncTenantStrategy or TenantStrategyShorthand or None` | - | Opt-in tenant strategy, or the `"session_attributes"` shorthand, applied to every session and connection lease. |
<!-- GENERATED:END config-table:sqlspec.adapters.snowflake.config.SnowflakeDriverFeatures -->

## Arrow Streaming and Bulk Loads

`select_to_arrow()` returns an `ArrowResult`; `get_data()` exposes its Arrow table. `select_stream()` reads batches lazily, and readers close their cursor on exhaustion, early close, or session close. Reads use `fetch_arrow_batches()` without pandas, preserve empty schemas, and fall back to rows for unsupported commands. Materialization promotes compatible Arrow schemas; streams cast later batches to the first schema. Uppercase column names become lowercase by default; mixed-case names remain unchanged.

`load_from_arrow(table, source)` uses bound batches below the stage threshold, which defaults to 1000 rows for config-managed sessions. Standalone loads own one DML transaction; existing caller transactions stay open. With `overwrite=True`, existing rows are deleted inside that transaction, so a failed load keeps them. `if_not_exists=True` table creation retains Snowflake DDL commit behavior. Larger loads write temporary Parquet, upload it with a user-stage `PUT`, run `COPY INTO` inside the load transaction, then remove staged files. Use `stage_table=True` with `conflict_keys` for staged `MERGE`; `load_from_storage()` accepts the same conflict options. See [Arrow and storage](../learn/arrow-and-storage.md).

## SQL, Query Jobs, and Extension Stores

Write `:name`, `:1`, `%s`, or `%(name)s` parameters; the default wire format is `?`. Snowflake stages (`@stage`, `@%table`, `@~`), `$1` stage/pipe columns, session variables, and `//` comments remain native SQL. For VARIANT values, use `INSERT INTO events (payload) SELECT PARSE_JSON(:payload)` and bind JSON text. `MERGE INTO ... USING (SELECT PARSE_JSON(:payload) AS payload)` also accepts bound JSON.

`execute_script()` binds supplied values statically, splits SQL with SQLSpec's Snowflake splitter, and executes each statement through its own managed cursor. Scripting blocks, comments, and dollar-quoted bodies stay intact. Transaction ownership comes from `CURRENT_TRANSACTION()` on the server. `execute_async_query()` submits a warehouse query and returns its query ID; `fetch_async_query_result(query_id)` waits for its result. These methods are synchronous Python calls. Async frameworks use `sqlspec.utils.sync_tools.async_()` to bridge synchronous work.

Litestar uses `SnowflakeStore` for binary sessions. Events use `SnowflakeEventQueueStore` with VARIANT payloads and the `poll_queue` backend by default. Google ADK uses `SnowflakeADKStore` and `SnowflakeADKMemoryStore`; memory recall uses `ILIKE`, and vector embeddings are unsupported. Built-in schema reconciliation creates extension tables without secondary indexes.

## Tenant Isolation

Set `driver_features={"tenant_strategy": "rls"}` (or `True`, `"default"`, `"session_attributes"`) to bind each session to a tenant for row access policies. Each lease opens its own connection, sets the tenant as an immutable session attribute with `CALL SET_SYS_CONTEXT('SNOWFLAKE$SESSION_ATTRIBUTES', ...)`, reads it back with `SYS_CONTEXT`, and closes the connection when the session ends. Snowflake cannot overwrite or clear an immutable session attribute, so tenant sessions are never returned to the pool. Setting your own session attributes needs no extra privilege.

```sql
CREATE ROW ACCESS POLICY tenant_rows AS (tenant_col VARCHAR) RETURNS BOOLEAN ->
  tenant_col = SYS_CONTEXT('SNOWFLAKE$SESSION_ATTRIBUTES', 'tenant_id');
ALTER TABLE orders ADD ROW ACCESS POLICY tenant_rows ON (tenant_id);
```

```python
from sqlspec.adapters.snowflake import SnowflakeConfig

config = SnowflakeConfig(
    connection_config={
        "account": "my_org-my_account",
        "user": "APP_SERVICE",
        "warehouse": "APP_WH",
    },
    driver_features={"tenant_strategy": "rls"},
)

with config.provide_session(tenant="acme") as session:
    session.select("SELECT order_id FROM orders")
```

`SYS_CONTEXT` returns `NULL` for an unset attribute, so the policy above hides every row from sessions without a tenant. Attribute names are case-sensitive. Use `SnowflakeSessionAttributeStrategy(tenant_attribute="tenant_id", variable_keys={"region": "tenant_region"})` to choose the attribute names and map `TenantContextPayload` variables to more attributes. `create_connection()` is rejected on tenant configurations; use `provide_session()` or `provide_connection()`. ADBC Snowflake configurations do not support `tenant_strategy`. See [Multi-Tenancy](../learn/recipes/multi-tenancy.md).

## Type Mappings and Related Reference

| Snowflake | Python / Arrow |
| --- | --- |
| `NUMBER`, `FLOAT` | `int` or `Decimal`, `float`; precision-aware Arrow numbers |
| `VARCHAR`, `BINARY`, `BOOLEAN` | `str`, `bytes`, `bool` |
| `VARIANT`, `OBJECT`, `ARRAY` | Decoded Python JSON values for row results; Arrow representation follows connector metadata |
| `TIMESTAMP_TZ`, `TIMESTAMP_LTZ`, `TIMESTAMP_NTZ`, `DATE`, `TIME` | Python and Arrow temporal values; aware datetimes bind explicitly as `TIMESTAMP_TZ` in `qmark`/`numeric` modes |

Catalog key reflection defaults to `PUBLIC`; qualified quoted table names retain their exact identity. Strict schema reconciliation recognizes VARIANT as JSON while rejecting unrelated STRUCT/OBJECT drift. See the [adapter API](../reference/adapters/snowflake.md) and [catalog reference](../reference/data-dictionary/dialects/snowflake.md).
