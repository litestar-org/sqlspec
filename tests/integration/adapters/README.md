# Database-Family Adapter Tests

Integration tests are organized by the database being exercised: `sqlite/`, `mysql/`, `postgres/`,
`cockroach/`, `oracle/`, `mssql/`, `db2/`, `duckdb/`, `bigquery/`, `spanner/`, `gizmosql/`, and
`snowflake/`. Each family collects its portable cases from `test_shared.py` and keeps irreducible
driver behavior below `<family>/<driver>/`.

Reusable test templates, behaviors, cases, assertions, and schema inputs live in the non-collected
`_shared/` package. A behavior is implemented once and parametrized over every capable driver, while
the family collection hook keeps each resulting case under its database family.

## How It Fits Together

Every contract is the cross product of three things:

1. **Cases** (`_cases.py`) — a `DriverCase` per adapter/mode, carrying the fixture name, dialect,
   marks, capability flags/policies, exceptional deviations, and extra-assertion keys. `SYNC_DRIVER_CASES` and
   `ASYNC_DRIVER_CASES` feed the parametrized fixtures; `DEFERRED_DRIVER_CASES` holds adapters that
   are not yet wired (each with a concrete `reason`).
2. **Behaviors** (`behaviors.py`) — reusable `assert_*_contract` functions. Central test bodies stay
   thin and just call a behavior with `(driver, case)` or `(make_config, case)`.
3. **Fixtures** (`tests/integration/fixtures/adapter_cases.py`) — resolve each case's `fixture_name` with
   `request.getfixturevalue()` so marks, xdist grouping, and optional service startup stay attached
   to the case metadata. The resolved object is a `DriverCaseContext(case, driver, make_config)`.

`DriverCaseContext` exposes two things to a contract:

- `context.driver` — a live driver session for behaviors that exercise queries.
- `context.make_config` — a per-adapter **config factory** (resolved from
  `case.config_factory_fixture`) for lifecycle contracts that must construct fresh configs and open
  their own pools/sessions. `None` when the case declares no factory.

## Directory Map

### Shared modules

| Module | Responsibility |
| --- | --- |
| `_cases.py` | `DriverCase`/`DriverCaseContext` dataclasses, all driver cases, capability flags, marks. |
| `behaviors.py` | All `assert_*_contract` behaviors, the extra-assertion registry, lifecycle config protocols. |
| `tests/integration/fixtures/adapter_cases.py` | Parametrized case fixtures, per-driver live sessions, and lifecycle config factories. |
| `_inputs.py` | Shared parameter/statement input data (used instead of branching on adapter names). |
| `_schema.py` | `ContractTable` schema metadata per dialect (`DEFAULT_/DUCKDB_/MYSQL_/ORACLE_/POSTGRES_CONTRACT_TABLE`). |
| `_assertions.py` | Low-level shared assertion helpers. |

### Extension cases & behaviors

| Module | Responsibility |
| --- | --- |
| `_adk_cases.py` / `adk_behaviors.py` | Google ADK session-store contract cases and behaviors. |
| `_events_cases.py` / `events_behaviors.py` | Event-queue store and native PostgreSQL LISTEN/NOTIFY contract cases and behaviors. |
| `_store_cases.py` / `store_behaviors.py` | Litestar session-store contract cases and behaviors. |
| `transaction_behaviors.py` | Nested transaction block behaviors used by the nested transactions suite. |
| `_migration_cases.py` / `migration_behaviors.py` | Migration contract cases and behaviors. |

### Shared test templates

| Test file | Covers |
| --- | --- |
| `suite_driver_contract.py` | CRUD lifecycle (`driver_basics`), `execute_many`, `StatementStack` happy path, `for_update` (gated), case metadata. |
| `suite_lifecycle_contract.py` | Config-factory contracts: pooling, `on_connection_create` hook, `connection_instance` injection, lowercase columns, UUID feature, custom JSON serializer, custom type adapters (all gated). |
| `suite_driver_features_contract.py` | `driver_features` folds (psycopg COPY, DuckDB `SET VARIABLE`, Oracle sequence + native JSON, BigQuery scalar SQL) via extra-assertions. |
| `suite_parameter_contract.py` / `suite_parameter_styles_contract.py` | Generic parameter profiles and binding styles from `_inputs.py`. |
| `suite_statement_inputs_contract.py` | Statement input variants (raw SQL, `SQL(...)` objects, filtered statements, loader input). |
| `suite_result_contract.py` | `SQLResult` API (`get_first`/`get_count`/`is_empty`/`one_or_none`). |
| `suite_query_contract.py` | Filters (`InCollection`/`LimitOffset`/`OrderBy`/`Search`) and complex queries (joins/subqueries/aggregates/CTEs). |
| `suite_execute_many_contract.py` | `execute_many` mutation/input variants and per-adapter specifics; skipped for active bulk-only cases. |
| `suite_explain_contract.py` | `EXPLAIN` plans (gated by `supports_explain`). |
| `suite_arrow_contract.py` | Arrow result export (gated by `supports_arrow`). |
| `suite_script_error_contract.py` | `execute_script` and script error handling. |
| `suite_exceptions_contract.py` | Exception translation (gated by `supports_exception_translation`). |
| `suite_storage_bridge_contract.py` / `suite_storage_bridge_rustfs_contract.py` | Storage bridge round-trips (local and RustFS/S3). |
| `suite_migrations_contract.py` | Migration apply/rollback (gated by `supports_migrations`). |
| `suite_nested_transactions_contract.py` | Nested `begin_transaction()` and `transaction()` blocks through savepoints, or a clear error when `supports_savepoints` is off (gated by `supports_transactions`). |
| `suite_adk_store_contract.py` / `suite_events_queue_contract.py` / `suite_listen_notify_contract.py` / `suite_litestar_store_contract.py` | Extension store contracts (ADK / events queue / native PostgreSQL LISTEN/NOTIFY / Litestar). |
| `suite_extra_assertions_proof_contract.py` | Proves the extra-assertion registry mechanism end-to-end (`driver_basics:noop`). |

## Capability Flags

`DriverCase` carries `supports_*` flags so a single contract can branch on **capability** rather than
adapter name. Flags are **additive and opt-in**: each defaults to a value meaning "no extra behavior"
(`False`, except a few baseline truths — `supports_transactions`, `supports_execute_many`,
`supports_execute_script`, `supports_filtered_statement`, `supports_loader_input`,
`supports_exception_translation` — that default `True`). An adapter that opts into nothing behaves
exactly as it did before the flag existed.

Complete flag set:

- **Result / IO**: `supports_arrow`, `supports_arrow_streaming`, `supports_native_arrow`,
  `supports_storage_bridge`, `supports_native_bulk_ingest`, `supports_copy`
- **Statements**: `supports_execute_many`, `supports_execute_script`, `supports_filtered_statement`,
  `supports_loader_input`, `supports_merge`, `supports_returning`, `supports_for_update`,
  `supports_for_share`
- **Statement policies**: `execute_rowcount_policy`, `execute_many_rowcount_policy`,
  `unsupported_explain_reason`
- **Types / codecs**: `supports_json`, `supports_json_native`, `supports_arrays`,
  `supports_native_array_codec`, `supports_vector`, `supports_lob`
- **Schema / migrations**: `supports_migrations`, `supports_schema_qualified_ddl`,
  `supports_multi_schema_migrations`, `supports_data_dictionary`, `supports_native_metadata`,
  `supports_native_statistics`
- **Connectivity / transactions**: `supports_transactions`, `supports_exception_translation`
- **Lifecycle (config-factory)**: `supports_pooling`, `supports_connection_hook`,
  `supports_connection_instance`, `supports_lowercase_columns`, `supports_uuid_feature`,
  `supports_custom_json_serializer`, `supports_custom_type_adapters`
- **Fixture/profile limits**: `supports_search_filter`, `supports_grouped_subquery`,
  `supports_stream_reopen_after_partial_iteration`, `stream_chunk_policy`,
  `invalid_sql_error_policy`

## Lifecycle Contracts & Config Factories

Some behavior cannot be proven from a single pre-built driver session — it needs to **construct fresh
configs and open multiple pools/sessions** (pooling, `on_connection_create` hooks, injected pools,
driver-feature toggles). These run through a per-adapter **config factory**.

1. Add a `lifecycle_config_<adapter>` fixture in `tests/integration/fixtures/adapter_cases.py` returning a `make(...)` callable that
   builds a fresh config. The callable accepts keyword toggles such as `pooled=`, `driver_features=`,
   `connection_overrides=`, and `connection_instance=`, threading each through to the real config.
2. Point the case at it with `config_factory_fixture="lifecycle_config_<adapter>"`. The fixture layer
   resolves it into `DriverCaseContext.make_config`.
3. Opt the case into the relevant `supports_*` lifecycle flag.
4. Parametrize the contract with cases filtered by the exact capability and call the behavior with the factory:

   ```python
   @pytest.mark.parametrize("sync_lifecycle_driver_case", sync_driver_params_with("supports_pooling"), indirect=True)
   def test_sync_pooling_contract(sync_lifecycle_driver_case: DriverCaseContext) -> None:
       assert_sync_pooling_contract(_sync_factory(sync_lifecycle_driver_case), sync_lifecycle_driver_case.case)
   ```

Lifecycle behaviors (each has a sync and async form):

| Flag | Behavior | Proves |
| --- | --- | --- |
| `supports_pooling` | `assert_*_pooling_contract` | Pooled config shares data across sessions drawn from one pool. |
| `supports_connection_hook` | `assert_*_connection_hook_contract` | The `on_connection_create` driver-feature hook fires. |
| `supports_connection_instance` | `assert_*_connection_instance_contract` | An injected `connection_instance` pool is honored (identity, `provide_pool` bypass, cross-session persistence); `None` builds a fresh pool. |
| `supports_lowercase_columns` | `assert_*_lowercase_columns_contract` | Column-name casing feature (default on; uppercase when disabled). |
| `supports_uuid_feature` | `assert_*_uuid_feature_contract` | UUID feature binds/returns `uuid.UUID` when enabled; raw form when disabled. |
| `supports_custom_json_serializer` | `assert_*_custom_json_serializer_contract` | A custom `json_serializer` driver feature is invoked for JSON binds. |
| `supports_custom_type_adapters` | `assert_sync_custom_type_adapters_contract` | Custom type adapters hydrate JSON columns to dict/list. |

The lifecycle config surface is typed via the `SyncLifecycleConfig` / `AsyncLifecycleConfig`
protocols in `behaviors.py` (`provide_session`, `provide_pool`, `close_pool`, `connection_instance`).

## Deviations vs Extra Assertions

Typed `DriverCase` metadata is the default way to express adapter differences. Use capability flags
for supported behavior, policy fields for rowcount/streaming/error-profile differences, and filtered
param lists so unsupported cases are not collected for a behavior they cannot run.

`deviations` is an exceptional **subtractive** escape hatch: a tuple of string keys that relax or skip a
generic assertion only when the difference cannot be represented as typed capability or policy data.
Every deviation key must be consumed by a behavior and justified by a contract metadata guard.

`extra_assertions` is the **additive** counterpart: a tuple of registered proof keys that let a single
contract run adapter-specific extra checks without a separate per-adapter file. This is how
driver-specific behavior (Oracle LOB/JSON, ADBC backends, psycopg COPY, DuckDB `SET VARIABLE`,
BigQuery SQL functions) folds into the contract layer.

- Register a proof in `behaviors.py` with `register_sync_extra_assertion(key, scope, fn)` /
  `register_async_extra_assertion(...)`.
- Opt a case in with `extra_assertions=("scope:key",)` (e.g. `"param_codecs:oracle"`).
- Dispatch from the owning behavior with `dispatch_sync_extra_assertions(driver, case, scope)` /
  `dispatch_async_extra_assertions(...)`.
- `validate_extra_assertions(case)` fails loud on any key registered nowhere, so consolidating a
  per-adapter test can never silently drop coverage.

Registered scopes:

| Scope | Owning contract | Folds |
| --- | --- | --- |
| `driver_basics` | driver basics | per-adapter no-op/extra CRUD proofs |
| `param_codecs` | parameter styles | per-adapter type/codec round-trips (Oracle LOB/RAW/DATE/JSON, ADBC backends, MySQL, postgres family, BigQuery) |
| `execute_many_specifics` | execute_many | per-adapter batch/analytics specifics (postgres, DuckDB) |
| `driver_features` | driver features | psycopg COPY, DuckDB `SET VARIABLE`, Oracle sequence + native JSON, BigQuery scalar SQL |
| `explain_modifiers` | explain | dialect-specific `EXPLAIN` modifiers |
| `arrow_specifics` | arrow | dialect-specific Arrow export details |

## Parameter Styles

Generic parameter-style behavior belongs in `suite_parameter_styles_contract.py` with cases from
`_inputs.py`: qmark tuple/list binding, named dictionaries, `SQL(...)` objects, repeated named binds,
`execute_many()` tuple and dictionary payloads, LIKE predicates, and injection-looking values. The
per-adapter parameter-variant files have been folded into this contract; do not reintroduce them.

Dialect-specific parameter and type-codec behavior (MySQL pyformat/boolean/float/empty-string,
DuckDB numeric/array, Oracle LOB/RAW/DATE/JSON, ADBC backends, postgres family, BigQuery) folds in
through the `param_codecs` extra-assertion scope rather than a separate per-adapter file — opt a case
in with `extra_assertions=("param_codecs:<adapter>",)`.

## What Stays Adapter-Local (Residuals)

The consolidation keeps the contract suite the single source of truth for cross-adapter behavior, but
deliberately leaves **irreducible** per-adapter tests in place. Do not fold these in:

- `StatementStack` `continue_on_error` / post-error recovery — transaction-poisoning diverges by
  engine (postgres/oracle poison the transaction; sqlite/mysql do not).
- `for_update` `NOWAIT` / `OF <tables>` / `marks_prepared` (e.g. asyncpg) — the contract covers only
  `FOR UPDATE` / `SKIP LOCKED` / `FOR SHARE`.
- Adapter `*_specific_features` and exact SQL-generation assertions.
- JSONB operator semantics; psqlpy `?`-in-comments / regex param-binding quirks.
- Connection/pool plumbing **internals** (`create_pool`/`create_connection`, memory→URI conversion,
  pool sizing, read-only/PRAGMA settings) — the shared "data persists across pooled sessions"
  guarantee is contracted, but adapter-internal assertions stay local.
- Spanner remains registered in `DEFERRED_DRIVER_CASES` until the shared harness can model its separate admin-DDL and read/write
  sessions without weakening the portable behavior checks.

Current residual inventory:

| Area | Local files | Why they stay local |
| --- | --- | --- |
| Oracle type handlers | `oracle/oracledb/test_json_storage_modes.py`, `oracle/oracledb/test_lob_fetch_parity.py`, `oracle/oracledb/test_msgspec_clob.py`, `oracle/oracledb/test_numpy_vectors.py`, `oracle/oracledb/test_smart_lob_coercion.py`, `oracle/oracledb/test_sparse_vectors.py`, `oracle/oracledb/test_uuid_binary.py` | Oracle-specific LOB/CLOB/BLOB hydration, 18c vs 23ai JSON storage rungs, NumPy/vector type handlers, sparse vector passthrough, and RAW/VARCHAR UUID coexistence. Portable vector, UUID, result, JSON, PL/SQL, and StatementStack behavior is contract-owned. |
| Oracle direct/load/driver & ADB wallet | `oracle/oracledb/test_adb_wallet.py`, `oracle/oracledb/test_direct_path_load.py`, `oracle/oracledb/test_driver.py`, `oracle/oracledb/test_execute_many.py`, `oracle/oracledb/test_features.py`, `oracle/oracledb/test_migrations.py`, `oracle/oracledb/test_stack_transaction.py` | Autonomous Database Free 23ai mTLS `wallet_zip` (`ewallet.p12` to `ewallet.pem`) and `DBMS_CLOUD` checks, direct-path loading, shared sync/async Oracle locking behavior, native pool protocols, and Oracle-specific migration/feature surfaces. |
| ADBC backend residuals | `sqlite/adbc/`, `duckdb/adbc/`, `postgres/adbc/`, `bigquery/adbc/`, `oracle/adbc/test_adbc_oracle.py`, `db2/adbc/test_adbc_db2.py`, `snowflake/adbc/test_adbc_snowflake.py` | Raw ADBC connection metadata, pure-Go enterprise ADBC drivers (`adbc-driver-oracle`, `adbc-driver-db2`, `adbc-driver-snowflake`), backend-native SQL, dialect fallback, and backend-specific recovery. Shared helpers live in `_shared/adbc_*.py`; each database family imports only its own tests. |
| GizmoSQL ADBC | `gizmosql/adbc/` | GizmoSQL FlightSQL service behavior (`gizmosql_service` and `gizmosql_sqlite_service`), result streams, Arrow storage, migration, and dictionary behavior. |
| IBM Db2 adapter residuals | `db2/test_adapter.py` | Db2 Community Edition catalog, CLI connection-keyword normalization, and dialect-specific lock/register behavior. |
| MySQL & MariaDB residuals | `mysql/test_adk_residuals.py`, `mysql/test_column_metadata.py`, `mysql/test_features.py`, `mysql/test_mariadb.py`, `mysql/test_table_fixtures.py` | Consolidated MySQL ADK schema/precision/foreign-key residuals across all MySQL drivers, MariaDB 11.8 (`mariadb_118_service`) compatibility, and MySQL table-fixture/column-metadata details. |
| CockroachDB residuals | `cockroach/test_column_metadata.py`, `cockroach/test_native_storage.py`, `cockroach/test_native_storage_capabilities.py`, `cockroach/test_retry.py`, `cockroach/test_table_fixtures.py` | CockroachDB `40001` serialization-failure retry isolation, native external storage bridges, and `unique_rowid()` sequence/metadata behavior. |
| BigQuery cloud/analytics specifics | `bigquery/bigquery/test_arrow.py`, `bigquery/bigquery/test_config.py`, `bigquery/bigquery/test_driver.py`, `bigquery/bigquery/test_native_export.py`, `bigquery/bigquery/test_parameter_variants.py`, `bigquery/bigquery/test_vector_functions.py` | Emulator/native BigQuery project-dataset setup, native storage export, job controls, parameter forms, and vector/analytics behavior beyond the active BigQuery contract case. |
| Spanner GoogleSQL | `spanner/spanner/test_arrow.py`, `spanner/spanner/test_async_driver.py`, `spanner/spanner/test_batch_write_api.py`, `spanner/spanner/test_crud_operations.py`, `spanner/spanner/test_driver.py`, `spanner/spanner/test_exceptions.py`, `spanner/spanner/test_execute_many.py`, `spanner/spanner/test_explain.py`, `spanner/spanner/test_load_from_arrow_mutations.py`, `spanner/spanner/test_parameter_variants.py`, `spanner/spanner/test_returning.py`, `spanner/spanner/test_session_defaults.py`, `spanner/spanner/test_transactions.py` | Spanner needs admin-API DDL, separate read/write sessions, SDK BYTES encoding, mutation transports, and `query_mode=PLAN`; it remains deferred until the contract table fixture can preserve those semantics safely. |
| Spangres placeholders | `spanner/spanner/test_spangres_driver.py`, `spanner/spanner/test_spangres_parameter_styles.py` | Runtime Spangres coverage needs PostgreSQL-dialect Spanner fixtures that do not exist yet. The files keep only executable dialect/default-style documentation assertions. |
| Google Cloud asyncpg & AlloyDB Omni | `postgres/asyncpg/test_cloud_connectors.py`, `postgres/test_alloydb_omni.py` | Cloud SQL and AlloyDB connector authentication plus AlloyDB Omni 18 (`alloydb_omni_18_service`) `enable_alloydb_columnar` session GUC verification across `asyncpg`, `psycopg`, and `psqlpy`. |
| PostgreSQL-family driver quirks | `postgres/asyncpg/test_driver.py`, `postgres/psqlpy/test_driver.py`, `postgres/psycopg/test_driver.py`, `postgres/psycopg/test_async_copy.py`, `postgres/*/extensions/events/test_listen_notify.py` | Driver-specific cursor/prepared statement/COPY behavior plus native LISTEN/NOTIFY concurrency and durable-hybrid regressions. Portable native delivery, metadata, backend selection, and ack behavior is contract-owned. |
| Adapter extension storage details | `*/*/extensions/adk/test_owner_id_column.py`, `*/*/extensions/adk/test_memory_store.py`, `sqlite/aiosqlite/extensions/litestar/test_numpy_serialization.py`, `spanner/spanner/extensions/adk/test_adk_artifact_store.py`, `spanner/spanner/extensions/adk/test_adk_store.py`, `spanner/spanner/extensions/litestar/test_store.py`, `spanner/spanner/extensions/events/test_queue_backend.py` | Extension storage/serialization details and Spanner extension stores (including GCS-backed ADK artifact version tracking against `floci_gcp_service`) that need adapter-specific DDL/session fixtures; generic ADK/events/Litestar behavior is handled by the extension contracts for active cases. |

## Adding A Case

1. Add or update the `DriverCase` in `_cases.py` (fixture name, dialect, mode, marks, capability flags).
2. Keep unsupported or unwired adapters in `DEFERRED_DRIVER_CASES` with a concrete `reason`.
3. Add fixture setup in `tests/integration/fixtures/adapter_cases.py` only when the driver needs shared-table setup, and a
   `lifecycle_config_<adapter>` factory if it opts into any lifecycle flag.
4. Put reusable assertions in `behaviors.py`; keep central test bodies thin.
5. Add input data to `_inputs.py` or schema metadata to `_schema.py` instead of branching on adapter
   names inside tests.
6. When deleting a per-adapter test, confirm its behavior maps to a wired, gated contract (or an
   `extra_assertions` proof) so no coverage is silently dropped.

## Running

```bash
# One database family
uv run pytest tests/integration/adapters/sqlite

# All shared family cases (service containers start through clientless pytest-databases)
uv run pytest -n 2 --dist=loadgroup tests/integration/adapters/*/test_shared.py

# Service-group integration shards (matching CI test-integration matrix)
make test-integration-postgres-cockroach
make test-integration-mysql-mariadb-embedded
make test-integration-oracle
make test-integration-mssql-db2
make test-integration-cloud-adbc-storage
```

`tests/integration/conftest.py` registers clientless `pytest_databases.docker.*` service plugins and
imports only `tests/integration/fixtures/<family>.py` plugins whose required Python driver packages
resolve through `_FIXTURE_DRIVER_REQUIREMENTS`.

Under `pytest-xdist` (`--dist=loadgroup`), `tests/integration/adapters/conftest.py` normalizes each
collected item to a single `xdist_group`: items carrying a dedicated secondary-container marker in
`_PRESERVED_CONTAINER_XDIST_GROUPS` (`alloydb`, `gizmosql_sqlite`, `mariadb`, `oracle_18c`,
`oracle_adb`, `paradedb`, `pgvector`, `snowflake`) keep that container group, while all other items
group under their database family.

BigQuery and Spanner emulator suites run automatically in CI (`CI=true`). Local runs opt in with
either `--run-bigquery-tests` or `SQLSPEC_ENABLE_BIGQUERY_TESTS=1` for BigQuery, and either
`--run-spanner-tests` or `SQLSPEC_ENABLE_SPANNER_TESTS=1` for Spanner.
