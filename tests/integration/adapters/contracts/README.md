# Adapter Contract Tests

This package is the single source of truth for behavior expected from more than one adapter.
A contract is written **once**, parametrized over every adapter that should satisfy it, and gated
by capability flags so the test body never branches on an adapter name. Per-adapter folders keep
only irreducible, adapter-specific behavior.

> Rule of thumb: move behavior here the moment two or more adapters should satisfy the same
> assertion. Keep adapter-specific SQL, optional service setup, and one-off regressions in the
> adapter-local files.

## How It Fits Together

Every contract is the cross product of three things:

1. **Cases** (`_cases.py`) — a `DriverCase` per adapter/mode, carrying the fixture name, dialect,
   marks, capability flags, deviations, and extra-assertion keys. `SYNC_DRIVER_CASES` and
   `ASYNC_DRIVER_CASES` feed the parametrized fixtures; `DEFERRED_DRIVER_CASES` holds adapters that
   are not yet wired (each with a concrete `reason`).
2. **Behaviors** (`behaviors.py`) — reusable `assert_*_contract` functions. Central test bodies stay
   thin and just call a behavior with `(driver, case)` or `(make_config, case)`.
3. **Fixtures** (`conftest.py`) — resolve each case's `fixture_name` with
   `request.getfixturevalue()` so marks, xdist grouping, and optional service startup stay attached
   to the case metadata. The resolved object is a `DriverCaseContext(case, driver, make_config)`.

`DriverCaseContext` exposes two things to a contract:

- `context.driver` — a live driver session for behaviors that exercise queries.
- `context.make_config` — a per-adapter **config factory** (resolved from
  `case.config_factory_fixture`) for lifecycle contracts that must construct fresh configs and open
  their own pools/sessions. `None` when the case declares no factory.

## Directory Map

### Core modules

| Module | Responsibility |
| --- | --- |
| `_cases.py` | `DriverCase`/`DriverCaseContext` dataclasses, all driver cases, capability flags, marks. |
| `behaviors.py` | All `assert_*_contract` behaviors, the extra-assertion registry, lifecycle config protocols. |
| `conftest.py` | Parametrized `*_driver_case` fixtures, per-adapter driver fixtures, and the `lifecycle_config_*` config factories. |
| `_inputs.py` | Shared parameter/statement input data (used instead of branching on adapter names). |
| `_schema.py` | `ContractTable` schema metadata per dialect (`DEFAULT_/DUCKDB_/MYSQL_/ORACLE_/POSTGRES_CONTRACT_TABLE`). |
| `_assertions.py` | Low-level shared assertion helpers. |

### Extension cases & behaviors

| Module | Responsibility |
| --- | --- |
| `_adk_cases.py` / `adk_behaviors.py` | Google ADK session-store contract cases and behaviors. |
| `_events_cases.py` / `events_behaviors.py` | Event-queue store contract cases and behaviors. |
| `_store_cases.py` / `store_behaviors.py` | Litestar session-store contract cases and behaviors. |
| `_migration_cases.py` / `migration_behaviors.py` | Migration contract cases and behaviors. |

### Contract test files

| Test file | Covers |
| --- | --- |
| `test_driver_contract.py` | CRUD lifecycle (`driver_basics`), `execute_many`, `StatementStack` happy path, `for_update` (gated), case metadata. |
| `test_lifecycle_contract.py` | Config-factory contracts: pooling, `on_connection_create` hook, `connection_instance` injection, lowercase columns, UUID feature, custom JSON serializer, custom type adapters (all gated). |
| `test_driver_features_contract.py` | `driver_features` folds (psycopg COPY, DuckDB `SET VARIABLE`, Oracle sequence + native JSON, BigQuery scalar SQL) via extra-assertions. |
| `test_parameter_contract.py` / `test_parameter_styles_contract.py` | Generic parameter profiles and binding styles from `_inputs.py`. |
| `test_statement_inputs_contract.py` | Statement input variants (raw SQL, `SQL(...)` objects, filtered statements, loader input). |
| `test_result_contract.py` | `SQLResult` API (`get_first`/`get_count`/`is_empty`/`one_or_none`). |
| `test_query_contract.py` | Filters (`InCollection`/`LimitOffset`/`OrderBy`/`Search`) and complex queries (joins/subqueries/aggregates/CTEs). |
| `test_execute_many_contract.py` | `execute_many` mutation/input variants and per-adapter specifics. |
| `test_explain_contract.py` | `EXPLAIN` plans (gated by `supports_explain`). |
| `test_arrow_contract.py` | Arrow result export (gated by `supports_arrow`). |
| `test_script_error_contract.py` | `execute_script` and script error handling. |
| `test_exceptions_contract.py` | Exception translation (gated by `supports_exception_translation`). |
| `test_storage_bridge_contract.py` / `test_storage_bridge_rustfs_contract.py` | Storage bridge round-trips (local and RustFS/S3). |
| `test_migrations_contract.py` | Migration apply/rollback (gated by `supports_migrations`). |
| `test_adk_store_contract.py` / `test_events_queue_contract.py` / `test_litestar_store_contract.py` | Extension store contracts (ADK / events / Litestar). |
| `test_extra_assertions_proof_contract.py` | Proves the extra-assertion registry mechanism end-to-end (`driver_basics:noop`). |

## Capability Flags

`DriverCase` carries `supports_*` flags so a single contract can branch on **capability** rather than
adapter name. Flags are **additive and opt-in**: each defaults to a value meaning "no extra behavior"
(`False`, except a few baseline truths — `supports_transactions`, `supports_execute_many`,
`supports_execute_script`, `supports_filtered_statement`, `supports_loader_input`,
`supports_exception_translation` — that default `True`). An adapter that opts into nothing behaves
exactly as it did before the flag existed.

Complete flag set:

- **Result / IO**: `supports_arrow`, `supports_explain`, `supports_storage_bridge`, `supports_copy`
- **Statements**: `supports_execute_many`, `supports_execute_script`, `supports_filtered_statement`,
  `supports_loader_input`, `supports_merge`, `supports_returning`, `supports_for_update`
- **Types / codecs**: `supports_json`, `supports_json_native`, `supports_arrays`,
  `supports_native_array_codec`, `supports_vector`, `supports_lob`
- **Schema / migrations**: `supports_migrations`, `supports_schema_qualified_ddl`,
  `supports_multi_schema_migrations`, `supports_data_dictionary`
- **Connectivity / transactions**: `supports_transactions`, `supports_exception_translation`
- **Lifecycle (config-factory)**: `supports_pooling`, `supports_connection_hook`,
  `supports_connection_instance`, `supports_lowercase_columns`, `supports_uuid_feature`,
  `supports_custom_json_serializer`, `supports_custom_type_adapters`

## Lifecycle Contracts & Config Factories

Some behavior cannot be proven from a single pre-built driver session — it needs to **construct fresh
configs and open multiple pools/sessions** (pooling, `on_connection_create` hooks, injected pools,
driver-feature toggles). These run through a per-adapter **config factory**.

1. Add a `lifecycle_config_<adapter>` fixture in `conftest.py` returning a `make(...)` callable that
   builds a fresh config. The callable accepts keyword toggles such as `pooled=`, `driver_features=`,
   `connection_overrides=`, and `connection_instance=`, threading each through to the real config.
2. Point the case at it with `config_factory_fixture="lifecycle_config_<adapter>"`. The fixture layer
   resolves it into `DriverCaseContext.make_config`.
3. Opt the case into the relevant `supports_*` lifecycle flag.
4. Gate the contract on the flag and call the behavior with the factory:

   ```python
   def test_sync_pooling_contract(sync_driver_case: DriverCaseContext) -> None:
       if not sync_driver_case.case.supports_pooling:
           pytest.skip(f"{sync_driver_case.case.adapter} has no verified pooling support")
       assert_sync_pooling_contract(_sync_factory(sync_driver_case), sync_driver_case.case)
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

`deviations` is **subtractive**: a tuple of string keys that relax or skip a generic assertion
(`if "key" not in case.deviations: ...`). Use it when an adapter cannot satisfy part of a shared
contract (e.g. `"no-returning"`, `"no-for-share"`, `"execute-rows-affected-unavailable"`).

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

Generic parameter-style behavior belongs in `test_parameter_styles_contract.py` with cases from
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
- Extension/vector detection deferred to its own chapter (e.g. pgvector/paradedb "not enabled").
- Deferred adapters (spanner, arrow_odbc, mssql_python) until their cases move from
  `DEFERRED_DRIVER_CASES` to active rows.

## Adding A Case

1. Add or update the `DriverCase` in `_cases.py` (fixture name, dialect, mode, marks, capability flags).
2. Keep unsupported or unwired adapters in `DEFERRED_DRIVER_CASES` with a concrete `reason`.
3. Add fixture setup in `conftest.py` only when the adapter needs contract-owned table setup, and a
   `lifecycle_config_<adapter>` factory if it opts into any lifecycle flag.
4. Put reusable assertions in `behaviors.py`; keep central test bodies thin.
5. Add input data to `_inputs.py` or schema metadata to `_schema.py` instead of branching on adapter
   names inside tests.
6. When deleting a per-adapter test, confirm its behavior maps to a wired, gated contract (or an
   `extra_assertions` proof) so no coverage is silently dropped.

## Running

```bash
# Local-only adapters (no containers): SQLite, aiosqlite, DuckDB, ADBC-sqlite/duckdb
uv run pytest tests/integration/adapters/contracts -k "sqlite or duckdb"

# Full matrix (auto-starts postgres/mysql/oracle containers via pytest-databases)
uv run pytest tests/integration/adapters/contracts
```

Service-backed adapters keep their opt-in marks and xdist groups when they move from deferred to
active rows. BigQuery integration runs in CI by default; local runs require
`SQLSPEC_ENABLE_BIGQUERY_TESTS=1` and `--run-bigquery-tests`.
