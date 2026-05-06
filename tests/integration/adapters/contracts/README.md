# Adapter Contract Consolidation Manifest

This directory contains cross-adapter integration contracts created by the test-suite consolidation work. Contract files are used only when the behavior is genuinely shared and the collected case id can still name the adapter and capability under test. Dialect-specific behavior stays in adapter-local files.

## Broad Manifest Status

The C5 broad adapter manifest was reviewed against the live tree after the MySQL async consolidation pass.

| Family | Status | Rationale |
|---|---|---|
| `test_parameter_styles.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_parameter_styles_mysql_async.py`. Remaining files encode different dialect targets, native placeholder syntaxes, or class-based sync/async driver splits. |
| `test_store.py` | Partially consolidated | `aiomysql` and `asyncmy` ADK store coverage moved to `test_adk_store_mysql_async.py`; their Litestar store coverage moved to `test_litestar_store_mysql_async.py`. Remaining store files stay local because they differ by extension family, table lifecycle, owner-id behavior, and framework storage contracts. |
| `test_exceptions.py` | Consolidated in C6 | SQLite, MySQL, Postgres-family, DuckDB, ADBC, Oracle, Spanner, and skipped BigQuery emulator-gated exception mapping now live in `test_exceptions.py`. The contract keeps Spanner read/write semantics, ADBC Arrow error mapping, DuckDB catalog errors, and the deliberate BigQuery skip state as named cases. |
| `test_driver.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_driver_mysql_async.py`. Remaining drivers have distinct sync/async lifecycles, native SQL features, cloud behavior, or dialect-specific lock semantics. |
| `test_explain.py` | Consolidated in C6 | SQLite, aiosqlite, MySQL async, Postgres-family, DuckDB, Oracle, and the existing skipped ADBC/BigQuery/Spanner cases now share `test_explain.py`. Dialect-specific statement shapes, plan options, and skip reasons are case data. |
| `test_arrow.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_arrow_mysql_async.py`. Remaining files cover different Arrow backends, optional dependencies, and native export/import capability surfaces. |
| `test_migrations.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_migrations_mysql_async.py`. Remaining migration files contain dialect DDL quirks, transactional behavior differences, or adapter-specific migration runner coverage. |
| `test_execute_many.py` | Kept local | The remaining execute-many files are dialect- and driver-family specific and are not a readable matrix without also moving their driver fixtures. |
| `test_config.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_config_mysql_async.py`. Remaining integration config tests cover unrelated adapter surfaces such as BigQuery, mysql-connector sync/async flags, and PyMySQL sync config. |
| `test_features.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_features_mysql_async.py`. Remaining files represent real capability differences rather than a common feature contract. |
| `test_owner_id_column.py` | Kept local | Standalone owner-id files are ADK store lifecycle tests with adapter-specific setup and foreign-key behavior. The MySQL async owner-id cases that were embedded in store files moved with `test_adk_store_mysql_async.py`. |

## Storage and Event Holdouts

The C3 storage/event pass now has explicit holdouts for the files that remain local:

| Family | Status | Rationale |
|---|---|---|
| `test_storage_bridge.py` | Consolidated in C6 | Storage-capable adapters now share `test_storage_bridge.py`. The contract covers SQLite, ADBC/Postgres, asyncpg, DuckDB, psqlpy, psycopg sync/async, and MySQL async while preserving per-adapter marks, local storage alias setup, sync/async cleanup, and optional FSSPEC/PyArrow skips as case data. MinIO/S3 backend behavior stays in the storage integration suite rather than the adapter matrix. |
| `extensions/events/test_queue_backend.py` | Partially consolidated | SQLite-family coverage lives in `test_event_queue_sqlite.py`; MySQL-family queue fallback coverage lives in `test_event_queue_mysql.py`. Native LISTEN/NOTIFY, Oracle AQ, Spanner DDL, DuckDB queue DDL, and Postgres-family table-queue variants stay local because backend name, migration setup, and lifecycle assertions diverge. |
| `extensions/events/test_listen_notify.py` | Kept local | Native notification behavior has timing and grouping concerns that need adapter-local isolation. |
| `extensions/events/test_oracle_aq.py` | Kept local | Oracle AQ is a dialect-specific backend, not a generic table-queue contract. |

## Framework Scenario Holdouts

The C4 framework pass moved shared `disable_di` behavior into `tests/integration/extensions/contracts/test_disable_di.py`. The broader framework session scenarios remain local:

| Family | Status | Rationale |
|---|---|---|
| `tests/integration/extensions/{fastapi,flask,sanic,starlette}/test_integration.py` | C6 follow-up target | These files still cover common nouns such as manual commit, autocommit, rollback, request/session caching, default session keys, and multi-database access. C6 treats them as future contract-harness work instead of final holdouts; framework-specific app/client plumbing should become case adapters when the next framework slice runs. |
| `tests/integration/extensions/sanic/test_integration.py::test_sanic_disable_di_preserves_pool_lifecycle` | Kept local | The cross-framework `disable_di` contract covers injection behavior. This Sanic case is app.ctx pool lifecycle coverage and should stay beside the Sanic integration tests. |
| Serializer parity scenarios | Deferred to active Flow | Non-Litestar serializer parity is still owned by `aa-serializer-parity` C3, so this consolidation branch does not absorb those tests. |

## Moved MySQL Async Families

These old paths were replaced by adapter-parameterized contract files. Collection count is preserved: the touched MySQL async slice collected 244 items before and 244 items after the move.

| Old paths | New path |
|---|---|
| `tests/integration/adapters/aiomysql/test_parameter_styles.py`, `tests/integration/adapters/asyncmy/test_parameter_styles.py` | `tests/integration/adapters/contracts/test_parameter_styles_mysql_async.py` |
| `tests/integration/adapters/aiomysql/test_arrow.py`, `tests/integration/adapters/asyncmy/test_arrow.py` | `tests/integration/adapters/contracts/test_arrow_mysql_async.py` |
| `tests/integration/adapters/aiomysql/test_explain.py`, `tests/integration/adapters/asyncmy/test_explain.py` | `tests/integration/adapters/contracts/test_explain.py` |
| `tests/integration/adapters/aiomysql/test_features.py`, `tests/integration/adapters/asyncmy/test_features.py` | `tests/integration/adapters/contracts/test_features_mysql_async.py` |
| `tests/integration/adapters/aiomysql/test_driver.py`, `tests/integration/adapters/asyncmy/test_driver.py` | `tests/integration/adapters/contracts/test_driver_mysql_async.py` |
| `tests/integration/adapters/aiomysql/test_migrations.py`, `tests/integration/adapters/asyncmy/test_migrations.py` | `tests/integration/adapters/contracts/test_migrations_mysql_async.py` |
| `tests/integration/adapters/aiomysql/test_config.py`, `tests/integration/adapters/asyncmy/test_config.py` | `tests/integration/adapters/contracts/test_config_mysql_async.py` |
| `tests/integration/adapters/aiomysql/test_storage_bridge.py`, `tests/integration/adapters/asyncmy/test_storage_bridge.py` | `tests/integration/adapters/contracts/test_storage_bridge.py` |
| `tests/integration/adapters/aiomysql/extensions/adk/test_store.py`, `tests/integration/adapters/asyncmy/extensions/adk/test_store.py` | `tests/integration/adapters/contracts/test_adk_store_mysql_async.py` |
| `tests/integration/adapters/aiomysql/extensions/litestar/test_store.py`, `tests/integration/adapters/asyncmy/extensions/litestar/test_store.py` | `tests/integration/adapters/contracts/test_litestar_store_mysql_async.py` |

Adapter integration collection stayed stable for the broad MySQL async pass: 200 files / 2099 items before the pass, 193 files / 2099 items after it.

The follow-up MySQL async extension-store pass preserved the 64 touched store cases while reducing four adapter-local files to two contract files. Adapter integration collection stayed stable at 193 files / 2099 items before the store pass and 191 files / 2099 items after it.

The C6 scope revision replaced the storage bridge holdout model with an every-adapter capability contract. Adapter integration collection stayed stable at 2,099 items while collected adapter files dropped from 190 to 184 for this pass.

The C6 EXPLAIN pass replaced ten adapter-local EXPLAIN files plus the MySQL async EXPLAIN contract with `tests/integration/adapters/contracts/test_explain.py`. Adapter integration collection stayed stable at 2,104 items while collected adapter files dropped from 175 to 165 for this pass.
