# Adapter Contract Consolidation Manifest

This directory contains cross-adapter integration contracts created by the test-suite consolidation work. Contract files are used only when the behavior is genuinely shared and the collected case id can still name the adapter and capability under test. Dialect-specific behavior stays in adapter-local files.

## Broad Manifest Status

The C5 broad adapter manifest was reviewed against the live tree after the MySQL async consolidation pass.

| Family | Status | Rationale |
|---|---|---|
| `test_parameter_styles.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_parameter_styles_mysql_async.py`. Remaining files encode different dialect targets, native placeholder syntaxes, or class-based sync/async driver splits. |
| `test_store.py` | Kept local | ADK and Litestar stores share names but differ by extension family, table lifecycle, owner-id behavior, and framework storage contracts. Existing local files keep extension-specific fixtures readable. |
| `test_exceptions.py` | Partially consolidated | SQLite and MySQL shared exception cases live in `test_exceptions_sqlite.py` and `test_exceptions_mysql.py`; remaining files validate adapter-specific exception mapping and dialect behavior. |
| `test_driver.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_driver_mysql_async.py`. Remaining drivers have distinct sync/async lifecycles, native SQL features, cloud behavior, or dialect-specific lock semantics. |
| `test_explain.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_explain_mysql_async.py`. Remaining files model different explain syntax and plan shapes. |
| `test_arrow.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_arrow_mysql_async.py`. Remaining files cover different Arrow backends, optional dependencies, and native export/import capability surfaces. |
| `test_migrations.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_migrations_mysql_async.py`. Remaining migration files contain dialect DDL quirks, transactional behavior differences, or adapter-specific migration runner coverage. |
| `test_execute_many.py` | Kept local | The remaining execute-many files are dialect- and driver-family specific and are not a readable matrix without also moving their driver fixtures. |
| `test_config.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_config_mysql_async.py`. Remaining integration config tests cover unrelated adapter surfaces such as BigQuery, mysql-connector sync/async flags, and PyMySQL sync config. |
| `test_features.py` | Partially consolidated | `aiomysql` and `asyncmy` moved to `test_features_mysql_async.py`. Remaining files represent real capability differences rather than a common feature contract. |
| `test_owner_id_column.py` | Kept local | Owner-id tests are ADK store lifecycle tests with adapter-specific setup and foreign-key behavior. They remain adjacent to the store fixtures. |

## Moved MySQL Async Families

These old paths were replaced by adapter-parameterized contract files. Collection count is preserved: the touched MySQL async slice collected 244 items before and 244 items after the move.

| Old paths | New path |
|---|---|
| `tests/integration/adapters/aiomysql/test_parameter_styles.py`, `tests/integration/adapters/asyncmy/test_parameter_styles.py` | `tests/integration/adapters/contracts/test_parameter_styles_mysql_async.py` |
| `tests/integration/adapters/aiomysql/test_arrow.py`, `tests/integration/adapters/asyncmy/test_arrow.py` | `tests/integration/adapters/contracts/test_arrow_mysql_async.py` |
| `tests/integration/adapters/aiomysql/test_explain.py`, `tests/integration/adapters/asyncmy/test_explain.py` | `tests/integration/adapters/contracts/test_explain_mysql_async.py` |
| `tests/integration/adapters/aiomysql/test_features.py`, `tests/integration/adapters/asyncmy/test_features.py` | `tests/integration/adapters/contracts/test_features_mysql_async.py` |
| `tests/integration/adapters/aiomysql/test_driver.py`, `tests/integration/adapters/asyncmy/test_driver.py` | `tests/integration/adapters/contracts/test_driver_mysql_async.py` |
| `tests/integration/adapters/aiomysql/test_migrations.py`, `tests/integration/adapters/asyncmy/test_migrations.py` | `tests/integration/adapters/contracts/test_migrations_mysql_async.py` |
| `tests/integration/adapters/aiomysql/test_config.py`, `tests/integration/adapters/asyncmy/test_config.py` | `tests/integration/adapters/contracts/test_config_mysql_async.py` |

Adapter integration collection stayed stable for the touched branch state: 200 files / 2099 items before this pass, 193 files / 2099 items after this pass.
