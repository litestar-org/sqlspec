# Adapter Contract Consolidation Manifest

This package contains adapter integration tests for behavior that is shared
across database backends. Case ids name the adapter and the capability under
test; adapter marks and service grouping stay attached to each case.

## Consolidated Families

| Family | Contract file | Coverage boundary |
|---|---|---|
| Storage bridge | `test_storage_bridge.py` | Local Arrow/storage load cases plus local-alias export/import round trips across SQLite, MySQL async, ADBC/Postgres, asyncpg, DuckDB, psqlpy, and psycopg. MinIO/RustFS/S3 behavior remains in storage integration tests. |
| Exception mapping | `test_exceptions.py` | Constraint, syntax, not-found, ADBC Arrow, DuckDB catalog, Oracle, Spanner, and skipped BigQuery emulator-gated cases. |
| EXPLAIN | `test_explain.py` | Shared EXPLAIN statement shapes across SQLite, MySQL async, Postgres-family, DuckDB, Oracle, and documented ADBC/BigQuery/Spanner skip cases. |
| Migrations | `test_migrations.py` | SQL/Python migration apply, downgrade, current version, failure handling, transactions, and config convenience methods across file-backed, MySQL, Postgres-family, DuckDB, and Oracle adapters. |
| Table event queue | `test_event_queue.py` | Publish, consume, and ack through table-backed event queues across SQLite, MySQL, DuckDB, psqlpy, psycopg, Oracle, and Spanner. Native LISTEN/NOTIFY and Oracle AQ remain local. |
| Arrow | `test_arrow.py` | Basic `select_to_arrow`, table format, parameter filtering, and empty result behavior across common Arrow-capable adapters, with BigQuery preserved as an emulator-gated skip. |
| Parameter styles | `test_parameter_styles.py` | Native placeholder execution for qmark, numeric, pyformat, Oracle colon, Spanner `@name`, and BigQuery skipped `@name` cases. Adapter-local edge-case files remain for type coercion, `None`, reuse, count validation, and dialect functions. |
| Driver basics | `test_driver_basics.py` | Shared `execute_many`, one-row reads, multi-row reads via `execute`, scalar reads, no-row reads, and large-result materialization. Adapter-local driver files remain for transactions, lock syntax, hooks, SQL objects, statement stacks, native data types, and dialect features. |

## Intentional Local Coverage

These local files are not generic holdouts:

- `extensions/events/test_listen_notify.py` files cover native notification backends and timing-sensitive behavior.
- `oracledb/extensions/events/test_oracle_aq.py` covers Oracle AQ, not the table queue fallback.
- Adapter-local `test_arrow.py`, `test_parameter_styles.py`, and driver files retain dialect-specific depth beyond the broad contracts.
- Adapter extension-store and framework-store files remain local unless their setup and lifecycle semantics are identical across adapters.
