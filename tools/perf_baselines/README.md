# Adapter benchmark baselines

These JSON files are produced by `tools/scripts/bench.py --extended` and are
compared by the opt-in `make bench-gate-adapters` command. They are development
and management snapshots, not an enabled CI gate: service and host conditions
can produce materially different timings. Re-capture a baseline only after a
deliberate performance review and run `bench_compare.py` against the previous
file first.

The checked-in snapshots are interpreted/uncompiled SQLSpec runs. Each file
records `metadata.mypyc_compiled: false`; no compiled-performance claim is made
by these baselines.

The snapshots use `pool_size: 1` so pooled SQLSpec paths are compared with the
single raw connection used by the matching scenario. Use `--pool-size` only
when deliberately measuring a different pool topology.

The current inventory is:

- `sqlite.json`, `aiosqlite.json`, and `duckdb.json` for local SQLite-family
  paths
- `postgres.json`, `psycopg_sync.json`, `psycopg_async.json`, and `asyncpg.json`
  for PostgreSQL
- `cockroach.json`, `cockroach_psycopg_sync.json`,
  `cockroach_psycopg_async.json`, and `cockroach_asyncpg.json` for CockroachDB
- `mysql.json` and `mysqlconnector.json` for MySQL Connector/Python
- `oracle.json` for the Oracle sync and async LOB paths
- `adbc.json`, `spanner.json`, and `duckdb.json` for the other registered C8
  scenarios

The service-backed scenarios use these environment variables:

- `SQLSPEC_BENCH_SPANNER_PROJECT`, `SQLSPEC_BENCH_SPANNER_INSTANCE_ID`,
  `SQLSPEC_BENCH_SPANNER_DATABASE_ID`, and `SQLSPEC_BENCH_SPANNER_API_ENDPOINT`
- `SQLSPEC_BENCH_MYSQL_HOST`, `SQLSPEC_BENCH_MYSQL_PORT`,
  `SQLSPEC_BENCH_MYSQL_USER`, `SQLSPEC_BENCH_MYSQL_PASSWORD`, and
  `SQLSPEC_BENCH_MYSQL_DATABASE`
- `SQLSPEC_BENCH_ADBC_URI`, `SQLSPEC_BENCH_ADBC_DRIVER_NAME`, and optional
  ADBC username, password, backend, and TLS variables
- `SQLSPEC_BENCH_POSTGRES_DSN` for PostgreSQL psycopg and asyncpg runs
- `SQLSPEC_BENCH_COCKROACH_DSN` for CockroachDB psycopg and asyncpg runs
- `SQLSPEC_BENCH_ORACLE_HOST`, `SQLSPEC_BENCH_ORACLE_PORT`,
  `SQLSPEC_BENCH_ORACLE_SERVICE_NAME`, `SQLSPEC_BENCH_ORACLE_USER`, and
  `SQLSPEC_BENCH_ORACLE_PASSWORD` for Oracle LOB runs

The Spanner baseline expects `sqlspec_bench_strings` to contain 1,000 rows.
The MySQL and ADBC scenarios create their benchmark tables during the run.
