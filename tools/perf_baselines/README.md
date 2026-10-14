# Adapter benchmark baselines

These JSON files are produced by `tools/scripts/bench.py --extended` (and
`tools/scripts/bench_subsystems.py` for `subsystems.json`) and are compared by
the opt-in `make bench-gate-adapters` command. They are development and
management snapshots, not an enabled CI gate: service and host conditions can
produce materially different timings. Re-capture a baseline only after a
deliberate performance review and run `bench_compare.py` against the previous
file first.

The checked-in snapshots are interpreted/uncompiled SQLSpec runs. Each file
records `metadata.mypyc_compiled: false`, `metadata.warmup`, and host
`metadata.environment` details (`python_version`, `platform`, `cpu_count`,
`sqlglot_version`, and `driver_versions`); no compiled-performance claim is made
by these baselines.

The snapshots use `pool_size: 1` so pooled SQLSpec paths are compared with the
single raw connection used by the matching scenario. Use `--pool-size` only
when deliberately measuring a different pool topology.

## Gate execution and comparison modes

- **Automatic parameter propagation**: `make bench-gate-adapters` invokes
  `tools/scripts/bench_compare.py <baseline> --print-run-args` before running
  `tools/scripts/bench.py`, propagating `--rows`, `--iterations`, `--pool-size`,
  and `--warmup` directly from the baseline's `metadata` block so fresh runs
  always match the recorded baseline topology.
- **Metadata parity checks**: `bench_compare.py` validates `rows`, `pool_size`,
  `iterations`, `warmup`, and `mypyc_compiled` between baseline and current JSON
  files and exits with code `2` on any mismatch unless
  `--allow-metadata-mismatch` is passed.
- **Ratio comparison mode (`--mode ratio`)**: `make bench-gate-adapters`
  defaults to `BENCHMARK_MODE=ratio` (`--mode ratio`), which compares the
  within-run `sqlspec / raw` median ratio (`current_ratio <= baseline_ratio *
  (1 + max_regression_pct / 100)`) to insulate comparisons from host CPU
  frequency scaling. Scenarios without a matching `raw` entry in the same JSON
  file fall back to absolute median comparison (`abs-fallback`). Pass
  `BENCHMARK_MODE=absolute` (or `--mode absolute`) to compare absolute median
  timings across all scenarios.

## Baseline inventory

- `sqlite.json`, `aiosqlite.json`, and `duckdb.json` for local SQLite-family and
  DuckDB paths
- `subsystems.json` for pure-Python subsystem and stub-driver hot-path
  micro-benchmarks (`tools/scripts/bench_subsystems.py`)
- `postgres.json`, `psycopg_sync.json`, `psycopg_async.json`, `asyncpg.json`,
  and `psqlpy.json` for PostgreSQL
- `cockroach.json`, `cockroach_psycopg_sync.json`,
  `cockroach_psycopg_async.json`, and `cockroach_asyncpg.json` for CockroachDB
- `mysql.json` and `mysqlconnector.json` for MySQL Connector/Python
- `oracle.json` for the Oracle sync and async paths
- `adbc.json` and `spanner.json` for ADBC and Cloud Spanner paths

## Extended hot-path scenarios

In addition to core read/write, iterative insert, repeated query, dict-key, and
schema-mapping scenarios, `--extended` captures five hot-path scenarios across
local and service-backed drivers:

- `scalar_param_select`: repeated `SELECT` calls passing a bare scalar parameter
  (`int` or `str`) rather than a wrapping tuple or list
- `no_param_select`: repeated parameterless `SELECT` queries
- `complex_query`: repeated `WITH ... JOIN ...` CTE + multi-join `SELECT`
  queries exercising SQLGlot AST caching
- `session_per_request`: repeated `provide_session()` acquire/release cycles
  executing a query per session
- `observability_select`: repeated `SELECT` queries with statement observers
  enabled via `ObservabilityConfig`

## Service-backed environment variables

The service-backed scenarios use these environment variables:

- `SQLSPEC_BENCH_SPANNER_PROJECT`, `SQLSPEC_BENCH_SPANNER_INSTANCE_ID`,
  `SQLSPEC_BENCH_SPANNER_DATABASE_ID`, and `SQLSPEC_BENCH_SPANNER_API_ENDPOINT`
- `SQLSPEC_BENCH_MYSQL_HOST`, `SQLSPEC_BENCH_MYSQL_PORT`,
  `SQLSPEC_BENCH_MYSQL_USER`, `SQLSPEC_BENCH_MYSQL_PASSWORD`, and
  `SQLSPEC_BENCH_MYSQL_DATABASE`
- `SQLSPEC_BENCH_ADBC_URI`, `SQLSPEC_BENCH_ADBC_DRIVER_NAME`, and optional
  ADBC username, password, backend, and TLS variables
- `SQLSPEC_BENCH_POSTGRES_DSN` for PostgreSQL `psycopg`, `asyncpg`, and `psqlpy`
  runs
- `SQLSPEC_BENCH_COCKROACH_DSN` for CockroachDB `psycopg` and `asyncpg` runs
- `SQLSPEC_BENCH_ORACLE_HOST`, `SQLSPEC_BENCH_ORACLE_PORT`,
  `SQLSPEC_BENCH_ORACLE_SERVICE_NAME`, `SQLSPEC_BENCH_ORACLE_USER`, and
  `SQLSPEC_BENCH_ORACLE_PASSWORD` for Oracle sync and async runs

The Spanner baseline expects `sqlspec_bench_strings` to contain 1,000 rows.
The MySQL, PostgreSQL, Oracle, and ADBC scenarios create their benchmark tables
during the run.
