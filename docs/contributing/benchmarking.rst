=====================
Benchmarking and Gates
=====================

SQLSpec keeps performance checks tiered so pull requests stay fast while adapter regressions still have scheduled coverage.

Pull request smoke reports
==========================

Pull requests run ``tools/scripts/bench_gate.py`` against SQLite with a small row count. This job is report-only by default: threshold failures should be reviewed, rerun when noisy, and then attributed before thresholds are changed.

Use ``--fail-on-regression`` only for maintainer-triggered validation where current scheduled artifacts show the thresholds are stable enough for a non-zero exit.

Scheduled adapter matrix
========================

The scheduled benchmark workflow runs ``tools/scripts/bench.py --driver all`` with container-backed PostgreSQL, MySQL, and Oracle services. Cloud adapters are registered in the matrix but require explicit environment configuration, so missing emulator or project settings are treated as unavailable local surfaces rather than shared-core proof.

CockroachDB benchmark entries require ``SQLSPEC_COCKROACH_DSN``. They are skipped when that target is not configured so PostgreSQL results are not reported as CockroachDB coverage.

Release validation
==================

The release workflow validates the benchmark registry and emits a SQLite gate report. The scheduled workflow remains the source of truth for full adapter-matrix timing because it owns the database service setup and optional cloud credentials.

Threshold ownership
===================

Thresholds are owned by the SQLSpec maintainers. A threshold update needs current benchmark output, the affected driver or shared-core path, and a short reason for accepting the new value.

Attribution rules:

* Regressions across SQLite, DuckDB, ADBC SQLite, and multiple SQLSpec adapter libraries usually point to shared-core paths.
* Regressions isolated to one adapter family belong to that adapter's bind, cursor, pool, dialect, or driver-local type handling layer.
* Noisy PR smoke results should be rerun before changing code or thresholds.
* Scheduled and release runs are the source of truth for threshold changes.
