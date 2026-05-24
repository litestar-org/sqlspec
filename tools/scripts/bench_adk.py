"""ADK micro-benchmark scenarios — chat_loop, list_replay, struct_scan.

This script exercises representative ADK service workloads against any
configured backend so latency-optimized variations (Chapter 16) can be
validated end-to-end. Each scenario produces ``mean / p50 / p95 / p99`` timing
output and exits non-zero when any provided ``--gate`` is breached.

Usage
-----

.. code-block:: console

   uv run --extra adk python tools/scripts/bench_adk.py \\
       --backend asyncpg \\
       --scenario chat_loop \\
       --iterations 200

Scenarios
---------

``chat_loop``
    Steady-state agent conversation. Loops ``create_session → append_event x N
    → get_session``, measuring the steady-state round-trip latency.

``list_replay``
    Analytics-replica path. Creates many sessions, then exercises
    ``list_sessions`` and ``get_events`` to reflect a replay workload.

``struct_scan``
    DuckDB-only. Exercises the V6 STRUCT-typed events optimization once it
    lands.

Backends
--------

Any registered SQLSpec ADK adapter is acceptable. Selecting BigQuery is
strongly discouraged — BigQuery is documented as the analytics-replica path
and produces noisy latency numbers that mask the variation you are measuring.
"""

import argparse
import sys


def main() -> int:
    """Entry point for the ADK benchmark harness.

    This stub records the harness contract so per-driver chapters can plug into
    a stable invocation surface. Scenario implementations land alongside the
    Chapter 16 variation work tracked in ``sqlspec-badb``.
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--backend",
        required=True,
        choices=(
            "adbc",
            "aiomysql",
            "aiosqlite",
            "asyncmy",
            "asyncpg",
            "bigquery",
            "cockroach_asyncpg",
            "cockroach_psycopg",
            "duckdb",
            "mysqlconnector",
            "oracledb",
            "psqlpy",
            "psycopg",
            "pymysql",
            "spanner",
            "sqlite",
        ),
        help="ADK adapter to exercise.",
    )
    parser.add_argument(
        "--scenario",
        required=True,
        choices=("chat_loop", "list_replay", "struct_scan"),
        help="Workload scenario.",
    )
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument(
        "--gate",
        action="append",
        default=[],
        metavar="STAT:THRESHOLD",
        help="Optional gate, e.g. p95:25ms. Non-zero exit when breached.",
    )

    args = parser.parse_args()

    print(  # noqa: T201
        f"bench_adk skeleton — backend={args.backend}, scenario={args.scenario},"
        f" iterations={args.iterations}, warmup={args.warmup}"
    )
    print("Scenarios are stubs pending the Chapter 16 implementation (sqlspec-badb).")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
