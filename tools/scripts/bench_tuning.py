"""Manual adapter tuning benchmarks for cache and fetch controls.

The scenarios in this script are intentionally conditional: each one requires a
real service and is skipped unless its documented environment variables are set.

Examples:
    uv run python tools/scripts/bench_tuning.py --list
    SQLSPEC_BENCH_ASYNCPG_DSN=postgresql://... uv run python tools/scripts/bench_tuning.py --scenario asyncpg_stmt_cache
    SQLSPEC_BENCH_ORACLE_DSN=... SQLSPEC_BENCH_ORACLE_USER=... SQLSPEC_BENCH_ORACLE_PASSWORD=... \
        uv run python tools/scripts/bench_tuning.py --scenario oracle_stmtcache
    SQLSPEC_BENCH_ARROW_ODBC_CONNECTION_STRING='Driver=...;Server=...' \
        uv run python tools/scripts/bench_tuning.py --scenario arrow_odbc_fetch
"""

import argparse
import asyncio
import json
import os
import statistics
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass

import click

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig

DEFAULT_ITERATIONS = 15
DEFAULT_WARMUP = 3


@dataclass(frozen=True)
class BenchmarkOptions:
    """Runtime options shared by tuning benchmarks."""

    iterations: int
    warmup: int


@dataclass(frozen=True)
class BenchmarkResult:
    """Timing result for one scenario variant."""

    scenario: str
    variant: str
    iterations: int
    median_seconds: float | None
    min_seconds: float | None
    skipped: bool
    message: str = ""


@dataclass(frozen=True)
class Scenario:
    """Description and runner for a conditional tuning scenario."""

    name: str
    description: str
    requires: str
    skip_message: str
    runner: Callable[[BenchmarkOptions], list[BenchmarkResult]]


def _summarize(scenario: str, variant: str, samples: list[float]) -> BenchmarkResult:
    return BenchmarkResult(
        scenario=scenario,
        variant=variant,
        iterations=len(samples),
        median_seconds=statistics.median(samples),
        min_seconds=min(samples),
        skipped=False,
    )


def _skipped(scenario: str, variant: str, options: BenchmarkOptions, message: str) -> BenchmarkResult:
    return BenchmarkResult(
        scenario=scenario,
        variant=variant,
        iterations=options.iterations,
        median_seconds=None,
        min_seconds=None,
        skipped=True,
        message=message,
    )


def _measure_sync(call: Callable[[], object], options: BenchmarkOptions) -> list[float]:
    for _ in range(options.warmup):
        call()
    samples: list[float] = []
    for _ in range(options.iterations):
        started = time.perf_counter()
        call()
        samples.append(time.perf_counter() - started)
    return samples


async def _measure_async(call: Callable[[], object], options: BenchmarkOptions) -> list[float]:
    for _ in range(options.warmup):
        result = call()
        if hasattr(result, "__await__"):
            await result
    samples: list[float] = []
    for _ in range(options.iterations):
        started = time.perf_counter()
        result = call()
        if hasattr(result, "__await__"):
            await result
        samples.append(time.perf_counter() - started)
    return samples


async def _run_asyncpg_variant(dsn: str, statement_cache_size: int, options: BenchmarkOptions) -> BenchmarkResult:
    import asyncpg

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=1, statement_cache_size=statement_cache_size)
    try:
        async with pool.acquire() as conn:
            await conn.execute("DROP TABLE IF EXISTS sqlspec_bench_tuning_asyncpg")
            await conn.execute("CREATE TEMP TABLE sqlspec_bench_tuning_asyncpg (id int PRIMARY KEY, value text)")
            await conn.executemany(
                "INSERT INTO sqlspec_bench_tuning_asyncpg (id, value) VALUES ($1, $2)",
                [(idx, f"value-{idx}") for idx in range(100)],
            )

            async def fetch_one() -> None:
                await conn.fetchval("SELECT value FROM sqlspec_bench_tuning_asyncpg WHERE id = $1", 42)

            samples = await _measure_async(fetch_one, options)
    finally:
        await pool.close()

    return _summarize("asyncpg_stmt_cache", f"statement_cache_size={statement_cache_size}", samples)


def run_asyncpg_stmt_cache(options: BenchmarkOptions) -> list[BenchmarkResult]:
    """Benchmark asyncpg native statement cache enabled vs disabled."""
    dsn = os.getenv("SQLSPEC_BENCH_ASYNCPG_DSN")
    if not dsn:
        return [
            _skipped(
                "asyncpg_stmt_cache",
                "statement_cache_size=100",
                options,
                "set SQLSPEC_BENCH_ASYNCPG_DSN to run asyncpg statement-cache tuning",
            )
        ]

    async def run() -> list[BenchmarkResult]:
        enabled = await _run_asyncpg_variant(dsn, 100, options)
        disabled = await _run_asyncpg_variant(dsn, 0, options)
        return [enabled, disabled]

    return asyncio.run(run())


def _run_oracle_variant(stmtcachesize: int, options: BenchmarkOptions) -> BenchmarkResult:
    import oracledb

    dsn = os.environ["SQLSPEC_BENCH_ORACLE_DSN"]
    user = os.environ["SQLSPEC_BENCH_ORACLE_USER"]
    password = os.environ["SQLSPEC_BENCH_ORACLE_PASSWORD"]
    connection = oracledb.connect(user=user, password=password, dsn=dsn, stmtcachesize=stmtcachesize)
    try:
        with connection.cursor() as cursor:
            samples = _measure_sync(lambda: cursor.execute("SELECT :value FROM dual", value=42).fetchone(), options)
    finally:
        connection.close()
    return _summarize("oracle_stmtcache", f"stmtcachesize={stmtcachesize}", samples)


def run_oracle_stmtcache(options: BenchmarkOptions) -> list[BenchmarkResult]:
    """Benchmark python-oracledb statement cache enabled vs disabled."""
    required = ("SQLSPEC_BENCH_ORACLE_DSN", "SQLSPEC_BENCH_ORACLE_USER", "SQLSPEC_BENCH_ORACLE_PASSWORD")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        return [
            _skipped(
                "oracle_stmtcache",
                "stmtcachesize=20",
                options,
                f"set {', '.join(required)} to run Oracle statement-cache tuning",
            )
        ]
    return [_run_oracle_variant(20, options), _run_oracle_variant(0, options)]


def _run_arrow_odbc_variant(chunk_size: int, options: BenchmarkOptions) -> BenchmarkResult:
    connection_string = os.environ["SQLSPEC_BENCH_ARROW_ODBC_CONNECTION_STRING"]
    query = os.getenv("SQLSPEC_BENCH_ARROW_ODBC_QUERY", "SELECT 1 AS value")
    config = ArrowOdbcConfig(
        connection_config={"connection_string": connection_string}, driver_features={"chunk_size": chunk_size}
    )
    with config.provide_session() as session:
        samples = _measure_sync(lambda: session.select_to_arrow(query, return_format="table"), options)
    return _summarize("arrow_odbc_fetch", f"chunk_size={chunk_size}", samples)


def run_arrow_odbc_fetch(options: BenchmarkOptions) -> list[BenchmarkResult]:
    """Benchmark arrow-odbc fetch chunk size on a user-supplied query."""
    if not os.getenv("SQLSPEC_BENCH_ARROW_ODBC_CONNECTION_STRING"):
        return [
            _skipped(
                "arrow_odbc_fetch",
                "chunk_size=65536",
                options,
                "set SQLSPEC_BENCH_ARROW_ODBC_CONNECTION_STRING to run arrow-odbc fetch tuning",
            )
        ]
    return [_run_arrow_odbc_variant(65_536, options), _run_arrow_odbc_variant(8_192, options)]


SCENARIOS: dict[str, Scenario] = {
    "asyncpg_stmt_cache": Scenario(
        name="asyncpg_stmt_cache",
        description="Compare asyncpg native statement cache enabled vs disabled on one pooled connection.",
        requires="SQLSPEC_BENCH_ASYNCPG_DSN",
        skip_message="Skipped unless SQLSPEC_BENCH_ASYNCPG_DSN points at a PostgreSQL database.",
        runner=run_asyncpg_stmt_cache,
    ),
    "oracle_stmtcache": Scenario(
        name="oracle_stmtcache",
        description="Compare python-oracledb stmtcachesize enabled vs disabled.",
        requires="SQLSPEC_BENCH_ORACLE_DSN, SQLSPEC_BENCH_ORACLE_USER, SQLSPEC_BENCH_ORACLE_PASSWORD",
        skip_message="Skipped unless Oracle connection environment variables are set.",
        runner=run_oracle_stmtcache,
    ),
    "arrow_odbc_fetch": Scenario(
        name="arrow_odbc_fetch",
        description="Compare arrow-odbc fetch chunk sizes for a user-supplied Arrow query.",
        requires="SQLSPEC_BENCH_ARROW_ODBC_CONNECTION_STRING; optional SQLSPEC_BENCH_ARROW_ODBC_QUERY",
        skip_message="Skipped unless an arrow-odbc connection string is set.",
        runner=run_arrow_odbc_fetch,
    ),
}


def build_parser() -> argparse.ArgumentParser:
    """Build the command line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    parser.add_argument("--scenario", action="append", choices=sorted(SCENARIOS), help="Scenario to run")
    parser.add_argument("--list", action="store_true", help="List scenarios and required environment")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    return parser


def _print_scenarios() -> None:
    click.echo("scenario             requires")
    for scenario in SCENARIOS.values():
        click.echo(f"{scenario.name:<20} {scenario.requires}")
        click.echo(f"  {scenario.description}")


def _print_results(results: list[BenchmarkResult]) -> None:
    click.echo("scenario             variant                    median_s   min_s      status")
    for result in results:
        if result.skipped:
            click.echo(f"{result.scenario:<20} {result.variant:<26} {'-':>8} {'-':>8} skipped: {result.message}")
            continue
        click.echo(
            f"{result.scenario:<20} {result.variant:<26} {result.median_seconds:>8.6f} {result.min_seconds:>8.6f} ok"
        )


def main() -> None:
    """Run selected tuning benchmark scenarios."""
    parser = build_parser()
    args = parser.parse_args()
    if args.list:
        _print_scenarios()
        return

    options = BenchmarkOptions(iterations=args.iterations, warmup=args.warmup)
    selected = args.scenario or list(SCENARIOS)
    results: list[BenchmarkResult] = []
    for name in selected:
        results.extend(SCENARIOS[name].runner(options))

    if args.json:
        click.echo(json.dumps([asdict(result) for result in results], indent=2, sort_keys=True))
        return

    _print_results(results)


if __name__ == "__main__":
    main()
