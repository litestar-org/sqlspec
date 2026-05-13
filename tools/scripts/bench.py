"""Benchmark script for comparing sqlspec vs raw drivers vs SQLAlchemy.

Originally contributed by euri10 (Benoit Barthelet) in PR #354.
"""

import asyncio
import cProfile
import gc
import inspect
import json
import os
import pstats
import sqlite3
import statistics
import tempfile
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import unquote, urlparse

import anyio
import click
from rich.console import Console
from rich.table import Table

from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.utils.schema import transform_dict_keys
from sqlspec.utils.text import camelize

# Pool leak detection helper
_leaked_pools: list[str] = []


def _is_compiled() -> bool:
    """Detect if sqlspec driver modules are mypyc-compiled."""
    try:
        from sqlspec.driver import _sync

        return hasattr(_sync, "__file__") and (_sync.__file__ or "").endswith(".so")
    except ImportError:
        return False


SQLSPEC_LABEL = "sqlspec (mypyc)" if _is_compiled() else "sqlspec"

__all__ = (
    "BENCHMARK_DRIVER_MATRIX",
    "CORE_SCENARIOS",
    "expand_driver_selection",
    "main",
    "print_benchmark_table",
    "raw_asyncpg_initialization",
    "raw_asyncpg_iterative_inserts",
    "raw_asyncpg_read_heavy",
    "raw_asyncpg_repeated_queries",
    "raw_asyncpg_write_heavy",
    "raw_duckdb_initialization",
    "raw_duckdb_iterative_inserts",
    "raw_duckdb_read_heavy",
    "raw_duckdb_repeated_queries",
    "raw_duckdb_write_heavy",
    "raw_sqlite_complex_parameters",
    "raw_sqlite_dict_key_transform",
    "raw_sqlite_initialization",
    "raw_sqlite_iterative_inserts",
    "raw_sqlite_read_heavy",
    "raw_sqlite_repeated_queries",
    "raw_sqlite_schema_mapping",
    "raw_sqlite_thin_path_stress",
    "raw_sqlite_write_heavy",
    "run_benchmark",
    "run_extended_benchmark",
    "sqlalchemy_asyncpg_initialization",
    "sqlalchemy_asyncpg_iterative_inserts",
    "sqlalchemy_asyncpg_read_heavy",
    "sqlalchemy_asyncpg_repeated_queries",
    "sqlalchemy_asyncpg_write_heavy",
    "sqlalchemy_duckdb_initialization",
    "sqlalchemy_duckdb_iterative_inserts",
    "sqlalchemy_duckdb_read_heavy",
    "sqlalchemy_duckdb_repeated_queries",
    "sqlalchemy_duckdb_write_heavy",
    "sqlalchemy_sqlite_initialization",
    "sqlalchemy_sqlite_iterative_inserts",
    "sqlalchemy_sqlite_read_heavy",
    "sqlalchemy_sqlite_repeated_queries",
    "sqlalchemy_sqlite_write_heavy",
    "sqlspec_asyncpg_initialization",
    "sqlspec_asyncpg_iterative_inserts",
    "sqlspec_asyncpg_read_heavy",
    "sqlspec_asyncpg_repeated_queries",
    "sqlspec_asyncpg_write_heavy",
    "sqlspec_duckdb_initialization",
    "sqlspec_duckdb_iterative_inserts",
    "sqlspec_duckdb_read_heavy",
    "sqlspec_duckdb_repeated_queries",
    "sqlspec_duckdb_write_heavy",
    "sqlspec_sqlite_complex_parameters",
    "sqlspec_sqlite_dict_key_transform",
    "sqlspec_sqlite_initialization",
    "sqlspec_sqlite_iterative_inserts",
    "sqlspec_sqlite_read_heavy",
    "sqlspec_sqlite_repeated_queries",
    "sqlspec_sqlite_schema_mapping",
    "sqlspec_sqlite_thin_path_stress",
    "sqlspec_sqlite_write_heavy",
)


ROWS_TO_INSERT = 10_000
POOL_SIZE = 5  # Default pool size for async adapters
DEFAULT_BENCH_ITERATIONS = 7
DEFAULT_BENCH_WARMUP = 3
NOISY_STDDEV_RATIO = 0.10
CORE_SCENARIOS = ("initialization", "write_heavy", "read_heavy", "iterative_inserts", "repeated_queries")


class BenchmarkDriverConfig(TypedDict):
    """Benchmark matrix entry for a selectable driver surface."""

    libraries: tuple[str, ...]
    scenarios: tuple[str, ...]
    tier: str


BENCHMARK_DRIVER_MATRIX: dict[str, BenchmarkDriverConfig] = {
    "sqlite": {"libraries": ("raw", "sqlspec", "sqlalchemy"), "scenarios": CORE_SCENARIOS, "tier": "pr"},
    "aiosqlite": {"libraries": ("raw", "sqlspec", "sqlalchemy"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "duckdb": {"libraries": ("raw", "sqlspec", "sqlalchemy"), "scenarios": CORE_SCENARIOS, "tier": "pr"},
    "asyncpg": {"libraries": ("raw", "sqlspec", "sqlalchemy"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "psycopg": {"libraries": ("raw", "sqlspec", "sqlalchemy"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "psycopg_async": {"libraries": ("raw", "sqlspec", "sqlalchemy"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "psqlpy": {"libraries": ("raw", "sqlspec"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "cockroach_asyncpg": {"libraries": ("raw", "sqlspec"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "cockroach_psycopg": {"libraries": ("raw", "sqlspec"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "cockroach_psycopg_async": {"libraries": ("raw", "sqlspec"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "aiomysql": {"libraries": ("raw", "sqlspec", "sqlalchemy"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "asyncmy": {"libraries": ("raw", "sqlspec", "sqlalchemy"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "pymysql": {"libraries": ("raw", "sqlspec", "sqlalchemy"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "mysqlconnector": {"libraries": ("raw", "sqlspec", "sqlalchemy"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "mysqlconnector_async": {"libraries": ("raw", "sqlspec"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "oracledb": {"libraries": ("raw", "sqlspec", "sqlalchemy"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "oracledb_async": {"libraries": ("raw", "sqlspec"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "adbc": {"libraries": ("raw", "sqlspec"), "scenarios": CORE_SCENARIOS, "tier": "scheduled"},
    "spanner": {"libraries": ("raw", "sqlspec"), "scenarios": CORE_SCENARIOS, "tier": "scheduled-cloud"},
    "bigquery": {"libraries": ("raw", "sqlspec"), "scenarios": CORE_SCENARIOS, "tier": "scheduled-cloud"},
}


class BenchmarkSkipError(RuntimeError):
    """Raised when a benchmark surface is registered but unavailable locally."""


def expand_driver_selection(drivers: Sequence[str]) -> tuple[str, ...]:
    """Expand CLI driver selectors, including the ``all`` alias."""
    expanded: list[str] = []
    for driver in drivers:
        if driver == "all":
            expanded.extend(BENCHMARK_DRIVER_MATRIX)
        else:
            expanded.append(driver)
    return tuple(dict.fromkeys(expanded))


@click.command()
@click.option(
    "--driver",
    multiple=True,
    default=["sqlite"],
    show_default=True,
    help="List of driver names to benchmark (default: sqlite)",
)
@click.option(
    "--rows", default=ROWS_TO_INSERT, show_default=True, help="Number of rows to insert/read in heavy scenarios"
)
@click.option(
    "--pool-size",
    default=POOL_SIZE,
    show_default=True,
    help="Connection pool size for async adapters (1=single connection, matches sync behavior)",
)
@click.option(
    "--iterations", default=DEFAULT_BENCH_ITERATIONS, show_default=True, help="Number of timed iterations per scenario"
)
@click.option(
    "--warmup", default=DEFAULT_BENCH_WARMUP, show_default=True, help="Number of warmup iterations (not timed)"
)
@click.option("--json-output", default=None, type=click.Path(), help="Write results to a JSON file")
@click.option("--extended/--no-extended", default=False, help="Include extended benchmark scenarios")
@click.option("--profile/--no-profile", default=False, help="Enable cProfile profiling for each scenario")
@click.option("--profile-scenario", default=None, help="Profile only a specific scenario name (e.g. iterative_inserts)")
def main(
    driver: tuple[str, ...],
    rows: int,
    pool_size: int,
    iterations: int,
    warmup: int,
    json_output: str | None,
    extended: bool,
    profile: bool,
    profile_scenario: str | None,
) -> None:
    """Run benchmarks for the specified drivers.

    Compares raw driver, sqlspec, and SQLAlchemy performance across
    initialization, write-heavy, and read-heavy scenarios.
    """
    global ROWS_TO_INSERT, POOL_SIZE
    ROWS_TO_INSERT = rows
    POOL_SIZE = pool_size

    # If --profile-scenario is given, implicitly enable profiling
    if profile_scenario:
        profile = True

    results: list[dict[str, Any]] = []
    errors: list[str] = []
    drivers = expand_driver_selection(driver)
    if _is_compiled():
        click.secho("mypyc compilation detected", fg="green")
    for drv in drivers:
        click.echo(
            f"Running benchmark for driver: {drv} "
            f"(rows={rows}, pool_size={pool_size}, iterations={iterations}, warmup={warmup})"
        )
        if profile:
            results.extend(
                run_benchmark_profiled(
                    drv, errors, iterations=iterations, warmup=warmup, profile_scenario=profile_scenario
                )
            )
        else:
            results.extend(run_benchmark(drv, errors, iterations=iterations, warmup=warmup))
        if extended:
            click.echo(f"Running extended benchmarks for driver: {drv}")
            results.extend(run_extended_benchmark(drv, errors, iterations=iterations, warmup=warmup))
    if results:
        print_benchmark_table(results)
    else:
        click.echo("No benchmark results to display.")
    if json_output:
        _write_json_results(results, json_output, rows=rows, pool_size=pool_size, iterations=iterations)
        click.secho(f"Results written to {json_output}", fg="green")
    if errors:
        for err in errors:
            if ": skipped (" in err:
                click.secho(f"Skipped: {err}", fg="yellow")
            else:
                click.secho(f"Error: {err}", fg="red")
    if _leaked_pools:
        click.secho("Pool leaks detected:", fg="yellow")
        for leak in _leaked_pools:
            click.secho(f"  - {leak}", fg="yellow")
        _leaked_pools.clear()
    click.echo(f"Benchmarks complete for drivers: {', '.join(drivers)}")


def _new_benchmark_loop() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


def _close_benchmark_loop(loop: asyncio.AbstractEventLoop) -> None:
    try:
        loop.run_until_complete(loop.shutdown_asyncgens())
        if hasattr(loop, "shutdown_default_executor"):
            loop.run_until_complete(loop.shutdown_default_executor())
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def _run_scenario_once(func: Any, loop: asyncio.AbstractEventLoop | None) -> None:
    if loop is not None:
        loop.run_until_complete(func())
    else:
        func()


def _run_benchmark_iterations(
    func: Any, *, is_async: bool, iterations: int, warmup: int, profiler: cProfile.Profile | None = None
) -> list[float]:
    loop = _new_benchmark_loop() if is_async else None
    try:
        for _ in range(warmup):
            _run_scenario_once(func, loop)

        times: list[float] = []
        for _ in range(iterations):
            gc.collect()
            gc_was_enabled = gc.isenabled()
            if gc_was_enabled:
                gc.disable()
            start = time.perf_counter()
            try:
                if profiler is not None:
                    profiler.enable()
                _run_scenario_once(func, loop)
            finally:
                if profiler is not None:
                    profiler.disable()
                if gc_was_enabled:
                    gc.enable()
            times.append(time.perf_counter() - start)
        return times
    finally:
        if loop is not None:
            _close_benchmark_loop(loop)


def _summarize_times(times: list[float]) -> dict[str, float | bool]:
    median_time = statistics.median(times)
    stddev = statistics.stdev(times) if len(times) > 1 else 0.0
    quartiles = statistics.quantiles(times, n=4, method="inclusive") if len(times) > 1 else (0.0, 0.0, 0.0)
    iqr = quartiles[2] - quartiles[0]
    noise_ratio = stddev / median_time if median_time > 0 else 0.0
    return {
        "time": median_time,
        "stddev": stddev,
        "iqr": iqr,
        "noise_ratio": noise_ratio,
        "noisy": noise_ratio > NOISY_STDDEV_RATIO,
    }


def run_benchmark(
    driver: str, errors: list[str], *, iterations: int = DEFAULT_BENCH_ITERATIONS, warmup: int = DEFAULT_BENCH_WARMUP
) -> list[dict[str, Any]]:
    """Run all benchmark scenarios for a driver.

    Args:
        driver: The database driver name (e.g., "sqlite", "asyncpg")
        errors: List to append error messages to
        iterations: Number of timed iterations per scenario
        warmup: Number of warmup iterations (not timed)

    Returns:
        List of benchmark result dictionaries
    """
    driver_config = BENCHMARK_DRIVER_MATRIX.get(driver)
    if driver_config is None:
        errors.append(f"Unknown benchmark driver: {driver}")
        return []
    libraries = driver_config["libraries"]
    scenarios = driver_config["scenarios"]
    results: list[dict[str, Any]] = []

    for scenario in scenarios:
        for lib in libraries:
            func = SCENARIO_REGISTRY.get((lib, driver, scenario))
            if func is None:
                errors.append(f"No implementation for library={lib}, driver={driver}, scenario={scenario}")
                continue

            is_async = inspect.iscoroutinefunction(func)

            try:
                times = _run_benchmark_iterations(func, is_async=is_async, iterations=iterations, warmup=warmup)
                stats = _summarize_times(times)
                label = SQLSPEC_LABEL if lib == "sqlspec" else lib
                results.append({"driver": driver, "library": label, "scenario": scenario, "times": times, **stats})
            except BenchmarkSkipError as exc:
                errors.append(f"{lib}/{driver}/{scenario}: skipped ({exc})")
            except Exception as exc:
                errors.append(f"{lib}/{driver}/{scenario}: {exc}")

    return results


def run_benchmark_profiled(
    driver: str,
    errors: list[str],
    *,
    iterations: int = DEFAULT_BENCH_ITERATIONS,
    warmup: int = DEFAULT_BENCH_WARMUP,
    profile_scenario: str | None = None,
) -> list[dict[str, Any]]:
    """Run benchmark scenarios with cProfile profiling enabled.

    Wraps each scenario execution in cProfile.Profile() and saves .prof files
    to tools/scripts/profiles/ for later analysis.

    Args:
        driver: The database driver name (e.g., "sqlite", "asyncpg")
        errors: List to append error messages to
        iterations: Number of timed iterations per scenario
        warmup: Number of warmup iterations (not timed)
        profile_scenario: If set, only profile this specific scenario name

    Returns:
        List of benchmark result dictionaries
    """
    profiles_dir = Path(__file__).parent / "profiles"
    profiles_dir.mkdir(exist_ok=True)

    driver_config = BENCHMARK_DRIVER_MATRIX.get(driver)
    if driver_config is None:
        errors.append(f"Unknown benchmark driver: {driver}")
        return []
    libraries = driver_config["libraries"]
    scenarios = driver_config["scenarios"]
    results: list[dict[str, Any]] = []
    console = Console()

    for scenario in scenarios:
        for lib in libraries:
            # If profiling a specific scenario, skip others
            if profile_scenario and scenario != profile_scenario:
                continue

            func = SCENARIO_REGISTRY.get((lib, driver, scenario))
            if func is None:
                errors.append(f"No implementation for library={lib}, driver={driver}, scenario={scenario}")
                continue

            is_async = inspect.iscoroutinefunction(func)
            label = SQLSPEC_LABEL if lib == "sqlspec" else lib
            prof_name = f"{driver}_{lib}_{scenario}"

            try:
                profiler = cProfile.Profile()
                times = _run_benchmark_iterations(
                    func, is_async=is_async, iterations=iterations, warmup=warmup, profiler=profiler
                )
                summary = _summarize_times(times)
                results.append({"driver": driver, "library": label, "scenario": scenario, "times": times, **summary})

                # Save profile data
                prof_path = profiles_dir / f"{prof_name}.prof"
                profiler.dump_stats(str(prof_path))
                click.echo(f"  Profile saved: {prof_path}")

                # Print top 20 summary
                console.print(f"\n  [bold cyan]Profile summary: {prof_name}[/bold cyan]")
                profile_stats = pstats.Stats(profiler)
                profile_stats.strip_dirs()
                profile_stats.sort_stats("cumulative")
                profile_stats.print_stats(20)

            except BenchmarkSkipError as exc:
                errors.append(f"{lib}/{driver}/{scenario}: skipped ({exc})")
            except Exception as exc:
                errors.append(f"{lib}/{driver}/{scenario}: {exc}")

    return results


def run_extended_benchmark(
    driver: str, errors: list[str], *, iterations: int = DEFAULT_BENCH_ITERATIONS, warmup: int = DEFAULT_BENCH_WARMUP
) -> list[dict[str, Any]]:
    """Run extended benchmark scenarios for a driver.

    Extended scenarios exercise sqlspec-specific features like dict key
    transformation, schema mapping, complex parameter handling, and
    thin execute path stress testing.

    Args:
        driver: The database driver name (e.g., "sqlite")
        errors: List to append error messages to
        iterations: Number of timed iterations per scenario
        warmup: Number of warmup iterations (not timed)

    Returns:
        List of benchmark result dictionaries
    """
    libraries = ["raw", "sqlspec"]
    scenarios = ["dict_key_transform", "schema_mapping", "complex_parameters", "thin_path_stress"]
    results: list[dict[str, Any]] = []

    for scenario in scenarios:
        for lib in libraries:
            func = SCENARIO_REGISTRY.get((lib, driver, scenario))
            if func is None:
                errors.append(f"No extended implementation for library={lib}, driver={driver}, scenario={scenario}")
                continue

            is_async = inspect.iscoroutinefunction(func)

            try:
                times = _run_benchmark_iterations(func, is_async=is_async, iterations=iterations, warmup=warmup)
                stats = _summarize_times(times)
                label = SQLSPEC_LABEL if lib == "sqlspec" else lib
                results.append({"driver": driver, "library": label, "scenario": scenario, "times": times, **stats})
            except Exception as exc:
                errors.append(f"{lib}/{driver}/{scenario}: {exc}")

    return results


def _write_json_results(
    results: list[dict[str, Any]], output_path: str, *, rows: int, pool_size: int, iterations: int
) -> None:
    """Write benchmark results to a JSON file.

    Args:
        results: List of benchmark result dictionaries
        output_path: Path to write the JSON file
        rows: Number of rows used in the benchmark
        pool_size: Pool size used in the benchmark
        iterations: Number of timed iterations per scenario
    """
    output = {
        "metadata": {
            "rows": rows,
            "pool_size": pool_size,
            "iterations": iterations,
            "mypyc_compiled": _is_compiled(),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        },
        "results": [
            {
                "driver": r["driver"],
                "library": r["library"],
                "scenario": r["scenario"],
                "time": r["time"],
                "times": r.get("times", [r["time"]]),
                "stddev": r.get("stddev", 0.0),
                "iqr": r.get("iqr", 0.0),
                "noise_ratio": r.get("noise_ratio", 0.0),
                "noisy": r.get("noisy", False),
            }
            for r in results
        ],
    }
    with Path(output_path).open("w") as f:
        json.dump(output, f, indent=2)


# --- Scenario helpers and registry ---
# SQLite implementations
# ------------------------------

CREATE_TEST_TABLE = "CREATE TABLE test (value TEXT);"
DROP_TEST_TABLE = "DROP TABLE IF EXISTS test;"
INSERT_TEST_VALUE = "INSERT INTO test (value) VALUES (?);"
INSERT_TEST_VALUE_ASYNCPG = "INSERT INTO test (value) VALUES ($1);"
SELECT_TEST_VALUES = "SELECT * FROM test;"
INSERT_TEST_VALUE_SQLA = "INSERT INTO test (value) VALUES (:value);"


def _optimize_raw_sqlite(conn: sqlite3.Connection) -> None:
    """Apply same PRAGMAs that sqlspec applies for fair comparison."""
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")


def raw_sqlite_initialization() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        _optimize_raw_sqlite(conn)
        conn.execute(CREATE_TEST_TABLE)
        conn.close()


def raw_sqlite_write_heavy() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        _optimize_raw_sqlite(conn)
        conn.execute(CREATE_TEST_TABLE)
        # Use executemany for fair comparison
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        conn.executemany(INSERT_TEST_VALUE, data)
        conn.commit()
        conn.close()


def raw_sqlite_read_heavy() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        _optimize_raw_sqlite(conn)
        conn.execute(CREATE_TEST_TABLE)
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        conn.executemany(INSERT_TEST_VALUE, data)
        conn.commit()
        cursor = conn.execute(SELECT_TEST_VALUES)
        rows = cursor.fetchall()
        assert len(rows) == ROWS_TO_INSERT
        conn.close()


def sqlspec_sqlite_initialization() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)


def sqlspec_sqlite_write_heavy() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            # Use execute_many for bulk inserts
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            session.execute_many(INSERT_TEST_VALUE, data)


def sqlspec_sqlite_read_heavy() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            session.execute_many(INSERT_TEST_VALUE, data)
            rows = session.fetch(SELECT_TEST_VALUES)
            assert len(rows) == ROWS_TO_INSERT


def _get_sqlalchemy() -> tuple[Any, Any]:
    """Import SQLAlchemy lazily."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        return None, None
    else:
        return create_engine, text


def sqlalchemy_sqlite_initialization() -> None:
    create_engine, text = _get_sqlalchemy()
    if create_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            conn.commit()


def sqlalchemy_sqlite_write_heavy() -> None:
    create_engine, text = _get_sqlalchemy()
    if create_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            # Use insert with bindparams for fair bulk comparison
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            conn.commit()


def sqlalchemy_sqlite_read_heavy() -> None:
    create_engine, text = _get_sqlalchemy()
    if create_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            conn.commit()
            result = conn.execute(text(SELECT_TEST_VALUES))
            rows = result.fetchall()
            assert len(rows) == ROWS_TO_INSERT


# DuckDB implementations
# DuckDB is sync like sqlite, but uses its own driver
# ------------------------------


def _get_duckdb() -> Any:
    """Import duckdb lazily."""
    try:
        import duckdb
    except ImportError:
        return None
    else:
        return duckdb


def raw_duckdb_initialization() -> None:
    duckdb = _get_duckdb()
    if duckdb is None:
        return
    # DuckDB needs to create the file itself - use temp name then delete
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()  # Delete so DuckDB can create fresh
    try:
        conn = duckdb.connect(str(tmp_path))
        conn.execute(CREATE_TEST_TABLE)
        conn.close()
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def raw_duckdb_write_heavy() -> None:
    duckdb = _get_duckdb()
    if duckdb is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        conn = duckdb.connect(str(tmp_path))
        conn.execute(CREATE_TEST_TABLE)
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        conn.executemany(INSERT_TEST_VALUE, data)
        conn.close()
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def raw_duckdb_read_heavy() -> None:
    duckdb = _get_duckdb()
    if duckdb is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        conn = duckdb.connect(str(tmp_path))
        conn.execute(CREATE_TEST_TABLE)
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        conn.executemany(INSERT_TEST_VALUE, data)
        rows = conn.execute(SELECT_TEST_VALUES).fetchall()
        assert len(rows) == ROWS_TO_INSERT
        conn.close()
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def raw_duckdb_iterative_inserts() -> None:
    """Individual inserts in a loop - shows per-call overhead."""
    duckdb = _get_duckdb()
    if duckdb is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        conn = duckdb.connect(str(tmp_path))
        conn.execute(CREATE_TEST_TABLE)
        for i in range(ROWS_TO_INSERT):
            conn.execute(INSERT_TEST_VALUE, (f"value_{i}",))
        conn.close()
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def raw_duckdb_repeated_queries() -> None:
    """Repeated single-row queries - tests query preparation overhead."""
    duckdb = _get_duckdb()
    if duckdb is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        conn = duckdb.connect(str(tmp_path))
        conn.execute(CREATE_TEST_TABLE)
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        conn.executemany(INSERT_TEST_VALUE, data)
        for i in range(ROWS_TO_INSERT):
            conn.execute(SELECT_BY_VALUE, (f"value_{i % 100}",)).fetchone()
        conn.close()
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def sqlspec_duckdb_initialization() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        spec = SQLSpec()
        config = DuckDBConfig(connection_config={"database": str(tmp_path)})
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def sqlspec_duckdb_write_heavy() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        spec = SQLSpec()
        config = DuckDBConfig(connection_config={"database": str(tmp_path)})
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            session.execute_many(INSERT_TEST_VALUE, data)
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def sqlspec_duckdb_read_heavy() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        spec = SQLSpec()
        config = DuckDBConfig(connection_config={"database": str(tmp_path)})
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            session.execute_many(INSERT_TEST_VALUE, data)
            rows = session.fetch(SELECT_TEST_VALUES)
            assert len(rows) == ROWS_TO_INSERT
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def sqlspec_duckdb_iterative_inserts() -> None:
    """Individual inserts in a loop - shows per-call overhead."""
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        spec = SQLSpec()
        config = DuckDBConfig(connection_config={"database": str(tmp_path)})
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            for i in range(ROWS_TO_INSERT):
                session.execute(INSERT_TEST_VALUE, (f"value_{i}",))
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def sqlspec_duckdb_repeated_queries() -> None:
    """Repeated single-row queries - tests query cache effectiveness."""
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        spec = SQLSpec()
        config = DuckDBConfig(connection_config={"database": str(tmp_path)})
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            session.execute_many(INSERT_TEST_VALUE, data)
            for i in range(ROWS_TO_INSERT):
                session.fetch_one_or_none(SELECT_BY_VALUE, (f"value_{i % 100}",))
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def _get_duckdb_engine() -> tuple[Any, Any]:
    """Import SQLAlchemy with duckdb_engine lazily."""
    try:
        import duckdb_engine  # noqa: F401
        from sqlalchemy import create_engine, text
    except ImportError:
        return None, None
    else:
        return create_engine, text


def sqlalchemy_duckdb_initialization() -> None:
    create_engine, text = _get_duckdb_engine()
    if create_engine is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        engine = create_engine(f"duckdb:///{tmp_path}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            conn.commit()
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def sqlalchemy_duckdb_write_heavy() -> None:
    create_engine, text = _get_duckdb_engine()
    if create_engine is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        engine = create_engine(f"duckdb:///{tmp_path}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            conn.commit()
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def sqlalchemy_duckdb_read_heavy() -> None:
    create_engine, text = _get_duckdb_engine()
    if create_engine is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        engine = create_engine(f"duckdb:///{tmp_path}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            conn.commit()
            result = conn.execute(text(SELECT_TEST_VALUES))
            rows = result.fetchall()
            assert len(rows) == ROWS_TO_INSERT
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def sqlalchemy_duckdb_iterative_inserts() -> None:
    """Individual inserts in a loop - shows per-call overhead."""
    create_engine, text = _get_duckdb_engine()
    if create_engine is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        engine = create_engine(f"duckdb:///{tmp_path}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            for i in range(ROWS_TO_INSERT):
                conn.execute(text(INSERT_TEST_VALUE_SQLA), {"value": f"value_{i}"})
            conn.commit()
    finally:
        with suppress(OSError):
            tmp_path.unlink()


def sqlalchemy_duckdb_repeated_queries() -> None:
    """Repeated single-row queries."""
    create_engine, text = _get_duckdb_engine()
    if create_engine is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    try:
        engine = create_engine(f"duckdb:///{tmp_path}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            conn.commit()
            for i in range(ROWS_TO_INSERT):
                result = conn.execute(text(SELECT_BY_VALUE_SQLA), {"value": f"value_{i % 100}"})
                result.fetchone()
    finally:
        with suppress(OSError):
            tmp_path.unlink()


# Iterative insert scenarios - tests per-call overhead
# This is what euri10's original benchmark measured for sqlspec
# but not for raw/sqlalchemy (which used executemany)


def raw_sqlite_iterative_inserts() -> None:
    """Individual inserts in a loop - shows per-call overhead."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        _optimize_raw_sqlite(conn)
        conn.execute(CREATE_TEST_TABLE)
        for i in range(ROWS_TO_INSERT):
            conn.execute(INSERT_TEST_VALUE, (f"value_{i}",))
        conn.commit()
        conn.close()


def sqlspec_sqlite_iterative_inserts() -> None:
    """Individual inserts in a loop - shows per-call overhead."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            for i in range(ROWS_TO_INSERT):
                session.execute(INSERT_TEST_VALUE, (f"value_{i}",))


def sqlalchemy_sqlite_iterative_inserts() -> None:
    """Individual inserts in a loop - shows per-call overhead."""
    create_engine, text = _get_sqlalchemy()
    if create_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            for i in range(ROWS_TO_INSERT):
                conn.execute(text(INSERT_TEST_VALUE_SQLA), {"value": f"value_{i}"})
            conn.commit()


# Query cache scenarios - tests repeated single-row operations
# These stress the query preparation/caching path
SELECT_BY_VALUE = "SELECT * FROM test WHERE value = ?;"
SELECT_BY_VALUE_ASYNCPG = "SELECT * FROM test WHERE value = $1;"
SELECT_BY_VALUE_SQLA = "SELECT * FROM test WHERE value = :value;"


def raw_sqlite_repeated_queries() -> None:
    """Repeated single-row queries - tests query preparation overhead."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        _optimize_raw_sqlite(conn)
        conn.execute(CREATE_TEST_TABLE)
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        conn.executemany(INSERT_TEST_VALUE, data)
        conn.commit()
        # Query same rows repeatedly with different params
        for i in range(ROWS_TO_INSERT):
            cursor = conn.execute(SELECT_BY_VALUE, (f"value_{i % 100}",))
            cursor.fetchone()
        conn.close()


def sqlspec_sqlite_repeated_queries() -> None:
    """Repeated single-row queries - tests query cache effectiveness."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            session.execute_many(INSERT_TEST_VALUE, data)
            # Query same rows repeatedly with different params
            # This should hit the query cache after the first few iterations
            for i in range(ROWS_TO_INSERT):
                session.fetch_one_or_none(SELECT_BY_VALUE, (f"value_{i % 100}",))


def sqlalchemy_sqlite_repeated_queries() -> None:
    """Repeated single-row queries."""
    create_engine, text = _get_sqlalchemy()
    if create_engine is None:
        return
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        with engine.connect() as conn:
            conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            conn.commit()
            # Query same rows repeatedly with different params
            for i in range(ROWS_TO_INSERT):
                result = conn.execute(text(SELECT_BY_VALUE_SQLA), {"value": f"value_{i % 100}"})
                result.fetchone()


# Aiosqlite implementations
# These test async sqlite performance


def _check_pool_leak(pool: Any, scenario_name: str) -> None:
    """Check for connection leaks in a pool.

    Args:
        pool: Connection pool with size() and checked_out() methods
        scenario_name: Name of the scenario for error reporting
    """
    if pool is None:
        return

    with suppress(AttributeError, TypeError):
        total = pool.size()
        checked_out = pool.checked_out()
        if checked_out > 0:
            _leaked_pools.append(f"{scenario_name}: {checked_out}/{total} connections leaked")


def _get_aiosqlite() -> Any:
    """Import aiosqlite lazily."""
    try:
        import aiosqlite
    except ImportError:
        return None
    else:
        return aiosqlite


def _get_aiosqlite_config() -> Any:
    """Import AiosqliteConfig lazily."""
    try:
        from sqlspec.adapters.aiosqlite import AiosqliteConfig
    except ImportError:
        return None
    else:
        return AiosqliteConfig


async def raw_aiosqlite_initialization() -> None:
    aiosqlite = _get_aiosqlite()
    if aiosqlite is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        async with aiosqlite.connect(str(tmp_path)) as conn:
            await conn.execute(CREATE_TEST_TABLE)
            await conn.commit()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def raw_aiosqlite_write_heavy() -> None:
    aiosqlite = _get_aiosqlite()
    if aiosqlite is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        async with aiosqlite.connect(str(tmp_path)) as conn:
            await conn.execute(CREATE_TEST_TABLE)
            data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await conn.executemany(INSERT_TEST_VALUE, data)
            await conn.commit()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def raw_aiosqlite_read_heavy() -> None:
    aiosqlite = _get_aiosqlite()
    if aiosqlite is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        async with aiosqlite.connect(str(tmp_path)) as conn:
            await conn.execute(CREATE_TEST_TABLE)
            data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await conn.executemany(INSERT_TEST_VALUE, data)
            await conn.commit()
            cursor = await conn.execute(SELECT_TEST_VALUES)
            rows = await cursor.fetchall()
            assert len(rows) == ROWS_TO_INSERT
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def raw_aiosqlite_iterative_inserts() -> None:
    aiosqlite = _get_aiosqlite()
    if aiosqlite is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        async with aiosqlite.connect(str(tmp_path)) as conn:
            await conn.execute(CREATE_TEST_TABLE)
            for i in range(ROWS_TO_INSERT):
                await conn.execute(INSERT_TEST_VALUE, (f"value_{i}",))
            await conn.commit()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def raw_aiosqlite_repeated_queries() -> None:
    aiosqlite = _get_aiosqlite()
    if aiosqlite is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        async with aiosqlite.connect(str(tmp_path)) as conn:
            await conn.execute(CREATE_TEST_TABLE)
            data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await conn.executemany(INSERT_TEST_VALUE, data)
            await conn.commit()
            for i in range(ROWS_TO_INSERT):
                cursor = await conn.execute(SELECT_BY_VALUE, (f"value_{i % 100}",))
                await cursor.fetchone()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def sqlspec_aiosqlite_initialization() -> None:
    AiosqliteConfig = _get_aiosqlite_config()  # noqa: N806
    if AiosqliteConfig is None:
        return
    # Use delete=False so we control when the file is deleted (after pool close)
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        spec = SQLSpec()
        config = AiosqliteConfig(database=str(tmp_path), pool_size=POOL_SIZE)
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
        # Properly close the pool to release all connections
        _check_pool_leak(config.connection_instance, "aiosqlite/initialization")
        await config.close_pool()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def sqlspec_aiosqlite_write_heavy() -> None:
    AiosqliteConfig = _get_aiosqlite_config()  # noqa: N806
    if AiosqliteConfig is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        spec = SQLSpec()
        config = AiosqliteConfig(database=str(tmp_path), pool_size=POOL_SIZE)
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await session.execute_many(INSERT_TEST_VALUE, data)
        _check_pool_leak(config.connection_instance, "aiosqlite/write_heavy")
        await config.close_pool()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def sqlspec_aiosqlite_read_heavy() -> None:
    AiosqliteConfig = _get_aiosqlite_config()  # noqa: N806
    if AiosqliteConfig is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        spec = SQLSpec()
        config = AiosqliteConfig(database=str(tmp_path), pool_size=POOL_SIZE)
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await session.execute_many(INSERT_TEST_VALUE, data)
            rows = await session.fetch(SELECT_TEST_VALUES)
            assert len(rows) == ROWS_TO_INSERT
        _check_pool_leak(config.connection_instance, "aiosqlite/read_heavy")
        await config.close_pool()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def sqlspec_aiosqlite_iterative_inserts() -> None:
    AiosqliteConfig = _get_aiosqlite_config()  # noqa: N806
    if AiosqliteConfig is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        spec = SQLSpec()
        config = AiosqliteConfig(database=str(tmp_path), pool_size=POOL_SIZE)
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
            for i in range(ROWS_TO_INSERT):
                await session.execute(INSERT_TEST_VALUE, (f"value_{i}",))
        _check_pool_leak(config.connection_instance, "aiosqlite/iterative_inserts")
        await config.close_pool()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def sqlspec_aiosqlite_repeated_queries() -> None:
    AiosqliteConfig = _get_aiosqlite_config()  # noqa: N806
    if AiosqliteConfig is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        spec = SQLSpec()
        config = AiosqliteConfig(database=str(tmp_path), pool_size=POOL_SIZE)
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await session.execute_many(INSERT_TEST_VALUE, data)
            for i in range(ROWS_TO_INSERT):
                await session.fetch_one_or_none(SELECT_BY_VALUE, (f"value_{i % 100}",))
        _check_pool_leak(config.connection_instance, "aiosqlite/repeated_queries")
        await config.close_pool()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def sqlalchemy_aiosqlite_initialization() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}")
        async with engine.connect() as conn:
            await conn.execute(text(CREATE_TEST_TABLE))
            await conn.commit()
        await engine.dispose()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def sqlalchemy_aiosqlite_write_heavy() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}")
        async with engine.connect() as conn:
            await conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            await conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            await conn.commit()
        await engine.dispose()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def sqlalchemy_aiosqlite_read_heavy() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}")
        async with engine.connect() as conn:
            await conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            await conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            await conn.commit()
            result = await conn.execute(text(SELECT_TEST_VALUES))
            rows = result.fetchall()
            assert len(rows) == ROWS_TO_INSERT
        await engine.dispose()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def sqlalchemy_aiosqlite_iterative_inserts() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}")
        async with engine.connect() as conn:
            await conn.execute(text(CREATE_TEST_TABLE))
            for i in range(ROWS_TO_INSERT):
                await conn.execute(text(INSERT_TEST_VALUE_SQLA), {"value": f"value_{i}"})
            await conn.commit()
        await engine.dispose()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


async def sqlalchemy_aiosqlite_repeated_queries() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}")
        async with engine.connect() as conn:
            await conn.execute(text(CREATE_TEST_TABLE))
            data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
            await conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
            await conn.commit()
            for i in range(ROWS_TO_INSERT):
                result = await conn.execute(text(SELECT_BY_VALUE_SQLA), {"value": f"value_{i % 100}"})
                result.fetchone()
        await engine.dispose()
    finally:
        with suppress(OSError):
            await anyio.Path(tmp_path).unlink()


# Asyncpg implementations
# These require asyncpg and optionally SQLAlchemy[asyncio] to be installed

POSTGRES_DSN = os.getenv("SQLSPEC_POSTGRES_DSN", "postgresql://postgres:postgres@localhost/postgres")
ASYNCPG_DSN = os.getenv("SQLSPEC_ASYNCPG_DSN", POSTGRES_DSN)
COCKROACH_DSN = os.getenv("SQLSPEC_COCKROACH_DSN", "")


def _get_asyncpg() -> Any:
    """Import asyncpg lazily."""
    try:
        from asyncpg import connect
    except ImportError:
        return None
    else:
        return connect


def _get_asyncpg_config() -> Any:
    """Import AsyncpgConfig lazily."""
    try:
        from sqlspec.adapters.asyncpg import AsyncpgConfig
    except ImportError:
        return None
    else:
        return AsyncpgConfig


def _get_async_sqlalchemy() -> tuple[Any, Any]:
    """Import async SQLAlchemy lazily."""
    try:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine
    except ImportError:
        return None, None
    else:
        return create_async_engine, text


async def raw_asyncpg_initialization() -> None:
    connect = _get_asyncpg()
    if connect is None:
        return
    conn = await connect(dsn=ASYNCPG_DSN)
    await conn.execute(DROP_TEST_TABLE)
    await conn.execute(CREATE_TEST_TABLE)
    await conn.close()


async def raw_asyncpg_write_heavy() -> None:
    connect = _get_asyncpg()
    if connect is None:
        return
    conn = await connect(dsn=ASYNCPG_DSN)
    await conn.execute(DROP_TEST_TABLE)
    await conn.execute(CREATE_TEST_TABLE)
    # Use executemany for fair comparison
    data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
    await conn.executemany(INSERT_TEST_VALUE_ASYNCPG, data)
    await conn.close()


async def raw_asyncpg_read_heavy() -> None:
    connect = _get_asyncpg()
    if connect is None:
        return
    conn = await connect(dsn=ASYNCPG_DSN)
    await conn.execute(DROP_TEST_TABLE)
    await conn.execute(CREATE_TEST_TABLE)
    data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
    await conn.executemany(INSERT_TEST_VALUE_ASYNCPG, data)
    rows = await conn.fetch(SELECT_TEST_VALUES)
    assert len(rows) == ROWS_TO_INSERT
    await conn.close()


async def raw_asyncpg_iterative_inserts() -> None:
    connect = _get_asyncpg()
    if connect is None:
        return
    conn = await connect(dsn=ASYNCPG_DSN)
    await conn.execute(DROP_TEST_TABLE)
    await conn.execute(CREATE_TEST_TABLE)
    for i in range(ROWS_TO_INSERT):
        await conn.execute(INSERT_TEST_VALUE_ASYNCPG, f"value_{i}")
    await conn.close()


async def raw_asyncpg_repeated_queries() -> None:
    connect = _get_asyncpg()
    if connect is None:
        return
    conn = await connect(dsn=ASYNCPG_DSN)
    await conn.execute(DROP_TEST_TABLE)
    await conn.execute(CREATE_TEST_TABLE)
    data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
    await conn.executemany(INSERT_TEST_VALUE_ASYNCPG, data)
    for i in range(ROWS_TO_INSERT):
        await conn.fetchrow(SELECT_BY_VALUE_ASYNCPG, f"value_{i % 100}")
    await conn.close()


async def sqlspec_asyncpg_initialization() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        return
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": ASYNCPG_DSN})
    async with spec.provide_session(config) as session:
        await session.execute(DROP_TEST_TABLE)
        await session.execute(CREATE_TEST_TABLE)


async def sqlspec_asyncpg_write_heavy() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        return
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": ASYNCPG_DSN})
    async with spec.provide_session(config) as session:
        await session.execute(DROP_TEST_TABLE)
        await session.execute(CREATE_TEST_TABLE)
        data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        await session.execute_many(INSERT_TEST_VALUE_ASYNCPG, data)


async def sqlspec_asyncpg_read_heavy() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        return
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": ASYNCPG_DSN})
    async with spec.provide_session(config) as session:
        await session.execute(DROP_TEST_TABLE)
        await session.execute(CREATE_TEST_TABLE)
        data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        await session.execute_many(INSERT_TEST_VALUE_ASYNCPG, data)
        rows = await session.fetch(SELECT_TEST_VALUES)
        assert len(rows) == ROWS_TO_INSERT


async def sqlspec_asyncpg_iterative_inserts() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        return
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": ASYNCPG_DSN})
    async with spec.provide_session(config) as session:
        await session.execute(DROP_TEST_TABLE)
        await session.execute(CREATE_TEST_TABLE)
        for i in range(ROWS_TO_INSERT):
            await session.execute(INSERT_TEST_VALUE_ASYNCPG, (f"value_{i}",))


async def sqlspec_asyncpg_repeated_queries() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        return
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": ASYNCPG_DSN})
    async with spec.provide_session(config) as session:
        await session.execute(DROP_TEST_TABLE)
        await session.execute(CREATE_TEST_TABLE)
        data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        await session.execute_many(INSERT_TEST_VALUE_ASYNCPG, data)
        for i in range(ROWS_TO_INSERT):
            await session.fetch_one_or_none(SELECT_BY_VALUE_ASYNCPG, (f"value_{i % 100}",))


async def sqlalchemy_asyncpg_initialization() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    engine = create_async_engine(f"postgresql+asyncpg://{ASYNCPG_DSN.split('://')[1]}")
    async with engine.connect() as conn:
        await conn.execute(text(DROP_TEST_TABLE))
        await conn.execute(text(CREATE_TEST_TABLE))
        await conn.commit()
    await engine.dispose()


async def sqlalchemy_asyncpg_write_heavy() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    engine = create_async_engine(f"postgresql+asyncpg://{ASYNCPG_DSN.split('://')[1]}")
    async with engine.connect() as conn:
        await conn.execute(text(DROP_TEST_TABLE))
        await conn.execute(text(CREATE_TEST_TABLE))
        data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
        await conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
        await conn.commit()
    await engine.dispose()


async def sqlalchemy_asyncpg_read_heavy() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    engine = create_async_engine(f"postgresql+asyncpg://{ASYNCPG_DSN.split('://')[1]}")
    async with engine.begin() as conn:
        await conn.execute(text(DROP_TEST_TABLE))
        await conn.execute(text(CREATE_TEST_TABLE))
        data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
        await conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
        result = await conn.execute(text(SELECT_TEST_VALUES))
        rows = result.fetchall()
        assert len(rows) == ROWS_TO_INSERT
    await engine.dispose()


async def sqlalchemy_asyncpg_iterative_inserts() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    engine = create_async_engine(f"postgresql+asyncpg://{ASYNCPG_DSN.split('://')[1]}")
    async with engine.connect() as conn:
        await conn.execute(text(DROP_TEST_TABLE))
        await conn.execute(text(CREATE_TEST_TABLE))
        for i in range(ROWS_TO_INSERT):
            await conn.execute(text(INSERT_TEST_VALUE_SQLA), {"value": f"value_{i}"})
        await conn.commit()
    await engine.dispose()


async def sqlalchemy_asyncpg_repeated_queries() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        return
    engine = create_async_engine(f"postgresql+asyncpg://{ASYNCPG_DSN.split('://')[1]}")
    async with engine.connect() as conn:
        await conn.execute(text(DROP_TEST_TABLE))
        await conn.execute(text(CREATE_TEST_TABLE))
        data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
        await conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
        await conn.commit()
        for i in range(ROWS_TO_INSERT):
            result = await conn.execute(text(SELECT_BY_VALUE_SQLA), {"value": f"value_{i % 100}"})
            result.fetchone()
    await engine.dispose()


# ===========================================================================
# Extended benchmark scenarios
# These exercise sqlspec-specific features (dict key transforms, schema
# mapping, complex parameter types, thin path stress testing).
# ===========================================================================

# --- SQL constants for extended scenarios ---

CREATE_WIDE_TABLE = """CREATE TABLE wide_test (
    first_name TEXT,
    last_name TEXT,
    email_address TEXT,
    phone_number TEXT,
    street_address TEXT,
    postal_code TEXT,
    country_name TEXT,
    birth_date TEXT,
    account_balance TEXT,
    is_active TEXT
);"""

INSERT_WIDE_ROW = (
    "INSERT INTO wide_test "
    "(first_name, last_name, email_address, phone_number, street_address, "
    "postal_code, country_name, birth_date, account_balance, is_active) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
)

SELECT_WIDE_ALL = "SELECT * FROM wide_test;"

CREATE_COMPLEX_TABLE = """CREATE TABLE complex_test (
    uuid_val TEXT,
    created_at TEXT,
    payload TEXT
);"""

INSERT_COMPLEX_ROW = "INSERT INTO complex_test (uuid_val, created_at, payload) VALUES (?, ?, ?);"

SELECT_COMPLEX_ALL = "SELECT * FROM complex_test;"


class WideRowDict(TypedDict):
    """TypedDict schema matching the wide_test table for schema_mapping benchmark."""

    first_name: str
    last_name: str
    email_address: str
    phone_number: str
    street_address: str
    postal_code: str
    country_name: str
    birth_date: str
    account_balance: str
    is_active: str


def _generate_wide_row(i: int) -> tuple[str, ...]:
    """Generate a single row tuple for the wide_test table."""
    return (
        f"First{i}",
        f"Last{i}",
        f"user{i}@example.com",
        f"+1-555-{i:04d}",
        f"{i} Main Street",
        f"{10000 + i}",
        "United States",
        f"199{i % 10}-{1 + (i % 12):02d}-{1 + (i % 28):02d}",
        f"{100.0 + i * 0.5:.2f}",
        "true" if i % 2 == 0 else "false",
    )


def _generate_complex_row(i: int) -> tuple[str, str, str]:
    """Generate a single row tuple for the complex_test table."""
    import uuid as uuid_mod

    uid = str(uuid_mod.UUID(int=i))
    created_at = f"2024-{1 + (i % 12):02d}-{1 + (i % 28):02d}T{i % 24:02d}:{i % 60:02d}:00"
    payload = f'{{"key": "value_{i}", "count": {i}, "tags": ["a", "b"]}}'
    return (uid, created_at, payload)


# --- dict_key_transform scenarios ---


def raw_sqlite_dict_key_transform() -> None:
    """SELECT 10K rows with 10 columns, then transform keys to camelCase (raw)."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        _optimize_raw_sqlite(conn)
        conn.execute(CREATE_WIDE_TABLE)
        data = [_generate_wide_row(i) for i in range(ROWS_TO_INSERT)]
        conn.executemany(INSERT_WIDE_ROW, data)
        conn.commit()
        # Raw driver: fetch as tuples; build dicts manually then transform
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(SELECT_WIDE_ALL)
        rows = cursor.fetchall()
        assert len(rows) == ROWS_TO_INSERT
        # No transform for raw - just fetch (raw baseline)
        conn.close()


def sqlspec_sqlite_dict_key_transform() -> None:
    """SELECT 10K rows with 10 columns, then transform keys to camelCase (sqlspec)."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_WIDE_TABLE)
            data: Sequence[tuple[str, ...]] = [_generate_wide_row(i) for i in range(ROWS_TO_INSERT)]
            session.execute_many(INSERT_WIDE_ROW, data)
            rows = session.fetch(SELECT_WIDE_ALL)
            assert len(rows) == ROWS_TO_INSERT
            # Apply dict key transformation from snake_case to camelCase
            transformed = [transform_dict_keys(row, camelize) for row in rows]
            assert len(transformed) == ROWS_TO_INSERT
            # Verify transformation worked on first row
            first = transformed[0]
            assert "firstName" in first


# --- schema_mapping scenarios ---


def raw_sqlite_schema_mapping() -> None:
    """SELECT 10K rows and return as raw tuples (baseline for schema mapping)."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        _optimize_raw_sqlite(conn)
        conn.execute(CREATE_WIDE_TABLE)
        data = [_generate_wide_row(i) for i in range(ROWS_TO_INSERT)]
        conn.executemany(INSERT_WIDE_ROW, data)
        conn.commit()
        cursor = conn.execute(SELECT_WIDE_ALL)
        rows = cursor.fetchall()
        assert len(rows) == ROWS_TO_INSERT
        conn.close()


def sqlspec_sqlite_schema_mapping() -> None:
    """SELECT 10K rows and map to TypedDict using schema_type= parameter."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_WIDE_TABLE)
            data: Sequence[tuple[str, ...]] = [_generate_wide_row(i) for i in range(ROWS_TO_INSERT)]
            session.execute_many(INSERT_WIDE_ROW, data)
            rows = session.fetch(SELECT_WIDE_ALL, schema_type=WideRowDict)
            assert len(rows) == ROWS_TO_INSERT
            # Verify schema mapping worked
            first = rows[0]
            assert "first_name" in first


# --- complex_parameters scenarios ---


def raw_sqlite_complex_parameters() -> None:
    """INSERT rows with complex parameter types: UUID, datetime, JSON (raw)."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        _optimize_raw_sqlite(conn)
        conn.execute(CREATE_COMPLEX_TABLE)
        data = [_generate_complex_row(i) for i in range(ROWS_TO_INSERT)]
        conn.executemany(INSERT_COMPLEX_ROW, data)
        conn.commit()
        # Read back to verify
        cursor = conn.execute(SELECT_COMPLEX_ALL)
        rows = cursor.fetchall()
        assert len(rows) == ROWS_TO_INSERT
        conn.close()


def sqlspec_sqlite_complex_parameters() -> None:
    """INSERT rows with complex parameter types: UUID, datetime, JSON (sqlspec)."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_COMPLEX_TABLE)
            data: Sequence[tuple[str, str, str]] = [_generate_complex_row(i) for i in range(ROWS_TO_INSERT)]
            session.execute_many(INSERT_COMPLEX_ROW, data)
            # Read back to verify
            rows = session.fetch(SELECT_COMPLEX_ALL)
            assert len(rows) == ROWS_TO_INSERT


# --- thin_path_stress scenarios ---


def raw_sqlite_thin_path_stress() -> None:
    """100K individual INSERTs to stress-test per-call execution path (raw)."""
    stress_count = ROWS_TO_INSERT * 10
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        _optimize_raw_sqlite(conn)
        conn.execute(CREATE_TEST_TABLE)
        for i in range(stress_count):
            conn.execute(INSERT_TEST_VALUE, (f"value_{i}",))
        conn.commit()
        conn.close()


def sqlspec_sqlite_thin_path_stress() -> None:
    """100K individual INSERTs to stress-test the thin execute path (sqlspec)."""
    stress_count = ROWS_TO_INSERT * 10
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            for i in range(stress_count):
                session.execute(INSERT_TEST_VALUE, (f"value_{i}",))


MYSQL_DSN = os.getenv("SQLSPEC_MYSQL_DSN", "mysql://root:mysql@localhost:3307/test")
ORACLE_DSN = os.getenv("SQLSPEC_ORACLE_DSN", "localhost:1522/FREEPDB1")
ORACLE_USER = os.getenv("SQLSPEC_ORACLE_USER", "system")
ORACLE_PASSWORD = os.getenv("SQLSPEC_ORACLE_PASSWORD", "oracle")
ADBC_DSN = os.getenv("SQLSPEC_ADBC_DSN", "")
SPANNER_PROJECT = os.getenv("SQLSPEC_SPANNER_PROJECT", "")
SPANNER_INSTANCE = os.getenv("SQLSPEC_SPANNER_INSTANCE", "")
SPANNER_DATABASE = os.getenv("SQLSPEC_SPANNER_DATABASE", "")
BIGQUERY_PROJECT = os.getenv("SQLSPEC_BIGQUERY_PROJECT", "")
BIGQUERY_DATASET = os.getenv("SQLSPEC_BIGQUERY_DATASET", "")

INSERT_TEST_VALUE_PYFORMAT = "INSERT INTO test (value) VALUES (%s);"
SELECT_BY_VALUE_PYFORMAT = "SELECT * FROM test WHERE value = %s;"
INSERT_TEST_VALUE_ORACLE = "INSERT INTO test (value) VALUES (:1)"
SELECT_BY_VALUE_ORACLE = "SELECT * FROM test WHERE value = :1"
ORACLE_DROP_TEST_TABLE = (
    "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test PURGE'; "
    "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
)
ORACLE_CREATE_TEST_TABLE = "CREATE TABLE test (value VARCHAR2(255))"


def _import_object(module_name: str, attr_name: str) -> Any:
    try:
        module = __import__(module_name, fromlist=[attr_name])
    except ImportError as exc:
        raise BenchmarkSkipError(f"{module_name} is not installed") from exc
    return getattr(module, attr_name)


def _mysql_connection_config(*, aiomysql: bool = False) -> dict[str, Any]:
    parsed = urlparse(MYSQL_DSN)
    database = parsed.path.lstrip("/") or "test"
    config = {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 3307,
        "user": unquote(parsed.username or "root"),
        "password": unquote(parsed.password or "mysql"),
        "database": database,
    }
    if aiomysql:
        config["db"] = config.pop("database")
    return config


def _oracle_connection_config() -> dict[str, Any]:
    return {"dsn": ORACLE_DSN, "user": ORACLE_USER, "password": ORACLE_PASSWORD}


def _postgres_dsn_for_driver(driver: str, *, asyncpg_driver: bool = False) -> str:
    if driver.startswith("cockroach"):
        if not COCKROACH_DSN:
            raise BenchmarkSkipError("set SQLSPEC_COCKROACH_DSN")
        return COCKROACH_DSN
    return ASYNCPG_DSN if asyncpg_driver else POSTGRES_DSN


def _postgres_connection_config(driver: str) -> dict[str, Any]:
    dsn = _postgres_dsn_for_driver(driver, asyncpg_driver=driver == "asyncpg")
    if driver in {"asyncpg", "cockroach_asyncpg", "psqlpy"}:
        return {"dsn": dsn}
    return {"conninfo": dsn}


def _adbc_connection_config() -> dict[str, Any]:
    if ADBC_DSN:
        return {"uri": ADBC_DSN}
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    return {"uri": f"sqlite://{tmp_path}"}


def _adbc_raw_uri() -> str:
    if ADBC_DSN.startswith("sqlite://"):
        return ADBC_DSN.removeprefix("sqlite://")
    if ADBC_DSN:
        return ADBC_DSN
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    return str(tmp_path)


def _spanner_connection_config() -> dict[str, Any]:
    if not (SPANNER_PROJECT and SPANNER_INSTANCE and SPANNER_DATABASE):
        raise BenchmarkSkipError("set SQLSPEC_SPANNER_PROJECT, SQLSPEC_SPANNER_INSTANCE, and SQLSPEC_SPANNER_DATABASE")
    return {"project": SPANNER_PROJECT, "instance_id": SPANNER_INSTANCE, "database_id": SPANNER_DATABASE}


def _bigquery_connection_config() -> dict[str, Any]:
    if not (BIGQUERY_PROJECT and BIGQUERY_DATASET):
        raise BenchmarkSkipError("set SQLSPEC_BIGQUERY_PROJECT and SQLSPEC_BIGQUERY_DATASET")
    return {"project": BIGQUERY_PROJECT, "dataset_id": BIGQUERY_DATASET}


def _sql_for_driver(driver: str) -> dict[str, str]:
    if driver in {"oracledb", "oracledb_async"}:
        return {
            "drop": ORACLE_DROP_TEST_TABLE,
            "create": ORACLE_CREATE_TEST_TABLE,
            "insert": INSERT_TEST_VALUE_ORACLE,
            "select_all": SELECT_TEST_VALUES,
            "select_by": SELECT_BY_VALUE_ORACLE,
        }
    if driver in {"asyncpg", "psqlpy", "cockroach_asyncpg"}:
        return {
            "drop": DROP_TEST_TABLE,
            "create": CREATE_TEST_TABLE,
            "insert": INSERT_TEST_VALUE_ASYNCPG,
            "select_all": SELECT_TEST_VALUES,
            "select_by": SELECT_BY_VALUE_ASYNCPG,
        }
    if driver in {"psycopg", "psycopg_async", "cockroach_psycopg", "cockroach_psycopg_async"}:
        return {
            "drop": DROP_TEST_TABLE,
            "create": CREATE_TEST_TABLE,
            "insert": INSERT_TEST_VALUE_PYFORMAT,
            "select_all": SELECT_TEST_VALUES,
            "select_by": SELECT_BY_VALUE_PYFORMAT,
        }
    if driver in {"aiomysql", "asyncmy", "pymysql", "mysqlconnector", "mysqlconnector_async"}:
        return {
            "drop": DROP_TEST_TABLE,
            "create": CREATE_TEST_TABLE,
            "insert": INSERT_TEST_VALUE_PYFORMAT,
            "select_all": SELECT_TEST_VALUES,
            "select_by": SELECT_BY_VALUE_PYFORMAT,
        }
    return {
        "drop": DROP_TEST_TABLE,
        "create": CREATE_TEST_TABLE,
        "insert": INSERT_TEST_VALUE,
        "select_all": SELECT_TEST_VALUES,
        "select_by": SELECT_BY_VALUE,
    }


def _run_sqlspec_sync_workload(driver: str, config_module: str, config_name: str, scenario: str) -> None:
    if driver in {"spanner", "bigquery"}:
        connection_config = _spanner_connection_config() if driver == "spanner" else _bigquery_connection_config()
    elif driver == "adbc":
        connection_config = _adbc_connection_config()
    elif driver == "oracledb":
        connection_config = _oracle_connection_config()
    elif driver in {"pymysql", "mysqlconnector"}:
        connection_config = _mysql_connection_config()
    else:
        connection_config = _postgres_connection_config(driver)
    Config = _import_object(config_module, config_name)  # noqa: N806
    statements = _sql_for_driver(driver)
    config = Config(connection_config=connection_config)
    spec = SQLSpec()
    try:
        with spec.provide_session(config) as session:
            _run_sync_session_workload(session, statements, scenario)
    finally:
        close_pool = getattr(config, "close_pool", None)
        if close_pool is not None:
            close_pool()


async def _run_sqlspec_async_workload(driver: str, config_module: str, config_name: str, scenario: str) -> None:
    if driver == "aiomysql":
        connection_config = _mysql_connection_config(aiomysql=True)
    elif driver in {"asyncmy", "mysqlconnector_async"}:
        connection_config = _mysql_connection_config()
    elif driver == "oracledb_async":
        connection_config = _oracle_connection_config()
    else:
        connection_config = _postgres_connection_config(driver)
    Config = _import_object(config_module, config_name)  # noqa: N806
    statements = _sql_for_driver(driver)
    config = Config(connection_config=connection_config)
    spec = SQLSpec()
    try:
        async with spec.provide_session(config) as session:
            await _run_async_session_workload(session, statements, scenario)
    finally:
        close_pool = getattr(config, "close_pool", None)
        if close_pool is not None:
            result = close_pool()
            if inspect.isawaitable(result):
                await result


def _run_sync_session_workload(session: Any, statements: dict[str, str], scenario: str) -> None:
    session.execute(statements["drop"])
    session.execute(statements["create"])
    if scenario == "initialization":
        return
    data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
    if scenario in {"write_heavy", "read_heavy", "repeated_queries"}:
        session.execute_many(statements["insert"], data)
    elif scenario == "iterative_inserts":
        for value in data:
            session.execute(statements["insert"], value)
        return
    if scenario == "read_heavy":
        rows = session.fetch(statements["select_all"])
        assert len(rows) == ROWS_TO_INSERT
    elif scenario == "repeated_queries":
        for i in range(ROWS_TO_INSERT):
            session.fetch_one_or_none(statements["select_by"], (f"value_{i % 100}",))


async def _run_async_session_workload(session: Any, statements: dict[str, str], scenario: str) -> None:
    await session.execute(statements["drop"])
    await session.execute(statements["create"])
    if scenario == "initialization":
        return
    data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
    if scenario in {"write_heavy", "read_heavy", "repeated_queries"}:
        await session.execute_many(statements["insert"], data)
    elif scenario == "iterative_inserts":
        for value in data:
            await session.execute(statements["insert"], value)
        return
    if scenario == "read_heavy":
        rows = await session.fetch(statements["select_all"])
        assert len(rows) == ROWS_TO_INSERT
    elif scenario == "repeated_queries":
        for i in range(ROWS_TO_INSERT):
            await session.fetch_one_or_none(statements["select_by"], (f"value_{i % 100}",))


def _run_dbapi_workload(connect: Any, statements: dict[str, str], scenario: str) -> None:
    conn = connect()
    cursor = conn.cursor()
    try:
        cursor.execute(statements["drop"])
        cursor.execute(statements["create"])
        if scenario == "initialization":
            return
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        if scenario in {"write_heavy", "read_heavy", "repeated_queries"}:
            cursor.executemany(statements["insert"], data)
        elif scenario == "iterative_inserts":
            for value in data:
                cursor.execute(statements["insert"], value)
            return
        if hasattr(conn, "commit"):
            conn.commit()
        if scenario == "read_heavy":
            cursor.execute(statements["select_all"])
            rows = cursor.fetchall()
            assert len(rows) == ROWS_TO_INSERT
        elif scenario == "repeated_queries":
            for i in range(ROWS_TO_INSERT):
                cursor.execute(statements["select_by"], (f"value_{i % 100}",))
                cursor.fetchone()
    finally:
        with suppress(Exception):
            cursor.close()
        with suppress(Exception):
            conn.close()


def _raw_psycopg_connect(driver: str) -> Any:
    psycopg = _import_object("psycopg", "connect")
    dsn = _postgres_dsn_for_driver(driver)
    return psycopg(dsn)


def _raw_pymysql_connect() -> Any:
    pymysql = _import_object("pymysql", "connect")
    return pymysql(**_mysql_connection_config())


def _raw_mysqlconnector_connect() -> Any:
    connector = _import_object("mysql.connector", "connect")
    return connector(**_mysql_connection_config())


def _raw_oracle_connect() -> Any:
    connect = _import_object("oracledb", "connect")
    return connect(**_oracle_connection_config())


def _raw_adbc_connect() -> Any:
    dbapi = _import_object("adbc_driver_sqlite.dbapi", "connect")
    uri = _adbc_raw_uri()
    return dbapi(uri=uri)


def _make_raw_dbapi_scenario(driver: str, connect: Any, scenario: str) -> Any:
    def run() -> None:
        _run_dbapi_workload(connect, _sql_for_driver(driver), scenario)

    return run


def _make_sqlspec_sync_scenario(driver: str, config_module: str, config_name: str, scenario: str) -> Any:
    def run() -> None:
        _run_sqlspec_sync_workload(driver, config_module, config_name, scenario)

    return run


def _make_sqlspec_async_scenario(driver: str, config_module: str, config_name: str, scenario: str) -> Any:
    async def run() -> None:
        await _run_sqlspec_async_workload(driver, config_module, config_name, scenario)

    return run


async def _run_asyncpg_like_raw_workload(driver: str, scenario: str) -> None:
    connect = _get_asyncpg()
    if connect is None:
        raise BenchmarkSkipError("asyncpg is not installed")
    statements = _sql_for_driver(driver)
    conn = await connect(dsn=_postgres_dsn_for_driver(driver, asyncpg_driver=True))
    try:
        await conn.execute(statements["drop"])
        await conn.execute(statements["create"])
        if scenario == "initialization":
            return
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        if scenario in {"write_heavy", "read_heavy", "repeated_queries"}:
            await conn.executemany(statements["insert"], data)
        elif scenario == "iterative_inserts":
            for value in data:
                await conn.execute(statements["insert"], value[0])
            return
        if scenario == "read_heavy":
            rows = await conn.fetch(statements["select_all"])
            assert len(rows) == ROWS_TO_INSERT
        elif scenario == "repeated_queries":
            for i in range(ROWS_TO_INSERT):
                await conn.fetchrow(statements["select_by"], f"value_{i % 100}")
    finally:
        await conn.close()


async def _run_aiomysql_like_raw_workload(driver: str, scenario: str) -> None:
    module_name = "aiomysql" if driver == "aiomysql" else "asyncmy"
    connect = _import_object(module_name, "connect")
    conn = await connect(**_mysql_connection_config(aiomysql=driver == "aiomysql"))
    cursor = await conn.cursor()
    statements = _sql_for_driver(driver)
    try:
        await cursor.execute(statements["drop"])
        await cursor.execute(statements["create"])
        if scenario == "initialization":
            return
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        if scenario in {"write_heavy", "read_heavy", "repeated_queries"}:
            await cursor.executemany(statements["insert"], data)
        elif scenario == "iterative_inserts":
            for value in data:
                await cursor.execute(statements["insert"], value)
            return
        await conn.commit()
        if scenario == "read_heavy":
            await cursor.execute(statements["select_all"])
            rows = await cursor.fetchall()
            assert len(rows) == ROWS_TO_INSERT
        elif scenario == "repeated_queries":
            for i in range(ROWS_TO_INSERT):
                await cursor.execute(statements["select_by"], (f"value_{i % 100}",))
                await cursor.fetchone()
    finally:
        with suppress(Exception):
            await cursor.close()
        close_result = conn.close()
        if inspect.isawaitable(close_result):
            await close_result


def _psqlpy_result_rows(result: Any) -> Any:
    return result.result() if hasattr(result, "result") else result


async def _run_psqlpy_raw_workload(scenario: str) -> None:
    ConnectionPool = _import_object("psqlpy", "ConnectionPool")  # noqa: N806
    pool = ConnectionPool(**_postgres_connection_config("psqlpy"))
    statements = _sql_for_driver("psqlpy")
    try:
        async with pool.acquire() as conn:
            await conn.execute(statements["drop"], [])
            await conn.execute(statements["create"], [])
            if scenario == "initialization":
                return
            data = [[f"value_{i}"] for i in range(ROWS_TO_INSERT)]
            if scenario in {"write_heavy", "read_heavy", "repeated_queries"}:
                await conn.execute_many(statements["insert"], data)
            elif scenario == "iterative_inserts":
                for value in data:
                    await conn.execute(statements["insert"], value)
                return
            if scenario == "read_heavy":
                rows = _psqlpy_result_rows(await conn.fetch(statements["select_all"], []))
                assert len(rows) == ROWS_TO_INSERT
            elif scenario == "repeated_queries":
                for i in range(ROWS_TO_INSERT):
                    await conn.fetch(statements["select_by"], [f"value_{i % 100}"])
    finally:
        with suppress(Exception):
            pool.close()


async def _run_mysqlconnector_async_raw_workload(scenario: str) -> None:
    connect = _import_object("mysql.connector.aio", "connect")
    conn = await connect(**_mysql_connection_config())
    cursor = await conn.cursor()
    statements = _sql_for_driver("mysqlconnector_async")
    try:
        await cursor.execute(statements["drop"])
        await cursor.execute(statements["create"])
        if scenario == "initialization":
            return
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        if scenario in {"write_heavy", "read_heavy", "repeated_queries"}:
            await cursor.executemany(statements["insert"], data)
        elif scenario == "iterative_inserts":
            for value in data:
                await cursor.execute(statements["insert"], value)
            return
        await conn.commit()
        if scenario == "read_heavy":
            await cursor.execute(statements["select_all"])
            rows = await cursor.fetchall()
            assert len(rows) == ROWS_TO_INSERT
        elif scenario == "repeated_queries":
            for i in range(ROWS_TO_INSERT):
                await cursor.execute(statements["select_by"], (f"value_{i % 100}",))
                await cursor.fetchone()
    finally:
        with suppress(Exception):
            await cursor.close()
        with suppress(Exception):
            await conn.close()


async def _run_psycopg_async_raw_workload(driver: str, scenario: str) -> None:
    AsyncConnection = _import_object("psycopg", "AsyncConnection")  # noqa: N806
    dsn = _postgres_dsn_for_driver(driver)
    conn = await AsyncConnection.connect(dsn)
    statements = _sql_for_driver(driver)
    try:
        await conn.execute(statements["drop"])
        await conn.execute(statements["create"])
        if scenario == "initialization":
            return
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        if scenario in {"write_heavy", "read_heavy", "repeated_queries"}:
            async with conn.cursor() as cursor:
                await cursor.executemany(statements["insert"], data)
        elif scenario == "iterative_inserts":
            for value in data:
                await conn.execute(statements["insert"], value)
            return
        await conn.commit()
        if scenario == "read_heavy":
            cursor = await conn.execute(statements["select_all"])
            rows = await cursor.fetchall()
            assert len(rows) == ROWS_TO_INSERT
        elif scenario == "repeated_queries":
            for i in range(ROWS_TO_INSERT):
                cursor = await conn.execute(statements["select_by"], (f"value_{i % 100}",))
                await cursor.fetchone()
    finally:
        await conn.close()


async def _run_oracle_async_raw_workload(scenario: str) -> None:
    connect_async = _import_object("oracledb", "connect_async")
    conn = await connect_async(**_oracle_connection_config())
    cursor = conn.cursor()
    statements = _sql_for_driver("oracledb_async")
    try:
        await cursor.execute(statements["drop"])
        await cursor.execute(statements["create"])
        if scenario == "initialization":
            return
        data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
        if scenario in {"write_heavy", "read_heavy", "repeated_queries"}:
            await cursor.executemany(statements["insert"], data)
        elif scenario == "iterative_inserts":
            for value in data:
                await cursor.execute(statements["insert"], value)
            return
        await conn.commit()
        if scenario == "read_heavy":
            await cursor.execute(statements["select_all"])
            rows = await cursor.fetchall()
            assert len(rows) == ROWS_TO_INSERT
        elif scenario == "repeated_queries":
            for i in range(ROWS_TO_INSERT):
                await cursor.execute(statements["select_by"], (f"value_{i % 100}",))
                await cursor.fetchone()
    finally:
        with suppress(Exception):
            await cursor.close()
        await conn.close()


def _make_raw_async_scenario(driver: str, scenario: str) -> Any:
    async def run() -> None:
        if driver in {"asyncpg", "cockroach_asyncpg"}:
            await _run_asyncpg_like_raw_workload(driver, scenario)
        elif driver in {"aiomysql", "asyncmy"}:
            await _run_aiomysql_like_raw_workload(driver, scenario)
        elif driver == "psqlpy":
            await _run_psqlpy_raw_workload(scenario)
        elif driver == "mysqlconnector_async":
            await _run_mysqlconnector_async_raw_workload(scenario)
        elif driver in {"psycopg_async", "cockroach_psycopg_async"}:
            await _run_psycopg_async_raw_workload(driver, scenario)
        elif driver == "oracledb_async":
            await _run_oracle_async_raw_workload(scenario)
        else:
            raise BenchmarkSkipError(f"{driver} raw async workload is not implemented")

    return run


def _resolve_benchmark_url(url: str | Callable[[], str]) -> str:
    return url() if callable(url) else url


def _make_sqlalchemy_sync_scenario(driver: str, url: str | Callable[[], str], scenario: str) -> Any:
    def run() -> None:
        create_engine, text = _get_sqlalchemy()
        if create_engine is None:
            raise BenchmarkSkipError("sqlalchemy is not installed")
        statements = _sql_for_driver(driver)
        engine = create_engine(_resolve_benchmark_url(url))
        try:
            with engine.connect() as conn:
                conn.execute(text(statements["drop"]))
                conn.execute(text(statements["create"]))
                if scenario == "initialization":
                    return
                data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
                if scenario in {"write_heavy", "read_heavy", "repeated_queries"}:
                    conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
                elif scenario == "iterative_inserts":
                    for row in data:
                        conn.execute(text(INSERT_TEST_VALUE_SQLA), row)
                    conn.commit()
                    return
                conn.commit()
                if scenario == "read_heavy":
                    rows = conn.execute(text(statements["select_all"])).fetchall()
                    assert len(rows) == ROWS_TO_INSERT
                elif scenario == "repeated_queries":
                    for i in range(ROWS_TO_INSERT):
                        conn.execute(text(SELECT_BY_VALUE_SQLA), {"value": f"value_{i % 100}"}).fetchone()
        finally:
            engine.dispose()

    return run


def _make_sqlalchemy_async_scenario(driver: str, url: str | Callable[[], str], scenario: str) -> Any:
    async def run() -> None:
        create_async_engine, text = _get_async_sqlalchemy()
        if create_async_engine is None:
            raise BenchmarkSkipError("sqlalchemy async is not installed")
        statements = _sql_for_driver(driver)
        engine = create_async_engine(_resolve_benchmark_url(url))
        try:
            async with engine.connect() as conn:
                await conn.execute(text(statements["drop"]))
                await conn.execute(text(statements["create"]))
                if scenario == "initialization":
                    return
                data = [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)]
                if scenario in {"write_heavy", "read_heavy", "repeated_queries"}:
                    await conn.execute(text(INSERT_TEST_VALUE_SQLA), data)
                elif scenario == "iterative_inserts":
                    for row in data:
                        await conn.execute(text(INSERT_TEST_VALUE_SQLA), row)
                    await conn.commit()
                    return
                await conn.commit()
                if scenario == "read_heavy":
                    rows = (await conn.execute(text(statements["select_all"]))).fetchall()
                    assert len(rows) == ROWS_TO_INSERT
                elif scenario == "repeated_queries":
                    for i in range(ROWS_TO_INSERT):
                        result = await conn.execute(text(SELECT_BY_VALUE_SQLA), {"value": f"value_{i % 100}"})
                        result.fetchone()
        finally:
            await engine.dispose()

    return run


def _postgres_sqlalchemy_url(driver: str, *, async_: bool = False) -> str:
    suffix = _postgres_dsn_for_driver(driver).split("://", 1)[1]
    if async_:
        return f"postgresql+psycopg://{suffix}"
    return f"postgresql+psycopg://{suffix}"


def _make_postgres_sqlalchemy_url_resolver(driver: str, *, async_: bool = False) -> Callable[[], str]:
    def resolve() -> str:
        return _postgres_sqlalchemy_url(driver, async_=async_)

    return resolve


def _mysql_sqlalchemy_url(driver: str) -> str:
    suffix = MYSQL_DSN.split("://", 1)[1]
    if driver == "pymysql":
        return f"mysql+pymysql://{suffix}"
    if driver == "aiomysql":
        return f"mysql+aiomysql://{suffix}"
    if driver == "asyncmy":
        return f"mysql+asyncmy://{suffix}"
    return f"mysql+mysqlconnector://{suffix}"


def _oracle_sqlalchemy_url() -> str:
    return f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_DSN}"


def _register_pr_c_driver_scenarios(registry: dict[tuple[str, str, str], Any]) -> None:
    sync_configs = {
        "psycopg": ("sqlspec.adapters.psycopg", "PsycopgSyncConfig"),
        "cockroach_psycopg": ("sqlspec.adapters.cockroach_psycopg", "CockroachPsycopgSyncConfig"),
        "pymysql": ("sqlspec.adapters.pymysql", "PyMysqlConfig"),
        "mysqlconnector": ("sqlspec.adapters.mysqlconnector", "MysqlConnectorSyncConfig"),
        "oracledb": ("sqlspec.adapters.oracledb", "OracleSyncConfig"),
        "adbc": ("sqlspec.adapters.adbc", "AdbcConfig"),
        "spanner": ("sqlspec.adapters.spanner", "SpannerSyncConfig"),
        "bigquery": ("sqlspec.adapters.bigquery", "BigQueryConfig"),
    }
    async_configs = {
        "psycopg_async": ("sqlspec.adapters.psycopg", "PsycopgAsyncConfig"),
        "psqlpy": ("sqlspec.adapters.psqlpy", "PsqlpyConfig"),
        "cockroach_asyncpg": ("sqlspec.adapters.cockroach_asyncpg", "CockroachAsyncpgConfig"),
        "cockroach_psycopg_async": ("sqlspec.adapters.cockroach_psycopg", "CockroachPsycopgAsyncConfig"),
        "aiomysql": ("sqlspec.adapters.aiomysql", "AiomysqlConfig"),
        "asyncmy": ("sqlspec.adapters.asyncmy", "AsyncmyConfig"),
        "mysqlconnector_async": ("sqlspec.adapters.mysqlconnector", "MysqlConnectorAsyncConfig"),
        "oracledb_async": ("sqlspec.adapters.oracledb", "OracleAsyncConfig"),
    }
    sync_raw = {
        "psycopg": lambda: _raw_psycopg_connect("psycopg"),
        "cockroach_psycopg": lambda: _raw_psycopg_connect("cockroach_psycopg"),
        "pymysql": _raw_pymysql_connect,
        "mysqlconnector": _raw_mysqlconnector_connect,
        "oracledb": _raw_oracle_connect,
        "adbc": _raw_adbc_connect,
        "spanner": lambda: (_ for _ in ()).throw(
            BenchmarkSkipError("Spanner raw workload requires emulator/client setup")
        ),
        "bigquery": lambda: (_ for _ in ()).throw(
            BenchmarkSkipError("BigQuery raw workload requires emulator/client setup")
        ),
    }
    for driver, (module_name, config_name) in sync_configs.items():
        for scenario in CORE_SCENARIOS:
            registry[("sqlspec", driver, scenario)] = _make_sqlspec_sync_scenario(
                driver, module_name, config_name, scenario
            )
            registry[("raw", driver, scenario)] = _make_raw_dbapi_scenario(driver, sync_raw[driver], scenario)
    for driver, (module_name, config_name) in async_configs.items():
        for scenario in CORE_SCENARIOS:
            registry[("sqlspec", driver, scenario)] = _make_sqlspec_async_scenario(
                driver, module_name, config_name, scenario
            )
            registry[("raw", driver, scenario)] = _make_raw_async_scenario(driver, scenario)
    for driver in ("psycopg", "cockroach_psycopg"):
        for scenario in CORE_SCENARIOS:
            registry[("sqlalchemy", driver, scenario)] = _make_sqlalchemy_sync_scenario(
                driver, _make_postgres_sqlalchemy_url_resolver(driver), scenario
            )
    for driver in ("psycopg_async",):
        for scenario in CORE_SCENARIOS:
            registry[("sqlalchemy", driver, scenario)] = _make_sqlalchemy_async_scenario(
                driver, _make_postgres_sqlalchemy_url_resolver(driver, async_=True), scenario
            )
    for driver in ("pymysql", "mysqlconnector", "aiomysql", "asyncmy"):
        for scenario in CORE_SCENARIOS:
            maker = (
                _make_sqlalchemy_async_scenario if driver in {"aiomysql", "asyncmy"} else _make_sqlalchemy_sync_scenario
            )
            registry[("sqlalchemy", driver, scenario)] = maker(driver, _mysql_sqlalchemy_url(driver), scenario)
    for scenario in CORE_SCENARIOS:
        registry[("sqlalchemy", "oracledb", scenario)] = _make_sqlalchemy_sync_scenario(
            "oracledb", _oracle_sqlalchemy_url(), scenario
        )


SCENARIO_REGISTRY: dict[tuple[str, str, str], Any] = {
    # SQLite scenarios
    ("raw", "sqlite", "initialization"): raw_sqlite_initialization,
    ("raw", "sqlite", "write_heavy"): raw_sqlite_write_heavy,
    ("raw", "sqlite", "read_heavy"): raw_sqlite_read_heavy,
    ("raw", "sqlite", "iterative_inserts"): raw_sqlite_iterative_inserts,
    ("raw", "sqlite", "repeated_queries"): raw_sqlite_repeated_queries,
    ("sqlspec", "sqlite", "initialization"): sqlspec_sqlite_initialization,
    ("sqlspec", "sqlite", "write_heavy"): sqlspec_sqlite_write_heavy,
    ("sqlspec", "sqlite", "read_heavy"): sqlspec_sqlite_read_heavy,
    ("sqlspec", "sqlite", "iterative_inserts"): sqlspec_sqlite_iterative_inserts,
    ("sqlspec", "sqlite", "repeated_queries"): sqlspec_sqlite_repeated_queries,
    ("sqlalchemy", "sqlite", "initialization"): sqlalchemy_sqlite_initialization,
    ("sqlalchemy", "sqlite", "write_heavy"): sqlalchemy_sqlite_write_heavy,
    ("sqlalchemy", "sqlite", "read_heavy"): sqlalchemy_sqlite_read_heavy,
    ("sqlalchemy", "sqlite", "iterative_inserts"): sqlalchemy_sqlite_iterative_inserts,
    ("sqlalchemy", "sqlite", "repeated_queries"): sqlalchemy_sqlite_repeated_queries,
    # DuckDB scenarios
    ("raw", "duckdb", "initialization"): raw_duckdb_initialization,
    ("raw", "duckdb", "write_heavy"): raw_duckdb_write_heavy,
    ("raw", "duckdb", "read_heavy"): raw_duckdb_read_heavy,
    ("raw", "duckdb", "iterative_inserts"): raw_duckdb_iterative_inserts,
    ("raw", "duckdb", "repeated_queries"): raw_duckdb_repeated_queries,
    ("sqlspec", "duckdb", "initialization"): sqlspec_duckdb_initialization,
    ("sqlspec", "duckdb", "write_heavy"): sqlspec_duckdb_write_heavy,
    ("sqlspec", "duckdb", "read_heavy"): sqlspec_duckdb_read_heavy,
    ("sqlspec", "duckdb", "iterative_inserts"): sqlspec_duckdb_iterative_inserts,
    ("sqlspec", "duckdb", "repeated_queries"): sqlspec_duckdb_repeated_queries,
    ("sqlalchemy", "duckdb", "initialization"): sqlalchemy_duckdb_initialization,
    ("sqlalchemy", "duckdb", "write_heavy"): sqlalchemy_duckdb_write_heavy,
    ("sqlalchemy", "duckdb", "read_heavy"): sqlalchemy_duckdb_read_heavy,
    ("sqlalchemy", "duckdb", "iterative_inserts"): sqlalchemy_duckdb_iterative_inserts,
    ("sqlalchemy", "duckdb", "repeated_queries"): sqlalchemy_duckdb_repeated_queries,
    # Aiosqlite scenarios
    ("raw", "aiosqlite", "initialization"): raw_aiosqlite_initialization,
    ("raw", "aiosqlite", "write_heavy"): raw_aiosqlite_write_heavy,
    ("raw", "aiosqlite", "read_heavy"): raw_aiosqlite_read_heavy,
    ("raw", "aiosqlite", "iterative_inserts"): raw_aiosqlite_iterative_inserts,
    ("raw", "aiosqlite", "repeated_queries"): raw_aiosqlite_repeated_queries,
    ("sqlspec", "aiosqlite", "initialization"): sqlspec_aiosqlite_initialization,
    ("sqlspec", "aiosqlite", "write_heavy"): sqlspec_aiosqlite_write_heavy,
    ("sqlspec", "aiosqlite", "read_heavy"): sqlspec_aiosqlite_read_heavy,
    ("sqlspec", "aiosqlite", "iterative_inserts"): sqlspec_aiosqlite_iterative_inserts,
    ("sqlspec", "aiosqlite", "repeated_queries"): sqlspec_aiosqlite_repeated_queries,
    ("sqlalchemy", "aiosqlite", "initialization"): sqlalchemy_aiosqlite_initialization,
    ("sqlalchemy", "aiosqlite", "write_heavy"): sqlalchemy_aiosqlite_write_heavy,
    ("sqlalchemy", "aiosqlite", "read_heavy"): sqlalchemy_aiosqlite_read_heavy,
    ("sqlalchemy", "aiosqlite", "iterative_inserts"): sqlalchemy_aiosqlite_iterative_inserts,
    ("sqlalchemy", "aiosqlite", "repeated_queries"): sqlalchemy_aiosqlite_repeated_queries,
    # Asyncpg scenarios
    ("raw", "asyncpg", "initialization"): raw_asyncpg_initialization,
    ("raw", "asyncpg", "write_heavy"): raw_asyncpg_write_heavy,
    ("raw", "asyncpg", "read_heavy"): raw_asyncpg_read_heavy,
    ("raw", "asyncpg", "iterative_inserts"): raw_asyncpg_iterative_inserts,
    ("raw", "asyncpg", "repeated_queries"): raw_asyncpg_repeated_queries,
    ("sqlspec", "asyncpg", "initialization"): sqlspec_asyncpg_initialization,
    ("sqlspec", "asyncpg", "write_heavy"): sqlspec_asyncpg_write_heavy,
    ("sqlspec", "asyncpg", "read_heavy"): sqlspec_asyncpg_read_heavy,
    ("sqlspec", "asyncpg", "iterative_inserts"): sqlspec_asyncpg_iterative_inserts,
    ("sqlspec", "asyncpg", "repeated_queries"): sqlspec_asyncpg_repeated_queries,
    ("sqlalchemy", "asyncpg", "initialization"): sqlalchemy_asyncpg_initialization,
    ("sqlalchemy", "asyncpg", "write_heavy"): sqlalchemy_asyncpg_write_heavy,
    ("sqlalchemy", "asyncpg", "read_heavy"): sqlalchemy_asyncpg_read_heavy,
    ("sqlalchemy", "asyncpg", "iterative_inserts"): sqlalchemy_asyncpg_iterative_inserts,
    ("sqlalchemy", "asyncpg", "repeated_queries"): sqlalchemy_asyncpg_repeated_queries,
    # Extended SQLite scenarios (raw vs sqlspec only)
    ("raw", "sqlite", "dict_key_transform"): raw_sqlite_dict_key_transform,
    ("sqlspec", "sqlite", "dict_key_transform"): sqlspec_sqlite_dict_key_transform,
    ("raw", "sqlite", "schema_mapping"): raw_sqlite_schema_mapping,
    ("sqlspec", "sqlite", "schema_mapping"): sqlspec_sqlite_schema_mapping,
    ("raw", "sqlite", "complex_parameters"): raw_sqlite_complex_parameters,
    ("sqlspec", "sqlite", "complex_parameters"): sqlspec_sqlite_complex_parameters,
    ("raw", "sqlite", "thin_path_stress"): raw_sqlite_thin_path_stress,
    ("sqlspec", "sqlite", "thin_path_stress"): sqlspec_sqlite_thin_path_stress,
}
_register_pr_c_driver_scenarios(SCENARIO_REGISTRY)


def print_benchmark_table(results: list[dict[str, Any]]) -> None:
    console = Console()
    table = Table(title="Benchmark Results")
    table.add_column("Driver", style="cyan", no_wrap=True)
    table.add_column("Library", style="magenta")
    table.add_column("Scenario", style="green")
    table.add_column("Time (s)", justify="right", style="yellow")
    table.add_column("Stddev", justify="right")
    table.add_column("IQR", justify="right")
    table.add_column("Noise", justify="center")
    table.add_column("% Slower vs Raw", justify="right", style="red")

    # Check if any result has multiple iterations
    multi_iter = any(len(row.get("times", [])) > 1 for row in results)

    # Build a lookup for raw times: {(driver, scenario): time}
    raw_times: dict[tuple[str, str], float] = {}
    for row in results:
        if row["library"] == "raw":
            raw_times[row["driver"], row["scenario"]] = row["time"]

    for row in results:
        driver = row["driver"]
        scenario = row["scenario"]
        lib = row["library"]
        t = row["time"]
        times = row.get("times", [t])
        stddev = float(row.get("stddev", 0.0))
        iqr = float(row.get("iqr", 0.0))
        noise = "yes" if row.get("noisy", False) else ""
        if lib == "raw":
            percent_slower = "---"
        else:
            raw_time = raw_times.get((driver, scenario))
            percent_slower = f"{100 * (t - raw_time) / raw_time:.1f}%" if raw_time and raw_time > 0 else "n/a"
        time_str = f"{t:.4f} ({min(times):.4f}-{max(times):.4f})" if multi_iter and len(times) > 1 else f"{t:.4f}"
        table.add_row(driver, lib, scenario, time_str, f"{stddev:.4f}", f"{iqr:.4f}", noise, percent_slower)
    console.print(table)


if __name__ == "__main__":
    main()
