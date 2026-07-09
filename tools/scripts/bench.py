"""Benchmark script for comparing sqlspec vs raw drivers vs SQLAlchemy.

Originally contributed by euri10 (Benoit Barthelet) in PR #354.
"""

import asyncio
import cProfile
import gc
import importlib
import inspect
import json
import os
import pstats
import sqlite3
import statistics
import tempfile
import time
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any, NoReturn, TypedDict
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import anyio
import click
from rich.console import Console
from rich.table import Table

from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.utils.schema import _convert_numpy_recursive, to_schema, transform_dict_keys
from sqlspec.utils.text import camelize

__all__ = (
    "main",
    "print_benchmark_table",
    "raw_adbc_rows",
    "raw_aiosqlite_worker_hops",
    "raw_asyncpg_initialization",
    "raw_asyncpg_iterative_inserts",
    "raw_asyncpg_read_heavy",
    "raw_asyncpg_repeated_queries",
    "raw_asyncpg_rows",
    "raw_asyncpg_write_heavy",
    "raw_cockroach_asyncpg_rows",
    "raw_cockroach_psycopg_async_rows",
    "raw_cockroach_psycopg_sync_rows",
    "raw_duckdb_bulk",
    "raw_duckdb_initialization",
    "raw_duckdb_iterative_inserts",
    "raw_duckdb_read_heavy",
    "raw_duckdb_repeated_queries",
    "raw_duckdb_write_heavy",
    "raw_mysqlconnector_json_rows",
    "raw_oracle_lob_fetch_1k",
    "raw_oracle_lob_fetch_100k",
    "raw_psycopg_async_rows",
    "raw_psycopg_sync_rows",
    "raw_spanner_strings",
    "raw_sqlite_complex_parameters",
    "raw_sqlite_dict_key_transform",
    "raw_sqlite_initialization",
    "raw_sqlite_iterative_inserts",
    "raw_sqlite_read_heavy",
    "raw_sqlite_repeated_queries",
    "raw_sqlite_schema_mapping",
    "raw_sqlite_schema_type_numpy",
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
    "sqlspec_adbc_rows",
    "sqlspec_aiosqlite_worker_hops",
    "sqlspec_asyncpg_initialization",
    "sqlspec_asyncpg_iterative_inserts",
    "sqlspec_asyncpg_read_heavy",
    "sqlspec_asyncpg_repeated_queries",
    "sqlspec_asyncpg_rows",
    "sqlspec_asyncpg_write_heavy",
    "sqlspec_cockroach_asyncpg_rows",
    "sqlspec_cockroach_psycopg_async_rows",
    "sqlspec_cockroach_psycopg_sync_rows",
    "sqlspec_duckdb_bulk",
    "sqlspec_duckdb_initialization",
    "sqlspec_duckdb_iterative_inserts",
    "sqlspec_duckdb_read_heavy",
    "sqlspec_duckdb_repeated_queries",
    "sqlspec_duckdb_write_heavy",
    "sqlspec_mysqlconnector_json_rows",
    "sqlspec_oracle_lob_fetch_1k",
    "sqlspec_oracle_lob_fetch_100k",
    "sqlspec_oracle_lob_fetch_async_1k",
    "sqlspec_oracle_lob_fetch_async_100k",
    "sqlspec_oracle_lob_fetch_async_fetch_lobs_true_1k",
    "sqlspec_oracle_lob_fetch_async_fetch_lobs_true_100k",
    "sqlspec_oracle_lob_fetch_fetch_lobs_true_1k",
    "sqlspec_oracle_lob_fetch_fetch_lobs_true_100k",
    "sqlspec_psycopg_async_rows",
    "sqlspec_psycopg_sync_rows",
    "sqlspec_spanner_strings",
    "sqlspec_sqlite_complex_parameters",
    "sqlspec_sqlite_dict_key_transform",
    "sqlspec_sqlite_initialization",
    "sqlspec_sqlite_iterative_inserts",
    "sqlspec_sqlite_read_heavy",
    "sqlspec_sqlite_repeated_queries",
    "sqlspec_sqlite_schema_mapping",
    "sqlspec_sqlite_schema_type_numpy",
    "sqlspec_sqlite_thin_path_stress",
    "sqlspec_sqlite_write_heavy",
)

# Pool leak detection helper
_leaked_pools: list[str] = []


class BenchmarkUnavailableError(RuntimeError):
    """Signal that an optional benchmark dependency or service is unavailable."""


def _benchmark_unavailable() -> NoReturn:
    raise BenchmarkUnavailableError


def _is_compiled() -> bool:
    """Detect if sqlspec driver modules are mypyc-compiled."""
    try:
        from sqlspec.driver import _sync

        return hasattr(_sync, "__file__") and (_sync.__file__ or "").endswith(".so")
    except ImportError:
        return False


SQLSPEC_LABEL = "sqlspec (mypyc)" if _is_compiled() else "sqlspec"


ROWS_TO_INSERT = 10_000
POOL_SIZE = 1  # Match the single raw connection used by each benchmark scenario
DEFAULT_BENCH_ITERATIONS = 7
DEFAULT_BENCH_WARMUP = 3
NOISY_STDDEV_RATIO = 0.10
CORE_LIBRARIES = ("raw", "sqlspec", "sqlalchemy")
CORE_SCENARIOS = ("initialization", "write_heavy", "read_heavy", "iterative_inserts", "repeated_queries")
ORACLE_LOB_ROWS = 100
ORACLE_LOB_PAYLOAD_SIZES = {"1k": 1024, "100k": 100 * 1024}
ORACLE_LOB_ENV_VARS = (
    "SQLSPEC_BENCH_ORACLE_HOST",
    "SQLSPEC_BENCH_ORACLE_PORT",
    "SQLSPEC_BENCH_ORACLE_SERVICE_NAME",
    "SQLSPEC_BENCH_ORACLE_USER",
    "SQLSPEC_BENCH_ORACLE_PASSWORD",
)
POSTGRES_DSN_ENV = "SQLSPEC_BENCH_POSTGRES_DSN"
COCKROACH_DSN_ENV = "SQLSPEC_BENCH_COCKROACH_DSN"
SQLITE_EXTENDED_SCENARIOS = (
    ("raw", "dict_key_transform"),
    ("sqlspec", "dict_key_transform"),
    ("raw", "schema_mapping"),
    ("sqlspec", "schema_mapping"),
    ("raw", "complex_parameters"),
    ("sqlspec", "complex_parameters"),
    ("raw", "thin_path_stress"),
    ("sqlspec", "thin_path_stress"),
    ("raw", "schema_type_numpy"),
    ("sqlspec", "schema_type_numpy"),
)
ORACLE_EXTENDED_SCENARIOS = (
    ("raw", "lob_fetch_1k"),
    ("sqlspec", "lob_fetch_1k"),
    ("sqlspec_fetch_lobs_true", "lob_fetch_1k"),
    ("sqlspec_async", "lob_fetch_1k"),
    ("sqlspec_async_fetch_lobs_true", "lob_fetch_1k"),
    ("raw", "lob_fetch_100k"),
    ("sqlspec", "lob_fetch_100k"),
    ("sqlspec_fetch_lobs_true", "lob_fetch_100k"),
    ("sqlspec_async", "lob_fetch_100k"),
    ("sqlspec_async_fetch_lobs_true", "lob_fetch_100k"),
)
AIOSQLITE_EXTENDED_SCENARIOS = (("raw", "worker_hops"), ("sqlspec", "worker_hops"))
SPANNER_EXTENDED_SCENARIOS = (("raw", "strings"), ("sqlspec", "strings"))
MYSQLCONNECTOR_EXTENDED_SCENARIOS = (("raw", "json_rows"), ("sqlspec", "json_rows"))
ADBC_EXTENDED_SCENARIOS = (("raw", "rows"), ("sqlspec", "rows"))
DUCKDB_EXTENDED_SCENARIOS = (("raw", "bulk"), ("sqlspec", "bulk"))
SERVICE_ROWS_SCENARIOS = (("raw", "rows"), ("sqlspec", "rows"))
EXTENDED_SCENARIOS_BY_DRIVER = {
    "sqlite": SQLITE_EXTENDED_SCENARIOS,
    "aiosqlite": AIOSQLITE_EXTENDED_SCENARIOS,
    "spanner": SPANNER_EXTENDED_SCENARIOS,
    "mysqlconnector": MYSQLCONNECTOR_EXTENDED_SCENARIOS,
    "adbc": ADBC_EXTENDED_SCENARIOS,
    "duckdb": DUCKDB_EXTENDED_SCENARIOS,
    "oracle": ORACLE_EXTENDED_SCENARIOS,
    "psycopg_sync": SERVICE_ROWS_SCENARIOS,
    "psycopg_async": SERVICE_ROWS_SCENARIOS,
    "asyncpg": SERVICE_ROWS_SCENARIOS,
    "cockroach_psycopg_sync": SERVICE_ROWS_SCENARIOS,
    "cockroach_psycopg_async": SERVICE_ROWS_SCENARIOS,
    "cockroach_asyncpg": SERVICE_ROWS_SCENARIOS,
}


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
    if _is_compiled():
        click.secho("mypyc compilation detected", fg="green")
    for drv in driver:
        click.echo(
            f"Running benchmark for driver: {drv} "
            f"(rows={rows}, pool_size={pool_size}, iterations={iterations}, warmup={warmup})"
        )
        has_core_scenarios = _driver_has_core_scenarios(drv)
        if profile and has_core_scenarios:
            results.extend(
                run_benchmark_profiled(
                    drv, errors, iterations=iterations, warmup=warmup, profile_scenario=profile_scenario
                )
            )
        elif has_core_scenarios or not extended:
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
            click.secho(f"Error: {err}", fg="red")
    if _leaked_pools:
        click.secho("Pool leaks detected:", fg="yellow")
        for leak in _leaked_pools:
            click.secho(f"  - {leak}", fg="yellow")
        _leaked_pools.clear()
    if errors:
        raise click.exceptions.Exit(1)
    click.echo(f"Benchmarks complete for drivers: {', '.join(driver)}")


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


def _benchmark_library_label(library: str) -> str:
    """Return a display label for a benchmark library key."""
    if library == "sqlspec":
        return SQLSPEC_LABEL
    if library == "sqlspec_fetch_lobs_true":
        return f"{SQLSPEC_LABEL} fetch_lobs=True"
    if library == "sqlspec_async":
        return f"{SQLSPEC_LABEL} async"
    if library == "sqlspec_async_fetch_lobs_true":
        return f"{SQLSPEC_LABEL} async fetch_lobs=True"
    return library


def _driver_has_core_scenarios(driver: str) -> bool:
    """Return True when a driver has any core benchmark scenario registered."""
    return any(
        (library, driver, scenario) in SCENARIO_REGISTRY for scenario in CORE_SCENARIOS for library in CORE_LIBRARIES
    )


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
    results: list[dict[str, Any]] = []

    for scenario in CORE_SCENARIOS:
        for lib in CORE_LIBRARIES:
            func = SCENARIO_REGISTRY.get((lib, driver, scenario))
            if func is None:
                errors.append(f"No implementation for library={lib}, driver={driver}, scenario={scenario}")
                continue

            is_async = inspect.iscoroutinefunction(func)

            try:
                times = _run_benchmark_iterations(func, is_async=is_async, iterations=iterations, warmup=warmup)
                stats = _summarize_times(times)
                label = _benchmark_library_label(lib)
                results.append({
                    "driver": driver,
                    "library": label,
                    "library_key": lib,
                    "scenario": scenario,
                    "times": times,
                    **stats,
                })
            except BenchmarkUnavailableError:
                continue
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

    results: list[dict[str, Any]] = []
    console = Console()

    for scenario in CORE_SCENARIOS:
        for lib in CORE_LIBRARIES:
            # If profiling a specific scenario, skip others
            if profile_scenario and scenario != profile_scenario:
                continue

            func = SCENARIO_REGISTRY.get((lib, driver, scenario))
            if func is None:
                errors.append(f"No implementation for library={lib}, driver={driver}, scenario={scenario}")
                continue

            is_async = inspect.iscoroutinefunction(func)
            prof_name = f"{driver}_{lib}_{scenario}"

            try:
                profiler = cProfile.Profile()
                times = _run_benchmark_iterations(
                    func, is_async=is_async, iterations=iterations, warmup=warmup, profiler=profiler
                )
                summary = _summarize_times(times)
                results.append({
                    "driver": driver,
                    "library": _benchmark_library_label(lib),
                    "library_key": lib,
                    "scenario": scenario,
                    "times": times,
                    **summary,
                })

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

            except BenchmarkUnavailableError:
                continue
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
    scenario_entries = EXTENDED_SCENARIOS_BY_DRIVER.get(driver, ())
    results: list[dict[str, Any]] = []

    for lib, scenario in scenario_entries:
        func = SCENARIO_REGISTRY.get((lib, driver, scenario))
        if func is None:
            errors.append(f"No extended implementation for library={lib}, driver={driver}, scenario={scenario}")
            continue

        is_async = inspect.iscoroutinefunction(func)

        try:
            times = _run_benchmark_iterations(func, is_async=is_async, iterations=iterations, warmup=warmup)
            stats = _summarize_times(times)
            label = _benchmark_library_label(lib)
            results.append({
                "driver": driver,
                "library": label,
                "library_key": lib,
                "scenario": scenario,
                "times": times,
                **stats,
            })
        except BenchmarkUnavailableError:
            continue
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
                "library_key": r.get("library_key", r["library"]),
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


def raw_duckdb_bulk() -> None:
    """Insert a batch of rows through the native DuckDB driver."""
    duckdb = _get_duckdb()
    if duckdb is None:
        _benchmark_unavailable()
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(DUCKDB_BULK_CREATE)
        conn.executemany(DUCKDB_BULK_INSERT, [(index, f"value_{index}") for index in range(BENCHMARK_ROWS)])
        result = conn.execute(DUCKDB_BULK_COUNT).fetchone()
        assert result is not None
        assert result[0] == BENCHMARK_ROWS
    finally:
        conn.close()


def sqlspec_duckdb_bulk() -> None:
    """Insert a batch of rows through SQLSpec's DuckDB bulk path."""
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    tmp_path.unlink()
    config = DuckDBConfig(connection_config={"database": str(tmp_path)})
    try:
        spec = SQLSpec()
        with spec.provide_session(config) as session:
            session.execute(DUCKDB_BULK_CREATE)
            session.execute_many(DUCKDB_BULK_INSERT, [(index, f"value_{index}") for index in range(BENCHMARK_ROWS)])
            result = session.fetch_one(DUCKDB_BULK_COUNT)
            assert result is not None
            assert (
                result[0]
                if isinstance(result, tuple)
                else result["count_star()"]
                if "count_star()" in result
                else next(iter(result.values()))
            ) == BENCHMARK_ROWS
    finally:
        config.close_pool()
        with suppress(OSError):
            tmp_path.unlink()


def _get_adbc_dbapi() -> Any:
    """Import the ADBC DB-API lazily."""
    try:
        from adbc_driver_manager import dbapi
    except ImportError:
        return None
    else:
        return dbapi


def _get_adbc_config() -> Any:
    """Import AdbcConfig lazily."""
    try:
        from sqlspec.adapters.adbc import AdbcConfig
    except ImportError:
        return None
    else:
        return AdbcConfig


def _adbc_benchmark_connection_config() -> dict[str, Any] | None:
    """Build an ADBC benchmark connection from service-provided environment values."""
    uri = os.environ.get("SQLSPEC_BENCH_ADBC_URI")
    driver_name = os.environ.get("SQLSPEC_BENCH_ADBC_DRIVER_NAME")
    if not uri or not driver_name:
        return None
    config: dict[str, Any] = {"uri": uri, "driver_name": driver_name}
    for env_name, config_name in (
        ("SQLSPEC_BENCH_ADBC_USERNAME", "username"),
        ("SQLSPEC_BENCH_ADBC_PASSWORD", "password"),
        ("SQLSPEC_BENCH_ADBC_GIZMOSQL_BACKEND", "gizmosql_backend"),
    ):
        value = os.environ.get(env_name)
        if value:
            config[config_name] = value
    if os.environ.get("SQLSPEC_BENCH_ADBC_TLS_SKIP_VERIFY") == "1":
        config["tls_skip_verify"] = True
    return config


def _adbc_benchmark_rows() -> list[tuple[int, str]]:
    """Return the stable row payload used by the ADBC benchmark."""
    return [(index, f"value_{index}") for index in range(BENCHMARK_ROWS)]


def _connect_adbc(dbapi: Any, config: dict[str, Any]) -> Any:
    driver_name = config["driver_name"]
    if driver_name in {"adbc_driver_sqlite", "adbc_driver_duckdb"}:
        module = importlib.import_module(f"{driver_name}.dbapi")
        return module.connect(uri=config["uri"], autocommit=True)
    db_kwargs = {
        key: config[key] for key in ("username", "password", "gizmosql_backend", "tls_skip_verify") if key in config
    }
    return dbapi.connect(driver=driver_name, uri=config["uri"], db_kwargs=db_kwargs, autocommit=True)


def raw_adbc_rows() -> None:
    """Fetch rows through the native ADBC DB-API path."""
    dbapi = _get_adbc_dbapi()
    config = _adbc_benchmark_connection_config()
    if dbapi is None or config is None:
        _benchmark_unavailable()
    with _connect_adbc(dbapi, config) as connection, connection.cursor() as cursor:
        cursor.execute(ADBC_ROWS_CREATE)
        cursor.execute(ADBC_ROWS_TRUNCATE)
        cursor.executemany(ADBC_ROWS_INSERT, _adbc_benchmark_rows())
        cursor.execute(ADBC_ROWS_SELECT)
        rows = cursor.fetchall()
    assert len(rows) == BENCHMARK_ROWS


def sqlspec_adbc_rows() -> None:
    """Fetch rows through SQLSpec's ADBC Arrow-backed path."""
    AdbcConfig = _get_adbc_config()  # noqa: N806
    config_values = _adbc_benchmark_connection_config()
    if AdbcConfig is None or config_values is None:
        _benchmark_unavailable()
    config = AdbcConfig(connection_config=config_values)
    try:
        with config.provide_session() as session:
            session.execute(ADBC_ROWS_CREATE)
            session.execute(ADBC_ROWS_TRUNCATE)
            session.execute_many(ADBC_ROWS_INSERT, _adbc_benchmark_rows())
            rows = session.fetch(ADBC_ROWS_SELECT)
            assert len(rows) == BENCHMARK_ROWS
    finally:
        config.close_pool()


def _get_mysql_connector() -> Any:
    """Import mysql-connector lazily."""
    try:
        import mysql.connector
    except ImportError:
        return None
    else:
        return mysql.connector


def _get_mysql_connector_config() -> Any:
    """Import MysqlConnectorSyncConfig lazily."""
    try:
        from sqlspec.adapters.mysqlconnector import MysqlConnectorSyncConfig
    except ImportError:
        return None
    else:
        return MysqlConnectorSyncConfig


def _mysql_benchmark_connection_config() -> dict[str, Any] | None:
    """Build a MySQL benchmark connection from service-provided environment values."""
    required: dict[str, Any] = {
        "host": os.environ.get("SQLSPEC_BENCH_MYSQL_HOST"),
        "port": os.environ.get("SQLSPEC_BENCH_MYSQL_PORT"),
        "user": os.environ.get("SQLSPEC_BENCH_MYSQL_USER"),
        "password": os.environ.get("SQLSPEC_BENCH_MYSQL_PASSWORD"),
        "database": os.environ.get("SQLSPEC_BENCH_MYSQL_DATABASE"),
    }
    if any(value is None for value in required.values()):
        return None
    required["port"] = int(required["port"])
    required["use_pure"] = True
    required["autocommit"] = True
    return required


def _json_benchmark_rows() -> list[tuple[int, str]]:
    """Return the stable JSON payload used by the MySQL benchmark."""
    return [(index, json.dumps({"index": index, "values": [index, index + 1]})) for index in range(BENCHMARK_ROWS)]


def _json_row_payload(row: Any) -> Any:
    payload = row.get("payload") if isinstance(row, dict) else row[1]
    return json.loads(payload) if isinstance(payload, str) else payload


def raw_mysqlconnector_json_rows() -> None:
    """Fetch JSON rows through the native MySQL connector."""
    connector = _get_mysql_connector()
    connection_config = _mysql_benchmark_connection_config()
    if connector is None or connection_config is None:
        _benchmark_unavailable()
    connection = connector.connect(**connection_config)
    try:
        with connection.cursor() as cursor:
            cursor.execute(MYSQL_JSON_CREATE)
            cursor.execute(f"TRUNCATE TABLE {MYSQL_JSON_TABLE}")
            cursor.executemany(MYSQL_JSON_INSERT_RAW, _json_benchmark_rows())
            cursor.execute(MYSQL_JSON_SELECT)
            rows = cursor.fetchall()
    finally:
        connection.close()
    assert len(rows) == BENCHMARK_ROWS
    assert _json_row_payload(rows[0])["values"] == [0, 1]


def sqlspec_mysqlconnector_json_rows() -> None:
    """Fetch JSON rows through SQLSpec's MySQL connector path."""
    Config = _get_mysql_connector_config()  # noqa: N806
    connection_config = _mysql_benchmark_connection_config()
    if Config is None or connection_config is None:
        _benchmark_unavailable()
    pool_config = {**connection_config, "pool_size": 1}
    config = Config(connection_config=pool_config, driver_features={"json_deserializer": json.loads})
    try:
        with config.provide_session() as session:
            session.execute(MYSQL_JSON_CREATE)
            session.execute(f"TRUNCATE TABLE {MYSQL_JSON_TABLE}")
            session.execute_many(MYSQL_JSON_INSERT, _json_benchmark_rows())
            rows = session.fetch(MYSQL_JSON_SELECT)
    finally:
        config.close_pool()
    assert len(rows) == BENCHMARK_ROWS
    assert _json_row_payload(rows[0])["values"] == [0, 1]


def _spanner_benchmark_config() -> dict[str, Any] | None:
    """Build a Spanner benchmark connection from service-provided environment values."""
    required: dict[str, Any] = {
        "project": os.environ.get("SQLSPEC_BENCH_SPANNER_PROJECT"),
        "instance_id": os.environ.get("SQLSPEC_BENCH_SPANNER_INSTANCE_ID"),
        "database_id": os.environ.get("SQLSPEC_BENCH_SPANNER_DATABASE_ID"),
        "api_endpoint": os.environ.get("SQLSPEC_BENCH_SPANNER_API_ENDPOINT"),
    }
    if any(value is None for value in required.values()):
        return None
    return required


def raw_spanner_strings() -> None:
    """Fetch string rows through the native Google Cloud Spanner client."""
    try:
        from google.auth.credentials import AnonymousCredentials
        from google.cloud import spanner
    except ImportError:
        _benchmark_unavailable()
    connection_config = _spanner_benchmark_config()
    if connection_config is None:
        _benchmark_unavailable()
    client = spanner.Client(
        project=connection_config["project"],
        credentials=AnonymousCredentials(),  # type: ignore[no-untyped-call]
        client_options={"api_endpoint": connection_config["api_endpoint"]},
    )
    try:
        database = client.instance(connection_config["instance_id"]).database(connection_config["database_id"])  # type: ignore[no-untyped-call]
        with database.snapshot() as snapshot:
            rows = list(snapshot.execute_sql(SPANNER_STRINGS_SELECT))
    finally:
        client.close()  # type: ignore[no-untyped-call]
    assert len(rows) == BENCHMARK_ROWS


def sqlspec_spanner_strings() -> None:
    """Fetch string rows through SQLSpec's Spanner column-plan path."""
    try:
        from sqlspec.adapters.spanner import SpannerSyncConfig
    except ImportError:
        _benchmark_unavailable()
    connection_config = _spanner_benchmark_config()
    if connection_config is None:
        _benchmark_unavailable()
    config = SpannerSyncConfig(
        connection_config={
            "project": connection_config["project"],
            "instance_id": connection_config["instance_id"],
            "database_id": connection_config["database_id"],
            "client_options": {"api_endpoint": connection_config["api_endpoint"]},
            "size": POOL_SIZE,
        }
    )
    try:
        with config.provide_read_session() as session:
            rows = session.fetch(SPANNER_STRINGS_SELECT)
    finally:
        config.close_pool()
    assert len(rows) == BENCHMARK_ROWS


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


async def raw_aiosqlite_worker_hops() -> None:
    aiosqlite = _get_aiosqlite()
    if aiosqlite is None:
        return
    async with aiosqlite.connect(":memory:") as conn:
        for i in range(ROWS_TO_INSERT):
            cursor = await conn.execute("SELECT ?", (i,))
            row = await cursor.fetchone()
            assert row[0] == i


async def sqlspec_aiosqlite_worker_hops() -> None:
    AiosqliteConfig = _get_aiosqlite_config()  # noqa: N806
    if AiosqliteConfig is None:
        return
    spec = SQLSpec()
    config = AiosqliteConfig(database=":memory:", pool_size=1)
    try:
        async with spec.provide_session(config) as session:
            for i in range(ROWS_TO_INSERT):
                value = await session.select_value("SELECT ?", i)
                assert value == i
        _check_pool_leak(config.connection_instance, "aiosqlite/worker_hops")
        await config.close_pool()
    finally:
        if config.connection_instance is not None:
            await config.close_pool()


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


def _postgres_benchmark_dsn() -> str:
    dsn = os.environ.get(POSTGRES_DSN_ENV)
    if not dsn:
        _benchmark_unavailable()
    return dsn


def _cockroach_benchmark_dsn() -> str:
    dsn = os.environ.get(COCKROACH_DSN_ENV)
    if not dsn:
        _benchmark_unavailable()
    return dsn


def _asyncpg_connect_kwargs(dsn: str) -> dict[str, Any]:
    """Adapt a service DSN to asyncpg's keyword set."""
    parsed = urlsplit(dsn)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    sslmode = query.pop("sslmode", None)
    clean_dsn = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))
    kwargs: dict[str, Any] = {"dsn": clean_dsn}
    if sslmode == "disable":
        kwargs["ssl"] = False
    return kwargs


async def raw_asyncpg_initialization() -> None:
    connect = _get_asyncpg()
    if connect is None:
        _benchmark_unavailable()
    conn = await connect(**_asyncpg_connect_kwargs(_postgres_benchmark_dsn()))
    await conn.execute(DROP_TEST_TABLE)
    await conn.execute(CREATE_TEST_TABLE)
    await conn.close()


async def raw_asyncpg_write_heavy() -> None:
    connect = _get_asyncpg()
    if connect is None:
        _benchmark_unavailable()
    conn = await connect(**_asyncpg_connect_kwargs(_postgres_benchmark_dsn()))
    await conn.execute(DROP_TEST_TABLE)
    await conn.execute(CREATE_TEST_TABLE)
    # Use executemany for fair comparison
    data = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
    await conn.executemany(INSERT_TEST_VALUE_ASYNCPG, data)
    await conn.close()


async def raw_asyncpg_read_heavy() -> None:
    connect = _get_asyncpg()
    if connect is None:
        _benchmark_unavailable()
    conn = await connect(**_asyncpg_connect_kwargs(_postgres_benchmark_dsn()))
    rows = await conn.fetch(SELECT_TEST_VALUES)
    assert len(rows) == ROWS_TO_INSERT
    await conn.close()


async def raw_asyncpg_iterative_inserts() -> None:
    connect = _get_asyncpg()
    if connect is None:
        _benchmark_unavailable()
    conn = await connect(**_asyncpg_connect_kwargs(_postgres_benchmark_dsn()))
    try:
        await conn.execute(DROP_TEST_TABLE)
        await conn.execute(CREATE_TEST_TABLE)
        for i in range(ROWS_TO_INSERT):
            await conn.execute(INSERT_TEST_VALUE_ASYNCPG, f"value_{i}")
    finally:
        await conn.close()


async def raw_asyncpg_repeated_queries() -> None:
    connect = _get_asyncpg()
    if connect is None:
        _benchmark_unavailable()
    conn = await connect(**_asyncpg_connect_kwargs(_postgres_benchmark_dsn()))
    try:
        await conn.execute(DROP_TEST_TABLE)
        await conn.execute(CREATE_TEST_TABLE)
        await conn.executemany(INSERT_TEST_VALUE_ASYNCPG, [(f"value_{i}",) for i in range(ROWS_TO_INSERT)])
        for i in range(ROWS_TO_INSERT):
            await conn.fetchrow(SELECT_BY_VALUE_ASYNCPG, f"value_{i % 100}")
    finally:
        await conn.close()


async def sqlspec_asyncpg_initialization() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        _benchmark_unavailable()
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": _postgres_benchmark_dsn(), "min_size": 1, "max_size": 1})
    try:
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
    finally:
        await config.close_pool()


async def sqlspec_asyncpg_write_heavy() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        _benchmark_unavailable()
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": _postgres_benchmark_dsn(), "min_size": 1, "max_size": 1})
    try:
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
            data: Sequence[tuple[str]] = [(f"value_{i}",) for i in range(ROWS_TO_INSERT)]
            await session.execute_many(INSERT_TEST_VALUE_ASYNCPG, data)
    finally:
        await config.close_pool()


async def sqlspec_asyncpg_read_heavy() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        _benchmark_unavailable()
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": _postgres_benchmark_dsn(), "min_size": 1, "max_size": 1})
    try:
        async with spec.provide_session(config) as session:
            rows = await session.fetch(SELECT_TEST_VALUES)
            assert len(rows) == ROWS_TO_INSERT
    finally:
        await config.close_pool()


async def sqlspec_asyncpg_iterative_inserts() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        _benchmark_unavailable()
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": _postgres_benchmark_dsn(), "min_size": 1, "max_size": 1})
    try:
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
            for i in range(ROWS_TO_INSERT):
                await session.execute(INSERT_TEST_VALUE_ASYNCPG, (f"value_{i}",))
    finally:
        await config.close_pool()


async def sqlspec_asyncpg_repeated_queries() -> None:
    AsyncpgConfig = _get_asyncpg_config()  # noqa: N806
    if AsyncpgConfig is None:
        _benchmark_unavailable()
    spec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": _postgres_benchmark_dsn(), "min_size": 1, "max_size": 1})
    try:
        async with spec.provide_session(config) as session:
            await session.execute(DROP_TEST_TABLE)
            await session.execute(CREATE_TEST_TABLE)
            await session.execute_many(INSERT_TEST_VALUE_ASYNCPG, [(f"value_{i}",) for i in range(ROWS_TO_INSERT)])
            for i in range(ROWS_TO_INSERT):
                await session.fetch_one_or_none(SELECT_BY_VALUE_ASYNCPG, (f"value_{i % 100}",))
    finally:
        await config.close_pool()


async def sqlalchemy_asyncpg_initialization() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        _benchmark_unavailable()
    dsn = _postgres_benchmark_dsn()
    engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://", 1))
    async with engine.connect() as conn:
        await conn.execute(text(DROP_TEST_TABLE))
        await conn.execute(text(CREATE_TEST_TABLE))
        await conn.commit()
    await engine.dispose()


async def sqlalchemy_asyncpg_write_heavy() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        _benchmark_unavailable()
    dsn = _postgres_benchmark_dsn()
    engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://", 1))
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
        _benchmark_unavailable()
    dsn = _postgres_benchmark_dsn()
    engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://", 1))
    async with engine.begin() as conn:
        result = await conn.execute(text(SELECT_TEST_VALUES))
        rows = result.fetchall()
        assert len(rows) == ROWS_TO_INSERT
    await engine.dispose()


async def sqlalchemy_asyncpg_iterative_inserts() -> None:
    create_async_engine, text = _get_async_sqlalchemy()
    if create_async_engine is None:
        _benchmark_unavailable()
    dsn = _postgres_benchmark_dsn()
    engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://", 1))
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
        _benchmark_unavailable()
    dsn = _postgres_benchmark_dsn()
    engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://", 1))
    async with engine.connect() as conn:
        await conn.execute(text(DROP_TEST_TABLE))
        await conn.execute(text(CREATE_TEST_TABLE))
        await conn.execute(text(INSERT_TEST_VALUE_SQLA), [{"value": f"value_{i}"} for i in range(ROWS_TO_INSERT)])
        await conn.commit()
        for i in range(ROWS_TO_INSERT):
            result = await conn.execute(text(SELECT_BY_VALUE_SQLA), {"value": f"value_{i % 100}"})
            result.fetchone()
    await engine.dispose()


SERVICE_ROWS_CREATE = "CREATE TABLE IF NOT EXISTS sqlspec_bench_rows (id INTEGER PRIMARY KEY, payload TEXT)"
SERVICE_ROWS_DROP = "DROP TABLE IF EXISTS sqlspec_bench_rows"
SERVICE_ROWS_INSERT_PG = "INSERT INTO sqlspec_bench_rows (id, payload) VALUES (%s, %s)"
SERVICE_ROWS_INSERT_ASYNCPG = "INSERT INTO sqlspec_bench_rows (id, payload) VALUES ($1, $2)"
SERVICE_ROWS_SELECT = "SELECT id, payload FROM sqlspec_bench_rows ORDER BY id"


def _service_benchmark_rows() -> list[tuple[int, str]]:
    return [(index, f"value_{index}") for index in range(BENCHMARK_ROWS)]


def _get_psycopg() -> Any:
    try:
        import psycopg
    except ImportError:
        return None
    else:
        return psycopg


def _run_raw_psycopg_sync_rows(dsn: str, *, cockroach: bool) -> None:
    psycopg = _get_psycopg()
    if psycopg is None:
        _benchmark_unavailable()
    if cockroach:
        from psycopg import crdb

        connect = crdb.connect
    else:
        connect = psycopg.connect
    with connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(SERVICE_ROWS_DROP)
            cursor.execute(SERVICE_ROWS_CREATE)
            cursor.executemany(SERVICE_ROWS_INSERT_PG, _service_benchmark_rows())
            cursor.execute(SERVICE_ROWS_SELECT)
            rows = cursor.fetchall()
        connection.commit()
    assert len(rows) == BENCHMARK_ROWS


def _get_sync_service_config(driver: str) -> Any:
    try:
        if driver == "psycopg_sync":
            from sqlspec.adapters.psycopg import PsycopgSyncConfig

            return PsycopgSyncConfig
        from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgSyncConfig
    except ImportError:
        return None
    else:
        return CockroachPsycopgSyncConfig


def _run_sqlspec_sync_service_rows(dsn: str, *, driver: str) -> None:
    config_class = _get_sync_service_config(driver)
    if config_class is None:
        _benchmark_unavailable()
    config = config_class(connection_config={"conninfo": dsn, "min_size": 1, "max_size": max(1, POOL_SIZE)})
    try:
        spec = SQLSpec()
        with spec.provide_session(config) as session:
            session.execute(SERVICE_ROWS_DROP)
            session.execute(SERVICE_ROWS_CREATE)
            session.execute_many(SERVICE_ROWS_INSERT_PG, _service_benchmark_rows())
            rows = session.fetch(SERVICE_ROWS_SELECT)
        assert len(rows) == BENCHMARK_ROWS
    finally:
        config.close_pool()


async def _run_raw_psycopg_async_rows(dsn: str) -> None:
    psycopg = _get_psycopg()
    if psycopg is None:
        _benchmark_unavailable()
    connection = await psycopg.AsyncConnection.connect(dsn)
    try:
        async with connection.cursor() as cursor:
            await cursor.execute(SERVICE_ROWS_DROP)
            await cursor.execute(SERVICE_ROWS_CREATE)
            await cursor.executemany(SERVICE_ROWS_INSERT_PG, _service_benchmark_rows())
            await cursor.execute(SERVICE_ROWS_SELECT)
            rows = await cursor.fetchall()
        await connection.commit()
    finally:
        await connection.close()
    assert len(rows) == BENCHMARK_ROWS


async def _run_raw_asyncpg_rows(dsn: str) -> None:
    connect = _get_asyncpg()
    if connect is None:
        _benchmark_unavailable()
    connection = await connect(**_asyncpg_connect_kwargs(dsn))
    try:
        await connection.execute(SERVICE_ROWS_DROP)
        await connection.execute(SERVICE_ROWS_CREATE)
        await connection.executemany(SERVICE_ROWS_INSERT_ASYNCPG, _service_benchmark_rows())
        rows = await connection.fetch(SERVICE_ROWS_SELECT)
    finally:
        await connection.close()
    assert len(rows) == BENCHMARK_ROWS


def _get_async_service_config(driver: str) -> Any:
    try:
        if driver in {"psycopg_async", "cockroach_psycopg_async"}:
            if driver == "psycopg_async":
                from sqlspec.adapters.psycopg import PsycopgAsyncConfig

                return PsycopgAsyncConfig
            from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig

            return CockroachPsycopgAsyncConfig
        if driver == "asyncpg":
            from sqlspec.adapters.asyncpg import AsyncpgConfig

            return AsyncpgConfig
        from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
    except ImportError:
        return None
    else:
        return CockroachAsyncpgConfig


async def _run_sqlspec_async_service_rows(dsn: str, *, driver: str) -> None:
    config_class = _get_async_service_config(driver)
    if config_class is None:
        _benchmark_unavailable()
    connection_key = "dsn" if "asyncpg" in driver else "conninfo"
    config = config_class(connection_config={connection_key: dsn, "min_size": 1, "max_size": max(1, POOL_SIZE)})
    try:
        spec = SQLSpec()
        async with spec.provide_session(config) as session:
            await session.execute(SERVICE_ROWS_DROP)
            await session.execute(SERVICE_ROWS_CREATE)
            insert_sql = SERVICE_ROWS_INSERT_ASYNCPG if "asyncpg" in driver else SERVICE_ROWS_INSERT_PG
            await session.execute_many(insert_sql, _service_benchmark_rows())
            rows = await session.fetch(SERVICE_ROWS_SELECT)
        assert len(rows) == BENCHMARK_ROWS
    finally:
        await config.close_pool()


def raw_psycopg_sync_rows() -> None:
    _run_raw_psycopg_sync_rows(_postgres_benchmark_dsn(), cockroach=False)


def sqlspec_psycopg_sync_rows() -> None:
    _run_sqlspec_sync_service_rows(_postgres_benchmark_dsn(), driver="psycopg_sync")


async def raw_psycopg_async_rows() -> None:
    await _run_raw_psycopg_async_rows(_postgres_benchmark_dsn())


async def sqlspec_psycopg_async_rows() -> None:
    await _run_sqlspec_async_service_rows(_postgres_benchmark_dsn(), driver="psycopg_async")


async def raw_asyncpg_rows() -> None:
    await _run_raw_asyncpg_rows(_postgres_benchmark_dsn())


async def sqlspec_asyncpg_rows() -> None:
    await _run_sqlspec_async_service_rows(_postgres_benchmark_dsn(), driver="asyncpg")


def raw_cockroach_psycopg_sync_rows() -> None:
    _run_raw_psycopg_sync_rows(_cockroach_benchmark_dsn(), cockroach=True)


def sqlspec_cockroach_psycopg_sync_rows() -> None:
    _run_sqlspec_sync_service_rows(_cockroach_benchmark_dsn(), driver="cockroach_psycopg_sync")


async def raw_cockroach_psycopg_async_rows() -> None:
    await _run_raw_psycopg_async_rows(_cockroach_benchmark_dsn())


async def sqlspec_cockroach_psycopg_async_rows() -> None:
    await _run_sqlspec_async_service_rows(_cockroach_benchmark_dsn(), driver="cockroach_psycopg_async")


async def raw_cockroach_asyncpg_rows() -> None:
    await _run_raw_asyncpg_rows(_cockroach_benchmark_dsn())


async def sqlspec_cockroach_asyncpg_rows() -> None:
    await _run_sqlspec_async_service_rows(_cockroach_benchmark_dsn(), driver="cockroach_asyncpg")


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

BENCHMARK_ROWS = 1_000
DUCKDB_BULK_TABLE = "sqlspec_bench_bulk"
DUCKDB_BULK_CREATE = f"CREATE TABLE {DUCKDB_BULK_TABLE} (id INTEGER, payload VARCHAR)"
DUCKDB_BULK_INSERT = f"INSERT INTO {DUCKDB_BULK_TABLE} (id, payload) VALUES (?, ?)"
DUCKDB_BULK_COUNT = f"SELECT COUNT(*) FROM {DUCKDB_BULK_TABLE}"
ADBC_ROWS_TABLE = "sqlspec_bench_adbc_rows"
ADBC_ROWS_CREATE = f"CREATE TABLE IF NOT EXISTS {ADBC_ROWS_TABLE} (id INTEGER, payload VARCHAR)"
ADBC_ROWS_TRUNCATE = f"DELETE FROM {ADBC_ROWS_TABLE}"
ADBC_ROWS_INSERT = f"INSERT INTO {ADBC_ROWS_TABLE} (id, payload) VALUES (?, ?)"
ADBC_ROWS_SELECT = f"SELECT id, payload FROM {ADBC_ROWS_TABLE} ORDER BY id"
MYSQL_JSON_TABLE = "sqlspec_bench_json_rows"
MYSQL_JSON_CREATE = f"CREATE TABLE IF NOT EXISTS {MYSQL_JSON_TABLE} (id INT PRIMARY KEY, payload JSON NOT NULL)"
MYSQL_JSON_INSERT_RAW = f"INSERT INTO {MYSQL_JSON_TABLE} (id, payload) VALUES (%s, %s)"
MYSQL_JSON_INSERT = f"INSERT INTO {MYSQL_JSON_TABLE} (id, payload) VALUES (?, ?)"
MYSQL_JSON_SELECT = f"SELECT id, payload FROM {MYSQL_JSON_TABLE} ORDER BY id"
SPANNER_STRINGS_TABLE = "sqlspec_bench_strings"
SPANNER_STRINGS_SELECT = f"SELECT id, payload FROM {SPANNER_STRINGS_TABLE} ORDER BY id"


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


SCHEMA_TYPE_NUMPY_ROW_COUNT = 5_000
SCHEMA_TYPE_NUMPY_FIRST_FIELD_4 = 4
_SCHEMA_TYPE_NUMPY_PAYLOAD: list[dict[str, int]] | None = None
_SCHEMA_TYPE_NUMPY_ROW_TYPE: type[Any] | None = None
_SCHEMA_TYPE_NUMPY_VECTOR_TYPE: type[Any] | None = None


def _schema_type_numpy_payload() -> list[dict[str, int]]:
    """Return the stable 5000x5 payload used by the schema_type numpy benchmark."""
    global _SCHEMA_TYPE_NUMPY_PAYLOAD
    if _SCHEMA_TYPE_NUMPY_PAYLOAD is None:
        _SCHEMA_TYPE_NUMPY_PAYLOAD = [
            {"field_0": index, "field_1": index + 1, "field_2": index + 2, "field_3": index + 3, "field_4": index + 4}
            for index in range(SCHEMA_TYPE_NUMPY_ROW_COUNT)
        ]
    return _SCHEMA_TYPE_NUMPY_PAYLOAD


def _schema_type_numpy_row_type() -> type[Any]:
    """Return the cached msgspec row type used by the schema_type numpy benchmark."""
    global _SCHEMA_TYPE_NUMPY_ROW_TYPE
    if _SCHEMA_TYPE_NUMPY_ROW_TYPE is None:
        import msgspec

        class SchemaTypeNumpyRow(msgspec.Struct):
            field_0: int
            field_1: int
            field_2: int
            field_3: int
            field_4: int

        _SCHEMA_TYPE_NUMPY_ROW_TYPE = SchemaTypeNumpyRow
    return _SCHEMA_TYPE_NUMPY_ROW_TYPE


def _schema_type_numpy_vector_type() -> type[Any]:
    """Return the cached msgspec vector row type used to assert ndarray fallback behavior."""
    global _SCHEMA_TYPE_NUMPY_VECTOR_TYPE
    if _SCHEMA_TYPE_NUMPY_VECTOR_TYPE is None:
        import msgspec

        class SchemaTypeNumpyVectorRow(msgspec.Struct):
            values: list[float]

        _SCHEMA_TYPE_NUMPY_VECTOR_TYPE = SchemaTypeNumpyVectorRow
    return _SCHEMA_TYPE_NUMPY_VECTOR_TYPE


def assert_schema_type_numpy_vector_fallback() -> None:
    """Assert ndarray -> list[float] fallback still works for msgspec schema conversion."""
    import numpy as np

    vector_type = _schema_type_numpy_vector_type()
    converted = to_schema([{"values": np.array([1.0, 2.0])}], schema_type=vector_type)
    assert len(converted) == 1
    assert converted[0].values == [1.0, 2.0]


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


# --- schema_type_numpy scenarios ---


def raw_sqlite_schema_type_numpy() -> None:
    """Legacy baseline: pre-walk a 5000x5 payload before msgspec schema conversion."""
    row_type = _schema_type_numpy_row_type()
    rows = to_schema(_convert_numpy_recursive(_schema_type_numpy_payload()), schema_type=row_type)
    assert len(rows) == SCHEMA_TYPE_NUMPY_ROW_COUNT
    assert rows[0].field_4 == SCHEMA_TYPE_NUMPY_FIRST_FIELD_4


def sqlspec_sqlite_schema_type_numpy() -> None:
    """Convert a 5000x5 payload through the current msgspec schema_type path."""
    row_type = _schema_type_numpy_row_type()
    rows = to_schema(_schema_type_numpy_payload(), schema_type=row_type)
    assert len(rows) == SCHEMA_TYPE_NUMPY_ROW_COUNT
    assert rows[0].field_4 == SCHEMA_TYPE_NUMPY_FIRST_FIELD_4


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


# --- Oracle LOB fetch scenarios ---

ORACLE_LOB_TABLES = {"1k": "SQLSPEC_LOB_1K", "100k": "SQLSPEC_LOB_100K"}


def _get_oracledb() -> Any:
    """Import python-oracledb lazily."""
    try:
        import oracledb
    except ImportError:
        return None
    else:
        return oracledb


def _oracle_connection_config_from_env() -> dict[str, Any]:
    """Build Oracle connection config from service-safe benchmark env vars."""
    missing = [name for name in ORACLE_LOB_ENV_VARS if not os.environ.get(name)]
    if missing:
        joined = ", ".join(missing)
        msg = f"Oracle benchmarks require service-derived environment variables: {joined}"
        raise RuntimeError(msg)

    return {
        "host": os.environ["SQLSPEC_BENCH_ORACLE_HOST"],
        "port": int(os.environ["SQLSPEC_BENCH_ORACLE_PORT"]),
        "service_name": os.environ["SQLSPEC_BENCH_ORACLE_SERVICE_NAME"],
        "user": os.environ["SQLSPEC_BENCH_ORACLE_USER"],
        "password": os.environ["SQLSPEC_BENCH_ORACLE_PASSWORD"],
        "min": 1,
        "max": max(1, POOL_SIZE),
        "increment": 1,
    }


def _oracle_connect_config_from_env() -> dict[str, Any]:
    config = _oracle_connection_config_from_env()
    for key in ("min", "max", "increment"):
        config.pop(key, None)
    return config


def _oracle_lob_payload(size_key: str) -> str:
    return "x" * ORACLE_LOB_PAYLOAD_SIZES[size_key]


def _oracle_lob_table(size_key: str) -> str:
    return ORACLE_LOB_TABLES[size_key]


def _oracle_drop_table_sql(table_name: str) -> str:
    return (
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name}'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def _oracle_create_lob_table_sql(table_name: str) -> str:
    return f"CREATE TABLE {table_name} (id NUMBER PRIMARY KEY, payload CLOB)"


def _oracle_insert_lob_sql(table_name: str) -> str:
    return f"INSERT INTO {table_name} (id, payload) VALUES (:1, :2)"


def _oracle_select_lob_sql(table_name: str) -> str:
    return f"SELECT id, payload FROM {table_name} ORDER BY id"


def _oracle_lob_rows(size_key: str) -> list[tuple[int, str]]:
    payload = _oracle_lob_payload(size_key)
    return [(index, payload) for index in range(1, ORACLE_LOB_ROWS + 1)]


def _read_oracle_lob_value(value: Any) -> Any:
    read = getattr(value, "read", None)
    if callable(read):
        return read()
    return value


async def _read_oracle_lob_value_async(value: Any) -> Any:
    read = getattr(value, "read", None)
    if not callable(read):
        return value
    result = read()
    if inspect.isawaitable(result):
        return await result
    return result


def _assert_oracle_lob_rows(rows: Sequence[Any], size_key: str) -> None:
    expected = _oracle_lob_payload(size_key)
    assert len(rows) == ORACLE_LOB_ROWS
    first_row = rows[0]
    payload = first_row["payload"] if isinstance(first_row, dict) else first_row[1]
    assert _read_oracle_lob_value(payload) == expected


async def _assert_oracle_lob_rows_async(rows: Sequence[Any], size_key: str) -> None:
    expected = _oracle_lob_payload(size_key)
    assert len(rows) == ORACLE_LOB_ROWS
    first_row = rows[0]
    payload = first_row["payload"] if isinstance(first_row, dict) else first_row[1]
    assert await _read_oracle_lob_value_async(payload) == expected


def _run_raw_oracle_lob_fetch(size_key: str) -> None:
    oracledb = _get_oracledb()
    if oracledb is None:
        return
    table_name = _oracle_lob_table(size_key)
    conn = oracledb.connect(**_oracle_connect_config_from_env())
    try:
        with conn.cursor() as cursor:
            cursor.execute(_oracle_drop_table_sql(table_name))
            cursor.execute(_oracle_create_lob_table_sql(table_name))
            cursor.executemany(_oracle_insert_lob_sql(table_name), _oracle_lob_rows(size_key))
            conn.commit()
            cursor.execute(_oracle_select_lob_sql(table_name))
            rows = cursor.fetchall()
            _assert_oracle_lob_rows(rows, size_key)
    finally:
        conn.close()


def _run_sqlspec_oracle_lob_fetch(size_key: str, *, fetch_lobs: bool) -> None:
    from sqlspec.adapters.oracledb import OracleSyncConfig

    table_name = _oracle_lob_table(size_key)
    spec = SQLSpec()
    config = OracleSyncConfig(
        connection_config=_oracle_connection_config_from_env(), driver_features={"fetch_lobs": fetch_lobs}
    )
    try:
        with spec.provide_session(config) as session:
            session.execute_script(_oracle_drop_table_sql(table_name))
            session.execute(_oracle_create_lob_table_sql(table_name))
            session.execute_many(_oracle_insert_lob_sql(table_name), _oracle_lob_rows(size_key))
            rows = session.fetch(_oracle_select_lob_sql(table_name))
            _assert_oracle_lob_rows(rows, size_key)
        _check_pool_leak(config.connection_instance, f"oracle/lob_fetch/{size_key}/fetch_lobs={fetch_lobs}")
        config.close_pool()
    finally:
        if config.connection_instance is not None:
            config.close_pool()


async def _run_sqlspec_oracle_lob_fetch_async(size_key: str, *, fetch_lobs: bool) -> None:
    from sqlspec.adapters.oracledb import OracleAsyncConfig

    table_name = _oracle_lob_table(size_key)
    spec = SQLSpec()
    config = OracleAsyncConfig(
        connection_config=_oracle_connection_config_from_env(), driver_features={"fetch_lobs": fetch_lobs}
    )
    try:
        async with spec.provide_session(config) as session:
            await session.execute_script(_oracle_drop_table_sql(table_name))
            await session.execute(_oracle_create_lob_table_sql(table_name))
            await session.execute_many(_oracle_insert_lob_sql(table_name), _oracle_lob_rows(size_key))
            rows = await session.fetch(_oracle_select_lob_sql(table_name))
            await _assert_oracle_lob_rows_async(rows, size_key)
        _check_pool_leak(config.connection_instance, f"oracle/lob_fetch_async/{size_key}/fetch_lobs={fetch_lobs}")
        await config.close_pool()
    finally:
        if config.connection_instance is not None:
            await config.close_pool()


def raw_oracle_lob_fetch_1k() -> None:
    """Fetch 100 1 KiB Oracle CLOB rows through raw python-oracledb."""
    _run_raw_oracle_lob_fetch("1k")


def raw_oracle_lob_fetch_100k() -> None:
    """Fetch 100 100 KiB Oracle CLOB rows through raw python-oracledb."""
    _run_raw_oracle_lob_fetch("100k")


def sqlspec_oracle_lob_fetch_1k() -> None:
    """Fetch 100 1 KiB Oracle CLOB rows with sqlspec direct LOB fetching."""
    _run_sqlspec_oracle_lob_fetch("1k", fetch_lobs=False)


def sqlspec_oracle_lob_fetch_100k() -> None:
    """Fetch 100 100 KiB Oracle CLOB rows with sqlspec direct LOB fetching."""
    _run_sqlspec_oracle_lob_fetch("100k", fetch_lobs=False)


def sqlspec_oracle_lob_fetch_fetch_lobs_true_1k() -> None:
    """Fetch 100 1 KiB Oracle CLOB rows with sqlspec LOB locators enabled."""
    _run_sqlspec_oracle_lob_fetch("1k", fetch_lobs=True)


def sqlspec_oracle_lob_fetch_fetch_lobs_true_100k() -> None:
    """Fetch 100 100 KiB Oracle CLOB rows with sqlspec LOB locators enabled."""
    _run_sqlspec_oracle_lob_fetch("100k", fetch_lobs=True)


async def sqlspec_oracle_lob_fetch_async_1k() -> None:
    """Fetch 100 1 KiB Oracle CLOB rows with async sqlspec direct LOB fetching."""
    await _run_sqlspec_oracle_lob_fetch_async("1k", fetch_lobs=False)


async def sqlspec_oracle_lob_fetch_async_100k() -> None:
    """Fetch 100 100 KiB Oracle CLOB rows with async sqlspec direct LOB fetching."""
    await _run_sqlspec_oracle_lob_fetch_async("100k", fetch_lobs=False)


async def sqlspec_oracle_lob_fetch_async_fetch_lobs_true_1k() -> None:
    """Fetch 100 1 KiB Oracle CLOB rows with async sqlspec LOB locators enabled."""
    await _run_sqlspec_oracle_lob_fetch_async("1k", fetch_lobs=True)


async def sqlspec_oracle_lob_fetch_async_fetch_lobs_true_100k() -> None:
    """Fetch 100 100 KiB Oracle CLOB rows with async sqlspec LOB locators enabled."""
    await _run_sqlspec_oracle_lob_fetch_async("100k", fetch_lobs=True)


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
    ("raw", "aiosqlite", "worker_hops"): raw_aiosqlite_worker_hops,
    ("sqlspec", "aiosqlite", "worker_hops"): sqlspec_aiosqlite_worker_hops,
    ("raw", "spanner", "strings"): raw_spanner_strings,
    ("sqlspec", "spanner", "strings"): sqlspec_spanner_strings,
    ("raw", "mysqlconnector", "json_rows"): raw_mysqlconnector_json_rows,
    ("sqlspec", "mysqlconnector", "json_rows"): sqlspec_mysqlconnector_json_rows,
    ("raw", "adbc", "rows"): raw_adbc_rows,
    ("sqlspec", "adbc", "rows"): sqlspec_adbc_rows,
    ("raw", "duckdb", "bulk"): raw_duckdb_bulk,
    ("sqlspec", "duckdb", "bulk"): sqlspec_duckdb_bulk,
    # PostgreSQL and CockroachDB service row scenarios
    ("raw", "psycopg_sync", "rows"): raw_psycopg_sync_rows,
    ("sqlspec", "psycopg_sync", "rows"): sqlspec_psycopg_sync_rows,
    ("raw", "psycopg_async", "rows"): raw_psycopg_async_rows,
    ("sqlspec", "psycopg_async", "rows"): sqlspec_psycopg_async_rows,
    ("raw", "asyncpg", "rows"): raw_asyncpg_rows,
    ("sqlspec", "asyncpg", "rows"): sqlspec_asyncpg_rows,
    ("raw", "cockroach_psycopg_sync", "rows"): raw_cockroach_psycopg_sync_rows,
    ("sqlspec", "cockroach_psycopg_sync", "rows"): sqlspec_cockroach_psycopg_sync_rows,
    ("raw", "cockroach_psycopg_async", "rows"): raw_cockroach_psycopg_async_rows,
    ("sqlspec", "cockroach_psycopg_async", "rows"): sqlspec_cockroach_psycopg_async_rows,
    ("raw", "cockroach_asyncpg", "rows"): raw_cockroach_asyncpg_rows,
    ("sqlspec", "cockroach_asyncpg", "rows"): sqlspec_cockroach_asyncpg_rows,
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
    ("raw", "sqlite", "schema_type_numpy"): raw_sqlite_schema_type_numpy,
    ("sqlspec", "sqlite", "schema_type_numpy"): sqlspec_sqlite_schema_type_numpy,
    ("raw", "sqlite", "complex_parameters"): raw_sqlite_complex_parameters,
    ("sqlspec", "sqlite", "complex_parameters"): sqlspec_sqlite_complex_parameters,
    ("raw", "sqlite", "thin_path_stress"): raw_sqlite_thin_path_stress,
    ("sqlspec", "sqlite", "thin_path_stress"): sqlspec_sqlite_thin_path_stress,
    # Extended Oracle LOB fetch scenarios
    ("raw", "oracle", "lob_fetch_1k"): raw_oracle_lob_fetch_1k,
    ("raw", "oracle", "lob_fetch_100k"): raw_oracle_lob_fetch_100k,
    ("sqlspec", "oracle", "lob_fetch_1k"): sqlspec_oracle_lob_fetch_1k,
    ("sqlspec", "oracle", "lob_fetch_100k"): sqlspec_oracle_lob_fetch_100k,
    ("sqlspec_fetch_lobs_true", "oracle", "lob_fetch_1k"): sqlspec_oracle_lob_fetch_fetch_lobs_true_1k,
    ("sqlspec_fetch_lobs_true", "oracle", "lob_fetch_100k"): sqlspec_oracle_lob_fetch_fetch_lobs_true_100k,
    ("sqlspec_async", "oracle", "lob_fetch_1k"): sqlspec_oracle_lob_fetch_async_1k,
    ("sqlspec_async", "oracle", "lob_fetch_100k"): sqlspec_oracle_lob_fetch_async_100k,
    ("sqlspec_async_fetch_lobs_true", "oracle", "lob_fetch_1k"): sqlspec_oracle_lob_fetch_async_fetch_lobs_true_1k,
    ("sqlspec_async_fetch_lobs_true", "oracle", "lob_fetch_100k"): sqlspec_oracle_lob_fetch_async_fetch_lobs_true_100k,
}


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
