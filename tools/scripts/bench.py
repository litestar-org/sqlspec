"""Benchmark script for comparing sqlspec vs raw drivers vs SQLAlchemy.

Originally contributed by euri10 (Benoit Barthelet) in PR #354.
"""

import asyncio
import cProfile
import inspect
import json
import pstats
import sqlite3
import statistics
import tempfile
import time
from collections.abc import Sequence  # noqa: TC003
from contextlib import suppress
from pathlib import Path
from typing import Any, TypedDict

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
    "main",
    "print_benchmark_table",
    "raw_asyncpg_initialization",
    "raw_asyncpg_read_heavy",
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
    "sqlalchemy_asyncpg_read_heavy",
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
    "sqlspec_asyncpg_read_heavy",
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
@click.option("--iterations", default=3, show_default=True, help="Number of timed iterations per scenario")
@click.option("--warmup", default=1, show_default=True, help="Number of warmup iterations (not timed)")
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
            click.secho(f"Error: {err}", fg="red")
    if _leaked_pools:
        click.secho("Pool leaks detected:", fg="yellow")
        for leak in _leaked_pools:
            click.secho(f"  - {leak}", fg="yellow")
        _leaked_pools.clear()
    click.echo(f"Benchmarks complete for drivers: {', '.join(driver)}")


def run_benchmark(driver: str, errors: list[str], *, iterations: int = 3, warmup: int = 1) -> list[dict[str, Any]]:
    """Run all benchmark scenarios for a driver.

    Args:
        driver: The database driver name (e.g., "sqlite", "asyncpg")
        errors: List to append error messages to
        iterations: Number of timed iterations per scenario
        warmup: Number of warmup iterations (not timed)

    Returns:
        List of benchmark result dictionaries
    """
    libraries = ["raw", "sqlspec", "sqlalchemy"]
    scenarios = ["initialization", "write_heavy", "read_heavy", "iterative_inserts", "repeated_queries"]
    results: list[dict[str, Any]] = []

    for scenario in scenarios:
        for lib in libraries:
            func = SCENARIO_REGISTRY.get((lib, driver, scenario))
            if func is None:
                errors.append(f"No implementation for library={lib}, driver={driver}, scenario={scenario}")
                continue

            is_async = inspect.iscoroutinefunction(func)

            try:
                # Warmup iterations (not timed)
                for _ in range(warmup):
                    if is_async:
                        asyncio.run(func())
                    else:
                        func()

                # Timed iterations
                times: list[float] = []
                for _ in range(iterations):
                    start = time.perf_counter()
                    if is_async:
                        asyncio.run(func())
                    else:
                        func()
                    times.append(time.perf_counter() - start)

                median_time = statistics.median(times)
                label = SQLSPEC_LABEL if lib == "sqlspec" else lib
                results.append({
                    "driver": driver,
                    "library": label,
                    "scenario": scenario,
                    "time": median_time,
                    "times": times,
                })
            except Exception as exc:
                errors.append(f"{lib}/{driver}/{scenario}: {exc}")

    return results


def run_benchmark_profiled(
    driver: str, errors: list[str], *, iterations: int = 3, warmup: int = 1, profile_scenario: str | None = None
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

    libraries = ["raw", "sqlspec", "sqlalchemy"]
    scenarios = ["initialization", "write_heavy", "read_heavy", "iterative_inserts", "repeated_queries"]
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
                # Warmup iterations (not timed, not profiled)
                for _ in range(warmup):
                    if is_async:
                        asyncio.run(func())
                    else:
                        func()

                # Profiled + timed iterations
                profiler = cProfile.Profile()
                times: list[float] = []
                for _ in range(iterations):
                    start = time.perf_counter()
                    profiler.enable()
                    if is_async:
                        asyncio.run(func())
                    else:
                        func()
                    profiler.disable()
                    times.append(time.perf_counter() - start)

                median_time = statistics.median(times)
                results.append({
                    "driver": driver,
                    "library": label,
                    "scenario": scenario,
                    "time": median_time,
                    "times": times,
                })

                # Save profile data
                prof_path = profiles_dir / f"{prof_name}.prof"
                profiler.dump_stats(str(prof_path))
                click.echo(f"  Profile saved: {prof_path}")

                # Print top 20 summary
                console.print(f"\n  [bold cyan]Profile summary: {prof_name}[/bold cyan]")
                stats = pstats.Stats(profiler)
                stats.strip_dirs()
                stats.sort_stats("cumulative")
                stats.print_stats(20)

            except Exception as exc:
                errors.append(f"{lib}/{driver}/{scenario}: {exc}")

    return results


def run_extended_benchmark(
    driver: str, errors: list[str], *, iterations: int = 3, warmup: int = 1
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
                # Warmup iterations (not timed)
                for _ in range(warmup):
                    if is_async:
                        asyncio.run(func())
                    else:
                        func()

                # Timed iterations
                times: list[float] = []
                for _ in range(iterations):
                    start = time.perf_counter()
                    if is_async:
                        asyncio.run(func())
                    else:
                        func()
                    times.append(time.perf_counter() - start)

                median_time = statistics.median(times)
                label = SQLSPEC_LABEL if lib == "sqlspec" else lib
                results.append({
                    "driver": driver,
                    "library": label,
                    "scenario": scenario,
                    "time": median_time,
                    "times": times,
                })
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

ASYNCPG_DSN = "postgresql://postgres:postgres@localhost/postgres"


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
    rows = await conn.fetch(SELECT_TEST_VALUES)
    assert len(rows) == ROWS_TO_INSERT
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
        rows = await session.fetch(SELECT_TEST_VALUES)
        assert len(rows) == ROWS_TO_INSERT


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
        result = await conn.execute(text(SELECT_TEST_VALUES))
        rows = result.fetchall()
        assert len(rows) == ROWS_TO_INSERT
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
    ("sqlspec", "asyncpg", "initialization"): sqlspec_asyncpg_initialization,
    ("sqlspec", "asyncpg", "write_heavy"): sqlspec_asyncpg_write_heavy,
    ("sqlspec", "asyncpg", "read_heavy"): sqlspec_asyncpg_read_heavy,
    ("sqlalchemy", "asyncpg", "initialization"): sqlalchemy_asyncpg_initialization,
    ("sqlalchemy", "asyncpg", "write_heavy"): sqlalchemy_asyncpg_write_heavy,
    ("sqlalchemy", "asyncpg", "read_heavy"): sqlalchemy_asyncpg_read_heavy,
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


def print_benchmark_table(results: list[dict[str, Any]]) -> None:
    console = Console()
    table = Table(title="Benchmark Results")
    table.add_column("Driver", style="cyan", no_wrap=True)
    table.add_column("Library", style="magenta")
    table.add_column("Scenario", style="green")
    table.add_column("Time (s)", justify="right", style="yellow")
    table.add_column("% Slower vs Raw", justify="right", style="red")

    # Check if any result has multiple iterations
    multi_iter = any(len(row.get("times", [])) > 1 for row in results)

    # Build a lookup for raw times: {(driver, scenario): time}
    raw_times: dict[tuple[str, str], float] = {}
    for row in results:
        if row["library"] == "raw":
            raw_times[(row["driver"], row["scenario"])] = row["time"]

    for row in results:
        driver = row["driver"]
        scenario = row["scenario"]
        lib = row["library"]
        t = row["time"]
        times = row.get("times", [t])
        if lib == "raw":
            percent_slower = "---"
        else:
            raw_time = raw_times.get((driver, scenario))
            percent_slower = f"{100 * (t - raw_time) / raw_time:.1f}%" if raw_time and raw_time > 0 else "n/a"
        time_str = f"{t:.4f} ({min(times):.4f}-{max(times):.4f})" if multi_iter and len(times) > 1 else f"{t:.4f}"
        table.add_row(driver, lib, scenario, time_str, percent_slower)
    console.print(table)


if __name__ == "__main__":
    main()
