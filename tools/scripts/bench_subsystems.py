"""Subsystem micro-benchmark script for sqlspec hot path profiling.

Isolates and measures each execution subsystem independently using timeit,
identifying exactly where time is spent during query execution.

Run with::

    uv run python tools/scripts/bench_subsystems.py
"""

import sqlite3
import tempfile
import timeit
from contextlib import suppress
from pathlib import Path
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from sqlspec.utils.profiling import HotPathProfiler

__all__ = ("SubsystemBenchmark", "main", "print_results_table", "run_benchmarks")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_temp_db() -> Path:
    """Create a temporary SQLite database file (caller must delete)."""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    tmp_path = Path(tmp.name)
    tmp.close()
    return tmp_path


def _setup_test_table(db_path: Path) -> None:
    """Create the test table and seed data in a raw sqlite connection."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("CREATE TABLE IF NOT EXISTS test (value TEXT)")
    data = [(f"value_{i}",) for i in range(100)]
    conn.executemany("INSERT INTO test (value) VALUES (?)", data)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Benchmark definitions
# ---------------------------------------------------------------------------


class SubsystemBenchmark:
    """Container for a single subsystem benchmark."""

    __slots__ = ("bench_fn", "description", "iterations", "name", "setup_fn")

    def __init__(
        self, name: str, bench_fn: Any, iterations: int = 10_000, setup_fn: Any = None, description: str = ""
    ) -> None:
        self.name = name
        self.bench_fn = bench_fn
        self.iterations = iterations
        self.setup_fn = setup_fn
        self.description = description


def _build_benchmarks(db_path: Path, iterations: int) -> list[SubsystemBenchmark]:
    """Build the list of subsystem benchmarks.

    Args:
        db_path: Path to the temporary SQLite database.
        iterations: Number of iterations per benchmark.

    Returns:
        List of SubsystemBenchmark instances.
    """
    benchmarks: list[SubsystemBenchmark] = []

    # --- 1. SQL object construction ---

    from sqlspec.core.statement import SQL

    def bench_sql_construction_no_params() -> None:
        SQL("INSERT INTO test (value) VALUES (?)")

    benchmarks.append(
        SubsystemBenchmark(
            name="SQL() construction (no params)",
            bench_fn=bench_sql_construction_no_params,
            iterations=iterations,
            description="Time SQL('INSERT INTO test (value) VALUES (?)')",
        )
    )

    def bench_sql_construction_with_params() -> None:
        SQL("INSERT INTO test (value) VALUES (?)", ("hello",))

    benchmarks.append(
        SubsystemBenchmark(
            name="SQL() construction (with params)",
            bench_fn=bench_sql_construction_with_params,
            iterations=iterations,
            description="Time SQL('INSERT ...', ('hello',))",
        )
    )

    def bench_sql_construction_select() -> None:
        SQL("SELECT * FROM test WHERE value = ?", ("value_1",))

    benchmarks.append(
        SubsystemBenchmark(
            name="SQL() construction (SELECT + param)",
            bench_fn=bench_sql_construction_select,
            iterations=iterations,
            description="Time SQL('SELECT ... WHERE ...', param)",
        )
    )

    # --- 2. SQL.compile() ---

    insert_stmt = SQL("INSERT INTO test (value) VALUES (?)", ("hello",))

    def bench_compile_insert() -> None:
        insert_stmt._processed_state = type(insert_stmt)._processed_state
        # Reset to force recompilation
        from sqlspec.typing import Empty

        insert_stmt._processed_state = Empty
        insert_stmt.compile()

    benchmarks.append(
        SubsystemBenchmark(
            name="SQL.compile() - INSERT",
            bench_fn=bench_compile_insert,
            iterations=iterations,
            description="Compile simple INSERT statement",
        )
    )

    select_stmt = SQL("SELECT * FROM test WHERE value = ?", ("value_1",))

    def bench_compile_select() -> None:
        from sqlspec.typing import Empty

        select_stmt._processed_state = Empty
        select_stmt.compile()

    benchmarks.append(
        SubsystemBenchmark(
            name="SQL.compile() - SELECT",
            bench_fn=bench_compile_select,
            iterations=iterations,
            description="Compile simple SELECT statement",
        )
    )

    complex_sql = (
        "SELECT u.id, u.name, o.total FROM users u "
        "JOIN orders o ON u.id = o.user_id "
        "WHERE u.status = ? AND o.date > ? AND o.total > ? "
        "ORDER BY o.total DESC LIMIT ?"
    )
    complex_stmt = SQL(complex_sql, ("active", "2024-01-01", 100, 50))

    def bench_compile_complex() -> None:
        from sqlspec.typing import Empty

        complex_stmt._processed_state = Empty
        complex_stmt.compile()

    benchmarks.append(
        SubsystemBenchmark(
            name="SQL.compile() - complex JOIN",
            bench_fn=bench_compile_complex,
            iterations=iterations,
            description="Compile complex JOIN with multiple params",
        )
    )

    # --- 3. Query cache cycle ---
    # We need a real SqliteDriver to test the QC path.

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    spec = SQLSpec()
    config = SqliteConfig(database=str(db_path))

    # We need to get a driver instance to test QC methods.
    # Use the session context to get the driver, but keep it alive.
    _session_ctx = spec.provide_session(config)
    session = _session_ctx.__enter__()

    # Create the test table and seed data through the sqlspec session
    session.execute("CREATE TABLE IF NOT EXISTS test (value TEXT)")
    data = [(f"value_{i}",) for i in range(100)]
    session.execute_many("INSERT INTO test (value) VALUES (?)", data)

    # Prime the query cache by executing a query twice (first stores, second hits cache)
    session.execute("INSERT INTO test (value) VALUES (?)", ("cache_prime",))
    session.execute("INSERT INTO test (value) VALUES (?)", ("cache_prime2",))

    # Now benchmark the direct prepare path
    def bench_qc_prepare_hit() -> None:
        session._qc_prepare_direct("INSERT INTO test (value) VALUES (?)", ("bench_val",))

    benchmarks.append(
        SubsystemBenchmark(
            name="QC _qc_prepare_direct() - cache hit",
            bench_fn=bench_qc_prepare_hit,
            iterations=iterations,
            description="Direct prepare with cache hit",
        )
    )

    def bench_qc_prepare_miss() -> None:
        session._qc_prepare_direct("INSERT INTO unique_table (col) VALUES (?)", ("val",))

    benchmarks.append(
        SubsystemBenchmark(
            name="QC _qc_prepare_direct() - cache miss",
            bench_fn=bench_qc_prepare_miss,
            iterations=iterations,
            description="Direct prepare with cache miss",
        )
    )

    # Full QC lookup cycle
    def bench_qc_lookup() -> None:
        session._qc_lookup("INSERT INTO test (value) VALUES (?)", ("bench_val",))

    benchmarks.append(
        SubsystemBenchmark(
            name="QC _qc_lookup() - full cycle",
            bench_fn=bench_qc_lookup,
            iterations=iterations,
            description="Full QC lookup -> prepare -> execute cycle",
        )
    )

    # --- 4. Parameter processing ---

    def bench_prepare_driver_params_tuple() -> None:
        session.prepare_driver_parameters(("value_1",), session.statement_config, is_many=False)

    benchmarks.append(
        SubsystemBenchmark(
            name="prepare_driver_parameters (tuple)",
            bench_fn=bench_prepare_driver_params_tuple,
            iterations=iterations,
            description="Prepare a single positional parameter tuple",
        )
    )

    def bench_prepare_driver_params_dict() -> None:
        session.prepare_driver_parameters({"value": "test_val"}, session.statement_config, is_many=False)

    benchmarks.append(
        SubsystemBenchmark(
            name="prepare_driver_parameters (dict)",
            bench_fn=bench_prepare_driver_params_dict,
            iterations=iterations,
            description="Prepare a single named parameter dict",
        )
    )

    def bench_format_parameter_set() -> None:
        session._format_parameter_set(("value_1", "value_2", "value_3"), session.statement_config)

    benchmarks.append(
        SubsystemBenchmark(
            name="_format_parameter_set (3 params)",
            bench_fn=bench_format_parameter_set,
            iterations=iterations,
            description="Format a 3-element positional parameter set",
        )
    )

    # --- 5. Result construction ---

    from sqlspec.core.result._base import SQLResult

    result_stmt = SQL("INSERT INTO test (value) VALUES (?)", ("hello",))
    result_stmt.compile()

    def bench_result_init_dml() -> None:
        SQLResult(statement=result_stmt, data=None, rows_affected=1, operation_type="INSERT")

    benchmarks.append(
        SubsystemBenchmark(
            name="SQLResult.__init__() - DML",
            bench_fn=bench_result_init_dml,
            iterations=iterations,
            description="Construct SQLResult for an INSERT operation",
        )
    )

    select_result_stmt = SQL("SELECT * FROM test", ())
    select_result_stmt.compile()
    sample_rows: list[tuple[str]] = [(f"value_{i}",) for i in range(100)]

    def bench_result_init_select() -> None:
        SQLResult(
            statement=select_result_stmt,
            data=sample_rows,
            rows_affected=0,
            operation_type="SELECT",
            column_names=["value"],
        )

    benchmarks.append(
        SubsystemBenchmark(
            name="SQLResult.__init__() - SELECT (100 rows)",
            bench_fn=bench_result_init_select,
            iterations=iterations,
            description="Construct SQLResult for a SELECT with 100 rows",
        )
    )

    # --- 6. Cursor context manager overhead ---

    raw_conn = sqlite3.connect(str(db_path))
    raw_conn.execute("PRAGMA journal_mode = WAL")

    from sqlspec.adapters.sqlite.driver import SqliteCursor

    def bench_cursor_context_manager() -> None:
        with SqliteCursor(raw_conn) as _cursor:
            pass

    benchmarks.append(
        SubsystemBenchmark(
            name="SqliteCursor context manager",
            bench_fn=bench_cursor_context_manager,
            iterations=iterations,
            description="Enter/exit SqliteCursor context manager",
        )
    )

    # Raw cursor for comparison
    def bench_raw_cursor() -> None:
        cursor = raw_conn.cursor()
        cursor.close()

    benchmarks.append(
        SubsystemBenchmark(
            name="raw sqlite3 cursor create+close",
            bench_fn=bench_raw_cursor,
            iterations=iterations,
            description="Raw sqlite3 cursor() + close() for comparison",
        )
    )

    # --- 7. Full execute() overhead (single statement, end-to-end) ---

    def bench_full_execute() -> None:
        session.execute("INSERT INTO test (value) VALUES (?)", ("bench_e2e",))

    benchmarks.append(
        SubsystemBenchmark(
            name="session.execute() - full path",
            bench_fn=bench_full_execute,
            iterations=iterations,
            description="Full sqlspec execute() including QC, dispatch, result",
        )
    )

    # Raw sqlite for comparison
    def bench_raw_execute() -> None:
        raw_conn.execute("INSERT INTO test (value) VALUES (?)", ("bench_raw",))

    benchmarks.append(
        SubsystemBenchmark(
            name="raw sqlite3 conn.execute()",
            bench_fn=bench_raw_execute,
            iterations=iterations,
            description="Raw sqlite3 execute for comparison baseline",
        )
    )

    # Store session context for cleanup
    benchmarks.append(
        SubsystemBenchmark(
            name="_cleanup_",
            bench_fn=lambda: None,
            iterations=0,
            setup_fn=lambda: (_session_ctx.__exit__(None, None, None), raw_conn.close()),
        )
    )

    return benchmarks


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_benchmarks(db_path: Path, iterations: int, warmup: int, profile: bool = False) -> list[dict[str, Any]]:
    """Run all subsystem benchmarks and return results.

    Args:
        db_path: Path to the temporary SQLite database.
        iterations: Number of iterations per benchmark.
        warmup: Number of warmup iterations.
        profile: Whether to profile the benchmarks using HotPathProfiler.

    Returns:
        List of result dictionaries with timing data.
    """
    benchmarks = _build_benchmarks(db_path, iterations)
    results: list[dict[str, Any]] = []

    for bench in benchmarks:
        if bench.name == "_cleanup_":
            if bench.setup_fn:
                bench.setup_fn()
            continue

        click.echo(f"  Benchmarking: {bench.name}...")

        # Warmup
        for _ in range(warmup):
            bench.bench_fn()

        # Timed run using timeit
        if profile:
            with HotPathProfiler() as prof:
                elapsed = timeit.timeit(bench.bench_fn, number=bench.iterations)
            click.echo(f"  Profile results for {bench.name}:")
            prof.print_report(limit=10)
            click.echo()
        else:
            elapsed = timeit.timeit(bench.bench_fn, number=bench.iterations)
        time_per_op_us = (elapsed / bench.iterations) * 1_000_000  # microseconds
        ops_per_sec = bench.iterations / elapsed if elapsed > 0 else float("inf")

        results.append({
            "name": bench.name,
            "time_per_op_us": time_per_op_us,
            "ops_per_sec": ops_per_sec,
            "total_time": elapsed,
            "iterations": bench.iterations,
            "description": bench.description,
        })

    return results


def print_results_table(results: list[dict[str, Any]]) -> None:
    """Print benchmark results as a rich table.

    Args:
        results: List of result dictionaries from run_benchmarks.
    """
    console = Console()

    # Calculate total overhead as sum of key subsystem times (excluding raw baselines)
    subsystem_times = [
        r["time_per_op_us"]
        for r in results
        if not r["name"].startswith("raw ") and r["name"] != "session.execute() - full path"
    ]
    total_subsystem_us = sum(subsystem_times) if subsystem_times else 1.0

    table = Table(title="Subsystem Micro-Benchmarks (sqlite)")
    table.add_column("Subsystem", style="cyan", no_wrap=True, max_width=42)
    table.add_column("Time/Op", justify="right", style="yellow")
    table.add_column("Ops/sec", justify="right", style="green")
    table.add_column("Relative Cost", justify="right", style="magenta")

    for r in results:
        name = r["name"]
        us = r["time_per_op_us"]
        ops = r["ops_per_sec"]
        pct = (us / total_subsystem_us) * 100 if total_subsystem_us > 0 else 0

        # Format time with appropriate unit
        _us_per_ms = 1000
        if us >= _us_per_ms:
            time_str = f"{us / _us_per_ms:.2f} ms"
        elif us >= 1:
            time_str = f"{us:.2f} us"
        else:
            time_str = f"{us * 1000:.1f} ns"

        # Format ops/sec
        _million = 1_000_000
        _thousand = 1_000
        if ops >= _million:
            ops_str = f"{ops / _million:.2f}M"
        elif ops >= _thousand:
            ops_str = f"{ops / _thousand:.1f}K"
        else:
            ops_str = f"{ops:.0f}"

        # Style for baselines
        if name.startswith("raw "):
            pct_str = "(baseline)"
        elif name == "session.execute() - full path":
            pct_str = "(end-to-end)"
        else:
            pct_str = f"{pct:.1f}%"

        table.add_row(name, time_str, ops_str, pct_str)

    console.print(table)

    # Print summary
    console.print()
    full_execute = next((r for r in results if r["name"] == "session.execute() - full path"), None)
    raw_execute = next((r for r in results if r["name"] == "raw sqlite3 conn.execute()"), None)

    if full_execute and raw_execute:
        overhead_us = full_execute["time_per_op_us"] - raw_execute["time_per_op_us"]
        overhead_pct = (overhead_us / raw_execute["time_per_op_us"]) * 100 if raw_execute["time_per_op_us"] > 0 else 0
        console.print(f"  [bold]Per-call overhead:[/bold] {overhead_us:.2f} us ({overhead_pct:.1f}% vs raw)")
        console.print(f"  [bold]Raw execute:[/bold] {raw_execute['time_per_op_us']:.2f} us/op")
        console.print(f"  [bold]sqlspec execute:[/bold] {full_execute['time_per_op_us']:.2f} us/op")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command()
@click.option("--iterations", default=10_000, show_default=True, help="Number of iterations per subsystem benchmark")
@click.option("--warmup", default=100, show_default=True, help="Number of warmup iterations (not timed)")
@click.option("--profile", is_flag=True, default=False, help="Profile the benchmarks using HotPathProfiler")
def main(iterations: int, warmup: int, profile: bool) -> None:
    """Run subsystem micro-benchmarks for sqlspec hot path profiling.

    Isolates each execution subsystem and measures it independently to
    identify exactly where time is spent during query execution.
    """
    console = Console()
    console.print("[bold]sqlspec Subsystem Micro-Benchmarks[/bold]")
    console.print(f"  iterations={iterations}, warmup={warmup}, profile={profile}")
    console.print()

    db_path = _make_temp_db()
    try:
        _setup_test_table(db_path)
        click.echo("Running benchmarks...")
        results = run_benchmarks(db_path, iterations, warmup, profile=profile)
        click.echo()
        print_results_table(results)
    finally:
        with suppress(OSError):
            db_path.unlink()

    click.echo()
    click.secho("Subsystem benchmarks complete.", fg="green")


if __name__ == "__main__":
    main()
