"""Subsystem micro-benchmark script for sqlspec hot path profiling.

Isolates and measures each execution subsystem independently using timeit,
identifying exactly where time is spent during query execution.

Run with::

    uv run python tools/scripts/bench_subsystems.py
"""

import gc
import io
import json
import sqlite3
import statistics
import tempfile
import time
import timeit
from contextlib import suppress
from pathlib import Path
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from tools.profiling import HotPathProfiler

__all__ = ("SubsystemBenchmark", "main", "print_results_table", "run_benchmarks")

STUB_COMPLEX_QUERY_SQL = """
WITH recent_events AS (
    SELECT user_id, kind, COUNT(*) AS cnt, SUM(amount) AS total_amount
    FROM events
    WHERE ts >= ?
    GROUP BY user_id, kind
)
SELECT u.id, u.name, o.tier, o.region, r.kind, r.cnt, r.total_amount
FROM users u
JOIN orgs o ON o.id = u.org_id
LEFT JOIN recent_events r ON r.user_id = u.id
WHERE u.status = 'active' AND o.region = 'us'
ORDER BY r.total_amount DESC
LIMIT 25
"""


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


def run_benchmarks(db_path: Path, iterations: int, warmup: int, profile: bool = False) -> list[dict[str, Any]]:
    """Run all subsystem benchmarks using interleaved round-robin rounds.

    Args:
        db_path: Path to the temporary SQLite database.
        iterations: Number of iterations per benchmark.
        warmup: Number of warmup iterations.
        profile: Whether to profile the benchmarks using HotPathProfiler.

    Returns:
        List of result dictionaries with timing data.
    """
    benchmarks = _build_benchmarks(db_path, iterations)
    active_benchmarks: list[SubsystemBenchmark] = []
    cleanup_benchmarks: list[SubsystemBenchmark] = []
    for bench in benchmarks:
        if bench.name == "_cleanup_":
            cleanup_benchmarks.append(bench)
        else:
            active_benchmarks.append(bench)

    try:
        for bench in active_benchmarks:
            for _ in range(warmup):
                bench.bench_fn()

        results: list[dict[str, Any]] = []
        if profile:
            for bench in active_benchmarks:
                click.echo(f"  Benchmarking: {bench.name}...")
                with HotPathProfiler() as prof:
                    elapsed = timeit.timeit(bench.bench_fn, number=bench.iterations)
                click.echo(f"  Profile results for {bench.name}:")
                prof.print_report(limit=10)
                click.echo()
                time_per_op_us = (elapsed / bench.iterations) * 1_000_000
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

        rounds = min(5, max(1, iterations))
        per_round_us: dict[str, list[float]] = {bench.name: [] for bench in active_benchmarks}
        total_elapsed: dict[str, float] = dict.fromkeys((bench.name for bench in active_benchmarks), 0.0)
        total_iters: dict[str, int] = dict.fromkeys((bench.name for bench in active_benchmarks), 0)

        for _ in range(rounds):
            for bench in active_benchmarks:
                batch_size = max(1, bench.iterations // rounds)
                gc.collect()
                gc_was_enabled = gc.isenabled()
                if gc_was_enabled:
                    gc.disable()
                try:
                    elapsed = timeit.timeit(bench.bench_fn, number=batch_size)
                finally:
                    if gc_was_enabled:
                        gc.enable()
                per_round_us[bench.name].append((elapsed / batch_size) * 1_000_000)
                total_elapsed[bench.name] += elapsed
                total_iters[bench.name] += batch_size

        for bench in active_benchmarks:
            click.echo(f"  Benchmarking: {bench.name}...")
            samples = per_round_us[bench.name]
            median_us = statistics.median(samples) if samples else 0.0
            ops_per_sec = 1_000_000.0 / median_us if median_us > 0 else float("inf")
            results.append({
                "name": bench.name,
                "time_per_op_us": median_us,
                "ops_per_sec": ops_per_sec,
                "total_time": total_elapsed[bench.name],
                "iterations": total_iters[bench.name],
                "description": bench.description,
            })

        return results
    finally:
        for bench in cleanup_benchmarks:
            if bench.setup_fn:
                bench.setup_fn()


def print_results_table(results: list[dict[str, Any]]) -> None:
    """Print benchmark results as a rich table.

    Args:
        results: List of result dictionaries from run_benchmarks.
    """
    console = Console()

    subsystem_times = [
        r["time_per_op_us"]
        for r in results
        if not r["name"].startswith("raw ") and r["name"] != "session.execute() - full path"
    ]
    total_subsystem_us = sum(subsystem_times) if subsystem_times else 1.0

    table = Table(title="Subsystem Micro-Benchmarks (sqlite)")
    table.add_column("Subsystem", style="cyan", no_wrap=True, max_width=52)
    table.add_column("Time/Op", justify="right", style="yellow")
    table.add_column("Ops/sec", justify="right", style="green")
    table.add_column("Relative Cost", justify="right", style="magenta")

    for r in results:
        name = r["name"]
        us = r["time_per_op_us"]
        ops = r["ops_per_sec"]
        pct = (us / total_subsystem_us) * 100 if total_subsystem_us > 0 else 0

        _us_per_ms = 1000
        if us >= _us_per_ms:
            time_str = f"{us / _us_per_ms:.2f} ms"
        elif us >= 1:
            time_str = f"{us:.2f} us"
        else:
            time_str = f"{us * 1000:.1f} ns"

        _million = 1_000_000
        _thousand = 1_000
        if ops >= _million:
            ops_str = f"{ops / _million:.2f}M"
        elif ops >= _thousand:
            ops_str = f"{ops / _thousand:.1f}K"
        else:
            ops_str = f"{ops:.0f}"

        if name.startswith("raw "):
            pct_str = "(baseline)"
        elif name == "session.execute() - full path":
            pct_str = "(end-to-end)"
        else:
            pct_str = f"{pct:.1f}%"

        table.add_row(name, time_str, ops_str, pct_str)

    console.print(table)

    console.print()
    full_execute = next((r for r in results if r["name"] == "session.execute() - full path"), None)
    raw_execute = next((r for r in results if r["name"] == "raw sqlite3 conn.execute()"), None)

    if full_execute and raw_execute:
        overhead_us = full_execute["time_per_op_us"] - raw_execute["time_per_op_us"]
        overhead_pct = (overhead_us / raw_execute["time_per_op_us"]) * 100 if raw_execute["time_per_op_us"] > 0 else 0
        console.print(f"  [bold]Per-call overhead:[/bold] {overhead_us:.2f} us ({overhead_pct:.1f}% vs raw)")
        console.print(f"  [bold]Raw execute:[/bold] {raw_execute['time_per_op_us']:.2f} us/op")
        console.print(f"  [bold]sqlspec execute:[/bold] {full_execute['time_per_op_us']:.2f} us/op")


@click.command()
@click.option("--iterations", default=10_000, show_default=True, help="Number of iterations per subsystem benchmark")
@click.option("--warmup", default=100, show_default=True, help="Number of warmup iterations (not timed)")
@click.option("--profile", is_flag=True, default=False, help="Profile the benchmarks using HotPathProfiler")
@click.option("--json-output", default=None, type=click.Path(), help="Write subsystem results to a JSON file")
def main(iterations: int, warmup: int, profile: bool, json_output: str | None) -> None:
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
        if json_output is not None:
            _write_json_results(results, json_output, iterations=iterations, warmup=warmup, profile=profile)
            click.secho(f"Subsystem report written to {json_output}", fg="green")
    finally:
        with suppress(OSError):
            db_path.unlink()

    click.echo()
    click.secho("Subsystem benchmarks complete.", fg="green")


class _StubArrowTable:
    """Minimal no-op Arrow table stub for pre-Phase-5 DuckDB dispatch_execute."""

    __slots__ = ("column_names", "num_rows")

    def __init__(self) -> None:
        self.column_names = ["id", "value"]
        self.num_rows = 1

    def to_pylist(self) -> list[dict[str, Any]]:
        return [{"id": 1, "value": "value_1"}]


class _StubDuckDBCursor:
    """Minimal no-op DB-API cursor isolating pure-Python SQLSpec driver overhead."""

    __slots__ = ("_arrow_table", "description", "rowcount")

    def __init__(self) -> None:
        self.description = [
            ("id", "NUMBER", None, None, None, None, None),
            ("value", "STRING", None, None, None, None, None),
        ]
        self.rowcount = 1
        self._arrow_table = _StubArrowTable()

    def execute(self, sql: str, parameters: Any = None) -> "_StubDuckDBCursor":
        return self

    def executemany(self, sql: str, parameters: Any = None) -> "_StubDuckDBCursor":
        return self

    def fetchall(self) -> list[tuple[int, str]]:
        return [(1, "value_1")]

    def to_arrow_table(self) -> _StubArrowTable:
        return self._arrow_table

    def close(self) -> None:
        return None


class _StubDuckDBConnection:
    """Minimal no-op DB-API connection returning a reusable stub cursor."""

    __slots__ = ("_cursor", "description", "rowcount")

    def __init__(self) -> None:
        self._cursor = _StubDuckDBCursor()
        self.description = self._cursor.description
        self.rowcount = 1

    def cursor(self) -> _StubDuckDBCursor:
        return self._cursor

    def execute(self, sql: str, parameters: Any = None) -> _StubDuckDBCursor:
        return self._cursor

    def executemany(self, sql: str, parameters: Any = None) -> _StubDuckDBCursor:
        return self._cursor

    def fetchall(self) -> list[tuple[int, str]]:
        return self._cursor.fetchall()

    def to_arrow_table(self) -> _StubArrowTable:
        return self._cursor.to_arrow_table()

    def register(self, name: str, table: Any) -> None:
        return None

    def unregister(self, name: str) -> None:
        return None

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def _build_stub_driver_benchmarks(iterations: int) -> list[SubsystemBenchmark]:
    """Build pure-Python stub-connection driver micro-benchmarks."""
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.duckdb.driver import DuckDBDriver

    stub_conn = _StubDuckDBConnection()
    shared_config = DuckDBConfig(
        connection_config={"database": ":memory:"}, driver_features={"enable_arrow_ingest": False}
    )
    shared_stmt_cfg = shared_config.statement_config
    shared_features = dict(shared_config.driver_features)
    stub_driver = DuckDBDriver(
        connection=stub_conn,  # type: ignore[arg-type]
        statement_config=shared_stmt_cfg,
        driver_features=shared_features,
    )

    select_sql = "SELECT id, value FROM test WHERE value = ?"
    insert_sql = "INSERT INTO test (value) VALUES (?)"
    no_param_sql = "SELECT 1"
    scalar_sql = "SELECT id, value FROM test WHERE id = ?"
    many_sql = "INSERT INTO test (id, value) VALUES (?, ?)"
    batch_100: list[tuple[int, str]] = [(idx, f"value_{idx}") for idx in range(100)]

    stub_driver.execute(select_sql, ("value_1",))
    stub_driver.execute(select_sql, ("value_1",))
    stub_driver.execute(insert_sql, ("value_1",))
    stub_driver.execute(insert_sql, ("value_1",))
    stub_driver.fetch_one_or_none(select_sql, ("value_1",))
    stub_driver.execute(no_param_sql)
    stub_driver.execute(scalar_sql, 1)
    stub_driver.execute(STUB_COMPLEX_QUERY_SQL, 1)
    stub_driver.execute_many(many_sql, batch_100)

    def bench_stub_fast_select() -> None:
        stub_driver.execute(select_sql, ("value_1",))

    def bench_stub_fast_insert() -> None:
        stub_driver.execute(insert_sql, ("value_1",))

    def bench_stub_fast_fetch_one_or_none() -> None:
        stub_driver.fetch_one_or_none(select_sql, ("value_1",))

    def bench_stub_no_param_select() -> None:
        stub_driver.execute(no_param_sql)

    def bench_stub_scalar_select() -> None:
        stub_driver.execute(scalar_sql, 1)

    def bench_stub_complex_cte() -> None:
        stub_driver.execute(STUB_COMPLEX_QUERY_SQL, 1)

    def bench_stub_session_per_request_simple() -> None:
        drv = DuckDBDriver(
            connection=stub_conn,  # type: ignore[arg-type]
            statement_config=shared_stmt_cfg,
            driver_features=shared_features,
        )
        drv.execute(select_sql, ("value_1",))

    def bench_stub_session_per_request_complex() -> None:
        drv = DuckDBDriver(
            connection=stub_conn,  # type: ignore[arg-type]
            statement_config=shared_stmt_cfg,
            driver_features=shared_features,
        )
        drv.execute(STUB_COMPLEX_QUERY_SQL, 1)

    def bench_stub_execute_many_100() -> None:
        stub_driver.execute_many(many_sql, batch_100)

    bounded_iters = max(1, min(iterations, 2_000))
    return [
        SubsystemBenchmark(
            name="stub driver: fast-path execute(SELECT, tuple)",
            bench_fn=bench_stub_fast_select,
            iterations=iterations,
            description="Pure-Python driver overhead on warm fast-path SELECT with 1-tuple param",
        ),
        SubsystemBenchmark(
            name="stub driver: fast-path execute(INSERT, tuple)",
            bench_fn=bench_stub_fast_insert,
            iterations=iterations,
            description="Pure-Python driver overhead on warm fast-path INSERT with 1-tuple param",
        ),
        SubsystemBenchmark(
            name="stub driver: fast-path fetch_one_or_none(SELECT, tuple)",
            bench_fn=bench_stub_fast_fetch_one_or_none,
            iterations=iterations,
            description="Pure-Python driver overhead on warm fetch_one_or_none with 1-tuple param",
        ),
        SubsystemBenchmark(
            name="stub driver: parameterless execute(SELECT)",
            bench_fn=bench_stub_no_param_select,
            iterations=iterations,
            description="Pure-Python driver overhead on parameterless SELECT 1",
        ),
        SubsystemBenchmark(
            name="stub driver: scalar-param execute(SELECT, int)",
            bench_fn=bench_stub_scalar_select,
            iterations=iterations,
            description="Pure-Python driver overhead on SELECT with scalar positional param",
        ),
        SubsystemBenchmark(
            name="stub driver: complex CTE+2-join execute(SELECT, int)",
            bench_fn=bench_stub_complex_cte,
            iterations=bounded_iters,
            description="Pure-Python driver overhead on CTE + 2-join query with scalar param",
        ),
        SubsystemBenchmark(
            name="stub driver: session-per-request simple SELECT",
            bench_fn=bench_stub_session_per_request_simple,
            iterations=bounded_iters,
            description="New driver instance per request executing simple SELECT",
        ),
        SubsystemBenchmark(
            name="stub driver: session-per-request complex CTE SELECT",
            bench_fn=bench_stub_session_per_request_complex,
            iterations=bounded_iters,
            description="New driver instance per request executing CTE + 2-join SELECT",
        ),
        SubsystemBenchmark(
            name="stub driver: execute_many 100 rows",
            bench_fn=bench_stub_execute_many_100,
            iterations=bounded_iters,
            description="Pure-Python driver overhead on execute_many with 100 parameter rows",
        ),
    ]


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


def _librt_string_writer_type() -> "type[Any] | None":
    """Return librt's StringWriter when the mypy 2 runtime helper is installed."""
    try:
        from librt.strings import StringWriter
    except ImportError:
        return None
    return StringWriter


def _build_librt_candidate_benchmarks(iterations: int) -> list[SubsystemBenchmark]:
    """Build optional benchmarks for candidate librt.strings replacements."""
    string_writer_type = _librt_string_writer_type()
    if string_writer_type is None:
        return []

    benchmarks: list[SubsystemBenchmark] = []

    placeholder_sql = (
        "SELECT * FROM accounts WHERE tenant_id = :tenant_id AND status = :status "
        "AND created_at >= :created_after AND created_at < :created_before "
        "AND region = :region ORDER BY created_at DESC LIMIT :limit_value"
    )
    placeholders = (":tenant_id", ":status", ":created_after", ":created_before", ":region", ":limit_value")
    replacements: list[tuple[int, str, str]] = []
    search_start = 0
    for index, placeholder in enumerate(placeholders, start=1):
        position = placeholder_sql.index(placeholder, search_start)
        replacements.append((position, placeholder, f"${index}"))
        search_start = position + len(placeholder)

    def bench_parameter_converter_baseline() -> None:
        segments: list[str] = []
        last_end = 0
        for position, placeholder, replacement in replacements:
            segments.extend((placeholder_sql[last_end:position], replacement))
            last_end = position + len(placeholder)
        segments.append(placeholder_sql[last_end:])
        "".join(segments)

    benchmarks.append(
        SubsystemBenchmark(
            name="librt ParameterConverter baseline",
            bench_fn=bench_parameter_converter_baseline,
            iterations=iterations,
            description="Current list-join placeholder assembly shape from ParameterConverter",
        )
    )

    def bench_parameter_converter_string_writer() -> None:
        writer = string_writer_type()
        last_end = 0
        for position, placeholder, replacement in replacements:
            writer.write(placeholder_sql[last_end:position])
            writer.write(replacement)
            last_end = position + len(placeholder)
        writer.write(placeholder_sql[last_end:])
        writer.getvalue()

    benchmarks.append(
        SubsystemBenchmark(
            name="librt ParameterConverter StringWriter",
            bench_fn=bench_parameter_converter_string_writer,
            iterations=iterations,
            description="librt.strings.StringWriter candidate for ParameterConverter assembly",
        )
    )

    token_values = tuple(
        token
        for _ in range(12)
        for token in (
            "SELECT",
            " ",
            "id",
            ", ",
            "name",
            " FROM ",
            "accounts",
            " WHERE ",
            "status",
            " = ",
            "'active'",
            ";",
        )
    )

    def bench_splitter_join_baseline() -> None:
        statement_chars: list[str] = []
        for token_value in token_values:
            statement_chars.append(token_value)  # noqa: PERF402
        "".join(statement_chars).strip()

    benchmarks.append(
        SubsystemBenchmark(
            name="librt splitter join baseline",
            bench_fn=bench_splitter_join_baseline,
            iterations=iterations,
            description="Current list-join statement assembly shape from StatementSplitter",
        )
    )

    def bench_splitter_string_writer() -> None:
        writer = string_writer_type()
        for token_value in token_values:
            writer.write(token_value)
        writer.getvalue().strip()

    benchmarks.append(
        SubsystemBenchmark(
            name="librt splitter StringWriter",
            bench_fn=bench_splitter_string_writer,
            iterations=iterations,
            description="librt.strings.StringWriter candidate for StatementSplitter assembly",
        )
    )

    copy_records: tuple[tuple[str, ...], ...] = tuple(
        (f"value_{row}_0", f"value_{row}_1", "contains\\tab\tand newline\n") for row in range(16)
    )

    def _escape_copy_text(value: str) -> str:
        return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")

    def bench_psqlpy_copy_baseline() -> None:
        buffer = io.StringIO()
        for record in copy_records:
            buffer.write("\t".join(_escape_copy_text(value) for value in record))
            buffer.write("\n")
        buffer.getvalue().encode("utf-8")

    benchmarks.append(
        SubsystemBenchmark(
            name="librt psqlpy copy baseline",
            bench_fn=bench_psqlpy_copy_baseline,
            iterations=iterations,
            description="Current StringIO row assembly shape from psqlpy binary copy encoding",
        )
    )

    def bench_psqlpy_copy_string_writer() -> None:
        writer = string_writer_type()
        for record in copy_records:
            first = True
            for value in record:
                if first:
                    first = False
                else:
                    writer.write("\t")
                writer.write(_escape_copy_text(value))
            writer.write("\n")
        writer.getvalue().encode("utf-8")

    benchmarks.append(
        SubsystemBenchmark(
            name="librt psqlpy copy StringWriter",
            bench_fn=bench_psqlpy_copy_string_writer,
            iterations=iterations,
            description="librt.strings.StringWriter candidate for psqlpy binary copy encoding",
        )
    )

    return benchmarks


def _build_benchmarks(db_path: Path, iterations: int) -> list[SubsystemBenchmark]:
    """Build the list of subsystem benchmarks.

    Args:
        db_path: Path to the temporary SQLite database.
        iterations: Number of iterations per benchmark.

    Returns:
        List of SubsystemBenchmark instances.
    """
    benchmarks: list[SubsystemBenchmark] = []

    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.adapters.sqlite.driver import SqliteCursor
    from sqlspec.core.result._base import SQLResult
    from sqlspec.core.statement import SQL
    from sqlspec.storage.pipeline import SyncStoragePipeline, _decode_arrow_payload, _encode_row_payload
    from sqlspec.storage.registry import StorageRegistry
    from sqlspec.typing import Empty

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

    builder_value = 0

    def bench_builder_same_shape_varying_values() -> None:
        nonlocal builder_value
        builder_value += 1
        sql.update("test").set(value=builder_value).where_eq("rowid", builder_value).to_statement()

    benchmarks.append(
        SubsystemBenchmark(
            name="QueryBuilder.to_statement() - same shape varying values",
            bench_fn=bench_builder_same_shape_varying_values,
            iterations=iterations,
            description="Convert same-shape UPDATE builders while parameter values vary",
        )
    )

    insert_stmt = SQL("INSERT INTO test (value) VALUES (?)", ("hello",))

    def bench_compile_insert() -> None:
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

    spec = SQLSpec()
    config = SqliteConfig(connection_config={"database": str(db_path), "isolation_level": None})

    _session_ctx = spec.provide_session(config)
    session = _session_ctx.__enter__()

    session.execute("CREATE TABLE IF NOT EXISTS test (value TEXT)")
    data = [(f"value_{i}",) for i in range(100)]
    session.execute_many("INSERT INTO test (value) VALUES (?)", data)

    session.execute("INSERT INTO test (value) VALUES (?)", ("cache_prime",))
    session.execute("INSERT INTO test (value) VALUES (?)", ("cache_prime2",))

    def bench_cache_execution_miss() -> None:
        session._cached_execution("INSERT INTO unique_table (col) VALUES (?)", ("val",))

    benchmarks.append(
        SubsystemBenchmark(
            name="QC _cached_execution() - cache miss",
            bench_fn=bench_cache_execution_miss,
            iterations=iterations,
            description="Cache lookup with cache miss (early return, no execution)",
        )
    )

    def bench_cached_execution() -> None:
        session._cached_execution("INSERT INTO test (value) VALUES (?)", ("bench_val",))

    benchmarks.append(
        SubsystemBenchmark(
            name="QC _cached_execution() - full cycle",
            bench_fn=bench_cached_execution,
            iterations=iterations,
            description="Full stmt_cache lookup -> prepare -> execute cycle",
        )
    )

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

    def bench_driver_parameters() -> None:
        session._driver_parameters(("value_1", "value_2", "value_3"), session.statement_config)

    benchmarks.append(
        SubsystemBenchmark(
            name="_driver_parameters (3 params)",
            bench_fn=bench_driver_parameters,
            iterations=iterations,
            description="Format a 3-element positional parameter set",
        )
    )

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

    storage_root = db_path.parent / "bench-storage-runtime"
    storage_root.mkdir(exist_ok=True)
    storage_path = storage_root / "bench-storage.jsonl"
    storage_uri = f"file://{storage_root}"
    storage_destination = "alias://bench_store/bench-storage.jsonl"
    storage_rows = [{"id": idx, "label": f"value_{idx}"} for idx in range(16)]
    storage_registry = StorageRegistry()
    storage_registry.register_alias("bench_store", storage_uri, backend="local")
    storage_registry.get("bench_store")
    storage_pipeline = SyncStoragePipeline(registry=storage_registry)
    jsonl_payload = _encode_row_payload(storage_rows, "jsonl")
    storage_pipeline.write_rows(storage_rows, storage_destination, format_hint="jsonl")

    def bench_storage_registry_cached_alias() -> None:
        storage_registry.get("bench_store")

    benchmarks.append(
        SubsystemBenchmark(
            name="StorageRegistry.get() - cached alias",
            bench_fn=bench_storage_registry_cached_alias,
            iterations=iterations,
            description="Resolve a cached storage alias through the registry hot path",
        )
    )

    def bench_storage_write_rows_local() -> None:
        storage_pipeline.write_rows(storage_rows, storage_destination, format_hint="jsonl")

    benchmarks.append(
        SubsystemBenchmark(
            name="SyncStoragePipeline.write_rows() - local jsonl",
            bench_fn=bench_storage_write_rows_local,
            iterations=iterations,
            description="Encode rows and route them through the sync local storage pipeline",
        )
    )

    def bench_storage_decode_jsonl_arrow() -> None:
        _decode_arrow_payload(jsonl_payload, "jsonl")

    benchmarks.append(
        SubsystemBenchmark(
            name="_decode_arrow_payload() - jsonl",
            bench_fn=bench_storage_decode_jsonl_arrow,
            iterations=iterations,
            description="Decode JSONL bytes through the Arrow boundary helper",
        )
    )

    raw_conn = sqlite3.connect(str(db_path), isolation_level=None)
    raw_conn.execute("PRAGMA journal_mode = WAL")

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

    benchmarks.extend(_build_stub_driver_benchmarks(iterations))
    benchmarks.extend(_build_librt_candidate_benchmarks(iterations))

    def cleanup_benchmarks() -> None:
        _session_ctx.__exit__(None, None, None)
        raw_conn.close()
        storage_path.unlink(missing_ok=True)
        with suppress(OSError):
            storage_root.rmdir()

    benchmarks.append(
        SubsystemBenchmark(name="_cleanup_", bench_fn=lambda: None, iterations=0, setup_fn=cleanup_benchmarks)
    )

    return benchmarks


def _write_json_results(
    results: list[dict[str, Any]], output_path: str | Path, *, iterations: int, warmup: int, profile: bool
) -> None:
    """Write subsystem benchmark results to JSON."""
    output = {
        "metadata": {
            "iterations": iterations,
            "warmup": warmup,
            "profile": profile,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        },
        "included_cases": [result["name"] for result in results],
        "results": results,
    }
    Path(output_path).write_text(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
