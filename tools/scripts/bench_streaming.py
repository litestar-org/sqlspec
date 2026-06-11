"""Compare SQLite ``execute`` and ``select_stream`` memory/time behavior."""

import argparse
import gc
import json
import statistics
import time
import tracemalloc
from collections.abc import Callable
from dataclasses import asdict, dataclass

import click

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.driver import SyncDriverAdapterBase

SELECT_SQL = "SELECT id, name, value FROM streaming_bench ORDER BY id"


@dataclass(frozen=True)
class BenchmarkResult:
    """Summary for one streaming benchmark scenario."""

    method: str
    median_seconds: float
    min_seconds: float
    peak_kib: float
    row_count: int


def _measure(fn: Callable[[], int]) -> tuple[float, float, int]:
    gc.collect()
    tracemalloc.start()
    started = time.perf_counter()
    row_count = fn()
    elapsed = time.perf_counter() - started
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return elapsed, peak_bytes / 1024, row_count


def _seed(driver: SyncDriverAdapterBase, rows: int) -> None:
    driver.execute_script(
        """
        CREATE TABLE streaming_bench (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL
        )
        """
    )
    payload = [(idx, f"name-{idx}", idx * 2) for idx in range(rows)]
    driver.execute_many("INSERT INTO streaming_bench (id, name, value) VALUES (?, ?, ?)", payload)


def _run_execute(driver: SyncDriverAdapterBase) -> int:
    rows = driver.execute(SELECT_SQL).get_data()
    return len(rows)


def _run_select_stream(driver: SyncDriverAdapterBase, chunk_size: int) -> int:
    row_count = 0
    with driver.select_stream(SELECT_SQL, chunk_size=chunk_size) as stream:
        for _row in stream:
            row_count += 1
    return row_count


def _summarize(method: str, samples: list[tuple[float, float, int]]) -> BenchmarkResult:
    row_counts = {sample[2] for sample in samples}
    if len(row_counts) != 1:
        msg = f"{method} returned inconsistent row counts: {sorted(row_counts)}"
        raise RuntimeError(msg)
    return BenchmarkResult(
        method=method,
        median_seconds=statistics.median(sample[0] for sample in samples),
        min_seconds=min(sample[0] for sample in samples),
        peak_kib=max(sample[1] for sample in samples),
        row_count=row_counts.pop(),
    )


def run_benchmark(rows: int, iterations: int, chunk_size: int) -> list[BenchmarkResult]:
    """Run the SQLite streaming benchmark scenarios."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    with spec.provide_session(config) as driver:
        _seed(driver, rows)
        execute_samples = [_measure(lambda: _run_execute(driver)) for _ in range(iterations)]
        stream_samples = [_measure(lambda: _run_select_stream(driver, chunk_size)) for _ in range(iterations)]
    return [_summarize("execute", execute_samples), _summarize("select_stream", stream_samples)]


def main() -> None:
    """Run the benchmark and print a compact report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    args = parser.parse_args()

    results = run_benchmark(args.rows, args.iterations, args.chunk_size)
    if args.json:
        click.echo(json.dumps([asdict(result) for result in results], indent=2, sort_keys=True))
        return

    click.echo(f"rows={args.rows} iterations={args.iterations} chunk_size={args.chunk_size}")
    click.echo("method          median_s   min_s      peak_kib   rows")
    for result in results:
        click.echo(
            f"{result.method:<14} {result.median_seconds:>8.6f} "
            f"{result.min_seconds:>8.6f} {result.peak_kib:>10.1f} {result.row_count:>6}"
        )


if __name__ == "__main__":
    main()
