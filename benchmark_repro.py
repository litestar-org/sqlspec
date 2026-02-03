import cProfile
import pstats
import sqlite3
import tempfile
import time
import tracemalloc
from pathlib import Path
from typing import TYPE_CHECKING

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.observability import LoggingConfig, ObservabilityConfig, TelemetryConfig

if TYPE_CHECKING:
    from collections.abc import Callable

ROWS = 10000
RUNS = 10


# -------------------------
# Raw sqlite3 benchmark
# -------------------------
def bench_raw_sqlite(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("create table if not exists notes (id integer primary key, body text)")
    conn.commit()
    for i in range(ROWS):
        cur.execute("insert into notes (body) values (?)", (f"note {i}",))
    conn.commit()
    conn.close()


# -------------------------
# SQLSpec benchmark
# -------------------------
def bench_sqlspec(db_path: Path) -> None:
    # Disable all observability for pure performance measurement
    obs_config = ObservabilityConfig(
        telemetry=TelemetryConfig(enable_spans=False),
        logging=LoggingConfig(include_sql_hash=False, include_trace_context=False),
        print_sql=False,
    )
    spec = SQLSpec(observability_config=obs_config)
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))
    with spec.provide_session(config) as session:
        session.execute("create table if not exists notes (id integer primary key, body text)")
        for i in range(ROWS):
            session.execute("insert into notes (body) values (?)", (f"note {i}",))


# -------------------------
# Timing helper
# -------------------------
def run_benchmark(fn: "Callable[[Path], None]", label: str) -> float:
    times: list[float] = []
    # warm-up run (not measured)
    with tempfile.TemporaryDirectory() as d:
        fn(Path(d) / "warmup.db")

    for _ in range(RUNS):
        with tempfile.TemporaryDirectory() as d:
            db_path = Path(d) / "test.db"
            start = time.perf_counter()
            fn(db_path)
            elapsed = time.perf_counter() - start
            times.append(elapsed)

    return sum(times) / len(times)


def run_benchmark_allocations(fn: "Callable[[Path], None]") -> "tuple[int, int]":
    """Return (current, peak) allocated bytes for a benchmark run."""
    with tempfile.TemporaryDirectory() as d:
        db_path = Path(d) / "alloc.db"
        tracemalloc.start()
        fn(db_path)
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    return current, peak

__all__ = (
    "assert_compile_bypass",
    "bench_raw_sqlite",
    "bench_sqlite_sqlglot",
    "bench_sqlite_sqlglot_copy",
    "bench_sqlite_sqlglot_nocache",
    "bench_sqlspec",
    "bench_sqlspec_dict",
    "profile_cache_hit_compile_calls",
    "run_benchmark",
)


# -------------------------
# Pure sqlite3 + sqlglot benchmark (parse once, cached SQL)
# -------------------------
def bench_sqlite_sqlglot(db_path: Path) -> None:
    """Benchmark raw sqlite3 with only sqlglot parsing overhead.

    This simulates optimal SQLSpec behavior: parse once, cache SQL, reuse.
    Shows the minimum overhead from using sqlglot for SQL parsing.
    """
    import sqlglot

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("create table if not exists notes (id integer primary key, body text)")
    conn.commit()

    # Parse the SQL once with sqlglot and cache the generated SQL
    sql = "insert into notes (body) values (?)"
    parsed = sqlglot.parse_one(sql, dialect="sqlite")
    cached_sql = parsed.sql(dialect="sqlite")  # Cache this!

    for i in range(ROWS):
        # Use cached SQL string (like SQLSpec does on cache hit)
        cur.execute(cached_sql, (f"note {i}",))

    conn.commit()
    conn.close()


# -------------------------
# Pure sqlite3 + sqlglot with .sql() per call (no caching)
# -------------------------
def bench_sqlite_sqlglot_nocache(db_path: Path) -> None:
    """Benchmark raw sqlite3 with sqlglot .sql() called each time.

    This shows the cost if we regenerated SQL from AST every time,
    which would be terrible and SQLSpec avoids via caching.
    """
    import sqlglot

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("create table if not exists notes (id integer primary key, body text)")
    conn.commit()

    sql = "insert into notes (body) values (?)"
    parsed = sqlglot.parse_one(sql, dialect="sqlite")

    for i in range(ROWS):
        # Regenerate SQL each time (NO CACHING - worst case)
        generated_sql = parsed.sql(dialect="sqlite")
        cur.execute(generated_sql, (f"note {i}",))

    conn.commit()
    conn.close()


# -------------------------
# Pure sqlite3 + sqlglot with expression.copy() benchmark
# -------------------------
def bench_sqlite_sqlglot_copy(db_path: Path) -> None:
    """Benchmark raw sqlite3 with sqlglot expression.copy() per call.

    This shows the overhead when we copy the expression each time,
    which happens in some SQLSpec code paths for safety.
    """
    import sqlglot

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("create table if not exists notes (id integer primary key, body text)")
    conn.commit()

    sql = "insert into notes (body) values (?)"
    parsed = sqlglot.parse_one(sql, dialect="sqlite")
    cached_sql = parsed.sql(dialect="sqlite")  # Cache the SQL

    for i in range(ROWS):
        # Copy expression each time (like SQLSpec's defensive copying)
        # but still use cached SQL for execution
        _ = parsed.copy()  # Overhead we're measuring
        cur.execute(cached_sql, (f"note {i}",))

    conn.commit()
    conn.close()


# -------------------------
# SQLSpec benchmark with dict parameters
# -------------------------
def bench_sqlspec_dict(db_path: Path) -> None:
    """Benchmark with dict parameters to test sorted() removal."""
    # Disable all observability for pure performance measurement
    obs_config = ObservabilityConfig(
        telemetry=TelemetryConfig(enable_spans=False),
        logging=LoggingConfig(include_sql_hash=False, include_trace_context=False),
        print_sql=False,
    )
    spec = SQLSpec(observability_config=obs_config)
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))
    with spec.provide_session(config) as session:
        session.execute("create table if not exists notes (id integer primary key, body text)")
        for i in range(ROWS):
            session.execute("insert into notes (body) values (:body)", {"body": f"note {i}"})


def profile_cache_hit_compile_calls(db_path: Path) -> int:
    """Return pipeline compilation call count for repeated inserts."""
    obs_config = ObservabilityConfig(
        telemetry=TelemetryConfig(enable_spans=False),
        logging=LoggingConfig(include_sql_hash=False, include_trace_context=False),
        print_sql=False,
    )
    spec = SQLSpec(observability_config=obs_config)
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    from sqlspec.core import pipeline as pipeline_module

    calls = 0
    original = pipeline_module.compile_with_pipeline

    def wrapped(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    with spec.provide_session(config) as session:
        session.execute("create table if not exists notes (id integer primary key, body text)")
        pipeline_module.compile_with_pipeline = wrapped
        try:
            for i in range(ROWS):
                session.execute("insert into notes (body) values (?)", (f"note {i}",))
        finally:
            pipeline_module.compile_with_pipeline = original

    return calls


def assert_compile_bypass(db_path: Path) -> None:
    """Assert compile is bypassed on cache hits after initial insert."""
    calls = profile_cache_hit_compile_calls(db_path)
    if calls != 1:
        msg = f"Expected 1 compilation call for repeated inserts, got {calls}"
        raise AssertionError(msg)

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as d:
        assert_compile_bypass(Path(d) / "compile_check.db")

    with tempfile.TemporaryDirectory() as d:
        db_path = Path(d) / "profile.db"
        profiler = cProfile.Profile()
        profiler.enable()
        bench_sqlspec(db_path)
        profiler.disable()
        stats = pstats.Stats(profiler).sort_stats("tottime")
        stats.print_stats(30)

    raw_time = run_benchmark(bench_raw_sqlite, "raw sqlite3")
    sqlspec_time = run_benchmark(bench_sqlspec, "sqlspec")

    slowdown = sqlspec_time / raw_time
