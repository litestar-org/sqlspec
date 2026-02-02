import sqlite3
import tempfile
import time
from pathlib import Path

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

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


from sqlspec.observability import LoggingConfig, ObservabilityConfig, TelemetryConfig


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
def run_benchmark(fn, label):
    times = []
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


import cProfile
import pstats
from pathlib import Path

__all__ = ("bench_raw_sqlite", "bench_sqlspec", "bench_sqlspec_dict", "run_benchmark")


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


ROWS = 10000
RUNS = 5  # Reduced for profiling

# ... (rest of the functions remain same)

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
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
