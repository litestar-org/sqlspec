import time
import sqlite3
import tempfile
from pathlib import Path
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

ROWS = 10000
RUNS = 10

# -------------------------
# Raw sqlite3 benchmark
# -------------------------
def bench_raw_sqlite(db_path: Path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "create table if not exists notes (id integer primary key, body text)"
    )
    conn.commit()
    for i in range(ROWS):
        cur.execute(
            "insert into notes (body) values (?)", (f"note {i}",),
        )
    conn.commit()
    conn.close()

# -------------------------
# SQLSpec benchmark
# -------------------------
def bench_sqlspec(db_path: Path):
    spec = SQLSpec()
    config = spec.add_config(
        SqliteConfig(connection_config={"database": str(db_path)})
    )
    with spec.provide_session(config) as session:
        session.execute(
            "create table if not exists notes (id integer primary key, body text)"
        )
        for i in range(ROWS):
            session.execute(
                "insert into notes (body) values (?)", (f"note {i}",),
            )

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
    
    avg = sum(times) / len(times)
    print(f"{label:<15} avg over {RUNS} runs: {avg:.4f}s")
    return avg

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    print(f"Benchmark: create table + insert {ROWS:,} rows\n")
    raw_time = run_benchmark(bench_raw_sqlite, "raw sqlite3")
    sqlspec_time = run_benchmark(bench_sqlspec, "sqlspec")
    
    slowdown = sqlspec_time / raw_time
    print("\nSummary")
    print("-------")
    print(f"raw sqlite3 : {raw_time:.4f}s")
    print(f"sqlspec     : {sqlspec_time:.4f}s")
    print(f"slowdown    : {slowdown:.2f}x")
