#!/usr/bin/env python3
import argparse
import time
import sqlite3
import tempfile
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Callable

from sqlspec import SQLSpec, SyncDatabaseConfig
from sqlalchemy import create_engine, text

# ==========================
# CLI
# ==========================

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=10_000)
    p.add_argument("--reads", type=int, default=5_000)
    p.add_argument(
        "--drivers",
        nargs="+",
        default=["sqlite"],
        help="Drivers to test (sqlite, duckdb, ...)",
    )
    p.add_argument(
        "--modes",
        nargs="+",
        default=["session", "write", "read"],
        choices=["session", "write", "read"],
    )
    p.add_argument("--wal", action="store_true")
    p.add_argument("--memory", action="store_true")
    return p.parse_args()

# ==========================
# UTIL
# ==========================

def now():
    return time.perf_counter()

def run(label: str, fn: Callable[[], None]) -> float:
    fn()  # warmup
    t0 = now()
    fn()
    dt = now() - t0
    print(f"{label:<30} {dt:.5f}s")
    return dt

def slowdown(base: float, other: float) -> float:
    return other / base if base else float("inf")

# ==========================
# DRIVER REGISTRY
# ==========================

@dataclass
class Driver:
    name: str
    raw_session: Callable[[str], Callable[[], None]]
    raw_write: Callable[[str], Callable[[], None]]
    raw_read: Callable[[str], Callable[[], None]]

    sqlspec_config: Callable[[SQLSpec, str], object]
    sqlalchemy_url: Callable[[str], str]

# ==========================
# SQLITE IMPLEMENTATION
# ==========================

def sqlite_connect(db, wal=False):
    conn = sqlite3.connect(db)
    if wal:
        conn.execute("pragma journal_mode=wal")
    return conn

def sqlite_raw_session(db, wal):
    return lambda: sqlite_connect(db, wal).close()

def sqlite_populate(conn: sqlite3.Connection, rows):
    cur = conn.cursor()
    cur.execute("drop table if exists notes")
    cur.execute("create table notes (id integer primary key, body text)")
    cur.executemany(
        "insert into notes (body) values (?)",
        [(f"note {i}",) for i in range(rows)],
    )
    conn.commit()

def sqlite_raw_write(db, rows, wal):
    def fn():
        conn = sqlite_connect(db, wal)
        sqlite_populate(conn, rows)
        conn.close()
    return fn

def sqlite_raw_read(db, rows, reads, wal):
    def fn():
        conn = sqlite_connect(db, wal)
        sqlite_populate(conn, rows)
        cur = conn.cursor()
        for i in range(reads):
            cur.execute(
                "select body from notes where id = ?",
                ((i % rows) + 1,),
            )
            cur.fetchone()
        conn.close()
    return fn


# ==========================
# SQLSPEC BENCHES
# ==========================

def sqlspec_session(spec: SQLSpec, cfg: SyncDatabaseConfig[Any, Any, Any]):
    def fn():
        with spec.provide_session(cfg):
            pass
    return fn

def sqlspec_populate(spec: SQLSpec, cfg: SyncDatabaseConfig[Any,Any,Any], rows):
    with spec.provide_session(cfg) as s:
        s.execute("drop table if exists notes")
        s.execute("create table notes (id integer primary key, body text)")
        s.execute_many(
            "insert into notes (body) values (?)",
            [(f"note {i}",) for i in range(rows)],
        )

def sqlspec_write(spec, cfg, rows):
    return lambda: sqlspec_populate(spec, cfg, rows)

def sqlspec_read(spec, cfg, rows, reads):
    def fn():
        sqlspec_populate(spec, cfg, rows)
        with spec.provide_session(cfg) as s:
            for i in range(reads):
                s.execute(
                    "select body from notes where id = ?",
                    ((i % rows) + 1,),
                ).one()
    return fn

# ==========================
# SQLALCHEMY BENCHES
# ==========================

def sa_session(url):
    engine = create_engine(url, future=True)
    return lambda: engine.connect().close()

def sa_populate(conn, rows):
    conn.execute(text("drop table if exists notes"))
    conn.execute(text("create table notes (id integer primary key, body text)"))
    conn.execute(
        text("insert into notes (body) values (:body)"),
        [{"body": f"note {i}"} for i in range(rows)],
    )

def sa_write(url, rows):
    engine = create_engine(url, future=True)
    def fn():
        with engine.begin() as conn:
            sa_populate(conn, rows)
    return fn

def sa_read(url, rows, reads):
    engine = create_engine(url, future=True)
    def fn():
        with engine.begin() as conn:
            sa_populate(conn, rows)
            for i in range(reads):
                conn.execute(
                    text("select body from notes where id = :id"),
                    {"id": (i % rows) + 1},
                ).fetchone()
    return fn

# ==========================
# RUNNER
# ==========================

def run_driver(driver: Driver, db: str, args):
    print(f"\n--- DRIVER: {driver.name} ---")

    spec = SQLSpec()
    cfg = driver.sqlspec_config(spec, db)
    sa_url = driver.sqlalchemy_url(db)

    if "session" in args.modes:
        print("\nSession / connection cost")
        t0 = run("raw", driver.raw_session(db))
        t1 = run("sqlspec", sqlspec_session(spec, cfg))
        t2 = run("sqlalchemy", sa_session(sa_url))
        print(
            f"Slowdowns vs raw: "
            f"sqlspec {slowdown(t0, t1):.2f}x, "
            f"sqlalchemy {slowdown(t0, t2):.2f}x"
        )

    if "write" in args.modes:
        print("\nWrite-heavy workload")
        t0 = run("raw", driver.raw_write(db))
        t1 = run("sqlspec", sqlspec_write(spec, cfg, args.rows))
        t2 = run("sqlalchemy", sa_write(sa_url, args.rows))
        print(
            f"Slowdowns vs raw: "
            f"sqlspec {slowdown(t0, t1):.2f}x, "
            f"sqlalchemy {slowdown(t0, t2):.2f}x"
        )

    if "read" in args.modes:
        print("\nRead-heavy workload")
        t0 = run("raw", driver.raw_read(db))
        t1 = run("sqlspec", sqlspec_read(spec, cfg, args.rows, args.reads))
        t2 = run("sqlalchemy", sa_read(sa_url, args.rows, args.reads))
        print(
            f"Slowdowns vs raw: "
            f"sqlspec {slowdown(t0, t1):.2f}x, "
            f"sqlalchemy {slowdown(t0, t2):.2f}x"
        )

# ==========================
# SQLITE DRIVER FACTORY
# ==========================
def make_sqlite_driver(args):
    from sqlspec.adapters.sqlite import SqliteConfig

    return Driver(
        name="sqlite",
        raw_session=lambda db: sqlite_raw_session(db, args.wal),
        raw_write=lambda db: sqlite_raw_write(db, args.rows, args.wal),
        raw_read=lambda db: sqlite_raw_read(db, args.rows, args.reads, args.wal),
        sqlspec_config=lambda spec, db: spec.add_config(
            SqliteConfig(connection_config={"database": db})
        ),
        sqlalchemy_url=lambda db: f"sqlite:///{db}",
    )


# ==========================
# MAIN
# ==========================

def main():
    args = parse_args()

    print(
        f"ROWS={args.rows:,}, READS={args.reads:,}, "
        f"WAL={'on' if args.wal else 'off'}, "
        f"DB={'memory' if args.memory else 'file'}"
    )

    drivers: dict[str, Callable[Any, Any]] = {
        "sqlite": lambda: make_sqlite_driver(args),
    }

    selected = [d for d in args.drivers if d in drivers]

    if args.memory:
        db = ":memory:"
        for name in selected:
            run_driver(drivers[name](), db, args)
    else:
        with tempfile.TemporaryDirectory() as d:
            db = str(Path(d) / "bench.db")
            for name in selected:
                run_driver(drivers[name](), db, args)

if __name__ == "__main__":
    main()

