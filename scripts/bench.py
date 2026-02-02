#!/usr/bin/env python
import asyncio
from asyncpg import connect
import inspect
import os
from typing import Any
from rich.console import Console
from rich.table import Table
from sqlalchemy import create_engine, text
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig


import sqlite3
import tempfile
import time
import click

@click.command()
@click.option(
    "--driver",
    multiple=True,
    default=["sqlite"],
    show_default=True,
    help="List of driver names to benchmark (default: sqlite)",
)
def main(driver: tuple[str, ...]) -> None:
    """Run benchmarks for the specified drivers."""
    results = []
    errors = []
    for drv in driver:
        click.echo(f"Running benchmark for driver: {drv}")
        results.extend(run_benchmark(drv, errors))
    if results:
        print_benchmark_table(results)
    else:
        click.echo("No benchmark results to display.")
    if errors:
        for err in errors:
            click.secho(f"Error: {err}", fg="red")
    click.echo(f"Benchmarks complete for drivers: {', '.join(driver)}")





def run_benchmark(driver: str, errors: list[str]) -> list[dict[str, Any]]:
    # List of (library, driver) pairs
    libraries = [
        ("raw", driver),
        ("sqlspec", driver),
        ("sqlalchemy", driver),
    ]
    scenarios = [
        "initialization",
        "write_heavy",
        "read_heavy",
    ]
    results = []
    for scenario in scenarios:
        for lib, drv in libraries:
            func = SCENARIO_REGISTRY.get((lib, drv, scenario))
            if func is None:
                errors.append(f"No implementation for library={lib}, driver={drv}, scenario={scenario}")
                continue
            start = time.perf_counter()
            if inspect.iscoroutinefunction(func):
                asyncio.run(func())
            else:
                func()
            elapsed = time.perf_counter() - start
            results.append({
                "driver": drv,
                "library": lib,
                "scenario": scenario,
                "time": elapsed,
            })
    return results


# --- Scenario helpers and registry ---
def raw_sqlite_initialization():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);")
        conn.close()

def raw_sqlite_write_heavy():
    ...

def raw_sqlite_read_heavy():
    ...

def sqlspec_sqlite_initialization():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);")

def sqlspec_sqlite_write_heavy():
    ...

def sqlspec_sqlite_read_heavy():
    ...

def sqlalchemy_sqlite_initialization():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        conn = engine.connect()
        conn.execute(text("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);"))
        conn.close()

def sqlalchemy_sqlite_write_heavy():
    ...

def sqlalchemy_sqlite_read_heavy():
    ...

async def raw_asyncpg_initialization():
    dsn = os.environ.get("ASYNC_PG_DSN", "postgresql://postgres:postgres@localhost/postgres")
    conn = await connect(dsn=dsn)
    await conn.execute("CREATE TABLE IF NOT EXISTS test (id serial PRIMARY KEY, value text);")
    await conn.close()


async def raw_asyncpg_write_heavy():
    ...

async def raw_asyncpg_read_heavy():
    ...

SCENARIO_REGISTRY = {
    ("raw", "sqlite", "initialization"): raw_sqlite_initialization,
    ("raw", "sqlite", "write_heavy"): raw_sqlite_write_heavy,
    ("raw", "sqlite", "read_heavy"): raw_sqlite_read_heavy,
    ("sqlspec", "sqlite", "initialization"): sqlspec_sqlite_initialization,
    ("sqlspec", "sqlite", "write_heavy"): sqlspec_sqlite_write_heavy,
    ("sqlspec", "sqlite", "read_heavy"): sqlspec_sqlite_read_heavy,
    ("sqlalchemy", "sqlite", "initialization"): sqlalchemy_sqlite_initialization,
    ("sqlalchemy", "sqlite", "write_heavy"): sqlalchemy_sqlite_write_heavy,
    ("sqlalchemy", "sqlite", "read_heavy"): sqlalchemy_sqlite_read_heavy,
    ("raw", "asyncpg", "initialization"): raw_asyncpg_initialization,
    ("raw", "asyncpg", "write_heavy"): raw_asyncpg_write_heavy,
    ("raw", "asyncpg", "read_heavy"): raw_asyncpg_read_heavy,
}

def print_benchmark_table(results):
    console = Console()
    table = Table(title="Benchmark Results")
    table.add_column("Driver", style="cyan", no_wrap=True)
    table.add_column("Library", style="magenta")
    table.add_column("Scenario", style="green")
    table.add_column("Time (s)", justify="right", style="yellow")
    table.add_column("% Slower vs Raw", justify="right", style="red")

    # Build a lookup for raw times: {(driver, scenario): time}
    raw_times = {}
    for row in results:
        if row["library"] == "raw":
            raw_times[(row["driver"], row["scenario"])] = row["time"]

    for row in results:
        driver = row["driver"]
        scenario = row["scenario"]
        lib = row["library"]
        t = row["time"]
        if lib == "raw":
            percent_slower = "—"
        else:
            raw_time = raw_times.get((driver, scenario))
            if raw_time and raw_time > 0:
                percent_slower = f"{100 * (t - raw_time) / raw_time:.1f}%"
            else:
                percent_slower = "n/a"
        table.add_row(
            driver,
            lib,
            scenario,
            f"{t:.4f}",
            percent_slower
        )
    console.print(table)


if __name__ == "__main__":
    main()


