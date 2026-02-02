import asyncio
import inspect
import os
import sqlite3
import tempfile
import time
from typing import Any

import click
from asyncpg import connect
from rich.console import Console
from rich.table import Table
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.sqlite import SqliteConfig


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
# SQLite implementations
# ------------------------------

CREATE_TEST_TABLE = "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);"
INSERT_TEST_VALUE = "INSERT INTO test (value) VALUES (?);"
INSERT_TEST_VALUE_SQLA = "INSERT INTO test (value) VALUES (:value);"

def raw_sqlite_initialization()-> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        conn.execute(CREATE_TEST_TABLE)
        conn.close()

def raw_sqlite_write_heavy()-> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        conn.execute(CREATE_TEST_TABLE)
        for i in range(10000):
            conn.execute(INSERT_TEST_VALUE, (f"value_{i}",))
        conn.commit()
        conn.close()

def raw_sqlite_read_heavy()-> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        conn = sqlite3.connect(tmp.name)
        conn.execute(CREATE_TEST_TABLE)
        for i in range(10000):
            conn.execute(INSERT_TEST_VALUE, (f"value_{i}",))
        conn.commit()
        cursor = conn.execute("SELECT * FROM test;")
        rows = cursor.fetchall()
        assert len(rows) == 10000
        conn.close()

def sqlspec_sqlite_initialization()-> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)

def sqlspec_sqlite_write_heavy()-> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            for i in range(10000):
                session.execute(INSERT_TEST_VALUE, f"value_{i}")

def sqlspec_sqlite_read_heavy()-> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        spec = SQLSpec()
        config = SqliteConfig(database=tmp.name)
        with spec.provide_session(config) as session:
            session.execute(CREATE_TEST_TABLE)
            for i in range(10000):
                session.execute(INSERT_TEST_VALUE, f"value_{i}")
            rows = session.fetch("SELECT * FROM test;")
            assert len(rows) == 10000

def sqlalchemy_sqlite_initialization()-> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        conn = engine.connect()
        conn.execute(text(CREATE_TEST_TABLE))
        conn.close()

def sqlalchemy_sqlite_write_heavy()-> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        conn = engine.connect()
        conn.execute(text(CREATE_TEST_TABLE))
        for i in range(10000):
            conn.execute(text(INSERT_TEST_VALUE_SQLA), {"value": f"value_{i}"})
        conn.close()

def sqlalchemy_sqlite_read_heavy()-> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        engine = create_engine(f"sqlite:///{tmp.name}")
        conn = engine.connect()
        conn.execute(text(CREATE_TEST_TABLE))
        for i in range(10000):
            conn.execute(text(INSERT_TEST_VALUE_SQLA), {"value": f"value_{i}"})
        result = conn.execute(text("SELECT * FROM test;"))
        rows = result.fetchall()
        assert len(rows) == 10000
        conn.close()

# Asyncpg implementations
async def raw_asyncpg_initialization()-> None:
    dsn = os.environ.get("ASYNC_PG_DSN", "postgresql://postgres:postgres@localhost/postgres")
    conn = await connect(dsn=dsn)
    await conn.execute(CREATE_TEST_TABLE)
    # truncate table to ensure clean state
    await conn.close()

async def raw_asyncpg_write_heavy()-> None:
    dsn = os.environ.get("ASYNC_PG_DSN", "postgresql://postgres:postgres@localhost/postgres")
    conn = await connect(dsn=dsn)
    for i in range(10000):
        await conn.execute(INSERT_TEST_VALUE, f"value_{i}")
    await conn.close()

async def raw_asyncpg_read_heavy():
    dsn = os.environ.get("ASYNC_PG_DSN", "postgresql://postgres:postgres@localhost/postgres")
    conn = await connect(dsn=dsn)
    rows = await conn.fetch("SELECT * FROM test;")
    assert len(rows) == 20000
    await conn.close()

async def sqlspec_asyncpg_initialization()-> None:
    sqlec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://postgres:postgres@localhost/postgres"})
    async with sqlec.provide_session(config) as session:
        await session.execute(CREATE_TEST_TABLE)

async def sqlspec_asyncpg_write_heavy()->None:
    sqlec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://postgres:postgres@localhost/postgres"})
    async with sqlec.provide_session(config) as session:
        for i in range(10000):
            await session.execute(INSERT_TEST_VALUE, f"value_{i}")

async def sqlspec_asyncpg_read_heavy()->None:
    sqlec = SQLSpec()
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://postgres:postgres@localhost/postgres"})
    async with sqlec.provide_session(config) as session:
        rows = await session.fetch("SELECT * FROM test;")
        assert len(rows) == 0

async def sqlalchemy_asyncpg_initialization()->None:
    engine = create_async_engine("postgresql+asyncpg://postgres:postgres@localhost/postgres")
    async with engine.connect() as conn:
        await conn.execute(text(CREATE_TEST_TABLE))

async def sqlalchemy_asyncpg_write_heavy() -> None:
    engine = create_async_engine("postgresql+asyncpg://postgres:postgres@localhost/postgres")
    async with engine.connect() as conn:
        for i in range(10000):
            await conn.execute(text(INSERT_TEST_VALUE), {"value": f"value_{i}"})

async def sqlalchemy_asyncpg_read_heavy()-> None:
    engine = create_async_engine("postgresql+asyncpg://postgres:postgres@localhost/postgres")
    async with engine.begin() as conn:
        result = await conn.execute(text("SELECT * FROM test;"))
        rows = result.fetchall()
        assert len(rows) == 0

SCENARIO_REGISTRY = {
        # SQLite scenarios
    ("raw", "sqlite", "initialization"): raw_sqlite_initialization,
    ("raw", "sqlite", "write_heavy"): raw_sqlite_write_heavy,
    ("raw", "sqlite", "read_heavy"): raw_sqlite_read_heavy,
    ("sqlspec", "sqlite", "initialization"): sqlspec_sqlite_initialization,
    ("sqlspec", "sqlite", "write_heavy"): sqlspec_sqlite_write_heavy,
    ("sqlspec", "sqlite", "read_heavy"): sqlspec_sqlite_read_heavy,
    ("sqlalchemy", "sqlite", "initialization"): sqlalchemy_sqlite_initialization,
    ("sqlalchemy", "sqlite", "write_heavy"): sqlalchemy_sqlite_write_heavy,
    ("sqlalchemy", "sqlite", "read_heavy"): sqlalchemy_sqlite_read_heavy,
    # Asyncpg scenarios
    # ("raw", "asyncpg", "initialization"): raw_asyncpg_initialization,
    # ("raw", "asyncpg", "write_heavy"): raw_asyncpg_write_heavy,
    # ("raw", "asyncpg", "read_heavy"): raw_asyncpg_read_heavy,
    # ("sqlspec", "asyncpg", "initialization"): sqlspec_asyncpg_initialization,
    # ("sqlspec", "asyncpg", "write_heavy"): sqlspec_asyncpg_write_heavy,
    # ("sqlspec", "asyncpg", "read_heavy"): sqlspec_asyncpg_read_heavy,
    # ("sqlalchemy", "asyncpg", "initialization"): sqlalchemy_asyncpg_initialization,
    # ("sqlalchemy", "asyncpg", "write_heavy"): sqlalchemy_asyncpg_write_heavy,
    # ("sqlalchemy", "asyncpg", "read_heavy"): sqlalchemy_asyncpg_read_heavy,
}

def print_benchmark_table(results: list[dict[str, Any]]) -> None:
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
            percent_slower = f"{100 * (t - raw_time) / raw_time:.1f}%" if raw_time and raw_time > 0 else "n/a"
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


