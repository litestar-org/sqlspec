#!/usr/bin/env python
from sqlalchemy import text
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
    for drv in driver:
        click.echo(f"Running benchmark for driver: {drv}")
        results.extend(run_benchmark(drv))
    if results:
        print_benchmark_table(results)
    else:
        click.echo("No benchmark results to display.")
    click.echo(f"Benchmarks complete for drivers: {', '.join(driver)}")



def run_benchmark(driver: str):
    """Benchmark three scenarios for a given driver and multiple libraries."""
    libraries = [
        ("raw", raw_driver_scenario),
        ("sqlspec", sqlspec_scenario),
        ("sqlalchemy", sqlalchemy_scenario),
    ]
    scenarios = [
        ("initialization", "initialization"),
        ("write_heavy", "write_heavy"),
        ("read_heavy", "read_heavy"),
    ]
    results = []
    for scenario_name, scenario_func_name in scenarios:
        for lib_name, lib_func in libraries:
            start = time.perf_counter()
            lib_func(driver, scenario_func_name)
            elapsed = time.perf_counter() - start
            results.append({
                "driver": driver,
                "library": lib_name,
                "scenario": scenario_name,
                "time": elapsed,
            })
    return results

def print_benchmark_table(results):
    from rich.console import Console
    from rich.table import Table
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



def raw_driver_scenario(driver: str, scenario: str):
    if driver == "sqlite" and scenario == "initialization":
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            # create a table to simulate some initialization work
            conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);")
            conn.close()
    else:
        time.sleep(0.01)  # Placeholder for other drivers/scenarios

def sqlspec_scenario(driver: str, scenario: str):
    if driver == "sqlite" and scenario == "initialization":
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            spec = SQLSpec()
            config = SqliteConfig(database=tmp.name)
            with spec.provide_session(config) as session:
                # create a table to simulate some initialization work
                session.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);")
    else:
        time.sleep(0.01)  # Placeholder

def sqlalchemy_scenario(driver: str, scenario: str):
    if driver == "sqlite" and scenario == "initialization":
        from sqlalchemy import create_engine
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            engine = create_engine(f"sqlite:///{tmp.name}")
            conn = engine.connect()
            # create a table to simulate some initialization work
            conn.execute(text("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);"))
            conn.close()
    else:
        time.sleep(0.01)  # Placeholder


if __name__ == "__main__":
    main()


