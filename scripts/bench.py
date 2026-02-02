#!/usr/bin/env python


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

    for row in results:
        table.add_row(
            row["driver"],
            row["library"],
            row["scenario"],
            f"{row['time']:.4f}"
        )
    console.print(table)

def raw_driver_scenario(driver: str, scenario: str):
    time.sleep(0.05)  # Placeholder

def sqlspec_scenario(driver: str, scenario: str):
    time.sleep(0.07)  # Placeholder

def sqlalchemy_scenario(driver: str, scenario: str):
    time.sleep(0.09)  # Placeholder





if __name__ == "__main__":
    main()


