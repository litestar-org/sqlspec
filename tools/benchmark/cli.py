"""CLI interface for benchmark tool."""

from pathlib import Path
from typing import Optional

import rich_click as click
from rich.console import Console
from rich.panel import Panel

from tools.benchmark.config import BenchmarkConfig
from tools.benchmark.core.runner import BenchmarkRunner
from tools.benchmark.infrastructure.containers import ContainerManager

# Configure rich-click
click.rich_click.USE_RICH_MARKUP = True
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True

# Create console with wide display to prevent table truncation
console = Console(width=200, force_terminal=True)


@click.group()
@click.option("--config", type=click.Path(exists=True, path_type=Path), help="Configuration file path")
@click.option("--storage", type=click.Path(path_type=Path), help="Override storage path for results")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.pass_context
def cli(ctx: click.Context, config: Optional[Path], storage: Optional[Path], verbose: bool) -> None:
    """SQLSpec Benchmark Tool - Performance testing framework."""
    # Create config
    benchmark_config = BenchmarkConfig.from_env()

    if storage:
        benchmark_config.storage_path = storage
    if verbose:
        benchmark_config.verbose = verbose

    # Store in context
    ctx.obj = {
        "config": benchmark_config,
        "console": console,
        "runner": BenchmarkRunner(benchmark_config, console),
        "container_manager": ContainerManager(console),
    }


@cli.command()
@click.option(
    "--suite",
    type=click.Choice([
        "parameters",
        "sql-compilation",
        "orm-comparison",
        "caching-comparison",
        "caching-optimization",
        "async-operations",
    ]),
    help="Specific benchmark suite to run",
)
@click.option("--adapter", default="all", help="Adapter to test or 'all'")
@click.option("--iterations", type=int, help="Override default iterations")
@click.option("--quick", is_flag=True, help="Run in quick mode with fewer iterations")
@click.option("--keep-containers", is_flag=True, help="Don't cleanup containers after run")
@click.option("--no-containers", is_flag=True, help="Skip container-based tests")
# Enhanced display options
@click.option("--show-all", is_flag=True, help="Show all results without limits")
@click.option("--max-items", type=int, default=20, help="Maximum items to display in tables (default: 20)")
@click.option("--table-width", type=int, help="Override table width (bypasses terminal detection)")
@click.option(
    "--display-mode",
    type=click.Choice(["compact", "detailed", "matrix"]),
    default="compact",
    help="Display mode for results",
)
@click.option("--no-truncate", is_flag=True, default=True, help="Disable all result truncation")
@click.pass_context
def run(
    ctx: click.Context,
    suite: Optional[str],
    adapter: str,
    iterations: Optional[int],
    quick: bool,
    keep_containers: bool,
    no_containers: bool,
    show_all: bool,
    max_items: int,
    table_width: Optional[int],
    display_mode: str,
    no_truncate: bool,
) -> None:
    """Run benchmark suites."""
    config: BenchmarkConfig = ctx.obj["config"]
    runner: BenchmarkRunner = ctx.obj["runner"]
    container_manager: ContainerManager = ctx.obj["container_manager"]

    # Update config based on flags
    if iterations:
        config.iterations = iterations
    if quick:
        config.quick_mode = True
        config.iterations = min(config.iterations, 100)
    config.keep_containers = keep_containers
    config.no_containers = no_containers

    # Cleanup old data
    runner.cleanup_old_data()

    console.print(
        Panel.fit(
            f"[bold]SQLSpec Benchmark Tool[/bold]\nIterations: [cyan]{config.iterations:,}[/cyan]", border_style="green"
        )
    )

    from tools.benchmark.suites.caching_comparison import CachingComparisonBenchmark
    from tools.benchmark.suites.caching_optimization import CachingOptimizationBenchmark
    from tools.benchmark.suites.orm_comparison import ORMComparisonBenchmark
    from tools.benchmark.suites.parameters import ParametersBenchmark
    from tools.benchmark.suites.sql_compilation import SQLCompilationBenchmark
    from tools.benchmark.visualization.reports import BenchmarkSummary

    # Map suite names to classes
    suite_map = {
        "parameters": ParametersBenchmark,
        "sql-compilation": SQLCompilationBenchmark,
        "orm-comparison": ORMComparisonBenchmark,
        "caching-comparison": CachingComparisonBenchmark,
        "caching-optimization": CachingOptimizationBenchmark,
    }

    # Determine which suites to run
    suites_to_run = []

    if suite:
        # Run specific suite
        if suite in suite_map:
            suites_to_run.append(suite_map[suite](config, runner, console))
        else:
            console.print(f"[red]Unknown suite: {suite}[/red]")
            return
    else:
        suites_to_run.extend(suite_class(config, runner, console) for suite_class in suite_map.values())

    # Run suites and collect all results
    all_results = {}
    regressions = []
    improvements = []

    try:
        # Create display options dictionary
        display_options = {
            "show_all": show_all,
            "max_items": max_items,
            "display_mode": display_mode,
            "no_truncate": no_truncate,
            "table_width": table_width,
        }

        summary = BenchmarkSummary(console, display_options)

        for benchmark_suite in suites_to_run:
            console.print(f"\n[bold cyan]Running {benchmark_suite.description}[/bold cyan]")
            suite_results = benchmark_suite.run(adapter=adapter)

            # Display suite-specific results if running single suite
            if len(suites_to_run) == 1:
                console.print("\n" + "=" * 80)
                summary.display_suite_results(benchmark_suite.name, suite_results)

            # Collect results for summary
            all_results.update(suite_results)

            # Collect regression/improvement info
            for operation, result in suite_results.items():
                # Extract operation name for proper lookup (remove adapter prefix if present)
                base_operation = operation.split("_", 1)[-1] if "_" in operation else operation
                regression_info = benchmark_suite.get_regression_info(base_operation, result, adapter)
                if regression_info:
                    info_text, pct_change = regression_info
                    if pct_change > 0:  # Regression (slower)
                        regressions.append((operation, info_text, pct_change))
                    else:  # Improvement (faster)
                        improvements.append((operation, info_text, abs(pct_change)))
    finally:
        # Cleanup containers if needed
        if not config.keep_containers and not config.no_containers:
            container_manager.cleanup_containers()

    # Display comprehensive summary
    if all_results:
        if len(suites_to_run) > 1:
            console.print("\n" + "=" * 80)
            console.print("\n[bold cyan]📊 Detailed Analysis by Suite[/bold cyan]\n")

            # Display all suite-specific analyses
            summary.display_suite_results("all", all_results)

            console.print("\n" + "=" * 80)

        summary.display_overall_summary(
            all_results, system_info=runner.system_info.to_dict(), regressions=regressions, improvements=improvements
        )

        console.print("\n")
        # Use show_all to determine count, otherwise use max_items
        top_count = len(all_results) if show_all else max_items
        summary.display_top_performers(all_results, count=top_count)

        # Show insights if we have enough data
        insights = summary.generate_benchmark_insights(all_results)
        if insights:
            console.print("\n[bold cyan]💡 Performance Insights[/bold cyan]")
            for insight in insights:
                console.print(f"• {insight}")

        console.print("\n[bold green]All benchmarks complete![/bold green]")
        console.print(f"[dim]Results stored in: {config.storage_path}[/dim]")
    else:
        console.print("\n[yellow]No benchmark results to summarize[/yellow]")


@cli.command()
@click.option("--days", default=7, help="Number of days to look back for comparison")
@click.option("--suite", help="Filter by benchmark suite")
@click.option("--adapter", help="Filter by adapter")
@click.pass_context
def compare(ctx: click.Context, days: int, suite: Optional[str], adapter: Optional[str]) -> None:
    """Compare recent benchmark results."""
    ctx.obj["runner"]

    # TODO: Implement comparison visualization using DuckDB queries
    console.print("[yellow]Comparison feature coming soon![/yellow]")


@cli.command()
@click.option("--json-dir", type=click.Path(exists=True, path_type=Path), help="Directory with JSON results")
@click.pass_context
def import_json(ctx: click.Context, json_dir: Path) -> None:
    """Import legacy JSON benchmark results."""
    runner: BenchmarkRunner = ctx.obj["runner"]

    if not json_dir:
        json_dir = Path(".benchmark")

    json_files = list(json_dir.glob("*.json"))

    if not json_files:
        console.print(f"[red]No JSON files found in {json_dir}[/red]")
        return

    console.print(f"[cyan]Found {len(json_files)} JSON files to import[/cyan]")

    imported = 0
    failed_imports = []

    def _import_file(file_path: Path) -> tuple[bool, str]:
        try:
            runner.storage.import_json_results(file_path)
            return True, ""
        except Exception as e:
            return False, str(e)

    for json_file in json_files:
        success, error_msg = _import_file(json_file)
        if success:
            imported += 1
            console.print(f"  [green]✓[/green] Imported {json_file.name}")
        else:
            failed_imports.append((json_file.name, error_msg))

    # Report failures after the loop
    for filename, error in failed_imports:
        console.print(f"  [red]✗[/red] Failed to import {filename}: {error}")

    console.print(f"\n[green]Successfully imported {imported} files[/green]")


@cli.command()
@click.option("--days", default=30, help="Delete data older than this many days")
@click.option("--dry-run", is_flag=True, help="Show what would be deleted without deleting")
@click.pass_context
def cleanup(ctx: click.Context, days: int, dry_run: bool) -> None:
    """Clean up old benchmark data."""
    runner: BenchmarkRunner = ctx.obj["runner"]

    if dry_run:
        # TODO: Query to show what would be deleted
        console.print("[yellow]Dry run - no data deleted[/yellow]")
    else:
        deleted = runner.storage.cleanup_old_data(days)
        console.print(f"[green]Deleted {deleted} old benchmark runs (older than {days} days)[/green]")


if __name__ == "__main__":
    cli()
