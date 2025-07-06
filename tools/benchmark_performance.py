#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "rich>=13.0.0",
#     "rich-click>=1.8.0",
#     "sqlspec @ file:///${PROJECT_ROOT}",
#     "msgspec>=0.18.0",
#     "psutil>=5.9.0",
# ]
# ///
"""Performance benchmarking tool for SQLSpec.

This tool benchmarks various SQLSpec operations to establish baseline metrics
and compare performance across different configurations.

Usage:
    uv run tools/benchmark_performance.py --help
    uv run tools/benchmark_performance.py all
    uv run tools/benchmark_performance.py parameter-styles --adapter sqlite
    uv run tools/benchmark_performance.py sql-compilation --iterations 10000
"""

import datetime as dt
import gc
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Optional

import psutil
import rich_click as click
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn
from rich.table import Table
from rich.text import Text
from sqlglot import parse_one

from sqlspec.statement.parameters import ParameterStyle, TypedParameter
from sqlspec.statement.sql import SQL

# Configure rich-click
click.rich_click.USE_RICH_MARKUP = True
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
click.rich_click.STYLE_ERRORS_SUGGESTION = "magenta italic"
click.rich_click.ERRORS_SUGGESTION = "Try running with [bold]--help[/bold] for more information."

console = Console()

# Constants
MIN_COMPARISON_FILES = 2
PERFORMANCE_IMPROVEMENT_THRESHOLD = -5  # Negative for improvement
PERFORMANCE_REGRESSION_THRESHOLD = 5

# Benchmark results storage
RESULTS_DIR = Path("benchmarks")
RESULTS_DIR.mkdir(exist_ok=True)


class BenchmarkRunner:
    """Runs and tracks benchmark results."""

    def __init__(self, name: str, iterations: int = 1000) -> None:
        self.name = name
        self.iterations = iterations
        self.results: dict[str, list[float]] = defaultdict(list)
        self.memory_before = 0
        self.memory_after = 0

    def measure(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> tuple[float, Any]:
        """Measure execution time and memory usage of a function."""
        gc.collect()
        process = psutil.Process()

        # Memory before
        self.memory_before = process.memory_info().rss / 1024 / 1024  # MB

        # Time execution
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start

        # Memory after
        gc.collect()
        self.memory_after = process.memory_info().rss / 1024 / 1024  # MB

        return elapsed, result

    def benchmark(self, name: str, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """Run a benchmark multiple times and collect results."""
        console.print(f"  [dim]Running {name}...[/dim]")

        # Warmup
        for _ in range(min(10, self.iterations // 10)):
            func(*args, **kwargs)

        # Actual benchmark
        for _ in range(self.iterations):
            elapsed, _ = self.measure(func, *args, **kwargs)
            self.results[name].append(elapsed * 1000)  # Convert to ms

    def report(self) -> Table:
        """Generate a rich table with benchmark results."""
        table = Table(title=f"[bold]{self.name}[/bold]", show_header=True)
        table.add_column("Operation", style="cyan", no_wrap=True)
        table.add_column("Iterations", justify="right")
        table.add_column("Min (ms)", justify="right", style="green")
        table.add_column("Avg (ms)", justify="right", style="yellow")
        table.add_column("Max (ms)", justify="right", style="red")
        table.add_column("Std Dev", justify="right")
        table.add_column("Ops/sec", justify="right", style="bold")

        for name, times in self.results.items():
            if times:
                min_time = min(times)
                avg_time = sum(times) / len(times)
                max_time = max(times)
                std_dev = (sum((t - avg_time) ** 2 for t in times) / len(times)) ** 0.5
                ops_per_sec = 1000 / avg_time if avg_time > 0 else 0

                table.add_row(
                    name,
                    str(len(times)),
                    f"{min_time:.3f}",
                    f"{avg_time:.3f}",
                    f"{max_time:.3f}",
                    f"{std_dev:.3f}",
                    f"{ops_per_sec:.0f}",
                )

        return table

    def save_results(self, adapter: Optional[str] = None) -> Path:
        """Save results to JSON file."""
        timestamp = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
        adapter_suffix = f"_{adapter}" if adapter else ""
        filename = f"{self.name.lower().replace(' ', '_')}{adapter_suffix}_{timestamp}.json"
        filepath = RESULTS_DIR / filename

        data = {
            "name": self.name,
            "timestamp": timestamp,
            "iterations": self.iterations,
            "adapter": adapter,
            "results": {
                name: {
                    "times_ms": times,
                    "min_ms": min(times) if times else 0,
                    "avg_ms": sum(times) / len(times) if times else 0,
                    "max_ms": max(times) if times else 0,
                    "count": len(times),
                }
                for name, times in self.results.items()
            },
        }

        filepath.write_text(json.dumps(data, indent=2), encoding="utf-8")

        return filepath


def benchmark_parameter_styles(adapter: str, iterations: int) -> None:
    """Benchmark different parameter styles."""
    runner = BenchmarkRunner(f"Parameter Styles - {adapter}", iterations)

    # Test queries
    queries = {
        "simple": "SELECT * FROM users WHERE id = 1",
        "multiple": "SELECT * FROM users WHERE age > 25 AND status = 'active'",
        "in_clause": "SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5)",
        "complex": """
            SELECT u.*, p.name as profile_name
            FROM users u
            JOIN profiles p ON u.id = p.user_id
            WHERE u.created_at > '2023-01-01'
            AND u.status = 'active'
            AND p.verified = true
        """,
    }

    # Parameter styles to test (all ParameterStyle enum values to test regex performance)
    styles = [
        ParameterStyle.QMARK,  # SQLGlot compatible
        ParameterStyle.NUMERIC,  # SQLGlot compatible
        ParameterStyle.NAMED_COLON,  # SQLGlot compatible
        ParameterStyle.NAMED_AT,  # SQLGlot compatible
        ParameterStyle.NAMED_DOLLAR,  # SQLGlot compatible
        ParameterStyle.POSITIONAL_PYFORMAT,  # SQLGlot incompatible - needs transformation
        ParameterStyle.NAMED_PYFORMAT,  # SQLGlot incompatible - needs transformation
        ParameterStyle.POSITIONAL_COLON,  # SQLGlot incompatible - needs transformation
        # Note: NONE and STATIC are not included as they don't have placeholders to compile
    ]

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(f"Benchmarking {adapter}...", total=len(queries) * len(styles))

        for query_name, query in queries.items():
            for style in styles:
                stmt = SQL(query)

                def compile_with_style(
                    s: SQL = stmt, st: ParameterStyle = style, qn: str = query_name
                ) -> tuple[str, Any]:
                    try:
                        return s.compile(placeholder_style=st)
                    except Exception as e:
                        console.print(f"[red]Error with {qn} + {st}: {e}[/red]")
                        return "", []

                runner.benchmark(f"{query_name}_{style}", compile_with_style)
                progress.advance(task)

    # Display results
    console.print("\n")
    console.print(runner.report())

    # Save results
    filepath = runner.save_results(adapter)
    console.print(f"\n[green]Results saved to:[/green] {filepath}")


def benchmark_sql_compilation(iterations: int) -> None:
    """Benchmark SQL compilation and caching."""
    runner = BenchmarkRunner("SQL Compilation", iterations)

    queries = {
        "simple_select": "SELECT id, name FROM users",
        "where_clause": "SELECT * FROM users WHERE status = 'active'",
        "join_query": "SELECT u.*, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        "complex_query": """
            WITH active_users AS (
                SELECT id, name FROM users WHERE status = 'active'
            )
            SELECT au.*, COUNT(o.id) as order_count
            FROM active_users au
            LEFT JOIN orders o ON au.id = o.user_id
            GROUP BY au.id, au.name
            HAVING COUNT(o.id) > 5
        """,
    }

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Benchmarking SQL compilation...", total=len(queries) * 3)

        for query_name, query in queries.items():
            # Parse once for reuse
            parse_one(query)

            # Benchmark: Create SQL object
            def create_sql(q: str = query) -> SQL:
                return SQL(q)

            runner.benchmark(f"{query_name}_create", create_sql)
            progress.advance(task)

            # Benchmark: Compile (no cache)
            sql = SQL(query)
            sql.config.enable_caching = False

            def compile_no_cache(s: SQL = sql) -> tuple[str, Any]:
                return s.compile()

            runner.benchmark(f"{query_name}_compile_nocache", compile_no_cache)
            progress.advance(task)

            # Benchmark: Compile (with cache)
            sql_cached = SQL(query)
            sql_cached.config.enable_caching = True
            # Prime the cache
            sql_cached.compile()

            def compile_with_cache(s: SQL = sql_cached) -> tuple[str, Any]:
                return s.compile()

            runner.benchmark(f"{query_name}_compile_cached", compile_with_cache)
            progress.advance(task)

    console.print("\n")
    console.print(runner.report())

    filepath = runner.save_results()
    console.print(f"\n[green]Results saved to:[/green] {filepath}")


def benchmark_typed_parameters(iterations: int) -> None:
    """Benchmark TypedParameter wrapping performance."""
    runner = BenchmarkRunner("TypedParameter Performance", iterations)

    # Test data
    test_params = {
        "simple": {"id": 1, "name": "test"},
        "mixed": {"id": 1, "price": 19.99, "active": True, "created": "2023-01-01"},
        "large": {f"param_{i}": i for i in range(100)},
    }

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Benchmarking TypedParameter...", total=len(test_params) * 2)

        for param_name, params in test_params.items():
            # Benchmark: Direct parameter usage (baseline)
            def use_direct(p: dict[str, Any] = params) -> dict[str, Any]:
                return p

            runner.benchmark(f"{param_name}_direct", use_direct)
            progress.advance(task)

            # Benchmark: TypedParameter wrapping
            def wrap_typed(p: dict[str, Any] = params) -> dict[str, Any]:
                # Simulate what wrap_parameters_with_types should do
                wrapped = {}
                for key, value in p.items():
                    if isinstance(value, (int, float)):
                        wrapped[key] = TypedParameter(value, "numeric")
                    elif isinstance(value, bool):
                        wrapped[key] = TypedParameter(value, "boolean")
                    elif isinstance(value, str):
                        wrapped[key] = TypedParameter(value, "string")
                    else:
                        wrapped[key] = value
                return wrapped

            runner.benchmark(f"{param_name}_typed", wrap_typed)
            progress.advance(task)

    console.print("\n")
    console.print(runner.report())

    filepath = runner.save_results()
    console.print(f"\n[green]Results saved to:[/green] {filepath}")


async def benchmark_async_operations(adapter: str, iterations: int) -> None:
    """Benchmark async adapter operations."""
    BenchmarkRunner(f"Async Operations - {adapter}", iterations)

    # For now, just show a placeholder
    console.print("[yellow]Async benchmarks not yet implemented[/yellow]")

    # TODO: Implement async benchmarks for asyncpg, aiosqlite, etc.


@click.group(invoke_without_command=True)
@click.option("--version", is_flag=True, help="Show version")
@click.pass_context
def cli(ctx: click.Context, version: bool) -> None:
    """[bold]SQLSpec Performance Benchmarking Tool[/bold]

    Benchmark various SQLSpec operations to establish baseline metrics
    and track performance improvements.

    [dim]Examples:[/dim]

        [green]# Run all benchmarks[/green]
        uv run tools/benchmark_performance.py all

        [green]# Benchmark parameter styles for SQLite[/green]
        uv run tools/benchmark_performance.py parameter-styles --adapter sqlite

        [green]# Benchmark SQL compilation with custom iterations[/green]
        uv run tools/benchmark_performance.py sql-compilation --iterations 10000
    """
    if version:
        console.print("[bold]SQLSpec Benchmark Tool v1.0.0[/bold]")
        ctx.exit()

    if ctx.invoked_subcommand is None:
        console.print(
            Panel.fit(
                "[bold yellow]SQLSpec Performance Benchmarking[/bold yellow]\n\n"
                "Run benchmarks to measure and track SQLSpec performance.\n\n"
                "Use [bold]--help[/bold] to see available commands.",
                border_style="yellow",
            )
        )


@cli.command()
@click.option("--adapter", default="sqlite", help="Database adapter to test")
@click.option("--iterations", default=1000, help="Number of iterations")
def parameter_styles(adapter: str, iterations: int) -> None:
    """Benchmark parameter style compilation performance."""
    console.print(
        Panel.fit(
            f"[bold]Parameter Styles Benchmark[/bold]\n"
            f"Adapter: [cyan]{adapter}[/cyan]\n"
            f"Iterations: [cyan]{iterations:,}[/cyan]",
            border_style="blue",
        )
    )
    benchmark_parameter_styles(adapter, iterations)


@cli.command()
@click.option("--iterations", default=1000, help="Number of iterations")
def sql_compilation(iterations: int) -> None:
    """Benchmark SQL compilation and caching."""
    console.print(
        Panel.fit(
            f"[bold]SQL Compilation Benchmark[/bold]\nIterations: [cyan]{iterations:,}[/cyan]", border_style="blue"
        )
    )
    benchmark_sql_compilation(iterations)


@cli.command()
@click.option("--iterations", default=1000, help="Number of iterations")
def typed_parameters(iterations: int) -> None:
    """Benchmark TypedParameter wrapping overhead."""
    console.print(
        Panel.fit(
            f"[bold]TypedParameter Benchmark[/bold]\nIterations: [cyan]{iterations:,}[/cyan]", border_style="blue"
        )
    )
    benchmark_typed_parameters(iterations)


def _safe_benchmark_adapter(adapter_name: str, iterations: int) -> None:
    """Safely benchmark an adapter, handling exceptions."""
    try:
        benchmark_parameter_styles(adapter_name, iterations)
    except Exception as e:
        console.print(f"[red]Skipping {adapter_name}: {e}[/red]")


@cli.command()
@click.option("--adapter", default="all", help="Adapter to test or 'all'")
@click.option("--iterations", default=1000, help="Number of iterations")
def run_all(adapter: str, iterations: int) -> None:
    """Run all benchmarks."""
    console.print(Panel.fit("[bold]Running All Benchmarks[/bold]", border_style="green"))

    # Run each benchmark
    console.print("\n[bold cyan]1. SQL Compilation[/bold cyan]")
    benchmark_sql_compilation(iterations)

    console.print("\n[bold cyan]2. Parameter Styles[/bold cyan]")
    if adapter == "all":
        adapters_to_test = ["sqlite", "duckdb"]  # Add more as needed
        for adp in adapters_to_test:
            _safe_benchmark_adapter(adp, iterations)
    else:
        benchmark_parameter_styles(adapter, iterations)

    console.print("\n[bold cyan]3. TypedParameter Performance[/bold cyan]")
    benchmark_typed_parameters(iterations)

    console.print("\n[bold green]All benchmarks complete![/bold green]")


@cli.command()
def compare() -> None:
    """Compare benchmark results over time."""
    if not RESULTS_DIR.exists() or not list(RESULTS_DIR.glob("*.json")):
        console.print("[red]No benchmark results found. Run some benchmarks first![/red]")
        return

    # Group results by benchmark type
    results_by_type: dict[str, list[Path]] = defaultdict(list)
    for result_file in RESULTS_DIR.glob("*.json"):
        # Extract benchmark type from filename
        parts = result_file.stem.split("_")
        bench_type = "_".join(parts[:-2])  # Remove timestamp
        results_by_type[bench_type].append(result_file)

    # Show comparison for each type
    for bench_type, files in results_by_type.items():
        if len(files) < MIN_COMPARISON_FILES:
            continue

        console.print(f"\n[bold]Comparing: {bench_type}[/bold]")

        # Load two most recent results
        files = sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)

        recent = json.loads(files[0].read_text(encoding="utf-8"))
        previous = json.loads(files[1].read_text(encoding="utf-8"))

        # Create comparison table
        table = Table(title=f"{bench_type} Comparison", show_header=True)
        table.add_column("Operation", style="cyan")
        table.add_column("Previous (ms)", justify="right")
        table.add_column("Current (ms)", justify="right")
        table.add_column("Change", justify="right")
        table.add_column("Δ %", justify="right")

        for op_name in recent["results"]:
            if op_name in previous["results"]:
                prev_avg = previous["results"][op_name]["avg_ms"]
                curr_avg = recent["results"][op_name]["avg_ms"]
                change = curr_avg - prev_avg
                pct_change = (change / prev_avg * 100) if prev_avg > 0 else 0

                # Color based on improvement/regression
                if pct_change < PERFORMANCE_IMPROVEMENT_THRESHOLD:
                    change_style = "green"
                elif pct_change > PERFORMANCE_REGRESSION_THRESHOLD:
                    change_style = "red"
                else:
                    change_style = "yellow"

                table.add_row(
                    op_name,
                    f"{prev_avg:.3f}",
                    f"{curr_avg:.3f}",
                    f"{change:+.3f}",
                    Text(f"{pct_change:+.1f}%", style=change_style),
                )

        console.print(table)


if __name__ == "__main__":
    cli()
