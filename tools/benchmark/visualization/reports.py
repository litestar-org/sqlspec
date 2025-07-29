"""Reporting and visualization for benchmark results."""

from typing import Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from tools.benchmark.core.metrics import TimingResult

# Performance thresholds
SPEEDUP_EXCELLENT = 1.5
SPEEDUP_GOOD = 1.1
SPEEDUP_SIGNIFICANT = 2.0
SPEEDUP_MODERATE = 1.2
IMPROVEMENT_GOOD = 1.2
WIN_RATE_HIGH = 60
WIN_RATE_MODERATE = 40
CACHE_RATE_HIGH = 50
SIGNIFICANT_RATE_HIGH = 30
IMPROVEMENT_TARGET = 15
CRITICAL_REGRESSION = 25
HIGH_REGRESSION = 10
MEDIUM_REGRESSION = 5

# Performance categories (ops/sec)
OPS_LIGHTNING_FAST = 100000
OPS_VERY_FAST = 10000
OPS_FAST = 1000

# Split counts
SPLIT_COUNT_TWO = 2


class BenchmarkSummary:
    """Generate comprehensive summaries of benchmark results."""

    def __init__(self, console: Optional[Console] = None) -> None:
        self.console = console or Console()

    def display_suite_results(self, suite_name: str, results: dict[str, TimingResult]) -> None:
        """Display results for a single, specific benchmark suite."""
        display_map = {
            "caching_comparison": self.display_caching_comparison,
            "orm_comparison": self.display_orm_comparison,
            "sql_compilation": self.display_sql_compilation_comparison,
            "parameters": self.display_parameters_comparison,
        }

        if suite_name == "all":
            # Display all suite-specific analyses
            for display_func in display_map.values():
                display_func(results)
        elif suite_name in display_map:
            display_map[suite_name](results)
        else:
            # Generic display for any other suite
            self.display_generic_suite_table(f"{suite_name.replace('_', ' ').title()} Results", results)

    def display_comprehensive_summary(
        self,
        all_results: dict[str, TimingResult],
        system_info: Optional[dict] = None,
        regressions: Optional[list[tuple[str, str, float]]] = None,
        improvements: Optional[list[tuple[str, str, float]]] = None,
    ) -> None:
        """Display a comprehensive summary when all benchmarks are run."""
        if not all_results:
            self.console.print("[yellow]No benchmark results to summarize[/yellow]")
            return

        self.console.print(
            Panel.fit("[bold magenta]🔬 Comprehensive Benchmark Analysis[/bold magenta]", border_style="magenta")
        )

        # Display individual suite reports
        self.display_orm_comparison(all_results)
        self.display_caching_comparison(all_results)
        self.display_sqlglot_comparison(all_results)
        self.display_sql_compilation_comparison(all_results)
        self.display_parameters_comparison(all_results)
        self.display_mypyc_comparison(all_results)

        # Display overall summary panels
        self.display_overall_summary(all_results, system_info, regressions, improvements)
        self.display_top_performers(all_results)

    def display_generic_suite_table(self, title: str, results: dict[str, TimingResult]) -> None:
        """A generic table for displaying benchmark results."""
        table = Table(title=title, show_header=True)
        table.add_column("Operation", style="cyan")
        table.add_column("Min (ms)", justify="right")
        table.add_column("Avg (ms)", justify="right")
        table.add_column("Max (ms)", justify="right")
        table.add_column("Std Dev", justify="right")
        table.add_column("Ops/sec", justify="right", style="green")

        for operation, result in sorted(results.items()):
            table.add_row(
                operation,
                f"{result.min_ms:.3f}",
                f"{result.avg_ms:.3f}",
                f"{result.max_ms:.3f}",
                f"{result.std_ms:.3f}",
                f"{result.ops_per_sec:.1f}",
            )
        self.console.print(table)

    def display_caching_comparison(self, results: dict[str, TimingResult]) -> None:
        """Display comprehensive caching comparison table."""
        caching_results = {k: v for k, v in results.items() if "cache_" in k}
        if not caching_results:
            return

        sql_types = {key.split("_", 2)[-1] for key in caching_results}

        table = Table(title="🚀 Caching Impact Analysis", show_header=True)
        table.add_column("SQL Operation", style="cyan", width=15)
        table.add_column("No Cache (ms)", justify="right", style="red")
        table.add_column("With Cache (ms)", justify="right", style="green")
        table.add_column("Speedup", justify="right", style="bold magenta")
        table.add_column("Cache Hit Benefit", justify="right", style="yellow")
        table.add_column("No Cache Ops/sec", justify="right")
        table.add_column("Cached Ops/sec", justify="right")

        speedups = []

        for sql_type in sorted(sql_types):
            no_cache_key = f"no_cache_{sql_type}"
            with_cache_key = f"with_cache_{sql_type}"

            if no_cache_key in results and with_cache_key in results:
                no_cache = results[no_cache_key]
                with_cache = results[with_cache_key]

                speedup = no_cache.avg_ms / with_cache.avg_ms
                speedups.append(speedup)

                cache_benefit = ((no_cache.avg_ms - with_cache.avg_ms) / no_cache.avg_ms) * 100

                # Color coding for speedup
                speedup_color = (
                    "green" if speedup > SPEEDUP_EXCELLENT else "yellow" if speedup > SPEEDUP_GOOD else "red"
                )

                table.add_row(
                    sql_type.replace("_", " ").title(),
                    f"{no_cache.avg_ms:.3f}",
                    f"{with_cache.avg_ms:.3f}",
                    f"[{speedup_color}]{speedup:.2f}x[/{speedup_color}]",
                    f"{cache_benefit:.1f}%",
                    f"{no_cache.ops_per_sec:.0f}",
                    f"{with_cache.ops_per_sec:.0f}",
                )

        self.console.print(table)

        # Summary insights
        if speedups:
            avg_speedup = sum(speedups) / len(speedups)
            max_speedup = max(speedups)
            min_speedup = min(speedups)

            self.console.print("\n[bold cyan]📊 Caching Performance Summary:[/bold cyan]")
            self.console.print(f"• Average Speedup: [bold]{avg_speedup:.2f}x[/bold]")
            self.console.print(f"• Best Case: [bold green]{max_speedup:.2f}x faster[/bold green]")
            self.console.print(f"• Worst Case: [bold red]{min_speedup:.2f}x faster[/bold red]")

            if avg_speedup > SPEEDUP_SIGNIFICANT:
                self.console.print("• [bold green]✅ Caching provides significant performance benefits[/bold green]")
            elif avg_speedup > SPEEDUP_MODERATE:
                self.console.print("• [bold yellow]⚠ Caching provides moderate benefits[/bold yellow]")
            else:
                self.console.print("• [bold red]❌ Caching provides minimal benefits[/bold red]")

    def display_sqlglot_comparison(self, results: dict[str, TimingResult]) -> None:
        """Display comprehensive SQLGlot configuration comparison table."""
        sqlglot_results = {
            k: v
            for k, v in results.items()
            if any(p in k for p in ["postgres_", "mysql_", "sqlite_", "default_", "optimized_"])
        }
        if not sqlglot_results:
            return

        sql_types = {k.split("_", 1)[1] for k in sqlglot_results}
        config_types = {k.split("_", 1)[0] for k in sqlglot_results}

        # Create main comparison table
        table = Table(title="🔧 SQLGlot Configuration Performance Analysis", show_header=True)
        table.add_column("SQL Type", style="cyan", width=12)

        # Add columns for each configuration
        for config in config_types:
            table.add_column(f"{config.title()}\n(ms)", justify="right", width=10)

        table.add_column("Best Config", style="bold green", width=12)
        table.add_column("Worst Config", style="bold red", width=12)
        table.add_column("Ratio", style="bold yellow", justify="right", width=8)

        best_configs = {}
        worst_configs = {}

        for sql_type in sql_types:
            row_data = [sql_type.replace("_", " ").title()]
            config_results = {}

            # Collect results for this SQL type across all configs
            for config in config_types:
                key = f"{config}_{sql_type}"
                if key in results:
                    result = results[key]
                    config_results[config] = result.avg_ms
                    row_data.append(f"{result.avg_ms:.3f}")
                else:
                    row_data.append("N/A")

            if config_results:
                # Find best and worst
                best_config = min(config_results, key=lambda k: config_results[k])
                worst_config = max(config_results, key=lambda k: config_results[k])
                best_time = config_results[best_config]
                worst_time = config_results[worst_config]
                ratio = worst_time / best_time if best_time > 0 else 1.0

                best_configs[sql_type] = best_config
                worst_configs[sql_type] = worst_config

                row_data.extend([best_config, worst_config, f"{ratio:.2f}x"])
            else:
                row_data.extend(["N/A", "N/A", "N/A"])

            table.add_row(*row_data)

        self.console.print(table)

    def display_orm_comparison(self, results: dict[str, TimingResult]) -> None:
        """Display comprehensive ORM comparison across all databases."""
        # ORM comparison variants to look for
        orm_variants = {"sqlspec_cache", "sqlspec_no_cache", "sqlalchemy_core", "sqlalchemy_orm"}
        
        # Filter for ORM-related results by checking if key contains any ORM variant
        orm_results = {}
        for key, result in results.items():
            if any(variant in key for variant in orm_variants):
                orm_results[key] = result
                
        if not orm_results:
            return

        # Group results by database and operation
        grouped_results: dict[str, dict[str, dict[str, TimingResult]]] = {}
        for key, result in orm_results.items():
            # Find the matching variant (search from longest to shortest to avoid partial matches)
            matched_variant = None
            for variant in sorted(orm_variants, key=len, reverse=True):
                if key.endswith(variant):
                    matched_variant = variant
                    break
            
            if not matched_variant:
                continue
                
            # Extract database and operation by removing the variant suffix
            prefix = key[:-len(matched_variant)].rstrip("_")
            parts = prefix.split("_", 1)  # Split into database and operation (operation may have underscores)
            
            if len(parts) < 2:
                continue
                
            db_name, op_name = parts[0], parts[1]
            
            if db_name not in grouped_results:
                grouped_results[db_name] = {}
            if op_name not in grouped_results[db_name]:
                grouped_results[db_name][op_name] = {}
            grouped_results[db_name][op_name][matched_variant] = result

        for db_name, ops in grouped_results.items():
            table = Table(title=f"ORM Comparison - {db_name.title()}", show_header=True)
            table.add_column("Operation", style="cyan")
            table.add_column("SQLSpec (Cached)", justify="right")
            table.add_column("SQLSpec (No Cache)", justify="right")
            table.add_column("SQLAlchemy Core", justify="right")
            table.add_column("SQLAlchemy ORM", justify="right")
            table.add_column("Winner", justify="right")

            for op_name, variants in ops.items():
                sqlspec_cached = variants.get("sqlspec_cache")
                sqlspec_no_cache = variants.get("sqlspec_no_cache")
                sqlalchemy_core = variants.get("sqlalchemy_core")
                sqlalchemy_orm = variants.get("sqlalchemy_orm")

                # Find the winner
                winner = ""
                winner_time = float("inf")
                if sqlspec_cached and sqlspec_cached.avg_ms < winner_time:
                    winner = "SQLSpec (Cached)"
                    winner_time = sqlspec_cached.avg_ms
                if sqlspec_no_cache and sqlspec_no_cache.avg_ms < winner_time:
                    winner = "SQLSpec (No Cache)"
                    winner_time = sqlspec_no_cache.avg_ms
                if sqlalchemy_core and sqlalchemy_core.avg_ms < winner_time:
                    winner = "SQLAlchemy Core"
                    winner_time = sqlalchemy_core.avg_ms
                if sqlalchemy_orm and sqlalchemy_orm.avg_ms < winner_time:
                    winner = "SQLAlchemy ORM"

                table.add_row(
                    op_name.replace("_", " ").title(),
                    f"{sqlspec_cached.avg_ms:.3f}ms" if sqlspec_cached else "N/A",
                    f"{sqlspec_no_cache.avg_ms:.3f}ms" if sqlspec_no_cache else "N/A",
                    f"{sqlalchemy_core.avg_ms:.3f}ms" if sqlalchemy_core else "N/A",
                    f"{sqlalchemy_orm.avg_ms:.3f}ms" if sqlalchemy_orm else "N/A",
                    winner,
                )

            self.console.print(table)

    def _display_performance_by_driver(
        self, db_results: dict[str, dict[str, TimingResult]], key_operations: list[tuple]
    ) -> None:
        """Display performance metrics organized by driver."""
        self.console.print("\n[bold cyan]📊 Performance Metrics by Driver and ORM Type[/bold cyan]")
        self.console.print("[dim]Higher statements/second = better performance[/dim]\n")

        # Find fastest SQLSpec result across all operations for baseline
        fastest_sqlspec_stmts_per_sec = 0
        for db_ops in db_results.values():
            for result_key, result in db_ops.items():
                if "sqlspec" in result_key:
                    stmts_per_sec = 1000.0 / result.avg_ms if result.avg_ms > 0 else 0
                    fastest_sqlspec_stmts_per_sec = max(fastest_sqlspec_stmts_per_sec, stmts_per_sec)

        # Group by driver type
        driver_groups = {
            "SQLite (Sync)": ["sqlite"],
            "SQLite (Async)": ["aiosqlite"],
            "PostgreSQL (Sync)": ["psycopg"],
            "PostgreSQL (Async)": ["psycopg-async", "asyncpg"],
            "Oracle (Sync)": ["oracledb"],
            "Oracle (Async)": ["oracledb-async"],
        }

        for driver_type, db_names in driver_groups.items():
            # Find matching databases
            matching_dbs = {db: ops for db, ops in db_results.items() if db.lower() in [n.lower() for n in db_names]}
            if not matching_dbs:
                continue

            table = Table(title=f"{driver_type} Performance", show_header=True)
            table.add_column("Operation", style="cyan", width=18)
            table.add_column("SQLSpec\n(cached)", justify="right", style="green", width=11)
            table.add_column("SQLSpec\n(no cache)", justify="right", style="green dim", width=11)
            table.add_column("Core\n(stmts/sec)", justify="right", style="yellow", width=11)
            table.add_column("ORM\n(stmts/sec)", justify="right", style="blue", width=11)
            table.add_column("Winner", justify="center", style="bold", width=10)

            for op_key, op_name in key_operations:
                # Aggregate results across matching databases
                sqlspec_cache_stmts = []
                sqlspec_no_cache_stmts = []
                core_stmts = []
                orm_stmts = []

                for db_ops in matching_dbs.values():
                    for result_key, result in db_ops.items():
                        if op_key in result_key or result_key.startswith(op_key):
                            stmts_per_sec = 1000.0 / result.avg_ms if result.avg_ms > 0 else 0

                            if "core" in result_key:
                                core_stmts.append(stmts_per_sec)
                            elif "orm" in result_key:
                                orm_stmts.append(stmts_per_sec)
                            elif "sqlspec_cache" in result_key:
                                sqlspec_cache_stmts.append(stmts_per_sec)
                            elif "sqlspec_no_cache" in result_key:
                                sqlspec_no_cache_stmts.append(stmts_per_sec)

                # Calculate averages
                avg_sqlspec_cache = sum(sqlspec_cache_stmts) / len(sqlspec_cache_stmts) if sqlspec_cache_stmts else 0
                avg_sqlspec_no_cache = (
                    sum(sqlspec_no_cache_stmts) / len(sqlspec_no_cache_stmts) if sqlspec_no_cache_stmts else 0
                )
                avg_core = sum(core_stmts) / len(core_stmts) if core_stmts else 0
                avg_orm = sum(orm_stmts) / len(orm_stmts) if orm_stmts else 0

                # Use the faster SQLSpec version as the best SQLSpec performance
                best_sqlspec = max(avg_sqlspec_cache, avg_sqlspec_no_cache)

                # Determine winner
                winner = "N/A"
                if best_sqlspec > 0 or avg_core > 0 or avg_orm > 0:
                    max_val = max(best_sqlspec, avg_core, avg_orm)
                    if max_val == best_sqlspec:
                        winner = "SQLSpec"
                    elif max_val == avg_core:
                        winner = "Core"
                    else:
                        winner = "ORM"

                table.add_row(
                    op_name,
                    f"{avg_sqlspec_cache:,.0f}" if avg_sqlspec_cache > 0 else "N/A",
                    f"{avg_sqlspec_no_cache:,.0f}" if avg_sqlspec_no_cache > 0 else "N/A",
                    f"{avg_core:,.0f}" if avg_core > 0 else "N/A",
                    f"{avg_orm:,.0f}" if avg_orm > 0 else "N/A",
                    winner,
                )

            self.console.print(table)
            self.console.print()

    def _display_statements_per_second(
        self, db_results: dict[str, dict[str, TimingResult]], key_operations: list[tuple]
    ) -> None:
        """Display detailed statements per second for each driver and operation."""
        self.console.print("\n[bold cyan]📈 Statements Per Second by Database Driver[/bold cyan]\n")

        # Create comprehensive table
        table = Table(title="Detailed Performance Metrics", show_header=True)
        table.add_column("Database", style="cyan", width=15)
        table.add_column("Sync/Async", justify="center", width=10)
        table.add_column("Operation", style="cyan", width=20)
        table.add_column("ORM Type", justify="center", width=10)
        table.add_column("Avg Time (ms)", justify="right", width=12)
        table.add_column("Stmts/sec", justify="right", style="bold green", width=12)
        table.add_column("Std Dev (ms)", justify="right", width=12)
        table.add_column("Min/Max (ms)", justify="right", width=15)

        # Process each database
        for db_name in sorted(db_results.keys()):
            db_ops = db_results[db_name]
            is_async = "async" in db_name.lower() or "aio" in db_name.lower()

            # Group operations by type
            for op_key, op_name in key_operations:
                # Find all results for this operation
                for result_key, result in sorted(db_ops.items()):
                    if op_key in result_key or result_key.startswith(op_key):
                        # Determine ORM type
                        if "core" in result_key:
                            orm_type = "Core"
                        elif "orm" in result_key:
                            orm_type = "ORM"
                        elif "sqlspec" in result_key:
                            orm_type = "SQLSpec"
                        else:
                            orm_type = "Unknown"

                        stmts_per_sec = 1000.0 / result.avg_ms if result.avg_ms > 0 else 0

                        table.add_row(
                            db_name,
                            "Async" if is_async else "Sync",
                            op_name,
                            orm_type,
                            f"{result.avg_ms:.3f}",
                            f"{stmts_per_sec:,.0f}",
                            f"{result.std_ms:.3f}",
                            f"{result.min_ms:.3f}/{result.max_ms:.3f}",
                        )

        self.console.print(table)

    def _display_performance_comparison(
        self, db_results: dict[str, dict[str, TimingResult]], key_operations: list[tuple]
    ) -> None:
        """Display performance comparison relative to fastest SQLSpec method."""
        self.console.print("\n[bold cyan]🏆 Performance Comparison (Relative to Fastest SQLSpec)[/bold cyan]\n")

        # Find the fastest SQLSpec result for each operation
        fastest_by_op = {}
        for op_key, _ in key_operations:
            fastest_ms = float("inf")
            for db_ops in db_results.values():
                for result_key, result in db_ops.items():
                    if (op_key in result_key or result_key.startswith(op_key)) and "sqlspec" in result_key:
                        fastest_ms = min(fastest_ms, result.avg_ms)
            if fastest_ms < float("inf"):
                fastest_by_op[op_key] = fastest_ms

        # Create comparison table
        table = Table(title="Performance Relative to Fastest SQLSpec Implementation", show_header=True)
        table.add_column("Database", style="cyan", width=15)
        table.add_column("Operation", style="cyan", width=20)
        table.add_column("SQLSpec\n(baseline)", justify="right", style="green", width=12)
        table.add_column("Core\n(% slower)", justify="right", style="yellow", width=12)
        table.add_column("ORM\n(% slower)", justify="right", style="blue", width=12)
        table.add_column("Best→Worst\nRatio", justify="right", style="magenta", width=12)

        for db_name in sorted(db_results.keys()):
            db_ops = db_results[db_name]

            for op_key, op_name in key_operations:
                if op_key not in fastest_by_op:
                    continue

                fastest_ms = fastest_by_op[op_key]

                # Find results for this operation
                sqlspec_ms = None
                core_ms = None
                orm_ms = None

                for result_key, result in db_ops.items():
                    if op_key in result_key or result_key.startswith(op_key):
                        if "core" in result_key:
                            core_ms = result.avg_ms
                        elif "orm" in result_key:
                            orm_ms = result.avg_ms
                        elif "sqlspec" in result_key:
                            sqlspec_ms = result.avg_ms

                if sqlspec_ms or core_ms or orm_ms:
                    # Calculate percentages
                    sqlspec_pct = ((sqlspec_ms / fastest_ms - 1) * 100) if sqlspec_ms else None
                    core_pct = ((core_ms / fastest_ms - 1) * 100) if core_ms else None
                    orm_pct = ((orm_ms / fastest_ms - 1) * 100) if orm_ms else None

                    # Calculate best to worst ratio
                    times = [t for t in [sqlspec_ms, core_ms, orm_ms] if t is not None]
                    ratio = max(times) / min(times) if times else None

                    table.add_row(
                        db_name,
                        op_name,
                        f"{sqlspec_pct:+.1f}%" if sqlspec_pct is not None else "N/A",
                        f"{core_pct:+.1f}%" if core_pct is not None else "N/A",
                        f"{orm_pct:+.1f}%" if orm_pct is not None else "N/A",
                        f"{ratio:.1f}x" if ratio is not None else "N/A",
                    )

        self.console.print(table)

    def _display_executive_summary(
        self, db_results: dict[str, dict[str, TimingResult]], key_operations: list[tuple]
    ) -> None:
        """Display executive summary of SQLSpec vs SQLAlchemy."""

        self.console.print("\n[bold cyan]📊 Executive Summary: SQLSpec Performance Analysis[/bold cyan]")

        # Calculate overall performance gains
        total_comparisons = 0
        sqlspec_wins = 0
        significant_gains = 0
        cache_benefits = 0

        for db_ops in db_results.values():
            for op_key, _ in key_operations:
                core_result = None
                sqlspec_cached = None
                sqlspec_no_cache = None

                for result_key, result in db_ops.items():
                    if op_key in result_key:
                        if "core" in result_key:
                            core_result = result
                        elif "sqlspec" in result_key and "cache" in result_key and "no_cache" not in result_key:
                            sqlspec_cached = result
                        elif "sqlspec" in result_key and "no_cache" in result_key:
                            sqlspec_no_cache = result

                if core_result and (sqlspec_cached or sqlspec_no_cache):
                    total_comparisons += 1

                    # Check if best SQLSpec beats SQLAlchemy Core
                    best_sqlspec = None
                    if sqlspec_cached and sqlspec_no_cache:
                        best_sqlspec = (
                            sqlspec_cached if sqlspec_cached.avg_ms < sqlspec_no_cache.avg_ms else sqlspec_no_cache
                        )
                    elif sqlspec_cached:
                        best_sqlspec = sqlspec_cached
                    elif sqlspec_no_cache:
                        best_sqlspec = sqlspec_no_cache

                    if best_sqlspec and best_sqlspec.avg_ms < core_result.avg_ms:
                        sqlspec_wins += 1
                        gain = core_result.avg_ms / best_sqlspec.avg_ms
                        if gain > IMPROVEMENT_GOOD:  # 20% improvement
                            significant_gains += 1

                    # Check cache benefit
                    if sqlspec_cached and sqlspec_no_cache and sqlspec_cached.avg_ms < sqlspec_no_cache.avg_ms:
                        cache_benefits += 1

        # Display insights
        if total_comparisons > 0:
            win_rate = (sqlspec_wins / total_comparisons) * 100
            significant_rate = (significant_gains / total_comparisons) * 100
            cache_rate = (cache_benefits / total_comparisons) * 100

            self.console.print(
                f"\n[green]✅ SQLSpec Performance Wins: {sqlspec_wins}/{total_comparisons} operations ({win_rate:.0f}%)[/green]"
            )
            self.console.print(
                f"[green]🚀 Significant Performance Gains (>20%): {significant_gains}/{total_comparisons} operations ({significant_rate:.0f}%)[/green]"
            )
            self.console.print(
                f"[blue]📈 Operations Benefiting from Caching: {cache_benefits}/{total_comparisons} operations ({cache_rate:.0f}%)[/blue]"
            )

            # Recommendations
            self.console.print("\n[bold yellow]💡 Migration Recommendations:[/bold yellow]")
            if win_rate > WIN_RATE_HIGH:
                self.console.print(
                    "  • [green]Strong case for SQLSpec adoption[/green] - majority of operations show performance gains"
                )
            elif win_rate > WIN_RATE_MODERATE:
                self.console.print(
                    "  • [yellow]Moderate case for SQLSpec adoption[/yellow] - evaluate specific use cases"
                )
            else:
                self.console.print(
                    "  • [red]Limited case for SQLSpec adoption[/red] - SQLAlchemy performs competitively"
                )

            if cache_rate > CACHE_RATE_HIGH:
                self.console.print(
                    "  • [blue]Enable caching for best performance[/blue] - significant impact across operations"
                )

            if significant_rate > SIGNIFICANT_RATE_HIGH:
                self.console.print(
                    "  • [green]Focus on high-impact operations[/green] - some workloads see substantial gains"
                )

    def display_mypyc_comparison(self, results: dict[str, TimingResult]) -> None:
        """Display mypyc compilation performance comparison."""
        mypyc_results = {k: v for k, v in results.items() if "_baseline" in k or "_compiled" in k}
        if not mypyc_results:
            return

        self.console.print("\n[bold cyan]🚀 MyPyC Compilation Performance Analysis[/bold cyan]\n")

        # Group by module
        modules = {}
        for key, result in mypyc_results.items():
            parts = key.split("_")
            if len(parts) >= SPLIT_COUNT_TWO:
                module_name = parts[0]
                variant = "baseline" if "_baseline" in key else "compiled"

                if module_name not in modules:
                    modules[module_name] = {}
                modules[module_name][variant] = result

        # Create comparison table
        table = Table(title="MyPyC Performance Improvements", show_header=True)
        table.add_column("Module", style="cyan", width=25)
        table.add_column("Baseline\n(ms)", justify="right", width=12)
        table.add_column("Compiled\n(ms)", justify="right", width=12)
        table.add_column("Speedup", justify="right", style="bold green", width=10)
        table.add_column("Improvement", justify="right", style="bold yellow", width=12)
        table.add_column("Baseline\n(ops/sec)", justify="right", width=12)
        table.add_column("Compiled\n(ops/sec)", justify="right", width=12)

        improvements = []
        for module_name, variants in sorted(modules.items()):
            if "baseline" in variants and "compiled" in variants:
                baseline = variants["baseline"]
                compiled = variants["compiled"]

                speedup = baseline.avg_ms / compiled.avg_ms if compiled.avg_ms > 0 else 1.0
                improvement = ((baseline.avg_ms - compiled.avg_ms) / baseline.avg_ms) * 100
                improvements.append((module_name, improvement))

                table.add_row(
                    module_name,
                    f"{baseline.avg_ms:.3f}",
                    f"{compiled.avg_ms:.3f}",
                    f"{speedup:.2f}x",
                    f"{improvement:.1f}%",
                    f"{baseline.ops_per_sec:,.0f}",
                    f"{compiled.ops_per_sec:,.0f}",
                )

        self.console.print(table)

        # Summary statistics
        if improvements:
            avg_improvement = sum(imp for _, imp in improvements) / len(improvements)
            best_module, best_improvement = max(improvements, key=lambda x: x[1])

            self.console.print("\n[bold]MyPyC Compilation Summary:[/bold]")
            self.console.print(f"• Average Performance Improvement: [bold green]{avg_improvement:.1f}%[/bold green]")
            self.console.print(
                f"• Best Improvement: [bold cyan]{best_module}[/bold cyan] with [bold green]{best_improvement:.1f}%[/bold green]"
            )

            if avg_improvement >= IMPROVEMENT_TARGET:
                self.console.print("• [bold green]✅ Target of 15%+ improvement achieved![/bold green]")
            else:
                self.console.print(
                    f"• [bold yellow]⚠ Current improvement {avg_improvement:.1f}% is below 15% target[/bold yellow]"
                )

    def display_parameters_comparison(self, results: dict[str, TimingResult]) -> None:
        """Display parameters benchmark results."""
        param_results = {k: v for k, v in results.items() if "_param" in k}
        if not param_results:
            return
        self.display_generic_suite_table("Parameters Benchmark Results", param_results)

    def display_sql_compilation_comparison(self, results: dict[str, TimingResult]) -> None:
        """Display SQL compilation benchmark results."""
        comp_results = {k: v for k, v in results.items() if "parse_" in k or "compile_" in k}
        if not comp_results:
            return
        self.display_generic_suite_table("SQL Compilation Benchmark Results", comp_results)

    def display_overall_summary(
        self,
        all_results: dict[str, TimingResult],
        system_info: Optional[dict] = None,
        regressions: Optional[list[tuple[str, str, float]]] = None,
        improvements: Optional[list[tuple[str, str, float]]] = None,
    ) -> None:
        """Display comprehensive summary of all benchmark results."""
        if not all_results:
            self.console.print("[yellow]No benchmark results to summarize[/yellow]")
            return

        # Overall statistics
        total_operations = len(all_results)
        total_ops_per_sec = sum(result.ops_per_sec for result in all_results.values())
        avg_ops_per_sec = total_ops_per_sec / total_operations if total_operations > 0 else 0

        # Find best and worst performers
        fastest = max(all_results.items(), key=lambda x: x[1].ops_per_sec)
        slowest = min(all_results.items(), key=lambda x: x[1].ops_per_sec)

        # Performance categories
        very_fast = [(k, v) for k, v in all_results.items() if v.ops_per_sec > OPS_LIGHTNING_FAST]
        fast = [(k, v) for k, v in all_results.items() if OPS_VERY_FAST <= v.ops_per_sec <= OPS_LIGHTNING_FAST]
        moderate = [(k, v) for k, v in all_results.items() if OPS_FAST <= v.ops_per_sec < OPS_VERY_FAST]
        slow = [(k, v) for k, v in all_results.items() if v.ops_per_sec < OPS_FAST]

        # Create summary panel
        summary_text = f"""
[bold cyan]Performance Overview[/bold cyan]
• Total Operations Tested: [bold]{total_operations}[/bold]
• Average Performance: [bold]{avg_ops_per_sec:.1f}[/bold] ops/sec
• Fastest Operation: [bold green]{fastest[0]}[/bold green] ([bold]{fastest[1].ops_per_sec:.1f}[/bold] ops/sec)
• Slowest Operation: [bold red]{slowest[0]}[/bold red] ([bold]{slowest[1].ops_per_sec:.1f}[/bold] ops/sec)

[bold cyan]Performance Distribution[/bold cyan]
• Very Fast (>100K ops/sec): [bold]{len(very_fast)}[/bold] operations
• Fast (10K-100K ops/sec): [bold]{len(fast)}[/bold] operations
• Moderate (1K-10K ops/sec): [bold]{len(moderate)}[/bold] operations
• Slow (<1K ops/sec): [bold]{len(slow)}[/bold] operations
        """.strip()

        # Add regression/improvement info if available
        if regressions:
            summary_text += "\n\n[bold red]⚠ Regressions Detected[/bold red]\n"
            for op, info, _ in regressions[:3]:  # Show top 3
                summary_text += f"• {op}: {info}\n"

        if improvements:
            summary_text += "\n[bold green]✓ Performance Improvements[/bold green]\n"
            for op, info, _ in improvements[:3]:  # Show top 3
                summary_text += f"• {op}: {info}\n"

        # Add system context if available
        if system_info:
            summary_text += f"""

[bold cyan]System Context[/bold cyan]
• Platform: [bold]{system_info.get("platform", "Unknown")}[/bold]
• Python: [bold]{system_info.get("python_version", "Unknown")}[/bold]
• CPU Cores: [bold]{system_info.get("cpu_count", "Unknown")}[/bold]
• Memory: [bold]{system_info.get("memory_gb", "Unknown"):.1f}[/bold] GB
            """.strip()

        self.console.print(Panel.fit(summary_text, title="[bold]Benchmark Summary[/bold]", border_style="green"))

    def display_top_performers(self, all_results: dict[str, TimingResult], count: int = 5) -> None:
        """Display table of top performing operations."""
        if not all_results:
            return

        # Sort by operations per second
        sorted_results = sorted(all_results.items(), key=lambda x: x[1].ops_per_sec, reverse=True)

        table = Table(title=f"Top {count} Fastest Operations", show_header=True)
        table.add_column("Rank", style="bold cyan", width=4)
        table.add_column("Operation", style="cyan")
        table.add_column("Avg Time (ms)", justify="right")
        table.add_column("Ops/sec", justify="right", style="bold green")
        table.add_column("Performance", justify="center")

        for i, (operation, result) in enumerate(sorted_results[:count], 1):
            # Performance indicator
            if result.ops_per_sec > OPS_LIGHTNING_FAST:
                perf_indicator = "🚀"
            elif result.ops_per_sec > OPS_VERY_FAST:
                perf_indicator = "⚡"
            elif result.ops_per_sec > OPS_FAST:
                perf_indicator = "✅"
            else:
                perf_indicator = "🐌"

            table.add_row(str(i), operation, f"{result.avg_ms:.3f}", f"{result.ops_per_sec:.1f}", perf_indicator)

        self.console.print(table)

    def display_performance_categories(self, all_results: dict[str, TimingResult]) -> None:
        """Display operations grouped by performance categories."""
        if not all_results:
            return

        # Categorize operations
        categories = {
            "🚀 Lightning Fast (>100K ops/sec)": [
                (k, v) for k, v in all_results.items() if v.ops_per_sec > OPS_LIGHTNING_FAST
            ],
            "⚡ Very Fast (10K-100K ops/sec)": [
                (k, v) for k, v in all_results.items() if OPS_VERY_FAST <= v.ops_per_sec <= OPS_LIGHTNING_FAST
            ],
            "✅ Fast (1K-10K ops/sec)": [
                (k, v) for k, v in all_results.items() if OPS_FAST <= v.ops_per_sec < OPS_VERY_FAST
            ],
            "🐌 Slow (<1K ops/sec)": [(k, v) for k, v in all_results.items() if v.ops_per_sec < OPS_FAST],
        }

        for category_name, operations in categories.items():
            if not operations:
                continue

            table = Table(title=category_name, show_header=True)
            table.add_column("Operation", style="cyan")
            table.add_column("Avg Time (ms)", justify="right")
            table.add_column("Ops/sec", justify="right", style="green")
            table.add_column("Std Dev (ms)", justify="right")

            # Sort by ops/sec within category
            operations.sort(key=lambda x: x[1].ops_per_sec, reverse=True)

            for operation, result in operations:
                table.add_row(operation, f"{result.avg_ms:.3f}", f"{result.ops_per_sec:.1f}", f"{result.std_ms:.3f}")

            self.console.print(table)
            self.console.print()  # Add spacing

    def display_regression_summary(self, regressions: list[tuple[str, str, float]]) -> None:
        """Display detailed regression summary."""
        if not regressions:
            self.console.print("[green]✓ No performance regressions detected![/green]")
            return

        table = Table(title="⚠ Performance Regressions", show_header=True)
        table.add_column("Operation", style="cyan")
        table.add_column("Regression", justify="right", style="red")
        table.add_column("Impact", justify="center")

        for operation, info, pct_change in regressions:
            # Categorize impact
            if pct_change > CRITICAL_REGRESSION:
                impact = "🔴 Critical"
            elif pct_change > HIGH_REGRESSION:
                impact = "🟠 High"
            elif pct_change > MEDIUM_REGRESSION:
                impact = "🟡 Medium"
            else:
                impact = "🟢 Low"

            table.add_row(operation, info, impact)

        self.console.print(table)

    def generate_benchmark_insights(self, all_results: dict[str, TimingResult]) -> list[str]:
        """Generate insights and recommendations based on results."""
        insights: list[str] = []

        if not all_results:
            return insights

        # Analyze parameter style performance
        param_results = {k: v for k, v in all_results.items() if "parameter_styles" in k.lower()}
        if param_results:
            fastest_param = max(param_results.items(), key=lambda x: x[1].ops_per_sec)
            slowest_param = min(param_results.items(), key=lambda x: x[1].ops_per_sec)

            insights.append(
                f"Parameter Style Performance: {fastest_param[0]} is "
                f"{fastest_param[1].ops_per_sec / slowest_param[1].ops_per_sec:.1f}x faster than {slowest_param[0]}"
            )

        # Analyze SQL compilation performance
        sql_results = {k: v for k, v in all_results.items() if "sql_compilation" in k.lower()}
        if sql_results:
            parse_results = {k: v for k, v in sql_results.items() if "parse_" in k}
            compile_results = {k: v for k, v in sql_results.items() if "compile_" in k}

            if parse_results and compile_results:
                avg_parse = sum(r.avg_ms for r in parse_results.values()) / len(parse_results)
                avg_compile = sum(r.avg_ms for r in compile_results.values()) / len(compile_results)

                if avg_compile > avg_parse:
                    overhead = ((avg_compile - avg_parse) / avg_parse) * 100
                    insights.append(
                        f"SQLSpec Compilation adds {overhead:.1f}% overhead compared to raw SQLGlot parsing"
                    )

        # Check for high variability
        high_variance = [(k, v) for k, v in all_results.items() if v.std_ms > v.avg_ms * 0.5]
        if high_variance:
            insights.append(
                f"High performance variability detected in {len(high_variance)} operations - "
                "consider increasing warmup iterations"
            )

        return insights
