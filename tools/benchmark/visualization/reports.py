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

# Ranking positions
FIRST_PLACE = 1
SECOND_PLACE = 2
THIRD_PLACE = 3
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

    def __init__(self, console: Optional[Console] = None, display_options: Optional[dict] = None) -> None:
        self.console = console or Console()

        # Set up display options with defaults (show all rows and no truncation by default)
        self.display_options = display_options or {}
        self.show_all = self.display_options.get("show_all", True)  # Default to showing all
        self.max_items = self.display_options.get("max_items", 20)
        self.display_mode = self.display_options.get("display_mode", "compact")
        self.no_truncate = self.display_options.get("no_truncate", True)

        # Calculate dynamic table width based on console width or override
        override_width = self.display_options.get("table_width")
        if override_width:
            self.table_width = override_width
        elif hasattr(self.console, "options") and hasattr(self.console.options, "max_width"):
            self.table_width = self.console.options.max_width or 200
        else:
            self.table_width = 200  # Default enhanced width

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

        # Dynamic column widths based on display options
        op_width = None if self.no_truncate else (20 if self.display_mode == "detailed" else 15)
        table.add_column("SQL Operation", style="cyan", width=op_width)
        table.add_column("No Cache (ms)", justify="right", style="red")
        table.add_column("With Cache (ms)", justify="right", style="green")
        table.add_column("Speedup", justify="right", style="bold magenta")

        if self.display_mode != "compact":
            table.add_column("Cache Hit Benefit", justify="right", style="yellow")

        if self.display_mode == "detailed":
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

                # Build row data based on display mode
                row_data = [
                    sql_type.replace("_", " ").title(),
                    f"{no_cache.avg_ms:.3f}",
                    f"{with_cache.avg_ms:.3f}",
                    f"[{speedup_color}]{speedup:.2f}x[/{speedup_color}]",
                ]

                if self.display_mode != "compact":
                    row_data.append(f"{cache_benefit:.1f}%")

                if self.display_mode == "detailed":
                    row_data.extend([
                        f"{no_cache.ops_per_sec:.0f}",
                        f"{with_cache.ops_per_sec:.0f}",
                    ])

                table.add_row(*row_data)

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
        sql_type_width = None if self.no_truncate else 12
        table.add_column("SQL Type", style="cyan", width=sql_type_width)

        # Add columns for each configuration
        config_width = None if self.no_truncate else 10
        for config in config_types:
            table.add_column(f"{config.title()}\n(ms)", justify="right", width=config_width)

        best_width = None if self.no_truncate else 12
        worst_width = None if self.no_truncate else 12
        ratio_width = None if self.no_truncate else 8
        table.add_column("Best Config", style="bold green", width=best_width)
        table.add_column("Worst Config", style="bold red", width=worst_width)
        table.add_column("Ratio", style="bold yellow", justify="right", width=ratio_width)

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
        orm_results = {
            key: result for key, result in results.items() if any(variant in key for variant in orm_variants)
        }

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
            prefix = key[: -len(matched_variant)].rstrip("_")
            parts = prefix.split("_", 1)  # Split into database and operation (operation may have underscores)

            min_required_parts = 2
            if len(parts) < min_required_parts:
                continue

            db_name, op_name = parts[0], parts[1]

            if db_name not in grouped_results:
                grouped_results[db_name] = {}
            if op_name not in grouped_results[db_name]:
                grouped_results[db_name][op_name] = {}
            grouped_results[db_name][op_name][matched_variant] = result

        for db_name, ops in grouped_results.items():
            table = Table(title=f"ORM Comparison - {db_name.title()}", show_header=True)

            # Dynamic column widths based on display options
            op_width = None if self.no_truncate else (25 if self.display_mode == "detailed" else 20)
            table.add_column("Operation", style="cyan", width=op_width)

            if self.display_mode == "compact":
                # Compact mode - just show best times
                table.add_column("SQLSpec (ms)", justify="right", style="green")
                table.add_column("Core (ms)", justify="right", style="yellow")
                table.add_column("ORM (ms)", justify="right", style="blue")
                table.add_column("Winner", justify="center")
            elif self.display_mode == "matrix":
                # Matrix mode - focus on comparison ratios
                table.add_column("SQLSpec", justify="right", style="green")
                table.add_column("Core", justify="right", style="yellow")
                table.add_column("ORM", justify="right", style="blue")
                table.add_column("Best→Worst", justify="right", style="magenta")
            else:
                # Detailed mode - full information
                table.add_column("SQLSpec (Cached)\nms | TPS", justify="right")
                table.add_column("SQLSpec (No Cache)\nms | TPS", justify="right")
                table.add_column("SQLAlchemy Core\nms | TPS", justify="right")
                table.add_column("SQLAlchemy ORM\nms | TPS", justify="right")
                table.add_column("Winner", justify="right")

            for op_name, variants in ops.items():
                sqlspec_cached = variants.get("sqlspec_cache")
                sqlspec_no_cache = variants.get("sqlspec_no_cache")
                sqlalchemy_core = variants.get("sqlalchemy_core")
                sqlalchemy_orm = variants.get("sqlalchemy_orm")

                # Find the best SQLSpec result
                best_sqlspec = None
                if sqlspec_cached and sqlspec_no_cache:
                    best_sqlspec = sqlspec_cached if sqlspec_cached.avg_ms < sqlspec_no_cache.avg_ms else sqlspec_no_cache
                elif sqlspec_cached:
                    best_sqlspec = sqlspec_cached
                elif sqlspec_no_cache:
                    best_sqlspec = sqlspec_no_cache

                # Find the winner
                winner = ""
                winner_time = float("inf")
                if best_sqlspec and best_sqlspec.avg_ms < winner_time:
                    winner = "SQLSpec"
                    winner_time = best_sqlspec.avg_ms
                if sqlalchemy_core and sqlalchemy_core.avg_ms < winner_time:
                    winner = "Core"
                    winner_time = sqlalchemy_core.avg_ms
                if sqlalchemy_orm and sqlalchemy_orm.avg_ms < winner_time:
                    winner = "ORM"

                # Build row based on display mode
                if self.display_mode == "compact":
                    row_data = [
                        op_name.replace("_", " ").title(),
                        f"{best_sqlspec.avg_ms:.3f}" if best_sqlspec else "N/A",
                        f"{sqlalchemy_core.avg_ms:.3f}" if sqlalchemy_core else "N/A",
                        f"{sqlalchemy_orm.avg_ms:.3f}" if sqlalchemy_orm else "N/A",
                        winner,
                    ]
                elif self.display_mode == "matrix":
                    # Calculate speedup ratios
                    def calc_speedup(base_result: Optional[TimingResult], compare_result: Optional[TimingResult]) -> str:
                        if base_result and compare_result and compare_result.avg_ms > 0:
                            return f"{base_result.avg_ms / compare_result.avg_ms:.1f}x"
                        return "N/A"

                    # Calculate best to worst ratio
                    times = [r.avg_ms for r in [best_sqlspec, sqlalchemy_core, sqlalchemy_orm] if r]
                    ratio = f"{max(times) / min(times):.1f}x" if times else "N/A"

                    row_data = [
                        op_name.replace("_", " ").title(),
                        f"{best_sqlspec.avg_ms:.3f}" if best_sqlspec else "N/A",
                        f"{sqlalchemy_core.avg_ms:.3f}" if sqlalchemy_core else "N/A",
                        f"{sqlalchemy_orm.avg_ms:.3f}" if sqlalchemy_orm else "N/A",
                        ratio,
                    ]
                else:
                    # Detailed mode
                    def format_result(result: Optional[TimingResult]) -> str:
                        if result:
                            return f"{result.avg_ms:.3f} | {result.ops_per_sec:.0f}"
                        return "N/A"

                    row_data = [
                        op_name.replace("_", " ").title(),
                        format_result(sqlspec_cached),
                        format_result(sqlspec_no_cache),
                        format_result(sqlalchemy_core),
                        format_result(sqlalchemy_orm),
                        winner,
                    ]

                table.add_row(*row_data)

            self.console.print(table)

        # Add unified comparison table with all databases and TPS
        self._display_unified_orm_comparison(grouped_results)

    def _display_performance_by_driver(
        self, db_results: dict[str, dict[str, TimingResult]], key_operations: list[tuple]
    ) -> None:
        """Display performance metrics organized by driver."""
        self.console.print("\n[bold cyan]📊 Performance Metrics by Driver and ORM Type[/bold cyan]")
        self.console.print("[dim]Higher statements/second = better performance[/dim]\n")

        # Find fastest SQLSpec result across all operations for baseline
        fastest_sqlspec_stmts_per_sec = 0.0
        for db_ops in db_results.values():
            for result_key, result in db_ops.items():
                if "sqlspec" in result_key:
                    stmts_per_sec = 1000.0 / result.avg_ms if result.avg_ms > 0 else 0.0
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
            op_width = None if self.no_truncate else 18
            metric_width = None if self.no_truncate else 11
            winner_width = None if self.no_truncate else 10
            table.add_column("Operation", style="cyan", width=op_width)
            table.add_column("SQLSpec\n(cached)", justify="right", style="green", width=metric_width)
            table.add_column("SQLSpec\n(no cache)", justify="right", style="green dim", width=metric_width)
            table.add_column("Core\n(stmts/sec)", justify="right", style="yellow", width=metric_width)
            table.add_column("ORM\n(stmts/sec)", justify="right", style="blue", width=metric_width)
            table.add_column("Winner", justify="center", style="bold", width=winner_width)

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
        db_width = None if self.no_truncate else 15
        sync_width = None if self.no_truncate else 10
        op_width = None if self.no_truncate else 20
        orm_width = None if self.no_truncate else 10
        time_width = None if self.no_truncate else 12
        tps_width = None if self.no_truncate else 12
        std_width = None if self.no_truncate else 12
        minmax_width = None if self.no_truncate else 15
        table.add_column("Database", style="cyan", width=db_width)
        table.add_column("Sync/Async", justify="center", width=sync_width)
        table.add_column("Operation", style="cyan", width=op_width)
        table.add_column("ORM Type", justify="center", width=orm_width)
        table.add_column("Avg Time (ms)", justify="right", width=time_width)
        table.add_column("Stmts/sec", justify="right", style="bold green", width=tps_width)
        table.add_column("Std Dev (ms)", justify="right", width=std_width)
        table.add_column("Min/Max (ms)", justify="right", width=minmax_width)

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
        db_width = None if self.no_truncate else 15
        op_width = None if self.no_truncate else 20
        metric_width = None if self.no_truncate else 12
        table.add_column("Database", style="cyan", width=db_width)
        table.add_column("Operation", style="cyan", width=op_width)
        table.add_column("SQLSpec\n(baseline)", justify="right", style="green", width=metric_width)
        table.add_column("Core\n(% slower)", justify="right", style="yellow", width=metric_width)
        table.add_column("ORM\n(% slower)", justify="right", style="blue", width=metric_width)
        table.add_column("Best→Worst\nRatio", justify="right", style="magenta", width=metric_width)

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
        modules: dict[str, dict[str, TimingResult]] = {}
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
        module_width = None if self.no_truncate else 25
        metric_width = None if self.no_truncate else 12
        speedup_width = None if self.no_truncate else 10
        table.add_column("Module", style="cyan", width=module_width)
        table.add_column("Baseline\n(ms)", justify="right", width=metric_width)
        table.add_column("Compiled\n(ms)", justify="right", width=metric_width)
        table.add_column("Speedup", justify="right", style="bold green", width=speedup_width)
        table.add_column("Improvement", justify="right", style="bold yellow", width=metric_width)
        table.add_column("Baseline\n(ops/sec)", justify="right", width=metric_width)
        table.add_column("Compiled\n(ops/sec)", justify="right", width=metric_width)

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
            # Use display options to determine how many to show
            reg_count = len(regressions) if self.show_all else min(3, len(regressions))
            for op, info, _ in regressions[:reg_count]:
                summary_text += f"• {op}: {info}\n"
            if not self.show_all and len(regressions) > 3:
                summary_text += f"• ... and {len(regressions) - 3} more (use --show-all to see all)\n"

        if improvements:
            summary_text += "\n[bold green]✓ Performance Improvements[/bold green]\n"
            # Use display options to determine how many to show
            imp_count = len(improvements) if self.show_all else min(3, len(improvements))
            for op, info, _ in improvements[:imp_count]:
                summary_text += f"• {op}: {info}\n"
            if not self.show_all and len(improvements) > 3:
                summary_text += f"• ... and {len(improvements) - 3} more (use --show-all to see all)\n"

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

    def display_top_performers(self, all_results: dict[str, TimingResult], count: Optional[int] = None) -> None:
        """Display table of top performing operations."""
        if not all_results:
            return

        # Use display options to determine count
        if count is None:
            if self.show_all:
                count = len(all_results)
            else:
                count = min(self.max_items, len(all_results))

        # Sort by operations per second
        sorted_results = sorted(all_results.items(), key=lambda x: x[1].ops_per_sec, reverse=True)

        # Adjust title based on display mode
        if self.show_all:
            title = "All Operations by Performance"
        else:
            title = f"Top {count} Fastest Operations"

        table = Table(title=title, show_header=True)
        rank_width = None if self.no_truncate else 4
        table.add_column("Rank", style="bold cyan", width=rank_width)

        # Dynamic column width based on display mode
        op_width = None if self.no_truncate else (40 if self.display_mode == "detailed" else 25)
        table.add_column("Operation", style="cyan", width=op_width)
        table.add_column("Avg Time (ms)", justify="right")
        table.add_column("Ops/sec", justify="right", style="bold green")

        if self.display_mode == "detailed":
            table.add_column("Performance", justify="center")
            table.add_column("Std Dev (ms)", justify="right")

        for i, (operation, result) in enumerate(sorted_results[:count], 1):
            row_data = [str(i), operation, f"{result.avg_ms:.3f}", f"{result.ops_per_sec:.1f}"]

            if self.display_mode == "detailed":
                # Performance indicator
                if result.ops_per_sec > OPS_LIGHTNING_FAST:
                    perf_indicator = "🚀"
                elif result.ops_per_sec > OPS_VERY_FAST:
                    perf_indicator = "⚡"
                elif result.ops_per_sec > OPS_FAST:
                    perf_indicator = "✅"
                else:
                    perf_indicator = "🐌"

                row_data.extend([perf_indicator, f"{result.std_ms:.3f}"])

            table.add_row(*row_data)

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

    def _display_unified_orm_comparison(self, grouped_results: dict[str, dict[str, dict[str, TimingResult]]]) -> None:
        """Display a unified comparison table showing all databases and their TPS."""
        if not grouped_results:
            return

        # Create unified table with dynamic layout
        title = "🚀 Unified ORM Performance Comparison"
        if self.display_mode != "compact":
            title += " (TPS = Transactions Per Second)"

        table = Table(title=title, show_header=True)

        # Dynamic column widths based on display options
        db_width = None if self.no_truncate else (12 if self.display_mode == "compact" else 15)
        op_width = None if self.no_truncate else (15 if self.display_mode == "compact" else 20)

        table.add_column("Database", style="cyan", width=db_width)
        table.add_column("Operation", style="cyan", width=op_width)

        if self.display_mode == "compact":
            # Compact mode - just show best results and winner
            table.add_column("Best SQLSpec", justify="right", style="green")
            table.add_column("Best SA", justify="right", style="yellow")
            table.add_column("Winner", justify="center", style="bold")
        elif self.display_mode == "matrix":
            # Matrix mode - show ratios and comparisons
            table.add_column("SQLSpec TPS", justify="right", style="green")
            table.add_column("Core TPS", justify="right", style="yellow")
            table.add_column("ORM TPS", justify="right", style="blue")
            table.add_column("Speedup", justify="right", style="bold magenta")
        else:
            # Detailed mode - show all data
            table.add_column("SQLSpec Cached\n(TPS)", justify="right", style="green")
            table.add_column("SQLSpec No Cache\n(TPS)", justify="right", style="green dim")
            table.add_column("SQLAlchemy Core\n(TPS)", justify="right", style="yellow")
            table.add_column("SQLAlchemy ORM\n(TPS)", justify="right", style="blue")
            table.add_column("Best TPS", justify="right", style="bold magenta")
            table.add_column("Winner", justify="center", style="bold")

        # Collect all operations across databases
        all_operations: set[str] = set()
        for ops in grouped_results.values():
            all_operations.update(ops.keys())

        for db_name in sorted(grouped_results.keys()):
            ops = grouped_results[db_name]
            for op_name in sorted(all_operations):
                if op_name not in ops:
                    continue

                variants = ops[op_name]
                sqlspec_cached = variants.get("sqlspec_cache")
                sqlspec_no_cache = variants.get("sqlspec_no_cache")
                sqlalchemy_core = variants.get("sqlalchemy_core")
                sqlalchemy_orm = variants.get("sqlalchemy_orm")

                # Find best TPS and winner
                best_tps = 0.0
                winner = "N/A"

                if sqlspec_cached and sqlspec_cached.ops_per_sec > best_tps:
                    best_tps = sqlspec_cached.ops_per_sec
                    winner = "SQLSpec (C)"
                if sqlspec_no_cache and sqlspec_no_cache.ops_per_sec > best_tps:
                    best_tps = sqlspec_no_cache.ops_per_sec
                    winner = "SQLSpec (NC)"
                if sqlalchemy_core and sqlalchemy_core.ops_per_sec > best_tps:
                    best_tps = sqlalchemy_core.ops_per_sec
                    winner = "SA Core"
                if sqlalchemy_orm and sqlalchemy_orm.ops_per_sec > best_tps:
                    best_tps = sqlalchemy_orm.ops_per_sec
                    winner = "SA ORM"

                def format_tps(result: Optional[TimingResult]) -> str:
                    return f"{result.ops_per_sec:.0f}" if result else "N/A"

                # Build row data based on display mode
                if self.display_mode == "compact":
                    # Find best SQLSpec and SQLAlchemy results
                    best_sqlspec_tps = 0.0
                    best_sa_tps = 0.0

                    if sqlspec_cached:
                        best_sqlspec_tps = max(best_sqlspec_tps, sqlspec_cached.ops_per_sec)
                    if sqlspec_no_cache:
                        best_sqlspec_tps = max(best_sqlspec_tps, sqlspec_no_cache.ops_per_sec)
                    if sqlalchemy_core:
                        best_sa_tps = max(best_sa_tps, sqlalchemy_core.ops_per_sec)
                    if sqlalchemy_orm:
                        best_sa_tps = max(best_sa_tps, sqlalchemy_orm.ops_per_sec)

                    row_data = [
                        db_name.title(),
                        op_name.replace("_", " ").title(),
                        f"{best_sqlspec_tps:.0f}" if best_sqlspec_tps > 0 else "N/A",
                        f"{best_sa_tps:.0f}" if best_sa_tps > 0 else "N/A",
                        winner,
                    ]
                elif self.display_mode == "matrix":
                    # Calculate best SQLSpec result for comparison
                    best_sqlspec_tps = 0.0
                    if sqlspec_cached:
                        best_sqlspec_tps = max(best_sqlspec_tps, sqlspec_cached.ops_per_sec)
                    if sqlspec_no_cache:
                        best_sqlspec_tps = max(best_sqlspec_tps, sqlspec_no_cache.ops_per_sec)

                    # Calculate speedup
                    core_tps = sqlalchemy_core.ops_per_sec if sqlalchemy_core else 0.0
                    speedup = "N/A"
                    if best_sqlspec_tps > 0 and core_tps > 0:
                        speedup = f"{best_sqlspec_tps / core_tps:.1f}x"

                    row_data = [
                        db_name.title(),
                        op_name.replace("_", " ").title(),
                        f"{best_sqlspec_tps:.0f}" if best_sqlspec_tps > 0 else "N/A",
                        f"{core_tps:.0f}" if core_tps > 0 else "N/A",
                        f"{sqlalchemy_orm.ops_per_sec:.0f}" if sqlalchemy_orm else "N/A",
                        speedup,
                    ]
                else:
                    # Detailed mode - show all data
                    row_data = [
                        db_name.title(),
                        op_name.replace("_", " ").title(),
                        format_tps(sqlspec_cached),
                        format_tps(sqlspec_no_cache),
                        format_tps(sqlalchemy_core),
                        format_tps(sqlalchemy_orm),
                        f"{best_tps:.0f}" if best_tps > 0 else "N/A",
                        winner,
                    ]

                table.add_row(*row_data)

        self.console.print(table)

        # Add summary statistics
        self._display_tps_summary(grouped_results)

    def _display_tps_summary(self, grouped_results: dict[str, dict[str, dict[str, TimingResult]]]) -> None:
        """Display summary statistics for TPS across all databases."""
        if not grouped_results:
            return

        # Collect TPS data by adapter type
        adapter_tps: dict[str, list[float]] = {"SQLSpec (Cached)": [], "SQLSpec (No Cache)": [], "SQLAlchemy Core": [], "SQLAlchemy ORM": []}

        database_tps: dict[str, list[float]] = {}  # Track TPS by database

        for db_name, ops in grouped_results.items():
            database_tps[db_name] = []

            for variants in ops.values():
                if "sqlspec_cache" in variants:
                    tps = variants["sqlspec_cache"].ops_per_sec
                    adapter_tps["SQLSpec (Cached)"].append(tps)
                    database_tps[db_name].append(tps)
                if "sqlspec_no_cache" in variants:
                    tps = variants["sqlspec_no_cache"].ops_per_sec
                    adapter_tps["SQLSpec (No Cache)"].append(tps)
                    database_tps[db_name].append(tps)
                if "sqlalchemy_core" in variants:
                    tps = variants["sqlalchemy_core"].ops_per_sec
                    adapter_tps["SQLAlchemy Core"].append(tps)
                    database_tps[db_name].append(tps)
                if "sqlalchemy_orm" in variants:
                    tps = variants["sqlalchemy_orm"].ops_per_sec
                    adapter_tps["SQLAlchemy ORM"].append(tps)
                    database_tps[db_name].append(tps)

        # Create summary table with dynamic layout
        summary_table = Table(title="📊 TPS Performance Summary", show_header=True)

        # Dynamic column widths
        adapter_width = None if self.no_truncate else (25 if self.display_mode == "detailed" else 20)
        summary_table.add_column("Adapter", style="cyan", width=adapter_width)
        summary_table.add_column("Avg TPS", justify="right", style="green")

        if self.display_mode != "compact":
            summary_table.add_column("Max TPS", justify="right", style="bold green")
            summary_table.add_column("Min TPS", justify="right", style="red")

        if self.display_mode == "detailed":
            summary_table.add_column("Operations", justify="right")

        for adapter_name, tps_values in adapter_tps.items():
            if tps_values:
                avg_tps = sum(tps_values) / len(tps_values)
                max_tps = max(tps_values)
                min_tps = min(tps_values)
                count = len(tps_values)

                # Build row data based on display mode
                row_data = [adapter_name, f"{avg_tps:.0f}"]

                if self.display_mode != "compact":
                    row_data.extend([f"{max_tps:.0f}", f"{min_tps:.0f}"])

                if self.display_mode == "detailed":
                    row_data.append(str(count))

                summary_table.add_row(*row_data)

        self.console.print(summary_table)

        # Database-specific summary
        if len(database_tps) > 1:
            # Limit display based on options
            display_count = len(database_tps)
            if not self.show_all:
                display_count = min(self.max_items, len(database_tps))

            db_summary_table = Table(title="🎯 Database Performance Ranking", show_header=True)

            # Dynamic column widths
            db_width = None if self.no_truncate else (15 if self.display_mode == "detailed" else 12)
            db_summary_table.add_column("Database", style="cyan", width=db_width)
            db_summary_table.add_column("Avg TPS", justify="right", style="green")

            if self.display_mode != "compact":
                db_summary_table.add_column("Max TPS", justify="right", style="bold green")

            db_summary_table.add_column("Rank", justify="center", style="bold")

            # Calculate averages and rank
            db_averages = []
            for db_name, tps_values in database_tps.items():
                if tps_values:
                    avg_tps = sum(tps_values) / len(tps_values)
                    max_tps = max(tps_values)
                    db_averages.append((db_name, avg_tps, max_tps))

            # Sort by average TPS and limit results
            db_averages.sort(key=lambda x: x[1], reverse=True)

            # Apply display limit
            limited_averages = db_averages[:display_count]

            for rank, (db_name, avg_tps, max_tps) in enumerate(limited_averages, 1):
                rank_icon = (
                    "🥇"
                    if rank == FIRST_PLACE
                    else "🥈"
                    if rank == SECOND_PLACE
                    else "🥉"
                    if rank == THIRD_PLACE
                    else "📍"
                )

                # Build row data based on display mode
                row_data = [db_name.title(), f"{avg_tps:.0f}"]

                if self.display_mode != "compact":
                    row_data.append(f"{max_tps:.0f}")

                row_data.append(f"{rank_icon} #{rank}")

                db_summary_table.add_row(*row_data)

            # Add truncation note if needed
            if not self.show_all and len(db_averages) > display_count:
                self.console.print(f"[dim]... and {len(db_averages) - display_count} more databases (use --show-all to see all)[/dim]")

            self.console.print(db_summary_table)
