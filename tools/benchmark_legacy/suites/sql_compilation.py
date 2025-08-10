"""SQL compilation benchmark suite."""

from typing import Any

from rich.panel import Panel
from sqlglot import parse_one

from sqlspec.core.statement import SQL
from tools.benchmark.core.metrics import BenchmarkMetrics, TimingResult
from tools.benchmark.suites.base import BaseBenchmarkSuite


class SQLCompilationBenchmark(BaseBenchmarkSuite):
    """Benchmark SQL parsing and compilation."""

    @property
    def name(self) -> str:
        return "sql_compilation"

    @property
    def description(self) -> str:
        return "Benchmarks SQL parsing and compilation performance"

    def run(self, adapter: str = "all", **kwargs: Any) -> dict[str, TimingResult]:
        """Run SQL compilation benchmarks."""
        self.console.print(
            Panel.fit(
                f"[bold]SQL Compilation Benchmark[/bold]\nIterations: [cyan]{self.config.iterations:,}[/cyan]",
                border_style="blue",
            )
        )

        # Start run
        self.runner.start_run(benchmark_type=self.name, adapter="sqlglot", metadata={"test_type": "compilation"})

        results = {}

        # Test cases with increasing complexity
        test_cases = [
            ("simple_select", "SELECT * FROM users"),
            ("where_clause", "SELECT id, name FROM users WHERE status = 'active'"),
            (
                "join_query",
                """
                SELECT u.id, u.name, p.bio
                FROM users u
                JOIN profiles p ON u.id = p.user_id
            """,
            ),
            (
                "complex_query",
                """
                WITH active_users AS (
                    SELECT id, name, created_at
                    FROM users
                    WHERE status = 'active' AND created_at > '2024-01-01'
                ),
                user_stats AS (
                    SELECT
                        user_id,
                        COUNT(*) as order_count,
                        SUM(total) as total_spent
                    FROM orders
                    GROUP BY user_id
                )
                SELECT
                    au.id,
                    au.name,
                    au.created_at,
                    COALESCE(us.order_count, 0) as orders,
                    COALESCE(us.total_spent, 0) as spent
                FROM active_users au
                LEFT JOIN user_stats us ON au.id = us.user_id
                ORDER BY us.total_spent DESC NULLS LAST
                LIMIT 100
            """,
            ),
        ]

        # Benchmark raw SQLGlot parsing
        self.console.print("\n[cyan]SQLGlot Parsing:[/cyan]")
        for name, query in test_cases:
            times = BenchmarkMetrics.time_operation(
                lambda q=query: parse_one(q), iterations=self.config.iterations, warmup=self.config.warmup_iterations
            )

            result = TimingResult(operation=f"parse_{name}", iterations=self.config.iterations, times=times)
            results[f"parse_{name}"] = result

            self.runner.storage.save_result(self.runner.current_run_id, result, save_samples=False)

        # Benchmark SQLSpec SQL object creation and compilation
        self.console.print("\n[cyan]SQLSpec Compilation:[/cyan]")
        for name, query in test_cases:
            # With parameters for more realistic scenario
            parameters = {"status": "active", "date": "2024-01-01", "limit": 100}

            times = BenchmarkMetrics.time_operation(
                lambda q=query, p=parameters: SQL(q, p).compile(),
                iterations=self.config.iterations,
                warmup=self.config.warmup_iterations,
            )

            result = TimingResult(operation=f"compile_{name}", iterations=self.config.iterations, times=times)
            results[f"compile_{name}"] = result

            self.runner.storage.save_result(self.runner.current_run_id, result, save_samples=False)

            # Check for regression
            regression_info = self.check_regression(f"compile_{name}", result, "sqlglot")
            if regression_info:
                self.console.print(f"  compile_{name}: {regression_info}")

        # Display results
        self.display_results(results)

        return results
