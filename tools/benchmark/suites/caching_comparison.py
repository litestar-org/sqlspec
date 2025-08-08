"""Caching impact comparison benchmark suite."""

from typing import Any

from rich.progress import Progress, TaskID

from tools.benchmark.core.metrics import BenchmarkMetrics, TimingResult
from tools.benchmark.suites.base import BaseBenchmarkSuite


class CachingComparisonBenchmark(BaseBenchmarkSuite):
    """Compare performance with and without caching enabled."""

    @property
    def name(self) -> str:
        return "caching_comparison"

    @property
    def description(self) -> str:
        return "Caching Impact Analysis"

    def run(self, adapter: str = "all", **kwargs: Any) -> dict[str, TimingResult]:
        """Run caching comparison benchmarks."""
        from sqlspec.core.statement import SQL, StatementConfig
        from sqlspec.core.statement.cache import sql_cache

        results = {}

        # Test SQL statements of varying complexity
        test_sqls = {
            "simple_select": "SELECT id, name FROM users WHERE id = ?",
            "complex_join": """
                SELECT u.name, p.title, c.content
                FROM users u
                JOIN posts p ON u.id = p.user_id
                JOIN comments c ON p.id = c.post_id
                WHERE u.created_at > ? AND p.status = ?
            """,
            "subquery": """
                SELECT * FROM users WHERE id IN (
                    SELECT user_id FROM orders
                    WHERE total > ? AND created_at > ?
                )
            """,
            "window_function": """
                SELECT
                    name,
                    salary,
                    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
                FROM employees
                WHERE department = ?
            """,
            "cte": """
                WITH ranked_users AS (
                    SELECT name, score,
                           ROW_NUMBER() OVER (ORDER BY score DESC) as rank
                    FROM users WHERE active = ?
                )
                SELECT * FROM ranked_users WHERE rank <= ?
            """,
        }

        self.console.print("\n[bold cyan]Testing caching impact...[/bold cyan]")

        if self.config.show_progress:
            progress_ctx = Progress(console=self.console)
            progress = progress_ctx.__enter__()
            task: TaskID = progress.add_task("Running caching benchmarks...", total=len(test_sqls) * 2)
        else:
            progress_ctx = None
            progress = None
            task = None

        try:
            for sql_name, sql_text in test_sqls.items():
                # Test WITHOUT caching
                sql_cache.clear()

                def no_cache_operation(text: str = sql_text) -> tuple[str, Any]:
                    stmt = SQL(text, config=StatementConfig(enable_caching=False))
                    return stmt.compile()

                times = BenchmarkMetrics.time_operation(
                    no_cache_operation,
                    iterations=self.config.iterations // 2,  # Fewer iterations for stability
                    warmup=5,
                )
                no_cache_result = TimingResult(
                    operation=f"no_cache_{sql_name}", iterations=self.config.iterations // 2, times=times
                )
                results[f"no_cache_{sql_name}"] = no_cache_result
                if progress:
                    progress.advance(task)

                # Test WITH caching
                sql_cache.clear()

                def with_cache_operation(text: str = sql_text) -> tuple[str, Any]:
                    stmt = SQL(text, config=StatementConfig(enable_caching=True))
                    return stmt.compile()

                # Warmup cache first
                warmup_stmt = SQL(sql_text, config=StatementConfig(enable_caching=True))
                warmup_stmt.compile()

                times = BenchmarkMetrics.time_operation(
                    with_cache_operation, iterations=self.config.iterations // 2, warmup=5
                )
                with_cache_result = TimingResult(
                    operation=f"with_cache_{sql_name}", iterations=self.config.iterations // 2, times=times
                )
                results[f"with_cache_{sql_name}"] = with_cache_result
                if progress:
                    progress.advance(task)

        finally:
            if progress_ctx:
                progress_ctx.__exit__(None, None, None)

        return results
