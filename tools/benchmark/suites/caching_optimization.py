"""Benchmark suite for caching optimization testing.

This suite measures the performance improvements from the enhanced
caching system including AST fragment caching and optimized expression caching.
"""

import time
from typing import Any

from sqlspec.statement.builder import Select
from sqlspec.statement.cache import ast_fragment_cache, optimized_expression_cache, sql_cache
from sqlspec.statement.cache_config import get_cache_stats, reset_cache_stats
from sqlspec.statement.sql import SQL
from tools.benchmark.core.metrics import BenchmarkMetrics, TimingResult
from tools.benchmark.suites.base import BaseBenchmarkSuite


class CachingOptimizationBenchmark(BaseBenchmarkSuite):
    """Benchmark suite to measure caching optimization performance."""

    @property
    def name(self) -> str:
        return "caching_optimization"

    @property
    def description(self) -> str:
        return "Test caching improvements for SQL parsing and optimization"

    def setup(self) -> None:
        """Setup benchmark environment."""
        # Clear all caches to start fresh
        sql_cache.clear()
        ast_fragment_cache.clear()
        optimized_expression_cache.clear()
        reset_cache_stats()

    def run(self, adapter: str = "all", **kwargs: Any) -> dict[str, TimingResult]:
        """Run all caching benchmarks."""
        # Start run
        self.runner.start_run(benchmark_type=self.name, adapter="cache", metadata={"test_type": "caching"})

        results = {}

        # Warm up caches with some queries
        self._warmup_caches()

        # Test 1: Fragment caching for common WHERE clauses
        results["fragment_caching"] = self._test_fragment_caching()

        # Test 2: Optimized expression caching
        results["optimization_caching"] = self._test_optimization_caching()

        # Test 3: Complex query with repeated subqueries
        results["complex_query_caching"] = self._test_complex_query_caching()

        # Test 4: Cache hit rates under load
        results["cache_hit_rates"] = self._test_cache_hit_rates()

        # Log final cache statistics
        get_cache_stats()

        return results

    def _warmup_caches(self) -> None:
        """Warm up caches with common patterns."""
        # Common WHERE clauses
        common_conditions = [
            "status = 'active'",
            "created_at > '2024-01-01'",
            "user_id IN (1, 2, 3, 4, 5)",
            "price BETWEEN 10 AND 100",
            "name LIKE '%test%'",
        ]

        for condition in common_conditions:
            SQL(f"SELECT * FROM users WHERE {condition}").compile()

        # Common JOIN patterns
        join_queries = [
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
            "SELECT * FROM products p LEFT JOIN categories c ON p.category_id = c.id",
        ]

        for query in join_queries:
            SQL(query).compile()

    def _test_fragment_caching(self) -> TimingResult:
        """Test fragment caching for common WHERE clauses."""
        # Reset stats for this test
        reset_cache_stats()

        # Common WHERE clause patterns that should benefit from caching
        where_patterns = [
            "status = 'active' AND deleted_at IS NULL",
            "created_at >= NOW() - INTERVAL '7 days'",
            "user_id = 123 AND role IN ('admin', 'moderator')",
            "price > 100 AND discount_percentage < 50",
            "(category = 'electronics' OR category = 'computers') AND in_stock = true",
        ]

        # Define the operation to time
        def run_fragment_test() -> None:
            for pattern in where_patterns:
                sql = SQL(f"SELECT id, name, email FROM users WHERE {pattern}")
                sql.compile()

        # Time the operation
        times = BenchmarkMetrics.time_operation(
            func=run_fragment_test,
            iterations=self.config.iterations // len(where_patterns),
            warmup=5,
        )

        get_cache_stats()

        return TimingResult(
            operation="fragment_caching_where_clauses",
            iterations=len(times),
            times=times,
        )

    def _test_optimization_caching(self) -> TimingResult:
        """Test caching of optimized expressions."""
        reset_cache_stats()

        # Create builders with optimization enabled
        schema = {
            "users": {"id": "INT", "name": "VARCHAR", "email": "VARCHAR", "status": "VARCHAR"},
            "orders": {"id": "INT", "user_id": "INT", "total": "DECIMAL", "status": "VARCHAR"},
            "products": {"id": "INT", "name": "VARCHAR", "price": "DECIMAL", "category_id": "INT"},
        }

        complex_queries = []
        for i in range(5):
            # Build similar but slightly different queries
            builder = Select(schema=schema)
            builder = (
                builder.select("u.id", "u.name", "COUNT(o.id) as order_count", "SUM(o.total) as total_spent")
                .from_("users", "u")
                .join("orders", on="u.id = o.user_id", alias="o")
                .where(f"u.status = 'active' AND o.status = 'completed' AND o.total > {i * 100}")
                .group_by("u.id", "u.name")
                .having("COUNT(o.id) > 5")
                .order_by("total_spent", desc=True)
                .limit(10)
            )
            complex_queries.append(builder)

        # Define the operation to time
        def run_optimization_test() -> None:
            for builder in complex_queries:
                builder.build()

        # Time the operation
        times = BenchmarkMetrics.time_operation(
            func=run_optimization_test,
            iterations=self.config.iterations // len(complex_queries),
            warmup=5,
        )

        get_cache_stats()

        return TimingResult(
            operation="optimization_caching",
            iterations=len(times),
            times=times,
        )

    def _test_complex_query_caching(self) -> TimingResult:
        """Test caching with complex queries containing repeated subqueries."""
        reset_cache_stats()

        # Complex query with repeated subqueries that should benefit from caching
        base_query = """
        WITH active_users AS (
            SELECT id, name, email
            FROM users
            WHERE status = 'active' AND verified = true
        ),
        recent_orders AS (
            SELECT user_id, COUNT(*) as order_count, SUM(total) as total_amount
            FROM orders
            WHERE created_at > NOW() - INTERVAL '30 days'
            GROUP BY user_id
        )
        SELECT
            au.id,
            au.name,
            au.email,
            COALESCE(ro.order_count, 0) as recent_orders,
            COALESCE(ro.total_amount, 0) as recent_total
        FROM active_users au
        LEFT JOIN recent_orders ro ON au.id = ro.user_id
        WHERE au.id IN (
            SELECT DISTINCT user_id
            FROM orders
            WHERE total > 100
        )
        ORDER BY recent_total DESC
        LIMIT 50
        """

        # Create variations of the query
        queries = []
        for days in [7, 14, 30, 60, 90]:
            query = base_query.replace("30 days", f"{days} days")
            queries.append(query)

        # Define the operation to time
        def run_complex_test() -> None:
            for query in queries:
                sql = SQL(query)
                sql.compile()

        # Time the operation
        times = BenchmarkMetrics.time_operation(
            func=run_complex_test,
            iterations=self.config.iterations // len(queries),
            warmup=5,
        )

        get_cache_stats()

        return TimingResult(
            operation="complex_query_caching",
            iterations=len(times),
            times=times,
        )

    def _test_cache_hit_rates(self) -> TimingResult:
        """Test cache hit rates under realistic load."""
        reset_cache_stats()

        # Simulate realistic query patterns with some repetition
        query_templates = [
            "SELECT * FROM {table} WHERE id = {id}",
            "SELECT * FROM {table} WHERE status = '{status}'",
            "SELECT COUNT(*) FROM {table} WHERE created_at > '{date}'",
            "SELECT {col1}, {col2} FROM {table} ORDER BY {col1} LIMIT {limit}",
            "SELECT * FROM {table1} JOIN {table2} ON {table1}.id = {table2}.{table1}_id",
        ]

        tables = ["users", "orders", "products", "categories", "reviews"]
        statuses = ["active", "inactive", "pending", "completed"]
        columns = ["id", "name", "created_at", "updated_at", "status"]

        # Define the operation to time
        def run_single_query(i: int) -> None:
            template = query_templates[i % len(query_templates)]

            # Create query with some repeated values to test caching
            table = tables[(i // 10) % len(tables)]
            status = statuses[(i // 20) % len(statuses)]
            col1 = columns[(i // 5) % len(columns)]
            col2 = columns[(i // 7) % len(columns)]

            query = template.format(
                table=table,
                table1=table,
                table2=tables[(i + 1) % len(tables)],
                id=(i % 100) + 1,
                status=status,
                date="2024-01-01",
                col1=col1,
                col2=col2,
                limit=(i % 50) + 10,
            )

            sql = SQL(query)
            sql.compile()

        # Time individual operations
        times = []
        for i in range(self.config.iterations):
            start = time.perf_counter()
            run_single_query(i)
            end = time.perf_counter()
            times.append(end - start)

        get_cache_stats()

        return TimingResult(
            operation="cache_hit_rates_under_load",
            iterations=len(times),
            times=times,
        )

    def cleanup(self) -> None:
        """Clean up after benchmarks."""
