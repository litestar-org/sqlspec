"""Parameters benchmark suite."""

import math
from typing import Any

from rich.panel import Panel

from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.types import TypedParameter
from sqlspec.statement.sql import SQL
from tools.benchmark.core.metrics import BenchmarkMetrics, TimingResult
from tools.benchmark.suites.base import BaseBenchmarkSuite


class ParametersBenchmark(BaseBenchmarkSuite):
    """Benchmark parameter handling, including style conversion and TypedParameter overhead."""

    @property
    def name(self) -> str:
        return "parameters"

    @property
    def description(self) -> str:
        return "Benchmarks parameter handling and TypedParameter overhead"

    def run(self, adapter: str = "all", **kwargs: Any) -> dict[str, TimingResult]:
        """Run parameter benchmarks."""
        adapters = self._get_adapters(adapter)
        results = {}

        for adp in adapters:
            self.console.print(
                Panel.fit(
                    f"[bold]Parameters - {adp}[/bold]\nIterations: [cyan]{self.config.iterations:,}[/cyan]",
                    border_style="blue",
                )
            )

            # Start run for this adapter
            self.runner.start_run(benchmark_type=self.name, adapter=adp, metadata={"test_type": "parameters"})

            # Get adapter-specific results
            adapter_results = self._benchmark_adapter(adp)

            # Save results
            for operation, result in adapter_results.items():
                self.runner.storage.save_result(self.runner.current_run_id, result, save_samples=False)

                # Check for regression
                regression_info = self.check_regression(operation, result, adp)
                if regression_info:
                    self.console.print(f"  {operation}: {regression_info}")

            # Merge into overall results
            for op, res in adapter_results.items():
                results[f"{adp}_{op}"] = res

        # Run typed parameter benchmarks (generic)
        results.update(self._benchmark_typed_parameters())

        return results

    def _get_adapters(self, adapter: str) -> list[str]:
        """Get list of adapters to test."""
        if adapter != "all":
            return [adapter]

        # Default adapters to test
        return ["sqlite", "duckdb", "psycopg", "aiosqlite", "asyncpg", "oracledb"]

    def _benchmark_adapter(self, adapter: str) -> dict[str, TimingResult]:
        """Benchmark parameter styles for a specific adapter."""
        results = {}

        # Map adapter to parameter style
        style_map = {
            "sqlite": ParameterStyle.QMARK,
            "duckdb": ParameterStyle.NAMED_DOLLAR,
            "psycopg": ParameterStyle.NAMED_PYFORMAT,
            "aiosqlite": ParameterStyle.QMARK,
            "asyncpg": ParameterStyle.NAMED_DOLLAR,
            "oracledb": ParameterStyle.NAMED_COLON,
        }
        target_style = style_map.get(adapter, ParameterStyle.QMARK)

        test_cases = [
            ("simple_select", "SELECT * FROM users WHERE id = :id", {"id": 1}),
            (
                "multi_param",
                "SELECT * FROM users WHERE age > :min_age AND age < :max_age",
                {"min_age": 18, "max_age": 65},
            ),
            ("in_clause", "SELECT * FROM users WHERE id IN (:id1, :id2, :id3)", {"id1": 1, "id2": 2, "id3": 3}),
        ]

        for name, query, params in test_cases:
            sql = SQL(query, params)

            def convert_params(stmt: "SQL" = sql) -> None:
                stmt.compile(placeholder_style=target_style.value)

            times = BenchmarkMetrics.time_operation(
                convert_params, iterations=self.config.iterations, warmup=self.config.warmup_iterations
            )
            results[name] = TimingResult(operation=name, iterations=self.config.iterations, times=times)
        return results

    def _benchmark_typed_parameters(self) -> dict[str, TimingResult]:
        """Benchmark TypedParameter wrapping and access overhead."""
        results = {}
        test_cases = [
            ("int_param", 42),
            ("float_param", math.pi),
            ("str_param", "Hello, World!"),
            ("large_str", "x" * 1000),
            ("large_list", list(range(100))),
        ]

        for name, value in test_cases:
            # Benchmark wrapping
            def wrap_param(val: Any = value) -> None:
                TypedParameter(val, sqlglot_type="str", type_hint="str")

            times = BenchmarkMetrics.time_operation(
                wrap_param, iterations=self.config.iterations, warmup=self.config.warmup_iterations
            )
            results[f"wrap_{name}"] = TimingResult(
                operation=f"wrap_{name}", iterations=self.config.iterations, times=times
            )

            # Benchmark access
            param = TypedParameter(value, sqlglot_type="str", type_hint="str")

            def access_param(p: "TypedParameter" = param) -> None:
                _ = p.value

            times = BenchmarkMetrics.time_operation(
                access_param, iterations=self.config.iterations, warmup=self.config.warmup_iterations
            )
            results[f"access_{name}"] = TimingResult(
                operation=f"access_{name}", iterations=self.config.iterations, times=times
            )
        return results
