"""Base class for benchmark suites."""

from abc import ABC, abstractmethod
from typing import Any, Optional

from rich.console import Console
from rich.table import Table

from tools.benchmark.config import BenchmarkConfig
from tools.benchmark.core.metrics import TimingResult
from tools.benchmark.core.runner import BenchmarkRunner


class BaseBenchmarkSuite(ABC):
    """Abstract base class for benchmark suites."""

    def __init__(self, config: BenchmarkConfig, runner: BenchmarkRunner, console: Optional[Console] = None) -> None:
        self.config = config
        self.runner = runner
        self.console = console or Console()

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the benchmark suite."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """Description of what this suite benchmarks."""
        ...

    @abstractmethod
    def run(self, adapter: str = "all", **kwargs: Any) -> dict[str, TimingResult]:
        """Run the benchmark suite.

        Args:
            adapter: Adapter to test or 'all'
            **kwargs: Additional arguments for the suite

        Returns:
            Dictionary mapping operation names to timing results
        """
        ...

    def display_results(self, results: dict[str, TimingResult]) -> None:
        """Display benchmark results in a table."""
        table = Table(title=f"{self.name} Results", show_header=True)
        table.add_column("Operation", style="cyan")
        table.add_column("Min (ms)", justify="right")
        table.add_column("Avg (ms)", justify="right")
        table.add_column("Max (ms)", justify="right")
        table.add_column("Std Dev", justify="right")
        table.add_column("Ops/sec", justify="right", style="green")

        for operation, result in results.items():
            table.add_row(
                operation,
                f"{result.min_ms:.3f}",
                f"{result.avg_ms:.3f}",
                f"{result.max_ms:.3f}",
                f"{result.std_ms:.3f}",
                f"{result.ops_per_sec:.1f}",
            )

        self.console.print(table)

    def check_regression(self, operation: str, current: TimingResult, adapter: str) -> Optional[str]:
        """Check if current result shows regression compared to baseline."""
        baseline = self.runner.storage.get_comparison_baseline(
            benchmark_type=self.name, adapter=adapter, operation=operation, days=self.config.comparison_days
        )

        if not baseline:
            return None

        # Calculate percentage change
        pct_change = ((current.avg_ms - baseline["avg_ms"]) / baseline["avg_ms"]) * 100

        if pct_change > self.config.regression_threshold * 100:
            return f"[red]↑ {pct_change:.1f}% regression vs {self.config.comparison_days}-day avg[/red]"
        if pct_change < -self.config.regression_threshold * 100:
            return f"[green]↓ {-pct_change:.1f}% improvement vs {self.config.comparison_days}-day avg[/green]"

        return None

    def get_regression_info(self, operation: str, current: TimingResult, adapter: str) -> Optional[tuple[str, float]]:
        """Get regression information as tuple for summary purposes."""
        baseline = self.runner.storage.get_comparison_baseline(
            benchmark_type=self.name, adapter=adapter, operation=operation, days=self.config.comparison_days
        )

        if not baseline:
            return None

        # Calculate percentage change
        pct_change = ((current.avg_ms - baseline["avg_ms"]) / baseline["avg_ms"]) * 100

        if abs(pct_change) > self.config.regression_threshold * 100:
            direction = "regression" if pct_change > 0 else "improvement"
            info = f"{abs(pct_change):.1f}% {direction} vs {self.config.comparison_days}-day avg"
            return (info, pct_change)

        return None
