"""Benchmark runner orchestration."""

import uuid
from typing import Any, Optional

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn

from tools.benchmark.config import BenchmarkConfig
from tools.benchmark.core.metrics import BenchmarkMetrics, SystemInfo
from tools.benchmark.core.storage import BenchmarkStorage


class BenchmarkRunner:
    """Orchestrates benchmark execution."""

    def __init__(self, config: BenchmarkConfig, console: Optional[Console] = None) -> None:
        self.config = config
        self.console = console or Console()
        self.storage = BenchmarkStorage(config)
        self.metrics = BenchmarkMetrics()
        self.system_info = SystemInfo()
        self.current_run_id: Optional[str] = None

    def start_run(
        self,
        benchmark_type: str,
        adapter: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """Start a new benchmark run."""
        self.current_run_id = str(uuid.uuid4())

        self.storage.save_run(
            run_id=self.current_run_id,
            benchmark_type=benchmark_type,
            adapter=adapter,
            iterations=self.config.iterations,
            system_info=self.system_info,
            metadata=metadata,
        )

        return self.current_run_id

    def get_progress(self, total: int, description: str = "Running benchmarks") -> Progress:
        """Create a progress bar for benchmark execution."""
        if not self.config.show_progress:
            return Progress(disable=True)

        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=self.console,
        )

    def cleanup_old_data(self) -> None:
        """Clean up old benchmark data based on retention policy."""
        if self.config.results_retention_days > 0:
            deleted = self.storage.cleanup_old_data(self.config.results_retention_days)
            if deleted > 0 and self.config.verbose:
                self.console.print(
                    f"[dim]Cleaned up {deleted} old benchmark runs "
                    f"(older than {self.config.results_retention_days} days)[/dim]"
                )
