"""Metrics collection and system information."""

import gc
import platform
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import psutil


@dataclass
class TimingResult:
    """Result of a timing operation."""

    operation: str
    iterations: int
    times: list[float]
    min_ms: float = field(init=False)
    max_ms: float = field(init=False)
    avg_ms: float = field(init=False)
    std_ms: float = field(init=False)
    ops_per_sec: float = field(init=False)

    def __post_init__(self) -> None:
        """Calculate derived metrics."""
        self.min_ms = min(self.times) * 1000
        self.max_ms = max(self.times) * 1000
        self.avg_ms = sum(self.times) / len(self.times) * 1000

        # Calculate standard deviation
        if len(self.times) > 1:
            mean = sum(self.times) / len(self.times)
            variance = sum((t - mean) ** 2 for t in self.times) / len(self.times)
            self.std_ms = (variance**0.5) * 1000
        else:
            self.std_ms = 0.0

        # Operations per second based on average time
        self.ops_per_sec = 1000 / self.avg_ms if self.avg_ms > 0 else 0


@dataclass
class SystemInfo:
    """System information for benchmark context."""

    platform: str = field(default_factory=lambda: platform.platform())
    python_version: str = field(default_factory=lambda: platform.python_version())
    cpu_model: str = field(default_factory=lambda: platform.processor() or "Unknown")
    cpu_count: int = field(default_factory=lambda: psutil.cpu_count())
    memory_gb: float = field(default_factory=lambda: psutil.virtual_memory().total / (1024**3))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "platform": self.platform,
            "python_version": self.python_version,
            "cpu_model": self.cpu_model,
            "cpu_count": self.cpu_count,
            "memory_gb": round(self.memory_gb, 2),
        }


class BenchmarkMetrics:
    """Utilities for measuring performance."""

    @staticmethod
    def time_operation(
        func: Callable[[], Any], iterations: int, warmup: int = 10, gc_enabled: bool = False
    ) -> list[float]:
        """Time an operation multiple times.

        Args:
            func: Function to time
            iterations: Number of iterations to run
            warmup: Number of warmup iterations
            gc_enabled: Whether to enable garbage collection during timing

        Returns:
            List of times in seconds for each iteration
        """
        # Warmup
        for _ in range(warmup):
            func()

        # Disable GC during timing if requested
        gc_state = gc.isenabled()
        if not gc_enabled:
            gc.disable()

        try:
            times = []
            for _ in range(iterations):
                start = time.perf_counter()
                func()
                end = time.perf_counter()
                times.append(end - start)

            return times
        finally:
            # Restore GC state
            if gc_state and not gc_enabled:
                gc.enable()

    @staticmethod
    def measure_memory(func: Callable[[], Any]) -> tuple[Any, float]:
        """Measure memory usage of a function.

        Args:
            func: Function to measure

        Returns:
            Tuple of (result, memory_mb)
        """
        # Force garbage collection before measurement
        gc.collect()

        # Get initial memory usage
        process = psutil.Process()
        initial_memory = process.memory_info().rss

        # Run function
        result = func()

        # Get final memory usage
        final_memory = process.memory_info().rss
        memory_mb = (final_memory - initial_memory) / (1024 * 1024)

        return result, memory_mb

    @staticmethod
    def get_sqlglot_info() -> dict[str, Any]:
        """Get SQLGlot version and features."""
        import sqlglot

        info = {"version": sqlglot.__version__, "has_rust": False}

        # Check for rust extension
        try:
            import sqlglot.rs  # type: ignore

            info["has_rust"] = True
        except ImportError:
            pass

        return info

    @staticmethod
    async def time_operation_async(
        func: Callable[[], Any], iterations: int, warmup: int = 10, gc_enabled: bool = False
    ) -> list[float]:
        """Time an async operation multiple times.

        Args:
            func: Async function to time
            iterations: Number of iterations to run
            warmup: Number of warmup iterations
            gc_enabled: Whether to enable garbage collection during timing

        Returns:
            List of times in seconds for each iteration
        """
        # Warmup
        for _ in range(warmup):
            await func()

        # Disable GC during timing if requested
        gc_state = gc.isenabled()
        if not gc_enabled:
            gc.disable()

        try:
            times = []
            for _ in range(iterations):
                start = time.perf_counter()
                await func()
                end = time.perf_counter()
                times.append(end - start)

            return times
        finally:
            # Restore GC state
            if gc_state and not gc_enabled:
                gc.enable()

    @staticmethod
    def get_sqlspec_info() -> dict[str, Any]:
        """Get SQLSpec version and configuration."""
        import sqlspec

        return {"version": sqlspec.__version__}
