"""Profiling helpers for internal SQLSpec tooling.

Provides a low-overhead profiler using ``sys.setprofile`` to capture call counts
and durations in critical execution paths.
"""

import sys
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import FrameType, TracebackType

__all__ = ("CallStats", "HotPathProfiler", "profile_hotpath")


@dataclass
class CallStats:
    """Statistics for a single function call."""

    count: int = 0
    total_time: float = 0.0
    min_time: float = float("inf")
    max_time: float = 0.0

    def update(self, duration: float) -> None:
        """Update statistics with a new call duration.

        Args:
            duration: Time taken for the call in seconds.
        """
        self.count += 1
        self.total_time += duration
        self.min_time = min(self.min_time, duration)
        self.max_time = max(self.max_time, duration)


@dataclass
class HotPathProfiler:
    """Low-overhead profiler using ``sys.setprofile``."""

    stats: dict[str, CallStats] = field(default_factory=dict)
    _stack: list[tuple[str, float]] = field(default_factory=list)
    _start_time: float = 0.0
    _enabled: bool = False

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: "TracebackType | None",
    ) -> None:
        self.stop()

    def start(self) -> None:
        """Start the profiler."""
        self._enabled = True
        self._start_time = time.perf_counter()
        sys.setprofile(self._profile_callback)

    def stop(self) -> None:
        """Stop the profiler."""
        sys.setprofile(None)
        self._enabled = False

    def _profile_callback(self, frame: "FrameType", event: str, arg: Any) -> None:
        """Callback for ``sys.setprofile``."""
        if not self._enabled:
            return

        now = time.perf_counter()

        if event in ("call", "c_call"):
            code = frame.f_code
            func_name = f"{code.co_filename}:{code.co_firstlineno}({code.co_name})" if event == "call" else str(arg)
            self._stack.append((func_name, now))
        elif event in ("return", "c_return", "c_exception") and self._stack:
            func_name, start_time = self._stack.pop()
            duration = now - start_time

            if func_name not in self.stats:
                self.stats[func_name] = CallStats()
            self.stats[func_name].update(duration)

    def print_report(self, limit: int = 20, sort_by: str = "count") -> None:
        """Print a formatted report of collected statistics."""
        from rich.console import Console
        from rich.table import Table

        console = Console()
        table = Table(title="Hot Path Profile Report")
        table.add_column("Function", style="cyan", no_wrap=False)
        table.add_column("Calls", justify="right", style="yellow")
        table.add_column("Total Time (ms)", justify="right", style="green")
        table.add_column("% Time", justify="right", style="red")
        table.add_column("Avg (us)", justify="right", style="magenta")
        table.add_column("Min (us)", justify="right", style="dim")
        table.add_column("Max (us)", justify="right", style="dim")

        items = list(self.stats.items())
        if sort_by == "count":
            items.sort(key=lambda item: item[1].count, reverse=True)
        else:
            items.sort(key=lambda item: item[1].total_time, reverse=True)

        total_captured_time = sum(stat.total_time for stat in self.stats.values())

        for name, stat in items[:limit]:
            avg_us = (stat.total_time / stat.count * 1_000_000) if stat.count > 0 else 0
            pct_time = (stat.total_time / total_captured_time * 100) if total_captured_time > 0 else 0
            table.add_row(
                name,
                str(stat.count),
                f"{stat.total_time * 1000:.3f}",
                f"{pct_time:.1f}%",
                f"{avg_us:.2f}",
                f"{stat.min_time * 1_000_000:.2f}",
                f"{stat.max_time * 1_000_000:.2f}",
            )

        console.print(table)


def profile_hotpath(limit: int = 20, sort_by: str = "count") -> "Callable[..., Any]":
    """Decorator to profile a function hot path."""

    def decorator(func: "Callable[..., Any]") -> "Callable[..., Any]":
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with HotPathProfiler() as profiler:
                result = func(*args, **kwargs)
            profiler.print_report(limit=limit, sort_by=sort_by)
            return result

        return wrapper

    return decorator
