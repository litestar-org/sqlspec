"""Telemetry helper objects for stack execution."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from sqlspec.observability import ObservabilityRuntime

__all__ = ("StackExecutionMetrics",)


class StackExecutionMetrics:
    """Capture telemetry facts about a stack execution."""

    __slots__ = (
        "adapter",
        "continue_on_error",
        "duration_s",
        "error_count",
        "error_type",
        "forced_disable",
        "native_pipeline",
        "statement_count",
    )

    def __init__(
        self,
        adapter: str,
        statement_count: int,
        *,
        continue_on_error: bool,
        native_pipeline: bool,
        forced_disable: bool,
    ) -> None:
        self.adapter = adapter
        self.statement_count = statement_count
        self.continue_on_error = continue_on_error
        self.native_pipeline = native_pipeline
        self.forced_disable = forced_disable
        self.duration_s = 0.0
        self.error_type: str | None = None
        self.error_count = 0

    def record_duration(self, duration: float) -> None:
        """Record execution duration in seconds."""

        self.duration_s = duration

    def record_operation_error(self, error: Exception) -> None:
        """Record an operation error when continue-on-error is enabled."""

        self.error_count += 1
        if not self.continue_on_error and self.error_type is None:
            self.error_type = type(error).__name__

    def record_error(self, error: Exception) -> None:
        """Record a terminal error."""

        self.error_type = type(error).__name__
        self.error_count = max(self.error_count, 1)

    def emit(self, runtime: "ObservabilityRuntime") -> None:
        """Emit collected metrics to the configured runtime."""

        runtime.increment_metric("stack.execute.invocations")
        runtime.increment_metric("stack.execute.statements", float(self.statement_count))

        mode = "continue" if self.continue_on_error else "failfast"
        runtime.increment_metric(f"stack.execute.mode.{mode}")

        pipeline_label = "native" if self.native_pipeline else "sequential"
        runtime.increment_metric(f"stack.execute.path.{pipeline_label}")

        if self.forced_disable:
            runtime.increment_metric("stack.execute.override.forced")

        runtime.increment_metric("stack.execute.duration_ms", self.duration_s * 1000.0)

        if self.error_type is not None:
            runtime.increment_metric("stack.execute.errors")
            runtime.increment_metric(f"stack.execute.errors.{self.error_type}")

        if self.error_count and self.continue_on_error:
            runtime.increment_metric("stack.execute.partial_errors", float(self.error_count))
