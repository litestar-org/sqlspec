from sqlspec.core.metrics import StackExecutionMetrics
from sqlspec.observability import ObservabilityRuntime


def test_stack_execution_metrics_emit() -> None:
    runtime = ObservabilityRuntime(config_name="TestDriver")
    metrics = StackExecutionMetrics(
        adapter="OracleAsyncDriver",
        statement_count=3,
        continue_on_error=False,
        native_pipeline=False,
        forced_disable=False,
    )
    metrics.record_duration(0.25)
    metrics.emit(runtime)

    snapshot = runtime.metrics_snapshot()
    assert snapshot["TestDriver.stack.execute.invocations"] == 1.0
    assert snapshot["TestDriver.stack.execute.statements"] == 3.0
    assert snapshot["TestDriver.stack.execute.mode.failfast"] == 1.0
    assert snapshot["TestDriver.stack.execute.path.sequential"] == 1.0
    assert snapshot["TestDriver.stack.execute.duration_ms"] == 250.0


def test_stack_execution_metrics_partial_errors() -> None:
    runtime = ObservabilityRuntime(config_name="TestDriver")
    metrics = StackExecutionMetrics(
        adapter="OracleAsyncDriver",
        statement_count=2,
        continue_on_error=True,
        native_pipeline=True,
        forced_disable=True,
    )
    metrics.record_operation_error(RuntimeError("boom"))
    metrics.record_duration(0.1)
    metrics.emit(runtime)

    snapshot = runtime.metrics_snapshot()
    assert snapshot["TestDriver.stack.execute.mode.continue"] == 1.0
    assert snapshot["TestDriver.stack.execute.path.native"] == 1.0
    assert snapshot["TestDriver.stack.execute.override.forced"] == 1.0
    assert snapshot["TestDriver.stack.execute.partial_errors"] == 1.0
