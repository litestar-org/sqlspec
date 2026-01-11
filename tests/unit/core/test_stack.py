"""Tests for the StatementStack builder utilities."""

from typing import Any

import pytest

from sqlspec import ObservabilityRuntime, StackOperation, StatementConfig, StatementStack
from sqlspec.core.metrics import StackExecutionMetrics

pytestmark = pytest.mark.xdist_group("core")


def test_push_execute_is_immutable() -> None:
    stack = StatementStack()
    new_stack = stack.push_execute("SELECT 1 WHERE id = :id", {"id": 1})

    assert len(stack) == 0
    assert len(new_stack) == 1
    operation = new_stack.operations[0]
    assert operation.method == "execute"
    assert operation.statement == "SELECT 1 WHERE id = :id"
    assert operation.arguments == ({"id": 1},)
    assert stack is not new_stack


def test_push_execute_many_validates_payload() -> None:
    stack = StatementStack()
    with pytest.raises(TypeError, match="sequence of parameter sets"):
        stack.push_execute_many("INSERT", "invalid")
    with pytest.raises(ValueError, match="cannot be empty"):
        stack.push_execute_many("INSERT", [])


def test_push_execute_script_requires_non_empty_sql() -> None:
    stack = StatementStack()
    with pytest.raises(ValueError, match="non-empty SQL"):
        stack.push_execute_script("   ")


def test_push_execute_many_stores_filters_and_kwargs() -> None:
    stack = StatementStack().push_execute_many(
        "INSERT", [{"x": 1}], {"filter": True}, statement_config=None, chunk_size=50
    )
    operation = stack.operations[0]
    assert operation.method == "execute_many"
    arguments = operation.arguments
    assert len(arguments) >= 2
    assert arguments[0] == ({"x": 1},)
    assert arguments[1] == {"filter": True}
    assert operation.keyword_arguments is not None
    assert operation.keyword_arguments["chunk_size"] == 50


def test_extend_and_from_operations() -> None:
    base = StatementStack().push_execute("SELECT 1")
    duplicate = StatementStack.from_operations(base.operations)
    merged = base.extend(duplicate)

    assert len(duplicate) == 1
    assert len(merged) == 2
    assert all(isinstance(op, StackOperation) for op in merged)


def test_reject_nested_stack() -> None:
    stack = StatementStack()
    with pytest.raises(TypeError, match="Nested StatementStack"):
        stack.push_execute(stack)  # type: ignore[arg-type]


def test_freeze_kwargs_includes_statement_config() -> None:
    config = StatementConfig()
    stack = StatementStack().push_execute("SELECT 1", statement_config=config)
    operation = stack.operations[0]
    assert operation.keyword_arguments is not None
    assert operation.keyword_arguments["statement_config"] is config


@pytest.mark.parametrize("statement", ["SELECT 1", object()])
def test_validate_statement_allows_non_strings(statement: Any) -> None:
    stack = StatementStack().push_execute(statement)
    assert stack.operations[0].statement is statement


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


def test_push_execute_arrow_records_kwargs() -> None:
    stack = StatementStack().push_execute_arrow(
        "SELECT * FROM items", {"limit": 10}, return_format="batch", native_only=True
    )
    operation = stack.operations[0]
    assert operation.method == "execute_arrow"
    arguments = operation.arguments
    assert arguments
    assert arguments[0] == {"limit": 10}
    assert operation.keyword_arguments is not None
    assert operation.keyword_arguments["return_format"] == "batch"
    assert operation.keyword_arguments["native_only"] is True
