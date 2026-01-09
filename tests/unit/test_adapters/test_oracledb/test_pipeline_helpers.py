from typing import Any, cast

import pytest

# pyright: reportPrivateUsage=false


pytest.importorskip("oracledb")

from sqlspec import StatementStack
from sqlspec.adapters.oracledb import OracleAsyncConnection
from sqlspec.adapters.oracledb.core import build_pipeline_stack_result, default_statement_config
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver
from sqlspec.driver import StackExecutionObserver


class _StubAsyncConnection:
    """Minimal async connection stub for OracleAsyncDriver tests."""

    def __init__(self) -> None:
        self.in_transaction = False


class _StubPipelineResult:
    """Pipeline result stub for driver helper tests."""

    def __init__(
        self,
        *,
        rows: list[tuple[Any, ...]] | None = None,
        columns: list[Any] | None = None,
        warning: Any | None = None,
        error: Exception | None = None,
        rowcount: int | None = None,
    ) -> None:
        self.rows = rows
        self.columns = columns
        self.warning = warning
        self.error = error
        self.rowcount = rowcount
        self.return_value = None


class _StubObserver:
    """Observer stub capturing recorded errors."""

    def __init__(self) -> None:
        self.errors: list[Exception] = []

    def record_operation_error(self, error: Exception) -> None:
        self.errors.append(error)


class _StubColumn:
    """Simple column metadata stub."""

    def __init__(self, name: str) -> None:
        self.name = name


def _make_driver() -> OracleAsyncDriver:
    connection = cast("OracleAsyncConnection", _StubAsyncConnection())
    return OracleAsyncDriver(connection=connection, statement_config=default_statement_config, driver_features={})


def test_stack_native_blocker_detects_arrow() -> None:
    driver = _make_driver()
    stack = StatementStack().push_execute_arrow("SELECT * FROM dual")
    assert driver._stack_native_blocker(stack) == "arrow_operation"


def test_stack_native_blocker_detects_script() -> None:
    driver = _make_driver()
    stack = StatementStack().push_execute_script("BEGIN NULL; END;")
    assert driver._stack_native_blocker(stack) == "script_operation"


def test_stack_native_blocker_allows_standard_operations() -> None:
    driver = _make_driver()
    stack = StatementStack().push_execute("SELECT 1 FROM dual")
    assert driver._stack_native_blocker(stack) is None


def test_pipeline_result_to_stack_result_uses_rowcount_attr() -> None:
    driver = _make_driver()
    stack = StatementStack().push_execute("SELECT 1 FROM dual")
    compiled = driver._prepare_pipeline_operation(stack.operations[0])
    pipeline_result = _StubPipelineResult(rows=[(1,)], columns=[_StubColumn("VALUE")], warning="warn", rowcount=7)

    stack_result = build_pipeline_stack_result(
        compiled.statement,
        compiled.method,
        compiled.returns_rows,
        compiled.parameters,
        pipeline_result,
        driver.driver_features,
    )

    assert stack_result.rows_affected == 7
    assert stack_result.warning == "warn"
    result = stack_result.result
    assert result is not None
    assert result.metadata is not None
    assert result.metadata["pipeline_operation"] == "execute"


def test_pipeline_result_execute_many_rowcount_fallback() -> None:
    driver = _make_driver()
    stack = StatementStack().push_execute_many("INSERT INTO demo VALUES (:1)", [(1,), (2,)])
    compiled = driver._prepare_pipeline_operation(stack.operations[0])
    pipeline_result = _StubPipelineResult()

    stack_result = build_pipeline_stack_result(
        compiled.statement,
        compiled.method,
        compiled.returns_rows,
        compiled.parameters,
        pipeline_result,
        driver.driver_features,
    )

    assert stack_result.rows_affected == 2


def test_build_stack_results_records_errors() -> None:
    driver = _make_driver()
    stack = StatementStack().push_execute("SELECT 1 FROM dual")
    compiled = driver._prepare_pipeline_operation(stack.operations[0])
    observer_stub = _StubObserver()
    observer = cast(StackExecutionObserver, observer_stub)

    results = driver._build_stack_results_from_pipeline(
        (compiled,), (_StubPipelineResult(error=RuntimeError("boom")),), True, observer
    )

    assert results[0].error is not None
    assert len(observer_stub.errors) == 1
