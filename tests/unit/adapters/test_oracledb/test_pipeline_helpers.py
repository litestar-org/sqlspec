# pyright: reportCallIssue=false
from collections.abc import Callable
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from sqlspec import StatementStack
from sqlspec.adapters.oracledb._typing import OracleAsyncConnection, OracleSyncConnection
from sqlspec.adapters.oracledb.core import (
    build_pipeline_stack_result,
    default_statement_config,
    normalize_execute_many_parameters_async,
    normalize_execute_many_parameters_sync,
)
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.data_dictionary import VersionInfo
from sqlspec.driver import StackExecutionObserver

pytest.importorskip("oracledb")


class _StubAsyncConnection:
    """Minimal async connection stub for OracleAsyncDriver tests."""

    def __init__(self) -> None:
        self.in_transaction = False


class _PipelineConnection:
    """Connection stub exposing the python-oracledb pipeline API."""

    def __init__(self, *, thin: bool = True) -> None:
        self.in_transaction = False
        self.thin = thin

    def run_pipeline(self, *_args: object, **_kwargs: object) -> list[object]:
        return []


class _AsyncPipelineConnection:
    """Async connection stub exposing the python-oracledb async pipeline API."""

    def __init__(self, *, thin: bool = True) -> None:
        self.in_transaction = False
        self.thin = thin

    async def run_pipeline(self, *_args: object, **_kwargs: object) -> list[object]:
        return []


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


pytest.importorskip("oracledb")


def _make_sync_driver() -> OracleSyncDriver:
    connection_mock = MagicMock()
    connection_mock.in_transaction = False
    return OracleSyncDriver(connection=cast("OracleSyncConnection", connection_mock))


def _make_sync_pipeline_driver() -> OracleSyncDriver:
    return OracleSyncDriver(
        connection=cast("OracleSyncConnection", _PipelineConnection()),
        statement_config=default_statement_config,
        driver_features={},
    )


def _make_async_pipeline_driver(*, thin: bool = True) -> OracleAsyncDriver:
    return OracleAsyncDriver(
        connection=cast("OracleAsyncConnection", _AsyncPipelineConnection(thin=thin)),
        statement_config=default_statement_config,
        driver_features={},
    )


def test_sync_pipeline_gate_requires_async_thin_26ai(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sync Oracle connections must not claim native pipeline support."""
    driver = _make_sync_pipeline_driver()
    monkeypatch.setattr(OracleSyncDriver, "_detect_oracledb_version", lambda _self: (4, 1, 0))
    monkeypatch.setattr(OracleSyncDriver, "_detect_oracle_version", lambda _self: VersionInfo(26, 0, 0))

    assert driver._pipeline_native_supported() is False
    assert driver._pipeline_support_reason == "asyncio_thin_required"


async def test_async_pipeline_gate_requires_thin_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async Oracle pipeline support is Thin-mode only."""
    driver = _make_async_pipeline_driver(thin=False)

    async def detect_oracle_version(_self: OracleAsyncDriver) -> VersionInfo:
        return VersionInfo(26, 0, 0)

    monkeypatch.setattr(OracleAsyncDriver, "_detect_oracledb_version", lambda _self: (4, 1, 0))
    monkeypatch.setattr(OracleAsyncDriver, "_detect_oracle_version", detect_oracle_version)

    assert await driver._pipeline_native_supported() is False
    assert driver._pipeline_support_reason == "thin_mode_required"


async def test_async_pipeline_gate_requires_26ai(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async Thin pipelines on pre-26ai databases do not reduce round trips."""
    driver = _make_async_pipeline_driver(thin=True)

    async def detect_oracle_version(_self: OracleAsyncDriver) -> VersionInfo:
        return VersionInfo(23, 0, 0)

    monkeypatch.setattr(OracleAsyncDriver, "_detect_oracledb_version", lambda _self: (4, 1, 0))
    monkeypatch.setattr(OracleAsyncDriver, "_detect_oracle_version", detect_oracle_version)

    assert await driver._pipeline_native_supported() is False
    assert driver._pipeline_support_reason == "database_version"


async def test_async_pipeline_gate_accepts_thin_26ai(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async Thin connections on Oracle 26ai can use native pipeline execution."""
    driver = _make_async_pipeline_driver(thin=True)

    async def detect_oracle_version(_self: OracleAsyncDriver) -> VersionInfo:
        return VersionInfo(26, 0, 0)

    monkeypatch.setattr(OracleAsyncDriver, "_detect_oracledb_version", lambda _self: (4, 1, 0))
    monkeypatch.setattr(OracleAsyncDriver, "_detect_oracle_version", detect_oracle_version)

    assert await driver._pipeline_native_supported() is True
    assert driver._pipeline_support_reason is None


def test_dispatch_execute_force_select_sync_dispatch_execute_force_select_fallback() -> None:
    """The sync driver must fetch rows when cursor metadata proves rows exist."""
    driver = _make_sync_driver()
    statement = MagicMock()
    statement.returns_rows.return_value = False
    statement.operation_type = "COMMAND"
    cursor = MagicMock()
    cursor.description = [("id", None, None, None, None, None, None)]
    cursor.fetchall.return_value = [(42,)]
    del cursor.statement_type
    with (
        patch.object(OracleSyncDriver, "_compiled_sql", return_value=("SELECT 1 FROM DUAL", {})),
        patch("sqlspec.adapters.oracledb.driver.coerce_large_parameters_sync", return_value={}),
        patch.object(OracleSyncDriver, "_resolve_row_metadata", return_value=(["id"], False)),
        patch("sqlspec.adapters.oracledb.driver.collect_sync_rows", return_value=([(42,)], ["id"])),
    ):
        result = driver.dispatch_execute(cursor, statement)
    assert result.is_select_result is True
    assert result.data_row_count == 1
    cursor.fetchall.assert_called_once()


def test_dispatch_execute_force_select_sync_dispatch_execute_no_force_select_when_returns_rows_false_and_no_description() -> (
    None
):
    """The sync driver must keep DML behavior when no row metadata exists."""
    driver = _make_sync_driver()
    statement = MagicMock()
    statement.returns_rows.return_value = False
    statement.operation_type = "COMMAND"
    cursor = MagicMock()
    cursor.description = None
    cursor.rowcount = 3
    del cursor.statement_type
    with (
        patch.object(OracleSyncDriver, "_compiled_sql", return_value=("DELETE FROM t", {})),
        patch("sqlspec.adapters.oracledb.driver.coerce_large_parameters_sync", return_value={}),
        patch("sqlspec.adapters.oracledb.driver.resolve_rowcount", return_value=3),
    ):
        result = driver.dispatch_execute(cursor, statement)
    assert result.is_select_result is False
    assert result.rowcount_override == 3
    cursor.fetchall.assert_not_called()


def test_dispatch_execute_force_select_sync_and_async_dispatch_execute_both_call_should_force_select() -> None:
    """Both Oracle dispatch implementations should use the force-select fallback."""
    import inspect

    from sqlspec.adapters.oracledb.driver import OracleAsyncDriver

    sync_src = inspect.getsource(OracleSyncDriver.dispatch_execute)
    async_src = inspect.getsource(OracleAsyncDriver.dispatch_execute)
    assert "_should_force_select" in sync_src
    assert "_should_force_select" in async_src
    assert "is_select_like" in sync_src


def test_normalize_execute_many_normalize_execute_many_parameters_sync_tuple_to_list() -> None:
    parameters = ({"x": 1}, {"x": 2})
    result = normalize_execute_many_parameters_sync(parameters)
    assert result == [{"x": 1}, {"x": 2}]
    assert isinstance(result, list)


def test_normalize_execute_many_normalize_execute_many_parameters_async_tuple_to_list() -> None:
    parameters = ({"x": 1}, {"x": 2})
    result = normalize_execute_many_parameters_async(parameters)
    assert result == [{"x": 1}, {"x": 2}]
    assert isinstance(result, list)


@pytest.mark.parametrize(
    "normalizer", [normalize_execute_many_parameters_sync, normalize_execute_many_parameters_async]
)
def test_normalize_execute_many_normalize_execute_many_parameters_rejects_empty(
    normalizer: Callable[[object], object],
) -> None:
    with pytest.raises(ValueError, match="execute_many requires parameters"):
        normalizer([])
