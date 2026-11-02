"""Oracle native pipeline stack commits continue-on-error successes.

The async native pipeline path only runs on new-enough Oracle databases, so
these tests drive ``_execute_stack_native`` directly with a fake pipeline
connection to guard the transaction-boundary decision regardless of gate state.
"""

from typing import Any, cast

import pytest

from sqlspec import StatementStack
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver


class _FakeOpResult:
    def __init__(self, error: "Any | None" = None) -> None:
        self.error = error


class _FakePipelineConnection:
    def __init__(self, results: "list[_FakeOpResult]", transaction_in_progress: bool = False) -> None:
        self._results = results
        self.transaction_in_progress = transaction_in_progress
        self.commit_count = 0
        self.rollback_count = 0
        self.begin_count = 0

    async def run_pipeline(self, pipeline: Any, continue_on_error: bool) -> "list[_FakeOpResult]":
        return self._results

    async def commit(self) -> None:
        self.commit_count += 1

    async def rollback(self) -> None:
        self.rollback_count += 1

    async def begin(self) -> None:
        self.begin_count += 1


def _stack() -> StatementStack:
    return (
        StatementStack()
        .push_execute("INSERT INTO t (id) VALUES (:1)", (1,))
        .push_execute("INSERT INTO t (id) VALUES (:1)", (1,))
        .push_execute("INSERT INTO t (id) VALUES (:1)", (2,))
    )


@pytest.mark.anyio
async def test_native_stack_continue_on_error_commits_successes() -> None:
    """Continue-on-error execution must commit when the stack owns the transaction."""
    results = [_FakeOpResult(), _FakeOpResult(ValueError("duplicate")), _FakeOpResult()]
    connection = _FakePipelineConnection(results)
    driver = OracleAsyncDriver(cast("Any", connection))

    stack_results = await driver._execute_stack_native(_stack(), continue_on_error=True)

    assert len(stack_results) == 3
    assert stack_results[0].error is None
    assert stack_results[1].error is not None
    assert stack_results[2].error is None
    assert connection.commit_count == 1
    assert connection.rollback_count == 0


@pytest.mark.anyio
async def test_native_stack_fail_fast_reports_operation_index() -> None:
    """Fail-fast execution must raise StackExecutionError carrying the failing op index."""
    from sqlspec.exceptions import StackExecutionError

    results = [_FakeOpResult(), _FakeOpResult(ValueError("duplicate")), _FakeOpResult()]
    connection = _FakePipelineConnection(results)
    driver = OracleAsyncDriver(cast("Any", connection))

    with pytest.raises(StackExecutionError) as excinfo:
        await driver._execute_stack_native(_stack(), continue_on_error=False)

    assert excinfo.value.operation_index == 1
    assert connection.rollback_count == 1
    assert connection.commit_count == 0


@pytest.mark.anyio
async def test_native_stack_inside_user_transaction_does_not_commit() -> None:
    """When already inside a user transaction the stack must not commit for the user."""
    results = [_FakeOpResult(), _FakeOpResult(ValueError("duplicate")), _FakeOpResult()]
    connection = _FakePipelineConnection(results)
    driver = OracleAsyncDriver(cast("Any", connection))
    await driver.begin()
    connection.commit_count = 0

    await driver._execute_stack_native(_stack(), continue_on_error=True)

    assert connection.commit_count == 0
    assert connection.rollback_count == 0


@pytest.mark.anyio
async def test_native_stack_inside_an_implicit_transaction_does_not_commit() -> None:
    """DML issued before the stack opens a transaction the stack does not own."""
    results = [_FakeOpResult(), _FakeOpResult()]
    connection = _FakePipelineConnection(results, transaction_in_progress=True)
    driver = OracleAsyncDriver(cast("Any", connection))

    await driver._execute_stack_native(_stack(), continue_on_error=True)

    assert connection.commit_count == 0
    assert connection.rollback_count == 0


@pytest.mark.anyio
async def test_native_stack_wraps_mapped_error() -> None:
    """Stack execution errors wrap mapped SQLSpec exceptions, not raw driver errors."""
    import oracledb

    from sqlspec.exceptions import SQLSpecError, StackExecutionError

    native_err = oracledb.DatabaseError("ORA-00001: unique constraint violated")
    results = [_FakeOpResult(native_err)]
    connection = _FakePipelineConnection(results)
    driver = OracleAsyncDriver(cast("Any", connection))

    stack_results = await driver._execute_stack_native(_stack(), continue_on_error=True)
    assert len(stack_results) == 1
    error = stack_results[0].error
    assert isinstance(error, StackExecutionError)
    assert isinstance(error.original_error, SQLSpecError)


@pytest.mark.anyio
@pytest.mark.parametrize("continue_on_error", [False, True])
async def test_stack_maps_native_pipeline_error_records(continue_on_error: bool) -> None:
    import oracledb
    from oracledb.errors import _Error

    from sqlspec.exceptions import StackExecutionError, UniqueViolationError

    native_error = _Error("ORA-00001: unique constraint violated", code=1)
    connection = _FakePipelineConnection([_FakeOpResult(), _FakeOpResult(native_error), _FakeOpResult()])
    driver = OracleAsyncDriver(cast("Any", connection))
    driver._pipeline_support = True

    if continue_on_error:
        results = await driver.execute_stack(_stack(), continue_on_error=True)
        assert len(results) == 3
        assert results[0].error is None
        assert results[2].error is None
        error = results[1].error
        assert connection.commit_count == 1
        assert connection.rollback_count == 0
    else:
        with pytest.raises(StackExecutionError) as exc_info:
            await driver.execute_stack(_stack())
        error = exc_info.value
        assert connection.commit_count == 0
        assert connection.rollback_count == 1

    assert isinstance(error, StackExecutionError)
    assert error.operation_index == 1
    assert isinstance(error.original_error, UniqueViolationError)
    cause = error.original_error.__cause__
    assert isinstance(cause, oracledb.DatabaseError)
    assert cause.args[0] is native_error


def test_sync_stack_executes_and_commits_without_a_storage_capability_flag() -> None:
    from sqlspec.adapters.oracledb._typing import OracleSyncConnection
    from sqlspec.adapters.oracledb.driver import OracleSyncDriver

    class _Cursor:
        description = None
        rowcount = 1

        def __init__(self, connection: "_Connection") -> None:
            self.connection = connection

        def execute(self, statement: str, parameters: object = None, **kwargs: object) -> None:
            self.connection.executed.append((statement, parameters))

        def close(self) -> None:
            return None

    class _Connection:
        transaction_in_progress = False

        def __init__(self) -> None:
            self.executed: list[tuple[str, object]] = []
            self.commits = 0

        def cursor(self) -> _Cursor:
            return _Cursor(self)

        def commit(self) -> None:
            self.commits += 1

    connection = _Connection()
    driver = OracleSyncDriver(cast("OracleSyncConnection", connection))
    stack = (
        StatementStack()
        .push_execute("INSERT INTO items VALUES (:1)", (1,))
        .push_execute("INSERT INTO items VALUES (:1)", (2,))
    )
    results = driver.execute_stack(stack)
    assert [result.rows_affected for result in results] == [1, 1]
    assert connection.executed == [("INSERT INTO items VALUES (:1)", [1]), ("INSERT INTO items VALUES (:1)", [2])]
    assert connection.commits == 1
