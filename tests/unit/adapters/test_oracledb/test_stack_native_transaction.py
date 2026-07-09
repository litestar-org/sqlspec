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
    def __init__(self, error: "Exception | None" = None) -> None:
        self.error = error


class _FakePipelineConnection:
    def __init__(self, results: "list[_FakeOpResult]") -> None:
        self._results = results
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
