"""Unit tests for the driver execute_stack implementations."""

import types

import pytest

from sqlspec import StatementStack
from sqlspec.exceptions import StackExecutionError
from tests.conftest import requires_interpreted


@requires_interpreted
async def test_async_execute_stack_fail_fast_rolls_back(aiosqlite_async_driver) -> None:
    await aiosqlite_async_driver.execute("CREATE TABLE t (id INTEGER)")
    original_execute = aiosqlite_async_driver.execute

    async def failing_execute(self, statement, *params, **kwargs):  # type: ignore[no-untyped-def]
        if isinstance(statement, str) and "FAIL" in statement:
            raise ValueError("boom")
        return await original_execute(statement, *params, **kwargs)

    aiosqlite_async_driver.execute = types.MethodType(failing_execute, aiosqlite_async_driver)

    stack = StatementStack().push_execute("INSERT INTO t (id) VALUES (1)").push_execute("FAIL SELECT 1")

    with pytest.raises(StackExecutionError) as excinfo:
        await aiosqlite_async_driver.execute_stack(stack)

    assert excinfo.value.operation_index == 1
    assert aiosqlite_async_driver.connection.in_transaction is False


@requires_interpreted
async def test_async_execute_stack_continue_on_error(aiosqlite_async_driver) -> None:
    await aiosqlite_async_driver.execute("CREATE TABLE t (id INTEGER)")
    original_execute = aiosqlite_async_driver.execute

    async def failing_execute(self, statement, *params, **kwargs):  # type: ignore[no-untyped-def]
        if isinstance(statement, str) and "FAIL" in statement:
            raise ValueError("boom")
        return await original_execute(statement, *params, **kwargs)

    aiosqlite_async_driver.execute = types.MethodType(failing_execute, aiosqlite_async_driver)

    stack = StatementStack().push_execute("INSERT INTO t (id) VALUES (1)").push_execute("FAIL SELECT 1")

    results = await aiosqlite_async_driver.execute_stack(stack, continue_on_error=True)

    assert len(results) == 2
    assert results[0].error is None
    assert isinstance(results[1].error, StackExecutionError)
    assert aiosqlite_async_driver.connection.in_transaction is False


@requires_interpreted
async def test_async_execute_stack_execute_arrow(aiosqlite_async_driver) -> None:
    sentinel = object()

    async def fake_select_to_arrow(self, statement, *params, **kwargs):  # type: ignore[no-untyped-def]
        return sentinel

    aiosqlite_async_driver.select_to_arrow = types.MethodType(fake_select_to_arrow, aiosqlite_async_driver)

    stack = StatementStack().push_execute_arrow("SELECT * FROM users")

    results = await aiosqlite_async_driver.execute_stack(stack)

    assert len(results) == 1
    assert results[0].result is sentinel


@requires_interpreted
def test_sync_execute_stack_fail_fast_rolls_back(sqlite_sync_driver) -> None:
    sqlite_sync_driver.execute("CREATE TABLE t (id INTEGER)")
    original_execute = sqlite_sync_driver.execute

    def failing_execute(self, statement, *params, **kwargs):  # type: ignore[no-untyped-def]
        if isinstance(statement, str) and "FAIL" in statement:
            raise ValueError("boom")
        return original_execute(statement, *params, **kwargs)

    sqlite_sync_driver.execute = types.MethodType(failing_execute, sqlite_sync_driver)

    stack = StatementStack().push_execute("INSERT INTO t (id) VALUES (1)").push_execute("FAIL SELECT 1")

    with pytest.raises(StackExecutionError) as excinfo:
        sqlite_sync_driver.execute_stack(stack)

    assert excinfo.value.operation_index == 1
    assert sqlite_sync_driver.connection.in_transaction is False


@requires_interpreted
def test_sync_execute_stack_continue_on_error(sqlite_sync_driver) -> None:
    sqlite_sync_driver.execute("CREATE TABLE t (id INTEGER)")
    original_execute = sqlite_sync_driver.execute

    def failing_execute(self, statement, *params, **kwargs):  # type: ignore[no-untyped-def]
        if isinstance(statement, str) and "FAIL" in statement:
            raise ValueError("boom")
        return original_execute(statement, *params, **kwargs)

    sqlite_sync_driver.execute = types.MethodType(failing_execute, sqlite_sync_driver)

    stack = StatementStack().push_execute("INSERT INTO t (id) VALUES (1)").push_execute("FAIL SELECT 1")

    results = sqlite_sync_driver.execute_stack(stack, continue_on_error=True)

    assert len(results) == 2
    assert results[0].error is None
    assert isinstance(results[1].error, StackExecutionError)
    assert sqlite_sync_driver.connection.in_transaction is False


@requires_interpreted
def test_sync_execute_stack_execute_arrow(sqlite_sync_driver) -> None:
    sentinel = object()

    def fake_select_to_arrow(self, statement, *params, **kwargs):  # type: ignore[no-untyped-def]
        return sentinel

    sqlite_sync_driver.select_to_arrow = types.MethodType(fake_select_to_arrow, sqlite_sync_driver)

    stack = StatementStack().push_execute_arrow("SELECT * FROM users")

    results = sqlite_sync_driver.execute_stack(stack)

    assert len(results) == 1
    assert results[0].result is sentinel
