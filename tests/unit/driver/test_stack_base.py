"""Unit tests for the driver execute_stack implementations."""

import types

import pytest

from sqlspec import StatementStack
from sqlspec.exceptions import StackExecutionError, TransactionError
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


def test_base_drivers_expose_savepoint_contract() -> None:
    """Both base driver classes must expose the savepoint methods."""
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

    for method in ("create_savepoint", "release_savepoint", "rollback_to_savepoint"):
        assert callable(getattr(SyncDriverAdapterBase, method))
        assert callable(getattr(AsyncDriverAdapterBase, method))


@pytest.mark.parametrize("bad_name", ["1; DROP TABLE users", "sp1; DROP TABLE t", "sp-1", "sp 1", "", '"sp"'])
@requires_interpreted
def test_sync_savepoint_rejects_unsafe_names(sqlite_sync_driver, bad_name) -> None:
    """Savepoint helpers must reject names that are not safe SQL identifiers."""
    with pytest.raises(TransactionError):
        sqlite_sync_driver.create_savepoint(bad_name)
    with pytest.raises(TransactionError):
        sqlite_sync_driver.release_savepoint(bad_name)
    with pytest.raises(TransactionError):
        sqlite_sync_driver.rollback_to_savepoint(bad_name)


@pytest.mark.parametrize("bad_name", ["1; DROP TABLE users", "sp1; DROP TABLE t", "sp-1", "sp 1", "", '"sp"'])
@requires_interpreted
async def test_async_savepoint_rejects_unsafe_names(aiosqlite_async_driver, bad_name) -> None:
    """Async savepoint helpers must reject names that are not safe SQL identifiers."""
    with pytest.raises(TransactionError):
        await aiosqlite_async_driver.create_savepoint(bad_name)
    with pytest.raises(TransactionError):
        await aiosqlite_async_driver.release_savepoint(bad_name)
    with pytest.raises(TransactionError):
        await aiosqlite_async_driver.rollback_to_savepoint(bad_name)


@requires_interpreted
def test_sync_savepoint_round_trip(sqlite_sync_driver) -> None:
    """Default savepoint DDL should roll back only the work after the savepoint."""
    sqlite_sync_driver.execute("CREATE TABLE sp (id INTEGER)")
    sqlite_sync_driver.begin()
    sqlite_sync_driver.execute("INSERT INTO sp (id) VALUES (1)")
    sqlite_sync_driver.create_savepoint("sp1")
    sqlite_sync_driver.execute("INSERT INTO sp (id) VALUES (2)")
    sqlite_sync_driver.rollback_to_savepoint("sp1")
    sqlite_sync_driver.release_savepoint("sp1")
    sqlite_sync_driver.commit()

    rows = sqlite_sync_driver.execute("SELECT id FROM sp ORDER BY id")
    assert [row["id"] for row in rows.get_data()] == [1]


@requires_interpreted
async def test_async_savepoint_round_trip(aiosqlite_async_driver) -> None:
    """Default async savepoint DDL should roll back only the work after the savepoint."""
    await aiosqlite_async_driver.execute("CREATE TABLE sp (id INTEGER)")
    await aiosqlite_async_driver.begin()
    await aiosqlite_async_driver.execute("INSERT INTO sp (id) VALUES (1)")
    await aiosqlite_async_driver.create_savepoint("sp1")
    await aiosqlite_async_driver.execute("INSERT INTO sp (id) VALUES (2)")
    await aiosqlite_async_driver.rollback_to_savepoint("sp1")
    await aiosqlite_async_driver.release_savepoint("sp1")
    await aiosqlite_async_driver.commit()

    rows = await aiosqlite_async_driver.execute("SELECT id FROM sp ORDER BY id")
    assert [row["id"] for row in rows.get_data()] == [1]
