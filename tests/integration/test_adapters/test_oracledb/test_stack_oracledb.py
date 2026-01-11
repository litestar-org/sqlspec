# pyright: reportPrivateUsage=false

"""Integration tests for Oracle StatementStack execution paths."""

from typing import Any

import pytest

from sqlspec import StackExecutionError, StatementStack
from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver

pytestmark = pytest.mark.xdist_group("oracle")


DROP_TEMPLATE = """
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE {table_name}';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
"""

CREATE_TEMPLATE = """
CREATE TABLE {table_name} (
    id   NUMBER PRIMARY KEY,
    name VARCHAR2(50)
)
"""


async def _reset_async_table(driver: OracleAsyncDriver, table_name: str) -> None:
    await driver.execute_script(DROP_TEMPLATE.format(table_name=table_name))
    await driver.execute_script(CREATE_TEMPLATE.format(table_name=table_name))


def _reset_sync_table(driver: OracleSyncDriver, table_name: str) -> None:
    driver.execute_script(DROP_TEMPLATE.format(table_name=table_name))
    driver.execute_script(CREATE_TEMPLATE.format(table_name=table_name))


@pytest.mark.asyncio(loop_scope="function")
async def test_async_statement_stack_native_pipeline(
    monkeypatch: pytest.MonkeyPatch, oracle_async_session: OracleAsyncDriver
) -> None:
    """Verify StatementStack execution routes through the native pipeline when supported."""

    if not await oracle_async_session._pipeline_native_supported():
        pytest.skip("Oracle native pipeline unavailable")

    table_name = "stack_async_pipeline"
    await _reset_async_table(oracle_async_session, table_name)

    call_counter = {"count": 0}
    original_execute_stack_native = OracleAsyncDriver._execute_stack_native

    async def tracking_execute_stack_native(
        self: OracleAsyncDriver, stack: StatementStack, *, continue_on_error: bool
    ) -> tuple[Any, ...]:
        call_counter["count"] += 1
        return await original_execute_stack_native(self, stack, continue_on_error=continue_on_error)

    monkeypatch.setattr(OracleAsyncDriver, "_execute_stack_native", tracking_execute_stack_native)

    stack = (
        StatementStack()
        .push_execute(f"INSERT INTO {table_name} (id, name) VALUES (:id, :name)", {"id": 1, "name": "alpha"})
        .push_execute(f"INSERT INTO {table_name} (id, name) VALUES (:id, :name)", {"id": 2, "name": "beta"})
        .push_execute(f"SELECT name FROM {table_name} WHERE id = :id", {"id": 2})
    )

    results = await oracle_async_session.execute_stack(stack)

    assert call_counter["count"] == 1, "Native pipeline was not invoked"
    assert len(results) == 3
    assert results[0].rows_affected == 1
    assert results[1].rows_affected == 1
    assert results[2].result is not None
    assert results[2].result.data is not None
    assert results[2].result.data[0]["name"] == "beta"

    await oracle_async_session.execute_script(DROP_TEMPLATE.format(table_name=table_name))


@pytest.mark.asyncio(loop_scope="function")
async def test_async_statement_stack_continue_on_error_pipeline(oracle_async_session: OracleAsyncDriver) -> None:
    """Ensure continue-on-error surfaces failures while executing remaining operations."""

    if not await oracle_async_session._pipeline_native_supported():
        pytest.skip("Oracle native pipeline unavailable")

    table_name = "stack_async_errors"
    await _reset_async_table(oracle_async_session, table_name)

    stack = (
        StatementStack()
        .push_execute(f"INSERT INTO {table_name} (id, name) VALUES (:id, :name)", {"id": 1, "name": "alpha"})
        .push_execute(  # duplicate PK to trigger ORA-00001
            f"INSERT INTO {table_name} (id, name) VALUES (:id, :name)", {"id": 1, "name": "duplicate"}
        )
        .push_execute(f"INSERT INTO {table_name} (id, name) VALUES (:id, :name)", {"id": 2, "name": "beta"})
    )

    results = await oracle_async_session.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[0].rows_affected == 1
    assert isinstance(results[1].error, StackExecutionError)
    assert results[2].rows_affected == 1

    verify_result = await oracle_async_session.execute(
        f"SELECT COUNT(*) as total_rows FROM {table_name} WHERE id = :id", {"id": 2}
    )
    assert verify_result.data is not None
    assert verify_result.data[0]["total_rows"] == 1

    await oracle_async_session.execute_script(DROP_TEMPLATE.format(table_name=table_name))


def test_sync_statement_stack_sequential_fallback(oracle_sync_session: OracleSyncDriver) -> None:
    """Sync driver should execute stacks sequentially when pipelines are unavailable."""

    table_name = "stack_sync_pipeline"
    _reset_sync_table(oracle_sync_session, table_name)

    stack = (
        StatementStack()
        .push_execute(f"INSERT INTO {table_name} (id, name) VALUES (:id, :name)", {"id": 1, "name": "sync-alpha"})
        .push_execute(f"SELECT name FROM {table_name} WHERE id = :id", {"id": 1})
    )

    results = oracle_sync_session.execute_stack(stack)

    assert len(results) == 2
    assert results[0].rows_affected == 1
    assert results[1].result is not None
    assert results[1].result.data is not None
    assert results[1].result.data[0]["name"] == "sync-alpha"

    oracle_sync_session.execute_script(DROP_TEMPLATE.format(table_name=table_name))
