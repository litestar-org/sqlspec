"""CockroachDB retry coverage against real driver connections."""

from typing import TYPE_CHECKING

import pytest

from sqlspec import StatementStack
from sqlspec.exceptions import SerializationConflictError

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgDriver
    from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group("cockroachdb")]

_CONFLICTS = pytest.mark.parametrize(
    "conflict",
    [
        pytest.param(lambda: SerializationConflictError("restart transaction"), id="translated"),
        pytest.param(lambda: _SqlstateConflict("restart transaction"), id="sqlstate"),
    ],
)


@_CONFLICTS
def test_cockroach_psycopg_sync_retries_whole_transaction(
    contract_cockroach_psycopg_sync_driver: "CockroachPsycopgSyncDriver", conflict: "Callable[[], Exception]"
) -> None:
    """Verify sync psycopg CockroachDB retries restart and commit the whole transaction."""
    calls = 0

    def operation() -> str:
        nonlocal calls
        calls += 1
        results = contract_cockroach_psycopg_sync_driver.execute_stack(
            StatementStack().push_execute("SELECT 1 AS transaction_probe")
        )
        assert len(results) == 1
        assert contract_cockroach_psycopg_sync_driver._transaction_active is True
        assert contract_cockroach_psycopg_sync_driver.connection.autocommit is False
        if calls == 1:
            raise conflict()
        return "ok"

    assert contract_cockroach_psycopg_sync_driver.run_transaction_with_retry(operation) == "ok"
    assert calls == 2


@_CONFLICTS
async def test_cockroach_asyncpg_retries_whole_transaction(
    contract_cockroach_asyncpg_driver: "CockroachAsyncpgDriver", conflict: "Callable[[], Exception]"
) -> None:
    """Verify asyncpg CockroachDB retries restart and commit the whole transaction."""
    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise conflict()
        return "ok"

    assert await contract_cockroach_asyncpg_driver.run_transaction_with_retry(operation) == "ok"
    assert calls == 2


@_CONFLICTS
async def test_cockroach_psycopg_async_retries_whole_transaction(
    contract_cockroach_psycopg_async_driver: "CockroachPsycopgAsyncDriver", conflict: "Callable[[], Exception]"
) -> None:
    """Verify async psycopg CockroachDB retries restart and commit the whole transaction."""
    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        results = await contract_cockroach_psycopg_async_driver.execute_stack(
            StatementStack().push_execute("SELECT 1 AS transaction_probe")
        )
        assert len(results) == 1
        assert contract_cockroach_psycopg_async_driver._transaction_active is True
        assert contract_cockroach_psycopg_async_driver.connection.autocommit is False
        if calls == 1:
            raise conflict()
        return "ok"

    assert await contract_cockroach_psycopg_async_driver.run_transaction_with_retry(operation) == "ok"
    assert calls == 2


class _SqlstateConflict(Exception):
    """Raw driver error carrying the serialization conflict only as SQLSTATE 40001."""

    sqlstate = "40001"
