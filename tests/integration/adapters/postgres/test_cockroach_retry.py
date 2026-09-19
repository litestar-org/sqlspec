"""CockroachDB retry coverage against real driver connections."""

from typing import TYPE_CHECKING

import pytest

from sqlspec import StatementStack
from sqlspec.exceptions import SerializationConflictError


class _SqlstateConflict(Exception):
    """A raw driver error that carries the conflict only as a SQLSTATE."""

    sqlstate = "40001"


_CONFLICTS = pytest.mark.parametrize(
    "conflict",
    [
        pytest.param(lambda: SerializationConflictError("restart transaction"), id="translated"),
        pytest.param(lambda: _SqlstateConflict("restart transaction"), id="sqlstate"),
    ],
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgDriver
    from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver

pytestmark = pytest.mark.xdist_group("postgres")


@_CONFLICTS
def test_cockroach_psycopg_sync_retries_whole_transaction(
    contract_cockroach_psycopg_sync_driver: "CockroachPsycopgSyncDriver", conflict: "Callable[[], Exception]"
) -> None:
    calls = 0

    def operation() -> str:
        nonlocal calls
        calls += 1
        results = contract_cockroach_psycopg_sync_driver.execute_stack(
            StatementStack().push_execute("SELECT 1 AS transaction_probe")
        )
        assert len(results) == 1
        assert contract_cockroach_psycopg_sync_driver._transaction_active is True  # pyright: ignore[reportPrivateUsage]
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
    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        results = await contract_cockroach_psycopg_async_driver.execute_stack(
            StatementStack().push_execute("SELECT 1 AS transaction_probe")
        )
        assert len(results) == 1
        assert contract_cockroach_psycopg_async_driver._transaction_active is True  # pyright: ignore[reportPrivateUsage]
        assert contract_cockroach_psycopg_async_driver.connection.autocommit is False
        if calls == 1:
            raise conflict()
        return "ok"

    assert await contract_cockroach_psycopg_async_driver.run_transaction_with_retry(operation) == "ok"
    assert calls == 2
