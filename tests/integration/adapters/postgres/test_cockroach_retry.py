"""CockroachDB retry coverage against real driver connections."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgDriver
    from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver

pytestmark = pytest.mark.xdist_group("postgres")


class _RetryableCockroachError(Exception):
    sqlstate = "40001"


def test_cockroach_psycopg_sync_retries_whole_transaction(
    contract_cockroach_psycopg_sync_driver: "CockroachPsycopgSyncDriver",
) -> None:
    calls = 0

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise _RetryableCockroachError("restart transaction")
        return "ok"

    assert contract_cockroach_psycopg_sync_driver.run_transaction_with_retry(operation) == "ok"
    assert calls == 2


async def test_cockroach_asyncpg_retries_whole_transaction(
    contract_cockroach_asyncpg_driver: "CockroachAsyncpgDriver",
) -> None:
    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise _RetryableCockroachError("restart transaction")
        return "ok"

    assert await contract_cockroach_asyncpg_driver.run_transaction_with_retry(operation) == "ok"
    assert calls == 2


async def test_cockroach_psycopg_async_retries_whole_transaction(
    contract_cockroach_psycopg_async_driver: "CockroachPsycopgAsyncDriver",
) -> None:
    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise _RetryableCockroachError("restart transaction")
        return "ok"

    assert await contract_cockroach_psycopg_async_driver.run_transaction_with_retry(operation) == "ok"
    assert calls == 2
