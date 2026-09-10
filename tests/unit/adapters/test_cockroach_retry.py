"""A secondary rollback failure must not replace a transaction's outcome."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgDriver
from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver


class _RetryableError(Exception):
    sqlstate = "40001"


@pytest.mark.parametrize("outcome", ["retry", "exhausted", "nonretryable"])
def test_sync_retry_preserves_outcome_when_rollback_fails(outcome: str, monkeypatch: pytest.MonkeyPatch) -> None:
    driver_type = CockroachPsycopgSyncDriver
    monkeypatch.setattr(driver_type, "_connection_in_transaction", lambda _self: False)
    begin = MagicMock()
    commit = MagicMock()
    rollback = MagicMock(side_effect=RuntimeError("secondary rollback failure"))
    monkeypatch.setattr(driver_type, "begin", begin)
    monkeypatch.setattr(driver_type, "commit", commit)
    monkeypatch.setattr(driver_type, "rollback", rollback)
    driver = driver_type(
        connection=MagicMock(),
        driver_features={"max_retries": 1, "retry_delay_base_ms": 0, "enable_retry_logging": False},
    )
    original = ValueError("operation failed") if outcome == "nonretryable" else _RetryableError("restart transaction")
    operation = MagicMock(side_effect=[original, "ok"] if outcome == "retry" else original)

    if outcome == "retry":
        assert driver.run_transaction_with_retry(operation) == "ok"
    else:
        with pytest.raises(type(original)) as caught:
            driver.run_transaction_with_retry(operation)
        assert caught.value is original

    attempts = 1 if outcome == "nonretryable" else 2
    assert operation.call_count == begin.call_count == attempts
    assert rollback.call_count == (1 if outcome == "retry" else attempts)
    assert commit.call_count == (1 if outcome == "retry" else 0)


@pytest.mark.parametrize("driver_type", [CockroachAsyncpgDriver, CockroachPsycopgAsyncDriver])
@pytest.mark.parametrize("outcome", ["retry", "exhausted", "nonretryable"])
async def test_async_retry_preserves_outcome_when_rollback_fails(
    driver_type: type[CockroachAsyncpgDriver | CockroachPsycopgAsyncDriver],
    outcome: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(driver_type, "_connection_in_transaction", lambda _self: False)
    begin = AsyncMock()
    commit = AsyncMock()
    rollback = AsyncMock(side_effect=RuntimeError("secondary rollback failure"))
    monkeypatch.setattr(driver_type, "begin", begin)
    monkeypatch.setattr(driver_type, "commit", commit)
    monkeypatch.setattr(driver_type, "rollback", rollback)
    driver = driver_type(
        connection=MagicMock(),
        driver_features={"max_retries": 1, "retry_delay_base_ms": 0, "enable_retry_logging": False},
    )
    original = ValueError("operation failed") if outcome == "nonretryable" else _RetryableError("restart transaction")
    operation = AsyncMock(side_effect=[original, "ok"] if outcome == "retry" else original)

    if outcome == "retry":
        assert await driver.run_transaction_with_retry(operation) == "ok"
    else:
        with pytest.raises(type(original)) as caught:
            await driver.run_transaction_with_retry(operation)
        assert caught.value is original

    attempts = 1 if outcome == "nonretryable" else 2
    assert operation.await_count == begin.await_count == attempts
    assert rollback.await_count == (1 if outcome == "retry" else attempts)
    assert commit.await_count == (1 if outcome == "retry" else 0)
