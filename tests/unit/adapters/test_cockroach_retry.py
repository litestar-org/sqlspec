"""A secondary rollback failure must not replace a transaction's outcome."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import psycopg
import pytest

from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgDriver
from sqlspec.adapters.cockroach_asyncpg.core import CockroachAsyncpgRetryConfig
from sqlspec.adapters.cockroach_asyncpg.core import calculate_backoff_seconds as asyncpg_backoff
from sqlspec.adapters.cockroach_asyncpg.core import is_retryable_error as asyncpg_is_retryable
from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgExceptionHandler
from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
from sqlspec.adapters.cockroach_psycopg.core import CockroachPsycopgRetryConfig
from sqlspec.adapters.cockroach_psycopg.core import calculate_backoff_seconds as psycopg_backoff
from sqlspec.adapters.cockroach_psycopg.core import is_retryable_error as psycopg_is_retryable
from sqlspec.adapters.cockroach_psycopg.driver import (
    CockroachPsycopgAsyncExceptionHandler,
    CockroachPsycopgSyncExceptionHandler,
)
from sqlspec.exceptions import SerializationConflictError


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


def test_sync_handler_translates_real_serialization_failure_to_a_retryable_error() -> None:
    handler = CockroachPsycopgSyncExceptionHandler()
    failure = psycopg.errors.SerializationFailure("restart transaction: TransactionRetryWithProtoRefreshError")

    with handler:
        raise failure

    assert isinstance(handler.pending_exception, SerializationConflictError)
    assert psycopg_is_retryable(handler.pending_exception)


async def test_async_handler_translates_real_serialization_failure_to_a_retryable_error() -> None:
    handler = CockroachPsycopgAsyncExceptionHandler()
    failure = psycopg.errors.SerializationFailure("restart transaction: TransactionRetryWithProtoRefreshError")

    async with handler:
        raise failure

    assert isinstance(handler.pending_exception, SerializationConflictError)
    assert psycopg_is_retryable(handler.pending_exception)


async def test_asyncpg_handler_translates_real_serialization_error_to_a_retryable_error() -> None:
    handler = CockroachAsyncpgExceptionHandler()
    failure = asyncpg.exceptions.SerializationError("restart transaction")

    async with handler:
        raise failure

    assert isinstance(handler.pending_exception, SerializationConflictError)
    assert asyncpg_is_retryable(handler.pending_exception)


def test_sync_retry_loop_retries_a_translated_serialization_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    driver_type = CockroachPsycopgSyncDriver
    monkeypatch.setattr(driver_type, "_connection_in_transaction", lambda _self: False)
    monkeypatch.setattr(driver_type, "begin", MagicMock())
    monkeypatch.setattr(driver_type, "commit", MagicMock())
    monkeypatch.setattr(driver_type, "rollback", MagicMock())
    driver = driver_type(
        connection=MagicMock(),
        driver_features={"max_retries": 2, "retry_delay_base_ms": 0, "enable_retry_logging": False},
    )
    operation = MagicMock(side_effect=[SerializationConflictError("restart transaction"), "ok"])

    assert driver.run_transaction_with_retry(operation) == "ok"
    assert operation.call_count == 2


@pytest.mark.parametrize("driver_type", [CockroachAsyncpgDriver, CockroachPsycopgAsyncDriver])
async def test_async_retry_loop_retries_a_translated_serialization_conflict(
    driver_type: "type[CockroachAsyncpgDriver | CockroachPsycopgAsyncDriver]", monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(driver_type, "_connection_in_transaction", lambda _self: False)
    monkeypatch.setattr(driver_type, "begin", AsyncMock())
    monkeypatch.setattr(driver_type, "commit", AsyncMock())
    monkeypatch.setattr(driver_type, "rollback", AsyncMock())
    driver = driver_type(
        connection=MagicMock(),
        driver_features={"max_retries": 2, "retry_delay_base_ms": 0, "enable_retry_logging": False},
    )
    operation = AsyncMock(side_effect=[SerializationConflictError("restart transaction"), "ok"])

    assert await driver.run_transaction_with_retry(operation) == "ok"
    assert operation.await_count == 2


@pytest.mark.parametrize(
    ("backoff", "config_type"),
    [(psycopg_backoff, CockroachPsycopgRetryConfig), (asyncpg_backoff, CockroachAsyncpgRetryConfig)],
)
def test_backoff_stays_jittered_at_the_delay_cap(backoff: Any, config_type: Any) -> None:
    config = config_type(max_retries=20, base_delay_ms=50.0, max_delay_ms=5000.0)

    delays = {backoff(12, config) for _ in range(20)}

    assert len(delays) > 1
    assert max(delays) <= 5.0
