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
from sqlspec.exceptions import SerializationConflictError, SQLSpecError


class _RetryableError(Exception):
    sqlstate = "40001"


@pytest.mark.parametrize("outcome", ["retryable", "nonretryable"])
def test_sync_retry_preserves_outcome_when_rollback_fails(outcome: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed rollback leaves the transaction aborted, so the original error is raised at once."""
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
    operation = MagicMock(side_effect=original)

    with pytest.raises(type(original)) as caught:
        driver.run_transaction_with_retry(operation)

    assert caught.value is original
    assert operation.call_count == begin.call_count == 1
    assert rollback.call_count == 1
    assert commit.call_count == 0


@pytest.mark.parametrize("driver_type", [CockroachAsyncpgDriver, CockroachPsycopgAsyncDriver])
@pytest.mark.parametrize("outcome", ["retryable", "nonretryable"])
async def test_async_retry_preserves_outcome_when_rollback_fails(
    driver_type: type[CockroachAsyncpgDriver | CockroachPsycopgAsyncDriver],
    outcome: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed rollback leaves the transaction aborted, so the original error is raised at once."""
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
    operation = AsyncMock(side_effect=original)

    with pytest.raises(type(original)) as caught:
        await driver.run_transaction_with_retry(operation)

    assert caught.value is original
    assert operation.await_count == begin.await_count == 1
    assert rollback.await_count == 1
    assert commit.await_count == 0


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


@pytest.mark.parametrize("is_retryable", [psycopg_is_retryable, asyncpg_is_retryable])
def test_commit_time_serialization_failure_is_retryable(is_retryable: Any) -> None:
    """CockroachDB reports write-skew conflicts at COMMIT, wrapped by transaction control."""
    driver_error = psycopg.errors.SerializationFailure("restart transaction: RETRY_SERIALIZABLE")
    try:
        try:
            raise driver_error
        except psycopg.Error as inner:
            raise SQLSpecError("Failed to commit transaction: restart transaction") from inner
    except SQLSpecError as wrapped:
        assert is_retryable(wrapped)


@pytest.mark.parametrize("is_retryable", [psycopg_is_retryable, asyncpg_is_retryable])
def test_unrelated_wrapped_error_is_not_retryable(is_retryable: Any) -> None:
    """Walking the cause chain must not turn every wrapped failure into a retry."""
    try:
        try:
            raise ValueError("bad value")
        except ValueError as inner:
            raise SQLSpecError("Failed to commit transaction: bad value") from inner
    except SQLSpecError as wrapped:
        assert not is_retryable(wrapped)


def test_sync_retry_loop_retries_a_commit_time_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    """The retry loop must see a conflict raised by commit, not just by the operation."""
    driver_type = CockroachPsycopgSyncDriver
    monkeypatch.setattr(driver_type, "_connection_in_transaction", lambda _self: False)
    monkeypatch.setattr(driver_type, "begin", MagicMock())
    monkeypatch.setattr(driver_type, "rollback", MagicMock())

    commits: list[int] = []

    def _commit(_self: Any) -> None:
        commits.append(1)
        if len(commits) == 1:
            try:
                raise psycopg.errors.SerializationFailure("restart transaction")
            except psycopg.Error as inner:
                raise SQLSpecError("Failed to commit transaction: restart transaction") from inner

    monkeypatch.setattr(driver_type, "commit", _commit)
    driver = driver_type(
        connection=MagicMock(),
        driver_features={"max_retries": 2, "retry_delay_base_ms": 0, "enable_retry_logging": False},
    )
    operation = MagicMock(return_value="ok")

    assert driver.run_transaction_with_retry(operation) == "ok"
    assert len(commits) == 2


def test_sync_retry_does_not_reuse_a_connection_whose_rollback_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reusing an aborted transaction turns the real conflict into an opaque 25P02."""
    driver_type = CockroachPsycopgSyncDriver
    monkeypatch.setattr(driver_type, "_connection_in_transaction", lambda _self: False)
    monkeypatch.setattr(driver_type, "begin", MagicMock())
    monkeypatch.setattr(driver_type, "commit", MagicMock())
    monkeypatch.setattr(driver_type, "rollback", MagicMock(side_effect=RuntimeError("rollback failed")))
    driver = driver_type(
        connection=MagicMock(),
        driver_features={"max_retries": 5, "retry_delay_base_ms": 0, "enable_retry_logging": False},
    )
    conflict = SerializationConflictError("restart transaction")
    operation = MagicMock(side_effect=conflict)

    with pytest.raises(SerializationConflictError) as caught:
        driver.run_transaction_with_retry(operation)

    assert caught.value is conflict
    assert operation.call_count == 1


def test_sync_retry_still_retries_when_rollback_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """A clean rollback leaves the connection usable, so the retry proceeds."""
    driver_type = CockroachPsycopgSyncDriver
    monkeypatch.setattr(driver_type, "_connection_in_transaction", lambda _self: False)
    monkeypatch.setattr(driver_type, "begin", MagicMock())
    monkeypatch.setattr(driver_type, "commit", MagicMock())
    monkeypatch.setattr(driver_type, "rollback", MagicMock())
    driver = driver_type(
        connection=MagicMock(),
        driver_features={"max_retries": 5, "retry_delay_base_ms": 0, "enable_retry_logging": False},
    )
    operation = MagicMock(side_effect=[SerializationConflictError("restart transaction"), "ok"])

    assert driver.run_transaction_with_retry(operation) == "ok"
    assert operation.call_count == 2


@pytest.mark.parametrize("is_retryable", [psycopg_is_retryable, asyncpg_is_retryable])
def test_a_conflict_the_caller_re_raised_as_its_own_error_is_not_retried(is_retryable: Any) -> None:
    """Translating a conflict into a domain error is a deliberate abort, not a restart request."""

    class DomainError(Exception):
        """An error the application raised on purpose."""

    try:
        try:
            raise SerializationConflictError("write skew")
        except SerializationConflictError as conflict:
            raise DomainError("business rule rejected the write") from conflict
    except DomainError as deliberate:
        assert is_retryable(deliberate) is False


@pytest.mark.parametrize("is_retryable", [psycopg_is_retryable, asyncpg_is_retryable])
def test_a_conflict_wrapped_by_transaction_control_is_retried(is_retryable: Any) -> None:
    """CockroachDB reports write skew at COMMIT, where the driver error is wrapped by SQLSpec."""
    try:
        try:
            raise _RetryableError("restart transaction")
        except _RetryableError as driver_error:
            raise SQLSpecError("Failed to commit transaction") from driver_error
    except SQLSpecError as wrapped:
        assert is_retryable(wrapped) is True


def test_a_failed_rollback_keeps_the_original_cause_and_reports_itself(monkeypatch: pytest.MonkeyPatch) -> None:
    """The escaping error must still carry its SQLSTATE cause, and name why rollback died."""
    driver_type = CockroachPsycopgSyncDriver
    monkeypatch.setattr(driver_type, "_connection_in_transaction", lambda _self: False)
    rollback_failure = RuntimeError("connection is broken")
    monkeypatch.setattr(driver_type, "begin", MagicMock())
    monkeypatch.setattr(driver_type, "commit", MagicMock())
    monkeypatch.setattr(driver_type, "rollback", MagicMock(side_effect=rollback_failure))
    driver = driver_type(
        connection=MagicMock(),
        driver_features={"max_retries": 3, "retry_delay_base_ms": 0, "enable_retry_logging": False},
    )
    driver_error = _RetryableError("restart transaction")
    conflict = SerializationConflictError("Failed to commit transaction")
    conflict.__cause__ = driver_error

    with pytest.raises(SerializationConflictError) as caught:
        driver.run_transaction_with_retry(MagicMock(side_effect=conflict))

    assert caught.value.__cause__ is driver_error
    assert caught.value.__context__ is rollback_failure


def test_sync_retry_stops_after_the_configured_attempts_when_rollback_works(monkeypatch: pytest.MonkeyPatch) -> None:
    """A conflict that never clears must stop at max_retries rather than loop forever."""
    driver_type = CockroachPsycopgSyncDriver
    monkeypatch.setattr(driver_type, "_connection_in_transaction", lambda _self: False)
    begin = MagicMock()
    rollback = MagicMock()
    monkeypatch.setattr(driver_type, "begin", begin)
    monkeypatch.setattr(driver_type, "commit", MagicMock())
    monkeypatch.setattr(driver_type, "rollback", rollback)
    driver = driver_type(
        connection=MagicMock(),
        driver_features={"max_retries": 2, "retry_delay_base_ms": 0, "enable_retry_logging": False},
    )
    conflict = _RetryableError("restart transaction")
    operation = MagicMock(side_effect=conflict)

    with pytest.raises(_RetryableError) as caught:
        driver.run_transaction_with_retry(operation)

    assert caught.value is conflict
    assert operation.call_count == 3
    assert begin.call_count == 3
    assert rollback.call_count == 3
