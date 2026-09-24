"""Unit tests for Spanner transaction retry closures."""

from typing import Any
from unittest.mock import MagicMock

from google.api_core.exceptions import Aborted

from sqlspec.adapters.spanner._typing import SpannerTransaction
from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.exceptions import DeadlockError


def _create_mock_database(attempts_before_success: int = 1) -> tuple[MagicMock, MagicMock]:
    """Create a mock database that retries on Aborted exceptions."""
    mock_db = MagicMock()
    mock_txn = MagicMock(spec=SpannerTransaction)
    attempts = [0]

    def mock_run_in_transaction(callback: Any, *args: Any, **kwargs: Any) -> Any:
        while True:
            attempts[0] += 1
            try:
                return callback(mock_txn, *args, **kwargs)
            except Aborted:
                if attempts[0] > attempts_before_success:
                    raise
                continue

    mock_db.run_in_transaction = MagicMock(side_effect=mock_run_in_transaction)
    return mock_db, mock_txn


def test_config_run_in_transaction_retry_on_aborted() -> None:
    """Verify that config.run_in_transaction retries when callback raises Aborted."""
    mock_db, _mock_txn = _create_mock_database(attempts_before_success=1)
    config = SpannerSyncConfig(connection_config={"project_id": "test", "instance_id": "inst", "database_id": "db"})
    config._database = mock_db

    call_count = [0]

    def unit_of_work(driver: SpannerSyncDriver) -> str:
        call_count[0] += 1
        if call_count[0] == 1:
            raise Aborted("Concurrency conflict")
        return "success"

    result = config.run_in_transaction(unit_of_work)

    assert result == "success"
    assert call_count[0] == 2
    mock_db.run_in_transaction.assert_called_once()


def test_config_run_in_transaction_retry_on_deadlock_error_with_aborted_cause() -> None:
    """Verify that config.run_in_transaction unwraps DeadlockError caused by Aborted."""
    mock_db, _mock_txn = _create_mock_database(attempts_before_success=1)
    config = SpannerSyncConfig(connection_config={"project_id": "test", "instance_id": "inst", "database_id": "db"})
    config._database = mock_db

    call_count = [0]

    def unit_of_work(driver: SpannerSyncDriver) -> str:
        call_count[0] += 1
        if call_count[0] == 1:
            abort_exc = Aborted("Lock conflict")
            deadlock_exc = DeadlockError("transaction aborted")
            deadlock_exc.__cause__ = abort_exc
            raise deadlock_exc
        return "success-after-deadlock"

    result = config.run_in_transaction(unit_of_work)

    assert result == "success-after-deadlock"
    assert call_count[0] == 2
    mock_db.run_in_transaction.assert_called_once()


def test_driver_run_in_transaction_delegates_to_database() -> None:
    """Verify that driver.run_in_transaction delegates to database when not in transaction."""
    mock_db, _mock_txn = _create_mock_database(attempts_before_success=1)
    mock_session = MagicMock()
    mock_session._database = mock_db
    mock_snapshot = MagicMock()
    mock_snapshot._session = mock_session

    driver = SpannerSyncDriver(connection=mock_snapshot)

    call_count = [0]

    def unit_of_work(txn_driver: SpannerSyncDriver) -> str:
        call_count[0] += 1
        if call_count[0] == 1:
            raise Aborted("Retryable abort")
        return "driver-delegated"

    result = driver.run_in_transaction(unit_of_work)

    assert result == "driver-delegated"
    assert call_count[0] == 2
    mock_db.run_in_transaction.assert_called_once()


def test_driver_run_in_transaction_in_existing_transaction() -> None:
    """Verify that driver.run_in_transaction runs directly when connection is already a transaction."""
    mock_txn = MagicMock(spec=SpannerTransaction)
    driver = SpannerSyncDriver(connection=mock_txn)

    def unit_of_work(txn_driver: SpannerSyncDriver) -> str:
        assert txn_driver is driver
        return "direct-execution"

    result = driver.run_in_transaction(unit_of_work)

    assert result == "direct-execution"
