"""Unit tests for Spanner last_statement execution and commit behavior."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

from sqlspec.adapters.spanner.config import SpannerConnectionContext, SpannerSyncConfig
from sqlspec.adapters.spanner.driver import SpannerSyncDriver


def test_execute_passes_last_statement_to_writer() -> None:
    """Verify that driver.execute forwards last_statement=True to writer.execute_update."""
    mock_cursor = MagicMock()
    mock_cursor.execute_update.return_value = 1
    mock_cursor.committed = None

    driver = SpannerSyncDriver(connection=mock_cursor)
    driver.execute("UPDATE t SET x = 1 WHERE id = 'a'", last_statement=True)

    mock_cursor.execute_update.assert_called_once()
    _, kwargs = mock_cursor.execute_update.call_args
    assert kwargs.get("last_statement") is True


def test_driver_commit_noop_when_transaction_already_committed() -> None:
    """Verify that driver.commit is a no-op when writer.committed is set."""
    mock_cursor = MagicMock()
    mock_cursor.committed = datetime.now(timezone.utc)
    mock_cursor.commit = MagicMock()

    driver = SpannerSyncDriver(connection=mock_cursor)
    driver.commit()

    mock_cursor.commit.assert_not_called()


def test_connection_context_exit_noop_when_already_committed() -> None:
    """Verify that SpannerConnectionContext.__exit__ skips commit when txn.committed is set."""
    mock_txn = MagicMock()
    mock_txn._transaction_id = b"tx1"
    mock_txn.committed = datetime.now(timezone.utc)
    mock_txn.commit = MagicMock()

    mock_session = MagicMock()
    mock_session.transaction.return_value = mock_txn

    mock_db = MagicMock()
    mock_db.sessions_manager.put_session = MagicMock()

    config = MagicMock(spec=SpannerSyncConfig)
    config.get_database.return_value = mock_db

    ctx = SpannerConnectionContext(config, transaction=True)
    ctx._session = mock_session
    ctx._connection = mock_txn

    ctx.__exit__(None, None, None)

    mock_txn.commit.assert_not_called()
