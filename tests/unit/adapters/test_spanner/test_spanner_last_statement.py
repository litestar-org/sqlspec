"""Unit tests for Spanner last_statement execution and commit behavior."""

from unittest.mock import MagicMock

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


def test_script_only_marks_final_dml_as_last_statement() -> None:
    cursor = MagicMock()
    cursor.execute_update.return_value = 1
    driver = SpannerSyncDriver(connection=cursor)
    driver.execute_script("UPDATE t SET x = 1; UPDATE t SET x = 2", last_statement=True)
    calls = cursor.execute_update.call_args_list
    assert len(calls) == 2
    assert "last_statement" not in calls[0].kwargs
    assert calls[1].kwargs["last_statement"] is True
