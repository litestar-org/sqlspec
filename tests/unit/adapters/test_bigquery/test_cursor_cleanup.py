"""BigQuery cursor resource cleanup tests."""

from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.bigquery import BigQueryCursor


@pytest.mark.parametrize("state", ["PENDING", "RUNNING"])
def test_cursor_exit_cancels_active_job_and_releases_reference(state: str) -> None:
    cursor = BigQueryCursor(MagicMock())
    job = MagicMock(state=state)
    cursor.job = job

    cursor.__exit__(None, None, None)

    job.cancel.assert_called_once_with()
    assert cursor.job is None


def test_cursor_exit_releases_completed_job_without_cancelling() -> None:
    cursor = BigQueryCursor(MagicMock())
    job = MagicMock(state="DONE")
    cursor.job = job

    cursor.__exit__(None, None, None)

    job.cancel.assert_not_called()
    assert cursor.job is None
