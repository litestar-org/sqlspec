"""Check Teradata tracker creation through the public tracker operation."""

from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.teradata.migrations import TeradataSyncMigrationTracker


@pytest.mark.parametrize("exists", [False, True])
def test_tracker_creates_only_missing_table(exists: bool) -> None:
    tracker = TeradataSyncMigrationTracker(version_table_name="history", version_table_schema="mart")
    driver = MagicMock()
    driver.driver_features = {"autocommit": False}
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": name, "data_type": "VARCHAR", "max_length": 4096}
        for name in (
            "version_num",
            "version_type",
            "execution_sequence",
            "description",
            "applied_at",
            "execution_time_ms",
            "checksum",
            "applied_by",
            "replaces",
        )
    ]
    driver.select_one_or_none.return_value = {"table_name": "history"} if exists else None
    tracker.ensure_tracking_table(driver)
    assert driver.select_one_or_none.call_args.args[1:] == ("MART", "HISTORY")
    if exists:
        driver.execute_script.assert_not_called()
    else:
        ddl = driver.execute_script.call_args.args[0]
        assert ddl.startswith('CREATE MULTISET TABLE "MART"."HISTORY" (')
        assert '"description" VARCHAR(4096)' in ddl
        assert '"replaces" VARCHAR(4096)' in ddl
        assert '"version_num" VARCHAR(32) NOT NULL PRIMARY KEY' in ddl
        assert "DEFAULT CURRENT_TIMESTAMP(6) NOT NULL" in ddl
        driver.commit.assert_called()


def test_tracker_preserves_explicitly_quoted_schema_and_table_case() -> None:
    tracker = TeradataSyncMigrationTracker(version_table_name='"mart"."history"')
    driver = MagicMock()
    driver.driver_features = {"autocommit": False}
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": name, "data_type": "VARCHAR", "max_length": 4096}
        for name in (
            "version_num",
            "version_type",
            "execution_sequence",
            "description",
            "applied_at",
            "execution_time_ms",
            "checksum",
            "applied_by",
            "replaces",
        )
    ]
    driver.select_one_or_none.return_value = None
    tracker.ensure_tracking_table(driver)
    assert driver.select_one_or_none.call_args.args[1:] == ("mart", "history")
    assert driver.execute_script.call_args.args[0].startswith('CREATE MULTISET TABLE "mart"."history" (')
