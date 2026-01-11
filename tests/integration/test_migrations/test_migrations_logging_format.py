"""Integration tests for migration logging format."""

import logging
import tempfile

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations.commands import SyncMigrationCommands


def _find_log_record(records: "list[logging.LogRecord]", message: str, logger_name: str) -> "logging.LogRecord":
    for record in records:
        if record.name != logger_name:
            continue
        if record.getMessage() == message:
            return record
    msg = f"Expected log message '{message}' from '{logger_name}' not found"
    raise AssertionError(msg)


def test_migration_list_logging_format(tmp_path, caplog) -> None:
    caplog.set_level(logging.DEBUG, logger="sqlspec.migrations.commands")

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name}, migration_config={"script_location": str(tmp_path)}
        )
        commands = SyncMigrationCommands(config)
        current = commands.current(verbose=False)

    assert current is None
    record = _find_log_record(caplog.records, "migration.list", "sqlspec.migrations.commands")
    extra_fields = record.__dict__.get("extra_fields")
    assert isinstance(extra_fields, dict)
    assert extra_fields.get("db_system") == "sqlite"
    assert extra_fields.get("status") == "empty"
    assert extra_fields.get("current_version") is None
    assert extra_fields.get("applied_count") == 0
    assert extra_fields.get("verbose") is False
