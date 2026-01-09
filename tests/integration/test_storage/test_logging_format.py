"""Integration tests for storage logging format."""

import logging

import pytest

from sqlspec.exceptions import FileNotFoundInStorageError
from sqlspec.storage.errors import execute_sync_storage_operation


def _find_log_record(
    records: "list[logging.LogRecord]", message: str, logger_name: str
) -> "logging.LogRecord":
    for record in records:
        if record.name != logger_name:
            continue
        if record.getMessage() == message:
            return record
    msg = f"Expected log message '{message}' from '{logger_name}' not found"
    raise AssertionError(msg)


def _raise_missing() -> None:
    raise FileNotFoundError("missing")


def test_storage_missing_logging_format(caplog) -> None:
    caplog.set_level(logging.INFO, logger="sqlspec.storage.errors")

    with pytest.raises(FileNotFoundInStorageError):
        execute_sync_storage_operation(_raise_missing, backend="fsspec", operation="read", path="missing.txt")

    record = _find_log_record(caplog.records, "storage.object.missing", "sqlspec.storage.errors")
    extra_fields = record.__dict__.get("extra_fields")
    assert isinstance(extra_fields, dict)
    assert extra_fields.get("backend_type") == "fsspec"
    assert extra_fields.get("operation") == "read"
    assert extra_fields.get("path") == "missing.txt"
    assert extra_fields.get("exception_type") == "FileNotFoundError"
    assert extra_fields.get("retryable") is False
