import pytest

from sqlspec.exceptions import FileNotFoundInStorageError, StorageOperationFailedError
from sqlspec.storage.errors import (
    _normalize_storage_error,  # pyright: ignore
    execute_sync_storage_operation,
    raise_storage_error,
)


def test_raise_normalized_storage_error_for_missing_file() -> None:
    with pytest.raises(FileNotFoundInStorageError) as excinfo:
        raise_storage_error(FileNotFoundError("missing"), backend="local", operation="read_bytes", path="file.txt")

    assert "local read_bytes failed" in str(excinfo.value)


def test_normalize_storage_error_marks_retryable() -> None:
    normalized = _normalize_storage_error(
        TimeoutError("timed out"), backend="fsspec", operation="write_bytes", path="s3://bucket/key"
    )
    assert normalized.retryable is True


def test_execute_with_normalized_errors_wraps_generic_failure() -> None:
    def _boom() -> None:
        raise RuntimeError("boom")

    with pytest.raises(StorageOperationFailedError) as excinfo:
        execute_sync_storage_operation(_boom, backend="obstore", operation="write_bytes", path="object")

    assert "obstore write_bytes failed" in str(excinfo.value)
