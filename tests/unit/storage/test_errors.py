import errno
from typing import NoReturn, get_type_hints

import pytest

from sqlspec.exceptions import FileExistsInStorageError, FileNotFoundInStorageError, StorageOperationFailedError
from sqlspec.storage.errors import (
    _storage_error,  # pyright: ignore
    execute_sync_storage_operation,
    raise_storage_error,
)


def test_raise_storage_error_is_typed_as_no_return() -> None:
    assert get_type_hints(raise_storage_error)["return"] is NoReturn


def test_raise_normalized_storage_error_for_missing_file() -> None:
    with pytest.raises(FileNotFoundInStorageError) as excinfo:
        raise_storage_error(FileNotFoundError("missing"), backend="local", operation="read_bytes", path="file.txt")

    assert "local read_bytes failed" in str(excinfo.value)


def test_storage_error_marks_retryable() -> None:
    normalized = _storage_error(
        TimeoutError("timed out"), backend="fsspec", operation="write_bytes", path="s3://bucket/key"
    )
    assert normalized.retryable is True


def test_execute_with_normalized_errors_wraps_generic_failure() -> None:
    def _boom() -> None:
        raise RuntimeError("boom")

    with pytest.raises(StorageOperationFailedError) as excinfo:
        execute_sync_storage_operation(_boom, backend="obstore", operation="write_bytes", path="object")

    assert "obstore write_bytes failed" in str(excinfo.value)


class AlreadyExistsError(Exception):
    """Synthetic backend already-exists exception matching obstore error naming."""


@pytest.mark.parametrize(
    "raw_error",
    [FileExistsError("already exists"), OSError(errno.EEXIST, "file exists"), AlreadyExistsError("object exists")],
)
def test_raise_normalized_storage_error_for_existing_object(raw_error: Exception) -> None:
    """Verify already-exists backend errors normalize to FileExistsInStorageError."""
    with pytest.raises(FileExistsInStorageError) as excinfo:
        raise_storage_error(raw_error, backend="local", operation="write_bytes", path="file.txt")

    assert "local write_bytes failed" in str(excinfo.value)
    assert excinfo.value.__cause__ is raw_error


def test_raise_storage_error_passes_through_file_exists_in_storage_error() -> None:
    """Verify FileExistsInStorageError is re-raised without re-wrapping."""
    existing = FileExistsInStorageError("already normalized")
    with pytest.raises(FileExistsInStorageError) as excinfo:
        raise_storage_error(existing, backend="local", operation="write_bytes", path="file.txt")

    assert excinfo.value is existing
