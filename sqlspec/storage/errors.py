"""Storage error normalization helpers."""

import errno
import logging
from typing import TYPE_CHECKING, TypeVar

from sqlspec.exceptions import FileNotFoundInStorageError, StorageOperationFailedError
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

__all__ = ("StorageError", "execute_async_storage_operation", "execute_sync_storage_operation", "raise_storage_error")


logger = get_logger(__name__)

T = TypeVar("T")


_NOT_FOUND_NAMES = {"NotFoundError", "ObjectNotFound", "NoSuchKey", "NoSuchBucket", "NoSuchFile"}


class StorageError:
    """Normalized view of a storage backend exception."""

    __slots__ = ("backend", "message", "operation", "original", "path", "retryable")

    def __init__(
        self, message: str, backend: str, operation: str, path: str | None, retryable: bool, original: Exception
    ) -> None:
        self.message = message
        self.backend = backend
        self.operation = operation
        self.path = path
        self.retryable = retryable
        self.original = original


def _is_missing_error(error: Exception) -> bool:
    if isinstance(error, FileNotFoundError):
        return True

    if isinstance(error, OSError) and error.errno in {errno.ENOENT, errno.ENOTDIR}:
        return True

    name = error.__class__.__name__
    return name in _NOT_FOUND_NAMES


def _is_retryable(error: Exception) -> bool:
    if isinstance(error, (ConnectionError, TimeoutError)):
        return True

    name = error.__class__.__name__
    return bool("Timeout" in name or "Temporary" in name)


def _normalize_storage_error(error: Exception, *, backend: str, operation: str, path: str | None) -> "StorageError":
    message = f"{backend} {operation} failed"
    if path:
        message = f"{message} for {path}"

    return StorageError(
        message=message, backend=backend, operation=operation, path=path, retryable=_is_retryable(error), original=error
    )


def raise_storage_error(error: Exception, *, backend: str, operation: str, path: str | None) -> None:
    is_missing = _is_missing_error(error)
    normalized = _normalize_storage_error(error, backend=backend, operation=operation, path=path)

    log_extra: Mapping[str, str | bool | None] = {
        "backend_type": backend,
        "operation": operation,
        "path": path,
        "exception_type": error.__class__.__name__,
        "retryable": normalized.retryable,
    }

    if is_missing:
        log_with_context(logger, logging.INFO, "storage.object.missing", **log_extra)
        raise FileNotFoundInStorageError(normalized.message) from error

    log_with_context(logger, logging.WARNING, "storage.operation.failed", **log_extra)
    raise StorageOperationFailedError(normalized.message) from error


def execute_sync_storage_operation(func: "Callable[[], T]", *, backend: str, operation: str, path: str | None) -> T:
    try:
        return func()
    except Exception as error:
        raise_storage_error(error, backend=backend, operation=operation, path=path)
        raise


async def execute_async_storage_operation(
    func: "Callable[[], Awaitable[T]]", *, backend: str, operation: str, path: str | None
) -> T:
    try:
        return await func()
    except Exception as error:
        raise_storage_error(error, backend=backend, operation=operation, path=path)
        raise
