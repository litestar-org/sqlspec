# ruff: noqa: PLR0904
"""Base class for instrumented storage backends.

This module provides a base class that adds instrumentation to storage operations,
including correlation tracking, performance monitoring, and structured logging.

All concrete backends should inherit from this class to get automatic instrumentation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger
from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlspec.config import InstrumentationConfig
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("InstrumentedObjectStore",)


class InstrumentedObjectStore(ABC):
    """Base class for instrumented storage backends.

    This class provides instrumentation for all storage operations,
    including logging, telemetry, and performance tracking.

    All methods use 'path' terminology consistent with object store patterns.
    Concrete implementations must provide both sync and async operations.
    """

    def __init__(
        self, instrumentation_config: InstrumentationConfig | None = None, backend_name: str | None = None
    ) -> None:
        """Initialize the instrumented storage backend.

        Args:
            instrumentation_config: Instrumentation configuration
            backend_name: Name of the backend for logging
        """
        from sqlspec.config import InstrumentationConfig

        self.instrumentation_config = instrumentation_config or InstrumentationConfig()
        self.backend_name = backend_name or self.__class__.__name__
        self.logger = get_logger(f"storage.{self.backend_name}")

    def _log_operation_start(self, op_name: str, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log the start of an operation if debug mode is enabled."""
        if self.instrumentation_config.log_storage_operations:
            extra = kwargs.setdefault("extra", {})
            extra["correlation_id"] = CorrelationContext.get()
            extra["backend"] = self.backend_type
            self.logger.debug(msg, *args, **kwargs)

    def _log_operation_success(self, op_name: str, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log the successful completion of an operation."""
        if self.instrumentation_config.log_storage_operations:
            extra = kwargs.setdefault("extra", {})
            extra["correlation_id"] = CorrelationContext.get()
            extra["backend"] = self.backend_type
            self.logger.info(msg, *args, **kwargs)

    def _log_operation_error(self, op_name: str, e: Exception, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an error during an operation."""
        extra = kwargs.setdefault("extra", {})
        extra["correlation_id"] = CorrelationContext.get()
        extra["backend"] = self.backend_type
        extra["error_type"] = type(e).__name__
        self.logger.error(msg, *args, **kwargs)

    @property
    def backend_type(self) -> str:
        """Return the backend type identifier.

        Default implementation uses the class name.
        Override if you need a different identifier.
        """
        return self.__class__.__name__.replace("Backend", "").lower()

    # Sync Operations with Instrumentation

    def read_bytes(self, path: str, **kwargs: Any) -> bytes:
        """Read bytes from storage with instrumentation.

        Args:
            path: Path to read from
            **kwargs: Additional backend-specific options

        Returns:
            The bytes read from storage
        """
        op_name = "storage.read_bytes"
        with instrument_operation(self, op_name, "storage", path=path, backend=self.backend_type):
            self._log_operation_start(op_name, "Reading bytes from %s", path, extra={"path": path})
            try:
                data = self._read_bytes(path, **kwargs)
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to read from %s", path, extra={"path": path})
                raise
            else:
                self._log_operation_success(
                    op_name, "Read %d bytes from %s", len(data), path, extra={"path": path, "size_bytes": len(data)}
                )
                return data

    def write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to storage with instrumentation.

        Args:
            path: Path to write to
            data: Bytes to write
            **kwargs: Additional backend-specific options
        """
        op_name = "storage.write_bytes"
        log_attrs = {"path": path, "size_bytes": len(data)}
        with instrument_operation(self, op_name, "storage", **log_attrs, backend=self.backend_type):
            self._log_operation_start(op_name, "Writing %d bytes to %s", len(data), path, extra=log_attrs)
            try:
                self._write_bytes(path, data, **kwargs)
                self._log_operation_success(op_name, "Wrote %d bytes to %s", len(data), path, extra=log_attrs)
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to write to %s", path, extra=log_attrs)
                raise

    def read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage with instrumentation.

        Args:
            path: Path to read from
            encoding: Text encoding
            **kwargs: Additional backend-specific options

        Returns:
            The text read from storage
        """
        op_name = "storage.read_text"
        with instrument_operation(self, op_name, "storage", path=path, encoding=encoding, backend=self.backend_type):
            self._log_operation_start(op_name, "Reading text from %s", path, extra={"path": path, "encoding": encoding})
            try:
                text = self._read_text(path, encoding, **kwargs)
            except Exception as e:
                self._log_operation_error(
                    op_name, e, "Failed to read text from %s", path, extra={"path": path, "encoding": encoding}
                )
                raise
            else:
                self._log_operation_success(
                    op_name,
                    "Read text from %s (%d chars)",
                    path,
                    len(text),
                    extra={"path": path, "char_count": len(text), "encoding": encoding},
                )
                return text

    def write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to storage with instrumentation.

        Args:
            path: Path to write to
            data: Text to write
            encoding: Text encoding
            **kwargs: Additional backend-specific options
        """
        op_name = "storage.write_text"
        log_attrs = {"path": path, "char_count": len(data), "encoding": encoding}
        with instrument_operation(self, op_name, "storage", **log_attrs, backend=self.backend_type):
            self._log_operation_start(op_name, "Writing text to %s (%d chars)", path, len(data), extra=log_attrs)
            try:
                self._write_text(path, data, encoding, **kwargs)
                self._log_operation_success(op_name, "Wrote text to %s (%d chars)", path, len(data), extra=log_attrs)
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to write text to %s", path, extra=log_attrs)
                raise

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects in storage with instrumentation.

        Args:
            prefix: Path prefix to list (empty for root)
            recursive: Whether to list recursively
            **kwargs: Additional backend-specific options

        Returns:
            List of object paths
        """
        op_name = "storage.list_objects"
        with instrument_operation(
            self, op_name, "storage", prefix=prefix, recursive=recursive, backend=self.backend_type
        ):
            self._log_operation_start(
                op_name, "Listing objects with prefix %s", prefix, extra={"prefix": prefix, "recursive": recursive}
            )
            try:
                objects = self._list_objects(prefix, recursive, **kwargs)
            except Exception as e:
                self._log_operation_error(
                    op_name,
                    e,
                    "Failed to list objects with prefix %s",
                    prefix,
                    extra={"prefix": prefix, "recursive": recursive},
                )
                raise
            else:
                self._log_operation_success(
                    op_name,
                    "Listed %d objects with prefix %s",
                    len(objects),
                    prefix,
                    extra={"prefix": prefix, "object_count": len(objects), "recursive": recursive},
                )
                return objects

    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if object exists with instrumentation.

        Args:
            path: Path to check
            **kwargs: Additional backend-specific options

        Returns:
            True if object exists
        """
        op_name = "storage.exists"
        with instrument_operation(self, op_name, "storage", path=path, backend=self.backend_type):
            try:
                exists = self._exists(path, **kwargs)
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to check existence of %s", path, extra={"path": path})
                raise
            else:
                self._log_operation_start(
                    op_name, "Checked existence of %s: %s", path, exists, extra={"path": path, "exists": exists}
                )
                return exists

    def delete(self, path: str, **kwargs: Any) -> None:
        """Delete object with instrumentation.

        Args:
            path: Path to delete
            **kwargs: Additional backend-specific options
        """
        op_name = "storage.delete"
        with instrument_operation(self, op_name, "storage", path=path, backend=self.backend_type):
            self._log_operation_start(op_name, "Deleting %s", path, extra={"path": path})
            try:
                self._delete(path, **kwargs)
                self._log_operation_success(op_name, "Deleted %s", path, extra={"path": path})
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to delete %s", path, extra={"path": path})
                raise

    def copy(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy object with instrumentation.

        Args:
            source: Source path
            destination: Destination path
            **kwargs: Additional backend-specific options
        """
        op_name = "storage.copy"
        log_attrs = {"source": source, "destination": destination}
        with instrument_operation(self, op_name, "storage", **log_attrs, backend=self.backend_type):
            self._log_operation_start(op_name, "Copying %s to %s", source, destination, extra=log_attrs)
            try:
                self._copy(source, destination, **kwargs)
                self._log_operation_success(op_name, "Copied %s to %s", source, destination, extra=log_attrs)
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to copy %s to %s", source, destination, extra=log_attrs)
                raise

    def move(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move object with instrumentation.

        Args:
            source: Source path
            destination: Destination path
            **kwargs: Additional backend-specific options
        """
        op_name = "storage.move"
        log_attrs = {"source": source, "destination": destination}
        with instrument_operation(self, op_name, "storage", **log_attrs, backend=self.backend_type):
            self._log_operation_start(op_name, "Moving %s to %s", source, destination, extra=log_attrs)
            try:
                self._move(source, destination, **kwargs)
                self._log_operation_success(op_name, "Moved %s to %s", source, destination, extra=log_attrs)
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to move %s to %s", source, destination, extra=log_attrs)
                raise

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching pattern with instrumentation.

        Args:
            pattern: Glob pattern
            **kwargs: Additional backend-specific options

        Returns:
            List of matching paths
        """
        op_name = "storage.glob"
        with instrument_operation(self, op_name, "storage", pattern=pattern, backend=self.backend_type):
            self._log_operation_start(op_name, "Globbing %s", pattern, extra={"pattern": pattern})
            try:
                matches = self._glob(pattern, **kwargs)
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to glob %s", pattern, extra={"pattern": pattern})
                raise
            else:
                self._log_operation_success(
                    op_name,
                    "Found %d matches for %s",
                    len(matches),
                    pattern,
                    extra={"pattern": pattern, "match_count": len(matches)},
                )
                return matches

    def get_metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get object metadata with instrumentation.

        Args:
            path: Path to get metadata for
            **kwargs: Additional backend-specific options

        Returns:
            Object metadata
        """
        op_name = "storage.get_metadata"
        with instrument_operation(self, op_name, "storage", path=path, backend=self.backend_type):
            self._log_operation_start(op_name, "Getting metadata for %s", path, extra={"path": path})
            try:
                metadata = self._get_metadata(path, **kwargs)
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to get metadata for %s", path, extra={"path": path})
                raise
            else:
                self._log_operation_start(
                    op_name, "Got metadata for %s", path, extra={"path": path, "metadata": metadata}
                )
                return metadata

    def is_object(self, path: str) -> bool:
        """Check if path is an object."""
        return self._is_object(path)

    def is_path(self, path: str) -> bool:
        """Check if path is a prefix/directory."""
        return self._is_path(path)

    def read_arrow(self, path: str, **kwargs: Any) -> ArrowTable:
        """Read Arrow table with instrumentation.

        Args:
            path: Path to read from
            **kwargs: Additional backend-specific options

        Returns:
            Arrow table
        """
        op_name = "storage.read_arrow"
        with instrument_operation(self, op_name, "storage", path=path, backend=self.backend_type):
            self._log_operation_start(op_name, "Reading Arrow table from %s", path, extra={"path": path})
            try:
                table = self._read_arrow(path, **kwargs)
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to read Arrow table from %s", path, extra={"path": path})
                raise
            else:
                self._log_operation_success(
                    op_name,
                    "Read Arrow table from %s (%d rows)",
                    path,
                    len(table),
                    extra={"path": path, "row_count": len(table), "column_count": len(table.columns)},
                )
                return table

    def write_arrow(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table with instrumentation.

        Args:
            path: Path to write to
            table: Arrow table to write
            **kwargs: Additional backend-specific options
        """
        op_name = "storage.write_arrow"
        log_attrs = {"path": path, "row_count": len(table)}
        with instrument_operation(self, op_name, "storage", **log_attrs, backend=self.backend_type):
            self._log_operation_start(op_name, "Writing Arrow table to %s (%d rows)", path, len(table), extra=log_attrs)
            try:
                self._write_arrow(path, table, **kwargs)
                self._log_operation_success(
                    op_name,
                    "Wrote Arrow table to %s (%d rows)",
                    path,
                    len(table),
                    extra={**log_attrs, "column_count": len(table.columns)},
                )
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to write Arrow table to %s", path, extra=log_attrs)
                raise

    def stream_arrow(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Stream Arrow record batches with instrumentation.

        Args:
            pattern: Pattern to match objects
            **kwargs: Additional backend-specific options

        Yields:
            Iterator of Arrow record batches
        """
        op_name = "storage.stream_arrow"
        with instrument_operation(self, op_name, "storage", pattern=pattern, backend=self.backend_type):
            self._log_operation_success(
                op_name, "Starting Arrow stream for pattern %s", pattern, extra={"pattern": pattern}
            )
            try:
                yield from self._stream_arrow(pattern, **kwargs)
            except Exception as e:
                self._log_operation_error(
                    op_name, e, "Failed to stream Arrow from %s", pattern, extra={"pattern": pattern}
                )
                raise

    # Async Operations with Instrumentation
    # Default implementations use sync-to-async conversion
    # Backends can override for native async support

    async def read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        """Async read bytes from storage."""
        op_name = "storage.read_bytes_async"
        async with instrument_operation_async(self, op_name, "storage", path=path, backend=self.backend_type):
            self._log_operation_start(op_name, "Async reading bytes from %s", path, extra={"path": path})
            try:
                data = await self._read_bytes_async(path, **kwargs)
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to async read from %s", path, extra={"path": path})
                raise
            else:
                self._log_operation_success(
                    op_name,
                    "Async read %d bytes from %s",
                    len(data),
                    path,
                    extra={"path": path, "size_bytes": len(data)},
                )
                return data

    async def write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Async write bytes to storage."""
        op_name = "storage.write_bytes_async"
        log_attrs = {"path": path, "size_bytes": len(data)}
        async with instrument_operation_async(self, op_name, "storage", **log_attrs, backend=self.backend_type):
            self._log_operation_start(op_name, "Async writing %d bytes to %s", len(data), path, extra=log_attrs)
            try:
                await self._write_bytes_async(path, data, **kwargs)
                self._log_operation_success(op_name, "Async wrote %d bytes to %s", len(data), path, extra=log_attrs)
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to async write to %s", path, extra=log_attrs)
                raise

    async def read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Async read text from storage."""
        op_name = "storage.read_text_async"
        log_attrs = {"path": path, "encoding": encoding}
        async with instrument_operation_async(self, op_name, "storage", **log_attrs, backend=self.backend_type):
            self._log_operation_start(
                op_name, "Async reading text from %s (encoding: %s)", path, encoding, extra=log_attrs
            )
            try:
                text = await self._read_text_async(path, encoding, **kwargs)
                self._log_operation_success(
                    op_name,
                    "Async read %d characters from %s",
                    len(text),
                    path,
                    extra={**log_attrs, "char_count": len(text)},
                )

            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to async read text from %s", path, extra=log_attrs)
                raise
            return text

    async def write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Async write text to storage."""
        op_name = "storage.write_text_async"
        log_attrs = {"path": path, "char_count": len(data), "encoding": encoding}
        async with instrument_operation_async(self, op_name, "storage", **log_attrs, backend=self.backend_type):
            self._log_operation_start(
                op_name, "Async writing %d characters to %s (encoding: %s)", len(data), path, encoding, extra=log_attrs
            )
            try:
                await self._write_text_async(path, data, encoding, **kwargs)
                self._log_operation_success(
                    op_name, "Async wrote %d characters to %s", len(data), path, extra=log_attrs
                )
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to async write text to %s", path, extra=log_attrs)
                raise

    async def exists_async(self, path: str, **kwargs: Any) -> bool:
        """Async check if object exists."""
        op_name = "storage.exists_async"
        async with instrument_operation_async(self, op_name, "storage", path=path, backend=self.backend_type):
            self._log_operation_start(op_name, "Async checking existence of %s", path, extra={"path": path})
            try:
                exists = await self._exists_async(path, **kwargs)
                self._log_operation_success(
                    op_name, "Async checked existence of %s: %s", path, exists, extra={"path": path, "exists": exists}
                )
            except Exception as e:
                self._log_operation_error(
                    op_name, e, "Failed to async check existence of %s", path, extra={"path": path}
                )
                raise
            return exists

    async def delete_async(self, path: str, **kwargs: Any) -> None:
        """Async delete object."""
        op_name = "storage.delete_async"
        async with instrument_operation_async(self, op_name, "storage", path=path, backend=self.backend_type):
            self._log_operation_start(op_name, "Async deleting %s", path, extra={"path": path})
            try:
                await self._delete_async(path, **kwargs)
                self._log_operation_success(op_name, "Async deleted %s", path, extra={"path": path})
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to async delete %s", path, extra={"path": path})
                raise

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Async list objects."""
        op_name = "storage.list_objects_async"
        log_attrs = {"prefix": prefix, "recursive": recursive}
        async with instrument_operation_async(self, op_name, "storage", **log_attrs, backend=self.backend_type):
            self._log_operation_start(op_name, "Async listing objects with prefix '%s'", prefix, extra=log_attrs)
            try:
                objects = await self._list_objects_async(prefix, recursive, **kwargs)
                self._log_operation_success(
                    op_name,
                    "Async listed %d objects with prefix '%s'",
                    len(objects),
                    prefix,
                    extra={**log_attrs, "object_count": len(objects)},
                )
            except Exception as e:
                self._log_operation_error(
                    op_name, e, "Failed to async list objects with prefix '%s'", prefix, extra=log_attrs
                )
                raise
        return objects

    async def copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async copy object."""
        op_name = "storage.copy_async"
        log_attrs = {"source": source, "destination": destination}
        async with instrument_operation_async(self, op_name, "storage", **log_attrs, backend=self.backend_type):
            self._log_operation_start(op_name, "Async copying %s to %s", source, destination, extra=log_attrs)
            try:
                await self._copy_async(source, destination, **kwargs)
                self._log_operation_success(op_name, "Async copied %s to %s", source, destination, extra=log_attrs)
            except Exception as e:
                self._log_operation_error(
                    op_name, e, "Failed to async copy %s to %s", source, destination, extra=log_attrs
                )
                raise

    async def move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async move object."""
        op_name = "storage.move_async"
        log_attrs = {"source": source, "destination": destination}
        async with instrument_operation_async(self, op_name, "storage", **log_attrs, backend=self.backend_type):
            self._log_operation_start(op_name, "Async moving %s to %s", source, destination, extra=log_attrs)
            try:
                await self._move_async(source, destination, **kwargs)
                self._log_operation_success(op_name, "Async moved %s to %s", source, destination, extra=log_attrs)
            except Exception as e:
                self._log_operation_error(
                    op_name, e, "Failed to async move %s to %s", source, destination, extra=log_attrs
                )
                raise

    async def get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Async get object metadata."""
        op_name = "storage.get_metadata_async"
        async with instrument_operation_async(self, op_name, "storage", path=path, backend=self.backend_type):
            self._log_operation_start(op_name, "Async getting metadata for %s", path, extra={"path": path})
            try:
                metadata = await self._get_metadata_async(path, **kwargs)
                self._log_operation_success(
                    op_name, "Async got metadata for %s", path, extra={"path": path, "metadata": metadata}
                )
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to async get metadata for %s", path, extra={"path": path})
                raise
            return metadata

    async def read_arrow_async(self, path: str, **kwargs: Any) -> ArrowTable:
        """Async read Arrow table."""
        op_name = "storage.read_arrow_async"
        async with instrument_operation_async(self, op_name, "storage", path=path, backend=self.backend_type):
            self._log_operation_start(op_name, "Async reading Arrow table from %s", path, extra={"path": path})
            try:
                table = await self._read_arrow_async(path, **kwargs)
                self._log_operation_success(
                    op_name,
                    "Async read Arrow table with %d rows from %s",
                    table.num_rows,
                    path,
                    extra={"path": path, "num_rows": table.num_rows, "num_columns": table.num_columns},
                )
            except Exception as e:
                self._log_operation_error(
                    op_name, e, "Failed to async read Arrow table from %s", path, extra={"path": path}
                )
                raise
            return table

    async def write_arrow_async(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Async write Arrow table."""
        op_name = "storage.write_arrow_async"
        log_attrs = {"path": path, "num_rows": table.num_rows, "num_columns": table.num_columns}
        async with instrument_operation_async(self, op_name, "storage", **log_attrs, backend=self.backend_type):
            self._log_operation_start(
                op_name, "Async writing Arrow table with %d rows to %s", table.num_rows, path, extra=log_attrs
            )
            try:
                await self._write_arrow_async(path, table, **kwargs)
                self._log_operation_success(
                    op_name, "Async wrote Arrow table with %d rows to %s", table.num_rows, path, extra=log_attrs
                )
            except Exception as e:
                self._log_operation_error(op_name, e, "Failed to async write Arrow table to %s", path, extra=log_attrs)
                raise

    async def stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator[ArrowRecordBatch]:
        """Async stream Arrow record batches.

        Args:
            pattern: Pattern to match objects
            **kwargs: Additional backend-specific options

        Yields:
            AsyncIterator of Arrow record batches
        """
        op_name = "storage.stream_arrow_async"
        async with instrument_operation_async(self, op_name, "storage", pattern=pattern, backend=self.backend_type):
            self._log_operation_start(
                op_name, "Async streaming Arrow batches for pattern %s", pattern, extra={"pattern": pattern}
            )
            try:
                batch_count = 0
                async for batch in self._stream_arrow_async(pattern, **kwargs):
                    batch_count += 1
                    yield batch

                self._log_operation_success(
                    op_name,
                    "Async streamed %d Arrow batches for pattern %s",
                    batch_count,
                    pattern,
                    extra={"pattern": pattern, "batch_count": batch_count},
                )
            except Exception as e:
                self._log_operation_error(
                    op_name,
                    e,
                    "Failed to async stream Arrow batches for pattern %s",
                    pattern,
                    extra={"pattern": pattern},
                )
                raise

    # Abstract methods that subclasses must implement

    @abstractmethod
    def _read_bytes(self, path: str, **kwargs: Any) -> bytes:
        """Actual implementation of read_bytes in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Actual implementation of write_bytes in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Actual implementation of read_text in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Actual implementation of write_text in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Actual implementation of list_objects in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _exists(self, path: str, **kwargs: Any) -> bool:
        """Actual implementation of exists in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _delete(self, path: str, **kwargs: Any) -> None:
        """Actual implementation of delete in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _copy(self, source: str, destination: str, **kwargs: Any) -> None:
        """Actual implementation of copy in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _move(self, source: str, destination: str, **kwargs: Any) -> None:
        """Actual implementation of move in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Actual implementation of glob in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _get_metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Actual implementation of get_metadata in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _is_object(self, path: str) -> bool:
        """Actual implementation of is_object in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _is_path(self, path: str) -> bool:
        """Actual implementation of is_path in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _read_arrow(self, path: str, **kwargs: Any) -> ArrowTable:
        """Actual implementation of read_arrow in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _write_arrow(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Actual implementation of write_arrow in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _stream_arrow(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Actual implementation of stream_arrow in subclasses."""
        raise NotImplementedError

    # Abstract async methods that subclasses must implement
    # Backends can either provide native async implementations or wrap sync methods

    @abstractmethod
    async def _read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        """Actual async implementation of read_bytes in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def _write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Actual async implementation of write_bytes in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def _read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Actual async implementation of read_text in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def _write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Actual async implementation of write_text in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def _list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Actual async implementation of list_objects in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def _exists_async(self, path: str, **kwargs: Any) -> bool:
        """Actual async implementation of exists in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def _delete_async(self, path: str, **kwargs: Any) -> None:
        """Actual async implementation of delete in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def _copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Actual async implementation of copy in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def _move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Actual async implementation of move in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def _get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Actual async implementation of get_metadata in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def _read_arrow_async(self, path: str, **kwargs: Any) -> ArrowTable:
        """Actual async implementation of read_arrow in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def _write_arrow_async(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Actual async implementation of write_arrow in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator[ArrowRecordBatch]:
        """Actual async implementation of stream_arrow in subclasses."""
        raise NotImplementedError
