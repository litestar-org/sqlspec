# ruff: noqa: PLR0904
"""Base class for instrumented storage backends.

This module provides a base class that adds instrumentation to storage operations,
including correlation tracking, performance monitoring, and structured logging.

All concrete backends should inherit from this class to get automatic instrumentation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Literal

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
        self,
        instrumentation_config: InstrumentationConfig | None = None,
        backend_name: str | None = None,
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
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.read_bytes",
            "storage",
            path=path,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Reading bytes from %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                data = self._read_bytes(path, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to read from %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Read %d bytes from %s",
                        len(data),
                        path,
                        extra={
                            "path": path,
                            "size_bytes": len(data),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return data

    def write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to storage with instrumentation.

        Args:
            path: Path to write to
            data: Bytes to write
            **kwargs: Additional backend-specific options
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.write_bytes",
            "storage",
            path=path,
            size_bytes=len(data),
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Writing %d bytes to %s",
                    len(data),
                    path,
                    extra={
                        "path": path,
                        "size_bytes": len(data),
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                self._write_bytes(path, data, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to write to %s",
                    path,
                    extra={
                        "path": path,
                        "size_bytes": len(data),
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Wrote %d bytes to %s",
                        len(data),
                        path,
                        extra={
                            "path": path,
                            "size_bytes": len(data),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

    def read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage with instrumentation.

        Args:
            path: Path to read from
            encoding: Text encoding
            **kwargs: Additional backend-specific options

        Returns:
            The text read from storage
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.read_text",
            "storage",
            path=path,
            encoding=encoding,
            backend=self.backend_type,
        ):
            try:
                text = self._read_text(path, encoding, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to read text from %s",
                    path,
                    extra={
                        "path": path,
                        "encoding": encoding,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Read text from %s (%d chars)",
                        path,
                        len(text),
                        extra={
                            "path": path,
                            "char_count": len(text),
                            "encoding": encoding,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
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
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.write_text",
            "storage",
            path=path,
            char_count=len(data),
            encoding=encoding,
            backend=self.backend_type,
        ):
            try:
                self._write_text(path, data, encoding, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to write text to %s",
                    path,
                    extra={
                        "path": path,
                        "char_count": len(data),
                        "encoding": encoding,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Wrote text to %s (%d chars)",
                        path,
                        len(data),
                        extra={
                            "path": path,
                            "char_count": len(data),
                            "encoding": encoding,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects in storage with instrumentation.

        Args:
            prefix: Path prefix to list (empty for root)
            recursive: Whether to list recursively
            **kwargs: Additional backend-specific options

        Returns:
            List of object paths
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.list_objects",
            "storage",
            prefix=prefix,
            recursive=recursive,
            backend=self.backend_type,
        ):
            try:
                objects = self._list_objects(prefix, recursive, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to list objects with prefix %s",
                    prefix,
                    extra={
                        "prefix": prefix,
                        "recursive": recursive,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Listed %d objects with prefix %s",
                        len(objects),
                        prefix,
                        extra={
                            "prefix": prefix,
                            "object_count": len(objects),
                            "recursive": recursive,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
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
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.exists",
            "storage",
            path=path,
            backend=self.backend_type,
        ):
            try:
                exists = self._exists(path, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to check existence of %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.debug_mode:
                    self.logger.debug(
                        "Checked existence of %s: %s",
                        path,
                        exists,
                        extra={
                            "path": path,
                            "exists": exists,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return exists

    def delete(self, path: str, **kwargs: Any) -> None:
        """Delete object with instrumentation.

        Args:
            path: Path to delete
            **kwargs: Additional backend-specific options
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.delete",
            "storage",
            path=path,
            backend=self.backend_type,
        ):
            try:
                self._delete(path, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to delete %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Deleted %s",
                        path,
                        extra={
                            "path": path,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

    def copy(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy object with instrumentation.

        Args:
            source: Source path
            destination: Destination path
            **kwargs: Additional backend-specific options
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.copy",
            "storage",
            source=source,
            destination=destination,
            backend=self.backend_type,
        ):
            try:
                self._copy(source, destination, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to copy %s to %s",
                    source,
                    destination,
                    extra={
                        "source": source,
                        "destination": destination,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Copied %s to %s",
                        source,
                        destination,
                        extra={
                            "source": source,
                            "destination": destination,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

    def move(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move object with instrumentation.

        Args:
            source: Source path
            destination: Destination path
            **kwargs: Additional backend-specific options
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.move",
            "storage",
            source=source,
            destination=destination,
            backend=self.backend_type,
        ):
            try:
                self._move(source, destination, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to move %s to %s",
                    source,
                    destination,
                    extra={
                        "source": source,
                        "destination": destination,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Moved %s to %s",
                        source,
                        destination,
                        extra={
                            "source": source,
                            "destination": destination,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching pattern with instrumentation.

        Args:
            pattern: Glob pattern
            **kwargs: Additional backend-specific options

        Returns:
            List of matching paths
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.glob",
            "storage",
            pattern=pattern,
            backend=self.backend_type,
        ):
            try:
                matches = self._glob(pattern, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to glob %s",
                    pattern,
                    extra={
                        "pattern": pattern,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Found %d matches for %s",
                        len(matches),
                        pattern,
                        extra={
                            "pattern": pattern,
                            "match_count": len(matches),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
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
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.get_metadata",
            "storage",
            path=path,
            backend=self.backend_type,
        ):
            try:
                metadata = self._get_metadata(path, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to get metadata for %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.debug_mode:
                    self.logger.debug(
                        "Got metadata for %s",
                        path,
                        extra={
                            "path": path,
                            "metadata": metadata,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return metadata

    def get_signed_url(
        self,
        path: str,
        operation: Literal["read", "write"] = "read",
        expires_in: int = 3600,
        **kwargs: Any,
    ) -> str:
        """Generate signed URL with instrumentation.

        Args:
            path: Path to sign
            operation: Operation type
            expires_in: Expiration in seconds
            **kwargs: Additional backend-specific options

        Returns:
            Signed URL
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.get_signed_url",
            "storage",
            path=path,
            operation=operation,
            expires_in=expires_in,
            backend=self.backend_type,
        ):
            try:
                url = self._get_signed_url(path, operation, expires_in, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to get signed URL for %s",
                    path,
                    extra={
                        "path": path,
                        "operation": operation,
                        "expires_in": expires_in,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Generated signed URL for %s (%s)",
                        path,
                        operation,
                        extra={
                            "path": path,
                            "operation": operation,
                            "expires_in": expires_in,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return url

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
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.read_arrow",
            "storage",
            path=path,
            backend=self.backend_type,
        ):
            try:
                table = self._read_arrow(path, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to read Arrow table from %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Read Arrow table from %s (%d rows)",
                        path,
                        len(table),
                        extra={
                            "path": path,
                            "row_count": len(table),
                            "column_count": len(table.columns),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return table

    def write_arrow(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table with instrumentation.

        Args:
            path: Path to write to
            table: Arrow table to write
            **kwargs: Additional backend-specific options
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.write_arrow",
            "storage",
            path=path,
            row_count=len(table),
            backend=self.backend_type,
        ):
            try:
                self._write_arrow(path, table, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to write Arrow table to %s",
                    path,
                    extra={
                        "path": path,
                        "row_count": len(table),
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Wrote Arrow table to %s (%d rows)",
                        path,
                        len(table),
                        extra={
                            "path": path,
                            "row_count": len(table),
                            "column_count": len(table.columns),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

    def stream_arrow(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Stream Arrow record batches with instrumentation.

        Args:
            pattern: Pattern to match objects
            **kwargs: Additional backend-specific options

        Yields:
            Iterator of Arrow record batches
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.stream_arrow",
            "storage",
            pattern=pattern,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.log_service_operations:
                self.logger.info(
                    "Starting Arrow stream for pattern %s",
                    pattern,
                    extra={
                        "pattern": pattern,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                yield from self._stream_arrow(pattern, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to stream Arrow from %s",
                    pattern,
                    extra={
                        "pattern": pattern,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise

    # Async Operations with Instrumentation
    # Default implementations use sync-to-async conversion
    # Backends can override for native async support

    async def read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        """Async read bytes from storage."""
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.read_bytes_async",
            "storage",
            path=path,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async reading bytes from %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                data = await self._read_bytes_async(path, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to async read from %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async read %d bytes from %s",
                        len(data),
                        path,
                        extra={
                            "path": path,
                            "size_bytes": len(data),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return data

    async def write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Async write bytes to storage."""
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.write_bytes_async",
            "storage",
            path=path,
            size_bytes=len(data),
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async writing %d bytes to %s",
                    len(data),
                    path,
                    extra={
                        "path": path,
                        "size_bytes": len(data),
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                await self._write_bytes_async(path, data, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to async write to %s",
                    path,
                    extra={
                        "path": path,
                        "size_bytes": len(data),
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async wrote %d bytes to %s",
                        len(data),
                        path,
                        extra={
                            "path": path,
                            "size_bytes": len(data),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

    async def read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Async read text from storage."""
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.read_text_async",
            "storage",
            path=path,
            encoding=encoding,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async reading text from %s (encoding: %s)",
                    path,
                    encoding,
                    extra={
                        "path": path,
                        "encoding": encoding,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                text = await self._read_text_async(path, encoding, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to async read text from %s",
                    path,
                    extra={
                        "path": path,
                        "encoding": encoding,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async read %d characters from %s",
                        len(text),
                        path,
                        extra={
                            "path": path,
                            "encoding": encoding,
                            "char_count": len(text),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return text

    async def write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Async write text to storage."""
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.write_text_async",
            "storage",
            path=path,
            char_count=len(data),
            encoding=encoding,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async writing %d characters to %s (encoding: %s)",
                    len(data),
                    path,
                    encoding,
                    extra={
                        "path": path,
                        "char_count": len(data),
                        "encoding": encoding,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                await self._write_text_async(path, data, encoding, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to async write text to %s",
                    path,
                    extra={
                        "path": path,
                        "char_count": len(data),
                        "encoding": encoding,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async wrote %d characters to %s",
                        len(data),
                        path,
                        extra={
                            "path": path,
                            "char_count": len(data),
                            "encoding": encoding,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

    async def exists_async(self, path: str, **kwargs: Any) -> bool:
        """Async check if object exists."""
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.exists_async",
            "storage",
            path=path,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async checking existence of %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                exists = await self._exists_async(path, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to async check existence of %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async checked existence of %s: %s",
                        path,
                        exists,
                        extra={
                            "path": path,
                            "exists": exists,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return exists

    async def delete_async(self, path: str, **kwargs: Any) -> None:
        """Async delete object."""
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.delete_async",
            "storage",
            path=path,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async deleting %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                await self._delete_async(path, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to async delete %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async deleted %s",
                        path,
                        extra={
                            "path": path,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Async list objects."""
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.list_objects_async",
            "storage",
            prefix=prefix,
            recursive=recursive,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async listing objects with prefix '%s'",
                    prefix,
                    extra={
                        "prefix": prefix,
                        "recursive": recursive,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                objects = await self._list_objects_async(prefix, recursive, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to async list objects with prefix '%s'",
                    prefix,
                    extra={
                        "prefix": prefix,
                        "recursive": recursive,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async listed %d objects with prefix '%s'",
                        len(objects),
                        prefix,
                        extra={
                            "prefix": prefix,
                            "recursive": recursive,
                            "object_count": len(objects),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return objects

    async def copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async copy object."""
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.copy_async",
            "storage",
            source=source,
            destination=destination,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async copying %s to %s",
                    source,
                    destination,
                    extra={
                        "source": source,
                        "destination": destination,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                await self._copy_async(source, destination, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to async copy %s to %s",
                    source,
                    destination,
                    extra={
                        "source": source,
                        "destination": destination,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async copied %s to %s",
                        source,
                        destination,
                        extra={
                            "source": source,
                            "destination": destination,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

    async def move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async move object."""
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.move_async",
            "storage",
            source=source,
            destination=destination,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async moving %s to %s",
                    source,
                    destination,
                    extra={
                        "source": source,
                        "destination": destination,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                await self._move_async(source, destination, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to async move %s to %s",
                    source,
                    destination,
                    extra={
                        "source": source,
                        "destination": destination,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async moved %s to %s",
                        source,
                        destination,
                        extra={
                            "source": source,
                            "destination": destination,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

    async def get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Async get object metadata."""
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.get_metadata_async",
            "storage",
            path=path,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async getting metadata for %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                metadata = await self._get_metadata_async(path, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to async get metadata for %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async got metadata for %s",
                        path,
                        extra={
                            "path": path,
                            "metadata": metadata,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return metadata

    async def read_arrow_async(self, path: str, **kwargs: Any) -> ArrowTable:
        """Async read Arrow table."""
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.read_arrow_async",
            "storage",
            path=path,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async reading Arrow table from %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                table = await self._read_arrow_async(path, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to async read Arrow table from %s",
                    path,
                    extra={
                        "path": path,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async read Arrow table with %d rows from %s",
                        table.num_rows,
                        path,
                        extra={
                            "path": path,
                            "num_rows": table.num_rows,
                            "num_columns": table.num_columns,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return table

    async def write_arrow_async(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Async write Arrow table."""
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.write_arrow_async",
            "storage",
            path=path,
            num_rows=table.num_rows,
            num_columns=table.num_columns,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async writing Arrow table with %d rows to %s",
                    table.num_rows,
                    path,
                    extra={
                        "path": path,
                        "num_rows": table.num_rows,
                        "num_columns": table.num_columns,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                await self._write_arrow_async(path, table, **kwargs)
            except Exception as e:
                self.logger.exception(
                    "Failed to async write Arrow table to %s",
                    path,
                    extra={
                        "path": path,
                        "num_rows": table.num_rows,
                        "num_columns": table.num_columns,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise
            else:
                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async wrote Arrow table with %d rows to %s",
                        table.num_rows,
                        path,
                        extra={
                            "path": path,
                            "num_rows": table.num_rows,
                            "num_columns": table.num_columns,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

    async def stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator[ArrowRecordBatch]:
        """Async stream Arrow record batches.

        Args:
            pattern: Pattern to match objects
            **kwargs: Additional backend-specific options

        Yields:
            AsyncIterator of Arrow record batches
        """
        correlation_id = CorrelationContext.get()

        async with instrument_operation_async(
            self,
            "storage.stream_arrow_async",
            "storage",
            pattern=pattern,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Async streaming Arrow batches for pattern %s",
                    pattern,
                    extra={
                        "pattern": pattern,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                batch_count = 0
                async for batch in self._stream_arrow_async(pattern, **kwargs):  # type: ignore
                    batch_count += 1
                    yield batch

                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Async streamed %d Arrow batches for pattern %s",
                        batch_count,
                        pattern,
                        extra={
                            "pattern": pattern,
                            "batch_count": batch_count,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )
            except Exception as e:
                self.logger.exception(
                    "Failed to async stream Arrow batches for pattern %s",
                    pattern,
                    extra={
                        "pattern": pattern,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
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
    def _get_signed_url(
        self,
        path: str,
        operation: Literal["read", "write"] = "read",
        expires_in: int = 3600,
        **kwargs: Any,
    ) -> str:
        """Actual implementation of get_signed_url in subclasses."""
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
    async def _stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator[ArrowRecordBatch]:
        """Actual async implementation of stream_arrow in subclasses."""
        raise NotImplementedError
