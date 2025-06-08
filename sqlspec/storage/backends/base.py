"""Base class for instrumented storage backends.

This module provides a base class that adds instrumentation to storage operations,
including correlation tracking, performance monitoring, and structured logging.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger
from sqlspec.utils.telemetry import instrument_operation

if TYPE_CHECKING:
    import pyarrow as pa

    from sqlspec.config import InstrumentationConfig

__all__ = ("InstrumentedStorageBackend",)


class InstrumentedStorageBackend(ABC):
    """Base class for instrumented storage backends.

    This class provides instrumentation for all storage operations,
    including logging, telemetry, and performance tracking.
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
    @abstractmethod
    def backend_type(self) -> str:
        """Return the backend type identifier."""
        raise NotImplementedError

    def read_bytes(self, uri: str, **kwargs: Any) -> bytes:
        """Read bytes from storage with instrumentation.

        Args:
            uri: URI to read from
            **kwargs: Additional backend-specific options

        Returns:
            The bytes read from storage
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.read_bytes",
            "storage",
            uri=uri,
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Reading bytes from %s",
                    uri,
                    extra={
                        "uri": uri,
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                data = self._read_bytes(uri, **kwargs)

                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Read %d bytes from %s",
                        len(data),
                        uri,
                        extra={
                            "uri": uri,
                            "size_bytes": len(data),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return data

            except Exception as e:
                self.logger.exception(
                    "Failed to read from %s",
                    uri,
                    extra={
                        "uri": uri,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise

    def write_bytes(self, uri: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to storage with instrumentation.

        Args:
            uri: URI to write to
            data: Bytes to write
            **kwargs: Additional backend-specific options
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.write_bytes",
            "storage",
            uri=uri,
            size_bytes=len(data),
            backend=self.backend_type,
        ):
            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "Writing %d bytes to %s",
                    len(data),
                    uri,
                    extra={
                        "uri": uri,
                        "size_bytes": len(data),
                        "backend": self.backend_type,
                        "correlation_id": correlation_id,
                    },
                )

            try:
                self._write_bytes(uri, data, **kwargs)

                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Wrote %d bytes to %s",
                        len(data),
                        uri,
                        extra={
                            "uri": uri,
                            "size_bytes": len(data),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

            except Exception as e:
                self.logger.exception(
                    "Failed to write to %s",
                    uri,
                    extra={
                        "uri": uri,
                        "size_bytes": len(data),
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise

    def read_text(self, uri: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage with instrumentation.

        Args:
            uri: URI to read from
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
            uri=uri,
            encoding=encoding,
            backend=self.backend_type,
        ):
            try:
                text = self._read_text(uri, encoding, **kwargs)

                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Read text from %s (%d chars)",
                        uri,
                        len(text),
                        extra={
                            "uri": uri,
                            "char_count": len(text),
                            "encoding": encoding,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return text

            except Exception as e:
                self.logger.exception(
                    "Failed to read text from %s",
                    uri,
                    extra={
                        "uri": uri,
                        "encoding": encoding,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise

    def write_text(self, uri: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to storage with instrumentation.

        Args:
            uri: URI to write to
            data: Text to write
            encoding: Text encoding
            **kwargs: Additional backend-specific options
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.write_text",
            "storage",
            uri=uri,
            char_count=len(data),
            encoding=encoding,
            backend=self.backend_type,
        ):
            try:
                self._write_text(uri, data, encoding, **kwargs)

                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Wrote text to %s (%d chars)",
                        uri,
                        len(data),
                        extra={
                            "uri": uri,
                            "char_count": len(data),
                            "encoding": encoding,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

            except Exception as e:
                self.logger.exception(
                    "Failed to write text to %s",
                    uri,
                    extra={
                        "uri": uri,
                        "char_count": len(data),
                        "encoding": encoding,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise

    def list_objects(self, uri: str, recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects in storage with instrumentation.

        Args:
            uri: URI to list
            recursive: Whether to list recursively
            **kwargs: Additional backend-specific options

        Returns:
            List of object URIs
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.list_objects",
            "storage",
            uri=uri,
            recursive=recursive,
            backend=self.backend_type,
        ):
            try:
                objects = self._list_objects(uri, recursive, **kwargs)

                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Listed %d objects in %s",
                        len(objects),
                        uri,
                        extra={
                            "uri": uri,
                            "object_count": len(objects),
                            "recursive": recursive,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return objects

            except Exception as e:
                self.logger.exception(
                    "Failed to list objects in %s",
                    uri,
                    extra={
                        "uri": uri,
                        "recursive": recursive,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise

    def exists(self, uri: str, **kwargs: Any) -> bool:
        """Check if object exists with instrumentation.

        Args:
            uri: URI to check
            **kwargs: Additional backend-specific options

        Returns:
            True if object exists
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.exists",
            "storage",
            uri=uri,
            backend=self.backend_type,
        ):
            try:
                exists = self._exists(uri, **kwargs)

                if self.instrumentation_config.debug_mode:
                    self.logger.debug(
                        "Checked existence of %s: %s",
                        uri,
                        exists,
                        extra={
                            "uri": uri,
                            "exists": exists,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return exists

            except Exception as e:
                self.logger.exception(
                    "Failed to check existence of %s",
                    uri,
                    extra={
                        "uri": uri,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise

    def delete(self, uri: str, **kwargs: Any) -> None:
        """Delete object with instrumentation.

        Args:
            uri: URI to delete
            **kwargs: Additional backend-specific options
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.delete",
            "storage",
            uri=uri,
            backend=self.backend_type,
        ):
            try:
                self._delete(uri, **kwargs)

                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Deleted %s",
                        uri,
                        extra={
                            "uri": uri,
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

            except Exception as e:
                self.logger.exception(
                    "Failed to delete %s",
                    uri,
                    extra={
                        "uri": uri,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise

    def read_arrow(self, uri: str, **kwargs: Any) -> pa.Table:
        """Read Arrow table with instrumentation.

        Args:
            uri: URI to read from
            **kwargs: Additional backend-specific options

        Returns:
            Arrow table
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.read_arrow",
            "storage",
            uri=uri,
            backend=self.backend_type,
        ):
            try:
                table = self._read_arrow(uri, **kwargs)

                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Read Arrow table from %s (%d rows)",
                        uri,
                        len(table),
                        extra={
                            "uri": uri,
                            "row_count": len(table),
                            "column_count": len(table.columns),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

                return table

            except Exception as e:
                self.logger.exception(
                    "Failed to read Arrow table from %s",
                    uri,
                    extra={
                        "uri": uri,
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise

    def write_arrow(self, uri: str, table: pa.Table, **kwargs: Any) -> None:
        """Write Arrow table with instrumentation.

        Args:
            uri: URI to write to
            table: Arrow table to write
            **kwargs: Additional backend-specific options
        """
        correlation_id = CorrelationContext.get()

        with instrument_operation(
            self,
            "storage.write_arrow",
            "storage",
            uri=uri,
            row_count=len(table),
            backend=self.backend_type,
        ):
            try:
                self._write_arrow(uri, table, **kwargs)

                if self.instrumentation_config.log_service_operations:
                    self.logger.info(
                        "Wrote Arrow table to %s (%d rows)",
                        uri,
                        len(table),
                        extra={
                            "uri": uri,
                            "row_count": len(table),
                            "column_count": len(table.columns),
                            "backend": self.backend_type,
                            "correlation_id": correlation_id,
                        },
                    )

            except Exception as e:
                self.logger.exception(
                    "Failed to write Arrow table to %s",
                    uri,
                    extra={
                        "uri": uri,
                        "row_count": len(table),
                        "backend": self.backend_type,
                        "error_type": type(e).__name__,
                        "correlation_id": correlation_id,
                    },
                )
                raise

    # Abstract methods that subclasses must implement

    @abstractmethod
    def _read_bytes(self, uri: str, **kwargs: Any) -> bytes:
        """Actual implementation of read_bytes in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _write_bytes(self, uri: str, data: bytes, **kwargs: Any) -> None:
        """Actual implementation of write_bytes in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _read_text(self, uri: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Actual implementation of read_text in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _write_text(self, uri: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Actual implementation of write_text in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _list_objects(self, uri: str, recursive: bool = True, **kwargs: Any) -> list[str]:
        """Actual implementation of list_objects in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _exists(self, uri: str, **kwargs: Any) -> bool:
        """Actual implementation of exists in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _delete(self, uri: str, **kwargs: Any) -> None:
        """Actual implementation of delete in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _read_arrow(self, uri: str, **kwargs: Any) -> pa.Table:
        """Actual implementation of read_arrow in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _write_arrow(self, uri: str, table: pa.Table, **kwargs: Any) -> None:
        """Actual implementation of write_arrow in subclasses."""
        raise NotImplementedError
