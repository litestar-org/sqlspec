"""Base class for storage backends."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator
from typing import Any

from mypy_extensions import mypyc_attr

from sqlspec.storage.backends._iterators import (
    AsyncArrowBatchIterator,
    AsyncBytesIterator,
    AsyncChunkedBytesIterator,
    AsyncObStoreStreamIterator,
    AsyncThreadedBytesIterator,
)
from sqlspec.typing import ArrowRecordBatch, ArrowTable
from sqlspec.utils.sync_tools import CapacityLimiter

__all__ = (
    "AsyncArrowBatchIterator",
    "AsyncBytesIterator",
    "AsyncChunkedBytesIterator",
    "AsyncObStoreStreamIterator",
    "AsyncThreadedBytesIterator",
    "ObjectStoreBase",
)

# Dedicated capacity limiter for storage I/O operations (100 concurrent ops)
# This is shared across all storage backends to prevent overwhelming the system
storage_limiter = CapacityLimiter(100)


@mypyc_attr(allow_interpreted_subclasses=True)
class ObjectStoreBase(ABC):
    """Base class for storage backends.

    All synchronous methods follow the *_sync naming convention for consistency
    with their async counterparts.
    """

    __slots__ = ()

    @abstractmethod
    def read_bytes_sync(self, path: str, **kwargs: Any) -> bytes:
        """Read bytes from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def write_bytes_sync(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def stream_read_sync(self, path: str, chunk_size: "int | None" = None, **kwargs: Any) -> Iterator[bytes]:
        """Stream bytes from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def read_text_sync(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def write_text_sync(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def list_objects_sync(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
        """List objects in storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def exists_sync(self, path: str, **kwargs: Any) -> bool:
        """Check if object exists in storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def delete_sync(self, path: str, **kwargs: Any) -> None:
        """Delete object from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def copy_sync(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy object within storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def move_sync(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move object within storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def glob_sync(self, pattern: str, **kwargs: Any) -> "list[str]":
        """Find objects matching pattern synchronously."""
        raise NotImplementedError

    @abstractmethod
    def get_metadata_sync(self, path: str, **kwargs: Any) -> "dict[str, object]":
        """Get object metadata from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def is_object_sync(self, path: str) -> bool:
        """Check if path points to an object synchronously."""
        raise NotImplementedError

    @abstractmethod
    def is_path_sync(self, path: str) -> bool:
        """Check if path points to a directory synchronously."""
        raise NotImplementedError

    @abstractmethod
    def read_arrow_sync(self, path: str, **kwargs: Any) -> ArrowTable:
        """Read Arrow table from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def write_arrow_sync(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table to storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    def stream_arrow_sync(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Stream Arrow record batches from storage synchronously."""
        raise NotImplementedError

    @abstractmethod
    async def read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        """Read bytes from storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def stream_read_async(
        self, path: str, chunk_size: "int | None" = None, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        """Stream bytes from storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
        """List objects in storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def exists_async(self, path: str, **kwargs: Any) -> bool:
        """Check if object exists in storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def delete_async(self, path: str, **kwargs: Any) -> None:
        """Delete object from storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy object within storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move object within storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def get_metadata_async(self, path: str, **kwargs: Any) -> "dict[str, object]":
        """Get object metadata from storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def read_arrow_async(self, path: str, **kwargs: Any) -> ArrowTable:
        """Read Arrow table from storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def write_arrow_async(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table to storage asynchronously."""
        raise NotImplementedError

    @abstractmethod
    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator[ArrowRecordBatch]:
        """Stream Arrow record batches from storage asynchronously."""
        raise NotImplementedError
