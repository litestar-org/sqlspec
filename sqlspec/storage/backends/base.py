"""Base class for storage backends."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator
from typing import Any

from mypy_extensions import mypyc_attr

from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("ObjectStoreBase",)


@mypyc_attr(allow_interpreted_subclasses=True)
class ObjectStoreBase(ABC):
    """Base class for storage backends."""

    __slots__ = ()

    @abstractmethod
    def read_bytes(self, path: str, **kwargs: Any) -> bytes:
        """Read bytes from storage."""
        raise NotImplementedError

    @abstractmethod
    def write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to storage."""
        raise NotImplementedError

    @abstractmethod
    def read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage."""
        raise NotImplementedError

    @abstractmethod
    def write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to storage."""
        raise NotImplementedError

    @abstractmethod
    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects in storage."""
        raise NotImplementedError

    @abstractmethod
    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if object exists in storage."""
        raise NotImplementedError

    @abstractmethod
    def delete(self, path: str, **kwargs: Any) -> None:
        """Delete object from storage."""
        raise NotImplementedError

    @abstractmethod
    def copy(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy object within storage."""
        raise NotImplementedError

    @abstractmethod
    def move(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move object within storage."""
        raise NotImplementedError

    @abstractmethod
    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching pattern."""
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get object metadata from storage."""
        raise NotImplementedError

    @abstractmethod
    def is_object(self, path: str) -> bool:
        """Check if path points to an object."""
        raise NotImplementedError

    @abstractmethod
    def is_path(self, path: str) -> bool:
        """Check if path points to a directory."""
        raise NotImplementedError

    @abstractmethod
    def read_arrow(self, path: str, **kwargs: Any) -> ArrowTable:
        """Read Arrow table from storage."""
        raise NotImplementedError

    @abstractmethod
    def write_arrow(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table to storage."""
        raise NotImplementedError

    @abstractmethod
    def stream_arrow(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Stream Arrow record batches from storage."""
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
    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
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
    async def get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
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
