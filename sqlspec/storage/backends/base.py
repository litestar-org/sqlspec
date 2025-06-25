"""Base class for storage backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("ObjectStoreBase",)


class ObjectStoreBase(ABC):
    """Base class for instrumented storage backends."""

    # Sync Operations
    @abstractmethod
    def read_bytes(self, path: str, **kwargs: Any) -> bytes:
        """Actual implementation of read_bytes in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Actual implementation of write_bytes in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Actual implementation of read_text in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Actual implementation of write_text in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Actual implementation of list_objects in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def exists(self, path: str, **kwargs: Any) -> bool:
        """Actual implementation of exists in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def delete(self, path: str, **kwargs: Any) -> None:
        """Actual implementation of delete in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def copy(self, source: str, destination: str, **kwargs: Any) -> None:
        """Actual implementation of copy in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def move(self, source: str, destination: str, **kwargs: Any) -> None:
        """Actual implementation of move in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Actual implementation of glob in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Actual implementation of get_metadata in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def is_object(self, path: str) -> bool:
        """Actual implementation of is_object in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def is_path(self, path: str) -> bool:
        """Actual implementation of is_path in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def read_arrow(self, path: str, **kwargs: Any) -> ArrowTable:
        """Actual implementation of read_arrow in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def write_arrow(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Actual implementation of write_arrow in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def stream_arrow(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Actual implementation of stream_arrow in subclasses."""
        raise NotImplementedError

    # Abstract async methods that subclasses must implement
    # Backends can either provide native async implementations or wrap sync methods

    @abstractmethod
    async def read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        """Actual async implementation of read_bytes in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Actual async implementation of write_bytes in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Actual async implementation of read_text in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Actual async implementation of write_text in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Actual async implementation of list_objects in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def exists_async(self, path: str, **kwargs: Any) -> bool:
        """Actual async implementation of exists in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def delete_async(self, path: str, **kwargs: Any) -> None:
        """Actual async implementation of delete in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Actual async implementation of copy in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Actual async implementation of move in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Actual async implementation of get_metadata in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def read_arrow_async(self, path: str, **kwargs: Any) -> ArrowTable:
        """Actual async implementation of read_arrow in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def write_arrow_async(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Actual async implementation of write_arrow in subclasses."""
        raise NotImplementedError

    @abstractmethod
    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator[ArrowRecordBatch]:
        """Actual async implementation of stream_arrow in subclasses."""
        raise NotImplementedError
