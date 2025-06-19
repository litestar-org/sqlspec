from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("ObjectStoreProtocol",)


@runtime_checkable
class ObjectStoreProtocol(Protocol):
    """Unified protocol for object storage operations.

    This protocol defines the interface for all storage backends with built-in
    instrumentation support. Backends must implement both sync and async operations
    where possible, with async operations suffixed with _async.

    All methods use 'path' terminology for consistency with object store patterns.
    """

    def __init__(self, uri: str, **kwargs: Any) -> None:
        return

    # Core Operations (sync)
    def read_bytes(self, path: str, **kwargs: Any) -> bytes:
        """Read bytes from an object."""
        return b""

    def write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to an object."""
        return

    def read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from an object."""
        return ""

    def write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to an object."""
        return

    # Object Operations
    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if an object exists."""
        return False

    def delete(self, path: str, **kwargs: Any) -> None:
        """Delete an object."""
        return

    def copy(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy an object."""
        return

    def move(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move an object."""
        return

    # Listing Operations
    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects with optional prefix."""
        return []

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching a glob pattern."""
        return []

    # Path Operations
    def is_object(self, path: str) -> bool:
        """Check if path points to an object."""
        return False

    def is_path(self, path: str) -> bool:
        """Check if path points to a prefix (directory-like)."""
        return False

    def get_metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get object metadata."""
        return {}

    # Arrow Operations
    def read_arrow(self, path: str, **kwargs: Any) -> "ArrowTable":
        """Read an Arrow table from storage.

        For obstore backend, this should use native arrow operations when available.
        """
        msg = "Arrow reading not implemented"
        raise NotImplementedError(msg)

    def write_arrow(self, path: str, table: "ArrowTable", **kwargs: Any) -> None:
        """Write an Arrow table to storage.

        For obstore backend, this should use native arrow operations when available.
        """
        msg = "Arrow writing not implemented"
        raise NotImplementedError(msg)

    def stream_arrow(self, pattern: str, **kwargs: Any) -> "Iterator[ArrowRecordBatch]":
        """Stream Arrow record batches from matching objects.

        For obstore backend, this should use native streaming when available.
        """
        msg = "Arrow streaming not implemented"
        raise NotImplementedError(msg)

    # Async versions
    async def read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        """Async read bytes from an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Async write bytes to an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Async read text from an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Async write text to an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def exists_async(self, path: str, **kwargs: Any) -> bool:
        """Async check if an object exists."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def delete_async(self, path: str, **kwargs: Any) -> None:
        """Async delete an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Async list objects with optional prefix."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async copy an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async move an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Async get object metadata."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def read_arrow_async(self, path: str, **kwargs: Any) -> "ArrowTable":
        """Async read an Arrow table from storage."""
        msg = "Async arrow reading not implemented"
        raise NotImplementedError(msg)

    async def write_arrow_async(self, path: str, table: "ArrowTable", **kwargs: Any) -> None:
        """Async write an Arrow table to storage."""
        msg = "Async arrow writing not implemented"
        raise NotImplementedError(msg)

    async def stream_arrow_async(self, pattern: str, **kwargs: Any) -> "AsyncIterator[ArrowRecordBatch]":
        """Async stream Arrow record batches from matching objects."""
        msg = "Async arrow streaming not implemented"
        raise NotImplementedError(msg)
