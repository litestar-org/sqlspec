from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    import pyarrow as pa

    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("ObjectStoreProtocol", "StorageBackendProtocol")


@runtime_checkable
class ObjectStoreProtocol(Protocol):
    """Unified protocol for object storage operations.

    This protocol defines the modern interface for all storage backends,
    using object store terminology consistent with cloud storage patterns.
    """

    # Core Operations (sync)
    def read_bytes(self, path: str) -> bytes:
        """Read bytes from an object."""
        return b""

    def write_bytes(self, path: str, data: bytes) -> None:
        """Write bytes to an object."""
        return

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        """Read text from an object."""
        return ""

    def write_text(self, path: str, data: str, encoding: str = "utf-8") -> None:
        """Write text to an object."""
        return

    # Object Operations
    def exists(self, path: str) -> bool:
        """Check if an object exists."""
        return False

    def delete(self, path: str) -> None:
        """Delete an object."""
        return

    def copy(self, source: str, destination: str) -> None:
        """Copy an object."""
        return

    def move(self, source: str, destination: str) -> None:
        """Move an object."""
        return

    # Listing Operations
    def list_objects(self, prefix: str = "", recursive: bool = True) -> list[str]:
        """List objects with optional prefix."""
        return []

    def glob(self, pattern: str) -> list[str]:
        """Find objects matching a glob pattern."""
        return []

    # Path Operations
    def is_object(self, path: str) -> bool:
        """Check if path points to an object."""
        return False

    def is_path(self, path: str) -> bool:
        """Check if path points to a prefix (directory-like)."""
        return False

    def get_metadata(self, path: str) -> dict[str, Any]:
        """Get object metadata."""
        return {}

    # Arrow Operations
    def read_arrow(self, path: str, **kwargs: Any) -> "ArrowTable":
        """Read an Arrow table from storage."""
        msg = "Arrow reading not implemented"
        raise NotImplementedError(msg)

    def write_arrow(self, path: str, table: "ArrowTable", **kwargs: Any) -> None:
        """Write an Arrow table to storage."""
        msg = "Arrow writing not implemented"
        raise NotImplementedError(msg)

    def stream_arrow(self, pattern: str) -> "Iterator[ArrowRecordBatch]":
        """Stream Arrow record batches from matching objects."""
        msg = "Arrow streaming not implemented"
        raise NotImplementedError(msg)

    # Async versions (optional, backend can raise NotImplementedError)
    async def read_bytes_async(self, path: str) -> bytes:
        """Async read bytes from an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def write_bytes_async(self, path: str, data: bytes) -> None:
        """Async write bytes to an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def read_text_async(self, path: str, encoding: str = "utf-8") -> str:
        """Async read text from an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def write_text_async(self, path: str, data: str, encoding: str = "utf-8") -> None:
        """Async write text to an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def exists_async(self, path: str) -> bool:
        """Async check if an object exists."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def delete_async(self, path: str) -> None:
        """Async delete an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True) -> list[str]:
        """Async list objects with optional prefix."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def stream_arrow_async(self, pattern: str) -> "AsyncIterator[ArrowRecordBatch]":
        """Async stream Arrow record batches from matching objects."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)


@runtime_checkable
class StorageBackendProtocol(Protocol):
    """Protocol for storage backends (file, S3, GCS, etc)."""

    def read_bytes(self, uri: str, **kwargs: Any) -> bytes:
        """Read bytes from a URI.

        Args:
            uri: The URI to read from.
            **kwargs: Backend-specific options.

        Returns:
            The bytes read from the URI.
        """
        return b""

    def write_bytes(self, uri: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to a URI.

        Args:
            uri: The URI to write to.
            data: The bytes to write.
            **kwargs: Backend-specific options.
        """
        return

    def read_text(self, uri: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from a URI.

        Args:
            uri: The URI to read from.
            encoding: The text encoding.
            **kwargs: Backend-specific options.

        Returns:
            The text read from the URI.
        """
        return ""

    def write_text(self, uri: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to a URI.

        Args:
            uri: The URI to write to.
            data: The text to write.
            encoding: The text encoding.
            **kwargs: Backend-specific options.
        """
        return

    def read_arrow(self, uri: str, **kwargs: Any) -> "pa.Table":
        """Read an Arrow table from a URI.

        Args:
            uri: The URI to read from.
            **kwargs: Backend-specific options.

        Returns:
            The Arrow table.

        Raises:
            NotImplementedError: If not supported by backend.
        """
        msg = "Arrow reading not supported by this backend"
        raise NotImplementedError(msg)

    def write_arrow(self, uri: str, table: "pa.Table", **kwargs: Any) -> None:
        """Write an Arrow table to a URI.

        Args:
            uri: The URI to write to.
            table: The Arrow table to write.
            **kwargs: Backend-specific options.

        Raises:
            NotImplementedError: If not supported by backend.
        """
        msg = "Arrow writing not supported by this backend"
        raise NotImplementedError(msg)

    def exists(self, uri: str, **kwargs: Any) -> bool:
        """Check if a URI exists.

        Args:
            uri: The URI to check.
            **kwargs: Backend-specific options.

        Returns:
            True if the URI exists, False otherwise.
        """
        return False

    def delete(self, uri: str, **kwargs: Any) -> None:
        """Delete a URI.

        Args:
            uri: The URI to delete.
            **kwargs: Backend-specific options.
        """
        return

    def list_files(self, uri: str, recursive: bool = True, **kwargs: Any) -> list[str]:
        """List files under a URI.

        Args:
            uri: The URI to list files from.
            recursive: Whether to list recursively.
            **kwargs: Backend-specific options.

        Returns:
            List of file URIs.
        """
        return []

    def get_signed_url(
        self, uri: str, operation: Literal["read", "write"] = "read", expires_in: int = 3600, **kwargs: Any
    ) -> str:
        """Generate a pre-signed URL for the given URI and operation.

        Args:
            uri: The URI to sign.
            operation: The operation ('read' or 'write').
            expires_in: Expiry in seconds.
            **kwargs: Backend-specific options.

        Returns:
            The signed URL.

        Raises:
            NotImplementedError: If not supported by backend.
        """
        msg = "Signed URL generation not supported by this backend"
        raise NotImplementedError(msg)

    @property
    def backend_type(self) -> str:
        """Return backend type identifier (e.g., 'obstore', 'fsspec', 'local')."""
        raise NotImplementedError

    @property
    def base_uri(self) -> str:
        """Return the base URI this backend is configured for (e.g., 's3://bucket')."""
        raise NotImplementedError
