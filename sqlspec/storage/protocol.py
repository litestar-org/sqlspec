from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    import pyarrow as pa


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
        ...

    def write_bytes(self, uri: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to a URI.

        Args:
            uri: The URI to write to.
            data: The bytes to write.
            **kwargs: Backend-specific options.
        """
        ...

    def read_text(self, uri: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from a URI.

        Args:
            uri: The URI to read from.
            encoding: The text encoding.
            **kwargs: Backend-specific options.

        Returns:
            The text read from the URI.
        """
        ...

    def write_text(self, uri: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to a URI.

        Args:
            uri: The URI to write to.
            data: The text to write.
            encoding: The text encoding.
            **kwargs: Backend-specific options.
        """
        ...

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
        ...

    def delete(self, uri: str, **kwargs: Any) -> None:
        """Delete a URI.

        Args:
            uri: The URI to delete.
            **kwargs: Backend-specific options.
        """
        ...

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
