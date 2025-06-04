import os
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import FileNotFoundInStorageError, MissingDependencyError, StorageOperationFailedError
from sqlspec.storage.protocol import StorageBackendProtocol
from sqlspec.storage.registry import default_storage_registry

if TYPE_CHECKING:
    import pyarrow as pa


class LocalFileBackend(StorageBackendProtocol):
    """Local file system backend for file:// and local paths."""

    def read_bytes(self, uri: str, **kwargs: Any) -> bytes:
        """Read bytes from a local file."""
        path = self._uri_to_path(uri)
        try:
            with open(path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            msg = f"File not found: {path}"
            raise FileNotFoundInStorageError(msg)
        except Exception as exc:
            msg = f"Failed to read bytes from {path}"
            raise StorageOperationFailedError(msg) from exc

    def write_bytes(self, uri: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to a local file."""
        path = self._uri_to_path(uri)
        try:
            with open(path, "wb") as f:
                f.write(data)
        except Exception as exc:
            msg = f"Failed to write bytes to {path}"
            raise StorageOperationFailedError(msg) from exc

    def read_text(self, uri: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from a local file."""
        path = self._uri_to_path(uri)
        try:
            with open(path, encoding=encoding) as f:
                return f.read()
        except FileNotFoundError:
            msg = f"File not found: {path}"
            raise FileNotFoundInStorageError(msg)
        except Exception as exc:
            msg = f"Failed to read text from {path}"
            raise StorageOperationFailedError(msg) from exc

    def write_text(self, uri: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to a local file."""
        path = self._uri_to_path(uri)
        try:
            with open(path, "w", encoding=encoding) as f:
                f.write(data)
        except Exception as exc:
            msg = f"Failed to write text to {path}"
            raise StorageOperationFailedError(msg) from exc

    def read_arrow(self, uri: str, **kwargs: Any) -> "pa.Table":
        """Read an Arrow table from a local file."""
        try:
            import pyarrow.parquet as pq
        except ImportError:
            msg = "pyarrow"
            raise MissingDependencyError(msg)
        path = self._uri_to_path(uri)
        try:
            return pq.read_table(path, **kwargs)
        except FileNotFoundError:
            msg = f"File not found: {path}"
            raise FileNotFoundInStorageError(msg)
        except Exception as exc:
            msg = f"Failed to read Arrow table from {path}"
            raise StorageOperationFailedError(msg) from exc

    def write_arrow(self, uri: str, table: "pa.Table", **kwargs: Any) -> None:
        """Write an Arrow table to a local file."""
        try:
            import pyarrow.parquet as pq
        except ImportError:
            msg = "pyarrow"
            raise MissingDependencyError(msg)
        path = self._uri_to_path(uri)
        try:
            pq.write_table(table, path, **kwargs)
        except Exception as exc:
            msg = f"Failed to write Arrow table to {path}"
            raise StorageOperationFailedError(msg) from exc

    def exists(self, uri: str, **kwargs: Any) -> bool:
        """Check if a local file exists."""
        path = self._uri_to_path(uri)
        try:
            return os.path.exists(path)
        except Exception as exc:
            msg = f"Failed to check existence of {path}"
            raise StorageOperationFailedError(msg) from exc

    def delete(self, uri: str, **kwargs: Any) -> None:
        """Delete a local file."""
        path = self._uri_to_path(uri)
        try:
            os.remove(path)
        except FileNotFoundError:
            msg = f"File not found: {path}"
            raise FileNotFoundInStorageError(msg)
        except Exception as exc:
            msg = f"Failed to delete {path}"
            raise StorageOperationFailedError(msg) from exc

    def get_signed_url(self, uri: str, operation: str = "read", expires_in: int = 3600, **kwargs: Any) -> str:
        """Signed URLs are not supported for local files."""
        msg = "Signed URLs are not supported for local files"
        raise NotImplementedError(msg)

    @staticmethod
    def _uri_to_path(uri: str) -> str:
        if uri.startswith("file://"):
            return uri[7:]
        return uri

    @classmethod
    def from_config(cls, config: "dict[str, Any]") -> "LocalFileBackend":
        base_path = config.get("base_path", "")
        return cls(base_path=base_path)

    def __init__(self, base_path: str = "") -> None:
        self._base_path = base_path or os.getcwd()
        os.makedirs(self._base_path, exist_ok=True)

    @property
    def backend_type(self) -> str:
        return "local"

    @property
    def base_uri(self) -> str:
        return f"file://{os.path.abspath(self._base_path)}"


# Register LocalFileBackend for 'file' scheme
default_storage_registry.register_backend("file", LocalFileBackend)
