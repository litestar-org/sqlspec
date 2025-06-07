from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from sqlspec.exceptions import FileNotFoundInStorageError, MissingDependencyError, StorageOperationFailedError
from sqlspec.storage.backends.base import InstrumentedStorageBackend
from sqlspec.storage.protocol import StorageBackendProtocol

if TYPE_CHECKING:
    import pyarrow as pa

    from sqlspec.config import InstrumentationConfig

__all__ = ("LocalBackend",)


class LocalBackend(InstrumentedStorageBackend, StorageBackendProtocol):
    """Local file system backend for file:// and local paths with instrumentation."""

    def _read_bytes(self, uri: str, **kwargs: Any) -> bytes:
        """Read bytes from a local file."""
        path = self._uri_to_path(uri)
        try:
            return Path(path).read_bytes()
        except FileNotFoundError:
            msg = f"File not found: {path}"
            raise FileNotFoundInStorageError(msg)
        except Exception as exc:
            msg = f"Failed to read bytes from {path}"
            raise StorageOperationFailedError(msg) from exc

    def _write_bytes(self, uri: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to a local file."""
        path = self._uri_to_path(uri)
        try:
            Path(path).write_bytes(data)
        except Exception as exc:
            msg = f"Failed to write bytes to {path}"
            raise StorageOperationFailedError(msg) from exc

    def _read_text(self, uri: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from a local file."""
        path = self._uri_to_path(uri)
        try:
            return Path(path).read_text(encoding=encoding)
        except FileNotFoundError:
            msg = f"File not found: {path}"
            raise FileNotFoundInStorageError(msg)
        except Exception as exc:
            msg = f"Failed to read text from {path}"
            raise StorageOperationFailedError(msg) from exc

    def _write_text(self, uri: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to a local file."""
        path = self._uri_to_path(uri)
        try:
            Path(path).write_text(data, encoding=encoding)
        except Exception as exc:
            msg = f"Failed to write text to {path}"
            raise StorageOperationFailedError(msg) from exc

    def _read_arrow(self, uri: str, **kwargs: Any) -> "pa.Table":
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

    def _write_arrow(self, uri: str, table: "pa.Table", **kwargs: Any) -> None:
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

    def _exists(self, uri: str, **kwargs: Any) -> bool:
        """Check if a local file exists."""
        path = self._uri_to_path(uri)
        try:
            return Path(path).exists()
        except Exception as exc:
            msg = f"Failed to check existence of {path}"
            raise StorageOperationFailedError(msg) from exc

    def _delete(self, uri: str, **kwargs: Any) -> None:
        """Delete a local file."""
        path = self._uri_to_path(uri)
        try:
            Path(path).unlink()
        except FileNotFoundError:
            msg = f"File not found: {path}"
            raise FileNotFoundInStorageError(msg)
        except Exception as exc:
            msg = f"Failed to delete {path}"
            raise StorageOperationFailedError(msg) from exc

    def _list_objects(self, uri: str, recursive: bool = True, **kwargs: Any) -> list[str]:
        """List files under a directory."""
        path = self._uri_to_path(uri)
        base_path = Path(path)

        if not base_path.exists():
            return []

        try:
            if not base_path.is_dir():
                # If it's a file, return just that file
                return [str(base_path)]

            files = []
            if recursive:
                # Use rglob for recursive listing
                files.extend(str(file_path) for file_path in base_path.rglob("*") if file_path.is_file())
            else:
                # Use iterdir for non-recursive listing
                files.extend(str(file_path) for file_path in base_path.iterdir() if file_path.is_file())

            return sorted(files)
        except Exception as exc:
            msg = f"Failed to list files in {path}"
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
    def from_config(cls, config: "dict[str, Any]") -> "LocalBackend":
        base_path = config.get("base_path", "")
        return cls(base_path=base_path)

    def __init__(self, base_path: str = "", instrumentation_config: Optional["InstrumentationConfig"] = None) -> None:
        super().__init__(instrumentation_config=instrumentation_config, backend_name="local")
        self._base_path = base_path or str(Path.cwd())
        Path(self._base_path).mkdir(parents=True, exist_ok=True)

    @property
    def backend_type(self) -> str:
        return "local"

    @property
    def base_uri(self) -> str:
        return f"file://{Path(self._base_path).resolve()}"
