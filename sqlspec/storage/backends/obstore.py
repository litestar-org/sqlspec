from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError
from sqlspec.storage.protocol import StorageBackendProtocol
from sqlspec.storage.registry import default_storage_registry
from sqlspec.typing import OBSTORE_INSTALLED

if TYPE_CHECKING:
    import pyarrow as pa


class ObstoreBackend(StorageBackendProtocol):
    """Backend for object storage using obstore."""

    @classmethod
    def from_config(cls, config: "dict[str, Any]") -> "ObstoreBackend":
        store_config = config.get("store_config", {})
        base_path = config.get("base_path", "")
        return cls(**store_config, base_path=base_path)

    def __init__(self, base_path: str = "", **store_config: "Any") -> None:
        if not OBSTORE_INSTALLED:
            msg = "obstore"
            raise MissingDependencyError(msg)
        try:
            import obstore

            self._base_path = base_path.rstrip("/")
            self._store_config = store_config
            self.client = obstore.Obstore(**store_config)
        except Exception as exc:
            msg = "Failed to initialize obstore client"
            raise StorageOperationFailedError(msg) from exc

    @property
    def backend_type(self) -> str:
        return "obstore"

    @property
    def base_uri(self) -> str:
        scheme = self._store_config.get("scheme", "unknown")
        bucket = self._store_config.get("bucket", "unknown")
        return f"{scheme}://{bucket}"

    def read_bytes(self, uri: str, **kwargs: Any) -> bytes:
        if not OBSTORE_INSTALLED:
            msg = "obstore"
            raise MissingDependencyError(msg)
        try:
            return self.client.read_bytes(uri, **kwargs)
        except Exception as exc:
            msg = f"Failed to read bytes from {uri}"
            raise StorageOperationFailedError(msg) from exc

    def write_bytes(self, uri: str, data: bytes, **kwargs: Any) -> None:
        if not OBSTORE_INSTALLED:
            msg = "obstore"
            raise MissingDependencyError(msg)
        try:
            self.client.write_bytes(uri, data, **kwargs)
        except Exception as exc:
            msg = f"Failed to write bytes to {uri}"
            raise StorageOperationFailedError(msg) from exc

    def read_text(self, uri: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        if not OBSTORE_INSTALLED:
            msg = "obstore"
            raise MissingDependencyError(msg)
        try:
            return self.client.read_text(uri, encoding=encoding, **kwargs)
        except Exception as exc:
            msg = f"Failed to read text from {uri}"
            raise StorageOperationFailedError(msg) from exc

    def write_text(self, uri: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        if not OBSTORE_INSTALLED:
            msg = "obstore"
            raise MissingDependencyError(msg)
        try:
            self.client.write_text(uri, data, encoding=encoding, **kwargs)
        except Exception as exc:
            msg = f"Failed to write text to {uri}"
            raise StorageOperationFailedError(msg) from exc

    def read_arrow(self, uri: str, **kwargs: Any) -> "pa.Table":
        if not OBSTORE_INSTALLED:
            msg = "obstore"
            raise MissingDependencyError(msg)
        try:
            import pyarrow.parquet as pq

            with self.client.open(uri, mode="rb", **kwargs) as f:
                return pq.read_table(f, **kwargs)
        except ImportError:
            msg = "pyarrow"
            raise MissingDependencyError(msg)
        except Exception as exc:
            msg = f"Failed to read Arrow table from {uri}"
            raise StorageOperationFailedError(msg) from exc

    def write_arrow(self, uri: str, table: "pa.Table", **kwargs: Any) -> None:
        if not OBSTORE_INSTALLED:
            msg = "obstore"
            raise MissingDependencyError(msg)
        try:
            import pyarrow.parquet as pq

            with self.client.open(uri, mode="wb", **kwargs) as f:
                pq.write_table(table, f, **kwargs)
        except ImportError:
            msg = "pyarrow"
            raise MissingDependencyError(msg)
        except Exception as exc:
            msg = f"Failed to write Arrow table to {uri}"
            raise StorageOperationFailedError(msg) from exc

    def exists(self, uri: str, **kwargs: Any) -> bool:
        if not OBSTORE_INSTALLED:
            msg = "obstore"
            raise MissingDependencyError(msg)
        try:
            return self.client.exists(uri, **kwargs)
        except Exception as exc:
            msg = f"Failed to check existence of {uri}"
            raise StorageOperationFailedError(msg) from exc

    def delete(self, uri: str, **kwargs: Any) -> None:
        if not OBSTORE_INSTALLED:
            msg = "obstore"
            raise MissingDependencyError(msg)
        try:
            self.client.delete(uri, **kwargs)
        except Exception as exc:
            msg = f"Failed to delete {uri}"
            raise StorageOperationFailedError(msg) from exc

    def get_signed_url(self, uri: str, operation: str = "read", expires_in: int = 3600, **kwargs: Any) -> str:
        msg = "Signed URLs are not yet implemented for obstore backend"
        raise NotImplementedError(msg)


# Register ObstoreBackend for 'obstore' scheme
default_storage_registry.register_backend("obstore", ObstoreBackend)
