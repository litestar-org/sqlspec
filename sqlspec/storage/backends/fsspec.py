from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError
from sqlspec.storage.protocol import StorageBackendProtocol
from sqlspec.storage.registry import default_storage_registry
from sqlspec.typing import FSSPEC_INSTALLED

if TYPE_CHECKING:
    import pyarrow as pa


class FsspecBackend(StorageBackendProtocol):
    """Backend for cloud/remote filesystems using fsspec."""

    @classmethod
    def from_config(cls, config: "dict[str, Any]") -> "FsspecBackend":
        protocol = config["protocol"]
        fs_config = config.get("fs_config", {})
        base_path = config.get("base_path", "")
        return cls(protocol=protocol, base_path=base_path, **fs_config)

    def __init__(self, protocol: str = "", base_path: str = "", **fs_config: "Any") -> None:
        if not FSSPEC_INSTALLED:
            msg = "fsspec"
            raise MissingDependencyError(msg)
        self._protocol = protocol
        self._base_path = base_path.rstrip("/")
        self._fs_config = fs_config
        self.fs = None
        self.fs_kwargs = fs_config

    @property
    def backend_type(self) -> str:
        return "fsspec"

    @property
    def base_uri(self) -> str:
        bucket = self._fs_config.get("bucket", self._fs_config.get("bucket_name", "unknown"))
        return f"{self._protocol}://{bucket}"

    def _get_fs(self, uri: str):
        if self.fs is not None:
            return self.fs
        try:
            import fsspec

            self.fs = fsspec.open(uri, **self.fs_kwargs).fs
            return self.fs
        except Exception as exc:
            msg = f"Failed to get fsspec filesystem for {uri}"
            raise StorageOperationFailedError(msg) from exc

    def read_bytes(self, uri: str, **kwargs: Any) -> bytes:
        if not FSSPEC_INSTALLED:
            msg = "fsspec"
            raise MissingDependencyError(msg)
        try:
            import fsspec

            with fsspec.open(uri, mode="rb", **self.fs_kwargs) as f:
                return f.read()
        except Exception as exc:
            msg = f"Failed to read bytes from {uri}"
            raise StorageOperationFailedError(msg) from exc

    def write_bytes(self, uri: str, data: bytes, **kwargs: Any) -> None:
        if not FSSPEC_INSTALLED:
            msg = "fsspec"
            raise MissingDependencyError(msg)
        try:
            import fsspec

            with fsspec.open(uri, mode="wb", **self.fs_kwargs) as f:
                f.write(data)
        except Exception as exc:
            msg = f"Failed to write bytes to {uri}"
            raise StorageOperationFailedError(msg) from exc

    def read_text(self, uri: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        if not FSSPEC_INSTALLED:
            msg = "fsspec"
            raise MissingDependencyError(msg)
        try:
            import fsspec

            with fsspec.open(uri, mode="rt", encoding=encoding, **self.fs_kwargs) as f:
                return f.read()
        except Exception as exc:
            msg = f"Failed to read text from {uri}"
            raise StorageOperationFailedError(msg) from exc

    def write_text(self, uri: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        if not FSSPEC_INSTALLED:
            msg = "fsspec"
            raise MissingDependencyError(msg)
        try:
            import fsspec

            with fsspec.open(uri, mode="wt", encoding=encoding, **self.fs_kwargs) as f:
                f.write(data)
        except Exception as exc:
            msg = f"Failed to write text to {uri}"
            raise StorageOperationFailedError(msg) from exc

    def read_arrow(self, uri: str, **kwargs: Any) -> "pa.Table":
        if not FSSPEC_INSTALLED:
            msg = "fsspec"
            raise MissingDependencyError(msg)
        try:
            import fsspec
            import pyarrow.parquet as pq

            with fsspec.open(uri, mode="rb", **self.fs_kwargs) as f:
                return pq.read_table(f, **kwargs)
        except ImportError:
            msg = "pyarrow"
            raise MissingDependencyError(msg)
        except Exception as exc:
            msg = f"Failed to read Arrow table from {uri}"
            raise StorageOperationFailedError(msg) from exc

    def write_arrow(self, uri: str, table: "pa.Table", **kwargs: Any) -> None:
        if not FSSPEC_INSTALLED:
            msg = "fsspec"
            raise MissingDependencyError(msg)
        try:
            import fsspec
            import pyarrow.parquet as pq

            with fsspec.open(uri, mode="wb", **self.fs_kwargs) as f:
                pq.write_table(table, f, **kwargs)
        except ImportError:
            msg = "pyarrow"
            raise MissingDependencyError(msg)
        except Exception as exc:
            msg = f"Failed to write Arrow table to {uri}"
            raise StorageOperationFailedError(msg) from exc

    def exists(self, uri: str, **kwargs: Any) -> bool:
        if not FSSPEC_INSTALLED:
            msg = "fsspec"
            raise MissingDependencyError(msg)
        try:
            fs = self._get_fs(uri)
            return fs.exists(uri)
        except Exception as exc:
            msg = f"Failed to check existence of {uri}"
            raise StorageOperationFailedError(msg) from exc

    def delete(self, uri: str, **kwargs: Any) -> None:
        if not FSSPEC_INSTALLED:
            msg = "fsspec"
            raise MissingDependencyError(msg)
        try:
            fs = self._get_fs(uri)
            fs.rm(uri)
        except Exception as exc:
            msg = f"Failed to delete {uri}"
            raise StorageOperationFailedError(msg) from exc

    def get_signed_url(self, uri: str, operation: str = "read", expires_in: int = 3600, **kwargs: Any) -> str:
        msg = "Signed URLs are not supported by the fsspec backend (use cloud SDKs directly)"
        raise NotImplementedError(msg)


# Register FsspecBackend for common cloud/remote schemes
for scheme in ("s3", "gs", "gcs", "https", "http"):
    default_storage_registry.register_backend(scheme, FsspecBackend)
