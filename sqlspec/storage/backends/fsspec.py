import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError
from sqlspec.storage.backends.base import ObjectStoreBase
from sqlspec.storage.capabilities import StorageCapabilities
from sqlspec.typing import FSSPEC_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from fsspec import AbstractFileSystem

    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("FSSpecBackend",)

logger = logging.getLogger(__name__)


class _ArrowStreamer:
    def __init__(self, backend: "FSSpecBackend", pattern: str, **kwargs: Any) -> None:
        self.backend = backend
        self.pattern = pattern
        self.kwargs = kwargs
        self.paths_iterator: Optional[Iterator[str]] = None
        self.batch_iterator: Optional[Iterator[ArrowRecordBatch]] = None

    def __aiter__(self) -> "_ArrowStreamer":
        return self

    async def _initialize(self) -> None:
        """Initialize the paths iterator."""
        if self.paths_iterator is None:
            paths = await async_(self.backend.glob)(self.pattern, **self.kwargs)
            self.paths_iterator = iter(paths)

    async def __anext__(self) -> "ArrowRecordBatch":
        await self._initialize()

        if self.batch_iterator:
            try:
                return next(self.batch_iterator)
            except StopIteration:
                self.batch_iterator = None

        if self.paths_iterator:
            try:
                path = next(self.paths_iterator)
                self.batch_iterator = await async_(self.backend._stream_file_batches)(path)
                return await self.__anext__()
            except StopIteration:
                raise StopAsyncIteration
        raise StopAsyncIteration


class FSSpecBackend(ObjectStoreBase):
    """Storage backend using fsspec.

    Implements the ObjectStoreProtocol using fsspec,
    providing support for various protocols including HTTP, HTTPS, FTP,
    and cloud storage services.
    """

    _default_capabilities: ClassVar[StorageCapabilities] = StorageCapabilities(
        supports_arrow=PYARROW_INSTALLED,
        supports_streaming=PYARROW_INSTALLED,
        supports_async=True,
        supports_compression=True,
        is_remote=True,
        is_cloud_native=False,
    )

    def __init__(self, fs: "Union[str, AbstractFileSystem]", base_path: str = "") -> None:
        if not FSSPEC_INSTALLED:
            raise MissingDependencyError(package="fsspec", install_package="fsspec")

        self.base_path = base_path.rstrip("/") if base_path else ""

        if isinstance(fs, str):
            import fsspec

            self.fs = fsspec.filesystem(fs.split("://")[0])
            self.protocol = fs.split("://")[0]
            self._fs_uri = fs
        else:
            self.fs = fs
            self.protocol = getattr(fs, "protocol", "unknown")
            self._fs_uri = f"{self.protocol}://"

        self._instance_capabilities = self._detect_capabilities()

        super().__init__()

    @classmethod
    def from_config(cls, config: "dict[str, Any]") -> "FSSpecBackend":
        protocol = config["protocol"]
        fs_config = config.get("fs_config", {})
        base_path = config.get("base_path", "")

        import fsspec

        fs_instance = fsspec.filesystem(protocol, **fs_config)

        return cls(fs=fs_instance, base_path=base_path)

    def _resolve_path(self, path: Union[str, Path]) -> str:
        """Resolve path relative to base_path."""
        path_str = str(path)
        if self.base_path:
            clean_base = self.base_path.rstrip("/")
            clean_path = path_str.lstrip("/")
            return f"{clean_base}/{clean_path}"
        return path_str

    def _detect_capabilities(self) -> StorageCapabilities:
        """Detect capabilities based on underlying filesystem protocol."""
        protocol = self.protocol.lower()

        if protocol in {"s3", "s3a", "s3n"}:
            return StorageCapabilities.s3_compatible()
        if protocol in {"gcs", "gs"}:
            return StorageCapabilities.gcs()
        if protocol in {"abfs", "az", "azure"}:
            return StorageCapabilities.azure_blob()
        if protocol in {"file", "local"}:
            return StorageCapabilities.local_filesystem()
        return StorageCapabilities(
            supports_arrow=PYARROW_INSTALLED,
            supports_streaming=PYARROW_INSTALLED,
            supports_async=True,
            supports_compression=True,
            is_remote=True,
            is_cloud_native=False,
        )

    @property
    def capabilities(self) -> StorageCapabilities:
        """Return instance-specific capabilities based on detected protocol."""
        return getattr(self, "_instance_capabilities", self.__class__._default_capabilities)

    @classmethod
    def has_capability(cls, capability: str) -> bool:
        """Check if backend has a specific capability."""
        return getattr(cls._default_capabilities, capability, False)

    @classmethod
    def get_capabilities(cls) -> StorageCapabilities:
        """Get all capabilities for this backend."""
        return cls._default_capabilities

    @property
    def backend_type(self) -> str:
        return "fsspec"

    @property
    def base_uri(self) -> str:
        return self._fs_uri

    def read_bytes(self, path: Union[str, Path], **kwargs: Any) -> bytes:
        """Read bytes from an object."""
        try:
            resolved_path = self._resolve_path(path)
            return self.fs.cat(resolved_path, **kwargs)  # type: ignore[no-any-return]  # pyright: ignore
        except Exception as exc:
            msg = f"Failed to read bytes from {path}"
            raise StorageOperationFailedError(msg) from exc

    def write_bytes(self, path: Union[str, Path], data: bytes, **kwargs: Any) -> None:
        """Write bytes to an object."""
        try:
            resolved_path = self._resolve_path(path)
            with self.fs.open(resolved_path, mode="wb", **kwargs) as f:
                f.write(data)  # pyright: ignore
        except Exception as exc:
            msg = f"Failed to write bytes to {path}"
            raise StorageOperationFailedError(msg) from exc

    def read_text(self, path: Union[str, Path], encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from an object."""
        data = self.read_bytes(path, **kwargs)
        return data.decode(encoding)

    def write_text(self, path: Union[str, Path], data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to an object."""
        self.write_bytes(path, data.encode(encoding), **kwargs)

    def exists(self, path: Union[str, Path], **kwargs: Any) -> bool:
        """Check if an object exists."""
        resolved_path = self._resolve_path(path)
        return self.fs.exists(resolved_path, **kwargs)  # type: ignore[no-any-return]

    def delete(self, path: Union[str, Path], **kwargs: Any) -> None:
        """Delete an object."""
        try:
            resolved_path = self._resolve_path(path)
            self.fs.rm(resolved_path, **kwargs)
        except Exception as exc:
            msg = f"Failed to delete {path}"
            raise StorageOperationFailedError(msg) from exc

    def copy(self, source: Union[str, Path], destination: Union[str, Path], **kwargs: Any) -> None:
        """Copy an object."""
        try:
            source_path = self._resolve_path(source)
            dest_path = self._resolve_path(destination)
            self.fs.copy(source_path, dest_path, **kwargs)
        except Exception as exc:
            msg = f"Failed to copy {source} to {destination}"
            raise StorageOperationFailedError(msg) from exc

    def move(self, source: Union[str, Path], destination: Union[str, Path], **kwargs: Any) -> None:
        """Move an object."""
        try:
            source_path = self._resolve_path(source)
            dest_path = self._resolve_path(destination)
            self.fs.mv(source_path, dest_path, **kwargs)
        except Exception as exc:
            msg = f"Failed to move {source} to {destination}"
            raise StorageOperationFailedError(msg) from exc

    def read_arrow(self, path: Union[str, Path], **kwargs: Any) -> "ArrowTable":
        """Read an Arrow table from storage."""
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")
        try:
            import pyarrow.parquet as pq

            resolved_path = self._resolve_path(path)
            with self.fs.open(resolved_path, mode="rb", **kwargs) as f:
                return pq.read_table(f)
        except Exception as exc:
            msg = f"Failed to read Arrow table from {path}"
            raise StorageOperationFailedError(msg) from exc

    def write_arrow(self, path: Union[str, Path], table: "ArrowTable", **kwargs: Any) -> None:
        """Write an Arrow table to storage."""
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")
        try:
            import pyarrow.parquet as pq

            resolved_path = self._resolve_path(path)
            with self.fs.open(resolved_path, mode="wb") as f:
                pq.write_table(table, f, **kwargs)  # pyright: ignore
        except Exception as exc:
            msg = f"Failed to write Arrow table to {path}"
            raise StorageOperationFailedError(msg) from exc

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects with optional prefix."""
        try:
            resolved_prefix = self._resolve_path(prefix)
            if recursive:
                return sorted(self.fs.find(resolved_prefix, **kwargs))
            return sorted(self.fs.ls(resolved_prefix, detail=False, **kwargs))
        except Exception as exc:
            msg = f"Failed to list objects with prefix '{prefix}'"
            raise StorageOperationFailedError(msg) from exc

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching a glob pattern."""
        try:
            resolved_pattern = self._resolve_path(pattern)
            return sorted(self.fs.glob(resolved_pattern, **kwargs))  # pyright: ignore
        except Exception as exc:
            msg = f"Failed to glob with pattern '{pattern}'"
            raise StorageOperationFailedError(msg) from exc

    def is_object(self, path: str) -> bool:
        """Check if path points to an object."""
        resolved_path = self._resolve_path(path)
        return self.fs.exists(resolved_path) and not self.fs.isdir(resolved_path)

    def is_path(self, path: str) -> bool:
        """Check if path points to a prefix (directory-like)."""
        resolved_path = self._resolve_path(path)
        return self.fs.isdir(resolved_path)  # type: ignore[no-any-return]

    def get_metadata(self, path: Union[str, Path], **kwargs: Any) -> dict[str, Any]:
        """Get object metadata."""
        try:
            resolved_path = self._resolve_path(path)
            info = self.fs.info(resolved_path, **kwargs)
            if isinstance(info, dict):
                return {
                    "path": resolved_path,
                    "exists": True,
                    "size": info.get("size"),
                    "last_modified": info.get("mtime"),
                    "type": info.get("type", "file"),
                }

        except FileNotFoundError:
            return {"path": self._resolve_path(path), "exists": False}
        except Exception as exc:
            msg = f"Failed to get metadata for {path}"
            raise StorageOperationFailedError(msg) from exc
        return {
            "path": resolved_path,
            "exists": True,
            "size": info.size,
            "last_modified": info.mtime,
            "type": info.type,
        }

    def _stream_file_batches(self, obj_path: Union[str, Path]) -> "Iterator[ArrowRecordBatch]":
        import pyarrow.parquet as pq

        with self.fs.open(obj_path, mode="rb") as f:
            parquet_file = pq.ParquetFile(f)  # pyright: ignore[reportArgumentType]
            yield from parquet_file.iter_batches()

    def stream_arrow(self, pattern: str, **kwargs: Any) -> "Iterator[ArrowRecordBatch]":
        if not FSSPEC_INSTALLED:
            raise MissingDependencyError(package="fsspec", install_package="fsspec")
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")

        for obj_path in self.glob(pattern, **kwargs):
            yield from self._stream_file_batches(obj_path)

    async def read_bytes_async(self, path: Union[str, Path], **kwargs: Any) -> bytes:
        """Read bytes from storage asynchronously."""
        return await async_(self.read_bytes)(path, **kwargs)

    async def write_bytes_async(self, path: Union[str, Path], data: bytes, **kwargs: Any) -> None:
        """Write bytes to storage asynchronously."""
        return await async_(self.write_bytes)(path, data, **kwargs)

    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> "AsyncIterator[ArrowRecordBatch]":
        """Stream Arrow record batches from storage asynchronously.

        Args:
            pattern: The glob pattern to match.
            **kwargs: Additional arguments to pass to the glob method.

        Returns:
            AsyncIterator of Arrow record batches
        """
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")

        return _ArrowStreamer(self, pattern, **kwargs)

    async def read_text_async(self, path: Union[str, Path], encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage asynchronously."""
        return await async_(self.read_text)(path, encoding, **kwargs)

    async def write_text_async(self, path: Union[str, Path], data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to storage asynchronously."""
        await async_(self.write_text)(path, data, encoding, **kwargs)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects in storage asynchronously."""
        return await async_(self.list_objects)(prefix, recursive, **kwargs)

    async def exists_async(self, path: Union[str, Path], **kwargs: Any) -> bool:
        """Check if object exists in storage asynchronously."""
        return await async_(self.exists)(path, **kwargs)

    async def delete_async(self, path: Union[str, Path], **kwargs: Any) -> None:
        """Delete object from storage asynchronously."""
        await async_(self.delete)(path, **kwargs)

    async def copy_async(self, source: Union[str, Path], destination: Union[str, Path], **kwargs: Any) -> None:
        """Copy object in storage asynchronously."""
        await async_(self.copy)(source, destination, **kwargs)

    async def move_async(self, source: Union[str, Path], destination: Union[str, Path], **kwargs: Any) -> None:
        """Move object in storage asynchronously."""
        await async_(self.move)(source, destination, **kwargs)

    async def get_metadata_async(self, path: Union[str, Path], **kwargs: Any) -> dict[str, Any]:
        """Get object metadata from storage asynchronously."""
        return await async_(self.get_metadata)(path, **kwargs)

    async def read_arrow_async(self, path: Union[str, Path], **kwargs: Any) -> "ArrowTable":
        """Read Arrow table from storage asynchronously."""
        return await async_(self.read_arrow)(path, **kwargs)

    async def write_arrow_async(self, path: Union[str, Path], table: "ArrowTable", **kwargs: Any) -> None:
        """Write Arrow table to storage asynchronously."""
        await async_(self.write_arrow)(path, table, **kwargs)
