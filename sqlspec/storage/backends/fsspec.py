import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import FSSPEC_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

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
        """Initialize paths iterator."""
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


class FSSpecBackend:
    """Storage backend using fsspec.

    Implements ObjectStoreProtocol using fsspec for various protocols
    including HTTP, HTTPS, FTP, and cloud storage services.
    """

    def __init__(self, uri: str, **kwargs: Any) -> None:
        self._ensure_fsspec()

        base_path = kwargs.pop("base_path", "")
        self.base_path = base_path.rstrip("/") if base_path else ""

        if "://" in uri:
            self.protocol = uri.split("://", maxsplit=1)[0]
            self._fs_uri = uri
        else:
            self.protocol = uri
            self._fs_uri = f"{uri}://"

        import fsspec

        self.fs = fsspec.filesystem(self.protocol, **kwargs)
        self.backend_type = "fsspec"

        super().__init__()

    @classmethod
    def from_config(cls, config: "dict[str, Any]") -> "FSSpecBackend":
        protocol = config["protocol"]
        fs_config = config.get("fs_config", {})
        base_path = config.get("base_path", "")

        uri = f"{protocol}://"
        kwargs = dict(fs_config)
        if base_path:
            kwargs["base_path"] = base_path

        return cls(uri=uri, **kwargs)

    def _ensure_fsspec(self) -> None:
        """Ensure fsspec is available for operations."""
        if not FSSPEC_INSTALLED:
            raise MissingDependencyError(package="fsspec", install_package="fsspec")

    def _ensure_pyarrow(self) -> None:
        """Ensure PyArrow is available for Arrow operations."""
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")

    def _resolve_path(self, path: Union[str, Path]) -> str:
        """Resolve path relative to base_path."""
        path_str = str(path)
        if self.base_path:
            clean_base = self.base_path.rstrip("/")
            clean_path = path_str.lstrip("/")
            return f"{clean_base}/{clean_path}"
        if self.protocol == "s3" and "://" in self._fs_uri:
            # For S3, we need to include the bucket from the URI
            # Extract bucket and path from URI like s3://bucket/path
            uri_parts = self._fs_uri.split("://", 1)[1]  # Remove s3://
            if "/" in uri_parts:
                # URI has bucket and base path
                return f"{uri_parts.rstrip('/')}/{path_str.lstrip('/')}"
            # URI has only bucket
            return f"{uri_parts}/{path_str.lstrip('/')}"
        return path_str

    @property
    def base_uri(self) -> str:
        return self._fs_uri

    def read_bytes(self, path: Union[str, Path], **kwargs: Any) -> bytes:
        """Read bytes from an object."""
        resolved_path = self._resolve_path(path)
        return self.fs.cat(resolved_path, **kwargs)  # type: ignore[no-any-return]  # pyright: ignore

    def write_bytes(self, path: Union[str, Path], data: bytes, **kwargs: Any) -> None:
        """Write bytes to an object."""
        resolved_path = self._resolve_path(path)

        # Only create directories for local file systems, not for cloud storage
        if self.protocol == "file":
            parent_dir = str(Path(resolved_path).parent)
            if parent_dir and not self.fs.exists(parent_dir):
                self.fs.makedirs(parent_dir, exist_ok=True)

        with self.fs.open(resolved_path, mode="wb", **kwargs) as f:
            f.write(data)  # pyright: ignore

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
        resolved_path = self._resolve_path(path)
        self.fs.rm(resolved_path, **kwargs)

    def copy(self, source: Union[str, Path], destination: Union[str, Path], **kwargs: Any) -> None:
        """Copy an object."""
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        self.fs.copy(source_path, dest_path, **kwargs)

    def move(self, source: Union[str, Path], destination: Union[str, Path], **kwargs: Any) -> None:
        """Move an object."""
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        self.fs.mv(source_path, dest_path, **kwargs)

    def read_arrow(self, path: Union[str, Path], **kwargs: Any) -> "ArrowTable":
        """Read an Arrow table from storage."""
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")
        import pyarrow.parquet as pq

        resolved_path = self._resolve_path(path)
        with self.fs.open(resolved_path, mode="rb", **kwargs) as f:
            return pq.read_table(f)

    def write_arrow(self, path: Union[str, Path], table: "ArrowTable", **kwargs: Any) -> None:
        """Write an Arrow table to storage."""
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")
        import pyarrow.parquet as pq

        resolved_path = self._resolve_path(path)
        with self.fs.open(resolved_path, mode="wb") as f:
            pq.write_table(table, f, **kwargs)  # pyright: ignore

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects with optional prefix."""
        resolved_prefix = self._resolve_path(prefix)
        if recursive:
            return sorted(self.fs.find(resolved_prefix, **kwargs))
        return sorted(self.fs.ls(resolved_prefix, detail=False, **kwargs))

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching a glob pattern."""
        resolved_pattern = self._resolve_path(pattern)
        return sorted(self.fs.glob(resolved_pattern, **kwargs))  # pyright: ignore

    def is_object(self, path: Union[str, Path]) -> bool:
        """Check if path points to an object."""
        resolved_path = self._resolve_path(path)
        return self.fs.exists(resolved_path) and not self.fs.isdir(resolved_path)

    def is_path(self, path: Union[str, Path]) -> bool:
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
        return {
            "path": resolved_path,
            "exists": True,
            "size": info.size,
            "last_modified": info.mtime,
            "type": info.type,
        }

    def sign(self, path: str, expires_in: int = 3600, for_upload: bool = False) -> str:
        """Generate a signed URL for the file."""
        resolved_path = self._resolve_path(path)
        return f"{self._fs_uri}{resolved_path}"

    def _stream_file_batches(self, obj_path: Union[str, Path]) -> "Iterator[ArrowRecordBatch]":
        import pyarrow.parquet as pq

        with self.fs.open(obj_path, mode="rb") as f:
            parquet_file = pq.ParquetFile(f)  # pyright: ignore[reportArgumentType]
            yield from parquet_file.iter_batches()

    def stream_arrow(self, pattern: str, **kwargs: Any) -> "Iterator[ArrowRecordBatch]":
        self._ensure_fsspec()
        self._ensure_pyarrow()

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
        self._ensure_pyarrow()

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

    async def sign_async(self, path: str, expires_in: int = 3600, for_upload: bool = False) -> str:
        """Generate a signed URL asynchronously."""
        return await async_(self.sign)(path, expires_in, for_upload)

    async def read_arrow_async(self, path: Union[str, Path], **kwargs: Any) -> "ArrowTable":
        """Read Arrow table from storage asynchronously."""
        return await async_(self.read_arrow)(path, **kwargs)

    async def write_arrow_async(self, path: Union[str, Path], table: "ArrowTable", **kwargs: Any) -> None:
        """Write Arrow table to storage asynchronously."""
        await async_(self.write_arrow)(path, table, **kwargs)
