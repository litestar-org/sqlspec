# pyright: ignore=reportUnknownVariableType
import logging
from io import BytesIO
from typing import TYPE_CHECKING, Any, Union

from sqlspec.exceptions import MissingDependencyError
from sqlspec.storage.backends.base import ObjectStoreBase
from sqlspec.typing import FSSPEC_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from fsspec import AbstractFileSystem

    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("FSSpecBackend",)

logger = logging.getLogger(__name__)

# Constants for URI validation
URI_PARTS_MIN_COUNT = 2
"""Minimum number of parts in a valid cloud storage URI (bucket/path)."""

AZURE_URI_PARTS_MIN_COUNT = 2
"""Minimum number of parts in an Azure URI (account/container)."""

AZURE_URI_BLOB_INDEX = 2
"""Index of blob name in Azure URI parts."""


def _join_path(prefix: str, path: str) -> str:
    if not prefix:
        return path
    prefix = prefix.rstrip("/")
    path = path.lstrip("/")
    return f"{prefix}/{path}"


class FSSpecBackend(ObjectStoreBase):
    """Extended protocol support via fsspec.

    This backend implements the ObjectStoreProtocol using fsspec,
    providing support for extended protocols not covered by obstore
    and offering fallback capabilities.
    """

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
        super().__init__()

    @classmethod
    def from_config(cls, config: "dict[str, Any]") -> "FSSpecBackend":
        protocol = config["protocol"]
        fs_config = config.get("fs_config", {})
        base_path = config.get("base_path", "")

        # Create filesystem instance from protocol
        import fsspec

        fs_instance = fsspec.filesystem(protocol, **fs_config)

        return cls(fs=fs_instance, base_path=base_path)

    def _resolve_path(self, path: str) -> str:
        """Resolve path relative to base_path."""
        if self.base_path:
            # Ensure no double slashes
            clean_base = self.base_path.rstrip("/")
            clean_path = path.lstrip("/")
            return f"{clean_base}/{clean_path}"
        return path

    @property
    def backend_type(self) -> str:
        return "fsspec"

    @property
    def base_uri(self) -> str:
        return self._fs_uri

    # Core Operations (sync)
    def read_bytes(self, path: str, **kwargs: Any) -> bytes:
        """Read bytes from an object."""
        resolved_path = self._resolve_path(path)
        return self.fs.cat(resolved_path, **kwargs)  # type: ignore[no-any-return]  # pyright: ignore

    def write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to an object."""
        resolved_path = self._resolve_path(path)
        with self.fs.open(resolved_path, mode="wb", **kwargs) as f:
            f.write(data)  # pyright: ignore

    def read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from an object."""
        data = self.read_bytes(path, **kwargs)
        return data.decode(encoding)

    def write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to an object."""
        self.write_bytes(path, data.encode(encoding), **kwargs)

    # Object Operations
    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if an object exists."""
        resolved_path = self._resolve_path(path)
        return self.fs.exists(resolved_path, **kwargs)  # type: ignore[no-any-return]

    def delete(self, path: str, **kwargs: Any) -> None:
        """Delete an object."""
        resolved_path = self._resolve_path(path)
        self.fs.rm(resolved_path, **kwargs)

    def copy(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy an object."""
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        self.fs.copy(source_path, dest_path, **kwargs)

    def move(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move an object."""
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        self.fs.mv(source_path, dest_path, **kwargs)

    # Arrow Operations
    def read_arrow(self, path: str, **kwargs: Any) -> "ArrowTable":
        """Read an Arrow table from storage."""
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")

        import pyarrow.parquet as pq

        resolved_path = self._resolve_path(path)
        with self.fs.open(resolved_path, mode="rb", **kwargs) as f:
            return pq.read_table(f)

    def write_arrow(self, path: str, table: "ArrowTable", **kwargs: Any) -> None:
        """Write an Arrow table to storage."""
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")

        import pyarrow.parquet as pq

        resolved_path = self._resolve_path(path)
        with self.fs.open(resolved_path, mode="wb") as f:
            pq.write_table(table, f, **kwargs)  # pyright: ignore

    # Listing Operations
    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects with optional prefix."""
        resolved_prefix = self._resolve_path(prefix) if prefix else self.base_path

        # Use fs.glob for listing files
        if recursive:
            pattern = f"{resolved_prefix}/**" if resolved_prefix else "**"
        else:
            pattern = f"{resolved_prefix}/*" if resolved_prefix else "*"

        # Get all files (not directories)
        paths = [str(path) for path in self.fs.glob(pattern, **kwargs) if not self.fs.isdir(path)]
        return sorted(paths)

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching a glob pattern."""
        resolved_pattern = self._resolve_path(pattern)
        # Use fsspec's native glob
        paths = [str(path) for path in self.fs.glob(resolved_pattern, **kwargs) if not self.fs.isdir(path)]
        return sorted(paths)

    # Path Operations
    def is_object(self, path: str) -> bool:
        """Check if path points to an object."""
        resolved_path = self._resolve_path(path)
        return self.fs.exists(resolved_path) and not self.fs.isdir(resolved_path)

    def is_path(self, path: str) -> bool:
        """Check if path points to a prefix (directory-like)."""
        resolved_path = self._resolve_path(path)
        return self.fs.isdir(resolved_path)  # type: ignore[no-any-return]

    def get_metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get object metadata."""
        info = self.fs.info(self._resolve_path(path), **kwargs)

        # Convert fsspec info to dict
        if isinstance(info, dict):
            return info

        # Try to get dict representation
        try:
            return vars(info)  # type: ignore[no-any-return]
        except AttributeError:
            pass

        # Fallback to basic metadata with safe attribute access
        resolved_path = self._resolve_path(path)
        return {
            "path": resolved_path,
            "exists": self.fs.exists(resolved_path),
            "size": getattr(info, "size", None),
            "type": getattr(info, "type", "file"),
        }

    def _stream_file_batches(self, obj_path: str) -> "Iterator[ArrowRecordBatch]":
        import pyarrow.parquet as pq

        with self.fs.open(obj_path, mode="rb") as f:
            parquet_file = pq.ParquetFile(f)  # pyright: ignore[reportArgumentType]
            yield from parquet_file.iter_batches()

    def stream_arrow(self, pattern: str, **kwargs: Any) -> "Iterator[ArrowRecordBatch]":
        if not FSSPEC_INSTALLED:
            raise MissingDependencyError(package="fsspec", install_package="fsspec")
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")

        # Stream each file as record batches
        for obj_path in self.glob(pattern, **kwargs):
            yield from self._stream_file_batches(obj_path)

    async def read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        """Async read bytes. Wraps the sync implementation."""
        return await async_(self.read_bytes)(path, **kwargs)

    async def write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Async write bytes. Wras the sync implementation."""
        return await async_(self.write_bytes)(path, data, **kwargs)

    async def _stream_file_batches_async(self, obj_path: str) -> "AsyncIterator[ArrowRecordBatch]":
        import pyarrow.parquet as pq

        data = await self.read_bytes_async(obj_path)
        parquet_file = pq.ParquetFile(BytesIO(data))
        for batch in parquet_file.iter_batches():
            yield batch

    async def stream_arrow_async(self, pattern: str, **kwargs: Any) -> "AsyncIterator[ArrowRecordBatch]":
        """Async stream Arrow record batches.

        This implementation provides file-level async streaming. Each file is
        read into memory before its batches are processed.

        Args:
            pattern: The glob pattern to match.
            **kwargs: Additional arguments to pass to the glob method.

        Yields:
            AsyncIterator of Arrow record batches
        """
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")

        # Get paths asynchronously
        paths = await async_(self.glob)(pattern, **kwargs)

        # Stream batches from each path
        for path in paths:
            async for batch in self._stream_file_batches_async(path):
                yield batch

    async def read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Async read text. Wraps the sync implementation."""
        return await async_(self.read_text)(path, encoding, **kwargs)

    async def write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Async write text. Wraps the sync implementation."""
        await async_(self.write_text)(path, data, encoding, **kwargs)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Async list objects. Wraps the sync implementation."""
        return await async_(self.list_objects)(prefix, recursive, **kwargs)

    async def exists_async(self, path: str, **kwargs: Any) -> bool:
        """Async exists check. Wraps the sync implementation."""
        return await async_(self.exists)(path, **kwargs)

    async def delete_async(self, path: str, **kwargs: Any) -> None:
        """Async delete. Wraps the sync implementation."""
        await async_(self.delete)(path, **kwargs)

    async def copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async copy. Wraps the sync implementation."""
        await async_(self.copy)(source, destination, **kwargs)

    async def move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async move. Wraps the sync implementation."""
        await async_(self.move)(source, destination, **kwargs)

    async def get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Async get metadata. Wraps the sync implementation."""
        return await async_(self.get_metadata)(path, **kwargs)

    async def read_arrow_async(self, path: str, **kwargs: Any) -> "ArrowTable":
        """Async read Arrow. Wraps the sync implementation."""
        return await async_(self.read_arrow)(path, **kwargs)

    async def write_arrow_async(self, path: str, table: "ArrowTable", **kwargs: Any) -> None:
        """Async write Arrow. Wraps the sync implementation."""
        await async_(self.write_arrow)(path, table, **kwargs)
