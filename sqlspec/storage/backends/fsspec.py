# pyright: reportPrivateUsage=false
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import MissingDependencyError
from sqlspec.storage._utils import ensure_pyarrow, resolve_storage_path
from sqlspec.typing import FSSPEC_INSTALLED
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("FSSpecBackend",)

logger = logging.getLogger(__name__)


class _ArrowStreamer:
    """Async iterator for streaming Arrow batches from FSSpec backend.

    Uses async_() to offload blocking operations to thread pool,
    preventing event loop blocking during file I/O and iteration.

    CRITICAL: Creates generators on main thread, offloads only next() calls.
    """

    __slots__ = ("_initialized", "backend", "batch_iterator", "kwargs", "paths_iterator", "pattern")

    def __init__(self, backend: "FSSpecBackend", pattern: str, **kwargs: Any) -> None:
        self.backend = backend
        self.pattern = pattern
        self.kwargs = kwargs
        self.paths_iterator: Iterator[str] | None = None
        self.batch_iterator: Iterator[ArrowRecordBatch] | None = None
        self._initialized = False

    def __aiter__(self) -> "_ArrowStreamer":
        return self

    async def _initialize(self) -> None:
        """Initialize paths iterator asynchronously."""
        if not self._initialized:
            paths = await async_(self.backend.glob)(self.pattern, **self.kwargs)
            self.paths_iterator = iter(paths)
            self._initialized = True

    async def __anext__(self) -> "ArrowRecordBatch":
        """Get next Arrow batch asynchronously.

        Iterative state machine that avoids recursion and blocking calls.

        Returns:
            Arrow record batches from matching files.

        Raises:
            StopAsyncIteration: When no more batches available.
        """
        await self._initialize()

        while True:
            if self.batch_iterator is not None:

                def _safe_next_batch() -> "ArrowRecordBatch":
                    try:
                        return next(self.batch_iterator)  # type: ignore[arg-type]
                    except StopIteration as e:
                        raise StopAsyncIteration from e

                try:
                    return await async_(_safe_next_batch)()
                except StopAsyncIteration:
                    self.batch_iterator = None
                    continue

            try:
                path = next(self.paths_iterator)  # type: ignore[arg-type]
            except StopIteration as e:
                raise StopAsyncIteration from e

            self.batch_iterator = self.backend._stream_file_batches(path)

    async def aclose(self) -> None:
        """Close underlying batch iterator."""
        if self.batch_iterator is not None:
            try:
                close_method = self.batch_iterator.close  # type: ignore[attr-defined]
                await async_(close_method)()
            except AttributeError:
                pass


@mypyc_attr(allow_interpreted_subclasses=True)
class FSSpecBackend:
    """Storage backend using fsspec.

    Implements ObjectStoreProtocol using fsspec for various protocols
    including HTTP, HTTPS, FTP, and cloud storage services.
    """

    __slots__ = ("_fs_uri", "backend_type", "base_path", "fs", "protocol")

    def __init__(self, uri: str, **kwargs: Any) -> None:
        if not FSSPEC_INSTALLED:
            raise MissingDependencyError(package="fsspec", install_package="fsspec")

        base_path = kwargs.pop("base_path", "")

        if "://" in uri:
            self.protocol = uri.split("://", maxsplit=1)[0]
            self._fs_uri = uri

            # For S3/cloud URIs, extract bucket/path from URI as base_path
            if self.protocol in {"s3", "gs", "az", "gcs"}:
                from urllib.parse import urlparse

                parsed = urlparse(uri)
                # Combine netloc (bucket) and path for base_path
                if parsed.netloc:
                    uri_base_path = parsed.netloc
                    if parsed.path and parsed.path != "/":
                        uri_base_path = f"{uri_base_path}{parsed.path}"
                    # Only use URI base_path if no explicit base_path provided
                    if not base_path:
                        base_path = uri_base_path
        else:
            self.protocol = uri
            self._fs_uri = f"{uri}://"

        self.base_path = base_path.rstrip("/") if base_path else ""

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

    @property
    def base_uri(self) -> str:
        return self._fs_uri

    def read_bytes(self, path: str | Path, **kwargs: Any) -> bytes:
        """Read bytes from an object."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)
        return self.fs.cat(resolved_path, **kwargs)  # type: ignore[no-any-return]  # pyright: ignore

    def write_bytes(self, path: str | Path, data: bytes, **kwargs: Any) -> None:
        """Write bytes to an object."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)

        if self.protocol == "file":
            parent_dir = str(Path(resolved_path).parent)
            if parent_dir and not self.fs.exists(parent_dir):
                self.fs.makedirs(parent_dir, exist_ok=True)

        with self.fs.open(resolved_path, mode="wb", **kwargs) as f:
            f.write(data)  # pyright: ignore

    def read_text(self, path: str | Path, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from an object."""
        data = self.read_bytes(path, **kwargs)
        return data.decode(encoding)

    def write_text(self, path: str | Path, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to an object."""
        self.write_bytes(path, data.encode(encoding), **kwargs)

    def exists(self, path: str | Path, **kwargs: Any) -> bool:
        """Check if an object exists."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)
        return self.fs.exists(resolved_path, **kwargs)  # type: ignore[no-any-return]

    def delete(self, path: str | Path, **kwargs: Any) -> None:
        """Delete an object."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)
        self.fs.rm(resolved_path, **kwargs)

    def copy(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Copy an object."""
        source_path = resolve_storage_path(source, self.base_path, self.protocol, strip_file_scheme=False)
        dest_path = resolve_storage_path(destination, self.base_path, self.protocol, strip_file_scheme=False)
        self.fs.copy(source_path, dest_path, **kwargs)

    def move(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Move an object."""
        source_path = resolve_storage_path(source, self.base_path, self.protocol, strip_file_scheme=False)
        dest_path = resolve_storage_path(destination, self.base_path, self.protocol, strip_file_scheme=False)
        self.fs.mv(source_path, dest_path, **kwargs)

    def read_arrow(self, path: str | Path, **kwargs: Any) -> "ArrowTable":
        """Read an Arrow table from storage."""
        ensure_pyarrow()
        import pyarrow.parquet as pq

        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)
        with self.fs.open(resolved_path, mode="rb", **kwargs) as f:
            return pq.read_table(f)

    def write_arrow(self, path: str | Path, table: "ArrowTable", **kwargs: Any) -> None:
        """Write an Arrow table to storage."""
        ensure_pyarrow()
        import pyarrow.parquet as pq

        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)
        with self.fs.open(resolved_path, mode="wb") as f:
            pq.write_table(table, f, **kwargs)  # pyright: ignore

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects with optional prefix."""
        resolved_prefix = resolve_storage_path(prefix, self.base_path, self.protocol, strip_file_scheme=False)
        if recursive:
            return sorted(self.fs.find(resolved_prefix, **kwargs))
        return sorted(self.fs.ls(resolved_prefix, detail=False, **kwargs))

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching a glob pattern."""
        resolved_pattern = resolve_storage_path(pattern, self.base_path, self.protocol, strip_file_scheme=False)
        return sorted(self.fs.glob(resolved_pattern, **kwargs))  # pyright: ignore

    def is_object(self, path: str | Path) -> bool:
        """Check if path points to an object."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)
        return self.fs.exists(resolved_path) and not self.fs.isdir(resolved_path)

    def is_path(self, path: str | Path) -> bool:
        """Check if path points to a prefix (directory-like)."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)
        return self.fs.isdir(resolved_path)  # type: ignore[no-any-return]

    def get_metadata(self, path: str | Path, **kwargs: Any) -> dict[str, Any]:
        """Get object metadata."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)
        try:
            info = self.fs.info(resolved_path, **kwargs)
        except FileNotFoundError:
            return {"path": resolved_path, "exists": False}
        else:
            if isinstance(info, dict):
                return {
                    "path": resolved_path,
                    "exists": True,
                    "size": info.get("size"),
                    "last_modified": info.get("mtime"),
                    "type": info.get("type", "file"),
                }
            return {
                "path": resolved_path,
                "exists": True,
                "size": info.size,
                "last_modified": info.mtime,
                "type": info.type,
            }

    def sign(self, path: str, expires_in: int = 3600, for_upload: bool = False) -> str:
        """Generate a signed URL for the file."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)
        return f"{self._fs_uri}{resolved_path}"

    def _stream_file_batches(self, obj_path: str | Path) -> "Iterator[ArrowRecordBatch]":
        import pyarrow.parquet as pq

        with self.fs.open(obj_path, mode="rb") as f:
            parquet_file = pq.ParquetFile(f)  # pyright: ignore[reportArgumentType]
            yield from parquet_file.iter_batches()

    def stream_arrow(self, pattern: str, **kwargs: Any) -> "Iterator[ArrowRecordBatch]":
        ensure_pyarrow()

        for obj_path in self.glob(pattern, **kwargs):
            yield from self._stream_file_batches(obj_path)

    async def read_bytes_async(self, path: str | Path, **kwargs: Any) -> bytes:
        """Read bytes from storage asynchronously."""
        return await async_(self.read_bytes)(path, **kwargs)

    async def write_bytes_async(self, path: str | Path, data: bytes, **kwargs: Any) -> None:
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
        ensure_pyarrow()

        return _ArrowStreamer(self, pattern, **kwargs)

    async def read_text_async(self, path: str | Path, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage asynchronously."""
        return await async_(self.read_text)(path, encoding, **kwargs)

    async def write_text_async(self, path: str | Path, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to storage asynchronously."""
        await async_(self.write_text)(path, data, encoding, **kwargs)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects in storage asynchronously."""
        return await async_(self.list_objects)(prefix, recursive, **kwargs)

    async def exists_async(self, path: str | Path, **kwargs: Any) -> bool:
        """Check if object exists in storage asynchronously."""
        return await async_(self.exists)(path, **kwargs)

    async def delete_async(self, path: str | Path, **kwargs: Any) -> None:
        """Delete object from storage asynchronously."""
        await async_(self.delete)(path, **kwargs)

    async def copy_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Copy object in storage asynchronously."""
        await async_(self.copy)(source, destination, **kwargs)

    async def move_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Move object in storage asynchronously."""
        await async_(self.move)(source, destination, **kwargs)

    async def get_metadata_async(self, path: str | Path, **kwargs: Any) -> dict[str, Any]:
        """Get object metadata from storage asynchronously."""
        return await async_(self.get_metadata)(path, **kwargs)

    async def sign_async(self, path: str, expires_in: int = 3600, for_upload: bool = False) -> str:
        """Generate a signed URL asynchronously."""
        return await async_(self.sign)(path, expires_in, for_upload)

    async def read_arrow_async(self, path: str | Path, **kwargs: Any) -> "ArrowTable":
        """Read Arrow table from storage asynchronously."""
        return await async_(self.read_arrow)(path, **kwargs)

    async def write_arrow_async(self, path: str | Path, table: "ArrowTable", **kwargs: Any) -> None:
        """Write Arrow table to storage asynchronously."""
        await async_(self.write_arrow)(path, table, **kwargs)
