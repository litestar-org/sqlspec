# pyright: reportPrivateUsage=false
import asyncio
import logging
from collections.abc import AsyncIterator, Iterator
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast, overload
from urllib.parse import urlparse

from mypy_extensions import mypyc_attr

from sqlspec.storage._utils import import_pyarrow_parquet, resolve_storage_path
from sqlspec.storage.backends.base import AsyncArrowBatchIterator, AsyncThreadedBytesIterator
from sqlspec.storage.errors import execute_sync_storage_operation
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.module_loader import ensure_fsspec
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("FSSpecBackend",)

logger = get_logger(__name__)


def _log_storage_event(
    event: str,
    *,
    backend_type: str,
    protocol: str,
    operation: str | None = None,
    path: str | None = None,
    source_path: str | None = None,
    destination_path: str | None = None,
    count: int | None = None,
    exists: bool | None = None,
) -> None:
    fields: dict[str, Any] = {
        "backend_type": backend_type,
        "protocol": protocol,
        "path": path,
        "source_path": source_path,
        "destination_path": destination_path,
        "count": count,
        "exists": exists,
    }
    if operation is not None:
        fields["operation"] = operation
    log_with_context(logger, logging.DEBUG, event, **fields)


def _write_fsspec_bytes(fs: Any, resolved_path: str, data: bytes, options: "dict[str, Any]") -> None:
    """Write raw bytes via an fsspec filesystem handle."""
    with fs.open(resolved_path, mode="wb", **options) as file_obj:
        file_obj.write(data)  # pyright: ignore


def _write_fsspec_arrow(fs: Any, resolved_path: str, table: "ArrowTable", pq: Any, options: "dict[str, Any]") -> None:
    """Write an Arrow table via an fsspec filesystem handle."""
    with fs.open(resolved_path, mode="wb") as file_obj:
        pq.write_table(table, file_obj, **options)  # pyright: ignore


@mypyc_attr(allow_interpreted_subclasses=True)
class FSSpecBackend:
    """Storage backend using fsspec.

    Implements ObjectStoreProtocol using fsspec for various protocols
    including HTTP, HTTPS, FTP, and cloud storage services.

    All synchronous methods use the *_sync suffix for consistency with async methods.
    """

    __slots__ = ("_fs_uri", "backend_type", "base_path", "fs", "protocol")

    def __init__(self, uri: str, **kwargs: Any) -> None:
        """Initialize the fsspec-backed storage backend.

        Args:
            uri: Filesystem URI (protocol://path).
            **kwargs: Additional fsspec configuration options, including an optional base_path.

        For cloud URIs (S3/GS/Azure) and file:// URIs, we derive a default base_path from the
        URI path when no explicit base_path is provided. When both URI and base_path are provided,
        they are combined (base_path is appended to URI-derived path).

        Examples:
            - FSSpecBackend("s3://bucket/prefix") -> base_path = "bucket/prefix"
            - FSSpecBackend("file:///home/user/storage") -> base_path = "/home/user/storage"
            - FSSpecBackend("file:///home/user", base_path="subdir") -> base_path = "/home/user/subdir"
        """
        ensure_fsspec()
        import fsspec

        explicit_base_path = kwargs.pop("base_path", "")

        if "://" in uri:
            self.protocol = uri.split("://", maxsplit=1)[0]
            self._fs_uri = uri

            if self.protocol in {"s3", "gs", "az", "gcs"}:
                parsed = urlparse(uri)
                if parsed.netloc:
                    uri_base_path = parsed.netloc
                    if parsed.path and parsed.path != "/":
                        uri_base_path = f"{uri_base_path}{parsed.path}"
                    # Combine URI path with explicit base_path if both provided
                    if explicit_base_path:
                        uri_base_path = f"{uri_base_path.rstrip('/')}/{explicit_base_path.lstrip('/')}"
                    explicit_base_path = uri_base_path
            elif self.protocol == "file":
                parsed = urlparse(uri)
                if parsed.path and parsed.path != "/":
                    # For file protocol, keep the path as-is (preserve leading slash for absolute paths)
                    uri_base_path = parsed.path
                    # Combine URI path with explicit base_path if both provided
                    if explicit_base_path:
                        uri_base_path = f"{uri_base_path.rstrip('/')}/{explicit_base_path.lstrip('/')}"
                    explicit_base_path = uri_base_path
        else:
            self.protocol = uri
            self._fs_uri = f"{uri}://"

        self.base_path = explicit_base_path.rstrip("/") if explicit_base_path else ""

        self.fs = fsspec.filesystem(self.protocol, **kwargs)
        self.backend_type = "fsspec"

        _log_storage_event(
            "storage.backend.ready",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="init",
            path=self._fs_uri,
        )

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

    def _resolve_path(self, path: str | Path) -> str:
        return resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)

    def read_bytes_sync(self, path: str | Path, **kwargs: Any) -> bytes:
        """Read bytes from an object synchronously."""
        resolved_path = self._resolve_path(path)
        result = cast(
            "bytes",
            execute_sync_storage_operation(
                partial(self.fs.cat, resolved_path, **kwargs),
                backend=self.backend_type,
                operation="read_bytes",
                path=resolved_path,
            ),
        )
        _log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="read_bytes",
            path=resolved_path,
        )
        return result

    def write_bytes_sync(self, path: str | Path, data: bytes, **kwargs: Any) -> None:
        """Write bytes to an object synchronously."""
        resolved_path = self._resolve_path(path)

        if self.protocol == "file":
            parent_dir = str(Path(resolved_path).parent)
            if parent_dir and not self.fs.exists(parent_dir):
                self.fs.makedirs(parent_dir, exist_ok=True)

        execute_sync_storage_operation(
            partial(_write_fsspec_bytes, self.fs, resolved_path, data, kwargs),
            backend=self.backend_type,
            operation="write_bytes",
            path=resolved_path,
        )
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_bytes",
            path=resolved_path,
        )

    def read_text_sync(self, path: str | Path, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from an object synchronously."""
        data = self.read_bytes_sync(path, **kwargs)
        return data.decode(encoding)

    def write_text_sync(self, path: str | Path, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to an object synchronously."""
        self.write_bytes_sync(path, data.encode(encoding), **kwargs)

    def exists_sync(self, path: str | Path, **kwargs: Any) -> bool:
        """Check if an object exists synchronously."""
        resolved_path = self._resolve_path(path)
        exists = bool(self.fs.exists(resolved_path, **kwargs))
        _log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="exists",
            path=resolved_path,
            exists=bool(exists),
        )
        return exists

    def delete_sync(self, path: str | Path, **kwargs: Any) -> None:
        """Delete an object synchronously."""
        resolved_path = self._resolve_path(path)
        execute_sync_storage_operation(
            partial(self.fs.rm, resolved_path, **kwargs),
            backend=self.backend_type,
            operation="delete",
            path=resolved_path,
        )
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="delete",
            path=resolved_path,
        )

    def copy_sync(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Copy an object synchronously."""
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        execute_sync_storage_operation(
            partial(self.fs.copy, source_path, dest_path, **kwargs),
            backend=self.backend_type,
            operation="copy",
            path=f"{source_path}->{dest_path}",
        )
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="copy",
            source_path=source_path,
            destination_path=dest_path,
        )

    def move_sync(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Move an object synchronously."""
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        execute_sync_storage_operation(
            partial(self.fs.mv, source_path, dest_path, **kwargs),
            backend=self.backend_type,
            operation="move",
            path=f"{source_path}->{dest_path}",
        )
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="move",
            source_path=source_path,
            destination_path=dest_path,
        )

    def read_arrow_sync(self, path: str | Path, **kwargs: Any) -> "ArrowTable":
        """Read an Arrow table from storage synchronously."""
        pq = import_pyarrow_parquet()

        resolved_path = self._resolve_path(path)
        result = cast(
            "ArrowTable",
            execute_sync_storage_operation(
                partial(self._read_parquet_table, resolved_path, pq, kwargs),
                backend=self.backend_type,
                operation="read_arrow",
                path=resolved_path,
            ),
        )
        _log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="read_arrow",
            path=resolved_path,
        )
        return result

    def write_arrow_sync(self, path: str | Path, table: "ArrowTable", **kwargs: Any) -> None:
        """Write an Arrow table to storage synchronously."""
        pq = import_pyarrow_parquet()

        resolved_path = self._resolve_path(path)

        execute_sync_storage_operation(
            partial(_write_fsspec_arrow, self.fs, resolved_path, table, pq, kwargs),
            backend=self.backend_type,
            operation="write_arrow",
            path=resolved_path,
        )
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_arrow",
            path=resolved_path,
        )

    def _read_parquet_table(self, resolved_path: str, pq: Any, options: "dict[str, Any]") -> Any:
        with self.fs.open(resolved_path, mode="rb", **options) as file_obj:
            return pq.read_table(file_obj)

    def list_objects_sync(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
        """List objects with optional prefix synchronously."""
        resolved_prefix = resolve_storage_path(prefix, self.base_path, self.protocol, strip_file_scheme=False)
        if recursive:
            results = sorted(self.fs.find(resolved_prefix, **kwargs))
        else:
            results = sorted(self.fs.ls(resolved_prefix, detail=False, **kwargs))
        _log_storage_event(
            "storage.list",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="list_objects",
            path=resolved_prefix,
            count=len(results),
        )
        return results

    def glob_sync(self, pattern: str, **kwargs: Any) -> "list[str]":
        """Find objects matching a glob pattern synchronously."""
        resolved_pattern = resolve_storage_path(pattern, self.base_path, self.protocol, strip_file_scheme=False)
        results = sorted(self.fs.glob(resolved_pattern, **kwargs))  # pyright: ignore
        _log_storage_event(
            "storage.list",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="glob",
            path=resolved_pattern,
            count=len(results),
        )
        return results

    def is_object_sync(self, path: str | Path) -> bool:
        """Check if path points to an object synchronously."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)
        return self.fs.exists(resolved_path) and not self.fs.isdir(resolved_path)

    def is_path_sync(self, path: str | Path) -> bool:
        """Check if path points to a prefix (directory-like) synchronously."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)
        return self.fs.isdir(resolved_path)  # type: ignore[no-any-return]

    def get_metadata_sync(self, path: str | Path, **kwargs: Any) -> "dict[str, object]":
        """Get object metadata synchronously."""
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

    @property
    def supports_signing(self) -> bool:
        """Whether this backend supports URL signing.

        FSSpec backends do not support URL signing. Use ObStoreBackend
        for S3, GCS, or Azure if you need signed URLs.

        Returns:
            Always False for fsspec backends.
        """
        return False

    @overload
    def sign_sync(self, paths: str, expires_in: int = 3600, for_upload: bool = False) -> str: ...

    @overload
    def sign_sync(self, paths: "list[str]", expires_in: int = 3600, for_upload: bool = False) -> "list[str]": ...

    def sign_sync(
        self, paths: "str | list[str]", expires_in: int = 3600, for_upload: bool = False
    ) -> "str | list[str]":
        """Generate signed URL(s).

        Raises:
            NotImplementedError: fsspec backends do not support URL signing.
                Use obstore backend for S3, GCS, or Azure if you need signed URLs.
        """
        msg = (
            f"URL signing is not supported for fsspec backend (protocol: {self.protocol}). "
            "For S3, GCS, or Azure signed URLs, use ObStoreBackend instead."
        )
        raise NotImplementedError(msg)

    def stream_read_sync(self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any) -> Iterator[bytes]:
        """Stream bytes from storage synchronously."""
        resolved_path = self._resolve_path(path)
        chunk_size = chunk_size or 65536

        with self.fs.open(resolved_path, mode="rb", **kwargs) as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    def stream_arrow_sync(self, pattern: str, **kwargs: Any) -> Iterator["ArrowRecordBatch"]:
        """Stream Arrow record batches from storage synchronously.

        Args:
            pattern: The glob pattern to match.
            **kwargs: Additional arguments to pass to the glob method.

        Yields:
            Arrow record batches from matching files.
        """
        pq = import_pyarrow_parquet()
        for obj_path in self.glob_sync(pattern, **kwargs):
            file_handle = execute_sync_storage_operation(
                partial(self.fs.open, obj_path, mode="rb"),
                backend=self.backend_type,
                operation="stream_open",
                path=str(obj_path),
            )
            with file_handle as stream:
                parquet_file = execute_sync_storage_operation(
                    partial(pq.ParquetFile, stream),
                    backend=self.backend_type,
                    operation="stream_arrow",
                    path=str(obj_path),
                )
                yield from parquet_file.iter_batches()  # pyright: ignore[reportUnknownMemberType]

    async def read_bytes_async(self, path: "str | Path", **kwargs: Any) -> bytes:
        """Read bytes from storage asynchronously."""
        return await async_(self.read_bytes_sync)(path, **kwargs)

    async def write_bytes_async(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:
        """Write bytes to storage asynchronously."""
        return await async_(self.write_bytes_sync)(path, data, **kwargs)

    async def stream_read_async(
        self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        """Stream bytes from storage asynchronously.

        Uses asyncio.to_thread() to read chunks of the file in a thread pool,
        ensuring the event loop is not blocked while avoiding buffering the
        entire file into memory.

        Args:
            path: Path to the file to read.
            chunk_size: Size of chunks to yield (default: 65536 bytes).
            **kwargs: Additional arguments passed to fs.open.

        Returns:
            AsyncIterator yielding chunks of bytes.
        """
        resolved_path = self._resolve_path(path)
        chunk_size = chunk_size or 65536

        # Open the file in a thread pool
        file_obj = await asyncio.to_thread(self.fs.open, resolved_path, mode="rb", **kwargs)

        return AsyncThreadedBytesIterator(file_obj, chunk_size)

    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator["ArrowRecordBatch"]:
        """Stream Arrow record batches from storage asynchronously.

        Args:
            pattern: The glob pattern to match.
            **kwargs: Additional arguments to pass to the glob method.

        Returns:
            AsyncIterator yielding Arrow record batches.
        """
        return AsyncArrowBatchIterator(self.stream_arrow_sync(pattern, **kwargs))

    async def read_text_async(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage asynchronously."""
        return await async_(self.read_text_sync)(path, encoding, **kwargs)

    async def write_text_async(self, path: str | Path, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to storage asynchronously."""
        await async_(self.write_text_sync)(path, data, encoding, **kwargs)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
        """List objects in storage asynchronously."""
        return await async_(self.list_objects_sync)(prefix, recursive, **kwargs)

    async def exists_async(self, path: str | Path, **kwargs: Any) -> bool:
        """Check if object exists in storage asynchronously."""
        return await async_(self.exists_sync)(path, **kwargs)

    async def delete_async(self, path: str | Path, **kwargs: Any) -> None:
        """Delete object from storage asynchronously."""
        await async_(self.delete_sync)(path, **kwargs)

    async def copy_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Copy object in storage asynchronously."""
        await async_(self.copy_sync)(source, destination, **kwargs)

    async def move_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:
        """Move object in storage asynchronously."""
        await async_(self.move_sync)(source, destination, **kwargs)

    async def get_metadata_async(self, path: str | Path, **kwargs: Any) -> "dict[str, object]":
        """Get object metadata from storage asynchronously."""
        return await async_(self.get_metadata_sync)(path, **kwargs)

    @overload
    async def sign_async(self, paths: str, expires_in: int = 3600, for_upload: bool = False) -> str: ...

    @overload
    async def sign_async(self, paths: "list[str]", expires_in: int = 3600, for_upload: bool = False) -> "list[str]": ...

    async def sign_async(
        self, paths: "str | list[str]", expires_in: int = 3600, for_upload: bool = False
    ) -> "str | list[str]":
        """Generate signed URL(s) asynchronously."""
        return await async_(self.sign_sync)(paths, expires_in, for_upload)  # type: ignore[arg-type]

    async def read_arrow_async(self, path: str | Path, **kwargs: Any) -> "ArrowTable":
        """Read Arrow table from storage asynchronously.

        Uses async_() with storage limiter to offload blocking PyArrow I/O to thread pool.
        """
        return await async_(self.read_arrow_sync)(path, **kwargs)

    async def write_arrow_async(self, path: str | Path, table: "ArrowTable", **kwargs: Any) -> None:
        """Write Arrow table to storage asynchronously.

        Uses async_() with storage limiter to offload blocking PyArrow I/O to thread pool.
        """
        await async_(self.write_arrow_sync)(path, table, **kwargs)
