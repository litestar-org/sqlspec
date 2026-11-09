"""Storage backend using fsspec."""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Iterator
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, TypeAlias, cast, overload
from urllib.parse import urlparse
from uuid import uuid4

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import StorageCapabilityError
from sqlspec.storage._arrow_payload import (
    StorageFormat,
    encode_arrow_payload,
    encode_arrow_to_async_sink,
    encode_arrow_to_sink,
    join_shielded,
    validate_arrow_write_options,
)
from sqlspec.storage._arrow_stream import iter_parquet_row_groups, validate_parquet_stream_options
from sqlspec.storage._paths import is_file_destination, parse_file_uri_path, resolve_storage_path
from sqlspec.storage._utils import import_fsspec, import_pyarrow, import_pyarrow_parquet, log_storage_event
from sqlspec.storage.backends.base import AsyncArrowBatchIterator, AsyncThreadedBytesIterator
from sqlspec.storage.backends.local import (
    _STAGE_PREFIX,
    _abort_local_stage,
    _close_and_publish_local_stage,
    _open_local_stage,
)
from sqlspec.storage.errors import execute_async_storage_operation, execute_sync_storage_operation, raise_storage_error
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.protocols import AsyncWritableBytesSink, WritableBytesSink
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("FSSpecBackend",)

_FsspecStrategy: TypeAlias = Literal["local", "commit", "block", "staged"]

_OBJECT_STORE_PROTOCOLS = {"s3", "gs", "az", "gcs"}
_FSSPEC_VALIDATED_PROTOCOLS: Final[frozenset[str]] = frozenset({"file", "memory"})
_FSSPEC_BASIC_OPEN_OPTIONS: Final[frozenset[str]] = frozenset({
    "autocommit",
    "block_size",
    "cache_options",
    "compression",
})
_FSSPEC_ARROW_FORMATS: Final[frozenset[str]] = frozenset({"parquet", "arrow-ipc", "csv"})
_COMMIT_PROTOCOLS: Final[frozenset[str]] = frozenset({"s3", "s3a", "gs", "gcs"})
_BLOCK_PROTOCOLS: Final[frozenset[str]] = frozenset({"az", "abfs", "abfss"})
_LOCAL_PROTOCOLS: Final[frozenset[str]] = frozenset({"file", "local"})


@mypyc_attr(allow_interpreted_subclasses=True)
class FSSpecBackend:
    """Storage backend using fsspec.

    Implements ObjectStoreProtocol using fsspec for various protocols
    including HTTP, HTTPS, FTP, and cloud storage services.

    All synchronous methods use the *_sync suffix for consistency with async methods.
    """

    __slots__ = ("_fs_uri", "base_path", "fs", "protocol")

    backend_type: ClassVar[str] = "fsspec"

    def __init__(self, uri: str, **kwargs: Any) -> None:
        """Initialize the fsspec-backed storage backend.

        Args:
            uri: Filesystem URI (protocol://path).
            **kwargs: Additional fsspec configuration options, including an optional base_path.

            For cloud URIs (S3/GS/Azure) and file:// URIs, we derive a default base_path from the
                URI path when no explicit base_path is provided. When both URI and base_path are provided,
                they are combined (base_path is appended to URI-derived path).
        """
        fsspec = import_fsspec()

        explicit_base_path = kwargs.pop("base_path", "")

        if "://" in uri:
            self.protocol = uri.split("://", maxsplit=1)[0]
            self._fs_uri = uri

            if self.protocol in _OBJECT_STORE_PROTOCOLS:
                parsed = urlparse(uri)
                if parsed.netloc:
                    uri_base_path = parsed.netloc
                    if parsed.path and parsed.path != "/":
                        uri_base_path = f"{uri_base_path}{parsed.path}"
                    if explicit_base_path:
                        uri_base_path = f"{uri_base_path.rstrip('/')}/{explicit_base_path.lstrip('/')}"
                    explicit_base_path = uri_base_path
            elif self.protocol == "file":
                parsed_path = parse_file_uri_path(uri)
                if parsed_path and parsed_path != "/":
                    path_obj = Path(parsed_path)
                    if not explicit_base_path and is_file_destination(path_obj):
                        parsed_path = str(path_obj.parent)
                    uri_base_path = parsed_path
                    if explicit_base_path:
                        uri_base_path = f"{uri_base_path.rstrip('/')}/{explicit_base_path.lstrip('/')}"
                    explicit_base_path = uri_base_path
        else:
            self.protocol = uri
            self._fs_uri = f"{uri}://"

        self.base_path = explicit_base_path.rstrip("/") if explicit_base_path else ""

        self.fs = fsspec.filesystem(self.protocol, **kwargs)
        log_storage_event(
            "storage.backend.ready",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="init",
            path=self._fs_uri,
        )

    @property
    def base_uri(self) -> str:
        return self._fs_uri

    @property
    def supports_signing(self) -> bool:
        """Whether this backend supports URL signing.

        FSSpec backends do not support URL signing. Use ObStoreBackend
        for S3, GCS, or Azure if you need signed URLs.

        Returns:
            Always False for fsspec backends.
        """
        return False

    def supports_direct_arrow_write(self, file_format: StorageFormat, *, overwrite: bool = True) -> bool:
        """Return whether direct streaming Arrow writes are supported for the protocol and overwrite mode.

        Args:
            file_format: Target storage format.
            overwrite: Whether existing objects may be replaced.

        Returns:
            True when the format and protocol strategy support direct streaming.
        """
        strategy = _fsspec_write_strategy(self.protocol)
        return file_format in _FSSPEC_ARROW_FORMATS and (overwrite or strategy != "staged")

    def open_write_stream_sync(
        self, path: "str | Path", *, overwrite: bool = True, **kwargs: Any
    ) -> "AbstractContextManager[WritableBytesSink]":
        """Open a synchronous direct write stream using the protocol's commit strategy."""
        strategy, open_mode, options = _prepare_fsspec_stream_options(self.protocol, overwrite, kwargs)
        destination = self._resolve_path(path)
        return _FsspecWriteStream(
            self.fs,
            destination,
            strategy=strategy,
            mode=open_mode,
            options=options,
            overwrite=overwrite,
            backend_type=self.backend_type,
            log_fields={"protocol": self.protocol, "operation": "write_stream", "mode": "sync"},
        )

    def open_write_stream_async(
        self, path: "str | Path", *, overwrite: bool = True, **kwargs: Any
    ) -> "AbstractAsyncContextManager[AsyncWritableBytesSink]":
        """Open an asynchronous direct write stream using the protocol's commit strategy."""
        strategy, open_mode, options = _prepare_fsspec_stream_options(self.protocol, overwrite, kwargs)
        destination = self._resolve_path(path)
        return _AsyncFsspecWriteStream(
            self.fs,
            destination,
            strategy=strategy,
            mode=open_mode,
            options=options,
            overwrite=overwrite,
            backend_type=self.backend_type,
            log_fields={"protocol": self.protocol, "operation": "write_stream", "mode": "async"},
        )

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

    def resolve_uri(self, path: str | Path) -> str:
        """Resolve a backend-relative path to an unsigned address.

        Args:
            path: The same backend-relative path accepted by read and write methods.

        Returns:
            An absolute filesystem path for ``file`` or a protocol-qualified URI
            for other filesystems. The target does not need to exist.
        """
        resolved_path = self._resolve_path(path)
        if self.protocol == "file":
            return str(Path(resolved_path).resolve())
        return str(self.fs.unstrip_protocol(resolved_path))

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
        log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="read_bytes",
            path=resolved_path,
        )
        return result

    def write_bytes_sync(self, path: str | Path, data: bytes, *, overwrite: bool = True, **kwargs: Any) -> None:
        """Write bytes to an object synchronously."""
        open_mode, options = _resolve_fsspec_write_options(self.protocol, overwrite, kwargs)
        resolved_path = self._resolve_path(path)

        if self.protocol == "file":
            parent_dir = str(Path(resolved_path).parent)
            if parent_dir and not self.fs.exists(parent_dir):
                self.fs.makedirs(parent_dir, exist_ok=True)

        execute_sync_storage_operation(
            partial(_write_fsspec_bytes, self.fs, resolved_path, data, open_mode, options),
            backend=self.backend_type,
            operation="write_bytes",
            path=resolved_path,
        )
        log_storage_event(
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

    def write_text_sync(
        self, path: str | Path, data: str, encoding: str = "utf-8", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write text to an object synchronously."""
        self.write_bytes_sync(path, data.encode(encoding), overwrite=overwrite, **kwargs)

    def exists_sync(self, path: str | Path, **kwargs: Any) -> bool:
        """Check if an object exists synchronously."""
        resolved_path = self._resolve_path(path)
        exists = bool(self.fs.exists(resolved_path, **kwargs))
        log_storage_event(
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
        log_storage_event(
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
        log_storage_event(
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
        log_storage_event(
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
        log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="read_arrow",
            path=resolved_path,
        )
        return result

    def write_arrow_sync(self, path: str | Path, table: "ArrowTable", *, overwrite: bool = True, **kwargs: Any) -> None:
        """Write an Arrow table to storage synchronously."""
        format_hint, compression, merged_write_options = _prepare_fsspec_arrow_write(kwargs)
        resolved_path = self._resolve_path(path)

        if self.supports_direct_arrow_write(format_hint, overwrite=overwrite):
            strategy = _fsspec_write_strategy(self.protocol)
            open_mode = "wb" if overwrite else "xb"

            def _do_stream_write() -> None:
                with _FsspecWriteStream(
                    self.fs,
                    resolved_path,
                    strategy=strategy,
                    mode=open_mode,
                    options={},
                    overwrite=overwrite,
                    backend_type=self.backend_type,
                    log_fields=None,
                ) as sink:
                    encode_arrow_to_sink(
                        table,
                        sink,
                        format_hint=format_hint,
                        compression=compression,
                        write_options=merged_write_options,
                    )

            execute_sync_storage_operation(
                _do_stream_write, backend=self.backend_type, operation="write_arrow", path=resolved_path
            )
        else:

            def _do_buffered_write() -> None:
                payload = encode_arrow_payload(
                    table, format_hint, compression=compression, write_options=merged_write_options
                )
                _write_fsspec_bytes(self.fs, resolved_path, payload, "xb", {})

            execute_sync_storage_operation(
                _do_buffered_write, backend=self.backend_type, operation="write_arrow", path=resolved_path
            )

        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_arrow",
            path=resolved_path,
        )

    def list_objects_sync(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
        """List objects with optional prefix synchronously."""
        resolved_prefix = resolve_storage_path(prefix, self.base_path, self.protocol, strip_file_scheme=False)
        if recursive:
            raw_results = execute_sync_storage_operation(
                partial(self.fs.find, resolved_prefix, **kwargs),
                backend=self.backend_type,
                operation="list_objects",
                path=resolved_prefix,
            )
            results = sorted(raw_results)
        else:
            raw_results = execute_sync_storage_operation(
                partial(self.fs.ls, resolved_prefix, detail=False, **kwargs),
                backend=self.backend_type,
                operation="list_objects",
                path=resolved_prefix,
            )
            results = sorted(raw_results)
        log_storage_event(
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
        raw_results = execute_sync_storage_operation(
            partial(self.fs.glob, resolved_pattern, **kwargs),
            backend=self.backend_type,
            operation="glob",
            path=resolved_pattern,
        )
        results = cast("list[str]", sorted(raw_results))
        log_storage_event(
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
        """Stream bytes from storage synchronously.

        Yields:
            Chunks of bytes from the file, with size determined by chunk_size (default: 65536 bytes).
        """
        resolved_path = self._resolve_path(path)
        chunk_size = chunk_size or 65536

        file_handle = execute_sync_storage_operation(
            partial(self.fs.open, resolved_path, mode="rb", **kwargs),
            backend=self.backend_type,
            operation="stream_read",
            path=resolved_path,
        )
        with file_handle as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield cast("bytes", chunk)

    def stream_arrow_sync(
        self, pattern: str, *, file_format: Literal["parquet"] = "parquet", batch_size: int = 65_536, **kwargs: Any
    ) -> Iterator["ArrowRecordBatch"]:
        """Stream Arrow record batches from storage synchronously.

        Args:
            pattern: The glob pattern to match.
            file_format: Storage format. Only Parquet supports bounded batch streaming.
            batch_size: Maximum number of rows in each yielded record batch.
            **kwargs: Additional arguments passed to PyArrow batch iteration.

        Yields:
            Arrow record batches from matching files.
        """
        validate_parquet_stream_options(pattern, file_format, batch_size)
        pq = import_pyarrow_parquet()
        arrow_schema = kwargs.pop("arrow_schema", None)
        kwargs.pop("read_buffer_bytes", None)
        for obj_path in self.glob_sync(pattern):
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
                yield from iter_parquet_row_groups(
                    parquet_file, batch_size=batch_size, arrow_schema=arrow_schema, **kwargs
                )

    async def read_bytes_async(self, path: "str | Path", **kwargs: Any) -> bytes:
        """Read bytes from storage asynchronously."""
        return await async_(self.read_bytes_sync)(path, **kwargs)

    async def write_bytes_async(
        self, path: "str | Path", data: bytes, *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write bytes to storage asynchronously."""
        await async_(self.write_bytes_sync)(path, data, overwrite=overwrite, **kwargs)

    async def stream_read_async(
        self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        """Stream bytes from storage asynchronously.

        Uses AsyncThreadedBytesIterator to read chunks of the file in a thread pool,
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

        file_obj = await execute_async_storage_operation(
            partial(async_(self.fs.open), resolved_path, mode="rb", **kwargs),
            backend=self.backend_type,
            operation="stream_read",
            path=resolved_path,
        )

        return AsyncThreadedBytesIterator(file_obj, chunk_size)

    def stream_arrow_async(
        self, pattern: str, *, file_format: Literal["parquet"] = "parquet", batch_size: int = 65_536, **kwargs: Any
    ) -> AsyncIterator["ArrowRecordBatch"]:
        """Stream Arrow record batches from storage asynchronously.

        Args:
            pattern: The glob pattern to match.
            file_format: Storage format. Only Parquet supports bounded batch streaming.
            batch_size: Maximum number of rows in each yielded record batch.
            **kwargs: Additional arguments passed to PyArrow batch iteration.

        Returns:
            AsyncIterator yielding Arrow record batches.
        """
        return AsyncArrowBatchIterator(
            self.stream_arrow_sync(pattern, file_format=file_format, batch_size=batch_size, **kwargs)
        )

    async def read_text_async(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage asynchronously."""
        return await async_(self.read_text_sync)(path, encoding, **kwargs)

    async def write_text_async(
        self, path: str | Path, data: str, encoding: str = "utf-8", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write text to storage asynchronously."""
        await async_(self.write_text_sync)(path, data, encoding, overwrite=overwrite, **kwargs)

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

    async def write_arrow_async(
        self, path: str | Path, table: "ArrowTable", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write an Arrow table to storage asynchronously."""
        format_hint, compression, merged_write_options = _prepare_fsspec_arrow_write(kwargs)
        resolved_path = self._resolve_path(path)

        if self.supports_direct_arrow_write(format_hint, overwrite=overwrite):
            strategy = _fsspec_write_strategy(self.protocol)
            open_mode = "wb" if overwrite else "xb"

            async def _do_stream_write_async() -> None:
                async with _AsyncFsspecWriteStream(
                    self.fs,
                    resolved_path,
                    strategy=strategy,
                    mode=open_mode,
                    options={},
                    overwrite=overwrite,
                    backend_type=self.backend_type,
                    log_fields=None,
                ) as sink:
                    await encode_arrow_to_async_sink(
                        table,
                        sink,
                        format_hint=format_hint,
                        compression=compression,
                        write_options=merged_write_options,
                    )

            await execute_async_storage_operation(
                _do_stream_write_async, backend=self.backend_type, operation="write_arrow", path=resolved_path
            )
        else:

            def _encode_and_write_xb() -> None:
                payload = encode_arrow_payload(
                    table, format_hint, compression=compression, write_options=merged_write_options
                )
                _write_fsspec_bytes(self.fs, resolved_path, payload, "xb", {})

            await execute_async_storage_operation(
                async_(_encode_and_write_xb), backend=self.backend_type, operation="write_arrow", path=resolved_path
            )

        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_arrow",
            path=resolved_path,
        )

    def _resolve_path(self, path: str | Path) -> str:
        return resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=False)

    def _read_parquet_table(self, resolved_path: str, pq: Any, options: "dict[str, Any]") -> Any:
        with self.fs.open(resolved_path, mode="rb", **options) as file_obj:
            return pq.read_table(file_obj)


def _resolve_fsspec_write_options(
    protocol: str, overwrite: bool, kwargs: "dict[str, Any]"
) -> "tuple[str, dict[str, Any]]":
    """Validate and resolve fsspec open mode and options for byte writes.

    Local and in-memory filesystems silently accept unknown ``open`` options, so
    only the options they honor are allowed there. Remote filesystems receive
    every option unchanged.

    Args:
        protocol: The fsspec protocol of the backend.
        overwrite: Whether an existing object may be replaced.
        kwargs: Caller-supplied ``fs.open`` options.

    Returns:
        The ``open`` mode (``"wb"`` or ``"xb"``) and the options to pass to ``fs.open``.

    Raises:
        TypeError: If ``mode`` or an option the local or in-memory filesystem ignores is passed.
    """
    if kwargs:
        if "mode" in kwargs:
            msg = "Unsupported keyword argument 'mode' for write_bytes; use 'overwrite' instead."
            raise TypeError(msg)
        if protocol in _FSSPEC_VALIDATED_PROTOCOLS:
            unsupported = [name for name in kwargs if name not in _FSSPEC_BASIC_OPEN_OPTIONS]
            if unsupported:
                names = ", ".join(sorted(unsupported))
                msg = f"Unsupported write keyword argument(s) for FSSpecBackend ({protocol}): {names}"
                raise TypeError(msg)
    return ("wb" if overwrite else "xb"), kwargs


def _write_fsspec_bytes(fs: Any, resolved_path: str, data: bytes, open_mode: str, options: "dict[str, Any]") -> None:
    """Write raw bytes via an fsspec filesystem handle."""
    with fs.open(resolved_path, mode=open_mode, **options) as file_obj:
        file_obj.write(data)  # pyright: ignore


def _prepare_fsspec_arrow_write(kwargs: "dict[str, Any]") -> "tuple[StorageFormat, str | None, dict[str, Any]]":
    """Validate and extract format_hint, compression, and writer options for FSSpecBackend Arrow writes."""
    import_pyarrow()
    format_hint: StorageFormat = kwargs.pop("format_hint", "parquet")
    if format_hint == "parquet":
        import_pyarrow_parquet()
    if format_hint not in _FSSPEC_ARROW_FORMATS:
        msg = f"Arrow storage writes support only Parquet, Arrow IPC, and CSV formats, got {format_hint!r}"
        raise StorageCapabilityError(
            msg, capability="arrow_write", remediation="Write row payloads with the row storage APIs instead."
        )

    write_options_arg = kwargs.pop("write_options", None)
    merged_write_options: dict[str, Any] = dict(write_options_arg) if write_options_arg else {}
    has_compression_in_kwargs = "compression" in kwargs
    compression_arg = kwargs.pop("compression", None)
    merged_write_options.update(kwargs)

    if has_compression_in_kwargs:
        compression: str | None = compression_arg
    elif "compression" in merged_write_options:
        compression = merged_write_options.get("compression")
    elif format_hint == "parquet":
        compression = "snappy"
    else:
        compression = None

    validate_arrow_write_options(format_hint, merged_write_options)
    return format_hint, compression, merged_write_options


def _fsspec_write_strategy(protocol: str) -> "_FsspecStrategy":
    """Return the direct-stream commit strategy for an fsspec protocol."""
    if protocol in _LOCAL_PROTOCOLS:
        return "local"
    if protocol in _COMMIT_PROTOCOLS:
        return "commit"
    if protocol in _BLOCK_PROTOCOLS:
        return "block"
    return "staged"


def _prepare_fsspec_stream_options(
    protocol: str, overwrite: bool, kwargs: "dict[str, Any]"
) -> "tuple[_FsspecStrategy, str, dict[str, Any]]":
    """Validate stream options and resolve strategy, open mode, and fs.open kwargs."""
    if "autocommit" in kwargs:
        msg = "Unsupported keyword argument 'autocommit' for open_write_stream; commit lifecycle is managed by the stream."
        raise TypeError(msg)
    open_mode, options = _resolve_fsspec_write_options(protocol, overwrite, dict(kwargs))
    strategy = _fsspec_write_strategy(protocol)
    if strategy == "staged" and not overwrite:
        msg = f"Direct write streams do not support overwrite=False for fsspec protocol {protocol!r}"
        raise StorageCapabilityError(msg, capability="overwrite_false_stream")
    return strategy, open_mode, options


def _as_create_conflict(error: Exception, path: str) -> Exception:
    """Return FileExistsError for an S3 precondition failure, otherwise the original error."""
    cause = getattr(error, "__cause__", None)
    response = getattr(cause, "response", None)
    if isinstance(response, dict):
        error_payload = response.get("Error")
        if isinstance(error_payload, dict) and error_payload.get("Code") == "PreconditionFailed":
            return FileExistsError(path)
    return error


def _forget_transaction_file(fs: Any, file: Any) -> None:
    """Remove a deferred-commit file from the filesystem's cached transaction list."""
    tx = getattr(fs, "transaction", None)
    files = getattr(tx, "files", None)
    if files is None:
        return
    remove_fn = getattr(files, "remove", None)
    if not callable(remove_fn):
        return
    for _ in range(3):
        try:
            remove_fn(file)
        except ValueError:
            return
        except IndexError:
            continue
        else:
            return


def _fsspec_stage_key(destination: str) -> str:
    """Construct a hidden sibling stage key in the destination's parent prefix."""
    if "/" in destination:
        parent, _ = destination.rsplit("/", 1)
        return f"{parent}/{_STAGE_PREFIX}{uuid4().hex}" if parent else f"/{_STAGE_PREFIX}{uuid4().hex}"
    return f"{_STAGE_PREFIX}{uuid4().hex}"


def _open_fsspec_stream_target(
    strategy: "_FsspecStrategy", fs: Any, destination: str, mode: str, options: "dict[str, Any]"
) -> "tuple[str | None, Any]":
    """Open the underlying file handle for an fsspec direct write stream."""
    if strategy == "local":
        stage_path, file_obj = _open_local_stage(Path(destination))
        return str(stage_path), file_obj
    if strategy == "commit":
        committed_file = fs.open(destination, mode=mode, autocommit=False, **options)
        _forget_transaction_file(fs, committed_file)
        return None, committed_file
    if strategy == "block":
        return None, fs.open(destination, mode=mode, **options)
    stage_key = _fsspec_stage_key(destination)
    return stage_key, fs.open(stage_key, mode="wb", **options)


def _close_and_publish_fsspec_stream(
    strategy: "_FsspecStrategy", fs: Any, stage: "str | None", file_obj: Any, destination: str, *, overwrite: bool
) -> None:
    """Close and atomically publish an fsspec direct write stream."""
    if strategy == "local":
        assert stage is not None
        _close_and_publish_local_stage(Path(stage), file_obj, Path(destination), overwrite=overwrite)
        return
    if strategy == "commit":
        try:
            file_obj.close()
            file_obj.commit()
        except Exception as error:
            with contextlib.suppress(Exception):
                file_obj.discard()
            with contextlib.suppress(Exception):
                file_obj.closed = True
            mapped = _as_create_conflict(error, destination)
            if mapped is not error:
                raise mapped from error
            raise
        except BaseException:
            with contextlib.suppress(Exception):
                file_obj.discard()
            with contextlib.suppress(Exception):
                file_obj.closed = True
            raise
        return
    if strategy == "block":
        file_obj.close()
        return
    assert stage is not None
    try:
        file_obj.close()
        fs.mv(stage, destination)
    finally:
        with contextlib.suppress(Exception):
            if fs.exists(stage):
                fs.rm(stage)


def _abort_fsspec_stream(strategy: "_FsspecStrategy", fs: Any, stage: "str | None", file_obj: Any) -> None:
    """Abort an fsspec direct write stream without touching the destination."""
    if strategy == "local":
        if stage is not None:
            _abort_local_stage(Path(stage), file_obj)
        return
    if strategy == "commit":
        with contextlib.suppress(Exception):
            file_obj.discard()
        with contextlib.suppress(Exception):
            file_obj.closed = True
        return
    if strategy == "block":
        with contextlib.suppress(Exception):
            file_obj.forced = True
        with contextlib.suppress(Exception):
            file_obj.close()
        return
    with contextlib.suppress(Exception):
        file_obj.close()
    if stage is not None:
        with contextlib.suppress(Exception):
            fs.rm(stage)


class _FsspecSink:
    """Write to one fsspec file, normalizing storage failures."""

    __slots__ = ("_backend", "_closed", "_file", "_path")

    def __init__(self, file: Any, *, backend: str, path: str) -> None:
        self._file = file
        self._backend = backend
        self._path = path
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed or bool(getattr(self._file, "closed", False))

    def write(self, data: Any, /) -> int:
        if self._closed:
            return 0
        payload = bytes(data) if isinstance(data, (bytearray, memoryview)) else data
        if not payload:
            return 0
        try:
            return int(self._file.write(payload))
        except Exception as error:
            raise_storage_error(error, backend=self._backend, operation="write_stream", path=self._path)

    def flush(self) -> None:
        if self._closed:
            return
        flush_fn = getattr(self._file, "flush", None)
        if callable(flush_fn):
            try:
                flush_fn()
            except Exception as error:
                raise_storage_error(error, backend=self._backend, operation="write_stream", path=self._path)

    def close(self) -> None:
        self._closed = True


class _AsyncFsspecSink:
    """Run fsspec file writes off the event loop, normalizing storage failures."""

    __slots__ = ("_backend", "_closed", "_file", "_path")

    def __init__(self, file: Any, *, backend: str, path: str) -> None:
        self._file = file
        self._backend = backend
        self._path = path
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed or bool(getattr(self._file, "closed", False))

    async def write(self, data: Any, /) -> int:
        if self._closed:
            return 0
        payload = bytes(data) if isinstance(data, (bytearray, memoryview)) else data
        if not payload:
            return 0
        try:
            return int(await async_(self._file.write)(payload))
        except Exception as error:
            raise_storage_error(error, backend=self._backend, operation="write_stream", path=self._path)

    async def flush(self) -> None:
        if self._closed:
            return
        flush_fn = getattr(self._file, "flush", None)
        if callable(flush_fn):
            try:
                await async_(flush_fn)()
            except Exception as error:
                raise_storage_error(error, backend=self._backend, operation="write_stream", path=self._path)

    async def close(self) -> None:
        self._closed = True


class _FsspecWriteStream:
    """Synchronous fsspec write stream context manager."""

    __slots__ = (
        "_backend_type",
        "_destination",
        "_file",
        "_fs",
        "_log_fields",
        "_mode",
        "_options",
        "_overwrite",
        "_sink",
        "_stage",
        "_strategy",
    )

    def __init__(
        self,
        fs: Any,
        destination: str,
        *,
        strategy: "_FsspecStrategy",
        mode: str,
        options: "dict[str, Any]",
        overwrite: bool,
        backend_type: str,
        log_fields: "dict[str, Any] | None",
    ) -> None:
        self._fs = fs
        self._destination = destination
        self._strategy: _FsspecStrategy = strategy
        self._mode = mode
        self._options = options
        self._overwrite = overwrite
        self._backend_type = backend_type
        self._log_fields = log_fields
        self._stage: str | None = None
        self._file: Any = None
        self._sink: _FsspecSink | None = None

    def __enter__(self) -> "_FsspecSink":
        stage, file_obj = execute_sync_storage_operation(
            lambda: _open_fsspec_stream_target(self._strategy, self._fs, self._destination, self._mode, self._options),
            backend=self._backend_type,
            operation="open_write_stream",
            path=self._destination,
        )
        self._stage = stage
        self._file = file_obj
        sink = _FsspecSink(file_obj, backend=self._backend_type, path=self._destination)
        self._sink = sink
        return sink

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        _ = (exc, traceback)
        sink = self._sink
        if sink is not None:
            sink._closed = True
        file_obj = self._file
        if file_obj is None:
            return
        if exc_type is None:
            execute_sync_storage_operation(
                partial(
                    _close_and_publish_fsspec_stream,
                    self._strategy,
                    self._fs,
                    self._stage,
                    file_obj,
                    self._destination,
                    overwrite=self._overwrite,
                ),
                backend=self._backend_type,
                operation="write_stream_close",
                path=self._destination,
            )
            if self._log_fields is not None:
                log_storage_event(
                    "storage.write", backend_type=self._backend_type, path=self._destination, **self._log_fields
                )
        else:
            _abort_fsspec_stream(self._strategy, self._fs, self._stage, file_obj)


class _AsyncFsspecWriteStream:
    """Asynchronous fsspec write stream context manager with shielded completion."""

    __slots__ = (
        "_backend_type",
        "_destination",
        "_file",
        "_fs",
        "_log_fields",
        "_mode",
        "_options",
        "_overwrite",
        "_sink",
        "_stage",
        "_strategy",
    )

    def __init__(
        self,
        fs: Any,
        destination: str,
        *,
        strategy: "_FsspecStrategy",
        mode: str,
        options: "dict[str, Any]",
        overwrite: bool,
        backend_type: str,
        log_fields: "dict[str, Any] | None",
    ) -> None:
        self._fs = fs
        self._destination = destination
        self._strategy: _FsspecStrategy = strategy
        self._mode = mode
        self._options = options
        self._overwrite = overwrite
        self._backend_type = backend_type
        self._log_fields = log_fields
        self._stage: str | None = None
        self._file: Any = None
        self._sink: _AsyncFsspecSink | None = None

    async def __aenter__(self) -> "_AsyncFsspecSink":
        open_fut = asyncio.ensure_future(
            async_(
                lambda: _open_fsspec_stream_target(
                    self._strategy, self._fs, self._destination, self._mode, self._options
                )
            )()
        )
        try:
            stage, file_obj = await execute_async_storage_operation(
                partial(join_shielded, open_fut),
                backend=self._backend_type,
                operation="open_write_stream",
                path=self._destination,
            )
        except asyncio.CancelledError:
            if open_fut.done() and not open_fut.cancelled() and open_fut.exception() is None:
                opened_stage, opened_file = open_fut.result()
                await join_shielded(
                    asyncio.ensure_future(
                        async_(_abort_fsspec_stream)(self._strategy, self._fs, opened_stage, opened_file)
                    ),
                    suppress_result=True,
                )
            raise
        self._stage = stage
        self._file = file_obj
        sink = _AsyncFsspecSink(file_obj, backend=self._backend_type, path=self._destination)
        self._sink = sink
        return sink

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        _ = (exc, traceback)
        sink = self._sink
        if sink is not None:
            sink._closed = True
        file_obj = self._file
        if file_obj is None:
            return
        if exc_type is None:
            publish_fut = asyncio.ensure_future(
                async_(
                    partial(
                        _close_and_publish_fsspec_stream,
                        self._strategy,
                        self._fs,
                        self._stage,
                        file_obj,
                        self._destination,
                        overwrite=self._overwrite,
                    )
                )()
            )
            await execute_async_storage_operation(
                partial(join_shielded, publish_fut),
                backend=self._backend_type,
                operation="write_stream_close",
                path=self._destination,
            )
            if self._log_fields is not None:
                log_storage_event(
                    "storage.write", backend_type=self._backend_type, path=self._destination, **self._log_fields
                )
        else:
            abort_fut = asyncio.ensure_future(
                async_(_abort_fsspec_stream)(self._strategy, self._fs, self._stage, file_obj)
            )
            await join_shielded(abort_fut, suppress_result=True)
