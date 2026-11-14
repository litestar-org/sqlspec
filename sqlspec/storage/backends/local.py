"""Local file system storage backend.

A simple, zero-dependency implementation for local file operations.
No external dependencies like fsspec or obstore required.
"""

import asyncio
import contextlib
import errno
import os
import shutil
from collections.abc import AsyncIterator, Iterator
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, ClassVar, Final, Literal, cast, overload

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import FileNotFoundInStorageError, StorageCapabilityError, StoragePathTraversalError
from sqlspec.storage._arrow_payload import (
    StorageFormat,
    encode_arrow_to_async_sink,
    encode_arrow_to_sink,
    join_shielded,
    validate_arrow_write_options,
)
from sqlspec.storage._arrow_stream import iter_parquet_row_groups, validate_parquet_stream_options
from sqlspec.storage._paths import ensure_path_within_root, parse_file_uri_path, reject_parent_traversal
from sqlspec.storage._utils import import_pyarrow, import_pyarrow_parquet, log_storage_event
from sqlspec.storage.backends.base import AsyncArrowBatchIterator, AsyncThreadedBytesIterator
from sqlspec.storage.errors import execute_async_storage_operation, execute_sync_storage_operation, raise_storage_error
from sqlspec.utils.sync_tools import async_
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.protocols import AsyncWritableBytesSink, WritableBytesSink
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

_LOCAL_ARROW_FORMATS: Final[frozenset[str]] = frozenset({"parquet", "arrow-ipc", "csv"})
_STAGE_PREFIX: Final[str] = ".sqlspec-stage-"
_NO_HARD_LINK_ERRNOS: Final[frozenset[int]] = frozenset({
    errno.EPERM,
    getattr(errno, "ENOTSUP", errno.EPERM),
    errno.ENOSYS,
})

__all__ = ("LocalStore",)


@mypyc_attr(allow_interpreted_subclasses=True)
class LocalStore:
    """Simple local file system storage backend.

    Provides file system operations without requiring fsspec or obstore.
    Supports file:// URIs and regular file paths.

    All synchronous methods use the *_sync suffix for consistency with async methods.
    """

    __slots__ = ("base_path", "protocol")

    backend_type: ClassVar[str] = "local"

    def __init__(self, uri: str = "", **kwargs: Any) -> None:
        """Initialize local storage backend.

        Args:
            uri: File URI or path
            **kwargs: Additional options including:
            - base_path: Subdirectory relative to URI path. If relative, it's combined
                with the URI path. If absolute, it takes precedence (backward compatible).

            The URI may be a file:// path (Windows style like file:///C:/path is supported).
            When both URI and base_path are provided, they are combined:
                - file:///home/user/storage + base_path="subdir" -> /home/user/storage/subdir
                - file:///home/user/storage + base_path="/other" -> /other (absolute takes precedence)
        """
        if uri.startswith("file://"):
            path = parse_file_uri_path(uri)
            self.base_path = Path(path).resolve() if path else Path.cwd().resolve()
        elif uri:
            self.base_path = Path(uri).resolve()
        else:
            self.base_path = Path.cwd().resolve()

        if "base_path" in kwargs:
            self.base_path = (self.base_path / kwargs["base_path"]).resolve()

        if not self.base_path.exists():
            self.base_path.mkdir(parents=True, exist_ok=True)
        elif self.base_path.is_file():
            self.base_path = self.base_path.parent.resolve()

        self.protocol = "file"

    @property
    def supports_signing(self) -> bool:
        """Whether this backend supports URL signing.

        Local file storage does not support URL signing.
        Local files are accessed directly via file:// URIs.

        Returns:
            Always False for local storage.
        """
        return False

    def supports_direct_arrow_write(self, file_format: StorageFormat, *, overwrite: bool = True) -> bool:
        """Return whether direct streaming Arrow writes are supported for the format and overwrite mode.

        Args:
            file_format: Target storage format.
            overwrite: Whether existing files may be replaced.

        Returns:
            True for parquet, arrow-ipc, and csv regardless of overwrite mode.
        """
        _ = overwrite
        return file_format in _LOCAL_ARROW_FORMATS

    def open_write_stream_sync(
        self, path: "str | Path", *, overwrite: bool = True, **kwargs: Any
    ) -> "AbstractContextManager[WritableBytesSink]":
        """Open a staged synchronous write stream that publishes atomically on clean exit.

        Args:
            path: Destination path relative to ``base_path``.
            overwrite: Replace an existing destination when True, or enforce exclusive create when False.
            **kwargs: Unsupported extra keyword arguments (rejected with ``TypeError``).

        Returns:
            Context manager yielding a writable byte sink.
        """
        _reject_unsupported_local_write_kwargs(kwargs)
        destination = self._resolve_path(path)
        return _LocalWriteStream(
            destination,
            overwrite=overwrite,
            backend_type=self.backend_type,
            log_fields={"protocol": self.protocol, "operation": "write_stream", "mode": "sync"},
        )

    def open_write_stream_async(
        self, path: "str | Path", *, overwrite: bool = True, **kwargs: Any
    ) -> "AbstractAsyncContextManager[AsyncWritableBytesSink]":
        """Open a staged asynchronous write stream that publishes atomically under shielded completion.

        Args:
            path: Destination path relative to ``base_path``.
            overwrite: Replace an existing destination when True, or enforce exclusive create when False.
            **kwargs: Unsupported extra keyword arguments (rejected with ``TypeError``).

        Returns:
            Async context manager yielding an async writable byte sink.
        """
        _reject_unsupported_local_write_kwargs(kwargs)
        destination = self._resolve_path(path)
        return _AsyncLocalWriteStream(
            destination,
            overwrite=overwrite,
            backend_type=self.backend_type,
            log_fields={"protocol": self.protocol, "operation": "write_stream", "mode": "async"},
        )

    def resolve_uri(self, path: "str | Path") -> str:
        """Resolve a backend-relative path to an absolute filesystem path.

        Args:
            path: The same backend-relative path accepted by read and write methods.

        Returns:
            The absolute filesystem path. The target does not need to exist.
        """
        return str(self._resolve_path(path).resolve())

    def read_bytes_sync(self, path: "str | Path", **kwargs: Any) -> bytes:
        """Read bytes from file synchronously."""
        resolved = self._resolve_path(path)
        try:
            return execute_sync_storage_operation(
                resolved.read_bytes, backend=self.backend_type, operation="read_bytes", path=str(resolved)
            )
        except FileNotFoundInStorageError as error:
            raise FileNotFoundError(str(resolved)) from error

    def write_bytes_sync(self, path: "str | Path", data: bytes, *, overwrite: bool = True, **kwargs: Any) -> None:
        """Write bytes to file synchronously."""
        _reject_unsupported_local_write_kwargs(kwargs)
        resolved = self._resolve_path(path)

        execute_sync_storage_operation(
            partial(_write_local_bytes, resolved, data, overwrite),
            backend=self.backend_type,
            operation="write_bytes",
            path=str(resolved),
        )

    def read_text_sync(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from file synchronously."""
        data = self.read_bytes_sync(path, **kwargs)
        return data.decode(encoding)

    def write_text_sync(
        self, path: "str | Path", data: str, encoding: str = "utf-8", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write text to file synchronously."""
        self.write_bytes_sync(path, data.encode(encoding), overwrite=overwrite, **kwargs)

    def stream_read_sync(self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any) -> Iterator[bytes]:
        """Stream bytes from file synchronously.

        Yields:
            Chunks of bytes from the file, with size determined by chunk_size (default: 65536 bytes).
        """
        resolved = self._resolve_path(path)
        chunk_size = chunk_size or 65536
        try:
            with resolved.open("rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk
        except FileNotFoundError as error:
            raise FileNotFoundError(str(resolved)) from error

    def list_objects_sync(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
        """List objects in directory synchronously.

        Args:
            prefix: Optional prefix that may look like a directory or filename filter.
            recursive: Whether to walk subdirectories.
            **kwargs: Additional backend-specific options (currently unused).

            When the prefix resembles a directory (contains a slash or ends with '/'), we treat it as a path; otherwise we filter filenames within the base path.
        """
        rel_prefix = prefix
        if prefix:
            reject_parent_traversal(prefix)
            if prefix.endswith("/") or "/" in prefix:
                search_path = self._resolve_path(prefix)
                if not search_path.exists():
                    return []
                if search_path.is_file():
                    return [str(search_path.relative_to(self.base_path))]
                rel_path = search_path.relative_to(self.base_path)
                rel_prefix = "" if str(rel_path) == "." else str(rel_path)
            else:
                search_path = self.base_path
        else:
            search_path = self.base_path

        pattern = "**/*" if recursive else "*"
        files = []
        for path in search_path.glob(pattern):
            if path.is_file():
                try:
                    resolved_file = path.resolve()
                    resolved_file.relative_to(self.base_path)
                    relative = path.relative_to(self.base_path)
                except (ValueError, OSError):
                    continue
                relative_str = str(relative)
                if not rel_prefix or relative_str.startswith(rel_prefix):
                    files.append(relative_str)

        return sorted(files)

    def exists_sync(self, path: "str | Path", **kwargs: Any) -> bool:
        """Check if file exists synchronously."""
        return self._resolve_path(path).exists()

    def delete_sync(self, path: "str | Path", **kwargs: Any) -> None:
        """Delete file or directory synchronously."""
        resolved = self._resolve_path(path)

        execute_sync_storage_operation(
            partial(_delete_local_path, resolved), backend=self.backend_type, operation="delete", path=str(resolved)
        )

    def copy_sync(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Copy file or directory synchronously."""
        src = self._resolve_path(source)
        dst = self._resolve_path(destination)
        dst.parent.mkdir(parents=True, exist_ok=True)

        execute_sync_storage_operation(
            partial(_copy_local_path, src, dst), backend=self.backend_type, operation="copy", path=f"{src}->{dst}"
        )

    def move_sync(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Move file or directory synchronously."""
        src = self._resolve_path(source)
        dst = self._resolve_path(destination)
        dst.parent.mkdir(parents=True, exist_ok=True)
        execute_sync_storage_operation(
            partial(shutil.move, str(src), str(dst)), backend=self.backend_type, operation="move", path=f"{src}->{dst}"
        )

    def glob_sync(self, pattern: str, **kwargs: Any) -> "list[str]":
        """Find files matching pattern synchronously.

        Supports both relative and absolute patterns inside ``base_path``.
        """
        if not pattern:
            return []
        reject_parent_traversal(pattern)
        pattern_path = Path(pattern)
        if pattern_path.is_absolute():
            try:
                rel_pattern_path = pattern_path.relative_to(self.base_path)
            except ValueError as exc:
                msg = f"Glob pattern resolves outside base_path '{self.base_path}': {pattern!r}"
                raise StoragePathTraversalError(msg) from exc
            rel_pattern = str(rel_pattern_path)
        else:
            rel_pattern = pattern

        matches = (
            self.base_path.rglob(rel_pattern)
            if "**" in rel_pattern and not rel_pattern.startswith("**/")
            else self.base_path.glob(rel_pattern)
        )

        results = []
        for match in matches:
            if match.is_file():
                try:
                    match.resolve().relative_to(self.base_path)
                    relative = match.relative_to(self.base_path)
                except (ValueError, OSError):
                    continue
                results.append(str(relative))

        return sorted(results)

    def get_metadata_sync(self, path: "str | Path", **kwargs: Any) -> "dict[str, object]":
        """Get file metadata synchronously."""
        resolved = self._resolve_path(path)
        return execute_sync_storage_operation(
            partial(self._collect_metadata, resolved),
            backend=self.backend_type,
            operation="get_metadata",
            path=str(resolved),
        )

    def is_object_sync(self, path: "str | Path") -> bool:
        """Check if path points to a file synchronously."""
        return self._resolve_path(path).is_file()

    def is_path_sync(self, path: "str | Path") -> bool:
        """Check if path points to a directory synchronously."""
        return self._resolve_path(path).is_dir()

    def read_arrow_sync(self, path: "str | Path", **kwargs: Any) -> "ArrowTable":
        """Read Arrow table from file synchronously."""
        pq = import_pyarrow_parquet()
        resolved = self._resolve_path(path)
        return cast(
            "ArrowTable",
            execute_sync_storage_operation(
                partial(pq.read_table, str(resolved), **kwargs),
                backend=self.backend_type,
                operation="read_arrow",
                path=str(resolved),
            ),
        )

    def write_arrow_sync(
        self, path: "str | Path", table: "ArrowTable", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write Arrow table to file synchronously through an atomic stage file."""
        format_hint, compression, merged_write_options = _prepare_local_arrow_write(kwargs)
        resolved = self._resolve_path(path)

        def _do_write() -> None:
            with _LocalWriteStream(
                resolved, overwrite=overwrite, backend_type=self.backend_type, log_fields=None
            ) as sink:
                encode_arrow_to_sink(
                    table, sink, format_hint=format_hint, compression=compression, write_options=merged_write_options
                )

        execute_sync_storage_operation(
            _do_write, backend=self.backend_type, operation="write_arrow", path=str(resolved)
        )
        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_arrow",
            mode="sync",
            path=str(resolved),
        )

    def stream_arrow_sync(
        self, pattern: str, *, file_format: Literal["parquet"] = "parquet", batch_size: int = 65_536, **kwargs: Any
    ) -> Iterator["ArrowRecordBatch"]:
        """Stream Arrow record batches from files matching pattern synchronously.

        Yields:
            Arrow record batches from matching files.
        """
        validate_parquet_stream_options(pattern, file_format, batch_size)
        pq = import_pyarrow_parquet()
        arrow_schema = kwargs.pop("arrow_schema", None)
        kwargs.pop("read_buffer_bytes", None)
        files = self.glob_sync(pattern)
        for file_path in files:
            resolved = self._resolve_path(file_path)
            resolved_str = str(resolved)
            parquet_file = execute_sync_storage_operation(
                partial(pq.ParquetFile, resolved_str),
                backend=self.backend_type,
                operation="stream_arrow",
                path=resolved_str,
            )
            yield from iter_parquet_row_groups(parquet_file, batch_size=batch_size, arrow_schema=arrow_schema, **kwargs)

    @overload
    def sign_sync(self, paths: str, expires_in: int = 3600, for_upload: bool = False) -> str: ...
    @overload
    def sign_sync(self, paths: "list[str]", expires_in: int = 3600, for_upload: bool = False) -> "list[str]": ...
    def sign_sync(
        self, paths: "str | list[str]", expires_in: int = 3600, for_upload: bool = False
    ) -> "str | list[str]":
        """Generate signed URL(s).

        Raises:
            NotImplementedError: Local file storage does not require URL signing.
            Local files are accessed directly via file:// URIs.
        """
        msg = "URL signing is not applicable to local file storage. Use file:// URIs directly."
        raise NotImplementedError(msg)

    async def read_bytes_async(self, path: "str | Path", **kwargs: Any) -> bytes:
        """Read bytes from file asynchronously."""
        return await async_(self.read_bytes_sync)(path, **kwargs)

    async def write_bytes_async(
        self, path: "str | Path", data: bytes, *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write bytes to file asynchronously."""
        await async_(self.write_bytes_sync)(path, data, overwrite=overwrite, **kwargs)

    async def read_text_async(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from file asynchronously."""
        return await async_(self.read_text_sync)(path, encoding, **kwargs)

    async def write_text_async(
        self, path: "str | Path", data: str, encoding: str = "utf-8", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write text to file asynchronously."""
        await async_(self.write_text_sync)(path, data, encoding, overwrite=overwrite, **kwargs)

    async def stream_read_async(
        self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        """Stream bytes from file asynchronously.

        Uses AsyncThreadedBytesIterator to offload blocking file I/O to a thread pool,
        ensuring the event loop is not blocked during read operations.

        Args:
            path: Path to the file to read.
            chunk_size: Size of chunks to yield (default: 65536 bytes).
            **kwargs: Additional arguments (unused).

        Returns:
            AsyncIterator yielding chunks of bytes.
        """
        resolved = self._resolve_path(path)
        chunk_size = chunk_size or 65536
        file_obj = await execute_async_storage_operation(
            partial(async_(_open_file_for_read), resolved),
            backend=self.backend_type,
            operation="stream_read",
            path=str(resolved),
        )
        return AsyncThreadedBytesIterator(file_obj, chunk_size)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
        """List objects asynchronously."""
        return await async_(self.list_objects_sync)(prefix, recursive, **kwargs)

    async def exists_async(self, path: "str | Path", **kwargs: Any) -> bool:
        """Check if file exists asynchronously."""
        return await async_(self.exists_sync)(path, **kwargs)

    async def delete_async(self, path: "str | Path", **kwargs: Any) -> None:
        """Delete file asynchronously."""
        await async_(self.delete_sync)(path, **kwargs)

    async def copy_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Copy file asynchronously."""
        await async_(self.copy_sync)(source, destination, **kwargs)

    async def move_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Move file asynchronously."""
        await async_(self.move_sync)(source, destination, **kwargs)

    async def get_metadata_async(self, path: "str | Path", **kwargs: Any) -> "dict[str, object]":
        """Get file metadata asynchronously."""
        return await async_(self.get_metadata_sync)(path, **kwargs)

    async def read_arrow_async(self, path: "str | Path", **kwargs: Any) -> "ArrowTable":
        """Read Arrow table asynchronously.

        Uses async_() to offload blocking PyArrow I/O to thread pool.
        """
        return await async_(self.read_arrow_sync)(path, **kwargs)

    async def write_arrow_async(
        self, path: "str | Path", table: "ArrowTable", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write Arrow table asynchronously through an atomic stage file."""
        format_hint, compression, merged_write_options = _prepare_local_arrow_write(kwargs)
        resolved = self._resolve_path(path)

        async def _do_write_async() -> None:
            async with _AsyncLocalWriteStream(
                resolved, overwrite=overwrite, backend_type=self.backend_type, log_fields=None
            ) as sink:
                await encode_arrow_to_async_sink(
                    table, sink, format_hint=format_hint, compression=compression, write_options=merged_write_options
                )

        await execute_async_storage_operation(
            _do_write_async, backend=self.backend_type, operation="write_arrow", path=str(resolved)
        )
        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_arrow",
            mode="async",
            path=str(resolved),
        )

    def stream_arrow_async(
        self, pattern: str, *, file_format: Literal["parquet"] = "parquet", batch_size: int = 65_536, **kwargs: Any
    ) -> AsyncIterator["ArrowRecordBatch"]:
        """Stream Arrow record batches asynchronously.

        Args:
            pattern: Glob pattern to match files.
            file_format: Storage format. Only Parquet supports bounded batch streaming.
            batch_size: Maximum number of rows in each yielded record batch.
            **kwargs: Additional arguments passed to stream_arrow_sync().

        Returns:
            AsyncIterator yielding Arrow record batches.
        """
        return AsyncArrowBatchIterator(
            self.stream_arrow_sync(pattern, file_format=file_format, batch_size=batch_size, **kwargs)
        )

    @overload
    async def sign_async(self, paths: str, expires_in: int = 3600, for_upload: bool = False) -> str: ...
    @overload
    async def sign_async(self, paths: "list[str]", expires_in: int = 3600, for_upload: bool = False) -> "list[str]": ...
    async def sign_async(
        self, paths: "str | list[str]", expires_in: int = 3600, for_upload: bool = False
    ) -> "str | list[str]":
        """Generate signed URL(s) asynchronously."""
        return await async_(self.sign_sync)(paths, expires_in, for_upload)  # type: ignore[arg-type]

    def _resolve_path(self, path: "str | Path") -> Path:
        """Resolve path relative to base_path, refusing anything outside it.

        Args:
            path: Path to resolve (absolute or relative).

        Returns:
            Resolved Path object inside ``base_path``.

        Raises:
            StoragePathTraversalError: If the path resolves outside ``base_path``.
        """
        relative = ensure_path_within_root(path, self.base_path)
        return self.base_path if not relative else self.base_path / relative

    def _collect_metadata(self, resolved: "Path") -> "dict[str, object]":
        if not resolved.exists():
            return {}

        stat = resolved.stat()
        return {
            "size": stat.st_size,
            "modified": stat.st_mtime,
            "created": stat.st_ctime,
            "is_file": resolved.is_file(),
            "is_dir": resolved.is_dir(),
            "path": str(resolved),
        }


def _reject_unsupported_local_write_kwargs(kwargs: "dict[str, Any]") -> None:
    """Raise TypeError when unsupported keyword arguments are passed to LocalStore writes."""
    if kwargs:
        names = ", ".join(sorted(kwargs))
        msg = f"Unsupported write keyword argument(s) for LocalStore: {names}"
        raise TypeError(msg)


def _write_local_bytes(resolved: "Path", data: bytes, overwrite: bool) -> None:
    """Write bytes to a local file, ensuring parent directories exist."""
    resolved.parent.mkdir(parents=True, exist_ok=True)
    if overwrite:
        resolved.write_bytes(data)
        return
    with resolved.open("xb") as file_obj:
        file_obj.write(data)


def _delete_local_path(resolved: "Path") -> None:
    """Delete a local file or directory."""
    if resolved.is_dir():
        shutil.rmtree(resolved)
    elif resolved.exists():
        resolved.unlink()


def _copy_local_path(src: "Path", dst: "Path") -> None:
    """Copy a local file or directory."""
    if src.is_dir():
        shutil.copytree(src, dst, dirs_exist_ok=True)
    else:
        shutil.copy2(src, dst)


def _prepare_local_arrow_write(kwargs: "dict[str, Any]") -> "tuple[StorageFormat, str | None, dict[str, Any]]":
    """Validate and extract format_hint, compression, and writer options for LocalStore Arrow writes."""
    import_pyarrow()
    format_hint: StorageFormat = kwargs.pop("format_hint", "parquet")
    if format_hint == "parquet":
        import_pyarrow_parquet()
    if format_hint not in _LOCAL_ARROW_FORMATS:
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


def _open_file_for_read(path: Path) -> Any:
    """Open a file for binary reading."""
    return path.open("rb")


def _open_local_stage(destination: "Path") -> "tuple[Path, BinaryIO]":
    """Create the parent directory and exclusively open a hidden sibling stage file with mode 0o666."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = destination.with_name(f"{_STAGE_PREFIX}{uuid4().hex}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    fd = os.open(stage, flags, 0o666)
    try:
        return stage, os.fdopen(fd, "wb")
    except BaseException:
        with contextlib.suppress(OSError):
            os.close(fd)
        with contextlib.suppress(OSError):
            stage.unlink(missing_ok=True)
        raise


def _publish_local_stage(stage: "Path", destination: "Path", *, overwrite: bool) -> None:
    """Atomically publish a stage file, always removing the stage."""
    try:
        if overwrite:
            if destination.exists():
                shutil.copymode(destination, stage)
            Path(stage).replace(destination)
            return

        try:
            os.link(stage, destination)
        except OSError as exc:
            if exc.errno not in _NO_HARD_LINK_ERRNOS:
                raise
            with stage.open("rb") as src_file, destination.open("xb") as dst_file:
                try:
                    shutil.copyfileobj(src_file, dst_file)
                except BaseException:
                    with contextlib.suppress(OSError):
                        dst_file.close()
                    with contextlib.suppress(OSError):
                        destination.unlink(missing_ok=True)
                    raise
    finally:
        with contextlib.suppress(OSError):
            stage.unlink(missing_ok=True)


def _close_and_publish_local_stage(
    stage: "Path", file_obj: "BinaryIO", destination: "Path", *, overwrite: bool
) -> None:
    """Close a stage file and atomically publish it to ``destination``."""
    try:
        file_obj.close()
    except BaseException:
        with contextlib.suppress(OSError):
            stage.unlink(missing_ok=True)
        raise
    _publish_local_stage(stage, destination, overwrite=overwrite)


def _abort_local_stage(stage: "Path", file_obj: "BinaryIO") -> None:
    """Close and unlink a stage file, suppressing OS cleanup errors."""
    with contextlib.suppress(OSError):
        file_obj.close()
    with contextlib.suppress(OSError):
        stage.unlink(missing_ok=True)


async def _abort_local_stage_async(stage: "Path", file_obj: "BinaryIO") -> None:
    """Abort an opened local stage file under cancellation shield."""
    try:
        await join_shielded(asyncio.ensure_future(async_(_abort_local_stage)(stage, file_obj)), suppress_result=True)
    except asyncio.CancelledError:
        return
    except Exception:
        return


@mypyc_attr(allow_interpreted_subclasses=False)
class _LocalFileSink:
    """Synchronous byte sink backed by a staged local file."""

    __slots__ = ("_backend", "_file", "_path", "closed")

    def __init__(self, file_obj: Any, *, backend: str = "local", path: str | None = None) -> None:
        self._file = file_obj
        self._backend = backend
        self._path = path
        self.closed = False

    def write(self, data: Any, /) -> int:
        if self.closed:
            return 0
        if not data:
            return 0
        try:
            return int(self._file.write(data))
        except Exception as error:
            raise_storage_error(error, backend=self._backend, operation="write_stream", path=self._path)

    def flush(self) -> None:
        if self.closed:
            return
        try:
            self._file.flush()
        except Exception as error:
            raise_storage_error(error, backend=self._backend, operation="write_stream", path=self._path)

    def close(self) -> None:
        self.closed = True


@mypyc_attr(allow_interpreted_subclasses=False)
class _AsyncLocalFileSink:
    """Asynchronous byte sink backed by a staged local file via ``async_``."""

    __slots__ = ("_backend", "_file", "_path", "closed")

    def __init__(self, file_obj: Any, *, backend: str = "local", path: str | None = None) -> None:
        self._file = file_obj
        self._backend = backend
        self._path = path
        self.closed = False

    async def write(self, data: Any, /) -> int:
        if self.closed:
            return 0
        payload = bytes(data) if isinstance(data, (bytearray, memoryview)) else data
        if not payload:
            return 0
        try:
            return int(await async_(self._file.write)(payload))
        except Exception as error:
            raise_storage_error(error, backend=self._backend, operation="write_stream", path=self._path)

    async def flush(self) -> None:
        if self.closed:
            return
        try:
            await async_(self._file.flush)()
        except Exception as error:
            raise_storage_error(error, backend=self._backend, operation="write_stream", path=self._path)

    async def close(self) -> None:
        self.closed = True


@mypyc_attr(allow_interpreted_subclasses=False)
class _LocalWriteStream:
    """Synchronous staged local write stream context manager."""

    __slots__ = ("_backend_type", "_destination", "_file", "_log_fields", "_overwrite", "_sink", "_stage")

    def __init__(
        self, destination: "Path", *, overwrite: bool, backend_type: str, log_fields: "dict[str, Any] | None"
    ) -> None:
        self._destination = destination
        self._overwrite = overwrite
        self._backend_type = backend_type
        self._log_fields = log_fields
        self._stage: Path | None = None
        self._file: BinaryIO | None = None
        self._sink: _LocalFileSink | None = None

    def __enter__(self) -> "_LocalFileSink":
        stage, file_obj = execute_sync_storage_operation(
            lambda: _open_local_stage(self._destination),
            backend=self._backend_type,
            operation="open_write_stream",
            path=str(self._destination),
        )
        self._stage = stage
        self._file = file_obj
        sink = _LocalFileSink(file_obj, backend=self._backend_type, path=str(self._destination))
        self._sink = sink
        return sink

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        _ = (exc, traceback)
        sink = self._sink
        if sink is not None:
            sink.closed = True
        stage = self._stage
        file_obj = self._file
        if stage is None or file_obj is None:
            return
        if exc_type is None:
            execute_sync_storage_operation(
                partial(_close_and_publish_local_stage, stage, file_obj, self._destination, overwrite=self._overwrite),
                backend=self._backend_type,
                operation="write_stream_close",
                path=str(self._destination),
            )
            if self._log_fields is not None:
                log_storage_event(
                    "storage.write", backend_type=self._backend_type, path=str(self._destination), **self._log_fields
                )
        else:
            _abort_local_stage(stage, file_obj)


@mypyc_attr(allow_interpreted_subclasses=False)
class _AsyncLocalWriteStream:
    """Asynchronous staged local write stream context manager with shielded completion."""

    __slots__ = ("_backend_type", "_destination", "_file", "_log_fields", "_overwrite", "_sink", "_stage")

    def __init__(
        self, destination: "Path", *, overwrite: bool, backend_type: str, log_fields: "dict[str, Any] | None"
    ) -> None:
        self._destination = destination
        self._overwrite = overwrite
        self._backend_type = backend_type
        self._log_fields = log_fields
        self._stage: Path | None = None
        self._file: BinaryIO | None = None
        self._sink: _AsyncLocalFileSink | None = None

    async def __aenter__(self) -> "_AsyncLocalFileSink":
        open_fut = asyncio.ensure_future(async_(lambda: _open_local_stage(self._destination))())
        opened: tuple[Path, BinaryIO] | None = None
        cancel_exc: asyncio.CancelledError | None = None
        try:
            opened = await execute_async_storage_operation(
                partial(join_shielded, open_fut),
                backend=self._backend_type,
                operation="open_write_stream",
                path=str(self._destination),
            )
        except asyncio.CancelledError as caught_cancel:
            cancel_exc = caught_cancel
        if cancel_exc is not None:
            if open_fut.done() and not open_fut.cancelled() and open_fut.exception() is None:
                opened_stage, opened_file = open_fut.result()
                await _abort_local_stage_async(opened_stage, opened_file)
            raise cancel_exc
        if opened is None:
            msg = "Unreachable"
            raise RuntimeError(msg)
        stage, file_obj = opened
        self._stage = stage
        self._file = file_obj
        sink = _AsyncLocalFileSink(file_obj, backend=self._backend_type, path=str(self._destination))
        self._sink = sink
        return sink

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        _ = (exc, traceback)
        sink = self._sink
        if sink is not None:
            sink.closed = True
        stage = self._stage
        file_obj = self._file
        if stage is None or file_obj is None:
            return
        if exc_type is None:
            publish_fut = asyncio.ensure_future(
                async_(
                    partial(
                        _close_and_publish_local_stage, stage, file_obj, self._destination, overwrite=self._overwrite
                    )
                )()
            )
            await execute_async_storage_operation(
                partial(join_shielded, publish_fut),
                backend=self._backend_type,
                operation="write_stream_close",
                path=str(self._destination),
            )
            if self._log_fields is not None:
                log_storage_event(
                    "storage.write", backend_type=self._backend_type, path=str(self._destination), **self._log_fields
                )
        else:
            abort_fut = asyncio.ensure_future(async_(_abort_local_stage)(stage, file_obj))
            await join_shielded(abort_fut, suppress_result=True)
