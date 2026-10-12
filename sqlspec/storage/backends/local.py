"""Local file system storage backend.

A simple, zero-dependency implementation for local file operations.
No external dependencies like fsspec or obstore required.
"""

import shutil
from collections.abc import AsyncIterator, Iterator
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast, overload

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import FileNotFoundInStorageError, StoragePathTraversalError
from sqlspec.storage._arrow_stream import iter_parquet_row_groups, validate_parquet_stream_options
from sqlspec.storage._paths import ensure_path_within_root, parse_file_uri_path, reject_parent_traversal
from sqlspec.storage._utils import import_pyarrow_parquet
from sqlspec.storage.backends.base import AsyncArrowBatchIterator, AsyncThreadedBytesIterator
from sqlspec.storage.errors import execute_async_storage_operation, execute_sync_storage_operation
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

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
            self.base_path = Path(path).resolve()
        elif uri:
            self.base_path = Path(uri).resolve()
        else:
            self.base_path = Path.cwd()

        if "base_path" in kwargs:
            self.base_path = (self.base_path / kwargs["base_path"]).resolve()

        if not self.base_path.exists():
            self.base_path.mkdir(parents=True, exist_ok=True)
        elif self.base_path.is_file():
            self.base_path = self.base_path.parent

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

    def write_bytes_sync(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:
        """Write bytes to file synchronously."""
        resolved = self._resolve_path(path)

        execute_sync_storage_operation(
            partial(_write_local_bytes, resolved, data),
            backend=self.backend_type,
            operation="write_bytes",
            path=str(resolved),
        )

    def read_text_sync(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from file synchronously."""
        data = self.read_bytes_sync(path, **kwargs)
        return data.decode(encoding)

    def write_text_sync(self, path: "str | Path", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to file synchronously."""
        encoded = data.encode(encoding)
        self.write_bytes_sync(path, encoded, **kwargs)

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
        if prefix:
            reject_parent_traversal(prefix)
            if prefix.endswith("/") or "/" in prefix:
                search_path = self._resolve_path(prefix)
                if not search_path.exists():
                    return []
                if search_path.is_file():
                    return [str(search_path.relative_to(self.base_path))]
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
                except ValueError:
                    continue
                relative_str = str(relative)
                if not prefix or relative_str.startswith(prefix):
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
                except ValueError:
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

    def write_arrow_sync(self, path: "str | Path", table: "ArrowTable", **kwargs: Any) -> None:
        """Write Arrow table to file synchronously."""
        pq = import_pyarrow_parquet()
        resolved = self._resolve_path(path)

        execute_sync_storage_operation(
            partial(_write_local_arrow, resolved, table, pq, kwargs),
            backend=self.backend_type,
            operation="write_arrow",
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
            yield from iter_parquet_row_groups(parquet_file, batch_size=batch_size, **kwargs)

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

    async def write_bytes_async(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:
        """Write bytes to file asynchronously."""
        await async_(self.write_bytes_sync)(path, data, **kwargs)

    async def read_text_async(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from file asynchronously."""
        return await async_(self.read_text_sync)(path, encoding, **kwargs)

    async def write_text_async(self, path: "str | Path", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to file asynchronously."""
        await async_(self.write_text_sync)(path, data, encoding, **kwargs)

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

    async def write_arrow_async(self, path: "str | Path", table: "ArrowTable", **kwargs: Any) -> None:
        """Write Arrow table asynchronously.

        Uses async_() to offload blocking PyArrow I/O to thread pool.
        """
        await async_(self.write_arrow_sync)(path, table, **kwargs)

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


def _write_local_bytes(resolved: "Path", data: bytes) -> None:
    """Write bytes to a local file, ensuring parent directories exist."""
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_bytes(data)


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


def _write_local_arrow(resolved: "Path", table: "ArrowTable", pq: Any, options: "dict[str, Any]") -> None:
    """Write an Arrow table to a local path."""
    resolved.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(resolved), **options)  # pyright: ignore


def _open_file_for_read(path: Path) -> Any:
    """Open a file for binary reading."""
    return path.open("rb")
