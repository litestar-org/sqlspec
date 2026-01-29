"""Local file system storage backend.

A simple, zero-dependency implementation for local file operations.
No external dependencies like fsspec or obstore required.
"""

import shutil
from collections.abc import AsyncIterator, Iterator
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast, overload
from urllib.parse import unquote, urlparse

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import FileNotFoundInStorageError
from sqlspec.storage._utils import import_pyarrow_parquet
from sqlspec.storage.backends.base import AsyncArrowBatchIterator, AsyncThreadedBytesIterator
from sqlspec.storage.errors import execute_sync_storage_operation
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("LocalStore",)


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


@mypyc_attr(allow_interpreted_subclasses=True)
class LocalStore:
    """Simple local file system storage backend.

    Provides file system operations without requiring fsspec or obstore.
    Supports file:// URIs and regular file paths.

    All synchronous methods use the *_sync suffix for consistency with async methods.
    """

    __slots__ = ("backend_type", "base_path", "protocol")

    def __init__(self, uri: str = "", **kwargs: Any) -> None:
        """Initialize local storage backend.

        Args:
            uri: File URI or path (e.g., "file:///path" or "/path")
            **kwargs: Additional options including:
                - base_path: Subdirectory relative to URI path. If relative, it's combined
                  with the URI path. If absolute, it takes precedence (backward compatible).

        The URI may be a file:// path (Windows style like file:///C:/path is supported).
        When both URI and base_path are provided, they are combined:
        - file:///home/user/storage + base_path="subdir" -> /home/user/storage/subdir
        - file:///home/user/storage + base_path="/other" -> /other (absolute takes precedence)
        """
        if uri.startswith("file://"):
            parsed = urlparse(uri)
            path = unquote(parsed.path)
            if path and len(path) > 2 and path[2] == ":":  # noqa: PLR2004
                path = path[1:]
            self.base_path = Path(path).resolve()
        elif uri:
            self.base_path = Path(uri).resolve()
        else:
            self.base_path = Path.cwd()

        if "base_path" in kwargs:
            # Combine URI path with base_path (Path division handles absolute paths correctly)
            # If base_path is absolute, it takes precedence (backward compatible)
            self.base_path = (self.base_path / kwargs["base_path"]).resolve()

        if not self.base_path.exists():
            self.base_path.mkdir(parents=True, exist_ok=True)
        elif self.base_path.is_file():
            self.base_path = self.base_path.parent

        self.protocol = "file"
        self.backend_type = "local"

    def _resolve_path(self, path: "str | Path") -> Path:
        """Resolve path relative to base_path.

        Args:
            path: Path to resolve (absolute or relative).

        Returns:
            Resolved Path object.
        """
        p = Path(path)
        return p if p.is_absolute() else self.base_path / p

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
        """Stream bytes from file synchronously."""
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
        Paths outside base_path are returned with their absolute names.
        """
        if prefix and (prefix.endswith("/") or "/" in prefix):
            search_path = self._resolve_path(prefix)
            if not search_path.exists():
                return []
            if search_path.is_file():
                return [str(search_path.relative_to(self.base_path))]
        else:
            search_path = self.base_path

        pattern = "**/*" if recursive else "*"
        files = []
        for path in search_path.glob(pattern):
            if path.is_file():
                try:
                    relative = path.relative_to(self.base_path)
                    relative_str = str(relative)
                    if not prefix or relative_str.startswith(prefix):
                        files.append(relative_str)
                except ValueError:
                    path_str = str(path)
                    if not prefix or path_str.startswith(prefix):
                        files.append(path_str)

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

        Supports both relative and absolute patterns by adjusting where the glob search begins.
        """
        if Path(pattern).is_absolute():
            base_path = Path(pattern).parent
            pattern_name = Path(pattern).name
            matches = base_path.rglob(pattern_name) if "**" in pattern else base_path.glob(pattern_name)
        else:
            matches = self.base_path.rglob(pattern) if "**" in pattern else self.base_path.glob(pattern)

        results = []
        for match in matches:
            if match.is_file():
                try:
                    relative = match.relative_to(self.base_path)
                    results.append(str(relative))
                except ValueError:
                    results.append(str(match))

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

    def stream_arrow_sync(self, pattern: str, **kwargs: Any) -> Iterator["ArrowRecordBatch"]:
        """Stream Arrow record batches from files matching pattern synchronously.

        Yields:
            Arrow record batches from matching files.
        """
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
            yield from parquet_file.iter_batches()  # pyright: ignore[reportUnknownMemberType]

    @property
    def supports_signing(self) -> bool:
        """Whether this backend supports URL signing.

        Local file storage does not support URL signing.
        Local files are accessed directly via file:// URIs.

        Returns:
            Always False for local storage.
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
        # Open file in thread pool to avoid blocking, then use threaded iterator
        file_obj = await async_(_open_file_for_read)(resolved)
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

    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator["ArrowRecordBatch"]:
        """Stream Arrow record batches asynchronously.

        Args:
            pattern: Glob pattern to match files.
            **kwargs: Additional arguments passed to stream_arrow_sync().

        Returns:
            AsyncIterator yielding Arrow record batches.
        """
        return AsyncArrowBatchIterator(self.stream_arrow_sync(pattern, **kwargs))

    @overload
    async def sign_async(self, paths: str, expires_in: int = 3600, for_upload: bool = False) -> str: ...

    @overload
    async def sign_async(self, paths: "list[str]", expires_in: int = 3600, for_upload: bool = False) -> "list[str]": ...

    async def sign_async(
        self, paths: "str | list[str]", expires_in: int = 3600, for_upload: bool = False
    ) -> "str | list[str]":
        """Generate signed URL(s) asynchronously."""
        return await async_(self.sign_sync)(paths, expires_in, for_upload)  # type: ignore[arg-type]
