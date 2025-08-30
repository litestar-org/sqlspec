"""Local file system storage backend.

A simple, zero-dependency implementation for local file operations.
No external dependencies like fsspec or obstore required.
"""

import shutil
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union
from urllib.parse import unquote, urlparse

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import PYARROW_INSTALLED
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    import asyncio

    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("LocalStore",)


class LocalStore:
    """Simple local file system storage backend.

    Provides file system operations without requiring fsspec or obstore.
    Supports file:// URIs and regular file paths.

    Implements ObjectStoreProtocol for type safety.
    """

    __slots__ = ("_loop", "backend_type", "base_path", "protocol")

    def __init__(self, uri: str = "", **kwargs: Any) -> None:
        """Initialize local storage backend.

        Args:
            uri: File URI or path (e.g., "file:///path" or "/path")
            **kwargs: Additional options (base_path for relative operations)
        """
        if uri.startswith("file://"):
            parsed = urlparse(uri)
            path = unquote(parsed.path)
            # Handle Windows paths (file:///C:/path)
            if path and len(path) > 2 and path[2] == ":":  # noqa: PLR2004
                path = path[1:]  # Remove leading slash for Windows
            self.base_path = Path(path).resolve()
        elif uri:
            self.base_path = Path(uri).resolve()
        else:
            self.base_path = Path.cwd()

        # Allow override with explicit base_path
        if "base_path" in kwargs:
            self.base_path = Path(kwargs["base_path"]).resolve()

        # Create base directory if it doesn't exist and it's actually a directory
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True, exist_ok=True)
        elif self.base_path.is_file():
            # If base_path points to a file, use its parent as the base directory
            self.base_path = self.base_path.parent
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        self.protocol = "file"
        self.backend_type = "local"

    def _ensure_pyarrow(self) -> None:
        """Ensure PyArrow is available for Arrow operations."""
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")

    def _resolve_path(self, path: "Union[str, Path]") -> Path:
        """Resolve path relative to base_path."""
        p = Path(path)
        if p.is_absolute():
            return p
        return self.base_path / p

    def read_bytes(self, path: "Union[str, Path]", **kwargs: Any) -> bytes:
        """Read bytes from file."""
        resolved = self._resolve_path(path)
        return resolved.read_bytes()

    def write_bytes(self, path: "Union[str, Path]", data: bytes, **kwargs: Any) -> None:
        """Write bytes to file."""
        resolved = self._resolve_path(path)
        resolved.parent.mkdir(parents=True, exist_ok=True)
        resolved.write_bytes(data)

    def read_text(self, path: "Union[str, Path]", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from file."""
        return self._resolve_path(path).read_text(encoding=encoding)

    def write_text(self, path: "Union[str, Path]", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to file."""
        resolved = self._resolve_path(path)
        resolved.parent.mkdir(parents=True, exist_ok=True)
        resolved.write_text(data, encoding=encoding)

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects in directory."""
        # If prefix looks like a directory path, treat as directory
        if prefix and (prefix.endswith("/") or "/" in prefix):
            search_path = self._resolve_path(prefix)
            if not search_path.exists():
                return []
            if search_path.is_file():
                return [str(search_path.relative_to(self.base_path))]
        else:
            # Treat as filename prefix filter
            search_path = self.base_path

        pattern = "**/*" if recursive else "*"
        files = []
        for path in search_path.glob(pattern):
            if path.is_file():
                try:
                    relative = path.relative_to(self.base_path)
                    relative_str = str(relative)
                    # Apply prefix filter if provided
                    if not prefix or relative_str.startswith(prefix):
                        files.append(relative_str)
                except ValueError:
                    # Path is outside base_path, use absolute
                    path_str = str(path)
                    if not prefix or path_str.startswith(prefix):
                        files.append(path_str)

        return sorted(files)

    def exists(self, path: "Union[str, Path]", **kwargs: Any) -> bool:
        """Check if file exists."""
        return self._resolve_path(path).exists()

    def delete(self, path: "Union[str, Path]", **kwargs: Any) -> None:
        """Delete file or directory."""
        resolved = self._resolve_path(path)
        if resolved.is_dir():
            shutil.rmtree(resolved)
        elif resolved.exists():
            resolved.unlink()

    def copy(self, source: "Union[str, Path]", destination: "Union[str, Path]", **kwargs: Any) -> None:
        """Copy file or directory."""
        src = self._resolve_path(source)
        dst = self._resolve_path(destination)
        dst.parent.mkdir(parents=True, exist_ok=True)

        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dst)

    def move(self, source: "Union[str, Path]", destination: "Union[str, Path]", **kwargs: Any) -> None:
        """Move file or directory."""
        src = self._resolve_path(source)
        dst = self._resolve_path(destination)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find files matching pattern."""
        # Handle both relative and absolute patterns
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

    def get_metadata(self, path: "Union[str, Path]", **kwargs: Any) -> dict[str, Any]:
        """Get file metadata."""
        resolved = self._resolve_path(path)
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

    def is_object(self, path: "Union[str, Path]") -> bool:
        """Check if path points to a file."""
        return self._resolve_path(path).is_file()

    def is_path(self, path: "Union[str, Path]") -> bool:
        """Check if path points to a directory."""
        return self._resolve_path(path).is_dir()

    def read_arrow(self, path: "Union[str, Path]", **kwargs: Any) -> "ArrowTable":
        """Read Arrow table from file."""
        self._ensure_pyarrow()
        import pyarrow.parquet as pq

        return pq.read_table(str(self._resolve_path(path)))

    def write_arrow(self, path: "Union[str, Path]", table: "ArrowTable", **kwargs: Any) -> None:
        """Write Arrow table to file."""
        self._ensure_pyarrow()
        import pyarrow.parquet as pq

        resolved = self._resolve_path(path)
        resolved.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(resolved))

    def stream_arrow(self, pattern: str, **kwargs: Any) -> Iterator["ArrowRecordBatch"]:
        """Stream Arrow record batches from files matching pattern.

        Yields:
            Arrow record batches from matching files.
        """
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")
        import pyarrow.parquet as pq

        files = self.glob(pattern)
        for file_path in files:
            resolved = self._resolve_path(file_path)
            parquet_file = pq.ParquetFile(str(resolved))
            yield from parquet_file.iter_batches()

    def sign(self, path: "Union[str, Path]", expires_in: int = 3600, for_upload: bool = False) -> str:
        """Generate a signed URL (returns file:// URI for local files)."""
        # For local files, just return a file:// URI
        # No actual signing needed for local files
        return self._resolve_path(path).as_uri()

    # Async methods using sync_tools.async_
    async def read_bytes_async(self, path: "Union[str, Path]", **kwargs: Any) -> bytes:
        """Read bytes from file asynchronously."""
        return await async_(self.read_bytes)(path, **kwargs)

    async def write_bytes_async(self, path: "Union[str, Path]", data: bytes, **kwargs: Any) -> None:
        """Write bytes to file asynchronously."""
        await async_(self.write_bytes)(path, data, **kwargs)

    async def read_text_async(self, path: "Union[str, Path]", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from file asynchronously."""
        return await async_(self.read_text)(path, encoding, **kwargs)

    async def write_text_async(
        self, path: "Union[str, Path]", data: str, encoding: str = "utf-8", **kwargs: Any
    ) -> None:
        """Write text to file asynchronously."""
        await async_(self.write_text)(path, data, encoding, **kwargs)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects asynchronously."""
        return await async_(self.list_objects)(prefix, recursive, **kwargs)

    async def exists_async(self, path: "Union[str, Path]", **kwargs: Any) -> bool:
        """Check if file exists asynchronously."""
        return await async_(self.exists)(path, **kwargs)

    async def delete_async(self, path: "Union[str, Path]", **kwargs: Any) -> None:
        """Delete file asynchronously."""
        await async_(self.delete)(path, **kwargs)

    async def copy_async(self, source: "Union[str, Path]", destination: "Union[str, Path]", **kwargs: Any) -> None:
        """Copy file asynchronously."""
        await async_(self.copy)(source, destination, **kwargs)

    async def move_async(self, source: "Union[str, Path]", destination: "Union[str, Path]", **kwargs: Any) -> None:
        """Move file asynchronously."""
        await async_(self.move)(source, destination, **kwargs)

    async def get_metadata_async(self, path: "Union[str, Path]", **kwargs: Any) -> dict[str, Any]:
        """Get file metadata asynchronously."""
        return await async_(self.get_metadata)(path, **kwargs)

    async def read_arrow_async(self, path: "Union[str, Path]", **kwargs: Any) -> "ArrowTable":
        """Read Arrow table asynchronously."""
        return await async_(self.read_arrow)(path, **kwargs)

    async def write_arrow_async(self, path: "Union[str, Path]", table: "ArrowTable", **kwargs: Any) -> None:
        """Write Arrow table asynchronously."""
        await async_(self.write_arrow)(path, table, **kwargs)

    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator["ArrowRecordBatch"]:
        """Stream Arrow record batches asynchronously."""

        # Convert sync iterator to async
        async def _stream() -> AsyncIterator["ArrowRecordBatch"]:
            for batch in self.stream_arrow(pattern, **kwargs):
                yield batch

        return _stream()

    async def sign_async(self, path: "Union[str, Path]", expires_in: int = 3600, for_upload: bool = False) -> str:
        """Generate a signed URL asynchronously (returns file:// URI for local files)."""
        return await async_(self.sign)(path, expires_in, for_upload)
