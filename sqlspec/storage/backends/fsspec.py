import logging
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError, wrap_exceptions
from sqlspec.storage.backends.base import InstrumentedObjectStore
from sqlspec.typing import FSSPEC_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from fsspec import AbstractFileSystem

    from sqlspec.config import InstrumentationConfig
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


class FSSpecBackend(InstrumentedObjectStore):
    """Extended protocol support via fsspec.

    This backend implements the ObjectStoreProtocol using fsspec,
    providing support for extended protocols not covered by obstore
    and offering fallback capabilities.
    """

    def __init__(
        self,
        fs: "Union[str, AbstractFileSystem]",
        base_path: str = "",
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
    ) -> None:
        if not FSSPEC_INSTALLED:
            raise MissingDependencyError(package="fsspec", install_package="fsspec")

        # Initialize the instrumented base class
        super().__init__(instrumentation_config, "FSSpec")

        import fsspec

        self.base_path = base_path.rstrip("/") if base_path else ""

        if isinstance(fs, str):
            self.fs = fsspec.filesystem(fs.split("://")[0])
            self.protocol = fs.split("://")[0]
            self._fs_uri = fs
        else:
            self.fs = fs
            self.protocol = "unknown"
            with wrap_exceptions(suppress=AttributeError):
                self.protocol = fs.protocol
            self._fs_uri = f"{self.protocol}://"

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
            return f"{self.base_path}/{path.lstrip('/')}"
        return path

    @property
    def backend_type(self) -> str:
        return "fsspec"

    @property
    def base_uri(self) -> str:
        return self._fs_uri

    # Core Operations (sync)
    def _read_bytes(self, path: str, **kwargs: Any) -> bytes:
        """Read bytes from an object."""
        try:
            resolved_path = self._resolve_path(path)
            return self.fs.cat(resolved_path)  # type: ignore
        except Exception as exc:
            msg = f"Failed to read bytes from {path}"
            raise StorageOperationFailedError(msg) from exc

    def _write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes to an object."""
        try:
            resolved_path = self._resolve_path(path)
            with self.fs.open(resolved_path, mode="wb") as f:
                f.write(data)
        except Exception as exc:
            msg = f"Failed to write bytes to {path}"
            raise StorageOperationFailedError(msg) from exc

    def _read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from an object."""
        try:
            data = self.read_bytes(path)
            return data.decode(encoding)
        except Exception as exc:
            msg = f"Failed to read text from {path}"
            raise StorageOperationFailedError(msg) from exc

    def _write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to an object."""
        try:
            self.write_bytes(path, data.encode(encoding))
        except Exception as exc:
            msg = f"Failed to write text to {path}"
            raise StorageOperationFailedError(msg) from exc

    # Object Operations
    def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if an object exists."""
        try:
            resolved_path = self._resolve_path(path)
            return self.fs.exists(resolved_path)
        except Exception as exc:
            msg = f"Failed to check existence of {path}"
            raise StorageOperationFailedError(msg) from exc

    def _delete(self, path: str, **kwargs: Any) -> None:
        """Delete an object."""
        try:
            resolved_path = self._resolve_path(path)
            self.fs.rm(resolved_path)
        except Exception as exc:
            msg = f"Failed to delete {path}"
            raise StorageOperationFailedError(msg) from exc

    def _copy(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy an object."""
        try:
            source_path = self._resolve_path(source)
            dest_path = self._resolve_path(destination)
            # fsspec has native copy support
            self.fs.copy(source_path, dest_path)
        except Exception as exc:
            msg = f"Failed to copy {source} to {destination}"
            raise StorageOperationFailedError(msg) from exc

    def _move(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move an object."""
        try:
            source_path = self._resolve_path(source)
            dest_path = self._resolve_path(destination)
            # fsspec has native move support
            self.fs.mv(source_path, dest_path)
        except Exception as exc:
            msg = f"Failed to move {source} to {destination}"
            raise StorageOperationFailedError(msg) from exc

    # Arrow Operations
    def _read_arrow(self, path: str, **kwargs: Any) -> "ArrowTable":
        """Read an Arrow table from storage."""
        try:
            import pyarrow.parquet as pq

            resolved_path = self._resolve_path(path)
            with self.fs.open(resolved_path, mode="rb") as f:
                return pq.read_table(f, **kwargs)
        except ImportError:
            msg = "pyarrow"
            raise MissingDependencyError(msg)
        except Exception as exc:
            msg = f"Failed to read Arrow table from {path}"
            raise StorageOperationFailedError(msg) from exc

    def _write_arrow(self, path: str, table: "ArrowTable", **kwargs: Any) -> None:
        """Write an Arrow table to storage."""
        try:
            import pyarrow.parquet as pq

            resolved_path = self._resolve_path(path)
            with self.fs.open(resolved_path, mode="wb") as f:
                pq.write_table(table, f, **kwargs)
        except ImportError:
            msg = "pyarrow"
            raise MissingDependencyError(msg)
        except Exception as exc:
            msg = f"Failed to write Arrow table to {path}"
            raise StorageOperationFailedError(msg) from exc

    # Listing Operations
    def _list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects with optional prefix."""
        try:
            resolved_prefix = self._resolve_path(prefix) if prefix else self.base_path

            # Use fs.find for listing files
            if recursive:
                pattern = f"{resolved_prefix}/**" if resolved_prefix else "**"
            else:
                pattern = f"{resolved_prefix}/*" if resolved_prefix else "*"

            # Get all files (not directories)
            objects = [path for path in self.fs.glob(pattern) if not self.fs.isdir(path)]

            return sorted(objects)  # pyright: ignore
        except Exception as exc:
            msg = f"Failed to list objects with prefix {prefix}"
            raise StorageOperationFailedError(msg) from exc

    def _glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching a glob pattern."""
        try:
            resolved_pattern = self._resolve_path(pattern)
            # Use fsspec's native glob
            objects = [path for path in self.fs.glob(resolved_pattern) if not self.fs.isdir(path)]
            return sorted(objects)  # pyright: ignore
        except Exception as exc:
            msg = f"Failed to glob pattern {pattern}"
            raise StorageOperationFailedError(msg) from exc

    # Path Operations
    def _is_object(self, path: str) -> bool:
        """Check if path points to an object."""
        try:
            resolved_path = self._resolve_path(path)
            return self.fs.exists(resolved_path) and not self.fs.isdir(resolved_path)
        except Exception as exc:
            msg = f"Failed to check if {path} is an object"
            raise StorageOperationFailedError(msg) from exc

    def _is_path(self, path: str) -> bool:
        """Check if path points to a prefix (directory-like)."""
        try:
            resolved_path = self._resolve_path(path)
            return self.fs.isdir(resolved_path)
        except Exception as exc:
            msg = f"Failed to check if {path} is a prefix"
            raise StorageOperationFailedError(msg) from exc

    def _get_metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get object metadata."""
        try:
            resolved_path = self._resolve_path(path)
            info = self.fs.info(resolved_path)
        except Exception as exc:
            msg = f"Failed to get metadata for {path}"
            raise StorageOperationFailedError(msg) from exc
        else:
            # Convert fsspec info to dict
            if isinstance(info, dict):
                return info

            # Try to get dict representation
            with wrap_exceptions(suppress=AttributeError):
                return vars(info)

            # Fallback to basic metadata with safe attribute access
            metadata = {
                "path": resolved_path,
                "exists": self.fs.exists(resolved_path),
            }

            with wrap_exceptions(suppress=AttributeError):
                metadata["size"] = info.size
            with wrap_exceptions(suppress=AttributeError):
                metadata["type"] = info.type

            # Set defaults if attributes weren't found
            metadata.setdefault("size", None)
            metadata.setdefault("type", "file")
            return metadata

    def _stream_file_batches(self, obj_path: str) -> "Iterator[ArrowRecordBatch]":
        try:
            import pyarrow.parquet as pq

            with self.fs.open(obj_path, mode="rb") as f:
                parquet_file = pq.ParquetFile(f)
                yield from parquet_file.iter_batches()
        except Exception as e:
            # Log but continue with other files
            logger.warning("Failed to read %s: %s", obj_path, e)
            return

    def _stream_arrow(self, pattern: str, **kwargs: Any) -> "Iterator[ArrowRecordBatch]":
        if not FSSPEC_INSTALLED:
            raise MissingDependencyError(package="fsspec", install_package="fsspec")
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")
        try:
            # Find all matching objects
            matching_objects = self.glob(pattern)

            # Stream each file as record batches
            for obj_path in matching_objects:
                yield from self._stream_file_batches(obj_path)

        except ImportError:
            msg = "pyarrow"
            raise MissingDependencyError(msg)
        except Exception as exc:
            msg = f"Failed to stream Arrow data for pattern {pattern}"
            raise StorageOperationFailedError(msg) from exc

    # FSSpec-specific async implementations for better performance
    # These are used by the base class when available

    async def _read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        """Async read bytes using native fsspec if available."""
        # Check if fsspec supports async natively
        with wrap_exceptions(suppress=(AttributeError, TypeError)):
            resolved_path = self._resolve_path(path)
            return await self.fs._cat(resolved_path)

        # Fall back to sync implementation
        return self._read_bytes(path, **kwargs)

    async def _write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Async write bytes using native fsspec if available."""
        # Check if fsspec supports async natively
        with wrap_exceptions(suppress=(AttributeError, TypeError)):
            resolved_path = self._resolve_path(path)
            await self.fs._pipe(resolved_path, data)
            return

        # Fall back to sync implementation
        self._write_bytes(path, data, **kwargs)

    async def _stream_file_batches_async(self, obj_path: str) -> "AsyncIterator[ArrowRecordBatch]":
        try:
            from io import BytesIO

            import pyarrow.parquet as pq

            data = await self.read_bytes_async(obj_path)
            parquet_file = pq.ParquetFile(BytesIO(data))

            for batch in parquet_file.iter_batches():
                yield batch
        except Exception as e:
            # Log but continue with other files
            logger.warning("Failed to read %s: %s", obj_path, e)
            return

    async def _stream_arrow_async(self, pattern: str, **kwargs: Any) -> "AsyncIterator[ArrowRecordBatch]":
        try:
            # Find all matching objects
            matching_objects = await self.list_objects_async()
            filtered_objects = [obj for obj in matching_objects if self._matches_pattern(obj, pattern)]

            # Stream each file as record batches
            for obj_path in filtered_objects:
                async for batch in self._stream_file_batches_async(obj_path):
                    yield batch

        except Exception as exc:
            msg = f"Failed to stream Arrow data for pattern {pattern}"
            raise StorageOperationFailedError(msg) from exc

    def _get_signed_url(
        self,
        path: str,
        operation: "Literal['read', 'write']" = "read",
        expires_in: int = 3600,
        **kwargs: Any,
    ) -> str:
        """Generate signed URL (not supported by FSSpec backend)."""
        msg = f"Signed URL generation not supported for {self.backend_type} backend"
        raise NotImplementedError(msg)

    def _matches_pattern(self, path: str, pattern: str) -> bool:
        """Check if path matches glob pattern."""
        import fnmatch

        resolved_pattern = self._resolve_path(pattern)
        return fnmatch.fnmatch(path, resolved_pattern)

    # Implement missing abstract async methods
    # Most wrap the sync implementations with async_() to avoid blocking event loop

    async def _read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Async read text - wraps sync implementation with async_()."""
        return await async_(self._read_text)(path, encoding, **kwargs)

    async def _write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Async write text - wraps sync implementation with async_()."""
        await async_(self._write_text)(path, data, encoding, **kwargs)

    async def _list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Async list objects - wraps sync implementation with async_()."""
        return await async_(self._list_objects)(prefix, recursive, **kwargs)

    async def _exists_async(self, path: str, **kwargs: Any) -> bool:
        """Async exists check - wraps sync implementation with async_()."""
        return await async_(self._exists)(path, **kwargs)

    async def _delete_async(self, path: str, **kwargs: Any) -> None:
        """Async delete - wraps sync implementation with async_()."""
        await async_(self._delete)(path, **kwargs)

    async def _copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async copy - wraps sync implementation with async_()."""
        await async_(self._copy)(source, destination, **kwargs)

    async def _move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async move - wraps sync implementation with async_()."""
        await async_(self._move)(source, destination, **kwargs)

    async def _get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Async get metadata - wraps sync implementation with async_()."""
        return await async_(self._get_metadata)(path, **kwargs)

    async def _read_arrow_async(self, path: str, **kwargs: Any) -> "ArrowTable":
        """Async read Arrow - wraps sync implementation with async_()."""
        return await async_(self._read_arrow)(path, **kwargs)

    async def _write_arrow_async(self, path: str, table: "ArrowTable", **kwargs: Any) -> None:
        """Async write Arrow - wraps sync implementation with async_()."""
        await async_(self._write_arrow)(path, table, **kwargs)
