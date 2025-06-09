"""High-performance object storage using obstore.

This backend implements the ObjectStoreProtocol using obstore,
providing native support for S3, GCS, Azure, and local file storage
with excellent performance characteristics and native Arrow support.
"""

from __future__ import annotations

import fnmatch
import logging
from typing import TYPE_CHECKING, Any, Literal

from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError
from sqlspec.storage.backends.base import InstrumentedObjectStore
from sqlspec.typing import OBSTORE_INSTALLED

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlspec.config import InstrumentationConfig
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("ObStoreBackend",)

logger = logging.getLogger(__name__)


class ObStoreBackend(InstrumentedObjectStore):
    """High-performance object storage backend using obstore.

    This backend leverages obstore's Rust-based implementation for maximum
    performance, providing native support for:
    - AWS S3 and S3-compatible stores
    - Google Cloud Storage
    - Azure Blob Storage
    - Local filesystem
    - HTTP endpoints

    Features native Arrow support and ~9x better performance than fsspec.
    """

    def __init__(
        self,
        store_uri: str,
        base_path: str = "",
        instrumentation_config: InstrumentationConfig | None = None,
        **store_options: Any,
    ) -> None:
        """Initialize obstore backend.

        Args:
            store_uri: Storage URI (e.g., 's3://bucket', 'file:///path', 'gs://bucket')
            base_path: Base path prefix for all operations
            instrumentation_config: Instrumentation configuration
            **store_options: Additional options for obstore configuration
        """
        super().__init__(instrumentation_config, "ObStore")

        if not OBSTORE_INSTALLED:
            raise MissingDependencyError(package="obstore", install_package="obstore")

        try:
            from obstore.store import from_url

            self.store_uri = store_uri
            self.base_path = base_path.rstrip("/") if base_path else ""
            self.store_options = store_options

            # Initialize obstore instance
            # Use obstore's from_url for automatic URI parsing
            self.store = from_url(store_uri, **store_options)

            if self.instrumentation_config.debug_mode:
                self.logger.debug(
                    "ObStore backend initialized",
                    extra={
                        "store_uri": store_uri,
                        "base_path": base_path,
                    },
                )

        except Exception as exc:
            msg = f"Failed to initialize obstore backend for {store_uri}"
            raise StorageOperationFailedError(msg) from exc

    def _resolve_path(self, path: str) -> str:
        """Resolve path relative to base_path."""
        if self.base_path:
            return f"{self.base_path}/{path.lstrip('/')}"
        return path

    @property
    def backend_type(self) -> str:
        """Return backend type identifier."""
        return "obstore"

    # Implementation of abstract methods from InstrumentedObjectStore

    def _read_bytes(self, path: str, **kwargs: Any) -> bytes:
        """Read bytes using obstore."""
        try:
            resolved_path = self._resolve_path(path)
            result = self.store.get(resolved_path)
            # GetResult provides .bytes() method for obstore
            return result.bytes()  # type: ignore[return-value]
        except Exception as exc:
            msg = f"Failed to read bytes from {path}"
            raise StorageOperationFailedError(msg) from exc

    def _write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes using obstore."""
        try:
            resolved_path = self._resolve_path(path)
            self.store.put(resolved_path, data)
        except Exception as exc:
            msg = f"Failed to write bytes to {path}"
            raise StorageOperationFailedError(msg) from exc

    def _read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text using obstore."""
        try:
            data = self._read_bytes(path, **kwargs)
            return data.decode(encoding)
        except Exception as exc:
            msg = f"Failed to read text from {path} with encoding {encoding}"
            raise StorageOperationFailedError(msg) from exc

    def _write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text using obstore."""
        try:
            encoded_data = data.encode(encoding)
            self._write_bytes(path, encoded_data, **kwargs)
        except Exception as exc:
            msg = f"Failed to write text to {path} with encoding {encoding}"
            raise StorageOperationFailedError(msg) from exc

    def _list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects using obstore."""
        try:
            resolved_prefix = self._resolve_path(prefix) if prefix else self.base_path or ""
            objects = []

            if not recursive:
                # Use delimiter-based listing for non-recursive (required by obstore)
                for item in self.store.list_with_delimiter(resolved_prefix):
                    # Try path first, then key, then string representation
                    if hasattr(item, "path"):
                        path = item.path
                    elif hasattr(item, "key"):
                        path = item.key
                    else:
                        path = str(item)
                    objects.append(path)
            else:
                # Standard recursive listing
                for item in self.store.list(resolved_prefix):
                    # Try path first, then key, then string representation
                    if hasattr(item, "path"):
                        path = item.path
                    elif hasattr(item, "key"):
                        path = item.key
                    else:
                        path = str(item)
                    objects.append(path)

            return sorted(objects)
        except Exception as exc:
            msg = f"Failed to list objects with prefix '{prefix}'"
            raise StorageOperationFailedError(msg) from exc

    def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if object exists using obstore."""
        try:
            resolved_path = self._resolve_path(path)
            # Use head() to check existence efficiently
            self.store.head(resolved_path)
        except Exception:
            # obstore raises exceptions for non-existent objects
            return False
        else:
            return True

    def _delete(self, path: str, **kwargs: Any) -> None:
        """Delete object using obstore."""
        try:
            resolved_path = self._resolve_path(path)
            self.store.delete(resolved_path)
        except Exception as exc:
            msg = f"Failed to delete {path}"
            raise StorageOperationFailedError(msg) from exc

    def _copy(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy object using obstore."""
        try:
            source_path = self._resolve_path(source)
            dest_path = self._resolve_path(destination)
            self.store.copy(source_path, dest_path)
        except Exception as exc:
            msg = f"Failed to copy {source} to {destination}"
            raise StorageOperationFailedError(msg) from exc

    def _move(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move object using obstore."""
        try:
            source_path = self._resolve_path(source)
            dest_path = self._resolve_path(destination)
            self.store.rename(source_path, dest_path)
        except Exception as exc:
            msg = f"Failed to move {source} to {destination}"
            raise StorageOperationFailedError(msg) from exc

    def _glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching pattern using obstore."""
        try:
            # List all objects and filter by pattern
            all_objects = self._list_objects(recursive=True, **kwargs)
            resolved_pattern = self._resolve_path(pattern)
            return [obj for obj in all_objects if fnmatch.fnmatch(obj, resolved_pattern)]
        except Exception as exc:
            msg = f"Failed to glob pattern {pattern}"
            raise StorageOperationFailedError(msg) from exc

    def _get_metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get object metadata using obstore."""
        try:
            resolved_path = self._resolve_path(path)
            metadata = self.store.head(resolved_path)
        except Exception as exc:
            msg = f"Failed to get metadata for {path}"
            raise StorageOperationFailedError(msg) from exc
        else:
            # Convert obstore ObjectMeta to dict
            result = {
                "path": resolved_path,
                "exists": True,
            }

            # Extract metadata attributes if available
            for attr in ["size", "last_modified", "e_tag", "version"]:
                if hasattr(metadata, attr):
                    result[attr] = getattr(metadata, attr)

            # Include custom metadata if available (check existence first)
            if hasattr(metadata, "metadata"):
                custom_metadata = getattr(metadata, "metadata", None)
                if custom_metadata:
                    result["custom_metadata"] = custom_metadata

            return result

    def _get_signed_url(
        self,
        path: str,
        operation: Literal["read", "write"] = "read",
        expires_in: int = 3600,
        **kwargs: Any,
    ) -> str:
        """Generate signed URL using obstore."""
        try:
            resolved_path = self._resolve_path(path)

            # Check if obstore supports signed URLs
            if hasattr(self.store, "sign_url"):
                return self.store.sign_url(resolved_path, operation, expires_in)  # type: ignore[union-attr]
            return self._raise_signed_url_not_supported()
        except Exception as exc:
            msg = f"Failed to generate signed URL for {path}"
            raise StorageOperationFailedError(msg) from exc

    def _raise_signed_url_not_supported(self) -> str:
        """Raise NotImplementedError for unsupported signed URL generation."""
        msg = f"Signed URL generation not supported for {self.backend_type} backend"
        raise NotImplementedError(msg)

    def _is_object(self, path: str) -> bool:
        """Check if path is an object using obstore."""
        try:
            resolved_path = self._resolve_path(path)
            # An object exists and doesn't end with /
            return self._exists(path) and not resolved_path.endswith("/")
        except Exception:
            return False

    def _is_path(self, path: str) -> bool:
        """Check if path is a prefix/directory using obstore."""
        try:
            resolved_path = self._resolve_path(path)

            # A path/prefix either ends with / or has objects under it
            if resolved_path.endswith("/"):
                return True

            # Check if there are any objects with this prefix
            objects = self._list_objects(prefix=path, recursive=False)
            return len(objects) > 0
        except Exception:
            return False

    def _read_arrow(self, path: str, **kwargs: Any) -> ArrowTable:
        """Read Arrow table using obstore."""
        try:
            resolved_path = self._resolve_path(path)
            return self.store.read_arrow(resolved_path, **kwargs)  # type: ignore[union-attr]
        except Exception as exc:
            msg = f"Failed to read Arrow table from {path}"
            raise StorageOperationFailedError(msg) from exc

    def _write_arrow(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table using obstore."""
        try:
            resolved_path = self._resolve_path(path)
            self.store.write_arrow(resolved_path, table, **kwargs)  # type: ignore[union-attr]
        except Exception as exc:
            msg = f"Failed to write Arrow table to {path}"
            raise StorageOperationFailedError(msg) from exc

    def _stream_arrow(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Stream Arrow record batches using obstore.

        Yields:
            Iterator of Arrow record batches from matching objects.
        """
        try:
            resolved_pattern = self._resolve_path(pattern)
            yield from self.store.stream_arrow(resolved_pattern, **kwargs)  # type: ignore[union-attr]
        except Exception as exc:
            msg = f"Failed to stream Arrow data for pattern {pattern}"
            raise StorageOperationFailedError(msg) from exc

    # Private async implementations for instrumentation support
    # These are called by the base class async methods after instrumentation

    async def _read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        """Private async read bytes using native obstore async if available."""
        try:
            resolved_path = self._resolve_path(path)
            result = await self.store.get_async(resolved_path)  # type: ignore[misc]
            return result.bytes()  # type: ignore[return-value]
        except Exception as exc:
            msg = f"Failed to async read bytes from {path}"
            raise StorageOperationFailedError(msg) from exc

    async def _write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Private async write bytes using native obstore async."""
        try:
            resolved_path = self._resolve_path(path)
            await self.store.put_async(resolved_path, data)
        except Exception as exc:
            msg = f"Failed to async write bytes to {path}"
            raise StorageOperationFailedError(msg) from exc

    async def _list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Private async list objects using native obstore async if available."""
        try:
            resolved_prefix = self._resolve_path(prefix) if prefix else self.base_path or ""
            objects = []

            async for item in self.store.list_async(resolved_prefix):  # type: ignore[union-attr]
                # Try path first, then key, then string representation
                if hasattr(item, "path"):
                    path = item.path
                elif hasattr(item, "key"):
                    path = item.key
                else:
                    path = str(item)
                objects.append(path)

            # Manual filtering for non-recursive if needed
            if not recursive and resolved_prefix:
                base_depth = resolved_prefix.count("/")
                objects = [obj for obj in objects if obj.count("/") <= base_depth + 1]

            return sorted(objects)
        except Exception as exc:
            msg = f"Failed to async list objects with prefix '{prefix}'"
            raise StorageOperationFailedError(msg) from exc

    # Implement all other required abstract async methods
    # ObStore provides native async for most operations

    async def _read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Async read text using native obstore async."""
        try:
            data = await self._read_bytes_async(path, **kwargs)
            # Handle obstore's Bytes object by converting to regular bytes first
            if hasattr(data, "to_bytes"):
                data = data.to_bytes()  # pyright: ignore
            elif hasattr(data, "__bytes__"):
                data = bytes(data)
            return data.decode(encoding)
        except Exception as exc:
            msg = f"Failed to async read text from {path} with encoding {encoding}"
            raise StorageOperationFailedError(msg) from exc

    async def _write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Async write text using native obstore async."""
        try:
            encoded_data = data.encode(encoding)
            await self._write_bytes_async(path, encoded_data, **kwargs)
        except Exception as exc:
            msg = f"Failed to async write text to {path} with encoding {encoding}"
            raise StorageOperationFailedError(msg) from exc

    async def _exists_async(self, path: str, **kwargs: Any) -> bool:
        """Async check if object exists using native obstore async."""
        try:
            resolved_path = self._resolve_path(path)
            await self.store.head_async(resolved_path)

        except Exception:
            return False
        return True

    async def _delete_async(self, path: str, **kwargs: Any) -> None:
        """Async delete object using native obstore async."""
        try:
            resolved_path = self._resolve_path(path)
            await self.store.delete_async(resolved_path)
        except Exception as exc:
            msg = f"Failed to async delete {path}"
            raise StorageOperationFailedError(msg) from exc

    async def _copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async copy object using native obstore async."""
        try:
            source_path = self._resolve_path(source)
            dest_path = self._resolve_path(destination)
            await self.store.copy_async(source_path, dest_path)
        except Exception as exc:
            msg = f"Failed to async copy {source} to {destination}"
            raise StorageOperationFailedError(msg) from exc

    async def _move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async move object using native obstore async."""
        try:
            source_path = self._resolve_path(source)
            dest_path = self._resolve_path(destination)
            await self.store.rename_async(source_path, dest_path)
        except Exception as exc:
            msg = f"Failed to async move {source} to {destination}"
            raise StorageOperationFailedError(msg) from exc

    async def _get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Async get object metadata using native obstore async."""
        try:
            resolved_path = self._resolve_path(path)
            metadata = await self.store.head_async(resolved_path)
            # Convert obstore ObjectMeta to dict
            result = {
                "path": resolved_path,
                "exists": True,
            }
            # Extract metadata attributes if available
            for attr in ["size", "last_modified", "e_tag", "version"]:
                if hasattr(metadata, attr):
                    result[attr] = getattr(metadata, attr)
            # Include custom metadata if available
            if hasattr(metadata, "metadata"):
                custom_metadata = getattr(metadata, "metadata", None)
                if custom_metadata:
                    result["custom_metadata"] = custom_metadata

        except Exception as exc:
            msg = f"Failed to async get metadata for {path}"
            raise StorageOperationFailedError(msg) from exc
        return result

    async def _read_arrow_async(self, path: str, **kwargs: Any) -> ArrowTable:
        """Async read Arrow table using native obstore async."""
        try:
            resolved_path = self._resolve_path(path)
            return await self.store.read_arrow_async(resolved_path, **kwargs)  # type: ignore[union-attr]
        except Exception as exc:
            msg = f"Failed to async read Arrow table from {path}"
            raise StorageOperationFailedError(msg) from exc

    async def _write_arrow_async(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        """Async write Arrow table using native obstore async."""
        try:
            resolved_path = self._resolve_path(path)
            await self.store.write_arrow_async(resolved_path, table, **kwargs)  # type: ignore[union-attr]
        except Exception as exc:
            msg = f"Failed to async write Arrow table to {path}"
            raise StorageOperationFailedError(msg) from exc

    async def _stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator[ArrowRecordBatch]:
        """Async stream Arrow record batches using native obstore async."""
        try:
            resolved_pattern = self._resolve_path(pattern)
            async for batch in self.store.stream_arrow_async(resolved_pattern, **kwargs):  # type: ignore[union-attr]
                yield batch
        except Exception as exc:
            msg = f"Failed to async stream Arrow data for pattern {pattern}"
            raise StorageOperationFailedError(msg) from exc
