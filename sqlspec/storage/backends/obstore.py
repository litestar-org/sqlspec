"""Object storage backend using obstore.

Implements the ObjectStoreProtocol using obstore,
providing native support for S3, GCS, Azure, and local file storage
with Arrow support.
"""

from __future__ import annotations

import fnmatch
import logging
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError
from sqlspec.storage.backends.base import ObjectStoreBase
from sqlspec.storage.capabilities import HasStorageCapabilities, StorageCapabilities
from sqlspec.typing import OBSTORE_INSTALLED

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator
    from pathlib import Path

    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("ObStoreBackend",)

logger = logging.getLogger(__name__)


class _AsyncArrowIterator:
    """Helper class to work around mypyc's lack of async generator support."""

    def __init__(self, store: Any, pattern: str, **kwargs: Any) -> None:
        self.store = store
        self.pattern = pattern
        self.kwargs = kwargs
        self._iterator: Any | None = None

    def __aiter__(self) -> _AsyncArrowIterator:
        return self

    async def __anext__(self) -> ArrowRecordBatch:
        if self._iterator is None:
            self._iterator = self.store.stream_arrow_async(self.pattern, **self.kwargs)
        if self._iterator is not None:
            return cast("ArrowRecordBatch", await self._iterator.__anext__())
        raise StopAsyncIteration


DEFAULT_OPTIONS: Final[dict[str, Any]] = {"connect_timeout": "30s", "request_timeout": "60s"}


@mypyc_attr(allow_interpreted_subclasses=True)
class ObStoreBackend(ObjectStoreBase, HasStorageCapabilities):
    """Object storage backend using obstore.

    Uses obstore's Rust-based implementation for storage operations,
    providing native support for AWS S3, Google Cloud Storage, Azure Blob Storage,
    local filesystem, and HTTP endpoints.

    Includes native Arrow support.
    """

    capabilities: ClassVar[StorageCapabilities] = StorageCapabilities(
        supports_arrow=True,
        supports_streaming=True,
        supports_async=True,
        supports_batch_operations=True,
        supports_multipart_upload=True,
        supports_compression=True,
        is_cloud_native=True,
        has_low_latency=True,
    )

    __slots__ = ("_path_cache", "base_path", "protocol", "store", "store_options", "store_uri")

    def __init__(self, store_uri: str, base_path: str = "", **store_options: Any) -> None:
        """Initialize obstore backend.

        Args:
            store_uri: Storage URI (e.g., 's3://bucket', 'file:///path', 'gs://bucket')
            base_path: Base path prefix for all operations
            **store_options: Additional options for obstore configuration
        """

        if not OBSTORE_INSTALLED:
            raise MissingDependencyError(package="obstore", install_package="obstore")

        try:
            self.store_uri = store_uri
            self.base_path = base_path.rstrip("/") if base_path else ""
            self.store_options = store_options
            self.store: Any
            self._path_cache: dict[str, str] = {}
            self.protocol = store_uri.split("://", 1)[0] if "://" in store_uri else "file"

            if store_uri.startswith("memory://"):
                from obstore.store import MemoryStore

                self.store = MemoryStore()
            elif store_uri.startswith("file://"):
                from obstore.store import LocalStore

                self.store = LocalStore("/")
            else:
                from obstore.store import from_url

                self.store = from_url(store_uri, **store_options)  # pyright: ignore[reportAttributeAccessIssue]

            logger.debug("ObStore backend initialized for %s", store_uri)

        except Exception as exc:
            msg = f"Failed to initialize obstore backend for {store_uri}"
            raise StorageOperationFailedError(msg) from exc

    def _resolve_path(self, path: str | Path) -> str:
        """Resolve path relative to base_path."""
        path_str = str(path)
        if path_str.startswith("file://"):
            path_str = path_str.removeprefix("file://")
        if self.store_uri.startswith("file://") and path_str.startswith("/"):
            return path_str.lstrip("/")
        if self.base_path:
            clean_base = self.base_path.rstrip("/")
            clean_path = path_str.lstrip("/")
            return f"{clean_base}/{clean_path}"
        return path_str

    @property
    def backend_type(self) -> str:
        """Return backend type identifier."""
        return "obstore"

    def read_bytes(self, path: str | Path, **kwargs: Any) -> bytes:  # pyright: ignore[reportUnusedParameter]
        """Read bytes using obstore."""
        try:
            result = self.store.get(self._resolve_path(path))
            return cast("bytes", result.bytes().to_bytes())
        except Exception as exc:
            msg = f"Failed to read bytes from {path}"
            raise StorageOperationFailedError(msg) from exc

    def write_bytes(self, path: str | Path, data: bytes, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write bytes using obstore."""
        try:
            self.store.put(self._resolve_path(path), data)
        except Exception as exc:
            msg = f"Failed to write bytes to {path}"
            raise StorageOperationFailedError(msg) from exc

    def read_text(self, path: str | Path, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text using obstore."""
        return self.read_bytes(path, **kwargs).decode(encoding)

    def write_text(self, path: str | Path, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text using obstore."""
        self.write_bytes(path, data.encode(encoding), **kwargs)

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:  # pyright: ignore[reportUnusedParameter]
        """List objects using obstore."""
        try:
            resolved_prefix = self._resolve_path(prefix) if prefix else self.base_path or ""
            items = (
                self.store.list_with_delimiter(resolved_prefix) if not recursive else self.store.list(resolved_prefix)
            )
            return sorted(str(getattr(item, "path", getattr(item, "key", str(item)))) for item in items)
        except Exception as exc:
            msg = f"Failed to list objects with prefix '{prefix}'"
            raise StorageOperationFailedError(msg) from exc

    def exists(self, path: str | Path, **kwargs: Any) -> bool:  # pyright: ignore[reportUnusedParameter]
        """Check if object exists using obstore."""
        try:
            self.store.head(self._resolve_path(path))
        except Exception:
            return False
        return True

    def delete(self, path: str | Path, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Delete object using obstore."""
        try:
            self.store.delete(self._resolve_path(path))
        except Exception as exc:
            msg = f"Failed to delete {path}"
            raise StorageOperationFailedError(msg) from exc

    def copy(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Copy object using obstore."""
        try:
            self.store.copy(self._resolve_path(source), self._resolve_path(destination))
        except Exception as exc:
            msg = f"Failed to copy {source} to {destination}"
            raise StorageOperationFailedError(msg) from exc

    def move(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Move object using obstore."""
        try:
            self.store.rename(self._resolve_path(source), self._resolve_path(destination))
        except Exception as exc:
            msg = f"Failed to move {source} to {destination}"
            raise StorageOperationFailedError(msg) from exc

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching pattern using obstore.

        Lists all objects and filters them client-side using the pattern.
        """
        from pathlib import PurePosixPath

        resolved_pattern = self._resolve_path(pattern)
        all_objects = self.list_objects(recursive=True, **kwargs)

        if "**" in pattern:
            matching_objects = []

            if pattern.startswith("**/"):
                suffix_pattern = pattern[3:]

                for obj in all_objects:
                    obj_path = PurePosixPath(obj)
                    if obj_path.match(resolved_pattern) or obj_path.match(suffix_pattern):
                        matching_objects.append(obj)
            else:
                for obj in all_objects:
                    obj_path = PurePosixPath(obj)
                    if obj_path.match(resolved_pattern):
                        matching_objects.append(obj)

            return matching_objects
        return [obj for obj in all_objects if fnmatch.fnmatch(obj, resolved_pattern)]

    def get_metadata(self, path: str | Path, **kwargs: Any) -> dict[str, Any]:  # pyright: ignore[reportUnusedParameter]
        """Get object metadata using obstore."""
        resolved_path = self._resolve_path(path)
        result: dict[str, Any] = {}
        try:
            metadata = self.store.head(resolved_path)
            result.update(
                {
                    "path": resolved_path,
                    "exists": True,
                    "size": getattr(metadata, "size", None),
                    "last_modified": getattr(metadata, "last_modified", None),
                    "e_tag": getattr(metadata, "e_tag", None),
                    "version": getattr(metadata, "version", None),
                }
            )
            if hasattr(metadata, "metadata") and metadata.metadata:
                result["custom_metadata"] = metadata.metadata

        except Exception:
            return {"path": resolved_path, "exists": False}
        else:
            return result

    def is_object(self, path: str | Path) -> bool:
        """Check if path is an object using obstore."""
        resolved_path = self._resolve_path(path)
        return self.exists(path) and not resolved_path.endswith("/")

    def is_path(self, path: str | Path) -> bool:
        """Check if path is a prefix/directory using obstore."""
        resolved_path = self._resolve_path(path)

        if resolved_path.endswith("/"):
            return True

        try:
            objects = self.list_objects(prefix=str(path), recursive=True)
            return len(objects) > 0
        except Exception:
            return False

    def read_arrow(self, path: str | Path, **kwargs: Any) -> ArrowTable:
        """Read Arrow table using obstore."""
        try:
            resolved_path = self._resolve_path(path)
            if hasattr(self.store, "read_arrow"):
                return self.store.read_arrow(resolved_path, **kwargs)  # type: ignore[no-any-return]  # pyright: ignore[reportAttributeAccessIssue]

            import io

            import pyarrow.parquet as pq

            data = self.read_bytes(resolved_path)
            buffer = io.BytesIO(data)
            return pq.read_table(buffer, **kwargs)
        except Exception as exc:
            msg = f"Failed to read Arrow table from {path}"
            raise StorageOperationFailedError(msg) from exc

    def write_arrow(self, path: str | Path, table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table using obstore."""
        try:
            resolved_path = self._resolve_path(path)
            if hasattr(self.store, "write_arrow"):
                self.store.write_arrow(resolved_path, table, **kwargs)  # pyright: ignore[reportAttributeAccessIssue]
            else:
                import io

                import pyarrow as pa
                import pyarrow.parquet as pq

                buffer = io.BytesIO()

                schema = table.schema
                if any(str(f.type).startswith("decimal64") for f in schema):
                    new_fields = []
                    for field in schema:
                        if str(field.type).startswith("decimal64"):
                            import re

                            match = re.match(r"decimal64\((\d+),\s*(\d+)\)", str(field.type))
                            if match:
                                precision, scale = int(match.group(1)), int(match.group(2))
                                new_fields.append(pa.field(field.name, pa.decimal128(precision, scale)))
                            else:
                                new_fields.append(field)  # pragma: no cover
                        else:
                            new_fields.append(field)
                    table = table.cast(pa.schema(new_fields))

                pq.write_table(table, buffer, **kwargs)
                buffer.seek(0)
                self.write_bytes(resolved_path, buffer.read())
        except Exception as exc:
            msg = f"Failed to write Arrow table to {path}"
            raise StorageOperationFailedError(msg) from exc

    def stream_arrow(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Stream Arrow record batches using obstore.

        Yields:
            Iterator of Arrow record batches from matching objects.
        """
        try:
            resolved_pattern = self._resolve_path(pattern)
            yield from self.store.stream_arrow(resolved_pattern, **kwargs)  # pyright: ignore[reportAttributeAccessIssue]
        except Exception as exc:
            msg = f"Failed to stream Arrow data for pattern {pattern}"
            raise StorageOperationFailedError(msg) from exc

    async def read_bytes_async(self, path: str | Path, **kwargs: Any) -> bytes:  # pyright: ignore[reportUnusedParameter]
        """Read bytes from storage asynchronously."""
        try:
            resolved_path = self._resolve_path(path)
            result = await self.store.get_async(resolved_path)
            bytes_obj = await result.bytes_async()
            return bytes_obj.to_bytes()  # type: ignore[no-any-return]  # pyright: ignore[reportAttributeAccessIssue]
        except Exception as exc:
            msg = f"Failed to read bytes from {path}"
            raise StorageOperationFailedError(msg) from exc

    async def write_bytes_async(self, path: str | Path, data: bytes, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write bytes to storage asynchronously."""
        resolved_path = self._resolve_path(path)
        await self.store.put_async(resolved_path, data)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:  # pyright: ignore[reportUnusedParameter]
        """List objects in storage asynchronously."""
        try:
            resolved_prefix = self._resolve_path(prefix) if prefix else self.base_path or ""

            objects = [str(item.path) async for item in self.store.list_async(resolved_prefix)]  # pyright: ignore[reportAttributeAccessIssue]

            if not recursive and resolved_prefix:
                base_depth = resolved_prefix.count("/")
                objects = [obj for obj in objects if obj.count("/") <= base_depth + 1]

            return sorted(objects)
        except Exception as exc:
            msg = f"Failed to list objects with prefix '{prefix}'"
            raise StorageOperationFailedError(msg) from exc

    async def read_text_async(self, path: str | Path, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage asynchronously."""
        data = await self.read_bytes_async(path, **kwargs)
        return data.decode(encoding)

    async def write_text_async(self, path: str | Path, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write text to storage asynchronously."""
        encoded_data = data.encode(encoding)
        await self.write_bytes_async(path, encoded_data, **kwargs)

    async def exists_async(self, path: str | Path, **kwargs: Any) -> bool:  # pyright: ignore[reportUnusedParameter]
        """Check if object exists in storage asynchronously."""
        resolved_path = self._resolve_path(path)
        try:
            await self.store.head_async(resolved_path)
        except Exception:
            return False
        return True

    async def delete_async(self, path: str | Path, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Delete object from storage asynchronously."""
        resolved_path = self._resolve_path(path)
        await self.store.delete_async(resolved_path)

    async def copy_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Copy object in storage asynchronously."""
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        await self.store.copy_async(source_path, dest_path)

    async def move_async(self, source: str | Path, destination: str | Path, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Move object in storage asynchronously."""
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        await self.store.rename_async(source_path, dest_path)

    async def get_metadata_async(self, path: str | Path, **kwargs: Any) -> dict[str, Any]:  # pyright: ignore[reportUnusedParameter]
        """Get object metadata from storage asynchronously."""
        resolved_path = self._resolve_path(path)
        result: dict[str, Any] = {}
        try:
            metadata = await self.store.head_async(resolved_path)
            result.update(
                {
                    "path": resolved_path,
                    "exists": True,
                    "size": metadata.size,
                    "last_modified": metadata.last_modified,
                    "e_tag": metadata.e_tag,
                    "version": metadata.version,
                }
            )
            if hasattr(metadata, "metadata") and metadata.metadata:
                result["custom_metadata"] = metadata.metadata

        except Exception:
            return {"path": resolved_path, "exists": False}
        else:
            return result

    async def read_arrow_async(self, path: str | Path, **kwargs: Any) -> ArrowTable:
        """Read Arrow table from storage asynchronously."""
        resolved_path = self._resolve_path(path)
        return await self.store.read_arrow_async(resolved_path, **kwargs)  # type: ignore[no-any-return]  # pyright: ignore[reportAttributeAccessIssue]

    async def write_arrow_async(self, path: str | Path, table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table to storage asynchronously."""
        resolved_path = self._resolve_path(path)
        if hasattr(self.store, "write_arrow_async"):
            await self.store.write_arrow_async(resolved_path, table, **kwargs)  # pyright: ignore[reportAttributeAccessIssue]
        else:
            import io

            import pyarrow.parquet as pq

            buffer = io.BytesIO()
            pq.write_table(table, buffer, **kwargs)
            buffer.seek(0)
            await self.write_bytes_async(resolved_path, buffer.read())

    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator[ArrowRecordBatch]:
        resolved_pattern = self._resolve_path(pattern)
        return _AsyncArrowIterator(self.store, resolved_pattern, **kwargs)
