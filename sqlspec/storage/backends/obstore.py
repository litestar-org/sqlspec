"""Object storage backend using obstore.

Implements the ObjectStoreProtocol using obstore for S3, GCS, Azure,
and local file storage.
"""

import fnmatch
import logging
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any, Final, Optional, Union, cast
from urllib.parse import urlparse

if TYPE_CHECKING:
    from pathlib import Path

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError
from sqlspec.typing import OBSTORE_INSTALLED, PYARROW_INSTALLED, ArrowRecordBatch, ArrowTable

__all__ = ("ObStoreBackend",)

logger = logging.getLogger(__name__)


class _AsyncArrowIterator:
    """Helper class to work around mypyc's lack of async generator support."""

    def __init__(self, backend: "ObStoreBackend", pattern: str, **kwargs: Any) -> None:
        self.backend = backend
        self.pattern = pattern
        self.kwargs = kwargs
        self._files_iterator: Optional[Iterator[str]] = None
        self._current_file_iterator: Optional[Iterator[ArrowRecordBatch]] = None

    def __aiter__(self) -> "_AsyncArrowIterator":
        return self

    async def __anext__(self) -> ArrowRecordBatch:
        if self._files_iterator is None:
            files = self.backend.glob(self.pattern, **self.kwargs)
            self._files_iterator = iter(files)

        while True:
            if self._current_file_iterator is not None:
                try:
                    return next(self._current_file_iterator)
                except StopIteration:
                    self._current_file_iterator = None

            try:
                next_file = next(self._files_iterator)
                # Stream from this file
                file_batches = self.backend.stream_arrow(next_file)
                self._current_file_iterator = iter(file_batches)
            except StopIteration:
                raise StopAsyncIteration


DEFAULT_OPTIONS: Final[dict[str, Any]] = {"connect_timeout": "30s", "request_timeout": "60s"}


@mypyc_attr(allow_interpreted_subclasses=True)
class ObStoreBackend:
    """Object storage backend using obstore.

    Implements ObjectStoreProtocol using obstore's Rust-based implementation
    for storage operations. Supports AWS S3, Google Cloud Storage, Azure Blob Storage,
    local filesystem, and HTTP endpoints.
    """

    __slots__ = ("_path_cache", "backend_type", "base_path", "protocol", "store", "store_options", "store_uri")

    def _ensure_obstore(self) -> None:
        """Ensure obstore is available for operations."""
        if not OBSTORE_INSTALLED:
            raise MissingDependencyError(package="obstore", install_package="obstore")

    def _ensure_pyarrow(self) -> None:
        """Ensure PyArrow is available for Arrow operations."""
        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="pyarrow")

    def __init__(self, uri: str, **kwargs: Any) -> None:
        """Initialize obstore backend.

        Args:
            uri: Storage URI (e.g., 's3://bucket', 'file:///path', 'gs://bucket')
            **kwargs: Additional options including base_path and obstore configuration
        """

        self._ensure_obstore()

        try:
            # Extract base_path from kwargs
            base_path = kwargs.pop("base_path", "")

            self.store_uri = uri
            self.base_path = base_path.rstrip("/") if base_path else ""
            self.store_options = kwargs
            self.store: Any
            self._path_cache: dict[str, str] = {}
            self.protocol = uri.split("://", 1)[0] if "://" in uri else "file"
            self.backend_type = "obstore"

            if uri.startswith("memory://"):
                from obstore.store import MemoryStore

                self.store = MemoryStore()
            elif uri.startswith("file://"):
                from pathlib import Path as PathlibPath

                from obstore.store import LocalStore

                parsed = urlparse(uri)
                path = parsed.path or "/"
                # Create directory if it doesn't exist (ObStore LocalStore requires it)
                PathlibPath(path).mkdir(parents=True, exist_ok=True)
                self.store = LocalStore(path)
            else:
                from obstore.store import from_url

                self.store = from_url(uri, **kwargs)  # pyright: ignore[reportAttributeAccessIssue]

            logger.debug("ObStore backend initialized for %s", uri)

        except Exception as exc:
            msg = f"Failed to initialize obstore backend for {uri}"
            raise StorageOperationFailedError(msg) from exc

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "ObStoreBackend":
        """Create backend from configuration dictionary."""
        store_uri = config["store_uri"]
        base_path = config.get("base_path", "")
        store_options = config.get("store_options", {})

        kwargs = dict(store_options)
        if base_path:
            kwargs["base_path"] = base_path

        return cls(uri=store_uri, **kwargs)

    def _resolve_path(self, path: "Union[str, Path]") -> str:
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

    def read_bytes(self, path: "Union[str, Path]", **kwargs: Any) -> bytes:  # pyright: ignore[reportUnusedParameter]
        """Read bytes using obstore."""
        result = self.store.get(self._resolve_path(path))
        return cast("bytes", result.bytes().to_bytes())

    def write_bytes(self, path: "Union[str, Path]", data: bytes, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write bytes using obstore."""
        self.store.put(self._resolve_path(path), data)

    def read_text(self, path: "Union[str, Path]", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text using obstore."""
        return self.read_bytes(path, **kwargs).decode(encoding)

    def write_text(self, path: "Union[str, Path]", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text using obstore."""
        self.write_bytes(path, data.encode(encoding), **kwargs)

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:  # pyright: ignore[reportUnusedParameter]
        """List objects using obstore."""
        resolved_prefix = self._resolve_path(prefix) if prefix else self.base_path or ""
        items = self.store.list_with_delimiter(resolved_prefix) if not recursive else self.store.list(resolved_prefix)
        paths: list[str] = []
        for batch in items:
            paths.extend(item["path"] for item in batch)
        return sorted(paths)

    def exists(self, path: "Union[str, Path]", **kwargs: Any) -> bool:  # pyright: ignore[reportUnusedParameter]
        """Check if object exists using obstore."""
        try:
            self.store.head(self._resolve_path(path))
        except Exception:
            return False
        return True

    def delete(self, path: "Union[str, Path]", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Delete object using obstore."""
        self.store.delete(self._resolve_path(path))

    def copy(self, source: "Union[str, Path]", destination: "Union[str, Path]", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Copy object using obstore."""
        self.store.copy(self._resolve_path(source), self._resolve_path(destination))

    def move(self, source: "Union[str, Path]", destination: "Union[str, Path]", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Move object using obstore."""
        self.store.rename(self._resolve_path(source), self._resolve_path(destination))

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching pattern.

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

    def get_metadata(self, path: "Union[str, Path]", **kwargs: Any) -> dict[str, Any]:  # pyright: ignore[reportUnusedParameter]
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

    def is_object(self, path: "Union[str, Path]") -> bool:
        """Check if path is an object using obstore."""
        resolved_path = self._resolve_path(path)
        return self.exists(path) and not resolved_path.endswith("/")

    def is_path(self, path: "Union[str, Path]") -> bool:
        """Check if path is a prefix/directory using obstore."""
        resolved_path = self._resolve_path(path)

        if resolved_path.endswith("/"):
            return True

        try:
            objects = self.list_objects(prefix=str(path), recursive=True)
            return len(objects) > 0
        except Exception:
            return False

    def read_arrow(self, path: "Union[str, Path]", **kwargs: Any) -> ArrowTable:
        """Read Arrow table using obstore."""
        resolved_path = self._resolve_path(path)
        if hasattr(self.store, "read_arrow"):
            return self.store.read_arrow(resolved_path, **kwargs)  # type: ignore[no-any-return]  # pyright: ignore[reportAttributeAccessIssue]

        self._ensure_pyarrow()
        import io

        import pyarrow.parquet as pq

        return pq.read_table(io.BytesIO(self.read_bytes(resolved_path)), **kwargs)

    def write_arrow(self, path: "Union[str, Path]", table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table using obstore."""
        resolved_path = self._resolve_path(path)
        if hasattr(self.store, "write_arrow"):
            self.store.write_arrow(resolved_path, table, **kwargs)  # pyright: ignore[reportAttributeAccessIssue]
        else:
            self._ensure_pyarrow()
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

    def stream_arrow(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Stream Arrow record batches.

        Yields:
            Iterator of Arrow record batches from matching objects.
        """
        self._ensure_pyarrow()
        from io import BytesIO

        import pyarrow.parquet as pq

        for obj_path in self.glob(pattern, **kwargs):
            result = self.store.get(self._resolve_path(obj_path))
            bytes_obj = result.bytes()
            data = bytes_obj.to_bytes()
            buffer = BytesIO(data)
            parquet_file = pq.ParquetFile(buffer)
            yield from parquet_file.iter_batches()

    def sign(self, path: str, expires_in: int = 3600, for_upload: bool = False) -> str:
        """Generate a signed URL for the object."""
        resolved_path = self._resolve_path(path)
        if hasattr(self.store, "sign_url") and callable(self.store.sign_url):
            return self.store.sign_url(resolved_path, expires_in=expires_in)  # type: ignore[no-any-return]
        return f"{self.store_uri}/{resolved_path}"

    async def read_bytes_async(self, path: "Union[str, Path]", **kwargs: Any) -> bytes:  # pyright: ignore[reportUnusedParameter]
        """Read bytes from storage asynchronously."""
        resolved_path = self._resolve_path(path)
        result = await self.store.get_async(resolved_path)
        bytes_obj = await result.bytes_async()
        return bytes_obj.to_bytes()  # type: ignore[no-any-return]  # pyright: ignore[reportAttributeAccessIssue]

    async def write_bytes_async(self, path: "Union[str, Path]", data: bytes, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write bytes to storage asynchronously."""
        resolved_path = self._resolve_path(path)
        await self.store.put_async(resolved_path, data)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:  # pyright: ignore[reportUnusedParameter]
        """List objects in storage asynchronously."""
        resolved_prefix = self._resolve_path(prefix) if prefix else self.base_path or ""

        objects: list[str] = []
        async for batch in self.store.list_async(resolved_prefix):  # pyright: ignore[reportAttributeAccessIssue]
            objects.extend(item["path"] for item in batch)

        if not recursive and resolved_prefix:
            base_depth = resolved_prefix.count("/")
            objects = [obj for obj in objects if obj.count("/") <= base_depth + 1]

        return sorted(objects)

    async def read_text_async(self, path: "Union[str, Path]", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage asynchronously."""
        data = await self.read_bytes_async(path, **kwargs)
        return data.decode(encoding)

    async def write_text_async(
        self, path: "Union[str, Path]", data: str, encoding: str = "utf-8", **kwargs: Any
    ) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write text to storage asynchronously."""
        encoded_data = data.encode(encoding)
        await self.write_bytes_async(path, encoded_data, **kwargs)

    async def exists_async(self, path: "Union[str, Path]", **kwargs: Any) -> bool:  # pyright: ignore[reportUnusedParameter]
        """Check if object exists in storage asynchronously."""
        resolved_path = self._resolve_path(path)
        try:
            await self.store.head_async(resolved_path)
        except Exception:
            return False
        return True

    async def delete_async(self, path: "Union[str, Path]", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Delete object from storage asynchronously."""
        resolved_path = self._resolve_path(path)
        await self.store.delete_async(resolved_path)

    async def copy_async(self, source: "Union[str, Path]", destination: "Union[str, Path]", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Copy object in storage asynchronously."""
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        await self.store.copy_async(source_path, dest_path)

    async def move_async(self, source: "Union[str, Path]", destination: "Union[str, Path]", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Move object in storage asynchronously."""
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        await self.store.rename_async(source_path, dest_path)

    async def get_metadata_async(self, path: "Union[str, Path]", **kwargs: Any) -> dict[str, Any]:  # pyright: ignore[reportUnusedParameter]
        """Get object metadata from storage asynchronously."""
        resolved_path = self._resolve_path(path)
        result: dict[str, Any] = {}
        try:
            metadata = await self.store.head_async(resolved_path)
            result.update(
                {
                    "path": resolved_path,
                    "exists": True,
                    "size": metadata.get("size"),
                    "last_modified": metadata.get("last_modified"),
                    "e_tag": metadata.get("e_tag"),
                    "version": metadata.get("version"),
                }
            )
            if metadata.get("metadata"):
                result["custom_metadata"] = metadata["metadata"]

        except Exception:
            return {"path": resolved_path, "exists": False}
        else:
            return result

    async def read_arrow_async(self, path: "Union[str, Path]", **kwargs: Any) -> ArrowTable:
        """Read Arrow table from storage asynchronously."""
        resolved_path = self._resolve_path(path)
        if hasattr(self.store, "read_arrow_async"):
            return await self.store.read_arrow_async(resolved_path, **kwargs)  # type: ignore[no-any-return]  # pyright: ignore[reportAttributeAccessIssue]

        self._ensure_pyarrow()
        import io

        import pyarrow.parquet as pq

        return pq.read_table(io.BytesIO(await self.read_bytes_async(resolved_path)), **kwargs)

    async def write_arrow_async(self, path: "Union[str, Path]", table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table to storage asynchronously."""
        resolved_path = self._resolve_path(path)
        if hasattr(self.store, "write_arrow_async"):
            await self.store.write_arrow_async(resolved_path, table, **kwargs)  # pyright: ignore[reportAttributeAccessIssue]
        else:
            self._ensure_pyarrow()
            import io

            import pyarrow.parquet as pq

            buffer = io.BytesIO()
            pq.write_table(table, buffer, **kwargs)
            buffer.seek(0)
            await self.write_bytes_async(resolved_path, buffer.read())

    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator[ArrowRecordBatch]:
        resolved_pattern = self._resolve_path(pattern)
        return _AsyncArrowIterator(self, resolved_pattern, **kwargs)

    async def sign_async(self, path: str, expires_in: int = 3600, for_upload: bool = False) -> str:
        """Generate a signed URL asynchronously."""
        resolved_path = self._resolve_path(path)
        if hasattr(self.store, "sign_url_async") and callable(self.store.sign_url_async):
            return await self.store.sign_url_async(resolved_path, expires_in=expires_in)  # type: ignore[no-any-return]
        return f"{self.store_uri}/{resolved_path}"
