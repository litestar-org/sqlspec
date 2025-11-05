"""Object storage backend using obstore.

Implements the ObjectStoreProtocol using obstore for S3, GCS, Azure,
and local file storage.
"""

import fnmatch
import io
import logging
import re
from collections.abc import AsyncIterator, Iterator
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Any, Final, cast
from urllib.parse import urlparse

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import StorageOperationFailedError
from sqlspec.storage._utils import import_pyarrow, import_pyarrow_parquet, resolve_storage_path
from sqlspec.storage.errors import execute_sync_storage_operation
from sqlspec.typing import ArrowRecordBatch, ArrowTable
from sqlspec.utils.module_loader import ensure_obstore
from sqlspec.utils.sync_tools import async_

__all__ = ("ObStoreBackend",)

logger = logging.getLogger(__name__)


class _AsyncArrowIterator:
    """Helper class to work around mypyc's lack of async generator support.

    Uses hybrid async/sync pattern:
    - Native async I/O for network operations (S3, GCS, Azure)
    - Thread pool for CPU-bound PyArrow parsing
    """

    __slots__ = ("_current_file_iterator", "_files_iterator", "backend", "kwargs", "pattern")

    def __init__(self, backend: "ObStoreBackend", pattern: str, **kwargs: Any) -> None:
        self.backend = backend
        self.pattern = pattern
        self.kwargs = kwargs
        self._files_iterator: Iterator[str] | None = None
        self._current_file_iterator: Iterator[ArrowRecordBatch] | None = None

    def __aiter__(self) -> "_AsyncArrowIterator":
        return self

    async def __anext__(self) -> ArrowRecordBatch:
        pq = import_pyarrow_parquet()

        if self._files_iterator is None:
            files = self.backend.glob(self.pattern, **self.kwargs)
            self._files_iterator = iter(files)

        while True:
            if self._current_file_iterator is not None:

                def _safe_next_batch() -> ArrowRecordBatch:
                    try:
                        return next(self._current_file_iterator)  # type: ignore[arg-type]
                    except StopIteration as e:
                        raise StopAsyncIteration from e

                try:
                    return await async_(_safe_next_batch)()
                except StopAsyncIteration:
                    self._current_file_iterator = None
                    continue

            try:
                next_file = next(self._files_iterator)
            except StopIteration as e:
                raise StopAsyncIteration from e

            data = await self.backend.read_bytes_async(next_file)
            parquet_file = pq.ParquetFile(io.BytesIO(data))
            self._current_file_iterator = parquet_file.iter_batches()

    async def aclose(self) -> None:
        """Close underlying file iterator."""
        if self._current_file_iterator is not None:
            try:
                close_method = self._current_file_iterator.close  # type: ignore[attr-defined]
                await async_(close_method)()  # pyright: ignore
            except AttributeError:
                pass


DEFAULT_OPTIONS: Final[dict[str, Any]] = {"connect_timeout": "30s", "request_timeout": "60s"}


@mypyc_attr(allow_interpreted_subclasses=True)
class ObStoreBackend:
    """Object storage backend using obstore.

    Implements ObjectStoreProtocol using obstore's Rust-based implementation
    for storage operations. Supports AWS S3, Google Cloud Storage, Azure Blob Storage,
    local filesystem, and HTTP endpoints.
    """

    __slots__ = (
        "_is_local_store",
        "_local_store_root",
        "_path_cache",
        "backend_type",
        "base_path",
        "protocol",
        "store",
        "store_options",
        "store_uri",
    )

    def __init__(self, uri: str, **kwargs: Any) -> None:
        """Initialize obstore backend.

        Args:
            uri: Storage URI (e.g., 's3://bucket', 'file:///path', 'gs://bucket')
            **kwargs: Additional options including base_path and obstore configuration

        """
        ensure_obstore()
        base_path = kwargs.pop("base_path", "")

        self.store_uri = uri
        self.base_path = base_path.rstrip("/") if base_path else ""
        self.store_options = kwargs
        self.store: Any
        self._path_cache: dict[str, str] = {}
        self._is_local_store = False
        self._local_store_root = ""
        self.protocol = uri.split("://", 1)[0] if "://" in uri else "file"
        self.backend_type = "obstore"
        try:
            if uri.startswith("memory://"):
                from obstore.store import MemoryStore

                self.store = MemoryStore()
            elif uri.startswith("file://"):
                from obstore.store import LocalStore

                parsed = urlparse(uri)
                path_str = parsed.path or "/"
                if parsed.fragment:
                    path_str = f"{path_str}#{parsed.fragment}"
                path_obj = Path(path_str)

                if path_obj.is_file():
                    path_str = str(path_obj.parent)

                local_store_root = self.base_path or path_str

                self._is_local_store = True
                self._local_store_root = local_store_root
                self.store = LocalStore(local_store_root, mkdir=True)
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

    def _resolve_path_for_local_store(self, path: "str | Path") -> str:
        """Resolve path for LocalStore which expects relative paths from its root."""

        path_obj = Path(str(path))

        if path_obj.is_absolute() and self._local_store_root:
            try:
                return str(path_obj.relative_to(self._local_store_root))
            except ValueError:
                return str(path).lstrip("/")

        return str(path)

    def read_bytes(self, path: "str | Path", **kwargs: Any) -> bytes:  # pyright: ignore[reportUnusedParameter]
        """Read bytes using obstore."""
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        def _action() -> bytes:
            result = self.store.get(resolved_path)
            return cast("bytes", result.bytes().to_bytes())

        return execute_sync_storage_operation(
            _action, backend=self.backend_type, operation="read_bytes", path=resolved_path
        )

    def write_bytes(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write bytes using obstore."""
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        execute_sync_storage_operation(
            lambda: self.store.put(resolved_path, data),
            backend=self.backend_type,
            operation="write_bytes",
            path=resolved_path,
        )

    def read_text(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text using obstore."""
        return self.read_bytes(path, **kwargs).decode(encoding)

    def write_text(self, path: "str | Path", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text using obstore."""
        self.write_bytes(path, data.encode(encoding), **kwargs)

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:  # pyright: ignore[reportUnusedParameter]
        """List objects using obstore."""
        resolved_prefix = (
            resolve_storage_path(prefix, self.base_path, self.protocol, strip_file_scheme=True)
            if prefix
            else self.base_path or ""
        )
        items = self.store.list_with_delimiter(resolved_prefix) if not recursive else self.store.list(resolved_prefix)
        paths: list[str] = []
        for batch in items:
            paths.extend(item["path"] for item in batch)
        return sorted(paths)

    def exists(self, path: "str | Path", **kwargs: Any) -> bool:  # pyright: ignore[reportUnusedParameter]
        """Check if object exists using obstore."""
        try:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)
            self.store.head(resolved_path)
        except Exception:
            return False
        return True

    def delete(self, path: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Delete object using obstore."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)
        execute_sync_storage_operation(
            lambda: self.store.delete(resolved_path), backend=self.backend_type, operation="delete", path=resolved_path
        )

    def copy(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Copy object using obstore."""
        source_path = resolve_storage_path(source, self.base_path, self.protocol, strip_file_scheme=True)
        dest_path = resolve_storage_path(destination, self.base_path, self.protocol, strip_file_scheme=True)
        execute_sync_storage_operation(
            lambda: self.store.copy(source_path, dest_path),
            backend=self.backend_type,
            operation="copy",
            path=f"{source_path}->{dest_path}",
        )

    def move(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Move object using obstore."""
        source_path = resolve_storage_path(source, self.base_path, self.protocol, strip_file_scheme=True)
        dest_path = resolve_storage_path(destination, self.base_path, self.protocol, strip_file_scheme=True)
        execute_sync_storage_operation(
            lambda: self.store.rename(source_path, dest_path),
            backend=self.backend_type,
            operation="move",
            path=f"{source_path}->{dest_path}",
        )

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching pattern.

        Lists all objects and filters them client-side using the pattern.
        """

        resolved_pattern = resolve_storage_path(pattern, self.base_path, self.protocol, strip_file_scheme=True)
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

    def get_metadata(self, path: "str | Path", **kwargs: Any) -> dict[str, Any]:  # pyright: ignore[reportUnusedParameter]
        """Get object metadata using obstore."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        try:
            metadata = self.store.head(resolved_path)
        except Exception:
            return {"path": resolved_path, "exists": False}
        else:
            if isinstance(metadata, dict):
                result = {
                    "path": resolved_path,
                    "exists": True,
                    "size": metadata.get("size"),
                    "last_modified": metadata.get("last_modified"),
                    "e_tag": metadata.get("e_tag"),
                    "version": metadata.get("version"),
                }
                if metadata.get("metadata"):
                    result["custom_metadata"] = metadata["metadata"]
                return result

            result = {
                "path": resolved_path,
                "exists": True,
                "size": metadata.size,
                "last_modified": metadata.last_modified,
                "e_tag": metadata.e_tag,
                "version": metadata.version,
            }

            if metadata.metadata:
                result["custom_metadata"] = metadata.metadata

            return result

    def is_object(self, path: "str | Path") -> bool:
        """Check if path is an object using obstore."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)
        return self.exists(path) and not resolved_path.endswith("/")

    def is_path(self, path: "str | Path") -> bool:
        """Check if path is a prefix/directory using obstore."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        if resolved_path.endswith("/"):
            return True

        try:
            objects = self.list_objects(prefix=str(path), recursive=True)
            return len(objects) > 0
        except Exception:
            return False

    def read_arrow(self, path: "str | Path", **kwargs: Any) -> ArrowTable:
        """Read Arrow table using obstore."""
        pq = import_pyarrow_parquet()
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)
        data = self.read_bytes(resolved_path)
        return cast(
            "ArrowTable",
            execute_sync_storage_operation(
                lambda: pq.read_table(io.BytesIO(data), **kwargs),
                backend=self.backend_type,
                operation="read_arrow",
                path=resolved_path,
            ),
        )

    def write_arrow(self, path: "str | Path", table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table using obstore."""
        pa = import_pyarrow()
        pq = import_pyarrow_parquet()
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        schema = table.schema
        if any(str(f.type).startswith("decimal64") for f in schema):
            new_fields = []
            for field in schema:
                if str(field.type).startswith("decimal64"):
                    match = re.match(r"decimal64\((\d+),\s*(\d+)\)", str(field.type))
                    if match:
                        precision, scale = int(match.group(1)), int(match.group(2))
                        new_fields.append(pa.field(field.name, pa.decimal128(precision, scale)))
                    else:
                        new_fields.append(field)
                else:
                    new_fields.append(field)
            table = table.cast(pa.schema(new_fields))

        buffer = io.BytesIO()
        execute_sync_storage_operation(
            lambda: pq.write_table(table, buffer, **kwargs),
            backend=self.backend_type,
            operation="write_arrow",
            path=resolved_path,
        )
        buffer.seek(0)
        self.write_bytes(resolved_path, buffer.read())

    def stream_arrow(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Stream Arrow record batches.

        Yields:
            Iterator of Arrow record batches from matching objects.
        """
        pq = import_pyarrow_parquet()
        for obj_path in self.glob(pattern, **kwargs):
            resolved_path = resolve_storage_path(obj_path, self.base_path, self.protocol, strip_file_scheme=True)
            result = execute_sync_storage_operation(
                partial(self.store.get, resolved_path),
                backend=self.backend_type,
                operation="stream_read",
                path=resolved_path,
            )
            bytes_obj = result.bytes()
            data = bytes_obj.to_bytes()
            buffer = io.BytesIO(data)
            parquet_file = execute_sync_storage_operation(
                partial(pq.ParquetFile, buffer), backend=self.backend_type, operation="stream_arrow", path=resolved_path
            )
            yield from parquet_file.iter_batches()

    def sign(self, path: str, expires_in: int = 3600, for_upload: bool = False) -> str:
        """Generate a signed URL for the object."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)
        return f"{self.store_uri}/{resolved_path}"

    async def read_bytes_async(self, path: "str | Path", **kwargs: Any) -> bytes:  # pyright: ignore[reportUnusedParameter]
        """Read bytes from storage asynchronously."""
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        result = await self.store.get_async(resolved_path)
        bytes_obj = await result.bytes_async()
        return bytes_obj.to_bytes()  # type: ignore[no-any-return]  # pyright: ignore[reportAttributeAccessIssue]

    async def write_bytes_async(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write bytes to storage asynchronously."""
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        await self.store.put_async(resolved_path, data)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:  # pyright: ignore[reportUnusedParameter]
        """List objects in storage asynchronously."""
        resolved_prefix = (
            resolve_storage_path(prefix, self.base_path, self.protocol, strip_file_scheme=True)
            if prefix
            else self.base_path or ""
        )

        objects: list[str] = []
        async for batch in self.store.list_async(resolved_prefix):  # pyright: ignore[reportAttributeAccessIssue]
            objects.extend(item["path"] for item in batch)

        if not recursive and resolved_prefix:
            base_depth = resolved_prefix.count("/")
            objects = [obj for obj in objects if obj.count("/") <= base_depth + 1]

        return sorted(objects)

    async def read_text_async(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage asynchronously."""
        data = await self.read_bytes_async(path, **kwargs)
        return data.decode(encoding)

    async def write_text_async(self, path: "str | Path", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write text to storage asynchronously."""
        encoded_data = data.encode(encoding)
        await self.write_bytes_async(path, encoded_data, **kwargs)

    async def exists_async(self, path: "str | Path", **kwargs: Any) -> bool:  # pyright: ignore[reportUnusedParameter]
        """Check if object exists in storage asynchronously."""
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        try:
            await self.store.head_async(resolved_path)
        except Exception:
            return False
        return True

    async def delete_async(self, path: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Delete object from storage asynchronously."""
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        await self.store.delete_async(resolved_path)

    async def copy_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Copy object in storage asynchronously."""
        if self._is_local_store:
            source_path = self._resolve_path_for_local_store(source)
            dest_path = self._resolve_path_for_local_store(destination)
        else:
            source_path = resolve_storage_path(source, self.base_path, self.protocol, strip_file_scheme=True)
            dest_path = resolve_storage_path(destination, self.base_path, self.protocol, strip_file_scheme=True)

        await self.store.copy_async(source_path, dest_path)

    async def move_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Move object in storage asynchronously."""
        if self._is_local_store:
            source_path = self._resolve_path_for_local_store(source)
            dest_path = self._resolve_path_for_local_store(destination)
        else:
            source_path = resolve_storage_path(source, self.base_path, self.protocol, strip_file_scheme=True)
            dest_path = resolve_storage_path(destination, self.base_path, self.protocol, strip_file_scheme=True)

        await self.store.rename_async(source_path, dest_path)

    async def get_metadata_async(self, path: "str | Path", **kwargs: Any) -> dict[str, Any]:  # pyright: ignore[reportUnusedParameter]
        """Get object metadata from storage asynchronously."""
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        result: dict[str, Any] = {}
        try:
            metadata = await self.store.head_async(resolved_path)
            result.update({
                "path": resolved_path,
                "exists": True,
                "size": metadata.get("size"),
                "last_modified": metadata.get("last_modified"),
                "e_tag": metadata.get("e_tag"),
                "version": metadata.get("version"),
            })
            if metadata.get("metadata"):
                result["custom_metadata"] = metadata["metadata"]

        except Exception:
            return {"path": resolved_path, "exists": False}
        else:
            return result

    async def read_arrow_async(self, path: "str | Path", **kwargs: Any) -> ArrowTable:
        """Read Arrow table from storage asynchronously."""
        pq = import_pyarrow_parquet()
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)
        data = await self.read_bytes_async(resolved_path)
        return cast("ArrowTable", pq.read_table(io.BytesIO(data), **kwargs))

    async def write_arrow_async(self, path: "str | Path", table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table to storage asynchronously."""
        pq = import_pyarrow_parquet()
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, **kwargs)
        buffer.seek(0)
        await self.write_bytes_async(resolved_path, buffer.read())

    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator[ArrowRecordBatch]:
        resolved_pattern = resolve_storage_path(pattern, self.base_path, self.protocol, strip_file_scheme=True)
        return _AsyncArrowIterator(self, resolved_pattern, **kwargs)

    async def sign_async(self, path: str, expires_in: int = 3600, for_upload: bool = False) -> str:
        """Generate a signed URL asynchronously."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)
        return f"{self.store_uri}/{resolved_path}"
