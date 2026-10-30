"""Object storage backend using obstore.

Implements the ObjectStoreProtocol using obstore for S3, GCS, Azure,
and local file storage.
"""

import contextlib
import io
import re
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, Self, cast, overload
from urllib.parse import urlparse

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import StorageOperationFailedError, StoragePathTraversalError
from sqlspec.storage._arrow_payload import resolve_parquet_use_dictionary
from sqlspec.storage._arrow_stream import iter_parquet_row_groups, validate_parquet_stream_options
from sqlspec.storage._paths import (
    ensure_path_within_root,
    extract_glob_static_prefix,
    glob_to_regex,
    is_file_destination,
    parse_file_uri_path,
    reject_parent_traversal,
    resolve_storage_path,
)
from sqlspec.storage._utils import (
    import_obstore,
    import_obstore_store,
    import_pyarrow,
    import_pyarrow_parquet,
    log_storage_event,
)
from sqlspec.storage.backends.base import AsyncArrowBatchIterator, AsyncObStoreStreamIterator

if TYPE_CHECKING:
    from obstore.store import ObjectStore

    from sqlspec.typing import ArrowRecordBatch, ArrowTable

from sqlspec.storage.errors import execute_async_storage_operation, execute_sync_storage_operation
from sqlspec.utils.module_loader import ensure_obstore
from sqlspec.utils.sync_tools import async_

_MAX_SIGN_EXPIRES_SECONDS: Final[int] = 604800
_SIGNABLE_PROTOCOLS: Final[frozenset[str]] = frozenset({"s3", "gs", "gcs", "az", "azure"})

__all__ = ("ObStoreBackend",)


@mypyc_attr(allow_interpreted_subclasses=True)
class ObStoreBackend:
    """Object storage backend using obstore.

    Implements ObjectStoreProtocol using obstore's Rust-based implementation
    for storage operations. Supports AWS S3, Google Cloud Storage, Azure Blob Storage,
    local filesystem, and HTTP endpoints.

    All synchronous methods use the *_sync suffix for consistency with async methods.

    Implementation Details & Invariants:
        - LocalStore Paths: For LocalStore, the base_path is already included in the store
          root (combined with the URI path; if base_path is absolute, Path division will
          use it directly). Hence, we use an empty prefix when resolving paths for LocalStore,
          whereas cloud stores use base_path as a prefix.
        - Native Streaming: Uses obstore's native streaming yielding Buffer objects, which
          are converted to bytes.
        - Seekable Streams: PyArrow's ParquetFile reads through obstore's seekable
          ``open_reader`` interface without draining the object into memory.
        - Thread Offloading: Uses async_() with a storage limiter to offload blocking
          PyArrow serialization/parsing to a thread pool, preventing event loop blocking.
    """

    __slots__ = ("_is_local_store", "_local_store_root", "base_path", "protocol", "store", "store_options", "store_uri")

    backend_type: ClassVar[str] = "obstore"

    def __init__(self, uri: str, **kwargs: Any) -> None:
        """Initialize obstore backend.

        Args:
            uri: Storage URI. Supported formats:
            - file:///absolute/path - Local filesystem
            - s3://bucket/prefix - AWS S3
            - gs://bucket/prefix - Google Cloud Storage
            - az://container/prefix - Azure Blob Storage
            - memory:// - In-memory storage (for testing)
            **kwargs: Additional options:
            - base_path (str): For local files (file://), this is combined with
            the URI path to form the storage root. For example:
            uri="file:///data" + base_path="uploads" → /data/uploads
                If base_path is absolute, it overrides the URI path (backward compat).
                For cloud storage, base_path is used as an object key prefix.
            - Other obstore configuration options (timeouts, credentials, etc.)
        """
        ensure_obstore()
        obs_store = import_obstore_store()
        base_path = kwargs.pop("base_path", "")

        self.store_uri = uri
        self.base_path = base_path.rstrip("/") if base_path else ""
        self.store_options = kwargs
        self.store: ObjectStore | Any
        self._is_local_store = False
        self._local_store_root = ""
        self.protocol = uri.split("://", 1)[0] if "://" in uri else "file"
        try:
            if uri.startswith("memory://"):
                self.store = obs_store.MemoryStore()
            elif uri.startswith("file://"):
                path_str = parse_file_uri_path(uri)
                path_obj = Path(path_str)

                if is_file_destination(path_obj):
                    path_str = str(path_obj.parent)

                local_store_root_obj = Path(path_str)
                if self.base_path:
                    local_store_root_obj /= self.base_path

                self._is_local_store = True
                self._local_store_root = str(local_store_root_obj.resolve())
                self.store = obs_store.LocalStore(self._local_store_root, mkdir=True)
            else:
                self.store = obs_store.from_url(uri, **kwargs)

            log_storage_event(
                "storage.backend.ready",
                backend_type=self.backend_type,
                protocol=self.protocol,
                operation="init",
                mode="sync",
                path=uri,
            )

        except Exception as exc:
            msg = f"Failed to initialize obstore backend for {uri}"
            raise StorageOperationFailedError(msg) from exc

    @property
    def supports_signing(self) -> bool:
        """Whether this backend supports URL signing.

        Only S3, GCS, and Azure backends support pre-signed URLs.
        Local file storage does not support URL signing.

        Returns:
            True if the protocol supports signing, False otherwise.
        """
        return self.protocol in _SIGNABLE_PROTOCOLS

    @classmethod
    def from_config(cls, config: "dict[str, Any]") -> "ObStoreBackend":
        """Create backend from configuration dictionary."""
        store_uri = config["store_uri"]
        base_path = config.get("base_path", "")
        store_options = config.get("store_options", {})

        kwargs = dict(store_options)
        if base_path:
            kwargs["base_path"] = base_path

        return cls(uri=store_uri, **kwargs)

    def resolve_uri(self, path: "str | Path") -> str:
        """Resolve a backend-relative path to an unsigned address.

        Args:
            path: The same backend-relative path accepted by read and write methods.

        Returns:
            An absolute filesystem path for local stores or a protocol-qualified
            URI for remote stores. The target does not need to exist.
        """
        resolved_path = self._resolve_path(path)
        if getattr(self, "_is_local_store", False):
            return str((Path(self._local_store_root) / resolved_path).resolve())

        parsed = urlparse(self.store_uri)
        joined_path = "/".join(part.strip("/") for part in (parsed.path, resolved_path) if part.strip("/"))
        authority = f"{parsed.scheme}://{parsed.netloc}"
        address = f"{authority}/{joined_path}" if parsed.netloc else f"{authority}{joined_path}"
        if parsed.query:
            address = f"{address}?{parsed.query}"
        if parsed.fragment:
            address = f"{address}#{parsed.fragment}"
        return address

    def read_bytes_sync(self, path: "str | Path", **kwargs: Any) -> bytes:
        """Read bytes using obstore synchronously."""
        _ = kwargs
        resolved_path = self._resolve_path(path)
        return self._read_bytes_resolved_sync(resolved_path)

    def write_bytes_sync(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:
        """Write bytes using obstore synchronously."""
        _ = kwargs
        resolved_path = self._resolve_path(path)
        self._write_bytes_resolved_sync(resolved_path, data)

    def read_text_sync(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text using obstore synchronously."""
        return self.read_bytes_sync(path, **kwargs).decode(encoding)

    def write_text_sync(self, path: "str | Path", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text using obstore synchronously."""
        self.write_bytes_sync(path, data.encode(encoding), **kwargs)

    def list_objects_sync(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
        """List objects using obstore synchronously."""
        _ = kwargs
        resolved_prefix = self._resolve_list_prefix(prefix)
        paths = self._list_resolved_sync(resolved_prefix, recursive)
        log_storage_event(
            "storage.list",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="list_objects",
            mode="sync",
            path=resolved_prefix,
            count=len(paths),
        )
        return paths

    def exists_sync(self, path: "str | Path", **kwargs: Any) -> bool:
        """Check if object exists using obstore synchronously."""
        _ = kwargs
        resolved_path = self._resolve_path(path)
        try:
            self.store.head(resolved_path)  # pyright: ignore[reportUnknownMemberType]
        except Exception:
            log_storage_event(
                "storage.read",
                backend_type=self.backend_type,
                protocol=self.protocol,
                operation="exists",
                mode="sync",
                path=str(path),
                exists=False,
            )
            return False
        log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="exists",
            mode="sync",
            path=resolved_path,
            exists=True,
        )
        return True

    def delete_sync(self, path: "str | Path", **kwargs: Any) -> None:
        """Delete object using obstore synchronously."""
        _ = kwargs
        resolved_path = self._resolve_path(path)
        execute_sync_storage_operation(
            partial(self.store.delete, resolved_path), backend=self.backend_type, operation="delete", path=resolved_path
        )
        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="delete",
            mode="sync",
            path=resolved_path,
        )

    def copy_sync(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Copy object using obstore synchronously."""
        _ = kwargs
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        execute_sync_storage_operation(
            partial(self.store.copy, source_path, dest_path),
            backend=self.backend_type,
            operation="copy",
            path=f"{source_path}->{dest_path}",
        )
        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="copy",
            mode="sync",
            source_path=source_path,
            destination_path=dest_path,
        )

    def move_sync(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Move object using obstore synchronously."""
        _ = kwargs
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        execute_sync_storage_operation(
            partial(self.store.rename, source_path, dest_path),
            backend=self.backend_type,
            operation="move",
            path=f"{source_path}->{dest_path}",
        )
        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="move",
            mode="sync",
            source_path=source_path,
            destination_path=dest_path,
        )

    def glob_sync(self, pattern: str, **kwargs: Any) -> "list[str]":
        """Find objects matching pattern synchronously.

        Lists objects under the pattern's static directory prefix and filters
        them client-side. ``*`` and ``?`` match
        within one path segment and ``**`` spans zero or more segments, matching
        the local and fsspec backends.
        """
        _ = kwargs
        if not pattern:
            return []
        reject_parent_traversal(pattern)
        if getattr(self, "_is_local_store", False):
            pattern_path = Path(pattern)
            if pattern_path.is_absolute() and self._local_store_root:
                try:
                    resolved_pattern = pattern_path.relative_to(Path(self._local_store_root)).as_posix()
                except ValueError as exc:
                    raise StoragePathTraversalError(pattern, self._local_store_root) from exc
            else:
                resolved_pattern = pattern
        else:
            resolved_pattern = resolve_storage_path(pattern, self.base_path, self.protocol, strip_file_scheme=True)
        all_objects = self._list_resolved_sync(extract_glob_static_prefix(resolved_pattern), recursive=True)
        matcher = glob_to_regex(resolved_pattern)
        results = [obj for obj in all_objects if matcher.match(obj)]
        log_storage_event(
            "storage.list",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="glob",
            mode="sync",
            path=resolved_pattern,
            count=len(results),
        )
        return results

    def get_metadata_sync(self, path: "str | Path", **kwargs: Any) -> "dict[str, object]":
        """Get object metadata using obstore synchronously."""
        _ = kwargs
        resolved_path = self._resolve_path(path)

        try:
            metadata = self.store.head(resolved_path)
        except Exception:
            return {"path": resolved_path, "exists": False}
        else:
            result = {
                "path": resolved_path,
                "exists": True,
                "size": metadata.get("size"),
                "last_modified": metadata.get("last_modified"),
                "e_tag": metadata.get("e_tag"),
                "version": metadata.get("version"),
            }
            metadata_dict = cast("dict[str, Any]", metadata)
            if custom_metadata := metadata_dict.get("metadata"):
                result["custom_metadata"] = custom_metadata
            return result

    def is_object_sync(self, path: "str | Path") -> bool:
        """Check if path is an object using obstore synchronously."""
        resolved_path = self._resolve_path(path)
        return self.exists_sync(path) and not str(path).endswith("/") and not resolved_path.endswith("/")

    def is_path_sync(self, path: "str | Path") -> bool:
        """Check if path is a prefix/directory using obstore synchronously."""
        resolved_path = self._resolve_path(path)

        if str(path).endswith("/") or resolved_path.endswith("/"):
            return True

        try:
            objects = self.list_objects_sync(prefix=str(path), recursive=True)
            return len(objects) > 0
        except Exception:
            return False

    def read_arrow_sync(self, path: "str | Path", **kwargs: Any) -> "ArrowTable":
        """Read Arrow table using obstore synchronously."""
        pq = import_pyarrow_parquet()
        resolved_path = self._resolve_path(path)
        data = self._read_bytes_resolved_sync(resolved_path)
        result = cast(
            "ArrowTable",
            execute_sync_storage_operation(
                partial(pq.read_table, io.BytesIO(data), **kwargs),
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
            mode="sync",
            path=resolved_path,
        )
        return result

    def write_arrow_sync(self, path: "str | Path", table: "ArrowTable", **kwargs: Any) -> None:
        """Write Arrow table using obstore synchronously."""
        import_pyarrow()
        import_pyarrow_parquet()
        resolved_path = self._resolve_path(path)

        execute_sync_storage_operation(
            partial(self._stream_parquet_sync, resolved_path, table, **kwargs),
            backend=self.backend_type,
            operation="write_arrow",
            path=resolved_path,
        )
        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_arrow",
            mode="sync",
            path=resolved_path,
        )

    def stream_read_sync(self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any) -> Iterator[bytes]:
        """Stream bytes using obstore's native streaming synchronously.

        Uses obstore's sync streaming iterator which yields chunks without
        loading the entire file into memory, for both local and remote backends.

        Yields:
            Chunks of bytes from the file, with size determined by chunk_size (default: 65536 bytes).
        """
        _ = kwargs
        resolved_path = self._resolve_path(path)
        chunk_size = chunk_size or 65536

        result = execute_sync_storage_operation(
            partial(self.store.get, resolved_path),
            backend=self.backend_type,
            operation="stream_read",
            path=resolved_path,
        )

        for chunk in result.stream(min_chunk_size=chunk_size):
            yield bytes(chunk)

    def stream_arrow_sync(
        self, pattern: str, *, file_format: Literal["parquet"] = "parquet", batch_size: int = 65_536, **kwargs: Any
    ) -> "Iterator[ArrowRecordBatch]":
        """Stream Arrow record batches using obstore's native streaming synchronously.

        For each matching file, PyArrow reads through obstore's seekable reader.

        Yields:
            Arrow record batches in file and row-group order.
        """
        obs = import_obstore()

        validate_parquet_stream_options(pattern, file_format, batch_size)
        pq = import_pyarrow_parquet()
        read_buffer_bytes = kwargs.pop("read_buffer_bytes", 16_777_216)
        arrow_schema = kwargs.pop("arrow_schema", None)
        for obj_path in self.glob_sync(pattern):
            reader = execute_sync_storage_operation(
                partial(_open_obstore_reader, obs, self.store, obj_path, read_buffer_bytes),
                backend=self.backend_type,
                operation="stream_open",
                path=obj_path,
            )
            with _ObStoreFileProxy(reader) as stream:
                parquet_file = execute_sync_storage_operation(
                    partial(pq.ParquetFile, stream), backend=self.backend_type, operation="stream_arrow", path=obj_path
                )
                yield from iter_parquet_row_groups(
                    parquet_file, batch_size=batch_size, arrow_schema=arrow_schema, **kwargs
                )

    @overload
    def sign_sync(self, paths: str, expires_in: int = 3600, for_upload: bool = False) -> str: ...
    @overload
    def sign_sync(self, paths: "list[str]", expires_in: int = 3600, for_upload: bool = False) -> "list[str]": ...
    def sign_sync(
        self, paths: "str | list[str]", expires_in: int = 3600, for_upload: bool = False
    ) -> "str | list[str]":
        """Generate signed URL(s) for the object(s).

        Args:
            paths: Single object path or list of paths to sign.
            expires_in: URL expiration time in seconds (default: 3600, max: 604800 = 7 days).
            for_upload: Whether the URL is for upload (PUT) vs download (GET).

        Returns:
            Single signed URL string if paths is a string, or list of signed URLs
            if paths is a list. Preserves input type for convenience.
        """
        obs = import_obstore()

        method, expires_delta, resolved_paths, is_single = self._prepare_sign_request(paths, expires_in, for_upload)

        try:
            signed_urls: list[str] = obs.sign(self.store, method, resolved_paths, expires_delta)
            return signed_urls[0] if is_single else signed_urls
        except Exception as exc:
            msg = f"Failed to generate signed URL(s) for {resolved_paths}"
            raise StorageOperationFailedError(msg) from exc

    async def read_bytes_async(self, path: "str | Path", **kwargs: Any) -> bytes:
        """Read bytes from storage asynchronously."""
        _ = kwargs
        resolved_path = self._resolve_path(path)
        return await self._read_bytes_resolved_async(resolved_path)

    async def write_bytes_async(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:
        """Write bytes to storage asynchronously."""
        _ = kwargs
        resolved_path = self._resolve_path(path)
        await self._write_bytes_resolved_async(resolved_path, data)

    async def stream_read_async(
        self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        """Stream bytes from storage asynchronously.

        Uses obstore's native async streaming to yield chunks of bytes
        without buffering the entire file into memory.
        """
        _ = kwargs
        resolved_path = self._resolve_path(path)
        effective_chunk_size = chunk_size or 65536

        result = await execute_async_storage_operation(
            partial(self.store.get_async, resolved_path),
            backend=self.backend_type,
            operation="stream_read",
            path=resolved_path,
        )
        try:
            stream = result.stream(min_chunk_size=effective_chunk_size)
        except TypeError:
            stream = result.stream()
        return AsyncObStoreStreamIterator(stream, effective_chunk_size)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":
        """List objects in storage asynchronously."""
        _ = kwargs
        resolved_prefix = self._resolve_list_prefix(prefix)

        results = await execute_async_storage_operation(
            partial(self._list_resolved_async, resolved_prefix, recursive),
            backend=self.backend_type,
            operation="list_objects",
            path=resolved_prefix,
        )
        log_storage_event(
            "storage.list",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="list_objects",
            mode="async",
            path=resolved_prefix,
            count=len(results),
        )
        return results

    async def read_text_async(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from storage asynchronously."""
        data = await self.read_bytes_async(path, **kwargs)
        return data.decode(encoding)

    async def write_text_async(self, path: "str | Path", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to storage asynchronously."""
        encoded_data = data.encode(encoding)
        await self.write_bytes_async(path, encoded_data, **kwargs)

    async def exists_async(self, path: "str | Path", **kwargs: Any) -> bool:
        """Check if object exists in storage asynchronously."""
        _ = kwargs
        resolved_path = self._resolve_path(path)

        try:
            await self.store.head_async(resolved_path)
        except Exception:
            log_storage_event(
                "storage.read",
                backend_type=self.backend_type,
                protocol=self.protocol,
                operation="exists",
                mode="async",
                path=str(path),
                exists=False,
            )
            return False
        log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="exists",
            mode="async",
            path=resolved_path,
            exists=True,
        )
        return True

    async def delete_async(self, path: "str | Path", **kwargs: Any) -> None:
        """Delete object from storage asynchronously."""
        _ = kwargs
        resolved_path = self._resolve_path(path)

        await execute_async_storage_operation(
            partial(self.store.delete_async, resolved_path),
            backend=self.backend_type,
            operation="delete",
            path=resolved_path,
        )
        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="delete",
            mode="async",
            path=resolved_path,
        )

    async def copy_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Copy object in storage asynchronously."""
        _ = kwargs
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)

        await execute_async_storage_operation(
            partial(self.store.copy_async, source_path, dest_path),
            backend=self.backend_type,
            operation="copy",
            path=f"{source_path}->{dest_path}",
        )
        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="copy",
            mode="async",
            source_path=source_path,
            destination_path=dest_path,
        )

    async def move_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Move object in storage asynchronously."""
        _ = kwargs
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)

        await execute_async_storage_operation(
            partial(self.store.rename_async, source_path, dest_path),
            backend=self.backend_type,
            operation="move",
            path=f"{source_path}->{dest_path}",
        )
        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="move",
            mode="async",
            source_path=source_path,
            destination_path=dest_path,
        )

    async def get_metadata_async(self, path: "str | Path", **kwargs: Any) -> "dict[str, object]":
        """Get object metadata from storage asynchronously."""
        _ = kwargs
        resolved_path = self._resolve_path(path)

        result: dict[str, object] = {}
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
            metadata_dict = cast("dict[str, Any]", metadata)
            if custom_metadata := metadata_dict.get("metadata"):
                result["custom_metadata"] = custom_metadata

        except Exception:
            return {"path": resolved_path, "exists": False}
        else:
            return result

    async def read_arrow_async(self, path: "str | Path", **kwargs: Any) -> "ArrowTable":
        """Read Arrow table from storage asynchronously.

        Uses async_() with storage limiter to offload blocking PyArrow I/O to thread pool.
        """
        pq = import_pyarrow_parquet()
        resolved_path = self._resolve_path(path)
        data = await self._read_bytes_resolved_async(resolved_path)

        result = await execute_async_storage_operation(
            partial(async_(pq.read_table), io.BytesIO(data), **kwargs),
            backend=self.backend_type,
            operation="read_arrow",
            path=resolved_path,
        )

        log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="read_arrow",
            mode="async",
            path=resolved_path,
        )
        return cast("ArrowTable", result)

    async def write_arrow_async(self, path: "str | Path", table: "ArrowTable", **kwargs: Any) -> None:
        """Write Arrow table to storage asynchronously.

        Uses async_() with storage limiter to offload blocking PyArrow serialization
        to thread pool, preventing event loop blocking.
        """
        import_pyarrow()
        import_pyarrow_parquet()
        resolved_path = self._resolve_path(path)

        await execute_async_storage_operation(
            partial(async_(self._stream_parquet_sync), resolved_path, table, **kwargs),
            backend=self.backend_type,
            operation="write_arrow",
            path=resolved_path,
        )

        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_arrow",
            mode="async",
            path=resolved_path,
        )

    def stream_arrow_async(
        self, pattern: str, *, file_format: Literal["parquet"] = "parquet", batch_size: int = 65_536, **kwargs: Any
    ) -> AsyncIterator["ArrowRecordBatch"]:
        """Stream Arrow record batches from storage asynchronously.

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
        """Generate signed URL(s) asynchronously.

        Args:
            paths: Single object path or list of paths to sign.
            expires_in: URL expiration time in seconds (default: 3600, max: 604800 = 7 days).
            for_upload: Whether the URL is for upload (PUT) vs download (GET).

        Returns:
            Single signed URL string if paths is a string, or list of signed URLs
            if paths is a list. Preserves input type for convenience.
        """
        obs = import_obstore()

        method, expires_delta, resolved_paths, is_single = self._prepare_sign_request(paths, expires_in, for_upload)

        try:
            signed_urls: list[str] = await obs.sign_async(self.store, method, resolved_paths, expires_delta)
            return signed_urls[0] if is_single else signed_urls
        except Exception as exc:
            msg = f"Failed to generate signed URL(s) for {resolved_paths}"
            raise StorageOperationFailedError(msg) from exc

    def _resolve_path(self, path: "str | Path") -> str:
        if getattr(self, "_is_local_store", False):
            return self._local_store_path(path)
        return resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

    def _resolve_list_prefix(self, prefix: str) -> str:
        """Resolve a caller-supplied listing prefix to a store-relative one.

        A local store's root already contains ``base_path``, so resolving the
        prefix against ``base_path`` again would look for that segment twice and
        match nothing.

        Args:
            prefix: Caller-supplied listing prefix, possibly empty.

        Returns:
            The prefix as the store expects it, or ``""`` to list everything.
        """
        is_local = getattr(self, "_is_local_store", False)
        if not prefix:
            return "" if is_local else (self.base_path or "")
        if is_local:
            return self._local_store_path(prefix)
        return resolve_storage_path(prefix, self.base_path, self.protocol, strip_file_scheme=True)

    def _local_store_path(self, path: "str | Path") -> str:
        """Resolve path for LocalStore, which expects relative paths from its root.

        Args:
            path: Caller-supplied storage path.

        Returns:
            The path relative to the store root.

        Raises:
            StoragePathTraversalError: If the path resolves outside the store root.
        """
        if not getattr(self, "_local_store_root", ""):
            reject_parent_traversal(path)
            return str(path)

        return ensure_path_within_root(path, self._local_store_root)

    def _read_bytes_resolved_sync(self, resolved_path: str) -> bytes:
        result = execute_sync_storage_operation(
            partial(_read_obstore_bytes, self.store, resolved_path),
            backend=self.backend_type,
            operation="read_bytes",
            path=resolved_path,
        )
        log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="read_bytes",
            mode="sync",
            path=resolved_path,
        )
        return result

    def _write_bytes_resolved_sync(self, resolved_path: str, data: bytes) -> None:
        execute_sync_storage_operation(
            partial(self.store.put, resolved_path, data),
            backend=self.backend_type,
            operation="write_bytes",
            path=resolved_path,
        )
        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_bytes",
            mode="sync",
            path=resolved_path,
        )

    def _list_resolved_sync(self, resolved_prefix: str, recursive: bool) -> "list[str]":
        """List object keys under a prefix that is already store-relative.

        Args:
            resolved_prefix: Prefix as the store expects it; ``""`` lists everything.
            recursive: Whether to descend into nested prefixes.

        Returns:
            Sorted object keys.
        """
        if not recursive:
            result = execute_sync_storage_operation(
                partial(self.store.list_with_delimiter, resolved_prefix),
                backend=self.backend_type,
                operation="list_objects",
                path=resolved_prefix,
            )
            return sorted(item["path"] for item in result["objects"])
        batches = execute_sync_storage_operation(
            partial(self.store.list, resolved_prefix),
            backend=self.backend_type,
            operation="list_objects",
            path=resolved_prefix,
        )
        return sorted(item["path"] for batch in batches for item in batch)

    async def _list_resolved_async(self, resolved_prefix: str, recursive: bool) -> "list[str]":
        if not recursive and hasattr(self.store, "list_with_delimiter_async"):
            result = await self.store.list_with_delimiter_async(resolved_prefix)
            return sorted(item["path"] for item in result["objects"])
        objects: list[str] = []
        async for batch in self.store.list_async(resolved_prefix):  # pyright: ignore[reportAttributeAccessIssue]
            objects.extend(item["path"] for item in batch)
        if not recursive:
            base_depth = resolved_prefix.rstrip("/").count("/") + (1 if resolved_prefix.strip("/") else 0)
            objects = [obj for obj in objects if obj.count("/") < base_depth]
        return sorted(objects)

    def _stream_parquet_sync(self, resolved_path: str, table: "ArrowTable", **kwargs: Any) -> None:
        """Serialize a table row group by row group into an obstore multipart writer.

        Peak memory is bounded by one serialized row group plus the upload
        buffer rather than the serialized size of the whole table. The writer
        and the sink are closed only after every row group is written; a
        failure leaves the multipart upload unfinished, and it is discarded
        when the writer is released, so no partial object is published.

        Args:
            resolved_path: Store-relative destination key.
            table: Table to serialize.
            **kwargs: Options forwarded to ``pyarrow.parquet.ParquetWriter``;
                ``row_group_size`` is forwarded to ``write_table``.
        """
        obs = import_obstore()

        pa = import_pyarrow()
        pq = import_pyarrow_parquet()
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

        row_group_size = kwargs.pop("row_group_size", None)
        if "use_dictionary" not in kwargs:
            kwargs["use_dictionary"] = resolve_parquet_use_dictionary(table.schema)
        sink = obs.open_writer(self.store, resolved_path)
        sink_adapter = _ObstoreSink(sink)
        writer: Any = None
        try:
            writer = pq.ParquetWriter(pa.PythonFile(sink_adapter, mode="w"), table.schema, **kwargs)
            writer.write_table(table, row_group_size=row_group_size)
            writer.close()
            sink_adapter.close()
        except BaseException:
            sink_adapter.abort()
            if writer is not None:
                with contextlib.suppress(Exception):
                    writer.close()
            if getattr(self, "_is_local_store", False):
                with contextlib.suppress(Exception):
                    sink.close()
                if getattr(self, "_local_store_root", ""):
                    with contextlib.suppress(Exception):
                        (Path(self._local_store_root) / resolved_path).unlink(missing_ok=True)
            raise

    def _prepare_sign_request(
        self, paths: "str | list[str]", expires_in: int, for_upload: bool
    ) -> "tuple[str, timedelta, list[str], bool]":
        if self.protocol not in _SIGNABLE_PROTOCOLS:
            msg = (
                f"URL signing is not supported for protocol '{self.protocol}'. "
                f"Only S3, GCS, and Azure backends support pre-signed URLs."
            )
            raise NotImplementedError(msg)

        if expires_in > _MAX_SIGN_EXPIRES_SECONDS:
            msg = f"expires_in cannot exceed {_MAX_SIGN_EXPIRES_SECONDS} seconds (7 days), got {expires_in}"
            raise ValueError(msg)

        method = "PUT" if for_upload else "GET"
        expires_delta = timedelta(seconds=expires_in)
        if isinstance(paths, str):
            path_list = [paths]
            is_single = True
        else:
            path_list = list(paths)
            is_single = False

        resolved_paths = [self._resolve_path(path) for path in path_list]
        return method, expires_delta, resolved_paths, is_single

    async def _read_bytes_resolved_async(self, resolved_path: str) -> bytes:
        data = await execute_async_storage_operation(
            partial(_read_obstore_bytes_async, self.store, resolved_path),
            backend=self.backend_type,
            operation="read_bytes",
            path=resolved_path,
        )
        log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="read_bytes",
            mode="async",
            path=resolved_path,
        )
        return data

    async def _write_bytes_resolved_async(self, resolved_path: str, data: bytes) -> None:
        await execute_async_storage_operation(
            partial(self.store.put_async, resolved_path, data),
            backend=self.backend_type,
            operation="write_bytes",
            path=resolved_path,
        )
        log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_bytes",
            mode="async",
            path=resolved_path,
        )


async def _read_obstore_bytes_async(store: Any, resolved_path: str) -> bytes:
    """Read bytes via obstore asynchronously."""
    result = await store.get_async(resolved_path)
    bytes_obj = await result.bytes_async()
    return cast("bytes", bytes_obj.to_bytes())


def _open_obstore_reader(obs: Any, store: Any, obj_path: str, read_buffer_bytes: int | None) -> Any:
    if read_buffer_bytes is not None:
        try:
            return obs.open_reader(store, obj_path, buffer_size=int(read_buffer_bytes))
        except TypeError:
            pass
    return obs.open_reader(store, obj_path)


def _read_obstore_bytes(store: Any, resolved_path: str) -> bytes:
    """Read bytes via obstore."""
    result = store.get(resolved_path)
    return cast("bytes", result.bytes().to_bytes())


class _ObStoreFileProxy:
    """Complete obstore's seekable reader interface for PyArrow."""

    __slots__ = ("_closed", "_reader")

    def __init__(self, reader: Any) -> None:
        self._reader = reader
        self._closed = False

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def readable(self) -> bool:
        return not self._closed

    def seekable(self) -> bool:
        return not self._closed and bool(self._reader.seekable())

    def writable(self) -> bool:
        return False

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            return cast("bytes", self._reader.readall())
        return cast("bytes", self._reader.read(size))

    def readinto(self, buffer: Any) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def seek(self, offset: int, whence: int = 0) -> int:
        return cast("int", self._reader.seek(offset, whence))

    def tell(self) -> int:
        return cast("int", self._reader.tell())

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._reader.close()


class _ObstoreSink:
    """Adapt an obstore writer to the file-like surface ``pyarrow.PythonFile`` expects.

    obstore exposes ``closed`` as a method; pyarrow reads it as an attribute.
    """

    __slots__ = ("_aborted", "_writer")

    def __init__(self, writer: Any) -> None:
        self._writer = writer
        self._aborted = False

    @property
    def closed(self) -> bool:
        return self._aborted or bool(self._writer.closed())

    def abort(self) -> None:
        self._aborted = True
        abort_fn = getattr(self._writer, "abort", None)
        if callable(abort_fn):
            with contextlib.suppress(Exception):
                abort_fn()

    def write(self, data: Any) -> int:
        payload = bytes(data) if isinstance(data, (bytearray, memoryview)) else data
        return int(self._writer.write(payload))

    def flush(self) -> None:
        if not self._aborted:
            self._writer.flush()

    def close(self) -> None:
        if not self._aborted:
            self._writer.close()
