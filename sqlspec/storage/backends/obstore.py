"""Object storage backend using obstore.

Implements the ObjectStoreProtocol using obstore for S3, GCS, Azure,
and local file storage.
"""

import fnmatch
import io
import re
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Any, ClassVar, Final, cast, overload
from urllib.parse import urlparse

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import StorageOperationFailedError
from sqlspec.storage._paths import resolve_storage_path
from sqlspec.storage._utils import _log_storage_event, import_pyarrow, import_pyarrow_parquet
from sqlspec.storage.backends.base import AsyncArrowBatchIterator, AsyncObStoreStreamIterator
from sqlspec.storage.errors import execute_sync_storage_operation
from sqlspec.typing import ArrowRecordBatch, ArrowTable
from sqlspec.utils.module_loader import ensure_obstore
from sqlspec.utils.sync_tools import async_

DEFAULT_OPTIONS: Final[dict[str, Any]] = {"connect_timeout": "30s", "request_timeout": "60s"}
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
    """

    __slots__ = (
        "_is_local_store",
        "_local_store_root",
        "_path_cache",
        "base_path",
        "protocol",
        "store",
        "store_options",
        "store_uri",
    )

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
        base_path = kwargs.pop("base_path", "")

        self.store_uri = uri
        self.base_path = base_path.rstrip("/") if base_path else ""
        self.store_options = kwargs
        self.store: Any
        self._path_cache: dict[str, str] = {}
        self._is_local_store = False
        self._local_store_root = ""
        self.protocol = uri.split("://", 1)[0] if "://" in uri else "file"
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

                # Combine URI path with base_path for correct storage location
                # If base_path is absolute, Path division will use it directly (backward compat)
                local_store_root_obj = Path(path_str)
                if self.base_path:
                    local_store_root_obj /= self.base_path

                self._is_local_store = True
                self._local_store_root = str(local_store_root_obj.resolve())
                self.store = LocalStore(self._local_store_root, mkdir=True)
            else:
                from obstore.store import from_url

                self.store = from_url(uri, **kwargs)  # pyright: ignore[reportAttributeAccessIssue]

            _log_storage_event(
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
    def is_local_store(self) -> bool:
        """Return whether the backend uses local storage."""
        return self._is_local_store

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

    def _resolve_path(self, path: "str | Path") -> str:
        if self._is_local_store:
            return self._resolve_path_for_local_store(path)
        return resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

    def _resolve_path_for_local_store(self, path: "str | Path") -> str:
        """Resolve path for LocalStore which expects relative paths from its root."""

        path_obj = Path(str(path))

        if path_obj.is_absolute() and self._local_store_root:
            try:
                rel = path_obj.relative_to(self._local_store_root)
                return "" if str(rel) == "." else str(rel)
            except ValueError:
                return str(path).lstrip("/")

        return str(path)

    def _read_bytes_resolved_sync(self, resolved_path: str) -> bytes:
        result = execute_sync_storage_operation(
            partial(_read_obstore_bytes, self.store, resolved_path),
            backend=self.backend_type,
            operation="read_bytes",
            path=resolved_path,
        )
        _log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="read_bytes",
            mode="sync",
            path=resolved_path,
        )
        return result

    def read_bytes_sync(self, path: "str | Path", **kwargs: Any) -> bytes:  # pyright: ignore[reportUnusedParameter]
        """Read bytes using obstore synchronously."""
        resolved_path = self._resolve_path(path)
        return self._read_bytes_resolved_sync(resolved_path)

    def _write_bytes_resolved_sync(self, resolved_path: str, data: bytes) -> None:
        execute_sync_storage_operation(
            partial(self.store.put, resolved_path, data),
            backend=self.backend_type,
            operation="write_bytes",
            path=resolved_path,
        )
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_bytes",
            mode="sync",
            path=resolved_path,
        )

    def write_bytes_sync(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write bytes using obstore synchronously."""
        resolved_path = self._resolve_path(path)
        self._write_bytes_resolved_sync(resolved_path, data)

    def read_text_sync(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text using obstore synchronously."""
        return self.read_bytes_sync(path, **kwargs).decode(encoding)

    def write_text_sync(self, path: "str | Path", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text using obstore synchronously."""
        self.write_bytes_sync(path, data.encode(encoding), **kwargs)

    def list_objects_sync(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":  # pyright: ignore[reportUnusedParameter]
        """List objects using obstore synchronously."""
        # For LocalStore, the base_path is already included in the store root,
        # so we use empty prefix when none is given. For cloud stores, use base_path.
        if prefix:
            resolved_prefix = resolve_storage_path(prefix, self.base_path, self.protocol, strip_file_scheme=True)
        elif self._is_local_store:
            resolved_prefix = ""
        else:
            resolved_prefix = self.base_path or ""
        if not recursive:
            result = self.store.list_with_delimiter(resolved_prefix)
            paths = sorted(item["path"] for item in result["objects"])
        else:
            paths = sorted(item["path"] for batch in self.store.list(resolved_prefix) for item in batch)
        _log_storage_event(
            "storage.list",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="list_objects",
            mode="sync",
            path=resolved_prefix,
            count=len(paths),
        )
        return paths

    def exists_sync(self, path: "str | Path", **kwargs: Any) -> bool:  # pyright: ignore[reportUnusedParameter]
        """Check if object exists using obstore synchronously."""
        try:
            resolved_path = self._resolve_path(path)
            self.store.head(resolved_path)  # pyright: ignore[reportUnknownMemberType]
        except Exception:
            _log_storage_event(
                "storage.read",
                backend_type=self.backend_type,
                protocol=self.protocol,
                operation="exists",
                mode="sync",
                path=str(path),
                exists=False,
            )
            return False
        _log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="exists",
            mode="sync",
            path=resolved_path,
            exists=True,
        )
        return True

    def delete_sync(self, path: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Delete object using obstore synchronously."""
        resolved_path = self._resolve_path(path)
        execute_sync_storage_operation(
            partial(self.store.delete, resolved_path), backend=self.backend_type, operation="delete", path=resolved_path
        )
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="delete",
            mode="sync",
            path=resolved_path,
        )

    def copy_sync(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Copy object using obstore synchronously."""
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        execute_sync_storage_operation(
            partial(self.store.copy, source_path, dest_path),
            backend=self.backend_type,
            operation="copy",
            path=f"{source_path}->{dest_path}",
        )
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="copy",
            mode="sync",
            source_path=source_path,
            destination_path=dest_path,
        )

    def move_sync(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Move object using obstore synchronously."""
        source_path = self._resolve_path(source)
        dest_path = self._resolve_path(destination)
        execute_sync_storage_operation(
            partial(self.store.rename, source_path, dest_path),
            backend=self.backend_type,
            operation="move",
            path=f"{source_path}->{dest_path}",
        )
        _log_storage_event(
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

        Lists all objects and filters them client-side using the pattern.
        """

        resolved_pattern = resolve_storage_path(pattern, self.base_path, self.protocol, strip_file_scheme=True)
        all_objects = self.list_objects_sync(recursive=True, **kwargs)

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
            results = matching_objects
        else:
            results = [obj for obj in all_objects if fnmatch.fnmatch(obj, resolved_pattern)]
        _log_storage_event(
            "storage.list",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="glob",
            mode="sync",
            path=resolved_pattern,
            count=len(results),
        )
        return results

    def get_metadata_sync(self, path: "str | Path", **kwargs: Any) -> "dict[str, object]":  # pyright: ignore[reportUnusedParameter]
        """Get object metadata using obstore synchronously."""
        resolved_path = self._resolve_path(path)

        # Keep in sync with get_metadata_async.
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
            if metadata.get("metadata"):
                result["custom_metadata"] = metadata["metadata"]
            return result

    def is_object_sync(self, path: "str | Path") -> bool:
        """Check if path is an object using obstore synchronously."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)
        return self.exists_sync(path) and not resolved_path.endswith("/")

    def is_path_sync(self, path: "str | Path") -> bool:
        """Check if path is a prefix/directory using obstore synchronously."""
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        if resolved_path.endswith("/"):
            return True

        try:
            objects = self.list_objects_sync(prefix=str(path), recursive=True)
            return len(objects) > 0
        except Exception:
            return False

    def read_arrow_sync(self, path: "str | Path", **kwargs: Any) -> ArrowTable:
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
        _log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="read_arrow",
            mode="sync",
            path=resolved_path,
        )
        return result

    def write_arrow_sync(self, path: "str | Path", table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table using obstore synchronously."""
        pa = import_pyarrow()
        pq = import_pyarrow_parquet()
        resolved_path = self._resolve_path(path)

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
            partial(pq.write_table, table, buffer, **kwargs),
            backend=self.backend_type,
            operation="write_arrow",
            path=resolved_path,
        )
        buffer.seek(0)
        self._write_bytes_resolved_sync(resolved_path, buffer.read())
        _log_storage_event(
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
        resolved_path = self._resolve_path(path)
        chunk_size = chunk_size or 65536

        result = execute_sync_storage_operation(
            partial(self.store.get, resolved_path),
            backend=self.backend_type,
            operation="stream_read",
            path=resolved_path,
        )

        # Use obstore's native streaming - yields Buffer objects
        # GetResult.stream(min_chunk_size) returns an iterator of chunks
        for chunk in result.stream(min_chunk_size=chunk_size):
            yield bytes(chunk)  # Convert Buffer to bytes

    def stream_arrow_sync(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        """Stream Arrow record batches using obstore's native streaming synchronously.

        For each matching file, streams data through a buffered wrapper
        that PyArrow can read directly without loading the entire file.

        Yields:
            Chunks of bytes from the file, with size determined by chunk_size (default: 65536 bytes).
        """
        pq = import_pyarrow_parquet()
        for obj_path in self.glob_sync(pattern, **kwargs):
            resolved_path = resolve_storage_path(obj_path, self.base_path, self.protocol, strip_file_scheme=True)
            result = execute_sync_storage_operation(
                partial(self.store.get, resolved_path),
                backend=self.backend_type,
                operation="stream_arrow",
                path=resolved_path,
            )

            # Create a file-like object that streams from obstore
            # PyArrow's ParquetFile needs a seekable file, so we buffer the stream
            buffer = io.BytesIO()
            for chunk in result.stream():
                buffer.write(chunk)
            buffer.seek(0)

            parquet_file = pq.ParquetFile(buffer)
            yield from parquet_file.iter_batches()

    @property
    def supports_signing(self) -> bool:
        """Whether this backend supports URL signing.

        Only S3, GCS, and Azure backends support pre-signed URLs.
        Local file storage does not support URL signing.

        Returns:
            True if the protocol supports signing, False otherwise.
        """
        return self.protocol in _SIGNABLE_PROTOCOLS

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

        resolved_paths = [
            resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True) for path in path_list
        ]
        return method, expires_delta, resolved_paths, is_single

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
        import obstore as obs

        method, expires_delta, resolved_paths, is_single = self._prepare_sign_request(paths, expires_in, for_upload)

        try:
            signed_urls: list[str] = obs.sign(self.store, method, resolved_paths, expires_delta)  # type: ignore[call-overload]
            return signed_urls[0] if is_single else signed_urls
        except Exception as exc:
            msg = f"Failed to generate signed URL(s) for {resolved_paths}"
            raise StorageOperationFailedError(msg) from exc

    async def _read_bytes_resolved_async(self, resolved_path: str) -> bytes:
        result = await self.store.get_async(resolved_path)
        bytes_obj = await result.bytes_async()  # pyright: ignore[reportAttributeAccessIssue]
        data = cast("bytes", bytes_obj.to_bytes())
        _log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="read_bytes",
            mode="async",
            path=resolved_path,
        )
        return data

    async def read_bytes_async(self, path: "str | Path", **kwargs: Any) -> bytes:  # pyright: ignore[reportUnusedParameter]
        """Read bytes from storage asynchronously."""
        resolved_path = self._resolve_path(path)
        return await self._read_bytes_resolved_async(resolved_path)

    async def _write_bytes_resolved_async(self, resolved_path: str, data: bytes) -> None:
        await self.store.put_async(resolved_path, data)
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_bytes",
            mode="async",
            path=resolved_path,
        )

    async def write_bytes_async(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write bytes to storage asynchronously."""
        resolved_path = self._resolve_path(path)
        await self._write_bytes_resolved_async(resolved_path, data)

    async def stream_read_async(
        self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        """Stream bytes from storage asynchronously.

        Uses obstore's native async streaming to yield chunks of bytes
        without buffering the entire file into memory.
        """
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        result = await self.store.get_async(resolved_path)
        return AsyncObStoreStreamIterator(result.stream(), chunk_size)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":  # pyright: ignore[reportUnusedParameter]
        """List objects in storage asynchronously."""
        # For LocalStore, the base_path is already included in the store root,
        # so we use empty prefix when none is given. For cloud stores, use base_path.
        if prefix:
            resolved_prefix = resolve_storage_path(prefix, self.base_path, self.protocol, strip_file_scheme=True)
        elif self._is_local_store:
            resolved_prefix = ""
        else:
            resolved_prefix = self.base_path or ""

        objects: list[str] = []
        async for batch in self.store.list_async(resolved_prefix):  # pyright: ignore[reportAttributeAccessIssue]
            objects.extend(item["path"] for item in batch)

        if not recursive and resolved_prefix:
            base_depth = resolved_prefix.count("/")
            objects = [obj for obj in objects if obj.count("/") <= base_depth + 1]

        results = sorted(objects)
        _log_storage_event(
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
            _log_storage_event(
                "storage.read",
                backend_type=self.backend_type,
                protocol=self.protocol,
                operation="exists",
                mode="async",
                path=str(path),
                exists=False,
            )
            return False
        _log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="exists",
            mode="async",
            path=resolved_path,
            exists=True,
        )
        return True

    async def delete_async(self, path: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Delete object from storage asynchronously."""
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        await self.store.delete_async(resolved_path)
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="delete",
            mode="async",
            path=resolved_path,
        )

    async def copy_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Copy object in storage asynchronously."""
        if self._is_local_store:
            source_path = self._resolve_path_for_local_store(source)
            dest_path = self._resolve_path_for_local_store(destination)
        else:
            source_path = resolve_storage_path(source, self.base_path, self.protocol, strip_file_scheme=True)
            dest_path = resolve_storage_path(destination, self.base_path, self.protocol, strip_file_scheme=True)

        await self.store.copy_async(source_path, dest_path)
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="copy",
            mode="async",
            source_path=source_path,
            destination_path=dest_path,
        )

    async def move_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Move object in storage asynchronously."""
        if self._is_local_store:
            source_path = self._resolve_path_for_local_store(source)
            dest_path = self._resolve_path_for_local_store(destination)
        else:
            source_path = resolve_storage_path(source, self.base_path, self.protocol, strip_file_scheme=True)
            dest_path = resolve_storage_path(destination, self.base_path, self.protocol, strip_file_scheme=True)

        await self.store.rename_async(source_path, dest_path)
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="move",
            mode="async",
            source_path=source_path,
            destination_path=dest_path,
        )

    async def get_metadata_async(self, path: "str | Path", **kwargs: Any) -> "dict[str, object]":  # pyright: ignore[reportUnusedParameter]
        """Get object metadata from storage asynchronously."""
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        result: dict[str, object] = {}
        # Keep in sync with get_metadata_sync.
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
        """Read Arrow table from storage asynchronously.

        Uses async_() with storage limiter to offload blocking PyArrow I/O to thread pool.
        """
        pq = import_pyarrow_parquet()
        resolved_path = self._resolve_path(path)
        data = await self._read_bytes_resolved_async(resolved_path)

        # Offload PyArrow parsing to thread pool
        result = await async_(pq.read_table)(io.BytesIO(data), **kwargs)

        _log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="read_arrow",
            mode="async",
            path=resolved_path,
        )
        return cast("ArrowTable", result)

    async def write_arrow_async(self, path: "str | Path", table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table to storage asynchronously.

        Uses async_() with storage limiter to offload blocking PyArrow serialization
        to thread pool, preventing event loop blocking.
        """
        pq = import_pyarrow_parquet()
        resolved_path = self._resolve_path(path)

        def _serialize() -> bytes:
            buffer = io.BytesIO()
            pq.write_table(table, buffer, **kwargs)
            buffer.seek(0)
            return buffer.read()

        data = await async_(_serialize)()
        await self._write_bytes_resolved_async(resolved_path, data)

        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_arrow",
            mode="async",
            path=resolved_path,
        )

    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator["ArrowRecordBatch"]:
        """Stream Arrow record batches from storage asynchronously.

        Args:
            pattern: Glob pattern to match files.
            **kwargs: Additional arguments passed to stream_arrow_sync().

        Returns:
            AsyncIterator yielding Arrow record batches.
        """
        resolved_pattern = resolve_storage_path(pattern, self.base_path, self.protocol, strip_file_scheme=True)
        return AsyncArrowBatchIterator(self.stream_arrow_sync(resolved_pattern, **kwargs))

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
        import obstore as obs

        method, expires_delta, resolved_paths, is_single = self._prepare_sign_request(paths, expires_in, for_upload)

        try:
            signed_urls: list[str] = await obs.sign_async(self.store, method, resolved_paths, expires_delta)  # type: ignore[call-overload]
            return signed_urls[0] if is_single else signed_urls
        except Exception as exc:
            msg = f"Failed to generate signed URL(s) for {resolved_paths}"
            raise StorageOperationFailedError(msg) from exc


def _read_obstore_bytes(store: Any, resolved_path: str) -> bytes:
    """Read bytes via obstore."""
    result = store.get(resolved_path)
    return cast("bytes", result.bytes().to_bytes())
