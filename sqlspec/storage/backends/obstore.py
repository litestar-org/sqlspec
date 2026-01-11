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
from typing import Any, Final, cast, overload
from urllib.parse import urlparse

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import StorageOperationFailedError
from sqlspec.storage._utils import import_pyarrow, import_pyarrow_parquet, resolve_storage_path
from sqlspec.storage.errors import execute_sync_storage_operation
from sqlspec.typing import ArrowRecordBatch, ArrowTable
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.module_loader import ensure_obstore

__all__ = ("ObStoreBackend",)

logger = get_logger(__name__)


DEFAULT_OPTIONS: Final[dict[str, Any]] = {"connect_timeout": "30s", "request_timeout": "60s"}


def _log_storage_event(
    event: str,
    *,
    backend_type: str,
    protocol: str,
    operation: str | None = None,
    mode: str | None = "sync",
    path: str | None = None,
    source_path: str | None = None,
    destination_path: str | None = None,
    count: int | None = None,
    exists: bool | None = None,
) -> None:
    fields: dict[str, Any] = {
        "backend_type": backend_type,
        "protocol": protocol,
        "mode": mode,
        "path": path,
        "source_path": source_path,
        "destination_path": destination_path,
        "count": count,
        "exists": exists,
    }
    if operation is not None:
        fields["operation"] = operation
    log_with_context(logger, logging.DEBUG, event, **fields)


def _read_obstore_bytes(store: Any, resolved_path: str) -> bytes:
    """Read bytes via obstore."""
    result = store.get(resolved_path)
    return cast("bytes", result.bytes().to_bytes())


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

            _log_storage_event(
                "storage.backend.ready",
                backend_type=self.backend_type,
                protocol=self.protocol,
                operation="init",
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
                return str(path_obj.relative_to(self._local_store_root))
            except ValueError:
                return str(path).lstrip("/")

        return str(path)

    def read_bytes(self, path: "str | Path", **kwargs: Any) -> bytes:  # pyright: ignore[reportUnusedParameter]
        """Read bytes using obstore."""
        resolved_path = self._resolve_path(path)

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
            path=resolved_path,
        )
        return result

    def write_bytes(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write bytes using obstore."""
        resolved_path = self._resolve_path(path)

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
            path=resolved_path,
        )

    def read_text(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text using obstore."""
        return self.read_bytes(path, **kwargs).decode(encoding)

    def write_text(self, path: "str | Path", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text using obstore."""
        self.write_bytes(path, data.encode(encoding), **kwargs)

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":  # pyright: ignore[reportUnusedParameter]
        """List objects using obstore."""
        resolved_prefix = (
            resolve_storage_path(prefix, self.base_path, self.protocol, strip_file_scheme=True)
            if prefix
            else self.base_path or ""
        )
        items = self.store.list_with_delimiter(resolved_prefix) if not recursive else self.store.list(resolved_prefix)
        paths = sorted(item["path"] for batch in items for item in batch)
        _log_storage_event(
            "storage.list",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="list_objects",
            path=resolved_prefix,
            count=len(paths),
        )
        return paths

    def exists(self, path: "str | Path", **kwargs: Any) -> bool:  # pyright: ignore[reportUnusedParameter]
        """Check if object exists using obstore."""
        try:
            resolved_path = self._resolve_path(path)
            self.store.head(resolved_path)  # pyright: ignore[reportUnknownMemberType]
        except Exception:
            _log_storage_event(
                "storage.read",
                backend_type=self.backend_type,
                protocol=self.protocol,
                operation="exists",
                path=str(path),
                exists=False,
            )
            return False
        _log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="exists",
            path=resolved_path,
            exists=True,
        )
        return True

    def delete(self, path: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Delete object using obstore."""
        resolved_path = self._resolve_path(path)
        execute_sync_storage_operation(
            partial(self.store.delete, resolved_path), backend=self.backend_type, operation="delete", path=resolved_path
        )
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="delete",
            path=resolved_path,
        )

    def copy(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Copy object using obstore."""
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
            source_path=source_path,
            destination_path=dest_path,
        )

    def move(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Move object using obstore."""
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
            source_path=source_path,
            destination_path=dest_path,
        )

    def glob(self, pattern: str, **kwargs: Any) -> "list[str]":
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
            results = matching_objects
        else:
            results = [obj for obj in all_objects if fnmatch.fnmatch(obj, resolved_pattern)]
        _log_storage_event(
            "storage.list",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="glob",
            path=resolved_pattern,
            count=len(results),
        )
        return results

    def get_metadata(self, path: "str | Path", **kwargs: Any) -> "dict[str, object]":  # pyright: ignore[reportUnusedParameter]
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
            path=resolved_path,
        )
        return result

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
            partial(pq.write_table, table, buffer, **kwargs),
            backend=self.backend_type,
            operation="write_arrow",
            path=resolved_path,
        )
        buffer.seek(0)
        self.write_bytes(resolved_path, buffer.read())
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_arrow",
            path=resolved_path,
        )

    def stream_read(self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any) -> Iterator[bytes]:
        """Stream bytes using obstore.

        Note:
            For remote backends, this currently performs a full read and yields chunks
            as obstore's sync client doesn't expose a streaming iterator.
            Use stream_read_async for true streaming.
        """
        resolved_path = self._resolve_path(path)
        data = self.read_bytes(resolved_path)

        if chunk_size:
            for i in range(0, len(data), chunk_size):
                yield data[i : i + chunk_size]
        else:
            yield data

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

    @property
    def supports_signing(self) -> bool:
        """Whether this backend supports URL signing.

        Only S3, GCS, and Azure backends support pre-signed URLs.
        Local file storage does not support URL signing.

        Returns:
            True if the protocol supports signing, False otherwise.
        """
        signable_protocols = {"s3", "gs", "gcs", "az", "azure"}
        return self.protocol in signable_protocols

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

        Raises:
            NotImplementedError: If the backend protocol does not support signing.
            ValueError: If expires_in exceeds maximum (604800 seconds).
        """
        import obstore as obs

        signable_protocols = {"s3", "gs", "gcs", "az", "azure"}
        if self.protocol not in signable_protocols:
            msg = (
                f"URL signing is not supported for protocol '{self.protocol}'. "
                f"Only S3, GCS, and Azure backends support pre-signed URLs."
            )
            raise NotImplementedError(msg)

        max_expires = 604800  # 7 days max per obstore/object_store limits
        if expires_in > max_expires:
            msg = f"expires_in cannot exceed {max_expires} seconds (7 days), got {expires_in}"
            raise ValueError(msg)

        from datetime import timedelta

        method = "PUT" if for_upload else "GET"
        expires_delta = timedelta(seconds=expires_in)

        if isinstance(paths, str):
            path_list = [paths]
            is_single = True
        else:
            path_list = list(paths)
            is_single = False

        resolved_paths = [
            resolve_storage_path(p, self.base_path, self.protocol, strip_file_scheme=True) for p in path_list
        ]

        try:
            signed_urls: list[str] = obs.sign(self.store, method, resolved_paths, expires_delta)  # type: ignore[call-overload]
            return signed_urls[0] if is_single else signed_urls
        except Exception as exc:
            msg = f"Failed to generate signed URL(s) for {resolved_paths}"
            raise StorageOperationFailedError(msg) from exc

    async def read_bytes_async(self, path: "str | Path", **kwargs: Any) -> bytes:  # pyright: ignore[reportUnusedParameter]
        """Read bytes from storage asynchronously."""
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

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

    async def write_bytes_async(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter]
        """Write bytes to storage asynchronously."""
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        await self.store.put_async(resolved_path, data)
        _log_storage_event(
            "storage.write",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="write_bytes",
            mode="async",
            path=resolved_path,
        )

    async def stream_read_async(
        self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        """Stream bytes from storage asynchronously."""
        if self._is_local_store:
            resolved_path = self._resolve_path_for_local_store(path)
        else:
            resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)

        result = await self.store.get_async(resolved_path)
        stream = result.stream()

        async def _generator() -> AsyncIterator[bytes]:
            async for chunk in stream:
                yield bytes(chunk)

            _log_storage_event(
                "storage.read",
                backend_type=self.backend_type,
                protocol=self.protocol,
                operation="stream_read",
                mode="async",
                path=resolved_path,
            )

        return _generator()

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> "list[str]":  # pyright: ignore[reportUnusedParameter]
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
        result = cast("ArrowTable", pq.read_table(io.BytesIO(data), **kwargs))
        _log_storage_event(
            "storage.read",
            backend_type=self.backend_type,
            protocol=self.protocol,
            operation="read_arrow",
            mode="async",
            path=resolved_path,
        )
        return result

    async def write_arrow_async(self, path: "str | Path", table: ArrowTable, **kwargs: Any) -> None:
        """Write Arrow table to storage asynchronously."""
        pq = import_pyarrow_parquet()
        resolved_path = resolve_storage_path(path, self.base_path, self.protocol, strip_file_scheme=True)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, **kwargs)
        buffer.seek(0)
        await self.write_bytes_async(resolved_path, buffer.read())
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
            **kwargs: Additional arguments passed to stream_arrow().

        Returns:
            AsyncIterator yielding Arrow record batches.
        """
        from sqlspec.storage.backends.base import AsyncArrowBatchIterator

        resolved_pattern = resolve_storage_path(pattern, self.base_path, self.protocol, strip_file_scheme=True)
        return AsyncArrowBatchIterator(self.stream_arrow(resolved_pattern, **kwargs))

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

        Raises:
            NotImplementedError: If the backend protocol does not support signing.
            ValueError: If expires_in exceeds maximum (604800 seconds).
        """
        import obstore as obs

        signable_protocols = {"s3", "gs", "gcs", "az", "azure"}
        if self.protocol not in signable_protocols:
            msg = (
                f"URL signing is not supported for protocol '{self.protocol}'. "
                f"Only S3, GCS, and Azure backends support pre-signed URLs."
            )
            raise NotImplementedError(msg)

        max_expires = 604800  # 7 days max per obstore/object_store limits
        if expires_in > max_expires:
            msg = f"expires_in cannot exceed {max_expires} seconds (7 days), got {expires_in}"
            raise ValueError(msg)

        from datetime import timedelta

        method = "PUT" if for_upload else "GET"
        expires_delta = timedelta(seconds=expires_in)

        if isinstance(paths, str):
            path_list = [paths]
            is_single = True
        else:
            path_list = list(paths)
            is_single = False

        resolved_paths = [
            resolve_storage_path(p, self.base_path, self.protocol, strip_file_scheme=True) for p in path_list
        ]

        try:
            signed_urls: list[str] = await obs.sign_async(self.store, method, resolved_paths, expires_delta)  # type: ignore[call-overload]
            return signed_urls[0] if is_single else signed_urls
        except Exception as exc:
            msg = f"Failed to generate signed URL(s) for {resolved_paths}"
            raise StorageOperationFailedError(msg) from exc
