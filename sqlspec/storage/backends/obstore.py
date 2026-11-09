"""Object storage backend using obstore.

Implements the ObjectStoreProtocol using obstore for S3, GCS, Azure,
and local file storage.
"""

import asyncio
import contextlib
import inspect
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, Self, cast, overload
from urllib.parse import urlparse
from uuid import uuid4

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import StorageCapabilityError, StorageOperationFailedError, StoragePathTraversalError
from sqlspec.storage._arrow_payload import (
    StorageFormat,
    encode_arrow_payload,
    encode_arrow_to_async_sink,
    encode_arrow_to_sink,
    join_shielded,
    validate_arrow_write_options,
)
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
    from obstore import Bytes
    from obstore.store import ObjectStore

    from sqlspec.protocols import AsyncWritableBytesSink, WritableBytesSink
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

from sqlspec.storage.errors import execute_async_storage_operation, execute_sync_storage_operation, raise_storage_error
from sqlspec.utils.module_loader import ensure_obstore
from sqlspec.utils.sync_tools import async_

_MAX_SIGN_EXPIRES_SECONDS: Final[int] = 604800
_SIGNABLE_PROTOCOLS: Final[frozenset[str]] = frozenset({"s3", "gs", "gcs", "az", "azure"})
_SUPPORTED_WRITE_KWARGS: Final[frozenset[str]] = frozenset({
    "attributes",
    "tags",
    "mode",
    "use_multipart",
    "chunk_size",
    "max_concurrency",
})
_DIRECT_ARROW_FORMATS: Final[frozenset[str]] = frozenset({"parquet", "arrow-ipc", "csv"})
_SUPPORTED_WRITER_KWARGS: Final[frozenset[str]] = frozenset({
    "attributes",
    "tags",
    "mode",
    "buffer_size",
    "chunk_size",
    "max_concurrency",
})
_OBSTORE_ARROW_STORE_KWARGS: Final[frozenset[str]] = frozenset({
    "attributes",
    "tags",
    "mode",
    "use_multipart",
    "buffer_size",
    "chunk_size",
    "max_concurrency",
})
_STAGE_PREFIX: Final[str] = ".sqlspec-stage-"
_ATOMIC_COPY_CREATE_PROTOCOLS: Final[frozenset[str]] = frozenset({
    "memory",
    "file",
    "gs",
    "gcs",
    "az",
    "azure",
    "abfs",
    "abfss",
})
_S3_PROTOCOLS: Final[frozenset[str]] = frozenset({"s3", "s3a"})

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

    def write_bytes_sync(self, path: "str | Path", data: bytes, *, overwrite: bool = True, **kwargs: Any) -> None:
        """Write bytes using obstore synchronously."""
        options = _resolve_obstore_put_options(overwrite, kwargs)
        resolved_path = self._resolve_path(path)
        self._write_bytes_resolved_sync(resolved_path, data, options)

    def read_text_sync(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text using obstore synchronously."""
        return self.read_bytes_sync(path, **kwargs).decode(encoding)

    def write_text_sync(
        self, path: "str | Path", data: str, encoding: str = "utf-8", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write text using obstore synchronously."""
        self.write_bytes_sync(path, data.encode(encoding), overwrite=overwrite, **kwargs)

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
        if getattr(self, "_is_local_store", False):
            target: Callable[[], ArrowTable] = partial(
                pq.read_table, str(Path(self._local_store_root) / resolved_path), **kwargs
            )
        else:
            target = partial(_read_obstore_arrow_table, self.store, resolved_path, import_pyarrow(), pq, kwargs)
        result = execute_sync_storage_operation(
            target, backend=self.backend_type, operation="read_arrow", path=resolved_path
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

    def _supports_atomic_copy_create(self) -> bool:
        """Return whether the underlying store supports atomic conditional copy (overwrite=False)."""
        if self.protocol in _ATOMIC_COPY_CREATE_PROTOCOLS:
            return True
        if self.protocol in _S3_PROTOCOLS:
            store_config = getattr(self.store, "config", None)
            has_store_setting = (
                bool(store_config.get("copy_if_not_exists")) if isinstance(store_config, dict) else False
            )
            return has_store_setting or bool(os.environ.get("AWS_COPY_IF_NOT_EXISTS"))
        return False

    def _can_stream_create_only(self, kwargs: "dict[str, Any]") -> bool:
        """Return whether create-only streaming can preserve the requested options on this store."""
        if not self._supports_atomic_copy_create():
            return False
        return not (
            self.protocol in _S3_PROTOCOLS and (kwargs.get("attributes") is not None or kwargs.get("tags") is not None)
        )

    def _prepare_create_only_stream_options(self, overwrite: bool, kwargs: "dict[str, Any]") -> "dict[str, Any]":
        """Validate create-only stream options and resolve options for the staged inner writer."""
        if kwargs:
            unsupported = sorted(k for k in kwargs if k not in _SUPPORTED_WRITER_KWARGS)
            if unsupported:
                names = ", ".join(unsupported)
                msg = f"Unsupported write stream keyword argument(s) for ObStoreBackend: {names}"
                raise TypeError(msg)
        mode = kwargs.get("mode")
        if not overwrite and mode is not None and mode != "create":
            msg = f"Conflicting write options: overwrite=False requires mode='create', got mode={mode!r}"
            raise ValueError(msg)
        if not self._supports_atomic_copy_create():
            msg = "Direct write streams do not support overwrite=False for this store"
            raise StorageCapabilityError(msg, capability="overwrite_false_stream")
        if self.protocol in _S3_PROTOCOLS and (kwargs.get("attributes") is not None or kwargs.get("tags") is not None):
            msg = "Direct create-only write streams on S3 do not preserve attributes or tags"
            raise StorageCapabilityError(msg, capability="overwrite_false_stream")
        stage_kwargs = {k: v for k, v in kwargs.items() if k != "mode"}
        return _resolve_obstore_writer_options(True, stage_kwargs)

    def supports_direct_arrow_write(self, file_format: "StorageFormat", *, overwrite: bool = True) -> bool:
        """Return whether ObStoreBackend can stream the given Arrow format directly.

        Args:
            file_format: Target Arrow serialization format.
            overwrite: Whether replacing an existing destination object is permitted.

        Returns:
            True when file_format is parquet, arrow-ipc, or csv and either overwrite is True
            or the store supports atomic conditional copy for staged create-only streams.
        """
        return file_format in _DIRECT_ARROW_FORMATS and (bool(overwrite) or self._supports_atomic_copy_create())

    def open_write_stream_sync(
        self, path: "str | Path", *, overwrite: bool = True, **kwargs: Any
    ) -> "AbstractContextManager[WritableBytesSink]":
        """Open a synchronous multipart byte sink that aborts on unhandled exceptions.

        Args:
            path: Destination object path relative to the store or base path.
            overwrite: Whether an existing destination may be overwritten.
            **kwargs: Optional writer settings (attributes, tags, buffer_size,
                chunk_size, max_concurrency, mode).

        Returns:
            Context manager entering a writable byte sink backed by obstore.open_writer.

        Raises:
            StoragePathTraversalError: If path escapes the store root.
            StorageCapabilityError: If overwrite=False is unsupported on the store or options.
            TypeError: If unsupported keyword arguments are supplied.
            ValueError: If writer options conflict or specify non-positive buffer sizes.
        """
        resolved_path = self._resolve_path(path)
        log_fields = {"protocol": self.protocol, "operation": "write_stream", "mode": "sync"}
        if not overwrite or kwargs.get("mode") == "create":
            writer_options = self._prepare_create_only_stream_options(overwrite, kwargs)
            stage = _obstore_stage_key(resolved_path)
            return _ObstoreStagedCreateStream(
                self.store, stage, resolved_path, writer_options, backend_type=self.backend_type, log_fields=log_fields
            )
        writer_options = _resolve_obstore_writer_options(overwrite, kwargs)
        return _ObstoreWriteStream(
            self.store, resolved_path, writer_options, backend_type=self.backend_type, log_fields=log_fields
        )

    def open_write_stream_async(
        self, path: "str | Path", *, overwrite: bool = True, **kwargs: Any
    ) -> "AbstractAsyncContextManager[AsyncWritableBytesSink]":
        """Open an asynchronous multipart byte sink with shielded abort on error or cancellation.

        Args:
            path: Destination object path relative to the store or base path.
            overwrite: Whether an existing destination may be overwritten.
            **kwargs: Optional writer settings (attributes, tags, buffer_size,
                chunk_size, max_concurrency, mode).

        Returns:
            Async context manager entering a byte sink backed by obstore.open_writer_async.

        Raises:
            StoragePathTraversalError: If path escapes the store root.
            StorageCapabilityError: If overwrite=False is unsupported on the store or options.
            TypeError: If unsupported keyword arguments are supplied.
            ValueError: If writer options conflict or specify non-positive buffer sizes.
        """
        resolved_path = self._resolve_path(path)
        log_fields = {"protocol": self.protocol, "operation": "write_stream", "mode": "async"}
        if not overwrite or kwargs.get("mode") == "create":
            writer_options = self._prepare_create_only_stream_options(overwrite, kwargs)
            stage = _obstore_stage_key(resolved_path)
            return _AsyncObstoreStagedCreateStream(
                self.store, stage, resolved_path, writer_options, backend_type=self.backend_type, log_fields=log_fields
            )
        writer_options = _resolve_obstore_writer_options(overwrite, kwargs)
        return _AsyncObstoreWriteStream(
            self.store, resolved_path, writer_options, backend_type=self.backend_type, log_fields=log_fields
        )

    def write_arrow_sync(
        self, path: "str | Path", table: "ArrowTable", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write an Arrow table via direct streaming or atomic buffered put when overwrite is False.

        Args:
            path: Destination object path relative to the store or base path.
            table: Arrow table to serialize and write.
            overwrite: Whether to overwrite an existing object or enforce atomic create.
            **kwargs: Format hint, compression, PyArrow writer options, and ObStore options.
        """
        import_pyarrow()
        resolved_path = self._resolve_path(path)

        format_hint: StorageFormat = kwargs.pop("format_hint", "parquet")
        if format_hint == "parquet":
            import_pyarrow_parquet()
        if format_hint not in _DIRECT_ARROW_FORMATS:
            msg = f"Arrow storage writes support only Parquet, Arrow IPC, and CSV formats, got {format_hint!r}"
            raise StorageCapabilityError(
                msg, capability="arrow_write", remediation="Write row payloads with the row storage APIs instead."
            )

        write_options_arg = kwargs.pop("write_options", None)
        merged_write_options: dict[str, Any] = dict(write_options_arg) if write_options_arg else {}
        store_kwargs: dict[str, Any] = {}
        has_compression_in_kwargs = "compression" in kwargs
        compression_arg = kwargs.pop("compression", None)

        for key, value in list(kwargs.items()):
            if key in _OBSTORE_ARROW_STORE_KWARGS:
                store_kwargs[key] = value
            else:
                merged_write_options[key] = value

        if has_compression_in_kwargs:
            compression = compression_arg
        elif "compression" in merged_write_options:
            compression = merged_write_options.get("compression")
        elif format_hint == "parquet":
            compression = "snappy"
        else:
            compression = None

        validate_arrow_write_options(format_hint, merged_write_options)

        is_create_only = (not overwrite) or (store_kwargs.get("mode") == "create")
        if not is_create_only:
            store_kwargs.pop("use_multipart", None)
            writer_options = _resolve_obstore_writer_options(overwrite, store_kwargs)

            def _do_stream_write() -> None:
                with _ObstoreWriteStream(
                    self.store, resolved_path, writer_options, backend_type=self.backend_type, log_fields=None
                ) as sink:
                    encode_arrow_to_sink(
                        table,
                        sink,
                        format_hint=format_hint,
                        compression=compression,
                        write_options=merged_write_options,
                    )

            execute_sync_storage_operation(
                _do_stream_write, backend=self.backend_type, operation="write_arrow", path=resolved_path
            )
        elif self._can_stream_create_only(store_kwargs):
            explicit_mode = store_kwargs.get("mode")
            if not overwrite and explicit_mode is not None and explicit_mode != "create":
                msg = f"Conflicting write options: overwrite=False requires mode='create', got mode={explicit_mode!r}"
                raise ValueError(msg)
            stage_store_kwargs = {k: v for k, v in store_kwargs.items() if k not in {"mode", "use_multipart"}}
            writer_options = _resolve_obstore_writer_options(True, stage_store_kwargs)
            stage = _obstore_stage_key(resolved_path)

            def _do_staged_stream_write() -> None:
                with _ObstoreStagedCreateStream(
                    self.store, stage, resolved_path, writer_options, backend_type=self.backend_type, log_fields=None
                ) as sink:
                    encode_arrow_to_sink(
                        table,
                        sink,
                        format_hint=format_hint,
                        compression=compression,
                        write_options=merged_write_options,
                    )

            execute_sync_storage_operation(
                _do_staged_stream_write, backend=self.backend_type, operation="write_arrow", path=resolved_path
            )
        else:
            buffer_size = store_kwargs.pop("buffer_size", None)
            chunk_size = store_kwargs.get("chunk_size")
            if buffer_size is not None and chunk_size is not None and buffer_size != chunk_size:
                msg = f"Conflicting buffer sizes: buffer_size={buffer_size} and chunk_size={chunk_size}"
                raise ValueError(msg)
            if buffer_size is not None and buffer_size <= 0:
                msg = f"buffer_size must be positive, got {buffer_size}"
                raise ValueError(msg)
            if chunk_size is not None and chunk_size <= 0:
                msg = f"chunk_size must be positive, got {chunk_size}"
                raise ValueError(msg)
            if buffer_size is not None and chunk_size is None:
                store_kwargs["chunk_size"] = buffer_size

            put_options = _resolve_obstore_put_options(overwrite, store_kwargs)

            def _do_buffered_write() -> None:
                payload = encode_arrow_payload(
                    table, format_hint, compression=compression, write_options=merged_write_options
                )
                self.store.put(resolved_path, payload, **put_options)

            execute_sync_storage_operation(
                _do_buffered_write, backend=self.backend_type, operation="write_arrow", path=resolved_path
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

    async def write_bytes_async(
        self, path: "str | Path", data: bytes, *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write bytes to storage asynchronously."""
        options = _resolve_obstore_put_options(overwrite, kwargs)
        resolved_path = self._resolve_path(path)
        await self._write_bytes_resolved_async(resolved_path, data, options)

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

    async def write_text_async(
        self, path: "str | Path", data: str, encoding: str = "utf-8", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write text to storage asynchronously."""
        encoded_data = data.encode(encoding)
        await self.write_bytes_async(path, encoded_data, overwrite=overwrite, **kwargs)

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
        if getattr(self, "_is_local_store", False):
            source: Any = str(Path(self._local_store_root) / resolved_path)
        else:
            source = await execute_async_storage_operation(
                partial(_read_obstore_arrow_source_async, self.store, resolved_path, import_pyarrow()),
                backend=self.backend_type,
                operation="read_arrow",
                path=resolved_path,
            )

        result = await execute_async_storage_operation(
            partial(async_(pq.read_table), source, **kwargs),
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

    async def write_arrow_async(
        self, path: "str | Path", table: "ArrowTable", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        """Write an Arrow table asynchronously via direct streaming or atomic buffered put_async.

        Args:
            path: Destination object path relative to the store or base path.
            table: Arrow table to serialize and write.
            overwrite: Whether to overwrite an existing object or enforce atomic create.
            **kwargs: Format hint, compression, PyArrow writer options, and ObStore options.
        """
        import_pyarrow()
        resolved_path = self._resolve_path(path)

        format_hint: StorageFormat = kwargs.pop("format_hint", "parquet")
        if format_hint == "parquet":
            import_pyarrow_parquet()
        if format_hint not in _DIRECT_ARROW_FORMATS:
            msg = f"Arrow storage writes support only Parquet, Arrow IPC, and CSV formats, got {format_hint!r}"
            raise StorageCapabilityError(
                msg, capability="arrow_write", remediation="Write row payloads with the row storage APIs instead."
            )

        write_options_arg = kwargs.pop("write_options", None)
        merged_write_options: dict[str, Any] = dict(write_options_arg) if write_options_arg else {}
        store_kwargs: dict[str, Any] = {}
        has_compression_in_kwargs = "compression" in kwargs
        compression_arg = kwargs.pop("compression", None)

        for key, value in list(kwargs.items()):
            if key in _OBSTORE_ARROW_STORE_KWARGS:
                store_kwargs[key] = value
            else:
                merged_write_options[key] = value

        if has_compression_in_kwargs:
            compression = compression_arg
        elif "compression" in merged_write_options:
            compression = merged_write_options.get("compression")
        elif format_hint == "parquet":
            compression = "snappy"
        else:
            compression = None

        validate_arrow_write_options(format_hint, merged_write_options)

        is_create_only = (not overwrite) or (store_kwargs.get("mode") == "create")
        if not is_create_only:
            store_kwargs.pop("use_multipart", None)
            writer_options = _resolve_obstore_writer_options(overwrite, store_kwargs)

            async def _do_stream_write_async() -> None:
                async with _AsyncObstoreWriteStream(
                    self.store, resolved_path, writer_options, backend_type=self.backend_type, log_fields=None
                ) as sink:
                    chunk_size = writer_options.get("buffer_size", 1048576)
                    await encode_arrow_to_async_sink(
                        table,
                        sink,
                        format_hint=format_hint,
                        compression=compression,
                        write_options=merged_write_options,
                        chunk_size=chunk_size,
                    )

            await execute_async_storage_operation(
                _do_stream_write_async, backend=self.backend_type, operation="write_arrow", path=resolved_path
            )
        elif self._can_stream_create_only(store_kwargs):
            explicit_mode = store_kwargs.get("mode")
            if not overwrite and explicit_mode is not None and explicit_mode != "create":
                msg = f"Conflicting write options: overwrite=False requires mode='create', got mode={explicit_mode!r}"
                raise ValueError(msg)
            stage_store_kwargs = {k: v for k, v in store_kwargs.items() if k not in {"mode", "use_multipart"}}
            writer_options = _resolve_obstore_writer_options(True, stage_store_kwargs)
            stage = _obstore_stage_key(resolved_path)

            async def _do_staged_stream_write_async() -> None:
                async with _AsyncObstoreStagedCreateStream(
                    self.store, stage, resolved_path, writer_options, backend_type=self.backend_type, log_fields=None
                ) as sink:
                    chunk_size = writer_options.get("buffer_size", 1048576)
                    await encode_arrow_to_async_sink(
                        table,
                        sink,
                        format_hint=format_hint,
                        compression=compression,
                        write_options=merged_write_options,
                        chunk_size=chunk_size,
                    )

            await execute_async_storage_operation(
                _do_staged_stream_write_async, backend=self.backend_type, operation="write_arrow", path=resolved_path
            )
        else:
            buffer_size = store_kwargs.pop("buffer_size", None)
            chunk_size = store_kwargs.get("chunk_size")
            if buffer_size is not None and chunk_size is not None and buffer_size != chunk_size:
                msg = f"Conflicting buffer sizes: buffer_size={buffer_size} and chunk_size={chunk_size}"
                raise ValueError(msg)
            if buffer_size is not None and buffer_size <= 0:
                msg = f"buffer_size must be positive, got {buffer_size}"
                raise ValueError(msg)
            if chunk_size is not None and chunk_size <= 0:
                msg = f"chunk_size must be positive, got {chunk_size}"
                raise ValueError(msg)
            if buffer_size is not None and chunk_size is None:
                store_kwargs["chunk_size"] = buffer_size

            put_options = _resolve_obstore_put_options(overwrite, store_kwargs)

            async def _do_buffered_write_async() -> None:
                payload = await async_(encode_arrow_payload)(
                    table, format_hint, compression=compression, write_options=merged_write_options
                )
                await self.store.put_async(resolved_path, payload, **put_options)

            await execute_async_storage_operation(
                _do_buffered_write_async, backend=self.backend_type, operation="write_arrow", path=resolved_path
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
        if getattr(self, "_is_local_store", False):
            target: Callable[[], bytes] = (Path(self._local_store_root) / resolved_path).read_bytes
        else:
            target = partial(_read_obstore_bytes, self.store, resolved_path)
        result = execute_sync_storage_operation(
            target, backend=self.backend_type, operation="read_bytes", path=resolved_path
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

    def _write_bytes_resolved_sync(self, resolved_path: str, data: bytes, options: "dict[str, Any]") -> None:
        execute_sync_storage_operation(
            partial(self.store.put, resolved_path, data, **options),
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
        if getattr(self, "_is_local_store", False):
            target: Callable[[], Awaitable[bytes]] = async_((Path(self._local_store_root) / resolved_path).read_bytes)
        else:
            target = partial(_read_obstore_bytes_async, self.store, resolved_path)
        data = await execute_async_storage_operation(
            target, backend=self.backend_type, operation="read_bytes", path=resolved_path
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

    async def _write_bytes_resolved_async(self, resolved_path: str, data: bytes, options: "dict[str, Any]") -> None:
        await execute_async_storage_operation(
            partial(self.store.put_async, resolved_path, data, **options),
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


def _resolve_obstore_put_options(overwrite: bool, kwargs: "dict[str, Any]") -> "dict[str, Any]":
    """Validate and resolve obstore put options for byte writes.

    Args:
        overwrite: Whether an existing object may be replaced.
        kwargs: Caller-supplied ``obstore.put`` options.

    Returns:
        The options to pass to ``put``, with ``mode="create"`` when overwriting is disabled.

    Raises:
        TypeError: If an option is not an ``obstore.put`` option.
        ValueError: If ``overwrite=False`` is combined with a mode other than ``"create"``.
    """
    if kwargs:
        unsupported = [name for name in kwargs if name not in _SUPPORTED_WRITE_KWARGS]
        if unsupported:
            names = ", ".join(sorted(unsupported))
            msg = f"Unsupported write keyword argument(s) for ObStoreBackend: {names}"
            raise TypeError(msg)
    if overwrite:
        return kwargs
    explicit_mode = kwargs.get("mode")
    if explicit_mode is not None and explicit_mode != "create":
        msg = f"Conflicting write options: overwrite=False requires mode='create', got mode={explicit_mode!r}"
        raise ValueError(msg)
    return {**kwargs, "mode": "create"}


async def _read_obstore_bytes_async(store: Any, resolved_path: str) -> bytes:
    """Read bytes via obstore asynchronously."""
    result = await store.get_async(resolved_path)
    bytes_obj = await result.bytes_async()
    return cast("bytes", bytes_obj.to_bytes())


def _read_obstore_arrow_table(
    store: Any, resolved_path: str, pa: Any, pq: Any, options: "dict[str, Any]"
) -> "ArrowTable":
    """Read an Arrow table via obstore using a zero-copy buffer reader."""
    result = store.get(resolved_path)
    return cast("ArrowTable", pq.read_table(pa.BufferReader(result.bytes()), **options))


async def _read_obstore_arrow_source_async(store: Any, resolved_path: str, pa: Any) -> Any:
    """Read an obstore object into a zero-copy Arrow buffer reader asynchronously."""
    result = await store.get_async(resolved_path)
    return pa.BufferReader(await result.bytes_async())


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

    def read(self, size: int = -1) -> "Bytes":
        """Read from the object without copying obstore's buffer.

        Args:
            size: Maximum number of bytes to read, or a negative value to read to the end.

        Returns:
            An ``obstore.Bytes`` buffer, which PyArrow reads through the buffer protocol.
        """
        if size < 0:
            return cast("Bytes", self._reader.readall())
        return cast("Bytes", self._reader.read(size))

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


def _resolve_obstore_writer_options(overwrite: bool, kwargs: "dict[str, Any]") -> "dict[str, Any]":
    """Validate and normalize keyword arguments for obstore.open_writer and open_writer_async.

    Args:
        overwrite: Whether overwriting existing destinations is allowed.
        kwargs: Raw keyword arguments supplied to open_write_stream_*.

    Returns:
        Validated options dictionary suitable for open_writer and open_writer_async.

    Raises:
        TypeError: If unsupported keyword arguments are present.
        ValueError: If mode conflicts with overwrite=False or buffer sizes conflict or are non-positive.
        StorageCapabilityError: If overwrite is False or mode is 'create'.
    """
    if kwargs:
        unsupported = sorted(k for k in kwargs if k not in _SUPPORTED_WRITER_KWARGS)
        if unsupported:
            names = ", ".join(unsupported)
            msg = f"Unsupported write stream keyword argument(s) for ObStoreBackend: {names}"
            raise TypeError(msg)

    mode = kwargs.get("mode")
    if not overwrite:
        if mode is not None and mode != "create":
            msg = f"Conflicting write options: overwrite=False requires mode='create', got mode={mode!r}"
            raise ValueError(msg)
        msg = "Direct write streams do not support overwrite=False"
        raise StorageCapabilityError(msg, capability="overwrite_false_stream")

    if mode == "create":
        msg = "Direct write streams do not support mode='create'"
        raise StorageCapabilityError(msg, capability="overwrite_false_stream")

    buffer_size = kwargs.get("buffer_size")
    chunk_size = kwargs.get("chunk_size")
    if buffer_size is not None and chunk_size is not None and buffer_size != chunk_size:
        msg = f"Conflicting buffer sizes: buffer_size={buffer_size} and chunk_size={chunk_size}"
        raise ValueError(msg)
    if buffer_size is not None and buffer_size <= 0:
        msg = f"buffer_size must be positive, got {buffer_size}"
        raise ValueError(msg)
    if chunk_size is not None and chunk_size <= 0:
        msg = f"chunk_size must be positive, got {chunk_size}"
        raise ValueError(msg)

    opts = dict(kwargs)
    opts.pop("mode", None)
    if chunk_size is not None and buffer_size is None:
        opts["buffer_size"] = chunk_size
    opts.pop("chunk_size", None)
    return opts


class _ObstoreWriteStream:
    """Commit or abort one obstore multipart writer around a caller's write block."""

    __slots__ = ("_backend_type", "_entered", "_log_fields", "_resolved_path", "_sink", "_store", "_writer_options")

    def __init__(
        self,
        store: Any,
        resolved_path: str,
        writer_options: "dict[str, Any]",
        *,
        backend_type: str,
        log_fields: "dict[str, Any] | None",
    ) -> None:
        self._store = store
        self._resolved_path = resolved_path
        self._writer_options = writer_options
        self._backend_type = backend_type
        self._log_fields = log_fields
        self._entered: Any = None
        self._sink: _ObstoreSink | None = None

    def __enter__(self) -> "_ObstoreSink":
        obs = import_obstore()
        raw_writer = execute_sync_storage_operation(
            partial(obs.open_writer, self._store, self._resolved_path, **self._writer_options),
            backend=self._backend_type,
            operation="open_write_stream",
            path=self._resolved_path,
        )
        entered_writer = execute_sync_storage_operation(
            raw_writer.__enter__, backend=self._backend_type, operation="open_write_stream", path=self._resolved_path
        )
        self._entered = entered_writer
        sink = _ObstoreSink(entered_writer, backend=self._backend_type, path=self._resolved_path)
        self._sink = sink
        return sink

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        entered_writer = self._entered
        sink = self._sink
        if sink is not None:
            sink._closed = True
        if entered_writer is None:
            return
        if exc_type is None:
            execute_sync_storage_operation(
                partial(entered_writer.__exit__, None, None, None),
                backend=self._backend_type,
                operation="write_stream_close",
                path=self._resolved_path,
            )
            if self._log_fields is not None:
                log_storage_event(
                    "storage.write", backend_type=self._backend_type, path=self._resolved_path, **self._log_fields
                )
        else:
            with contextlib.suppress(Exception):
                entered_writer.__exit__(exc_type, exc, traceback)


class _AsyncObstoreWriteStream:
    """Commit or abort one obstore async writer with shielded completion."""

    __slots__ = ("_backend_type", "_entered", "_log_fields", "_resolved_path", "_sink", "_store", "_writer_options")

    def __init__(
        self,
        store: Any,
        resolved_path: str,
        writer_options: "dict[str, Any]",
        *,
        backend_type: str,
        log_fields: "dict[str, Any] | None",
    ) -> None:
        self._store = store
        self._resolved_path = resolved_path
        self._writer_options = writer_options
        self._backend_type = backend_type
        self._log_fields = log_fields
        self._entered: Any = None
        self._sink: _AsyncObstoreSink | None = None

    async def __aenter__(self) -> "_AsyncObstoreSink":
        obs = import_obstore()
        raw_writer = execute_sync_storage_operation(
            partial(obs.open_writer_async, self._store, self._resolved_path, **self._writer_options),
            backend=self._backend_type,
            operation="open_write_stream",
            path=self._resolved_path,
        )
        entered_writer = await execute_async_storage_operation(
            raw_writer.__aenter__, backend=self._backend_type, operation="open_write_stream", path=self._resolved_path
        )
        self._entered = entered_writer
        sink = _AsyncObstoreSink(entered_writer, backend=self._backend_type, path=self._resolved_path)
        self._sink = sink
        return sink

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        entered_writer = self._entered
        sink = self._sink
        if sink is not None:
            sink._closed = True
        if entered_writer is None:
            return
        if exc_type is None:
            exit_fut = entered_writer.__aexit__(None, None, None)
            if inspect.isawaitable(exit_fut):
                await execute_async_storage_operation(
                    partial(join_shielded, asyncio.ensure_future(exit_fut)),
                    backend=self._backend_type,
                    operation="write_stream_close",
                    path=self._resolved_path,
                )
            if self._log_fields is not None:
                log_storage_event(
                    "storage.write", backend_type=self._backend_type, path=self._resolved_path, **self._log_fields
                )
        else:
            try:
                exit_fut = entered_writer.__aexit__(exc_type, exc, traceback)
                if inspect.isawaitable(exit_fut):
                    await join_shielded(asyncio.ensure_future(exit_fut), suppress_result=True)
            except Exception:
                return


def _obstore_stage_key(destination: str) -> str:
    """Construct a hidden stage key in the destination's parent prefix."""
    if "/" in destination:
        parent, _ = destination.rsplit("/", 1)
        return f"{parent}/{_STAGE_PREFIX}{uuid4().hex}" if parent else f"{_STAGE_PREFIX}{uuid4().hex}"
    return f"{_STAGE_PREFIX}{uuid4().hex}"


class _ObstoreStagedCreateStream:
    """Stage a create-only obstore stream to a temporary key and publish via conditional copy."""

    __slots__ = ("_backend_type", "_destination", "_inner", "_log_fields", "_stage", "_store")

    def __init__(
        self,
        store: Any,
        stage: str,
        destination: str,
        writer_options: "dict[str, Any]",
        *,
        backend_type: str,
        log_fields: "dict[str, Any] | None",
    ) -> None:
        self._store = store
        self._stage = stage
        self._destination = destination
        self._backend_type = backend_type
        self._log_fields = log_fields
        self._inner = _ObstoreWriteStream(store, stage, writer_options, backend_type=backend_type, log_fields=None)

    def __enter__(self) -> "_ObstoreSink":
        return self._inner.__enter__()

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is not None:
            self._inner.__exit__(exc_type, exc, traceback)
            return
        self._inner.__exit__(None, None, None)
        obs = import_obstore()
        try:
            execute_sync_storage_operation(
                partial(obs.copy, self._store, self._stage, self._destination, overwrite=False),
                backend=self._backend_type,
                operation="write_stream_close",
                path=self._destination,
            )
        finally:
            with contextlib.suppress(Exception):
                obs.delete(self._store, self._stage)
        if self._log_fields is not None:
            log_storage_event(
                "storage.write", backend_type=self._backend_type, path=self._destination, **self._log_fields
            )


async def _delete_obstore_stage_async(store: Any, stage: str) -> None:
    """Delete a temporary staged obstore object while suppressing cleanup errors."""
    obs = import_obstore()
    try:
        del_coro = obs.delete_async(store, stage)
        await join_shielded(asyncio.ensure_future(del_coro), suppress_result=True)
    except Exception:
        return


class _AsyncObstoreStagedCreateStream:
    """Asynchronously stage a create-only obstore stream and publish via shielded conditional copy."""

    __slots__ = ("_backend_type", "_destination", "_inner", "_log_fields", "_stage", "_store")

    def __init__(
        self,
        store: Any,
        stage: str,
        destination: str,
        writer_options: "dict[str, Any]",
        *,
        backend_type: str,
        log_fields: "dict[str, Any] | None",
    ) -> None:
        self._store = store
        self._stage = stage
        self._destination = destination
        self._backend_type = backend_type
        self._log_fields = log_fields
        self._inner = _AsyncObstoreWriteStream(store, stage, writer_options, backend_type=backend_type, log_fields=None)

    async def __aenter__(self) -> "_AsyncObstoreSink":
        return await self._inner.__aenter__()

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is not None:
            await self._inner.__aexit__(exc_type, exc, traceback)
            return
        obs = import_obstore()
        cancel_exc: asyncio.CancelledError | None = None
        try:
            await self._inner.__aexit__(None, None, None)
        except asyncio.CancelledError as caught_cancel:
            cancel_exc = caught_cancel
        if cancel_exc is not None:
            await _delete_obstore_stage_async(self._store, self._stage)
            raise cancel_exc
        pending_exc: BaseException | None = None
        try:
            copy_coro = obs.copy_async(self._store, self._stage, self._destination, overwrite=False)
            await execute_async_storage_operation(
                partial(join_shielded, asyncio.ensure_future(copy_coro)),
                backend=self._backend_type,
                operation="write_stream_close",
                path=self._destination,
            )
        except BaseException as publish_exc:
            pending_exc = publish_exc
        await _delete_obstore_stage_async(self._store, self._stage)
        if pending_exc is not None:
            raise pending_exc
        if self._log_fields is not None:
            log_storage_event(
                "storage.write", backend_type=self._backend_type, path=self._destination, **self._log_fields
            )


class _ObstoreSink:
    """Adapt an obstore WritableFile to WritableBytesSink and pyarrow.PythonFile."""

    __slots__ = ("_backend", "_closed", "_path", "_writer")

    def __init__(self, writer: Any, *, backend: str = "obstore", path: str | None = None) -> None:
        self._writer = writer
        self._backend = backend
        self._path = path
        self._closed = False

    @property
    def closed(self) -> bool:
        if self._closed:
            return True
        writer_closed = getattr(self._writer, "closed", None)
        if callable(writer_closed):
            return bool(writer_closed())
        if writer_closed is not None:
            return bool(writer_closed)
        return False

    def write(self, data: Any, /) -> int:
        if self._closed:
            return 0
        payload = bytes(data) if isinstance(data, (bytearray, memoryview)) else data
        if not payload:
            return 0
        return int(
            execute_sync_storage_operation(
                partial(self._writer.write, payload),
                backend=self._backend,
                operation="write_stream_write",
                path=self._path,
            )
        )

    def flush(self) -> None:
        if self._closed:
            return
        flush_fn = getattr(self._writer, "flush", None)
        if callable(flush_fn):
            execute_sync_storage_operation(
                flush_fn, backend=self._backend, operation="write_stream_flush", path=self._path
            )

    def close(self) -> None:
        self._closed = True


class _AsyncObstoreSink:
    """Adapt an obstore AsyncWritableFile to AsyncWritableBytesSink with storage error translation."""

    __slots__ = ("_backend", "_closed", "_path", "_writer")

    def __init__(self, writer: Any, *, backend: str = "obstore", path: str | None = None) -> None:
        self._writer = writer
        self._backend = backend
        self._path = path
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def write(self, data: Any, /) -> int:
        if self._closed:
            return 0
        payload = bytes(data) if isinstance(data, (bytearray, memoryview)) else data
        if not payload:
            return 0
        try:
            result = self._writer.write(payload)
            if inspect.isawaitable(result):
                result = await result
            return int(result)
        except Exception as error:
            raise_storage_error(error, backend=self._backend, operation="write_stream_write", path=self._path)

    async def flush(self) -> None:
        if self._closed:
            return
        flush_fn = getattr(self._writer, "flush", None)
        if callable(flush_fn):
            try:
                result = flush_fn()
                if inspect.isawaitable(result):
                    await result
            except Exception as error:
                raise_storage_error(error, backend=self._backend, operation="write_stream_flush", path=self._path)

    async def close(self) -> None:
        self._closed = True
