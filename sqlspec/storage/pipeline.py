"""Storage pipeline scaffolding for driver-aware storage bridge."""

from collections import deque
from functools import partial
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, NamedTuple, NotRequired, TypeAlias, cast
from urllib.parse import urlparse

from mypy_extensions import mypyc_attr
from typing_extensions import TypedDict

from sqlspec.exceptions import ImproperConfigurationError, StorageCapabilityError
from sqlspec.storage._arrow_payload import (
    StorageFormat,
    batches_to_arrow_table,
    decode_arrow_payload,
    encode_arrow_payload,
)
from sqlspec.storage._arrow_stream import iter_streamed_arrow_batches
from sqlspec.storage._paths import FILE_PROTOCOL, FILE_SCHEME_PREFIX, parse_file_uri_path
from sqlspec.storage.backends.base import AsyncArrowBatchIterator
from sqlspec.storage.errors import execute_async_storage_operation, execute_sync_storage_operation
from sqlspec.storage.registry import StorageRegistry, storage_registry
from sqlspec.utils.serializers import get_serializer_metrics, serialize_collection, to_json
from sqlspec.utils.sync_tools import async_
from sqlspec.utils.type_guards import supports_async_read_bytes, supports_async_write_bytes
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlspec.protocols import ObjectStoreProtocol
    from sqlspec.typing import ArrowRecordBatch, ArrowTable


__all__ = (
    "AsyncStoragePipeline",
    "PartitionStrategyConfig",
    "ResolvedStorageTarget",
    "StorageBridgeJob",
    "StorageCapabilities",
    "StorageDestination",
    "StorageDiagnostics",
    "StorageFormat",
    "StorageTelemetry",
    "SyncStoragePipeline",
    "create_storage_bridge_job",
    "get_recent_storage_events",
    "get_storage_bridge_diagnostics",
    "get_storage_bridge_metrics",
    "record_storage_diagnostic_event",
    "reset_storage_bridge_events",
    "reset_storage_bridge_metrics",
)

StorageDestination: TypeAlias = str | Path
StorageDiagnostics: TypeAlias = dict[str, float]


class StorageCapabilities(TypedDict):
    """Runtime-evaluated driver storage capabilities."""

    arrow_export_enabled: bool
    """Whether the driver supports exporting query results as Apache Arrow tables or batches."""
    arrow_import_enabled: bool
    """Whether the driver supports ingesting Apache Arrow tables or record batches."""
    parquet_export_enabled: bool
    """Whether direct or staged Parquet export is available."""
    parquet_import_enabled: bool
    """Whether direct or staged Parquet ingestion is available."""
    partition_strategies: "list[str]"
    """Supported partition fan-out strategy names (such as ``fixed``, ``rows_per_chunk``, or ``manifest``)."""
    default_storage_profile: NotRequired[str | None]
    """Named storage registry profile used when callers omit an explicit backend URI."""


class PartitionStrategyConfig(TypedDict, total=False):
    """Configuration for partition fan-out strategies."""

    kind: str
    """Partitioning strategy identifier (``fixed``, ``rows_per_chunk``, or ``manifest``)."""
    partitions: int
    """Target number of output partitions to produce."""
    rows_per_chunk: int
    """Maximum row count written per output file or batch chunk."""
    manifest_path: str
    """Optional destination URI or relative path for writing a partition manifest."""


class StorageTelemetry(TypedDict, total=False):
    """Telemetry payload for storage bridge operations."""

    destination: str
    """Resolved target or source storage URI for the bridge transfer."""
    bytes_processed: int
    """Total uncompressed or encoded payload bytes read or written."""
    rows_processed: int
    """Total number of records transferred across the storage bridge."""
    partitions_created: int
    """Number of partitioned output objects written during export."""
    duration_s: float
    """Elapsed wall-clock transfer time in seconds."""
    read_duration_s: float
    """Elapsed time spent reading source storage or Arrow batches in seconds."""
    encode_duration_s: float
    """Elapsed time spent encoding batches into wire payloads in seconds."""
    write_duration_s: float
    """Elapsed time spent writing payloads to the database or storage sink in seconds."""
    commit_duration_s: float
    """Elapsed time spent committing the transaction or finalizing the sink in seconds."""
    shards_processed: int
    """Number of storage shards loaded during a multi-shard operation."""
    shards_skipped: int
    """Number of already-checkpointed storage shards skipped during resume."""
    wal_bytes: int
    """Estimated or observed Write-Ahead Log bytes generated during ingestion."""
    format: str
    """Serialized wire/file format (such as ``parquet``, ``arrow``, ``csv``, or ``jsonl``)."""
    extra: "dict[str, object]"
    """Adapter-specific diagnostic counters or native COPY options."""
    backend: str
    """Storage backend implementation name (``local``, ``obstore``, or ``fsspec``)."""
    correlation_id: str
    """Active request correlation identifier attached to the storage job."""
    config: str
    """Adapter configuration class name executing the storage transfer."""
    bind_key: str
    """Registered SQLSpec configuration bind key, when configured."""


class ResolvedStorageTarget(NamedTuple):
    """A storage destination resolved to an address and its backend protocol."""

    uri: str
    protocol: str


class StorageBridgeJob(NamedTuple):
    """Handle representing a storage bridge operation."""

    job_id: str
    status: str
    telemetry: StorageTelemetry


class _StorageBridgeMetrics:
    __slots__ = ("bytes_written",)

    def __init__(self) -> None:
        self.bytes_written = 0

    def record_bytes(self, count: int) -> None:
        self.bytes_written += max(count, 0)

    def snapshot(self) -> "dict[str, int]":
        return {"storage_bridge.bytes_written": self.bytes_written}

    def reset(self) -> None:
        self.bytes_written = 0


_METRICS = _StorageBridgeMetrics()
_RECENT_STORAGE_EVENTS: "deque[StorageTelemetry]" = deque(maxlen=25)
_EMPTY_STORAGE_OPTIONS: dict[str, Any] = {}
_ARROW_WRITE_FORMATS = frozenset({"parquet", "arrow-ipc", "csv"})
_ROW_WRITE_FORMATS = frozenset({"json", "jsonl"})
_CLOUD_OBJECT_STORE_SCHEMES = frozenset({"s3", "gs", "gcs", "az", "azure"})


def get_storage_bridge_metrics() -> "dict[str, int]":
    """Return aggregated storage bridge metrics."""

    return _METRICS.snapshot()


def reset_storage_bridge_metrics() -> None:
    """Reset aggregated storage bridge metrics."""

    _METRICS.reset()


def record_storage_diagnostic_event(telemetry: StorageTelemetry) -> None:
    """Record telemetry for inclusion in diagnostics snapshots."""

    _RECENT_STORAGE_EVENTS.appendleft(cast("StorageTelemetry", dict(telemetry)))


def get_recent_storage_events() -> "list[StorageTelemetry]":
    """Return recent storage telemetry events (most recent first)."""

    return [cast("StorageTelemetry", dict(entry)) for entry in _RECENT_STORAGE_EVENTS]


def reset_storage_bridge_events() -> None:
    """Clear recorded storage telemetry events."""

    _RECENT_STORAGE_EVENTS.clear()


def create_storage_bridge_job(status: str, telemetry: StorageTelemetry) -> StorageBridgeJob:
    """Create a storage bridge job handle with a unique identifier."""

    job = StorageBridgeJob(job_id=str(uuid4()), status=status, telemetry=telemetry)
    record_storage_diagnostic_event(job.telemetry)
    return job


def get_storage_bridge_diagnostics() -> "StorageDiagnostics":
    """Return aggregated storage bridge + serializer cache metrics."""

    diagnostics: dict[str, float] = {key: float(value) for key, value in get_storage_bridge_metrics().items()}
    serializer_metrics = get_serializer_metrics()
    for key, value in serializer_metrics.items():
        diagnostics[f"serializer.{key}"] = float(value)
    return diagnostics


@mypyc_attr(allow_interpreted_subclasses=True)
class _StoragePipelineBase:
    """Shared registry and backend-resolution state for storage pipelines."""

    __slots__ = ("_csv_write_options", "_resolved_backend_cache", "_storage_options", "registry")

    def __init__(
        self, *, registry: StorageRegistry | None = None, storage_options: "dict[str, Any] | None" = None
    ) -> None:
        self.registry = storage_registry if registry is None else registry
        self._resolved_backend_cache: dict[str, tuple[ObjectStoreProtocol, str, str]] = {}
        self._storage_options = _EMPTY_STORAGE_OPTIONS if storage_options is None else storage_options
        self._csv_write_options = _csv_write_options_from_kwargs(self._storage_options)

    def clear_cache(self) -> None:
        """Clear cached storage backend resolutions for this pipeline instance."""
        self._resolved_backend_cache.clear()

    def resolve_destination(
        self, destination: StorageDestination, storage_options: "dict[str, Any] | None" = None
    ) -> ResolvedStorageTarget:
        """Resolve a destination without opening a database session or reading an object.

        Direct remote URIs retain their address. Alias paths resolve relative to
        the registered backend. Local paths resolve to absolute filesystem paths
        through the backend's path checks.

        Args:
            destination: Remote URI, local path, or ``alias://name/path``.
            storage_options: Explicit backend options. Pipeline writer defaults
                are not inherited by this method.

        Returns:
            The resolved address and backend protocol.

        Raises:
            ImproperConfigurationError: If the destination or alias is invalid.
            StoragePathTraversalError: If the backend rejects the local path.
        """
        backend, path, _backend_name = self._backend(destination, storage_options)
        destination_str = str(destination)
        if destination_str.startswith("alias://"):
            uri = backend.resolve_uri(path)
        elif backend.protocol == FILE_PROTOCOL:
            if destination_str.startswith(FILE_SCHEME_PREFIX):
                path = parse_file_uri_path(destination_str)
            uri = backend.resolve_uri(Path(path).expanduser().resolve())
        else:
            uri = destination_str
        return ResolvedStorageTarget(uri, backend.protocol)

    def _backend(
        self, destination: StorageDestination, backend_options: "dict[str, Any] | None"
    ) -> "tuple[ObjectStoreProtocol, str, str]":
        """Resolve storage backend and normalized path for a destination."""
        cache_key = _backend_cache_key(destination, backend_options)
        if cache_key is None:
            return _storage_backend(self.registry, destination, backend_options)
        cached = self._resolved_backend_cache.get(cache_key)
        if cached is not None:
            return cached
        resolved = _storage_backend(self.registry, destination, backend_options)
        self._resolved_backend_cache[cache_key] = resolved
        return resolved


@mypyc_attr(allow_interpreted_subclasses=True)
class SyncStoragePipeline(_StoragePipelineBase):
    """Pipeline coordinating storage registry operations and telemetry."""

    __slots__ = ()

    def write_rows(
        self,
        rows: "list[dict[str, Any]]",
        destination: StorageDestination,
        *,
        format_hint: StorageFormat | None = None,
        storage_options: "dict[str, Any] | None" = None,
    ) -> StorageTelemetry:
        """Write dictionary rows to storage using cached serializers."""

        format_choice = format_hint or "jsonl"
        _validate_row_write_format(format_choice)
        serialized = serialize_collection(rows)
        payload = _encode_row_payload(serialized, format_choice)
        resolved_options = _storage_options(self._storage_options, storage_options)
        return self._write_bytes(
            payload, destination, rows=len(serialized), format_label=format_choice, storage_options=resolved_options
        )

    def write_arrow(
        self,
        table: "ArrowTable",
        destination: StorageDestination,
        *,
        format_hint: StorageFormat | None = None,
        storage_options: "dict[str, Any] | None" = None,
        compression: str | None = None,
    ) -> StorageTelemetry:
        """Write an Arrow table to storage using zero-copy buffers."""

        format_choice = format_hint or "parquet"
        _validate_arrow_write_format(format_choice)
        resolved_options = _storage_options(self._storage_options, storage_options)
        format_write_options = _csv_write_options(
            format_choice, resolved_options, self._storage_options, self._csv_write_options
        )
        payload = _encode_arrow_payload(
            table, format_choice, compression=compression, write_options=format_write_options
        )
        return self._write_bytes(
            payload, destination, rows=int(table.num_rows), format_label=format_choice, storage_options=resolved_options
        )

    def read_arrow(
        self, source: StorageDestination, *, file_format: StorageFormat, storage_options: "dict[str, Any] | None" = None
    ) -> "tuple[ArrowTable, StorageTelemetry]":
        """Read an artifact from storage and decode it into an Arrow table."""

        backend, path, backend_name = self._backend(source, storage_options)
        payload = _read_backend_sync(backend, path, backend_name=backend_name)
        table = _decode_arrow_payload(payload, file_format)
        rows_processed = int(table.num_rows)
        telemetry: StorageTelemetry = {
            "destination": path,
            "bytes_processed": len(payload),
            "rows_processed": rows_processed,
            "format": file_format,
            "backend": backend_name,
        }
        return table, telemetry

    def stream_read(
        self,
        source: StorageDestination,
        *,
        chunk_size: int | None = None,
        storage_options: "dict[str, Any] | None" = None,
    ) -> "Iterator[bytes]":
        """Stream bytes from an artifact."""
        backend, path, _backend_name = self._backend(source, storage_options)
        return backend.stream_read_sync(path, chunk_size=chunk_size)

    def read_arrow_batches(
        self,
        source: StorageDestination,
        *,
        file_format: str = "parquet",
        batch_size: int = 65_536,
        arrow_schema: Any = None,
        storage_options: "dict[str, Any] | None" = None,
    ) -> "Iterator[ArrowRecordBatch]":
        """Stream Arrow record batches from storage synchronously."""
        return _read_arrow_batches_sync(
            self,
            source,
            file_format=file_format,
            batch_size=batch_size,
            arrow_schema=arrow_schema,
            storage_options=storage_options,
        )

    def write_arrow_batches(
        self,
        destination: StorageDestination,
        batches: Any,
        *,
        file_format: StorageFormat = "parquet",
        compression: str | None = None,
        write_options: "dict[str, Any] | None" = None,
        storage_options: "dict[str, Any] | None" = None,
    ) -> StorageTelemetry:
        """Write Arrow record batches to storage synchronously."""
        _validate_arrow_write_format(file_format)
        resolved_options = _storage_options(self._storage_options, storage_options)
        format_write_options = write_options or _csv_write_options(
            file_format, resolved_options, self._storage_options, self._csv_write_options
        )
        table = batches_to_arrow_table(batches)
        payload = _encode_arrow_payload(table, file_format, compression=compression, write_options=format_write_options)
        return self._write_bytes(
            payload, destination, rows=int(table.num_rows), format_label=file_format, storage_options=resolved_options
        )

    def list_sources(
        self,
        source: StorageDestination,
        *,
        file_format: str | None = None,
        storage_options: "dict[str, Any] | None" = None,
    ) -> "list[str]":
        """Expand a single URI, directory prefix, or glob pattern into sorted source URIs."""
        return _resolve_storage_sources_sync(self, source, file_format=file_format, storage_options=storage_options)

    def _write_bytes(
        self,
        payload: bytes,
        destination: StorageDestination,
        *,
        rows: int,
        format_label: str,
        storage_options: "dict[str, Any]",
    ) -> StorageTelemetry:
        backend, path, backend_name = self._backend(destination, storage_options)
        start = perf_counter()
        _write_backend_sync(backend, path, payload, backend_name=backend_name)
        elapsed = perf_counter() - start
        bytes_written = len(payload)
        _METRICS.record_bytes(bytes_written)
        telemetry: StorageTelemetry = {
            "destination": path,
            "bytes_processed": bytes_written,
            "rows_processed": rows,
            "duration_s": elapsed,
            "format": format_label,
            "backend": backend_name,
        }
        return telemetry


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncStoragePipeline(_StoragePipelineBase):
    """Async variant of the storage pipeline leveraging async-capable backends when available."""

    __slots__ = ()

    async def write_rows(
        self,
        rows: "list[dict[str, Any]]",
        destination: StorageDestination,
        *,
        format_hint: StorageFormat | None = None,
        storage_options: "dict[str, Any] | None" = None,
    ) -> StorageTelemetry:
        format_choice = format_hint or "jsonl"
        _validate_row_write_format(format_choice)
        payload, row_count = await async_(_serialize_and_encode_row_payload)(rows, format_choice)
        resolved_options = _storage_options(self._storage_options, storage_options)
        return await self._write_bytes_async(
            payload, destination, rows=row_count, format_label=format_choice, storage_options=resolved_options
        )

    async def write_arrow(
        self,
        table: "ArrowTable",
        destination: StorageDestination,
        *,
        format_hint: StorageFormat | None = None,
        storage_options: "dict[str, Any] | None" = None,
        compression: str | None = None,
    ) -> StorageTelemetry:
        format_choice = format_hint or "parquet"
        _validate_arrow_write_format(format_choice)
        resolved_options = _storage_options(self._storage_options, storage_options)
        format_write_options = _csv_write_options(
            format_choice, resolved_options, self._storage_options, self._csv_write_options
        )
        payload = await async_(_encode_arrow_payload)(
            table, format_choice, compression=compression, write_options=format_write_options
        )
        return await self._write_bytes_async(
            payload, destination, rows=int(table.num_rows), format_label=format_choice, storage_options=resolved_options
        )

    async def read_arrow_async(
        self, source: StorageDestination, *, file_format: StorageFormat, storage_options: "dict[str, Any] | None" = None
    ) -> "tuple[ArrowTable, StorageTelemetry]":
        backend, path, backend_name = self._backend(source, storage_options)
        if supports_async_read_bytes(backend):
            payload = await execute_async_storage_operation(
                partial(backend.read_bytes_async, path), backend=backend_name, operation="read_bytes", path=path
            )
        else:
            payload = await async_(_read_backend_sync)(backend=backend, path=path, backend_name=backend_name)

        table = await async_(_decode_arrow_payload)(payload, file_format)
        rows_processed = int(table.num_rows)
        telemetry: StorageTelemetry = {
            "destination": path,
            "bytes_processed": len(payload),
            "rows_processed": rows_processed,
            "format": file_format,
            "backend": backend_name,
        }
        return table, telemetry

    async def stream_read_async(
        self,
        source: StorageDestination,
        *,
        chunk_size: int | None = None,
        storage_options: "dict[str, Any] | None" = None,
    ) -> "AsyncIterator[bytes]":
        """Stream bytes from an artifact asynchronously."""
        backend, path, _backend_name = self._backend(source, storage_options)
        return await backend.stream_read_async(path, chunk_size=chunk_size)

    def read_arrow_batches_async(
        self,
        source: StorageDestination,
        *,
        file_format: str = "parquet",
        batch_size: int = 65_536,
        arrow_schema: Any = None,
        storage_options: "dict[str, Any] | None" = None,
    ) -> "AsyncIterator[ArrowRecordBatch]":
        """Stream Arrow record batches from storage asynchronously."""
        return AsyncArrowBatchIterator(
            _read_arrow_batches_sync(
                self,
                source,
                file_format=file_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
                storage_options=storage_options,
            )
        )

    async def write_arrow_batches_async(
        self,
        destination: StorageDestination,
        batches: Any,
        *,
        file_format: StorageFormat = "parquet",
        compression: str | None = None,
        write_options: "dict[str, Any] | None" = None,
        storage_options: "dict[str, Any] | None" = None,
    ) -> StorageTelemetry:
        """Write Arrow record batches to storage asynchronously."""
        _validate_arrow_write_format(file_format)
        resolved_options = _storage_options(self._storage_options, storage_options)
        format_write_options = write_options or _csv_write_options(
            file_format, resolved_options, self._storage_options, self._csv_write_options
        )
        table = await async_(batches_to_arrow_table)(batches)
        payload = await async_(_encode_arrow_payload)(
            table, file_format, compression=compression, write_options=format_write_options
        )
        return await self._write_bytes_async(
            payload, destination, rows=int(table.num_rows), format_label=file_format, storage_options=resolved_options
        )

    async def list_sources_async(
        self,
        source: StorageDestination,
        *,
        file_format: str | None = None,
        storage_options: "dict[str, Any] | None" = None,
    ) -> "list[str]":
        """Expand a single URI, directory prefix, or glob pattern into sorted source URIs asynchronously."""
        return await async_(_resolve_storage_sources_sync)(
            self, source, file_format=file_format, storage_options=storage_options
        )

    async def _write_bytes_async(
        self,
        payload: bytes,
        destination: StorageDestination,
        *,
        rows: int,
        format_label: str,
        storage_options: "dict[str, Any]",
    ) -> StorageTelemetry:
        backend, path, backend_name = self._backend(destination, storage_options)
        start = perf_counter()
        if supports_async_write_bytes(backend):
            await execute_async_storage_operation(
                partial(backend.write_bytes_async, path, payload),
                backend=backend_name,
                operation="write_bytes",
                path=path,
            )
        else:
            await async_(_write_backend_sync)(backend=backend, path=path, payload=payload, backend_name=backend_name)

        elapsed = perf_counter() - start
        bytes_written = len(payload)
        _METRICS.record_bytes(bytes_written)
        telemetry: StorageTelemetry = {
            "destination": path,
            "bytes_processed": bytes_written,
            "rows_processed": rows,
            "duration_s": elapsed,
            "format": format_label,
            "backend": backend_name,
        }
        return telemetry


def _storage_options(default_options: "dict[str, Any]", storage_options: "dict[str, Any] | None") -> "dict[str, Any]":
    return default_options if storage_options is None else storage_options


def _csv_write_options_from_kwargs(storage_options: "dict[str, Any]") -> "dict[str, Any] | None":
    return cast("dict[str, Any] | None", storage_options.get("write_options"))


def _csv_write_options(
    format_choice: StorageFormat,
    resolved_options: "dict[str, Any]",
    default_options: "dict[str, Any]",
    default_write_options: "dict[str, Any] | None",
) -> "dict[str, Any] | None":
    if format_choice != "csv":
        return None
    if resolved_options is default_options:
        return default_write_options
    return _csv_write_options_from_kwargs(resolved_options)


def _encode_row_payload(rows: "list[Any]", format_hint: StorageFormat) -> bytes:
    if format_hint == "json":
        return to_json(rows, as_bytes=True)
    buffer = bytearray()
    for row in rows:
        buffer.extend(to_json(row, as_bytes=True))
        buffer.extend(b"\n")
    return bytes(buffer)


def _serialize_and_encode_row_payload(rows: "list[dict[str, Any]]", format_hint: StorageFormat) -> "tuple[bytes, int]":
    serialized = serialize_collection(rows)
    return _encode_row_payload(serialized, format_hint), len(serialized)


def _validate_arrow_write_format(format_choice: StorageFormat) -> None:
    """Reject Arrow-table writes for formats that cannot carry an Arrow payload.

    Args:
        format_choice: Requested storage format.

    Raises:
        StorageCapabilityError: If the format is not an Arrow write format.
    """
    if format_choice not in _ARROW_WRITE_FORMATS:
        msg = "Arrow storage writes support only Parquet, Arrow IPC, and CSV formats"
        raise StorageCapabilityError(
            msg, capability="arrow_write", remediation="Write row payloads with the row storage APIs instead."
        )


def _validate_row_write_format(format_choice: StorageFormat) -> None:
    """Reject row writes for formats that cannot carry a row payload.

    Args:
        format_choice: Requested storage format.

    Raises:
        StorageCapabilityError: If the format is not a row write format.
    """
    if format_choice not in _ROW_WRITE_FORMATS:
        msg = "Row storage writes support only JSON and JSONL formats"
        raise StorageCapabilityError(
            msg, capability="row_write", remediation="Write Arrow tables with the Arrow storage APIs instead."
        )


def _encode_arrow_payload(
    table: "ArrowTable",
    format_choice: StorageFormat,
    *,
    compression: str | None,
    write_options: "dict[str, Any] | None" = None,
) -> bytes:
    return encode_arrow_payload(table, format_choice, compression=compression, write_options=write_options)


def _write_backend_sync(backend: "ObjectStoreProtocol", path: str, payload: bytes, *, backend_name: str) -> None:
    execute_sync_storage_operation(
        partial(backend.write_bytes_sync, path, payload), backend=backend_name, operation="write_bytes", path=path
    )


def _read_backend_sync(backend: "ObjectStoreProtocol", path: str, *, backend_name: str) -> bytes:
    return execute_sync_storage_operation(
        partial(backend.read_bytes_sync, path), backend=backend_name, operation="read_bytes", path=path
    )


def _decode_arrow_payload(payload: bytes, format_choice: StorageFormat) -> "ArrowTable":
    return decode_arrow_payload(payload, format_choice)


def _resolve_alias_destination(
    registry: StorageRegistry, destination: str, backend_options: "dict[str, Any]"
) -> "tuple[ObjectStoreProtocol, str, str] | None":
    if not destination.startswith("alias://"):
        return None
    payload = destination.removeprefix("alias://")
    alias_name, _, relative_path = payload.partition("/")
    alias = alias_name.strip()
    if not alias:
        msg = "Alias destinations must include a registry alias before the path component"
        raise ImproperConfigurationError(msg)
    path_segment = relative_path.strip()
    if not path_segment:
        msg = "Alias destinations must include an object path after the alias name"
        raise ImproperConfigurationError(msg)
    backend = registry.get(alias, **backend_options)
    return backend, path_segment.lstrip("/"), backend.backend_type


def _backend_path(destination: str) -> str:
    if destination.startswith("file://"):
        return parse_file_uri_path(destination)
    if "://" in destination:
        scheme, remainder = destination.split("://", 1)
        if scheme.lower() in _CLOUD_OBJECT_STORE_SCHEMES:
            parsed = urlparse(destination)
            if parsed.netloc:
                return parsed.path.lstrip("/")
        return remainder.lstrip("/")
    return destination


def _storage_backend(
    registry: StorageRegistry, destination: StorageDestination, backend_options: "dict[str, Any] | None"
) -> "tuple[ObjectStoreProtocol, str, str]":
    destination_str = destination.as_posix() if isinstance(destination, Path) else str(destination)
    options = _EMPTY_STORAGE_OPTIONS if backend_options is None else backend_options
    alias_resolution = _resolve_alias_destination(registry, destination_str, options)
    if alias_resolution is not None:
        return alias_resolution
    backend = registry.get(destination_str, **options)
    normalized_path = _backend_path(destination_str)
    if getattr(backend, "protocol", None) == FILE_PROTOCOL:
        normalized_path = Path(normalized_path).expanduser().resolve().as_posix()
    return backend, normalized_path, backend.backend_type


def _backend_cache_key(destination: StorageDestination, backend_options: "dict[str, Any] | None") -> "str | None":
    if backend_options:
        return None
    return destination.as_posix() if isinstance(destination, Path) else str(destination)


def _read_arrow_batches_sync(
    pipeline: _StoragePipelineBase,
    source: StorageDestination,
    *,
    file_format: str = "parquet",
    batch_size: int = 65_536,
    arrow_schema: Any = None,
    storage_options: "dict[str, Any] | None" = None,
) -> "Iterator[ArrowRecordBatch]":
    normalized_format = file_format.lower().strip()
    backend, path, _backend_name = pipeline._backend(source, storage_options)
    if normalized_format == "parquet":
        return backend.stream_arrow_sync(path, file_format="parquet", batch_size=batch_size, arrow_schema=arrow_schema)
    return iter_streamed_arrow_batches(
        backend.stream_read_sync(path), file_format=normalized_format, batch_size=batch_size, arrow_schema=arrow_schema
    )


def _resolve_storage_sources_sync(
    pipeline: _StoragePipelineBase,
    source: StorageDestination,
    *,
    file_format: str | None = None,
    storage_options: "dict[str, Any] | None" = None,
) -> "list[str]":
    source_str = source.as_posix() if isinstance(source, Path) else str(source)
    backend, path, _backend_name = pipeline._backend(source, storage_options)
    has_glob = any(ch in path for ch in ("*", "?", "["))
    if has_glob:
        matched = backend.glob_sync(path)
    elif source_str.endswith("/") or backend.is_path_sync(path):
        matched = backend.list_objects_sync(prefix=path, recursive=True)
        if file_format:
            suffix = f".{file_format.lower().lstrip('.')}"
            matched = [item for item in matched if item.lower().endswith(suffix)]
    else:
        return [source_str]
    sorted_matches = sorted(matched)
    if source_str.startswith("alias://"):
        alias_name = source_str.removeprefix("alias://").partition("/")[0]
        return [f"alias://{alias_name}/{item.lstrip('/')}" for item in sorted_matches]
    return [backend.resolve_uri(item) for item in sorted_matches]
