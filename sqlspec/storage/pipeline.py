"""Storage pipeline scaffolding for driver-aware storage bridge."""

from collections import deque
from functools import partial
from pathlib import Path
from time import perf_counter, time
from typing import TYPE_CHECKING, Any, NamedTuple, TypeAlias, cast
from uuid import uuid4

from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired, TypedDict

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.storage._arrow_payload import StorageFormat, decode_arrow_payload, encode_arrow_payload
from sqlspec.storage.errors import execute_async_storage_operation, execute_sync_storage_operation
from sqlspec.storage.registry import StorageRegistry, storage_registry
from sqlspec.utils.serializers import get_serializer_metrics, serialize_collection, to_json
from sqlspec.utils.sync_tools import async_
from sqlspec.utils.type_guards import supports_async_delete, supports_async_read_bytes, supports_async_write_bytes

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlspec.protocols import ObjectStoreProtocol
    from sqlspec.typing import ArrowTable


__all__ = (
    "AsyncStoragePipeline",
    "PartitionStrategyConfig",
    "StagedArtifact",
    "StorageBridgeJob",
    "StorageCapabilities",
    "StorageDestination",
    "StorageDiagnostics",
    "StorageFormat",
    "StorageLoadRequest",
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
    arrow_import_enabled: bool
    parquet_export_enabled: bool
    parquet_import_enabled: bool
    requires_staging_for_load: bool
    staging_protocols: "list[str]"
    partition_strategies: "list[str]"
    default_storage_profile: NotRequired[str | None]


class PartitionStrategyConfig(TypedDict, total=False):
    """Configuration for partition fan-out strategies."""

    kind: str
    partitions: int
    rows_per_chunk: int
    manifest_path: str


class StorageLoadRequest(TypedDict):
    """Request describing a staging allocation."""

    partition_id: str
    destination_uri: str
    ttl_seconds: int
    correlation_id: str
    source_uri: NotRequired[str]


class StagedArtifact(TypedDict):
    """Metadata describing a staged artifact managed by the pipeline."""

    partition_id: str
    uri: str
    cleanup_token: str
    ttl_seconds: int
    expires_at: float
    correlation_id: str


class StorageTelemetry(TypedDict, total=False):
    """Telemetry payload for storage bridge operations."""

    destination: str
    bytes_processed: int
    rows_processed: int
    partitions_created: int
    duration_s: float
    format: str
    extra: "dict[str, object]"
    backend: str
    correlation_id: str
    config: str
    bind_key: str


class StorageBridgeJob(NamedTuple):
    """Handle representing a storage bridge operation."""

    job_id: str
    status: str
    telemetry: StorageTelemetry


class _StorageBridgeMetrics:
    __slots__ = ("bytes_written", "partitions_created")

    def __init__(self) -> None:
        self.bytes_written = 0
        self.partitions_created = 0

    def record_bytes(self, count: int) -> None:
        self.bytes_written += max(count, 0)

    def record_partitions(self, count: int) -> None:
        self.partitions_created += max(count, 0)

    def snapshot(self) -> "dict[str, int]":
        return {
            "storage_bridge.bytes_written": self.bytes_written,
            "storage_bridge.partitions_created": self.partitions_created,
        }

    def reset(self) -> None:
        self.bytes_written = 0
        self.partitions_created = 0


_METRICS = _StorageBridgeMetrics()
_RECENT_STORAGE_EVENTS: "deque[StorageTelemetry]" = deque(maxlen=25)
_EMPTY_STORAGE_OPTIONS: dict[str, Any] = {}


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


def get_storage_bridge_metrics() -> "dict[str, int]":
    """Return aggregated storage bridge metrics."""

    return _METRICS.snapshot()


def reset_storage_bridge_metrics() -> None:
    """Reset aggregated storage bridge metrics."""

    _METRICS.reset()


def record_storage_diagnostic_event(telemetry: StorageTelemetry) -> None:
    """Record telemetry for inclusion in diagnostics snapshots."""

    _RECENT_STORAGE_EVENTS.append(cast("StorageTelemetry", dict(telemetry)))


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


def _encode_row_payload(rows: "list[Any]", format_hint: StorageFormat) -> bytes:
    if format_hint == "json":
        return to_json(rows, as_bytes=True)
    buffer = bytearray()
    for row in rows:
        buffer.extend(to_json(row, as_bytes=True))
        buffer.extend(b"\n")
    return bytes(buffer)


def _encode_arrow_payload(
    table: "ArrowTable",
    format_choice: StorageFormat,
    *,
    compression: str | None,
    write_options: "dict[str, Any] | None" = None,
) -> bytes:
    return encode_arrow_payload(table, format_choice, compression=compression, write_options=write_options)


def _delete_backend_sync(backend: "ObjectStoreProtocol", path: str, *, backend_name: str) -> None:
    execute_sync_storage_operation(
        partial(backend.delete_sync, path), backend=backend_name, operation="delete", path=path
    )


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
        return destination.removeprefix("file://")
    if "://" in destination:
        _, remainder = destination.split("://", 1)
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
    return backend, normalized_path, backend.backend_type


def _backend_cache_key(destination: StorageDestination, backend_options: "dict[str, Any] | None") -> "str | None":
    if backend_options:
        return None
    return destination.as_posix() if isinstance(destination, Path) else str(destination)


@mypyc_attr(allow_interpreted_subclasses=True)
class _StoragePipelineBase:
    """Shared registry and backend-resolution state for storage pipelines."""

    __slots__ = ("_csv_write_options", "_resolved_backend_cache", "_storage_options", "registry")

    def __init__(
        self, *, registry: StorageRegistry | None = None, storage_options: "dict[str, Any] | None" = None
    ) -> None:
        self.registry = registry or storage_registry
        self._resolved_backend_cache: dict[str, tuple[ObjectStoreProtocol, str, str]] = {}
        self._storage_options = _EMPTY_STORAGE_OPTIONS if storage_options is None else storage_options
        self._csv_write_options = _csv_write_options_from_kwargs(self._storage_options)

    def clear_cache(self) -> None:
        """Clear cached storage backend resolutions for this pipeline instance."""
        self._resolved_backend_cache.clear()

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

        serialized = serialize_collection(rows)
        format_choice = format_hint or "jsonl"
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

    def allocate_staging_artifacts(self, requests: "list[StorageLoadRequest]") -> "list[StagedArtifact]":
        """Allocate staging metadata for upcoming loads."""

        artifacts: list[StagedArtifact] = []
        now = time()

        for request in requests:
            ttl = max(request["ttl_seconds"], 0)
            cleanup_token = f"{request['correlation_id']}::{request['partition_id']}"
            artifacts.append({
                "partition_id": request["partition_id"],
                "uri": request["destination_uri"],
                "cleanup_token": cleanup_token,
                "ttl_seconds": ttl,
                "expires_at": now + ttl if ttl else now,
                "correlation_id": request["correlation_id"],
            })
        if artifacts:
            _METRICS.record_partitions(len(artifacts))
        return artifacts

    def cleanup_staging_artifacts(self, artifacts: "list[StagedArtifact]", *, ignore_errors: bool = True) -> None:
        """Delete staged artifacts best-effort."""

        for artifact in artifacts:
            backend, path, backend_name = self._backend(artifact["uri"], None)
            try:
                _delete_backend_sync(backend, path, backend_name=backend_name)
            except Exception:
                if not ignore_errors:
                    raise

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
        serialized = serialize_collection(rows)
        format_choice = format_hint or "jsonl"
        payload = await async_(_encode_row_payload)(serialized, format_choice)
        resolved_options = _storage_options(self._storage_options, storage_options)
        return await self._write_bytes_async(
            payload, destination, rows=len(serialized), format_label=format_choice, storage_options=resolved_options
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

    async def cleanup_staging_artifacts(self, artifacts: "list[StagedArtifact]", *, ignore_errors: bool = True) -> None:
        for artifact in artifacts:
            backend, path, backend_name = self._backend(artifact["uri"], None)
            if supports_async_delete(backend):
                try:
                    await execute_async_storage_operation(
                        partial(backend.delete_async, path), backend=backend_name, operation="delete", path=path
                    )
                except Exception:
                    if not ignore_errors:
                        raise
                continue

            try:
                await async_(_delete_backend_sync)(backend=backend, path=path, backend_name=backend_name)
            except Exception:
                if not ignore_errors:
                    raise

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
