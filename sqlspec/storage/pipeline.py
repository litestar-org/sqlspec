"""Storage pipeline scaffolding for driver-aware storage bridge."""

from functools import partial
from pathlib import Path
from time import perf_counter, time
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, TypeAlias, cast
from uuid import uuid4

from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired, TypedDict

from sqlspec.storage._utils import import_pyarrow, import_pyarrow_parquet
from sqlspec.storage.errors import execute_async_storage_operation, execute_sync_storage_operation
from sqlspec.storage.registry import StorageRegistry, storage_registry
from sqlspec.utils.serializers import from_json, get_serializer_metrics, serialize_collection, to_json
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.protocols import ObjectStoreProtocol
    from sqlspec.typing import ArrowTable


__all__ = (
    "AsyncStoragePipeline",
    "PartitionStrategyConfig",
    "StagedArtifact",
    "StorageBridgeJob",
    "StorageCapabilities",
    "StorageDestination",
    "StorageFormat",
    "StorageLoadRequest",
    "StorageTelemetry",
    "SyncStoragePipeline",
    "create_storage_bridge_job",
    "get_storage_bridge_diagnostics",
    "get_storage_bridge_metrics",
    "reset_storage_bridge_metrics",
)

StorageFormat = Literal["jsonl", "json", "parquet", "arrow-ipc"]
StorageDestination: TypeAlias = str | Path


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
    extra: "dict[str, Any]"
    backend: str


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


def get_storage_bridge_metrics() -> "dict[str, int]":
    """Return aggregated storage bridge metrics."""

    return _METRICS.snapshot()


def reset_storage_bridge_metrics() -> None:
    """Reset aggregated storage bridge metrics."""

    _METRICS.reset()


def create_storage_bridge_job(status: str, telemetry: StorageTelemetry) -> StorageBridgeJob:
    """Create a storage bridge job handle with a unique identifier."""

    return StorageBridgeJob(job_id=str(uuid4()), status=status, telemetry=telemetry)


def get_storage_bridge_diagnostics() -> "dict[str, int]":
    """Return aggregated storage bridge + serializer cache metrics."""

    diagnostics = dict(get_storage_bridge_metrics())
    serializer_metrics = get_serializer_metrics()
    for key, value in serializer_metrics.items():
        diagnostics[f"serializer.{key}"] = value
    return diagnostics


def _encode_row_payload(rows: "list[Any]", format_hint: StorageFormat) -> bytes:
    if format_hint == "json":
        data = to_json(rows, as_bytes=True)
        if isinstance(data, bytes):
            return data
        return data.encode()
    buffer = bytearray()
    for row in rows:
        buffer.extend(to_json(row, as_bytes=True))
        buffer.extend(b"\n")
    return bytes(buffer)


def _encode_arrow_payload(table: "ArrowTable", format_choice: StorageFormat, *, compression: str | None) -> bytes:
    pa = import_pyarrow()
    sink = pa.BufferOutputStream()
    if format_choice == "arrow-ipc":
        writer = pa.ipc.new_file(sink, table.schema)
        writer.write_table(table)
        writer.close()
    else:
        pq = import_pyarrow_parquet()
        pq.write_table(table, sink, compression=compression)
    buffer = sink.getvalue()
    result_bytes: bytes = buffer.to_pybytes()
    return result_bytes


def _decode_arrow_payload(payload: bytes, format_choice: StorageFormat) -> "ArrowTable":
    pa = import_pyarrow()
    if format_choice == "parquet":
        pq = import_pyarrow_parquet()
        return cast("ArrowTable", pq.read_table(pa.BufferReader(payload)))
    if format_choice == "arrow-ipc":
        reader = pa.ipc.open_file(pa.BufferReader(payload))
        return cast("ArrowTable", reader.read_all())
    text_payload = payload.decode()
    if format_choice == "json":
        data = from_json(text_payload)
        rows = data if isinstance(data, list) else [data]
        return cast("ArrowTable", pa.Table.from_pylist(rows))
    if format_choice == "jsonl":
        rows = [from_json(line) for line in text_payload.splitlines() if line.strip()]
        return cast("ArrowTable", pa.Table.from_pylist(rows))
    msg = f"Unsupported storage format for Arrow decoding: {format_choice}"
    raise ValueError(msg)


@mypyc_attr(allow_interpreted_subclasses=True)
class SyncStoragePipeline:
    """Pipeline coordinating storage registry operations and telemetry."""

    __slots__ = ("registry",)

    def __init__(self, *, registry: StorageRegistry | None = None) -> None:
        self.registry = registry or storage_registry

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
        return self._write_bytes(
            payload,
            destination,
            rows=len(serialized),
            format_label=format_choice,
            storage_options=storage_options or {},
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
        payload = _encode_arrow_payload(table, format_choice, compression=compression)
        return self._write_bytes(
            payload,
            destination,
            rows=int(getattr(table, "num_rows", 0)),
            format_label=format_choice,
            storage_options=storage_options or {},
        )

    def read_arrow(
        self, source: StorageDestination, *, file_format: StorageFormat, storage_options: "dict[str, Any] | None" = None
    ) -> "tuple[ArrowTable, StorageTelemetry]":
        """Read an artifact from storage and decode it into an Arrow table."""

        backend, path = self._resolve_backend(source, **(storage_options or {}))
        backend_name = getattr(backend, "backend_type", "storage")
        payload = execute_sync_storage_operation(
            partial(backend.read_bytes, path), backend=backend_name, operation="read_bytes", path=path
        )
        table = _decode_arrow_payload(payload, file_format)
        telemetry: StorageTelemetry = {
            "destination": path,
            "bytes_processed": len(payload),
            "rows_processed": int(getattr(table, "num_rows", 0)),
            "format": file_format,
            "backend": backend_name,
        }
        return table, telemetry

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
            backend, path = self._resolve_backend(artifact["uri"])
            try:
                execute_sync_storage_operation(
                    partial(backend.delete, path),
                    backend=getattr(backend, "backend_type", "storage"),
                    operation="delete",
                    path=path,
                )
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
        backend, path = self._resolve_backend(destination, **storage_options)
        backend_name = getattr(backend, "backend_type", "storage")
        start = perf_counter()
        execute_sync_storage_operation(
            partial(backend.write_bytes, path, payload), backend=backend_name, operation="write_bytes", path=path
        )
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

    def _resolve_backend(
        self, destination: StorageDestination, **backend_options: Any
    ) -> "tuple[ObjectStoreProtocol, str]":
        destination_str = destination.as_posix() if isinstance(destination, Path) else str(destination)
        backend = self.registry.get(destination_str, **backend_options)
        normalized_path = self._normalize_path_for_backend(destination_str)
        return backend, normalized_path

    def _normalize_path_for_backend(self, destination: str) -> str:
        if destination.startswith("file://"):
            return destination.removeprefix("file://")
        if "://" in destination:
            _, remainder = destination.split("://", 1)
            return remainder.lstrip("/")
        return destination


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncStoragePipeline:
    """Async variant of the storage pipeline leveraging async-capable backends when available."""

    __slots__ = ("registry",)

    def __init__(self, *, registry: StorageRegistry | None = None) -> None:
        self.registry = registry or storage_registry

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
        payload = _encode_row_payload(serialized, format_choice)
        return await self._write_bytes_async(
            payload,
            destination,
            rows=len(serialized),
            format_label=format_choice,
            storage_options=storage_options or {},
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
        payload = _encode_arrow_payload(table, format_choice, compression=compression)
        return await self._write_bytes_async(
            payload,
            destination,
            rows=int(getattr(table, "num_rows", 0)),
            format_label=format_choice,
            storage_options=storage_options or {},
        )

    async def cleanup_staging_artifacts(self, artifacts: "list[StagedArtifact]", *, ignore_errors: bool = True) -> None:
        for artifact in artifacts:
            backend, path = self._resolve_backend(artifact["uri"])
            backend_name = getattr(backend, "backend_type", "storage")
            delete_async = getattr(backend, "delete_async", None)
            if delete_async is not None:
                try:
                    await execute_async_storage_operation(
                        partial(delete_async, path), backend=backend_name, operation="delete", path=path
                    )
                except Exception:
                    if not ignore_errors:
                        raise
                continue

            def _delete_sync(
                backend: "ObjectStoreProtocol" = backend, path: str = path, backend_name: str = backend_name
            ) -> None:
                execute_sync_storage_operation(
                    partial(backend.delete, path), backend=backend_name, operation="delete", path=path
                )

            try:
                await async_(_delete_sync)()
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
        backend, path = self._resolve_backend(destination, **storage_options)
        backend_name = getattr(backend, "backend_type", "storage")
        writer = getattr(backend, "write_bytes_async", None)
        start = perf_counter()
        if writer is not None:
            await execute_async_storage_operation(
                partial(writer, path, payload), backend=backend_name, operation="write_bytes", path=path
            )
        else:

            def _write_sync(
                backend: "ObjectStoreProtocol" = backend,
                path: str = path,
                payload: bytes = payload,
                backend_name: str = backend_name,
            ) -> None:
                execute_sync_storage_operation(
                    partial(backend.write_bytes, path, payload),
                    backend=backend_name,
                    operation="write_bytes",
                    path=path,
                )

            await async_(_write_sync)()

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
        backend, path = self._resolve_backend(source, **(storage_options or {}))
        backend_name = getattr(backend, "backend_type", "storage")
        reader = getattr(backend, "read_bytes_async", None)
        if reader is not None:
            payload = await execute_async_storage_operation(
                partial(reader, path), backend=backend_name, operation="read_bytes", path=path
            )
        else:

            def _read_sync(
                backend: "ObjectStoreProtocol" = backend, path: str = path, backend_name: str = backend_name
            ) -> bytes:
                return execute_sync_storage_operation(
                    partial(backend.read_bytes, path), backend=backend_name, operation="read_bytes", path=path
                )

            payload = await async_(_read_sync)()

        table = _decode_arrow_payload(payload, file_format)
        telemetry: StorageTelemetry = {
            "destination": path,
            "bytes_processed": len(payload),
            "rows_processed": int(getattr(table, "num_rows", 0)),
            "format": file_format,
            "backend": backend_name,
        }
        return table, telemetry

    def _resolve_backend(
        self, destination: StorageDestination, **backend_options: Any
    ) -> "tuple[ObjectStoreProtocol, str]":
        destination_str = destination.as_posix() if isinstance(destination, Path) else str(destination)
        backend = self.registry.get(destination_str, **backend_options)
        normalized_path = self._normalize_path_for_backend(destination_str)
        return backend, normalized_path

    def _normalize_path_for_backend(self, destination: str) -> str:
        if destination.startswith("file://"):
            return destination.removeprefix("file://")
        if "://" in destination:
            _, remainder = destination.split("://", 1)
            return remainder.lstrip("/")
        return destination
