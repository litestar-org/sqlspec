"""Storage bridge mixin shared by sync and async drivers."""

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import StorageCapabilityError
from sqlspec.storage import (
    AsyncStoragePipeline,
    StorageBridgeJob,
    StorageCapabilities,
    StorageDestination,
    StorageFormat,
    StorageTelemetry,
    SyncStoragePipeline,
    create_storage_bridge_job,
)
from sqlspec.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from sqlspec.core import StatementConfig, StatementFilter
    from sqlspec.core.result import ArrowResult
    from sqlspec.core.statement import SQL
    from sqlspec.observability import ObservabilityRuntime
    from sqlspec.typing import ArrowTable, StatementParameters

__all__ = ("StorageDriverMixin",)


CAPABILITY_HINTS: dict[str, str] = {
    "arrow_export_enabled": "native Arrow export",
    "arrow_import_enabled": "native Arrow import",
    "parquet_export_enabled": "native Parquet export",
    "parquet_import_enabled": "native Parquet import",
}


@mypyc_attr(allow_interpreted_subclasses=True)
class StorageDriverMixin:
    """Mixin providing capability-aware storage bridge helpers."""

    __slots__ = ()
    storage_pipeline_factory: "type[SyncStoragePipeline | AsyncStoragePipeline] | None" = None
    driver_features: dict[str, Any]

    if TYPE_CHECKING:

        @property
        def observability(self) -> "ObservabilityRuntime": ...

    def storage_capabilities(self) -> StorageCapabilities:
        """Return cached storage capabilities for the active driver."""

        capabilities = self.driver_features.get("storage_capabilities")
        if capabilities is None:
            msg = "Storage capabilities are not configured for this driver."
            raise StorageCapabilityError(msg, capability="storage_capabilities")
        return cast("StorageCapabilities", dict(capabilities))

    def select_to_storage(
        self,
        statement: "SQL | str",
        destination: StorageDestination,
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, Any] | None" = None,
        format_hint: StorageFormat | None = None,
        telemetry: StorageTelemetry | None = None,
    ) -> "StorageBridgeJob | Awaitable[StorageBridgeJob]":
        """Stream a SELECT statement directly into storage."""

        self._raise_not_implemented("select_to_storage")
        raise NotImplementedError

    def select_to_arrow(
        self,
        statement: "SQL | str",
        /,
        *parameters: "StatementParameters | StatementFilter",
        partitioner: "dict[str, Any] | None" = None,
        memory_pool: Any | None = None,
        statement_config: "StatementConfig | None" = None,
    ) -> "ArrowResult | Awaitable[ArrowResult]":
        """Execute a SELECT that returns an ArrowResult."""

        self._raise_not_implemented("select_to_arrow")
        raise NotImplementedError

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, Any] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob | Awaitable[StorageBridgeJob]":
        """Load Arrow data into the target table."""

        self._raise_not_implemented("load_from_arrow")
        raise NotImplementedError

    def load_from_storage(
        self,
        table: str,
        source: StorageDestination,
        *,
        file_format: StorageFormat,
        partitioner: "dict[str, Any] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob | Awaitable[StorageBridgeJob]":
        """Load artifacts from storage into the target table."""

        self._raise_not_implemented("load_from_storage")
        raise NotImplementedError

    def stage_artifact(self, request: "dict[str, Any]") -> "dict[str, Any]":
        """Provision staging metadata for adapters that require remote URIs."""

        self._raise_not_implemented("stage_artifact")
        raise NotImplementedError

    def flush_staging_artifacts(self, artifacts: "list[dict[str, Any]]", *, error: Exception | None = None) -> None:
        """Clean up staged artifacts after a job completes."""

        if artifacts:
            self._raise_not_implemented("flush_staging_artifacts")

    def get_storage_job(self, job_id: str) -> StorageBridgeJob | None:
        """Fetch a previously created job handle."""

        return None

    def _storage_pipeline(self) -> "SyncStoragePipeline | AsyncStoragePipeline":
        factory = self.storage_pipeline_factory
        if factory is None:
            if getattr(self, "is_async", False):
                return AsyncStoragePipeline()
            return SyncStoragePipeline()
        return factory()

    def _raise_not_implemented(self, capability: str) -> None:
        msg = f"{capability} is not implemented for this driver"
        remediation = "Override StorageDriverMixin methods on the adapter to enable this capability."
        raise StorageCapabilityError(msg, capability=capability, remediation=remediation)

    def _require_capability(self, capability_flag: str) -> None:
        capabilities = self.storage_capabilities()
        if capabilities.get(capability_flag, False):
            return
        human_label = CAPABILITY_HINTS.get(capability_flag, capability_flag)
        remediation = "Check adapter supports this capability or stage artifacts via storage pipeline."
        msg = f"{human_label} is not available for this adapter"
        raise StorageCapabilityError(msg, capability=capability_flag, remediation=remediation)

    def _attach_partition_telemetry(self, telemetry: StorageTelemetry, partitioner: "dict[str, Any] | None") -> None:
        if not partitioner:
            return
        extra = dict(telemetry.get("extra", {}))
        extra["partitioner"] = partitioner
        telemetry["extra"] = extra

    def _create_storage_job(
        self, produced: StorageTelemetry, provided: StorageTelemetry | None = None, *, status: str = "completed"
    ) -> StorageBridgeJob:
        merged = cast("StorageTelemetry", dict(produced))
        if provided:
            source_bytes = provided.get("bytes_processed")
            if source_bytes is not None:
                merged["bytes_processed"] = int(merged.get("bytes_processed", 0)) + int(source_bytes)
            extra = dict(merged.get("extra", {}))
            extra["source"] = provided
            merged["extra"] = extra
        return create_storage_bridge_job(status, merged)

    def _write_result_to_storage_sync(
        self,
        result: "ArrowResult",
        destination: StorageDestination,
        *,
        format_hint: StorageFormat | None = None,
        storage_options: "dict[str, Any] | None" = None,
        pipeline: "SyncStoragePipeline | None" = None,
    ) -> StorageTelemetry:
        runtime = self.observability
        span = runtime.start_storage_span(
            "write", destination=self._stringify_storage_target(destination), format_label=format_hint
        )
        try:
            telemetry = result.write_to_storage_sync(
                destination, format_hint=format_hint, storage_options=storage_options, pipeline=pipeline
            )
        except Exception as exc:  # pragma: no cover - passthrough
            runtime.end_storage_span(span, error=exc)
            raise
        telemetry = runtime.annotate_storage_telemetry(telemetry)
        runtime.end_storage_span(span, telemetry=telemetry)
        return telemetry

    async def _write_result_to_storage_async(
        self,
        result: "ArrowResult",
        destination: StorageDestination,
        *,
        format_hint: StorageFormat | None = None,
        storage_options: "dict[str, Any] | None" = None,
        pipeline: "AsyncStoragePipeline | None" = None,
    ) -> StorageTelemetry:
        runtime = self.observability
        span = runtime.start_storage_span(
            "write", destination=self._stringify_storage_target(destination), format_label=format_hint
        )
        try:
            telemetry = await result.write_to_storage_async(
                destination, format_hint=format_hint, storage_options=storage_options, pipeline=pipeline
            )
        except Exception as exc:  # pragma: no cover - passthrough
            runtime.end_storage_span(span, error=exc)
            raise
        telemetry = runtime.annotate_storage_telemetry(telemetry)
        runtime.end_storage_span(span, telemetry=telemetry)
        return telemetry

    def _read_arrow_from_storage_sync(
        self, source: StorageDestination, *, file_format: StorageFormat, storage_options: "dict[str, Any] | None" = None
    ) -> "tuple[ArrowTable, StorageTelemetry]":
        runtime = self.observability
        span = runtime.start_storage_span(
            "read", destination=self._stringify_storage_target(source), format_label=file_format
        )
        pipeline = cast("SyncStoragePipeline", self._storage_pipeline())
        try:
            table, telemetry = pipeline.read_arrow(source, file_format=file_format, storage_options=storage_options)
        except Exception as exc:  # pragma: no cover - passthrough
            runtime.end_storage_span(span, error=exc)
            raise
        telemetry = runtime.annotate_storage_telemetry(telemetry)
        runtime.end_storage_span(span, telemetry=telemetry)
        return table, telemetry

    async def _read_arrow_from_storage_async(
        self, source: StorageDestination, *, file_format: StorageFormat, storage_options: "dict[str, Any] | None" = None
    ) -> "tuple[ArrowTable, StorageTelemetry]":
        runtime = self.observability
        span = runtime.start_storage_span(
            "read", destination=self._stringify_storage_target(source), format_label=file_format
        )
        pipeline = cast("AsyncStoragePipeline", self._storage_pipeline())
        try:
            table, telemetry = await pipeline.read_arrow_async(
                source, file_format=file_format, storage_options=storage_options
            )
        except Exception as exc:  # pragma: no cover - passthrough
            runtime.end_storage_span(span, error=exc)
            raise
        telemetry = runtime.annotate_storage_telemetry(telemetry)
        runtime.end_storage_span(span, telemetry=telemetry)
        return table, telemetry

    @staticmethod
    def _build_ingest_telemetry(table: "ArrowTable", *, format_label: str = "arrow") -> StorageTelemetry:
        rows = int(getattr(table, "num_rows", 0))
        bytes_processed = int(getattr(table, "nbytes", 0))
        return {"rows_processed": rows, "bytes_processed": bytes_processed, "format": format_label}

    def _coerce_arrow_table(self, source: "ArrowResult | Any") -> "ArrowTable":
        ensure_pyarrow()
        import pyarrow as pa

        if hasattr(source, "get_data"):
            table = source.get_data()
            if isinstance(table, pa.Table):
                return table
            msg = "ArrowResult did not return a pyarrow.Table instance"
            raise TypeError(msg)
        if isinstance(source, pa.Table):
            return source
        if isinstance(source, pa.RecordBatch):
            return pa.Table.from_batches([source])
        if isinstance(source, Iterable):
            return pa.Table.from_pylist(list(source))
        msg = f"Unsupported Arrow source type: {type(source).__name__}"
        raise TypeError(msg)

    @staticmethod
    def _stringify_storage_target(target: StorageDestination | None) -> str | None:
        if target is None:
            return None
        if isinstance(target, Path):
            return target.as_posix()
        return str(target)

    @staticmethod
    def _arrow_table_to_rows(
        table: "ArrowTable", columns: "list[str] | None" = None
    ) -> "tuple[list[str], list[tuple[Any, ...]]]":
        ensure_pyarrow()
        resolved_columns = columns or list(table.column_names)
        if not resolved_columns:
            msg = "Arrow table has no columns to import"
            raise ValueError(msg)
        batches = table.to_pylist()
        records: list[tuple[Any, ...]] = []
        for row in batches:
            record = tuple(row.get(col) for col in resolved_columns)
            records.append(record)
        return resolved_columns, records
