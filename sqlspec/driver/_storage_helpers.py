"""Pure helper functions for storage operations.

These functions are extracted from StorageDriverMixin to eliminate
cross-trait attribute access that causes mypyc segmentation faults.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.storage import StorageBridgeJob, StorageTelemetry, create_storage_bridge_job
from sqlspec.utils.arrow_helpers import arrow_batch_to_rows as _arrow_batch_to_rows_impl
from sqlspec.utils.arrow_helpers import arrow_table_needs_parameter_preparation as _arrow_rows_need_preparation
from sqlspec.utils.arrow_helpers import arrow_table_to_rows as _arrow_table_to_rows_impl
from sqlspec.utils.arrow_helpers import build_ingest_telemetry as _ingest_telemetry_impl
from sqlspec.utils.arrow_helpers import coerce_arrow_batches as _coerce_arrow_batches_impl
from sqlspec.utils.arrow_helpers import coerce_arrow_table as _coerce_arrow_table_impl
from sqlspec.utils.arrow_helpers import records_to_arrow_table as _records_to_arrow_table_impl

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping

    from sqlspec.core.result import ArrowResult
    from sqlspec.storage import StorageDestination
    from sqlspec.typing import ArrowRecordBatch, ArrowTable


__all__ = (
    "CAPABILITY_HINTS",
    "arrow_batch_to_rows",
    "arrow_table_needs_parameter_preparation",
    "arrow_table_to_rows",
    "attach_partition_telemetry",
    "build_ingest_telemetry",
    "coerce_arrow_batches",
    "coerce_arrow_table",
    "create_storage_job",
    "ingest_telemetry_from_batches",
    "records_to_arrow_table",
    "stringify_storage_target",
)


CAPABILITY_HINTS: Final[dict[str, str]] = {
    "arrow_export_enabled": "native Arrow export",
    "arrow_import_enabled": "native Arrow import",
    "parquet_export_enabled": "native Parquet export",
    "parquet_import_enabled": "native Parquet import",
}


def stringify_storage_target(target: "StorageDestination | None") -> str | None:
    """Convert storage target to string representation.

    Args:
        target: Storage destination path or None.

    Returns:
        String representation of the path or None.
    """
    if target is None:
        return None
    if isinstance(target, Path):
        return target.as_posix()
    return str(target)


def coerce_arrow_table(source: "ArrowResult | Any") -> "ArrowTable":
    """Coerce various sources to a PyArrow Table.

    Args:
        source: ArrowResult, PyArrow Table, RecordBatch, or iterable of dicts.

    Returns:
        PyArrow Table.
    """
    return _coerce_arrow_table_impl(source)


def coerce_arrow_batches(
    source: "ArrowResult | Any", *, batch_size: int = 65_536
) -> "tuple[list[str], Iterator[ArrowRecordBatch]]":
    """Coerce an Arrow source into column names and a lazy RecordBatch iterator.

    Args:
        source: ArrowResult, RecordBatchReader, RecordBatch, Table, or row iterable.
        batch_size: Maximum rows per batch when chunking an in-memory Table.

    Returns:
        Tuple of ``(column_names, batch_iterator)``.
    """
    return _coerce_arrow_batches_impl(source, batch_size=batch_size)


def arrow_batch_to_rows(batch: "ArrowRecordBatch", columns: "list[str] | None" = None) -> "list[tuple[Any, ...]]":
    """Convert a single Arrow RecordBatch into row tuples.

    Args:
        batch: Arrow RecordBatch to convert.
        columns: Optional subset or order of columns to extract.

    Returns:
        List of row tuples.
    """
    return _arrow_batch_to_rows_impl(batch, columns)


def arrow_table_to_rows(
    table: "ArrowTable", columns: "list[str] | None" = None
) -> "tuple[list[str], list[tuple[Any, ...]]]":
    """Convert Arrow table to column names and row tuples.

    Args:
        table: Arrow table to convert.
        columns: Optional list of columns to extract. Defaults to all columns.

    Returns:
        Tuple of (column_names, list of row tuples).
    """
    return _arrow_table_to_rows_impl(table, columns)


def arrow_table_needs_parameter_preparation(table: "ArrowTable") -> bool:
    """Return whether Arrow rows may contain nested values needing preparation."""
    return _arrow_rows_need_preparation(table)


def build_ingest_telemetry(table: "ArrowTable", *, format_label: str = "arrow") -> "StorageTelemetry":
    """Build telemetry dict from Arrow table statistics.

    Args:
        table: Arrow table to extract statistics from.
        format_label: Format label for telemetry.

    Returns:
        StorageTelemetry dict with row/byte counts.
    """
    telemetry = _ingest_telemetry_impl(table, format_label=format_label)
    return cast("StorageTelemetry", telemetry)


def ingest_telemetry_from_batches(
    rows_processed: int,
    bytes_processed: int,
    *,
    duration_s: float | None = None,
    format_label: str = "arrow",
    read_duration_s: float | None = None,
    encode_duration_s: float | None = None,
    write_duration_s: float | None = None,
    commit_duration_s: float | None = None,
    shards_processed: int | None = None,
    shards_skipped: int | None = None,
    wal_bytes: int | None = None,
) -> "StorageTelemetry":
    """Build a StorageTelemetry payload from accumulated batch metrics.

    Args:
        rows_processed: Total number of rows processed across all batches.
        bytes_processed: Total uncompressed or wire bytes processed across all batches.
        duration_s: Optional total wall-clock duration in seconds.
        format_label: Payload format label (default ``"arrow"``).
        read_duration_s: Optional source read time in seconds.
        encode_duration_s: Optional payload encoding time in seconds.
        write_duration_s: Optional database/storage write time in seconds.
        commit_duration_s: Optional commit/finalization time in seconds.
        shards_processed: Optional count of processed storage shards.
        shards_skipped: Optional count of checkpoint-skipped storage shards.
        wal_bytes: Optional WAL byte delta recorded during ingestion.

    Returns:
        Populated ``StorageTelemetry`` dictionary.
    """
    telemetry: StorageTelemetry = {
        "rows_processed": int(rows_processed),
        "bytes_processed": int(bytes_processed),
        "format": format_label,
    }
    if duration_s is not None:
        telemetry["duration_s"] = float(duration_s)
    if read_duration_s is not None:
        telemetry["read_duration_s"] = float(read_duration_s)
    if encode_duration_s is not None:
        telemetry["encode_duration_s"] = float(encode_duration_s)
    if write_duration_s is not None:
        telemetry["write_duration_s"] = float(write_duration_s)
    if commit_duration_s is not None:
        telemetry["commit_duration_s"] = float(commit_duration_s)
    if shards_processed is not None:
        telemetry["shards_processed"] = int(shards_processed)
    if shards_skipped is not None:
        telemetry["shards_skipped"] = int(shards_skipped)
    if wal_bytes is not None:
        telemetry["wal_bytes"] = int(wal_bytes)
    return telemetry


def records_to_arrow_table(
    records: "Iterable[Mapping[str, Any]] | Iterable[Iterable[Any]]", columns: "list[str] | None"
) -> "ArrowTable":
    """Normalize mapping or positional records into a PyArrow table for ingest."""
    return _records_to_arrow_table_impl(records, columns)


def attach_partition_telemetry(telemetry: "StorageTelemetry", partitioner: "dict[str, object] | None") -> None:
    """Attach partitioner info to telemetry dict (mutates in place).

    Args:
        telemetry: Telemetry dict to update.
        partitioner: Partitioner configuration or None.
    """
    if not partitioner:
        return
    extra = dict(telemetry.get("extra", {}))
    extra["partitioner"] = partitioner
    telemetry["extra"] = extra


def create_storage_job(
    produced: "StorageTelemetry", provided: "StorageTelemetry | None" = None, *, status: str = "completed"
) -> "StorageBridgeJob":
    """Create a StorageBridgeJob from telemetry data.

    Args:
        produced: Telemetry from the production side of the operation.
        provided: Optional telemetry from the source side.
        status: Job status string.

    Returns:
        StorageBridgeJob instance.
    """
    merged = cast("StorageTelemetry", dict(produced))
    if provided:
        source_bytes = provided.get("bytes_processed")
        if source_bytes is not None:
            merged["bytes_processed"] = int(merged.get("bytes_processed", 0)) + int(source_bytes)
        extra = dict(merged.get("extra", {}))
        extra["source"] = provided
        merged["extra"] = extra
    return create_storage_bridge_job(status, merged)
