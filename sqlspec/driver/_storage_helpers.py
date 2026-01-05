"""Pure helper functions for storage operations.

These functions are extracted from StorageDriverMixin to eliminate
cross-trait attribute access that causes mypyc segmentation faults.
"""

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.storage import StorageBridgeJob, StorageTelemetry, create_storage_bridge_job
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.type_guards import has_arrow_table_stats, has_get_data

if TYPE_CHECKING:
    from sqlspec.core.result import ArrowResult
    from sqlspec.storage import StorageDestination
    from sqlspec.typing import ArrowTable


__all__ = (
    "CAPABILITY_HINTS",
    "arrow_table_to_rows",
    "attach_partition_telemetry",
    "build_ingest_telemetry",
    "coerce_arrow_table",
    "create_storage_job",
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

    Raises:
        TypeError: If source type is not supported.

    """
    ensure_pyarrow()
    import pyarrow as pa

    if has_get_data(source):
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


def arrow_table_to_rows(
    table: "ArrowTable", columns: "list[str] | None" = None
) -> "tuple[list[str], list[tuple[Any, ...]]]":
    """Convert Arrow table to column names and row tuples.

    Args:
        table: Arrow table to convert.
        columns: Optional list of columns to extract. Defaults to all columns.

    Returns:
        Tuple of (column_names, list of row tuples).

    Raises:
        ValueError: If table has no columns to import.

    """
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


def build_ingest_telemetry(table: "ArrowTable", *, format_label: str = "arrow") -> "StorageTelemetry":
    """Build telemetry dict from Arrow table statistics.

    Args:
        table: Arrow table to extract statistics from.
        format_label: Format label for telemetry.

    Returns:
        StorageTelemetry dict with row/byte counts.

    """
    if has_arrow_table_stats(table):
        rows = int(table.num_rows)
        bytes_processed = int(table.nbytes)
    else:
        rows = 0
        bytes_processed = 0
    return {"rows_processed": rows, "bytes_processed": bytes_processed, "format": format_label}


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
